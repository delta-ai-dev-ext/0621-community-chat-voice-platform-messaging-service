// Command server is the messaging_service entrypoint: it loads configuration,
// runs migrations, wires the HTTP edge, the projection consumers and the outbox
// relay, then serves until SIGTERM/SIGINT with an ordered graceful shutdown.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"messaging_service/internal/auth"
	"messaging_service/internal/config"
	"messaging_service/internal/database"
	"messaging_service/internal/eventbus"
	"messaging_service/internal/handler"
	"messaging_service/internal/health"
	"messaging_service/internal/models"
	"messaging_service/internal/objectstore"
	"messaging_service/internal/observability"
	"messaging_service/internal/repository"
	"messaging_service/internal/service"
)

// processedEventRetention bounds the consumer dedupe table.
const processedEventRetention = 7 * 24 * time.Hour

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "messaging_service: %v\n", err)
		os.Exit(1)
	}
}

func run(baseContext context.Context) error {
	// .env is local-development convenience only; production reads plain env.
	_ = godotenv.Load()

	cfg, err := config.LoadFromEnv()
	if err != nil {
		return err
	}
	logger := newLogger(cfg)
	defer func() { _ = logger.Sync() }()

	ctx, stop := signal.NotifyContext(baseContext, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	metrics := observability.NewMetrics(prometheus.NewRegistry())

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("main: creating postgres pool: %w", err)
	}
	defer pool.Close()

	if err := database.Migrate(ctx, cfg.DatabaseURL); err != nil {
		return err
	}
	logger.Info("migrations applied", zap.String("operation", "migrate"))

	objects, err := objectstore.NewS3(ctx, cfg, time.Now)
	if err != nil {
		return err
	}

	kafkaClient, err := eventbus.NewKafkaClient(eventbus.KafkaOptions{
		Brokers:      cfg.KafkaBrokers,
		GroupID:      cfg.KafkaConsumerGroup,
		Topics:       consumedTopics(),
		SASLUsername: cfg.KafkaSASLUsername,
		SASLPassword: cfg.KafkaSASLPassword,
	})
	if err != nil {
		return err
	}
	defer func() { _ = kafkaClient.Close() }()

	publisher, err := eventbus.NewSaramaPublisher(kafkaClient)
	if err != nil {
		return err
	}

	txManager := repository.NewTxManager(pool)
	messageRepository := repository.NewMessageRepository(pool)
	communityRepository := repository.NewCommunityProjectionRepository(pool)
	conversationRepository := repository.NewConversationProjectionRepository(pool)
	attachmentRepository := repository.NewAttachmentRepository(pool)
	rateRepository := repository.NewRateRepository(pool)
	outboxRepository := repository.NewOutboxRepository(pool)
	processedRepository := repository.NewProcessedEventRepository(pool)

	deps := service.Deps{
		Messages:      messageRepository,
		Community:     communityRepository,
		Conversations: conversationRepository,
		Attachments:   attachmentRepository,
		Rates:         rateRepository,
		Outbox:        outboxRepository,
		Tx:            txManager,
		Objects:       objects,
		Clock:         time.Now,
		Logger:        logger,
		Metrics:       metrics,
		Options: service.Options{
			MaxAttachmentBytes:   cfg.AttachmentMaxBytes,
			UploadURLTTL:         cfg.UploadURLTTL,
			DownloadURLTTL:       cfg.DownloadURLTTL,
			PendingUploadGCAfter: cfg.PendingUploadGCAfter,
			AppealURLBase:        cfg.AppealURLBase,
			PageLimitDefault:     cfg.PageLimitDefault,
			PageLimitMax:         cfg.PageLimitMax,
		},
	}

	processor := eventbus.NewProcessor(eventbus.ProcessorDeps{
		Community:     communityRepository,
		Conversations: conversationRepository,
		Processed:     processedRepository,
		Tx:            txManager,
		Metrics:       metrics,
		Logger:        logger,
	})
	consumer, err := eventbus.NewConsumer(kafkaClient, cfg.KafkaConsumerGroup, consumedTopics(), processor, logger)
	if err != nil {
		return err
	}
	lagReporter, err := eventbus.NewLagReporter(kafkaClient, cfg.KafkaConsumerGroup, consumedTopics(), metrics)
	if err != nil {
		return err
	}
	defer func() { _ = lagReporter.Close() }()

	relay := eventbus.NewRelay(outboxRepository, publisher, metrics, logger, time.Now, eventbus.RelayOptions{
		Interval: cfg.OutboxRelayInterval,
	})

	verifier, err := auth.NewVerifier(auth.Config{
		Issuer:        cfg.IdentityIssuer,
		Audience:      cfg.IdentityAudience,
		PublicKey:     cfg.IdentityPublicKey,
		NextPublicKey: cfg.IdentityNextPublicKey,
	})
	if err != nil {
		return err
	}

	checker := health.NewChecker(map[string]health.Check{
		"postgres": func(checkCtx context.Context) error { return pool.Ping(checkCtx) },
		"kafka":    publisher.Check,
		"storage":  objects.Check,
	}, 2*time.Second)

	attachmentService := service.NewAttachmentService(deps)
	router := handler.NewRouter(handler.Deps{
		Messages:     service.NewMessageService(deps),
		Attachments:  attachmentService,
		Scopes:       service.NewScopeService(deps),
		Verifier:     verifier,
		Health:       checker,
		Metrics:      metrics,
		Logger:       logger,
		ServiceToken: cfg.InternalServiceToken,
		PageLimitMax: cfg.PageLimitMax,
	})

	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	var background sync.WaitGroup
	background.Add(4)
	go func() {
		defer background.Done()
		relay.Run(ctx)
	}()
	go func() {
		defer background.Done()
		if runErr := consumer.Run(ctx); runErr != nil && ctx.Err() == nil {
			logger.Error("projection consumer stopped", zap.Error(runErr))
		}
	}()
	go func() {
		defer background.Done()
		runAttachmentGC(ctx, attachmentService, processedRepository, cfg.GCInterval, logger)
	}()
	go func() {
		defer background.Done()
		runLagReporting(ctx, lagReporter, logger)
	}()

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("http server listening", zap.String("port", cfg.Port))
		if listenErr := server.ListenAndServe(); listenErr != nil && !errors.Is(listenErr, http.ErrServerClosed) {
			serveErr <- listenErr
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received", zap.String("operation", "shutdown"))
	case err := <-serveErr:
		logger.Error("http server failed", zap.Error(err))
	}

	// Shutdown order matters: fail readiness first so the load balancer stops
	// routing, drain HTTP, then stop consumers (committing offsets), flush the
	// outbox and finally release clients.
	checker.BeginShutdown()

	drainCtx, cancelDrain := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelDrain()
	if err := server.Shutdown(drainCtx); err != nil {
		logger.Warn("http drain did not finish cleanly", zap.Error(err))
	}
	if err := consumer.Close(); err != nil {
		logger.Warn("closing consumer group", zap.Error(err))
	}
	background.Wait()

	flushCtx, cancelFlush := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	relay.Flush(flushCtx)
	cancelFlush()

	if err := publisher.Close(); err != nil {
		logger.Warn("closing kafka producer", zap.Error(err))
	}
	logger.Info("shutdown complete", zap.String("operation", "shutdown"))
	return nil
}

// consumedTopics are the topics the projection consumers subscribe to.
func consumedTopics() []string {
	return []string{models.TopicCommunityEvents, models.TopicSocialEvents, models.TopicModerationEvents}
}

// runAttachmentGC sweeps never-attached uploads and prunes the dedupe table on a
// fixed interval. Both jobs are idempotent and safe to run on every replica.
func runAttachmentGC(ctx context.Context, attachments *service.AttachmentService,
	processed repository.ProcessedEventRepository, interval time.Duration, logger *zap.Logger) {
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweepCtx, cancel := context.WithTimeout(ctx, interval)
			collected, err := attachments.SweepPendingUploads(sweepCtx, 100)
			if err != nil && ctx.Err() == nil {
				logger.Warn("attachment gc sweep failed", zap.Error(err))
			}
			if collected > 0 {
				logger.Info("attachment gc swept uploads", zap.Int("count", collected))
			}
			if _, pruneErr := processed.PruneOlderThan(sweepCtx, time.Now().UTC().Add(-processedEventRetention)); pruneErr != nil && ctx.Err() == nil {
				logger.Warn("processed event pruning failed", zap.Error(pruneErr))
			}
			cancel()
		}
	}
}

// runLagReporting samples consumer lag for the lag gauge.
func runLagReporting(ctx context.Context, reporter *eventbus.LagReporter, logger *zap.Logger) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := reporter.Report(ctx); err != nil && ctx.Err() == nil {
				logger.Debug("consumer lag sample failed", zap.Error(err))
			}
		}
	}
}

// newLogger builds the structured JSON logger used across the service.
func newLogger(cfg *config.Config) *zap.Logger {
	serviceName := cfg.ServiceName
	if serviceName == "" {
		serviceName = models.ProducerName
	}
	logger, err := zap.NewProduction(zap.Fields(zap.String("service_name", serviceName)))
	if err != nil {
		return zap.NewNop()
	}
	return logger
}
