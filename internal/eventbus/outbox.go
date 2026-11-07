// Package eventbus owns the Kafka edge: the transactional-outbox relay that
// publishes EVT-009 and the projection consumers that apply EVT-002/003/005/008.
package eventbus

import (
	"context"
	"time"

	"go.uber.org/zap"

	"messaging_service/internal/models"
	"messaging_service/internal/observability"
	"messaging_service/internal/repository"
)

// Publisher is the Kafka producer port. Tests substitute a fake that records
// published events.
type Publisher interface {
	// Publish sends one event with the container id as the message key, so
	// per-container ordering is preserved by the broker.
	Publish(ctx context.Context, topic, key string, event models.Event) error
	// Check verifies broker connectivity for the readiness probe.
	Check(ctx context.Context) error
	// Close releases producer resources.
	Close() error
}

// Relay publishes unpublished outbox entries in id order. Publishing is at
// least once: an entry is only marked published after the broker acknowledged
// it, so a crash between commit and publish never loses an event and a
// re-publish is deduped downstream by `event_id`.
type Relay struct {
	outbox    repository.OutboxRepository
	publisher Publisher
	metrics   *observability.Metrics
	logger    *zap.Logger
	clock     func() time.Time
	interval  time.Duration
	batch     int
}

// RelayOptions configures the relay loop.
type RelayOptions struct {
	Interval time.Duration
	Batch    int
}

// NewRelay builds the outbox relay.
func NewRelay(outbox repository.OutboxRepository, publisher Publisher, metrics *observability.Metrics,
	logger *zap.Logger, clock func() time.Time, options RelayOptions) *Relay {
	if logger == nil {
		logger = zap.NewNop()
	}
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	if options.Interval <= 0 {
		options.Interval = 500 * time.Millisecond
	}
	if options.Batch <= 0 {
		options.Batch = 100
	}
	return &Relay{
		outbox: outbox, publisher: publisher, metrics: metrics, logger: logger,
		clock: clock, interval: options.Interval, batch: options.Batch,
	}
}

// Run polls the outbox until the context is cancelled.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.PublishBatch(ctx); err != nil && ctx.Err() == nil {
				r.logger.Warn("outbox relay batch failed", zap.Error(err))
			}
		}
	}
}

// PublishBatch publishes at most one batch of unpublished entries. It stops at
// the first failed publish so events for one container are never reordered.
func (r *Relay) PublishBatch(ctx context.Context) error {
	entries, err := r.outbox.ListUnpublished(ctx, r.batch)
	if err != nil {
		return err
	}
	if stopped, err := r.publishEntries(ctx, entries); err != nil {
		return err
	} else if stopped {
		return nil
	}
	return r.reportDepth(ctx)
}

// publishEntries publishes the given entries in id order, stopping at the first
// broker failure so per-container ordering is preserved. It reports whether it
// stopped early because a publish failed.
func (r *Relay) publishEntries(ctx context.Context, entries []models.OutboxEntry) (bool, error) {
	for _, entry := range entries {
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		event := models.Event{Envelope: entry.Envelope, Payload: entry.Payload}
		if publishErr := r.publisher.Publish(ctx, entry.Topic, entry.MessageKey, event); publishErr != nil {
			r.metrics.IncOutboxPublishFailure()
			r.logger.Warn("outbox publish failed",
				zap.String("trace_id", entry.Envelope.TraceID),
				zap.String("topic", entry.Topic),
				zap.Int64("outbox_id", entry.ID),
				zap.Int("attempts", entry.Attempts),
				zap.Error(publishErr))
			if recordErr := r.outbox.RecordAttempt(ctx, entry.ID); recordErr != nil {
				return true, recordErr
			}
			return true, nil
		}
		if markErr := r.outbox.MarkPublished(ctx, entry.ID, r.clock()); markErr != nil {
			return true, markErr
		}
	}
	return false, nil
}

// Flush pushes whatever is unpublished within the given budget. It is called
// during shutdown, after which no new state changes can occur.
func (r *Relay) Flush(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		entries, err := r.outbox.ListUnpublished(ctx, r.batch)
		if err != nil {
			r.logger.Warn("outbox flush could not read entries", zap.Error(err))
			return
		}
		if len(entries) == 0 {
			return
		}
		stopped, publishErr := r.publishEntries(ctx, entries)
		if publishErr != nil {
			r.logger.Warn("outbox flush batch failed", zap.Error(publishErr))
			return
		}
		if stopped {
			return
		}
	}
}

// reportDepth publishes the relay backlog depth.
func (r *Relay) reportDepth(ctx context.Context) error {
	depth, err := r.outbox.CountUnpublished(ctx)
	if err != nil {
		return err
	}
	r.metrics.SetOutboxUnpublished(float64(depth))
	return nil
}
