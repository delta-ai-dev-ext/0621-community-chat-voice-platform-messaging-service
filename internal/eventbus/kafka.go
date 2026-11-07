package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	"messaging_service/internal/models"
	"messaging_service/internal/observability"
)

// KafkaOptions configures the shared Kafka client.
type KafkaOptions struct {
	Brokers      []string
	GroupID      string
	Topics       []string
	SASLUsername string
	SASLPassword string
}

// NewKafkaClient builds the shared client used by the producer, the projection
// consumer group and the lag reporter.
func NewKafkaClient(options KafkaOptions) (sarama.Client, error) {
	settings := sarama.NewConfig()
	settings.Version = sarama.V2_8_0_0
	settings.ClientID = models.ProducerName
	// Exactly-once-ish producer: the outbox relay needs broker acknowledgement
	// before an entry is marked published.
	settings.Producer.RequiredAcks = sarama.WaitForAll
	settings.Producer.Retry.Max = 5
	settings.Producer.Return.Successes = true
	settings.Producer.Idempotent = true
	settings.Net.MaxOpenRequests = 1
	settings.Consumer.Offsets.Initial = sarama.OffsetOldest
	settings.Consumer.Return.Errors = true
	if options.SASLUsername != "" {
		settings.Net.SASL.Enable = true
		settings.Net.SASL.User = options.SASLUsername
		settings.Net.SASL.Password = options.SASLPassword
		settings.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}
	client, err := sarama.NewClient(options.Brokers, settings)
	if err != nil {
		return nil, fmt.Errorf("eventbus: connecting to kafka: %w", err)
	}
	return client, nil
}

// saramaPublisher publishes outbox entries with the envelope mirrored into the
// Kafka headers for routing and observability.
type saramaPublisher struct {
	client   sarama.Client
	producer sarama.SyncProducer
}

// NewSaramaPublisher builds the Kafka publisher over an existing client.
func NewSaramaPublisher(client sarama.Client) (Publisher, error) {
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("eventbus: building producer: %w", err)
	}
	return &saramaPublisher{client: client, producer: producer}, nil
}

// Publish sends one event keyed by the container id.
func (p *saramaPublisher) Publish(_ context.Context, topic, key string, event models.Event) error {
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("eventbus: encoding event: %w", err)
	}
	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
		Headers: []sarama.RecordHeader{
			{Key: []byte("trace_id"), Value: []byte(event.Envelope.TraceID)},
			{Key: []byte("event_type"), Value: []byte(event.Envelope.EventType)},
		},
	}
	if _, _, sendErr := p.producer.SendMessage(message); sendErr != nil {
		return fmt.Errorf("eventbus: publishing %s: %w", event.Envelope.EventType, sendErr)
	}
	return nil
}

// Check verifies broker connectivity for /readyz.
func (p *saramaPublisher) Check(context.Context) error {
	if err := p.client.RefreshMetadata(); err != nil {
		return fmt.Errorf("eventbus: kafka unreachable: %w", err)
	}
	return nil
}

// Close releases producer resources.
func (p *saramaPublisher) Close() error { return p.producer.Close() }

// Consumer runs the projection consumer group over the three consumed topics.
type Consumer struct {
	group     sarama.ConsumerGroup
	processor *Processor
	topics    []string
	logger    *zap.Logger
}

// NewConsumer builds the projection consumer group.
func NewConsumer(client sarama.Client, groupID string, topics []string, processor *Processor, logger *zap.Logger) (*Consumer, error) {
	group, err := sarama.NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		return nil, fmt.Errorf("eventbus: building consumer group: %w", err)
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Consumer{group: group, processor: processor, topics: topics, logger: logger}, nil
}

// Run consumes until the context is cancelled or the group closes.
func (c *Consumer) Run(ctx context.Context) error {
	handler := &groupHandler{processor: c.processor, logger: c.logger}
	for {
		if err := c.group.Consume(ctx, c.topics, handler); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("eventbus: consuming: %w", err)
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// Close stops the group; closing commits offsets already marked by the session.
func (c *Consumer) Close() error { return c.group.Close() }

// groupHandler applies each message through the processor and only marks it for
// commit once it was applied (or dead-lettered) successfully.
type groupHandler struct {
	processor *Processor
	logger    *zap.Logger
}

// Setup implements sarama.ConsumerGroupHandler.
func (h *groupHandler) Setup(sarama.ConsumerGroupSession) error { return nil }

// Cleanup implements sarama.ConsumerGroupHandler.
func (h *groupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim implements sarama.ConsumerGroupHandler.
func (h *groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if err := h.processor.Handle(session.Context(), message.Topic, message.Value); err != nil {
				h.logger.Warn("projection event failed",
					zap.String("topic", message.Topic),
					zap.Int32("partition", message.Partition),
					zap.Error(err))
				return err
			}
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

// LagReporter publishes `kafka_consumer_lag` per consumed topic.
type LagReporter struct {
	admin   sarama.ClusterAdmin
	client  sarama.Client
	groupID string
	topics  []string
	metrics *observability.Metrics
}

// NewLagReporter builds the lag reporter.
func NewLagReporter(client sarama.Client, groupID string, topics []string, metrics *observability.Metrics) (*LagReporter, error) {
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("eventbus: building cluster admin: %w", err)
	}
	return &LagReporter{admin: admin, client: client, groupID: groupID, topics: topics, metrics: metrics}, nil
}

// Report takes one lag sample per topic.
func (r *LagReporter) Report(context.Context) error {
	for _, topic := range r.topics {
		partitions, err := r.client.Partitions(topic)
		if err != nil {
			return fmt.Errorf("eventbus: reading partitions of %s: %w", topic, err)
		}
		offsets, err := r.admin.ListConsumerGroupOffsets(r.groupID, map[string][]int32{topic: partitions})
		if err != nil {
			return fmt.Errorf("eventbus: reading committed offsets of %s: %w", topic, err)
		}
		var lag int64
		for _, partition := range partitions {
			newest, offsetErr := r.client.GetOffset(topic, partition, sarama.OffsetNewest)
			if offsetErr != nil {
				return fmt.Errorf("eventbus: reading newest offset of %s: %w", topic, offsetErr)
			}
			committed := int64(0)
			if block := offsets.GetBlock(topic, partition); block != nil {
				committed = block.Offset
			}
			if newest > committed {
				lag += newest - committed
			}
		}
		r.metrics.SetKafkaLag(topic, r.groupID, float64(lag))
	}
	return nil
}

// Close releases the cluster admin.
func (r *LagReporter) Close() error { return r.admin.Close() }
