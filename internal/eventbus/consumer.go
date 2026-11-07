package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"messaging_service/internal/models"
	"messaging_service/internal/observability"
	"messaging_service/internal/repository"
)

// defaultMaxAttempts bounds how often one event may fail before it is routed to
// the dead-letter logging path instead of blocking its partition.
const defaultMaxAttempts = 3

// Processor applies consumed projection events. Every apply is version-guarded
// and deduped on `envelope.event_id`, so at-least-once delivery converges
// (ADR-001).
type Processor struct {
	community     repository.CommunityProjectionRepository
	conversations repository.ConversationProjectionRepository
	processed     repository.ProcessedEventRepository
	tx            repository.TransactionManager
	metrics       *observability.Metrics
	logger        *zap.Logger
	maxAttempts   int

	mu       sync.Mutex
	attempts map[uuid.UUID]int
}

// ProcessorDeps is the dependency set of the projection processor.
type ProcessorDeps struct {
	Community     repository.CommunityProjectionRepository
	Conversations repository.ConversationProjectionRepository
	Processed     repository.ProcessedEventRepository
	Tx            repository.TransactionManager
	Metrics       *observability.Metrics
	Logger        *zap.Logger
	MaxAttempts   int
}

// NewProcessor builds the projection processor.
func NewProcessor(deps ProcessorDeps) *Processor {
	logger := deps.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	maxAttempts := deps.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}
	return &Processor{
		community:     deps.Community,
		conversations: deps.Conversations,
		processed:     deps.Processed,
		tx:            deps.Tx,
		metrics:       deps.Metrics,
		logger:        logger,
		maxAttempts:   maxAttempts,
		attempts:      make(map[uuid.UUID]int),
	}
}

// Handle decodes and applies one Kafka message. A nil return means the offset may
// be committed, including for messages that are duplicates, unknown event types
// or that exhausted their attempts (dead-lettered with the full envelope).
func (p *Processor) Handle(ctx context.Context, topic string, value []byte) error {
	var event models.Event
	if err := json.Unmarshal(value, &event); err != nil {
		p.logger.Error("dropping undecodable event", zap.String("topic", topic), zap.Error(err))
		p.metrics.IncConsumerFailure(topic)
		return nil
	}
	if err := event.Envelope.Validate(); err != nil {
		p.logger.Error("dropping event with invalid envelope",
			zap.String("topic", topic), zap.ByteString("payload", value), zap.Error(err))
		p.metrics.IncConsumerFailure(topic)
		return nil
	}

	alreadyProcessed, err := p.processed.IsProcessed(ctx, event.Envelope.EventID)
	if err != nil {
		return p.fail(topic, event, err)
	}
	if alreadyProcessed {
		p.logger.Debug("skipping already processed event",
			zap.String("event_id", event.Envelope.EventID.String()),
			zap.String("trace_id", event.Envelope.TraceID))
		return nil
	}

	err = p.tx.InTx(ctx, func(txCtx context.Context) error {
		if applyErr := p.apply(txCtx, event); applyErr != nil {
			return applyErr
		}
		if _, markErr := p.processed.MarkProcessed(txCtx, event.Envelope.EventID, event.Envelope.EventType); markErr != nil {
			return markErr
		}
		return nil
	})
	if err != nil {
		return p.fail(topic, event, err)
	}
	p.reset(event.Envelope.EventID)
	return nil
}

// apply dispatches one validated event to its projection.
func (p *Processor) apply(ctx context.Context, event models.Event) error {
	switch event.Envelope.EventType {
	case models.EventCommunityMemberChanged:
		var payload models.CommunityMemberChangedPayload
		if err := decodePayload(event, &payload); err != nil {
			return err
		}
		if err := payload.Validate(); err != nil {
			return err
		}
		_, err := p.community.UpsertMembership(ctx, models.CommunityMember{
			CommunityID: payload.CommunityID, UserID: payload.UserID, Status: payload.Status,
			IsBanned: payload.IsBanned, MutedUntil: payload.MutedUntil, RoleIDs: payload.RoleIDs,
			MemberVersion: payload.MemberVersion,
		})
		return err

	case models.EventCommunityChannelChanged:
		var payload models.CommunityChannelChangedPayload
		if err := decodePayload(event, &payload); err != nil {
			return err
		}
		if err := payload.Validate(); err != nil {
			return err
		}
		if payload.Kind != "text" {
			// Voice channels are not messaging containers; nothing to project.
			return nil
		}
		if payload.Change == "deleted" {
			_, err := p.community.DeleteTextChannel(ctx, payload.ChannelID, payload.ChannelVersion)
			return err
		}
		_, err := p.community.UpsertTextChannel(ctx, models.TextChannel{
			ChannelID: payload.ChannelID, CommunityID: payload.CommunityID, Name: payload.Name,
			Position: payload.Position, ChannelVersion: payload.ChannelVersion,
		})
		return err

	case models.EventAutomodRulesChanged:
		var payload models.AutomodRulesChangedPayload
		if err := decodePayload(event, &payload); err != nil {
			return err
		}
		if err := payload.Validate(); err != nil {
			return err
		}
		_, err := p.community.UpsertAutomodRuleSet(ctx, models.AutomodRuleSet{
			CommunityID: payload.CommunityID, RuleSetVersion: payload.RuleSetVersion,
			Spam: payload.Spam, BannedWords: payload.BannedWords, ExemptRoleIDs: payload.ExemptRoleIDs,
		})
		return err

	case models.EventConversationChanged:
		var payload models.ConversationChangedPayload
		if err := decodePayload(event, &payload); err != nil {
			return err
		}
		if err := payload.Validate(); err != nil {
			return err
		}
		_, err := p.conversations.UpsertConversation(ctx, models.Conversation{
			ConversationID: payload.ConversationID, Kind: payload.Kind, Name: payload.Name,
			ConversationVersion: payload.ConversationVersion, ParticipantIDs: payload.ParticipantUserIDs,
		})
		return err

	default:
		// Unknown types are future events; committing them keeps the partition
		// moving instead of replaying them forever.
		p.logger.Warn("skipping unsupported event type",
			zap.String("event_type", event.Envelope.EventType),
			zap.String("trace_id", event.Envelope.TraceID))
		return nil
	}
}

// fail records an attempt and decides whether to retry or dead-letter. It adopts
// the causing event's trace id so the failure is traceable end to end.
func (p *Processor) fail(topic string, event models.Event, cause error) error {
	p.metrics.IncConsumerFailure(topic)
	p.mu.Lock()
	p.attempts[event.Envelope.EventID]++
	attempts := p.attempts[event.Envelope.EventID]
	p.mu.Unlock()

	if attempts < p.maxAttempts {
		p.logger.Warn("event application failed, will retry",
			zap.String("event_id", event.Envelope.EventID.String()),
			zap.String("event_type", event.Envelope.EventType),
			zap.String("trace_id", event.Envelope.TraceID),
			zap.Int("attempt", attempts),
			zap.Error(cause))
		return fmt.Errorf("eventbus: applying %s: %w", event.Envelope.EventType, cause)
	}

	// Dead-letter path: log the full envelope and commit so the partition is not
	// blocked indefinitely.
	p.logger.Error("event dead-lettered after repeated failures",
		zap.String("topic", topic),
		zap.String("event_id", event.Envelope.EventID.String()),
		zap.String("event_type", event.Envelope.EventType),
		zap.String("trace_id", event.Envelope.TraceID),
		zap.String("correlation_id", event.Envelope.CorrelationID.String()),
		zap.Int("attempts", attempts),
		zap.Error(cause))
	p.reset(event.Envelope.EventID)
	return nil
}

// reset forgets the retry counter for an event.
func (p *Processor) reset(eventID uuid.UUID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.attempts, eventID)
}

// decodePayload decodes an event payload strictly enough to catch contract drift.
func decodePayload(event models.Event, target any) error {
	if len(event.Payload) == 0 {
		return fmt.Errorf("eventbus: %s payload is empty", event.Envelope.EventType)
	}
	if err := json.Unmarshal(event.Payload, target); err != nil {
		return fmt.Errorf("eventbus: decoding %s payload: %w", event.Envelope.EventType, err)
	}
	return nil
}
