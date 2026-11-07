package models

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// Kafka topics this service consumes from and produces to.
const (
	TopicCommunityEvents  = "community_events"
	TopicSocialEvents     = "social_events"
	TopicModerationEvents = "moderation_events"
	TopicMessageEvents    = "message_events"
)

// Event type names (EVENT_CONTRACTS.md).
const (
	EventCommunityMemberChanged  = "community_member_changed"
	EventCommunityChannelChanged = "community_channel_changed"
	EventAutomodRulesChanged     = "automod_rules_changed"
	EventConversationChanged     = "conversation_changed"
	EventMessageChanged          = "message_changed"
)

// ProducerName is this service's `producer` envelope value.
const ProducerName = "messaging_service"

// Envelope is the mandatory nine-field event envelope. It is modeled exactly
// once and reused by every consume and produce path; payloads never repeat an
// envelope field at their top level.
type Envelope struct {
	EventID       uuid.UUID  `json:"event_id"`
	EventType     string     `json:"event_type"`
	EventVersion  int        `json:"event_version"`
	OccurredAt    time.Time  `json:"occurred_at"`
	Producer      string     `json:"producer"`
	TraceID       string     `json:"trace_id"`
	CorrelationID uuid.UUID  `json:"correlation_id"`
	CausationID   *uuid.UUID `json:"causation_id"`
	SchemaRef     string     `json:"schema_ref"`
}

// Event is one Kafka message body: envelope plus payload.
type Event struct {
	Envelope Envelope        `json:"envelope"`
	Payload  json.RawMessage `json:"payload"`
}

// EnvelopeInput carries the caller-controlled parts of a new envelope. The
// builder fills the rest (event id, producer, schema ref, occurred_at default).
type EnvelopeInput struct {
	EventType     string
	EventVersion  int
	OccurredAt    time.Time
	TraceID       string
	CorrelationID uuid.UUID
	CausationID   *uuid.UUID
}

// SchemaRefFor renders the canonical `schema_ref` for an event type/version.
func SchemaRefFor(eventType string, version int) string {
	return fmt.Sprintf("EVENT_CONTRACTS.md#%s-v%d", eventType, version)
}

var (
	eventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	traceIDPattern   = regexp.MustCompile(`^[0-9a-f]{32}$`)
	schemaRefPattern = regexp.MustCompile(`^EVENT_CONTRACTS\.md#[a-z][a-z0-9_]*-v[0-9]+$`)
)

// MintTraceID returns a fresh 32-hex-character trace id. It is used at the edge
// of flows that do not originate from an HTTP request (for example the
// attachment GC sweep), because `trace_id` must never be empty.
func MintTraceID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand failure is unrecoverable and must not silently produce an
		// empty trace id; fall back to a time-derived value.
		return fmt.Sprintf("%032x", time.Now().UTC().UnixNano())
	}
	return hex.EncodeToString(buf[:])
}

// NewEnvelope builds a valid envelope, minting a trace id when the caller has
// none and defaulting the occurrence time to now.
func NewEnvelope(in EnvelopeInput, now time.Time) (Envelope, error) {
	if !eventTypePattern.MatchString(in.EventType) {
		return Envelope{}, fmt.Errorf("invalid event type %q", in.EventType)
	}
	version := in.EventVersion
	if version == 0 {
		version = 1
	}
	if version < 1 {
		return Envelope{}, fmt.Errorf("invalid event version %d", version)
	}
	traceID := in.TraceID
	if traceID == "" {
		traceID = MintTraceID()
	}
	if !traceIDPattern.MatchString(traceID) {
		return Envelope{}, fmt.Errorf("invalid trace id %q", traceID)
	}
	correlationID := in.CorrelationID
	if correlationID == uuid.Nil {
		correlationID = uuid.New()
	}
	occurredAt := in.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = now
	}
	return Envelope{
		EventID:       uuid.New(),
		EventType:     in.EventType,
		EventVersion:  version,
		OccurredAt:    occurredAt.UTC(),
		Producer:      ProducerName,
		TraceID:       traceID,
		CorrelationID: correlationID,
		CausationID:   in.CausationID,
		SchemaRef:     SchemaRefFor(in.EventType, version),
	}, nil
}

// Validate enforces the envelope contract on consumed messages so a malformed
// event never reaches a projection.
func (e Envelope) Validate() error {
	if e.EventID == uuid.Nil {
		return fmt.Errorf("envelope: event_id is required")
	}
	if !eventTypePattern.MatchString(e.EventType) {
		return fmt.Errorf("envelope: invalid event_type %q", e.EventType)
	}
	if e.EventVersion < 1 {
		return fmt.Errorf("envelope: event_version must be >= 1")
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("envelope: occurred_at is required")
	}
	if e.Producer == "" {
		return fmt.Errorf("envelope: producer is required")
	}
	if !traceIDPattern.MatchString(e.TraceID) {
		return fmt.Errorf("envelope: trace_id must be 32 lowercase hex characters")
	}
	if e.CorrelationID == uuid.Nil {
		return fmt.Errorf("envelope: correlation_id is required")
	}
	if !schemaRefPattern.MatchString(e.SchemaRef) {
		return fmt.Errorf("envelope: invalid schema_ref %q", e.SchemaRef)
	}
	return nil
}
