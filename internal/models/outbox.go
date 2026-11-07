package models

import (
	"encoding/json"
	"time"
)

// OutboxEntry is one row of the transactional outbox. Envelope and Payload are
// the exact JSON documents that will be produced to Kafka, so a re-publish
// after a crash is byte-identical and deduped downstream by `event_id`.
type OutboxEntry struct {
	ID          int64
	Topic       string
	MessageKey  string
	Envelope    Envelope
	Payload     json.RawMessage
	Attempts    int
	PublishedAt *time.Time
}

// OutboxEntryView is the in-memory shape written by the send/edit/delete paths
// before it is persisted.
type OutboxEntryView struct {
	Topic      string
	MessageKey string
	Envelope   Envelope
	Payload    json.RawMessage
}
