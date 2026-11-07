package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"messaging_service/internal/models"
)

// outboxRepository is the pgx-backed transactional outbox. Append runs inside
// the caller's transaction (base.q resolves the transaction from the context),
// which is what makes a committed message and its EVT-009 atomic.
type outboxRepository struct {
	base
}

// NewOutboxRepository builds the outbox repository.
func NewOutboxRepository(db Querier) OutboxRepository { return &outboxRepository{base{db: db}} }

// Append writes one event into the outbox.
func (r *outboxRepository) Append(ctx context.Context, entry models.OutboxEntryView) error {
	envelope, err := json.Marshal(entry.Envelope)
	if err != nil {
		return fmt.Errorf("repository: encoding outbox envelope: %w", err)
	}
	const query = `INSERT INTO event_outbox (topic, message_key, envelope, payload)
VALUES ($1, $2, $3::jsonb, $4::jsonb)`
	if _, err := r.q(ctx).Exec(ctx, query, entry.Topic, entry.MessageKey, string(envelope), string(entry.Payload)); err != nil {
		return fmt.Errorf("repository: appending outbox entry: %w", err)
	}
	return nil
}

// ListUnpublished returns the oldest unpublished entries, preserving per-container
// ordering because the relay publishes in id order.
func (r *outboxRepository) ListUnpublished(ctx context.Context, limit int) ([]models.OutboxEntry, error) {
	const query = `SELECT id, topic, message_key, envelope, payload, attempts, published_at
FROM event_outbox WHERE published_at IS NULL ORDER BY id ASC LIMIT $1`
	rows, err := r.q(ctx).Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("repository: listing outbox entries: %w", err)
	}
	defer rows.Close()
	var entries []models.OutboxEntry
	for rows.Next() {
		var (
			entry          models.OutboxEntry
			envelope, body []byte
		)
		if err := rows.Scan(&entry.ID, &entry.Topic, &entry.MessageKey, &envelope, &body, &entry.Attempts,
			&entry.PublishedAt); err != nil {
			return nil, fmt.Errorf("repository: scanning outbox entry: %w", err)
		}
		if err := json.Unmarshal(envelope, &entry.Envelope); err != nil {
			return nil, fmt.Errorf("repository: decoding outbox envelope: %w", err)
		}
		entry.Payload = append(json.RawMessage(nil), body...)
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: reading outbox entries: %w", err)
	}
	return entries, nil
}

// MarkPublished stamps an entry as published and counts the attempt.
func (r *outboxRepository) MarkPublished(ctx context.Context, id int64, publishedAt time.Time) error {
	const query = `UPDATE event_outbox SET published_at = $2, attempts = attempts + 1 WHERE id = $1`
	if _, err := r.q(ctx).Exec(ctx, query, id, publishedAt); err != nil {
		return fmt.Errorf("repository: marking outbox entry published: %w", err)
	}
	return nil
}

// RecordAttempt counts a failed publish so repeated failures are visible.
func (r *outboxRepository) RecordAttempt(ctx context.Context, id int64) error {
	const query = `UPDATE event_outbox SET attempts = attempts + 1 WHERE id = $1`
	if _, err := r.q(ctx).Exec(ctx, query, id); err != nil {
		return fmt.Errorf("repository: recording outbox attempt: %w", err)
	}
	return nil
}

// CountUnpublished reports the relay backlog depth for the depth/age gauge.
func (r *outboxRepository) CountUnpublished(ctx context.Context) (int64, error) {
	const query = `SELECT count(*) FROM event_outbox WHERE published_at IS NULL`
	var count int64
	if err := r.q(ctx).QueryRow(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("repository: counting outbox entries: %w", err)
	}
	return count, nil
}
