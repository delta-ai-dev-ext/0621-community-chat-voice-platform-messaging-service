package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"messaging_service/internal/models"
)

// processedEventRepository is the consumer-side dedupe table. It is written in
// the same transaction as the projection change, so a committed projection
// always has its dedupe marker.
type processedEventRepository struct {
	base
}

// NewProcessedEventRepository builds the dedupe repository.
func NewProcessedEventRepository(db Querier) ProcessedEventRepository {
	return &processedEventRepository{base{db: db}}
}

// IsProcessed reports whether the event id was already applied.
func (r *processedEventRepository) IsProcessed(ctx context.Context, eventID uuid.UUID) (bool, error) {
	const query = `SELECT EXISTS (SELECT 1 FROM processed_events WHERE event_id = $1)`
	var exists bool
	if err := r.q(ctx).QueryRow(ctx, query, uuidParam(eventID)).Scan(&exists); err != nil {
		return false, fmt.Errorf("repository: checking processed event: %w", err)
	}
	return exists, nil
}

// MarkProcessed records an event id; inserted is false for a duplicate, which
// lets the consumer report the skip without treating it as a failure.
func (r *processedEventRepository) MarkProcessed(ctx context.Context, eventID uuid.UUID, eventType string) (bool, error) {
	const query = `INSERT INTO processed_events (event_id, event_type) VALUES ($1, $2) ON CONFLICT DO NOTHING`
	tag, err := r.q(ctx).Exec(ctx, query, uuidParam(eventID), eventType)
	if err != nil {
		return false, fmt.Errorf("repository: marking event processed: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// PruneOlderThan trims the dedupe table beyond the retention window.
func (r *processedEventRepository) PruneOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	const query = `DELETE FROM processed_events WHERE processed_at < $1`
	tag, err := r.q(ctx).Exec(ctx, query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("repository: pruning processed events: %w", err)
	}
	return tag.RowsAffected(), nil
}

// rateRepository stores the per-member spam counters used by the automod engine.
type rateRepository struct {
	base
}

// NewRateRepository builds the spam-counter repository.
func NewRateRepository(db Querier) RateRepository { return &rateRepository{base{db: db}} }

// GetRate returns the counter state or nil when the member has no counter.
func (r *rateRepository) GetRate(ctx context.Context, communityID, userID uuid.UUID) (*models.MemberRate, error) {
	const query = `SELECT community_id::text, user_id::text, window_started_at, message_count, recent_fingerprints::text[]
FROM member_message_rates WHERE community_id = $1 AND user_id = $2`
	var (
		community, user    string
		windowStartedAt    time.Time
		messageCount       int
		recentFingerprints []string
	)
	if err := r.q(ctx).QueryRow(ctx, query, uuidParam(communityID), uuidParam(userID)).
		Scan(&community, &user, &windowStartedAt, &messageCount, &recentFingerprints); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("repository: loading member rate: %w", err)
	}
	parsedCommunity, err := parseUUID(community)
	if err != nil {
		return nil, err
	}
	parsedUser, err := parseUUID(user)
	if err != nil {
		return nil, err
	}
	return &models.MemberRate{
		CommunityID:        parsedCommunity,
		UserID:             parsedUser,
		WindowStartedAt:    windowStartedAt,
		MessageCount:       messageCount,
		RecentFingerprints: recentFingerprints,
	}, nil
}

// UpsertRate writes the counter state after a successful send.
func (r *rateRepository) UpsertRate(ctx context.Context, rate models.MemberRate) error {
	const query = `INSERT INTO member_message_rates (community_id, user_id, window_started_at, message_count, recent_fingerprints)
VALUES ($1, $2, $3, $4, $5::text[])
ON CONFLICT (community_id, user_id) DO UPDATE
SET window_started_at = EXCLUDED.window_started_at, message_count = EXCLUDED.message_count,
    recent_fingerprints = EXCLUDED.recent_fingerprints`
	if _, err := r.q(ctx).Exec(ctx, query, uuidParam(rate.CommunityID), uuidParam(rate.UserID),
		rate.WindowStartedAt, rate.MessageCount, rate.RecentFingerprints); err != nil {
		return fmt.Errorf("repository: upserting member rate: %w", err)
	}
	return nil
}
