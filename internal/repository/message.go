package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"messaging_service/internal/models"
)

// messageRepository is the pgx-backed MessageRepository. Attachment binding and
// mention writes are called from inside the send transaction, so they
// deliberately take the transaction from the context via base.q.
type messageRepository struct {
	base
}

// NewMessageRepository builds the pgx MessageRepository over a querier (a
// *pgxpool.Pool in production).
func NewMessageRepository(db Querier) MessageRepository { return &messageRepository{base{db: db}} }

// Insert stores a message or returns the stored original on an idempotent
// retry. The partial unique index on (container_id, author_user_id,
// client_message_id) makes the decision in one statement, so concurrent retries
// cannot create duplicates (ADR-002).
func (r *messageRepository) Insert(ctx context.Context, msg *models.Message) (*models.Message, bool, error) {
	const query = `INSERT INTO messages
	(id, container_type, container_id, community_id, author_user_id, thread_root_id, content, client_message_id, message_version, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (container_id, author_user_id, client_message_id) WHERE client_message_id IS NOT NULL DO NOTHING
RETURNING ` + messageColumns

	row := r.q(ctx).QueryRow(ctx, query,
		uuidParam(msg.ID), string(msg.ContainerType), uuidParam(msg.ContainerID), uuidParamPtr(msg.CommunityID),
		uuidParam(msg.AuthorUserID), uuidParamPtr(msg.ThreadRootID), msg.Content, msg.ClientMessageID,
		msg.MessageVersion, msg.CreatedAt)
	stored := &models.Message{}
	if err := scanMessage(row, stored); err != nil {
		if errors.Is(err, pgx.ErrNoRows) && msg.ClientMessageID != nil {
			existing, getErr := r.getByClientMessageID(ctx, msg.ContainerID, msg.AuthorUserID, *msg.ClientMessageID)
			if getErr != nil {
				return nil, false, getErr
			}
			return existing, false, nil
		}
		return nil, false, fmt.Errorf("repository: inserting message: %w", err)
	}
	return stored, true, nil
}

func (r *messageRepository) getByClientMessageID(ctx context.Context, containerID, authorID uuid.UUID, clientMessageID string) (*models.Message, error) {
	const query = `SELECT ` + messageColumns + ` FROM messages
WHERE container_id = $1 AND author_user_id = $2 AND client_message_id = $3`
	row := r.q(ctx).QueryRow(ctx, query, uuidParam(containerID), uuidParam(authorID), clientMessageID)
	stored := &models.Message{}
	if err := scanMessage(row, stored); err != nil {
		return nil, fmt.Errorf("repository: loading deduplicated message: %w", err)
	}
	return stored, nil
}

// Get loads a message with its attachments, mentions and reactions.
func (r *messageRepository) Get(ctx context.Context, id uuid.UUID) (*models.MessageAggregate, error) {
	const query = `SELECT ` + messageColumns + ` FROM messages WHERE id = $1`
	row := r.q(ctx).QueryRow(ctx, query, uuidParam(id))
	stored := &models.Message{}
	if err := scanMessage(row, stored); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("repository: loading message: %w", err)
	}
	aggregates := []models.MessageAggregate{{Message: *stored}}
	if err := r.loadRelated(ctx, aggregates); err != nil {
		return nil, err
	}
	return &aggregates[0], nil
}

// Page loads one history page. Ordering is newest-first unless an `after`
// cursor asks for chronological catch-up.
func (r *messageRepository) Page(ctx context.Context, q models.HistoryQuery) ([]models.MessageAggregate, error) {
	query := `SELECT ` + messageColumns + ` FROM messages WHERE container_type = $1 AND container_id = $2`
	args := []any{string(q.ContainerType), uuidParam(q.ContainerID)}
	switch {
	case q.Before != nil:
		query += ` AND (created_at, id) < ($3, $4) ORDER BY created_at DESC, id DESC LIMIT $5`
		args = append(args, models.NewCursor(q.Before.CreatedAt, q.Before.ID).CreatedAt, uuidParam(q.Before.ID), q.Limit)
	case q.After != nil:
		query += ` AND (created_at, id) > ($3, $4) ORDER BY created_at ASC, id ASC LIMIT $5`
		args = append(args, models.NewCursor(q.After.CreatedAt, q.After.ID).CreatedAt, uuidParam(q.After.ID), q.Limit)
	default:
		query += ` ORDER BY created_at DESC, id DESC LIMIT $3`
		args = append(args, q.Limit)
	}
	return r.queryAggregates(ctx, query, args...)
}

// Replies loads one chronological page of thread replies.
func (r *messageRepository) Replies(ctx context.Context, rootID uuid.UUID, cursor *models.Cursor, limit int) ([]models.MessageAggregate, error) {
	query := `SELECT ` + messageColumns + ` FROM messages WHERE thread_root_id = $1`
	args := []any{uuidParam(rootID)}
	if cursor != nil {
		query += ` AND (created_at, id) > ($2, $3)`
		args = append(args, models.NewCursor(cursor.CreatedAt, cursor.ID).CreatedAt, uuidParam(cursor.ID))
	}
	query += fmt.Sprintf(` ORDER BY created_at ASC, id ASC LIMIT $%d`, len(args)+1)
	args = append(args, limit)
	return r.queryAggregates(ctx, query, args...)
}

// ReplyCount counts replies including tombstones so the count stays stable after
// a reply is deleted.
func (r *messageRepository) ReplyCount(ctx context.Context, rootID uuid.UUID) (int, error) {
	const query = `SELECT count(*)::int FROM messages WHERE thread_root_id = $1`
	var count int
	if err := r.q(ctx).QueryRow(ctx, query, uuidParam(rootID)).Scan(&count); err != nil {
		return 0, fmt.Errorf("repository: counting replies: %w", err)
	}
	return count, nil
}

// UpdateContent replaces content, bumps the message version and returns the row.
func (r *messageRepository) UpdateContent(ctx context.Context, id uuid.UUID, content string, editedAt time.Time) (*models.Message, error) {
	const query = `UPDATE messages SET content = $2, edited_at = $3, message_version = message_version + 1
WHERE id = $1 RETURNING ` + messageColumns
	row := r.q(ctx).QueryRow(ctx, query, uuidParam(id), content, editedAt)
	updated := &models.Message{}
	if err := scanMessage(row, updated); err != nil {
		return nil, fmt.Errorf("repository: updating message content: %w", err)
	}
	return updated, nil
}

// SoftDelete tombstones a live message. The `deleted_at IS NULL` guard keeps the
// operation idempotent at the storage level.
func (r *messageRepository) SoftDelete(ctx context.Context, id uuid.UUID, reason models.DeletionReason, deletedAt time.Time) (*models.Message, error) {
	const query = `UPDATE messages
SET deleted_at = $2, deletion_reason = $3, content = NULL, message_version = message_version + 1
WHERE id = $1 AND deleted_at IS NULL
RETURNING ` + messageColumns
	row := r.q(ctx).QueryRow(ctx, query, uuidParam(id), deletedAt, string(reason))
	deleted := &models.Message{}
	if err := scanMessage(row, deleted); err != nil {
		return nil, fmt.Errorf("repository: deleting message: %w", err)
	}
	return deleted, nil
}

// BumpVersion increments the message version, gating EVT-009 ordering.
func (r *messageRepository) BumpVersion(ctx context.Context, id uuid.UUID) (int64, error) {
	const query = `UPDATE messages SET message_version = message_version + 1 WHERE id = $1 RETURNING message_version`
	var version int64
	if err := r.q(ctx).QueryRow(ctx, query, uuidParam(id)).Scan(&version); err != nil {
		return 0, fmt.Errorf("repository: bumping message version: %w", err)
	}
	return version, nil
}

// ReplaceMentions stores exactly the given mention set for a message.
func (r *messageRepository) ReplaceMentions(ctx context.Context, messageID uuid.UUID, mentioned []uuid.UUID) error {
	db := r.q(ctx)
	if _, err := db.Exec(ctx, `DELETE FROM mentions WHERE message_id = $1`, uuidParam(messageID)); err != nil {
		return fmt.Errorf("repository: clearing mentions: %w", err)
	}
	if len(mentioned) == 0 {
		return nil
	}
	const query = `INSERT INTO mentions (message_id, mentioned_user_id)
SELECT $1, unnest($2::text[]::uuid[])
ON CONFLICT DO NOTHING`
	if _, err := db.Exec(ctx, query, uuidParam(messageID), uuidStrings(mentioned)); err != nil {
		return fmt.Errorf("repository: inserting mentions: %w", err)
	}
	return nil
}

// BindAttachments flips verified attachments to `attached` and binds them to the
// message inside the send transaction. It fails when any attachment is no longer
// bindable, which keeps the send all-or-nothing.
func (r *messageRepository) BindAttachments(ctx context.Context, messageID uuid.UUID, uploaderID uuid.UUID,
	verifiedAt time.Time, attachments []models.Attachment) error {
	if len(attachments) == 0 {
		return nil
	}
	ids := make([]uuid.UUID, 0, len(attachments))
	types := make([]string, 0, len(attachments))
	sizes := make([]int64, 0, len(attachments))
	for _, attachment := range attachments {
		ids = append(ids, attachment.ID)
		contentType := attachment.DeclaredContentType
		if attachment.ActualContentType != nil {
			contentType = *attachment.ActualContentType
		}
		types = append(types, contentType)
		size := attachment.DeclaredSizeBytes
		if attachment.ActualSizeBytes != nil {
			size = *attachment.ActualSizeBytes
		}
		sizes = append(sizes, size)
	}
	const query = `UPDATE attachments AS a
SET message_id = $4, status = 'attached', actual_content_type = u.content_type,
    actual_size_bytes = u.size_bytes, verified_at = $5
FROM unnest($1::text[]::uuid[], $2::text[], $3::bigint[]) AS u(id, content_type, size_bytes)
WHERE a.id = u.id AND a.uploader_user_id = $6 AND a.status = 'pending_upload' AND a.message_id IS NULL`
	tag, err := r.q(ctx).Exec(ctx, query, uuidStrings(ids), types, sizes, uuidParam(messageID), verifiedAt, uuidParam(uploaderID))
	if err != nil {
		return fmt.Errorf("repository: binding attachments: %w", err)
	}
	if int(tag.RowsAffected()) != len(ids) {
		return fmt.Errorf("repository: bound %d of %d attachments", tag.RowsAffected(), len(ids))
	}
	return nil
}

// DetachAttachments deletes the attachment rows of a message and returns the
// object keys so the caller can remove the stored objects.
func (r *messageRepository) DetachAttachments(ctx context.Context, messageID uuid.UUID) ([]string, error) {
	const query = `DELETE FROM attachments WHERE message_id = $1 RETURNING object_key`
	rows, err := r.q(ctx).Query(ctx, query, uuidParam(messageID))
	if err != nil {
		return nil, fmt.Errorf("repository: detaching attachments: %w", err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("repository: scanning detached attachment: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: reading detached attachments: %w", err)
	}
	return keys, nil
}

// AddReaction is idempotent: the primary key makes a repeat a no-op.
func (r *messageRepository) AddReaction(ctx context.Context, messageID, userID uuid.UUID, emoji string, createdAt time.Time) (bool, error) {
	const query = `INSERT INTO reactions (message_id, user_id, emoji, created_at) VALUES ($1, $2, $3, $4)
ON CONFLICT (message_id, user_id, emoji) DO NOTHING`
	tag, err := r.q(ctx).Exec(ctx, query, uuidParam(messageID), uuidParam(userID), emoji, createdAt)
	if err != nil {
		return false, fmt.Errorf("repository: adding reaction: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// RemoveReaction is idempotent: removing an absent reaction reports changed=false.
func (r *messageRepository) RemoveReaction(ctx context.Context, messageID, userID uuid.UUID, emoji string) (bool, error) {
	const query = `DELETE FROM reactions WHERE message_id = $1 AND user_id = $2 AND emoji = $3`
	tag, err := r.q(ctx).Exec(ctx, query, uuidParam(messageID), uuidParam(userID), emoji)
	if err != nil {
		return false, fmt.Errorf("repository: removing reaction: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// ReactionSummary aggregates per-emoji counts, ordered by emoji for a stable
// response body.
func (r *messageRepository) ReactionSummary(ctx context.Context, messageID uuid.UUID) ([]models.ReactionSummary, error) {
	const query = `SELECT emoji, count(*)::int FROM reactions WHERE message_id = $1 GROUP BY emoji ORDER BY emoji ASC`
	rows, err := r.q(ctx).Query(ctx, query, uuidParam(messageID))
	if err != nil {
		return nil, fmt.Errorf("repository: loading reactions: %w", err)
	}
	defer rows.Close()
	summaries := make([]models.ReactionSummary, 0, 4)
	for rows.Next() {
		var summary models.ReactionSummary
		if err := rows.Scan(&summary.Emoji, &summary.Count); err != nil {
			return nil, fmt.Errorf("repository: scanning reaction summary: %w", err)
		}
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: reading reaction summary: %w", err)
	}
	return summaries, nil
}

// queryAggregates runs a message query and hydrates attachments, mentions and
// reactions for the whole page with three batched follow-up queries, so paging
// stays O(1) round trips regardless of page size.
func (r *messageRepository) queryAggregates(ctx context.Context, query string, args ...any) ([]models.MessageAggregate, error) {
	rows, err := r.q(ctx).Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("repository: querying messages: %w", err)
	}
	defer rows.Close()

	aggregates := make([]models.MessageAggregate, 0, 8)
	for rows.Next() {
		var aggregate models.MessageAggregate
		if err := scanMessage(rows, &aggregate.Message); err != nil {
			return nil, fmt.Errorf("repository: scanning message: %w", err)
		}
		aggregates = append(aggregates, aggregate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: reading messages: %w", err)
	}
	if len(aggregates) == 0 {
		return aggregates, nil
	}
	if err := r.loadRelated(ctx, aggregates); err != nil {
		return nil, err
	}
	return aggregates, nil
}

// loadRelated hydrates attachments, mentions and reactions in place.
func (r *messageRepository) loadRelated(ctx context.Context, aggregates []models.MessageAggregate) error {
	index := make(map[uuid.UUID]int, len(aggregates))
	ids := make([]uuid.UUID, 0, len(aggregates))
	for i, aggregate := range aggregates {
		index[aggregate.Message.ID] = i
		ids = append(ids, aggregate.Message.ID)
	}

	if err := r.loadReactions(ctx, aggregates, index, ids); err != nil {
		return err
	}
	if err := r.loadMentions(ctx, aggregates, index, ids); err != nil {
		return err
	}
	return r.loadAttachments(ctx, aggregates, index, ids)
}

func (r *messageRepository) loadReactions(ctx context.Context, aggregates []models.MessageAggregate, index map[uuid.UUID]int, ids []uuid.UUID) error {
	const query = `SELECT message_id::text, emoji, count(*)::int FROM reactions
WHERE message_id = ANY($1::text[]::uuid[]) GROUP BY message_id, emoji ORDER BY message_id, emoji`
	rows, err := r.q(ctx).Query(ctx, query, uuidStrings(ids))
	if err != nil {
		return fmt.Errorf("repository: loading page reactions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			messageID string
			summary   models.ReactionSummary
		)
		if err := rows.Scan(&messageID, &summary.Emoji, &summary.Count); err != nil {
			return fmt.Errorf("repository: scanning page reaction: %w", err)
		}
		id, err := parseUUID(messageID)
		if err != nil {
			return err
		}
		if position, ok := index[id]; ok {
			aggregates[position].Reactions = append(aggregates[position].Reactions, summary)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("repository: reading page reactions: %w", err)
	}
	return nil
}

func (r *messageRepository) loadMentions(ctx context.Context, aggregates []models.MessageAggregate, index map[uuid.UUID]int, ids []uuid.UUID) error {
	const query = `SELECT message_id::text, mentioned_user_id::text FROM mentions
WHERE message_id = ANY($1::text[]::uuid[]) ORDER BY message_id, mentioned_user_id`
	rows, err := r.q(ctx).Query(ctx, query, uuidStrings(ids))
	if err != nil {
		return fmt.Errorf("repository: loading page mentions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var messageID, mentionedID string
		if err := rows.Scan(&messageID, &mentionedID); err != nil {
			return fmt.Errorf("repository: scanning page mention: %w", err)
		}
		id, err := parseUUID(messageID)
		if err != nil {
			return err
		}
		mentioned, err := parseUUID(mentionedID)
		if err != nil {
			return err
		}
		if position, ok := index[id]; ok {
			aggregates[position].MentionedUserIDs = append(aggregates[position].MentionedUserIDs, mentioned)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("repository: reading page mentions: %w", err)
	}
	return nil
}

func (r *messageRepository) loadAttachments(ctx context.Context, aggregates []models.MessageAggregate, index map[uuid.UUID]int, ids []uuid.UUID) error {
	const query = `SELECT ` + attachmentColumns + ` FROM attachments
WHERE message_id = ANY($1::text[]::uuid[]) ORDER BY created_at, id`
	rows, err := r.q(ctx).Query(ctx, query, uuidStrings(ids))
	if err != nil {
		return fmt.Errorf("repository: loading page attachments: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var attachment models.Attachment
		if err := scanAttachment(rows, &attachment); err != nil {
			return fmt.Errorf("repository: scanning page attachment: %w", err)
		}
		if attachment.MessageID == nil {
			continue
		}
		if position, ok := index[*attachment.MessageID]; ok {
			aggregates[position].Attachments = append(aggregates[position].Attachments, attachment)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("repository: reading page attachments: %w", err)
	}
	return nil
}

// ensure the pgx import is used even if every query above switches strategy.
var _ = pgx.ErrNoRows
var _ = strings.TrimSpace
