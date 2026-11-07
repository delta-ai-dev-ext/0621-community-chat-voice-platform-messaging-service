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

// attachmentRepository is the pgx-backed attachment reservation store. The
// two-phase lifecycle (reserve → verify on send → serve → collect) lives in the
// service layer; this type only persists rows.
type attachmentRepository struct {
	base
}

// NewAttachmentRepository builds the attachment repository.
func NewAttachmentRepository(db Querier) AttachmentRepository {
	return &attachmentRepository{base{db: db}}
}

// Create reserves an attachment. When the uploader retries with the same
// Idempotency-Key the partial unique index resolves the conflict and the
// original row is returned instead of a second reservation.
func (r *attachmentRepository) Create(ctx context.Context, attachment *models.Attachment) (*models.Attachment, bool, error) {
	const query = `INSERT INTO attachments
	(id, uploader_user_id, filename, declared_content_type, declared_size_bytes, object_key, status, idempotency_key, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (uploader_user_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING ` + attachmentColumns

	row := r.q(ctx).QueryRow(ctx, query, uuidParam(attachment.ID), uuidParam(attachment.UploaderUserID),
		attachment.Filename, attachment.DeclaredContentType, attachment.DeclaredSizeBytes, attachment.ObjectKey,
		string(models.AttachmentPendingUpload), attachment.IdempotencyKey, attachment.CreatedAt)
	stored := &models.Attachment{}
	if err := scanAttachment(row, stored); err != nil {
		if errors.Is(err, pgx.ErrNoRows) && attachment.IdempotencyKey != nil {
			existing, getErr := r.findByIdempotencyKey(ctx, attachment.UploaderUserID, *attachment.IdempotencyKey)
			if getErr != nil {
				return nil, false, getErr
			}
			return existing, false, nil
		}
		return nil, false, fmt.Errorf("repository: creating attachment: %w", err)
	}
	return stored, true, nil
}

func (r *attachmentRepository) findByIdempotencyKey(ctx context.Context, uploaderID uuid.UUID, key string) (*models.Attachment, error) {
	const query = `SELECT ` + attachmentColumns + ` FROM attachments WHERE uploader_user_id = $1 AND idempotency_key = $2`
	stored := &models.Attachment{}
	if err := scanAttachment(r.q(ctx).QueryRow(ctx, query, uuidParam(uploaderID), key), stored); err != nil {
		return nil, fmt.Errorf("repository: loading idempotent attachment: %w", err)
	}
	return stored, nil
}

// Get returns the attachment or nil when it does not exist.
func (r *attachmentRepository) Get(ctx context.Context, id uuid.UUID) (*models.Attachment, error) {
	const query = `SELECT ` + attachmentColumns + ` FROM attachments WHERE id = $1`
	stored := &models.Attachment{}
	if err := scanAttachment(r.q(ctx).QueryRow(ctx, query, uuidParam(id)), stored); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("repository: loading attachment: %w", err)
	}
	return stored, nil
}

// ListPendingOlderThan returns the never-attached uploads the GC sweep collects.
func (r *attachmentRepository) ListPendingOlderThan(ctx context.Context, cutoff time.Time, limit int) ([]models.Attachment, error) {
	const query = `SELECT ` + attachmentColumns + ` FROM attachments
WHERE status = 'pending_upload' AND created_at < $1 ORDER BY created_at ASC LIMIT $2`
	rows, err := r.q(ctx).Query(ctx, query, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("repository: listing stale attachments: %w", err)
	}
	defer rows.Close()
	var stale []models.Attachment
	for rows.Next() {
		var attachment models.Attachment
		if err := scanAttachment(rows, &attachment); err != nil {
			return nil, fmt.Errorf("repository: scanning stale attachment: %w", err)
		}
		stale = append(stale, attachment)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: reading stale attachments: %w", err)
	}
	return stale, nil
}

// Delete removes an attachment row.
func (r *attachmentRepository) Delete(ctx context.Context, id uuid.UUID) error {
	if _, err := r.q(ctx).Exec(ctx, `DELETE FROM attachments WHERE id = $1`, uuidParam(id)); err != nil {
		return fmt.Errorf("repository: deleting attachment: %w", err)
	}
	return nil
}
