package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"messaging_service/internal/models"
	"messaging_service/internal/objectstore"
	"messaging_service/internal/observability"
)

// maxIdempotencyKeyLen matches the Idempotency-Key header bound in the OpenAPI
// spec.
const maxIdempotencyKeyLen = 128

// AttachmentService owns the two-phase attachment lifecycle: reserve a slot with
// a presigned PUT, verify on send, serve with fresh download URLs, and
// garbage-collect never-attached uploads (ADR-003).
type AttachmentService struct {
	deps Deps
}

// NewAttachmentService builds the attachment service.
func NewAttachmentService(deps Deps) *AttachmentService { return &AttachmentService{deps: deps} }

// ReserveCommand is one POST /attachments request.
type ReserveCommand struct {
	UploaderUserID uuid.UUID
	Filename       string
	ContentType    string
	SizeBytes      int64
	IdempotencyKey string
}

// Reserve validates the declared metadata, stores a pending_upload row and
// returns a presigned PUT URL bound to the declared content type. Replaying the
// same Idempotency-Key returns the original attachment instead of a new row.
func (s *AttachmentService) Reserve(ctx context.Context, cmd ReserveCommand) (*models.AttachmentCreated, error) {
	if !models.SupportedImageType(cmd.ContentType) {
		s.deps.Metrics.IncAttachments(observability.AttachmentResultUnsupportedType)
		return nil, models.ErrUnsupportedImageType
	}
	if cmd.SizeBytes > s.deps.Options.MaxAttachmentBytes {
		s.deps.Metrics.IncAttachments(observability.AttachmentResultTooLarge)
		return nil, models.ErrImageTooLarge
	}
	if cmd.SizeBytes <= 0 {
		return nil, models.ErrInvalidRequest.WithDetail("size_bytes must be between 1 and 10485760.")
	}
	filename := strings.TrimSpace(cmd.Filename)
	if filename == "" || len(filename) > 255 {
		return nil, models.ErrInvalidRequest.WithDetail("filename must be between 1 and 255 characters.")
	}
	if len(cmd.IdempotencyKey) > maxIdempotencyKeyLen {
		return nil, models.ErrInvalidRequest.WithDetail("Idempotency-Key must be at most 128 characters.")
	}

	attachment := &models.Attachment{
		ID:                  uuid.New(),
		UploaderUserID:      cmd.UploaderUserID,
		Filename:            filename,
		DeclaredContentType: cmd.ContentType,
		DeclaredSizeBytes:   cmd.SizeBytes,
		ObjectKey:           objectKey(filename, cmd.ContentType),
		Status:              models.AttachmentPendingUpload,
		CreatedAt:           s.deps.now(),
	}
	if cmd.IdempotencyKey != "" {
		key := cmd.IdempotencyKey
		attachment.IdempotencyKey = &key
	}

	stored, created, err := s.deps.Attachments.Create(ctx, attachment)
	if err != nil {
		return nil, err
	}
	signed, err := s.deps.Objects.PresignPut(ctx, stored.ObjectKey, stored.DeclaredContentType, s.deps.Options.UploadURLTTL)
	if err != nil {
		// Reservation is useless without an upload URL; fail closed so the client
		// retries instead of believing the slot is usable.
		return nil, models.ErrStorageUnavailable
	}
	if created {
		s.deps.Metrics.IncAttachments(observability.AttachmentResultAccepted)
	}
	return &models.AttachmentCreated{
		AttachmentID:       stored.ID.String(),
		UploadURL:          signed.URL,
		UploadURLExpiresAt: signed.ExpiresAt,
		Filename:           stored.Filename,
		ContentType:        stored.DeclaredContentType,
		DeclaredSizeBytes:  stored.DeclaredSizeBytes,
	}, nil
}

// SweepPendingUploads deletes never-attached uploads older than
// GC_PENDING_AFTER and their stored objects. The sweep is idempotent and
// replica-safe: deleting a row or object twice is a no-op, and a failed object
// deletion keeps the row so the next sweep retries it.
func (s *AttachmentService) SweepPendingUploads(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	cutoff := s.deps.now().Add(-s.deps.Options.PendingUploadGCAfter)
	stale, err := s.deps.Attachments.ListPendingOlderThan(ctx, cutoff, limit)
	if err != nil {
		return 0, err
	}
	// The sweep originates no request, so it mints its own trace id at the edge.
	traceID := models.MintTraceID()
	collector := 0
	for _, attachment := range stale {
		if err := s.deps.Objects.Delete(ctx, attachment.ObjectKey); err != nil && !errors.Is(err, objectstore.ErrObjectNotFound) {
			s.deps.logger().Warn("attachment gc could not delete object",
				zap.String("trace_id", traceID),
				zap.String("attachment_id", attachment.ID.String()),
				zap.Error(err))
			continue
		}
		if err := s.deps.Attachments.Delete(ctx, attachment.ID); err != nil {
			return collector, fmt.Errorf("service: deleting stale attachment: %w", err)
		}
		collector++
	}
	return collector, nil
}

// objectKey builds a random, non-guessable object key. The extension is derived
// from the declared type only to keep objects inspectable; authorization never
// depends on it.
func objectKey(filename, contentType string) string {
	extension := "bin"
	switch contentType {
	case "image/png":
		extension = "png"
	case "image/jpeg":
		extension = "jpg"
	case "image/gif":
		extension = "gif"
	case "image/webp":
		extension = "webp"
	}
	_ = filename
	return fmt.Sprintf("attachments/%s.%s", uuid.New().String(), extension)
}
