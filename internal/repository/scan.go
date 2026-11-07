package repository

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"messaging_service/internal/models"
)

// rowScanner is satisfied by both pgx.Row and pgx.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

// messageColumns is the projection used by every message read. UUID and UUID[]
// columns are cast to text so scanning never depends on the driver's binary
// representation; the cast is done by the server and cannot be injected into.
const messageColumns = `id::text, container_type, container_id::text, community_id::text, author_user_id::text, ` +
	`thread_root_id::text, content, client_message_id, edited_at, deleted_at, deletion_reason, message_version, created_at`

// attachmentColumns is the projection used by every attachment read.
const attachmentColumns = `id::text, uploader_user_id::text, filename, declared_content_type, declared_size_bytes, ` +
	`object_key, status, actual_content_type, actual_size_bytes, message_id::text, idempotency_key, created_at, verified_at`

// uuidParam converts a domain uuid into a driver value for a uuid parameter.
func uuidParam(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: [16]byte(id), Valid: true}
}

// uuidParamPtr converts a nullable domain uuid into a driver value.
func uuidParamPtr(id *uuid.UUID) pgtype.UUID {
	if id == nil {
		return pgtype.UUID{}
	}
	return uuidParam(*id)
}

// uuidStrings renders uuids as text, for uuid[] parameters that the caller casts
// explicitly in SQL.
func uuidStrings(ids []uuid.UUID) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, id.String())
	}
	return out
}

// parseUUID parses a uuid column that was selected as text.
func parseUUID(raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, fmt.Errorf("repository: parsing uuid %q: %w", raw, err)
	}
	return id, nil
}

// parseUUIDPtr parses a nullable uuid column selected as text.
func parseUUIDPtr(raw *string) (*uuid.UUID, error) {
	if raw == nil || *raw == "" {
		return nil, nil
	}
	id, err := parseUUID(*raw)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// uuidList parses a uuid[] column selected as text[].
func uuidList(raw []string) ([]uuid.UUID, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]uuid.UUID, 0, len(raw))
	for _, item := range raw {
		id, err := parseUUID(item)
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, nil
}

// scanMessage scans one row of messageColumns.
func scanMessage(row rowScanner, m *models.Message) error {
	var (
		id, containerType, containerID, authorID string
		communityID, threadRootID                *string
		content, clientMessageID                 *string
		deletionReason                           *string
		editedAt, deletedAt                      *time.Time
	)
	if err := row.Scan(&id, &containerType, &containerID, &communityID, &authorID, &threadRootID,
		&content, &clientMessageID, &editedAt, &deletedAt, &deletionReason, &m.MessageVersion, &m.CreatedAt); err != nil {
		return err
	}
	messageID, err := parseUUID(id)
	if err != nil {
		return err
	}
	containerUUID, err := parseUUID(containerID)
	if err != nil {
		return err
	}
	authorUUID, err := parseUUID(authorID)
	if err != nil {
		return err
	}
	if m.CommunityID, err = parseUUIDPtr(communityID); err != nil {
		return err
	}
	if m.ThreadRootID, err = parseUUIDPtr(threadRootID); err != nil {
		return err
	}
	m.ID = messageID
	m.ContainerType = models.ContainerType(containerType)
	m.ContainerID = containerUUID
	m.AuthorUserID = authorUUID
	m.Content = content
	m.ClientMessageID = clientMessageID
	m.EditedAt = editedAt
	m.DeletedAt = deletedAt
	if deletionReason != nil && *deletionReason != "" {
		reason := models.DeletionReason(*deletionReason)
		m.DeletionReason = &reason
	}
	return nil
}

// scanAttachment scans one row of attachmentColumns.
func scanAttachment(row rowScanner, a *models.Attachment) error {
	var (
		id, uploaderID, filename, declaredType string
		declaredSize                           int64
		objectKey, status                      string
		actualType                             *string
		actualSize                             *int64
		messageID                              *string
		idempotencyKey                         *string
		createdAt                              time.Time
		verifiedAt                             *time.Time
	)
	if err := row.Scan(&id, &uploaderID, &filename, &declaredType, &declaredSize, &objectKey, &status,
		&actualType, &actualSize, &messageID, &idempotencyKey, &createdAt, &verifiedAt); err != nil {
		return err
	}
	attachmentID, err := parseUUID(id)
	if err != nil {
		return err
	}
	uploaderUUID, err := parseUUID(uploaderID)
	if err != nil {
		return err
	}
	if a.MessageID, err = parseUUIDPtr(messageID); err != nil {
		return err
	}
	a.ID = attachmentID
	a.UploaderUserID = uploaderUUID
	a.Filename = filename
	a.DeclaredContentType = declaredType
	a.DeclaredSizeBytes = declaredSize
	a.ObjectKey = objectKey
	a.Status = models.AttachmentStatus(status)
	a.ActualContentType = actualType
	a.ActualSizeBytes = actualSize
	a.IdempotencyKey = idempotencyKey
	a.CreatedAt = createdAt
	a.VerifiedAt = verifiedAt
	return nil
}
