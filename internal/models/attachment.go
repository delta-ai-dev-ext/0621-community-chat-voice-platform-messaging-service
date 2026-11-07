package models

import (
	"time"

	"github.com/google/uuid"
)

// AttachmentStatus is the lifecycle state of an attachment row.
type AttachmentStatus string

// Supported attachment statuses.
const (
	// AttachmentPendingUpload means the row exists but the object has not been
	// verified yet; it is collectable after GC_PENDING_AFTER.
	AttachmentPendingUpload AttachmentStatus = "pending_upload"
	// AttachmentAttached means the object was verified and bound to a message.
	AttachmentAttached AttachmentStatus = "attached"
)

// SupportedImageType reports whether the content type is one of the four image
// types the service accepts.
func SupportedImageType(contentType string) bool {
	switch contentType {
	case "image/png", "image/jpeg", "image/gif", "image/webp":
		return true
	default:
		return false
	}
}

// Attachment is the stored attachment row.
//
// DeclaredContentType/DeclaredSizeBytes are what the client claimed at
// reservation time; ActualContentType/ActualSizeBytes are what the storage
// HEAD check observed at verify-on-send time (ADR-003).
type Attachment struct {
	ID                  uuid.UUID
	UploaderUserID      uuid.UUID
	Filename            string
	DeclaredContentType string
	DeclaredSizeBytes   int64
	ObjectKey           string
	Status              AttachmentStatus
	ActualContentType   *string
	ActualSizeBytes     *int64
	MessageID           *uuid.UUID
	IdempotencyKey      *string
	CreatedAt           time.Time
	VerifiedAt          *time.Time
}

// CreateAttachmentRequest is the `CreateAttachmentRequest` schema.
type CreateAttachmentRequest struct {
	Filename    string `json:"filename" binding:"required,min=1,max=255"`
	ContentType string `json:"content_type" binding:"required"`
	SizeBytes   int64  `json:"size_bytes" binding:"required,min=1"`
}

// AttachmentCreated is the `AttachmentCreated` schema returned by
// POST /attachments.
type AttachmentCreated struct {
	AttachmentID       string    `json:"attachment_id"`
	UploadURL          string    `json:"upload_url"`
	UploadURLExpiresAt time.Time `json:"upload_url_expires_at"`
	Filename           string    `json:"filename"`
	ContentType        string    `json:"content_type"`
	DeclaredSizeBytes  int64     `json:"size_bytes"`
}
