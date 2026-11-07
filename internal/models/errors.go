package models

import (
	"errors"
	"fmt"
	"time"
)

// Error codes are the stable machine-readable identifiers returned in the
// `error` field of every error response body. They are part of the public
// contract in docs/openapi_spec.yaml and must not change casually.
const (
	CodeInvalidRequest      = "invalid_request"
	CodeUnauthenticated     = "unauthenticated"
	CodeForbidden           = "forbidden"
	CodeNotFound            = "not_found"
	CodeNotAMember          = "not_a_member"
	CodeMemberMuted         = "member_muted"
	CodeNotAuthor           = "not_author"
	CodeMessageDeleted      = "message_deleted"
	CodeBlockedByAutomod    = "blocked_by_automod"
	CodeContentTooLong      = "content_too_long"
	CodeAttachmentInvalid   = "attachment_invalid"
	CodeImageTooLarge       = "image_too_large"
	CodeUnsupportedImageTyp = "unsupported_image_type"
	CodeStorageUnavailable  = "storage_unavailable"
	CodeServiceUnavailable  = "service_unavailable"
	CodeInternal            = "internal_error"
)

// AppError is a domain error carrying the HTTP status and the stable error code
// that the handler edge maps onto the wire. Services return these; handlers only
// translate them.
type AppError struct {
	// Status is the HTTP status code to emit.
	Status int
	// Code is the stable snake_case error code.
	Code string
	// Detail is the human-readable explanation.
	Detail string
	// Extra holds contract fields that some errors carry in addition to
	// `error`/`detail` (for example `muted_until` and `appeal_url`).
	Extra map[string]any
}

// Error implements the error interface.
func (e *AppError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Detail)
}

// Is lets errors.Is compare domain errors by their stable code, which keeps the
// sentinels below useful even when a caller rewrites the human-readable detail.
func (e *AppError) Is(target error) bool {
	var other *AppError
	if !errors.As(target, &other) {
		return false
	}
	return e.Code == other.Code
}

// WithDetail returns a copy of the error with a different human-readable detail.
func (e *AppError) WithDetail(detail string) *AppError {
	clone := *e
	clone.Detail = detail
	return &clone
}

// NewAppError builds a domain error.
func NewAppError(status int, code, detail string) *AppError {
	return &AppError{Status: status, Code: code, Detail: detail}
}

// Domain sentinels. Callers use errors.Is against these.
var (
	// ErrUnauthenticated is returned when the access token is missing or invalid.
	ErrUnauthenticated = NewAppError(401, CodeUnauthenticated, "A valid access token is required.")
	// ErrServiceTokenInvalid is returned for a missing or wrong internal service token.
	ErrServiceTokenInvalid = NewAppError(401, CodeUnauthenticated, "A valid service token is required.")
	// ErrInvalidRequest is returned when request validation fails.
	ErrInvalidRequest = NewAppError(400, CodeInvalidRequest, "The request could not be validated.")
	// ErrInvalidCursor is returned when an opaque cursor cannot be decoded.
	ErrInvalidCursor = NewAppError(400, CodeInvalidRequest, "The cursor is not valid.")
	// ErrNotFound is returned when the addressed resource does not exist.
	ErrNotFound = NewAppError(404, CodeNotFound, "The requested resource was not found.")
	// ErrNotAMember is returned when the caller may not access the container.
	ErrNotAMember = NewAppError(403, CodeNotAMember, "You are not a member of this community.")
	// ErrNotAParticipant is returned when the caller is not a conversation participant.
	ErrNotAParticipant = NewAppError(403, CodeNotAMember, "You are not a participant of this conversation.")
	// ErrNotAuthor is returned when a non-author attempts an author-only action.
	ErrNotAuthor = NewAppError(403, CodeNotAuthor, "Only the author can modify a message.")
	// ErrMessageDeleted is returned when editing an already deleted message.
	ErrMessageDeleted = NewAppError(409, CodeMessageDeleted, "Deleted messages cannot be edited.")
	// ErrBlockedByAutomod is returned when an automod block rule matches.
	ErrBlockedByAutomod = NewAppError(422, CodeBlockedByAutomod, "Message blocked by automatic filters.")
	// ErrContentTooLong is returned when content exceeds 4000 characters.
	ErrContentTooLong = NewAppError(422, CodeContentTooLong, "Message content must be at most 4000 characters.")
	// ErrAttachmentInvalid is returned when a referenced attachment cannot be used.
	ErrAttachmentInvalid = NewAppError(422, CodeAttachmentInvalid, "The referenced attachment is not available.")
	// ErrImageTooLarge is returned when an attachment exceeds the size limit.
	ErrImageTooLarge = NewAppError(413, CodeImageTooLarge, "Images may be at most 10 MB.")
	// ErrUnsupportedImageType is returned for a non-image or unsupported image type.
	ErrUnsupportedImageType = NewAppError(415, CodeUnsupportedImageTyp,
		"Supported types are image/png, image/jpeg, image/gif, image/webp.")
	// ErrStorageUnavailable is returned when object storage cannot be reached.
	ErrStorageUnavailable = NewAppError(503, CodeStorageUnavailable,
		"Attachment storage is temporarily unavailable.")
	// ErrServiceUnavailable is returned while the process is shutting down.
	ErrServiceUnavailable = NewAppError(503, CodeServiceUnavailable, "The service is not ready.")
	// ErrInternal is the fallback for unexpected failures.
	ErrInternal = NewAppError(500, CodeInternal, "An unexpected error occurred.")
)

// NewMemberMuted builds the mute rejection including the mute expiry and a
// pointer to the appeal flow, as required by US-002/US-012.
func NewMemberMuted(mutedUntil time.Time, appealURL string) *AppError {
	until := mutedUntil.UTC().Format(time.RFC3339)
	return &AppError{
		Status: 403,
		Code:   CodeMemberMuted,
		Detail: fmt.Sprintf("You are muted in this community until %s. You can appeal this sanction.", until),
		Extra: map[string]any{
			"muted_until": until,
			"appeal_url":  appealURL,
		},
	}
}

// NewBlockedByAutomod builds the automod rejection with the matched category in
// the detail, as required by US-013.
func NewBlockedByAutomod(category string) *AppError {
	return &AppError{
		Status: 422,
		Code:   CodeBlockedByAutomod,
		Detail: fmt.Sprintf("Message blocked by automatic filters (category %s).", category),
		Extra:  map[string]any{"category": category},
	}
}

// NewAttachmentInvalid builds the invalid-attachment rejection.
func NewAttachmentInvalid(detail string) *AppError {
	return ErrAttachmentInvalid.WithDetail(detail)
}
