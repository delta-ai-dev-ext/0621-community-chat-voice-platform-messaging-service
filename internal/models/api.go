package models

import (
	"time"

	"github.com/google/uuid"
)

// UserScopesRequest is the API-003 request body.
type UserScopesRequest struct {
	UserID uuid.UUID `json:"user_id"`
}

// UserScopesResponse is the API-003 response body. Unknown users yield empty
// lists rather than an error (US-016).
type UserScopesResponse struct {
	UserID          uuid.UUID   `json:"user_id"`
	CommunityIDs    []uuid.UUID `json:"community_ids"`
	ConversationIDs []uuid.UUID `json:"conversation_ids"`
	GeneratedAt     time.Time   `json:"generated_at"`
}

// SendMessageRequest is the `SendMessageRequest` schema for channel and
// conversation sends.
type SendMessageRequest struct {
	ClientMessageID string      `json:"client_message_id"`
	Content         string      `json:"content"`
	AttachmentIDs   []uuid.UUID `json:"attachment_ids"`
	ThreadRootID    *uuid.UUID  `json:"thread_root_id"`
}

// EditMessageRequest is the `EditMessageRequest` schema.
type EditMessageRequest struct {
	Content string `json:"content"`
}

// HealthStatus is the probe response body.
type HealthStatus struct {
	Status string `json:"status"`
}

// Probe status values.
const (
	StatusOK          = "ok"
	StatusUnavailable = "unavailable"
)
