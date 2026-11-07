package models

import (
	"time"

	"github.com/google/uuid"
)

// ContainerType distinguishes the two containers a message can live in.
type ContainerType string

// Supported container types.
const (
	ContainerChannel      ContainerType = "channel"
	ContainerConversation ContainerType = "conversation"
)

// Valid reports whether the container type is one of the two contract values.
func (c ContainerType) Valid() bool {
	return c == ContainerChannel || c == ContainerConversation
}

// DeletionReason records why a message became a tombstone.
type DeletionReason string

// Supported deletion reasons. They are the values of the EVT-009
// `deletion_reason` field.
const (
	DeletionReasonAuthor  DeletionReason = "author"
	DeletionReasonAutomod DeletionReason = "automod"
)

// MessageChange is the EVT-009 `change` discriminator.
type MessageChange string

// Supported message change kinds.
const (
	ChangeCreated          MessageChange = "created"
	ChangeEdited           MessageChange = "edited"
	ChangeDeleted          MessageChange = "deleted"
	ChangeReactionsChanged MessageChange = "reactions_changed"
)

// Message is the stored message row.
//
// Content is nil for tombstones (deleted messages) and DeletionReason is set
// whenever DeletedAt is set, so history keeps the message position stable.
type Message struct {
	ID              uuid.UUID
	ContainerType   ContainerType
	ContainerID     uuid.UUID
	CommunityID     *uuid.UUID
	AuthorUserID    uuid.UUID
	ThreadRootID    *uuid.UUID
	Content         *string
	ClientMessageID *string
	EditedAt        *time.Time
	DeletedAt       *time.Time
	DeletionReason  *DeletionReason
	MessageVersion  int64
	CreatedAt       time.Time
}

// IsDeleted reports whether the message is a tombstone.
func (m Message) IsDeleted() bool { return m.DeletedAt != nil }

// ReactionSummary is the aggregated per-emoji reaction count.
type ReactionSummary struct {
	Emoji string `json:"emoji"`
	Count int    `json:"count"`
}

// MessageAggregate is a message together with the rows that make up its public
// representation: bound attachments, resolved mentions and reaction summaries.
type MessageAggregate struct {
	Message          Message
	Attachments      []Attachment
	MentionedUserIDs []uuid.UUID
	Reactions        []ReactionSummary
}

// AttachmentView is the client-facing attachment representation including a
// freshly generated short-lived download URL.
type AttachmentView struct {
	AttachmentID string    `json:"attachment_id"`
	ContentType  string    `json:"content_type"`
	SizeBytes    int64     `json:"size_bytes"`
	DownloadURL  string    `json:"download_url"`
	URLExpiresAt time.Time `json:"url_expires_at"`
}

// MessageView is the `Message` schema of docs/openapi_spec.yaml.
type MessageView struct {
	MessageID        string            `json:"message_id"`
	ContainerType    string            `json:"container_type"`
	ContainerID      string            `json:"container_id"`
	CommunityID      *string           `json:"community_id"`
	AuthorUserID     string            `json:"author_user_id"`
	ThreadRootID     *string           `json:"thread_root_id"`
	Content          *string           `json:"content"`
	Attachments      []AttachmentView  `json:"attachments"`
	MentionedUserIDs []string          `json:"mentioned_user_ids"`
	Reactions        []ReactionSummary `json:"reactions"`
	DeletionReason   *string           `json:"deletion_reason"`
	CreatedAt        time.Time         `json:"created_at"`
	EditedAt         *time.Time        `json:"edited_at"`
	DeletedAt        *time.Time        `json:"deleted_at"`
	MessageVersion   int64             `json:"message_version"`
}

// MessagePage is the `MessagePage` schema: newest page first plus the cursor
// for the next (older) page.
type MessagePage struct {
	Data       []MessageView `json:"data"`
	NextCursor *string       `json:"next_cursor"`
	HasMore    bool          `json:"has_more"`
}

// ThreadPage is the `ThreadPage` schema: root message, total reply count and a
// page of replies.
type ThreadPage struct {
	Root       MessageView   `json:"root"`
	ReplyCount int           `json:"reply_count"`
	Replies    []MessageView `json:"replies"`
	NextCursor *string       `json:"next_cursor"`
	HasMore    bool          `json:"has_more"`
}

// ReactionSummaryResponse is the aggregated summary returned by the reaction
// endpoints.
type ReactionSummaryResponse struct {
	MessageID string            `json:"message_id"`
	Reactions []ReactionSummary `json:"reactions"`
}

// HistoryQuery describes a single history page request.
//
// Without a cursor the newest page is returned (descending). With Before the
// page contains messages strictly older than the cursor (scrollback). With
// After the page contains messages strictly newer than the cursor in
// chronological order (reconnect catch-up).
type HistoryQuery struct {
	ContainerType ContainerType
	ContainerID   uuid.UUID
	Limit         int
	Before        *Cursor
	After         *Cursor
}

// Ascending reports whether the query pages forward in time.
func (q HistoryQuery) Ascending() bool { return q.After != nil }
