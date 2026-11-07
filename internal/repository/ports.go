package repository

import (
	"context"
	"time"

	"github.com/google/uuid"

	"messaging_service/internal/models"
)

// MessageRepository is the persistence port for messages, their mentions,
// their attachments and their reactions.
type MessageRepository interface {
	// Insert stores a message. created is false when the
	// (container_id, author_user_id, client_message_id) unique index already
	// held this send, in which case the stored original is returned unchanged
	// (ADR-002).
	Insert(ctx context.Context, msg *models.Message) (*models.Message, bool, error)
	// Get loads one message with its attachments, mentions and reactions.
	// A missing message yields (nil, nil).
	Get(ctx context.Context, id uuid.UUID) (*models.MessageAggregate, error)
	// Page loads one history page; callers request limit+1 to detect more pages.
	Page(ctx context.Context, q models.HistoryQuery) ([]models.MessageAggregate, error)
	// Replies loads one chronological page of a thread's replies.
	Replies(ctx context.Context, rootID uuid.UUID, cursor *models.Cursor, limit int) ([]models.MessageAggregate, error)
	// ReplyCount counts the replies of a root message, tombstones included.
	ReplyCount(ctx context.Context, rootID uuid.UUID) (int, error)
	// UpdateContent replaces the content, bumps message_version and returns the
	// updated row.
	UpdateContent(ctx context.Context, id uuid.UUID, content string, editedAt time.Time) (*models.Message, error)
	// SoftDelete tombstones a message, bumps message_version and returns the
	// updated row. It only matches messages that are not yet deleted.
	SoftDelete(ctx context.Context, id uuid.UUID, reason models.DeletionReason, deletedAt time.Time) (*models.Message, error)
	// BumpVersion increments message_version and returns the new value.
	BumpVersion(ctx context.Context, id uuid.UUID) (int64, error)
	// ReplaceMentions stores exactly the given mention set for a message.
	ReplaceMentions(ctx context.Context, messageID uuid.UUID, mentioned []uuid.UUID) error
	// BindAttachments flips verified attachments to `attached` on a message.
	BindAttachments(ctx context.Context, messageID uuid.UUID, uploaderID uuid.UUID, verifiedAt time.Time,
		attachments []models.Attachment) error
	// DetachAttachments removes the attachments of a message and returns the
	// object keys whose stored objects must be deleted.
	DetachAttachments(ctx context.Context, messageID uuid.UUID) ([]string, error)
	// AddReaction is idempotent; added reports whether a new row was written.
	AddReaction(ctx context.Context, messageID, userID uuid.UUID, emoji string, createdAt time.Time) (bool, error)
	// RemoveReaction is idempotent; removed reports whether a row was deleted.
	RemoveReaction(ctx context.Context, messageID, userID uuid.UUID, emoji string) (bool, error)
	// ReactionSummary returns the aggregated per-emoji counts.
	ReactionSummary(ctx context.Context, messageID uuid.UUID) ([]models.ReactionSummary, error)
}

// CommunityProjectionRepository is the read/write port over the projections fed
// by EVT-002, EVT-003 and EVT-005.
type CommunityProjectionRepository interface {
	// GetTextChannel returns nil when the channel is unknown or not text.
	GetTextChannel(ctx context.Context, channelID uuid.UUID) (*models.TextChannel, error)
	// UpsertTextChannel applies the row only when the incoming version is newer.
	UpsertTextChannel(ctx context.Context, channel models.TextChannel) (bool, error)
	// DeleteTextChannel removes the channel row only when the incoming version
	// is newer than the stored one.
	DeleteTextChannel(ctx context.Context, channelID uuid.UUID, version int64) (bool, error)
	// GetMembership returns nil when no membership has been projected yet.
	GetMembership(ctx context.Context, communityID, userID uuid.UUID) (*models.CommunityMember, error)
	// UpsertMembership applies the row only when member_version is newer.
	UpsertMembership(ctx context.Context, member models.CommunityMember) (bool, error)
	// ActiveCommunityIDs lists communities where the user is active and not banned.
	ActiveCommunityIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
	// FilterActiveMemberIDs keeps only the candidate ids that are active members.
	FilterActiveMemberIDs(ctx context.Context, communityID uuid.UUID, userIDs []uuid.UUID) ([]uuid.UUID, error)
	// GetAutomodRuleSet returns nil when the community has no projected rules.
	GetAutomodRuleSet(ctx context.Context, communityID uuid.UUID) (*models.AutomodRuleSet, error)
	// UpsertAutomodRuleSet replaces the snapshot only when rule_set_version is newer.
	UpsertAutomodRuleSet(ctx context.Context, ruleSet models.AutomodRuleSet) (bool, error)
}

// ConversationProjectionRepository is the port over the projections fed by
// EVT-008.
type ConversationProjectionRepository interface {
	// GetConversation returns nil when the conversation is unknown.
	GetConversation(ctx context.Context, conversationID uuid.UUID) (*models.Conversation, error)
	// UpsertConversation applies the snapshot (conversation plus participant
	// list) only when conversation_version is newer.
	UpsertConversation(ctx context.Context, conversation models.Conversation) (bool, error)
	// IsParticipant reports whether the user currently participates.
	IsParticipant(ctx context.Context, conversationID, userID uuid.UUID) (bool, error)
	// FilterParticipantIDs keeps only the candidate ids that are participants.
	FilterParticipantIDs(ctx context.Context, conversationID uuid.UUID, userIDs []uuid.UUID) ([]uuid.UUID, error)
	// ConversationIDsForUser lists the user's current conversations.
	ConversationIDsForUser(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
}

// AttachmentRepository is the persistence port for attachment reservations.
type AttachmentRepository interface {
	// Create stores a pending attachment. created is false when the uploader's
	// Idempotency-Key already reserved one, in which case the original is
	// returned.
	Create(ctx context.Context, attachment *models.Attachment) (*models.Attachment, bool, error)
	// Get returns nil when the attachment is unknown.
	Get(ctx context.Context, id uuid.UUID) (*models.Attachment, error)
	// ListPendingOlderThan returns never-attached uploads older than cutoff.
	ListPendingOlderThan(ctx context.Context, cutoff time.Time, limit int) ([]models.Attachment, error)
	// Delete removes the attachment row.
	Delete(ctx context.Context, id uuid.UUID) error
}

// ProcessedEventRepository is the consumer-side dedupe table.
type ProcessedEventRepository interface {
	// IsProcessed reports whether the event id was already applied.
	IsProcessed(ctx context.Context, eventID uuid.UUID) (bool, error)
	// MarkProcessed records the event id; inserted is false on a duplicate.
	MarkProcessed(ctx context.Context, eventID uuid.UUID, eventType string) (bool, error)
	// PruneOlderThan deletes dedupe rows older than cutoff.
	PruneOlderThan(ctx context.Context, cutoff time.Time) (int64, error)
}

// OutboxRepository is the transactional outbox used to publish EVT-009.
type OutboxRepository interface {
	// Append writes an event inside the caller's transaction.
	Append(ctx context.Context, entry models.OutboxEntryView) error
	// ListUnpublished returns the oldest unpublished entries.
	ListUnpublished(ctx context.Context, limit int) ([]models.OutboxEntry, error)
	// MarkPublished stamps an entry as published at the given instant.
	MarkPublished(ctx context.Context, id int64, publishedAt time.Time) error
	// RecordAttempt increments the attempt counter of a failed publish.
	RecordAttempt(ctx context.Context, id int64) error
	// CountUnpublished reports the relay backlog depth.
	CountUnpublished(ctx context.Context) (int64, error)
}

// RateRepository stores the per-member spam counters.
type RateRepository interface {
	// GetRate returns nil when the member has no counter yet.
	GetRate(ctx context.Context, communityID, userID uuid.UUID) (*models.MemberRate, error)
	// UpsertRate writes the counter state.
	UpsertRate(ctx context.Context, rate models.MemberRate) error
}
