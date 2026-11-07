package mocks

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"

	"messaging_service/internal/models"
	"messaging_service/internal/objectstore"
)

// MessageRepository is a testify mock of repository.MessageRepository.
type MessageRepository struct{ mock.Mock }

// Insert implements repository.MessageRepository.
func (m *MessageRepository) Insert(ctx context.Context, msg *models.Message) (*models.Message, bool, error) {
	args := m.Called(ctx, msg)
	created, _ := args.Get(1).(bool)
	stored, _ := args.Get(0).(*models.Message)
	return stored, created, args.Error(2)
}

// Get implements MessageRepository.
func (m *MessageRepository) Get(ctx context.Context, id uuid.UUID) (*models.MessageAggregate, error) {
	args := m.Called(ctx, id)
	aggregate, _ := args.Get(0).(*models.MessageAggregate)
	return aggregate, args.Error(1)
}

// Page implements MessageRepository.
func (m *MessageRepository) Page(ctx context.Context, query models.HistoryQuery) ([]models.MessageAggregate, error) {
	args := m.Called(ctx, query)
	return aggregateSlice(args.Get(0)), args.Error(1)
}

// Replies implements MessageRepository.
func (m *MessageRepository) Replies(ctx context.Context, rootID uuid.UUID, cursor *models.Cursor, limit int) ([]models.MessageAggregate, error) {
	args := m.Called(ctx, rootID, cursor, limit)
	return aggregateSlice(args.Get(0)), args.Error(1)
}

// ReplyCount implements MessageRepository.
func (m *MessageRepository) ReplyCount(ctx context.Context, rootID uuid.UUID) (int, error) {
	args := m.Called(ctx, rootID)
	return args.Int(0), args.Error(1)
}

// UpdateContent implements MessageRepository.
func (m *MessageRepository) UpdateContent(ctx context.Context, id uuid.UUID, content string, editedAt time.Time) (*models.Message, error) {
	args := m.Called(ctx, id, content, editedAt)
	stored, _ := args.Get(0).(*models.Message)
	return stored, args.Error(1)
}

// SoftDelete implements MessageRepository.
func (m *MessageRepository) SoftDelete(ctx context.Context, id uuid.UUID, reason models.DeletionReason, deletedAt time.Time) (*models.Message, error) {
	args := m.Called(ctx, id, reason, deletedAt)
	stored, _ := args.Get(0).(*models.Message)
	return stored, args.Error(1)
}

// BumpVersion implements MessageRepository.
func (m *MessageRepository) BumpVersion(ctx context.Context, id uuid.UUID) (int64, error) {
	args := m.Called(ctx, id)
	return args.Get(0).(int64), args.Error(1)
}

// ReplaceMentions implements MessageRepository.
func (m *MessageRepository) ReplaceMentions(ctx context.Context, messageID uuid.UUID, mentioned []uuid.UUID) error {
	return m.Called(ctx, messageID, mentioned).Error(0)
}

// BindAttachments implements MessageRepository.
func (m *MessageRepository) BindAttachments(ctx context.Context, messageID, uploaderID uuid.UUID, verifiedAt time.Time, attachments []models.Attachment) error {
	return m.Called(ctx, messageID, uploaderID, verifiedAt, attachments).Error(0)
}

// DetachAttachments implements MessageRepository.
func (m *MessageRepository) DetachAttachments(ctx context.Context, messageID uuid.UUID) ([]string, error) {
	args := m.Called(ctx, messageID)
	keys, _ := args.Get(0).([]string)
	return keys, args.Error(1)
}

// AddReaction implements MessageRepository.
func (m *MessageRepository) AddReaction(ctx context.Context, messageID, userID uuid.UUID, emoji string, createdAt time.Time) (bool, error) {
	args := m.Called(ctx, messageID, userID, emoji, createdAt)
	return args.Bool(0), args.Error(1)
}

// RemoveReaction implements MessageRepository.
func (m *MessageRepository) RemoveReaction(ctx context.Context, messageID, userID uuid.UUID, emoji string) (bool, error) {
	args := m.Called(ctx, messageID, userID, emoji)
	return args.Bool(0), args.Error(1)
}

// ReactionSummary implements MessageRepository.
func (m *MessageRepository) ReactionSummary(ctx context.Context, messageID uuid.UUID) ([]models.ReactionSummary, error) {
	args := m.Called(ctx, messageID)
	summaries, _ := args.Get(0).([]models.ReactionSummary)
	return summaries, args.Error(1)
}

// CommunityProjectionRepository is a testify mock of the community projections.
type CommunityProjectionRepository struct{ mock.Mock }

// GetTextChannel implements the interface.
func (m *CommunityProjectionRepository) GetTextChannel(ctx context.Context, channelID uuid.UUID) (*models.TextChannel, error) {
	args := m.Called(ctx, channelID)
	channel, _ := args.Get(0).(*models.TextChannel)
	return channel, args.Error(1)
}

// UpsertTextChannel implements the interface.
func (m *CommunityProjectionRepository) UpsertTextChannel(ctx context.Context, channel models.TextChannel) (bool, error) {
	args := m.Called(ctx, channel)
	return args.Bool(0), args.Error(1)
}

// DeleteTextChannel implements the interface.
func (m *CommunityProjectionRepository) DeleteTextChannel(ctx context.Context, channelID uuid.UUID, version int64) (bool, error) {
	args := m.Called(ctx, channelID, version)
	return args.Bool(0), args.Error(1)
}

// GetMembership implements the interface.
func (m *CommunityProjectionRepository) GetMembership(ctx context.Context, communityID, userID uuid.UUID) (*models.CommunityMember, error) {
	args := m.Called(ctx, communityID, userID)
	member, _ := args.Get(0).(*models.CommunityMember)
	return member, args.Error(1)
}

// UpsertMembership implements the interface.
func (m *CommunityProjectionRepository) UpsertMembership(ctx context.Context, member models.CommunityMember) (bool, error) {
	args := m.Called(ctx, member)
	return args.Bool(0), args.Error(1)
}

// ActiveCommunityIDs implements the interface.
func (m *CommunityProjectionRepository) ActiveCommunityIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	args := m.Called(ctx, userID)
	ids, _ := args.Get(0).([]uuid.UUID)
	return ids, args.Error(1)
}

// FilterActiveMemberIDs implements the interface.
func (m *CommunityProjectionRepository) FilterActiveMemberIDs(ctx context.Context, communityID uuid.UUID, userIDs []uuid.UUID) ([]uuid.UUID, error) {
	args := m.Called(ctx, communityID, userIDs)
	ids, _ := args.Get(0).([]uuid.UUID)
	return ids, args.Error(1)
}

// GetAutomodRuleSet implements the interface.
func (m *CommunityProjectionRepository) GetAutomodRuleSet(ctx context.Context, communityID uuid.UUID) (*models.AutomodRuleSet, error) {
	args := m.Called(ctx, communityID)
	ruleSet, _ := args.Get(0).(*models.AutomodRuleSet)
	return ruleSet, args.Error(1)
}

// UpsertAutomodRuleSet implements the interface.
func (m *CommunityProjectionRepository) UpsertAutomodRuleSet(ctx context.Context, ruleSet models.AutomodRuleSet) (bool, error) {
	args := m.Called(ctx, ruleSet)
	return args.Bool(0), args.Error(1)
}

// ConversationProjectionRepository is a testify mock of the conversation projections.
type ConversationProjectionRepository struct{ mock.Mock }

// GetConversation implements the interface.
func (m *ConversationProjectionRepository) GetConversation(ctx context.Context, conversationID uuid.UUID) (*models.Conversation, error) {
	args := m.Called(ctx, conversationID)
	conversation, _ := args.Get(0).(*models.Conversation)
	return conversation, args.Error(1)
}

// UpsertConversation implements the interface.
func (m *ConversationProjectionRepository) UpsertConversation(ctx context.Context, conversation models.Conversation) (bool, error) {
	args := m.Called(ctx, conversation)
	return args.Bool(0), args.Error(1)
}

// IsParticipant implements the interface.
func (m *ConversationProjectionRepository) IsParticipant(ctx context.Context, conversationID, userID uuid.UUID) (bool, error) {
	args := m.Called(ctx, conversationID, userID)
	return args.Bool(0), args.Error(1)
}

// FilterParticipantIDs implements the interface.
func (m *ConversationProjectionRepository) FilterParticipantIDs(ctx context.Context, conversationID uuid.UUID, userIDs []uuid.UUID) ([]uuid.UUID, error) {
	args := m.Called(ctx, conversationID, userIDs)
	ids, _ := args.Get(0).([]uuid.UUID)
	return ids, args.Error(1)
}

// ConversationIDsForUser implements the interface.
func (m *ConversationProjectionRepository) ConversationIDsForUser(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	args := m.Called(ctx, userID)
	ids, _ := args.Get(0).([]uuid.UUID)
	return ids, args.Error(1)
}

// AttachmentRepository is a testify mock of repository.AttachmentRepository.
type AttachmentRepository struct{ mock.Mock }

// Create implements the interface.
func (m *AttachmentRepository) Create(ctx context.Context, attachment *models.Attachment) (*models.Attachment, bool, error) {
	args := m.Called(ctx, attachment)
	stored, _ := args.Get(0).(*models.Attachment)
	return stored, args.Bool(1), args.Error(2)
}

// Get implements the interface.
func (m *AttachmentRepository) Get(ctx context.Context, id uuid.UUID) (*models.Attachment, error) {
	args := m.Called(ctx, id)
	attachment, _ := args.Get(0).(*models.Attachment)
	return attachment, args.Error(1)
}

// ListPendingOlderThan implements the interface.
func (m *AttachmentRepository) ListPendingOlderThan(ctx context.Context, cutoff time.Time, limit int) ([]models.Attachment, error) {
	args := m.Called(ctx, cutoff, limit)
	stale, _ := args.Get(0).([]models.Attachment)
	return stale, args.Error(1)
}

// Delete implements the interface.
func (m *AttachmentRepository) Delete(ctx context.Context, id uuid.UUID) error {
	return m.Called(ctx, id).Error(0)
}

// OutboxRepository is a testify mock of repository.OutboxRepository.
type OutboxRepository struct{ mock.Mock }

// Append implements the interface.
func (m *OutboxRepository) Append(ctx context.Context, entry models.OutboxEntryView) error {
	return m.Called(ctx, entry).Error(0)
}

// ListUnpublished implements the interface.
func (m *OutboxRepository) ListUnpublished(ctx context.Context, limit int) ([]models.OutboxEntry, error) {
	args := m.Called(ctx, limit)
	entries, _ := args.Get(0).([]models.OutboxEntry)
	return entries, args.Error(1)
}

// MarkPublished implements the interface.
func (m *OutboxRepository) MarkPublished(ctx context.Context, id int64, publishedAt time.Time) error {
	return m.Called(ctx, id, publishedAt).Error(0)
}

// RecordAttempt implements the interface.
func (m *OutboxRepository) RecordAttempt(ctx context.Context, id int64) error {
	return m.Called(ctx, id).Error(0)
}

// CountUnpublished implements the interface.
func (m *OutboxRepository) CountUnpublished(ctx context.Context) (int64, error) {
	args := m.Called(ctx)
	return args.Get(0).(int64), args.Error(1)
}

// RateRepository is a testify mock of repository.RateRepository.
type RateRepository struct{ mock.Mock }

// GetRate implements the interface.
func (m *RateRepository) GetRate(ctx context.Context, communityID, userID uuid.UUID) (*models.MemberRate, error) {
	args := m.Called(ctx, communityID, userID)
	rate, _ := args.Get(0).(*models.MemberRate)
	return rate, args.Error(1)
}

// UpsertRate implements the interface.
func (m *RateRepository) UpsertRate(ctx context.Context, rate models.MemberRate) error {
	return m.Called(ctx, rate).Error(0)
}

// ProcessedEventRepository is a testify mock of the dedupe table.
type ProcessedEventRepository struct{ mock.Mock }

// IsProcessed implements the interface.
func (m *ProcessedEventRepository) IsProcessed(ctx context.Context, eventID uuid.UUID) (bool, error) {
	args := m.Called(ctx, eventID)
	return args.Bool(0), args.Error(1)
}

// MarkProcessed implements the interface.
func (m *ProcessedEventRepository) MarkProcessed(ctx context.Context, eventID uuid.UUID, eventType string) (bool, error) {
	args := m.Called(ctx, eventID, eventType)
	return args.Bool(0), args.Error(1)
}

// PruneOlderThan implements the interface.
func (m *ProcessedEventRepository) PruneOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	args := m.Called(ctx, cutoff)
	return args.Get(0).(int64), args.Error(1)
}

// TransactionManager runs the function directly, mirroring the pgx behaviour
// closely enough for service tests while recording the calls.
type TransactionManager struct{ mock.Mock }

// InTx implements repository.TransactionManager.
func (m *TransactionManager) InTx(ctx context.Context, fn func(context.Context) error) error {
	m.Called(ctx)
	return fn(ctx)
}

// ObjectStore is a testify mock of objectstore.ObjectStore.
type ObjectStore struct{ mock.Mock }

// PresignPut implements the interface.
func (m *ObjectStore) PresignPut(ctx context.Context, key, contentType string, ttl time.Duration) (objectstore.SignedURL, error) {
	args := m.Called(ctx, key, contentType, ttl)
	signed, _ := args.Get(0).(objectstore.SignedURL)
	return signed, args.Error(1)
}

// PresignGet implements the interface.
func (m *ObjectStore) PresignGet(ctx context.Context, key string, ttl time.Duration) (objectstore.SignedURL, error) {
	args := m.Called(ctx, key, ttl)
	signed, _ := args.Get(0).(objectstore.SignedURL)
	return signed, args.Error(1)
}

// Head implements the interface.
func (m *ObjectStore) Head(ctx context.Context, key string) (*objectstore.ObjectInfo, error) {
	args := m.Called(ctx, key)
	info, _ := args.Get(0).(*objectstore.ObjectInfo)
	return info, args.Error(1)
}

// Delete implements the interface.
func (m *ObjectStore) Delete(ctx context.Context, key string) error {
	return m.Called(ctx, key).Error(0)
}

// Check implements the interface.
func (m *ObjectStore) Check(ctx context.Context) error { return m.Called(ctx).Error(0) }

// Publisher is a testify mock of eventbus.Publisher.
type Publisher struct{ mock.Mock }

// Publish implements the interface.
func (m *Publisher) Publish(ctx context.Context, topic, key string, event models.Event) error {
	return m.Called(ctx, topic, key, event).Error(0)
}

// Check implements the interface.
func (m *Publisher) Check(ctx context.Context) error { return m.Called(ctx).Error(0) }

// Close implements the interface.
func (m *Publisher) Close() error { return m.Called().Error(0) }

func aggregateSlice(value any) []models.MessageAggregate {
	aggregates, _ := value.([]models.MessageAggregate)
	return aggregates
}
