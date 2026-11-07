package models

import (
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// CommunityMemberChangedPayload is the EVT-002 payload (producer
// community_service, topic community_events, key community_id).
type CommunityMemberChangedPayload struct {
	CommunityID   uuid.UUID   `json:"community_id"`
	UserID        uuid.UUID   `json:"user_id"`
	Change        string      `json:"change"`
	Status        string      `json:"status"`
	IsBanned      bool        `json:"is_banned"`
	MutedUntil    *time.Time  `json:"muted_until"`
	RoleIDs       []uuid.UUID `json:"role_ids"`
	SanctionID    *uuid.UUID  `json:"sanction_id"`
	MemberVersion int64       `json:"member_version"`
}

var memberChangeKinds = map[string]struct{}{
	"joined": {}, "left": {}, "removed": {}, "banned": {},
	"unbanned": {}, "muted": {}, "unmuted": {}, "roles_changed": {},
}

// Validate enforces the EVT-002 payload contract.
func (p CommunityMemberChangedPayload) Validate() error {
	if p.CommunityID == uuid.Nil || p.UserID == uuid.Nil {
		return fmt.Errorf("member payload: community_id and user_id are required")
	}
	if _, ok := memberChangeKinds[p.Change]; !ok {
		return fmt.Errorf("member payload: invalid change %q", p.Change)
	}
	if p.Status != MemberStatusActive && p.Status != MemberStatusRemoved {
		return fmt.Errorf("member payload: invalid status %q", p.Status)
	}
	if p.MemberVersion < 1 {
		return fmt.Errorf("member payload: member_version must be >= 1")
	}
	return nil
}

// CommunityChannelChangedPayload is the EVT-003 payload (producer
// community_service, topic community_events, key community_id).
type CommunityChannelChangedPayload struct {
	CommunityID    uuid.UUID `json:"community_id"`
	ChannelID      uuid.UUID `json:"channel_id"`
	Change         string    `json:"change"`
	Kind           string    `json:"kind"`
	Name           string    `json:"name"`
	Position       int       `json:"position"`
	Capacity       *int      `json:"capacity"`
	ChannelVersion int64     `json:"channel_version"`
}

// Validate enforces the EVT-003 payload contract.
func (p CommunityChannelChangedPayload) Validate() error {
	if p.CommunityID == uuid.Nil || p.ChannelID == uuid.Nil {
		return fmt.Errorf("channel payload: community_id and channel_id are required")
	}
	switch p.Change {
	case "created", "updated", "deleted":
	default:
		return fmt.Errorf("channel payload: invalid change %q", p.Change)
	}
	if p.Kind != "text" && p.Kind != "voice" {
		return fmt.Errorf("channel payload: invalid kind %q", p.Kind)
	}
	if p.Name == "" || utf8.RuneCountInString(p.Name) > 100 {
		return fmt.Errorf("channel payload: name must be 1..100 characters")
	}
	if p.Position < 0 {
		return fmt.Errorf("channel payload: position must be >= 0")
	}
	if p.ChannelVersion < 1 {
		return fmt.Errorf("channel payload: channel_version must be >= 1")
	}
	return nil
}

// AutomodRulesChangedPayload is the EVT-005 payload (producer
// moderation_service, topic moderation_events, key community_id). Every field
// is a full snapshot, so consumers replace their stored copy.
type AutomodRulesChangedPayload struct {
	CommunityID    uuid.UUID        `json:"community_id"`
	RuleSetVersion int64            `json:"rule_set_version"`
	Spam           SpamRules        `json:"spam"`
	BannedWords    BannedWordsRules `json:"banned_words"`
	ExemptRoleIDs  []uuid.UUID      `json:"exempt_role_ids"`
}

// Validate enforces the EVT-005 payload contract.
func (p AutomodRulesChangedPayload) Validate() error {
	if p.CommunityID == uuid.Nil {
		return fmt.Errorf("automod payload: community_id is required")
	}
	if p.RuleSetVersion < 1 {
		return fmt.Errorf("automod payload: rule_set_version must be >= 1")
	}
	if err := p.Spam.validate(); err != nil {
		return err
	}
	return p.BannedWords.validate()
}

func (s SpamRules) validate() error {
	if s.Action != AutomodActionBlock && s.Action != AutomodActionRemove {
		return fmt.Errorf("automod payload: invalid spam action %q", s.Action)
	}
	if s.MaxMessagesPerWindow < 1 {
		return fmt.Errorf("automod payload: max_messages_per_window must be >= 1")
	}
	if s.WindowSeconds < 1 || s.WindowSeconds > 3600 {
		return fmt.Errorf("automod payload: window_seconds must be within 1..3600")
	}
	if s.MaxDuplicateMessages < 1 {
		return fmt.Errorf("automod payload: max_duplicate_messages must be >= 1")
	}
	if s.MaxMentionsPerMessage < 1 {
		return fmt.Errorf("automod payload: max_mentions_per_message must be >= 1")
	}
	if s.MaxLinksPerMessage < 0 {
		return fmt.Errorf("automod payload: max_links_per_message must be >= 0")
	}
	return nil
}

func (b BannedWordsRules) validate() error {
	if b.Action != AutomodActionBlock && b.Action != AutomodActionRemove {
		return fmt.Errorf("automod payload: invalid banned words action %q", b.Action)
	}
	if b.MatchMode != "whole_word" && b.MatchMode != "substring" {
		return fmt.Errorf("automod payload: invalid match_mode %q", b.MatchMode)
	}
	if len(b.Words) > 1000 {
		return fmt.Errorf("automod payload: at most 1000 banned words")
	}
	for _, word := range b.Words {
		if word == "" || utf8.RuneCountInString(word) > 100 {
			return fmt.Errorf("automod payload: banned words must be 1..100 characters")
		}
	}
	return nil
}

// ConversationChangedPayload is the EVT-008 payload (producer social_service,
// topic social_events, key conversation_id).
type ConversationChangedPayload struct {
	ConversationID      uuid.UUID   `json:"conversation_id"`
	Kind                string      `json:"kind"`
	Change              string      `json:"change"`
	Name                *string     `json:"name"`
	ParticipantUserIDs  []uuid.UUID `json:"participant_user_ids"`
	AddedUserIDs        []uuid.UUID `json:"added_user_ids"`
	RemovedUserIDs      []uuid.UUID `json:"removed_user_ids"`
	ConversationVersion int64       `json:"conversation_version"`
}

// Validate enforces the EVT-008 payload contract.
func (p ConversationChangedPayload) Validate() error {
	if p.ConversationID == uuid.Nil {
		return fmt.Errorf("conversation payload: conversation_id is required")
	}
	if p.Kind != ConversationKindDirect && p.Kind != ConversationKindGroup {
		return fmt.Errorf("conversation payload: invalid kind %q", p.Kind)
	}
	switch p.Change {
	case "created", "participants_changed", "renamed":
	default:
		return fmt.Errorf("conversation payload: invalid change %q", p.Change)
	}
	if len(p.ParticipantUserIDs) < 1 || len(p.ParticipantUserIDs) > 10 {
		return fmt.Errorf("conversation payload: participant_user_ids must have 1..10 entries")
	}
	if p.ConversationVersion < 1 {
		return fmt.Errorf("conversation payload: conversation_version must be >= 1")
	}
	return nil
}

// AttachmentSnapshot is the EVT-009 attachment representation. It always
// carries a freshly generated short-lived download URL.
type AttachmentSnapshot struct {
	AttachmentID string    `json:"attachment_id"`
	ContentType  string    `json:"content_type"`
	SizeBytes    int64     `json:"size_bytes"`
	DownloadURL  string    `json:"download_url"`
	URLExpiresAt time.Time `json:"url_expires_at"`
}

// MessageChangedPayload is the EVT-009 payload this service produces on
// message_events keyed by container_id.
type MessageChangedPayload struct {
	MessageID        string               `json:"message_id"`
	Change           string               `json:"change"`
	ContainerType    string               `json:"container_type"`
	ContainerID      string               `json:"container_id"`
	CommunityID      *string              `json:"community_id"`
	AuthorUserID     string               `json:"author_user_id"`
	ThreadRootID     *string              `json:"thread_root_id"`
	Content          *string              `json:"content"`
	Attachments      []AttachmentSnapshot `json:"attachments"`
	MentionedUserIDs []string             `json:"mentioned_user_ids"`
	Reactions        []ReactionSummary    `json:"reactions"`
	DeletionReason   *string              `json:"deletion_reason"`
	CreatedAt        time.Time            `json:"created_at"`
	EditedAt         *time.Time           `json:"edited_at"`
	MessageVersion   int64                `json:"message_version"`
}

// Validate enforces the EVT-009 payload contract before it reaches the outbox.
func (p MessageChangedPayload) Validate() error {
	if p.MessageID == "" {
		return fmt.Errorf("message payload: message_id is required")
	}
	switch MessageChange(p.Change) {
	case ChangeCreated, ChangeEdited, ChangeDeleted, ChangeReactionsChanged:
	default:
		return fmt.Errorf("message payload: invalid change %q", p.Change)
	}
	if p.ContainerType != string(ContainerChannel) && p.ContainerType != string(ContainerConversation) {
		return fmt.Errorf("message payload: invalid container_type %q", p.ContainerType)
	}
	if p.ContainerID == "" {
		return fmt.Errorf("message payload: container_id is required")
	}
	if p.MessageVersion < 1 {
		return fmt.Errorf("message payload: message_version must be >= 1")
	}
	if len(p.Attachments) > 10 {
		return fmt.Errorf("message payload: at most 10 attachments")
	}
	return nil
}
