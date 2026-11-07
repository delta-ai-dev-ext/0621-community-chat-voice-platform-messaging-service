package models

import (
	"time"

	"github.com/google/uuid"
)

// Member statuses carried by EVT-002 and mirrored into the projection.
const (
	MemberStatusActive  = "active"
	MemberStatusRemoved = "removed"
)

// ConversationKinds carried by EVT-008.
const (
	ConversationKindDirect = "direct"
	ConversationKindGroup  = "group"
)

// Automod actions carried by EVT-005 rule sets.
const (
	AutomodActionBlock  = "block"
	AutomodActionRemove = "remove"
)

// Automod categories used in `messaging_automod_hits_total` and in the
// `blocked_by_automod` detail.
const (
	AutomodCategorySpam        = "spam"
	AutomodCategoryBannedWords = "banned_words"
)

// TextChannel is the text-channel projection row (EVT-003).
type TextChannel struct {
	ChannelID      uuid.UUID
	CommunityID    uuid.UUID
	Name           string
	Position       int
	ChannelVersion int64
}

// CommunityMember is the membership projection row (EVT-002).
type CommunityMember struct {
	CommunityID   uuid.UUID
	UserID        uuid.UUID
	Status        string
	IsBanned      bool
	MutedUntil    *time.Time
	RoleIDs       []uuid.UUID
	MemberVersion int64
}

// HasAccess reports whether the mirrored membership currently grants access.
// Muted members keep access (they may read); removed or banned members do not.
func (m CommunityMember) HasAccess() bool {
	return m.Status == MemberStatusActive && !m.IsBanned
}

// IsMuted reports whether the mute is still active at the given instant.
func (m CommunityMember) IsMuted(at time.Time) bool {
	return m.MutedUntil != nil && m.MutedUntil.After(at)
}

// Conversation is the conversation projection row (EVT-008).
type Conversation struct {
	ConversationID      uuid.UUID
	Kind                string
	Name                *string
	ConversationVersion int64
	ParticipantIDs      []uuid.UUID
}

// IsParticipant reports whether the given user is a current participant.
func (c Conversation) IsParticipant(userID uuid.UUID) bool {
	for _, id := range c.ParticipantIDs {
		if id == userID {
			return true
		}
	}
	return false
}

// SpamRules is the spam block of an EVT-005 rule set.
type SpamRules struct {
	Enabled               bool   `json:"enabled"`
	Action                string `json:"action"`
	MaxMessagesPerWindow  int    `json:"max_messages_per_window"`
	WindowSeconds         int    `json:"window_seconds"`
	MaxDuplicateMessages  int    `json:"max_duplicate_messages"`
	MaxMentionsPerMessage int    `json:"max_mentions_per_message"`
	MaxLinksPerMessage    int    `json:"max_links_per_message"`
}

// BannedWordsRules is the banned-words block of an EVT-005 rule set.
type BannedWordsRules struct {
	Enabled   bool     `json:"enabled"`
	Action    string   `json:"action"`
	MatchMode string   `json:"match_mode"`
	Words     []string `json:"words"`
}

// AutomodRuleSet is the community's current automod configuration mirrored
// from EVT-005. The payload is a full snapshot, so consumers replace their copy.
type AutomodRuleSet struct {
	CommunityID    uuid.UUID
	RuleSetVersion int64
	Spam           SpamRules
	BannedWords    BannedWordsRules
	ExemptRoleIDs  []uuid.UUID
}

// IsExempt reports whether any of the member's roles is automod-exempt.
func (rs AutomodRuleSet) IsExempt(roleIDs []uuid.UUID) bool {
	if len(rs.ExemptRoleIDs) == 0 || len(roleIDs) == 0 {
		return false
	}
	exempt := make(map[uuid.UUID]struct{}, len(rs.ExemptRoleIDs))
	for _, id := range rs.ExemptRoleIDs {
		exempt[id] = struct{}{}
	}
	for _, id := range roleIDs {
		if _, ok := exempt[id]; ok {
			return true
		}
	}
	return false
}

// MemberRate is the per-member sliding-window spam counter.
type MemberRate struct {
	CommunityID        uuid.UUID
	UserID             uuid.UUID
	WindowStartedAt    time.Time
	MessageCount       int
	RecentFingerprints []string
}
