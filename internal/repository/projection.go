package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"messaging_service/internal/models"
)

// communityProjectionRepository is the pgx-backed implementation of the
// membership, text-channel and automod read models fed by EVT-002/003/005.
//
// Every write is a version-guarded upsert: the row is only replaced when the
// incoming entity version is strictly newer than the stored one, which makes
// at-least-once delivery converge instead of regressing state.
type communityProjectionRepository struct {
	base
}

// NewCommunityProjectionRepository builds the community projection repository.
func NewCommunityProjectionRepository(db Querier) CommunityProjectionRepository {
	return &communityProjectionRepository{base{db: db}}
}

// GetTextChannel returns the projected text channel, or nil when unknown.
func (r *communityProjectionRepository) GetTextChannel(ctx context.Context, channelID uuid.UUID) (*models.TextChannel, error) {
	const query = `SELECT channel_id::text, community_id::text, name, position, channel_version
FROM text_channels WHERE channel_id = $1`
	var (
		id, communityID, name string
		position              int
		version               int64
	)
	if err := r.q(ctx).QueryRow(ctx, query, uuidParam(channelID)).Scan(&id, &communityID, &name, &position, &version); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("repository: loading text channel: %w", err)
	}
	parsedChannelID, err := parseUUID(id)
	if err != nil {
		return nil, err
	}
	parsedCommunityID, err := parseUUID(communityID)
	if err != nil {
		return nil, err
	}
	return &models.TextChannel{
		ChannelID:      parsedChannelID,
		CommunityID:    parsedCommunityID,
		Name:           name,
		Position:       position,
		ChannelVersion: version,
	}, nil
}

// UpsertTextChannel applies EVT-003 when it carries a newer channel version.
func (r *communityProjectionRepository) UpsertTextChannel(ctx context.Context, channel models.TextChannel) (bool, error) {
	const query = `INSERT INTO text_channels (channel_id, community_id, name, position, channel_version)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (channel_id) DO UPDATE
SET community_id = EXCLUDED.community_id, name = EXCLUDED.name,
    position = EXCLUDED.position, channel_version = EXCLUDED.channel_version
WHERE text_channels.channel_version < EXCLUDED.channel_version
RETURNING channel_version`
	var version int64
	err := r.q(ctx).QueryRow(ctx, query, uuidParam(channel.ChannelID), uuidParam(channel.CommunityID),
		channel.Name, channel.Position, channel.ChannelVersion).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("repository: upserting text channel: %w", err)
	}
	return true, nil
}

// DeleteTextChannel removes a deleted channel when the event is newer.
func (r *communityProjectionRepository) DeleteTextChannel(ctx context.Context, channelID uuid.UUID, version int64) (bool, error) {
	const query = `DELETE FROM text_channels WHERE channel_id = $1 AND channel_version < $2`
	tag, err := r.q(ctx).Exec(ctx, query, uuidParam(channelID), version)
	if err != nil {
		return false, fmt.Errorf("repository: deleting text channel: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// GetMembership returns the projected membership, or nil when absent.
func (r *communityProjectionRepository) GetMembership(ctx context.Context, communityID, userID uuid.UUID) (*models.CommunityMember, error) {
	const query = `SELECT community_id::text, user_id::text, status, is_banned, muted_until, role_ids::text[], member_version
FROM community_members WHERE community_id = $1 AND user_id = $2`
	member, err := scanMembership(r.q(ctx).QueryRow(ctx, query, uuidParam(communityID), uuidParam(userID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("repository: loading membership: %w", err)
	}
	return member, nil
}

// UpsertMembership applies EVT-002 when it carries a newer member version.
func (r *communityProjectionRepository) UpsertMembership(ctx context.Context, member models.CommunityMember) (bool, error) {
	const query = `INSERT INTO community_members (community_id, user_id, status, is_banned, muted_until, role_ids, member_version)
VALUES ($1, $2, $3, $4, $5, $6::text[]::uuid[], $7)
ON CONFLICT (community_id, user_id) DO UPDATE
SET status = EXCLUDED.status, is_banned = EXCLUDED.is_banned, muted_until = EXCLUDED.muted_until,
    role_ids = EXCLUDED.role_ids, member_version = EXCLUDED.member_version
WHERE community_members.member_version < EXCLUDED.member_version
RETURNING member_version`
	var version int64
	err := r.q(ctx).QueryRow(ctx, query, uuidParam(member.CommunityID), uuidParam(member.UserID), member.Status,
		member.IsBanned, member.MutedUntil, uuidStrings(member.RoleIDs), member.MemberVersion).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("repository: upserting membership: %w", err)
	}
	return true, nil
}

// ActiveCommunityIDs answers API-003 for community scopes: muted members keep
// access, removed and banned members lose it.
func (r *communityProjectionRepository) ActiveCommunityIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	const query = `SELECT community_id::text FROM community_members
WHERE user_id = $1 AND status = 'active' AND is_banned = false ORDER BY community_id`
	return r.queryUUIDList(ctx, query, uuidParam(userID))
}

// FilterActiveMemberIDs keeps only candidates with an active membership, which
// is how mention tokens are validated.
func (r *communityProjectionRepository) FilterActiveMemberIDs(ctx context.Context, communityID uuid.UUID, userIDs []uuid.UUID) ([]uuid.UUID, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}
	const query = `SELECT user_id::text FROM community_members
WHERE community_id = $1 AND status = 'active' AND is_banned = false AND user_id = ANY($2::text[]::uuid[])
ORDER BY user_id`
	return r.queryUUIDList(ctx, query, uuidParam(communityID), uuidStrings(userIDs))
}

// GetAutomodRuleSet returns the projected rule set, or nil when the community
// has none.
func (r *communityProjectionRepository) GetAutomodRuleSet(ctx context.Context, communityID uuid.UUID) (*models.AutomodRuleSet, error) {
	const query = `SELECT community_id::text, rule_set_version, spam_enabled, spam_action, max_messages_per_window,
       window_seconds, max_duplicate_messages, max_mentions_per_message, max_links_per_message,
       banned_words_enabled, banned_words_action, banned_words_match_mode,
       banned_words::text[], exempt_role_ids::text[]
FROM automod_rule_sets WHERE community_id = $1`
	var (
		id                                                                string
		version                                                           int64
		spamEnabled, bannedEnabled                                        bool
		spamAction, bannedAction, matchMode                               string
		maxPerWindow, windowSeconds, maxDuplicates, maxMentions, maxLinks int
		words, exemptRoles                                                []string
	)
	if err := r.q(ctx).QueryRow(ctx, query, uuidParam(communityID)).Scan(&id, &version, &spamEnabled, &spamAction,
		&maxPerWindow, &windowSeconds, &maxDuplicates, &maxMentions, &maxLinks, &bannedEnabled, &bannedAction,
		&matchMode, &words, &exemptRoles); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("repository: loading automod rule set: %w", err)
	}
	parsedID, err := parseUUID(id)
	if err != nil {
		return nil, err
	}
	exemptIDs, err := uuidList(exemptRoles)
	if err != nil {
		return nil, err
	}
	return &models.AutomodRuleSet{
		CommunityID:    parsedID,
		RuleSetVersion: version,
		Spam: models.SpamRules{
			Enabled:               spamEnabled,
			Action:                spamAction,
			MaxMessagesPerWindow:  maxPerWindow,
			WindowSeconds:         windowSeconds,
			MaxDuplicateMessages:  maxDuplicates,
			MaxMentionsPerMessage: maxMentions,
			MaxLinksPerMessage:    maxLinks,
		},
		BannedWords: models.BannedWordsRules{
			Enabled:   bannedEnabled,
			Action:    bannedAction,
			MatchMode: matchMode,
			Words:     words,
		},
		ExemptRoleIDs: exemptIDs,
	}, nil
}

// UpsertAutomodRuleSet replaces the snapshot when the version is newer.
func (r *communityProjectionRepository) UpsertAutomodRuleSet(ctx context.Context, ruleSet models.AutomodRuleSet) (bool, error) {
	const query = `INSERT INTO automod_rule_sets (community_id, rule_set_version, spam_enabled, spam_action,
    max_messages_per_window, window_seconds, max_duplicate_messages, max_mentions_per_message, max_links_per_message,
    banned_words_enabled, banned_words_action, banned_words_match_mode, banned_words, exempt_role_ids)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::text[], $14::text[]::uuid[])
ON CONFLICT (community_id) DO UPDATE
SET rule_set_version = EXCLUDED.rule_set_version, spam_enabled = EXCLUDED.spam_enabled,
    spam_action = EXCLUDED.spam_action, max_messages_per_window = EXCLUDED.max_messages_per_window,
    window_seconds = EXCLUDED.window_seconds, max_duplicate_messages = EXCLUDED.max_duplicate_messages,
    max_mentions_per_message = EXCLUDED.max_mentions_per_message, max_links_per_message = EXCLUDED.max_links_per_message,
    banned_words_enabled = EXCLUDED.banned_words_enabled, banned_words_action = EXCLUDED.banned_words_action,
    banned_words_match_mode = EXCLUDED.banned_words_match_mode, banned_words = EXCLUDED.banned_words,
    exempt_role_ids = EXCLUDED.exempt_role_ids
WHERE automod_rule_sets.rule_set_version < EXCLUDED.rule_set_version
RETURNING rule_set_version`
	var version int64
	err := r.q(ctx).QueryRow(ctx, query, uuidParam(ruleSet.CommunityID), ruleSet.RuleSetVersion,
		ruleSet.Spam.Enabled, ruleSet.Spam.Action, ruleSet.Spam.MaxMessagesPerWindow, ruleSet.Spam.WindowSeconds,
		ruleSet.Spam.MaxDuplicateMessages, ruleSet.Spam.MaxMentionsPerMessage, ruleSet.Spam.MaxLinksPerMessage,
		ruleSet.BannedWords.Enabled, ruleSet.BannedWords.Action, ruleSet.BannedWords.MatchMode,
		ruleSet.BannedWords.Words, uuidStrings(ruleSet.ExemptRoleIDs)).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("repository: upserting automod rule set: %w", err)
	}
	return true, nil
}

// scanMembership scans one community_members row.
func scanMembership(row rowScanner) (*models.CommunityMember, error) {
	var (
		communityID, userID, status string
		isBanned                    bool
		mutedUntil                  *time.Time
		roleIDs                     []string
		version                     int64
	)
	if err := row.Scan(&communityID, &userID, &status, &isBanned, &mutedUntil, &roleIDs, &version); err != nil {
		return nil, err
	}
	parsedCommunityID, err := parseUUID(communityID)
	if err != nil {
		return nil, err
	}
	parsedUserID, err := parseUUID(userID)
	if err != nil {
		return nil, err
	}
	roles, err := uuidList(roleIDs)
	if err != nil {
		return nil, err
	}
	return &models.CommunityMember{
		CommunityID:   parsedCommunityID,
		UserID:        parsedUserID,
		Status:        status,
		IsBanned:      isBanned,
		MutedUntil:    mutedUntil,
		RoleIDs:       roles,
		MemberVersion: version,
	}, nil
}

// queryUUIDList runs a single-column uuid::text query and parses the result.
func (r *communityProjectionRepository) queryUUIDList(ctx context.Context, query string, args ...any) ([]uuid.UUID, error) {
	rows, err := r.q(ctx).Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("repository: querying uuid list: %w", err)
	}
	defer rows.Close()
	var raw []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, fmt.Errorf("repository: scanning uuid list: %w", err)
		}
		raw = append(raw, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: reading uuid list: %w", err)
	}
	return uuidList(raw)
}

// conversationProjectionRepository is the pgx-backed implementation of the
// conversation and participant read models fed by EVT-008.
type conversationProjectionRepository struct {
	base
}

// NewConversationProjectionRepository builds the conversation projection repository.
func NewConversationProjectionRepository(db Querier) ConversationProjectionRepository {
	return &conversationProjectionRepository{base{db: db}}
}

// GetConversation loads the conversation and its current participant list.
func (r *conversationProjectionRepository) GetConversation(ctx context.Context, conversationID uuid.UUID) (*models.Conversation, error) {
	const query = `SELECT conversation_id::text, kind, name, conversation_version FROM conversations WHERE conversation_id = $1`
	var (
		id, kind string
		name     *string
		version  int64
	)
	if err := r.q(ctx).QueryRow(ctx, query, uuidParam(conversationID)).Scan(&id, &kind, &name, &version); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("repository: loading conversation: %w", err)
	}
	parsedID, err := parseUUID(id)
	if err != nil {
		return nil, err
	}
	participants, err := r.queryUUIDList(ctx,
		`SELECT user_id::text FROM conversation_participants WHERE conversation_id = $1 ORDER BY user_id`,
		uuidParam(conversationID))
	if err != nil {
		return nil, err
	}
	return &models.Conversation{
		ConversationID:      parsedID,
		Kind:                kind,
		Name:                name,
		ConversationVersion: version,
		ParticipantIDs:      participants,
	}, nil
}

// UpsertConversation replaces the conversation snapshot and its participant set
// when the incoming version is newer. The participant table is only touched for
// an applied update, so a stale event cannot rewrite the participant list.
func (r *conversationProjectionRepository) UpsertConversation(ctx context.Context, conversation models.Conversation) (bool, error) {
	const upsert = `INSERT INTO conversations (conversation_id, kind, name, conversation_version)
VALUES ($1, $2, $3, $4)
ON CONFLICT (conversation_id) DO UPDATE
SET kind = EXCLUDED.kind, name = EXCLUDED.name, conversation_version = EXCLUDED.conversation_version
WHERE conversations.conversation_version < EXCLUDED.conversation_version
RETURNING conversation_version`
	var version int64
	err := r.q(ctx).QueryRow(ctx, upsert, uuidParam(conversation.ConversationID), conversation.Kind,
		conversation.Name, conversation.ConversationVersion).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("repository: upserting conversation: %w", err)
	}

	const prune = `DELETE FROM conversation_participants
WHERE conversation_id = $1 AND NOT (user_id = ANY($2::text[]::uuid[]))`
	if _, err := r.q(ctx).Exec(ctx, prune, uuidParam(conversation.ConversationID),
		uuidStrings(conversation.ParticipantIDs)); err != nil {
		return false, fmt.Errorf("repository: pruning participants: %w", err)
	}
	if len(conversation.ParticipantIDs) == 0 {
		return true, nil
	}
	const insert = `INSERT INTO conversation_participants (conversation_id, user_id)
SELECT $1, unnest($2::text[]::uuid[]) ON CONFLICT DO NOTHING`
	if _, err := r.q(ctx).Exec(ctx, insert, uuidParam(conversation.ConversationID),
		uuidStrings(conversation.ParticipantIDs)); err != nil {
		return false, fmt.Errorf("repository: inserting participants: %w", err)
	}
	return true, nil
}

// IsParticipant reports whether the user currently participates.
func (r *conversationProjectionRepository) IsParticipant(ctx context.Context, conversationID, userID uuid.UUID) (bool, error) {
	const query = `SELECT EXISTS (SELECT 1 FROM conversation_participants WHERE conversation_id = $1 AND user_id = $2)`
	var exists bool
	if err := r.q(ctx).QueryRow(ctx, query, uuidParam(conversationID), uuidParam(userID)).Scan(&exists); err != nil {
		return false, fmt.Errorf("repository: checking participant: %w", err)
	}
	return exists, nil
}

// FilterParticipantIDs keeps only candidates that participate in the
// conversation, which is how conversation mention tokens are validated.
func (r *conversationProjectionRepository) FilterParticipantIDs(ctx context.Context, conversationID uuid.UUID, userIDs []uuid.UUID) ([]uuid.UUID, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}
	const query = `SELECT user_id::text FROM conversation_participants
WHERE conversation_id = $1 AND user_id = ANY($2::text[]::uuid[]) ORDER BY user_id`
	rows, err := r.q(ctx).Query(ctx, query, uuidParam(conversationID), uuidStrings(userIDs))
	if err != nil {
		return nil, fmt.Errorf("repository: filtering participants: %w", err)
	}
	defer rows.Close()
	var raw []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, fmt.Errorf("repository: scanning participant: %w", err)
		}
		raw = append(raw, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: reading participants: %w", err)
	}
	return uuidList(raw)
}

// ConversationIDsForUser answers API-003 for conversation scopes.
func (r *conversationProjectionRepository) ConversationIDsForUser(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	const query = `SELECT conversation_id::text FROM conversation_participants WHERE user_id = $1 ORDER BY conversation_id`
	rows, err := r.q(ctx).Query(ctx, query, uuidParam(userID))
	if err != nil {
		return nil, fmt.Errorf("repository: loading conversation scopes: %w", err)
	}
	defer rows.Close()
	var raw []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, fmt.Errorf("repository: scanning conversation scope: %w", err)
		}
		raw = append(raw, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: reading conversation scopes: %w", err)
	}
	return uuidList(raw)
}

// queryUUIDList is shared with communityProjectionRepository via this free
// function so both projection repositories parse uuid text columns identically.
func (r *conversationProjectionRepository) queryUUIDList(ctx context.Context, query string, args ...any) ([]uuid.UUID, error) {
	rows, err := r.q(ctx).Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("repository: querying uuid list: %w", err)
	}
	defer rows.Close()
	var raw []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, fmt.Errorf("repository: scanning uuid list: %w", err)
		}
		raw = append(raw, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repository: reading uuid list: %w", err)
	}
	return uuidList(raw)
}
