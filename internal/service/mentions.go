package service

import (
	"context"
	"regexp"

	"github.com/google/uuid"

	"messaging_service/internal/models"
)

// mentionPattern matches the `<@{user_id}>` token syntax of the message contract.
// Only canonical UUIDs are accepted, so malformed tokens are simply not mentions.
var mentionPattern = regexp.MustCompile(
	`<@([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})>`)

// ParseMentions extracts the mention candidates from content, deduplicated and in
// order of first appearance.
func ParseMentions(content string) []uuid.UUID {
	matches := mentionPattern.FindAllStringSubmatch(content, -1)
	if len(matches) == 0 {
		return nil
	}
	seen := make(map[uuid.UUID]struct{}, len(matches))
	mentions := make([]uuid.UUID, 0, len(matches))
	for _, match := range matches {
		id, err := uuid.Parse(match[1])
		if err != nil {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		mentions = append(mentions, id)
	}
	return mentions
}

// resolveChannelMentions validates mention candidates against the community's
// membership projection. Unknown or removed users are silently dropped, as
// required by US-011.
func (d Deps) resolveChannelMentions(ctx context.Context, communityID uuid.UUID, content string) ([]uuid.UUID, error) {
	candidates := ParseMentions(content)
	if len(candidates) == 0 {
		return nil, nil
	}
	return d.Community.FilterActiveMemberIDs(ctx, communityID, candidates)
}

// resolveConversationMentions validates mention candidates against the
// conversation's participant projection.
func (d Deps) resolveConversationMentions(ctx context.Context, conversationID uuid.UUID, content string) ([]uuid.UUID, error) {
	candidates := ParseMentions(content)
	if len(candidates) == 0 {
		return nil, nil
	}
	return d.Conversations.FilterParticipantIDs(ctx, conversationID, candidates)
}

// countLinks counts http(s) link prefixes for the automod link-flood rule.
func countLinks(content string) int {
	return len(linkPattern.FindAllStringIndex(content, -1))
}

var linkPattern = regexp.MustCompile(`https?://`)

// mentionsExceeding reports whether the resolved mention count breaches the rule.
func mentionsExceed(mentionCount, limit int) bool {
	return limit >= 0 && mentionCount > limit
}

// isAttachmentSupportedType reports whether the verified content type is one of
// the four allowed image types.
func isAttachmentSupportedType(contentType string) bool {
	return models.SupportedImageType(contentType)
}
