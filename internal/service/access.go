package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"messaging_service/internal/models"
)

// authorizer answers every access question from local projections only, which is
// what keeps the hot path free of synchronous peer calls (ADR-001).
type authorizer struct {
	deps Deps
}

func newAuthorizer(deps Deps) *authorizer { return &authorizer{deps: deps} }

// channelAccess bundles the projection rows an authorized channel request needs.
type channelAccess struct {
	Channel models.TextChannel
	Member  models.CommunityMember
}

// channelForWrite resolves the text channel and enforces membership and mute.
// Order matches the documented send path: unknown channel → 404, not a member →
// 403, active mute → 403 with muted_until and the appeal pointer.
func (a *authorizer) channelForWrite(ctx context.Context, channelID, userID uuid.UUID, now time.Time) (*channelAccess, error) {
	channel, err := a.deps.Community.GetTextChannel(ctx, channelID)
	if err != nil {
		return nil, err
	}
	if channel == nil {
		return nil, models.ErrNotFound.WithDetail("The channel does not exist.")
	}
	member, err := a.deps.Community.GetMembership(ctx, channel.CommunityID, userID)
	if err != nil {
		return nil, err
	}
	if member == nil || !member.HasAccess() {
		return nil, models.ErrNotAMember
	}
	if member.IsMuted(now) {
		return nil, models.NewMemberMuted(*member.MutedUntil, a.appealURL(channel.CommunityID))
	}
	return &channelAccess{Channel: *channel, Member: *member}, nil
}

// channelForRead allows muted members to read history; only removal or a ban
// takes reading away.
func (a *authorizer) channelForRead(ctx context.Context, channelID, userID uuid.UUID) (*channelAccess, error) {
	channel, err := a.deps.Community.GetTextChannel(ctx, channelID)
	if err != nil {
		return nil, err
	}
	if channel == nil {
		return nil, models.ErrNotFound.WithDetail("The channel does not exist.")
	}
	member, err := a.deps.Community.GetMembership(ctx, channel.CommunityID, userID)
	if err != nil {
		return nil, err
	}
	if member == nil || !member.HasAccess() {
		return nil, models.ErrNotAMember
	}
	return &channelAccess{Channel: *channel, Member: *member}, nil
}

// conversationForWrite resolves the conversation and enforces participation.
func (a *authorizer) conversationForWrite(ctx context.Context, conversationID, userID uuid.UUID) (*models.Conversation, error) {
	conversation, err := a.deps.Conversations.GetConversation(ctx, conversationID)
	if err != nil {
		return nil, err
	}
	if conversation == nil {
		return nil, models.ErrNotFound.WithDetail("The conversation does not exist.")
	}
	if !conversation.IsParticipant(userID) {
		return nil, models.ErrNotAParticipant
	}
	return conversation, nil
}

// conversationForRead resolves the conversation for a read; conversation
// content is never revealed to non-participants.
func (a *authorizer) conversationForRead(ctx context.Context, conversationID, userID uuid.UUID) (*models.Conversation, error) {
	return a.conversationForWrite(ctx, conversationID, userID)
}

// messageForRead enforces read access on the container that holds a message.
func (a *authorizer) messageForRead(ctx context.Context, message models.Message, userID uuid.UUID) error {
	switch message.ContainerType {
	case models.ContainerChannel:
		if message.CommunityID == nil {
			return models.ErrNotFound.WithDetail("The channel does not exist.")
		}
		member, err := a.deps.Community.GetMembership(ctx, *message.CommunityID, userID)
		if err != nil {
			return err
		}
		if member == nil || !member.HasAccess() {
			return models.ErrNotAMember
		}
		return nil
	case models.ContainerConversation:
		_, err := a.conversationForRead(ctx, message.ContainerID, userID)
		return err
	default:
		return models.ErrNotFound
	}
}

// messageForWrite enforces read access and authorship on a message.
func (a *authorizer) messageForWrite(ctx context.Context, message models.Message, userID uuid.UUID) error {
	if err := a.messageForRead(ctx, message, userID); err != nil {
		return err
	}
	if message.AuthorUserID != userID {
		return models.ErrNotAuthor
	}
	return nil
}

// appealURL renders the appeal pointer handed to muted members.
func (a *authorizer) appealURL(communityID uuid.UUID) string {
	base := strings.TrimSuffix(a.deps.Options.AppealURLBase, "/")
	separator := "?"
	if strings.Contains(base, "?") {
		separator = "&"
	}
	return fmt.Sprintf("%s%scommunity_id=%s", base, separator, communityID)
}
