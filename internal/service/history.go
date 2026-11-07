package service

import (
	"context"

	"github.com/google/uuid"

	"messaging_service/internal/models"
)

// HistoryCommand describes one history page request.
type HistoryCommand struct {
	ContainerType models.ContainerType
	ContainerID   uuid.UUID
	UserID        uuid.UUID
	Limit         int
	Before        *models.Cursor
	After         *models.Cursor
}

// ThreadCommand describes one thread page request.
type ThreadCommand struct {
	MessageID uuid.UUID
	UserID    uuid.UUID
	Cursor    *models.Cursor
	Limit     int
}

// ReactCommand describes one reaction add/remove request.
type ReactionCommand struct {
	MessageID uuid.UUID
	UserID    uuid.UUID
	Emoji     string
	Add       bool
}

// History pages through a container's messages newest-first, honouring a
// `before` cursor for scrollback and an `after` cursor for reconnect catch-up.
// Deleted messages stay in place as tombstones (US-005, US-006).
func (s *MessageService) History(ctx context.Context, cmd HistoryCommand) (*models.MessagePage, error) {
	if cmd.Before != nil && cmd.After != nil {
		return nil, models.ErrInvalidRequest.WithDetail("Specify only one of before or after.")
	}
	if err := s.authorizeRead(ctx, cmd.ContainerType, cmd.ContainerID, cmd.UserID); err != nil {
		return nil, err
	}
	limit := s.deps.pageLimit(cmd.Limit)
	aggregates, err := s.deps.Messages.Page(ctx, models.HistoryQuery{
		ContainerType: cmd.ContainerType,
		ContainerID:   cmd.ContainerID,
		Limit:         limit + 1,
		Before:        cmd.Before,
		After:         cmd.After,
	})
	if err != nil {
		return nil, err
	}
	hasMore := len(aggregates) > limit
	if hasMore {
		aggregates = aggregates[:limit]
	}
	views, err := s.deps.messageViews(ctx, aggregates)
	if err != nil {
		return nil, err
	}
	return &models.MessagePage{
		Data:       views,
		NextCursor: nextCursor(aggregates, hasMore),
		HasMore:    hasMore,
	}, nil
}

// Thread returns the thread root, the total reply count and a page of replies.
// A reply addressed directly resolves to its top-level root (no nested threads).
func (s *MessageService) Thread(ctx context.Context, cmd ThreadCommand) (*models.ThreadPage, error) {
	aggregate, err := s.loadAggregate(ctx, cmd.MessageID)
	if err != nil {
		return nil, err
	}
	if authErr := s.auth.messageForRead(ctx, aggregate.Message, cmd.UserID); authErr != nil {
		return nil, authErr
	}
	rootID := aggregate.Message.ID
	if aggregate.Message.ThreadRootID != nil {
		rootID = *aggregate.Message.ThreadRootID
		parent, loadErr := s.loadAggregate(ctx, rootID)
		if err != nil {
			return nil, loadErr
		}
		aggregate = parent
	}

	replyCount, err := s.deps.Messages.ReplyCount(ctx, rootID)
	if err != nil {
		return nil, err
	}
	limit := s.deps.pageLimit(cmd.Limit)
	replies, err := s.deps.Messages.Replies(ctx, replyRootID(aggregate.Message, rootID), cmd.Cursor, limit+1)
	if err != nil {
		return nil, err
	}
	hasMore := len(replies) > limit
	if hasMore {
		replies = replies[:limit]
	}
	rootView, err := s.deps.messageView(ctx, *aggregate)
	if err != nil {
		return nil, err
	}
	replyViews, err := s.deps.messageViews(ctx, replies)
	if err != nil {
		return nil, err
	}
	return &models.ThreadPage{
		Root:       rootView,
		ReplyCount: replyCount,
		Replies:    replyViews,
		NextCursor: nextCursor(replies, hasMore),
		HasMore:    hasMore,
	}, nil
}

// replyRootID returns the root identifier replies hang off.
func replyRootID(message models.Message, resolved uuid.UUID) uuid.UUID {
	if message.ThreadRootID != nil {
		return *message.ThreadRootID
	}
	return resolved
}

// React adds or removes the caller's reaction. Both directions are idempotent;
// an unchanged summary emits no event (US-010, US-017).
func (s *MessageService) React(ctx context.Context, cmd ReactionCommand) (*models.ReactionSummaryResponse, error) {
	aggregate, err := s.loadAggregate(ctx, cmd.MessageID)
	if err != nil {
		return nil, err
	}
	if authErr := s.auth.messageForRead(ctx, aggregate.Message, cmd.UserID); authErr != nil {
		return nil, authErr
	}
	if aggregate.Message.IsDeleted() {
		// Reacting to a tombstone is a 404: the message is gone for readers.
		return nil, models.ErrNotFound.WithDetail("The message does not exist.")
	}

	now := s.deps.now()
	var (
		changed bool
		summary []models.ReactionSummary
	)
	err = s.deps.Tx.InTx(ctx, func(txCtx context.Context) error {
		if cmd.Add {
			changed, err = s.deps.Messages.AddReaction(txCtx, aggregate.Message.ID, cmd.UserID, cmd.Emoji, now)
		} else {
			changed, err = s.deps.Messages.RemoveReaction(txCtx, aggregate.Message.ID, cmd.UserID, cmd.Emoji)
		}
		if err != nil {
			return err
		}
		if !changed {
			summary = aggregate.Reactions
			return nil
		}
		version, versionErr := s.deps.Messages.BumpVersion(txCtx, aggregate.Message.ID)
		if versionErr != nil {
			return versionErr
		}
		current, summaryErr := s.deps.Messages.ReactionSummary(txCtx, aggregate.Message.ID)
		if summaryErr != nil {
			return summaryErr
		}
		summary = current
		changedMessage := aggregate.Message
		changedMessage.MessageVersion = version
		return s.appendOutbox(txCtx, models.MessageAggregate{
			Message:          changedMessage,
			Attachments:      aggregate.Attachments,
			MentionedUserIDs: aggregate.MentionedUserIDs,
			Reactions:        current,
		}, models.ChangeReactionsChanged)
	})
	if err != nil {
		return nil, err
	}

	action := "remove"
	if cmd.Add {
		action = "add"
	}
	if changed {
		s.deps.Metrics.IncReactions(action)
	}
	if summary == nil {
		summary = []models.ReactionSummary{}
	}
	return &models.ReactionSummaryResponse{
		MessageID: aggregate.Message.ID.String(),
		Reactions: summary,
	}, nil
}

// authorizeRead enforces read access on a container.
func (s *MessageService) authorizeRead(ctx context.Context, containerType models.ContainerType, containerID, userID uuid.UUID) error {
	switch containerType {
	case models.ContainerChannel:
		_, err := s.auth.channelForRead(ctx, containerID, userID)
		return err
	case models.ContainerConversation:
		_, err := s.auth.conversationForRead(ctx, containerID, userID)
		return err
	default:
		return models.ErrInvalidRequest.WithDetail("Unknown container type.")
	}
}

// nextCursor renders the cursor of the last returned message, or nil when the
// page is exhausted.
func nextCursor(aggregates []models.MessageAggregate, hasMore bool) *string {
	if !hasMore || len(aggregates) == 0 {
		return nil
	}
	last := aggregates[len(aggregates)-1].Message
	encoded := models.NewCursor(last.CreatedAt, last.ID).Encode()
	return &encoded
}
