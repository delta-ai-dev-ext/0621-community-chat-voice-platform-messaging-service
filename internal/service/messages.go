package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"messaging_service/internal/models"
	"messaging_service/internal/objectstore"
)

// MessageService owns the message lifecycle: idempotent sends, threads, edits,
// soft deletes and reaction bookkeeping.
type MessageService struct {
	deps    Deps
	auth    *authorizer
	automod *automodEngine
}

// NewMessageService builds the message service.
func NewMessageService(deps Deps) *MessageService {
	return &MessageService{deps: deps, auth: newAuthorizer(deps), automod: newAutomodEngine(deps)}
}

// SendCommand is one send attempt, already authenticated and validated at the
// HTTP boundary.
type SendCommand struct {
	ContainerType   models.ContainerType
	ContainerID     uuid.UUID
	AuthorUserID    uuid.UUID
	ClientMessageID string
	Content         string
	AttachmentIDs   []uuid.UUID
	ThreadRootID    *uuid.UUID
}

// EditCommand is one edit attempt.
type EditCommand struct {
	MessageID uuid.UUID
	UserID    uuid.UUID
	Content   string
}

// DeleteCommand is one delete attempt.
type DeleteCommand struct {
	MessageID uuid.UUID
	UserID    uuid.UUID
}

// Send posts a message, enforcing access, mentions, automod and attachment
// verification in the documented order, then persists everything in one
// transaction that also appends the EVT-009 outbox record.
func (s *MessageService) Send(ctx context.Context, cmd SendCommand) (*models.MessageView, error) {
	switch cmd.ContainerType {
	case models.ContainerChannel:
		return s.sendChannel(ctx, cmd)
	case models.ContainerConversation:
		return s.sendConversation(ctx, cmd)
	default:
		return nil, models.ErrInvalidRequest.WithDetail("Unknown container type.")
	}
}

func (s *MessageService) sendChannel(ctx context.Context, cmd SendCommand) (*models.MessageView, error) {
	now := s.deps.now()
	access, err := s.auth.channelForWrite(ctx, cmd.ContainerID, cmd.AuthorUserID, now)
	if err != nil {
		return nil, err
	}
	mentions, err := s.deps.resolveChannelMentions(ctx, access.Channel.CommunityID, cmd.Content)
	if err != nil {
		return nil, err
	}
	decision, err := s.automod.evaluate(ctx, automodInput{
		CommunityID:  access.Channel.CommunityID,
		UserID:       cmd.AuthorUserID,
		RoleIDs:      access.Member.RoleIDs,
		Content:      cmd.Content,
		MentionCount: len(mentions),
		Now:          now,
	})
	if err != nil {
		return nil, err
	}
	if decision.Action == automodBlock {
		// A blocked message is never persisted, so nothing downstream sees it.
		return nil, models.NewBlockedByAutomod(decision.Category)
	}
	communityID := access.Channel.CommunityID
	return s.persist(ctx, persistParams{
		ContainerType:   models.ContainerChannel,
		ContainerID:     cmd.ContainerID,
		CommunityID:     &communityID,
		AuthorUserID:    cmd.AuthorUserID,
		ClientMessageID: cmd.ClientMessageID,
		Content:         cmd.Content,
		Mentions:        mentions,
		AttachmentIDs:   cmd.AttachmentIDs,
		ThreadRootID:    cmd.ThreadRootID,
		Now:             now,
		RemoveByAutomod: decision.Action == automodRemove,
		NextRate:        decision.NextRate,
	})
}

func (s *MessageService) sendConversation(ctx context.Context, cmd SendCommand) (*models.MessageView, error) {
	now := s.deps.now()
	conversation, err := s.auth.conversationForWrite(ctx, cmd.ContainerID, cmd.AuthorUserID)
	if err != nil {
		return nil, err
	}
	mentions, err := s.deps.resolveConversationMentions(ctx, conversation.ConversationID, cmd.Content)
	if err != nil {
		return nil, err
	}
	return s.persist(ctx, persistParams{
		ContainerType:   models.ContainerConversation,
		ContainerID:     cmd.ContainerID,
		AuthorUserID:    cmd.AuthorUserID,
		ClientMessageID: cmd.ClientMessageID,
		Content:         cmd.Content,
		Mentions:        mentions,
		AttachmentIDs:   cmd.AttachmentIDs,
		ThreadRootID:    cmd.ThreadRootID,
		Now:             now,
	})
}

// persistParams is the fully authorized, automod-evaluated send request.
type persistParams struct {
	ContainerType   models.ContainerType
	ContainerID     uuid.UUID
	CommunityID     *uuid.UUID
	AuthorUserID    uuid.UUID
	ClientMessageID string
	Content         string
	Mentions        []uuid.UUID
	AttachmentIDs   []uuid.UUID
	ThreadRootID    *uuid.UUID
	Now             time.Time
	// RemoveByAutomod accepts the message and then tombstones it with reason
	// `automod` in the same transaction (US-014).
	RemoveByAutomod bool
	// NextRate is the spam-counter state to persist with the accepted message.
	NextRate *models.MemberRate
}

func (s *MessageService) persist(ctx context.Context, params persistParams) (*models.MessageView, error) {
	threadRootID, err := s.resolveThreadRoot(ctx, params)
	if err != nil {
		return nil, err
	}
	verified, err := s.verifyAttachments(ctx, params.AuthorUserID, params.AttachmentIDs, params.Now)
	if err != nil {
		return nil, err
	}

	clientMessageID := params.ClientMessageID
	message := &models.Message{
		ID:              uuid.New(),
		ContainerType:   params.ContainerType,
		ContainerID:     params.ContainerID,
		CommunityID:     params.CommunityID,
		AuthorUserID:    params.AuthorUserID,
		ThreadRootID:    threadRootID,
		Content:         &params.Content,
		ClientMessageID: &clientMessageID,
		MessageVersion:  1,
		CreatedAt:       params.Now,
	}

	var (
		result    models.MessageAggregate
		duplicate bool
	)
	err = s.deps.Tx.InTx(ctx, func(txCtx context.Context) error {
		stored, created, insertErr := s.deps.Messages.Insert(txCtx, message)
		if insertErr != nil {
			return insertErr
		}
		if !created {
			// The (container, author, client_message_id) index already held this
			// send: return the original unchanged, with no second event.
			duplicate = true
			existing, loadErr := s.deps.Messages.Get(txCtx, stored.ID)
			if loadErr != nil {
				return loadErr
			}
			if existing == nil {
				return fmt.Errorf("service: deduplicated message %s disappeared", stored.ID)
			}
			result = *existing
			return nil
		}

		if len(verified) > 0 {
			if bindErr := s.deps.Messages.BindAttachments(txCtx, stored.ID, params.AuthorUserID, params.Now, verified); bindErr != nil {
				return bindErr
			}
		}
		if mentionErr := s.deps.Messages.ReplaceMentions(txCtx, stored.ID, params.Mentions); mentionErr != nil {
			return mentionErr
		}
		if params.NextRate != nil {
			if rateErr := s.deps.Rates.UpsertRate(txCtx, *params.NextRate); rateErr != nil {
				return rateErr
			}
		}

		accepted := models.MessageAggregate{
			Message:          *stored,
			Attachments:      verified,
			MentionedUserIDs: params.Mentions,
			Reactions:        []models.ReactionSummary{},
		}
		if eventErr := s.appendOutbox(txCtx, accepted, models.ChangeCreated); eventErr != nil {
			return eventErr
		}
		result = accepted

		if params.RemoveByAutomod {
			deleted, deleteErr := s.deps.Messages.SoftDelete(txCtx, stored.ID, models.DeletionReasonAutomod, params.Now)
			if deleteErr != nil {
				return deleteErr
			}
			if _, detachErr := s.deps.Messages.DetachAttachments(txCtx, stored.ID); detachErr != nil {
				return detachErr
			}
			removed := models.MessageAggregate{
				Message:          *deleted,
				MentionedUserIDs: params.Mentions,
				Reactions:        []models.ReactionSummary{},
			}
			if eventErr := s.appendOutbox(txCtx, removed, models.ChangeDeleted); eventErr != nil {
				return eventErr
			}
			result = removed
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if duplicate {
		s.deps.Metrics.IncDuplicateSend()
	} else {
		s.deps.Metrics.IncMessagesSent(string(params.ContainerType))
		s.deps.Metrics.AddMentions(len(params.Mentions))
		if params.RemoveByAutomod {
			s.deps.Metrics.IncMessagesDeleted(string(models.DeletionReasonAutomod))
		}
	}
	view, err := s.deps.messageView(ctx, result)
	if err != nil {
		return nil, err
	}
	return &view, nil
}

// resolveThreadRoot validates the reply target: the root must exist, must not be
// deleted, must live in the same container, and replies never nest (a reply to a
// reply attaches to the same top-level root).
func (s *MessageService) resolveThreadRoot(ctx context.Context, params persistParams) (*uuid.UUID, error) {
	if params.ThreadRootID == nil {
		return nil, nil
	}
	root, err := s.deps.Messages.Get(ctx, *params.ThreadRootID)
	if err != nil {
		return nil, err
	}
	if root == nil || root.Message.IsDeleted() || root.Message.ContainerID != params.ContainerID {
		return nil, models.ErrNotFound.WithDetail("The message being replied to does not exist.")
	}
	rootID := root.Message.ID
	if root.Message.ThreadRootID != nil {
		rootID = *root.Message.ThreadRootID
	}
	return &rootID, nil
}

// verifyAttachments performs verify-on-send: every referenced attachment must
// belong to the sender, must still be unattached, and its stored object must
// really be a supported image within the size limit. Storage failures fail the
// send closed with 503 (ADR-003).
func (s *MessageService) verifyAttachments(ctx context.Context, uploaderID uuid.UUID, ids []uuid.UUID, now time.Time) ([]models.Attachment, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	verified := make([]models.Attachment, 0, len(ids))
	for _, id := range ids {
		attachment, err := s.deps.Attachments.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if attachment == nil || attachment.UploaderUserID != uploaderID ||
			attachment.Status != models.AttachmentPendingUpload || attachment.MessageID != nil {
			return nil, models.NewAttachmentInvalid("The referenced attachment is not available.")
		}
		info, err := s.deps.Objects.Head(ctx, attachment.ObjectKey)
		if err != nil {
			if errors.Is(err, objectstore.ErrObjectNotFound) {
				return nil, models.NewAttachmentInvalid("The referenced file was never uploaded.")
			}
			return nil, models.ErrStorageUnavailable
		}
		if info.SizeBytes <= 0 {
			return nil, models.NewAttachmentInvalid("The referenced file was never uploaded.")
		}
		if info.SizeBytes > s.deps.Options.MaxAttachmentBytes {
			return nil, models.ErrImageTooLarge.WithDetail("The referenced image exceeds the 10 MB limit.")
		}
		if !isAttachmentSupportedType(info.ContentType) || info.ContentType != attachment.DeclaredContentType {
			return nil, models.ErrUnsupportedImageType.WithDetail("The referenced file is not a supported image type.")
		}
		contentType := info.ContentType
		size := info.SizeBytes
		verifiedAt := now
		attachment.ActualContentType = &contentType
		attachment.ActualSizeBytes = &size
		attachment.VerifiedAt = &verifiedAt
		verified = append(verified, *attachment)
	}
	return verified, nil
}

// Edit replaces the content of the author's own message, re-evaluating automod
// and emitting an `edited` event (US-008).
func (s *MessageService) Edit(ctx context.Context, cmd EditCommand) (*models.MessageView, error) {
	aggregate, err := s.loadAggregate(ctx, cmd.MessageID)
	if err != nil {
		return nil, err
	}
	if authErr := s.auth.messageForWrite(ctx, aggregate.Message, cmd.UserID); authErr != nil {
		return nil, authErr
	}
	if aggregate.Message.IsDeleted() {
		return nil, models.ErrMessageDeleted
	}

	now := s.deps.now()
	message := aggregate.Message
	var mentions []uuid.UUID
	removeByAutomod := false
	var nextRate *models.MemberRate

	if message.ContainerType == models.ContainerChannel {
		if message.CommunityID == nil {
			return nil, models.ErrNotFound.WithDetail("The channel does not exist.")
		}
		member, memberErr := s.deps.Community.GetMembership(ctx, *message.CommunityID, cmd.UserID)
		if memberErr != nil {
			return nil, memberErr
		}
		mentions, err = s.deps.resolveChannelMentions(ctx, *message.CommunityID, cmd.Content)
		if err != nil {
			return nil, err
		}
		var roleIDs []uuid.UUID
		if member != nil {
			roleIDs = member.RoleIDs
		}
		decision, evalErr := s.automod.evaluate(ctx, automodInput{
			CommunityID: *message.CommunityID, UserID: cmd.UserID, RoleIDs: roleIDs,
			Content: cmd.Content, MentionCount: len(mentions), Now: now,
		})
		if evalErr != nil {
			return nil, evalErr
		}
		if decision.Action == automodBlock {
			return nil, models.NewBlockedByAutomod(decision.Category)
		}
		removeByAutomod = decision.Action == automodRemove
		nextRate = decision.NextRate
	} else {
		mentions, err = s.deps.resolveConversationMentions(ctx, message.ContainerID, cmd.Content)
		if err != nil {
			return nil, err
		}
	}

	var (
		result     models.MessageAggregate
		objectKeys []string
	)
	err = s.deps.Tx.InTx(ctx, func(txCtx context.Context) error {
		updated, updateErr := s.deps.Messages.UpdateContent(txCtx, message.ID, cmd.Content, now)
		if updateErr != nil {
			return updateErr
		}
		if mentionErr := s.deps.Messages.ReplaceMentions(txCtx, message.ID, mentions); mentionErr != nil {
			return mentionErr
		}
		if nextRate != nil {
			if rateErr := s.deps.Rates.UpsertRate(txCtx, *nextRate); rateErr != nil {
				return rateErr
			}
		}
		edited := models.MessageAggregate{
			Message:          *updated,
			Attachments:      aggregate.Attachments,
			MentionedUserIDs: mentions,
			Reactions:        aggregate.Reactions,
		}
		if eventErr := s.appendOutbox(txCtx, edited, models.ChangeEdited); eventErr != nil {
			return eventErr
		}
		result = edited

		if removeByAutomod {
			deleted, deleteErr := s.deps.Messages.SoftDelete(txCtx, message.ID, models.DeletionReasonAutomod, now)
			if deleteErr != nil {
				return deleteErr
			}
			keys, detachErr := s.deps.Messages.DetachAttachments(txCtx, message.ID)
			if detachErr != nil {
				return detachErr
			}
			objectKeys = keys
			removed := models.MessageAggregate{
				Message:          *deleted,
				MentionedUserIDs: mentions,
				Reactions:        aggregate.Reactions,
			}
			if eventErr := s.appendOutbox(txCtx, removed, models.ChangeDeleted); eventErr != nil {
				return eventErr
			}
			result = removed
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	s.deps.deleteObjects(ctx, objectKeys)
	s.deps.Metrics.IncMessagesEdited()
	if removeByAutomod {
		s.deps.Metrics.IncMessagesDeleted(string(models.DeletionReasonAutomod))
	}
	view, err := s.deps.messageView(ctx, result)
	if err != nil {
		return nil, err
	}
	return &view, nil
}

// Delete soft-deletes the author's own message. It is idempotent: deleting an
// already deleted message reports success without a second event (US-009).
func (s *MessageService) Delete(ctx context.Context, cmd DeleteCommand) error {
	aggregate, err := s.loadAggregate(ctx, cmd.MessageID)
	if err != nil {
		return err
	}
	if authErr := s.auth.messageForWrite(ctx, aggregate.Message, cmd.UserID); authErr != nil {
		return authErr
	}
	if aggregate.Message.IsDeleted() {
		return nil
	}

	now := s.deps.now()
	var objectKeys []string
	err = s.deps.Tx.InTx(ctx, func(txCtx context.Context) error {
		deleted, deleteErr := s.deps.Messages.SoftDelete(txCtx, aggregate.Message.ID, models.DeletionReasonAuthor, now)
		if deleteErr != nil {
			return deleteErr
		}
		keys, detachErr := s.deps.Messages.DetachAttachments(txCtx, aggregate.Message.ID)
		if deleteErr != nil {
			return detachErr
		}
		objectKeys = keys
		removed := models.MessageAggregate{
			Message:          *deleted,
			MentionedUserIDs: aggregate.MentionedUserIDs,
			Reactions:        aggregate.Reactions,
		}
		return s.appendOutbox(txCtx, removed, models.ChangeDeleted)
	})
	if err != nil {
		return err
	}
	s.deps.deleteObjects(ctx, objectKeys)
	s.deps.Metrics.IncMessagesDeleted(string(models.DeletionReasonAuthor))
	return nil
}

// loadAggregate loads a message with its related rows, mapping a missing row to
// the domain's not-found error.
func (s *MessageService) loadAggregate(ctx context.Context, id uuid.UUID) (*models.MessageAggregate, error) {
	aggregate, err := s.deps.Messages.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if aggregate == nil {
		return nil, models.ErrNotFound.WithDetail("The message does not exist.")
	}
	return aggregate, nil
}

// appendOutbox writes the EVT-009 record in the caller's transaction, so the
// state change and its event commit atomically (ADR pinned decision).
func (s *MessageService) appendOutbox(ctx context.Context, aggregate models.MessageAggregate, change models.MessageChange) error {
	payload, err := s.deps.messagePayload(ctx, aggregate, change)
	if err != nil {
		return err
	}
	if validateErr := payload.Validate(); validateErr != nil {
		return fmt.Errorf("service: building %s event: %w", change, validateErr)
	}
	now := s.deps.now()
	trace := traceFrom(ctx)
	envelope, err := models.NewEnvelope(models.EnvelopeInput{
		EventType:     models.EventMessageChanged,
		EventVersion:  1,
		OccurredAt:    now,
		TraceID:       trace.TraceID,
		CorrelationID: trace.CorrelationID,
	}, now)
	if err != nil {
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("service: encoding message event: %w", err)
	}
	return s.deps.Outbox.Append(ctx, models.OutboxEntryView{
		Topic:      models.TopicMessageEvents,
		MessageKey: aggregate.Message.ContainerID.String(),
		Envelope:   envelope,
		Payload:    body,
	})
}

// deleteObjects removes stored objects after the transaction committed. A
// failure is logged and retried by the GC sweep rather than failing the request.
func (d Deps) deleteObjects(ctx context.Context, keys []string) {
	for _, key := range keys {
		if err := d.Objects.Delete(ctx, key); err != nil {
			d.logger().Warn("attachment object deletion failed",
				zap.String("object_key", key), zap.Error(err))
		}
	}
}
