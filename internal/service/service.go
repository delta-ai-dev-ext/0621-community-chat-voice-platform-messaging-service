// Package service holds the business logic: message lifecycle, automod
// enforcement, attachment two-phase flow, mention resolution and the realtime
// scope oracle. Services never import gin; they return domain errors that the
// handler edge maps onto HTTP statuses.
package service

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"messaging_service/internal/models"
	"messaging_service/internal/objectstore"
	"messaging_service/internal/observability"
	"messaging_service/internal/repository"
)

// Options are the tunable business parameters handed to the services.
type Options struct {
	// MaxAttachmentBytes is the upload ceiling (default 10 MB).
	MaxAttachmentBytes int64
	// UploadURLTTL is the presigned PUT lifetime.
	UploadURLTTL time.Duration
	// DownloadURLTTL is the presigned GET lifetime.
	DownloadURLTTL time.Duration
	// PendingUploadGCAfter bounds the age of unused `pending_upload` rows.
	PendingUploadGCAfter time.Duration
	// AppealURLBase is the appeal pointer included in mute rejections.
	AppealURLBase string
	// PageLimitDefault and PageLimitMax bound history page sizes.
	PageLimitDefault int
	PageLimitMax     int
}

// Deps is the dependency set every service is constructed from. Everything is an
// interface so the whole layering is testable without infrastructure.
type Deps struct {
	Messages      repository.MessageRepository
	Community     repository.CommunityProjectionRepository
	Conversations repository.ConversationProjectionRepository
	Attachments   repository.AttachmentRepository
	Rates         repository.RateRepository
	Outbox        repository.OutboxRepository
	Tx            repository.TransactionManager
	Objects       objectstore.ObjectStore
	Clock         func() time.Time
	Logger        *zap.Logger
	Metrics       *observability.Metrics
	Options       Options
}

// now returns the injected clock instant in UTC. Every decision that depends on
// time (mute expiry, GC cutoffs, cursors, event timestamps) goes through this.
func (d Deps) now() time.Time {
	if d.Clock == nil {
		return time.Now().UTC()
	}
	return d.Clock().UTC()
}

// logger returns a usable logger even when none was injected (tests).
func (d Deps) logger() *zap.Logger {
	if d.Logger == nil {
		return zap.NewNop()
	}
	return d.Logger
}

// Trace is the per-request trace pair carried into produced events.
type Trace struct {
	TraceID       string
	CorrelationID uuid.UUID
}

// traceFrom extracts the trace stored in the request context.
func traceFrom(ctx context.Context) Trace {
	trace := observability.TraceFromContext(ctx)
	return Trace{TraceID: trace.TraceID, CorrelationID: trace.CorrelationID}
}

// pageLimit normalises a requested page size to the configured bounds.
func (d Deps) pageLimit(requested int) int {
	if requested <= 0 {
		return d.Options.PageLimitDefault
	}
	if d.Options.PageLimitMax > 0 && requested > d.Options.PageLimitMax {
		return d.Options.PageLimitMax
	}
	return requested
}

// attachmentView presigns one attachment for a response or an event snapshot.
func (d Deps) attachmentView(ctx context.Context, attachment models.Attachment) (models.AttachmentView, error) {
	signed, err := d.Objects.PresignGet(ctx, attachment.ObjectKey, d.Options.DownloadURLTTL)
	if err != nil {
		return models.AttachmentView{}, err
	}
	contentType := attachment.DeclaredContentType
	if attachment.ActualContentType != nil {
		contentType = *attachment.ActualContentType
	}
	size := attachment.DeclaredSizeBytes
	if attachment.ActualSizeBytes != nil {
		size = *attachment.ActualSizeBytes
	}
	return models.AttachmentView{
		AttachmentID: attachment.ID.String(),
		ContentType:  contentType,
		SizeBytes:    size,
		DownloadURL:  signed.URL,
		URLExpiresAt: signed.ExpiresAt,
	}, nil
}

// messageView renders the public representation of a stored message. Deleted
// messages are rendered as tombstones: no content, no attachments, but the same
// position and version so history stays stable.
func (d Deps) messageView(ctx context.Context, aggregate models.MessageAggregate) (models.MessageView, error) {
	message := aggregate.Message
	view := models.MessageView{
		MessageID:        message.ID.String(),
		ContainerType:    string(message.ContainerType),
		ContainerID:      message.ContainerID.String(),
		AuthorUserID:     message.AuthorUserID.String(),
		Content:          message.Content,
		Attachments:      make([]models.AttachmentView, 0, len(aggregate.Attachments)),
		MentionedUserIDs: make([]string, 0, len(aggregate.MentionedUserIDs)),
		Reactions:        aggregate.Reactions,
		CreatedAt:        message.CreatedAt.UTC(),
		EditedAt:         message.EditedAt,
		DeletedAt:        message.DeletedAt,
		MessageVersion:   message.MessageVersion,
	}
	if view.Reactions == nil {
		view.Reactions = []models.ReactionSummary{}
	}
	if message.CommunityID != nil {
		communityID := message.CommunityID.String()
		view.CommunityID = &communityID
	}
	if message.ThreadRootID != nil {
		threadRootID := message.ThreadRootID.String()
		view.ThreadRootID = &threadRootID
	}
	if message.DeletionReason != nil {
		reason := string(*message.DeletionReason)
		view.DeletionReason = &reason
	}
	if message.IsDeleted() {
		// A tombstone exposes neither content nor attachment URLs.
		view.Attachments = []models.AttachmentView{}
	}
	for _, mentioned := range aggregate.MentionedUserIDs {
		view.MentionedUserIDs = append(view.MentionedUserIDs, mentioned.String())
	}
	if !message.IsDeleted() {
		for _, attachment := range aggregate.Attachments {
			rendered, err := d.attachmentView(ctx, attachment)
			if err != nil {
				return models.MessageView{}, err
			}
			view.Attachments = append(view.Attachments, rendered)
		}
	}
	return view, nil
}

// messageViews renders a page of messages.
func (d Deps) messageViews(ctx context.Context, aggregates []models.MessageAggregate) ([]models.MessageView, error) {
	views := make([]models.MessageView, 0, len(aggregates))
	for _, aggregate := range aggregates {
		view, err := d.messageView(ctx, aggregate)
		if err != nil {
			return nil, err
		}
		views = append(views, view)
	}
	return views, nil
}

// messagePayload renders the EVT-009 snapshot for one message. Deleted
// messages carry no content and no attachments, per the event contract.
func (d Deps) messagePayload(ctx context.Context, aggregate models.MessageAggregate, change models.MessageChange) (models.MessageChangedPayload, error) {
	message := aggregate.Message
	payload := models.MessageChangedPayload{
		MessageID:        message.ID.String(),
		Change:           string(change),
		ContainerType:    string(message.ContainerType),
		ContainerID:      message.ContainerID.String(),
		AuthorUserID:     message.AuthorUserID.String(),
		Content:          message.Content,
		Attachments:      make([]models.AttachmentSnapshot, 0, len(aggregate.Attachments)),
		MentionedUserIDs: make([]string, 0, len(aggregate.MentionedUserIDs)),
		Reactions:        aggregate.Reactions,
		CreatedAt:        message.CreatedAt.UTC(),
		EditedAt:         message.EditedAt,
		MessageVersion:   message.MessageVersion,
	}
	if payload.Reactions == nil {
		payload.Reactions = []models.ReactionSummary{}
	}
	if message.CommunityID != nil {
		communityID := message.CommunityID.String()
		payload.CommunityID = &communityID
	}
	if message.ThreadRootID != nil {
		threadRootID := message.ThreadRootID.String()
		payload.ThreadRootID = &threadRootID
	}
	if message.DeletionReason != nil {
		reason := string(*message.DeletionReason)
		payload.DeletionReason = &reason
	}
	for _, mentioned := range aggregate.MentionedUserIDs {
		payload.MentionedUserIDs = append(payload.MentionedUserIDs, mentioned.String())
	}
	if !message.IsDeleted() {
		for _, attachment := range aggregate.Attachments {
			rendered, err := d.attachmentView(ctx, attachment)
			if err != nil {
				return models.MessageChangedPayload{}, err
			}
			payload.Attachments = append(payload.Attachments, models.AttachmentSnapshot{
				AttachmentID: rendered.AttachmentID,
				ContentType:  rendered.ContentType,
				SizeBytes:    rendered.SizeBytes,
				DownloadURL:  rendered.DownloadURL,
				URLExpiresAt: rendered.URLExpiresAt,
			})
		}
	}
	return payload, nil
}
