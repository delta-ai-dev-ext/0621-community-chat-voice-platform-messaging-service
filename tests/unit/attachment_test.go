package unit

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"messaging_service/internal/models"
	"messaging_service/internal/objectstore"
	"messaging_service/internal/service"
)

func pendingAttachment(key string) models.Attachment {
	return models.Attachment{
		ID: testMessageID, UploaderUserID: testAuthorID, Filename: "cat.png",
		DeclaredContentType: "image/png", DeclaredSizeBytes: 2048, ObjectKey: key,
		Status: models.AttachmentPendingUpload, CreatedAt: testNow,
	}
}

func TestReserveAttachment(t *testing.T) {
	t.Parallel()

	t.Run("returns a presigned upload URL", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		stored := pendingAttachment("attachments/key.png")
		h.attachments.On("Create", matchContext(), mock.Anything).Return(&stored, true, nil).Once()
		h.objects.On("PresignPut", matchContext(), stored.ObjectKey, "image/png", h.deps.Options.UploadURLTTL).
			Return(objectstore.SignedURL{URL: "https://storage.local/put", ExpiresAt: h.now.Add(15 * time.Minute)}, nil).Once()

		created, err := h.attachmentService().Reserve(contextBackground(), service.ReserveCommand{
			UploaderUserID: testAuthorID, Filename: "cat.png", ContentType: "image/png", SizeBytes: 2048,
			IdempotencyKey: "idem-1",
		})
		require.NoError(t, err)
		require.NotNil(t, created)
		assert.Equal(t, "https://storage.local/key.png", "https://storage.local/key.png")
		assert.Equal(t, stored.ID.String(), created.AttachmentID)
		assert.Equal(t, "https://storage.local/put", created.UploadURL)
		assert.Equal(t, "image/png", created.ContentType)
		assert.Equal(t, h.now.Add(15*time.Minute), created.UploadURLExpiresAt)
	})

	t.Run("replaying the idempotency key returns the original reservation", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		stored := pendingAttachment("attachments/key.png")
		key := "idem-1"
		stored.IdempotencyKey = &key
		// The repository reports created=false, mirroring the partial unique
		// index conflict that resolves to the original row.
		h.attachments.On("Create", matchContext(), mock.Anything).Return(&stored, false, nil).Once()
		h.objects.On("PresignPut", matchContext(), stored.ObjectKey, "image/png", h.deps.Options.UploadURLTTL).
			Return(objectstore.SignedURL{URL: "https://storage.local/put", ExpiresAt: h.now.Add(15 * time.Minute)}, nil).Once()

		created, err := h.attachmentService().Reserve(contextBackground(), service.ReserveCommand{
			UploaderUserID: testAuthorID, Filename: "cat.png", ContentType: "image/png", SizeBytes: 2048,
			IdempotencyKey: key,
		})
		require.NoError(t, err)
		// The original id is returned, not the freshly generated one.
		assert.Equal(t, stored.ID.String(), created.AttachmentID)
	})

	t.Run("rejects oversized uploads", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		_, err := h.attachmentService().Reserve(contextBackground(), service.ReserveCommand{
			UploaderUserID: testAuthorID, Filename: "big.png", ContentType: "image/png",
			SizeBytes: h.deps.Options.MaxAttachmentBytes + 1,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrImageTooLarge))
	})

	t.Run("rejects unsupported types", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		_, err := h.attachmentService().Reserve(contextBackground(), service.ReserveCommand{
			UploaderUserID: testAuthorID, Filename: "doc.pdf", ContentType: "application/pdf", SizeBytes: 10,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrUnsupportedImageType))
	})

	t.Run("rejects empty and over-long metadata", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		_, err := h.attachmentService().Reserve(contextBackground(), service.ReserveCommand{
			UploaderUserID: testAuthorID, Filename: "cat.png", ContentType: "image/png", SizeBytes: 0,
		})
		require.Error(t, err)

		_, err = h.attachmentService().Reserve(contextBackground(), service.ReserveCommand{
			UploaderUserID: testAuthorID, Filename: "   ", ContentType: "image/png", SizeBytes: 10,
		})
		require.Error(t, err)

		_, err = h.attachmentService().Reserve(contextBackground(), service.ReserveCommand{
			UploaderUserID: testAuthorID, Filename: "cat.png", ContentType: "image/png", SizeBytes: 10,
			IdempotencyKey: string(make([]byte, 200)),
		})
		require.Error(t, err)
	})

	t.Run("storage failure fails closed", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		stored := pendingAttachment("attachments/key.png")
		h.attachments.On("Create", matchContext(), mock.Anything).Return(&stored, true, nil).Once()
		h.objects.On("PresignPut", matchContext(), stored.ObjectKey, "image/png", h.deps.Options.UploadURLTTL).
			Return(objectstore.SignedURL{}, objectstore.ErrUnavailable).Once()

		_, err := h.attachmentService().Reserve(contextBackground(), service.ReserveCommand{
			UploaderUserID: testAuthorID, Filename: "cat.png", ContentType: "image/png", SizeBytes: 2048,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrStorageUnavailable))
	})
}

func TestSweepPendingUploads(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	serviceUnderTest := h.attachmentService()

	collected := pendingAttachment("attachments/collected.png")
	failing := pendingAttachment("attachments/failing.png")
	failing.ID = uuid.New()
	h.attachments.On("ListPendingOlderThan", matchContext(), h.now.Add(-24*time.Hour), 100).
		Return([]models.Attachment{collected, failing}, nil).Once()
	h.objects.On("Delete", matchContext(), collected.ObjectKey).Return(nil).Once()
	h.objects.On("Delete", matchContext(), failing.ObjectKey).Return(objectstore.ErrUnavailable).Once()
	h.attachments.On("Delete", matchContext(), collected.ID).Return(nil).Once()

	swept, err := serviceUnderTest.SweepPendingUploads(contextBackground(), 100)
	require.NoError(t, err)
	// The failing object is retried on the next sweep, so its row is kept.
	assert.Equal(t, 1, swept)
}

func TestSweptAttachmentCannotBeBound(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
	h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(nil, nil).Once()
	// The GC sweep removed the row, so the send references nothing bindable.
	h.attachments.On("Get", matchContext(), testMessageID).Return(nil, nil).Once()

	_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
		ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
		ClientMessageID: "c1", Content: "with image", AttachmentIDs: []uuid.UUID{testMessageID},
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, models.ErrAttachmentInvalid))
}

func TestSendVerifiesAttachments(t *testing.T) {
	t.Parallel()
	attachmentID := testMessageID

	cases := []struct {
		name       string
		attachment *models.Attachment
		head       *objectstore.ObjectInfo
		headErr    error
		wantErr    error
		wantDetail string
	}{
		{
			name:       "unsupported stored type",
			attachment: pointerAttachment("image/png"),
			head:       &objectstore.ObjectInfo{SizeBytes: 10, ContentType: "application/pdf"},
			wantErr:    models.ErrUnsupportedImageType,
		},
		{
			name:       "declared and stored type mismatch",
			attachment: pointerAttachment("image/png"),
			head:       &objectstore.ObjectInfo{SizeBytes: 10, ContentType: "image/gif"},
			wantErr:    models.ErrUnsupportedImageType,
		},
		{
			name:       "stored object exceeds the limit",
			attachment: pointerAttachment("image/png"),
			head:       &objectstore.ObjectInfo{SizeBytes: 10485761, ContentType: "image/png"},
			wantErr:    models.ErrImageTooLarge,
		},
		{
			name:       "object missing",
			attachment: pointerAttachment("image/png"),
			headErr:    objectstore.ErrObjectNotFound,
			wantErr:    models.ErrAttachmentInvalid,
		},
		{
			name:       "storage unavailable",
			attachment: pointerAttachment("image/png"),
			headErr:    objectstore.ErrUnavailable,
			wantErr:    models.ErrStorageUnavailable,
		},
		{
			name:       "empty object",
			attachment: pointerAttachment("image/png"),
			head:       &objectstore.ObjectInfo{SizeBytes: 0, ContentType: "image/png"},
			wantErr:    models.ErrAttachmentInvalid,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
			h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(nil, nil).Once()
			h.attachments.On("Get", matchContext(), attachmentID).Return(tc.attachment, nil).Once()
			h.objects.On("Head", matchContext(), tc.attachment.ObjectKey).Return(tc.head, tc.headErr).Once()

			_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
				ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
				ClientMessageID: "c1", Content: "with image", AttachmentIDs: []uuid.UUID{attachmentID},
			})
			require.Error(t, err)
			assert.True(t, errors.Is(err, tc.wantErr), "got %v want %v", err, tc.wantErr)
		})
	}
}

func TestSendBindsVerifiedAttachments(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
	h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(nil, nil).Once()

	attachment := pendingAttachment("attachments/key.png")
	h.attachments.On("Get", matchContext(), attachment.ID).Return(&attachment, nil).Once()
	h.objects.On("Head", matchContext(), attachment.ObjectKey).
		Return(&objectstore.ObjectInfo{SizeBytes: 2048, ContentType: "image/png"}, nil).Once()
	h.tx.On("InTx", matchContext()).Return(nil).Once()
	stored := storedMessage(testContainer, "look at this")
	h.messages.On("Insert", matchContext(), mock.Anything).Return(&stored, true, nil).Once()
	h.messages.On("BindAttachments", matchContext(), stored.ID, testAuthorID, h.now, mock.Anything).Return(nil).Once()
	h.messages.On("ReplaceMentions", matchContext(), stored.ID, mock.Anything).Return(nil).Once()
	h.objects.On("PresignGet", matchContext(), attachment.ObjectKey, h.deps.Options.DownloadURLTTL).
		Return(objectstore.SignedURL{URL: "https://storage.local/get", ExpiresAt: h.now.Add(15 * time.Minute)}, nil).
		Times(2)
	h.expectOutbox(1)

	view, err := h.messagesService().Send(contextBackground(), service.SendCommand{
		ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
		ClientMessageID: "c1", Content: "look at this", AttachmentIDs: []uuid.UUID{attachment.ID},
	})
	require.NoError(t, err)
	require.NotNil(t, view)
	require.Len(t, view.Attachments, 1)
	assert.Equal(t, "https://storage.local/get", view.Attachments[0].DownloadURL)
	assert.Equal(t, "image/png", view.Attachments[0].ContentType)
	assert.Equal(t, int64(2048), view.Attachments[0].SizeBytes)
}

// pointerAttachment builds a pending attachment with the given declared type.
func pointerAttachment(contentType string) *models.Attachment {
	attachment := pendingAttachment("attachments/key.png")
	attachment.DeclaredContentType = contentType
	return &attachment
}
