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

func TestHistoryPaging(t *testing.T) {
	t.Parallel()
	cursor := models.NewCursor(testNow.Add(-time.Minute), testMessageID)

	t.Run("returns a cursor and has_more when a page is full", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
		older := storedMessage(testContainer, "older")
		older.ID = uuid.New()
		older.CreatedAt = testNow.Add(-time.Minute)
		h.messages.On("Page", matchContext(), mock.MatchedBy(func(query models.HistoryQuery) bool {
			return query.Limit == 3 && query.Before == nil && query.After == nil
		})).Return([]models.MessageAggregate{
			{Message: storedMessage(testContainer, "newest")},
			{Message: storedMessage(testContainer, "middle")},
			{Message: older},
		}, nil).Once()

		page, err := h.messagesService().History(contextBackground(), service.HistoryCommand{
			ContainerType: models.ContainerChannel, ContainerID: testContainer, UserID: testAuthorID, Limit: 2,
		})
		require.NoError(t, err)
		require.NotNil(t, page)
		assert.True(t, page.HasMore)
		require.NotNil(t, page.NextCursor)
		decoded, err := models.DecodeCursor(*page.NextCursor)
		require.NoError(t, err)
		// The cursor points at the last message actually returned.
		assert.Equal(t, testMessageID, decoded.ID)
		assert.Equal(t, testNow, decoded.CreatedAt)
		assert.Len(t, page.Data, 2)
	})

	t.Run("exhausted page has no cursor", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
		h.messages.On("Page", matchContext(), mock.Anything).
			Return([]models.MessageAggregate{{Message: storedMessage(testContainer, "only")}}, nil).Once()

		page, err := h.messagesService().History(contextBackground(), service.HistoryCommand{
			ContainerType: models.ContainerChannel, ContainerID: testContainer, UserID: testAuthorID, Limit: 50,
		})
		require.NoError(t, err)
		assert.False(t, page.HasMore)
		assert.Nil(t, page.NextCursor)
	})

	t.Run("passes the before cursor through", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
		h.messages.On("Page", matchContext(), mock.MatchedBy(func(query models.HistoryQuery) bool {
			return query.Before != nil && query.Before.ID == testMessageID
		})).Return([]models.MessageAggregate{}, nil).Once()

		_, err := h.messagesService().History(contextBackground(), service.HistoryCommand{
			ContainerType: models.ContainerChannel, ContainerID: testContainer, UserID: testAuthorID,
			Limit: 10, Before: &cursor,
		})
		require.NoError(t, err)
	})

	t.Run("rejects both cursors at once", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		_, err := h.messagesService().History(contextBackground(), service.HistoryCommand{
			ContainerType: models.ContainerChannel, ContainerID: testContainer, UserID: testAuthorID,
			Before: &cursor, After: &cursor,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrInvalidRequest))
	})

	t.Run("rejects unknown container types", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		_, err := h.messagesService().History(contextBackground(), service.HistoryCommand{
			ContainerType: models.ContainerType("thread"), ContainerID: testContainer, UserID: testAuthorID,
		})
		require.Error(t, err)
	})

	t.Run("muted members may still read", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		mutedUntil := h.now.Add(time.Hour)
		member := activeMembership(testCommunity, testAuthorID)
		member.MutedUntil = &mutedUntil
		h.expectChannelAccess(testContainer, testCommunity, testAuthorID, member)
		h.messages.On("Page", matchContext(), mock.Anything).Return(nil, nil).Once()

		_, err := h.messagesService().History(contextBackground(), service.HistoryCommand{
			ContainerType: models.ContainerChannel, ContainerID: testContainer, UserID: testAuthorID,
		})
		require.NoError(t, err)
	})
}

func TestThreadPaging(t *testing.T) {
	t.Parallel()

	t.Run("returns root, reply count and replies", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		root := storedMessage(testContainer, "root")
		root.CommunityID = nil
		root.CommunityID = &testCommunity
		h.messages.On("Get", matchContext(), testMessageID).Return(storedAggregate(root), nil).Once()
		h.community.On("GetMembership", matchContext(), testCommunity, testAuthorID).
			Return(activeMembership(testCommunity, testAuthorID), nil).Once()
		h.messages.On("ReplyCount", matchContext(), testMessageID).Return(2, nil).Once()

		reply := storedMessage(testContainer, "reply")
		reply.ID = uuid.New()
		reply.ThreadRootID = &testMessageID
		h.messages.On("Replies", matchContext(), testMessageID, (*models.Cursor)(nil), 2).
			Return([]models.MessageAggregate{{Message: reply}}, nil).Once()

		page, err := h.messagesService().Thread(contextBackground(), service.ThreadCommand{
			MessageID: testMessageID, UserID: testAuthorID, Limit: 1,
		})
		require.NoError(t, err)
		require.NotNil(t, page)
		assert.Equal(t, 2, page.ReplyCount)
		assert.Equal(t, testMessageID.String(), page.Root.MessageID)
		require.Len(t, page.Replies, 1)
		assert.False(t, page.HasMore)
	})

	t.Run("a reply resolves to its top-level root", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		replyID := uuid.New()
		reply := storedMessage(testContainer, "reply")
		reply.ID = replyID
		reply.ThreadRootID = &testMessageID
		root := storedMessage(testContainer, "root")
		root.ID = testMessageID
		h.messages.On("Get", matchContext(), replyID).Return(storedAggregate(reply), nil).Once()
		h.community.On("GetMembership", matchContext(), testCommunity, testAuthorID).
			Return(activeMembership(testCommunity, testAuthorID), nil).Once()
		h.messages.On("Get", matchContext(), testMessageID).Return(storedAggregate(root), nil).Once()
		h.messages.On("ReplyCount", matchContext(), testMessageID).Return(1, nil).Once()
		h.messages.On("Replies", matchContext(), testMessageID, (*models.Cursor)(nil), 51).
			Return([]models.MessageAggregate{}, nil).Once()

		page, err := h.messagesService().Thread(contextBackground(), service.ThreadCommand{
			MessageID: reply.ID, UserID: testAuthorID,
		})
		require.NoError(t, err)
		assert.Equal(t, testMessageID.String(), page.Root.MessageID)
	})

	t.Run("missing message is a 404", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.messages.On("Get", matchContext(), testMessageID).Return(nil, nil).Once()
		_, err := h.messagesService().Thread(contextBackground(), service.ThreadCommand{
			MessageID: testMessageID, UserID: testAuthorID,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrNotFound))
	})
}

func TestEditMessage(t *testing.T) {
	t.Parallel()

	t.Run("author edits and an edited event is emitted", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		stored := storedMessage(testContainer, "before")
		h.messages.On("Get", matchContext(), stored.ID).Return(storedAggregate(stored), nil).Once()
		h.community.On("GetMembership", matchContext(), testCommunity, testAuthorID).
			Return(activeMembership(testCommunity, testAuthorID), nil).Twice()
		h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(nil, nil).Once()
		h.tx.On("InTx", matchContext()).Return(nil).Once()
		edited := stored
		edited.Content = pointer("after")
		edited.MessageVersion = 2
		edited.EditedAt = &h.now
		h.messages.On("UpdateContent", matchContext(), stored.ID, "edited", h.now).Return(&edited, nil).Once()
		h.messages.On("ReplaceMentions", matchContext(), stored.ID, mock.Anything).Return(nil).Once()
		recorder := h.expectOutbox(1)

		view, err := h.messagesService().Edit(contextBackground(), service.EditCommand{
			MessageID: testMessageID, UserID: testAuthorID, Content: "edited",
		})
		require.NoError(t, err)
		require.NotNil(t, view)
		payloads := recorder.payloads(t)
		require.Len(t, payloads, 1)
		assert.Equal(t, string(models.ChangeEdited), payloads[0].Change)
	})

	t.Run("non-author is rejected", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		other := uuid.New()
		message := storedMessage(testContainer, "text")
		h.messages.On("Get", matchContext(), testMessageID).Return(storedAggregate(message), nil).Once()
		h.community.On("GetMembership", matchContext(), testCommunity, other).
			Return(activeMembership(testCommunity, other), nil).Once()

		_, err := h.messagesService().Edit(contextBackground(), service.EditCommand{
			MessageID: testMessageID, UserID: other, Content: "hacked",
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrNotAuthor))
	})

	t.Run("editing a tombstone is rejected", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		deleted := storedMessage(testContainer, "gone")
		reason := models.DeletionReasonAuthor
		deleted.DeletionReason = &reason
		deleted.DeletedAt = &h.now
		h.messages.On("Get", matchContext(), testMessageID).Return(storedAggregate(deleted), nil).Once()
		h.community.On("GetMembership", matchContext(), testCommunity, testAuthorID).
			Return(activeMembership(testCommunity, testAuthorID), nil).Once()

		_, err := h.messagesService().Edit(contextBackground(), service.EditCommand{
			MessageID: testMessageID, UserID: testAuthorID, Content: "again",
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrMessageDeleted))
	})
}

func TestDeleteMessage(t *testing.T) {
	t.Parallel()

	t.Run("tombstones and detaches attachments", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		stored := storedMessage(testContainer, "bye")
		aggregate := storedAggregate(stored)
		aggregate.Attachments = []models.Attachment{pendingAttachment("attachments/key.png")}
		h.messages.On("Get", matchContext(), testMessageID).Return(aggregate, nil).Once()
		h.community.On("GetMembership", matchContext(), testCommunity, testAuthorID).
			Return(activeMembership(testCommunity, testAuthorID), nil).Once()

		deleted := stored
		deleted.Content = nil
		reason := models.DeletionReasonAuthor
		deleted.DeletionReason = &reason
		deleted.DeletedAt = &h.now
		deleted.MessageVersion = 2
		h.tx.On("InTx", matchContext()).Return(nil).Once()
		h.messages.On("SoftDelete", matchContext(), testMessageID, models.DeletionReasonAuthor, h.now).
			Return(&deleted, nil).Once()
		h.messages.On("DetachAttachments", matchContext(), testMessageID).
			Return([]string{"attachments/key.png"}, nil).Once()
		h.objects.On("Delete", matchContext(), "attachments/key.png").Return(nil).Once()
		recorder := h.expectOutbox(1)

		require.NoError(t, h.messagesService().Delete(contextBackground(), service.DeleteCommand{
			MessageID: testMessageID, UserID: testAuthorID,
		}))
		payloads := recorder.payloads(t)
		require.Len(t, payloads, 1)
		assert.Equal(t, string(models.ChangeDeleted), payloads[0].Change)
		assert.Empty(t, payloads[0].Attachments)
	})

	t.Run("deleting twice is a no-op", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		deleted := storedMessage(testContainer, "gone")
		reason := models.DeletionReasonAuthor
		deleted.DeletionReason = &reason
		deleted.DeletedAt = &h.now
		h.messages.On("Get", matchContext(), testMessageID).Return(storedAggregate(deleted), nil).Once()
		h.community.On("GetMembership", matchContext(), testCommunity, testAuthorID).
			Return(activeMembership(testCommunity, testAuthorID), nil).Once()

		require.NoError(t, h.messagesService().Delete(contextBackground(), service.DeleteCommand{
			MessageID: testMessageID, UserID: testAuthorID,
		}))
		// No InTx or outbox expectation: nothing else may happen.
	})

	t.Run("object deletion failure does not fail the request", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		stored := storedMessage(testContainer, "bye")
		h.messages.On("Get", matchContext(), testMessageID).Return(storedAggregate(stored), nil).Once()
		h.community.On("GetMembership", matchContext(), testCommunity, testAuthorID).
			Return(activeMembership(testCommunity, testAuthorID), nil).Once()
		deleted := stored
		reason := models.DeletionReasonAuthor
		deleted.DeletionReason = &reason
		deleted.DeletedAt = &h.now
		h.tx.On("InTx", matchContext()).Return(nil).Once()
		h.messages.On("SoftDelete", matchContext(), testMessageID, models.DeletionReasonAuthor, h.now).
			Return(&deleted, nil).Once()
		h.messages.On("DetachAttachments", matchContext(), testMessageID).
			Return([]string{"attachments/key.png"}, nil).Once()
		h.objects.On("Delete", matchContext(), "attachments/key.png").Return(objectstore.ErrUnavailable).Once()
		h.expectOutbox(1)

		require.NoError(t, h.messagesService().Delete(contextBackground(), service.DeleteCommand{
			MessageID: testMessageID, UserID: testAuthorID,
		}))
	})
}

func TestReactions(t *testing.T) {
	t.Parallel()

	t.Run("adding a reaction bumps the version and emits an event", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		stored := storedMessage(testContainer, "nice")
		h.messages.On("Get", matchContext(), testMessageID).Return(storedAggregate(stored), nil).Once()
		h.community.On("GetMembership", matchContext(), testCommunity, testAuthorID).
			Return(activeMembership(testCommunity, testAuthorID), nil).Once()
		h.tx.On("InTx", matchContext()).Return(nil).Once()
		h.messages.On("AddReaction", matchContext(), testMessageID, testAuthorID, "🎉", h.now).Return(true, nil).Once()
		h.messages.On("BumpVersion", matchContext(), testMessageID).Return(int64(2), nil).Once()
		h.messages.On("ReactionSummary", matchContext(), testMessageID).
			Return([]models.ReactionSummary{{Emoji: "🎉", Count: 1}}, nil).Once()
		recorder := h.expectOutbox(1)

		response, err := h.messagesService().React(contextBackground(), service.ReactionCommand{
			MessageID: testMessageID, UserID: testAuthorID, Emoji: "🎉", Add: true,
		})
		require.NoError(t, err)
		require.NotNil(t, response)
		assert.Equal(t, []models.ReactionSummary{{Emoji: "🎉", Count: 1}}, response.Reactions)
		payloads := recorder.payloads(t)
		require.Len(t, payloads, 1)
		assert.Equal(t, string(models.ChangeReactionsChanged), payloads[0].Change)
		assert.Equal(t, int64(2), payloads[0].MessageVersion)
	})

	t.Run("a repeated reaction emits nothing", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		stored := storedMessage(testContainer, "nice")
		aggregate := storedAggregate(stored)
		aggregate.Reactions = []models.ReactionSummary{{Emoji: "🎉", Count: 1}}
		h.messages.On("Get", matchContext(), testMessageID).Return(aggregate, nil).Once()
		h.community.On("GetMembership", matchContext(), testCommunity, testAuthorID).
			Return(activeMembership(testCommunity, testAuthorID), nil).Once()
		h.tx.On("InTx", matchContext()).Return(nil).Once()
		h.messages.On("AddReaction", matchContext(), testMessageID, testAuthorID, "🎉", h.now).Return(false, nil).Once()

		response, err := h.messagesService().React(contextBackground(), service.ReactionCommand{
			MessageID: testMessageID, UserID: testAuthorID, Emoji: "🎉", Add: true,
		})
		require.NoError(t, err)
		assert.Equal(t, []models.ReactionSummary{{Emoji: "🎉", Count: 1}}, response.Reactions)
	})

	t.Run("removing a reaction is idempotent", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		stored := storedMessage(testContainer, "nice")
		h.messages.On("Get", matchContext(), testMessageID).Return(storedAggregate(stored), nil).Once()
		h.community.On("GetMembership", matchContext(), testCommunity, testAuthorID).
			Return(activeMembership(testCommunity, testAuthorID), nil).Once()
		h.tx.On("InTx", matchContext()).Return(nil).Once()
		h.messages.On("RemoveReaction", matchContext(), testMessageID, testAuthorID, "🎉").Return(true, nil).Once()
		h.messages.On("BumpVersion", matchContext(), testMessageID).Return(int64(2), nil).Once()
		h.messages.On("ReactionSummary", matchContext(), testMessageID).
			Return([]models.ReactionSummary{}, nil).Once()
		h.expectOutbox(1)

		response, err := h.messagesService().React(contextBackground(), service.ReactionCommand{
			MessageID: testMessageID, UserID: testAuthorID, Emoji: "🎉",
		})
		require.NoError(t, err)
		assert.Empty(t, response.Reactions)
	})

	t.Run("reacting to a tombstone is a 404", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		deleted := storedMessage(testContainer, "gone")
		reason := models.DeletionReasonAuthor
		deleted.DeletionReason = &reason
		deleted.DeletedAt = &h.now
		h.messages.On("Get", matchContext(), testMessageID).Return(storedAggregate(deleted), nil).Once()
		h.community.On("GetMembership", matchContext(), testCommunity, testAuthorID).
			Return(activeMembership(testCommunity, testAuthorID), nil).Once()

		_, err := h.messagesService().React(contextBackground(), service.ReactionCommand{
			MessageID: testMessageID, UserID: testAuthorID, Emoji: "🎉", Add: true,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrNotFound))
	})
}

func TestMentionResolution(t *testing.T) {
	t.Parallel()

	t.Run("parses, deduplicates and preserves order", func(t *testing.T) {
		t.Parallel()
		first := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
		second := uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
		content := "hi <@" + first.String() + "> and <@" + second.String() + "> and <@" + first.String() + ">"

		mentions := service.ParseMentions(content)
		assert.Equal(t, []uuid.UUID{first, second}, mentions)
	})

	t.Run("ignores malformed tokens", func(t *testing.T) {
		t.Parallel()
		assert.Empty(t, service.ParseMentions("hi <@not-a-uuid> and @someone"))
	})

	t.Run("unknown community members are dropped", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		known := uuid.MustParse("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
		stranger := uuid.MustParse("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
		h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
		h.community.On("FilterActiveMemberIDs", matchContext(), testCommunity, mock.Anything).
			Return([]uuid.UUID{known}, nil).Once()
		h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(nil, nil).Once()
		h.tx.On("InTx", matchContext()).Return(nil).Once()
		stored := storedMessage(testContainer, "hello")
		h.messages.On("Insert", matchContext(), mock.Anything).Return(&stored, true, nil).Once()
		h.messages.On("ReplaceMentions", matchContext(), stored.ID, []uuid.UUID{known}).Return(nil).Once()
		h.expectOutbox(1)

		_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
			ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
			ClientMessageID: "c1",
			Content:         "hi <@" + known.String() + "> and <@" + stranger.String() + ">",
		})
		require.NoError(t, err)
	})
}
