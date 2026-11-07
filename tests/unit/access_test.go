package unit

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"messaging_service/internal/models"
	"messaging_service/internal/service"
)

func TestConversationReadAccess(t *testing.T) {
	t.Parallel()
	conversationID := uuid.MustParse("66666666-6666-4666-8666-666666666666")

	t.Run("participant reads history", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.conversations.On("GetConversation", matchContext(), conversationID).Return(&models.Conversation{
			ConversationID: conversationID, Kind: models.ConversationKindGroup, ConversationVersion: 1,
			ParticipantIDs: []uuid.UUID{testAuthorID},
		}, nil).Once()
		h.messages.On("Page", matchContext(), mock.Anything).Return([]models.MessageAggregate{}, nil).Once()

		page, err := h.messagesService().History(contextBackground(), service.HistoryCommand{
			ContainerType: models.ContainerConversation, ContainerID: conversationID, UserID: testAuthorID,
		})
		require.NoError(t, err)
		assert.Empty(t, page.Data)
	})

	t.Run("non-participant is rejected", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.conversations.On("GetConversation", matchContext(), conversationID).Return(&models.Conversation{
			ConversationID: conversationID, Kind: models.ConversationKindDirect, ConversationVersion: 1,
			ParticipantIDs: []uuid.UUID{uuid.New()},
		}, nil).Once()

		_, err := h.messagesService().History(contextBackground(), service.HistoryCommand{
			ContainerType: models.ContainerConversation, ContainerID: conversationID, UserID: testAuthorID,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrNotAParticipant))
	})

	t.Run("unknown conversation is a 404", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.conversations.On("GetConversation", matchContext(), conversationID).Return(nil, nil).Once()

		_, err := h.messagesService().History(contextBackground(), service.HistoryCommand{
			ContainerType: models.ContainerConversation, ContainerID: conversationID, UserID: testAuthorID,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrNotFound))
	})
}

func TestBannedMemberCannotRead(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	member := &models.CommunityMember{Status: models.MemberStatusActive, IsBanned: true, MemberVersion: 3}
	h.expectChannelAccess(testContainer, testCommunity, testAuthorID, member)

	_, err := h.messagesService().History(contextBackground(), service.HistoryCommand{
		ContainerType: models.ContainerChannel, ContainerID: testContainer, UserID: testAuthorID,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, models.ErrNotAMember))
}

func TestDeleteRequiresAuthorship(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	other := uuid.New()
	message := storedMessage(testContainer, "text")
	h.messages.On("Get", matchContext(), testMessageID).Return(storedAggregate(message), nil).Once()
	h.community.On("GetMembership", matchContext(), testCommunity, other).
		Return(activeMembership(testCommunity, other), nil).Once()

	err := h.messagesService().Delete(contextBackground(), service.DeleteCommand{
		MessageID: testMessageID, UserID: other,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, models.ErrNotAuthor))
}

func TestConversationSendRejectsMutedMember(t *testing.T) {
	t.Parallel()
	conversationID := uuid.MustParse("66666666-6666-4666-8666-666666666666")
	h := newHarness(t)
	// Direct conversations have no mute, so membership alone decides access.
	h.conversations.On("GetConversation", matchContext(), conversationID).Return(&models.Conversation{
		ConversationID: conversationID, Kind: models.ConversationKindGroup, ConversationVersion: 1,
		ParticipantIDs: []uuid.UUID{testAuthorID},
	}, nil).Once()
	h.tx.On("InTx", matchContext()).Return(nil).Once()
	stored := storedMessage(conversationID, "hi")
	stored.ContainerType = models.ContainerConversation
	stored.CommunityID = nil
	h.messages.On("Insert", matchContext(), mock.Anything).Return(&stored, true, nil).Once()
	h.messages.On("ReplaceMentions", matchContext(), stored.ID, mock.Anything).Return(nil).Once()
	h.expectOutbox(1)

	view, err := h.messagesService().Send(contextBackground(), service.SendCommand{
		ContainerType: models.ContainerConversation, ContainerID: conversationID, AuthorUserID: testAuthorID,
		ClientMessageID: "c1", Content: "hi",
	})
	require.NoError(t, err)
	require.NotNil(t, view)
}

func TestThreadReplyToReplyUsesTopLevelRoot(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
	h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(nil, nil).Once()

	// Replies never nest: a reply to a reply is attached to the top-level root.
	reply := storedMessage(testContainer, "root reply")
	reply.ID = uuid.MustParse("77777777-7777-4777-8777-777777777777")
	reply.ThreadRootID = &testMessageID
	h.messages.On("Get", matchContext(), reply.ID).Return(storedAggregate(reply), nil).Once()
	h.tx.On("InTx", matchContext()).Return(nil).Once()
	stored := storedMessage(testContainer, "nested")
	h.messages.On("Insert", matchContext(), mock.MatchedBy(func(message *models.Message) bool {
		return message.ThreadRootID != nil && *message.ThreadRootID == testMessageID
	})).Return(&stored, true, nil).Once()
	h.messages.On("ReplaceMentions", matchContext(), stored.ID, mock.Anything).Return(nil).Once()
	h.expectOutbox(1)

	_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
		ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
		ClientMessageID: "c1", Content: "nested", ThreadRootID: &reply.ID,
	})
	require.NoError(t, err)
}

func TestThreadReplyRequiresSameContainer(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
	h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(nil, nil).Once()

	root := storedMessage(uuid.MustParse("77777777-7777-4777-8777-777777777777"), "elsewhere")
	h.messages.On("Get", matchContext(), root.ID).Return(storedAggregate(root), nil).Once()

	_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
		ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
		ClientMessageID: "c1", Content: "cross", ThreadRootID: &root.ID,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, models.ErrNotFound))
}

func TestThreadReplyRejectedOnDeletedRoot(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
	h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(nil, nil).Once()

	root := storedMessage(testContainer, "gone")
	reason := models.DeletionReasonAuthor
	root.DeletionReason = &reason
	deletedAt := h.now
	root.DeletedAt = &deletedAt
	h.messages.On("Get", matchContext(), root.ID).Return(storedAggregate(root), nil).Once()

	_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
		ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
		ClientMessageID: "c1", Content: "reply", ThreadRootID: &root.ID,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, models.ErrNotFound))
}

func TestScopeOracle(t *testing.T) {
	t.Parallel()

	t.Run("returns communities and conversations", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		conversationID := uuid.MustParse("66666666-6666-4666-8666-666666666666")
		h.community.On("ActiveCommunityIDs", matchContext(), testAuthorID).
			Return([]uuid.UUID{testCommunity}, nil).Once()
		h.conversations.On("ConversationIDsForUser", matchContext(), testAuthorID).
			Return([]uuid.UUID{conversationID}, nil).Once()

		response, err := h.scopeService().UserScopes(contextBackground(), testAuthorID)
		require.NoError(t, err)
		assert.Equal(t, testAuthorID, response.UserID)
		assert.Equal(t, []uuid.UUID{testCommunity}, response.CommunityIDs)
		assert.Equal(t, []uuid.UUID{conversationID}, response.ConversationIDs)
		assert.False(t, response.GeneratedAt.IsZero())
	})

	t.Run("unknown user yields empty slices, never null", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		unknown := uuid.New()
		h.community.On("ActiveCommunityIDs", matchContext(), unknown).Return(nil, nil).Once()
		h.conversations.On("ConversationIDsForUser", matchContext(), unknown).Return(nil, nil).Once()

		response, err := h.scopeService().UserScopes(contextBackground(), unknown)
		require.NoError(t, err)
		assert.NotNil(t, response.CommunityIDs)
		assert.NotNil(t, response.ConversationIDs)
		assert.Empty(t, response.CommunityIDs)
	})
}
