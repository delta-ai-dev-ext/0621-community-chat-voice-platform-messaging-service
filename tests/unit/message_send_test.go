package unit

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"messaging_service/internal/models"
	"messaging_service/internal/service"
)

// outboxRecorder captures the events a service appends to the outbox.
type outboxRecorder struct {
	entries []models.OutboxEntryView
}

func (r *outboxRecorder) record(args mock.Arguments) {
	entry, _ := args.Get(1).(models.OutboxEntryView)
	r.entries = append(r.entries, entry)
}

// payloads decodes the captured EVT-009 payloads.
func (r *outboxRecorder) payloads(t *testing.T) []models.MessageChangedPayload {
	t.Helper()
	decoded := make([]models.MessageChangedPayload, 0, len(r.entries))
	for _, entry := range r.entries {
		var payload models.MessageChangedPayload
		require.NoError(t, json.Unmarshal(entry.Payload, &payload))
		decoded = append(decoded, payload)
	}
	return decoded
}

// expectOutbox primes the outbox mock and returns the recorder.
func (h *harness) expectOutbox(times int) *outboxRecorder {
	recorder := &outboxRecorder{}
	call := h.outbox.On("Append", matchContext(), mock.Anything)
	if times > 0 {
		call = call.Times(times)
	}
	call.Run(recorder.record).Return(nil)
	return recorder
}

func TestSendChannelMessageHappyPath(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
	h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(nil, nil).Once()
	h.tx.On("InTx", matchContext()).Return(nil).Once()
	stored := storedMessage(testContainer, "hello world")
	h.messages.On("Insert", matchContext(), mock.Anything).Return(&stored, true, nil).Once()
	h.messages.On("ReplaceMentions", matchContext(), stored.ID, mock.Anything).Return(nil).Once()
	recorder := h.expectOutbox(1)

	view, err := h.messagesService().Send(contextBackground(), service.SendCommand{
		ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
		ClientMessageID: "client-1", Content: "hello world",
	})
	require.NoError(t, err)
	require.NotNil(t, view)
	assert.Equal(t, testMessageID.String(), view.MessageID)
	assert.Equal(t, "hello world", *view.Content)
	assert.Equal(t, []models.AttachmentView{}, view.Attachments)
	assert.Equal(t, []string{}, view.MentionedUserIDs)
	assert.Equal(t, "https://support.local/appeals", h.deps.Options.AppealURLBase)

	payloads := recorder.payloads(t)
	require.Len(t, payloads, 1)
	assert.Equal(t, string(models.ChangeCreated), payloads[0].Change)
	assert.Equal(t, testContainer.String(), payloads[0].ContainerID)
	assert.Equal(t, testAuthorID.String(), payloads[0].AuthorUserID)
	require.Len(t, recorder.entries, 1)
	assert.Equal(t, models.TopicMessageEvents, recorder.entries[0].Topic)
	assert.Equal(t, testContainer.String(), recorder.entries[0].MessageKey)
	assert.Len(t, recorder.entries[0].Envelope.TraceID, 32)
	assert.Equal(t, models.SchemaRefFor(models.EventMessageChanged, 1), recorder.entries[0].Envelope.SchemaRef)
}

func TestSendChannelMessageAccessFailures(t *testing.T) {
	t.Parallel()

	t.Run("unknown channel is a 404", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.community.On("GetTextChannel", matchContext(), testContainer).Return(nil, nil).Once()
		_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
			ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
			ClientMessageID: "c1", Content: "hello",
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrNotFound))
	})

	t.Run("missing membership is not_a_member", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.expectChannelAccess(testContainer, testCommunity, testAuthorID, nil)
		_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
			ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
			ClientMessageID: "c1", Content: "hello",
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrNotAMember))
	})

	t.Run("removed member is not_a_member", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		member := &models.CommunityMember{Status: models.MemberStatusRemoved, MemberVersion: 2}
		h.expectChannelAccess(testContainer, testCommunity, testAuthorID, member)
		_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
			ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
			ClientMessageID: "c1", Content: "hello",
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrNotAMember))
	})

	t.Run("banned member is not_a_member", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		member := &models.CommunityMember{Status: models.MemberStatusActive, IsBanned: true, MemberVersion: 2}
		h.expectChannelAccess(testContainer, testCommunity, testAuthorID, member)
		_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
			ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
			ClientMessageID: "c1", Content: "hello",
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrNotAMember))
	})

	t.Run("active mute reports muted_until and the appeal pointer", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		mutedUntil := h.now.Add(2 * time.Hour)
		member := activeMembership(testCommunity, testAuthorID)
		member.MutedUntil = &mutedUntil
		h.expectChannelAccess(testContainer, testCommunity, testAuthorID, member)

		_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
			ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
			ClientMessageID: "c1", Content: "hello",
		})
		require.Error(t, err)
		var appErr *models.AppError
		require.True(t, errors.As(err, &appErr))
		assert.Equal(t, 403, appErr.Status)
		assert.Equal(t, models.CodeMemberMuted, appErr.Code)
		assert.Contains(t, appErr.Extra["appeal_url"], "community_id="+testCommunity.String())
		assert.NotEmpty(t, appErr.Extra["muted_until"])
	})

	t.Run("expired mute posts normally", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		expired := h.now.Add(-time.Minute)
		member := activeMembership(testCommunity, testAuthorID)
		member.MutedUntil = &expired
		h.expectChannelAccess(testContainer, testCommunity, testAuthorID, member)
		h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(nil, nil).Once()
		h.tx.On("InTx", matchContext()).Return(nil).Once()
		stored := storedMessage(testContainer, "hello")
		h.messages.On("Insert", matchContext(), mock.Anything).Return(&stored, true, nil).Once()
		h.messages.On("ReplaceMentions", matchContext(), stored.ID, mock.Anything).Return(nil).Once()
		h.expectOutbox(1)

		view, err := h.messagesService().Send(contextBackground(), service.SendCommand{
			ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
			ClientMessageID: "c1", Content: "hello",
		})
		require.NoError(t, err)
		require.NotNil(t, view)
	})
}

func TestSendConversationMessage(t *testing.T) {
	t.Parallel()
	conversationID := uuid.MustParse("66666666-6666-4666-8666-666666666666")

	t.Run("participant sends", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		conversation := &models.Conversation{
			ConversationID: conversationID, Kind: models.ConversationKindDirect, ConversationVersion: 1,
			ParticipantIDs: []uuid.UUID{testAuthorID},
		}
		h.conversations.On("GetConversation", matchContext(), conversationID).Return(conversation, nil).Once()
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
		assert.Equal(t, string(models.ContainerConversation), view.ContainerType)
		assert.Nil(t, view.CommunityID)
	})

	t.Run("non-participant is rejected", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		conversation := &models.Conversation{
			ConversationID: conversationID, Kind: models.ConversationKindGroup, ConversationVersion: 1,
			ParticipantIDs: []uuid.UUID{uuid.New()},
		}
		h.conversations.On("GetConversation", matchContext(), conversationID).Return(conversation, nil).Once()
		_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
			ContainerType: models.ContainerConversation, ContainerID: conversationID, AuthorUserID: testAuthorID,
			ClientMessageID: "c1", Content: "hi",
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrNotAParticipant))
	})

	t.Run("unknown conversation is a 404", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.conversations.On("GetConversation", matchContext(), conversationID).Return(nil, nil).Once()
		_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
			ContainerType: models.ContainerConversation, ContainerID: conversationID, AuthorUserID: testAuthorID,
			ClientMessageID: "c1", Content: "hi",
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrNotFound))
	})
}

func TestSendIsIdempotent(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
	h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(nil, nil).Once()
	h.tx.On("InTx", matchContext()).Return(nil).Once()

	existing := storedMessage(testContainer, "first attempt")
	h.messages.On("Insert", matchContext(), mock.Anything).Return(&existing, false, nil).Once()
	h.messages.On("Get", matchContext(), existing.ID).Return(storedAggregate(existing), nil).Once()

	view, err := h.messagesService().Send(contextBackground(), service.SendCommand{
		ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
		ClientMessageID: "client-1", Content: "first attempt",
	})
	require.NoError(t, err)
	require.NotNil(t, view)
	assert.Equal(t, existing.ID.String(), view.MessageID)
	assert.Equal(t, existing.MessageVersion, view.MessageVersion)
	// No outbox expectation was registered: a duplicate must not emit a second
	// event, and testify fails the test if Append is called.
}
