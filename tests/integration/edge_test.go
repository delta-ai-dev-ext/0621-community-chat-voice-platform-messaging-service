package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"messaging_service/internal/models"
)

func TestTraceContinuationAndMetrics(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	t.Run("echoes the inbound W3C trace id", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		primeSendFlow(t, f, "hello")

		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/channels/"+channelID.String()+"/messages",
			strings.NewReader(`{"client_message_id":"c1","content":"hello"}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+f.token)
		request.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
		f.router.ServeHTTP(recorder, request)

		require.Equal(t, http.StatusCreated, recorder.Code)
		assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", recorder.Header().Get("X-Trace-Id"))
	})

	t.Run("mints a trace id when none is supplied", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		response := doRequest(t, f, http.MethodGet, "/healthz", nil)
		require.Equal(t, http.StatusOK, response.Code)
		assert.Len(t, response.Header().Get("X-Trace-Id"), 32)
	})

	t.Run("serves prometheus metrics", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		// Generate one request first so the RED metrics are populated.
		require.Equal(t, http.StatusOK, doRequest(t, f, http.MethodGet, "/healthz", nil).Code)
		response := doRequest(t, f, http.MethodGet, "/metrics", nil)
		require.Equal(t, http.StatusOK, response.Code)
		assert.Contains(t, response.Body.String(), "http_requests")
	})

	t.Run("unmatched routes are counted separately", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		response := doRequest(t, f, http.MethodGet, "/does-not-exist", nil)
		assert.Equal(t, http.StatusNotFound, response.Code)
	})
}

func TestConversationEndpoints(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	t.Run("participant posts", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		f.conversations.On("GetConversation", mock.Anything, conversation).Return(&models.Conversation{
			ConversationID: conversation, Kind: models.ConversationKindGroup, ConversationVersion: 1,
			ParticipantIDs: []uuid.UUID{authorID},
		}, nil).Once()
		f.tx.On("InTx", mock.Anything).Return(nil).Once()
		content := "hi"
		stored := models.Message{ID: messageID, ContainerType: models.ContainerConversation,
			ContainerID: conversation, AuthorUserID: authorID, Content: &content,
			MessageVersion: 1, CreatedAt: fixedNow}
		f.messages.On("Insert", mock.Anything, mock.Anything).Return(&stored, true, nil).Once()
		f.messages.On("ReplaceMentions", mock.Anything, messageID, mock.Anything).Return(nil).Once()
		f.outbox.On("Append", mock.Anything, mock.Anything).Return(nil).Once()

		response := doRequest(t, f, http.MethodPost, "/conversations/"+conversation.String()+"/messages",
			map[string]string{"client_message_id": "c1", "content": "hi"})
		require.Equal(t, http.StatusCreated, response.Code)
		assert.Contains(t, response.Body.String(), conversation.String())
	})

	t.Run("non-participant is rejected", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		f.conversations.On("GetConversation", mock.Anything, conversation).Return(&models.Conversation{
			ConversationID: conversation, Kind: models.ConversationKindDirect, ConversationVersion: 1,
			ParticipantIDs: []uuid.UUID{uuid.New()},
		}, nil).Once()

		response := doRequest(t, f, http.MethodPost, "/conversations/"+conversation.String()+"/messages",
			map[string]string{"client_message_id": "c1", "content": "hi"})
		assert.Equal(t, http.StatusForbidden, response.Code)
		assert.Contains(t, response.Body.String(), models.CodeNotAMember)
	})

	t.Run("history pages conversation messages", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		f.conversations.On("GetConversation", mock.Anything, conversation).Return(&models.Conversation{
			ConversationID: conversation, Kind: models.ConversationKindGroup, ConversationVersion: 1,
			ParticipantIDs: []uuid.UUID{authorID},
		}, nil).Once()
		f.messages.On("Page", mock.Anything, mock.Anything).Return([]models.MessageAggregate{}, nil).Once()

		response := doRequest(t, f, http.MethodGet, "/conversations/"+conversation.String()+"/messages", nil)
		require.Equal(t, http.StatusOK, response.Code)
		var page models.MessagePage
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &page))
		assert.Empty(t, page.Data)
		assert.False(t, page.HasMore)
	})
}

func TestDeleteRejectsNonAuthor(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)
	f := newFixture(t)
	other := uuid.New()
	content := "mine"
	stored := models.Message{ID: messageID, ContainerType: models.ContainerChannel, ContainerID: channelID,
		CommunityID: &communityID, AuthorUserID: authorID, Content: &content, MessageVersion: 1, CreatedAt: fixedNow}
	f.messages.On("Get", mock.Anything, messageID).Return(&models.MessageAggregate{Message: stored}, nil).Once()
	f.community.On("GetMembership", mock.Anything, communityID, other).
		Return(&models.CommunityMember{Status: models.MemberStatusActive}, nil).Once()
	f.token = signToken(t, other)

	response := doRequest(t, f, http.MethodDelete, "/messages/"+messageID.String(), nil)
	assert.Equal(t, http.StatusForbidden, response.Code)
	assert.Contains(t, response.Body.String(), models.CodeNotAuthor)
}
