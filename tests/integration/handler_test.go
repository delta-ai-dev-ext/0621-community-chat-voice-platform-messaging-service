package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"messaging_service/internal/auth"
	"messaging_service/internal/handler"
	"messaging_service/internal/health"
	"messaging_service/internal/models"
	"messaging_service/internal/objectstore"
	"messaging_service/internal/observability"
	"messaging_service/internal/service"
	"messaging_service/tests/mocks"
)

const (
	testIssuer   = "https://identity.internal"
	testAudience = "messaging"
	serviceToken = "edge-token"
)

var (
	fixedNow     = time.Date(2026, 1, 14, 12, 0, 0, 0, time.UTC)
	channelID    = uuid.MustParse("33333333-3333-4333-8333-333333333333")
	communityID  = uuid.MustParse("44444444-4444-4444-8444-444444444444")
	authorID     = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	messageID    = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	conversation = uuid.MustParse("66666666-6666-4666-8666-666666666666")
)

// signingKey is generated once for the whole test binary.
var signingKey = mustGenerateKey()

func mustGenerateKey() *rsa.PrivateKey {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return key
}

// fixture wires the real router, real services and real middleware against
// mocked repositories, storage and broker.
type fixture struct {
	messages      *mocks.MessageRepository
	community     *mocks.CommunityProjectionRepository
	conversations *mocks.ConversationProjectionRepository
	attachments   *mocks.AttachmentRepository
	rates         *mocks.RateRepository
	outbox        *mocks.OutboxRepository
	tx            *mocks.TransactionManager
	objects       *mocks.ObjectStore
	checker       *health.Checker
	router        *gin.Engine
	token         string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	gin.SetMode(gin.TestMode)
	f := &fixture{
		messages:      &mocks.MessageRepository{},
		community:     &mocks.CommunityProjectionRepository{},
		conversations: &mocks.ConversationProjectionRepository{},
		attachments:   &mocks.AttachmentRepository{},
		rates:         &mocks.RateRepository{},
		outbox:        &mocks.OutboxRepository{},
		tx:            &mocks.TransactionManager{},
		objects:       &mocks.ObjectStore{},
		checker:       health.NewChecker(nil, time.Second),
	}
	deps := service.Deps{
		Messages:      f.messages,
		Community:     f.community,
		Conversations: f.conversations,
		Attachments:   f.attachments,
		Rates:         f.rates,
		Outbox:        f.outbox,
		Tx:            f.tx,
		Objects:       f.objects,
		Clock:         func() time.Time { return fixedNow },
		Logger:        zap.NewNop(),
		Metrics:       observability.NewMetrics(nil),
		Options: service.Options{
			MaxAttachmentBytes:   10485760,
			UploadURLTTL:         15 * time.Minute,
			DownloadURLTTL:       15 * time.Minute,
			PendingUploadGCAfter: 24 * time.Hour,
			AppealURLBase:        "https://support.local/appeals",
			PageLimitDefault:     50,
			PageLimitMax:         100,
		},
	}

	verifier, err := auth.NewVerifier(auth.Config{
		Issuer:    testIssuer,
		Audience:  testAudience,
		PublicKey: publicKeyPEM(),
	})
	require.NoError(t, err)

	f.router = handler.NewRouter(handler.Deps{
		Messages:     service.NewMessageService(deps),
		Attachments:  service.NewAttachmentService(deps),
		Scopes:       service.NewScopeService(deps),
		Verifier:     verifier,
		Health:       checker(t, f),
		Metrics:      observability.NewMetrics(nil),
		Logger:       zap.NewNop(),
		ServiceToken: serviceToken,
		PageLimitMax: 100,
	})
	f.token = signToken(t, authorID)
	return f
}

func TestSendChannelMessageEndpoint(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	t.Run("creates a message", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		primeSendFlow(t, f, "hello")

		response := doRequest(t, f, http.MethodPost, "/channels/"+channelID.String()+"/messages",
			map[string]any{"client_message_id": "client-1", "content": "hello"})
		require.Equal(t, http.StatusCreated, response.Code)
		assert.NotEmpty(t, response.Header().Get("X-Trace-Id"))

		var body models.MessageView
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		assert.Equal(t, messageID.String(), body.MessageID)
		assert.Equal(t, "hello", *body.Content)
	})

	t.Run("requires authentication", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/channels/"+channelID.String()+"/messages",
			strings.NewReader(`{"client_message_id":"c1","content":"hi"}`))
		request.Header.Set("Content-Type", "application/json")
		f.router.ServeHTTP(recorder, request)
		assert.Equal(t, http.StatusUnauthorized, recorder.Code)
		assert.Contains(t, recorder.Body.String(), "unauthenticated")
	})

	t.Run("rejects a malformed body", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		response := doRawRequest(t, f, "/channels/"+channelID.String()+"/messages", "{")
		assert.Equal(t, 400, response.Code)
	})

	t.Run("rejects a missing client_message_id", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		response := doRequest(t, f, http.MethodPost, "/channels/"+channelID.String()+"/messages",
			map[string]string{"content": "hello"})
		assert.Equal(t, 400, response.Code)
		assert.Contains(t, response.Body.String(), models.CodeInvalidRequest)
	})

	t.Run("rejects over-long content with 422", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		response := doRequest(t, f, http.MethodPost, "/channels/"+channelID.String()+"/messages",
			map[string]string{"client_message_id": "c1", "content": strings.Repeat("a", 4001)})
		assert.Equal(t, http.StatusUnprocessableEntity, response.Code)
		assert.Contains(t, response.Body.String(), models.CodeContentTooLong)
	})

	t.Run("rejects a malformed channel id", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		response := doRequest(t, f, http.MethodPost, "/channels/not-a-uuid/messages",
			map[string]string{"client_message_id": "c1", "content": "hi"})
		assert.Equal(t, 400, response.Code)
	})

	t.Run("maps a mute to 403 with the appeal pointer", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		mutedUntil := fixedNow.Add(time.Hour)
		member := &models.CommunityMember{Status: models.MemberStatusActive, MemberVersion: 1, MutedUntil: &mutedUntil}
		f.community.On("GetTextChannel", mock.Anything, channelID).
			Return(&models.TextChannel{ChannelID: channelID, CommunityID: communityID, Name: "general"}, nil).Once()
		f.community.On("GetMembership", mock.Anything, communityID, authorID).Return(member, nil).Once()

		response := doRequest(t, f, http.MethodPost, "/channels/"+channelID.String()+"/messages",
			map[string]string{"client_message_id": "c1", "content": "hi"})
		assert.Equal(t, http.StatusForbidden, response.Code)
		body := response.Body.String()
		assert.Contains(t, body, models.CodeMemberMuted)
		assert.Contains(t, body, "appeal_url")
		assert.Contains(t, body, "muted_until")
	})

	t.Run("maps an unknown channel to 404", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		f.community.On("GetTextChannel", mock.Anything, channelID).Return(nil, nil).Once()
		response := doRequest(t, f, http.MethodPost, "/channels/"+channelID.String()+"/messages",
			map[string]string{"client_message_id": "c1", "content": "hi"})
		assert.Equal(t, http.StatusNotFound, response.Code)
	})
}

func TestHistoryEndpoint(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	t.Run("pages messages", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		f.community.On("GetTextChannel", mock.Anything, channelID).
			Return(&models.TextChannel{ChannelID: channelID, CommunityID: communityID}, nil).Once()
		f.community.On("GetMembership", mock.Anything, communityID, authorID).
			Return(&models.CommunityMember{Status: models.MemberStatusActive}, nil).Once()
		content := "hello"
		f.messages.On("Page", mock.Anything, mock.Anything).Return([]models.MessageAggregate{
			{Message: models.Message{ID: messageID, ContainerType: models.ContainerChannel, ContainerID: channelID,
				CommunityID: &communityID, AuthorUserID: authorID, Content: &content, MessageVersion: 1,
				CreatedAt: fixedNow}, Reactions: []models.ReactionSummary{}},
		}, nil).Once()

		response := doRequest(t, f, http.MethodGet, "/channels/"+channelID.String()+"/messages?limit=10", nil)
		require.Equal(t, http.StatusOK, response.Code)
		var page models.MessagePage
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &page))
		assert.False(t, page.HasMore)
		assert.Nil(t, page.NextCursor)
		require.Len(t, page.Data, 1)
	})

	t.Run("rejects a tampered cursor", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		response := doRequest(t, f, http.MethodGet,
			"/channels/"+channelID.String()+"/messages?before=not-a-cursor", nil)
		assert.Equal(t, 400, response.Code)
	})

	t.Run("rejects an out-of-range limit", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		response := doRequest(t, f, http.MethodGet, "/channels/"+channelID.String()+"/messages?limit=101", nil)
		assert.Equal(t, 400, response.Code)
	})

	t.Run("rejects both cursors", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		cursor := models.NewCursor(fixedNow, messageID).Encode()
		response := doRequest(t, f, http.MethodGet,
			"/channels/"+channelID.String()+"/messages?before="+cursor+"&after="+cursor, nil)
		assert.Equal(t, 400, response.Code)
	})
}

func TestThreadEditDeleteAndReactions(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	t.Run("thread returns root and replies", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		content := "root"
		root := models.Message{ID: messageID, ContainerType: models.ContainerChannel, ContainerID: channelID,
			CommunityID: &communityID, AuthorUserID: authorID, Content: &content, MessageVersion: 1, CreatedAt: fixedNow}
		f.messages.On("Get", mock.Anything, messageID).Return(&models.MessageAggregate{Message: root}, nil).Once()
		f.community.On("GetMembership", mock.Anything, communityID, authorID).
			Return(&models.CommunityMember{Status: models.MemberStatusActive}, nil).Once()
		f.messages.On("ReplyCount", mock.Anything, messageID).Return(0, nil).Once()
		f.messages.On("Replies", mock.Anything, messageID, (*models.Cursor)(nil), 51).Return(nil, nil).Once()

		response := doRequest(t, f, http.MethodGet, "/messages/"+messageID.String()+"/thread", nil)
		require.Equal(t, http.StatusOK, response.Code)
		var page models.ThreadPage
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &page))
		assert.Equal(t, 0, page.ReplyCount)
		assert.Equal(t, messageID.String(), page.Root.MessageID)
	})

	t.Run("thread missing is 404", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		f.messages.On("Get", mock.Anything, messageID).Return(nil, nil).Once()
		response := doRequest(t, f, http.MethodGet, "/messages/"+messageID.String()+"/thread", nil)
		assert.Equal(t, http.StatusNotFound, response.Code)
	})

	t.Run("edit returns the updated message", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		content := "before"
		stored := models.Message{ID: messageID, ContainerType: models.ContainerChannel, ContainerID: channelID,
			CommunityID: &communityID, AuthorUserID: authorID, Content: &content, MessageVersion: 1, CreatedAt: fixedNow}
		f.messages.On("Get", mock.Anything, messageID).Return(&models.MessageAggregate{Message: stored}, nil).Once()
		f.community.On("GetMembership", mock.Anything, communityID, authorID).
			Return(&models.CommunityMember{Status: models.MemberStatusActive}, nil).Twice()
		f.community.On("GetAutomodRuleSet", mock.Anything, communityID).Return(nil, nil).Once()
		f.tx.On("InTx", mock.Anything).Return(nil).Once()
		edited := stored
		edited.Content = stringPointer("after")
		edited.MessageVersion = 2
		f.messages.On("UpdateContent", mock.Anything, messageID, "after", fixedNow).Return(&edited, nil).Once()
		f.messages.On("ReplaceMentions", mock.Anything, messageID, mock.Anything).Return(nil).Once()
		f.outbox.On("Append", mock.Anything, mock.Anything).Return(nil).Once()

		response := doRequest(t, f, http.MethodPatch, "/messages/"+messageID.String(), map[string]string{"content": "after"})
		require.Equal(t, http.StatusOK, response.Code)
		assert.Contains(t, response.Body.String(), "after")
	})

	t.Run("delete returns 204", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		content := "bye"
		stored := models.Message{ID: messageID, ContainerType: models.ContainerChannel, ContainerID: channelID,
			CommunityID: &communityID, AuthorUserID: authorID, Content: &content, MessageVersion: 1, CreatedAt: fixedNow}
		f.messages.On("Get", mock.Anything, messageID).Return(&models.MessageAggregate{Message: stored}, nil).Once()
		f.community.On("GetMembership", mock.Anything, communityID, authorID).
			Return(&models.CommunityMember{Status: models.MemberStatusActive}, nil).Once()
		f.tx.On("InTx", mock.Anything).Return(nil).Once()
		deleted := stored
		reason := models.DeletionReasonAuthor
		deleted.DeletionReason = &reason
		deleted.DeletedAt = &fixedNow
		deleted.MessageVersion = 2
		f.messages.On("SoftDelete", mock.Anything, messageID, models.DeletionReasonAuthor, fixedNow).
			Return(&deleted, nil).Once()
		f.messages.On("DetachAttachments", mock.Anything, messageID).Return([]string{}, nil).Once()
		f.outbox.On("Append", mock.Anything, mock.Anything).Return(nil).Once()

		response := doRequest(t, f, http.MethodDelete, "/messages/"+messageID.String(), nil)
		assert.Equal(t, http.StatusNoContent, response.Code)
	})

	t.Run("reactions add and remove", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		content := "nice"
		stored := models.Message{ID: messageID, ContainerType: models.ContainerChannel, ContainerID: channelID,
			CommunityID: &communityID, AuthorUserID: authorID, Content: &content, MessageVersion: 1, CreatedAt: fixedNow}
		f.messages.On("Get", mock.Anything, messageID).
			Return(&models.MessageAggregate{Message: stored, Reactions: []models.ReactionSummary{}}, nil).Twice()
		f.community.On("GetMembership", mock.Anything, communityID, authorID).
			Return(&models.CommunityMember{Status: models.MemberStatusActive}, nil).Twice()
		f.tx.On("InTx", mock.Anything).Return(nil).Twice()
		f.messages.On("AddReaction", mock.Anything, messageID, authorID, "🎉", fixedNow).Return(true, nil).Once()
		f.messages.On("RemoveReaction", mock.Anything, messageID, authorID, "🎉").Return(true, nil).Once()
		f.messages.On("BumpVersion", mock.Anything, messageID).Return(int64(2), nil).Twice()
		f.messages.On("ReactionSummary", mock.Anything, messageID).
			Return([]models.ReactionSummary{{Emoji: "🎉", Count: 1}}, nil).Twice()
		f.outbox.On("Append", mock.Anything, mock.Anything).Return(nil).Twice()

		added := doRequest(t, f, http.MethodPut,
			"/messages/"+messageID.String()+"/reactions/"+emojiPath("🎉"), nil)
		require.Equal(t, http.StatusOK, added.Code)

		removed := doRequest(t, f, http.MethodDelete,
			"/messages/"+messageID.String()+"/reactions/"+emojiPath("🎉"), nil)
		require.Equal(t, http.StatusOK, removed.Code)
	})

	t.Run("rejects an over-long emoji", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		response := doRequest(t, f, http.MethodPut,
			"/messages/"+messageID.String()+"/reactions/"+strings.Repeat("x", 40), nil)
		assert.Equal(t, 400, response.Code)
	})
}

func TestAttachmentEndpoint(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	t.Run("reserves an upload", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		stored := models.Attachment{ID: messageID, UploaderUserID: authorID, Filename: "cat.png",
			DeclaredContentType: "image/png", DeclaredSizeBytes: 100, ObjectKey: "attachments/key.png",
			Status: models.AttachmentPendingUpload, CreatedAt: fixedNow}
		f.attachments.On("Create", mock.Anything, mock.Anything).Return(&stored, true, nil).Once()
		f.objects.On("PresignPut", mock.Anything, stored.ObjectKey, "image/png", 15*time.Minute).
			Return(objectstore.SignedURL{URL: "https://storage.local/put", ExpiresAt: fixedNow.Add(15 * time.Minute)}, nil).Once()

		response := doRequest(t, f, http.MethodPost, "/attachments", map[string]any{
			"filename": "cat.png", "content_type": "image/png", "size_bytes": 100,
		})
		require.Equal(t, http.StatusCreated, response.Code)
		assert.Contains(t, response.Body.String(), "https://storage.local/put")
	})

	t.Run("rejects an unsupported type", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		response := doRequest(t, f, http.MethodPost, "/attachments", map[string]any{
			"filename": "doc.pdf", "content_type": "application/pdf", "size_bytes": 100,
		})
		assert.Equal(t, http.StatusUnsupportedMediaType, response.Code)
	})

	t.Run("rejects an oversized upload", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		response := doRequest(t, f, http.MethodPost, "/attachments", map[string]any{
			"filename": "huge.png", "content_type": "image/png", "size_bytes": 10485761,
		})
		assert.Equal(t, http.StatusRequestEntityTooLarge, response.Code)
	})
}

func TestInternalScopesEndpoint(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	t.Run("returns scopes with the service token", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		f.community.On("ActiveCommunityIDs", mock.Anything, authorID).
			Return([]uuid.UUID{communityID}, nil).Once()
		f.conversations.On("ConversationIDsForUser", mock.Anything, authorID).
			Return([]uuid.UUID{conversation}, nil).Once()

		recorder := httptest.NewRecorder()
		body := `{"user_id":"` + authorID.String() + `"}`
		request := httptest.NewRequest(http.MethodPost, "/internal/realtime/user-scopes", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+serviceToken)
		f.router.ServeHTTP(recorder, request)

		require.Equal(t, http.StatusOK, recorder.Code)
		var scopes models.UserScopesResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &scopes))
		assert.Equal(t, authorID, scopes.UserID)
		assert.Equal(t, []uuid.UUID{communityID}, scopes.CommunityIDs)
		assert.Equal(t, []uuid.UUID{conversation}, scopes.ConversationIDs)
	})

	t.Run("rejects an access token", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/internal/realtime/user-scopes",
			strings.NewReader(`{"user_id":"`+authorID.String()+`"}`))
		request.Header.Set("Authorization", "Bearer "+f.token)
		f.router.ServeHTTP(recorder, request)
		assert.Equal(t, http.StatusUnauthorized, recorder.Code)
		assert.Equal(t, "application/problem+json", recorder.Header().Get("Content-Type"))
	})

	t.Run("rejects an invalid body", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/internal/realtime/user-scopes",
			strings.NewReader(`{"user_id":"not-a-uuid"}`))
		request.Header.Set("Authorization", "Bearer "+serviceToken)
		f.router.ServeHTTP(recorder, request)
		assert.Equal(t, 400, recorder.Code)
	})
}

func TestProbes(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	t.Run("liveness never touches dependencies", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		checker(t, f).Add("postgres", func(context.Context) error { return assert.AnError })

		response := doRequest(t, f, http.MethodGet, "/healthz", nil)
		assert.Equal(t, http.StatusOK, response.Code)
	})

	t.Run("readiness reports failing dependencies", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		checker(t, f).Add("postgres", func(context.Context) error { return assert.AnError })

		response := doRequest(t, f, http.MethodGet, "/readyz", nil)
		assert.Equal(t, http.StatusServiceUnavailable, response.Code)
		assert.Contains(t, response.Body.String(), "postgres")
	})

	t.Run("readiness fails once shutdown begins", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		checker(t, f).BeginShutdown()

		response := doRequest(t, f, http.MethodGet, "/readyz", nil)
		assert.Equal(t, http.StatusServiceUnavailable, response.Code)
		// Liveness still answers during drain.
		assert.Equal(t, http.StatusOK, doRequest(t, f, http.MethodGet, "/healthz", nil).Code)
	})
}

// helpers -------------------------------------------------------------------

// checker normalises the fixture's health checker accessor.
func checker(t *testing.T, f *fixture) *health.Checker {
	t.Helper()
	return f.checker
}

// primeSendFlow registers the mocks a successful channel send needs.
func primeSendFlow(t *testing.T, f *fixture, content string) {
	t.Helper()
	f.community.On("GetTextChannel", mock.Anything, channelID).
		Return(&models.TextChannel{ChannelID: channelID, CommunityID: communityID, Name: "general"}, nil).Once()
	f.community.On("GetMembership", mock.Anything, communityID, authorID).
		Return(&models.CommunityMember{Status: models.MemberStatusActive}, nil).Once()
	f.community.On("GetAutomodRuleSet", mock.Anything, communityID).Return(nil, nil).Once()
	f.tx.On("InTx", mock.Anything).Return(nil).Once()
	stored := models.Message{ID: messageID, ContainerType: models.ContainerChannel, ContainerID: channelID,
		CommunityID: &communityID, AuthorUserID: authorID, Content: &content, MessageVersion: 1, CreatedAt: fixedNow}
	f.messages.On("Insert", mock.Anything, mock.Anything).Return(&stored, true, nil).Once()
	f.messages.On("ReplaceMentions", mock.Anything, messageID, mock.Anything).Return(nil).Once()
	f.outbox.On("Append", mock.Anything, mock.Anything).Return(nil).Once()
}

func doRequest(t *testing.T, f *fixture, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(encoded)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, reader)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+f.token)
	f.router.ServeHTTP(recorder, request)
	return recorder
}

func doRawRequest(t *testing.T, f *fixture, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+f.token)
	f.router.ServeHTTP(recorder, request)
	return recorder
}

func signToken(t *testing.T, userID uuid.UUID) string {
	if t != nil {
		t.Helper()
	}
	claims := jwt.MapClaims{
		"sub": userID.String(),
		"sid": "session-1",
		"iss": testIssuer,
		"aud": []string{testAudience},
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Add(-time.Minute).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signed, err := token.SignedString(signingKey)
	if err != nil && t != nil {
		t.Fatalf("signing token: %v", err)
	}
	return signed
}

func publicKeyPEM() string {
	der, err := x509.MarshalPKIXPublicKey(&signingKey.PublicKey)
	if err != nil {
		panic(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

func emojiPath(emoji string) string {
	return url.PathEscape(emoji)
}

func stringPointer(value string) *string { return &value }
