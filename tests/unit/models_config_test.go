package unit

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"messaging_service/internal/config"
	"messaging_service/internal/models"
)

func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()
	id := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	// 123456789 nanoseconds are lost by design: timestamptz has microsecond
	// precision, so the cursor keeps exactly what PostgreSQL can store.
	created := time.Date(2026, 1, 14, 12, 0, 0, 123456000, time.UTC)

	cursor := models.NewCursor(created, id)
	decoded, err := models.DecodeCursor(cursor.Encode())
	require.NoError(t, err)
	assert.Equal(t, created, decoded.CreatedAt)
	assert.Equal(t, id, decoded.ID)
}

func TestDecodeCursorRejectsTamperedTokens(t *testing.T) {
	t.Parallel()
	valid := models.NewCursor(time.Now().UTC(), uuid.New()).Encode()

	cases := map[string]string{
		"not base64":    "!!!not-base64!!!",
		"missing pipe":  encodeRaw("1700000000000000"),
		"bad uuid":      encodeRaw("1700000000000000|not-a-uuid"),
		"bad number":    encodeRaw("abc|11111111-1111-4111-8111-111111111111"),
		"negative time": encodeRaw("-5|11111111-1111-4111-8111-111111111111"),
	}
	for name, token := range cases {
		token := token
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := models.DecodeCursor(token)
			require.Error(t, err)
			assert.True(t, errors.Is(err, models.ErrInvalidCursor))
			var appErr *models.AppError
			require.True(t, errors.As(err, &appErr))
			assert.Equal(t, 400, appErr.Status)
		})
	}

	decoded, err := models.DecodeCursor(valid)
	require.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, decoded.ID)
}

func TestEnvelopeConstruction(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 14, 12, 0, 0, 0, time.UTC)

	t.Run("mints a trace id when none is supplied", func(t *testing.T) {
		t.Parallel()
		envelope, err := models.NewEnvelope(models.EnvelopeInput{EventType: models.EventMessageChanged}, now)
		require.NoError(t, err)
		assert.Len(t, envelope.TraceID, 32)
		assert.Equal(t, models.ProducerName, envelope.Producer)
		assert.Equal(t, models.SchemaRefFor(models.EventMessageChanged, 1), envelope.SchemaRef)
		assert.Equal(t, now, envelope.OccurredAt)
		require.NotNil(t, envelope.EventID)
		assert.NotEqual(t, uuid.Nil, envelope.CorrelationID)
		require.NoError(t, envelope.Validate())
	})

	t.Run("keeps an inbound trace and correlation id", func(t *testing.T) {
		t.Parallel()
		correlation := uuid.New()
		causation := uuid.New()
		envelope, err := models.NewEnvelope(models.EnvelopeInput{
			EventType: models.EventMessageChanged, TraceID: "0123456789abcdef0123456789abcdef",
			CorrelationID: correlation, CausationID: &causation,
		}, now)
		require.NoError(t, err)
		assert.Equal(t, "0123456789abcdef0123456789abcdef", envelope.TraceID)
		assert.Equal(t, correlation, envelope.CorrelationID)
		assert.Equal(t, &causation, envelope.CausationID)
	})

	t.Run("rejects invalid inputs", func(t *testing.T) {
		t.Parallel()
		_, err := models.NewEnvelope(models.EnvelopeInput{EventType: "Not-Valid"}, now)
		require.Error(t, err)
		_, err = models.NewEnvelope(models.EnvelopeInput{
			EventType: models.EventMessageChanged, TraceID: "zzz",
		}, now)
		require.Error(t, err)
		_, err = models.NewEnvelope(models.EnvelopeInput{
			EventType: models.EventMessageChanged, EventVersion: -1,
		}, now)
		require.Error(t, err)
	})
}

func TestEnvelopeValidate(t *testing.T) {
	t.Parallel()
	valid, err := models.NewEnvelope(models.EnvelopeInput{
		EventType: models.EventCommunityMemberChanged,
	}, time.Now().UTC())
	require.NoError(t, err)

	cases := map[string]func(*models.Envelope){
		"nil event id":     func(e *models.Envelope) { e.EventID = uuid.Nil },
		"bad event type":   func(e *models.Envelope) { e.EventType = "Bad" },
		"bad version":      func(e *models.Envelope) { e.EventVersion = 0 },
		"missing occurred": func(e *models.Envelope) { e.OccurredAt = time.Time{} },
		"missing producer": func(e *models.Envelope) { e.Producer = "" },
		"empty trace":      func(e *models.Envelope) { e.TraceID = "" },
		"nil correlation":  func(e *models.Envelope) { e.CorrelationID = uuid.Nil },
		"bad schema ref":   func(e *models.Envelope) { e.SchemaRef = "nope" },
	}
	for name, mutate := range cases {
		mutate := mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			copy := valid
			mutate(&copy)
			require.Error(t, copy.Validate())
		})
	}
	require.NoError(t, valid.Validate())
}

func TestMintTraceID(t *testing.T) {
	t.Parallel()
	first := models.MintTraceID()
	second := models.MintTraceID()
	assert.Len(t, first, 32)
	assert.NotEqual(t, first, second)
	assert.Regexp(t, "^[0-9a-f]{32}$", first)
}

func TestAppErrorBehaviour(t *testing.T) {
	t.Parallel()

	muted := models.NewMemberMuted(testNow.Add(time.Hour), "https://support.local/appeals?community_id=x")
	assert.Equal(t, 403, muted.Status)
	assert.Equal(t, models.CodeMemberMuted, muted.Code)
	assert.Contains(t, muted.Detail, "You can appeal")
	assert.Equal(t, "https://support.local/appeals?community_id=x", muted.Extra["appeal_url"])
	assert.NotEmpty(t, muted.Extra["muted_until"])

	blocked := models.NewBlockedByAutomod(models.AutomodCategorySpam)
	assert.Equal(t, 422, blocked.Status)
	assert.Contains(t, blocked.Detail, "spam")
	assert.Equal(t, models.AutomodCategorySpam, blocked.Extra["category"])

	wrapped := models.ErrNotFound.WithDetail("custom")
	assert.True(t, errors.Is(wrapped, models.ErrNotFound))
	assert.Equal(t, "custom", wrapped.Detail)
	assert.False(t, errors.Is(wrapped, models.ErrNotAMember))
	assert.False(t, errors.Is(wrapped, errors.New("other")))

	invalid := models.NewAttachmentInvalid("nope")
	assert.Equal(t, 422, invalid.Status)
	assert.True(t, errors.Is(invalid, models.ErrAttachmentInvalid))
}

func TestPayloadContracts(t *testing.T) {
	t.Parallel()
	communityID := uuid.New()
	userID := uuid.New()
	channelID := uuid.New()
	conversationID := uuid.New()

	validMember := models.CommunityMemberChangedPayload{
		CommunityID: communityID, UserID: userID, Change: "muted", Status: models.MemberStatusActive,
		MemberVersion: 2,
	}
	require.NoError(t, validMember.Validate())
	badMember := validMember
	badMember.Status = "pending"
	require.Error(t, badMember.Validate())
	badMember = validMember
	badMember.Change = "teleported"
	require.Error(t, badMember.Validate())
	badMember = validMember
	badMember.MemberVersion = 0
	require.Error(t, badMember.Validate())

	validChannel := models.CommunityChannelChangedPayload{
		CommunityID: communityID, ChannelID: channelID, Change: "created", Kind: "text", Name: "general",
		ChannelVersion: 1,
	}
	require.NoError(t, validChannel.Validate())
	badChannel := validChannel
	badChannel.Kind = "stage"
	require.Error(t, badChannel.Validate())
	badChannel = validChannel
	badChannel.Name = ""
	require.Error(t, badChannel.Validate())
	badChannel = validChannel
	badChannel.Change = "archived"
	require.Error(t, badChannel.Validate())
	badChannel = validChannel
	badChannel.Position = -1
	require.Error(t, badChannel.Validate())

	validRules := models.AutomodRulesChangedPayload{
		CommunityID: communityID, RuleSetVersion: 3,
		Spam: models.SpamRules{Action: "block", MaxMessagesPerWindow: 5, WindowSeconds: 10,
			MaxDuplicateMessages: 2, MaxMentionsPerMessage: 3, MaxLinksPerMessage: 1},
		BannedWords: models.BannedWordsRules{Action: "remove", MatchMode: "whole_word", Words: []string{"spam"}},
	}
	require.NoError(t, validRules.Validate())
	badRules := validRules
	badRules.Spam.WindowSeconds = 0
	require.Error(t, badRules.Validate())
	badRules = validRules
	badRules.Spam.Action = "ban"
	require.Error(t, badRules.Validate())
	badRules = validRules
	badRules.BannedWords.MatchMode = "regex"
	require.Error(t, badRules.Validate())
	badRules = validRules
	badRules.BannedWords.Words = []string{""}
	require.Error(t, badRules.Validate())

	validConversation := models.ConversationChangedPayload{
		ConversationID: conversationID, Kind: models.ConversationKindGroup, Change: "created",
		ParticipantUserIDs: []uuid.UUID{userID}, ConversationVersion: 1,
	}
	require.NoError(t, validConversation.Validate())
	badConversation := validConversation
	badConversation.ParticipantUserIDs = nil
	require.Error(t, badConversation.Validate())
	badConversation = validConversation
	badConversation.Kind = "broadcast"
	require.Error(t, badConversation.Validate())
	badConversation = validConversation
	badConversation.Change = "deleted"
	require.Error(t, badConversation.Validate())
}

func TestMessageChangedPayloadContract(t *testing.T) {
	t.Parallel()
	payload := models.MessageChangedPayload{
		MessageID:        uuid.New().String(),
		Change:           string(models.ChangeCreated),
		ContainerType:    string(models.ContainerChannel),
		ContainerID:      uuid.New().String(),
		AuthorUserID:     uuid.New().String(),
		Content:          pointer("hello"),
		Attachments:      []models.AttachmentSnapshot{},
		MentionedUserIDs: []string{},
		Reactions:        []models.ReactionSummary{},
		CreatedAt:        testNow,
		MessageVersion:   1,
	}
	require.NoError(t, payload.Validate())

	encoded, err := json.Marshal(validEvent(t, models.EventMessageChanged, payload))
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"envelope"`)
	assert.Contains(t, string(encoded), `"payload"`)

	bad := payload
	bad.Change = "archived"
	require.Error(t, bad.Validate())
	bad = payload
	bad.ContainerType = "thread"
	require.Error(t, bad.Validate())
	bad = payload
	bad.MessageVersion = 0
	require.Error(t, bad.Validate())
}

func TestProjectionHelpers(t *testing.T) {
	t.Parallel()
	member := models.CommunityMember{Status: models.MemberStatusActive}
	assert.True(t, member.HasAccess())
	require.Nil(t, member.MutedUntil)

	mutedUntil := testNow.Add(time.Hour)
	member.MutedUntil = &mutedUntil
	assert.True(t, member.IsMuted(testNow))
	assert.False(t, member.IsMuted(testNow.Add(2*time.Hour)))
	assert.True(t, member.HasAccess())

	banned := models.CommunityMember{Status: models.MemberStatusActive, IsBanned: true}
	assert.False(t, banned.HasAccess())
	removed := models.CommunityMember{Status: models.MemberStatusRemoved}
	assert.False(t, removed.HasAccess())

	roleID := uuid.New()
	ruleSet := models.AutomodRuleSet{ExemptRoleIDs: []uuid.UUID{roleID}}
	assert.True(t, ruleSet.IsExempt([]uuid.UUID{roleID}))
	assert.False(t, ruleSet.IsExempt([]uuid.UUID{uuid.New()}))
	assert.False(t, ruleSet.IsExempt(nil))
	assert.False(t, models.AutomodRuleSet{}.IsExempt([]uuid.UUID{roleID}))

	conversation := models.Conversation{ParticipantIDs: []uuid.UUID{testAuthorID}}
	assert.True(t, conversation.IsParticipant(testAuthorID))
	assert.False(t, conversation.IsParticipant(uuid.New()))

	assert.True(t, models.SupportedImageType("image/webp"))
	assert.False(t, models.SupportedImageType("application/pdf"))
	assert.True(t, models.ContainerChannel.Valid())
	assert.False(t, models.ContainerType("voice").Valid())
}

func TestConfigLoad(t *testing.T) {
	t.Parallel()
	minimal := map[string]string{
		"DATABASE_URL":           "postgres://localhost:5432/messaging",
		"KAFKA_BROKERS":          "kafka-1:9092, kafka-2:9092",
		"S3_ENDPOINT_URL":        "http://minio:9000",
		"S3_BUCKET":              "attachments",
		"IDENTITY_ISSUER":        "https://identity.internal",
		"IDENTITY_AUDIENCE":      "messaging",
		"IDENTITY_PUBLIC_KEY":    "-----BEGIN PUBLIC KEY-----",
		"INTERNAL_SERVICE_TOKEN": "secret-token",
	}
	getenv := func(key string) string { return minimal[key] }

	cfg, err := config.Load(getenv)
	require.NoError(t, err)
	assert.Equal(t, config.DefaultPort, cfg.Port)
	assert.Equal(t, []string{"kafka-1:9092", "kafka-2:9092"}, cfg.KafkaBrokers)
	assert.Equal(t, config.DefaultAttachmentMaxBytes, cfg.AttachmentMaxBytes)
	assert.Equal(t, config.DefaultUploadURLTTL, cfg.UploadURLTTL)
	assert.Equal(t, config.DefaultPendingUploadGCAfter, cfg.PendingUploadGCAfter)
	assert.Equal(t, config.DefaultShutdownTimeout, cfg.ShutdownTimeout)
	assert.Equal(t, config.DefaultPageLimit, cfg.PageLimitDefault)

	t.Run("missing requirements are reported together", func(t *testing.T) {
		t.Parallel()
		_, err := config.Load(func(string) string { return "" })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DATABASE_URL")
		assert.Contains(t, err.Error(), "INTERNAL_SERVICE_TOKEN")
	})

	t.Run("invalid values are rejected", func(t *testing.T) {
		t.Parallel()
		overrides := map[string]string{
			"UPLOAD_URL_TTL": "not-a-duration",
		}
		_, err := config.Load(func(key string) string {
			if value, ok := overrides[key]; ok {
				return value
			}
			return minimal[key]
		})
		require.Error(t, err)

		overrides = map[string]string{"ATTACHMENT_MAX_BYTES": "abc"}
		_, err = config.Load(func(key string) string {
			if value, ok := overrides[key]; ok {
				return value
			}
			return minimal[key]
		})
		require.Error(t, err)
	})

	t.Run("nil getenv is rejected", func(t *testing.T) {
		t.Parallel()
		_, err := config.Load(nil)
		require.Error(t, err)
	})
}

// helpers -------------------------------------------------------------------

func pointer(value string) *string { return &value }

func validEvent(t *testing.T, eventType string, payload any) models.Event {
	t.Helper()
	envelope, err := models.NewEnvelope(models.EnvelopeInput{EventType: eventType}, testNow)
	if err != nil {
		t.Fatalf("building envelope: %v", err)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshalling payload: %v", err)
	}
	return models.Event{Envelope: envelope, Payload: body}
}

func encodeRaw(raw string) string {
	return base64URL([]byte(raw))
}
