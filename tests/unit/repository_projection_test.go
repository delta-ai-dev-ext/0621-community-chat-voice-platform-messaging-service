package unit

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"messaging_service/internal/models"
	"messaging_service/internal/repository"
	"messaging_service/tests/mocks"
)

func TestCommunityProjectionRepository(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mutedUntil := testNow.Add(24 * time.Hour)
	roleID := uuid.MustParse("55555555-5555-4555-8555-555555555555")

	t.Run("text channel missing", func(t *testing.T) {
		t.Parallel()
		repo := repository.NewCommunityProjectionRepository(mocks.NewScript().QueueRow(mocks.NoRow()))
		channel, err := repo.GetTextChannel(ctx, testContainer)
		require.NoError(t, err)
		assert.Nil(t, channel)
	})

	t.Run("text channel found", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().QueueRow(mocks.NewRow(
			testContainer.String(), testCommunity.String(), "general", 2, int64(4)))
		channel, err := repository.NewCommunityProjectionRepository(script).GetTextChannel(ctx, testContainer)
		require.NoError(t, err)
		require.NotNil(t, channel)
		assert.Equal(t, "general", channel.Name)
		assert.Equal(t, int64(4), channel.ChannelVersion)
	})

	t.Run("channel upsert honours the version guard", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().QueueRow(mocks.NewRow(int64(4))).QueueRow(mocks.NoRow())
		repo := repository.NewCommunityProjectionRepository(script)
		channel := models.TextChannel{ChannelID: testContainer, CommunityID: testCommunity, Name: "general", ChannelVersion: 4}

		applied, err := repo.UpsertTextChannel(ctx, channel)
		require.NoError(t, err)
		assert.True(t, applied)

		applied, err = repo.UpsertTextChannel(ctx, channel)
		require.NoError(t, err)
		assert.False(t, applied)
		assert.Contains(t, script.Calls()[0].SQL, "WHERE text_channels.channel_version < EXCLUDED.channel_version")
	})

	t.Run("delete channel", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().QueueExec(mocks.NewTag(1))
		applied, err := repository.NewCommunityProjectionRepository(script).DeleteTextChannel(ctx, testContainer, 5)
		require.NoError(t, err)
		assert.True(t, applied)
	})

	t.Run("membership load and upsert", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().
			QueueRow(mocks.NewRow(testCommunity.String(), testAuthorID.String(), "active", false,
				mutedUntil, []string{roleID.String()}, int64(3))).
			QueueRow(mocks.NewRow(int64(3)))
		repo := repository.NewCommunityProjectionRepository(script)

		member, err := repo.GetMembership(ctx, testCommunity, testAuthorID)
		require.NoError(t, err)
		require.NotNil(t, member)
		assert.True(t, member.IsMuted(testNow))
		assert.Equal(t, []uuid.UUID{roleID}, member.RoleIDs)

		applied, err := repo.UpsertMembership(ctx, *member)
		require.NoError(t, err)
		assert.True(t, applied)
	})

	t.Run("membership missing", func(t *testing.T) {
		t.Parallel()
		member, err := repository.NewCommunityProjectionRepository(mocks.NewScript().QueueRow(mocks.NoRow())).
			GetMembership(ctx, testCommunity, testAuthorID)
		require.NoError(t, err)
		assert.Nil(t, member)
	})

	t.Run("scope and mention filters", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().
			QueueRows(mocks.NewRows([]any{testCommunity.String()})).
			QueueRows(mocks.NewRows([]any{testAuthorID.String()}))
		repo := repository.NewCommunityProjectionRepository(script)

		communities, err := repo.ActiveCommunityIDs(ctx, testAuthorID)
		require.NoError(t, err)
		assert.Equal(t, []uuid.UUID{testCommunity}, communities)

		active, err := repo.FilterActiveMemberIDs(ctx, testCommunity, []uuid.UUID{testAuthorID})
		require.NoError(t, err)
		assert.Equal(t, []uuid.UUID{testAuthorID}, active)

		none, err := repo.FilterActiveMemberIDs(ctx, testCommunity, nil)
		require.NoError(t, err)
		assert.Nil(t, none)
	})

	t.Run("automod rule set", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().
			QueueRow(mocks.NewRow(testCommunity.String(), int64(2), true, "block", 5, 10, 3, 4, 2,
				true, "remove", "substring", []string{"bad"}, []string{roleID.String()})).
			QueueRow(mocks.NewRow(int64(2)))
		repo := repository.NewCommunityProjectionRepository(script)

		ruleSet, err := repo.GetAutomodRuleSet(ctx, testCommunity)
		require.NoError(t, err)
		require.NotNil(t, ruleSet)
		assert.Equal(t, "block", ruleSet.Spam.Action)
		assert.Equal(t, []string{"bad"}, ruleSet.BannedWords.Words)
		assert.Equal(t, []uuid.UUID{roleID}, ruleSet.ExemptRoleIDs)

		applied, err := repo.UpsertAutomodRuleSet(ctx, *ruleSet)
		require.NoError(t, err)
		assert.True(t, applied)
	})

	t.Run("automod missing", func(t *testing.T) {
		t.Parallel()
		ruleSet, err := repository.NewCommunityProjectionRepository(mocks.NewScript().QueueRow(mocks.NoRow())).
			GetAutomodRuleSet(ctx, testCommunity)
		require.NoError(t, err)
		assert.Nil(t, ruleSet)
	})
}

func TestConversationProjectionRepository(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conversationID := uuid.MustParse("66666666-6666-4666-8666-666666666666")
	name := "trip"

	t.Run("load conversation", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().
			QueueRow(mocks.NewRow(conversationID.String(), "group", name, int64(2))).
			QueueRows(mocks.NewRows([]any{testAuthorID.String()}))
		conversation, err := repository.NewConversationProjectionRepository(script).GetConversation(ctx, conversationID)
		require.NoError(t, err)
		require.NotNil(t, conversation)
		assert.Equal(t, "group", conversation.Kind)
		assert.True(t, conversation.IsParticipant(testAuthorID))
	})

	t.Run("conversation missing", func(t *testing.T) {
		t.Parallel()
		conversation, err := repository.NewConversationProjectionRepository(mocks.NewScript().QueueRow(mocks.NoRow())).
			GetConversation(ctx, conversationID)
		require.NoError(t, err)
		assert.Nil(t, conversation)
	})

	t.Run("upsert replaces participants", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().QueueRow(mocks.NewRow(int64(2)))
		applied, err := repository.NewConversationProjectionRepository(script).UpsertConversation(ctx, models.Conversation{
			ConversationID: conversationID, Kind: "group", Name: &name, ConversationVersion: 2,
			ParticipantIDs: []uuid.UUID{testAuthorID},
		})
		require.NoError(t, err)
		assert.True(t, applied)
		assert.Len(t, script.ExecCalls(), 2)
	})

	t.Run("stale upsert leaves participants untouched", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().QueueRow(mocks.NoRow())
		applied, err := repository.NewConversationProjectionRepository(script).UpsertConversation(ctx, models.Conversation{
			ConversationID: conversationID, Kind: "group", ConversationVersion: 1,
		})
		require.NoError(t, err)
		assert.False(t, applied)
		assert.Empty(t, script.ExecCalls())
	})

	t.Run("participant queries", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().
			QueueRow(mocks.NewRow(true)).
			QueueRows(mocks.NewRows([]any{testAuthorID.String()})).
			QueueRows(mocks.NewRows([]any{conversationID.String()}))
		repo := repository.NewConversationProjectionRepository(script)

		ok, err := repo.IsParticipant(ctx, conversationID, testAuthorID)
		require.NoError(t, err)
		assert.True(t, ok)

		filtered, err := repo.FilterParticipantIDs(ctx, conversationID, []uuid.UUID{testAuthorID})
		require.NoError(t, err)
		assert.Equal(t, []uuid.UUID{testAuthorID}, filtered)

		ids, err := repo.ConversationIDsForUser(ctx, testAuthorID)
		require.NoError(t, err)
		assert.Equal(t, []uuid.UUID{conversationID}, ids)

		empty, err := repo.FilterParticipantIDs(ctx, conversationID, nil)
		require.NoError(t, err)
		assert.Nil(t, empty)
	})
}

func TestAttachmentRepository(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	attachmentID := uuid.MustParse("77777777-7777-4777-8777-777777777777")
	key := "idem-key"

	row := func(status string) []any {
		return []any{attachmentID.String(), testAuthorID.String(), "cat.png", "image/png", int64(10), "key-1",
			status, nil, nil, nil, key, testNow, nil}
	}

	t.Run("create and idempotent retry", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().
			QueueRow(mocks.NewRow(row("pending_upload")...)).
			QueueRow(mocks.NoRow()).
			QueueRow(mocks.NewRow(row("pending_upload")...))
		repo := repository.NewAttachmentRepository(script)

		created, ok, err := repo.Create(ctx, &models.Attachment{
			ID: attachmentID, UploaderUserID: testAuthorID, Filename: "cat.png", DeclaredContentType: "image/png",
			DeclaredSizeBytes: 10, ObjectKey: "key-1", IdempotencyKey: &key, CreatedAt: testNow,
		})
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, attachmentID, created.ID)

		reused, ok, err := repo.Create(ctx, &models.Attachment{
			ID: uuid.New(), UploaderUserID: testAuthorID, IdempotencyKey: &key, CreatedAt: testNow,
		})
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, attachmentID, reused.ID)
	})

	t.Run("get and delete", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().
			QueueRow(mocks.NewRow(row("attached")...)).
			QueueRow(mocks.NoRow()).
			QueueExec(mocks.NewTag(1))
		repo := repository.NewAttachmentRepository(script)

		found, err := repo.Get(ctx, attachmentID)
		require.NoError(t, err)
		require.NotNil(t, found)
		assert.Equal(t, models.AttachmentAttached, found.Status)

		missing, err := repo.Get(ctx, uuid.New())
		require.NoError(t, err)
		assert.Nil(t, missing)

		require.NoError(t, repo.Delete(ctx, attachmentID))
	})

	t.Run("list pending older than cutoff", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().QueueRows(mocks.NewRows(row("pending_upload")))
		stale, err := repository.NewAttachmentRepository(script).ListPendingOlderThan(ctx, testNow, 10)
		require.NoError(t, err)
		require.Len(t, stale, 1)
		assert.Equal(t, []any{testNow, 10}, script.Calls()[0].Args)
	})
}

func TestOutboxRepository(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	envelope, err := models.NewEnvelope(models.EnvelopeInput{
		EventType: models.EventMessageChanged, TraceID: "0123456789abcdef0123456789abcdef",
	}, testNow)
	require.NoError(t, err)
	payload := json.RawMessage(`{"message_id":"x"}`)

	t.Run("append joins the open transaction", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript()
		txManager := repository.NewTxManager(script)
		outbox := repository.NewOutboxRepository(script)

		require.NoError(t, txManager.InTx(ctx, func(txCtx context.Context) error {
			return outbox.Append(txCtx, models.OutboxEntryView{
				Topic: models.TopicMessageEvents, MessageKey: testContainer.String(),
				Envelope: envelope, Payload: payload,
			})
		}))
		calls := script.ExecCalls()
		require.Len(t, calls, 1)
		assert.Contains(t, calls[0].SQL, "INSERT INTO event_outbox")
		assert.Equal(t, models.TopicMessageEvents, calls[0].Args[0])
	})

	t.Run("list unpublished decodes jsonb columns", func(t *testing.T) {
		t.Parallel()
		envelopeJSON, err := json.Marshal(envelope)
		require.NoError(t, err)
		script := mocks.NewScript().QueueRows(mocks.NewRows([]any{
			int64(1), models.TopicMessageEvents, testContainer.String(), envelopeJSON, []byte(payload), 0, nil,
		}))
		entries, err := repository.NewOutboxRepository(script).ListUnpublished(ctx, 10)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, envelope.EventID, entries[0].Envelope.EventID)
		assert.JSONEq(t, string(payload), string(entries[0].Payload))
	})

	t.Run("publish bookkeeping", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().QueueExec(mocks.NewTag(1)).QueueExec(mocks.NewTag(1))
		outbox := repository.NewOutboxRepository(script)
		require.NoError(t, outbox.MarkPublished(ctx, 1, testNow))
		require.NoError(t, outbox.RecordAttempt(ctx, 2))

		countScript := mocks.NewScript().QueueRow(mocks.NewRow(int64(4)))
		count, err := repository.NewOutboxRepository(countScript).CountUnpublished(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(4), count)
	})
}

func TestProcessedEventRepository(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	eventID := uuid.MustParse("88888888-8888-4888-8888-888888888888")

	script := mocks.NewScript().
		QueueRow(mocks.NewRow(true)).
		QueueExec(mocks.NewTag(1)).
		QueueExec(mocks.NewTag(0)).
		QueueExec(mocks.NewTag(3))
	repo := repository.NewProcessedEventRepository(script)

	processed, err := repo.IsProcessed(ctx, eventID)
	require.NoError(t, err)
	assert.True(t, processed)

	inserted, err := repo.MarkProcessed(ctx, eventID, models.EventCommunityMemberChanged)
	require.NoError(t, err)
	assert.True(t, inserted)

	inserted, err = repo.MarkProcessed(ctx, eventID, models.EventCommunityMemberChanged)
	require.NoError(t, err)
	assert.False(t, inserted)

	pruned, err := repo.PruneOlderThan(ctx, testNow)
	require.NoError(t, err)
	assert.Equal(t, int64(3), pruned)
}

func TestRateRepository(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	script := mocks.NewScript().
		QueueRow(mocks.NewRow(testCommunity.String(), testAuthorID.String(), testNow, 4, []string{"abc"})).
		QueueExec(mocks.NewTag(1))
	repo := repository.NewRateRepository(script)

	rate, err := repo.GetRate(ctx, testCommunity, testAuthorID)
	require.NoError(t, err)
	require.NotNil(t, rate)
	assert.Equal(t, 4, rate.MessageCount)
	assert.Equal(t, []string{"abc"}, rate.RecentFingerprints)
	require.NoError(t, repo.UpsertRate(ctx, *rate))

	empty, err := repository.NewRateRepository(mocks.NewScript().QueueRow(mocks.NoRow())).
		GetRate(ctx, testCommunity, testAuthorID)
	require.NoError(t, err)
	assert.Nil(t, empty)
}

func TestTxManagerBehaviour(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("commits on success", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript()
		require.NoError(t, repository.NewTxManager(script).InTx(ctx, func(context.Context) error { return nil }))
	})

	t.Run("rolls back on error", func(t *testing.T) {
		t.Parallel()
		manager := repository.NewTxManager(mocks.NewScript())
		sentinel := errors.New("boom")
		require.ErrorIs(t, manager.InTx(ctx, func(context.Context) error { return sentinel }), sentinel)
	})

	t.Run("joins an open transaction", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript()
		manager := repository.NewTxManager(script)
		require.NoError(t, manager.InTx(ctx, func(outer context.Context) error {
			return manager.InTx(outer, func(inner context.Context) error {
				assert.NotNil(t, repository.TxFromContext(inner))
				return nil
			})
		}))
		assert.Equal(t, 1, countBeginCalls(script.Calls()))
	})

	t.Run("begin failure propagates", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().SetBeginError(errors.New("no pool"))
		require.Error(t, repository.NewTxManager(script).InTx(ctx, func(context.Context) error { return nil }))
	})

	t.Run("commit failure propagates", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().SetCommitError(errors.New("gone"))
		err := repository.NewTxManager(script).InTx(ctx, func(context.Context) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "committing transaction")
	})

	t.Run("nil function rejected", func(t *testing.T) {
		t.Parallel()
		require.Error(t, repository.NewTxManager(mocks.NewScript()).InTx(ctx, nil))
	})
}

func countBeginCalls(calls []mocks.Call) int {
	count := 0
	for _, call := range calls {
		if call.SQL == "BEGIN" {
			count++
		}
	}
	return count
}
