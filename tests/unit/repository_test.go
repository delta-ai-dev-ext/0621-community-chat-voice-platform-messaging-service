package unit

import (
	"context"
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

var (
	testNow       = time.Date(2026, 1, 14, 12, 0, 0, 0, time.UTC)
	testMessageID = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	testAuthorID  = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	testContainer = uuid.MustParse("33333333-3333-4333-8333-333333333333")
	testCommunity = uuid.MustParse("44444444-4444-4444-8444-444444444444")
)

// messageRow renders the 13 columns of repository.messageColumns.
func messageRow(content *string, version int64) []any {
	var community any
	if content == nil {
		community = testCommunity.String()
	}
	return []any{
		testMessageID.String(), "channel", testContainer.String(), community, testAuthorID.String(),
		nil, content, "client-1", nil, nil, nil, version, testNow,
	}
}

func TestMessageRepositoryInsert(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	content := "hello"
	clientID := "client-1"

	t.Run("creates a message", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().QueueRow(mocks.NewRow(messageRow(&content, 1)...))
		repo := repository.NewMessageRepository(script)
		stored, created, err := repo.Insert(ctx, &models.Message{
			ID: testMessageID, ContainerType: models.ContainerChannel, ContainerID: testContainer,
			AuthorUserID: testAuthorID, Content: &content, ClientMessageID: &clientID,
			MessageVersion: 1, CreatedAt: testNow,
		})
		require.NoError(t, err)
		assert.True(t, created)
		assert.Equal(t, testMessageID, stored.ID)
		assert.Contains(t, script.Calls()[0].SQL, "ON CONFLICT (container_id, author_user_id, client_message_id)")
	})

	t.Run("duplicate returns the original row", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().
			QueueRow(mocks.NoRow()).
			QueueRow(mocks.NewRow(messageRow(&content, 1)...))
		repo := repository.NewMessageRepository(script)
		stored, created, err := repo.Insert(ctx, &models.Message{
			ID: uuid.New(), ContainerType: models.ContainerChannel, ContainerID: testContainer,
			AuthorUserID: testAuthorID, ClientMessageID: &clientID, MessageVersion: 1, CreatedAt: testNow,
		})
		require.NoError(t, err)
		assert.False(t, created)
		assert.Equal(t, testMessageID, stored.ID)
		assert.Len(t, script.Calls(), 2)
	})

	t.Run("insert failure surfaces", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().QueueError(errors.New("boom"))
		repo := repository.NewMessageRepository(script)
		_, _, err := repo.Insert(ctx, &models.Message{ContainerType: models.ContainerChannel})
		require.Error(t, err)
	})
}

func TestMessageRepositoryGet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	content := "hello"

	t.Run("hydrates related rows", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().
			QueueRow(mocks.NewRow(messageRow(&content, 2)...)).
			QueueRows(mocks.NewRows([]any{testMessageID.String(), "🎉", 3})).
			QueueRows(mocks.NewRows([]any{testMessageID.String(), testAuthorID.String()})).
			QueueRows(mocks.NewRows([]any{
				uuid.New().String(), testAuthorID.String(), "cat.png", "image/png", int64(10), "key-1", "attached",
				nil, nil, testMessageID.String(), nil, testNow, nil,
			}))
		repo := repository.NewMessageRepository(script)
		aggregate, err := repo.Get(ctx, testMessageID)
		require.NoError(t, err)
		require.NotNil(t, aggregate)
		assert.Equal(t, testMessageID, aggregate.Message.ID)
		assert.Equal(t, []models.ReactionSummary{{Emoji: "🎉", Count: 3}}, aggregate.Reactions)
		assert.Equal(t, []uuid.UUID{testAuthorID}, aggregate.MentionedUserIDs)
		require.Len(t, aggregate.Attachments, 1)
		assert.Equal(t, "key-1", aggregate.Attachments[0].ObjectKey)
	})

	t.Run("missing message yields nil", func(t *testing.T) {
		t.Parallel()
		repo := repository.NewMessageRepository(mocks.NewScript().QueueRow(mocks.NoRow()))
		aggregate, err := repo.Get(ctx, testMessageID)
		require.NoError(t, err)
		assert.Nil(t, aggregate)
	})
}

func TestMessageRepositoryPageModes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	content := "hello"
	cursor := models.NewCursor(testNow.Add(-time.Minute), testMessageID)

	cases := []struct {
		name          string
		query         models.HistoryQuery
		wantOrdering  string
		wantArgsCount int
	}{
		{
			name:          "newest first",
			query:         models.HistoryQuery{ContainerType: models.ContainerChannel, ContainerID: testContainer, Limit: 3},
			wantOrdering:  "ORDER BY created_at DESC, id DESC",
			wantArgsCount: 3,
		},
		{
			name: "before scrollback",
			query: models.HistoryQuery{ContainerType: models.ContainerChannel, ContainerID: testContainer,
				Limit: 3, Before: &cursor},
			wantOrdering:  "AND (created_at, id) < ($3, $4)",
			wantArgsCount: 5,
		},
		{
			name: "after catch-up",
			query: models.HistoryQuery{ContainerType: models.ContainerChannel, ContainerID: testContainer,
				Limit: 3, After: &cursor},
			wantOrdering:  "ORDER BY created_at ASC, id ASC",
			wantArgsCount: 5,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			script := mocks.NewScript().
				QueueRows(mocks.NewRows(messageRow(&content, 1))).
				QueueRows(mocks.NewRows()).
				QueueRows(mocks.NewRows()).
				QueueRows(mocks.NewRows())
			repo := repository.NewMessageRepository(script)
			aggregates, err := repo.Page(ctx, tc.query)
			require.NoError(t, err)
			require.Len(t, aggregates, 1)
			calls := script.Calls()
			require.NotEmpty(t, calls)
			assert.Contains(t, calls[0].SQL, tc.wantOrdering)
			assert.Len(t, calls[0].Args, tc.wantArgsCount)
		})
	}
}

func TestMessageRepositoryPageEmptySkipsRelatedQueries(t *testing.T) {
	t.Parallel()
	script := mocks.NewScript().QueueRows(mocks.NewRows())
	repo := repository.NewMessageRepository(script)
	aggregates, err := repo.Page(context.Background(), models.HistoryQuery{
		ContainerType: models.ContainerConversation, ContainerID: testContainer, Limit: 5,
	})
	require.NoError(t, err)
	assert.Empty(t, aggregates)
	assert.Len(t, script.Calls(), 1)
}

func TestMessageRepositoryRepliesAndCount(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	content := "reply"
	cursor := models.NewCursor(testNow, testMessageID)

	script := mocks.NewScript().
		QueueRows(mocks.NewRows(messageRow(&content, 1))).
		QueueRows(mocks.NewRows()).
		QueueRows(mocks.NewRows()).
		QueueRows(mocks.NewRows()).
		QueueRow(mocks.NewRow(7))

	repo := repository.NewMessageRepository(script)
	replies, err := repo.Replies(ctx, testMessageID, &cursor, 10)
	require.NoError(t, err)
	require.Len(t, replies, 1)
	assert.Contains(t, script.Calls()[0].SQL, "AND (created_at, id) > ($2, $3)")

	count, err := repo.ReplyCount(ctx, testMessageID)
	require.NoError(t, err)
	assert.Equal(t, 7, count)
}

func TestMessageRepositoryMutations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	content := "edited"

	t.Run("update content", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().QueueRow(mocks.NewRow(messageRow(&content, 2)...))
		repo := repository.NewMessageRepository(script)
		updated, err := repo.UpdateContent(ctx, testMessageID, content, testNow)
		require.NoError(t, err)
		assert.Equal(t, int64(2), updated.MessageVersion)
		assert.Contains(t, script.Calls()[0].SQL, "message_version = message_version + 1")
	})

	t.Run("soft delete", func(t *testing.T) {
		t.Parallel()
		row := messageRow(nil, 3)
		row[9] = testNow
		row[10] = "author"
		script := mocks.NewScript().QueueRow(mocks.NewRow(row...))
		repo := repository.NewMessageRepository(script)
		deleted, err := repo.SoftDelete(ctx, testMessageID, models.DeletionReasonAuthor, testNow)
		require.NoError(t, err)
		require.NotNil(t, deleted.DeletionReason)
		assert.Equal(t, models.DeletionReasonAuthor, *deleted.DeletionReason)
	})

	t.Run("bump version", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().QueueRow(mocks.NewRow(int64(9)))
		repo := repository.NewMessageRepository(script)
		version, err := repo.BumpVersion(ctx, testMessageID)
		require.NoError(t, err)
		assert.Equal(t, int64(9), version)
	})

	t.Run("replace mentions", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript()
		repo := repository.NewMessageRepository(script)
		require.NoError(t, repo.ReplaceMentions(ctx, testMessageID, []uuid.UUID{testAuthorID}))
		assert.Len(t, script.ExecCalls(), 2)

		empty := mocks.NewScript()
		require.NoError(t, repository.NewMessageRepository(empty).ReplaceMentions(ctx, testMessageID, nil))
		assert.Len(t, empty.ExecCalls(), 1)
	})

	t.Run("bind attachments", func(t *testing.T) {
		t.Parallel()
		attachment := models.Attachment{ID: uuid.New(), DeclaredContentType: "image/png", DeclaredSizeBytes: 5}

		ok := mocks.NewScript().QueueExec(mocks.NewTag(1))
		require.NoError(t, repository.NewMessageRepository(ok).
			BindAttachments(ctx, testMessageID, testAuthorID, testNow, []models.Attachment{attachment}))

		mismatch := mocks.NewScript().QueueExec(mocks.NewTag(0))
		require.Error(t, repository.NewMessageRepository(mismatch).
			BindAttachments(ctx, testMessageID, testAuthorID, testNow, []models.Attachment{attachment}))

		none := mocks.NewScript()
		require.NoError(t, repository.NewMessageRepository(none).BindAttachments(ctx, testMessageID, testAuthorID, testNow, nil))
	})

	t.Run("detach attachments", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().QueueRows(mocks.NewRows([]any{"key-1"}, []any{"key-2"}))
		keys, err := repository.NewMessageRepository(script).DetachAttachments(ctx, testMessageID)
		require.NoError(t, err)
		assert.Equal(t, []string{"key-1", "key-2"}, keys)
	})
}

func TestMessageRepositoryReactions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("add is idempotent", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().QueueExec(mocks.NewTag(1)).QueueExec(mocks.NewTag(0))
		repo := repository.NewMessageRepository(script)
		added, err := repo.AddReaction(ctx, testMessageID, testAuthorID, "🎉", testNow)
		require.NoError(t, err)
		assert.True(t, added)
		added, err = repo.AddReaction(ctx, testMessageID, testAuthorID, "🎉", testNow)
		require.NoError(t, err)
		assert.False(t, added)
	})

	t.Run("remove is idempotent", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().QueueExec(mocks.NewTag(0))
		removed, err := repository.NewMessageRepository(script).
			RemoveReaction(ctx, testMessageID, testAuthorID, "🎉")
		require.NoError(t, err)
		assert.False(t, removed)
	})

	t.Run("summary is ordered", func(t *testing.T) {
		t.Parallel()
		script := mocks.NewScript().QueueRows(mocks.NewRows(
			[]any{"🎉", 2},
			[]any{"👍", 1},
		))
		summary, err := repository.NewMessageRepository(script).ReactionSummary(ctx, testMessageID)
		require.NoError(t, err)
		assert.Equal(t, []models.ReactionSummary{{Emoji: "🎉", Count: 2}, {Emoji: "👍", Count: 1}}, summary)
	})
}
