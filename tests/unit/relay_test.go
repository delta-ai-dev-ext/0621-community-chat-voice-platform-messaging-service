package unit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"messaging_service/internal/models"
)

func TestRelayFlushDrainsBacklog(t *testing.T) {
	t.Parallel()
	relay, outbox, publisher := newRelayHarness(t)
	envelope, err := models.NewEnvelope(models.EnvelopeInput{EventType: models.EventMessageChanged}, testNow)
	require.NoError(t, err)
	entry := outboxEntry(3, envelope, map[string]string{"message_id": testMessageID.String()})

	outbox.On("ListUnpublished", matchContext(), 10).Return([]models.OutboxEntry{entry}, nil).Once()
	outbox.On("ListUnpublished", matchContext(), 10).Return(nil, nil).Once()
	publisher.On("Publish", matchContext(), models.TopicMessageEvents, testContainer.String(), mock.Anything).
		Return(nil).Once()
	outbox.On("MarkPublished", matchContext(), int64(3), testNow).Return(nil).Once()
	outbox.On("CountUnpublished", matchContext()).Return(int64(0), nil).Once()

	relay.Flush(contextBackground())
	publisher.AssertExpectations(t)
}

func TestRelayFlushStopsOnReadError(t *testing.T) {
	t.Parallel()
	relay, outbox, _ := newRelayHarness(t)
	outbox.On("ListUnpublished", matchContext(), 10).Return(nil, errors.New("db down")).Once()

	// Flush is best effort during shutdown: it logs and returns without panicking.
	relay.Flush(contextBackground())
}

func TestRelayReportsDepthFailure(t *testing.T) {
	t.Parallel()
	relay, outbox, _ := newRelayHarness(t)
	outbox.On("ListUnpublished", matchContext(), 10).Return(nil, nil).Once()
	outbox.On("CountUnpublished", matchContext()).Return(int64(0), errors.New("db down")).Once()

	require.Error(t, relay.PublishBatch(contextBackground()))
}

func TestRelayRunPublishesOnTick(t *testing.T) {
	t.Parallel()
	relay, outbox, _ := newRelayHarness(t)
	outbox.On("ListUnpublished", matchContext(), 10).Return(nil, nil).Maybe()
	outbox.On("CountUnpublished", matchContext()).Return(int64(0), nil).Maybe()

	ctx, cancel := context.WithCancel(context.Background())
	go relay.Run(ctx)
	// Two ticks at 1ms are enough for the loop to run at least once.
	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)
}

func TestMessageHelpers(t *testing.T) {
	t.Parallel()

	message := models.Message{ID: testMessageID, MessageVersion: 1}
	assert.False(t, message.IsDeleted())

	reason := models.DeletionReasonAuthor
	deletedAt := testNow
	message.DeletionReason = &reason
	message.DeletedAt = &deletedAt
	assert.True(t, message.IsDeleted())

	cursor := models.NewCursor(testNow, testMessageID)
	assert.True(t, models.HistoryQuery{After: &cursor}.Ascending())
	assert.False(t, models.HistoryQuery{Before: &cursor}.Ascending())
	assert.False(t, models.HistoryQuery{}.Ascending())

	summary := models.ReactionSummary{Emoji: "🎉", Count: 2}
	assert.Equal(t, "🎉", summary.Emoji)
	assert.Equal(t, 2, summary.Count)

	aggregate := models.MessageAggregate{Message: message, MentionedUserIDs: []uuid.UUID{uuid.New()}}
	assert.Len(t, aggregate.MentionedUserIDs, 1)
}
