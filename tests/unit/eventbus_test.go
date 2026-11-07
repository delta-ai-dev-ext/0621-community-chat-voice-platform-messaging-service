package unit

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"messaging_service/internal/eventbus"
	"messaging_service/internal/models"
	"messaging_service/internal/observability"
	"messaging_service/tests/mocks"
)

func outboxEntry(id int64, first models.Envelope, payload any) models.OutboxEntry {
	body, _ := json.Marshal(payload)
	return models.OutboxEntry{
		ID: id, Topic: models.TopicMessageEvents, MessageKey: testContainer.String(),
		Envelope: first, Payload: body,
	}
}

func TestRelayPublishesAndMarksEntries(t *testing.T) {
	t.Parallel()
	relay, outbox, publisher := newRelayHarness(t)
	envelope, err := models.NewEnvelope(models.EnvelopeInput{EventType: models.EventMessageChanged}, testNow)
	require.NoError(t, err)
	entry := outboxEntry(7, envelope, map[string]string{"message_id": testMessageID.String()})

	outbox.On("ListUnpublished", matchContext(), 10).Return([]models.OutboxEntry{entry}, nil).Once()
	publisher.On("Publish", matchContext(), models.TopicMessageEvents, testContainer.String(),
		mock.MatchedBy(func(event models.Event) bool {
			return event.Envelope.EventID == envelope.EventID && len(event.Payload) > 0
		})).Return(nil).Once()
	outbox.On("MarkPublished", matchContext(), int64(7), testNow).Return(nil).Once()
	outbox.On("CountUnpublished", matchContext()).Return(int64(0), nil).Once()

	require.NoError(t, relay.PublishBatch(contextBackground()))
}

func TestRelayStopsAtFirstFailureToPreserveOrder(t *testing.T) {
	t.Parallel()
	relay, outbox, publisher := newRelayHarness(t)
	envelope, err := models.NewEnvelope(models.EnvelopeInput{EventType: models.EventMessageChanged}, testNow)
	require.NoError(t, err)
	first := outboxEntry(1, envelope, map[string]string{"message_id": "a"})
	second := outboxEntry(2, envelope, map[string]string{"message_id": "b"})

	outbox.On("ListUnpublished", matchContext(), 10).Return([]models.OutboxEntry{first, second}, nil).Once()
	publisher.On("Publish", matchContext(), models.TopicMessageEvents, testContainer.String(), mock.Anything).
		Return(errors.New("broker unavailable")).Once()
	outbox.On("RecordAttempt", matchContext(), int64(1)).Return(nil).Once()

	require.NoError(t, relay.PublishBatch(contextBackground()))
	// The second entry is never attempted in this batch; ordering is preserved.
	outbox.AssertNotCalled(t, "MarkPublished", matchContext(), mock.Anything, mock.Anything)
}

func TestRelayReportsDepth(t *testing.T) {
	t.Parallel()
	relay, outbox, _ := newRelayHarness(t)
	outbox.On("ListUnpublished", matchContext(), 10).Return(nil, nil).Once()
	outbox.On("CountUnpublished", matchContext()).Return(int64(3), nil).Once()

	require.NoError(t, relay.PublishBatch(contextBackground()))
}

func TestRelayRunStopsOnContextCancel(t *testing.T) {
	t.Parallel()
	relay, outbox, _ := newRelayHarness(t)
	outbox.On("ListUnpublished", matchContext(), 10).Return(nil, nil).Maybe()
	outbox.On("CountUnpublished", matchContext()).Return(int64(0), nil).Maybe()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		relay.Run(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("relay did not stop after cancellation")
	}
}

// processorHarness wires the projection processor against mocks.
type processorHarness struct {
	community     *mocks.CommunityProjectionRepository
	conversations *mocks.ConversationProjectionRepository
	processed     *mocks.ProcessedEventRepository
	tx            *mocks.TransactionManager
	processor     *eventbus.Processor
}

func newProcessorHarness(t *testing.T) *processorHarness {
	t.Helper()
	h := &processorHarness{
		community:     &mocks.CommunityProjectionRepository{},
		conversations: &mocks.ConversationProjectionRepository{},
		processed:     &mocks.ProcessedEventRepository{},
		tx:            &mocks.TransactionManager{},
	}
	h.processor = eventbus.NewProcessor(eventbus.ProcessorDeps{
		Community:     h.community,
		Conversations: h.conversations,
		Processed:     h.processed,
		Tx:            h.tx,
		Metrics:       observability.NewMetrics(nil),
		Logger:        zap.NewNop(),
	})
	return h
}

// encodeEvent renders an event the way the producer writes it.
func encodeEvent(t *testing.T, eventType string, payload any) []byte {
	t.Helper()
	event := validEvent(t, eventType, payload)
	body, err := json.Marshal(event)
	require.NoError(t, err)
	return body
}

func TestProcessorAppliesMembershipAndDedupes(t *testing.T) {
	t.Parallel()
	h := newProcessorHarness(t)
	payload := models.CommunityMemberChangedPayload{
		CommunityID: testCommunity, UserID: testAuthorID, Change: "joined",
		Status: models.MemberStatusActive, MemberVersion: 5,
	}
	value := encodeEvent(t, models.EventCommunityMemberChanged, payload)
	eventID := uuid.MustParse(eventIDFrom(t, value))

	h.processed.On("IsProcessed", matchContext(), eventID).Return(false, nil).Once()
	h.tx.On("InTx", matchContext()).Return(nil).Once()
	h.community.On("UpsertMembership", matchContext(), mock.MatchedBy(func(member models.CommunityMember) bool {
		return member.CommunityID == testCommunity && member.UserID == testAuthorID &&
			member.MemberVersion == 5 && member.Status == models.MemberStatusActive
	})).Return(true, nil).Once()
	h.processed.On("MarkProcessed", matchContext(), eventID, models.EventCommunityMemberChanged).
		Return(true, nil).Once()

	require.NoError(t, h.processor.Handle(contextBackground(), models.TopicCommunityEvents, value))
}

func TestProcessorSkipsReplayedEvent(t *testing.T) {
	t.Parallel()
	h := newProcessorHarness(t)
	payload := models.CommunityMemberChangedPayload{
		CommunityID: testCommunity, UserID: testAuthorID, Change: "joined",
		Status: models.MemberStatusActive, MemberVersion: 5,
	}
	value := encodeEvent(t, models.EventCommunityMemberChanged, payload)
	eventID := uuid.MustParse(eventIDFrom(t, value))

	// The dedupe table already holds this event id, so nothing is applied again.
	h.processed.On("IsProcessed", matchContext(), eventID).Return(true, nil).Once()

	require.NoError(t, h.processor.Handle(contextBackground(), models.TopicCommunityEvents, value))
	h.community.AssertNotCalled(t, "UpsertMembership", matchContext(), mock.Anything)
	h.processed.AssertNotCalled(t, "MarkProcessed", matchContext(), mock.Anything, mock.Anything)
}

func TestProcessorChannelAndAutomodEvents(t *testing.T) {
	t.Parallel()

	t.Run("text channel upsert", func(t *testing.T) {
		t.Parallel()
		h := newProcessorHarness(t)
		payload := models.CommunityChannelChangedPayload{
			CommunityID: testCommunity, ChannelID: testContainer, Change: "created", Kind: "text",
			Name: "general", ChannelVersion: 2,
		}
		value := encodeEvent(t, models.EventCommunityChannelChanged, payload)
		eventID := uuid.MustParse(eventIDFrom(t, value))

		h.processed.On("IsProcessed", matchContext(), eventID).Return(false, nil).Once()
		h.tx.On("InTx", matchContext()).Return(nil).Once()
		h.community.On("UpsertTextChannel", matchContext(), mock.MatchedBy(func(channel models.TextChannel) bool {
			return channel.ChannelID == testContainer && channel.ChannelVersion == 2
		})).Return(true, nil).Once()
		h.processed.On("MarkProcessed", matchContext(), eventID, models.EventCommunityChannelChanged).
			Return(true, nil).Once()

		require.NoError(t, h.processor.Handle(contextBackground(), models.TopicCommunityEvents, value))
	})

	t.Run("voice channels are not projected", func(t *testing.T) {
		t.Parallel()
		h := newProcessorHarness(t)
		payload := models.CommunityChannelChangedPayload{
			CommunityID: testCommunity, ChannelID: testContainer, Change: "created", Kind: "voice",
			Name: "General Voice", ChannelVersion: 2,
		}
		value := encodeEvent(t, models.EventCommunityChannelChanged, payload)
		eventID := uuid.MustParse(eventIDFrom(t, value))

		h.processed.On("IsProcessed", matchContext(), eventID).Return(false, nil).Once()
		h.tx.On("InTx", matchContext()).Return(nil).Once()
		h.processed.On("MarkProcessed", matchContext(), eventID, models.EventCommunityChannelChanged).
			Return(true, nil).Once()

		require.NoError(t, h.processor.Handle(contextBackground(), models.TopicCommunityEvents, value))
		h.community.AssertNotCalled(t, "UpsertTextChannel", matchContext(), mock.Anything)
	})

	t.Run("channel deletion", func(t *testing.T) {
		t.Parallel()
		h := newProcessorHarness(t)
		payload := models.CommunityChannelChangedPayload{
			CommunityID: testCommunity, ChannelID: testContainer, Change: "deleted", Kind: "text",
			Name: "general", ChannelVersion: 3,
		}
		value := encodeEvent(t, models.EventCommunityChannelChanged, payload)
		eventID := uuid.MustParse(eventIDFrom(t, value))

		h.processed.On("IsProcessed", matchContext(), eventID).Return(false, nil).Once()
		h.tx.On("InTx", matchContext()).Return(nil).Once()
		h.community.On("DeleteTextChannel", matchContext(), testContainer, int64(3)).Return(true, nil).Once()
		h.processed.On("MarkProcessed", matchContext(), eventID, models.EventCommunityChannelChanged).
			Return(true, nil).Once()

		require.NoError(t, h.processor.Handle(contextBackground(), models.TopicCommunityEvents, value))
	})

	t.Run("automod rules", func(t *testing.T) {
		t.Parallel()
		h := newProcessorHarness(t)
		payload := models.AutomodRulesChangedPayload{
			CommunityID: testCommunity, RuleSetVersion: 4,
			Spam: models.SpamRules{Action: "block", MaxMessagesPerWindow: 5, WindowSeconds: 10,
				MaxDuplicateMessages: 2, MaxMentionsPerMessage: 3, MaxLinksPerMessage: 1},
			BannedWords: models.BannedWordsRules{Action: "block", MatchMode: "substring", Words: []string{"spam"}},
		}
		value := encodeEvent(t, models.EventAutomodRulesChanged, payload)
		eventID := uuid.MustParse(eventIDFrom(t, value))

		h.processed.On("IsProcessed", matchContext(), eventID).Return(false, nil).Once()
		h.tx.On("InTx", matchContext()).Return(nil).Once()
		h.community.On("UpsertAutomodRuleSet", matchContext(), mock.MatchedBy(func(ruleSet models.AutomodRuleSet) bool {
			return ruleSet.RuleSetVersion == 4 && len(ruleSet.BannedWords.Words) == 1
		})).Return(true, nil).Once()
		h.processed.On("MarkProcessed", matchContext(), eventID, models.EventAutomodRulesChanged).
			Return(true, nil).Once()

		require.NoError(t, h.processor.Handle(contextBackground(), models.TopicModerationEvents, value))
	})
}

func TestProcessorConversationEvent(t *testing.T) {
	t.Parallel()
	h := newProcessorHarness(t)
	conversationID := uuid.MustParse("66666666-6666-4666-8666-666666666666")
	payload := models.ConversationChangedPayload{
		ConversationID: conversationID, Kind: models.ConversationKindGroup, Change: "participants_changed",
		ParticipantUserIDs: []uuid.UUID{testAuthorID}, ConversationVersion: 3,
	}
	value := encodeEvent(t, models.EventConversationChanged, payload)
	eventID := uuid.MustParse(eventIDFrom(t, value))

	h.processed.On("IsProcessed", matchContext(), eventID).Return(false, nil).Once()
	h.tx.On("InTx", matchContext()).Return(nil).Once()
	h.conversations.On("UpsertConversation", matchContext(), mock.MatchedBy(func(conversation models.Conversation) bool {
		return conversation.ConversationID == conversationID && conversation.ConversationVersion == 3 &&
			len(conversation.ParticipantIDs) == 1
	})).Return(true, nil).Once()
	h.processed.On("MarkProcessed", matchContext(), eventID, models.EventConversationChanged).
		Return(true, nil).Once()

	require.NoError(t, h.processor.Handle(contextBackground(), models.TopicSocialEvents, value))
}

func TestProcessorDeadLettersAfterRepeatedFailures(t *testing.T) {
	t.Parallel()
	h := newProcessorHarness(t)
	payload := models.CommunityMemberChangedPayload{
		CommunityID: testCommunity, UserID: testAuthorID, Change: "joined",
		Status: models.MemberStatusActive, MemberVersion: 5,
	}
	value := encodeEvent(t, models.EventCommunityMemberChanged, payload)
	eventID := uuid.MustParse(eventIDFrom(t, value))

	h.processed.On("IsProcessed", matchContext(), eventID).Return(false, nil).Times(3)
	h.tx.On("InTx", matchContext()).Return(nil).Times(3)
	h.community.On("UpsertMembership", matchContext(), mock.Anything).Return(false, errors.New("db down")).Times(3)

	// The first two attempts surface an error so the offset is not committed.
	require.Error(t, h.processor.Handle(contextBackground(), models.TopicCommunityEvents, value))
	require.Error(t, h.processor.Handle(contextBackground(), models.TopicCommunityEvents, value))
	// The third exhausts the retry budget and dead-letters, allowing the commit.
	require.NoError(t, h.processor.Handle(contextBackground(), models.TopicCommunityEvents, value))
}

func TestProcessorRejectsMalformedEvents(t *testing.T) {
	t.Parallel()
	h := newProcessorHarness(t)

	require.NoError(t, h.processor.Handle(contextBackground(), models.TopicCommunityEvents, []byte("not json")))

	invalid := models.Event{Envelope: models.Envelope{EventType: "Bad"}}
	body, err := json.Marshal(invalid)
	require.NoError(t, err)
	require.NoError(t, h.processor.Handle(contextBackground(), models.TopicCommunityEvents, body))
}

// eventIDFrom extracts the envelope event id from an encoded event.
func eventIDFrom(t *testing.T, value []byte) string {
	t.Helper()
	var event models.Event
	require.NoError(t, json.Unmarshal(value, &event))
	return event.Envelope.EventID.String()
}
