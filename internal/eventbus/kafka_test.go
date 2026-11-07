package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"messaging_service/internal/models"
	"messaging_service/internal/observability"
	"messaging_service/tests/mocks"
)

// fakeSession implements sarama.ConsumerGroupSession for consume-loop tests.
type fakeSession struct {
	ctx    context.Context
	marked []*sarama.ConsumerMessage
}

func (s *fakeSession) Claims() map[string][]int32               { return nil }
func (s *fakeSession) MemberID() string                         { return "member-1" }
func (s *fakeSession) GenerationID() int32                      { return 1 }
func (s *fakeSession) MarkOffset(string, int32, int64, string)  {}
func (s *fakeSession) Commit()                                  {}
func (s *fakeSession) ResetOffset(string, int32, int64, string) {}
func (s *fakeSession) MarkMessage(message *sarama.ConsumerMessage, _ string) {
	s.marked = append(s.marked, message)
}
func (s *fakeSession) Context() context.Context { return s.ctx }

// fakeClaim implements sarama.ConsumerGroupClaim over a fixed channel.
type fakeClaim struct {
	messages <-chan *sarama.ConsumerMessage
}

func (c *fakeClaim) Topic() string                            { return models.TopicCommunityEvents }
func (c *fakeClaim) Partition() int32                         { return 0 }
func (c *fakeClaim) InitialOffset() int64                     { return 0 }
func (c *fakeClaim) HighWaterMarkOffset() int64               { return 1 }
func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

func newTestProcessor(t *testing.T, processed *mocks.ProcessedEventRepository) *Processor {
	t.Helper()
	return NewProcessor(ProcessorDeps{
		Community:     &mocks.CommunityProjectionRepository{},
		Conversations: &mocks.ConversationProjectionRepository{},
		Processed:     processed,
		Tx:            &mocks.TransactionManager{},
		Metrics:       observability.NewMetrics(nil),
		Logger:        zap.NewNop(),
	})
}

// memberJoinedEvent builds a valid EVT-002 message body.
func memberJoinedEvent(t *testing.T) []byte {
	t.Helper()
	envelope, err := models.NewEnvelope(models.EnvelopeInput{
		EventType: models.EventCommunityMemberChanged,
	}, time.Now().UTC())
	require.NoError(t, err)
	payload, err := json.Marshal(models.CommunityMemberChangedPayload{
		CommunityID:   uuid.MustParse("44444444-4444-4444-8444-444444444444"),
		UserID:        uuid.MustParse("22222222-2222-4222-8222-222222222222"),
		Change:        "joined",
		Status:        models.MemberStatusActive,
		MemberVersion: 1,
	})
	require.NoError(t, err)
	body, err := json.Marshal(models.Event{Envelope: envelope, Payload: payload})
	require.NoError(t, err)
	return body
}

func TestGroupHandlerMarksProcessedMessages(t *testing.T) {
	t.Parallel()
	processed := &mocks.ProcessedEventRepository{}
	processed.On("IsProcessed", mock.Anything, mock.Anything).Return(true, nil).Once()

	handler := &groupHandler{processor: newTestProcessor(t, processed), logger: zap.NewNop()}
	messages := make(chan *sarama.ConsumerMessage, 1)
	messages <- &sarama.ConsumerMessage{Topic: models.TopicCommunityEvents, Value: memberJoinedEvent(t)}
	close(messages)

	session := &fakeSession{ctx: context.Background()}
	require.NoError(t, handler.Setup(session))
	require.NoError(t, handler.Cleanup(session))
	require.NoError(t, handler.ConsumeClaim(session, &fakeClaim{messages: messages}))
	assert.Len(t, session.marked, 1)
	processed.AssertExpectations(t)
}

func TestGroupHandlerReturnsErrorForRetry(t *testing.T) {
	t.Parallel()
	processed := &mocks.ProcessedEventRepository{}
	// A failing dedupe lookup surfaces as an error so the offset is not committed
	// and the partition is redelivered.
	processed.On("IsProcessed", mock.Anything, mock.Anything).Return(false, errors.New("db down")).Once()

	handler := &groupHandler{processor: newTestProcessor(t, processed), logger: zap.NewNop()}
	messages := make(chan *sarama.ConsumerMessage, 1)
	messages <- &sarama.ConsumerMessage{Topic: models.TopicCommunityEvents, Value: memberJoinedEvent(t)}
	close(messages)

	session := &fakeSession{ctx: context.Background()}
	require.Error(t, handler.ConsumeClaim(session, &fakeClaim{messages: messages}))
	assert.Empty(t, session.marked)
}

func TestGroupHandlerStopsOnSessionCancel(t *testing.T) {
	t.Parallel()
	handler := &groupHandler{
		processor: newTestProcessor(t, &mocks.ProcessedEventRepository{}),
		logger:    zap.NewNop(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	session := &fakeSession{ctx: ctx}

	messages := make(chan *sarama.ConsumerMessage)
	done := make(chan error, 1)
	go func() { done <- handler.ConsumeClaim(session, &fakeClaim{messages: messages}) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("consume claim did not stop after the session ended")
	}
}

func TestNewKafkaClientRejectsUnreachableBrokers(t *testing.T) {
	t.Parallel()
	_, err := NewKafkaClient(KafkaOptions{Brokers: []string{"127.0.0.1:1"}, GroupID: "group"})
	require.Error(t, err)
}
