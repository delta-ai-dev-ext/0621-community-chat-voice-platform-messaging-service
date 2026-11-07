package observability

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsRecordEverything(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics(prometheus.NewRegistry())

	metrics.IncMessagesSent("channel")
	metrics.IncMessagesSent("conversation")
	metrics.IncMessagesEdited()
	metrics.IncMessagesDeleted("author")
	metrics.IncMessagesDeleted("automod")
	metrics.IncReactions("add")
	metrics.IncReactions("remove")
	metrics.AddMentions(2)
	metrics.AddMentions(0)
	metrics.IncAttachments(AttachmentResultAccepted)
	metrics.IncAutomodHit("spam", "block")
	metrics.IncDuplicateSend()
	metrics.ObserveHTTP("/channels/:channel_id/messages", "POST", 201, 12*time.Millisecond)
	metrics.ObserveHTTP("/messages/:message_id", "DELETE", 500, 3*time.Millisecond)
	metrics.SetKafkaLag("community_events", "messaging_service", 42)
	metrics.IncConsumerFailure("community_events")
	metrics.SetOutboxUnpublished(7)
	metrics.IncOutboxPublishFailure()

	require.NotNil(t, metrics.Handler())
}

func TestMetricsAreNilSafe(t *testing.T) {
	t.Parallel()
	var metrics *Metrics
	metrics.IncMessagesSent("channel")
	metrics.IncMessagesEdited()
	metrics.IncMessagesDeleted("author")
	metrics.IncReactions("add")
	metrics.AddMentions(1)
	metrics.IncAttachments(AttachmentResultAccepted)
	metrics.IncAutomodHit("spam", "block")
	metrics.IncDuplicateSend()
	metrics.ObserveHTTP("/x", "GET", 200, time.Millisecond)
	metrics.SetKafkaLag("topic", "group", 1)
	metrics.IncConsumerFailure("topic")
	metrics.SetOutboxUnpublished(1)
	metrics.IncOutboxPublishFailure()

	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	assert.Equal(t, 200, recorder.Code)
}

func TestMetricsExposeTextFormat(t *testing.T) {
	t.Parallel()
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	metrics.IncMessagesSent("channel")

	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	body := recorder.Body.String()
	assert.Contains(t, body, "messaging_messages_sent")
	assert.Contains(t, body, `container_type="channel"`)
}

func TestTraceFromHeaders(t *testing.T) {
	t.Parallel()

	continued := TraceFromHeaders(
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"",
		"22222222-2222-4222-8222-222222222222",
	)
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", continued.TraceID)
	assert.Equal(t, "22222222-2222-4222-8222-222222222222", continued.CorrelationID.String())

	fromHeader := TraceFromHeaders("", "0123456789ABCDEF0123456789ABCDEF", "")
	assert.Equal(t, "0123456789abcdef0123456789abcdef", fromHeader.TraceID)
	assert.NotNil(t, fromHeader.CorrelationID)

	minted := TraceFromHeaders("", "", "")
	assert.Len(t, minted.TraceID, 32)

	// A zero traceparent (all zeroes) is not a valid trace id.
	zero := TraceFromHeaders("00-00000000000000000000000000000000-00f067aa0ba902b7-01", "", "")
	assert.NotEqual(t, "00000000000000000000000000000000", traceIDFromTraceparent(
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01"))
	assert.Len(t, zero.TraceID, 32)

	assert.Empty(t, traceIDFromTraceparent("garbage"))
	assert.Empty(t, traceIDFromTraceparent("00-abc-def-01"))
}

func TestTraceContext(t *testing.T) {
	t.Parallel()
	trace := Trace{TraceID: "0123456789abcdef0123456789abcdef"}
	ctx := WithTrace(context.Background(), trace)
	assert.Equal(t, trace, TraceFromContext(ctx))

	empty := TraceFromContext(context.Background())
	assert.Empty(t, empty.TraceID)
}
