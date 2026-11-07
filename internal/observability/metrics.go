// Package observability holds the metrics registry, trace propagation and
// logging helpers shared by the HTTP edge, the services and the event pipeline.
package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Attachment metric results (low-cardinality label values).
const (
	AttachmentResultAccepted        = "accepted"
	AttachmentResultTooLarge        = "too_large"
	AttachmentResultUnsupportedType = "unsupported_type"
)

// Metrics owns every business and RED collector. All methods are nil-safe so
// tests can construct services without a registry.
type Metrics struct {
	registry *prometheus.Registry

	messagesSent       *prometheus.CounterVec
	messagesEdited     prometheus.Counter
	messagesDeleted    *prometheus.CounterVec
	reactions          *prometheus.CounterVec
	mentions           prometheus.Counter
	attachments        *prometheus.CounterVec
	automodHits        *prometheus.CounterVec
	duplicateSends     prometheus.Counter
	httpRequests       *prometheus.CounterVec
	httpDuration       *prometheus.HistogramVec
	kafkaConsumerLag   *prometheus.GaugeVec
	consumerFailures   *prometheus.CounterVec
	outboxUnpublished  prometheus.Gauge
	outboxPublishFails prometheus.Counter
}

// NewMetrics builds and registers all collectors.
func NewMetrics(registerer prometheus.Registerer) *Metrics {
	if registerer == nil {
		registerer = prometheus.NewRegistry()
	}
	m := &Metrics{
		registry: registerer.(*prometheus.Registry),
		messagesSent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "messaging_messages_sent", Help: "Messages created, by container type.",
		}, []string{"container_type"}),
		messagesEdited: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "messaging_messages_edited", Help: "Messages edited.",
		}),
		messagesDeleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "messaging_messages_deleted", Help: "Messages deleted, by reason.",
		}, []string{"reason"}),
		reactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "messaging_reactions", Help: "Reaction mutations.",
		}, []string{"action"}),
		mentions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "messaging_mentions", Help: "Mentions resolved and stored.",
		}),
		attachments: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "messaging_attachments", Help: "Attachment reservation outcomes.",
		}, []string{"result"}),
		automodHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "messaging_automod_hits", Help: "Automod rule hits.",
		}, []string{"category", "action"}),
		duplicateSends: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "messaging_duplicate_sends_suppressed", Help: "Idempotent re-sends returned instead of duplicated.",
		}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests", Help: "HTTP requests by route, method and status class.",
		}, []string{"route", "method", "status_class"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "http_request_duration_seconds", Help: "HTTP request latency.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 2},
		}, []string{"route", "method"}),
		kafkaConsumerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_consumer_lag", Help: "Consumer lag per topic and group.",
		}, []string{"topic", "consumer_group"}),
		consumerFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_consumer_failures", Help: "Failed event applications.",
		}, []string{"topic"}),
		outboxUnpublished: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "messaging_outbox_unpublished", Help: "Outbox entries not yet published.",
		}),
		outboxPublishFails: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "messaging_outbox_publish_failures", Help: "Outbox publish attempts that failed.",
		}),
	}
	registerer.MustRegister(
		m.messagesSent, m.messagesEdited, m.messagesDeleted, m.reactions, m.mentions,
		m.attachments, m.automodHits, m.duplicateSends, m.httpRequests, m.httpDuration,
		m.kafkaConsumerLag, m.consumerFailures, m.outboxUnpublished, m.outboxPublishFails,
	)
	return m
}

// Handler exposes the registry in Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	registry := prometheus.NewRegistry()
	if m != nil {
		registry = m.registry
	}
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

// IncMessagesSent counts a created message.
func (m *Metrics) IncMessagesSent(containerType string) {
	if m == nil {
		return
	}
	m.messagesSent.WithLabelValues(containerType).Inc()
}

// IncMessagesEdited counts an edit.
func (m *Metrics) IncMessagesEdited() {
	if m == nil {
		return
	}
	m.messagesEdited.Inc()
}

// IncMessagesDeleted counts a deletion by reason.
func (m *Metrics) IncMessagesDeleted(reason string) {
	if m == nil {
		return
	}
	m.messagesDeleted.WithLabelValues(reason).Inc()
}

// IncReactions counts an add or remove that actually changed state.
func (m *Metrics) IncReactions(action string) {
	if m == nil {
		return
	}
	m.reactions.WithLabelValues(action).Inc()
}

// AddMentions counts resolved mentions.
func (m *Metrics) AddMentions(count int) {
	if m == nil || count <= 0 {
		return
	}
	m.mentions.Add(float64(count))
}

// IncAttachments counts an attachment reservation outcome.
func (m *Metrics) IncAttachments(result string) {
	if m == nil {
		return
	}
	m.attachments.WithLabelValues(result).Inc()
}

// IncAutomodHit counts an automod enforcement decision.
func (m *Metrics) IncAutomodHit(category, action string) {
	if m == nil {
		return
	}
	m.automodHits.WithLabelValues(category, action).Inc()
}

// IncDuplicateSend counts a suppressed duplicate send.
func (m *Metrics) IncDuplicateSend() {
	if m == nil {
		return
	}
	m.duplicateSends.Inc()
}

// ObserveHTTP records one request for the RED metrics.
func (m *Metrics) ObserveHTTP(route, method string, status int, duration time.Duration) {
	if m == nil {
		return
	}
	m.httpRequests.WithLabelValues(route, method, statusClass(status)).Inc()
	m.httpDuration.WithLabelValues(route, method).Observe(duration.Seconds())
}

// SetKafkaLag publishes the consumer lag sample for a topic.
func (m *Metrics) SetKafkaLag(topic, group string, lag float64) {
	if m == nil {
		return
	}
	m.kafkaConsumerLag.WithLabelValues(topic, group).Set(lag)
}

// IncConsumerFailure counts an event that failed to apply.
func (m *Metrics) IncConsumerFailure(topic string) {
	if m == nil {
		return
	}
	m.consumerFailures.WithLabelValues(topic).Inc()
}

// SetOutboxUnpublished publishes the relay backlog depth.
func (m *Metrics) SetOutboxUnpublished(depth float64) {
	if m == nil {
		return
	}
	m.outboxUnpublished.Set(depth)
}

// IncOutboxPublishFailure counts a failed publish attempt.
func (m *Metrics) IncOutboxPublishFailure() {
	if m == nil {
		return
	}
	m.outboxPublishFails.Inc()
}

func statusClass(status int) string {
	return strconv.Itoa(status/100) + "xx"
}
