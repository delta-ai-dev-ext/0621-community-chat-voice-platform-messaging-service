package observability

import (
	"context"
	"regexp"
	"strings"

	"github.com/google/uuid"

	"messaging_service/internal/models"
)

// Trace carries the request's trace and correlation identifiers. Every produced
// event records them in the envelope, and every log line repeats the trace id.
type Trace struct {
	TraceID       string
	CorrelationID uuid.UUID
}

var (
	traceIDPattern   = regexp.MustCompile(`^[0-9a-f]{32}$`)
	traceparentParts = 3
)

// TraceFromHeaders derives the trace from inbound headers, continuing a
// W3C `traceparent` when present, then falling back to `X-Trace-Id`, and
// finally minting a fresh id. correlationID is what the caller passed in
// `X-Correlation-Id` (or zero, which is replaced by a new uuid).
func TraceFromHeaders(traceparent, traceHeader, correlationHeader string) Trace {
	traceID := traceIDFromTraceparent(traceparent)
	if traceID == "" && traceIDPattern.MatchString(strings.ToLower(strings.TrimSpace(traceHeader))) {
		traceID = strings.ToLower(strings.TrimSpace(traceHeader))
	}
	if traceID == "" {
		traceID = models.MintTraceID()
	}
	correlationID, err := uuid.Parse(strings.TrimSpace(correlationHeader))
	if err != nil {
		correlationID = uuid.New()
	}
	return Trace{TraceID: traceID, CorrelationID: correlationID}
}

// traceIDFromTraceparent extracts the trace id of a W3C traceparent header:
// version-traceid-spanid-flags.
func traceIDFromTraceparent(traceparent string) string {
	parts := strings.Split(strings.TrimSpace(traceparent), "-")
	if len(parts) != 4 || len(parts) != traceparentParts+1 {
		return ""
	}
	candidate := strings.ToLower(parts[1])
	if !traceIDPattern.MatchString(candidate) || candidate == strings.Repeat("0", 32) {
		return ""
	}
	return candidate
}

type traceContextKey struct{}

// WithTrace stores the trace in the context.
func WithTrace(ctx context.Context, trace Trace) context.Context {
	return context.WithValue(ctx, traceContextKey{}, trace)
}

// TraceFromContext returns the trace, or an empty value when absent.
func TraceFromContext(ctx context.Context) Trace {
	trace, _ := ctx.Value(traceContextKey{}).(Trace)
	return trace
}
