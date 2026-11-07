package handler

import (
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"messaging_service/internal/auth"
	"messaging_service/internal/models"
	"messaging_service/internal/observability"
)

// gin context keys used to hand request-scoped values to handlers.
const (
	contextUserID    = "authenticated_user_id"
	contextSessionID = "authenticated_session_id"
)

// traceMiddleware continues an inbound trace (W3C traceparent first, then
// X-Trace-Id) and echoes the trace id back to the client.
func traceMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		trace := observability.TraceFromHeaders(
			c.GetHeader("traceparent"),
			c.GetHeader("X-Trace-Id"),
			c.GetHeader("X-Correlation-Id"),
		)
		c.Request = c.Request.WithContext(observability.WithTrace(c.Request.Context(), trace))
		c.Header("X-Trace-Id", trace.TraceID)
		c.Next()
	}
}

// metricsMiddleware records the RED metrics for every request.
func metricsMiddleware(metrics *observability.Metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		metrics.ObserveHTTP(route, c.Request.Method, c.Writer.Status(), time.Since(start))
	}
}

// requireAccessToken verifies the bearer access token locally and stores the
// authenticated user id in the context. Verification failures answer 401 with
// the standard error body.
func (h *Handler) requireAccessToken() gin.HandlerFunc {
	return func(c *gin.Context) {
		token, err := auth.BearerToken(c.GetHeader("Authorization"))
		if err != nil {
			respond(c, models.ErrUnauthenticated)
			c.Abort()
			return
		}
		if h.deps.Verifier == nil {
			respond(c, models.ErrUnauthenticated)
			c.Abort()
			return
		}
		principal, verifyErr := h.deps.Verifier.Verify(token)
		if verifyErr != nil {
			h.log.Debug("access token rejected", zap.String("trace_id", observability.TraceFromContext(c.Request.Context()).TraceID))
			respond(c, models.ErrUnauthenticated)
			c.Abort()
			return
		}
		c.Set(contextUserID, principal.UserID)
		c.Set(contextSessionID, principal.SessionID)
		c.Next()
	}
}

// requireServiceToken authenticates /internal/* calls with the per-edge service
// token. Responses are application/problem+json for this group.
func (h *Handler) requireServiceToken() gin.HandlerFunc {
	return func(c *gin.Context) {
		token, err := auth.BearerToken(c.GetHeader("Authorization"))
		if err != nil || auth.VerifyServiceToken(token, h.deps.ServiceToken) != nil {
			respondProblem(c, models.ErrServiceTokenInvalid)
			c.Abort()
			return
		}
		c.Next()
	}
}

// currentUser returns the authenticated user id.
func currentUser(c *gin.Context) (uuid.UUID, bool) {
	value, ok := c.Get(contextUserID)
	if !ok {
		return uuid.Nil, false
	}
	userID, ok := value.(uuid.UUID)
	return userID, ok && userID != uuid.Nil
}

// requestTrace extracts the trace stored by traceMiddleware.
func requestTrace(c *gin.Context) observability.Trace {
	return observability.TraceFromContext(c.Request.Context())
}

// bearerPrefix is re-exported for readability in handler tests.
const bearerPrefix = "Bearer "

// trimBearer is a small helper used by tests to build headers.
func trimBearer(header string) string {
	return strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
}
