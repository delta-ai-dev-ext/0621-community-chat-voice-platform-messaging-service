// Package handler is the HTTP edge: thin gin handlers that bind and validate
// requests, delegate to services and map domain errors onto the documented
// statuses and bodies.
package handler

import (
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"messaging_service/internal/auth"
	"messaging_service/internal/health"
	"messaging_service/internal/observability"
	"messaging_service/internal/service"
)

// Deps is the handler dependency set.
type Deps struct {
	Messages     *service.MessageService
	Attachments  *service.AttachmentService
	Scopes       *service.ScopeService
	Verifier     *auth.Verifier
	Health       *health.Checker
	Metrics      *observability.Metrics
	Logger       *zap.Logger
	ServiceToken string
	PageLimitMax int
}

// Handler carries the dependencies shared by every endpoint.
type Handler struct {
	deps Deps
	log  *zap.Logger
}

// NewRouter builds the gin engine with the public and internal route groups.
// `/internal/*` is a separate middleware chain authenticated by the per-edge
// service token and is never exposed through the public ingress.
func NewRouter(deps Deps) *gin.Engine {
	logger := deps.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	h := &Handler{deps: deps, log: logger}

	router := gin.New()
	router.RedirectTrailingSlash = false
	router.Use(gin.Recovery(), traceMiddleware(), metricsMiddleware(deps.Metrics))

	router.GET("/healthz", h.healthz)
	router.GET("/readyz", h.readyz)
	if deps.Metrics != nil {
		router.GET("/metrics", gin.WrapH(deps.Metrics.Handler()))
	}

	public := router.Group("")
	public.Use(h.requireAccessToken())
	public.POST("/attachments", h.createAttachment)
	public.POST("/channels/:channel_id/messages", h.sendChannelMessage)
	public.GET("/channels/:channel_id/messages", h.listChannelMessages)
	public.POST("/conversations/:conversation_id/messages", h.sendConversationMessage)
	public.GET("/conversations/:conversation_id/messages", h.listConversationMessages)
	public.GET("/messages/:message_id/thread", h.getMessageThread)
	public.PATCH("/messages/:message_id", h.editMessage)
	public.DELETE("/messages/:message_id", h.deleteMessage)
	public.PUT("/messages/:message_id/reactions/:emoji", h.addReaction)
	public.DELETE("/messages/:message_id/reactions/:emoji", h.removeReaction)

	internal := router.Group("/internal")
	internal.Use(h.requireServiceToken())
	internal.POST("/realtime/user-scopes", h.userScopes)

	return router
}
