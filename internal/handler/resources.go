package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"messaging_service/internal/models"
	"messaging_service/internal/service"
)

// createAttachment handles POST /attachments: validate declared metadata, store a
// pending reservation and return a presigned PUT URL. Replaying the same
// Idempotency-Key returns the original reservation.
func (h *Handler) createAttachment(c *gin.Context) {
	userID, ok := currentUser(c)
	if !ok {
		respond(c, models.ErrUnauthenticated)
		return
	}
	var request models.CreateAttachmentRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		respond(c, models.ErrInvalidRequest.WithDetail("filename, content_type and size_bytes are required."))
		return
	}
	created, err := h.deps.Attachments.Reserve(c.Request.Context(), service.ReserveCommand{
		UploaderUserID: userID,
		Filename:       request.Filename,
		ContentType:    request.ContentType,
		SizeBytes:      request.SizeBytes,
		IdempotencyKey: c.GetHeader("Idempotency-Key"),
	})
	if err != nil {
		respond(c, err)
		return
	}
	c.JSON(http.StatusCreated, created)
}

// userScopes answers API-003 for the realtime gateway from local projections.
func (h *Handler) userScopes(c *gin.Context) {
	var request models.UserScopesRequest
	if err := c.ShouldBindJSON(&request); err != nil || request.UserID == uuid.Nil {
		respondProblem(c, models.ErrInvalidRequest.WithDetail("user_id must be a uuid."))
		return
	}
	response, err := h.deps.Scopes.UserScopes(c.Request.Context(), request.UserID)
	if err != nil {
		respondProblem(c, err)
		return
	}
	c.JSON(http.StatusOK, response)
}

// healthz is the liveness probe: it only reports that the process is alive and
// never probes PostgreSQL, Kafka or storage.
func (h *Handler) healthz(c *gin.Context) {
	c.JSON(http.StatusOK, models.HealthStatus{Status: models.StatusOK})
}

// readyz is the readiness probe: it verifies every dependency and answers 503 as
// soon as shutdown begins.
func (h *Handler) readyz(c *gin.Context) {
	ready, failures := h.deps.Health.Ready(c.Request.Context())
	if !ready {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": models.StatusUnavailable,
			"checks": failures,
		})
		return
	}
	c.JSON(http.StatusOK, models.HealthStatus{Status: models.StatusOK})
}
