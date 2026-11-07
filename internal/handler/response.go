package handler

import (
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"messaging_service/internal/models"
)

// JSON content types used by the two error surfaces: `application/json` for the
// public API and `application/problem+json` for /internal/*.
const (
	contentTypeJSON    = "application/json"
	contentTypeProblem = "application/problem+json"
)

// respond writes a domain error (or 500 for anything unexpected) as the standard
// `{"error","detail"}` body plus any contract extras.
func respond(c *gin.Context, err error) { writeError(c, err, contentTypeJSON) }

// respondProblem writes an internal-endpoint error as problem+json.
func respondProblem(c *gin.Context, err error) { writeError(c, err, contentTypeProblem) }

func writeError(c *gin.Context, err error, contentType string) {
	var appErr *models.AppError
	if !errors.As(err, &appErr) {
		appErr = models.ErrInternal
	}
	payload := gin.H{"error": appErr.Code, "detail": appErr.Detail}
	for key, value := range appErr.Extra {
		payload[key] = value
	}
	c.Header("Content-Type", contentType)
	c.JSON(appErr.Status, payload)
}

// parsePathUUID parses a uuid path parameter, answering 400 when malformed.
func parsePathUUID(c *gin.Context, name string) (uuid.UUID, bool) {
	id, err := uuid.Parse(c.Param(name))
	if err != nil || id == uuid.Nil {
		respond(c, models.ErrInvalidRequest.WithDetail("The path parameter "+name+" must be a UUID."))
		return uuid.Nil, false
	}
	return id, true
}

// parseLimit reads the optional `limit` query parameter within the documented
// bound and applies the default.
func parseLimit(c *gin.Context, defaultLimit, maxLimit int) (int, bool) {
	raw := c.Query("limit")
	if raw == "" {
		return defaultLimit, true
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || (maxLimit > 0 && value > maxLimit) {
		respond(c, models.ErrInvalidRequest.WithDetail("limit must be an integer between 1 and 100."))
		return 0, false
	}
	return value, true
}

// parseCursor reads and decodes an opaque cursor query parameter.
func parseCursor(c *gin.Context, name string) (*models.Cursor, error) {
	raw := c.Query(name)
	if raw == "" {
		return nil, nil
	}
	cursor, err := models.DecodeCursor(raw)
	if err != nil {
		return nil, err
	}
	return &cursor, nil
}

// bindSendRequest decodes and validates a SendMessageRequest.
func bindSendRequest(c *gin.Context) (*models.SendMessageRequest, error) {
	var request models.SendMessageRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		return nil, models.ErrInvalidRequest.WithDetail("The request body could not be parsed.")
	}
	if strings.TrimSpace(request.ClientMessageID) == "" {
		return nil, models.ErrInvalidRequest.WithDetail("client_message_id is required.")
	}
	if utf8.RuneCountInString(request.ClientMessageID) > 128 {
		return nil, models.ErrInvalidRequest.WithDetail("client_message_id must be at most 128 characters.")
	}
	if err := validateContent(request.Content); err != nil {
		return nil, err
	}
	if len(request.AttachmentIDs) > 10 {
		return nil, models.ErrInvalidRequest.WithDetail("At most 10 attachments may be referenced.")
	}
	seen := make(map[uuid.UUID]struct{}, len(request.AttachmentIDs))
	for _, id := range request.AttachmentIDs {
		if id == uuid.Nil {
			return nil, models.ErrInvalidRequest.WithDetail("attachment_ids must be UUIDs.")
		}
		if _, ok := seen[id]; ok {
			return nil, models.ErrInvalidRequest.WithDetail("attachment_ids must be unique.")
		}
		seen[id] = struct{}{}
	}
	return &request, nil
}

// bindEditRequest decodes and validates an EditMessageRequest.
func bindEditRequest(c *gin.Context) (*models.EditMessageRequest, error) {
	var request models.EditMessageRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		return nil, models.ErrInvalidRequest.WithDetail("The request body could not be parsed.")
	}
	if err := validateContent(request.Content); err != nil {
		return nil, err
	}
	return &request, nil
}

// validateContent enforces the 4,000 character content budget: an empty body is a
// 400 while an over-long one is the documented 422 content_too_long.
func validateContent(content string) error {
	if strings.TrimSpace(content) == "" {
		return models.ErrInvalidRequest.WithDetail("content is required.")
	}
	if utf8.RuneCountInString(content) > 4000 {
		return models.ErrContentTooLong
	}
	return nil
}

// validateEmoji enforces the 1..32 character emoji path parameter.
func validateEmoji(c *gin.Context) (string, bool) {
	emoji := c.Param("emoji")
	count := utf8.RuneCountInString(emoji)
	if count < 1 || count > 32 {
		respond(c, models.ErrInvalidRequest.WithDetail("emoji must be between 1 and 32 characters."))
		return "", false
	}
	return emoji, true
}
