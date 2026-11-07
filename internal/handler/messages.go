package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"messaging_service/internal/models"
	"messaging_service/internal/service"
)

// sendChannelMessage handles POST /channels/{channel_id}/messages.
func (h *Handler) sendChannelMessage(c *gin.Context) {
	userID, ok := currentUser(c)
	if !ok {
		respond(c, models.ErrUnauthenticated)
		return
	}
	channelID, ok := parsePathUUID(c, "channel_id")
	if !ok {
		return
	}
	request, err := bindSendRequest(c)
	if err != nil {
		respond(c, err)
		return
	}
	view, err := h.deps.Messages.Send(c.Request.Context(), service.SendCommand{
		ContainerType:   models.ContainerChannel,
		ContainerID:     channelID,
		AuthorUserID:    userID,
		ClientMessageID: request.ClientMessageID,
		Content:         request.Content,
		AttachmentIDs:   request.AttachmentIDs,
		ThreadRootID:    request.ThreadRootID,
	})
	if err != nil {
		respond(c, err)
		return
	}
	c.JSON(http.StatusCreated, view)
}

// sendConversationMessage handles POST /conversations/{conversation_id}/messages.
func (h *Handler) sendConversationMessage(c *gin.Context) {
	userID, ok := currentUser(c)
	if !ok {
		respond(c, models.ErrUnauthenticated)
		return
	}
	conversationID, ok := parsePathUUID(c, "conversation_id")
	if !ok {
		return
	}
	request, err := bindSendRequest(c)
	if err != nil {
		respond(c, err)
		return
	}
	view, err := h.deps.Messages.Send(c.Request.Context(), service.SendCommand{
		ContainerType:   models.ContainerConversation,
		ContainerID:     conversationID,
		AuthorUserID:    userID,
		ClientMessageID: request.ClientMessageID,
		Content:         request.Content,
		AttachmentIDs:   request.AttachmentIDs,
		ThreadRootID:    request.ThreadRootID,
	})
	if err != nil {
		respond(c, err)
		return
	}
	c.JSON(http.StatusCreated, view)
}

// listChannelMessages handles GET /channels/{channel_id}/messages.
func (h *Handler) listChannelMessages(c *gin.Context) {
	h.listMessages(c, models.ContainerChannel, "channel_id")
}

// listConversationMessages handles GET /conversations/{conversation_id}/messages.
func (h *Handler) listConversationMessages(c *gin.Context) {
	h.listMessages(c, models.ContainerConversation, "conversation_id")
}

// listMessages implements the shared history paging behaviour.
func (h *Handler) listMessages(c *gin.Context, containerType models.ContainerType, param string) {
	userID, ok := currentUser(c)
	if !ok {
		respond(c, models.ErrUnauthenticated)
		return
	}
	containerID, ok := parsePathUUID(c, param)
	if !ok {
		return
	}
	limit, ok := parseLimit(c, h.defaultLimit(), h.deps.PageLimitMax)
	if !ok {
		return
	}
	before, err := parseCursor(c, "before")
	if err != nil {
		respond(c, err)
		return
	}
	after, err := parseCursor(c, "after")
	if err != nil {
		respond(c, err)
		return
	}
	page, err := h.deps.Messages.History(c.Request.Context(), service.HistoryCommand{
		ContainerType: containerType,
		ContainerID:   containerID,
		UserID:        userID,
		Limit:         limit,
		Before:        before,
		After:         after,
	})
	if err != nil {
		respond(c, err)
		return
	}
	c.JSON(http.StatusOK, page)
}

// getMessageThread handles GET /messages/{message_id}/thread.
func (h *Handler) getMessageThread(c *gin.Context) {
	userID, ok := currentUser(c)
	if !ok {
		respond(c, models.ErrUnauthenticated)
		return
	}
	messageID, ok := parsePathUUID(c, "message_id")
	if !ok {
		return
	}
	limit, ok := parseLimit(c, h.defaultLimit(), h.deps.PageLimitMax)
	if !ok {
		return
	}
	cursor, err := parseCursor(c, "cursor")
	if err != nil {
		respond(c, err)
		return
	}
	thread, err := h.deps.Messages.Thread(c.Request.Context(), service.ThreadCommand{
		MessageID: messageID,
		UserID:    userID,
		Cursor:    cursor,
		Limit:     limit,
	})
	if err != nil {
		respond(c, err)
		return
	}
	c.JSON(http.StatusOK, thread)
}

// editMessage handles PATCH /messages/{message_id}.
func (h *Handler) editMessage(c *gin.Context) {
	userID, ok := currentUser(c)
	if !ok {
		respond(c, models.ErrUnauthenticated)
		return
	}
	messageID, ok := parsePathUUID(c, "message_id")
	if !ok {
		return
	}
	request, err := bindEditRequest(c)
	if err != nil {
		respond(c, err)
		return
	}
	view, err := h.deps.Messages.Edit(c.Request.Context(), service.EditCommand{
		MessageID: messageID,
		UserID:    userID,
		Content:   request.Content,
	})
	if err != nil {
		respond(c, err)
		return
	}
	c.JSON(http.StatusOK, view)
}

// deleteMessage handles DELETE /messages/{message_id}. Delete is idempotent, so
// an already deleted message still answers 204.
func (h *Handler) deleteMessage(c *gin.Context) {
	userID, ok := currentUser(c)
	if !ok {
		respond(c, models.ErrUnauthenticated)
		return
	}
	messageID, ok := parsePathUUID(c, "message_id")
	if !ok {
		return
	}
	if err := h.deps.Messages.Delete(c.Request.Context(), service.DeleteCommand{
		MessageID: messageID,
		UserID:    userID,
	}); err != nil {
		respond(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// addReaction handles PUT /messages/{message_id}/reactions/{emoji}.
func (h *Handler) addReaction(c *gin.Context) { h.react(c, true) }

// removeReaction handles DELETE /messages/{message_id}/reactions/{emoji}.
func (h *Handler) removeReaction(c *gin.Context) { h.react(c, false) }

func (h *Handler) react(c *gin.Context, add bool) {
	userID, ok := currentUser(c)
	if !ok {
		respond(c, models.ErrUnauthenticated)
		return
	}
	messageID, ok := parsePathUUID(c, "message_id")
	if !ok {
		return
	}
	emoji, ok := validateEmoji(c)
	if !ok {
		return
	}
	summary, err := h.deps.Messages.React(c.Request.Context(), service.ReactionCommand{
		MessageID: messageID,
		UserID:    userID,
		Emoji:     emoji,
		Add:       add,
	})
	if err != nil {
		respond(c, err)
		return
	}
	c.JSON(http.StatusOK, summary)
}

// defaultLimit is the documented default page size.
func (h *Handler) defaultLimit() int { return 50 }
