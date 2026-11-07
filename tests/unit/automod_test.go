package unit

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"messaging_service/internal/models"
	"messaging_service/internal/service"
)

// automodRules builds a projected rule set for the given thresholds.
func automodRules(spam models.SpamRules, banned models.BannedWordsRules, exempt []uuid.UUID) *models.AutomodRuleSet {
	return &models.AutomodRuleSet{
		CommunityID:    testCommunity,
		RuleSetVersion: 4,
		Spam:           spam,
		BannedWords:    banned,
		ExemptRoleIDs:  exempt,
	}
}

// activeSpamRules is the default enabled spam rule set used by tests.
func activeSpamRules() models.SpamRules {
	return models.SpamRules{
		Enabled:               true,
		Action:                models.AutomodActionBlock,
		MaxMessagesPerWindow:  5,
		WindowSeconds:         10,
		MaxDuplicateMessages:  2,
		MaxMentionsPerMessage: 3,
		MaxLinksPerMessage:    1,
	}
}

func TestAutomodBlocksBannedWords(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
	h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(automodRules(
		activeSpamRules(),
		models.BannedWordsRules{Enabled: true, Action: models.AutomodActionBlock, MatchMode: "whole_word",
			Words: []string{"casino"}},
		nil,
	), nil).Once()

	_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
		ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
		ClientMessageID: "c1", Content: "join my casino now",
	})
	require.Error(t, err)
	var appErr *models.AppError
	require.True(t, errors.As(err, &appErr))
	assert.Equal(t, 422, appErr.Status)
	assert.Equal(t, models.CodeBlockedByAutomod, appErr.Code)
	assert.Equal(t, models.AutomodCategoryBannedWords, appErr.Extra["category"])
	assert.Contains(t, appErr.Detail, "banned_words")
	// No Insert expectation exists: a blocked message must never be persisted.
}

func TestAutomodRemoveAcceptsThenTombstones(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
	h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(automodRules(
		activeSpamRules(),
		models.BannedWordsRules{Enabled: true, Action: models.AutomodActionRemove, MatchMode: "substring",
			Words: []string{"spoiler"}},
		nil,
	), nil).Once()
	h.tx.On("InTx", matchContext()).Return(nil).Once()

	accepted := storedMessage(testContainer, "spoiler alert")
	h.messages.On("Insert", matchContext(), mock.Anything).Return(&accepted, true, nil).Once()
	h.messages.On("ReplaceMentions", matchContext(), accepted.ID, mock.Anything).Return(nil).Once()
	deleted := accepted
	deleted.Content = nil
	reason := models.DeletionReasonAutomod
	deleted.DeletionReason = &reason
	deleted.DeletedAt = &h.now
	deleted.MessageVersion = 2
	h.messages.On("SoftDelete", matchContext(), accepted.ID, models.DeletionReasonAutomod, h.now).
		Return(&deleted, nil).Once()
	h.messages.On("DetachAttachments", matchContext(), accepted.ID).Return([]string{}, nil).Once()
	recorder := h.expectOutbox(2)

	view, err := h.messagesService().Send(contextBackground(), service.SendCommand{
		ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
		ClientMessageID: "c1", Content: "spoiler alert",
	})
	require.NoError(t, err)
	require.NotNil(t, view)
	require.NotNil(t, view.DeletionReason)
	assert.Equal(t, string(models.DeletionReasonAutomod), *view.DeletionReason)
	assert.Nil(t, view.Content)

	payloads := recorder.payloads(t)
	require.Len(t, payloads, 2)
	assert.Equal(t, string(models.ChangeCreated), payloads[0].Change)
	assert.Equal(t, string(models.ChangeDeleted), payloads[1].Change)
	require.NotNil(t, payloads[1].DeletionReason)
	assert.Equal(t, string(models.DeletionReasonAutomod), *payloads[1].DeletionReason)
	assert.Nil(t, payloads[1].Content)
	assert.Empty(t, payloads[1].Attachments)
}

func TestAutomodExemptRolesSkipEnforcement(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	exemptRole := uuid.MustParse("99999999-9999-4999-8999-999999999999")
	member := activeMembership(testCommunity, testAuthorID)
	member.RoleIDs = []uuid.UUID{exemptRole}
	h.expectChannelAccess(testContainer, testCommunity, testAuthorID, member)
	h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(automodRules(
		activeSpamRules(),
		models.BannedWordsRules{Enabled: true, Action: models.AutomodActionBlock, MatchMode: "substring",
			Words: []string{"casino"}},
		[]uuid.UUID{exemptRole},
	), nil).Once()
	h.tx.On("InTx", matchContext()).Return(nil).Once()
	stored := storedMessage(testContainer, "casino night")
	h.messages.On("Insert", matchContext(), mock.Anything).Return(&stored, true, nil).Once()
	h.messages.On("ReplaceMentions", matchContext(), stored.ID, mock.Anything).Return(nil).Once()
	h.expectOutbox(1)

	view, err := h.messagesService().Send(contextBackground(), service.SendCommand{
		ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
		ClientMessageID: "c1", Content: "casino night",
	})
	require.NoError(t, err)
	require.NotNil(t, view)
}

func TestAutomodSpamThresholds(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		content  string
		mentions []uuid.UUID
		rate     *models.MemberRate
	}{
		{
			name:    "messages per window",
			content: "hello there",
			rate: &models.MemberRate{
				CommunityID: testCommunity, UserID: testAuthorID, WindowStartedAt: testNow,
				MessageCount: 5,
			},
		},
		{
			name:    "duplicate messages",
			content: "same text",
			rate: &models.MemberRate{
				CommunityID: testCommunity, UserID: testAuthorID, WindowStartedAt: testNow,
				MessageCount:       1,
				RecentFingerprints: []string{fingerprintOf("same text"), fingerprintOf("same text")},
			},
		},
		{
			name:     "mention flood",
			content:  "hey",
			mentions: []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()},
		},
		{
			name:    "link flood",
			content: "see https://a.example and https://b.example",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			// Mention tokens must be present in the content for the parser to
			// produce candidates at all.
			content := tc.content
			if len(tc.mentions) > 0 {
				var builder strings.Builder
				builder.WriteString(content)
				for _, mention := range tc.mentions {
					builder.WriteString(" <@" + mention.String() + ">")
				}
				content = builder.String()
			}
			h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
			h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(automodRules(
				activeSpamRules(),
				models.BannedWordsRules{Enabled: false, Action: models.AutomodActionBlock, MatchMode: "substring"},
				nil,
			), nil).Once()
			if tc.mentions != nil {
				h.community.On("FilterActiveMemberIDs", matchContext(), testCommunity, mock.Anything).
					Return(tc.mentions, nil).Once()
			}
			h.rates.On("GetRate", matchContext(), testCommunity, testAuthorID).Return(tc.rate, nil).Once()

			_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
				ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
				ClientMessageID: "c1", Content: content,
			})
			require.Error(t, err)
			var appErr *models.AppError
			require.True(t, errors.As(err, &appErr))
			assert.Equal(t, models.CodeBlockedByAutomod, appErr.Code)
			assert.Equal(t, models.AutomodCategorySpam, appErr.Extra["category"])
		})
	}
}

func TestAutomodSpamCounterPersistsOnAcceptedSend(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
	h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(automodRules(
		activeSpamRules(),
		models.BannedWordsRules{Enabled: false, Action: models.AutomodActionBlock, MatchMode: "substring"},
		nil,
	), nil).Once()
	h.rates.On("GetRate", matchContext(), testCommunity, testAuthorID).Return(nil, nil).Once()

	stored := storedMessage(testContainer, "first message")
	h.tx.On("InTx", matchContext()).Return(nil).Once()
	h.messages.On("Insert", matchContext(), mock.Anything).Return(&stored, true, nil).Once()
	h.messages.On("ReplaceMentions", matchContext(), stored.ID, mock.Anything).Return(nil).Once()
	h.rates.On("UpsertRate", matchContext(), mock.MatchedBy(
		func(rate models.MemberRate) bool {
			return rate.MessageCount == 1 && rate.WindowStartedAt.Equal(testNow) &&
				len(rate.RecentFingerprints) == 1
		})).Return(nil).Once()
	h.expectOutbox(1)

	_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
		ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
		ClientMessageID: "c1", Content: "first message",
	})
	require.NoError(t, err)
}

func TestAutomodDisabledDoesNotFilter(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
	h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(automodRules(
		models.SpamRules{Enabled: false, Action: models.AutomodActionBlock, MaxMessagesPerWindow: 1,
			WindowSeconds: 10, MaxDuplicateMessages: 1, MaxMentionsPerMessage: 1},
		models.BannedWordsRules{Enabled: false, Action: models.AutomodActionBlock, MatchMode: "substring"},
		nil,
	), nil).Once()
	h.tx.On("InTx", matchContext()).Return(nil).Once()
	stored := storedMessage(testContainer, "anything at all")
	h.messages.On("Insert", matchContext(), mock.Anything).Return(&stored, true, nil).Once()
	h.messages.On("ReplaceMentions", matchContext(), stored.ID, mock.Anything).Return(nil).Once()
	h.expectOutbox(1)

	_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
		ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
		ClientMessageID: "c1", Content: "anything at all",
	})
	require.NoError(t, err)
}

func TestAutomodBannedWordMatching(t *testing.T) {
	t.Parallel()

	t.Run("whole word does not match inside a larger word", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
		h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(automodRules(
			models.SpamRules{Enabled: false, Action: models.AutomodActionBlock},
			models.BannedWordsRules{Enabled: true, Action: models.AutomodActionBlock, MatchMode: "whole_word",
				Words: []string{"spam"}},
			nil,
		), nil).Once()
		h.tx.On("InTx", matchContext()).Return(nil).Once()
		stored := storedMessage(testContainer, "spammers unite")
		h.messages.On("Insert", matchContext(), mock.Anything).Return(&stored, true, nil).Once()
		h.messages.On("ReplaceMentions", matchContext(), stored.ID, mock.Anything).Return(nil).Once()
		h.expectOutbox(1)

		_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
			ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
			ClientMessageID: "c1", Content: "spammers unite",
		})
		require.NoError(t, err)
	})

	t.Run("substring matches inside a larger word", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		h.expectChannelAccess(testContainer, testCommunity, testAuthorID, activeMembership(testCommunity, testAuthorID))
		h.community.On("GetAutomodRuleSet", matchContext(), testCommunity).Return(automodRules(
			models.SpamRules{Enabled: false, Action: models.AutomodActionBlock},
			models.BannedWordsRules{Enabled: true, Action: models.AutomodActionBlock, MatchMode: "substring",
				Words: []string{"spam"}},
			nil,
		), nil).Once()

		_, err := h.messagesService().Send(contextBackground(), service.SendCommand{
			ContainerType: models.ContainerChannel, ContainerID: testContainer, AuthorUserID: testAuthorID,
			ClientMessageID: "c1", Content: "spammers unite",
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, models.ErrBlockedByAutomod))
	})
}
