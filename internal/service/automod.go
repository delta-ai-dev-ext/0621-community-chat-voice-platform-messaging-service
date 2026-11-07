package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"

	"messaging_service/internal/models"
)

// automodAction is the engine's verdict for one message attempt.
type automodAction string

const (
	// automodNone means the message may be posted as-is.
	automodNone automodAction = "none"
	// automodBlock rejects the message before it is persisted.
	automodBlock automodAction = "block"
	// automodRemove accepts the message and tombstones it with reason automod.
	automodRemove automodAction = "remove"
)

// maxFingerprints bounds the duplicate-detection history kept per member.
const maxFingerprints = 20

// automodDecision is the result of evaluating a community's rule set together
// with the spam-counter state to persist when the message is accepted.
type automodDecision struct {
	Action   automodAction
	Category string
	NextRate *models.MemberRate
}

// automodInput carries everything the engine needs for one evaluation. The
// mention count is the number of *resolved* mentions, so a message full of
// invalid tokens cannot trip the mention-flood rule.
type automodInput struct {
	CommunityID  uuid.UUID
	UserID       uuid.UUID
	RoleIDs      []uuid.UUID
	Content      string
	MentionCount int
	Now          time.Time
}

// automodEngine applies the current rule-set projection on every channel
// message and on every edit (US-013, US-014).
type automodEngine struct {
	deps Deps
}

func newAutomodEngine(deps Deps) *automodEngine { return &automodEngine{deps: deps} }

// evaluate applies the community's rule set. Exempt roles skip enforcement
// entirely; without a projected rule set nothing is filtered.
func (e *automodEngine) evaluate(ctx context.Context, input automodInput) (automodDecision, error) {
	ruleSet, err := e.deps.Community.GetAutomodRuleSet(ctx, input.CommunityID)
	if err != nil {
		return automodDecision{}, err
	}
	if ruleSet == nil {
		return automodDecision{Action: automodNone}, nil
	}
	if ruleSet.IsExempt(input.RoleIDs) {
		return automodDecision{Action: automodNone}, nil
	}
	if decision, matched := e.matchBannedWords(ruleSet, input.Content); matched {
		return decision, nil
	}
	return e.evaluateSpam(ctx, ruleSet, input)
}

// matchBannedWords applies the content-based rule.
func (e *automodEngine) matchBannedWords(ruleSet *models.AutomodRuleSet, content string) (automodDecision, bool) {
	rules := ruleSet.BannedWords
	if !rules.Enabled || len(rules.Words) == 0 {
		return automodDecision{}, false
	}
	wholeWord := rules.MatchMode == "whole_word"
	for _, word := range rules.Words {
		if word == "" {
			continue
		}
		if matchesBannedWord(content, word, wholeWord) {
			action := actionFor(rules.Action)
			e.deps.Metrics.IncAutomodHit(models.AutomodCategoryBannedWords, string(action))
			return automodDecision{Action: action, Category: models.AutomodCategoryBannedWords}, true
		}
	}
	return automodDecision{}, false
}

// evaluateSpam applies the rate, duplicate, mention and link limits.
func (e *automodEngine) evaluateSpam(ctx context.Context, ruleSet *models.AutomodRuleSet, input automodInput) (automodDecision, error) {
	rules := ruleSet.Spam
	if !rules.Enabled {
		return automodDecision{Action: automodNone}, nil
	}
	current, err := e.deps.Rates.GetRate(ctx, input.CommunityID, input.UserID)
	if err != nil {
		return automodDecision{}, err
	}
	next := nextRateState(current, input, rules)

	violated := false
	switch {
	case input.MentionCount > rules.MaxMentionsPerMessage:
		violated = true
	case countLinks(input.Content) > rules.MaxLinksPerMessage:
		violated = true
	case countFingerprint(next.RecentFingerprints, fingerprint(input.Content)) > rules.MaxDuplicateMessages:
		violated = true
	case next.MessageCount > rules.MaxMessagesPerWindow:
		violated = true
	}
	if !violated {
		return automodDecision{Action: automodNone, NextRate: next}, nil
	}
	action := actionFor(rules.Action)
	e.deps.Metrics.IncAutomodHit(models.AutomodCategorySpam, string(action))
	return automodDecision{Action: action, Category: models.AutomodCategorySpam, NextRate: next}, nil
}

// actionFor maps the contract's rule action onto the engine's verdict.
func actionFor(action string) automodAction {
	if action == models.AutomodActionRemove {
		return automodRemove
	}
	return automodBlock
}

// nextRateState computes the counter state after accepting one message,
// resetting the window when it has elapsed (a member cannot stay muted by a
// stale window).
func nextRateState(current *models.MemberRate, input automodInput, rules models.SpamRules) *models.MemberRate {
	window := time.Duration(rules.WindowSeconds) * time.Second
	state := &models.MemberRate{
		CommunityID:     input.CommunityID,
		UserID:          input.UserID,
		WindowStartedAt: input.Now,
	}
	if current != nil {
		state.WindowStartedAt = current.WindowStartedAt
		state.MessageCount = current.MessageCount
		state.RecentFingerprints = append([]string(nil), current.RecentFingerprints...)
	}
	if current == nil || input.Now.Before(state.WindowStartedAt) || input.Now.Sub(state.WindowStartedAt) >= window {
		state.WindowStartedAt = input.Now
		state.MessageCount = 0
		state.RecentFingerprints = nil
	}
	state.MessageCount++
	state.RecentFingerprints = append(state.RecentFingerprints, fingerprint(input.Content))
	if len(state.RecentFingerprints) > maxFingerprints {
		state.RecentFingerprints = state.RecentFingerprints[len(state.RecentFingerprints)-maxFingerprints:]
	}
	return state
}

// fingerprint normalises content into a stable duplicate-detection key.
func fingerprint(content string) string {
	normalised := strings.ToLower(strings.Join(strings.Fields(content), " "))
	sum := sha256.Sum256([]byte(normalised))
	return hex.EncodeToString(sum[:])
}

// countFingerprint counts how often a fingerprint occurs in the window.
func countFingerprint(fingerprints []string, target string) int {
	count := 0
	for _, candidate := range fingerprints {
		if candidate == target {
			count++
		}
	}
	return count
}

// matchesBannedWord reports whether the word occurs in content. In whole-word
// mode the match must not be embedded in a larger word, so "spam" does not fire
// on "spammer".
func matchesBannedWord(content, word string, wholeWord bool) bool {
	haystack := strings.ToLower(content)
	needle := strings.ToLower(word)
	if needle == "" {
		return false
	}
	offset := 0
	for {
		index := strings.Index(haystack[offset:], needle)
		if index < 0 {
			return false
		}
		position := offset + index
		if !wholeWord || wordBounded(haystack, position, len(needle)) {
			return true
		}
		offset = position + len(needle)
		if offset >= len(haystack) {
			return false
		}
	}
}

// wordBounded reports whether the rune before/after a match is a non-word rune.
func wordBounded(text string, start, length int) bool {
	if start > 0 {
		previous, _ := utf8.DecodeLastRuneInString(text[:start])
		if isWordRune(previous) {
			return false
		}
	}
	end := start + length
	if end < len(text) {
		next, _ := utf8.DecodeRuneInString(text[end:])
		if isWordRune(next) {
			return false
		}
	}
	return true
}

func isWordRune(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) }
