package health

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadyReportsFailures(t *testing.T) {
	t.Parallel()
	checker := NewChecker(map[string]Check{
		"postgres": func(context.Context) error { return nil },
		"kafka":    func(context.Context) error { return errors.New("broker down") },
	}, time.Second)

	ready, failures := checker.Ready(context.Background())
	assert.False(t, ready)
	assert.Equal(t, "broker down", failures["kafka"])
	assert.NotContains(t, failures, "postgres")
}

func TestReadyHandlesTimeouts(t *testing.T) {
	t.Parallel()
	checker := NewChecker(map[string]Check{
		"slow": func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
				return nil
			}
		},
	}, 10*time.Millisecond)

	ready, failures := checker.Ready(context.Background())
	assert.False(t, ready)
	assert.Contains(t, failures, "slow")
}

func TestReadyDuringShutdown(t *testing.T) {
	t.Parallel()
	checker := NewChecker(map[string]Check{
		"postgres": func(context.Context) error { return nil },
	}, time.Second)

	ready, _ := checker.Ready(context.Background())
	require.True(t, ready)
	assert.False(t, checker.Draining())

	checker.BeginShutdown()
	assert.True(t, checker.Draining())
	ready, failures := checker.Ready(context.Background())
	assert.False(t, ready)
	assert.Equal(t, "shutting_down", failures["service"])
}

func TestAddRegistersChecks(t *testing.T) {
	t.Parallel()
	checker := NewChecker(nil, 0)
	ready, failures := checker.Ready(context.Background())
	assert.True(t, ready)
	assert.Empty(t, failures)

	checker.Add("storage", func(context.Context) error { return errors.New("unavailable") })
	ready, failures = checker.Ready(context.Background())
	assert.False(t, ready)
	assert.Contains(t, failures, "storage")
}

func TestNilCheckerIsReady(t *testing.T) {
	t.Parallel()
	var checker *Checker
	ready, failures := checker.Ready(context.Background())
	assert.True(t, ready)
	assert.Nil(t, failures)
}
