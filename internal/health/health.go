// Package health implements the liveness and readiness probes. Liveness never
// touches a dependency; readiness checks PostgreSQL, Kafka and storage and flips
// to 503 as soon as shutdown begins.
package health

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Check verifies one dependency. A nil error means healthy.
type Check func(ctx context.Context) error

// Checker owns the dependency checks and the shutdown flag.
type Checker struct {
	mu       sync.RWMutex
	checks   map[string]Check
	timeout  time.Duration
	draining atomic.Bool
}

// NewChecker builds a checker. The timeout bounds each individual check.
func NewChecker(checks map[string]Check, timeout time.Duration) *Checker {
	owned := make(map[string]Check, len(checks))
	for name, check := range checks {
		owned[name] = check
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Checker{checks: owned, timeout: timeout}
}

// BeginShutdown flips readiness to failing before in-flight work drains.
func (c *Checker) BeginShutdown() { c.draining.Store(true) }

// Draining reports whether shutdown has begun.
func (c *Checker) Draining() bool { return c.draining.Load() }

// Ready runs every dependency check and reports the failed ones. It returns
// false immediately once shutdown has begun, so the load balancer stops routing
// before the listener closes.
func (c *Checker) Ready(ctx context.Context) (bool, map[string]string) {
	if c == nil {
		return true, nil
	}
	if c.draining.Load() {
		return false, map[string]string{"service": "shutting_down"}
	}
	c.mu.RLock()
	checks := make(map[string]Check, len(c.checks))
	for name, check := range c.checks {
		checks[name] = check
	}
	timeout := c.timeout
	c.mu.RUnlock()

	failures := make(map[string]string)
	for name, check := range checks {
		checkCtx, cancel := context.WithTimeout(ctx, timeout)
		err := check(checkCtx)
		cancel()
		if err != nil {
			failures[name] = err.Error()
		}
	}
	return len(failures) == 0, failures
}

// Add registers an additional check (used by tests and optional dependencies).
func (c *Checker) Add(name string, check Check) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checks[name] = check
}
