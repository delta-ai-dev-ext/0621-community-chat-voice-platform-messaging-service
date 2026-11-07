package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unreachableDSN points at a closed port so connection attempts fail fast without
// requiring a database for the test suite.
const unreachableDSN = "postgres://messaging:messaging@127.0.0.1:1/messaging?sslmode=disable&connect_timeout=1"

func TestMigrateFailsWhenDatabaseIsUnreachable(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := Migrate(ctx, unreachableDSN)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "migration driver")
}

func TestVersionFailsWhenDatabaseIsUnreachable(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	version, dirty, err := Version(ctx, unreachableDSN)
	require.Error(t, err)
	assert.Equal(t, uint(0), version)
	assert.False(t, dirty)
}

func TestEmbeddedMigrationsArePresent(t *testing.T) {
	t.Parallel()
	entries, err := migrationFiles.ReadDir("migrations")
	require.NoError(t, err)
	assert.NotEmpty(t, entries)

	up, err := migrationFiles.ReadFile("migrations/0001_init.up.sql")
	require.NoError(t, err)
	assert.Contains(t, string(up), "CREATE TABLE messages")
	assert.Contains(t, string(up), "CREATE TABLE event_outbox")

	down, err := migrationFiles.ReadFile("migrations/0001_init.down.sql")
	require.NoError(t, err)
	assert.Contains(t, string(down), "DROP TABLE")
}
