package models

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Cursor is the opaque position of a message inside a newest-first timeline.
// It mirrors the (created_at, id) tuple used by the history indexes and is
// rendered to clients as base64url so the ordering key stays an implementation
// detail. Timestamps are truncated to microseconds, matching PostgreSQL
// `timestamptz` precision, so an encode/decode round trip is lossless.
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// NewCursor builds a cursor from a stored message position.
func NewCursor(createdAt time.Time, id uuid.UUID) Cursor {
	return Cursor{CreatedAt: createdAt.UTC().Truncate(time.Microsecond), ID: id}
}

// Encode renders the cursor as an opaque base64url token.
func (c Cursor) Encode() string {
	raw := fmt.Sprintf("%d|%s", c.CreatedAt.UTC().UnixMicro(), c.ID.String())
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeCursor parses an opaque cursor token. Any malformed token yields
// ErrInvalidCursor so the handler edge can answer 400.
func DecodeCursor(token string) (Cursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	parts := strings.Split(string(decoded), "|")
	if len(parts) != 2 {
		return Cursor{}, ErrInvalidCursor
	}
	var micros int64
	if _, err := fmt.Sscanf(parts[0], "%d", &micros); err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	if micros <= 0 {
		return Cursor{}, ErrInvalidCursor
	}
	return Cursor{CreatedAt: time.UnixMicro(micros).UTC(), ID: id}, nil
}
