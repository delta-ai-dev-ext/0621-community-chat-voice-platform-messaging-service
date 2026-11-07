# ADR-002: Idempotent message send via `client_message_id`

## Status
Accepted

## Date
2026-01-14

## Context
The reconnect flow (system design flow 3) requires clients to re-send pending
messages with the same `client_message_id` after a dropped connection. Without
server-side dedupe, retries after network failures would duplicate messages — the
single most visible correctness defect in a chat product. The platform-wide
`Idempotency-Key` convention covers generic creation endpoints, but the message
contract specifies a body-carried `client_message_id`.

## Decision
Enforce uniqueness on `(container_id, author_user_id, client_message_id)` via a
partial unique index in PostgreSQL. On conflict, return the original message
unchanged (HTTP 201 with the same body, same `message_version`) instead of creating
a duplicate, and increment `messaging_duplicate_sends_suppressed_total`. Scoping the
key by container and author permits id collision across containers/authors.

## Alternatives Considered
- **`Idempotency-Key` HTTP header** — rejected for sends: the message contract puts
  the key in the body; the header form is still used on `POST /attachments`.
- **Client-side-only dedupe** — rejected: retries happen precisely when the client
  is unsure the first attempt landed.

## Consequences
- Returns-after-drop are exactly-once from the user's perspective even though
  delivery infrastructure is at-least-once.
- A conflicting retry returns the original message rather than an error; clients
  cannot detect the difference, which is the desired behaviour.
- The unique index doubles as the fail-safe backstop if retry storms hit
  concurrently: one winner, the rest read.
