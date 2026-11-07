# messaging_service — Service Architecture

## 1. Responsibilities & Component Boundaries

`messaging_service` owns the platform's hottest write/read path: messages in **text
channels** (community scope) and **private conversations** (DM / group), threads,
reactions, @mentions, image attachments, and **automod enforcement** on the posting
path. It also acts as the **realtime scope oracle** for `realtime_gateway` (API-003).

Boundary rules:

- **No synchronous calls to other services on the hot path.** Authorization inputs
  (community membership, mute state, channel existence, conversation participants,
  automod rule sets) come exclusively from **local PostgreSQL projections** fed by
  consumed Kafka events (EVT-002, EVT-003, EVT-005, EVT-008).
- This service **owns**: message storage, threads, reactions, mention resolution,
  attachment lifecycle and object storage objects, automod *enforcement*.
- This service **does not own**: sanction issuance, automod rule *configuration*
  (`moderation_service`), membership/participants *authority* (`community_service` /
  `social_service`) — it mirrors them.

### Internal components

| Component | Responsibility |
|---|---|
| HTTP API layer | Public endpoints (attachment reservation, message send/read/edit/delete, reactions, thread) and the internal `POST /internal/realtime/user-scopes` (API-003). Verifies access tokens locally against identity's public verification keys. |
| Message service (core) | Idempotent send, edit, soft delete, thread assembly, history paging (cursor-based), reaction bookkeeping. |
| Access enforcement | Per-request membership/mute/participation checks answered from projections only; muted members get `403 member_muted` (with `muted_until` + appeal pointer), removed/banned or non-participants get `403 not_a_member`. |
| Mention resolver | Parses `<@{user_id}>` tokens from content, validates each against the container's membership/participant projection, stores the resolved set on the message and on EVT-009. Invalid tokens are silently dropped. |
| Automod engine | Applies the community's current rule-set projection on every channel message: `block` rules reject with `422 blocked_by_automod` (category in `detail`); `remove` rules accept then remove shortly after (deletion reason `automod`). Spam rate detection uses per-member recent-message counts stored in PostgreSQL. Members whose projected roles intersect the rule set's `exempt_role_ids` are exempt. |
| Attachment service | Presigned-PUT reservation (declared type/size validated), verify-on-send (`HEAD` object: real size ≤ 10 MB and recorded content type within the allowed set), presigned-GET download URLs (short-lived, refreshed on read), 24 h garbage collection of never-attached uploads. |
| Projection consumers | Kafka consumers for `community_events` (EVT-002, EVT-003), `social_events` (EVT-008), `moderation_events` (EVT-005). Deduplicate on `envelope.event_id`, apply only newer entity versions. |
| Event publisher | Emits EVT-009 `message_changed` on `message_events` keyed by `container_id`, via the transactional outbox. |
| Realtime scope oracle | Answers API-003 from the membership and participant projections. |

## 2. Request Flows

### 2.1 Send a channel message (hot path)

1. Authenticate the access token locally (JWT signature against configured public
   verification keys; `sub` = user id). Missing/expired → `401`.
2. Load the channel from the text-channel projection; unknown or non-text → `404`.
3. Look up the caller's membership projection for the channel's community:
   - status `removed` or banned → `403 not_a_member`;
   - `muted_until` in the future → `403 member_muted` (body carries `muted_until` and an appeal pointer).
4. Resolve mentions from `<@{user_id}>` tokens against the same membership projection.
5. Evaluate the automod engine (skip entirely for exempt roles; channel messages only):
   - `block` match → `422 blocked_by_automod` with the rule category. Nothing is persisted.
6. Verify any referenced attachments (see §2.4). A storage failure during verification
   fails the send (`503`) — fail closed for attachment-carrying sends; text-only sends
   are unaffected.
7. In **one PostgreSQL transaction**: insert the message (or return the existing row on
   `client_message_id` conflict), bind attachments, insert mentions, update the spam
   counter, and append the EVT-009 record to the transactional outbox.
8. Commit; the outbox relay publishes the event. The HTTP response returns after commit.

### 2.2 History, threads, edits, deletes, reactions

- **History paging**: newest-first with opaque cursors (`(created_at, id)` pairs,
  base64url-encoded); `before` scrolls back, `after` returns chronological messages
  newer than the cursor for reconnect catch-up. Deleted messages appear as tombstones
  (`content` null, `deletion_reason` set) so threads and positions stay stable.
- **Thread**: replies carry `thread_root_id`; the thread endpoint returns root + reply
  count + paged replies. Replying to a deleted message → `404`.
- **Edit**: author-only (`403 not_author`), rejected on deleted messages
  (`409 message_deleted`); the new content is re-evaluated by the automod engine.
- **Delete**: author-only soft delete; attachments are detached and their stored
  objects removed. Idempotent (`204` again).
- **Reactions**: add/remove are idempotent; the aggregated per-emoji summary is
  returned, and a `reactions_changed` EVT-009 is emitted when the summary changed.

### 2.3 Realtime scope check (API-003)

`realtime_gateway` calls `POST /internal/realtime/user-scopes` (service-token
authenticated, not exposed via public ingress). The service answers from the
membership and conversation-participant projections: active memberships (muted
included, removed/banned excluded) and current conversation participances. Unknown
users yield `200` with empty lists. Read-only and safe to retry.

### 2.4 Attachment lifecycle (S3-compatible storage, private bucket)

1. **Reserve** — `POST /attachments`: validate declared type
   (`image/png|jpeg|gif|webp`) and declared size ≤ 10 MB (`413 image_too_large` /
   `415 unsupported_image_type` otherwise); create a `pending_upload` attachment row
   with a random object key; return a presigned PUT URL (15-min TTL) bound to the
   declared content type.
2. **Verify on send** — referenced attachments must belong to the sender and be in
   `pending_upload`/`verified` state; a `HEAD` on the object confirms the real size ≤
   10 MB and the recorded content type; the attachment becomes `attached` in the send
   transaction. Mismatched or missing objects → `422 attachment_invalid` (or
   `413`/`415` when size/type is wrong).
3. **Serve** — every read and every EVT-009 snapshot carries freshly generated
   presigned GET URLs (short-lived); clients refresh expired URLs via history.
4. **Garbage-collect** — a scheduled sweep deletes `pending_upload` rows older than
   24 h and their stored objects.

## 3. Event Flows

### 3.1 Consumed projections (at-least-once, dedupe on `envelope.event_id`)

| Topic | Event | Applied to |
|---|---|---|
| `community_events` | EVT-002 `community_member_changed` | `community_members` projection (status active/removed, banned, `muted_until`, role ids) — upsert only when `member_version` is newer |
| `community_events` | EVT-003 `community_channel_changed` | `text_channels` projection (created/updated/deleted, text kind only) — upsert only when `channel_version` is newer |
| `social_events` | EVT-008 `conversation_changed` | `conversations` + `conversation_participants` projections (kind, name, current participants) — upsert only when `conversation_version` is newer |
| `moderation_events` | EVT-005 `automod_rules_changed` | `automod_rule_sets` projection (full rule-set snapshot replaced per version) |

Every consumed message is ignored when `envelope.event_id` was already processed
(dedupe table) or when the carried entity version is not newer than the stored one.
Topic partition keys (`community_id`, `conversation_id`) preserve per-entity ordering.

### 3.2 Produced events

EVT-009 `message_changed` on `message_events`, keyed by `container_id`, version 1,
for `created`, `edited`, `deleted`, and `reactions_changed`, carrying the message
snapshot (content, attachments with fresh download URLs, mentioned user ids, reaction
summaries, deletion reason, `message_version`). Events are written to a transactional
outbox in the same transaction as the state change; a relay loop publishes to Kafka
with retries, so a crash between commit and publish never loses the event and a
re-publish is deduped downstream by `event_id`.

## 4. Failure Modes & Resilience

- **Duplicate sends** (client retry after dropped connection): deduplicated by
  `(container_id, author_user_id, client_message_id)`; the original message is returned
  unchanged (`messaging_duplicate_sends_suppressed_total` incremented).
- **At-least-once consumption**: every consumer is idempotent; state changes are guarded
  by entity-version comparisons; the `processed_events` dedupe table backs the check.
- **Outbox relay outage**: sends still succeed (committed locally); events publish when
  Kafka recovers. Consumer lag is monitored (< 1 s p95 target).
- **S3-compatible storage unavailable**: attachment reservation and attachment-carrying
  sends fail with `503`; text-only sends continue.
- **Poison messages**: repeated consumer failures on one message move it to a
  dead-letter handling path (logged with the full envelope, alarm raised) instead of
  blocking the partition indefinitely.
- **Clock/mute edge cases**: mute enforcement compares `muted_until` to request time;
  an expired mute no longer blocks.

## Inter-Service Contracts

This service's slice of the project-level `API_CONTRACTS.md` and `EVENT_CONTRACTS.md`,
copied verbatim. Registry edges involving this service: DEP-003 (provide API-003),
DEP-005 (consume EVT-002), DEP-006→consume EVT-003, DEP-008 (consume EVT-005),
DEP-011 (consume EVT-008), DEP-012 (produce EVT-009).

### API-003 — Realtime subscription scopes (this service PROVIDES; consumer: `realtime_gateway`)

Operation: `POST /internal/realtime/user-scopes` · Auth: per-edge service token,
`Authorization: Bearer` · Internal only (never exposed via public ingress) ·
Idempotency: not required (read-only, safe to retry) · Errors: `400` (malformed body),
`401` (missing or invalid service token), `500`, `503` — all
`application/problem+json` referencing `shared:ErrorResponse`. An unknown user
returns `200` with empty lists.

**Request** (`application/json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["user_id"],
  "properties": {
    "user_id": {"type": "string", "format": "uuid"}
  }
}
```

**Response 200** (`application/json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["user_id", "community_ids", "conversation_ids", "generated_at"],
  "properties": {
    "user_id": {"type": "string", "format": "uuid"},
    "community_ids": {
      "type": "array",
      "items": {"type": "string", "format": "uuid"},
      "uniqueItems": true,
      "description": "Communities where the user is an active member (muted members included; removed/banned excluded)."
    },
    "conversation_ids": {
      "type": "array",
      "items": {"type": "string", "format": "uuid"},
      "uniqueItems": true,
      "description": "Private conversations (direct and group) where the user is a current participant."
    },
    "generated_at": {"type": "string", "format": "date-time"}
  }
}
```

### Event envelope (mandatory, every event this service consumes or produces)

Every message on every topic is a JSON object with exactly two top-level keys:
`envelope` and `payload`. The envelope is mirrored into Kafka message headers
(`trace_id`, `event_type`) for routing and observability; the JSON envelope remains
the source of truth. Delivery is at-least-once; consumers dedupe on
`envelope.event_id` and apply changes idempotently. `trace_id` is never empty — it is
propagated from the originating request or minted at the edge of flows that did not
originate from an HTTP request. When a consumer emits an event because of another
event, it copies `trace_id` and `correlation_id` from the causing event and sets
`causation_id` to the causing `event_id`. A payload schema MUST NOT declare an
envelope field name at its top level.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["event_id", "event_type", "event_version", "occurred_at", "producer", "trace_id", "correlation_id", "causation_id", "schema_ref"],
  "properties": {
    "event_id": {"type": "string", "format": "uuid"},
    "event_type": {"type": "string", "pattern": "^[a-z][a-z0-9_]*$"},
    "event_version": {"type": "integer", "minimum": 1},
    "occurred_at": {"type": "string", "format": "date-time"},
    "producer": {"type": "string", "pattern": "^[a-z][a-z0-9_]*$"},
    "trace_id": {"type": "string", "pattern": "^[0-9a-f]{32}$"},
    "correlation_id": {"type": "string", "format": "uuid"},
    "causation_id": {"type": ["string", "null"], "format": "uuid"},
    "schema_ref": {"type": "string", "pattern": "^EVENT_CONTRACTS\\.md#[a-z][a-z0-9_]*-v[0-9]+$"}
  }
}
```

### EVT-002 — `community_member_changed` v1 (this service CONSUMES; topic `community_events`; producer `community_service`)

Key: `community_id`. Delivery: at-least-once; consumers dedupe on `envelope.event_id`;
apply only if `member_version` is newer.

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["community_id", "user_id", "change", "status", "is_banned", "muted_until", "role_ids", "sanction_id", "member_version"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "user_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["joined", "left", "removed", "banned", "unbanned", "muted", "unmuted", "roles_changed"]},
    "status": {"type": "string", "enum": ["active", "removed"]},
    "is_banned": {"type": "boolean"},
    "muted_until": {"type": ["string", "null"], "format": "date-time"},
    "role_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true},
    "sanction_id": {"type": ["string", "null"], "format": "uuid"},
    "member_version": {"type": "integer", "minimum": 1}
  }
}
```

### EVT-003 — `community_channel_changed` v1 (this service CONSUMES; topic `community_events`; producer `community_service`)

Key: `community_id`. Delivery: at-least-once; consumers dedupe on `envelope.event_id`;
apply only if `channel_version` is newer.

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["community_id", "channel_id", "change", "kind", "name", "position", "capacity", "channel_version"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "channel_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["created", "updated", "deleted"]},
    "kind": {"type": "string", "enum": ["text", "voice"]},
    "name": {"type": "string", "minLength": 1, "maxLength": 100},
    "position": {"type": "integer", "minimum": 0},
    "capacity": {"type": ["integer", "null"], "minimum": 1, "maximum": 10, "description": "Voice channels only; null for text channels."},
    "channel_version": {"type": "integer", "minimum": 1}
  }
}
```

### EVT-005 — `automod_rules_changed` v1 (this service CONSUMES; topic `moderation_events`; producer `moderation_service`)

Key: `community_id`. Delivery: at-least-once; consumers dedupe on `envelope.event_id`;
apply only if `rule_set_version` is newer. The payload is a full rule-set snapshot, so
consumers replace their copy.

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["community_id", "rule_set_version", "spam", "banned_words", "exempt_role_ids"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "rule_set_version": {"type": "integer", "minimum": 1},
    "spam": {
      "type": "object",
      "additionalProperties": false,
      "required": ["enabled", "action", "max_messages_per_window", "window_seconds", "max_duplicate_messages", "max_mentions_per_message", "max_links_per_message"],
      "properties": {
        "enabled": {"type": "boolean"},
        "action": {"type": "string", "enum": ["block", "remove"]},
        "max_messages_per_window": {"type": "integer", "minimum": 1},
        "window_seconds": {"type": "integer", "minimum": 1, "maximum": 3600},
        "max_duplicate_messages": {"type": "integer", "minimum": 1},
        "max_mentions_per_message": {"type": "integer", "minimum": 1},
        "max_links_per_message": {"type": "integer", "minimum": 0}
      }
    },
    "banned_words": {
      "type": "object",
      "additionalProperties": false,
      "required": ["enabled", "action", "match_mode", "words"],
      "properties": {
        "enabled": {"type": "boolean"},
        "action": {"type": "string", "enum": ["block", "remove"]},
        "match_mode": {"type": "string", "enum": ["whole_word", "substring"]},
        "words": {"type": "array", "items": {"type": "string", "minLength": 1, "maxLength": 100}, "maxItems": 1000, "uniqueItems": true}
      }
    },
    "exempt_role_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true}
  }
}
```

### EVT-008 — `conversation_changed` v1 (this service CONSUMES; topic `social_events`; producer `social_service`)

Key: `conversation_id`. Delivery: at-least-once; consumers dedupe on
`envelope.event_id`; apply only if `conversation_version` is newer. The payload always
carries the full current participant list, plus a delta.

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["conversation_id", "kind", "change", "name", "participant_user_ids", "added_user_ids", "removed_user_ids", "conversation_version"],
  "properties": {
    "conversation_id": {"type": "string", "format": "uuid"},
    "kind": {"type": "string", "enum": ["direct", "group"]},
    "change": {"type": "string", "enum": ["created", "participants_changed", "renamed"]},
    "name": {"type": ["string", "null"], "maxLength": 100},
    "participant_user_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "minItems": 1, "maxItems": 10, "uniqueItems": true},
    "added_user_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true},
    "removed_user_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true},
    "conversation_version": {"type": "integer", "minimum": 1}
  }
}
```

### EVT-009 — `message_changed` v1 (this service PRODUCES; topic `message_events`; consumers `realtime_gateway`, `notification_service`)

Key: `container_id`. Delivery: at-least-once; consumers dedupe on
`envelope.event_id`; ordering guaranteed per container; apply only if
`message_version` is newer. On deletion, `content` is null and `attachments` is
empty. Attachment download URLs are short-lived presigned URLs, and clients refresh
them through message history.

**Payload** (the `payload` object)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["message_id", "change", "container_type", "container_id", "community_id", "author_user_id", "thread_root_id", "content", "attachments", "mentioned_user_ids", "reactions", "deletion_reason", "created_at", "edited_at", "message_version"],
  "properties": {
    "message_id": {"type": "string", "format": "uuid"},
    "change": {"type": "string", "enum": ["created", "edited", "deleted", "reactions_changed"]},
    "container_type": {"type": "string", "enum": ["channel", "conversation"]},
    "container_id": {"type": "string", "format": "uuid", "description": "channel_id or conversation_id."},
    "community_id": {"type": ["string", "null"], "format": "uuid", "description": "Set for channel messages; null for conversation messages."},
    "author_user_id": {"type": "string", "format": "uuid"},
    "thread_root_id": {"type": ["string", "null"], "format": "uuid"},
    "content": {"type": ["string", "null"], "maxLength": 4000},
    "attachments": {
      "type": "array",
      "maxItems": 10,
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["attachment_id", "content_type", "size_bytes", "download_url", "url_expires_at"],
        "properties": {
          "attachment_id": {"type": "string", "format": "uuid"},
          "content_type": {"type": "string", "enum": ["image/png", "image/jpeg", "image/gif", "image/webp"]},
          "size_bytes": {"type": "integer", "minimum": 1, "maximum": 10485760},
          "download_url": {"type": "string", "format": "uri"},
          "url_expires_at": {"type": "string", "format": "date-time"}
        }
      }
    },
    "mentioned_user_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true},
    "reactions": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["emoji", "count"],
        "properties": {
          "emoji": {"type": "string", "minLength": 1, "maxLength": 32},
          "count": {"type": "integer", "minimum": 1}
        }
      }
    },
    "deletion_reason": {"type": ["string", "null"], "enum": ["author", "automod", null]},
    "created_at": {"type": "string", "format": "date-time"},
    "edited_at": {"type": ["string", "null"], "format": "date-time"},
    "message_version": {"type": "integer", "minimum": 1}
  }
}
```

## 6. PostgreSQL Database Schema

Owned and defined here. Types are PostgreSQL-native.

### 6.1 Domain tables

```sql
CREATE TABLE messages (
    id                 uuid PRIMARY KEY,
    container_type     text NOT NULL CHECK (container_type IN ('channel','conversation')),
    container_id       uuid NOT NULL,
    community_id       uuid,                       -- set for channel messages, NULL for conversation messages
    author_user_id     uuid NOT NULL,
    thread_root_id     uuid REFERENCES messages(id),
    content            text,                       -- ≤ 4000 chars enforced at the API boundary; NULL when deleted
    client_message_id  varchar(128),
    edited_at          timestamptz,
    deleted_at         timestamptz,
    deletion_reason    text CHECK (deletion_reason IN ('author','automod')),
    message_version    bigint NOT NULL DEFAULT 1,  -- monotonic per message; gates EVT-009 ordering
    created_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT deleted_needs_reason CHECK ((deleted_at IS NULL) OR (deletion_reason IS NOT NULL))
);
-- Idempotent send: one message per (container, author, client_message_id)
CREATE UNIQUE INDEX uq_messages_client_message_id
    ON messages (container_id, author_user_id, client_message_id)
    WHERE client_message_id IS NOT NULL;
-- History paging (newest-first) and catch-up
CREATE INDEX idx_messages_container_created ON messages (container_id, created_at DESC, id DESC);
CREATE INDEX idx_messages_thread ON messages (thread_root_id, created_at) WHERE thread_root_id IS NOT NULL;

CREATE TABLE reactions (
    message_id  uuid NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    user_id     uuid NOT NULL,
    emoji       varchar(32) NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (message_id, user_id, emoji)
);

CREATE TABLE mentions (
    message_id        uuid NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    mentioned_user_id uuid NOT NULL,
    PRIMARY KEY (message_id, mentioned_user_id)
);

CREATE TABLE attachments (
    id                    uuid PRIMARY KEY,
    uploader_user_id      uuid NOT NULL,
    filename              varchar(255) NOT NULL,
    declared_content_type text NOT NULL CHECK (declared_content_type IN ('image/png','image/jpeg','image/gif','image/webp')),
    declared_size_bytes   bigint NOT NULL CHECK (declared_size_bytes BETWEEN 1 AND 10485760),
    object_key            text NOT NULL UNIQUE,
    status                text NOT NULL DEFAULT 'pending_upload' CHECK (status IN ('pending_upload','attached')),
    actual_content_type   text,
    actual_size_bytes     bigint,
    message_id            uuid REFERENCES messages(id),
    created_at            timestamptz NOT NULL DEFAULT now(),
    verified_at           timestamptz
);
CREATE INDEX idx_attachments_gc ON attachments (created_at) WHERE status = 'pending_upload';
```

### 6.2 Local projections (read models owned locally, fed by EVT-002/003/005/008)

```sql
CREATE TABLE community_members (
    community_id   uuid NOT NULL,
    user_id        uuid NOT NULL,
    status         text NOT NULL CHECK (status IN ('active','removed')),
    is_banned      boolean NOT NULL DEFAULT false,
    muted_until    timestamptz,
    role_ids       uuid[] NOT NULL DEFAULT '{}',
    member_version bigint NOT NULL,
    PRIMARY KEY (community_id, user_id)
);
CREATE INDEX idx_memberships_by_user ON community_members (user_id) WHERE status = 'active';

CREATE TABLE text_channels (
    channel_id      uuid PRIMARY KEY,
    community_id    uuid NOT NULL,
    name            varchar(100) NOT NULL,
    position        integer NOT NULL,
    channel_version bigint NOT NULL
);
CREATE INDEX idx_channels_by_community ON text_channels (community_id);

CREATE TABLE conversations (
    conversation_id       uuid PRIMARY KEY,
    kind                  text NOT NULL CHECK (kind IN ('direct','group')),
    name                  varchar(100),
    conversation_version  bigint NOT NULL
);

CREATE TABLE conversation_participants (
    conversation_id uuid NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    user_id         uuid NOT NULL,
    PRIMARY KEY (conversation_id, user_id)
);
CREATE INDEX idx_participants_by_user ON conversation_participants (user_id);

CREATE TABLE automod_rule_sets (
    community_id     uuid PRIMARY KEY,
    rule_set_version bigint NOT NULL,
    spam_enabled         boolean NOT NULL,
    spam_action          text NOT NULL CHECK (spam_action IN ('block','remove')),
    max_messages_per_window integer NOT NULL,
    window_seconds       integer NOT NULL,
    max_duplicate_messages  integer NOT NULL,
    max_mentions_per_message integer NOT NULL,
    max_links_per_message    integer NOT NULL,
    banned_words_enabled boolean NOT NULL,
    banned_words_action  text NOT NULL CHECK (banned_words_action IN ('block','remove')),
    banned_words_match_mode text NOT NULL CHECK (banned_words_match_mode IN ('whole_word','substring')),
    banned_words         text[] NOT NULL DEFAULT '{}',
    exempt_role_ids      uuid[] NOT NULL DEFAULT '{}'
);

-- Spam rate detection: recent-message counts per member (sliding window reset on
-- window boundary; duplicate detection via content fingerprints of recent messages)
CREATE TABLE member_message_rates (
    community_id     uuid NOT NULL,
    user_id          uuid NOT NULL,
    window_started_at timestamptz NOT NULL,
    message_count    integer NOT NULL DEFAULT 0,
    recent_fingerprints text[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (community_id, user_id)
);
```

### 6.3 Infrastructure tables

```sql
-- Consumer-side event dedupe (cleanup job prunes entries older than retention)
CREATE TABLE processed_events (
    event_id      uuid PRIMARY KEY,
    event_type    text NOT NULL,
    processed_at  timestamptz NOT NULL DEFAULT now()
);

-- Transactional outbox for EVT-009 (written in the send transaction, relayed to Kafka)
CREATE TABLE event_outbox (
    id           bigserial PRIMARY KEY,
    topic        text NOT NULL,
    message_key  text NOT NULL,          -- container_id
    envelope     jsonb NOT NULL,
    payload      jsonb NOT NULL,
    published_at timestamptz,
    attempts     integer NOT NULL DEFAULT 0
);
CREATE INDEX idx_outbox_unpublished ON event_outbox (id) WHERE published_at IS NULL;
```

## 7. Stateless Container Operational Model

- Strictly stateless: all durable state lives in PostgreSQL and the S3-compatible
  bucket; replicas ≥ 1, all endpoints and consumers safe for concurrent replicas.
- **SIGTERM/SIGINT**: immediately fail `/readyz` (503), stop accepting new HTTP
  requests, pause and commit Kafka consumer loops, drain in-flight requests within a
  bounded timeout (~20 s), flush any unpublished outbox entries, close the PostgreSQL
  pool, Kafka producer and S3 client, then exit 0.
- **`/healthz`** (liveness): lightweight process check; never fails on transient
  downstream hiccups.
- **`/readyz`** (readiness): verifies the PostgreSQL pool, Kafka
  producer/consumer connectivity, and S3-compatible storage reachability.
- Scheduled jobs (attachment GC, outbox pruning, dedupe-table pruning) run inside the
  process; the 24 h attachment GC sweep is a leader-elected loop safe to run on every
  replica because deletes are idempotent.
- Observability SLOs honored here: send p95 < 200 ms, history page p95 < 150 ms,
  5xx < 0.1 %, projection consumer lag < 1 s p95, end-to-end send → gateway push
  p95 < 500 ms.

## 8. Observability

Business metrics (labels are low-cardinality; never user/community/message ids):

| Metric | Type | Labels |
|---|---|---|
| `messaging_messages_sent_total` | counter | `container_type` (`channel|conversation`) |
| `messaging_messages_edited_total` | counter | — |
| `messaging_messages_deleted_total` | counter | `reason` (`author|automod`) |
| `messaging_reactions_total` | counter | `action` (`add|remove`) |
| `messaging_mentions_total` | counter | — |
| `messaging_attachments_total` | counter | `result` (`accepted|too_large|unsupported_type`) |
| `messaging_automod_hits_total` | counter | `category` (`spam|banned_words`), `action` (`block|remove`) |
| `messaging_duplicate_sends_suppressed_total` | counter | — |

Technical metrics: RED metrics
(`http_requests_total{route,method,status_class}`,
`http_request_duration_seconds{route,method}`), `kafka_consumer_lag{topic,consumer_group}`
for the three consumed topics, and outbox relay depth/age
(`messaging_outbox_unpublished`) with publish-attempt failures logged.

Tracing: `traceparent` / `X-Trace-Id` is read from inbound HTTP requests and continued;
every produced event carries the trace in the envelope (`trace_id`, `correlation_id`,
`causation_id` per §Inter-Service Contracts envelope rules). Structured JSON logs carry
the same `trace_id`.
