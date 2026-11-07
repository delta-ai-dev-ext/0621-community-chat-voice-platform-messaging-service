-- 0001_init — messaging_service schema (docs/architecture.md §6).
-- Domain tables, local projections, and infrastructure tables are created
-- together because the service owns all of them and has no pre-existing data.
--
-- One deliberate extension to the documented schema: attachments.idempotency_key
-- backs the `Idempotency-Key` header of POST /attachments (US-001 retry safety).
-- It is additive: the unique index is partial, so attachments created without a
-- client key behave exactly as documented.

CREATE TABLE messages (
    id                 uuid PRIMARY KEY,
    container_type     text NOT NULL CHECK (container_type IN ('channel','conversation')),
    container_id       uuid NOT NULL,
    community_id       uuid,
    author_user_id     uuid NOT NULL,
    thread_root_id     uuid REFERENCES messages(id),
    content            text,
    client_message_id  varchar(128),
    edited_at          timestamptz,
    deleted_at         timestamptz,
    deletion_reason    text CHECK (deletion_reason IN ('author','automod')),
    message_version    bigint NOT NULL DEFAULT 1,
    created_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT deleted_needs_reason CHECK ((deleted_at IS NULL) OR (deletion_reason IS NOT NULL))
);

CREATE UNIQUE INDEX uq_messages_client_message_id
    ON messages (container_id, author_user_id, client_message_id)
    WHERE client_message_id IS NOT NULL;

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
    idempotency_key       varchar(128),
    created_at            timestamptz NOT NULL DEFAULT now(),
    verified_at           timestamptz
);
CREATE INDEX idx_attachments_gc ON attachments (created_at) WHERE status = 'pending_upload';
CREATE UNIQUE INDEX uq_attachments_idempotency
    ON attachments (uploader_user_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

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
    conversation_id uuid NOT NULL REFERENCES conversations(conversation_id) ON DELETE CASCADE,
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
    message_key  text NOT NULL,
    envelope     jsonb NOT NULL,
    payload      jsonb NOT NULL,
    published_at timestamptz,
    attempts     integer NOT NULL DEFAULT 0
);
CREATE INDEX idx_outbox_unpublished ON event_outbox (id) WHERE published_at IS NULL;
