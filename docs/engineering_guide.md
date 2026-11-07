# engineering_guide.md — messaging_service

Persistent technical memory for this service. New decisions append rows to
`## Pinned Decisions`; existing rows are never silently replaced.

## Runtime & Toolchain

- **Language / runtime:** Go 1.26 (module path `messaging_service`, initialised with
  `go mod init messaging_service`; internal imports reference the module name directly —
  never a `github.com/...` host prefix).
- **Package manager:** Go modules (`go.mod` / `go.sum` in sync; add with
  `go get <module>@latest`, finish with `go mod tidy`; never hand-edit).
- **Build/test entrypoints (Makefile):** `make install` (`go mod download`, always run
  first on a cold workspace), `make test`
  (`go test ./... -race -count=1 -parallel 4`), `make build` (`docker build`,
  executed by CI — no Docker daemon in the agent runtime), `make run`
  (`go run ./cmd/server`), `make lint` (`golangci-lint run ./...`).
- **Container image:** multi-stage build `golang:1.26-alpine` → distroless
  (`gcr.io/distroless/static-debian12`), `CGO_ENABLED=0`, non-root `USER nonroot:nonroot`.

## Libraries & External Provider SDKs

| Concern | Package / Library | Source | Usage Notes |
|---|---|---|---|
| HTTP framework | `github.com/gin-gonic/gin` | language-skill | Gin context for binding/validation; routes grouped public vs `/internal/*` middleware chains. |
| Request validation | Gin binding (built on `go-playground/validator`) | language-skill | Validate at HTTP boundary only; `binding:"required,max=4000"` style tags plus custom rules for image types. |
| PostgreSQL driver | `github.com/jackc/pgx/v5` | language-skill | Pool via `pgxpool`; parameterized queries only — never `fmt.Sprintf` into SQL. |
| Migrations | `github.com/golang-migrate/migrate/v4` | language-skill | Versioned SQL migrations; run at startup before serving. |
| Kafka client | `github.com/IBM/sarama` | language-skill | One shared consumer group per topic set (`community_events`, `social_events`, `moderation_events`); commit offsets only after the idempotent apply + dedupe write succeeds; producer used by the outbox relay. |
| S3-compatible storage client | `github.com/aws/aws-sdk-go-v2/service/s3` (+ `aws-sdk-go-v2/config`, `aws-sdk-go-v2/credentials`) | language-skill | Point at any S3-compatible endpoint via `S3_ENDPOINT_URL`; use the Presign client for PUT (bound to declared content type) and GET URLs; `HeadObject` for verify-on-send. |
| JWT (access-token verification) | `github.com/golang-jwt/jwt/v5` | tech-lead-research | Verified locally: parse with the configured identity public key(s) (current + next key for rotation), validate `exp`/`iss`/`aud`; claims `sub`, `sid`, `email_verified`. Latest stable v5.x (v5.3.1 at time of writing). |
| Logging | `go.uber.org/zap` | language-skill | Structured JSON logger injected via DI; never log secrets or message content. |
| Metrics | `github.com/prometheus/client_golang/prometheus` | language-skill | Register business + RED metrics at startup; low-cardinality labels only. |
| Environment loading | `github.com/joho/godotenv` | language-skill | Local development only; production reads plain env vars. |
| Testing | standard `testing` + `github.com/stretchr/testify` | language-skill | `testify/assert`, `require`; table-driven tests with `t.Parallel()`. |
| Mocking | `github.com/stretchr/testify/mock` | language-skill | Mock repositories, producer/publisher, presigner interfaces. |
| HTTP mocking | `github.com/jarcoal/httpmock` | language-skill | Only if a typed HTTP client is ever introduced; SDK calls are mocked behind interfaces instead. |

There are **no external third-party provider SDKs** for this service (its external
surface is the S3-compatible storage client above).

## Container Lifecycle & Probes

- **Statelessness:** no local disk or in-memory cross-request state; every replica can
  serve any request and run any consumer partition. Subscription scope answers (API-003)
  come from PostgreSQL projections only.
- **`SIGTERM`/`SIGINT` handling (order matters):**
  1. Flip `/readyz` to `503` immediately (load balancer stops routing).
  2. Stop accepting new HTTP requests; pause Kafka consumer loops and commit offsets.
  3. Drain in-flight HTTP requests within a bounded shutdown timeout (~20 s, configurable).
  4. Flush the outbox relay (best-effort publish), close the PostgreSQL pool, Kafka
     producer, and S3 client; exit `0`.
  Implement with `signal.NotifyContext` + `http.Server.Shutdown(ctx)`.
- **`GET /healthz`** (liveness): process responsiveness only; MUST NOT probe PostgreSQL,
  Kafka, or storage.
- **`GET /readyz`** (readiness): PostgreSQL pool ping, Kafka connectivity, S3-compatible
  storage reachability; returns `503` while shutting down.
- Scheduled in-process jobs: attachment GC sweep (pending uploads > 24 h), outbox
  unpublished-entry retry, `processed_events` pruning. All idempotent and replica-safe.

## Testing Strategy

- **Unit tests** (`tests/unit`): cover every business branch — idempotent send dedupe,
  mute/not-a-member gating, mention resolution, automod block/remove and exemption,
  attachment verification paths (413/415/422/503), cursor paging (before/after,
  tombstones), reaction idempotency, envelope construction, outbox relay retries.
  All external dependencies mocked: repository interfaces via `testify/mock`,
  Kafka producer/consumer via fake publisher/consumer doubles,
  S3 via a `Presigner`/`ObjectStore` interface stub, no real network calls.
- **Integration tests** (`tests/integration`): HTTP handlers + service + repository
  against mocked/in-process implementations of PostgreSQL access (repository mocks or
  in-memory fakes), broker mocks, and storage mocks; assert full request/response
  behaviour including error codes. MUST NOT rely on real infrastructure or network.
- **Determinism:** inject the clock (mute windows, GC deadlines, cursors) — no
  `time.Sleep`-based assertions; no shared mutable state between tests; all tests run
  in parallel (`t.Parallel()`).
- **Contract fixtures:** sample EVT-002/003/005/008 envelopes and payloads are stored as
  test fixtures and asserted against the consumed schema contracts; EVT-009 emissions
  are asserted against its contract schema.
- Coverage target ≥ 80 % with focus on the send path and automod engine.

## Coding Conventions & Project Structure

```text
cmd/server/main.go          # entrypoint: config, DI, router, consumers, graceful shutdown
internal/config/config.go   # typed Config struct from env, validated at startup
internal/handler/           # thin HTTP handlers (public + internal), auth middleware
internal/service/           # business logic: messages, automod, attachments, scopes, mentions
internal/repository/        # pgx-backed persistence + projection upserts
internal/models/            # domain types, request/response structs, event envelope types
internal/eventbus/          # consumer wiring, outbox relay, envelope construction
internal/objectstore/       # presigner / object-store abstraction over the S3 client
tests/unit/, tests/integration/, tests/mocks/
Makefile, Dockerfile, go.mod, go.sum
```

- Layering: handlers → services → repositories; services never import `gin`.
- Errors: wrapped sentinel/domain errors mapped to HTTP codes at the handler edge; never
  panic outside init; error responses always `{"error": "<code>", "detail": "<human text>"}`.
- Typing: explicit types for all exported symbols with Go doc comments; no `any` leakage
  across layers; envelope modeled as one reusable struct — never map-literals at call sites.
- SQL: parameterized only; projections updated with version-guarded upserts
  (`WHERE stored.version < incoming.version`).
- No TODOs, no commented-out code, no debug leftovers; comments explain *why*.

## Configuration & Secrets Management

12-factor env vars, validated at startup (fail fast with a structured log error):

| Variable | Purpose |
|---|---|
| `PORT` | HTTP listen port |
| `DATABASE_URL` | PostgreSQL connection string (portable) |
| `KAFKA_BROKERS` | Comma-separated broker list; optional `KAFKA_SASL_*`/TLS vars |
| `S3_ENDPOINT_URL`, `S3_REGION`, `S3_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` | S3-compatible storage (private attachments bucket) |
| `IDENTITY_ISSUER`, `IDENTITY_AUDIENCE` | Access-token `iss`/`aud` validation |
| `IDENTITY_PUBLIC_KEY` / `IDENTITY_NEXT_PUBLIC_KEY` | Current + next verification keys from deployment configuration (rotation support); PEM/JWK material |
| `INTERNAL_SERVICE_TOKEN` | Expected bearer token for `/internal/*` endpoints (API-003) |
| `ATTACHMENT_MAX_BYTES` | Default `10485760` (10 MB) |
| `UPLOAD_URL_TTL`, `DOWNLOAD_URL_TTL` | Presign TTLs (defaults 15 m) |
| `GC_PENDING_AFTER` | Default `24h` |
| `SHUTDOWN_TIMEOUT` | Default `20s` |

Secrets never enter the repo; `.env` is local-dev only and gitignored.

## Observability & Logging Conventions

- **Structured JSON logging** (zap): `timestamp`, `level`, `service_name=messaging_service`,
  `request_id`, `trace_id`, `operation`, `status`; error logs include stack/context, never
  message content or secrets.
- **Trace propagation:** read `traceparent`/`X-Trace-Id` on inbound HTTP and continue it;
  every produced event carries the trace in the envelope; consumed envelopes' `trace_id`
  is adopted for downstream logs of that message.
- **Prometheus metrics:** business metrics (`messaging_messages_sent_total{container_type}`,
  `messaging_messages_edited_total`, `messaging_messages_deleted_total{reason}`,
  `messaging_reactions_total{action}`, `messaging_mentions_total`,
  `messaging_attachments_total{result}`, `messaging_automod_hits_total{category,action}`,
  `messaging_duplicate_sends_suppressed_total`), RED metrics
  (`http_requests_total{route,method,status_class}`,
  `http_request_duration_seconds{route,method}`), and
  `kafka_consumer_lag{topic,consumer_group}`. Low-cardinality labels only.
- **Mandatory event envelope:** every produced or consumed message is
  `{"envelope": {...}, "payload": {...}}`. The envelope carries exactly the nine fields
  `event_id`, `event_type`, `event_version`, `occurred_at`, `producer`, `trace_id`,
  `correlation_id`, `causation_id`, `schema_ref` (see `docs/architecture.md` §Inter-Service
  Contracts for the schema). `trace_id` is **never empty** — propagated from the
  originating HTTP request or minted at the edge of flows with no originating request
  (attachment GC sweep). `causation_id` is `null` only for the first event of a flow.
  A payload MUST NOT carry an envelope field at its top level. Produce via the outbox so
  state change and event write share one transaction.

## Pinned Decisions

| Dependency / Area | Pin / Constraint | Rationale | Added By | Date |
|---|---|---|---|---|
| JWT verification library | `github.com/golang-jwt/jwt/v5` | De-facto standard for Go JWT validation incl. EdDSA/RSA with key rotation; verified current (v5.3.x) | tech-lead | 2026-01-14 |
| Event publication | Transactional outbox table, no direct produce from request path | Guarantees send-commit and EVT-009 emission are atomic under crashes; re-publishes are deduped by `event_id` | tech-lead | 2026-01-14 |
