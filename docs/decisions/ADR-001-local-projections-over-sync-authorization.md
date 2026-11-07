# ADR-001: Enforce access from local projections instead of synchronous authorization calls

## Status
Accepted

## Date
2026-01-14

## Context
Every message send must be gated by community membership, mute state, channel
existence, conversation participation, and the community's automod rule set. These
facts are authoritative in `community_service`, `social_service`, and
`moderation_service`. The send path is the platform's hottest write path with a
send p95 < 200 ms and end-to-end send → gateway push p95 < 500 ms target, and the
system design explicitly assigns messaging "access enforcement from local
projections (no sync calls on the hot path)".

## Decision
Consume EVT-002 (`community_member_changed`), EVT-003 (`community_channel_changed`),
EVT-008 (`conversation_changed`), and EVT-005 (`automod_rules_changed`) into local
PostgreSQL projections and answer every access/automod question from those
projections. Envelope dedupe (`event_id`) plus entity-version guards
(`member_version`, `channel_version`, `conversation_version`, `rule_set_version`)
make the projections converge and tolerate at-least-once redelivery.

## Alternatives Considered
- **Per-send synchronous permission checks against the owning services** — rejected:
  adds cross-service latency to every send, couples the hottest path to peer
  availability, and contradicts the system design.
- **Central shared database read by messaging** — rejected: violates service
  ownership boundaries and couples schema evolution across services.

## Consequences
- Membership/mute/automod changes take effect within projection consumer lag
  (< 1 s p95 target) rather than instantly; the mute `muted_until` comparison at
  request time bounds stale-mute impact.
- The outbox + idempotent send design ensures enforcement decisions remain
  consistent with what was actually persisted.
- API-003 scope answers are likewise served from projections, keeping the
  gateway-connect path dependency-free.
