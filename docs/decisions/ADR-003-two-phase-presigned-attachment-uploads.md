# ADR-003: Two-phase presigned uploads with verify-on-send for image attachments

## Status
Accepted

## Date
2026-01-14

## Context
Messages can carry image attachments up to 10 MB (`image/png`, `image/jpeg`,
`image/gif`, `image/webp`). The declared type/size at reservation time is not
trustworthy: a client can declare one type and upload another, or upload bytes of a
different size. Yet sends must be fast (p95 < 200 ms), so no bytes may transit the
backend, and oversized or unsupported images must be rejected deterministically.

## Decision
Two-phase flow backed by S3-compatible storage (private bucket):

1. **Reserve** — `POST /attachments` validates the *declared* type and size, stores a
   `pending_upload` row, and returns a presigned PUT URL (15-min TTL) whose
   content-type condition is bound to the declared type.
2. **Verify on send** — referenced attachments must belong to the sender; a
   `HeadObject` check confirms the real object size ≤ 10 MB and the recorded
   (condition-bound) content type; the row flips to `attached` inside the send
   transaction.
3. Downloads use short-lived presigned GET URLs regenerated on every read and on
   every EVT-009 snapshot; never-attached uploads are garbage-collected after 24 h.

## Alternatives Considered
- **Proxying uploads through the backend** — rejected: moves 10 MB bodies through the
  hottest service, harming latency and consuming bandwidth for no authorization gain.
- **Trusting declared metadata** — rejected: a hostile client could attach
  arbitrary non-image content; verify-on-send closes this gap at negligible cost
  (one `HeadObject` per attachment).

## Consequences
- Storage failures during verification fail the send closed (`503`) only for
  attachment-carrying sends; text-only sends are unaffected.
- The GC sweep plus object deletion on message delete keeps the bucket free of
  orphaned objects.
- Real content-type sniffing (magic bytes) is not performed; the presign
  content-type condition plus `HeadObject` is deemed sufficient at launch.
