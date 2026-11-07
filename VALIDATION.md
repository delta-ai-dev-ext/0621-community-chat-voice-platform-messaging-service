# Validation report

Every check below was executed in this repository with `make` and `go test`. The
suite needs no PostgreSQL, Kafka or object-store endpoint: repositories, storage
and the broker are replaced by the fakes in `tests/mocks`, and the only socket
traffic is a deliberate connection to a closed local port to assert fail-closed
error mapping.

## Commands

| Command | Result |
| --- | --- |
| `make lint` (`gofmt -l`, `go vet ./...`) | pass, no output |
| `make test` (`go test ./... -race -count=1 -parallel 4`) | pass, all packages `ok` |
| `make cover` (suite + coverage gate) | pass, `total coverage: 80.3% (threshold 80%)` |
| `make build` (`CGO_ENABLED=0 … ./cmd/server`) | pass, `bin/messaging_service` produced |

Package results from `make test`:

```
ok  messaging_service/internal/database      1.014s
ok  messaging_service/internal/eventbus      1.777s
ok  messaging_service/internal/health        1.021s
ok  messaging_service/internal/objectstore   6.555s
ok  messaging_service/internal/observability 1.017s
ok  messaging_service/internal/repository    1.012s
ok  messaging_service/tests/integration      1.430s
ok  messaging_service/tests/unit             2.087s
```

## Story coverage

| Story | Evidence |
| --- | --- |
| US-001/US-002 send to channel/conversation | `TestSendChannelMessageEndpoint`, `TestConversationEndpoints/participant_posts` |
| US-003 auth + membership failures | `TestSendChannelMessageAccessFailures`, `TestSendChannelMessageEndpoint/maps_a_mute_to_403_with_the_appeal_pointer` |
| US-004 idempotent send (ADR-002) | `TestSendIsIdempotent` (no second outbox event) |
| US-006/007 history + threads | `TestHistoryPaging`, `TestThreadPaging`, `TestHistoryEndpoint` |
| US-009/010 edit/delete | `TestEditMessage`, `TestDeleteMessage`, `TestThreadEditDeleteAndReactions` |
| US-011 reactions | `TestReactions`, `TestThreadEditDeleteAndReactions/reactions_add_and_remove` |
| US-012 mentions | `TestMentionResolution`, `TestMessageHelpers` |
| US-013/014 automod block/remove | `TestAutomodBlocksBannedWords`, `TestAutomodRemoveAcceptsThenTombstones`, `TestAutomodSpamThresholds` |
| US-015 attachments (ADR-003) | `TestReserveAttachment`, `TestSendVerifiesAttachments`, `TestSendBindsVerifiedAttachments` |
| US-016 pending-upload GC | `TestSweepPendingUploads`, `TestSweptAttachmentCannotBeBound` |
| US-017 realtime scopes (API-003) | `TestInternalScopesEndpoint`, `TestScopeOracle` |
| Projection consumers | `TestProcessorAppliesMembershipAndDedupes`, `TestProcessorSkipsReplayedEvent`, `TestProcessorChannelAndAutomodEvents`, `TestProcessorConversationEvent` |
| Outbox relay | `TestRelayPublishesAndMarksEntries`, `TestRelayStopsAtFirstFailureToPreserveOrder`, `TestRelayFlushDrainsBacklog` |
| Ops behaviour | `TestProbes` (liveness vs readiness, shutdown flips readiness), `TestTraceContinuationAndMetrics` |

## Not exercised here

- Live PostgreSQL/Kafka/S3 round trips; migration SQL is asserted by
  `TestEmbeddedMigrationsArePresent` and the DDL is applied at startup by
  `internal/database.Migrate`.
- `cmd/server` shutdown sequencing is wired but only the probe semantics are
  asserted in tests.

## Deliberate divergence from the spec

`attachments.idempotency_key` plus the partial unique index
`uq_attachments_idempotency` is an additive column so the documented
`Idempotency-Key` header has storage. `docs/architecture.md` §6 does not define
that column; `docs/` was left untouched.
