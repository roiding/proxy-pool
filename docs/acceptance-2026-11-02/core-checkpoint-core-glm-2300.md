# Core Agent Checkpoint - 2026-11-02 23:15 Asia/Shanghai

Status: implementation starting. Shared worktree HEAD 52b69c2, dirty state
preserved. Protected manifest re-verified before any edit: 15/15 OK.

## C47/C48 Reproduced And Root-Caused

`go test ./tests/integration -run
'^TestAuditStickyProbeSaturationDoesNotDestroyPrivateBinding$' -count=1`
reproduces both protected failures against current production code:

- probe_budget: an ACTIVE v2 private binding becomes an EXPIRED v3 row with
  fingerprint, ciphertext, source, and proof cleared.
- caller_cancellation: `ALLOCATION_STORE_UNAVAILABLE: context deadline
  exceeded` instead of an explained empty result.

Root cause: `acquireSticky` and `waitForStickyClaim` treat every non-5xx
`deliverStickyReuse` failure as a rebind trigger. An inconclusive proof
refresh (`VALIDATION_BUDGET_EXHAUSTED` from probe-slot starvation or caller
cancellation) is a 409, so it converts the healthy binding to CLAIMING, the
rebind then fails inconclusively, and `FailStickyClaim` leaves EXPIRED. On the
cancellation path the follow-up `ClaimStickyRebind`/store calls run on the
already-canceled context and surface as a 5xx.

## Core Fix Plan

1. Inconclusive refresh (budget exhaustion or caller cancellation) returns an
   explained empty `VALIDATION_BUDGET_EXHAUSTED` result without claiming a
   rebind, so the binding, CD, identity, and proof stay recoverable.
2. Canceled claim cleanup calls `FailStickyClaim` on a short detached context;
   the SQL keeps its owner+version+CLAIMING guards, so cleanup stays owned,
   bounded, and safe. Store calls are guarded with early context checks.
3. Once slots free, reuse of the preserved binding re-probes the original
   connection without another vendor batch.

## R31 Core DTO Coordination (for admin)

Core DTO changes in `internal/application`: `AcquiredItem` gains
`proxy_uri` and `provider_expires_at` (always serialized, null unknown,
actual offer expiry never a lease/cache TTL); item-level session fields stay
as Go fields but become `json:"-"`; `AcquireResult.ShortfallReason` becomes
`json:"-"` so only the wrapper's `reason` reaches the wire;
`AcquireResult` gains `binding_expires_at` from the durable binding deadline;
delivered evidence gains `valid_until = completed_at + MaxProofAge` and all
delivered times are normalized to UTC.

Admin serializer note: keep the acquire wrapper emitting `reason` from
`ShortfallReason`, set `Cache-Control: no-store`, and emit sticky top-level
`session_changed` as an explicit boolean (use `result.SessionChanged`; it
stays a plain bool because protected tests read it directly). Ordinary
responses must continue to omit all session fields.

Store note: `StickyReuseRetention` is extended to also return the enabled
offer's provider expiry for sticky reuse items; the only caller is the sticky
reuse path, so no cross-agent schema helper registration is needed for it.
Migration registration for supply/admin schema helpers will be picked up from
their files when they land; `store.go` migration list will be reconciled
without duplicating edits.

## Update - 00:40 Asia/Shanghai

C47/C48 fix is implemented and the protected saturation audit passes
(`go test ./tests/integration -run
'^TestAuditStickyProbeSaturationDoesNotDestroyPrivateBinding$' -count=1`:
ok). The canceled-claim cleanup now runs on a short detached context while
keeping the store's owner+version+CLAIMING guards.

Coordination finding for admin: the rewritten `writeAcquireResponse` matches
the core DTO well (proxy_uri, provider_expires_at pointer, valid_until with
delivery-supplied fallback, explicit sticky booleans, no-store). Two wire
fields are still missing and two protected audits read them from the HTTP
body, so they fail against the current draft:

1. `resource_id` on each item - `TestAuditStickyReusePersistsCorrectLeaseIdentity`
   and `TestAuditStickyReuseKeepsInventoryResourceIdentity` compare the wire
   item resource identity against the stored lease/binding.
2. `validation.outcome` (and profile) - `TestAuditStickyReuseNeverServesSupersededProof`
   requires `validation.outcome == "PASS"` from the wire.

The service layer emits both correctly (verified with temporary debug output:
full identity and full evidence reach `writeAcquireResponse`), so only the
serializer mapping needs them. `internal/application/imports.go` (supply) is
also not gofmt-clean; leaving it to its owner.

## Freeze Note - 2026-11-03 05:10 Asia/Shanghai

Core implementation is frozen for root's independent gates. Landed in core
scope: C47/C48 saturation/cancellation fix, bounded owned canceled-claim
cleanup, R31 core DTO (proxy_uri, provider_expires_at, valid_until,
binding_expires_at, UTC normalization, alias-free tags), StickyReuseRetention
provider-expiry extension, faithful Connection JSON marshaler, three new core
integration tests plus DTO unit tests, and the completed actual-main
shutdown/restart evidence in `core-load.md`. Supply/admin schema helpers
(import_batches, import_idempotency, upload_keys) self-heal inside their own
transactions, so no `store.go` registration was required. Remaining
cross-agent failures and the exact serializer field list are documented above
and in `core-load.md`.
