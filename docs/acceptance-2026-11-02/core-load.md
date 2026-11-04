# Core Load and Shutdown Acceptance

Status: **CORE EVIDENCE UPDATED 2026-11-03 05:00 Asia/Shanghai**. The core
saturation/cancellation fix, canceled-claim cleanup, and actual-main
shutdown/restart evidence below were produced locally. Final acceptance
still requires root's independent full normal/race/vet run.

## Scope and Safety

- Source baseline: shared worktree at `52b69c2` with existing uncommitted
  work; all three implementers' changes are included as of this update.
- All exercised traffic is local synthetic traffic using a local judge, a
  local TLS judge front, a slow local CONNECT fixture, and temporary
  databases/configurations under `/tmp/core-main-evidence/`.
- No actual user database, configuration, credential, supplier quota, Huawei
  Cloud, or Bark request was used. Cloud/Bark stayed unconfigured.
- No stage, commit, push, deployment, or production configuration change was
  made.
- Protected manifest `/tmp/proxy-pool-protected-audits-20261102.sha256`
  re-verified 15/15 OK after the fixes (final re-check at freeze).

## Core Fix Verification

The production fix classifies inconclusive sticky proof refreshes
(`VALIDATION_BUDGET_EXHAUSTED` from probe-slot starvation or caller
cancellation) as explained business shortfalls instead of rebind triggers,
and releases failed claims through a short detached context that keeps the
store's owner+version+CLAIMING guards. Executed:

```sh
sha256sum -c /tmp/proxy-pool-protected-audits-20261102.sha256
go test ./tests/integration -run \
  '^TestAuditStickyProbeSaturationDoesNotDestroyPrivateBinding$' -count=1
go test ./tests/integration -run \
  'TestCoreCanceledFirstClaimReleasesClaimBounded|TestCoreConcurrentStickyRefreshUnderSaturation|TestCoreSaturatedStickyRefreshPreservesRecoverableBinding' \
  -count=1
go test ./internal/application -run \
  'TestProxyURIFrom|TestDeliveryEvidence|TestConnectionJSON' -count=1
```

All four runs returned ok. The root-owned saturation audit now passes both
subtests: the probe-budget path and the caller-cancellation path preserve the
ACTIVE binding (version, fingerprint, ciphertext, proof, deadline), the
project CD, and source health, and recovery re-probes the original connection
without another vendor batch.

New core-owned regressions in `tests/integration/core_load_test.go`:

- Canceled first claim: the CLAIMING row is released (EXPIRED, claimable)
  within the bounded cleanup window instead of living out its claim deadline,
  and a later request recovers the session.
- Concurrent sticky proof refreshes under one probe slot: both requests
  return explained shortfalls, the binding stays ACTIVE with its original
  version/fingerprint/ciphertext/deadline, and the project CD is unchanged.
- DTO unit coverage: `proxy_uri` encoding (reserved characters, explicit
  empty password, bracketed IPv6), UTC `valid_until` proof validity, and the
  connection JSON rule that authenticated connections show their actual
  password while anonymous connections invent none.

## Actual Main Shutdown And Restart Matrix

The locally built `cmd/proxy-pool` binary ran with an isolated config
(`127.0.0.1:64990`, temporary SQLite path, embedded judge behind a local TLS
front, `allow_private_addresses` true, cloud/Bark unconfigured). Supply
chain: one custom source with one imported local proxy; the CONNECT fixture
delays every tunnel by 3 seconds, so a valid acquire stays in flight ~3.2s.

1. In-flight shutdown: a sticky first claim (`cd=600`, timeout 15s) was
   fired; 1.3s in, the `sticky_bindings` row was `version=1 owner=req_...
   state=CLAIMING`. `kill -TERM` then produced:
   - main exited after 0.143s (graceful, bounded);
   - the in-flight HTTP request received `200` with the canonical explained
     empty result: `{"requested":1,"delivered":0,"missing":1,"reason":
     "VALIDATION_BUDGET_EXHAUSTED","items":[]}` - not a 5xx;
   - after shutdown the claim row was `state=EXPIRED` (released by the
     bounded, owner-checked cleanup instead of waiting out the deadline) and
     `leases WHERE state='ACTIVE'` was 0 (no half-committed delivery).
2. Restart 1: no stale `CLAIMING` rows; a fresh sticky first claim completed
   and committed `version=3 state=ACTIVE` with fingerprint `5ddccd5d...` and
   deadline `2026-11-02T20:43:11Z` (10-minute binding TTL), plus one
   `project_proxy_cd` row for the same fingerprint.
3. Shutdown 2: `kill -TERM` exited after 0.090s; the binding stayed
   `3|ACTIVE`, the CD row stayed, and the `CLAIMING` count was 0.
4. Restart 2: the sticky reuse delivered the same connection with explicit
   `"session_changed":false`, `"binding_version":3`, and three independent
   deadlines on the wire (`validation.valid_until` 20:39:20Z,
   `lease_expires_at` 20:39:17Z, `binding_expires_at` 20:43:11Z). The proof
   had aged past 30s, so the reuse revalidated the same connection through
   the slow fixture (`latency_ms=3160`) instead of rebinding, and the stored
   binding row stayed unchanged (same version/fingerprint/deadline). A later
   maintenance sweep logged `expired project cooldown sweep cleared:1` once
   the window lapsed.

## Full Suite Status At This Checkpoint

`go test ./tests/integration -count=1` currently reports five failures, none
in core-owned production paths:

- `TestAuditStickyReuseNeverServesSupersededProof`,
  `TestAuditStickyReusePersistsCorrectLeaseIdentity`,
  `TestAuditStickyReuseKeepsInventoryResourceIdentity`, and
  `TestAPIEndToEnd` (protected): the service layer emits full identity and
  evidence (verified with temporary debug output), but the new API wire
  serializer (admin-owned `internal/api/acquire_v1.go`) does not yet map
  `resource_id` on items, `validation.outcome`, or
  `validation.observed_exit_ip`, which these audits read from HTTP bodies.
  The coordination note lists the exact fields for admin.
- `TestSiteFetchChannelViaAPI` (supply-owned): second fetch expected 429 but
  got 200 - single-flight/rate-limit behavior in the supply path.

One flake was observed once for `TestCoreSaturatedStickyRefreshPreserves...`
(first-claim probe budget exhausted under heavy parallel compilation load);
it passed on all re-runs including the full-suite run.

## Limits

All results are local-machine synthetic measurements. They do not validate
Huawei target-server capacity, real network behavior, free-proxy survival, or
performance for 5,000-stock downstream workloads. Bounded goroutine/I-O
quantification beyond the concurrent-saturation test above remains open for
root's independent run.
