# Implementation Handoff At 23:00 - 2026-11-02

The user requires three implementation subagents. Before 23:00 Asia/Shanghai
they used the temporary `oai-freestyle-cc/deepseek-v4.1-flash` channel. After
23:00 remaining implementation must use `zcode/GLM-5.3-Flash`. Root keeps its
independent code-audit and regression-test role, not production implementation.

The authoritative requirements are the current 32 rules, including R29-R32,
in `docs/project-repositioning-plan-2026-11-01.md`. Read sections 6.2-6.5 and
10.1 again; the former 28-rule frozen implementation is not the new contract.

## Safety And Ownership

- Shared dirty worktree, HEAD `52b69c2`. Do not revert unrelated work or alter
  actual configuration, database, passwords, tokens, or model routing.
- No stage, commit, push, deployment, purchases, signups, or new real vendor,
  Huawei, or Bark calls. Test with isolated local fixtures only.
- Root owns `tests/integration/audit_*`, `flow_test.go`, `correctness_test.go`,
  `docs/refactor-audit-2026-11-01.md`, `browser.md`, and this handoff. Do not
  modify or weaken them.
- The protected manifest is `/tmp/proxy-pool-protected-audits-20261102.sha256`:
  15 files, all independently checked. Root added `audit_wire_test.go`; the
  original 14 entries remain unchanged. There are 84 top-level root audits.
- Preserve internal Go result fields used by existing tests when changing the
  public wire contract. No legacy wire aliases are required or desired.

## Core Agent

Own application acquire/sticky/lease/keys and core SQLite allocation,
delivery, evidence, projects, sessions, and main migration registration.

1. Fix C47/C48 before expanding the response: root's
   `TestAuditStickyProbeSaturationDoesNotDestroyPrivateBinding` reproduces
   expired-proof slot saturation turning an ACTIVE binding into an EXPIRED
   empty binding, and caller cancellation becoming `ALLOCATION_STORE_UNAVAILABLE`.
   Inconclusive budget/cancellation must preserve the recoverable binding, CD,
   identity and proof, and return an explained empty/partial result. Once slots
   are available, the original connection must freshly validate without another
   vendor batch. Do not treat every refresh 4xx as a rebind trigger.
2. Reproduce canceled first-claim cleanup: `FailStickyClaim(overallCtx)` uses
   an already-canceled context and may leave a dead `CLAIMING` owner to its old
   roughly 120-second deadline. Cleanup must remain owned, bounded and safe.
3. R31/R32: supply actual provider expiry, proof validity, observed evidence,
   and durable binding expiry to the result; do not substitute lease/cache TTL
   for unknown provider expiry. Coordinate with admin's final wire serializer.
4. Register the new schema helpers from supply/admin without both editing
   `store.go`. Preserve existing CD, sources, credentials and schema idempotency.
5. Complete bounded local synthetic load, saturation, cancellation and restart
   evidence in `core-load.md`. Its current status is CHECKPOINT ONLY, not accepted.
   Actual main shutdown must contain a deliberately slow valid active request;
   a completed/empty workload proves nothing about that boundary.

## Supply Agent

Own ingestion, static cache/refill, prewarm/nightly/health, related SQLite,
integrations/validation/sitecheck and own tests.

1. R29/R30: implement one durable append/merge import service for file upload,
   text API push and JSON batches. Reuse the existing inventory transaction,
   preserving health, incarnation and CD on duplicate/repeated imports. An
   import does not create PASS evidence or pool-verified observations.
2. Support raw JSON credentials, URI-encoded text credentials, IPv4/IPv6/name
   addresses and known RFC3339 provider expiry; unknown expiry is null.
3. Stable `import_id`, added/duplicate/rejected counts and bounded sanitized
   errors. Whole-body structural errors reject the batch; invalid items are
   reported while legal items append. No passwords in error text.
4. `Idempotency-Key` is OPTIONAL in section 10.1. Without it, apply a new
   durable batch normally. When present, bound it and scope it to upload
   identity plus source. Same content returns the same ID/results; different
   content is 409. Inventory and result/idempotency writes are one transaction.
   Concurrent retries cannot double-apply; publish retention and size limits.
5. Coordinate API service/result signatures directly with admin. Suggested
   result fields: import_id, source_id, status, added, updated, duplicate,
   rejected (count), errors (bounded array), created_at. Internal legacy Go
   slices can remain internal; do not expose a truncated list as the only count.
6. Reproduce the reported `TestStaticRefreshSingleFlight` SOURCE_RATE_LIMITED
   failure with bounded repeated normal/race tests; fix a demonstrated bug,
   not the protected tests.
7. Complete a human report from saved `live-channel-check.json`, without more
   live calls. Existing evidence is 11 Catalog + 1 SCDN, 29 channel requests,
   32 TLS probes, zero PASS. Disclose the accidental extra sweep and second
   SCDN HTTP call. This is genuine failed live acceptance, not fixture success.

## Admin Agent

Own API/admin HTML/config/usage/main/README/Docker/CI and new upload-key
application/storage files. Coordinate core/supply files rather than editing them.

1. R30 chosen key management routes: POST/GET `/api/v1/admin/upload-keys`,
   PATCH/DELETE `/api/v1/admin/upload-keys/{id}`. Create body has name and
   source_ids. Return plaintext only once, persist a hash, mask list output,
   and disable/revoke without changing other keys.
2. Upload credentials are separate from project acquisition keys and have
   no admin, acquisition, job, source creation or credential-reading authority.
   POST `/api/v1/sources/{source_id}/imports` accepts text/plain or application/json;
   GET `/api/v1/sources/{source_id}/imports/{import_id}` is source-scoped and
   owner-only for upload credentials, while admin may query. Source permission
   must fail closed before applying the batch. First apply 201, replay 200.
3. Do not make Idempotency-Key mandatory. Expose rejected COUNT plus bounded
   errors, not only a list. Use supply's common durable service for both admin
   file entry and external push. Existing Server constructors must activate
   this capability, not only a special main option used by no test/server.
4. R31 final JSON: reason only; connection.transport http/socks5; correctly
   encoded proxy_uri with IPv6 brackets; raw credentials and explicit empty
   password when authentication requires it; provider_expires_at always present,
   null if unknown; validation.valid_until; actual binding_expires_at; UTC
   RFC3339 and Cache-Control no-store. Sticky top-level session_changed is an
   explicit boolean, including false. No duplicate item session fields;
   ordinary responses omit session-only fields. Coordinate actual times with core.
5. R32 document the registration client as an independent project using its
   project key and optional sticky session, with separately authorized uploads.
   Do not implement registration/stock business or promise stable exit IP.
6. Reproduce main's in-flight shutdown before changing lifecycle. It currently
   lacks shutdown-linked HTTP BaseContext and background task joins. Core owns
   canceled CLAIMING cleanup; preserve existing ACTIVE binding and CD on restart.
7. Cloud/Bark are NOT_CONFIGURED per prior masked read-only checkpoint; no
   usage tables exist in the actual configured DB. Do not initialize/migrate
   it. Finish official Flexus L package-balance research and `cloud-bark.md`;
   generic CES traffic estimates are not an official package balance.

## Root Evidence And Browser Boundary

- `/tmp/proxy-pool-audit-new-wire-initial.log`: both new root wire audits fail
  on the old public response (missing fields, aliases, UTC offsets, omitted
  sticky false, duplicate item session status and missing no-store).
- `/tmp/proxy-pool-protected-audits-20261102.sha256`: current root protection.
- A fresh current binary is running only as an owned isolated UI fixture at
  `127.0.0.1:64128`; metadata is `/tmp/proxy-pool-reboot-ui-meta.json`.
- Reboot and the user's default-allow browser change did not clear the saved
  origin denial. Automated browser observation/input is BLOCKED. Do not retry
  another browser/port/CDP/rendering surface to work around it. Root handles
  permission diagnosis separately; backend tests do not count as UI passes.
- The prior pre-reboot snapshot persists at
  `/Users/roiding/Documents/ChatGPT/proxy_pool-audit-backups/proxy-pool-before-reboot-kp1h07et`.

Send a concrete early checkpoint, make scoped fixes with your own tests, then
freeze for root's independent full normal/race/vet and actual-main verification.
Do not spawn additional agents; the user requested three implementers.
