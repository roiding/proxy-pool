# Proxy Pool

A single-node proxy supply service: it finds, validates, and issues proxies to
downstream projects. It does not relay downstream business traffic, and it does
not implement stock-site scraping, login, CAPTCHA, or response-body handling.

This README describes the current implementation. The binding product rules
are in [the repositioning plan, updated 2026-11-02](docs/project-repositioning-plan-2026-11-01.md).
Older architecture documents and old API examples are historical references
where they conflict with that plan.

## Proxy lifetime rules (updated 2026-11-05)

| Supply | Completed validation failure | Successful delivery | Retention |
| --- | --- | --- | --- |
| Network channels (`provider_api`, `site_fetch`, `static_url`, `account_list`) | Temporarily blacklist it in the same source | Temporarily blacklist it in the same source | 30 minutes per fetched batch |
| Uploaded `custom` supply | Remove the candidate | Keep it | Persistent; no automatic cache expiry |

An exact-scope PASS has no separate 30-second expiry for list/upload supply.
Network delivery/failure clears its reusable PASS, not the downloaded row. A
persistent source-scoped blacklist skips the connection during prewarm and final
delivery, including after a restart or another download. The default window is
30 minutes; admins can change `network_reuse_cooldown_ms` in downstream project
configuration (UI: seconds). Zero uses the default, and lowering the setting
does not shorten an existing window. This is separate from project CD.
After the window expires, a candidate must pass validation again before reuse.
An unexpired list snapshot is reused even when every row is blacklisted; manual
refresh does not refetch it. Concurrent fetches collapse into one operation,
and persistent fetch cadence/backoff protects absent or failed snapshots.
Prewarm shares a bounded concurrent pool, takes turns across authorized sources,
and stops at the unique total target. Cancellation is not a proxy failure.
Existing leases, project CD, access checks and source/version guards remain in place.
All enabled, project-authorized network adapters share this lifecycle, including
SCDN. Set `auto_prewarm_target` in a downstream project (0 disables it) to keep
that many validated, unused candidates ready. The bounded scheduler replenishes
gaps through the same concurrent prewarm service and persists its backoff.
Before creating a job, it checks for candidates that can still be validated or
an uncached source whose fetch window is open. Exhausted caches and upload
inventory wait without creating empty jobs. New uploads, newly eligible sources,
cache expiry and cooldown expiry are re-evaluated on later checks; a shortfall
alone does not trigger another job or bypass a source's fetch backoff.
A sticky session is a private assigned snapshot, not reusable network pool stock.

Legacy list/proof TTL configuration fields are kept for loading old configurations;
they no longer control list/upload PASS reuse. `validation.valid_until` is null for
uploads and the batch deadline for ordinary list deliveries, never a fabricated
physical expiry. See [the lifecycle change record](docs/proxy-lifecycle-2026-11-05.md).
The current Huawei deployment and bounded real-network checks are recorded in
[the unified-channel release record](docs/unified-channels-and-console-2026-11-05.md).

## Current Contract

- Downstream projects authenticate with a project API key. A project may hold
  several keys; rotating or deleting one key does not remove its project's CD.
- Project CD is keyed by the stable project and the actual connection
  fingerprint. The stored expiration is fixed at delivery. A later `cd`
  reduction, `cd: 0`, a new key, or a different source cannot shorten it.
- All network adapters use the same bounded 30-minute memory cache and may
  supply acquisition and automatic prewarm. They do not write persistent
  proxy inventory. Assigned-account lists use the same disposable cache with an
  explicit source-side private project scope. Only uploaded custom inventory
  is persistent candidate stock.
- Recent passing evidence may be reused only for the same validation scope
  and current source configuration. Target TLS evidence for one target cannot
  authorize another target; basic validation cannot impersonate TLS.
- A completed failed validation temporarily blacklists a network connection
  in its source; only failed uploaded supply is deleted. Request cancellation
  and total request budget exhaustion do not mark a proxy dead. The persistent
  blacklist survives downloads and restarts until its configured deadline.
- A successful acquire returns HTTP 200 with the delivered items and a
  shortfall reason even when delivery is partial or empty. Invalid request
  fields return 400; authentication failures return 401; authorization
  failures return 403; persistence failures do not masquerade as supply
  shortage.
- `count` defaults to 1 only when omitted. `null`, zero, negative, or
  out-of-range values are rejected. Country entries must be non-empty
  two-letter codes. `source_ids: null` means project scope, while `[]` means
  no source is allowed.
- Sticky mode is opt-in and requires a non-empty `session_id` with `count: 1`.
  `sticky_binding_ttl_ms` owns the logical binding lifetime independently of
  `default_lease_ttl_ms`; delivering a lease never extends the binding. An
  existing binding for that project and session may reuse the connection, but
  a new session does not inherit it. If its cached proof ages out, the service
  re-probes the same bound connection and commits refreshed evidence without
  fetching another proxy or opening another project CD window.

```text
Provider API / static lists / uploaded inventory
                    |
              source adapters
                    |
      project scope + CD + health filters
                    |
          bounded request-time probes
                    |
      atomic lease + project CD + binding
                    |
               JSON response
```

The service cannot promise that a later business request succeeds, that a
rotating gateway keeps the same exit IP, or that a free public proxy remains
available.

## Downstream API

All downstream calls use `Authorization: Bearer <project-key>` (or
`X-API-Key`). Credentials in the URL and old `/v1` aliases are not accepted.

### Acquire

`POST /api/v1/proxies/acquire`

```json
{
  "count": 2,
  "timeout": 30,
  "cd": 600,
  "country": ["CN"],
  "protocols": ["http"],
  "source_ids": ["upload-main"],
  "validation": {"mode": "tls", "target": "api.example.com:443"}
}
```

| Field | Meaning |
| --- | --- |
| `count` | Required positive count if supplied; default 1 only when omitted. Bounded by server config. |
| `timeout` | Whole-request budget in seconds. It covers queueing, refill, probing, source switching, and finalization. |
| `cd` | New-window seconds. Does not shorten existing project CD windows. |
| `country` | Optional two-letter ISO country filters. Unknown observations must be verified before matching. |
| `protocols` | Optional `http` / `socks5` filters. |
| `source_ids` | Optional source scope. Omitted/null uses project scope; `[]` means none. |
| `validation` | Omit for project/global default. `tls` requires `host:port`; only TLS handshake/certificate verification occurs, not a business HTTP request. |
| `mode` / `session_id` | `ordinary` by default. `sticky` requires `session_id` and `count: 1`. |

A result includes `requested`, `delivered`, `missing`, `items`, and a `reason`
only when supply is short. Each item carries the source,
`connection.transport`, host, port, credentials (raw strings; an
authenticated connection always serializes `password`, including `""`), a
convenience `proxy_uri` (URI-encoded userinfo, bracketed IPv6), `observed`
evidence when real measurements exist, `validation` (with the actual
proof-reuse `valid_until`), `lease_expires_at`, and `provider_expires_at`
(`null` when the supplier declared no expiry). All timestamps are UTC
RFC 3339 and credential-bearing responses set `Cache-Control: no-store`.

Sticky responses add `session_id`, `binding_version`, `session_changed`
(always an explicit boolean, including `false`), `binding_expires_at`, and
`change_reason` only on rebind. Ordinary responses omit these session-only
fields entirely.

HTTP proxies may reach HTTPS targets through CONNECT; TLS to the proxy itself
(`transport: https`) is not currently supported.

### Registration Client Integration

The registration tool (注册机) is an independent downstream program; this
service ships no registration/stock business logic. It authenticates with its
own project key exactly like any other consumer. For flows that benefit from
a logical binding it may pass `mode: "sticky"` with a `session_id`; the
program decides how to continue when `session_changed` is true.

Sticky bindings identify this service's logical connection, not a vendor
guarantee of a stable exit IP. The registration client must treat
`session_changed`, lease expiry, and validation evidence as first-class
signals instead of assuming the same exit address for a whole registration
run. Pushing proxies into the pool is a separately authorized operation: a
program that both acquires and uploads uses a project key for acquisition and
its own upload key for imports (below).

### Program Imports (Upload Keys)

External programs push proxies with an upload key (`uk_…`), which authorizes
nothing except imports into its listed sources and reading its own batch
results. It cannot acquire proxies or call any admin endpoint.

- `POST /api/v1/sources/{source_id}/imports` with `text/plain`
  (`http://[user:pass@]host:port` / `socks5://…` lines) or `application/json`
  (`{"items":[{"connection":{...},"provider_expires_at":null}]}`). First
  application returns 201, a replayed batch returns 200.
- `Idempotency-Key` is optional (1–128 characters, scoped to the upload key
  and source). The same key with different content is a 409 conflict.
- The result reports `import_id`, `added`, `updated`, `duplicate`,
  `rejected` (count) and a bounded `errors` array with sanitized messages.
  Structural body errors reject the whole batch; invalid items are reported
  while legal items still append.
- `GET /api/v1/sources/{source_id}/imports/{import_id}` returns a stored
  batch result. Upload keys can read only their own batches on their own
  sources; admins can read all.

Other downstream routes:

- `POST /api/v1/prewarm` creates a durable prewarm job (HTTP 202). Its `count`
  target uses the same `max_count_per_request` safety ceiling as acquire.
- `GET /api/v1/jobs/{id}` and `POST /api/v1/jobs/{id}/cancel` are scoped to the
  owning project.
- `POST /api/v1/leases/{id}/release` and `/feedback` require the key that owns
  the lease. Another key in the same project gets 404.
- `DELETE /api/v1/sessions/{id}` releases a sticky binding without clearing
  its already-effective project CD.

## Admin API and Console

`GET /admin` serves the embedded console; it has no external asset dependency.
The admin session is an HttpOnly, SameSite=Strict cookie, while the master API
token can also authenticate directly. Cookie mutations require the
`X-PP-Admin: 1` header. An explicitly empty or invalid `Authorization` header
never falls back to an admin cookie.

The console separates channels, inventory/static cache, jobs, projects,
sessions, usage, and system status. Channel creation/editing uses labeled form
controls for protocol, batch size, country, URL and page count. Advanced settings
are collapsed; JSON is read-only diagnostics, not a required editing surface.
The form emits the typed nested config the API requires:

- provider: `{"provider":{"protocol":"http","fetch_count":20}}`
- catalogue site: `{"site":{"site_id":"ip89","max_pages":1}}`
- generic static URL: `{"static":{"url":"https://example.com/list.txt","default_transport":"http"}}`
- assigned account: `{"account":{"vendor_id":"webshare","protocol":"http","mode":"direct"},"access":{"project_ids":["project-id"]}}`
- upload channel: `{}` plus upload through its import endpoint

Flat legacy provider/site fields are deliberately rejected. Source updates
use optimistic `config_version`; the explicit delete endpoint directly removes
the source and its inventory. Saving updates the definition; enabled sources
participate in the next authorized automatic-maintenance run.

Project source scope is tri-state. `null` restores unrestricted access, `[]`
is an empty allowlist, and a non-empty array is an explicit allowlist. Project
updates use `config_version` compare-and-swap.

Main admin routes include:

- `GET/POST /api/v1/admin/projects` and
  `GET/PATCH/DELETE /api/v1/admin/projects/{id}`
- `GET/POST /api/v1/admin/projects/{id}/keys`
- `GET/PUT /api/v1/admin/projects/{id}/sources`
- `GET/POST /api/v1/admin/sources`, `GET/PATCH/DELETE /api/v1/admin/sources/{id}`
- `POST /api/v1/admin/sources/{id}/imports`, `/fetch`, `/validate`
- `POST /api/v1/admin/sources/{id}/refresh` (asynchronous list download only)
- `GET /api/v1/admin/sources/{id}/cache`, `/fetch-state`
- `GET/PUT/DELETE /api/v1/admin/sources/{id}/secret` (account-key status/write/clear)
- `GET /api/v1/admin/channel-templates` (curated supplier families; protocol/filter variants within each supplier)
- `GET /api/v1/admin/adapters`, `/sites`, `/jobs`, `/sessions`, `/stats`,
  `/validation`, `/daily`, `/system`
- `GET/PUT /api/v1/admin/settings`; `POST /api/v1/admin/settings/bark/test`
- `GET /api/v1/admin/usage`; `POST /api/v1/admin/usage/refresh`

The admin import endpoint and the upload-key import endpoint share the same
durable importer and persistent inventory. Both accept `text/plain` URI lines
or an `application/json` `items` batch. Limits are 8 MiB per body, 10,000 items
per batch, and 256 bytes per username or password. A batch reports `added`,
`updated`, `duplicate`, `rejected`, and at most 100 sanitized row errors while
the rejected count remains complete. Batch results and idempotency records are
retained for 7 days. See the
[Importer contract](docs/source-contract.md#55-importer-contract) for the exact
wire format and authorization rules.

`GET /api/v1/admin/system` reports the executable version/limits and identifies
the default project. A master-token or admin-session prewarm job is therefore
explicitly shown as belonging to `default`, not to an arbitrary other project.

## Channel Types

The code-owned adapter registry contains:

- `custom`: persistent inventory shared by the admin uploader and upload-key
  API pushes. The durable Importer accepts `lines_v1` text and structured
  `application/json` batches; both are additive and duplicates do not reset
  health or CD. Periodic validation and manual validation are separate
  operations.
- `provider_api`: the SCDN public API. Its returned proxies use the same
  cache, blacklist and prewarm path as the other network adapters. The vendor
  documents a maximum of 20 proxies per API call.
- `site_fetch`: one catalogued public-list site with a code-owned parser.
- `static_url`: a generic bounded HTTP(S) static list.
- `account_list`: Webshare direct mode, reading only already-assigned proxies.
  It does not buy, replace or allocate proxies. Complete pagination is required
  before a new account snapshot replaces the old one.

Fetches are bounded and single-flight per source. Network cache lifetime is
30 minutes; memory limits come from `static_cache_max_entries` and
`static_cache_max_bytes`. The old `static_cache_ttl_ms` field is read for
configuration compatibility but does not override this lifecycle. API and static channel configuration is nested and typed; no old flat
adapter payload is accepted.

### List refresh, privacy and prewarm

The console's template create action sends `enabled: false`. Inspect the
protocol, account and project scope before explicitly enabling a new instance.
An `active` catalog template means its adapter is implemented, **not** that the
current upstream or any proxy has passed live acceptance. Unknown contracts,
unknown SOCKS versions and TLS-intercepting gateways are not enabled by guessing
a parser or disabling certificate verification.

List sources optionally accept:

```json
{
  "site": {"site_id": "openproxylist-http", "max_pages": 1},
  "refresh": {
    "mode": "interval",
    "interval_ms": 300000,
    "cache_ttl_ms": 600000,
    "max_candidates": 1000
  },
  "access": {"project_ids": null}
}
```

- Omitted refresh configuration retains demand refresh and the deployment TTL.
  `mode: "demand"` has no `interval_ms`; interval mode requires an explicit TTL
  at least as long as the interval. Supplied intervals/TTLs are 1 second–24 hours.
- Source `access.project_ids` is a second ceiling in addition to the project's
  allowlist: omitted/null is public, `[]` denies every project, and an explicit
  list permits only those projects. Private source filtering precedes bounded
  selection/refill/Ready pages and is checked again at final delivery.
- Account sources require an explicit non-null private scope. Create them
  disabled, configure the API key separately, then enable. The deployment
  `secret_encryption_key` must have at least 32 bytes. Keys are AES-GCM sealed
  with source/version binding; GET returns status only. PUT/DELETE take a
  `config_version` CAS, invalidate the old cache/proofs, and preserve project CD.
- A refresh job's `ready` field means retained **unvalidated cached records**,
  not live proxies. Refresh jobs are admin-only, including when their project
  ID is `default`. They share durable job limits with prewarm and are interrupted
  after restart rather than silently resumed.
- Prewarm accepts every enabled, project-authorized network adapter and uploaded
  inventory. Slow downloads do not block validation of already loaded channels.
  It probes a bounded project-scoped batch and caches same-scope proof;
  it creates neither a lease nor project CD. A newer FAIL overrides PASS.
  Missing, expired, removed or replaced evidence cannot authorize a delivery.
- Cache limits include at most 1,000 retained candidates per source, 4,096
  globally and 1,024 proof entries per source, in addition to entry/estimated-byte
  limits. Deduplication precedes bounded prefix/reservoir sampling. The byte
  accounting is conservative cache accounting, **not** an RSS guarantee.
- Failed refreshes do not extend the old hard TTL. HTTP 304 retains the batch
  and existing proof ages; a memory replay is reported separately as `cache_hit`.
  Persistent fetch cadence/backoff and account-credential shared rate limits
  survive restart. `Retry-After` is respected.

### One-channel acceptance

OpenProxyList is included under the existing `openproxylist-http` and
`openproxylist-https` IDs, using the respective `http.txt`/`https.txt` endpoints
at [api.openproxylist.xyz](https://api.openproxylist.xyz/). The HTTPS list denotes
the supplier's HTTP CONNECT claim, not TLS-to-proxy support or a pool PASS.

The CLI performs no networking without an explicit selection:

```sh
go run ./cmd/sitecheck -list
go run ./cmd/sitecheck -site openproxylist-http -max-pages 1 -timeout 30 -json
# Explicit, bounded proxy validation; use your own reachable signed Judge.
go run ./cmd/sitecheck -site openproxylist-http -probe 3 \
  -judge https://judge.example.com/v1/judge -timeout 30 -seed 1 -json
```

Default selected-site behavior is fetch/parse only (`fetch_only`). A successful
sample is reported as `sample_has_pass` or `sample_all_pass`, never an all-site
availability guarantee. `-all` is opt-in; SCDN requires a separate `-scdn` call
that may consume vendor quota. A paused endpoint may be diagnostically rechecked
only if its protocol and parser are already known. Reports omit raw candidate
connections, credentials and vendor response bodies. Follow the
[per-channel runbook](docs/channel-acceptance-runbook-2026-11-04.md) for the
console/API path, evidence capture and rejection conditions.

## Validation, Health, and Jobs

Default validation is a signed exit-IP judge probe. Optional target TLS mode
tunnels through the candidate proxy, verifies the target certificate, and then
closes without sending business HTTP bytes. DNS/address policy is applied to
both targets and actual dial destinations.

Source circuits distinguish vendor/API health from proxy quality. Temporary
errors back off; they do not become permanent deletion by count alone.
Explicit proxy authentication failure is the main immediate-delete case.
Country eligibility uses observed exit data; an unobserved candidate may be
probed rather than rejected solely for lacking a country label.

Prewarm jobs are durable and project-scoped. Ready means currently eligible
under the job's evidence window, source/project authorization, filters, and
project CD. It is not a cumulative success count and does not write CD.
Jobs support cancellation, terminal states, and interrupted recovery on
restart. Static-list caches provide bounded public acquisition supply without
persistent inventory rows; API candidate batches remain request-local and
never become public stock.

## Usage Alerts and Bark

Usage accounting has its own SQLite tables on the same database. Secrets are
encrypted with `secret_encryption_key` and never returned in plaintext. An
explicit admin clear is persisted as a sentinel, so a restart cannot silently
resurrect a cleared bootstrap credential or region.

The production Huawei traffic reader is intentionally unavailable until the
official Flexus L billing/measurement API, permissions, accounting period, and
units are verified. The console must show that absence; it must not fabricate
zero usage or claim official billing data.

Usage thresholds only notify. They never stop acquisition, fetching, probing,
or prewarm. Failed daily reads keep the last valid snapshot marked stale. The
scheduler uses a persisted bounded retry backoff, so an already-passed daily
slot cannot become a reader/log/SQLite hot loop; a restart honors that backoff.
Notification outbox rows are deduplicated and use an atomic persistent claim
plus a bounded number of real send attempts. SKIPPED checks (Bark disabled or
unconfigured) do not consume that budget. Bark URLs reject credentials/query
fragments, use HTTPS outside genuine loopback IPs, pin policy-checked DNS
results, and never include the device key in errors.

## Security Boundaries

- Admin/master credentials are separate from downstream project keys. Secrets
  are masked on read; plaintext key tokens are shown once at creation.
- Upload keys (`uk_…`) are a third credential kind: source-scoped import and
  batch-result read access only. They are stored HMAC-hashed, never grant
  acquisition or admin rights, and cannot read project credentials.
- Management endpoints require admin/master identity. Downstream keys can only
  use the delivery surface, and job/session/lease ownership is enforced.
- Source downloads, the judge, target TLS, and Bark use bounded transport
  policies. Private/loopback/link-local/CGNAT/metadata addresses, DNS
  rebinding, redirects, environment proxies, body sizes, and decompression are
  handled explicitly.
- The judge is exposed at `/v1/judge`; it is an internal oracle path, not part
  of the downstream API contract.
- SQLite uses WAL, full synchronous mode, a bounded connection count, and an
  external singleton lock. Back up the database together with WAL, config, and
  the secret-encryption material.

## Build, Test, and Run

```sh
go test ./...
go vet ./...
go test -race ./...
go build -o proxy-pool ./cmd/proxy-pool
cp -n config.example.json config.json
./proxy-pool -config config.json
```

`config.example.json` binds `127.0.0.1:8080` for a native local run. The
container command below cannot reach that loopback listener from the host, so
the container deployment must set `listen_addr` to `0.0.0.0:8080` in its own
config and rely on a host firewall or TLS reverse proxy.

The signed `/v1/judge` endpoint must be reachable by the remote candidate
proxies, so `judge_url` cannot be a localhost address for real remote proxy
validation. Publish only that judge route: use verified HTTPS, or signed plain
HTTP outside loopback with a strong random `judge_secret`. If a trusted
reverse proxy supplies the peer address, set `judge_trust_forwarded` only when
the backend is reachable exclusively through that gateway and the gateway
strips client-supplied `CF-Connecting-IP`/`X-Forwarded-For` before setting
the observed peer. The local reverse proxy must also remove any incoming
`CF-Connecting-IP` header and overwrite `X-Forwarded-For` with the peer it
actually observed. `trusted_proxy_cidrs` does not enforce Judge header trust.
Otherwise leave `judge_trust_forwarded` false and use the direct peer. Keep the
admin console and management API on loopback, a private network, or TLS with
strong admin authentication; they must not be publicly reachable without
protection.

The process creates the parent directories for both `lock_path` and
`database_path` before opening the lock or database. A fresh configuration
therefore starts without a pre-created `data/` directory. On startup, the
central allocator schema is migrated first, then `usage.New` creates/updates
only its own usage tables.

### CI Release Artifact

The `docker` workflow runs formatting, `go vet`, the full normal test suite,
and the race suite before publishing either image or native artifacts. Its
`native-linux-amd64` job produces a statically linked amd64 Go executable with
the SQLite driver included. The Actions artifact is named
`proxy-pool-linux-amd64` and is retained for 14 days; a `v*` tag push also
attaches the same tarball and `.sha256` file to that GitHub release.

The immutable GitHub Actions run ID and commit are the source of truth, not a
filename. Download the artifact, verify its checksum, and inspect the binary:

```sh
sha256sum -c proxy-pool-<version>-linux-amd64.tar.gz.sha256
tar -xzf proxy-pool-<version>-linux-amd64.tar.gz
cd proxy-pool-<version>-linux-amd64
sha256sum -c SHA256SUMS
file proxy-pool
./proxy-pool -version
```

The tarball contains `proxy-pool`, `VERSION`, `REVISION`, and `SHA256SUMS`.
`file` must report an ELF 64-bit x86-64 executable with no dynamic
interpreter; this is the artifact validated by CI. Because this driver is
CGO-backed, the CI path builds the Dockerfile's Alpine/musl `release` stage,
which uses the same static-link checks as the image. Only Linux amd64 is
published; no aarch64 or 32-bit artifact is claimed.

Container build:

```sh
docker build -t proxy-pool .
sudo install -d -o root -g root -m 0755 /etc/proxy-pool
sudo install -o 10001 -g 10001 -m 0440 \
  /absolute/path/to/config.json /etc/proxy-pool/container-config.json
docker run --rm -p 127.0.0.1:8080:8080 \
  -v /etc/proxy-pool/container-config.json:/var/lib/proxy-pool/config.json:ro \
  -v proxy-pool-data:/var/lib/proxy-pool/data proxy-pool
```

The mounted config must be readable by container UID/GID `10001`; the example
keeps a separate `0440` copy instead of changing the protected host config or
making it world-readable. The image creates and owns
`/var/lib/proxy-pool/data`, so a fresh named volume starts writable by `app`.
Publishing the container only on host loopback still requires a public TLS
reverse proxy or load balancer for `/v1/judge`; keep the admin routes on the
private side of that proxy.

The image is Linux amd64 and its Dockerfile asserts the embedded version and
revision. For the 2 CPU / 1 GiB Flexus profile, the native binary under systemd
is the lower-overhead option; Docker is optional. See
[Low-Memory Go Deployment Profile](docs/deployment-go-lite.md) for the install,
backup, upgrade, and rollback procedure.

A successful image or artifact build alone is not acceptance: migration,
fresh-database startup, admin login, acquire/prewarm/session behavior, and
per-channel reports still need explicit verification on the target host.

## Acceptance Status

The earlier frozen source acceptance recorded 97 root audits under both the
normal and race suites (see `docs/acceptance-2026-11-03`); that historical
report is not a live-availability claim for newly added channels. The channel
development and current local verification are recorded separately in
[the adapter/storage plan](docs/channel-adapters-and-storage-plan-2026-11-04.md)
and [the per-channel acceptance runbook](docs/channel-acceptance-runbook-2026-11-04.md).
Release acceptance is still incomplete. Remaining external gates include the official Huawei usage reader
verification, per-channel live acquisition/probe reports, target host
architecture/resource verification, public Judge reachability, and TLS/admin
security checks. The user has separately authorized a deployment to Huawei
Flexus L, but the target SSH host, OS/libc/CPU details, and existing service
topology are not yet known. This repository has not performed that deployment,
and no CI workflow holds production SSH credentials or mutates a target host.

### Curated channel directory (2026-11-05)

The operator directory now admits **14 supplier families** (12 public-list
families, SCDN, Webshare), not one channel for every protocol, anonymity filter,
subsite or same-name repository path. HTTP/SOCKS5 and filter choices live in a
channel's `variants`; they do not multiply the channel count. Each variant has
its own complete configuration. Selecting/saving a template does not fetch.
Existing source IDs, account scopes, cache versions, leases and project CD are
preserved; grouped instance rows are presentation only.

New built-in sources must pass the engineering admission list. TheSpeedX,
Kuaidaili, stale/ambiguous endpoints, unverified commercial APIs and
TLS-intercepting gateways are not offered as new channels. Legacy parser IDs
remain for diagnostics/unchanged existing definitions, not as a bypass for new
creation. `sitecheck -list` shows supplier families; explicit `-all` selects one
primary variant per admitted public supplier. `-site ID` remains an endpoint
level diagnostic. Low-level `/admin/sites` is retained for compatibility, not
the curated creation directory.

Engineering admission is **not live-network acceptance** and does not promise
independent supply or daily free credits. See the
[channel value review](docs/channel-value-review-2026-11-05.md) and
[acceptance runbook](docs/channel-acceptance-runbook-2026-11-04.md).

Local integration validation for this batch is recorded separately in the
[2026-11-05 verification log](docs/channel-local-verification-2026-11-05.md);
it is not public supplier availability or a daily-free-quota claim.

The first **live-network** small-sample run (2026-11-05) is recorded in the
[one-row-per-supplier acceptance summary](docs/acceptance-2026-11-05/channel-summary.md).
No supplier was established as a stable recommendation. Spys produced verified
TLS passes but results varied by sample/time; others had no sample pass, blocked
fetches or missing account credentials. This is distinct from local fixtures.
