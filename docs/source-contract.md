> **统一渠道与自动补足更新（2026-11-05）**：所有已启用、项目有权使用的网络渠道（含 SCDN `provider_api`）统一使用 30 分钟内存缓存、持久临时黑名单和并发预热。项目 `auto_prewarm_target` 控制自动维持数量；0 关闭。本文下方较早的 provider 仅请求内生存等口径已被替代。管理台改为可视化表单。详见 `unified-channels-and-console-2026-11-05.md`。

> **2026-11-05 最新生命周期规则优先于本文旧 TTL 口径**：网络代理验活失败/成功下发后，同一来源默认临时禁用 30 分钟，不再预热或下发；黑名单持久化，下游项目的 `network_reuse_cooldown_ms` 可配置时长。网络列表整批缓存 30 分钟，有效缓存期间不重复拉取。自上传代理永久保存，只有实际验活失败才删除。列表/上传 PASS 无独立 30 秒失效。项目权限、准确验证范围、配置版本与 CD 检查不变。详见 `proxy-lifecycle-2026-11-05.md`。

# Source Integration Contract

Version: 0.9 (requirements clarified 2026-11-02). Status: the durable custom
Importer is implemented with `lines_v1` text, structured custom JSON,
source-scoped upload keys, batch idempotency, and result queries. The first
provider adapter family (`provider_api` + `json_mapping_v1`, the proxy.scdn.io
get_proxy API) is implemented under
`internal/integrations/scdn/`. Further provider families follow the same
one-channel-at-a-time path. Source-level circuits, demand refill, prewarm, and
offline exit-IP/geo observations are implemented in the shared acquisition
path.

Product rules follow [the repositioning plan, updated 2026-11-02](project-repositioning-plan-2026-11-01.md).
File uploads and API pushes are equivalent inputs to the same persistent
custom inventory. Static URL lists retain their bounded TTL cache, while
on-demand provider API candidates remain request-local. The custom Importer
accepts text and structured JSON through both the admin and upload-key entry
points and enforces the same authorization, inventory, and retention rules.

This contract allows formats to be added one channel at a time without changing
the acquisition, validation, lease, or eviction workflows. The older
[architecture specification](architecture.md) is a reference where it agrees
with the repositioning plan; illustrative parser families below do not imply
implemented support.

## 1. Separate the Three Boundaries

```text
Source adapter          Parser                 Normalizer
-----------------       ------------------     --------------------------
How to obtain data      How to read records     What the records mean
API auth and cadence    JSON / lines / CSV      Transport and auth identity
Vendor errors/billing   Fields and delimiters   Expiry/session semantics
Pagination/batches      Row-level errors        Tenant/network policy
       |                       |                         |
       +----- RawBatch --------+---- ParsedRecords ------+---- SourceOffers
```

One source adapter type can use multiple parser versions. Multiple providers
can share a simple parser. Complex vendor authentication/session logic belongs
in a provider module, not in a giant generic parser with hidden conditionals.

The normalizer is shared. A provider module may supply declared semantic
conversions, but it cannot write leases, clear health history, or return proxies
directly to downstream clients.

## 2. Adapter Interface

Illustrative Go interfaces, not implemented declarations. Domain types are
defined during implementation; keep the normalized record schema at version 1.

```go
type SourceAdapter interface {
	Capabilities() SourceCapabilities
	Fetch(ctx context.Context, scope FetchContext, req FetchRequest) (RawBatch, error)
}

type Parser interface {
	Parse(ctx context.Context, batch RawBatch, cfg ParserConfig,
		emit func(ParsedRecord) error) (ParseResult, error)
}

type Normalizer interface {
	Normalize(ctx context.Context, record ParsedRecord,
		scope NormalizeContext) (NormalizedCandidate, error)
}
```

The parser emits records incrementally rather than returning a second full
in-memory record slice. `ParseResult` summarizes accepted/rejected counts,
bounded diagnostics, and complete-snapshot status. An `emit` error or context
cancellation stops parsing and propagates to the ingestion operation.
Adapter packages are statically registered Go code, not runtime plugin processes.

`FetchContext` contains the source configuration/version, tenant, secret
resolver, bounded deadline, authorized filter scope, budget reservation, and
fetch-operation/idempotency identifiers. It MUST NOT expose arbitrary downstream
URLs or allow a caller to change the source's tenant.

`FetchRequest` expresses desired candidate count and compatible source-supported
filters. Unsupported filter translation is explicit; the adapter must not
pretend that a provider enforced a filter it ignored.

### Source capabilities

Declare at least:

- Supported proxy transports and destination-capability semantics.
- `static_endpoint`, `sticky_gateway`, and/or `rotating_gateway` resource kinds.
- Proxy authentication mode and session-binding method, if any.
- Pagination, complete-snapshot versus additive-batch semantics, and record IDs.
- Expiry representation, time zone/unit, relative-TTL anchor, and uncertainty.
- Minimum fetch interval, provider QPS/account limits, and extraction quota.
- Whether fetch consumes money or credits and whether it is safely idempotent.
- Supported filters, regional claims, and restrictions on client sharing/use.

Unsupported capabilities are rejected, not silently simulated by the pool.

## 3. Raw Batch and Parsing Results

`RawBatch` includes:

| Field | Contract |
| --- | --- |
| `source_id`, `configuration_version`, `fetch_operation_id` | Bind provenance and prevent mixing configuration generations |
| `received_at` | UTC response/import receipt time; not automatically the provider's resource issue time |
| `format`, `parser_version`, `payload` | Explicit bounded representation; credentials must never enter ordinary logs |
| `batch_mode` | `DELTA` or `SNAPSHOT` |
| `snapshot_id`, `generation`, `cursor` | Vendor/import ordering and pagination metadata where available |
| `complete` | True only when the whole authoritative snapshot has been fetched |
| `next_fetch_not_before`, `rate_limit_reset_at` | Source cadence and backoff instructions |
| `billing_result` | Reserved/consumed/unknown cost or quota outcome |

Valid records are emitted to the ingestion service. `ParseResult` contains counts,
completion status, and bounded structured row errors. Parse failures have record
position, safe error code, and redacted diagnostic information.

- Set maximum response bytes, record count, nesting, and per-field lengths.
- Use bounded readers and typed Go records. Avoid unrestricted `io.ReadAll`,
  whole-pool `[]map[string]any` materialization, and unbounded diagnostic slices.
- Handle vendor HTTP `200` error envelopes before parsing proxy records.
- Isolate malformed rows for a delta import, but do not claim an invalid partial
  snapshot is authoritative. Replacement requires complete parse success.
- Fetch all snapshot pages before atomically revoking missing offers. A failed
  later page must not erase records from the previous valid snapshot.
- Stage larger snapshots in bounded SQLite batches keyed by generation rather
  than keeping all pages in memory. Do not advertise `complete=true` until every
  required page and row has been consumed and validated successfully.
- Avoid general-purpose expressions, `eval`, downloaded plugin code, or implicit
  format guessing. Use bounded declarative mappings and registered parser code.
- Secrets can exist transiently in authorized ingestion memory, but must be
  converted to encrypted/immutable references before persistent candidate storage.

## 4. Normalized Candidate Schema

Illustrative schema-v1 record. Documentation address ranges are not eligible
production proxies under the default public-address policy.

```json
{
  "schema_version": 1,
  "source_id": "vendor_a",
  "source_record_id": "provider-item-001",
  "source_generation": "batch-001",
  "connection": {
    "transport": "http",
    "host": "203.0.113.10",
    "port": 8080,
    "credential_ref": "secret://proxy/vendor-a/account-1/v1",
    "dns_mode": "proxy"
  },
  "resource_kind": "static_endpoint",
  "session_policy": {
    "mode": "static"
  },
  "declared_capabilities": ["http_forward", "https_connect"],
  "provider_expires_at": "2026-10-31T04:02:00Z",
  "local_retention_deadline": null,
  "expiry_basis": "provider",
  "claims": {
    "country": "US",
    "exit_type": "datacenter"
  },
  "policy": {
    "max_pool_leases": 1,
    "capacity_group": "vendor-a-account-1"
  }
}
```

### Field rules

| Field | Rules |
| --- | --- |
| `source_id` / tenant | Taken from the authorized source context; payload cannot overwrite scope |
| `source_record_id` | Preserve stable vendor identity if available; otherwise derive a source-local canonical association |
| `connection.transport` | Explicit registered transport; defaults only when declared in source configuration |
| `connection.host` / `port` | Canonical DNS/IP and integer `1..65535`; bracketed IPv6 parsed correctly |
| `credential_ref` | Immutable secret version; absence means explicit no-auth, not missing unknown credentials |
| `dns_mode` | Applies to destination resolution; adapters must declare actual transport support rather than assume SOCKS behavior |
| `resource_kind` / `session_policy` | Map known vendor semantics; generated sticky tokens are bound later to concrete validation/lease contexts |
| `provider_expires_at` | UTC timestamp only when upstream semantics support it; otherwise `null` |
| `local_retention_deadline` | Optional pool storage/selection deadline; never represented as guaranteed provider lifetime |
| `expiry_basis` | `provider`, `pool_policy`, or `unknown`; does not promise uninterrupted liveness |
| `claims` | Source claims with provenance, not observations independently verified by the pool |
| `policy` | Source/operator constraints, bounded by deployment/tenant policy; payload cannot relax global limits |

If `expires_in` is relative, the adapter MUST specify whether it starts at vendor
issue time, extraction time, or response time. If that cannot be determined,
preserve unknown physical expiry and apply a separately named local policy.
Do not convert an ambiguous number into a guessed timestamp.

Credential rotation, session-policy changes, and transport changes create a new
connection identity/context version. A stale provider response cannot overwrite
a newer source generation or reset its administrative disable state.

## 5. Initial Format Families

Implement only families backed by real sample inputs:

| Parser family | Example | Required explicit configuration |
| --- | --- | --- |
| `lines_v1` | One `host:port` or supported proxy URI per line | Default transport, accepted URI syntax, auth defaults, TTL/retention policy |
| Structured custom JSON | `items` array containing `connection` and optional `provider_expires_at` | Same normalized connection schema and additive inventory semantics as text imports |
| `json_mapping_v1` | Array or nested provider list | Item path, field names, error envelope, protocol semantics, expiry units/zone |
| `csv_mapping_v1` | Header-based rows | Encoding, delimiter, columns, time semantics, credential fields |
| Vendor parser module | Signed/encoded/nested proprietary format | Versioned fixtures, semantic conversion, limits, error handling |

Custom resource entry points are normalized through the same ingestion service
and write to the same persistent inventory:

- Authorized programmatic text push or file-content upload, using `lines_v1`.
- Structured JSON candidate batches, using the schema in
  [the import requirements](project-repositioning-plan-2026-11-01.md#101-文件上传与-api-推送).
- Configuration-owned static endpoint/gateway definitions.

Clients submit file contents, not arbitrary server filesystem paths. Files and
API pushes may use the same source or different authorized sources; the entry
method does not create a separate pool, connection identity, or CD scope.

Supported line forms should be declared, not inferred endlessly. URI examples
may include `http://host:port`, `socks5://host:port`, percent-encoded
authenticated URIs, and bracketed IPv6. A bare `host:port` requires a
source-configured transport. Credentials with reserved characters require
proper encoding or structured fields.

Custom additive semantics: each line's source-record ID is
the content fingerprint of its transport/host/port/credentials, so uploads are
append-only and identical re-updates preserve source relationships, health,
and project CD. This applies equally when the same connection arrives through
a file and an API push. There is no snapshot replacement. Explicitly failed
connections are removed under the plan's failure classification, without
tombstones; repeated uncertain timeouts alone do not establish permanent
failure. New imports begin unvalidated. The shared custom inventory receives
bounded periodic revalidation, and delivery still checks current eligibility.

Static URL downloads use the configured bounded downloader and separate cache;
cache expiry never deletes file/API-imported inventory. Executable "custom
fetch scripts", webhook callbacks, provider reservation/release APIs, and
unknown formats remain outside the initial contract.

### 5.5 Importer Contract

The admin entry (`POST /api/v1/admin/sources/{source_id}/imports`) and the
upload-key entry (`POST /api/v1/sources/{source_id}/imports`) call the same
durable importer. Both accept exactly one of:

- `text/plain`: one `http://[user:pass@]host:port` or
  `socks5://[user:pass@]host:port` URI per line. Blank lines and lines starting
  with `#` are ignored. A bare `host:port` uses the source's configured default
  transport.
- `application/json`: one object containing an `items` array. Each item has a
  `connection` object and may include a UTC RFC 3339 `provider_expires_at`.

```json
{
  "items": [
    {
      "connection": {
        "transport": "http",
        "host": "203.0.113.10",
        "port": 8080,
        "username": "user",
        "password": "p@ss"
      },
      "provider_expires_at": null
    }
  ]
}
```

`transport` is exactly `http` or `socks5`. Unknown envelope fields, a trailing
JSON value, and a missing or non-array `items` reject the whole batch. A
malformed item, including an unknown item field or invalid connection, is
isolated with a sanitized row error while valid items still append. Raw
credentials are never echoed in errors.

The published limits are:

| Resource | Limit |
| --- | --- |
| Request body | 8 MiB |
| Items per batch | 10,000 |
| JSON username or password | 256 bytes each |
| Stored error details | 100 entries per batch; `rejected` remains complete |
| Batch result and idempotency retention | 7 days |
| Idempotency-Key | 128 characters |

Upload keys are the only non-admin credentials accepted by the program entry.
They are bound to an explicit source allowlist and may push only there. Their
batches and result queries are scoped to the key identity; the key cannot use
the admin entry or acquire proxies. The handler passes the authenticated key ID
to the importer as trusted identity. It never derives authorization from a
request header, uploader label, or other caller-supplied text.

The source row is read for parsing, then its enabled state, custom adapter type,
`config_version`, and the upload key's enabled and source-bound state are
rechecked inside the commit transaction, before idempotency lookup. A key
disabled, deleted, or unbound while the request is in flight therefore rejects
both a new batch and a replay without mutating inventory, batch records, or
idempotency records. A source edit or delete-and-recreate under the same ID
likewise rejects a stale apply/replay.

An optional `Idempotency-Key` is scoped to the authenticated uploader and
source. The same key with the same body hash returns the stored batch and
`import_id`; different content returns 409. The first application returns 201
and a replay returns 200. A result has a stable `import_id`, `source_id`,
`status`, `added`, `updated`, `duplicate`, `rejected`, bounded `errors`, and
`created_at`. Inventory changes and the batch/idempotency records commit in one
transaction. Import success never creates PASS evidence or a verified
observation; new candidates still require normal validation.

## 6. Provider Error Contract

| Error class | Behavior |
| --- | --- |
| `SOURCE_AUTH_FAILED` | Disable or open source circuit until configuration/account remediation |
| `SOURCE_RATE_LIMITED` | Respect explicit reset/cooldown; do not blindly retry within the request |
| `SOURCE_QUOTA_EXHAUSTED` | Stop fetching under that budget; distinguish from endpoint health |
| `SOURCE_TIMEOUT` / `SOURCE_UNAVAILABLE` | Bounded source backoff; existing valid inventory may still serve |
| `SOURCE_PURCHASE_AMBIGUOUS` | Retain budget reservation and reconcile/idempotently retry only if supported |
| `SOURCE_FORMAT_ERROR` | Quarantine/redact diagnostic sample; no snapshot replacement |
| `SOURCE_EMPTY` | Successful fetch with zero candidates; respect cadence instead of spinning |
| `SOURCE_CONFIG_ERROR` | Fail configuration validation; do not start proxy probes |

The adapter reports attribution and retry guidance. It MUST NOT decrement every
proxy's quality score because the provider API returned an error.

Fetch operation state and account budget consumption are durable. A provider
timeout can leave purchase outcome unknown even if local cancellation succeeded;
a second call may cost money again. Lock expiry is not an exactly-once guarantee.

## 7. Source Configuration Shape

The following is a design example, not a deployed YAML schema:

```yaml
schema_version: 1
source_id: vendor_a
adapter_type: http_api_v1
parser_type: json_mapping_v1
enabled: true
api:
  endpoint: https://api.vendor.example/proxies
  method: GET
  authentication_ref: secret://sources/vendor-a/api-key/v1
parser:
  items_path: data.proxies
  fields:
    host: ip
    port: port
    provider_expires_at: expire_time
  expiry_format: rfc3339
  default_proxy_transport: http
resource_kind: static_endpoint
refill:
  mode: demand
  min_interval_ms: 1000
limits:
  max_fetch_qps: 1
  max_candidates_per_fetch: 100
  daily_purchase_budget: 0
policy:
  max_pool_leases: 1
  local_retention_ms: 1800000
```

Here `daily_purchase_budget: 0` disallows paid extraction until an administrator
explicitly authorizes spending. Units/currencies or credit denominators must be
specified for a real billable provider. This API key is separate from proxy
credentials returned or configured for individual resources.

Simple formats should require configuration plus fixtures, not edits to the
core selector. Complex formats should require one registered provider module
and fixtures. Registry entries are application-owned code, not user-downloaded
plugins executed at runtime.

## 8. Per-Channel Onboarding Checklist

For every new channel, collect and commit redacted fixtures describing:

1. Success, empty, auth-error, quota-error, rate-limit, malformed, and partial
   payloads, including HTTP status/headers and application-level error codes.
2. API method/authentication, supported filters, pagination/snapshot semantics,
   record identity/generation, extraction cadence, and charging/idempotency rules.
3. Connection formats, separate proxy authentication, transport-label meaning,
   IPv6/hostname support, and credential rotation.
4. TTL representation, units/time zone/anchor, near-expiry behavior, gateway
   rotation, sticky-session binding, and whether probe traffic consumes credits.
5. Provider/account concurrency, sharing policy, source claims, and use terms.

Contract tests MUST cover:

- Stable normalized identity and distinct identity for different credentials.
- Mixed valid/invalid rows, vendor `200` error envelopes, and oversized data.
- Proper expiry conversion and preservation of unknown TTL.
- Replay/upsert without health reset or revival of disabled entries.
- Complete versus partial snapshot handling and duplicate source provenance.
- Transport/auth/session semantics and safe secret handling.
- Safe bounded error/backoff/billing behavior under timeouts and retries.

Adding a format is complete only when its normalized contract fixtures pass and
the shared acquisition path can fetch/read or import (according to its supply
mode), validate, and lease the connection. A list adapter must not satisfy this
check by quietly copying its disposable candidates into persistent inventory.

## 9. Runtime Channel Contract — 2026-11-04

The current JSON source registry is closed: `custom + lines_v1`,
`provider_api + json_mapping_v1` (SCDN only), `site_fetch + site_v1`,
`static_url + lines_v1`, and `account_list + webshare_list_v1`. Section 7's YAML
and generic mapping/budget examples remain design examples, not accepted API
payloads. No runtime plugins, scripts, arbitrary vendor extraction URL or
user-supplied field-mapping expressions are enabled.

`SourceConfig` is typed, with one of `provider`, `site`, `static`, `account` (or
none for uploads), and optional `refresh`/`access`. The admin console creates
templates disabled. Saving or enabling a definition does not itself fetch or
probe; an explicitly configured interval list is subsequently scheduled.

### List snapshots and authority

- Public files/HTML/JSON remain lists even when their URL contains `/api/`.
  They use bounded per-source memory snapshots. Only uploads create persistent
  candidate stock; on-demand provider batches die with the requesting operation.
- Parsed/fetched, unique, retained and rejected counts are distinct. Error
  details are bounded independently of the full rejection count. Sampling or
  page truncation is explicitly incomplete coverage, not a full-site snapshot.
- The supplier's protocol, anonymity, country, `valid`, update/check timestamp
  and latency are claims, not pool validation evidence. Unknown expiry stays
  null. An HTTP CONNECT/HTTPS label does not mean TLS to the proxy itself.
- A list TTL starts at successful download/parse. Refresh failure never extends
  the old hard deadline. A 304 preserves its incarnation and proof ages, merging
  intervening FAIL/removal state at installation; memory replay is `cache_hit`,
  not `not_modified`. A new 200 batch gets a new incarnation and no inherited
  PASS proof.
- Same-scope proof lives in the bounded batch cache. A newer FAIL supersedes
  PASS. Final delivery fences replacement/removal/evidence updates and checks
  actual time, source version, project/key access and candidate presence in its
  transaction. Evicting evidence costs another probe, not a bypass.
- A used sticky connection may be revalidated without retaining its original
  list in inventory. It cannot silently reuse missing list proof. Authentication
  removal clears only the current source configuration's owned binding snapshots
  and preserves project CD and independent sources.

### Account keys, project ceilings and jobs

Webshare reads only the fixed direct-list endpoint. Every page, count, `next`
origin/path/query and valid record is checked before replacement. Any page
failure or configured full-snapshot overflow rejects the whole refresh.

An account source requires `access.project_ids` to be an explicit array. Source
access is the intersection with project/request source ceilings and is applied
before bounded selection/refill/Ready limits, then again at final reserve and
delivery. Omitted/null access is public for non-account sources; `[]` denies all.

Account API keys are separate from returned proxy credentials and source JSON.
They are encrypted at rest with the deployment `secret_encryption_key` (at least
32 bytes), using independent derivation and AES-GCM source/secret-version AAD.
There is no fallback to the identity secret or admin token. PUT/DELETE require
the live source `config_version`; rotation/clear invalidates old cache and proof.
GET exposes status/version only, never plaintext or ciphertext.

`source_refresh` is an admin-only bounded job. Its historical `ready` field means
retained unvalidated cached rows, unlike prewarm's scoped validated qualification.
Both kinds share durable job caps. Startup marks abandoned jobs interrupted.
`NextFetchAt` is the persisted upstream floor/backoff; `NextRefreshAt` is a
separate configured interval schedule. The same Webshare credential uses a
shared HMAC-keyed read-rate window across source instances/restart; it is **not**
a daily credit, purchasing or quota accounting implementation.

Commercial extraction contracts without current account/API/quota fixtures are
explicitly pending. TLS-intercepting scraping gateways remain incompatible with
the existing end-to-end target TLS contract. They are not enabled through a
generic parser or weaker certificate policy. Local fixture tests are development
verification; real per-channel acceptance is a separate gate described in the
[runbook](channel-acceptance-runbook-2026-11-04.md).

### 9.8 Supplier-level channel admission (2026-11-05)

`GET /api/v1/admin/channel-templates` is the curated supplier directory. One
supplier has one stable `id`, combined `transports`, and complete `variants`
(protocol/endpoint/filter configurations). It contains only engineering-admitted
products, not retired, incompatible or awaiting-contract placeholders. API and
UI grouping does not merge persisted `source_id`s, project access, account
secrets, caches, evidence, leases or CD. `/admin/sites` and explicit CLI `-site`
remain low-level compatibility/diagnostic surfaces, not the admitted directory.

New site sources (including legacy bootstrap/upsert API creation) require
admission; a configuration cannot change to an excluded endpoint. Unchanged
existing definitions remain editable without deletion or migration. Template
admission has no bearing on live fetch/probe success and is not a promise of
daily free allocation. Rejected proposals live only in the value-review document.
