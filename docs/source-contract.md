# Source Integration Contract

Version: 0.6. Status: the `lines_v1` custom family is implemented; provider
API adapter families remain design-only.

This contract allows formats to be added one channel at a time without changing
the acquisition, validation, lease, or eviction workflows. Read it together
with [the architecture specification](architecture.md).

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
| `json_mapping_v1` | Array or nested provider list | Item path, field names, error envelope, protocol semantics, expiry units/zone |
| `csv_mapping_v1` | Header-based rows | Encoding, delimiter, columns, time semantics, credential fields |
| Vendor parser module | Signed/encoded/nested proprietary format | Versioned fixtures, semantic conversion, limits, error handling |

Custom resource entry points are normalized through the same ingestion service:

- Admin request body containing structured candidates.
- Authorized text/file upload; no arbitrary server filesystem path from clients.
- Configuration-owned static endpoint/gateway definitions.

Supported line forms should be declared, not inferred endlessly. URI examples
may include `http://host:port`, `socks5://host:port`, percent-encoded
authenticated URIs, and bracketed IPv6. A bare `host:port` requires a
source-configured transport. Credentials with reserved characters require
proper encoding or structured fields.

Self-upload additive semantics (implemented): each line's source-record ID is
the content fingerprint of its transport/host/port/credentials, so uploads are
append-only and identical re-updates update offers in place. There is no
snapshot replacement; dead supply is cleaned up by validation instead
(authentication failures immediately, other attributed failures via the
consecutive-failure threshold with tombstones). Self-uploaded sources are the
only ones eligible for bounded periodic revalidation, which refreshes health
evidence but never authorizes delivery.

Do not initially add executable "custom fetch scripts", arbitrary remote import
URLs, webhook callbacks, provider reservation/release APIs, or unknown formats.
They can be added behind explicit capability/security contracts when requested.

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
the unchanged acquisition path can import, validate, and lease its resources.
