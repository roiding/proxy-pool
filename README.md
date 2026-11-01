# Proxy Pool

An extensible proxy-resource pool with strict, request-time validation.

Status: v0.9, 2026-11-01. The Go vertical slice plus the self-upload workflow
(web console with session login, additive imports, nightly full validation and
dead-proxy cleanup, per-source daily success rates, built-in signed judge) and
the first provider API channel (proxy.scdn.io), source circuits, demand
refill, target-count prewarming, and offline geo observations are implemented
and tested, together with downstream API keys and per-channel proxy cooldowns.
Remaining work is listed under Implementation status.
The default deployment targets one Huawei Cloud Flexus L instance with 2 CPU
cores and 1 GiB RAM; a container build is provided for final deployment.

## Core Contract

Every proxy returned by an acquisition request must pass a new validation
attempt belonging to that delivery request, before the response is sent.

The inventory stores candidates, not a continuously verified "available pool".
Historical validation results help select candidates; they never authorize a
new delivery. Self-uploaded custom sources additionally get one full validation
pass per night (configurable time, default 03:00 local) that refreshes health
and retires repeatedly dead proxies; provider-API inventory is not swept, and
idle provider inventory never generates liveness probes.

```text
Provider APIs / custom resources
             |
   Adapter -> Parser -> Normalizer
             |
       Candidate inventory
             |
Downstream request -> Select -> Reserve -> Validate
                                            |
                              pass -> Commit lease -> Return
                              fail -> Classify -> Try another candidate
```

Validation confirms an observation under a specified probe profile. It cannot
guarantee that a later business request will succeed, or that a rotating
gateway will use the same exit IP for the next connection.

## Design Documents

- [Architecture and lifecycle specification](docs/architecture.md): components,
  request flows, reservations, leases, validation, eviction, APIs, storage,
  security, operational limits, and acceptance criteria.
- [Source integration contract](docs/source-contract.md): the stable boundary
  for adding provider formats and custom resources one at a time.
- [Low-memory Go deployment](docs/deployment-go-lite.md): the default Go/SQLite
  stack, bounded work, memory controls, build constraints, and measurement plan.

## Web Console

`GET /admin` serves an embedded single-page console (no external assets).
Login with the configured API token issues an HttpOnly session cookie (12-hour,
SameSite=Strict, in-memory server-side session so logout works; cookie
mutations also require a fetch-only header as a CSRF guard). The console
uploads/pastes `lines_v1` txt lists, creates provider API channels, and shows
pool stats, per-source health, and each source's validation success rate for
the current day. It also manages downstream API keys and target-count
prewarming. Custom-source rows have a manual "验活" button that starts an
immediate validation pass; provider rows have a "拉取" button that fetches one
vendor batch. Progress (probed/retired counters) is polled live and the
buttons are disabled while a pass is running (single-flight, shared probe
slots).

## Provider API Channels

The first provider channel is [proxy.scdn.io](https://proxy.scdn.io/api_docs.php),
registered as the `provider_api` adapter with a `json_mapping_v1` parser.

```sh
# Create a channel (protocol declares the transport mapping; batch max 20)
curl -X POST http://127.0.0.1:8080/v1/sources -H "Authorization: Bearer $TOKEN" \
  -d '{"source_id":"scdn","adapter_type":"provider_api","protocol":"http","fetch_count":20}'

# Pull one vendor batch (additive import; can be repeated from the console)
curl -X POST http://127.0.0.1:8080/v1/sources/scdn/fetch -H "Authorization: Bearer $TOKEN"
```

Semantics and limits:

- The vendor returns bare `host:port` entries with no expiry and no per-proxy
  protocol metadata, so `protocol` is the declared transport: `http` and
  `https` map to the pool's http transport (https additionally stores a
  `vendor_protocol` claim), `socks5` maps to socks5. `all` and `socks4` are
  rejected at creation instead of being guessed.
- Fetches are additive and idempotent per content fingerprint, single-flight
  per source, and guarded by a minimum interval (default 1s, configurable via
  `min_fetch_interval_ms` in the channel creation body). The payload is
  bounded (64 KiB), the envelope (`code`/`message`) is checked, and invalid
  rows are reported as `rejected` without failing the batch.
- Provider offers carry no provider expiry; they live under the pool's
  retention policy (`local_retention_ms`, when set). Delivery still requires a
  fresh request-time validation — importing never authorizes a proxy.
- Provider sources are never swept nightly. Dead supply is retired by
  maintenance once acquire-attempt evidence reaches the
  `custom_nightly_retire_failures` threshold, and re-fetching a tombstoned
  proxy cannot revive it.

### Free Site Channels

`site_fetch` channels import curated free-proxy list pages into the normal
pool. The catalog (`internal/integrations/sites`) defines one entry per site
with a code-owned parser (HTML tables, aria-role cell tables, embedded
`fpsList` JSON, `data.list` JSON, or `ip:port` text). Each fetch walks a
bounded number of pages (max 20, per-page body cap 512 KiB, per-page
delay, stop after three empty pages) and imports additively like a custom
upload, so nightly validation and acquire evidence keep quality honest.

Create a channel from the console's 免费站点源 card or via API:

```sh
curl -X POST http://127.0.0.1:8080/v1/sources -H "Authorization: Bearer $TOKEN" \
  -d '{"source_id":"lumiproxy-elite","adapter_type":"site_fetch","site_id":"lumiproxy-elite","max_pool_leases":1}'
curl -X POST http://127.0.0.1:8080/v1/sources/lumiproxy-elite/fetch -H "Authorization: Bearer $TOKEN"
```

`GET /v1/admin/sites` returns the catalog. Every channel was verified live
from the Singapore deployment on 2026-11-01 with
`go run ./cmd/sitecheck -max-pages 1` (rerun it to re-check; `-json` prints
machine-readable results):

| Channel | Parser | First-page entries |
| --- | --- | --- |
| `ip3366-normal` 云代理(普通) | table_2col | 15 |
| `ip89` 89免费代理 | text | ~3000 |
| `jiliuip-elite` 积流代理(高匿) | role_cells | 9 |
| `qiyunip-elite` 齐云代理(高匿) | table_th | 15 |
| `lumiproxy-transparent/anon/elite` | json_list | 24 each |
| `proxyscrape-anon/elite` | text | 752 / 355 |
| `openproxylist-http/https` | text | ~6400 / ~5100 |

Sources that no longer work (checked the same day) are excluded instead of
shipped dead: kuaidaili (JS anti-bot challenge), kxdaili (TLS handshake
failure), ip3366 elite route (HTTP 521), 911Proxy/ProxyLite/ProxyShare free
list APIs (404), FineProxy (404). The reference implementation for several
parsers is [fog-forest/free-proxy-pool](https://github.com/fog-forest/free-proxy-pool) (MIT).

## Dynamic Scheduling

The pool does not treat every source as equally healthy. Three mechanisms
cooperate:

- **Source circuits.** Each source has a durable circuit. Attributed probe
  failures (transport/TLS/auth; never judge or budget failures) accumulate per
  source. At `source_failure_threshold` (default 3) the source opens for
  `source_cooldown_base_ms` (default 30s); each failed post-cooldown trial
  doubles the backoff up to `source_cooldown_max_ms` (default 15min). A pass
  closes and resets the circuit. Candidate selection excludes cooling
  sources, ranks expired-cooldown sources last (half-open trials), and then
  prefers recently passing resources, so acquire switches sources without
  operator intervention. When every eligible source is cooling, the shortfall
  says so instead of pretending the inventory is empty.
- **Demand refill.** When an acquire request finds no candidates and an
  eligible `provider_api` source exists, the pool performs one demand fetch
  (single-flight, per-source cadence guard) and re-selects. An empty local
  inventory is not a hard failure if the vendor can supply fresh proxies.
- **Prewarm.** `POST /v1/prewarm` with `{"count":N,"deadline_ms":120000}`
  starts one asynchronous fill job. It probes inventory, preferring recently
  passing then untested resources, records evidence/health, retires repeatedly dead
  supply, and stops at the target, the deadline, or
  `prewarm_max_attempts`. Progress is available at `GET /v1/admin/prewarm`
  and in the console. Prewarm refreshes evidence but never authorizes
  delivery: each acquire still performs its own request-owned validation.

## Exit IP and Region

Every passing probe already knows the exit IP from the signed judge. If
`geoip_db_path` points at an offline MaxMind-format database (GeoLite2 City
or DB-IP Lite City), the pool resolves country/region/city locally and stores
the latest observation per resource. No third-party lookup is made during
probing, so there is no per-IP rate limit and no unsigned response to trust.

Acquire responses include `observed: {exit_ip, country_code, country, region,
city, observed_at}` when available, and requests may filter with
`"filters": {"countries": ["CN"]}`. Download DB-IP Lite (no account) or
GeoLite2 and set `geoip_db_path`; an empty path disables geo enrichment while
still recording exit IPs.

## Downstream Keys and Channel Cooldown

The console and management API are protected by `admin_password` (when set;
otherwise the legacy `api_token` is accepted). The master `api_token` remains
valid for admin API calls — never hand it to downstream consumers. Downstream
consumers should use dedicated keys:

```sh
# Admin creates a key; the plaintext token is returned exactly once.
curl -X POST http://127.0.0.1:8080/v1/admin/keys \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"name":"business-a","default_cd_seconds":300}'

# Downstream acquire with a per-request CD of 300 seconds.
curl -X POST "http://127.0.0.1:8080/v1/proxies/acquire?cd=300" \
  -H "X-API-Key: pk_..." \
  -d '{"count":2}'
```

Keys are accepted through `Authorization: Bearer`, `X-API-Key`, or the `key`
query parameter, and may only call acquire, release, and feedback — source
management, key management, prewarm, and admin endpoints require the master
token or a console session. Only an HMAC hash and a display prefix are stored;
the token cannot be recovered after creation. `GET/PATCH/DELETE
/v1/admin/keys` list, enable/disable, and delete keys.

`cd` is in seconds. The effective window is the request's `cd` when present,
otherwise the key's `default_cd_seconds`; zero disables the channel cooldown.
It is tracked per `(key, proxy)` and starts only after a successful delivery,
so failed acquire attempts do not burn the window. The same key will not
receive that proxy again until the window expires; different keys are
independent. This applies equally to provider-API and custom inventory because
the filter is part of candidate selection.

### Why no Redis

The deployment is a single Go process with one SQLite database in WAL mode.
Lease fencing, reservations, source circuits, and per-key cooldowns all need
the same transactional authority; SQLite provides that with immediate
transactions and durable state, without another service to operate, secure,
and back up on a 1 GiB instance. Redis would only become necessary when
multiple application instances must share in-flight state or when sustained
QPS exceeds a single SQLite writer. At that point the right shape is a
transactional database (PostgreSQL) as the source of truth with Redis as a
cache/rate-limit layer — not Redis alone holding leases and cooldowns.

## Judge Endpoint

`GET /v1/judge?nonce=<hex>` is the built-in owned judge: it echoes the nonce,
reports the exit IP taken from the actual TCP peer (never forwarded headers),
and signs the answer with `judge_secret` (HMAC-SHA256 over `nonce|exit_ip`).
Point `judge_url` at this endpoint's public address. A plain-HTTP judge outside
loopback requires the secret, and probes refuse unverifiable answers as
suspected interception. Expose the port directly — a reverse proxy would
replace the true peer address. The endpoint is rate limited per source IP.

Behind a trusted gateway (for example Traefik in front of the container with
the public domain), set `judge_trust_forwarded: true` and `judge_url` to the
gateway's HTTPS address: the judge then takes the exit IP from the rightmost
`X-Forwarded-For` entry (the one your gateway appended), ignoring anything the
proxied client spoofed ahead of it. TLS comes from the gateway's certificate,
so no `judge_ca_pem` is needed.

## Traffic Budget

The instance's 1024 GB monthly traffic package is not a constraint for this
service: it distributes proxy resources and does not relay clients' business
traffic — downstream users connect directly to the proxies they receive.

| Traffic | Direction | Size |
| --- | --- | --- |
| Uploading a 10k-line txt | inbound | ~0.6 MB (inbound is free on Flexus) |
| Acquire response | outbound | a few hundred bytes |
| Probe via HTTP signed judge | outbound | ~0.6 KB (CONNECT + request + judge response) |
| Probe via TLS judge | outbound | ~1-1.5 KB (the certificate handshake dominates) |
| Dead-proxy probe | outbound | tens of bytes (SYN/RST only) |

A nightly full pass over 10,000 proxies with roughly 30% alive costs about
2 MB per night with the HTTP judge (~60 MB/month) and about 4 MB with a TLS
judge. Even the worst case of 10,000 alive probes over TLS is ~10 MB per night
(~300 MB/month) — under 1% of the package. Probing more often would scale this
linearly; the default is once per night.

The real traffic risks are relaying business traffic through this machine
(not part of this design) and public abuse of `/v1/judge` or `/admin`. The
judge endpoint is rate limited to 600 requests/min per source IP with ~200-byte
responses, so a single IP pinned at the limit around the clock adds at most
~5 GB/month; front the port with a security group and set a cloud outbound
traffic alarm (for example, 2 GB/day) to catch anomalies early.

## Recommended First Implementation

- One Go process using `net/http`, `database/sql`, and bounded goroutines.
- Local SQLite in WAL mode as inventory, reservation, lease, and budget authority.
- No Redis, PostgreSQL, ORM, web framework, or external task queue by default.
- Four deployment-wide probe slots, two per request, and small bounded batches.
- The built-in signed judge endpoint and an explicit proxy-transport layer.
- First integrations: manual `lines_v1` import; one actual provider API later.

Do not begin with microservices, periodic full-pool validation, or a universal
parser that tries to infer arbitrary provider formats. PostgreSQL and optional
Redis are later scale-out choices, not dependencies of the single-machine build.

## Implementation Status

Implemented and covered by tests:

- SQLite storage (WAL, fenced reservations, leases, scoped health, cooldowns,
  tombstones, sweeps) under `internal/storage/sqlite/`.
- `https_basic_v1` validation over a fresh proxy connection with verified TLS
  and nonce-checked judge responses under `internal/validation/`.
- The acquire flow (select, reserve, probe, commit, dispatch guard, partial
  results) under `internal/application/acquire.go`.
- `lines_v1` custom import with address policy and HMAC-based canonical
  identity under `internal/integrations/custom/` and `internal/domain/`.
- The first provider API channel (`provider_api` / `json_mapping_v1`) for
  proxy.scdn.io under `internal/integrations/scdn/`: additive fetch imports
  via `POST /v1/sources/{id}/fetch`, declared protocol-to-transport mapping,
  bounded payloads, per-source fetch single-flight and cadence guard, and
  maintenance-driven retirement of dead provider supply.
- Durable source circuits with exponential cooldown, automatic source
  switching, and demand refill of eligible provider channels during acquire
  (`internal/storage/sqlite/scheduling.go`, `internal/application/acquire.go`).
- Target-count prewarming via `POST /v1/prewarm` with live status at
  `GET /v1/admin/prewarm` and in the console
  (`internal/application/prewarm.go`).
- Signed exit-IP observations plus optional offline geo lookup
  (`internal/geoip/`), returned in acquire responses and filterable by
  country.
- Separate admin password, hashed downstream API keys with per-key default CD,
  and per-`(key, proxy)` delivery cooldowns enforced during candidate
  selection (`internal/api/auth.go`, `internal/application/keys.go`,
  `internal/storage/sqlite/keys.go`).
- Self-upload additive semantics: each line keeps a stable content-derived
  record ID, re-uploads update offers in place, and dead supply is cleaned up
  by validation instead of list replacement. HTTP and SOCKS5 lines are
  supported (`http://` / `socks5://`, bare lines use the source's default).
- Nightly full validation for custom sources (`custom_nightly_validation_time`,
  default 03:00): authentication failures (e.g. proxy `407`, SOCKS5 auth
  rejection) retire the resource immediately; other attributed failures apply
  cooldowns and remove the resource (tombstoned so re-uploads cannot revive
  it) once the consecutive-failure threshold
  (`custom_nightly_retire_failures`, default 2) is reached. Evidence never
  authorizes delivery.
- Embedded admin console at `/admin` with session login, `/v1/admin/sources`
  overview, `/v1/admin/daily` per-source success rates, and manual per-source
  validation via `POST /v1/sources/{id}/validate` with live progress at
  `GET /v1/admin/validation`. Channels can be removed with
  `DELETE /v1/sources/{id}`, which drops their offers and tombstones so a
  later re-upload rebuilds the pool from scratch.
- Authenticated HTTP API (`acquire`, `release`, `feedback`, `sources`,
  `imports`, `stats`) under `internal/api/`.
- In-process maintenance sweeps, singleton lock, and graceful shutdown.
- Built-in signed judge endpoint `/v1/judge` with per-IP rate limiting; probes
  verify nonce and HMAC, treating unverifiable answers as interception.

Not implemented yet (later milestones): additional provider API formats,
idempotency-key replay, sticky/rotating gateway semantics, SOCKS transports,
feedback trust weighting, capacity groups, and the PostgreSQL/Redis scale-out
path.

## Build and Run

```sh
go test ./...
go build -o proxy-pool ./cmd/proxy-pool
cp config.example.json config.json   # then edit secrets and judge_url
./proxy-pool -config config.json
```

Container build (the CGO toolchain is provided by the builder stage):

```sh
docker build -t proxy-pool .
docker run -p 8080:8080 -v "$PWD/config.json:/var/lib/proxy-pool/config.json:ro" \
  -v proxy-pool-data:/var/lib/proxy-pool/data proxy-pool
```
