# Proxy Pool

An extensible proxy-resource pool with strict, request-time validation.

Status: v0.6, 2026-10-31. The Go vertical slice plus the self-upload workflow
(web console with session login, additive imports, nightly full validation and
dead-proxy cleanup, per-source daily success rates, built-in signed judge) are
implemented and tested.
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
uploads/pastes `lines_v1` txt lists and shows pool stats, per-source health,
and each source's validation success rate for the current day. Each source row
has a manual "验活" button that starts an immediate validation pass for that
source; progress (probed/retired counters) is polled live and the buttons are
disabled while a pass is running (single-flight, shared probe slots).

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

Not implemented yet (later milestones): provider API adapters, idempotency-key
replay, sticky/rotating gateway semantics, SOCKS transports, feedback trust
weighting, capacity groups, and the PostgreSQL/Redis scale-out path.

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
