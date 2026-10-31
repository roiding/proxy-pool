# Low-Memory Go Deployment Profile

Version: 0.6. Status: default profile implemented and covered by the
integration tests; RSS benchmarks on the target Linux instance are still pending.

Target: one Huawei Cloud Flexus L instance, 2 CPU cores, 1 GiB RAM. The reported
1024-G traffic allowance does not change the memory budget. This service
distributes proxy resources; it does not relay clients' business traffic.

This is the default deployment profile for [the architecture](architecture.md).
Lower memory use must not weaken fresh validation, TLS verification, credentials,
atomic allocation, quota durability, or expiry guards.

## 1. Minimal Runtime Stack

| Concern | Default |
| --- | --- |
| Application | One Go binary, one active process, supported Go release |
| HTTP API | Standard `net/http`, native routing, bounded request admission |
| Network I/O | Context-bound `net/http` clients and explicit proxy transport |
| Database | Local SQLite with WAL, through `database/sql` |
| SQLite driver | Prefer evaluating `github.com/mattn/go-sqlite3` with CGO |
| Logging | Standard `log/slog`, redacted, no unlimited asynchronous buffer |
| Configuration | Small JSON configuration plus protected secret-key material |
| Maintenance | A small `time.Ticker` loop with bounded SQL batches and bounded custom-source validation |
| Judge | Built-in `GET /v1/judge`, signed, per-IP rate limited, direct exposure |
| Process management | Native Linux service under systemd, no container required |

No Gin/Echo, GORM, Redis, PostgreSQL, Celery-style queue service, external scheduler,
external judge deployment, vault daemon, or monitoring-server stack is required
for this deployment.
Code modules are in-process packages, not separate long-lived services.

## 2. SQLite Driver Decision

The initial driver candidate is CGO-based `mattn/go-sqlite3`. It avoids choosing
a large pure-Go SQLite dependency solely to obtain effortless cross-compilation.
This is a design preference to measure, not a claim that CGO always uses less RSS.

- CGO-based SQLite requires a C toolchain at build time and a compatible Linux
  runtime/linking target. A macOS binary cannot run on the cloud Linux instance.
- Do not use `CGO_ENABLED=0` with this driver. Build for the actual Linux CPU/libc,
  using a Linux builder or a correctly configured cross C toolchain.
- Build on a separate machine when convenient; the production instance needs
  the resulting compatible binary, not a permanent compiler or Docker daemon.
- Pure-Go SQLite is a fallback if CGO is operationally unacceptable. Compare
  peak/idle RSS and cancellation behavior using the same representative workload.
- Stripping symbols reduces binary size, not necessarily runtime heap/RSS.
  Do not use executable compression as a substitute for memory measurement.

No driver is installed or pinned until application implementation begins.

## 3. SQLite Connection and Cache Budget

Start with `SetMaxOpenConns(1)` and `SetMaxIdleConns(1)`. Apply settings on every
opened connection, using driver configuration or a connection initializer;
one startup `Exec` is not sufficient if a replacement connection is later opened.

Illustrative connection settings:

```sql
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 1000;
PRAGMA synchronous = FULL;
PRAGMA cache_size = -2048;
PRAGMA mmap_size = 0;
PRAGMA temp_store = FILE;
```

- The negative cache size expresses a roughly 2-MiB page-cache target; it is not
  a hard cap on all SQLite allocations. CGO/SQLite memory is outside Go's limiter.
- WAL permits reader/writer coexistence, but SQLite still has one writer and a
  one-connection pool serializes SQL operations. Benchmark before increasing it.
- Begin allocation with `BEGIN IMMEDIATE`, use owner/fence/version conditions,
  check affected rows, and commit before any proxy or provider network I/O.
- With `go-sqlite3`, an immediate-transaction option such as `_txlock=immediate`
  can be used with `database/sql` transactions. Keep the whole transaction on its
  owned connection; do not issue `BEGIN` through `DB.Exec` and assume unrelated
  pooled queries form one transaction merely because the pool currently has size 1.
- Release `sql.Rows` and transactions promptly. With a one-connection pool,
  issuing another DB query while holding rows/transaction can block indefinitely.
- Preserve durability for purchases and leases. Do not set `synchronous=OFF`, use
  in-memory storage, or omit durable monetary budgets to obtain lower RSS.
- Use indexed limited queries, not full-pool reads or unbounded ranking sorts.
- Checkpoint WAL and delete old history in small scheduled batches. Long-running
  readers, failed checkpoints, and disk-full conditions need explicit monitoring.
- Use SQLite's backup API or a consistent offline backup. Do not copy only the
  main database file while active WAL transactions may exist.

## 4. Bounded Goroutines and Work

| Limit | Starting value |
| --- | --- |
| Application processes | `1` |
| Active acquisition operations | `8` |
| Deployment-wide probe workers | `4` |
| Simultaneous probes per request | `2` |
| Queued validation jobs | `8` |
| Delivered items per request | At most `5` |
| SQL candidate page | At most `8` compact records |
| Concurrent provider fetches | At most `2` globally and `1` per source |
| Provider candidates per fetch | At most `100` |
| Periodic custom validation | At most `60` probes per 5-minute cycle, 4 shared slots, single-flight |

These are initial bounds, not measured throughput guarantees.

- Reject overload with a safe `429`/`503` reason; never grow an unbounded backlog
  of goroutines waiting for a semaphore. Queue time consumes the request deadline.
- Schedule replacements incrementally. Do not launch one goroutine for every
  resource in the database, or materialize a task list proportional to pool size.
- Cancellation/deadline checks occur before queued jobs start network I/O. Always
  release owned capacity and close sockets/bodies on failure/cancellation paths.
- Bound accepted HTTP connections as well as application work, and apply read/
  header/write/idle timeouts. Admission alone does not prevent slow idle clients
  from consuming connection/goroutine memory.
- Single-flight source fetching is bounded and does not extend indefinitely for
  new waiters. Budgets and source-operation fences remain in SQLite.
- Exactly one application process is a deployment invariant. Before adding a
  second process, redesign global admission and use the PostgreSQL scale-out path.

## 5. Network and Body Allocation

| Input/response | Starting bound |
| --- | --- |
| Acquisition request body | `8 KiB` |
| HTTP request headers | `16 KiB` |
| Provider/custom import body | `8 MiB` (about 100k `lines_v1` rows), staged in one bounded transaction |
| Owned judge body | `4 KiB` |
| Judge requests per source IP | `600/min`, burst `120` |
| Candidate raw diagnostic retention | Redacted, sampled, size-limited |

- Use bounded readers and streaming/typed JSON, line, or CSV decoding. Do not
  retain both a full raw body and a second full generic record graph by default.
- Check HTTP error envelopes, truncated bodies, parser errors, and required EOF;
  a response limit or cancellation is not a successful authoritative snapshot.
- The current-request probe opens a fresh proxy connection. Reusing a live
  earlier connection may hide new-connection failure and violates this profile.
- Do not retain one idle `http.Transport` pool per candidate. Close probe bodies
  and connections promptly, including non-success paths and abandoned responses.
- Provider API clients may reuse a small bounded connection pool. Default limits
  should be explicit, for example 8 total idle connections and 1 per origin.
- Do not disable TLS verification, remove response checks, or switch to TCP-only
  validation to save memory. Body limits do not replace network/TLS deadlines.

## 6. Runtime Memory Controls

Illustrative launch environment, not a service already installed:

```sh
GOMAXPROCS=2 GOMEMLIMIT=64MiB ./proxy-pool -config /etc/proxy-pool/config.json
```

`GOMEMLIMIT` is a soft Go-runtime memory limit. It excludes SQLite/CGO allocations,
memory mapped outside Go, and other memory not managed by the Go runtime. It
MUST NOT be reported as a 64-MiB process RSS ceiling.

- Keep the default GC policy initially. An excessively small soft limit can
  cause GC thrashing and hurt both CPU use and acquisition deadlines.
- Measure RSS separately from Go heap/runtime metrics. The OS, filesystem cache,
  systemd, other services, and peak network/SQLite use need their own headroom.
- Optional systemd/cgroup memory ceilings may protect the host, but crossing a
  hard ceiling can kill the process. Choose them after load/restart tests; they
  are not a substitute for bounds, nor a guarantee that work will complete.
- Do not promise a fixed RSS figure before measuring the actual Linux binary
  under an explicit inventory, concurrency, and workload.
- Configuration limits govern memory growth; runtime soft limits are a backstop,
  not permission to allocate an unlimited queue and rely on GC to fix it.

## 7. Persistent Data, Secrets, and History

- Keep resource/offer/lease/budget state in SQLite, with compact typed records.
- Store immutable encrypted credential versions; keep the encryption key outside
  the database in protected configuration/key material, not in Git or logs.
- Avoid a separate secrets-server process on the small instance. External secret
  storage can be introduced later without changing the credential reference model.
- Persist compact validation outcomes and timings, not full judge/provider HTML.
- Prune detailed history under an explicit retention policy; preserve aggregates,
  active allocations, unresolved purchases, bans, and required tombstones.
- Do not expose production `pprof` or admin/database endpoints publicly. Collect
  profiles via an authenticated/internal diagnostic path when needed.

## 8. Required Measurement Before Claiming Low Memory

Benchmark the actual Linux binary with fake controlled providers/proxies and
bounded owned judges. Record hardware, toolchain, driver, inventory size,
request pattern, and configured limits alongside results.

Measure idle and sustained/peak load for:

1. Process RSS/peak RSS, Go runtime memory/heap, goroutines, file descriptors,
   SQLite page/cache behavior, WAL size, and database/response latency.
2. Increasing inventory sizes with the same active workload: memory should not
   scale as if the entire pool were cached in Go objects.
3. Saturation beyond the admission/queue limits: reject excess requests, keeping
   queue/goroutine/body memory bounded and returned evidence valid.
4. Failed, slow, malformed, oversized, canceled, and interrupted probes/imports:
   no monotonically growing transport/body/task retention.
5. Process restart and database recovery: leases, fences, and monetary budget
   decisions remain correct without Redis or in-memory fallback allocation.

Go tooling such as `go test -race`, benchmarks, and private `pprof` helps diagnose
behavior. Race-instrumented runs have extra overhead; do not quote their RSS as
the release binary's normal footprint. No such application benchmarks exist yet.
