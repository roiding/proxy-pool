# Low-Memory Go Deployment Profile

Version: 0.7. Status: CI-gated native release profile documented; RSS
benchmarks remain pending. The existing Huawei Docker deployment was upgraded
on 2026-11-05; see [the actual deployment record](deployment-huawei-channels-2026-11-05.md).
The native/systemd instructions below remain a separate deployment profile.

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
| SQLite driver | CGO-based `github.com/mattn/go-sqlite3` |
| Logging | Standard `log/slog`, redacted, no unlimited asynchronous buffer |
| Configuration | Small JSON configuration plus protected secret-key material |
| Maintenance | A small `time.Ticker` loop with bounded SQL batches and bounded custom-source validation |
| Judge | Built-in signed `GET /v1/judge`, publicly reachable by candidates, per-IP rate limited |
| Process management | Native Linux service under systemd, no container required |

No Gin/Echo, GORM, Redis, PostgreSQL, Celery-style queue service, external scheduler,
external judge deployment, vault daemon, or monitoring-server stack is required
for this deployment.
Code modules are in-process packages, not separate long-lived services.

## 2. SQLite Driver Decision

The selected driver is CGO-based `mattn/go-sqlite3`. It avoids choosing a large
pure-Go SQLite dependency solely to obtain effortless cross-compilation. This is
a design choice to measure, not a claim that CGO always uses less RSS.

- CGO-based SQLite requires a C toolchain at build time and a compatible Linux
  runtime/linking target. A macOS binary cannot run on the cloud Linux instance.
- Do not use `CGO_ENABLED=0` with this driver. The CI native artifact uses
  `CGO_ENABLED=1`, `GOOS=linux`, `GOARCH=amd64`, and the Alpine/musl builder
  from the Dockerfile; CGO compilation is not claimed to work without a target
  C toolchain. Only amd64 is published.
- The CI artifact is built with `-linkmode=external` from
  `CGO_ENABLED=1` and is checked for an ELF x86-64 format with no dynamic
  interpreter. Build on a release runner or another Linux machine; the
  production instance needs the resulting binary, not a permanent compiler or
  Docker daemon.
- Pure-Go SQLite is a fallback if CGO is operationally unacceptable. Compare
  peak/idle RSS and cancellation behavior using the same representative workload.
- Stripping symbols reduces binary size, not necessarily runtime heap/RSS.
  Do not use executable compression as a substitute for memory measurement.

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
| Delivered items per request | Configured `max_count_per_request` (example: `100`) |
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
- Keep `config.json`, the database, WAL/SHM sidecars, and
  `secret_encryption_key` together as one recovery unit. Back up the directory
  or use SQLite's online backup API; a main-file-only copy during WAL writes is
  not a consistent backup.
- Before a binary or schema-changing upgrade, stop the service, back up the
  complete data directory and protected configuration, and verify the backup
  opens read-only. Preserve the database and its schema across rollback; do not
  restore code by deleting or replacing the live database.
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

## 9. Native CI Artifact

The `docker` workflow's `native-linux-amd64` job depends on the same
format/vet/full-test/race gate as the image job. It builds a statically linked
`proxy-pool` for Linux amd64, records the exact source revision, and publishes:

- `proxy-pool-<version>-linux-amd64.tar.gz`
- `proxy-pool-<version>-linux-amd64.tar.gz.sha256`

The tarball contains the executable plus `VERSION`, `REVISION`, and a
`SHA256SUMS` manifest. Main-branch and manual runs upload an Actions artifact;
a `v*` tag push also attaches the tarball and checksum to the GitHub release.
No aarch64 or 32-bit artifact is published. Confirm the target CPU architecture
before selecting the artifact.

Download and verify:

```sh
sha256sum -c proxy-pool-<version>-linux-amd64.tar.gz.sha256
tar -xzf proxy-pool-<version>-linux-amd64.tar.gz
cd proxy-pool-<version>-linux-amd64
sha256sum -c SHA256SUMS
file proxy-pool
./proxy-pool -version
```

The expected `file` result is a statically linked ELF 64-bit x86-64 executable.
CI rejects a dynamically linked or non-amd64 result. A named release tag is
stamped as the version; branch builds use the commit SHA.

## 10. Flexus L Native Install

This is a procedure, not evidence that the target host has been changed. Use an
ordinary, non-root service account, protect the configuration and data, and
confirm the host firewall, public name, reachable judge address, and TLS
termination before enabling traffic.

1. Download and checksum-verify the CI artifact from the intended Actions run.
2. Inspect the existing host and service before replacing anything:

```sh
uname -m
test -f /etc/ssl/certs/ca-certificates.crt
test -f /usr/share/zoneinfo/Asia/Shanghai
getent passwd proxy-pool || true
systemctl cat proxy-pool.service || true
systemctl status proxy-pool.service --no-pager || true
```

3. Stage the release in a temporary directory without replacing the
   configuration or data, then install it under absolute paths:

```sh
package_dir=/tmp/proxy-pool-<version>-linux-amd64
release_tmp=/tmp/proxy-pool-staged-<version>-linux-amd64
getent passwd proxy-pool >/dev/null || \
  sudo useradd --system --home-dir /var/lib/proxy-pool \
    --shell /usr/sbin/nologin proxy-pool
sudo install -d -o root -g root -m 0755 "$release_tmp"
sudo install -o root -g root -m 0644 \
  "$package_dir/proxy-pool" \
  "$package_dir/VERSION" \
  "$package_dir/REVISION" \
  "$package_dir/SHA256SUMS" "$release_tmp"
(
  cd "$release_tmp"
  sudo sha256sum -c SHA256SUMS
)
sudo install -d -o root -g root -m 0755 /opt/proxy-pool/releases
sudo install -d -o root -g root -m 0755 /opt/proxy-pool/releases/<version>
sudo install -o root -g root -m 0755 \
  "$release_tmp/proxy-pool" /opt/proxy-pool/releases/<version>/proxy-pool
sudo install -d -o proxy-pool -g proxy-pool -m 0750 /var/lib/proxy-pool
```

Do not switch `/opt/proxy-pool/current` until the pre-upgrade backup in the
next step has completed and its listing has been checked.

4. Never replace or reset the existing production config, secrets, channels,
   database, project CD state, upstream credentials, or systemd service unless
   the operator explicitly approves the change. Create a new config from
   `config.example.json` only for a first install, then replace every
   placeholder secret. For a native install keep `listen_addr:
   "127.0.0.1:8080"` and use a local TLS reverse proxy. The signed
   `/v1/judge` route must be reachable from real remote candidates; a
   localhost `judge_url` works only for local/test candidates. Use verified
   HTTPS or a strong random `judge_secret` with signed HTTP. If a trusted
   reverse proxy supplies the peer address, set `judge_trust_forwarded` only
   when the backend is reachable exclusively through that gateway and the
   gateway strips client-supplied `CF-Connecting-IP`/`X-Forwarded-For` before
   setting the observed peer. `trusted_proxy_cidrs` does not enforce Judge
   header trust. Otherwise leave `judge_trust_forwarded` false and use the
   direct peer. The local proxy must remove any incoming `CF-Connecting-IP`
   header and overwrite `X-Forwarded-For` with its observed peer.

5. For an existing installation, stop the old service and take a consistent
   backup while SQLite is quiescent. This block applies only to upgrades. On a
   first install, record in the deployment record that there is no previous
   service, config, or database, then skip these backup commands:

```sh
backup_dir=/var/backups/proxy-pool
backup=$backup_dir/data-$(date -u +%Y%m%dT%H%M%SZ).tar.gz
config_backup=$backup_dir/config-$(date -u +%Y%m%dT%H%M%SZ).json
checksums=$backup_dir/checksums-$(date -u +%Y%m%dT%H%M%SZ).sha256
config_path=/etc/proxy-pool/config.json
data_dir=/var/lib/proxy-pool
db_rel=data/proxy-pool.db
sudo install -d -o root -g root -m 0700 "$backup_dir"
previous_release="$(readlink -f /opt/proxy-pool/current 2>/dev/null || true)"
printf 'previous_release=%s\n' "${previous_release:-none}"
sudo systemctl stop proxy-pool.service
sudo test -f "$config_path"
sudo test -f "$data_dir/$db_rel"
sudo sh -c 'umask 077; tar -C "$1" -czf "$2" .' _ "$data_dir" "$backup"
sudo sh -c 'umask 077; cp -a "$1" "$2"' _ "$config_path" "$config_backup"
sudo test -s "$backup"
sudo test -s "$config_backup"
sudo tar -tzf "$backup" >/dev/null
sudo sh -c 'sha256sum "$1" "$2" > "$3"' _ \
  "$backup" "$config_backup" "$checksums"
sudo sha256sum -c "$checksums"

db_check=$(mktemp -d /tmp/proxy-pool-db-check.XXXXXX)
trap 'command rm -rf "$db_check"' EXIT
sudo tar -C "$db_check" -xzf "$backup"
sudo chown -R "$(id -u):$(id -g)" "$db_check"
sqlite3 "file:$db_check/$db_rel?mode=ro" 'PRAGMA quick_check;'
```

Adjust `config_path` and `data_dir` to the existing service; do not copy only
the SQLite main file while the process is running. The extracted snapshot
includes the WAL/SHM sidecars when present, and the check opens it read-only
without `immutable=1` so WAL contents are considered. The expected
`PRAGMA quick_check` result is `ok`. Keep the printed previous-release path,
checksum-verify the backup, and retain the previous binary and service unit. A
database upgraded to a newer schema must not be silently downgraded or
deleted; use a tested forward fix, or restore the backup together with the
previous binary and configuration after rollback is approved.

6. Install the provided example unit only for a new service, replacing the
   placeholders. For an existing service, keep its established unit and paths.
   Find the unit in `deploy/proxy-pool.service`; the example starts the service
   as `proxy-pool`, reads `/etc/proxy-pool/config.json`, and stores data under
   `/var/lib/proxy-pool`.

```sh
sudo install -d -o root -g proxy-pool -m 0750 /etc/proxy-pool
sudo install -o root -g proxy-pool -m 0640 \
  /absolute/path/to/config.json /etc/proxy-pool/config.json  # first install only
sudo install -o root -g root -m 0644 \
  /absolute/path/to/deploy/proxy-pool.service \
  /etc/systemd/system/proxy-pool.service
sudo ln -sfn /opt/proxy-pool/releases/<version> /opt/proxy-pool/current
sudo systemctl daemon-reload
sudo systemctl enable --now proxy-pool.service
```

For both first and existing installs, use absolute `database_path` and
`lock_path` values under the writable data directory. For an existing
installation, reuse its existing service name, unit path, config path,
database path, lock path, and file ownership. Do not rename or replace those
assets with the example template. For example:

```json
{
  "database_path": "/var/lib/proxy-pool/data/proxy-pool.db",
  "lock_path": "/var/lib/proxy-pool/data/proxy-pool.lock"
}
```

7. Verify the service, listen address, health endpoint, version, and migration
   logs. Do not claim deployment acceptance from a running process alone:

```sh
systemctl status proxy-pool.service --no-pager
curl -fsS http://127.0.0.1:8080/healthz
/opt/proxy-pool/current/proxy-pool -version
journalctl -u proxy-pool.service -n 100 --no-pager
```

8. Confirm the public Judge URL, admin authentication, TLS certificate, DNS,
   and firewall rules before sending downstream traffic. The judge must see the
   real peer address (or a correctly configured trusted proxy). The admin
   console and management API must not be exposed without TLS and strong
   credentials.

   The candidate-facing Judge address must be public to the proxies under
   test, not the target host's own loopback or private address. Verify the
   exact `/v1/judge` URL from an external source, then run a real acquire
   probe against a controlled proxy. Admin access should remain on loopback or
   a private network; a public admin route requires verified TLS, strong
   credentials, and explicit operator approval.

If verification fails, stop the new unit, point `/opt/proxy-pool/current` back
to the previous verified release, restore the previous unit/configuration, and
restore the verified pre-upgrade database and WAL backup. Before accepting the
new release, keep traffic stopped or admitted only through a controlled test
path. Once the new release has served real traffic, do not blindly restore the
older database: new leases and project CD state would be lost or become
inconsistent. Reconcile or explicitly invalidate those records first, then
restore only with operator approval. Preserve the failed database and WAL for
diagnosis instead of overwriting them.

No CI job has target SSH credentials, invokes SSH, or mutates a production
host. Actual installation and deployment verification remain an operator
action after target identity and host prerequisites are confirmed.
