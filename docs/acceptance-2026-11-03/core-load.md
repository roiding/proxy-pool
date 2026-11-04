# Core Synthetic Bounded-Load Report

Date: 2026-11-03

## Scope And Provenance

- Source tree: dirty working tree based on `HEAD 52b69c2`; core changes remain uncommitted.
- Runtime scope: service-level `application.Acquire` calls against a fresh temporary SQLite database.
- HTTP scope: not applicable. These tests do not issue HTTP API requests, so every JSON record has `http_status = "N/A (service-level call; no HTTP request)"`.
- Supply scope: local macOS test host with loopback fake judges and proxies only. No supplier, static-source, cloud, or production endpoint was called.
- Performance scope: these are bounded local correctness/load smoke results. They are not Huawei, 5000-stock, target-server, production throughput, or capacity measurements.
- JSON source: `/tmp/proxy-pool-root-acceptance-20261103/c58-candidate-normal-load.json`, copied byte-for-byte to `docs/acceptance-2026-11-03/core-load.json`.
- JSON SHA-256: `a57e70621fa107629deb49fa02d3c729667882faaf6e8cb6a7a92271e3c606cf`.
- Workload size: five scenarios and 15 service-level calls total. Concurrency is capped at six requests.
- Finalization reserve: request work ends at the total deadline minus `finalizeBudget(total)`, leaving that slice for final eligibility checks, lease/CD commit, and response assembly. For the 500ms deadline scenario, the finalization reserve is 125ms, the work window is 375ms, and the measured result returned below the 500ms total deadline.

The JSON is a root-captured normal-run artifact from the existing array-format metrics test. The instrumentation now writes an artifact only when `CORE_LOAD_REPORT_PATH` is explicitly set. With the variable unset, normal and race test runs log metrics but do not rewrite the committed JSON.

## Percentile Method

Percentiles use the nearest-rank definition over completed service calls:

`index = ceil(percentile * sample_count) - 1`

The two six-request workloads therefore have a meaningful third-sample P50, while P95 and P99 both select the sixth sorted sample. Scenarios with one request have degenerate P50, P95, and P99 values equal to that request's single latency; they are not distributions.

Goroutine "sampled high" values come from a 1ms periodic sampler. They are not absolute all-instant maxima.

## Measured Results

All latency values are milliseconds.

| Scenario | Requests | Deadline | Delivered / Missing | Business result | Min | P50 | P95 | P99 | Max |
|---|---:|---:|---:|---|---:|---:|---:|---:|---:|
| Shared project CD, 3 proxies | 6 | 5000ms | 3 / 3 | 3 delivered; 3 empty `NO_ELIGIBLE_CANDIDATES` | 9.174 | 9.268 | 21.884 | 21.884 | 21.884 |
| One proxy, capacity 2 | 6 | 5000ms | 2 / 4 | 2 delivered; 4 empty `NO_ELIGIBLE_CANDIDATES` | 5.490 | 7.011 | 13.843 | 13.843 | 13.843 |
| Partial result, one slot | 1 | 1200ms | 1 / 1 | 1 partial `NO_ELIGIBLE_CANDIDATES` | 8.510 | 8.510 | 8.510 | 8.510 | 8.510 |
| Blocking proxy, total deadline | 1 | 500ms | 0 / 1 | empty `VALIDATION_BUDGET_EXHAUSTED` | 380.169 | 380.169 | 380.169 | 380.169 | 380.169 |
| Caller cancellation | 1 | 3000ms | 0 / 1 | empty `VALIDATION_BUDGET_EXHAUSTED` | 4.426 | 4.426 | 4.426 | 4.426 | 4.426 |

Observed goroutine counts:

| Scenario | Before | Sampled high (1ms) | After |
|---|---:|---:|---:|
| Shared project CD | 7 | 48 | 7 |
| Capacity 2 | 10 | 33 | 10 |
| Partial result | 13 | 23 | 13 |
| Total deadline | 16 | 22 | 16 |
| Caller cancellation | 19 | 24 | 19 |

## Resource And Ownership Checks

The two six-request workloads and the partial workload finish their calls with no active reservations. Delivered resources retain only their intended active leases and project-CD rows until the report's explicit cleanup step.

| Scenario | After-load leases / reservations / CD | Final leases / reservations / CD |
|---|---:|---:|
| Shared project CD | 3 / 0 / 3 | 0 / 0 / 0 |
| Capacity 2 | 2 / 0 / 0 | 0 / 0 / 0 |
| Partial result | 1 / 0 / 0 | 0 / 0 / 0 |
| Total deadline | 0 / 0 / 0 | 0 / 0 / 0 |
| Caller cancellation | 0 / 0 / 0 | 0 / 0 / 0 |

Notes:

- Shared project CD: three distinct loopback connections were delivered to six concurrent requests under one shared project scope. The original three CD rows are intentionally retained after delivery. Cleanup releases the delivered leases and removes those rows using `ClearExpiredProjectCooldowns` at a deterministic time 11 minutes in the future; the final count is zero.
- Capacity 2: one connection with two slots produced exactly two deliveries under six concurrent requests, with no invented project CD.
- Partial result: one available slot promptly returned one valid delivery and an honest partial-reason rather than consuming the remaining request deadline.
- Total deadline: a blocking fake CONNECT proxy reached the request-work deadline. The result was empty rather than a false success, the reservation was released, and no lease or CD was created.
- Caller cancellation: the test observed one reserved slot during the blocked call, then canceled the caller. The final call returned promptly as a business shortfall, with the reservation released and no lease or CD left behind.

## Related Sticky Regressions

These three service-level sticky tests were exercised separately from the five-workload metrics artifact:

- `TestCoreCanceledFirstClaimReleasesClaimBounded`: a canceled first claim is released within the bounded cleanup window and the same session can recover.
- `TestCoreConcurrentStickyRefreshUnderSaturation`: two concurrent refreshes under saturation return explained shortfalls without changing the recoverable binding's version, fingerprint, ciphertext, or deadline.
- `TestCoreSaturatedStickyRefreshPreservesRecoverableBinding`: request cancellation leaves the active binding, circuit state, and original project CD intact while returning `VALIDATION_BUDGET_EXHAUSTED`.

These results cover local service behavior only. Actual binary shutdown behavior is recorded separately below.

## Local Binary Shutdown Addendum

Root separately collected two local actual-binary harness results:

- Main shutdown: `/tmp/proxy-pool-root-acceptance-20261103/c58-main-shutdown.json` reports SIGTERM completion in 8.644ms, two successful restarts, proof/TTL/CD integrity retained, and integrity checks on an owned fresh database.
- Nightly shutdown: `/tmp/proxy-pool-root-acceptance-20261103/c58-nightly-shutdown.json` reports completion in 4.399ms after a real scheduled pass joined before exit.

These are independent local signed-HTTP judge smokes. They are not target-TLS, 5000-stock, Huawei, or live-production performance evidence.

## Reproduction

Normal test or race runs do not write the artifact:

```sh
go test ./tests/integration -run '^TestCoreSyntheticBoundedLoadMetrics$' -count=1 -v
```

Explicit artifact generation:

```sh
CORE_LOAD_REPORT_PATH=/absolute/path/core-load-instrumented.json \
  go test ./tests/integration -run '^TestCoreSyntheticBoundedLoadMetrics$' -count=1 -v
```

The committed `core-load.json` is the root-captured normal result identified above, not a rewritten historical observation. Final all-repo gates remain root-owned; this service-level report does not substitute for them.
