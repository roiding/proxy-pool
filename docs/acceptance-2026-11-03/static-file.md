# Static File Offline and Live Acceptance

Date: 2026-11-03

## Scope

This run verifies the read-only supplied static file through the production
parser and production target-TLS validator. It made no repository commit, did
not modify production configuration, routes, tokens, or databases, and did not
purchase or register any service. The supplied file and the root offline
report were not modified.

No raw URI, connection host, IP address, username, password, or temporary
credential database value appears in this report.

## Input

- Size: 78,278 bytes
- SHA-256: `1c315c5240fc0c248bdd3de61a686b87d9bb95d1f75a09627aea3ccb484b9237`
- Nonempty records: 1,400
- Production parser: 1,400 accepted, 0 rejected; every record authenticated
- Normalized transport distribution: HTTP 1,400
- Vendor expiry: not inferred from filename or modification time

The source was SHA-256 checked immediately before the live sample run. This
report does not infer vendor expiry from the filename or file modification
time.

## Offline Results

Root's prior isolated acceptance remains authoritative for durable import,
idempotency, unknown-expiry, integrity, and cleanup results:
`static-file-offline-root.json`.

- Isolated durable import: 1,400 added, 0 rejected
- Same-batch replay: PASS
- JSON cross-entry duplicates: 1,400
- Unknown provider expiry: 1,400
- Integrity: PASS
- Temporary credential database: removed

The current P0/reference regression also passed the full repository test suite
and vet. The full test run occurred before the report-only correction, so its
result covers production source and protected tests, not these documentation
edits:

```text
go test ./...   PASS
go vet ./...    PASS
```

The integration suite includes the protected 9-top-level import audit and
protected source/key guard cases. The parser boundary cases cover
unterminated userinfo in both URI and bare-default forms, text and JSON host
bounds, JSON control and URI-looking hosts, and credential redaction. Test
count statements are limited to observed top-level results because that is
the canonical group count.

## Live Execution

The actual run was executed from the local macOS host. The network egress and
geographic path are unknown. An exact wall-clock interval was not captured.
The following local metadata was observed before this report-only correction:

- Local macOS: Darwin `24.6.0`, x86_64, macOS `15.7.9`
- Executable file modification time: `2026-11-03T12:12:24.581240+08:00`
- Pre-correction JSON modification time: `2026-11-03T12:13:06.945169+08:00`
- Difference between those observed timestamps: approximately 42.36 seconds

The JSON metadata was later changed by this report-only correction, so it no
longer preserves the original timestamp. The difference between the two
original timestamps includes process execution, report generation, and
filesystem metadata overhead; it is not a measured start-to-end runtime. The
report narrative was written later.

The original executed source is preserved verbatim at `executed-probe12.go.txt`.
Its text-only extension keeps the historical live script out of normal Go
package discovery; its bytes and SHA-256 remain unchanged.
That script's `distributedIndex` divides by `n-1` (`1399`) instead of
`total-1` (`11`). Therefore the observed indices 0 through 11 are the first 12
records, not a distributed sample. A corrected future algorithm is preserved
separately at `future-probe12-distribution.go.txt`; it was not executed and did
not produce the JSON observations.

## Probe Configuration

- Records attempted: the first 12 records, indices 0 through 11
- Untested records: 1,388
- Target: `example.com:443`
- Requested method: production `validation.Probe` in TLS mode via the
  candidate's HTTP CONNECT or SOCKS5 transport
- Certificate verification was required by configuration: TLS 1.2 or newer
  with system-root certificate and hostname verification. No TLS handshake
  completed in this run, so no certificate verification occurred and a
  successful verification was not observed.
- Boundary: each sample had a 5-second total deadline
- No target application HTTP request or target response body was sent
- No retry, full-file probe, extra target, supplier expansion, or fallback
  transport was attempted

## Live Results

Single authorized run:

| Metric | Result |
| --- | ---: |
| Samples attempted | 12 |
| PASS | 0 |
| FAIL | 12 |
| AUTH failures | 12 |
| TLS handshakes completed | 0 |

Sample latencies ranged from 333 ms to 577 ms. Aggregate and per-sample index
results, without secrets, are in `static-file-live12.json`.

All 12 attempted records returned the `AUTH` class in the probe classifier's
transport-authentication failure category, before a target TLS handshake
completed. This is an observation about the first 12 records only. It is not
an independent proof of every proxy's or upstream provider's credential state.
It does not prove individual testing of the remaining 1,388 records, does not
establish that the whole supplied file's credentials are expired, and does not
establish target reachability or certificate validity.

## Execution Evidence

The command was:

```text
/tmp/proxy-pool-supply-live12-20261103/probe12
```

The observed stdout is preserved without raw connection material in
`static-file-live12-stdout.txt`. This evidence comes from the live tool result
and records aggregate output plus index, transport, result class, and latency.
The command path alone does not independently prove environment or time.

## Remaining Work

- No further probe or full-file scan was performed; this 12-attempt run is the
  complete live acceptance evidence for this pass, covering only the first 12
  records.
- Authenticated upstream access needs fresh, operator-approved credentials
  before any retry can be meaningful.
- A future retest must again use certificate verification, a bounded target
  handshake, and no application-layer request.

## Freeze

Freeze this acceptance pass at the evidence above. Do not reset the dirty
worktree, alter protected audit/flow/correctness/manifest fixtures, modify
README/source-contract ownership, or change production configuration,
database, tokens, routing, or credentials based on these results.
