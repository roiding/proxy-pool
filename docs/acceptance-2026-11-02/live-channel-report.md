# Live Channel Check - Human Report (2026-11-02)

Source of truth: `live-channel-check.json` in this directory. No new live
vendor, catalog, or SCDN calls were made for this report; every number below
is read back from the saved machine record.

## Verdict

Genuine failed live acceptance. Across the whole acceptance window not one of
the 32 TLS probes passed, so the live channel did not demonstrate working
proxy supply. Nothing in this run was a fixture or synthetic success: every
fetch hit the real public sites and every probe dialed through a real proxy
candidate to `example.com:443`.

## What ran

- Window (Asia/Shanghai): 2026-11-02 20:55:18 to 21:19:22, on the temporary
  pre-23:00 channel; baseline git HEAD `52b69c2`; sitecheck binary
  `abbe9881...` (sha256, first 8 shown).
- Denominator: 12 channels - 11 catalog sites + 1 SCDN provider channel.
- Actual requests: 27 catalog fetch requests + 2 SCDN HTTP calls = 29 channel
  requests. No target-site business requests were made
  (`target_http_business_requests: 0`).

## Result by phase

- Primary sweep: 6 of 11 catalog sites fetched (10,319 entries parsed,
  14 parser-rejected); 12 TLS probes attempted, 0 passed, all 12 failed with
  kind `PROXY`. The SCDN call failed with `connection reset by peer`
  (requested 2, fetched 0; diagnostic only - on-demand candidates never enter
  the pool).
- Targeted retests (5 sources): `qiyunip-elite` returned HTTP 403 again;
  `openproxylist-https`, `proxyscrape-anon`, and `proxyscrape-elite` timed out
  again; `lumiproxy-transparent` fetched 19 entries but both probes failed
  (`PROXY`). No retest passed.
- Probe summary: 32 attempted, 0 passed, 32 failed at the proxy hop. Fetched
  entry counts describe parsing only; they say nothing about liveness.

## Incident disclosure (required)

The second SCDN HTTP retry omitted `-site`, so in addition to its SCDN call it
repeated all 11 catalog checks as one accidental extra full sweep. That sweep
fetched 7 of 11 sites (15,351 entries parsed, 16 rejected) and ran 14 more
probes (0 passed). This was not an authorized retest; no calls of any kind
followed it. It is included in the 29-request/32-probe totals above and in the
`accidental_extra_sweep` block of the saved JSON.

## Interpretation

The acceptance failed on supply quality, not on tooling: sites that served
content produced parseable candidates, and every probe reached the dial stage
but died at the proxy. Timeout sites (4 of 11) and the 403 site are
reachability/anti-bot issues on the catalog side, while the SCDN vendor
endpoint refused both connections. None of these outcomes authorizes treating
any candidate as validated; per the supply contract, live failures are not
fixture passes and no PASS evidence was created anywhere.
