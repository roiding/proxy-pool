# Cloud Usage And Bark Report - 2026-11-03 (Admin Agent)

## Scope Of This Report

This document completes the admin agent's assigned research and status
documentation scope only. It is NOT a release-green statement: the cloud
usage reader and Bark integration remain intentionally unavailable, no live
vendor verification has happened, and official acceptance of the Flexus L
balance path stays open behind the external verification gate. Nothing here
changes runtime behavior.

## Deployment Status: NOT_CONFIGURED

- `config.json` in the deployment directory contains only the base keys
  (`api_token`, `identity_secret`, `judge_*`, `listen_addr`, `database_path`,
  `lock_path`, nightly knobs). No `huawei_*`, `bark_*`, or usage-setting keys
  exist. The service therefore runs with every Huawei reader and Bark sender
  disabled by construction.
- Per the prior read-only checkpoint, the actual configured database contains
  no usage tables (`usage_settings`, snapshots, outbox, schedules). Nothing
  was initialized or migrated for this report; the usage schema only appears
  when the usage service is configured and started.
- No Huawei API call, Bark push, purchase, or signup was made for this
  report. `internal/usage/reader.go` still returns
  `ErrUsageReaderUnavailable`, which the console renders as the honest
  "official traffic basis not verified" note.

## Official Flexus L Package-Balance Research

Question to answer: how can the deployment read the official remaining
balance of a Flexus L instance (云耀云服务器 L 实例) monthly traffic package?

Verified this session (2026-11-03, no vendor calls):

- The Huawei Cloud Flexus product page
  (`https://www.huaweicloud.com/product/flexus.html`, fetched successfully,
  272 KB) shows the L-instance plans bundling a fixed monthly traffic
  package in their specs, e.g. `2核 | 2G | 系统盘 40GiB | 峰值带宽 2Mbit/s |
  流量包 200GB`. The traffic package is part of the plan purchase, not a
  pay-per-GB meter.
- The documentation site
  (`https://support.huaweicloud.com/api-bpconsole/bpconsole_apiv2_0001.html`)
  returns an HTTP 200 body of ~2 KB titled "Security Verification" (a bot
  interstitial, not the document). Bing, DuckDuckGo HTML, and the Huawei site
  search returned no parseable documentation links. The per-user browser
  surface is blocked for this agent by the audit boundary, and no circumvention
  was attempted.

Consequently the exact BSS (Billing) package-usage API endpoint, its required
IAM permission, and the accounting-period semantics remain UNVERIFIED in this
environment. The official balance must come from one of these two sources
once doc access is available; both are billing-plane facts, not metric-plane
estimates:

1. The BSS customer resource-package usage API (查询资源包使用情况 /
   resource package usage) authenticated with a least-privilege read-only
   IAM credential, queried for the specific package/resource ID.
2. The billing console's resource-package page (费用中心 → 资源包), read
   manually as a one-time human cross-check.

Generic CES (Cloud Eye) NIC traffic metrics for the ECS are explicitly NOT an
official package balance: they measure interface bytes, not the package
deduction, and must not be wired into the reader even as a fallback. Any
CES-based number stays labeled `Source: estimate` at most, per the reader's
snapshot contract.

## Implementation Consequence

- The unavailable reader stays unavailable. Configuring AK/SK without the
  verified official API changes nothing (the service still reports the
  unavailable-reader note), which matches the code's fail-closed design.
- Before a real reader lands, the verification gate must record: endpoint +
  method, IAM permission name, package/resource identifier source, billing
  period boundary (calendar month vs purchase anchor), reset time, units, and
  whether over-package behavior throttles bandwidth or bills overage.
- Bark notification behavior (thresholds notify only, deduplicated outbox,
  bounded attempts) is unchanged and inert while Bark is unconfigured.
