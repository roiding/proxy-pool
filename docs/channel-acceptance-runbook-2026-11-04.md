# 渠道逐项验收手册

日期：2026-11-04；目录口径更新：2026-11-05，Asia/Shanghai。

本批次是适配器、缓存、账号安全、调度和管理台开发，**不是实网验收结论**。本地合成 fixture、受控 HTTP/代理/Judge 测试不能替代部署网络上的渠道抓取和候选验活。未自动新建/启用生产渠道，也未注册、购买或批量探测免费代理。

## 1. 每次只选一个品牌渠道及其配置

1. 管理台「渠道」选择品牌模板，再选择协议/筛选/端点配置。HTTP 与 SOCKS5 不算两个品牌渠道；不同账号/项目的实例仍各自隔离。创建停用定义，检查 `site_id`、协议、页数和项目范围。
2. 仅在协议/解析器已实现的模板上显式启用。待协议、待厂商契约、TLS 不兼容项不做强启。
3. 点「刷新列表（不验活）」，到「任务」查看 `source_refresh`。`COMPLETED` 和缓存数量只代表下载/解析。
4. 核对抓取/唯一/保留/拒绝/截断数、页数、覆盖标记、硬 TTL、限流/周期时间；错误页、验证码页、半截账号分页不能算成功空列表。
5. 「任务」发起 `prewarm`：填该一个 `source_id`，先从 `count: 1` 开始，明确 basic 或 TLS 目标。
6. 用获准项目的真实 Key 获取一个候选，检查来源、真实验证范围、`cache_hit`、证据时间和租约/CD。需要目标 TLS 时，basic PASS 不能代替目标的证书验证。
7. 验收完保留脱敏报告，再决定是否开启周期刷新。遇到 AUTH、协议不符、不可用、限流或网络问题，记录实际结果，不将一次错误写成品牌永久关闭。

**不要使用登录凭证、业务 Cookie、注册信息或敏感载荷来验收公开免费代理。** 本项目只供应连接，不执行下游业务请求。

## 2. CLI：先抓取，后显式抽样

在仓库根目录执行：

```sh
# 仅列已评估的品牌目录（每个品牌一行），没有网络请求。
go run ./cmd/sitecheck -list

# 只抓取/解析指定 OpenProxyList HTTP 入口。
go run ./cmd/sitecheck -site openproxylist-http -max-pages 1 \
  -timeout 30 -json > openproxylist-http-fetch.json

# 明确抽检三个唯一候选：basic 要使用自己可达的 Judge。
go run ./cmd/sitecheck -site openproxylist-http -probe 3 \
  -judge https://judge.example.com/v1/judge -seed 1 \
  -timeout 30 -probe-timeout 6 -json > openproxylist-http-probe.json

# 或验目标端到端 TLS（不发送业务 HTTP 内容）。
go run ./cmd/sitecheck -site openproxylist-http -probe 2 \
  -probe-mode tls -probe-target example.com:443 -timeout 30 -json
```

普通 HTTP Judge 的共享秘密建议用 `PROXY_POOL_JUDGE_SECRET` 环境变量，不把秘密放在报告、URL 或截图里。CLI 默认不使用环境/系统代理，正常验证来源 TLS 证书。

- `fetch_only`：只确认抓取/解析成功，代理未验活。
- `sample_has_pass`：有抽样候选通过；报告同时保留失败类别和覆盖数。
- `sample_all_pass`：实际完成的整个所选样本通过，不保证全列表可用。
- `fetch_failed` / `not_run` / `no_proxy_pass`：未通过该次请求的对应阶段。
- 超过总预算而取消的尝试不是代理死亡证据。
- `-recheck-paused` 只用于已确认协议/解析器的暂停端点；Spys SOCKS、Spys.one、jetkai、7yip 等未知契约不能用它猜协议。
- 不将 `-all` 当作默认操作；它只检查每个已纳入公共品牌的一个主配置，不轮番抓同品牌的所有协议/筛选；SCDN 用单独 `-scdn`，可能消耗供应商免费额度。

## 3. 管理 API 与 Webshare

管理员身份才能调用以下操作；Cookie 会话的变更请求需 `X-PP-Admin: 1`：

| 操作 | 路径 / 说明 |
| --- | --- |
| 目录 | `GET /api/v1/admin/channel-templates` |
| 创建停用实例 | `POST /api/v1/admin/sources`，明确 `enabled: false` |
| 启用/保存 | `PATCH /api/v1/admin/sources/{id}`，携带当前 `config_version` |
| 有界异步刷新 | `POST /api/v1/admin/sources/{id}/refresh` → HTTP 202 + job ID |
| 缓存/限流元数据 | `GET /api/v1/admin/sources/{id}/cache`、`/fetch-state` |
| 任务 | `GET /api/v1/jobs/{job_id}`；`POST .../cancel`；刷新任务仅管理员可读/取消 |
| 小批量验活 | `POST /api/v1/prewarm`，例如 `{"count":1,"source_ids":["channel-id"],"timeout":30,"validation":{"mode":"basic"}}` |
| 项目获取 | `POST /api/v1/proxies/acquire`，获准项目 Key，明确同一 source/validation 范围 |

Webshare 模板是 `account_list + webshare_list_v1`，固定读取已分配 direct 列表，不是免费注册/取号/换号或购买接口。验收前需管理员提供真实账号 API Key 和获准项目：

```json
{
  "account": {"vendor_id":"webshare","protocol":"http","mode":"direct","page_size":100,"max_pages":10},
  "access": {"project_ids":["your-project-id"]},
  "refresh": {"mode":"demand","cache_ttl_ms":60000,"max_candidates":1000}
}
```

部署配置需显式的 `secret_encryption_key`（至少 32 字节）；不得用身份秘密或管理员 Token 代替。创建停用实例后，通过 `PUT /api/v1/admin/sources/{id}/secret` 写入 `{"config_version":N,"value":"..."}`；GET 仅查状态。轮换/清除 Key 会递增配置版本、失效旧批次和证明，但不清空项目 CD。

完整分页读失败、返回 count 改变、重复 ID、next 越域/改参数、必需字段不合法或超页都会整批拒绝；旧缓存最多使用到其原硬 TTL。供应商 `valid`、国家、检查时间只是声明，不生成本池 PASS 或虚构代理到期时间。

## 4. 判定与记录

每个入口记录：

- 日期/时区、运行代码版本与配置版本、`site_id` / adapter/parser 版本。
- 测试网络出口（默认受控直连）、来源 TLS 校验是否正常、真实 HTTP 状态/错误类别。
- 请求页数、解析/唯一/保留/拒绝数、完整/截断窗口、上游更新时间（未知就写未知）。
- 实际抽样大小/seed、Judge 或 TLS target、PASS/FAIL/取消数及原因。
- 是否写入库存（列表应为否）、账号项目隔离、证据复用与失效、CD 不因换源/换 Key 重置。
- 结论分别写「抓取通过」「样本验活通过」「部署链路通过」，不可用首页或 TCP 开端口替代后两项。

报告不保存原始代理凭证、账号 Key、提取 URL 中的秘密或供应商原始敏感 body。若真实页面与合成 fixture 不同，只修该入口的解析器/fixture，重新验收后再启用；不得打开网页 JS 执行或降低 TLS/地址策略来凑通过。

## 5. 暂不开放的产品

- Spys SOCKS 文件：版本未确认，不默认当 SOCKS5。
- Spys.one / jetkai / 7yip：协议或当前页面 fixture 待补。
- Proxy-List.download：保留协议独立暂停入口，恢复后单独诊断。
- 品赞、芝麻、太阳HTTP：产品 API、授权和免费额度契约待账号样本，不借 SCDN 请求格式拼接假适配。
- ScraperAPI / ScrapingBee / Crawlbase：现有目标端到端 TLS 契约不兼容，不关闭证书校验包装成普通代理。
- 极光旧入口：旧域名退役，不创建可运行来源。

这些提案不出现在可创建渠道目录；原因见 [渠道价值评估](channel-value-review-2026-11-05.md)。旧 parser ID 只保留诊断与既有配置兼容，不能据此新建被排除的渠道。**工程上值得构建也不等于实网验收通过**。

TheSpeedX 与快代理本批次也不默认纳入：当前证据不足以证明其边际供给值得新增维护。roosterkid 同名仓库归 OpenProxyList 的显式路径选择，不另计渠道，不自动回退，不假设两份内容相同。

## 6. 首轮实网执行结果（2026-11-05）

主代理已按品牌执行首轮真实抓取和有界TLS样本，并修正解析/双栈选择问题。结果见 [简单验收表](acceptance-2026-11-05/channel-summary.md)。**未出现稳定推荐渠道**；Spys有PASS但波动；另有网络/访问限制和缺账号项。此前本地验证记录仍只代表本地测试，不被改写为实网通过。
