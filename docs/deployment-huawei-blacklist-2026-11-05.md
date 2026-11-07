> 此版本已由 `unified-20261105-db86196c23dc` 替代，最新统一渠道、自动补足与表单见 [更新记录](unified-channels-and-console-2026-11-05.md)。

# 华为云临时黑名单版：部署与验收（2026-11-05）

## 当前状态

- 已升级既有 Flexus L 服务：`blacklist-20261105-1607db3b582d`。
- 管理台：<https://proxy-pool.522401.xyz/admin>，原登录方式不变。
- 沿用原 Docker Compose、Traefik、Cloudflare 来源限制与 loopback 端口，无新增公网端口。
- 一致性备份：`/opt/proxy-pool/backups/blacklist-20261105-1607db3b582d`。
- 发行目录：`/opt/proxy-pool/releases/blacklist-20261105-1607db3b582d`。
- 原配置与 GeoIP 文件 SHA-256 不变，SQLite 完整性与外键检查通过。
- 切换时保留来源 4、项目 1、库存资源 0、租约历史 17；没有清空租约/CD，也没有自动创建或开启新来源。
- SCDN/Webshare 未调用；没有注册、购买或更换代理账号。

## 生效规则

1. 网络代理实际下发或实际验活失败：同一来源临时禁用，默认 **30 分钟**，清除旧 PASS，不再重复预热/下发。
2. 黑名单持久化，下载新列表或重启不能绕过；到期后必须重新验活。
3. 管理台「下游项目 → 项目详情 → 网络代理黑名单（秒）」可调整时长；API 为 `network_reuse_cooldown_ms`。0 采用默认值，降低配置不缩短已生效的窗口。
4. 网络列表缓存 **30 分钟**；有效缓存期间复用，全部候选都被禁用也不反复拉取。并发抓取合并，缺失/失败快照仍受持久限流与退避保护。
5. 上传 `custom` 永久保存，下发后保留；只有实际验活失败才删除当前来源对应的库存。
6. 列表/上传 PASS 不再按 **30 秒**失效。代理可下发仍受准确验证范围、较新失败、权限、版本、黑名单、租约容量及项目 CD 约束；不保证物理持续在线。
7. 授权渠道使用共享有界并发探测池，轮询公平取样，按去重后的总数量凑目标。

## 最新真实云端复测

2026-11-05 **21:53:16–21:54:37 北京时间**，使用既有三个启用网络来源：Free-Proxy-List、ProxyScrape、Spys。基础验活访问生产签名公网 Judge，正常 TLS 校验。

| 步骤 | 实际结果 |
| --- | --- |
| 第一轮预热，目标 3、预算 45 秒 | 探测 **48**，失败 **46**，结束待下发 **2/3**，`PARTIAL/deadline` |
| 等 36 秒，目标改为已有的 2 条 | 待下发 **2/2**，探测 **0**，失败 **0**，`COMPLETED` |
| 领取 1 条 | 实际下发 **1**，`cache_hit=true`，同来源黑名单增加 |
| 测试租约 | 已显式释放；不会撤销已经形成的临时黑名单 |
| 公网链路 | HTTPS 健康/管理页 200，匿名管理 API 401；签名 Judge nonce/HMAC 校验通过 |

第二轮目标 2 是为了验证已有 PASS 不再 30 秒过期，不是把第一轮 2/3 改称为 3/3 达标。免费供给有波动，本轮不承诺 100 条目标一定凑齐，不代表所有品牌已通过验收。

最新脱敏文件：

- [部署收据](acceptance-2026-11-05/huawei-blacklist-deployment-receipt.json)
- [真实预热、36 秒复用与下发结果](acceptance-2026-11-05/huawei-blacklist-validation-smoke.json)
- [有效缓存的手动刷新复用与服务状态](acceptance-2026-11-05/huawei-blacklist-cache-reuse.json)
- [最终构建、测试与清理摘要](acceptance-2026-11-05/huawei-blacklist-verification.json)

22:01:53 北京时间再次对上述三个来源逐一点击等效的手动刷新 API：全部 `COMPLETED/cache_reused`，下载批次、抓取时间和持久抓取状态不变。容器 `running healthy`；最近 15 分钟应用 ERROR 为 0，有 1 条 `usage check failed` 的云流量统计 WARN，不是代理验活错误。

## 受控本地管理台验收

只使用自建 localhost 列表、代理与签名 Judge，不使用供应商凭据或公共代理：

- 下发后缓存显示 **候选 1 / 已通过 0 / 黑名单 1**。
- 点击手动刷新并再次预热，列表请求次数保持 2、代理请求次数保持 1，均未增加；第二次预热待下发 0、探测 0，结束原因 `inventory_exhausted`。
- 项目黑名单配置 **1800→1801→1800 秒**，保存和回读通过。
- [受控本地 API 收据](acceptance-2026-11-05/blacklist-local-ui-receipt.json)。

该验证证明缓存/黑名单路径，不作为厂商可用率证据。

## 构建与数据安全

- 本机全仓 normal、race、vet、两命令构建、管理台 JavaScript 语法与 diff-check 均通过；候选自身超时判定修复和收尾格式化后再次完成全仓 normal/race/vet。
- 目标 Linux 隔离构建：全仓 `go test -p 1 -count=1 ./...`、`go vet -p 1 ./...` 通过，两个 CGO 静态 amd64 ELF 构建通过。
- 无网络、无发布端口的数据库一致副本迁移预演通过；正式升级前停服务，完整备份配置/数据及存在的 WAL/SHM，并校验。
- 未提交/推送公开仓库。发行包只含白名单源码，不包含本机配置、数据库或 SSH Key；收尾仅做本地 Go 格式化与文档补录，运行代码行为未再变更。
- 完整源快照 SHA-256：`1607db3b582d8f3ac9225c1173c251ec6ce9f3ee478eca11ac57657b58bb9493`，基线 `b248bb38367a68bea1552eef195860864191d74d`。
- `proxy-pool` SHA-256：`f44674861cecf80bcad2f198af586e7c9c71aee41bca981661ee660135100930`。
- `sitecheck` SHA-256：`28cdf31584dff3629a749d2bc26151b97cc22c2b9e8d8fcb1069138be646174b`。

回滚前先保护当前数据库及新租约/CD。不得直接恢复旧数据库抹掉升级后状态；先验证旧程序与当前 schema 的兼容性，保留当前和旧版本发行包及一致性备份。
