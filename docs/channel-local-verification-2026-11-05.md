# 渠道开发本地验证记录

日期：2026-11-05，Asia/Shanghai。代码基线 `b248bb3`，本批次未提交的工作区。验证范围是渠道适配、缓存/权限/刷新/预热、账号秘密与品牌级目录整合。**不是公共渠道实网验收报告。**

## 本地命令

实际执行并通过：

- `go test -count=1 ./...`
- `go test -race -count=1 ./...`
- `go vet ./...`
- `go build`：`cmd/proxy-pool` 与 `cmd/sitecheck`（输出到隔离 `/tmp`）
- 提取管理台内嵌 JS 后 `node --check`
- `git diff --check`
- 新版 `sitecheck -list -json`：14 个唯一品牌，不触发网络。

macOS race 链接阶段有 `LC_DYSYMTAB` warning，测试仍以退出码 0 完成；未发现 race 报告。普通全仓测试先发现旧 fetched 分母与未知 site 错误码断言差异，已修实际语义后重跑通过；不以失败批次充作通过记录。

## 新增边界覆盖

- 17 种解析器的合成/脱敏形状、明确空/未知空、业务错误信封、HTML challenge/error、GBK/BOM、凭证保留和脱敏。
- Geonode camelCase 字段、Unix 秒时间、协议去重/过滤；Spys 厂商 SSL/Google 标记不产生本池 PASS/无完整日期不造 year-zero 时间。
- 各表头/容器和相邻列读取、demo 排除、HTTP/SOCKS5 歧义拒绝、纯 89 body/br 形状、不从 HTML 正则提取任意 IP。
- 37 个低层 endpoint 的合成 fetch matrix（暂停/未知项必须零请求）、保留原 11 ID；这是低层兼容检查，不是 37 个用户渠道。
- 后续页 401/403/429/challenge 整批失败、Retry-After、普通部分覆盖、20 页/512 KiB/1 万记录上限、取消、真实凭证参与跨页去重。
- 下载 DNS pin/地址策略、正常 TLS、禁环境代理/降级、跨域秘密头隔离、POST redirect 拒绝、总 deadline、解压字节上限/EOF 终态。
- 硬 TTL/304/CacheHit 区分、证据/出口变化/最终交付 fence、取消释放预约、source/project 权限、Key轮换、持久限流及账号完整分页。
- 品牌目录 14 个唯一名称与完整配置选项、Webshare HTTP/SOCKS5 显式私有数组、新建/旧upsert准入挡板、既有来源ID/权限不合并。

## 最终管理台受控交互

Go 服务、配置、SQLite、lock 和 Node 上游均在新建隔离 `/tmp/proxy-pool-channel-final-qa.*`。没有使用生产配置、真实供应商 Key 或用户业务载荷。

1. 管理台每个品牌一行；HTTP/SOCKS5 合并协议列，配置选项默认折叠。
2. OpenProxyList 在一个品牌内选择 HTTP 域名或同名仓库 SOCKS5，完整 `site_id` 配置正确切换，没有网络请求。
3. 创建两个停用 ProxyScrape HTTP/SOCKS5 实例，显示同一品牌组“2 个实例”，独立 ID/配置仍可选择操作。
4. 任意自定义 ID 的 Webshare SOCKS5 实例正确显示 Webshare 品牌，`access.project_ids=[]` 保留；没有 Key 时禁止启用/刷新。没有调用 Webshare。
5. 创建本地 static 列表并启用：受控上游计数仍是 list=0/proxy=0，证明保存/启用不自动抓取。
6. 手动刷新：任务 COMPLETED，1 条缓存（未验活），probed=0。
7. 对该本地列表 basic 预热：任务 COMPLETED，Ready 1/1，probed=1，failed=0；Node 代理只允许到本地签名 Judge，拒绝公共目的地。
8. 最终重建/reload 后再次手动刷新与导航/顶部刷新，详情实时显示 1 条新鲜缓存、0 个证明，证明不会从旧页面状态虚报。
9. SQLite 资源/offer 均为 0，未写持久代理库存。最终受控上游累计 list=2/proxy=1/denied=0（额外下载来自重建清空内存后的显式手动刷新）。
10. 最终管理台 console error/warn 为空；自己启动的测试服务在交付前停止。

截图：

- 品牌目录：`channel-brand-catalog.jpg`
- 刷新≠预热：`channel-local-qa.jpg`

位于本聊天可视化目录 `/Users/roiding/.codex/visualizations/2026/11/04/01a1a592-55b4-7271-a2a5-fc21ffab528d/`。截图是受控本地测试，不是公共供应商通过证明。

## 未做项

没有新增公共代理批量验活、真实商业提取、账号购买/换号、生产部署或提交/推送；生产 `config.json`、`data` 未更改。当前值得构建的品牌仍须用户逐品牌/选中配置执行真实网络 fetch/parse 与少量 Judge/目标 TLS 验收。不能从本地 fixture 推导当前网站可用率、每日免费额度、独立增量供给或进程 RSS 上限。

本轮子代理均指定临时 `jpn-cdn-moxuna/deepseek-v4.1-flash`，未用 auto。UI代理有真实共享文件落盘，主代理整合并验证；价值评估代理未及时交付文件，主代理接手完成，未将未交付工作冒称完成。
