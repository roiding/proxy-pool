# 固定渠道与统一开关（2026-11-08）

已发布 `fixedchannels-20261108-c6f972d999cb`，管理台沿用 <https://proxy-pool.522401.xyz/admin>。渠道页直接列出内置供应商，每个渠道只有一个开关；HTTP/SOCKS5 是该渠道支持的协议。协议路径、抓取参数和页数由程序管理，移除了模板选择、协议选择、新建网络渠道及配置编辑表单。上传功能和程序上传密钥折叠收起。

## 渠道行为

- 开启渠道时，在一个事务中启用既有来源，并补齐缺少的内置协议供给；关闭时一起停用。读取渠道清单不会创建来源或抓取列表。
- 程序启动时为已开启的公共渠道补齐内置协议，重复启动不重复创建，也不改变既有来源版本。已关闭的渠道保持关闭。
- 同协议的匿名筛选或镜像不作为新的内置供给。OpenProxyList 保留指定 API 域名入口，未自动启用同名仓库路径。
- 未接入凭据的 Webshare 显示“待接入账号”，开关不可用。既有账号的凭据与私有访问范围继续独立保存。
- 新增协议继承原来源的访问上限；项目的显式来源允许列表继续生效。原来源 ID、配置、库存、租约及黑名单不因渠道管理分组而合并或删除。与供应商同名的上传来源仍是独立上传渠道。
- 开关采用版本检查，并在事务中复核来源集合，避免并发新增或修改导致部分协议已开启、部分未开启。

管理接口为 `GET /api/v1/admin/channels` 与 `PATCH /api/v1/admin/channels/{id}`，开关请求包含 `enabled` 和 `version`。旧来源 API 继续兼容集成程序。

## 补足与线上结果

当前默认项目自动维持目标为 **10**，达标后的检查间隔为 **5 分钟**，沿用 11 月 7 日已经生效的设置。保留自动补足、同范围 PASS 复用及候选耗尽后的等待规则，没有恢复成纯请求时验活。

线上有 **15 个渠道入口**（14 个内置供应商和上传来源 `own`），**5 个已开启**：Free-Proxy-List、ProxyScrape、SCDN、Spys、own。Free-Proxy-List、ProxyScrape、SCDN 补上了 SOCKS5，其旧 HTTP 来源定义与版本完全保留。未开启其他供应商。

上传供给记录保持 **87**，租约历史保持 **19**，项目保持 **1**。原配置文件哈希、项目完整配置和旧来源定义检查一致；SQLite 完整性及外键检查通过。容器 `healthy`，重启次数 0。

公网 HTTPS 管理页、健康接口为 200；匿名渠道管理 API 为 401。通过正常 TLS 校验取得的公网管理页与发行源码逐字节匹配。

## 验证与恢复材料

本机全仓 normal/race/vet、JavaScript 语法和 diff 检查通过。真实本地认证 API 配合浏览器验证了固定渠道清单、ProxyScrape 单开关启停 HTTP/SOCKS5、配置表单移除、上传折叠及未接入账号的禁用开关。该本地检查没有创建验活/刷新任务，也未探测公共代理。

目标 Linux 全仓 normal/vet 和两个 amd64 静态二进制检查通过。正式切换前，真实数据库完整副本在无网络、无发布端口的隔离容器中启动并检查通过；仅副本的自动目标改成 0，生产目标始终为 10。

完整停服备份：`/opt/proxy-pool/backups/fixedchannels-20261108-c6f972d999cb`。备份包含数据、配置、Compose 与逐文件校验清单。发行目录：`/opt/proxy-pool/releases/fixedchannels-20261108-c6f972d999cb`。Compose 仅切换镜像，原网关、端口、登录方式与数据目录沿用；旧镜像与备份保留。没有提交或推送 Git。

记录：

- [本机测试及浏览器操作](acceptance-2026-11-08/local-verification.json)
- [Linux 构建摘要](acceptance-2026-11-08/build.json)
- [升级前运行设置](acceptance-2026-11-08/huawei-before.json)
- [数据库副本启动](acceptance-2026-11-08/huawei-preflight.json)
- [备份、部署及库存保留](acceptance-2026-11-08/huawei-deployment.json)
- [线上渠道及项目](acceptance-2026-11-08/huawei-runtime.json)
- [公网完整页面及认证](acceptance-2026-11-08/public-check.json)
