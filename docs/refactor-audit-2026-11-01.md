# 本轮重构审计与验收记录

创建日期：2026-11-01，Asia/Shanghai。续接实施与复核日期：2026-11-02。

当前状态：新增需求的实现已落盘，但 2026-11-03 独立全仓复核尚未通过；真实外部渠道、云接口及浏览器验收也未完成，不代表已经部署。最新结果见第 6.10 节。

## 1. 依据与分工

唯一的最新业务基线为 `docs/project-repositioning-plan-2026-11-01.md`。
旧文档和旧测试与它冲突的行为不作为本轮需求；不通过删除测试或修改需求来获得通过。

最初按用户要求使用虾蹬 `kunyou/deepseek-v4.1-flash-1017` 子代理。
20:27 用户改为要求使用 ZCode GLM；旧实现代理均已停止，保留已落地的共享类型。
后续三个实现子代理均明确以 `zcode/GLM-5.3-Flash` 独立启动：

| 执行者 | 职责 |
| --- | --- |
| `core_glm` | 项目与密钥、固定 CD、迁移、原子交付、取号调度、可选粘性 |
| `supply_glm` | 适配器、上传与静态缓存、API 临时候选、目标 TLS、健康与任务 |
| `admin_glm` | 下游及管理 API、配置、管理台、云用量预留、安全与 CI |
| 主代理 | 独立审计、黑盒及迁移回归、集成验收、问题反馈和修复复核 |

2026-11-02 续接时前一批代理已结束；实现继续由同一指定模型的
`core_finish`、`supply_finish`、`admin_finish` 接手对应模块。
用户特别明确：只按讨论的新契约实施，不把旧代码的 8 秒默认值或旧业务 API 当作兼容目标。
必要的数据、凭证与已生效 CD 保留不等于保留旧业务行为。

主代理不替子代理实现生产业务代码。审计测试与旧契约断言的修订由主代理完成。
所有代理共享工作区，按文件分工，跨模块的类型与迁移先协调再修改。

2026-11-02 本次续接的模型调用记录：

- ZCode 的三个收尾代理均报告 `exceed quota limit`；用户要求继续后对管理代理恢复一次，仍是相同额度错误，未反复重试。
- 用户随后指定“临时”渠道 `oai-freestyle-cc/deepseek-v4.1-flash`，三个代理在启动时均报告 `404 Invalid URL (POST /responses)`，没有进入代码执行。未擅自修改渠道配置或改用别的模型。
- 用户再明确指定虾蹬 DSF，三个实现代理改为 `kunyou/deepseek-v4.1-flash-1017`：`core_dsf`、`supply_dsf`、`admin_dsf`；职责与核心、供给、管理的原分工相同。临时渠道由用户自行排查。
- 切换前保存的当前差异快照：`/tmp/proxy-pool-ds-handoff.kMPQ3i`。模型切换不重置工作区，未授权提交、推送或部署。
- 虾蹬三个代理已确认读取完整需求并实际修改了管理 API、前端、静态缓存与预热等代码，尚未收尾验收。
- 用户随后要求改回“临时”渠道，先中断虾蹬三个执行者，确认均为 interrupted，再由 `core_temp_finish`、`supply_temp_finish`、`admin_temp_finish` 以 `oai-freestyle-cc/deepseek-v4.1-flash` 接续。此时的完整差异快照为 `/tmp/proxy-pool-temp-resume.YCcGsn`；旧代理不再继续写文件。切换时集成编译仍有 `usage/service.go` 的未使用变量中间态，已交给管理接续代理修复，不能把该状态当作发布完成。
- 三个临时渠道代理随后均进入了实际读取、修改和本地测试；本次没有再出现前次启动的 `/responses` 404。主代理仍只修改独立审计测试和审计记录，不接管生产实现。
- 随后核心代理因新请求返回 `403 Insufficient user quota, remaining ¥0` 中断，保留未完成的证据与 sticky 交付修改。供给代理曾越过分工尝试修正 `evidence.go` 的 SQL 参数但未完成；主代理发现后停止新改动并请用户确认，未回滚任何文件。冻结快照为 `/tmp/proxy-pool-after-quota.cgod4F`。
- 用户再次确认“继续，临时额度还有额度”后，三个原临时代理各恢复一次。核心和供给的新请求仍分别报告 403；管理代理实际完成了读取、README 修正和检查。该差异不能解释为所有临时账号都没有余额，也不因此修改用户渠道或凭证。
- 两个失败代理保持停止。管理任务冻结后，主代理明确将核心文件所有权移交给 `admin_temp_finish` 收尾，仍沿用原临时模型；不是让主代理接管实现，也不允许停止的旧代理继续并发写核心文件。后续修复结果见第 6.4 节。
- 接手代理的新核心请求随后也报告 403。中断前它确实留下了证据 SQL、foundation fixture 时间、sticky 租约与提交守卫的部分修改，全部保留；没有因为自动错误消息缺少工作报告而假设它完全未写代码。最新编译中间态及诊断见第 6.5 节。
- 用户随后要求改 CPA，并在实际创建前改为商汤 DSF 4.1；CPA 代理没有启动。三个 `imported-9/deepseek-v4.1-flash` 创建请求均被当前子代理工具拒绝为 `Unknown model`，工具返回的可用列表只有五个 `auto/...` 别名。复查没有新代理被创建，也没有擅自用自动路由代替商汤。这是子代理工具的模型 ID 支持错误，不是已调用商汤后发现额度不足。当前切换快照为 `/tmp/proxy-pool-shangtang-handoff.RjjR6u`。
- 用户随后明确指定 HUB DSF1017。使用准确 ID `hub/deepseek/deepseek-v4.1-flash-1017` 创建核心接续代理时，工具在创建前返回 `Unknown model`；实际可用列表为 `auto/deepseek-v4.1-flash`、`auto/claude-sonnet-5-5`、`auto/claude-haiku-5-5`、`auto/glm-5.3-flash`、`imported/gpt-5.6-sol`。未继续提交两个相同的无效创建请求，未启动任何 HUB 子代理，未调用 HUB 模型或测试其额度，亦未用自动路由冒充 HUB。切换前的完整差异快照为 `/tmp/proxy-pool-hub-resume.SKWUgf`，受保护审计测试的 SHA-256 清单核对通过，生产文件未因本次模型选择而改动。
- 用户说明已更改模型名称，并授权再尝试一次。使用此前的 HUB ID 重试，工具仍在创建前返回 `Unknown model`，返回的五个可用 ID 不变，仍无新子代理。更名后的完整工具 ID 尚未提供，需确认新 ID 后再创建；不猜测名字或自动换路由。
- 随后同一 HUB ID 已能成功创建核心、供应和管理三个子代理，并进入实际读取、修改和测试；不能把此前的创建失败当成后续没有执行。它们修复了粘性私有复用、提交期限、国家匹配、静态缓存预算及渠道删除重建的版本 ABA，实际工作区的 74 项独立审计通过。恢复执行后，三个 HUB 请求均报告 `stream disconnected before completion: HUB: Invalid API key`；这不是文件权限问题，没有修改用户 API Key 或路由配置。
- 用户最新明确改用“临时”的 DeepSeek 4.1 Flash 后，重新创建 `core_temp_final3`、`supply_temp_final3`、`admin_temp_final3`，模型为 `oai-freestyle-cc/deepseek-v4.1-flash`，继续原文件分工；三个均已进入运行。此次实际权限已变为 unrestricted / never approval，没有沿用此前的沙箱限制解释错误；仍不因此授权真实渠道取号、云操作、推送或部署。

## 2. 原始状态保护

- 起点是本地提交 `52b69c2` 加原有未提交改动，而非干净的提交树。
- 原有 API 不入池改造、管理页修改、README 入口和最新规划都保留并继续完善。
- 原 `tests/integration/provider_test.go` 已处于删除状态，按新契约重写，而非恢复旧的 API 入池断言。
- 原始差异及需求的临时快照位于 `/tmp/proxy-pool-audit-baseline.YR6w5v`。
- 模型切换时另保存差异快照 `/tmp/proxy-pool-model-handoff.eyEouK`；切换不重置任何修改。
- 本次收尾的完整差异与非忽略新文件快照为 `/tmp/proxy-pool-resume-final.YOUJkx`，保留最后的生产编译中间态、69 项独立审计及已有改造，不包含被忽略的真实配置/数据库。
- 最新临时三组重启前另保存 `/tmp/proxy-pool-temp-final3-ybaagctb`，包含当时实际工作区的 tracked patch 与非忽略新文件，不包含真实配置或数据库。模型切换不重置文件。
- 最终冻结快照为 `/tmp/proxy-pool-temp-final3-frozen-6fcmzy_m`，保存最终差异、非忽略新文件、全仓普通/race/vet 日志及受保护 SHA-256 清单；没有把真实配置或数据库打包进去。
- 不改实际配置、数据库、管理密码或 Token；不提交、推送或部署。

## 3. 初始检查

重构开始、审计测试加入之前，执行 `go test -count=1 ./...`，全部通过。
这仅证明原工作区在旧测试下通过，不证明最新需求已实现。

已加入独立审计测试：

当前共有 81 个顶层 `TestAudit`，不含其子场景、实现代理自有测试或旧测试文件。

| 测试文件 | 检查重点 |
| --- | --- |
| `tests/integration/audit_contract_test.go` | 严格参数、默认空结果、总超时保留已通过部分、来源范围、URL 凭证拒绝、TLS 不发送业务正文 |
| `tests/integration/audit_allocation_test.go` | 跨来源与 IPv4-mapped 地址不能缩短 CD；同出口的不同连接独立；API 渠道在探测中停用不能提交 |
| `tests/integration/audit_validation_test.go` | 正确代理认证头、地址策略变化使缓存失效、失败完成时间、CONNECT 主动取消 |
| `tests/integration/audit_projects_test.go` | 新项目多 Key / 轮换、来源空数组与 null、默认目标 TLS、任务归属 |
| `tests/integration/audit_evidence_test.go` | 旧证据写入不得重新保鲜；晚到旧 PASS 不得覆盖更新的 FAIL |
| `tests/integration/audit_sticky_test.go` | 绑定、交付与 CD 原子性；粘性复用仍执行来源、项目授权、协议、国家与目标验活约束 |
| `tests/integration/audit_jobs_test.go` | 新任务不能把当前运行任务伪装成重启中断；任务总预算主动取消进行中的探测 |
| `tests/integration/audit_supply_test.go` | API 默认偏好；冷/热静态缓存接入取号但不进入库存；过期 304 刷新；单飞等待者取消 |
| `tests/integration/audit_cache_test.go` | 更新来源配置导致缓存失效后，旧批次必须释放字节预算，不能挤掉下一批合法候选 |
| `tests/integration/audit_health_race_test.go` | 旧 AUTH 探测晚到，不能删除相同连接重导后新 incarnation 的对象 |
| `tests/integration/audit_usage_test.go` | 秘密空字符串保留、null 清除、设置失败整批回滚、Bark 错误脱敏及实际拨号地址策略 |
| `tests/integration/audit_migration_test.go` | 使用真实旧 DDL 和旧指纹算法迁移，保留旧 Token、上传库存、未到期 CD，重启不失效 |
| `tests/integration/testdata/legacy_v6.sql` | 冻结的改造前 v6 schema；不从未来 schema 反向生成旧库 |
| `tests/integration/flow_test.go` | 把旧 503、CD 释放及失败黑名单断言改成最终业务规则，保留测试场景 |

## 4. 审计门槛

| 范围 | 不能放行的问题 |
| --- | --- |
| 身份与 CD | 使用 Key ID 代替项目；换 Key、来源、地址写法或传 CD=0 解除旧窗口；库存删除级联清 CD |
| 原子交付 | CD 写入错误被忽略；未入选响应的候选启动 CD；并发两次通过同一项目固定窗口 |
| 超时与取消 | 慢探测使先成功的部分全部丢失；单代理超时累加；达标后继续无界取号或多发 |
| 来源资格 | 停用、配置更新、权限撤销或熔断后，旧候选仍能提交；空授权范围退化为所有来源 |
| API 生命周期 | API 进入公共库存、预热或周期扫描；普通历史记录成为可复用的影子库存 |
| 验活证据 | 基础 PASS 冒充目标 TLS；目标 A 冒充 B；新负面证据、配置或凭证变更后仍命中旧 PASS |
| 网络安全 | DNS 检查与实际拨号分离；私网、映射 IPv6 或重定向绕过；环境代理绕过下载边界 |
| 健康与删除 | 不确定超时按次数永久删除；目标或 Judge 异常判代理死亡；删除重导后旧任务复活对象 |
| 预热任务 | 没有任务归属或取消；Ready 累加而非当前资格；预热写 CD 或调用按需 API 囤货 |
| 粘性 | 默认建立绑定；新会话伪装命中绕 CD；旧换绑覆盖新版；失效连接快照继续返回 |
| 凭证与管理台 | 明文秘密回读、写日志或 localStorage；脱敏值覆盖真秘密；不可信文字通过 HTML 执行 |
| 用量告警 | 未核实接口就伪造官方用量；失败写零；阈值或通知故障自动阻断取号 |
| 发布 | 构建成功代替测试；无版本追溯；没有旧数据库迁移或恢复检查 |

## 5. 外部核验边界

此前对 SCDN 文档的只读访问未成功：终端报告连接重置，独立浏览器报告连接关闭。
没有关闭证书验证或绕过浏览器安全，也没有因此把真实渠道验收记为通过。

本次续接从用户已打开的 Chrome 文档页成功只读取得接口说明：
`GET https://proxy.scdn.io/api/get_proxy.php`，数量默认 1、最大 20，参数为 `protocol`、
`count`、`country_code`，成功 JSON 含 `code`、`message`、`data.proxies` 和 `data.count`。
当前 SCDN 适配器的地址、参数、上限与 envelope 与这份公开说明相符。文档支持的 `all`
和 `socks4` 不等于本服务已支持：前者没有逐条协议标识，后者不在本轮传输能力范围，
因此本服务要求显式 HTTP/HTTPS/SOCKS5 配置。未点击取代理示例、调用真实供应商 API
或探测其代理；这只更新文档核验结果，不提升为真实渠道获取/验活通过。

全渠道验收以实际注册表和渠道实例为分母。Fixture 解析、真实候选获取、真实代理验活
是三个不同结果；未配置凭证、网络不可达或尚未实测的渠道必须明确标记。

华为云 Flexus L 套餐的官方用量接口、权限、账期及计费方向尚未核实，也没有用户云凭证。
管理配置、加密存储及可注入接口可以实现和隔离测试；不能据此宣布真实每日查询已经可用。

## 6. 已复现与复核

配置增量的默认 30 秒 / 可配上限 120 秒、安全参数及缓存窗口测试通过。
主代理发现并反馈了 Duration 溢出、非有限 jitter、非法 Bark scheme 和有效缓存窗口的风险；
配置子代理已修正并加入回归。完整服务尚未验收，不能将配置包通过当作全仓通过。

目标 TLS 子代理的原测试与竞态测试通过后，主代理又独立复现以下问题：

| 编号 | 级别 | 已复现问题 | 独立回归 | 状态 |
| --- | --- | --- | --- | --- |
| T01 | P1 | CONNECT 将代理凭证写入 Authorization，而非 Proxy-Authorization；正确凭证也被误判 AUTH | `TestAuditTLSUsesProxyAuthorizationHeader` | 已修复，独立回归通过 |
| T02 | P1 | CONNECT 等待只设置固定 deadline，不响应主动取消，拖延达标早停 | `TestAuditTLSConnectWaitRespondsToCancellation` | 已修复，独立回归通过 |
| T03 | P1 | 缓存安全摘要遗漏 AllowPrivate，收紧策略后仍可复用宽松证据 | `TestAuditTLSCacheSecurityHashIncludesAddressPolicy` | 已修复，独立回归通过 |
| T04 | P2 | 失败路径 CompletedAt 为零，按时间查询的统计丢失失败探测 | `TestAuditTLSFailuresHaveCompletionTimestamps` | 已修复，独立回归通过 |

为避免被其他模块编译中间态阻塞，复现使用临时 Go overlay 独立运行验证包；
永久回归仍保存于仓库的 `audit_validation_test.go`，不会依赖临时文件作为最终交付。
另要求补充恶意 CONNECT 超长响应头及声称响应正文的有界读取 / 取消测试。

2026-11-02 起始全仓测试：各库单测通过，集成测试有 6 项未通过：总超时丢掉已验证部分、
新 API 丢失目标 TLS 范围、夜间不确定失败被删除、认证失败留下 tombstone，以及两项仍按
旧“供给不足必须报错”断言的测试。已按模块交回实现代理，未将该状态当成可发布。
新增地址策略审计又复现 CGNAT 云元数据地址、0/8、基准测试 / 保留 / 广播地址漏拦截，
统一地址策略修复和复核仍在进行。

### 6.1 本次续接独立复核

完整 `go test -count=1 ./...` 的一次基线运行：库单测通过，集成测试 5 项失败；
`go vet ./...` 在当时工作区通过。这不是最终通过记录。
其中两项 CD 测试被租约 ownership identity 错误挡在 CD 断言之前；主代理把内部释放 helper
改为 KeyID，并单列真实 HTTP 的 owning-key 释放测试，使 CD 与 handler 缺陷分别可复现。
旧页面测试只把“控制台”标题断言改为新页面的“proxy-pool 管理台”，没有放宽认证或业务要求。

新增审计后，`go test -count=1 ./tests/integration -run '^TestAudit'` 实测有 13 个顶层测试未通过。
其余固定 CD、真实 v6 迁移、默认目标 TLS、总预算保留部分、秘密事务与已有网络安全审计
在该次运行中未报告失败。下表只记录已复现的缺陷，不能通过删除或放宽断言来放行。

| 编号 | 级别 | 已复现问题 | 回归测试 | 续接状态 |
| --- | --- | --- | --- | --- |
| C01 | P1 | 绑定写入失败后，粘性请求仍交付 1 条并留下租约与 CD | `TestAuditStickyBindingPersistenceIsAtomicWithDelivery` | 交回核心实现 |
| C02 | P1 | 粘性旧快照绕过停用来源、项目撤权、本次来源/协议/国家及目标 TLS | `TestAuditStickyReuseHonorsCurrentEligibility` | 六个子场景均复现，交回核心 |
| C03 | P1 | 租约或 CD 事务失败被吞成 HTTP 200 普通供给不足 | `TestAuditDeliveryPersistenceFailuresAreNotSupplyShortfalls` | 存储原子回滚成立，错误报告仍需修 |
| C04 | P1 | 显式空 Authorization 携带有效 cookie 时恢复为管理员身份 | `TestAuditEmptyAuthorizationNeverFallsBackToAdminCookie` | 空值与空白值均复现，交回管理 |
| C05 | P1 | 默认调度先将上传库存排在可用按需 API 前 | `TestAuditDefaultSupplyPrefersOnDemandAPI` | 交回核心与供给协作 |
| C06 | P1 | 静态 URL 的冷/热缓存均没有接入下游取号 | `TestAuditStaticURLSuppliesAcquisitionWithoutPublicInventory` | 两种状态均复现，交回核心与供给 |
| C07 | P1 | 创建新预热任务把另一条仍在网络探测中的任务改成 service_restart 中断 | `TestAuditStartingPrewarmDoesNotInterruptAnotherLiveJob` | 交回供给 |
| C08 | P1 | 150ms 预热总预算到 750ms 仍 RUNNING，探测未取消 | `TestAuditPrewarmTotalBudgetCancelsInFlightProbe` | 交回供给 |
| C09 | P2 | 项目来源 PUT 的显式 null 被指针解码为未提供，返回 400 | `TestAuditProjectEmptyAllowlistAndExplicitNullAreDistinct` | 同类 PATCH 清除语义也需修 |
| C10 | P2 | HTTP handler 把项目 ID 当成租约 KeyID，原密钥无法释放自己的交付 | `TestAuditOwningKeyCanReleaseItsDeliveredLease` | 其他 Key 拒绝成立，owner 错误交回管理 |
| C11 | P2 | 过期缓存收到合法 304 后永久无法复用被保留的内容 | `TestAuditExpiredStaticCacheCanRevalidateWith304` | 交回供给 |
| C12 | P1 | 单飞刷新等待者不响应自己的 deadline | `TestAuditStaticRefreshFollowerHonorsItsOwnCancellation` | 交回供给；结果未写入 flight 也已代码定位 |
| C13 | P2 | 缺少完成时间的 PASS 被存储层补成当前时间，成为新鲜证据 | `TestAuditEvidenceWithoutCompletionCannotBecomeFreshPass` | 交回核心 |
| C14 | P2 | AUTH 删除资源/来源关系后，按当前 offers JOIN 的日报丢失真实失败统计 | `TestAcquireFailureRecordedAsFail` | 完整基线实测失败，交回供给 |
| C15 | P1 | 已验证候选在最终响应因证明过期被剔除，但遗留项目 CD | `TestAuditUndispatchedExpiredProofDoesNotStartCooldown` | 已独立复现，交回核心；并发清理不能删别的交付窗口 |
| C16 | P1 | 删除后重导为新 incarnation 的资源，仍被晚到的旧 AUTH 探测删除 | `TestAuditLateAuthFailureCannotDeleteReimportedIncarnation` | 已独立复现，交回核心与供给 |
| C17 | P2 | 目标 CONNECT 返回 502，被错误归因为代理本身死亡证据 | `TestAuditTLSUpstreamConnectFailureIsNotProxyAuthenticationOrDeath` | 已独立复现，交回供给 |

另外补充了整秒与分数秒的精确时间顺序测试：当前 `sqlite.fmtTime` 已使用固定九位小数，
`TestAuditSameSecondNewFailureHidesWholeSecondPass` 独立通过。它是保持原修复的回归门槛，
不是当前未修缺陷。并发清理不得解除另一请求 CD 的测试、配置引导秘密的 null 清除不能在
重启复活、SKIPPED 不消耗真实通知重试额度，以及严格拒绝 null 数量和空国家代码的测试
已补充；需在正在编辑的模块恢复可编译后复核，不把尚未运行的断言写成“已通过”。

独立竞态子集 `go test -race -count=1 ./internal/domain ./internal/validation
./internal/integrations/staticdl ./internal/integrations/scdn ./internal/integrations/sites`
已通过。这只覆盖这些库，不替代集成、任务、交付或前端的最终竞态验收。

### 6.2 管理台只读检查

只使用 `/tmp/proxy-pool-ui-qa.8Pzznq/overlay.json` 启动的临时本地 fixture，生成假的测试身份、
数据库及 header shim，不改真实配置或密码，不执行新增/删除/保存设置、云查询或 Bark 推送。
桌面和 390×844 手机视口中，已授权 GET 已渲染数据库数据，但 `login.hidden=false`、
`display:flex` 的登录遮罩仍阻挡管理页。源码启动只调用 `switchView`，成功读数据未隐藏遮罩；
成功登录设置 hidden 后还缺少优先于 `#login` display 的隐藏规则。这是未通过的 UI 验收，
不能把 JavaScript 语法检查通过当成实体操作或响应式验收通过。手机视口覆盖已恢复默认。

管理前端部分修复落地后，又用 `/tmp/proxy-pool-readonly-frontend.0EOCCf/server.cjs` 临时服务
读取当前 HTML，仅转发上述 fixture 的 GET，所有非 GET 强制 405。此检查不冒充当前生产
主程序与 API 的完整集成验收。新前端登录遮罩已正确隐藏，桌面八个实体页及项目详情能够
读取，密钥只呈现前缀，控制台未报告错误；但有以下接线与响应式缺口，已交回管理实现：

- 新渠道表单将 provider/site/static 的字段发送在 config 顶层，与新 API 严格的嵌套配置不符；应修表单，不能添加旧扁平参数兼容层。
- 手机 390px 项目详情的页面 scrollWidth 为 493px，长密钥 ID 表格撑宽整页，应改为局部横滚或断行，不能把页面能加载当作响应式通过。
- 任务创建未显示当前默认项目主体；项目选择或默认主体说明应明确，避免将默认项目 Ready 误解为别的项目资格。
- 真实主程序调用 usage.New 前缺少用量表初始化；fixture 手工初始化掩盖了该缺口。需用临时新数据库执行真实二进制启动检查。

前端测试没有创建密钥、保存配置、删除记录、调用供应商或发送推送。第二次手机视口覆盖也已恢复。

### 6.3 临时渠道接续复核

本轮 48 个顶层独立审计测试的一次完整运行耗时 41.621 秒；仍有五个顶层失败：
并发独立 CD 交付、独立项目取号、六个粘性当前资格子场景、默认 API 偏好，以及顶层 JSON
null。其余测试在该次运行未报告失败，包括绑定写失败的原子回滚、租约/CD 持久化失败 5xx、
静态冷/热缓存获取、304 重验证、等待者取消、晚到 AUTH 的 incarnation 保护和预热总预算。
该状态不是全仓通过，也不是最终验收。

接着加入更细的独立回归，该阶段记录时共有 60 个顶层审计测试；当前新增测试见第 6.4 节。
新增实测与代码审读：

| 编号 | 级别 | 问题或门槛 | 当前独立结果 |
| --- | --- | --- | --- |
| C18 | P1 | 候选 SELECT 真正 SQL 失败、预约 INSERT 触发失败仍被当成 HTTP 200 供给不足 | 两个子场景均失败，交回核心；普通资格/容量短缺仍应允许部分结果 |
| C19 | P1 | 默认每日用量检查在当天时点已过且 Reader 立即失败时热循环 | 150ms 假 Reader 被调用 561 次；修复后的同一回归已通过，不调用真实云接口 |
| C20 | P1 | 并发阈值检查越过同一通知的实际重试额度 | 32 个并发隔离检查产生一条通知但 Attempts=9，超过 5 次；交回管理，私网拨号守卫在 TCP 前拒绝，无真实推送 |
| C21 | P2 | 取号尚未验证出口时，国家筛选把 API/静态未知观察候选全部提前删除 | 两个子场景先失败，允许未知候选进入 Judge+Geo 后均通过，最终仍严格验证出口国家 |
| C22 | P2 | 探测中缩短为仍可用的本地保留期，最终交付没有按当前期限收紧 | 新回归当前未通过，交回核心；不能夸大 lease_expires_at，也不应丢掉独立合格候选 |
| C23 | P1 | 静态缓存 TTL/数量/字节配置仅声明，运行时使用固定常量；当前 batch 复查未接交付 | 代码定位，供给与核心/管理协调修复，尚未最终验证 |
| C24 | P1 | 已有 version=7 数据库不执行新附加列和供给表的幂等安装 | 代码定位，交回核心；新库 fixture 通过不等于已有数据库升级成功 |
| C25 | P1 | 普通成功候选丢失 reservation_id，最终容量复查将自己的预约算作外部占位 | 60 项完整审计中大范围交付为 0；已精确定位并交回核心，不弱化交付断言 |
| C26 | P1 | 无候选幸存时仍将粘性 claim 更新为 ACTIVE，下一次提交才报告 claim 丢失 | 粘性初建的多个独立子场景返回 503；应与空交付一起保持未激活 |
| C27 | P1 | 旧 incarnation 探测的较晚完成时间覆盖新 incarnation 的当前 PASS | `TestAuditOldIncarnationEvidenceCannotEvictCurrentPass` 实测失败；存在性与配置/对象版本需要在写语句中复查 |

补充不同凭证但相同入口的连接去重回归：全链必须按含凭证的真实连接指纹，不按 host:port
合并。失败 API 候选不能结束供给搜索的回归不假定随机源排序：无论先选到哪个源，fixture
第一次返回坏候选，第二次才提供好候选，因而只靠继续搜索才能成功。新增绑定复用门槛包括
资源退休、来源熔断及新负面证据；旧 PASS 后如重新真正验证出更晚 PASS，允许合法恢复。

真实主程序的新库启动另复现：示例配置使用尚不存在的 data 子目录时，程序先打开 singleton
lock 才创建父目录，直接退出。已交回管理修复；后续须重新构建真实二进制，而不是靠手工
预建目录或 fixture 的手工 usage.EnsureSchema 来掩盖启动链。

随后真实二进制的隔离新库启动检查通过，测试目录为
`/tmp/proxy-pool-startup-audit.XGReet/fresh-run-8ac6lqsg`：程序自行创建 data/lock 父目录，
system 报告默认 30000ms、上限 120000ms；settings 不回读秘密、云/Bark 均未配置；usage
保持缺失且 stale；嵌入管理 HTML、假密码登录及 cookie 项目 GET 均成功。初次 harness
误传 password 字段，已按实际页面的 token 字段修正；未以此要求旧获取 API 兼容。
只运行自己启动并经 lsof 确認拥有端口的 PID 36567，SIGTERM 后 exit=0，无残留该进程；
SQLite integrity_check=ok，供给及 usage 表齐全。没有读取或覆盖真实用户配置、数据或凭证。

最新原语竞态复核再次通过：domain、validation、staticdl、scdn、sites 五个库。管理实现的
阶段单测、静态检查及 isolated usage 回归不能代替主代理的最终全仓验收。该阶段完整审计
仍被 C25/C26 及任务预算中日志错误转 503 的核心回归挡住；后续结果见第 6.4 节，不能
把这段中间态或后来一次包单测成功写成最终全部通过。

对当前 HTML 的 GET-only 前端 fixture 复核：390×844 项目详情 scrollWidth=390，已不撑宽
整页；登录遮罩正确隐藏；任务中心明确 default 主体及 Ready 语义；未报告前端 warn/error。
表单源码已按 provider/site/static 配置键嵌套。此次仍未执行真实业务修改、外部拉取或推送，
手机 viewport 已恢复默认，不将只读前端复核冒充完整实际渠道验收。

### 6.4 额度中断后的独立审计与续接

核心最后几次修改前，一次全仓 race 只剩自有 foundation 证据 fixture 失败；这不是最新
工作区的结果。证据参数修改中断后重新执行：

- `go test -count=1 ./...`：库包通过，集成失败，耗时 43.025 秒；日志为 `/tmp/proxy-pool-full-test-after-quota.log`。
- `go test -race -count=1 ./...`：库包通过，集成失败，耗时 42.401 秒；日志为 `/tmp/proxy-pool-full-race-after-quota.log`。
- 两次都复现证据 SQL `want 16 got 14`、sticky 第二次复用不足、并发首次 sticky 仅一方成功、预热 Ready=0；自有 `sticky_core_test.go` 的失败诊断还访问空 Items 而 panic，导致后续测试未全部执行。
- 该次 `go vet ./...`、`gofmt -l .`、`git diff --check` 通过；race 命令失败不能表述为 race 门禁通过。macOS 链接警告不等于 race finding。

用户授权恢复后，默认沙箱禁止 fixture 监听本地端口，API 包测试报告 `bind: operation not
permitted`。主代理按权限规则申请沙箱外重跑，而非修改安全策略或把它记为代码缺陷。
此后独立复核已通过：

- API、config、usage 三个包的非 race 测试。
- 用量/Bark 审计 race 子集，含并发重试额度和存活 claim 的两个回归；均无真实云查询/推送。
- domain、validation、staticdl、scdn、sites 五个库的 race 测试。

Bark 的最后修复已被独立验证：outbox claim 覆盖实际 `BarkSendBudget + 5s`，推送与退避
使用同一个总预算；不能按较短的云读取 timeout 提前释放一个仍在发送的通知 claim。

主代理先新增六个顶层测试，该阶段总数为 68；随后第 6.5 节增加一项提交期限审计。
下表均为部分修复落盘前的独立实测失败，不是只读源码推测；最新状态以第 6.5 节为准：

| 编号 | 级别 | 已复现问题 | 独立回归 | 收尾状态 |
| --- | --- | --- | --- | --- |
| C28 | P1 | 证据 INSERT/UPSERT 需要 16 个 SQL 参数，实际只有 14 个；多个证据、预热和复用场景无法完成 | 原有证据审计、foundation、预热测试 | 交给正式接手的核心实现代理 |
| C29 | P1 | sticky 将初建请求剩余等待预算变成租约 TTL；健康代理在 timeout=1 时被 min-usable 过滤 | `TestAuditStickyRequestBudgetDoesNotBecomeLeaseTTL` | 新失败已交回核心，不弱化默认可用期限 |
| C30 | P1 | 同项目同会话的 API 私有绑定首次成功，第二次返回零条；库存存在性和公共证据表要求挡住合法私有复用 | `TestAuditStickyPrivateAPIBindingReusesWithoutAnotherFetch` | 假供应商只被调用一次；测试仍要求不能公共入池，待核心修复 |
| C31 | P1 | sticky 复用忽略当前本地保留期：仍够用的 15 秒期限未收紧，已过期对象也继续交付 | `TestAuditStickyReuseRechecksCurrentRetention` | 两个子场景都失败，交回核心 |
| C32 | P1 | sticky 提交使用预检查的旧 Now，允许在绑定实际过期后继续插入租约 | `TestAuditStickyReuseUsesActualCommitTime` | 50ms 到期、80ms 后提交实测失败，交回核心 |
| C33 | P2 | 静态缓存失效只删 entry、不扣 bytes，频繁改配置累计虚假占用，驱逐下一批合法缓存 | `TestAuditStaticCacheInvalidationReleasesByteBudget` | 第 9 次配置更新后缓存消失；另定位 order 保留无效键的有界风险 |
| C34 | P1 | sticky 复用把真实 `v1-...` 库存 ResourceID 换成 CD 指纹；租约与响应互相一致但都不再指向原库存 | `TestAuditStickyReuseKeepsInventoryResourceIdentity` | 实测两种 ID 不同，影响健康/反馈定位，已交回核心 |

期限、时钟和库存身份三个隔离测试只把首次真实 probe 的同一份 PASS 在临时 fixture
里重存一次，以便独立于 C28 验证最终提交守卫；没有伪造新 PASS、改动生产写入方式或
绕过正式证据回归。API 私有绑定测试没有这样重存，必须真正满足不公共入池的边界。

上一轮临时 IAB 标签页已在结束时自动清理，本次库存中没有这两个标签页；未关闭用户的
Chrome 文档页，也未读取渠道控制台/账号页面。原临时 QA 服务器再次核对为自己创建的
fixture 后，经批准停止 PID 18714、18774、25074；复查这些进程及端口 51847/58868
均无存活监听。未停止用户服务或模型路由服务。

### 6.5 历史编译中间态与隔离诊断

本节是 HUB 成功接续前的历史失败记录；最新实际工作区结果见第 6.6、6.7 节。

接手核心的代理在再次额度错误前更新了 `evidence.go`、`delivery.go`、`sticky.go` 和
自有 foundation fixture。证据 SQL 参数已重新匹配，fixture 的旧证据 StartedAt 已与
CompletedAt 保持有效顺序；sticky 不再把请求剩余预算直接当作租约 TTL，并增加当前
保留期预检查、实际事务时钟和真实库存 ID 的使用。这些都是部分实现，未据此放行。

最后两轮实际工作区门禁：

| 命令 | 当前实际结果 |
| --- | --- |
| `go test -json -count=1 ./...` | 编译失败，`delivery.go:599:8: no new variables on left side of :=`；集成审计未执行 |
| `go test -race -json -count=1 ./...` | 同一编译失败；没有集成 race 通过结论 |
| `go vet ./...` | 同一重复声明错误，exit=1 |
| `gofmt -l .` / `git diff --check` | 通过 |

实际日志分别为 `/tmp/proxy-pool-full-test-resume-final.jsonl` 和
`/tmp/proxy-pool-full-race-resume-final.jsonl`。主代理没有替实现代理修生产文件：只在
`/tmp/proxy-pool-core-audit-overlay-lvtovfwh/delivery.go` 的副本中把这一处重复声明换为
赋值，用 Go overlay 隔离核查其余业务；原工作区仍保留该编译错误。

该诊断副本执行全部 69 项顶层独立审计，结果为 66 通过、3 失败，耗时 7.952 秒。
日志为 `/tmp/proxy-pool-core-audit-overlay-lvtovfwh/audits.jsonl`。这个结果不是工作区
测试通过，不是完整 `./...` 通过，也不是发布或 race 验收。

| 当前未收口问题 | 诊断结果 |
| --- | --- |
| C30 API 私有 sticky 复用 | 初建成功，第二次 HTTP 503 `resource no longer eligible`；读侧跳过库存不足以修复事务里无条件的库存/公共证据要求 |
| C33 静态缓存失效的字节预算 | 仍在第 9 次配置更新后丢掉可容纳的新缓存；不扣 bytes 的问题未修 |
| C35 sticky 提交遇到新保留期仍写过长租约 | `TestAuditStickyCommitCannotOutliveCurrentRetention` 新增并实测失败：当前保留期为 15 秒，事务仍保存原约 30 秒的 deadline |

C29 短等待预算、C31 顺序的当前保留期、C32 实际提交时钟、C34 真实库存 ID，以及
原有证据和其他独立审计在这个副本中通过。C35 刻意绕开已经通过的读侧预检查：当
保留期在准备后改变，事务不能只检查“尚未到期”，还必须约束最终租约期限，并使响应
与持久化的真实期限一致。证据首写的完整源版本/Inc 守卫仍需在正式可编译代码下复核。

较早成功构建的本轮二进制完成另一次隔离启动烟测，目录为
`/tmp/proxy-pool-startup-audit.XGReet/fresh-run-cinz9wsy`：父目录自行创建、默认
30000ms/上限 120000ms、脱敏且未配置的 settings、缺失且 stale 的 usage、假密码
登录及 cookie 项目只读 GET 均通过。自有 PID 49900 正常 SIGTERM 后 exit=0，SQLite
integrity_check=ok。此构建早于最后落盘的编译错误，不包装成当前最终源码已成功构建。

三个原模型代理当前均停于额度错误；没有继续重试、擅自切换模型、修改路由配置或接管
生产实现。主代理的审计、flow/correctness 测试已保存 SHA-256 清单并核对，未被实现
代理修改。当前仍是原 HEAD `52b69c2` 加未提交工作，没有 stage/commit/push/deploy。

### 6.6 HUB 实际实施后的工作区复核

HUB 三个实现代理实际执行后，生产编译错误已消失。核心的 `CommitStickyReuse` 返回
事务真正提交的期限；API/静态私有绑定不用伪造公共库存；实际库存 ID、当前源版本、项目
和密钥、对象 incarnation、熔断、证据及保留期均在最终事务复查。匹配国家的库存/API
会话复用、供应商到期安全余量和当前本地保留期已加入独立回归并通过。

供应侧修复缓存移除时的字节和顺序记录释放，失效操作不再清空尚有效的下载节流时间。
渠道配置版本使用持久的 per-ID watermark，删除再创建同一 source_id 不复用旧版本。
这个版本记录不是失效代理黑名单，也不保存坏连接详情。

以下结果均为实际工作区源码，未使用临时 overlay：

| 独立检查 | 实际结果与日志 |
| --- | --- |
| 全部 74 项顶层 `TestAudit` | 74 通过、0 失败，8.28 秒；`/tmp/proxy-pool-hub-audits-74-current.jsonl` |
| 直接 sticky 提交时钟/保留期/供应商到期 | 通过；fixture 携带实际 BindingCiphertext 与 DeliverySafety，且核对返回期限等于持久化期限，避免资格拒绝导致伪通过 |
| 临时三组创建后的 `go test -timeout 120s -count=1 ./...` | 全仓通过；`/tmp/proxy-pool-temp-final3-full-test.log`，集成 21.218 秒 |
| 同阶段 `go test -race -timeout 180s -count=1 ./...` | 全仓通过、没有 race finding；`/tmp/proxy-pool-temp-final3-full-race.log`，集成 16.573 秒 |
| `go vet ./...`、`gofmt -l .`、`git diff --check` | 通过；vet 日志 `/tmp/proxy-pool-temp-final3-vet.log` |

该阶段全仓命令早于下一节新增的三个审计，不能代替它们修复后的最终门禁。
macOS 链接器的 `malformed LC_DYSYMTAB` 是非致命警告，不当成竞态检测结论。

### 6.7 最新临时三组与新增审计

主代理继续按新需求检查真实生产路径。此处最初新增三个顶层回归，总数由 74 变为 77；
后续配置 TTL、首次绑定期限、渠道运行态及晚到刷新竞态的四项门槛，使最终总数达到 81。

| 编号 | 门槛或已复现问题 | 实际结果 |
| --- | --- | --- |
| C40 | 初次写入验活证据也必须复查当前渠道版本，不能只在 UPSERT 冲突时检查 | updated/disabled 两场景先复现接受旧 proof、写入 1 行；核心修复后，库存/无库存 ID 共四场景通过，最终全仓及 race 也通过。首轮失败日志 `/tmp/proxy-pool-temp-final3-evidence-first-insert.log` |
| C41 | 私有 sticky 绑定仍有效但 proof 过期时，应重新验活原连接；不能把证据过期当成坏代理并被自己的 CD 排除 | 先复现 500ms proof 过期后返回零条；核心修复后通过。真实新 probe、同连接、无新 vendor batch、不延长 CD/绑定 TTL、不进入公共库存均由断言核对。首轮失败日志 `/tmp/proxy-pool-temp-final3-sticky-revalidation.log` |
| C42 | 既有 v7 缺 watermark 时从当前版本补建，更新/删除/实际重开 DB 后版本仍单调且保留项目 CD | `TestAuditSourceVersionWatermarkSurvivesMigrationAndRestart` 通过，两次真实临时数据库关闭/重开；日志 `/tmp/proxy-pool-temp-final3-source-watermark.log` |
| C43 | `sticky_binding_ttl_ms` 必须控制会话期限，不能只是未被生产调用的配置声明 | 15s/90s 配置均先复现仍写入 30s 租约期限；核心分离 binding/lease/proof/provider 期限后，两场景通过。失败日志 `/tmp/proxy-pool-temp-final3-ttl-resource-less.log` |
| C44 | 首次粘性发放在 probe 中遇到供应商到期/本地保留期变化，绑定也必须按最终事务的当前期限收紧 | 代码审读定位准备的绑定期限未跟随候选收紧；`TestAuditStickyInitialBindingRespectsChangedOfferDeadline` 的供应商/保留期两门槛在最终源码通过 |
| C45 | 旧版本探测的 PASS/FAIL 不能写入更新或删除重建后的渠道熔断运行态 | 新 `RecordSourceOutcomeOwned` 在同事务检查版本并写入；所有 acquire/sticky/prewarm 生产调用携带原观察版本。受控旧 API PASS 与新 OPEN circuit 的独立门槛、供给自有 PASS/FAIL 回归通过 |
| C46 | 晚到 proof 刷新不能因为 CAS 失去竞争就清空另一个请求刚提交的健康私有绑定 | 新 `TestAuditStickyLateRefreshCannotReplaceNewerPrivateProof` 在完成 probe 后阻塞 Geo lookup，让较新请求先提交；两个请求均取得同连接，最新 proof 与 ACTIVE 绑定保留，vendor 只调用一次；最终普通/race 均通过 |

另定位预热生产路径仍调用 metadata 形式 `PutEvidence`；供给代理已经改为生产守卫并
增加旧源版本拒绝测试。attempt 的 source version attribution 是统计记录，不是资格
守卫，不能替代证据语句本身的原子检查。主代理仅增加审计测试和记录；没有接管生产
实现或删除业务断言来获取通过。

管理代理另修复并加入自有回归：Bark 测试请求在推送之前拒绝畸形、超限、未知字段、
顶层 null 与尾随 JSON；合法空请求/空对象仍按实际接口处理。通过真正 Handler 路由
核对无/错误身份 401、缺 CSRF 的 cookie 修改 403、合法未配置测试 502，以及秘密 URL
query/fragment 在配置和 API 两处拒绝且不回显秘密。窄屏登录 panel 改为 100% 宽、
360px 上限，脚本和样式断言通过；本轮视觉复核边界见第 6.8 节。

### 6.8 最终实际源码独立门禁

三个临时渠道实现者最终均报告完成并冻结；主代理随后重新执行，未使用 overlay、
未复用子代理的测试结论、未调用真实供应商或云/Bark。

| 独立命令或检查 | 最终实际结果 |
| --- | --- |
| `go test -json -timeout 120s -count=1 ./...` | 全仓通过；81 个顶层 `TestAudit` 全部 PASS、0 FAIL，集成 25.441s。日志 `/tmp/proxy-pool-temp-final3-final-tests.jsonl` |
| `go test -race -timeout 180s -count=1 ./...` | 全仓通过，没有 race finding，集成 27.714s。日志 `/tmp/proxy-pool-temp-final3-final-race.log` |
| `go vet ./...` | 通过；日志 `/tmp/proxy-pool-temp-final3-final-vet.log` |
| `gofmt -l .` / `git diff --check` | 无格式遗漏、无 whitespace 错误 |
| 14 个受保护审计/flow/correctness 文件 SHA-256 | 全部 OK；新增断言均由主代理更新对应清单项，没有整体重建清单来掩盖第三方变动 |
| 嵌入管理脚本语法 | 独立 `node --check` 通过；路由、认证、秘密清除/脱敏与 Bark body 测试包含在全仓门禁中 |

从最终源码构建 `audit-temp-final3-20261102 (52b69c2-dirty)`，使用全新临时目录
`/tmp/proxy-pool-startup-audit.XGReet/fresh-run-q2niforb` 及假配置完成真实主程序烟测：
程序自行创建数据库/锁父目录，默认 30000ms 与上限 120000ms 正确，settings 脱敏且
未配置，usage 缺失且 stale，嵌入 HTML、假密码登录、cookie 项目 GET 均通过。
经 lsof 确认自己 PID 15161 拥有端口 60266；SIGTERM 后 exit=0，SQLite integrity_check=ok，
供给/用量表存在。未操作真实用户配置、数据库或身份。

为对窄屏登录进行当前真实 HTML 的视觉复核，另外启动同一最终二进制的隔离 fixture：
PID 16728、端口 64128。浏览器工具拒绝 localhost origin 授权并明确禁止绕过，主代理
停止视觉操作，没有切换浏览器、代理 origin 或使用其他 UI 技术绕过；因此不宣称本轮
320px 视觉回归通过。只按既有静态/脚本与此前 390px 只读 UI 结果记录边界。核对 PID
仍是自己的假配置进程后发送 SIGTERM，退出 0；没有关闭用户浏览器或服务。

发布侧剩余覆盖包括目标服务器压力/资源饱和、满载关闭及真实收费/计费口径测试。
usage/maintenance 背景任务会收到取消，但未显式 join；此次空闲烟测不证明这些满载
边界。非致命 macOS `LC_DYSYMTAB` 链接警告没有被当作 race failure 或消除后再假报。

### 6.9 用户授权继续真实验收后的当前状态

2026-11-02 晚间，用户要求重试被拒权限并验收剩余项目。三个原临时模型子代理继续按
原分工进行有界真实渠道检查、本地合成负载及云/Bark 配置与官方口径核对；没有切换
模型、提交、部署或修改真实配置/数据库。

内置浏览器的同 origin 重试仍被 saved permission / managed policy 拒绝。隔离服务
实际于 20:49:55 启动、20:53:31 在拒绝后正常退出；随后为用户协助排查而在同一
`127.0.0.1:64128` 重启。主代理核对 PID 22521 的假配置命令和 LISTEN，HTTP 可达性
为 200。用户手动打开页面后，主代理读取该现有标签页仍被明确拒绝 observation
authority；没有新开 origin、换浏览器或用 CDP/其他渲染机制绕过。手动可见与 HTTP
服务可达不等于代理获准读取/输入；当前桌面/窄屏及 CRUD 浏览器验收均没有新通过
结论。完整记录见 `docs/acceptance-2026-11-02/browser.md`。

主代理新增 `TestAuditStickyProbeSaturationDoesNotDestroyPrivateBinding`，顶层独立
审计总数由 81 增至 82。仅该主代理所有的 sticky 审计条目更新 SHA-256，其余 13 个
受保护文件仍逐项 OK。该测试不只要求返回短缺，还要求原 ACTIVE 绑定、证据、渠道
健康及 CD 不变，解除饱和后能重新验活同一连接，且不再获取 vendor 批次、不公共入池。

| 编号 | 当前独立复现 | 收口状态 |
| --- | --- | --- |
| C47 | proof 已过期、probe 槽位占满且 probe 自身预算到期：原 ACTIVE v2 被转换为 EXPIRED v3，私有连接与身份字段清空 | 已交核心实现代理；不能把预算不足当作坏连接并 rebind |
| C48 | 同样的饱和刷新遇到 caller cancellation：返回 `ALLOCATION_STORE_UNAVAILABLE: context deadline exceeded`，不是合法且有解释的空结果 | 已交核心实现代理；需区分取消与真实 SQLite 故障 |

实际失败日志为 `/tmp/proxy-pool-audit-sticky-saturation-diagnostic.log`。最初常规命令
因主代理新 fixture 和核心新 `core_load_test.go` 同时误用不存在的 `GetSourceCircuit`
而未编译；主代理已将自有 fixture 改为原表只读全字段快照。随后诊断命令只排除尚在
编写且未编译的核心自有测试文件，使用真实生产源码、没有 production overlay，得到
上述两个业务失败。此诊断不冒充全仓门禁；待实现和自有 fixture 修复后必须重新执行
正常的全仓普通/race/vet。

管理子代理的只读脱敏配置 checkpoint 表明实际配置缺 Huawei AK/SK/region/resource/
package、加密主密钥及 Bark URL，既有实际数据库没有 `usage_*` 表。未在该库初始化
usage 或迁移，因此云/Bark 当前为 NOT_CONFIGURED，未调用真实云接口或推送；官方
Flexus L 包余量接口的结论仍待证据报告，不以 generic CES 估算冒充余额接口。
渠道及有慢请求的关闭/重启结果仍在生成，未把计划执行或子代理运行状态记作通过。

### 6.10 2026-11-03 新增需求后的独立状态核对

用户要求 23:00 后使用 zcode 的 GLM-5.3-Flash；主代理已停止三个临时执行者，分别以
`zcode/GLM-5.3-Flash` 接续核心、供给及管理分工。新增 R29-R32 也由它们实现，主代理
负责独立审计，不接管生产业务代码。持久交接快照为
`/Users/roiding/Documents/ChatGPT/proxy_pool-audit-backups/proxy-pool-glm-handoff-20261102-230750`。

三个实现者报告自己的范围已完成并冻结。当前落盘能力包括共用持久导入服务、文本与
JSON 批次、独立来源受限上传 Key、可选幂等键与批次查询、新标准 JSON 响应，以及
sticky 预算不足/取消与慢在途关闭的修复。实现者自测不替代主代理的全仓门禁。

主代理于 08:38-08:39 Asia/Shanghai 对真实工作区重新执行，未使用 overlay：

| 独立检查 | 实际结果 |
| --- | --- |
| `go test -json -timeout 150s -count=1 ./...` | 库包通过，集成包失败；84 个顶层独立审计中 81 通过、3 失败，另外 2 个非审计集成测试失败；集成 25.966s |
| C47/C48 预算与取消回归 | 两个子场景均通过；不再沿用上一节的历史失败状态 |
| 新标准 JSON 两项审计 | 通过，覆盖 reason 单字段、URI/原始认证、UTC、独立期限及 sticky 显式布尔值 |
| `go vet ./...` / `go build ./...` | 通过 |
| `gofmt -l .` | `internal/application/imports.go`、`internal/storage/sqlite/imports_test.go` 仍有格式遗漏 |
| `git diff --check` / 15 个受保护文件 hash | 通过；旧 14 项未被实现者改动，主代理仅追加新的 wire 审计条目 |

完整测试日志为 `/tmp/proxy-pool-glm-20261103-root-full-tests.jsonl`，vet 日志为
`/tmp/proxy-pool-glm-20261103-root-vet.log`。该轮尚未进行最终完整 race 复跑，旧阶段的
race 通过不能代替新增导入功能和最新生产代码的最终门禁。

失败项为 `TestAuditStickyReuseNeverServesSupersededProof`、
`TestAuditStickyReusePersistsCorrectLeaseIdentity`、
`TestAuditStickyReuseKeepsInventoryResourceIdentity`、`TestAPIEndToEnd` 和
`TestSiteFetchChannelViaAPI`。前四项读取了新输出不再提供的旧内部身份/重复观察字段，
需要按新契约使用持久租约/证据与标准 observed 字段重新保持同等业务断言，不能据此
直接恢复旧 API 兼容层，也不能删掉证据新旧、实际库存身份与 lease 一致性的门槛。
最后一项涉及显式 fetch 与新鲜缓存返回的限流语义，已交供给代理核对真实调用次数。
这些未收口前不宣称全仓通过；新导入的正文、授权和原子事务边界还需主代理补独立审计。

实网供给报告已经落盘：11 个目录渠道加 SCDN 共 12 个渠道，实际 29 次渠道请求及
32 次目标 TLS 探测，0 次 PASS；包括已披露的额外整轮检查和第二次 SCDN HTTP 调用。
这是失败的真实验收，不是因为解析出地址就通过。后续实现者没有再发实网请求。
官方 Huawei Reader 仍不可用、真实云/Bark 配置缺失；内置浏览器在重启和用户修改默认
允许后仍被保存的 origin 权限拒绝。各自的独立报告位于 `docs/acceptance-2026-11-02/`。

主代理已重新唤起三个相同 zcode 模型执行者核对剩余问题。代码仍为 HEAD `52b69c2`
加未提交工作区，staged 路径数为 0，没有 commit、push、deploy 或真实配置/数据库修改。

### 6.11 虾蹬接续、新静态文件与导入独立审计

2026-11-03，用户要求改回虾蹬渠道并继续，三个实现子代理以
`kunyou/deepseek-v4.1-flash-1017` 接续原核心、供给和管理分工；主代理继续负责独立
审计、回归与记录，不接管生产实现。最新输入为用户提供的只读文件
`/Users/roiding/Downloads/11.02-7PM-1.txt`：78,278 字节、1,400 条非空且无完全重复的
含认证 HTTP URI 记录，UTF-8 可解码，SHA-256 为
`1c315c5240fc0c248bdd3de61a686b87d9bb95d1f75a09627aea3ccb484b9237`。
文件名及修改时间不作为供应商到期时间；不复制原始文件、URI 或认证到仓库/报告。
供给实现者独占这份新输入的生产解析、临时库导入及 12 条分散样本的有界实网探测；
尚未收到实际报告前，不把解析、旧文件或计划抽检记为新文件验活通过。

主代理合法对齐四个旧字段 fixture：HTTP 层读取标准 `observed.exit_ip`，内部身份与
PASS/证据新旧则查询真实已提交 lease/binding，仍核对 connection fingerprint、
resource incarnation、request/tenant/source 与 wire/proof 时间一致。没有恢复旧
`resource_id`、`validation.outcome` 或重复观察字段的公共兼容别名。

新增 `audit_imports_test.go` 的 9 项顶层门槛后，独立审计总数为 93，受保护清单为
16 项。只修改主代理所有的 sticky/flow fixture 和新 imports 清单项；其他受保护
文件逐项 hash 不变。初次执行被遗留 `zz_debug_test.go` 的虚构
`sites.ParserLines` 阻塞，未将编译失败当作业务测试结果；随后正常生产源码下的
targeted run 可执行，未使用 overlay。

`/tmp/proxy-pool-root-acceptance-20261103/imports-canonical-first.jsonl` 记录 10:15
Asia/Shanghai 的实际首轮：13 项顶层测试中 8 PASS、5 FAIL；四项 canonical fixture
全部 PASS，9 项 imports 中 4 PASS、5 FAIL。跨文件/JSON 去重且保持健康/CD、上传
身份与来源隔离、库存/结果/幂等同事务回滚、并发同批一次应用均通过。

| 编号 | 新独立复现 | 当前收口状态 |
| --- | --- | --- |
| C49 | source read 之后更新配置或删除重建，旧解析快照仍 201 写入库存/批次；disable 场景被现有守卫拒绝 | 已交供给实现者；提交事务须检查观察版本及 delete/recreate ABA |
| C50 | API 正文后 key recheck 与 service source read 之后撤销 Key，新批仍 201、幂等 replay 仍 200 | 已交管理/供给实现者；须将可信上传 Key 身份传入同一最终事务，不由任意 uploader 文本推断权限 |
| C51 | 畸形 MIME 降为 text 并生成批次；JSON unknown-field 及 URI `%ZZ` 错误回显/持久保存秘密；text 的 257 字节密码绕过 JSON 的字段上限 | 已交供给实现者；统一格式边界与无秘密的有界诊断，保留合法条目逐项导入语义 |
| C52 | 已提交结果 `errors_json` 损坏时，重放错误被当作窗口失效，201 创建新批并替换原幂等 marker | 已交供给实现者；真实结果读取错误应 5xx 且回滚，不能据此重新应用已提交批次 |

主代理另从当前真实 main 构建审计二进制，以新的临时 DB/锁父目录、本地签名 HTTP
Judge 和慢代理闸门独立验证在途关闭。fixture 明确只允许本地目标，不代表 TLS 或
目标服务器负载验收。真实 120s acquisition 已到代理且持久状态为 CLAIMING 后
SIGTERM，程序 7.972ms exit=0，HTTP 200/空结果/预算不足原因，claim 为 EXPIRED、
无 ACTIVE lease、无虚构 CD；两次实际重启后可恢复交付并刷新过期 proof，保持同一
connection/binding 身份、版本/期限及原 CD，普通 `cd=0` 仍无交付。三次关闭均 exit=0，
最终 integrity_check=ok、无 CLAIMING 残留。脚本与证据位于
`/tmp/proxy-pool-root-acceptance-20261103/main_shutdown.py` 和
`/private/tmp/proxy-pool-root-acceptance-20261103/main-slow-5xrr7lky/result.json`。
这是当前阶段的独立诊断，仍须在实现者最终冻结后重建/复跑。

新增需求后的最终全仓 normal/race/vet 尚未收口；不沿用第 6.8 节历史绿灯。云/Bark
仍无真实配置、官方 Reader 不可用、浏览器保存的 origin 策略仍阻塞自动视觉/CRUD，
旧目录/SCDN 的实网失败也未被新文件结果替代。没有新增这些真实服务调用、修改真实
配置/DB、提交或部署。

### 6.12 临时渠道接续与新文件独立验收

2026-11-03，虾蹬实现任务实际遇到 API key disabled、401 及上游 120 秒读取超时。
用户明确授权失败后改用临时 DeepSeek 4.1 Flash，管理、供给和核心三个实现分工以
`oai-freestyle-cc/deepseek-v4.1-flash` 接续；管理已完成冻结，供给生产实现已冻结，
核心继续小规模本地合成负载。没有为模型切换修改用户的路由配置、凭证或 Git 状态。

新需求的此前全仓独立 normal 基线实际为 93 项 `TestAudit` 中 91 PASS / 2 FAIL，
集成包共三项顶层失败：错误脱敏、在途撤销上传 Key、站点缓存 fixture。
该证据保留于 `/tmp/proxy-pool-root-acceptance-20261103/current-full-tests.jsonl`；
此前 vet 已通过，不能将供给实现者自报的绿灯代替主代理最终复跑。

主代理从当前生产源码、不使用 overlay 执行
`go test -json -timeout 90s -count=1 ./tests/integration -run '^(TestAuditImport|TestSiteFetchChannelViaAPI)'`，
10 项顶层均 PASS，集成 7.479s，日志为
`/tmp/proxy-pool-root-acceptance-20261103/imports-site-final.jsonl`。
16 项受保护审计/flow/correctness hash 逐项 OK，没有重建清单掩盖实现者修改。

| 历史编号 | 当前独立核对 |
| --- | --- |
| C49 | 提交前检查 source enabled/type/观察版本；更新、禁用、删除重建 × 新批/重放均拒绝，库存与 marker 不变 |
| C50 | 两个入口传入已认证的 `UploadKeyID`；最终事务先检查 Key enabled/source scope，再查幂等；禁用、删除、解绑 × 新批/重放均拒绝 |
| C51 | URI 与 JSON 共用 host/credential 边界；非法 MIME、JSON unknown-field、畸形 URI及无 `@`/无 `://` 的裸非法 userinfo 不回显或持久保存认证；当前独立测试通过 |
| C52 | 已提交结果损坏返回存储错误，不重新应用、替换 import_id 或 marker；同事务回滚与八并发一次应用均通过 |

主代理独立使用用户的新文件执行生产 parser 和临时库 importer：1,400 条解析/导入，
0 非法，JSON raw-auth 跨入口再导入为 1,400 条重复，同幂等批次重放不改变结果。
1,400 条供应商 expiry 均为 unknown；evidence/observation/health/lease/CD 均为 0，
integrity_check=ok。含真实代理凭证的临时数据库已关闭删除，未写实际数据库。
脱敏离线报告为
`/Users/roiding/Documents/ChatGPT/proxy_pool/docs/acceptance-2026-11-03/static-file-offline-root.json`。

供给实现者仅执行一次获授权的新文件实网探测，实际为文件前 12 条（零基索引 0–11），
12 FAIL / 0 PASS，全部 AUTH，延迟 333–577ms。生产 HTTP CONNECT 验证器的 AUTH
对应代理 HTTP 407；该结果未完成目标 TLS，不能宣称证书验证已成功、文件全部过期，
或剩余 1,388 条已验证。没有重试、扩扫或发送目标业务 HTTP。

主代理检查实际执行脚本发现 `distributedIndex` 错用 1399 作为分母，实际索引并非
计划中的 `round(i*1399/11)`。已要求保留真实执行证据、纠正报告为前 12 条抽检，并
将“要求证书校验”与“已完成握手”明确分开；不得为修正叙述再做实网调用。
所有三个当前 static-file 报告已用源文件的真实 URI/host/认证值在内存中扫描，
未发现这些私密值，源文件字节数与 SHA-256 均保持不变。

当前源码重建为 `audit-temp-imports-20261103 (52b69c2-dirty)` 后，主代理复跑真实
main 取号关闭/重启矩阵通过：在途 120s sticky 请求已到本地慢代理，SIGTERM
8.823ms exit=0，claim 释放为 EXPIRED、HTTP 200/零交付且解释预算不足；两次重启后
同一绑定可刷新过期 proof，版本/TTL/CD 保留，普通 `cd=0` 不绕过旧 CD。系统默认、
嵌入 HTML、settings 脱敏、假密码登录/cookie GET 与最终 SQLite integrity 均通过。
证据为 `/tmp/proxy-pool-root-acceptance-20261103/final-main-shutdown.json`。

但计划任务关闭不是稳定绿灯：同一当前二进制的真实 nightly 已到本地慢代理且
status=running 后 SIGTERM，9.865ms exit=0，integrity=ok，却没有任务完成日志，
独立脚本明确 FAIL（`nightly-slow-hbpbj75x`）。C54 已从原代码阅读线索升级为独立
动态复现：`main` 的 background WaitGroup 不含 `nightly.Run`，而
`nightly.Shutdown` 只等待手动异步任务，不等待调度的同步 `RunOnce`。不能把进程
退出快或上次偶然出现完成日志当作已 join 所有后台任务。
证据保留于 `/tmp/proxy-pool-root-acceptance-20261103/final-nightly-shutdown.json`，
已交管理实现者在原共享 15s 关闭预算内修复，不改主代理验收断言。

管理实现者已将 `nightly.Run` 纳入真实 background WaitGroup，并抽出共享 deadline
内的 `joinBackground`；其自身 main 普通/race 及等待/超时测试通过，已冻结。
主代理尚待从核心最终冻结后的源码重建并复跑真实计划任务，不能先将 C54 标为
独立动态复验通过。

主代理另独立执行核心新负载测试，确实复现两项库存预约错误，而不是采用代理自报：
`final-core-load-root.jsonl` 中合成 metrics FAIL，三个原有粘性饱和/取消场景 PASS。
五个合成场景均有界；共享项目 CD 场景交付三个不同连接、无预约残留，部分结果
场景交付一个且释放后无残留，但单连接 `cd=0/max_pool_leases=2` 六并发只交付一个，
残留一个 ACTIVE reservation；caller cancel 同样在返回后残留一个预约。

| 编号 | 新独立复现与修复边界 |
| --- | --- |
| C55 | caller cancel 后 `processCandidate` 使用已取消的 overallCtx 释放预约，或 recordAttempt 取消分支提前返回；须有界同步清理仅自己实际拥有的预约，不影响其他 owner/lease/CD |
| C56 | 共享证据表被后一次新鲜健康 PASS 更新后，最终交付因 completed_at 不严格相等将前一次有效 PASS 筛掉；须保留 source/incarnation/scope/proof-age/future-time/failure-epoch 守卫，区别健康更新与负面/畸形证据 |
| C57 | 快速容量拒绝在 14–17ms 返回，却报告 5000ms 请求预算耗尽；须区别无资格/槽位不足与实际超时、调用方取消，不为纠正引入旧兼容层 |

主代理新增两个独立顶层预约审计后，共 95 项 `TestAudit`，只更新
`audit_allocation_test.go` 对应 hash，其他 15 项逐项不变。其首次执行
`owned-reservations-first.jsonl` 为两项新审计 FAIL、旧跨请求 CD 清理保护 PASS：
取消确实已进入 probe 并持有预约，返回后自身预约残留、另一真实 owner 的窗口未
改变；两槽闸门确实使两请求进入 probe，最终只交一个并残留一个预约。新增的快速
容量缺口断言要求不能伪报预算耗尽。已授权核心实现者在自己范围修复，不修改或
弱化主代理断言；95 项最终门禁尚未执行。

实网报告勘误已独立复核：实际 stdout 与 JSON 的十二项 index/transport/class/耗时
完全一致，全部 AUTH、0 完成 TLS；原执行脚本字节/SHA-256 不变，以 `.go.txt`
归档避免被正常 Go package 扫描。未来算法仅做离线算术检查，没有执行或追加
实网调用。离线报告的库存数字不变，原始脱敏版本另留于临时目录，当前版本只将
过时的 PENDING live 上下文更新为对这份十二条失败报告的引用。

核心负载与供给报告纠正任务随后均遇到临时模型网关 502，没有丢弃部分改动或擅自
换渠道。已按用户的继续指示用同一临时模型重新唤起核心与管理分工，等待实际收口，
不将代理启动等同于修复或验收通过。

最终全仓 normal/race/vet、合成负载与当前二进制复验尚待冻结后执行；云/Bark、内置
浏览器 origin 策略及目标服务器性能边界未因终端完全权限而自动完成。旧目录/SCDN
的 0 PASS 实网报告独立保留，不由新文件结果替代。

### 6.13 预约、预算归因与关闭收口复验

2026-11-03，临时核心执行者在部分修复后遇到 401 Invalid token，原任务已冻结，
部分代码没有丢弃。按用户允许重拉子代理的指示，以同一个临时 DeepSeek 4.1 Flash
模型重新拉起核心收口任务；未更改模型路由、真实凭证、配置或数据库。

主代理先收集此前真实二进制的两个在途关闭复验，均 PASS：
`closeout-main-shutdown.json` 为 SIGTERM 后 15.988ms exit=0，原 CLAIMING 释放为
EXPIRED，无虚构 lease/CD；两次重启后恢复私有绑定并刷新 proof，保留版本/期限/CD，
系统与假密码/cookie 烟测通过。`closeout-nightly-shutdown.json` 中真实每日定时器
触发到本地慢代理后 SIGTERM，8.794ms exit=0，任务结束日志在退出前出现，
integrity_check=ok、非本地请求数为 0。这两个结果仍是最终源码冻结前的诊断，
之后必须重建并复跑，不复用旧二进制代替最终门禁。

主代理从当前生产源码执行四项 targeted root audits，全部 PASS，包括 C55 的取消
后 owner-and-ID 清理、C56 的同连接两槽/CD=0 并发交付、旧跨请求 CD 清理保护，
以及 C57 的无资格库存缺口与真实工作 deadline 原因区分。C57 新增为一个包含两项
子场景的顶层审计，此时顶层审计为 96 项。日志为
`/tmp/proxy-pool-root-acceptance-20261103/closeout-new-core-audits.jsonl`。
此前候选全仓执行只有 95/96 项 root audits PASS，且执行中 acquire.go 曾变化，
另有核心自己编写的证据守卫 fixture 写入 ErrStale；该中间失败证据保留，不能
据此放宽生产 evidence 的 source/version/incarnation/时间/负面结果守卫。

进一步独立动态复现 C58：`TestAuditCancellationDuringCandidateSQLIsNotStorageOutage`
使用临时库的成功返回 authorizer/UDF 闸门，让调用方恰好在 SELECT 或 reservation
INSERT 内取消；不制造 SQL 拒绝、不调用实网。两个子场景均返回伪存储故障 503：
SELECT 为 context canceled，INSERT 为 context 取消触发回滚竞态的 sql.ErrTxDone。
库存/lease/CD 没有污染，但取消原因、missing 数量与业务结果被丢弃。首轮日志为
`/tmp/proxy-pool-root-acceptance-20261103/candidate-sql-cancellation-first.jsonl`。

| 编号 | 新独立复现与修复边界 |
| --- | --- |
| C58 | 在候选 SQL 内取消被当作存储 outage；只归类确由当前请求取消/deadline 导致的 context 错误与 rollback 竞态，真实 SELECT/reservation/commit 错误仍必须 5xx |

已交核心实现者修复，不由主代理接管生产代码。新增 C58 后共 97 项顶层 root audits；
仅更新 `audit_allocation_test.go` 对应的受保护 hash，其他 15 项逐项不变。
截至此阶段，仍等待实现者最终冻结、全仓 normal/race/vet 与最终重建二进制复验，
不宣布当前全部完成。没有新增实网探测、云/Bark 调用或真实配置/DB 修改。

### 6.14 当前冻结源码的最终独立门禁

2026-11-03，本轮核心、供给和管理实现分工已完成并冻结；临时核心子代理确认没有
待落地的生产代码或测试修改。主代理只补独立审计、测量和报告，不接管生产实现。
第 6.10–6.13 节的失败与待验声明保留为当时的真实中间记录；当前状态以本节为准。

C58 已修复：只把当前请求确已取消/deadline 结束、且底层错误为 context 错误或
回滚竞态 sql.ErrTxDone 的候选 SQL 失败归为请求预算不足。独立测试的 SELECT 和
reservation INSERT 两项取消场景均 PASS；真实 SELECT/reservation/lease/CD
触发器拒绝仍 5xx，没有把任意存储故障吞成库存缺口。C55–C58 与真实存储拒绝的
相关独立审计还连续五轮 race 通过，日志为 `closeout-c55-c58-repeat-race.log`。

| 当前门禁 | 主代理实际结果 |
| --- | --- |
| 全仓 `go test -json -timeout 120s -count=1 ./...` | PASS；97/97 顶层 root audits，通过的集成包耗时 20.976s |
| 全仓 `go test -race -json -timeout 180s -count=1 ./...` | PASS；97/97 顶层 root audits，集成 23.067s，无竞态报告 |
| `go vet ./...`、`gofmt -l .`、`git diff --check` | 均 PASS |
| 嵌入 admin JS 的 `node --check` | PASS；仅语法门禁，不冒充浏览器视觉/CRUD 验收 |
| 16 项受保护审计/flow/correctness hash | 全部逐项 OK，没有重建清单掩盖实现者修改 |
| 128 项 Go/HTML/module 源码及测试 hash | 最终全仓门禁前后完全相同；HEAD `52b69c2`、staged 路径数 0 |

最终测试日志为 `/tmp/proxy-pool-root-acceptance-20261103/final-full-tests.jsonl`、
`final-full-race.jsonl`，完整主代理门禁汇总为 `final-gates-summary.json`。
macOS 链接器的 LC_DYSYMTAB 警告不致使测试失败；不把它当作竞态发现。
128 文件 map 的规范 JSON SHA-256 为
`2f65b92b6ddbeaacdd5b648cff04b725f8aa4cc8637f1b9f510d68bb5e1ff893`；
持久清单为 `docs/acceptance-2026-11-03/source-root-final.sha256`。

| 历史编号 | 当前收口 |
| --- | --- |
| C54 | 真实每日定时器到达本地慢代理后取消，计划任务已在进程退出前结束；共享关闭预算内 join 已独立复验 |
| C55 | 取消和未交付候选使用独立短预算同步释放仅自己的 owner/ID 预约；另一真实 owner 的状态与期限不变 |
| C56 | 同一新鲜 scope 的更新健康 PASS 不再误伤旧但仍合格的 PASS；两槽/CD=0 六并发交付两次且无预约残留；负面、畸形、未来时间及身份守卫仍在 |
| C57 | 已知库存/容量不足及时返回 `NO_ELIGIBLE_CANDIDATES`；真实工作 deadline/调用方取消返回 `VALIDATION_BUDGET_EXHAUSTED`，不合成代理死亡 |
| C58 | 候选 SQL 内取消不再误报存储 outage；真实存储错误仍 fail-closed 为 5xx |

核心合成报告已审核：原主代理 normal 测量 JSON 逐字保留，五个场景共 15 个
service-level 调用、最多六并发，HTTP status 为 N/A。P95/P99 按 nearest-rank
取样，n=1 为退化值；goroutine high 只是 1ms 采样，不是所有瞬间最大值。
500ms 请求实际保留 125ms finalization slice，工作窗口为 375ms，报告中的错误
100ms 说明已经纠正。项目 CD 在释放 lease 后仍保留，只在确定性的未来维护时间
清理过期窗口，不假称真的等待了 11 分钟。

普通/race 测试现在默认只写测试日志，不再自动重写仓库验收 JSON；显式
`CORE_LOAD_REPORT_PATH` 才生成测量报告。主代理还独立执行当前冻结 instrumentation
的一次 opt-in 测量，五个场景均 PASS，返回后 ACTIVE reservation 均为 0，清理后
lease/reservation 均为 0，且 128 文件 hash 不变。主代理最新实际测量 P95 为：
共享项目 CD 29.740ms、两槽容量 19.380ms、部分结果 10.441ms、工作 deadline
379.497ms、调用方取消 3.011ms。实际 runtime 为 Go 1.25.1 / darwin / amd64、
GOMAXPROCS=4；该数据不代表华为云服务器或 5000 只股票真实业务性能。
独立含 provenance 的报告为 `docs/acceptance-2026-11-03/core-load-root-final.json`；
实现者原 normal 观测及说明保留于 `core-load.json` 和 `core-load.md`，不相互覆盖。

主代理从同一冻结生产源码重新构建实际 main 为
`audit-refactor-final-20261103 (52b69c2-dirty)`，再独立执行完整关闭/重启矩阵：

- 在途真实 120s sticky 请求已经进入本地慢代理后 SIGTERM，8.160ms exit=0，
  HTTP 200/零交付/预算不足，CLAIMING 释放为 EXPIRED，无虚构 lease/CD。
- 两次实际重启后恢复原会话并刷新过期 proof，保留连接/版本/绑定期限与原 CD；
  普通 `cd=0` 仍不能绕过原窗口。最后两次退出为 7.696ms 和 3.739ms。
- 真实每日定时器在 14:15 进入本地慢代理且 running 后 SIGTERM，4.003ms exit=0；
  计划任务完成日志在退出前出现，不再只以进程退出快当作后台任务已 join。
- 两个临时库 integrity_check 均 ok；系统默认、嵌入 HTML、脱敏设置、假密码登录
  和 cookie GET 烟测均 PASS。非本地目标调用为 0，所有自己启动的 fixture 已退出，
  两个监听端口已独立确认关闭，没有关闭用户浏览器标签或用户服务。

这仍是本地签名 HTTP Judge 的真实进程验收，不是目标 TLS 或实网业务成功证明。
持久证据为 `docs/acceptance-2026-11-03/main-shutdown-root-final.json`、
`nightly-shutdown-root-final.json` 和 `root-final-summary.json`。

新静态文件继续使用用户提供的 1,400 条只读输入。输入字节/SHA 未变化，生产 parser
及临时 importer 的离线结果已通过，真实凭证临时库已删除。唯一实网抽检仍是前
12 条：0 PASS/12 AUTH、HTTP 407、0 TLS 握手完成；未测试剩余 1,388 条，不能断言
整个文件已过期。当前收口没有重试、扩扫、云读取或 Bark 真推送。验收文本继续
使用真实 URI/host/credential 值在内存中做脱敏扫描，没有复制原始输入或这些秘密。

代码保持未提交工作区；没有 stage/commit/push/deploy，没有修改实际配置、密码、
Token 或数据库。未提交源码、未跟踪实现文件、受保护清单及独立日志另存为最终
本地快照；不把忽略的实际 config/DB 或 Downloads 原始代理文件装入快照。

### 6.15 发布授权与推送前独立复核

2026-11-04，用户明确授权推送本次重构、核验 GitHub Actions，并在完成后部署到
华为云 Flexus L。此前“不提交、不推送、不部署”的记录描述当时的授权边界；
本次授权并不允许覆盖现有密码、身份/加密密钥、代理凭据、数据库或项目 CD。

主代理实际确认 `origin` 为公开仓库 `roiding/proxy-pool`，默认分支为 `main`；
远端在推送前仍为 `52b69c2`，没有为此次工作新建仓库。已有 `docker` Actions
负责主分支镜像构建；本轮由临时 dsf 子代理补充共享测试门禁、静态 Linux amd64
原生包、版本/修订校验、SHA256 清单和最小 systemd 示例。主代理独立审核发布
权限、链接参数、备份/回滚及 Judge 入口约束，不把子代理自报当成部署验收。

| 本轮推送前门禁 | 主代理结果 |
| --- | --- |
| 全仓 normal / race 非缓存测试 | 均 PASS；各 97/97 顶层 root audits，无竞态报告 |
| vet、格式、whitespace、嵌入 JS 语法 | 均 PASS |
| 128 项 Go/HTML/module 源码及测试 | 本轮测试前后完全相同，继续匹配第 6.14 节清单 |
| 16 项受保护审计/flow/correctness hash | 全部逐项匹配，未重建清单 |
| GitHub Actions 静态检查 | 主代理使用校验过发行包 checksum 的 actionlint，候选 workflow PASS |
| 公开仓库推送安全 | 真实配置秘密及 1,400 条原始 URI 的内存精确扫描未发现泄露；提交前还需复核 staged blobs |

本轮完整本地门禁日志和仅本地发布准备快照保存在
`/tmp/proxy-pool-release-1104/`；不包含忽略的真实配置、数据库或 Downloads 原始
代理文件。128 项清单仅覆盖指定源码及测试，不是所有文档、工作流和 Git 历史的
全仓无秘密证明。Linux 静态包必须等实际 Actions 构建、运行版本校验和下载后的
SHA256/ELF 检查通过，不能用 macOS 测试替代。

部署前还缺目标实例的 SSH 地址/用户/端口/认证路径及现有服务拓扑。本机 SSH
配置未识别出 Flexus L 别名；不会猜测或部署到其他云服务器。实际本地配置的
Judge 地址仍为 loopback，不能把它直接当作远端代理可达的公网 Judge 地址。
部署时必须确认候选可达的 Judge、受保护的管理入口、原有配置/密钥、完整的
SQLite/WAL 备份和启动后的数据/CD 保留，再开放下游流量。

本节记录的是推送前状态，尚不声称提交、Actions 执行或服务器部署已经成功。
旧渠道、新文件的实网失败与 Huawei 官方用量 Reader 的未验状态保持不变；
没有为本次发布准备新增供应商探测、付费、云用量调用或 Bark 真推送。

### 6.16 首次推送与实际 Linux 发布构建失败

主代理已提交并推送 `3d7789adf7912b4bf93f05a3b0f8a934104ca5e2` 到 `main`，
远端 SHA 已逐项核对。提交前对 164 个 index blobs 重新扫描：141 个本次改动文件
没有真实配置、DB/WAL/锁、私钥、macOS AppleDouble 文件或已知代理秘密；128 项
源码及 16 项受保护测试在 index 中仍逐项匹配原清单。没有 force push 或 amend。

实际 Actions run `41794100287` 的 Linux 格式、vet、全仓 normal 和 race 测试均
PASS，但镜像和原生包构建均失败。此时没有发布新的镜像或原生包，更没有部署：

| 编号 | 真实失败与处理 |
| --- | --- |
| C59 | Dockerfile 中 `-extldflags='-static -no-pie'` 的引号从 token 中部开始，Go 的 quoted.Split 不把它视为整体，linker 实际收到 `'-static` 并报 `unterminated ' string` |

发布子代理已仅将 Dockerfile 参数改为独立的 `-extldflags '-static -no-pie'`。
主代理独立重现错误 value 的非零退出，以及完整 value 的 `go tool link -V` 成功；
这只证明参数解析，不替代 Linux 静态链接。静态 ELF、版本/修订和 checksum 门禁
没有放宽，生产 Go 源码和受保护测试没有变化。修正必须再次提交并通过实际 Actions
构建后，才能使用该次不可变运行的产物，不得拿旧 `latest` 镜像冒充新版本。

首次失败的完整脱敏日志和运行状态保存在
`/tmp/proxy-pool-release-1104/actions-failed.log` 和
`actions-first-failure-3d7789a.json`。运行链接为
<https://github.com/roiding/proxy-pool/actions/runs/41794100287>。
本节记录时，修正后的第二次 CI 尚未执行；目标 Flexus L 的 SSH 连接信息仍未提供。

## 7. 最终验收

本轮代码实现与本地独立验收已经收口：当前冻结源码的 97 项 root audits、全仓
normal/race/vet、格式/JS/保护清单、合成负载与真实 main 关闭/重启均已通过。
不是沿用旧的 81 项绿灯，也没有使用 overlay 或实现者自报替代主代理复跑。
最新本地结论与证据见第 6.14 节，发布授权及本轮复核见第 6.15 节，实际 Linux
发布构建的失败与修正见第 6.16 节。

完整发布/实网验收仍未通过：新文件前 12 条实测均 407，旧 11 个目录渠道加 SCDN
的实网报告仍为 0 PASS；官方 Huawei package Reader 未验证且不可用，实际云/Bark
配置缺失；目标服务器/5000-stock 负载与当前浏览器视觉验收未完成。配置预留、
解析/导入成功、局部 TLS fixture 或本地六并发通过都不替代这些外部门槛。
没有为消除这些边界擅自扩扫、付费、变更真实凭证或部署。
