> 2026-11-05 最新用户决策已覆盖下文历史的“provider 不参与预热”限制：所有生效网络渠道统一缓存和自动补足。当前落地见 `unified-channels-and-console-2026-11-05.md`。

# 免费代理渠道适配与预存储方案

日期：2026-11-04，Asia/Shanghai。代码核对基线：`b248bb3`。

状态：方案基线 + 开发落地记录。第 1–11 节保留开发前核对/设计口径；第 12 节记录本批次实现与未开放能力；第 13 节及 2026-11-05 价值评估更新当前渠道口径。本文覆盖前两轮答复中 **26 个站点/仓库入口**及代码目录另有的来源；不代表账号已开通或渠道已通过生产验收。参数是试运行建议，不是供应商承诺。

结论：将可用来源纳入统一渠道管理。公开列表建议预抓取到有界内存缓存，并支持按项目、小批量预热；按需分配的短效 API 候选继续只存在于当前请求。首期不为公开列表新增持久代理库存，也不启用磁盘候选缓存。上传/外部推送仍进入既有持久库存。渠道配置、额度、调度状态、发放/CD 等必要元数据应持久化。

本提案遵循[需求基线](project-repositioning-plan-2026-11-01.md)第 4.2、7.4、10、12 节及[来源契约](source-contract.md)。其中“静态列表可缓存”按供给语义判断：JSON 或带有 `/api/` 的 URL 不自动成为按需取号渠道。

## 1. 先校正渠道与实测口径

一个网站品牌是一个渠道；HTTP / SOCKS5、匿名/地区筛选、子站页面是该渠道的配置，不计成多个渠道。不同账号/项目仍需隔离的部署实例，多个品牌可以共用一个解析器。需要做到“每个入口都有明确适配方案和验收结果”，而不是为每个域名复制一套获取、验证、发放流程。

前两轮答复的结果只能作为来源线索，以下表述不能沿用为项目结论：

| 原判断 | 本方案使用的证据口径 |
| --- | --- |
| TCP 抽检 10/10，因此 HTTP 高可用 | TCP 开端口不等于代理协议、Judge 或目标 TLS 通过。之前 monosans 的 3 个实际 HTTP 样本只有 1 个成功；不能推出整体高可用率。 |
| 502 / 403 / 超时证明平台永久关闭 | 只证明当时测试路径失败。记录 `unavailable` / `access_blocked`，保留可恢复的目录条目。域名出售只能证明该域名当前不提供代理，不能证明整个品牌消失。 |
| 快代理的重复 IP 是虚假诱饵 | 抽检重复样本、连接拒绝不足以证明造假。应先识别演示行、真实表格、去重及正确端点，再做独立候选验活。 |
| 301 后三个子站应全部删除 | 将旧域名设为别名；SSL / US / SOCKS 子页面的筛选内容不同，仍可作为同一适配器的独立配置。 |
| 商业官网可访问，等于免费账号/API 正常 | 之前没有商业账号的取号或配额验证。分别记录文档可访问、账号未配置、提取未验收。免费试用也不等于每日持续赠送。 |
| Geonode `lastChecked` 是毫秒时间戳 | 前次样本是 Unix 秒；按秒转换，并保留为上游声明，不能写为本池 PASS。 |
| 国内只剩两家 | 小幻此前也取得了含代理的页面，不能从覆盖表遗漏。 |
| 两轮实测能代表生产网络 | 前次 Python 请求禁用了 TLS 校验，并使用了 macOS 系统代理；部分 curl/TCP 请求走直连。不同路径的结果不能混为一项生产验收。 |

`jetkai/proxy-list` 前次 GitHub 查询显示最后提交为 2023-04-15，足以暂停其“每日更新源”资格，但不证明文件内每个地址已经死亡。`7yip.cn` 与项目已有的 `qiyunip.com` 必须分别验证，不能因中文名相似相互替代。

本轮额外读取了官方公开文档及部分仓库 README，开启正常 TLS 校验、明确使用本机现有 HTTP 上游；没有进行账号提取、购买、注册或新的代理批量验活。文档证据见第 11 节。

## 2. 当前项目已具备什么、还缺什么

| 位置 | 当前代码事实 | 对本次设计的影响 |
| --- | --- | --- |
| `internal/application/ingest.go` | 注册类型是 `custom`、`provider_api`、`site_fetch`、`static_url`；`ProviderConfig` 仅适配 SCDN | 不能给 `provider_api` 填任意商业 API URL 就认为适配完成。 |
| `internal/integrations/sites/catalog.go` | 有 11 个目录项：云代理普通、积流、齐云、89、LumiProxy 三类、ProxyScrape 两类、OpenProxyList 两类 | “已有代码目录项”不等于已创建实例，更不等于当前可用。 |
| `internal/integrations/sites/parse.go` | `ParserText` 提取任意位置的 IPv4:port，并统一标为 HTTP；JSON 解析主要处理 `protocol: 1/2` | 不可直接拿它接 SOCKS5 文本、Geonode 的 `protocols[]` 或混合协议文件。 |
| `internal/integrations/staticdl/staticdl.go` | 通用列表下载已有地址策略、DNS 固定、逐跳重定向检查、条件请求、4 MiB 响应上限 | 纯文本来源首先复用此能力。 |
| `internal/application/ingest.go`、`cmd/proxy-pool/main.go` | site/SCDN 客户端默认仍为 `http.DefaultClient`；未在启动时换成 `staticdl` 的受控下载器 | 扩容渠道前统一下载边界；不能假定所有现有分支已有相同重定向、环境代理及 DNS 防护。 |
| `internal/application/staticcache.go` | 内存缓存；每源最多保留 1,000 个候选；默认运行配置 TTL 5 分钟、256 个来源条目、4 MiB 计费上限；304 保留 batch ID | `256` 是来源缓存条目数，不是代理数。5 分钟来自 `config.Default`/启动配置；独立构造器默认值为 10 分钟。 |
| 同上 | `staticEntryBytes` 主要累计部分字符串；未完整计入结构体、切片容量、map、临时解析分配；保留前 1,000 条 | 现有“4 MiB”不是完整实际内存上限。需要更准确计费及公平取样，不能简单增大列表。 |
| `internal/application/prewarm.go` | 通过 SQLite `SelectPrewarmCandidates` 扫描持久库存；没有静态缓存候选入口 | 静态预抓取和静态候选预热需要分别接线。 |
| `internal/application/acquire.go` | `Ephemeral` 分支对 API/静态候选均实际探测，普通路径不复用/写入库存式证据缓存；静态批次另有最终检查 | 要让静态预热惠及后续获取，需要显式新增静态证据路径，不能只增加定时抓取。 |
| `internal/api/sources_v1.go` | 严格嵌套配置；保存配置不抓取；更新有配置版本检查 | 新字段及供应商配置必须扩展真实 schema；设计示例不能直接当成当前 API。 |
| `internal/integrations/sites/sites_test.go` | 目录测试固定了 11 项名单；fixture 多数按解析器类型生成 | 增加目录项时同时增加真实形状的脱敏 fixture、更新完整目录验收，不能把名单检查删掉充数。 |

## 3. 统一适配结构

保持单进程 Go + SQLite。品牌专属逻辑止于“取回并解释候选”，统一的权限、去重、CD、验证、租约、粘性流程仍由应用层负责。

```text
目录模板 / 供应商产品定义
          ↓ 创建实例
Source（配置版本、协议、账号引用、限制、项目范围）
          ↓
受控下载 → 专属解析 → 共享规范化
          ├─ public/account list → 有界候选缓存
          ├─ on-demand API      → 请求内候选
          └─ external upload    → 既有持久库存
                       ↓
候选分页/去重 → 当前项目资格 → 同范围证据或实际验活
                       ↓
原子交付：来源版本 + 候选代次 + 租约 + 项目 CD
```

### 3.1 适配器家族与版本

| 家族 | 项目映射 | 负责的差异 |
| --- | --- | --- |
| 纯文本列表 | 复用 `static_url + lines_v1`；增加代码拥有的来源模板 | URL、默认协议、更新线索、响应上限及来源标签；兼容的品牌不各写解析代码。 |
| 公开 HTML / 特定 JSON / 带头文本 | 扩充 `site_fetch + site_v1` 目录及内部解析器版本 | Geonode、Spys、各站 DOM/编码、错误页和分页语义。 |
| 按需提取 API | 扩展 `provider_api` 的 `vendor_id`，按供应商/产品选择独立模块 | API 认证、数量、账户限流、错误信封、过期时间、额度扣减。旧配置缺省 `vendor_id=scdn`。 |
| 已分配账号代理列表 | 新增 `account_list`，首个实现 Webshare direct list | 只读同步已经分配的连接，返回账号作用域快照，复用短期缓存；不调用换号/购买接口。 |
| 配置型网关 | 保留 `configured_gateway` 的扩展设计，首期不启用抓取服务网关 | 已确认支持透传的固定网关才可输出连接；真正的上游 session 参数参与连接身份。 |
| 文件/程序上传 | 既有 `custom` | 同一个持久导入入口，不把公开列表抓取偷偷改成自动上传。 |

新类型要修改注册表、配置解析、候选选择、统计和管理台，不能只新增包。`account_list` 的 SQL 选择条件需要加入静态供给路径；其缓存不能被 `adapter_type IN ('site_fetch','static_url')` 漏掉。

建议在内部能力声明中增加 `supply_mode`，值为 `list_snapshot`、`on_demand`、`persistent_upload`、`configured_gateway`。它由代码注册表决定，不能让用户随意把按次取号设为快照以绕过不囤货规则。逐步替代以 `Ephemeral`、`ExpiryBasis` 字符串推断全部生命周期的分支；来源过期依据仍保留独立含义。

### 3.2 每个适配器必须提供的契约

沿用 `source-contract.md` 的 Fetch / Parser / Normalizer 分层，落地以下最小内容：

- 请求：`source_id`、配置版本、请求/操作 ID、数量上限、可支持的筛选、总截止时间、账号及保密配置引用。
- 结果：增量发出的候选、实际接受/拒绝/去重/截断数、取回时间、上游更新声明、已读/计划页数、分页覆盖范围、ETag/Last-Modified、下次允许请求时间。
- 候选：明确的 `http` 或 `socks5`、规范化地址/端口/真实凭证、上游 record ID、已知供应商到期时间、上游 claims。未知到期时间为 `null`。
- 能力：是否列表读取或新分配、支持的协议/筛选、是否计费/消耗额度、是否幂等、是否权威完整快照、账户限制共享键、目标 TLS 是否真正透传。
- 错误：分别返回格式错误、明确空列表、认证失败、限流、额度耗尽、网络不可达、提取结果不确定；HTTP 200 内的错误页面/JSON 错误信封必须先识别。

每个供应商产品用强类型配置和版本化 fixture。不能用通用 `map[string]any` 加任意表达式/脚本来代替接口语义。

### 3.3 共享规则

1. `https` 标签只有在来源明确表示“HTTP CONNECT 可达 HTTPS”时映射为 `transport=http`，并记 vendor claim；它不等于连接代理本身使用 TLS。SOCKS4、V2Ray、真正的 HTTPS-to-proxy 目前不支持。
2. 同一个地址同时支持 HTTP/HTTPS 时合并 HTTP 连接身份；若还支持 SOCKS5，SOCKS5 是另一真实连接上下文。身份继续使用 `domain.ConnectionFingerprint`，不含 source/project，不以更换来源绕过 CD。
3. 供应商“高匿、有效、国家、延迟、lastChecked”只能作为 claims；地理过滤使用本池观察，PASS 只由本池对应范围的验证产生。不能因为出现 `Via` 就直接断定泄露真实客户端 IP。
4. 按来源、配置版本和固定查询参数缓存。带账号来源额外绑定账号/凭证版本；不得把不同账号的快照或同 URL 不同认证混用。
5. 分页失败或超限不伪装为完整权威快照。公开滚动列表允许发布显式标注的有界样本；Webshare 等账号完整列表只有全部页面和必需字段成功时才整体替换。
6. 403/429/登录页/出售页/验证码页不作为“成功空列表”。有明确结构的真实空列表才可安装空批次；保留的旧批次也不越过硬有效期继续供给。
7. 下载器所有分支使用相同的地址策略、TLS 验证、逐跳检查、字节/页数/截止时间上限。账号认证不跨域转发；限制响应解压后的体积；响应/日志不记录提取 URL 的 Key 或原始凭证。
8. 需要不同网络出口时增加显式、可审计的 fetch egress 配置，并定义 DNS/地址检查行为。不得依赖运行机的隐式系统代理，也不得让抓取来源自动依赖尚未验证的本池代理形成循环。

## 4. 全部 26 个入口的渠道设计

“拟开放”表示优先完成适配与生产路径验收后可启用；“暂停”仍保留设计及状态，不会自动进入运行候选集。所有公开列表只建议缓存，不写入 `proxy_resources/source_offers` 长期库存。

| # | 来源/拟用模板 | 适配方式与主要处理 | 预存储及接入结论 |
| --- | --- | --- | --- |
| 01 | ProxyScrape：`proxyscrape-http` / `proxyscrape-socks5`，保留既有 anon/elite ID | 既有 `site_fetch`；补保留协议的行解析；v4/v2 用分别验证的固定模板，禁止未知格式自动猜测 | 预抓取、短缓存；已有 HTTP 代码，SOCKS5 需补；拟开放。 |
| 02 | Geonode：`geonode-http` / `geonode-socks5` | 新增 `geonode_json_v1`，读 `data[]`、`protocols[]`、秒级 `lastChecked`；有界滚动分页 | 预抓取、短缓存；JSON 列表，不按新分配 API 处理；拟开放。 |
| 03 | Proxy-List.download：HTTP/HTTPS/SOCKS5 | 纯行模板；HTTPS 按经确认的 CONNECT 语义映射；SOCKS4 拒绝 | 接口恢复后短缓存；当前暂停，502/429 不标“永久停运”。 |
| 04 | Spys.me：HTTP / 待定 SOCKS | `spys_text_v1`：跳过固定头、读取首列连接，解析国家/匿名标识/更新时间为 claims | HTTP 预抓取、短缓存；`socks.txt` 缺少明确版本，本轮保持待核验，不猜 SOCKS5。 |
| 05 | Spys.one | 首选关联 Spys.me 的供给模板；独立 HTML 路线预留 `spys_html_v1`，需处理站点特有端口表达式 | 默认不重复抓 HTML；若独立接入，仅缓存，须先补真实 fixture，不运行页面 JS。 |
| 06 | Free-Proxy-List.net：主列表 | `freeproxy_html_v1`，定位带列表签名的 textarea 或指定表格，排除全页广告文本 | 预抓取、短缓存；拟开放。 |
| 07 | SSLProxies.org | 旧域名为别名，固定规范页面 `free-proxy-list.net/ssl-proxy.html`；复用前项解析器 | 独立筛选配置、短缓存；合并域名别名，不删除 SSL 子列表。 |
| 08 | US-Proxy.org | 别名映射到 `free-proxy-list.net/us-proxy.html`；国家仍是来源声明 | 独立筛选配置、短缓存；拟开放。 |
| 09 | Socks-Proxy.net | 别名映射到 `free-proxy-list.net/socks-proxy.html`；从有协议字段的记录识别 SOCKS5 | 仅已明确 SOCKS5 的记录短缓存；模糊/仅 SOCKS4 数据不供给。 |
| 10 | monosans/proxy-list：HTTP/SOCKS5 | 当前 `static_url + lines_v1` 可复用；协议分文件；带认证行保留真实凭证 | 预抓取、短缓存；首批优先来源。混合 `all.txt` 包含 SOCKS4，暂不用作首期默认模板。 |
| 11 | TheSpeedX/PROXY-List：HTTP/SOCKS5 | 同一行适配家族；README 仍给出 `TheSpeedX/SOCKS-List/master/...`，把仓库更名/URL 别名显式登记 | 预抓取、短缓存；大列表采用公平有界取样，拟开放。 |
| 12 | jetkai/proxy-list | 复用行模板，但必须选择可证明协议的文件；混合裸 IP:port 不可推断协议 | 暂停；先通过数据新鲜度及协议核验再恢复，不为旧数据预热/落盘。 |
| 13 | roosterkid/openproxylist：HTTPS/SOCKS5 | `HTTPS_RAW.txt` 与 `SOCKS5_RAW.txt` 两个协议模板；HTTPS 文件按已确认 CONNECT 语义映射 HTTP | 预抓取、短缓存；与现有 OpenProxyList 域名源分别保留 provenance，核对后才关联镜像。 |
| 14 | 快代理免费区：`kuaidaili-free` | `kuaidaili_html_v1`；验证当前免费表格/数据脚本形状，识别演示内容；去重后计数 | 低优先级候选，先修正取样和真实数据识别；验收通过后缓存，不以旧重复样本判造假。 |
| 15 | 89 免费代理：现有 `ip89` + 拟 `ip89-html` | 现有 `api.89ip.cn/tqdl.html?...` 与前次访问的 `www.89ip.cn/tq.html?...` 不是同一端点；分别用行解析和两列表格解析 | 可预抓取、短缓存；官网表格可作显式备用模板，不能把 HTML 当文本 API。 |
| 16 | 齐云旧入口 `7yip.cn` | 暂停的 HTML 模板；仅在原域名恢复并获得实际页面 fixture 后确定列映射 | 不预抓代理、不缓存候选；不连带停用 `qiyunip.com`。 |
| 17 | 云代理 `ip3366.net`：普通/高匿页面 | 复用表格家族，按 GB2312/GBK 转码；绑定表格与协议列，保留各分页/匿名类别 | 预抓取、短缓存；既有 `ip3366-normal` 保留，新增类别独立验收。 |
| 18 | 小幻 `ip.ihuan.me`：`ihuan-free` | `ihuan_html_v1`；提取 IP 链接的文本与相邻 port，不把旗帜/地区链接串入 IP；状态/日期作为 claims | 预抓取、短缓存；补回覆盖表，拟开放。 |
| 19 | Webshare：`webshare-direct` | 新 `account_list + webshare_list_v1`；Token 认证，完整分页读取已分配列表；账号与代理凭证分开 | 账号作用域短缓存；凭证引用/配置持久化；不自动替换、购买或囤取号结果，待账号验收。 |
| 20 | ScraperAPI | 目标内容 API 不输出候选；Proxy Port 可设计配置网关，但官方文档要求供应商 CA/关闭 SSL 校验 | 当前目标 TLS 契约下不开放常规供给；不预存代理。保留产品能力/不兼容原因。 |
| 21 | ScrapingBee | 目标内容 API 不输出候选；HTTP/SOCKS5 Proxy Mode 有固定网关和参数化密码，但官方要求关闭 SSL 校验 | 同上；不能因为有 SOCKS5 端口就视为普通透传代理。 |
| 22 | Crawlbase | Crawling API 不输出候选；Smart Proxy 文档明确拦截目标 TLS | 同上；供应商宣称验证了其到目标的连接不能替代本池端到端证明。 |
| 23 | 品赞 `ipzan.com`：拟 `ipzan-extract` | 独立 provider 模块；数量/地区/协议和账号授权按实际提取文档映射；区分直连与隧道产品 | 按需提取不预存；账号/额度状态持久化。官网只证实有产品，真实取号协议待账号样本。 |
| 24 | 芝麻：拟 `zhima-extract` | 独立 provider 模块；先核实当前官方域名、产品和授权方式，禁止从其他厂商照搬请求参数 | 当前暂停/待资料；未来按需提取不预存，保留可恢复元数据。 |
| 25 | 太阳HTTP：拟 `taiyang-extract` | 独立 provider 模块；官方指南区分数量提取、隧道、账密，选择产品后分别适配 JSON/TXT 与错误信封 | 数量提取不预存；只读已分配静态产品可走账号列表缓存，不能混为同一生命周期。 |
| 26 | 极光旧入口 `jiguangdaili.com` | 域名出售页，不能作为代理来源；保留 retired endpoint 记录。恢复需重新核验官方身份/文档/样本 | 不预抓/不预存，当前不创建可运行适配器实例。 |

FastGit、ghproxy、jsDelivr 是之前提到的传输/镜像方式，**不是额外代理供应渠道**。如确需使用，记录 origin 与 mirror，明确缓存新鲜度/内容来源、沿用原候选 provenance 和 CD；不将第三方镜像加入默认路径，也不把旧加速服务的名称当成可用性证据。

### 4.1 ProxyScrape 与 Geonode

ProxyScrape 继续维护既有目录 ID，不用新的概念分类破坏现有实例。现有 `proxyscrape-anon` 查询包含 `Anonymous,Transparent`，不能给整批盖“anonymous”标签。新解析器遵守 URI 的真实协议；裸行按该模板唯一默认协议解释。若切换 v4/v2，必须是经过测试的版本模板，并递增配置/解析器版本，不能网络失败就切向不明地址。

Geonode 示例入口：

`https://proxylist.geonode.com/api/proxy-list?limit=50&page=1&sort_by=lastChecked&sort_type=desc`

解析 `data[].ip`、`port`、`protocols[]`、`country`、`anonymityLevel`、`lastChecked`。协议展开后去重；未知协议行记拒绝数。`lastChecked` 转换为 UTC 的供应商检查时间，不用来生成 `provider_expires_at`。首批只读取至多 5 页、每页 50 条；这是采样窗口，不声称遍历全站。排序期间数据可变，允许页面间去重；不因第二页失败删除第一轮缓存或上传库存。

### 4.2 GitHub 三个活跃候选与一个停更候选

首期模板使用前次取得过内容的协议文件：

| 来源 | HTTP/CONNECT 文件 | SOCKS5 文件 |
| --- | --- | --- |
| monosans | `https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt` | `https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt` |
| TheSpeedX | `https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt` | `https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt` |
| roosterkid | `https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt` | `https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt` |

优先条件 GET，不为每次 acquire 调 GitHub commits API。更新声明/最近数据提交检查使用独立低频动作；README 修改、响应 Date、CDN 的 200/304 都不能自动当成代理文件内容更新时间。monosans 当前 README 写每小时复查，先前答复的“每 30 分钟”不应写死到代码里。

`all.txt` 若有逐行 `protocol://` 可在未来混合协议模板中安全过滤；首期使用协议文件更易隔离错误。monosans README 还允许 `username:password@host:port`，不得用简单 IPv4 正则剥掉认证信息。

大文件不能永远只看前 N 行。可保留小部分前排候选，再对其余行做有界 reservoir sample，并在缓存批次内用游标轮转；排序仅是供应商质量提示。当前 4 MiB 读取上限以内也要限制行长、总解析记录数和错误详情数。文件超过硬限额应报告超限，不能将截断文本当完整快照。

jetkai 不需要重新实现下载器；缺的是新鲜度资格和真实协议信息。默认暂停即可，不建立“全部 IP 黑名单”。

### 4.3 Spys 与 Free-Proxy-List 家族

Spys HTTP 固定入口为 `https://spys.me/proxy.txt`；每行是 `IP:Port` 加标记，并非纯 `lines_v1`。以首列为连接、固定头部为格式证据，显式映射 N/A/H 等上游代码；无法识别的标记记 unknown。更新时间带时区，保留原值及解析结果。不要把 Google 标记当本池目标域名通过。

本轮读取的 `https://spys.me/socks.txt` 头部只写 Proxy list，没有 SOCKS4/5 版本字段。这个入口在获得供应商协议声明或受控协议识别之前不能产出 `socks5` 候选。若实现协议识别，它属于有界诊断步骤，失败归因是“协议未确认”，不能擅自降级 HTTP 或对整批宣布死亡。

Spys.one 若以后需要独立 HTML 数据源，使用只支持已知端口表达式形状的解码器；无法识别时停止该模板，不加载网页 JavaScript 执行器。仅使用 Spys.me 时将 Spys.one 作为品牌/来源链接，不虚增渠道供给数。

Free-Proxy-List 家族用一个解析器、四个固定页面配置。textarea 只适用于协议已明确的 HTTP/SSL/US 页。SOCKS 页若 textarea 丢失了协议字段，应读取表格原字段或拒绝歧义行。HTTP/SSL/US 页的同一真实连接跨来源去重，但保留来源关联，项目授权不因去重被扩张。

### 4.4 国内 HTML 来源

所有 DOM 解析先确认页面标题/列表容器/列头，再逐行读取，区分正常空表、改版和访问受限。HTMLParser 能解析页面不等于识别了代理表。按站点读取明确编码，UTF-8/GBK 不靠忽略解码错误糊过去。

- **快代理**：仅对确认的免费数据容器启用解码器；真实页面如果改成嵌入 JSON，就新增版本化 fixture，不把本项目现有 `fpsList` 正则默认当作其当前实现。重复行先合并再抽样，并记录 `rows_raw/unique/probed` 三个分母。
- **89**：继续测试现有 API 模板；官网 HTML 备用模板固定到 `http://www.89ip.cn/`。一份响应不能同时作为“纯文本成功”和“HTML 成功”计数，备用的选择及原因应写入 fetch report。
- **云代理**：保留既有普通页 `http://www.ip3366.net/free/?stype=2&page={page}`；首页/高匿页另建模板。只按模板申明的协议及列含义输出，匿名级别不合并。
- **小幻**：固定到 `https://ip.ihuan.me/` 起步，首期一页；以后分页只跟随已许可的同源路径，不能遍历所有详情链接。
- **7yip**：保持暂停，不凭一个 403 页面编造表格 schema。

### 4.5 Webshare：API 读取已分配列表

官方文档确认：

```text
GET https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=25
Authorization: Token <account-api-key>
```

返回 `count/next/previous/results`，记录包含 `id`、`proxy_address`、`port`、`username`、`password`、`valid`、`last_verification` 等。`valid` 和 `last_verification` 仍是上游声明。

首期只支持 direct 模式、账密连接；协议由已确认的账号产品/配置选择。不要把 backbone 模式的地址/端口和 direct 列表混用。白名单授权只有在池与下游实际出口均满足其条件时才可提供，池自己验活成功并不替下游建立白名单。

`next` 只允许已确认 API origin/path，带 Token 不追随任意跨域链接。分页超过安全上限、账号响应不完整或凭证变更时，不发布“完整快照”。有完整新列表后整体替换本来源的旧缓存，绝不影响 `custom` 库存。

这类调用读取已有分配，不产生新代理。因此它适合短期缓存，初期重启后重新读取即可；免费套餐数量、流量、续期条件从实际账户状态核验，不把前次给出的“10 个/1GB”当作长期固定规则。`refresh proxy list`、替换、订阅变更、购买都不属于该只读适配器。

账号 Key 使用持久加密引用；列表中的连接凭证只进入受限缓存及必要的会话私有状态。来源启用前明确允许哪些项目使用。若要对 `AllowedSourceIDs=null` 的开放项目隐藏该账号，需要增加来源侧允许项目约束；当前单向项目 allowlist 不能表达这种“私有来源默认隐藏”。

### 4.6 品赞、芝麻、太阳HTTP、极光

每个供应商都保留独立模块与产品 ID，不能因为均返回 `IP:Port` 就共享签名/配额/到期解释。

| 模块 | 请求与认证设计 | 响应/期限设计 | 实施前需要的真实材料 |
| --- | --- | --- | --- |
| `providers/ipzan` | 选择直连数量提取产品；数量、协议、地区白名单映射到官方字段；API Key/生成提取链接保密配置，代理账密或白名单另列 | 优先带期限 JSON；若只有 TXT，按配置声明协议且 expiry 未知；隧道产品另建网关配置，不伪装多条静态 IP | 官方 API 请求样本、免费额度口径、成功/空/鉴权/扣额失败信封、期限锚点。当前公开页是 SPA/产品说明，不能替代 API fixture。 |
| `providers/zhima` | 当前官方域名和产品先确认；然后固定 API origin、账号、数量上限及签名规则 | 严格按该产品字段处理；不照搬历史域名的接口或其他品牌错误码 | 官方身份、当前文档、账号和完整样本未齐，保持 `awaiting_contract`。 |
| `providers/taiyang` | 官方指南区分数量提取/隧道/账密；首期仅数量提取，控制台生成的专属链接以密钥对象保存 | 支持经 fixture 验证的 TXT 或 JSON，识别错误信封和有效期；报价页“5 分钟–1 天”不是每条代理的真实 expires_at | 当前产品专属 API 链接结构、免费/计费边界、429/余额/白名单错误、返回时间的格式与时区。 |
| `providers/jiguang` | 旧域名停止作为可执行端点；只有核实新的官方服务后才实现请求 | 无真实响应契约时不编造解析器 | 旧域名出售，不假设存在可调用的替代 API。 |

通用执行顺序：读取并锁定账户免费预算 → 记录 fetch operation → 一次有界提取 → 解析/规范化 → 当前请求按范围验活 → 发放 → 未使用候选销毁。只有供应商明确支持幂等键时才以相同 operation 重试；提取请求超时且是否扣额不明，记录 `SOURCE_PURCHASE_AMBIGUOUS` 并阻止盲目再取。

`wanted` 只覆盖本次缺口加有上限的失败余量，不能自动多取几百个存起来。账户限制按供应商真实范围共享（账号/产品/出口），不同 source ID 不能绕过限流。免费额度耗尽时停该账户的提取并切其他获准来源；这与全局云流量只告警的规则不同。

“签到/领额度”是权益管理，不是候选获取。首期只展示权益/待领取状态及已确认的充值免费额度；不实现浏览器登录、验证码或每日自动签到。后续若供应商有正式领取 API，单独设计权益操作、幂等和额度读取，不能把它藏进每次 Fetch。

### 4.7 ScraperAPI、ScrapingBee、Crawlbase

三家的目标内容 API 收到 URL 后返回网页/JSON，无法经规范化变成代理候选，本项目不调用这些 API 转发下游业务。

它们确实有代理模式，需记录真实能力而不是笼统写成“不是代理”：

| 产品 | 官方连接形状 | 与当前项目契约的冲突 |
| --- | --- | --- |
| ScraperAPI Proxy Port | `http://scraperapi:<API_KEY>@proxy-server.scraperapi.com:8001`，额外参数可进用户名 | 文档要求不验证 SSL，或手动信任其 Proxy CA；获得的是供应商中间层证据。 |
| ScrapingBee Proxy Mode | HTTP `proxy.scrapingbee.com:8886`；SOCKS5 `socks.scrapingbee.com:8888`；用户名为 Key，密码为参数 | 文档明确要求关闭 SSL 验证；代理模式仍按抓取 API 的语义、渲染及费用处理。 |
| Crawlbase Smart Proxy | HTTP `smartproxy.crawlbase.com:8012`，Token 为用户名、空密码；另有 HTTPS-to-proxy 8013 | 文档明确拦截 TLS 并呈现 Crawlbase 证书；8013 还涉及项目尚不支持的代理侧 TLS 传输。 |

当前结论是标记 `incompatible_target_tls`、不开放常规供给、不缓存候选。即使可用供应商 CA 完成其 TLS，也不能标成项目已有的 `validation.mode=tls` PASS；不会为了接入修改系统信任库或关闭证书验证。

未来若确需这些产品，应单独提议验证模式、客户端能力、证据含义和计费协议。即便成为配置网关，也只保存真实网关/账号/参数；不能复制同一个 host/port/凭证凑出 N 个代理，不能靠虚构 session_id 绕过当前 CD。实现接口可以返回“不兼容当前供给契约”的能力结果，而非创建一个看似可取号的空适配器。

### 4.8 已在项目中、但前两轮未列全的来源

| 既有目录/模块 | 本方案处理 |
| --- | --- |
| `jiliuip-elite` | 保留 role-cell 解析；加入相同缓存/任务/验收路径，需真实 fixture 及当前可用性检查。 |
| `qiyunip-elite` (`qiyunip.com`) | 单独处理 TH 表格与 403；不受 `7yip.cn` 结论影响。 |
| `lumiproxy-transparent/anon/elite` | 保留各 JSON 列表模板，按平台共享限流，protocol 1/2 语义继续明确；候选只短缓存。 |
| `openproxylist-http/https` (`api.openproxylist.xyz`) | 保留两目录项；与 roosterkid 是否同源须单独验证，在证据不足时不合并身份或计数。 |
| `scdn` / 现有 `provider_api` | 保留请求内候选生命周期和现有兼容配置；不得随着新的公开 JSON 列表接入被改为囤货。 |

## 5. 到底需要预存储什么

### 5.1 四个容易混淆的动作

| 动作 | 是否推荐 | 含义与边界 |
| --- | --- | --- |
| 预抓取 | 活跃公开列表推荐，闲置来源按需 | 提前下载并解析，消除 acquire 路径上的网页/多页等待。结果仍未验证。 |
| 内存缓存候选 | 推荐 | 每源/全局数量、字节及期限有界；过期刷新；不写入持久代理库存。 |
| 预热少量可用候选 | 按项目需求推荐，需新增静态接线 | 以数量、目标/范围、时间、尝试数和共享并发为界验活；不发放、不预留专属库存、不写 CD。 |
| 持久化候选清单 | 公开源首期不需要 | 可重取、寿命不确定且大量重复，重启恢复旧清单也必须再检查资格/验活。 |

Webshare 只读列表适合账号短缓存；按需取号 API 禁止公共预抓/预热。固定网关的连接配置需要持久保存，但保存一项配置不等于囤积一池出口。用户上传和外部程序推送有“不能从上游重取”的价值，继续持久化。

### 5.2 三种时间必须独立

- `list_fetched_at / list_checked_at / refresh_after / cache_expires_at`：下载缓存何时刷新、何时停止选取。
- `evidence.completed_at / valid_until`：本池对具体连接、来源版本、代次、验证模式/目标/安全配置的证明窗口。
- `provider_expires_at`：供应商明确声明的该资源到期时间；未知为 `null`。租约/逻辑会话期限又是单独的本池决策。

缓存可以保留 10 分钟，而证据只能复用 30 秒。304 只延长列表检查有效性，不更新代理 PASS 时间、provider expiry 或数据发布时刻。200 内容相同也不延长证据。

对静态来源，缓存硬到期是“当前批次不可新发放”的门槛，不对下游宣称是代理物理寿命。最终提交时必须再次检查来源版本、批次仍为当前且未过硬截止；当前 `StaticBatchCurrent` 只检查 batch ID，这一硬截止及与刷新并发的正确性应作为新增设计的验收点。

实现时定义提交的线性化点：静态缓存提供有截止时间的短期批次读取令牌，源配置/权限仍由 SQLite 交付事务检查；刷新/淘汰与提交不能仅靠一次事务前 bool 检查竞争。全局缓存锁不得覆盖网络 I/O；需要用按来源代次的读写门闩或等效 fencing，配合事务实际提交时间检查。

### 5.3 建议的来源级刷新策略

以下仅对验收通过且管理员显式启用周期刷新的实例生效。旧实例省略新字段时保留当前按需刷新与全局 TTL 行为。保存配置本身不发网络请求。

| 类别 | 试运行刷新间隔 | 缓存硬有效期 | 每实例候选上限 |
| --- | ---: | ---: | ---: |
| ProxyScrape | 5 分钟 | 10 分钟 | 1,000 |
| Geonode | 5 分钟 | 10 分钟 | 250，首期 5×50 条 |
| monosans / TheSpeedX / roosterkid | 10 分钟 | 20 分钟 | 1,000 |
| Spys.me HTTP | 30 分钟 | 60 分钟 | 1,000 |
| Free-Proxy-List 各子页 | 10 分钟 | 20 分钟 | 1,000 |
| 国内 HTML 列表 | 15 分钟 | 30 分钟 | 500，先从一页开始 |
| Webshare direct 只读列表 | 10 分钟 | 20 分钟 | 账号已分配数量，且不超过本地安全上限 |
| 暂停/不兼容/待账号契约 | 不调度 | 无 | 0 |
| 按需数量提取 | 仅当前请求触发 | 不适用 | 本次缺口与配置批次上限 |

供应商真实频率规则、Retry-After 或 reset 时间优先；若冲突，调整整组刷新/截止参数，而非无视限流。上游数据发布时间单独设 `max_source_age`：已有“每日更新”承诺且能验证数据文件更新的模板可用 36 小时作为告警起点，不能将该阈值泛化到所有未提供时间戳的来源。

`max_source_age` 超限先降权/暂停新周期预热并报告，不删除其他来源或用户上传的同地址。未知更新时间不能因为服务器返回当日 Date 就记为今日更新。

### 5.4 调度及小内存边界

增加进程内有界 `SourceRefreshService`，复用已有 jobs/取消机制；不用 Redis、外部 cron 或每源永久 goroutine。到点扫描只处理少量 due source，按来源合并并发刷新，平台级限制共用 budget key。

首期建议：抓取全局并发 2、每源 1；背景列表预热同时至多 1 个探测并占用现有全局 4 个 probe slots，在线工作优先、后台保留有限公平份额。所有队列有界，排队计入任务截止时间。失败按类别退避；访问受限/身份待修复不做高频重试。源启停及凭证改版立即作废相应缓存/任务令牌。

候选缓存继续从当前 4 MiB 的预算起步，但先修复计费：计入紧凑记录、字符串、claims、切片容量及固定开销；另设全局候选数上限（建议起步 4,096）、每源上限、来源数上限。三者任何一个先到即限制。抓取/解析的临时空间另按并发×响应上限预算；不把缓存预算当作进程 RSS 上限。

使用共享候选游标，避免每次 request 都取列表前几条。游标按来源/配置版本/批次隔离；当前请求有独立的 seen 集。项目因 CD 排除所有候选时，停止无收益重抓、给出 shortage 原因，不把该项目的资格问题记成来源故障。

缓存刷新失败时，未过硬期限的旧批次仍可参与正常验证；硬过期后停止新分配。完整新批次可以原子替换，304 保留代次；非 304 的真实替换采用新代次，保守地不继承旧批次 PASS。确认坏条目仅从当前批次移除，不建立阻止以后重新出现的持久黑名单。

### 5.5 静态预热需要新增的项目能力

将预热候选提供者拆为持久库存页与当前静态缓存页两种实现，共用数量/时间预算及验证器；不把缓存复制进 SQLite 库存以兼容旧预热器。按需 API 不作为预热候选提供者。

静态证据缓存采用明确键：

```text
(connection_fingerprint, source_id, source_config_version,
 batch_incarnation, validation_scope_hash)
```

建议将静态证据与其候选批次一起保存在有界内存中，复用同一个验证资格检查器；不能只在预热结束时维护一个永不下降的 ready 计数。已有 `validation_evidence` 表继续承担原有职责；如未来选择复用它，必须处理其当前 `(fingerprint, scope_hash)` 唯一键以及来源/代次覆盖问题，不能未经迁移直接冒称多来源证据共存。

修改 acquire 的静态分支以允许同范围、同来源版本、同代次的近期静态证据复用；按需 API 的普通获取仍执行当前请求验证。更晚 FAIL 使旧 PASS 不可用；来源/账号改版、批次替换、超期都使 ready 立即下降。

`Ready(project, filters, scope, now)` 必须重新计算：当前获准来源、未失效缓存/资源、实际连接去重、项目 CD、并发资格、过滤及新鲜 PASS。达到目标就结束；不做永久“保持 1 万活跃”的后台任务。任务完成是时间点报告，不预留未来发放数量。

## 6. 为什么首期不落盘公开候选

持久化解决的是“重启后要重新下载”这件事，不能解决“拿到候选后验不过”。公开免费列表的主要成本通常在验证和选择；实际占比需要生产路径计时，不能从前次 TCP 抽检推断。

用一个明确的预算例子评估预热，而非按站点数量预设大池：设需要预热的可用数量为 `R`、在同一验证范围的去重样本通过率为 `p`、平均一次实际验证占用为 `L` 秒、可用并发为 `C`，则粗略需要 `R/p` 次验证、耗时约 `R×L/(p×C)`。这只是均值估算；不能作为成功数量保证，重叠来源和相关失败还会使估算偏乐观。

例如 `R=10, p=0.1, L=2.5s, C=4`，估算约 62.5 秒；若后台只用 1 槽则约 250 秒，已超过示例配置的 30 秒证据复用窗口。提前存下十万个地址对此没有帮助；需要减少热目标、提高来源质量、缩小验证范围或接受按需等待。缓存过期与存活期也不能为了“维持库存量”而延长。

下载量也可以直接核算：假设 12 个列表各 256 KiB、每 5 分钟完整重取，约 864 MiB/天的响应体；实际条件请求通常能减少它。这里只算本池下载，不包含验证往返、TLS 开销及下游直接通过代理的业务流量。

上线观察后，只有同时出现“重启/发布后的重下载明显破坏首个请求预算”且“仍新鲜的缓存确实能被恢复利用”，才考虑独立磁盘 TTL 缓存。先记录冷启动恢复时间、fetch p95、验证耗时、缓存命中率、唯一连接贡献及 restart 频率，再定收益阈值。

如果进入第二阶段，磁盘缓存至少保存：来源与配置/解析器版本、批次 ID、fetch/checked/截止时间、上游更新声明、校验和、加密候选负载；设独立磁盘容量与淘汰规则。恢复校验版本/来源状态/硬期限，生成本进程新代次并清空可复用 PASS；损坏或过期即丢弃。缓存表与持久上传库存完全分离，API 临时候选永不写入。

不能仅持久化 ETag，却在没有响应体/候选的冷启动时直接发条件请求并接受 304。冷缓存要无条件取回一次有效内容；304 本身不能重建候选。

## 7. 应持久化的元数据

| 数据 | 方案 |
| --- | --- |
| 渠道配置/启停/版本/项目范围 | 复用 `sources` 与项目表；扩展模板/供应商版本及状态字段。 |
| 账号/API Key/连接配置 | 新增通用 source secret 引用与加密存储；不直接把 Key 塞进当前会原样回显的 `ConfigJSON`。区分 API 认证与代理认证。 |
| 下一次允许拉取、限流/退避、状态原因 | 新增 `source_fetch_state`，保留上次有效行为及 reset 信息；重启不立即重复冲击受限渠道。 |
| fetch/验证/供给统计 | `source_fetch_runs` 或扩展任务报告：时间、源、版本、计数、耗时、失败类、覆盖范围；保存有界摘要，非完整 HTML/代理地址历史。 |
| 免费额度/计费及提取操作 | `provider_accounts` + `fetch_operations`；记录额度单位、reset 时区/时间、reserved/consumed/unknown；未确认 outcome 不因进程重启自动释放。 |
| 租约、项目 CD、私有粘性绑定、任务 | 复用既有表和原子事务；禁止从这些记录重建公共候选库存。 |
| 上传/外部推送候选 | 继续既有 `proxy_resources` / `source_offers` 及 Importer，保留跨来源身份、健康与 CD。 |
| 公开列表候选/原始正文 | 首期仅受限内存；日志/统计不保留可重新供给的完整地址集合。 |

来源 `availability`（active/unavailable/access_blocked/stale_data/awaiting_contract/incompatible/retired_endpoint 等）与管理员 `enabled` 分开。适配代码本身不会因为一次网络失败从二进制消失；明确停用的来源也不会因定时健康检查自动重新启用。

如果账号类来源需要默认私有，追加来源侧项目允许列表，并在候选选择、预热、读凭证及最终交付时与项目侧范围取交集。新增来源不会自动向所有开放项目暴露账号凭证。

## 8. 配置及管理台方案

对简单来源，当前接口已经足以表达原型配置，例如以下是**当前 schema 可表达**的 monosans HTTP 来源定义；创建只保存配置，不代表抓取/验活已通过：

```json
{
  "source_id": "monosans-http",
  "adapter_type": "static_url",
  "parser_type": "lines_v1",
  "enabled": false,
  "config": {
    "static": {
      "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
      "default_transport": "http"
    },
    "max_pool_leases": 1
  }
}
```

以下为**待实现配置草案**，不能发给当前服务器：

```json
{
  "source_id": "geonode-socks5",
  "adapter_type": "site_fetch",
  "parser_type": "site_v1",
  "enabled": false,
  "config": {
    "site": {
      "site_id": "geonode-socks5",
      "max_pages": 5
    },
    "refresh": {
      "mode": "interval",
      "interval_ms": 300000,
      "cache_ttl_ms": 600000,
      "max_candidates": 250
    }
  }
}
```

新增公共配置只放在实际使用的类型上：`refresh` 仅用于列表缓存；`provider.vendor_id` 选择供应商，供应商专属字段由其配置 schema 验证；`account` 引用 Webshare 账号；`access` 表达来源私有范围。拒绝多个供给配置同时存在、按需 provider 配置周期预存、无意义或未知字段。

管理台每条渠道显示：模板/产品、启停、可用性状态、配置/解析器版本、账号已配置状态、最近抓取与上游更新声明、实际候选数与重复率、同范围实测通过数、发放数、耗时、配额/冷却、下一次刷新、缓存容量/截止时间。

操作分为保存配置、启用/停用、配置诊断、下载刷新、项目范围预热、查看报告；只呈现该类来源真正支持的操作。账号列表不显示“新增购买量”；按需提取不显示“持久库存数”；过期的预热通过数不能叫“当前可用”。

## 9. 实施拆分与验收

| 阶段 | 具体变更位置 | 完成标准 |
| --- | --- | --- |
| A：纠正基础语义 | `integrations/sites`、共享 HTTP 下载包、`application/ingest.go`、`staticcache.go` | 类型保真、每种真实错误页可识别、资源有界；修复“文本一律 HTTP”和部分响应误成功；现有 11 项回归不丢。 |
| B：接入无账号来源 | GitHub 行模板、ProxyScrape、Geonode、Spys、Free-Proxy-List 家族及国内可验证 HTML 模块；注册表与管理目录 | 第 4 节每项都有适配实现/复用关系及明确状态；每个拟启用实例有独立生产网络 fetch/parse/probe 报告。 |
| C：列表预抓与预热 | 新 `application/source_refresh.go`；扩展 `prewarm.go`、`acquire.go`、缓存证据及批次令牌；启动/停止接线 | 热列表可提前准备；静态预热不入 SQLite 库存、不写 CD；证据精确复用；到期、刷新、停用竞态不误发。 |
| D：账号列表与按需供应商 | 新 secret/账户/操作表；`integrations/webshare`、`providers/ipzan`、`providers/taiyang` 等；严格配置与管理台 | 一个供应商产品一组真实脱敏 fixture；账户共享限流/额度正确；提取结果不入公共库存；未知扣额不盲重试。无账号项保持 pending。 |
| E：观察与容量调整 | 扩展 `cmd/sitecheck` 到来源模板/实例；低内存与重启场景报告 | 依据真实唯一供给和时延调整刷新/容量；达到第 6 节条件再提独立磁盘缓存。 |

暂停/不兼容来源有“为何不运行”的验收项，不以伪 fixture 假装真实取号通过。可以完成其设计和状态建模，但只有获取真实接口/账号后才算完成供应商适配实现。

实现时需要的关键测试矩阵：

1. 每模板的成功、合法空、HTML 错误页、JSON 错误信封、协议歧义、超大/截断、重复、编码、分页失败；所有错误详情脱敏且数量有界。
2. SOCKS5 不被标 HTTP；供应商 HTTPS 标记不被错当代理侧 TLS；混合文件中的 SOCKS4 明确跳过并统计。
3. 条件请求 304 无正文/冷缓存、未修改内容、旧缓存失败、硬到期、配置更新/删除重建、批次替换与提交并发；保存配置零外部请求。
4. 静态预抓/预热后持久候选库存不增长；provider 提取/诊断/预热后公共库存为零；上传库不受静态刷新与失败波及。
5. 静态证据只在同连接/来源版本/代次/范围复用；FAIL 比旧 PASS 新；换目标、证书设置、账号或协议必须重验。
6. 源别名/镜像/多项目多 Key 不绕过 CD；按项目重新算 Ready，任务取消释放资源，重启时任务状态可信。
7. 账户完整分页、跨域 next、Token 轮换、免费额度耗尽、共享账号多实例并发、提取超时扣额未知、不得自动付费；新私有来源不暴露给开放项目。
8. 固定网关不复制凑数，抓取网关不会通过关闭 TLS 校验伪造目标证据。
9. 2 核/1 GiB 目标负载测量 RSS、Go heap、下载/探测并发、SQLite WAL/写入、候选字节预算、退出和重启恢复；不以源码中的字节计数代替 RSS 观测。

离线测试以受控上游/代理及版本化 fixture 为主体。涉及实现后按变更范围运行普通测试、race、vet；纯设计文档不据此宣布代码测试或新增适配器已通过。

线上验收报告每个模板/账号实例分开记录：执行环境与 fetch/probe 出口、TLS 配置、版本、起止时间、抓取状态、原始/唯一/缓存候选数、抽样方式和分母、实际 Judge/目标 TLS 通过数、失败类、时延、配额消耗及未测项。抽样前去重；可采用固定种子分层抽样，不能再把重复的前十条当十个独立节点。

“验证通过”的证据必须来自生产验证器的 Judge 或目标 TLS；TCP 只作为独立阶段指标。`fetch_only`、`pending_credentials`、`blocked`、`incompatible`、`sample_pass`、`sample_fail` 不能合成一个“全部正常”。已冻结的旧验收文件保留，新增报告另存，不回写历史数字。

## 10. 本次交付边界

本节保留开发前的方案边界；后续用户已要求实施，实际落地与验证记录见第 12 节。首期工程组合仍是：**公开列表有界预抓缓存 + 按项目小批预热 + 按需供应商请求内取号 + 上传库持久化**。磁盘公开候选缓存暂缓，账号/调度及交付元数据按必要性持久化。未知商业产品的购买、额度预留和不确定扣额操作没有被包装成可运行适配器。

## 11. 本轮使用的公开文档

以下均为 2026-11-04 读取的公开文档/README；它们提供协议及产品线索，不证明实际账号余额、免费期限或代理可用率：

- [Webshare List proxies](https://apidocs.webshare.io/proxy-list/list)：direct 列表分页、Token 认证与字段。
- [Webshare Proxy Connection](https://apidocs.webshare.io/proxy-connection)：direct/backbone、账密和 IP authorization 的区别。
- [ScraperAPI Proxy Port Method](https://docs.scraperapi.com/synchronous-apis/proxy-port-method)：用户名/API Key、SSL 限制以及其 Proxy CA。
- [ScrapingBee Proxy Mode](https://www.scrapingbee.com/documentation/proxy-mode/)：HTTP/SOCKS5 地址、认证参数及 SSL 验证要求；独立页面首次请求 TLS 失败，第二次读取成功。
- [Crawlbase Smart Proxy](https://crawlbase.com/docs/smart-proxy)：TLS 拦截声明、HTTP/HTTPS 代理地址及 Token 认证。
- [品赞官网](https://www.ipzan.com/)：账号产品、授权和提取能力的产品说明，未获得完整取号 API 契约。
- [太阳HTTP 提取指南](http://www.taiyanghttp.com/getapi/index)：数量、隧道、账密产品区分及控制台生成 API 链接说明；公开页为 HTTP，具体账户 API 仍待验证。
- [monosans README](https://raw.githubusercontent.com/monosans/proxy-list/main/README.md)：协议文件、混合 URI 文件、认证行、更新声明。
- [TheSpeedX README](https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/README.md)：仓库名与实际下载链接别名。
- [roosterkid README](https://raw.githubusercontent.com/roosterkid/openproxylist/main/README.md)：项目及来源网站说明。
- [Spys HTTP](https://spys.me/proxy.txt)、[Spys SOCKS](https://spys.me/socks.txt)：本轮读取的文本头/字段形状；SOCKS 文件未给明确版本。

## 12. 本批次实施与验收记录

### 12.1 实现口径

代码实现与逐渠道实网验收是两个阶段。用户将逐个入口验收；本批次不因合成 fixture、首页可访问或本地代理/Judge 通过，就宣称供应商渠道已验活。

| 供给家族 | 本批次落地位置与语义 | 是否预存储 |
| --- | --- | --- |
| 上传库存 `custom` | 沿用上传、持久资源/offer 和已有验证、租约/CD | 持久化；不是公开源自动抓取的后门 |
| 公共目录 `site_fetch + site_v1` | 代码拥有的站点/协议模板共用受控下载和专属解析；GitHub 协议文件也作为目录项复用行解析 | 有界内存缓存；不写持久代理库存 |
| 通用 `static_url + lines_v1` | 管理员配置列表 URL；裸行继承默认协议，显式行保留真实 HTTP/SOCKS5 和账密，拒绝附带请求路径/查询的代理 URI | 同上；条件请求只续列表有效期 |
| 按需 `provider_api` | 当前只有 SCDN 请求契约实现；每次取号是本次请求的候选批次 | 不后台取号、不预热、不持久缓存候选 |
| 已分配 `account_list + webshare_list_v1` | 固定读取 Webshare direct 列表；分页全部成功后才发布；不调用购买、替换或订阅变更 | 账号私有短缓存；配置、加密 Key 和读取限流持久化 |

公开目录保留原 11 个 ID，包括 `openproxylist-http` 和 `openproxylist-https`，分别使用 `https://api.openproxylist.xyz/http.txt`、`https://api.openproxylist.xyz/https.txt`。HTTP CONNECT 文件的 HTTPS 标签保留为厂商声明；不生成代理侧 TLS 传输或本池 PASS。

暂停、待协议、待账号契约、TLS 不兼容和退役入口仍在管理目录中说明原因，但不能自动启用。现阶段品赞、芝麻、太阳HTTP 未获得账号产品真实契约；抓取型网关未获得符合当前目标端到端 TLS 的供给契约。它们是设计和状态记录，**不是已完成真实提取的适配器**。

### 12.2 缓存、权限与任务

- 每源最多 1,000 候选、全局最多 4,096 候选、每源最多 1,024 个范围证明；同时受来源数、字节计费和硬 TTL 限制。计费包含结构体、切片容量、字符串、claims/证明等保守估算，不是进程 RSS 保证。
- 先按真实连接和凭证去重，再采用小比例前排与 reservoir 取样；容量很小时不将首条永久固定为唯一候选。缓存游标轮转，不虚增同一个连接的供给量。
- 来源侧项目允许列表与项目侧范围取交集，SQL 候选页在 `LIMIT` 前过滤；预热、读取账号和最终交付继续复核。账号源必须显式私有范围，创建停用、写入 Key 后再显式启用。
- 账号 Key 单独 AES-GCM 加密，部署主密钥独立派生、来源/secret-version 绑定 AAD；GET 只返回状态，PUT/DELETE 使用配置版本 CAS。Key 轮换或清除失效旧批次和证明，不清项目 CD。
- 刷新只下载/解析。`source_refresh` 任务的 Ready 是缓存保留数，probed 为零；`prewarm` 才执行按项目、同范围的真实验证，既不发放也不写 CD。
- 下载全局最多 2 个、手动刷新运行最多 2 个；所有预热任务共享一个后台探测准入槽。任务限额跨刷新/预热合计，队列/探测受总截止时间限制，取消释放 I/O 和预约。
- `next_fetch_at` 与 `next_refresh_at` 独立持久化。失败退避 30 秒至 15 分钟，供应商 Retry-After 最多 24 小时；同一 Webshare Key 的读取限流由 HMAC 身份共享并跨重启保留。这不是每日免费额度或购买配额模型。
- 最终交付在短事务中复核来源、项目、Key、版本、硬 TTL 和证明年龄；读写 fence 防止刷新/淘汰/较新失败与最终下发竞争。网络请求不持有该 fence。
- 304 保留批次和旧证明年龄，合并期间新增的失败/移除，不复活认证失败候选；内存命中使用 `cache_hit`，不伪称 HTTP 304。已用静态粘性绑定缺少当前列表证明时必须真实重验。

### 12.3 本地验证与未做项

本地验证报告见 [2026-11-05 本地验证记录](channel-local-verification-2026-11-05.md)，验收步骤见 [逐渠道验收手册](channel-acceptance-runbook-2026-11-04.md)。本批次未注册/购买供应商账号，没有真实商业 Key，没有新一轮公共代理批量验活，也没有改生产配置、提交、推送或部署。

上线前仍需要用户逐入口执行受控直连 fetch/parse 和小样本 Judge/目标 TLS 验收；每个渠道分别保存运行版本、网络出口、真实 HTTP 错误、覆盖窗口和样本分母。公开候选是否落盘仍按第 6 节的真实冷启动收益决定，不能从本地测试直接推导。

## 13. 2026-11-05 目录收敛与价值准入（当前口径）

前面“37 个协议模板/暂停产品目录”的实现不再作为用户渠道列表。
当前只提供 14 个值得维护的供应商品牌（12 个公共列表 + SCDN + Webshare），
每个品牌一行，协议、匿名/地区、API/HTML 或同名仓库路径进入 `variants`。
Free-Proxy-List 的主/SSL/US/SOCKS 子页不重复计渠道；roosterkid 不另建品牌，
仅作为 OpenProxyList 显式选择的同名仓库路径，不断言镜像内容相同，也无自动回退。

新渠道必须具有可维护契约与足够的预期收益，判断见
[逐项价值评估](channel-value-review-2026-11-05.md)。TheSpeedX、快代理、旧停更/歧义入口、
未知商业提取契约及 TLS 拦截网关不作为新渠道模板，也禁止通过旧创建 API 绕过准入。
原低层 parser/endpoint ID 保留诊断与既有定义兼容；不批量删除既有来源或合并账密/项目权限。
管理台实例按品牌分组只影响显示，真实 source_id、缓存、版本、证明、租约和 CD 保持独立。

值得构建是工程判断，不是已证明独立供给、每日免费、网络稳定或候选 PASS；
逐品牌及选中的协议配置仍需用户部署网络验收。公开/账号候选仍为有界内存缓存，
未因目录聚合新增持久代理库存。
