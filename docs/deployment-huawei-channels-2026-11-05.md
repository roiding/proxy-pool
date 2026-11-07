# 华为云渠道版部署记录（2026-11-05）

> 本文记录当天首次渠道版部署，非当前版本。最新黑名单/缓存复用版及云端复测见 [最新部署记录](deployment-huawei-blacklist-2026-11-05.md)。

## 结果

已升级用户已有的新加坡 Flexus L 服务，没有另起一套网关或清空数据。

- 管理入口：<https://proxy-pool.522401.xyz/admin>，继续使用原管理密码。
- 版本：`channels-20261105-41dad296e55c`。
- 服务器：Debian 12 / Linux amd64，报告的物理内存总量约 800 MiB（不是当前剩余内存）。
- 沿用 `/opt/proxy-pool/docker-compose.yml`、原 Traefik、`web_network` 和 Cloudflare 来源限制。
- 应用端口仍只映射到 `127.0.0.1:8080`，没有开放新的公网端口。
- 14 个品牌模板：12 个公共列表品牌及 SCDN、Webshare；HTTP/SOCKS5/子列表是同一品牌下的配置选项。
- 原配置和 GeoIP 文件 SHA-256 不变；来源、资源、租约、项目、API Key 行数没有重置。
- 没有自动创建或启用渠道，没有调用 SCDN，也没有填写或调用 Webshare Key。

部署成功不等于所有渠道的代理验活成功。

## 怎么在服务器上测试

### 网页操作

1. 登录管理页，在「渠道 → 新建渠道」选择品牌和一个配置选项，创建停用渠道。
2. 选择该实例，点击「启用渠道」，再点击「刷新列表（不验活）」。
3. 到「任务」填写这个实例的 `source_id`，创建预热任务，查看真实通过/失败数量。

**刷新只是下载；预热才验活。** 第一次建议目标数量为 1，其他限制保持默认。不要一次开启全部渠道。

### 单渠道命令

SSH 登录服务器后可直接运行，不需要本地安装 Go：

```sh
docker exec proxy-pool /usr/local/bin/sitecheck -list

docker exec proxy-pool /usr/local/bin/sitecheck \
  -site openproxylist-http -max-pages 1 \
  -probe 5 -probe-mode tls -probe-target example.com:443 \
  -probe-timeout 6 -timeout 90 -seed 20261105 -json
```

`-site` 选择品牌内的具体配置，不是额外品牌。命令只做有界抓取和验活，不写库存、租约或项目 CD。抓取/验活未达标时非零退出是正常报告，不表示服务崩溃。SCDN 必须单独显式调用；不要盲重试可能消耗额度的取号接口。

## 实际验证

本轮在目标 Linux 的隔离构建容器中完成：

- `go test -p 1 -count=1 ./...`：通过。
- `go vet -p 1 ./...`：通过。
- `proxy-pool`、`sitecheck`：CGO 开启，均为静态 Linux x86-64 ELF，版本和 SHA-256 已核对。
- 用 SQLite 在线备份得到数据库副本，在**无网络、无发布端口**的临时容器中启动新版并完成迁移预演：通过，副本及容器已清理。
- 正式切换前停旧容器，完整备份配置、数据目录及存在的 WAL/SHM；逐文件 SHA-256 比较、备份 `quick_check` 与外键检查：通过。
- 新版数据库完整性和关键持久行数检查：通过。
- 外网 HTTPS 管理页、健康检查：HTTP 200；未认证管理 API：401；认证查询显示新版版本。
- 公网 Judge：使用正常 TLS、生产探测 User-Agent，随机 nonce 和 HMAC 均验证通过，出口为该服务器公网 IP。
- 最终容器：`healthy`、重启数 0、应用 ERROR 日志数 0。

默认 Python urllib User-Agent 曾被公网边界返回 403；curl、管理页以及生产的 `proxy-pool-probe/1` User-Agent 均通过。未因此关闭 TLS 校验或放宽网关来源限制。

首轮默认 C 编译超过隔离构建容器的 256 MiB 上限；旧服务未受影响。最终构建使用 Go 1.25.14、单编译并发，以及：

```text
CGO_CFLAGS=-O1 -g0 --param ggc-min-expand=5 --param ggc-min-heapsize=16384
```

编译工具容器、专用工具镜像和缓存已清理。原主机运行环境没有新增常驻编译服务。运行时仍保留原 `GOMEMLIMIT=64MiB`；最终一次 Docker 内存观测约 10.27 MiB，**不是负载下 RSS 上限或性能承诺**。本轮没有在云端重跑全仓 race；此前本机 race 结果见本地验证记录。

## 云端 OpenProxyList 首轮结果

2026-11-05 11:41（北京时间），华为云出口，HTTP 列表配置：

| 项目 | 结果 |
| --- | --- |
| 下载 / 解析 | 成功，8,540 条，0 条解析拒绝 |
| TLS 目标 | `example.com:443`，正常证书校验 |
| 固定 seed、唯一样本 | 5 条 |
| 通过 / 失败 | **0 / 5** |
| 失败分类 | 4 条超时，1 条目标 TLS 握手失败 |

这证明该配置在云端可以抓取，但不能把这批代理称为可用供给；小样本也不能证明全渠道永久不可用。其余品牌本轮未在华为云重新验活，SCDN/Webshare 未调用。

脱敏结果：

- `acceptance-2026-11-05/huawei-openproxylist-results.json`
- `acceptance-2026-11-05/huawei-deployment-receipt.json`

## 发行包与备份

- 发行目录：`/opt/proxy-pool/releases/channels-20261105-41dad296e55c`
- 当前镜像：`proxy-pool:channels-20261105-41dad296e55c`
- 旧镜像保留标签：`proxy-pool:before-channels-20261105-41dad296e55c`
- 一致性备份：`/opt/proxy-pool/backups/channels-20261105-41dad296e55c`，备份父目录仅 root 可访问。
- 原配置：`/opt/proxy-pool/config.json`
- 原数据：`/opt/proxy-pool/data`

发行包为当前未提交工作区的白名单源代码快照，完整内容 SHA-256 为：

```text
41dad296e55ce758144277cd2630515e74f68b1a237a2ddb005f7d076949e23a
```

基线为 `b248bb38367a68bea1552eef195860864191d74d`。源代码归档未包含本机配置、数据库、SSH Key 或 Git 历史；没有提交或推送公开仓库。完整 manifest、二进制校验和、构建与验证脚本保存在服务器发行目录中。

回滚前先停服务、备份当前数据库并确认是否已经产生新租约/CD/账号配置。**不得直接恢复旧数据库抹掉升级后的状态。** 必须验证旧代码与当前 schema 的兼容性，或在明确批准、状态已协调后恢复一致的代码/配置/数据库组合。保留失败版本数据库用于诊断。
