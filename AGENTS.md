# 发布流程

- 生产发布必须先提交并推送 GitHub `main`，等待该提交的 `docker` Actions 全部成功，再从 GHCR 拉取对应提交镜像。
- 使用 `ghcr.io/roiding/proxy-pool:sha-<短提交号>` 定位镜像，核验 OCI revision 与完整提交号一致，并以不可变 digest 部署。
- 不得用工作区打包、服务器 `docker build` 或本机编译产物替代 Actions 镜像发布。服务器只负责拉取、数据库副本预检、备份、切换和运行验收。
- 发布前保留旧镜像，备份 Compose、配置和一致数据库；验收版本、健康状态、认证、页面及业务数据保留情况。失败时回滚旧镜像。
- 临时 AK/SK、Bark 设备 Key、SSH 私钥及部署密钥不得写入 Git、构建上下文、测试记录或日志。凭证只通过现有加密设置保存。

# 当前服务

- 华为云服务路径为 `/opt/proxy-pool`，沿用 Compose 与 Traefik 网关配置。
- 改动不得覆盖会话内已经完成的固定渠道、统一开关与自动补足设置。
