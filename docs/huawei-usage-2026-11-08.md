# Flexus L 套餐流量与 Bark（2026-11-08）

本轮将停用占位读取器替换为 AK/SK 签名的官方只读查询，并保留现有加密设置、每日调度、持久化退避和通知去重。云端额度优先于本地备用总量；告警周期采用套餐返回的重置周期开始和结束时间，同一资源、使用周期和阈值只产生一条告警。读取失败保留最后有效快照并标记过期，不把空结果保存成零用量。Bark 除 HTTP 状态外还检查应用层回执。

## 已核实与待验收

- 区域为 `ap-southeast-3`。L 实例 ID 为 `6ad8ba01a83ac34d79fbe3a2`；RMS 返回的流量包 ID 为 `564b02f8-dfe9-4ddd-9579-9cad92f5921e`。
- 官方 `ListAllResources(type=hcss.l-instance)` 查询成功。实例的 `properties.resources` 中包含类型 `huaweicloudinternal_cbc_freeresource`，其 `physical_resource_id` 即为流量包 ID。
- 官方 `ListFreeResourceUsages` 查询成功。临时凭证和线上已保存凭证均返回：剩余 `1023.543 GB`、总量 `1024 GB`、度量单位 `10`（GB）、上行流量，使用周期为 UTC `2026-10-21T12:00:00Z` 至 `2026-11-21T16:00:00Z`。
- 读取实现可只用 L 实例 ID 自动发现当前流量包；也可以填写流量包 ID 直查。实测真实云端响应与 API 文档的字段和重置语义一致。

## 验证与发布约束

本机 normal、race、vet 全仓检查通过。新增检查覆盖签名参考向量、实际周期选择、缺失/歧义/非法额度拒绝、权限失败保留历史、云端额度优先、周期去重、每日时刻相等不重复执行，以及 Bark 应用回执。

生产发布必须通过 GitHub `main` 推送触发 `docker` Actions，再拉取该提交的 GHCR 镜像并核对 revision/digest。服务器本机构建的试验镜像不用于正式发布。线上验收记录另存于 `docs/acceptance-2026-11-08/huawei-usage/`，仅实际成功后补充。
