# asset-app v13

本版本在 v12 拆分结构基础上增加同步任务持久化与 PC 端状态标签修复。

## 重点

- `sync_jobs`：保存定时同步与失败重试任务。
- 调度器每分钟扫描到期任务并启动。
- 容器重启后会把中断中的 `running/claimed` 任务恢复为 `queued`。
- Dockerfile 已复制 `asset_app/`，支持拆分后的项目结构。
