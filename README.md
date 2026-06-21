# asset-app v10

资产智能管控台。当前版本在 v9 同步修复基础上，继续优化稳定性和日常操作体验。

## v10 更新

- 自动同步增加更清晰的状态字段：`last_scheduled_at`、`last_success_at`、`last_failed_at`、`last_error`。
- 定时同步不再把“已创建任务”误当成“已成功同步”。同步成功、失败会分开记录。
- 数据源列表显示资产数量、上次成功时间和失败原因。
- 删除数据源更安全：默认只删除数据源配置并保留资产；只有输入 `DELETE` 才会一起删除该数据源下的资产。
- 新增“同步全部启用”按钮，可批量创建所有启用数据源的同步任务。
- 地址复制按钮增加“已复制”反馈。
- PC 端资产列表和数据源列表做了轻量整理，手机端继续保持极简资产视图。

## 运行

```bash
docker compose up -d --build
```

## 环境变量

复制示例文件：

```bash
cp .env.example .env
```

常用配置：

```env
DB_PATH=/app/data/asset_management.db
COOKIE_SECURE=false
REGISTRATION_ENABLED=true
REQUEST_TIMEOUT_SECONDS=30
MAX_PAGES=2000
```

如果使用 HTTPS，可以把 `COOKIE_SECURE=true`。

## 数据库

SQLite 数据库不进入 Git：

```text
data/asset_management.db
```

迁移服务器时，把数据库复制到新服务器的 `data/asset_management.db`，然后修复权限：

```bash
mkdir -p data export_backups
chown -R 10001:10001 data export_backups
chmod -R u+rwX,g+rwX data export_backups
```

## 备份

建议定期备份数据库：

```bash
mkdir -p /root/asset-db-backups
cp data/asset_management.db /root/asset-db-backups/asset_management_$(date +%F_%H%M).db
```

## Git 忽略

`.env`、数据库、备份目录、缓存目录都不应上传到 GitHub。
