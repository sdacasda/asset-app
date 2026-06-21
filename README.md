# asset-app v11 稳定增强版

FastAPI 资产管理工具，支持数据源同步、资产看板、移动端极简查看、数据源归类、自动备份和回收站。

## v11 新增

- 自动同步失败后自动重试，默认最多 3 次，每次间隔 5 分钟。
- 数据源管理增加“测试连接”，可快速检查网址、Cookie 和首页解析结果。
- 手机端默认只看“纯净可用”资产，主界面保持地址 + 状态 + 复制按钮。
- PC 端资产列表优化为更紧凑的表格化布局。
- 增加“复制纯净地址”，可一键复制当前筛选下的全部纯净地址。
- 增加数据库自动备份，默认每 24 小时备份一次，保留最近 7 个备份。
- 删除资产改为进入回收站，可恢复；清空回收站才会永久删除。

## 部署

```bash
docker compose up -d --build
```

## 环境变量

复制 `.env.example` 为 `.env` 后按需修改。

```bash
cp .env.example .env
```

重要配置：

```env
DB_PATH=/app/data/asset_management.db
BACKUP_DIR=/app/export_backups
COOKIE_SECURE=false
AUTO_BACKUP_ENABLED=true
AUTO_BACKUP_INTERVAL_HOURS=24
AUTO_BACKUP_RETENTION_COUNT=7
SYNC_RETRY_ENABLED=true
SYNC_RETRY_MAX_ATTEMPTS=3
SYNC_RETRY_DELAY_SECONDS=300
```

## 数据文件

SQLite 数据库不进入 Git：

```text
data/asset_management.db
```

## 备份

手动创建备份可在后台“备份恢复”里操作。v11 也会自动定时备份数据库和核心代码文件。

## GitHub 上传

```bash
git add .
git commit -m "Improve stability and workflow"
git push
```
