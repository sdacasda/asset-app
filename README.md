# asset-app v23 稳定运维版

本版本以 **v18 稳定版** 为基础，只做低风险优化，不改资产同步核心逻辑。

## v23 新增

- 页面和系统维护区显示当前版本号 `v23`。
- 页面响应头加入 `X-Asset-App-Version`，方便确认容器实际版本。
- 首页、登录页、注册页禁用缓存，减少浏览器看到旧页面的问题。
- 前端脚本异常时会在页面底部显示错误提示，避免按钮点不动但不知道原因。
- 新增 `scripts/emergency_backup.sh`：一键紧急备份数据库和当前代码。
- 新增 `scripts/smoke_check.sh`：部署后检查版本、健康状态和数据库数量。
- 新增 `scripts/safe_update.sh`：以后更新包可以更安全地执行备份、解压、重建、检查。

## 部署

```bash
cd /root/recovered-asset-app
cp app/asset_app/legacy_app.py /root/legacy_app_before_v23.py
tar -xzvf asset-app-v23-safe-ops.tar.gz

cd /root/recovered-asset-app/app
chown -R 10001:10001 data export_backups
chmod -R u+rwX,g+rwX data export_backups

docker compose down
docker compose build --no-cache
docker compose up -d
./scripts/smoke_check.sh
```

## 重要说明

v23 不合并 v19-v21 的高风险前端改动。原因是 v21 曾导致页面按钮失效。后续如需继续恢复“同步状态变化展示”等功能，建议单独做小版本，每次只改一个点并验证。


## v23

基于 v22/v18 稳定代码，小步恢复 v21 的关键体验：版本号显示、同步完成提示清理、最近同步状态变化展示。
