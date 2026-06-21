# asset-app v14

资产智能管控台。v14 合并了数据源健康监控和 PC 端表格体验优化：同步任务继续使用 v13 的持久化队列，PC 端资产列表更紧凑，勾选框直接放到地址行前面。

## v14 改动

- 数据源列表新增健康状态：正常、需检查、同步中、待同步、未定时、已停用。
- 数据源接口返回活跃任务数、失败任务数、健康说明，方便判断哪个源有问题。
- PC 端资产列表重新整理为更紧凑的表格化卡片。
- 勾选框移动到每条地址前方，删除勾选记录时不再占用上方空白区域。
- 修复 PC 端纯净 / 风控状态标签尺寸和对齐问题。
- 保持手机端极简资产查看模式。
- 保留 v13 的 `sync_jobs` 持久化同步队列。

## 运行

```bash
docker compose up -d --build
```

## 本地运行

```bash
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
```

## 数据文件

默认数据库路径建议使用：

```text
/app/data/asset_management.db
```

不要把下面这些提交到 GitHub：

```text
.env
*.db
*.sqlite
*.sqlite3
data/
backups/
export_backups/
__pycache__/
```

## 后续计划

下一步建议继续拆分：

```text
asset_app/services/sync_service.py
asset_app/routes/sync.py
asset_app/routes/assets.py
asset_app/routes/sources.py
asset_app/static/app.css
asset_app/static/app.js
asset_app/templates/index.html
```

## v15

- 增加旧网址别名分类修复，避免历史资产因为网址变更变成未知分类。
- 内置旧网址归属：盛世、万盛、嘉盛。
- 数据源修改网址时自动记录旧网址别名。
- 资产看板分页增加页码输入跳转。

## v17

- 批量同步现在会在“当前任务”面板中显示总体进度。
- 同步全部启用时会显示完成数量、成功数量、异常数量、每个数据源任务进度。
- 取消任务支持同时取消批量同步创建的多个任务。
- 单个同步任务和批量同步任务都保留进度条反馈，避免只显示顶部提示。

## v17 更新

- 同一标签下同一地址只显示一条记录，即使来自不同历史网址。
- 风控优先级高于纯净：同一地址同时出现风控和纯净时，保留风控状态。
- 重复记录会软删除进入回收站，不会直接物理删除。
- 导出 CSV 也会按相同规则去重。
