# services

后续把同步、备份、数据源测试等业务逻辑从 `asset_app/legacy_app.py` 迁移到这里。

计划拆分：

- `sync_service.py`：手动同步、自动同步、失败重试、任务状态。
- `backup_service.py`：自动备份、上传合并、下载备份。
- `source_service.py`：数据源测试、数据源健康检查。
- `asset_service.py`：资产筛选、删除、回收站、复制列表。
