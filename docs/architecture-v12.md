# v12 架构说明

v12 的目标是先做“安全拆分”，不在一次版本里重写全部业务，避免再次影响同步、登录和 UI。

## 当前结构

```text
app.py                    # 兼容入口，保持 uvicorn app:app 不变
asset_app/
  legacy_app.py           # v11 完整业务，当前仍由这里提供 FastAPI app
  config.py               # 新配置模块，新功能应从这里读取 env
  database.py             # 新数据库连接边界
  routes/                 # 预留路由拆分目录
  services/               # 预留业务服务拆分目录
```

## 为什么不一次性全拆

原始 `app.py` 包含登录、资产、数据源、同步、备份、HTML、CSS、JS 等所有逻辑。一次性全拆风险较高，容易引入：

- 同步线程参数不一致
- 移动端样式污染 PC 端
- 数据库迁移遗漏
- 登录 Cookie 行为变化

v12 先把应用放进标准 Python package，并保留旧入口。后续每次只迁移一个模块，迁移后单独测试。

## v13 建议

优先迁移同步逻辑：

```text
asset_app/services/sync_service.py
asset_app/routes/sync.py
```

原因：同步是当前最核心、最容易出错的部分。
