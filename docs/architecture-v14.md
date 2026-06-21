# v14 说明

v14 目标是把 v14 的数据源健康监控和 v15 的 PC 端表格体验合并到一个稳定版本里。

## 数据源健康状态

`/api/sources` 现在会返回：

- `active_job_count`
- `failed_job_count`
- `health_status`
- `health_label`
- `health_detail`

前端数据源列表会基于这些字段显示状态。

## PC 资产列表

PC 端资产列表调整为：

```text
[勾选框] 地址 | 复制 | 状态
        状态 / 关键词 / 更新时间 / 分类 / 当前网址
```

手机端仍然隐藏勾选框和详细字段，只显示地址、复制和状态。
