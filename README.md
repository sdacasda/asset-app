# asset-app v6

本版修复内容：

- 修复 v5 中 PC 端 CSS 被错误插入导致桌面布局异常的问题。
- 手机端保留极简资产看板：数据源、状态筛选、地址、状态、复制按钮、分页。
- 每条地址旁新增“一键复制”按钮，方便复制到地址库查验。
- PC 端恢复左侧固定导航和完整资产信息展示。
- 手机端侧边栏默认隐藏，点击菜单打开。

## 部署

```bash
cd /root/recovered-asset-app
cp app/app.py /root/app_before_v6.py

tar -xzvf asset-app-v6-copy-mobile-pcfix.tar.gz

cd /root/recovered-asset-app/app
chown -R 10001:10001 data export_backups
chmod -R u+rwX,g+rwX data export_backups

docker compose down
docker compose up -d --build
docker logs -f asset-app
```

看到 `Application startup complete` 后刷新页面。手机建议用无痕窗口或清缓存测试。

## v8 display update

- Source dropdown and selected source label no longer display URL.
- Source options display as `Header｜Label`, for example `创世｜管城`.
- Group headers are still shown in the dropdown, and the selected value also keeps the header to avoid choosing the wrong source.
