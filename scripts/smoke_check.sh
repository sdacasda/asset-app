#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
echo "[1/4] docker status"
docker ps --filter name=asset-app --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
echo "[2/4] health"
curl -fsS http://127.0.0.1:8088/health || true
echo
echo "[3/4] app version inside container"
docker exec -i asset-app python - <<'PY'
import asset_app.legacy_app as m
print('APP_VERSION=', getattr(m, 'APP_VERSION', 'unknown'))
print('FILE=', m.__file__)
PY
echo "[4/4] database counts"
docker exec -i asset-app python - <<'PY'
import os, sqlite3
db=os.getenv('DB_PATH','/app/data/asset_management.db')
conn=sqlite3.connect(db)
cur=conn.cursor()
for table in ['asset_records','source_configs','users']:
    try: print(table, cur.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0])
    except Exception as e: print(table, 'ERR', e)
PY
