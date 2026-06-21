#!/usr/bin/env bash
set -euo pipefail
PKG="${1:-}"
if [ -z "$PKG" ]; then
  echo "usage: ./scripts/safe_update.sh /path/to/asset-app-vXX.tar.gz"
  exit 1
fi
ROOT="/root/recovered-asset-app"
APP="$ROOT/app"
cd "$APP"
./scripts/emergency_backup.sh || true
cp -a asset_app/legacy_app.py "/root/legacy_app_before_update_$(date +%F-%H%M%S).py" 2>/dev/null || true
cd "$ROOT"
tar -xzvf "$PKG"
cd "$APP"
chown -R 10001:10001 data export_backups
chmod -R u+rwX,g+rwX data export_backups
docker compose down
docker compose build --no-cache
docker compose up -d
sleep 2
./scripts/smoke_check.sh
