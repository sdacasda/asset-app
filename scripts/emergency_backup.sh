#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
out="/root/asset-app-emergency-$(date +%F-%H%M%S)"
mkdir -p "$out"
cp -a asset_management.db "$out/asset_management.root.db" 2>/dev/null || true
cp -a data/asset_management.db "$out/asset_management.data.db" 2>/dev/null || true
cp -a asset_app/legacy_app.py "$out/legacy_app.py" 2>/dev/null || true
tar -czf "$out.tar.gz" -C "$(dirname "$out")" "$(basename "$out")"
echo "created: $out.tar.gz"
