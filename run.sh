#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
echo "[1/2] run pipeline (generate demo data + 5 stages)..."
python -m src.pipeline --config config/pipeline.json
echo "[2/2] refresh dashboard data (dashboard/data.js)..."
python dashboard/build_data.py || echo "[WARN] dashboard refresh failed; dashboard.html shows stale data"
echo "[OK] done, see run/latest.json; open dashboard/dashboard.html"
