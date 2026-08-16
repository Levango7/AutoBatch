#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
echo "[1/1] run pipeline (generate demo data + 5 stages)..."
python -m src.pipeline --config config/pipeline.json
echo "[OK] done, see run/latest.json"
