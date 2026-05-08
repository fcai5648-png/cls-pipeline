#!/bin/bash
# launchd 入口:启动 cls-pipeline enrichment worker
set -uo pipefail

export HOME="${HOME:-/Users/jintianyouyu}"
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

PROJECT_DIR="$HOME/projects/cls-pipeline"
PYTHON="$HOME/a_stock_ai_selector/.venv/bin/python"

cd "$PROJECT_DIR"
mkdir -p logs

export PYTHONWARNINGS="ignore::Warning"

exec "$PYTHON" "$PROJECT_DIR/src/enrich_worker.py"
