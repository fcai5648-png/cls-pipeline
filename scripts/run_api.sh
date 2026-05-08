#!/bin/bash
# launchd 入口:启动 cls-pipeline HTTP API server
set -uo pipefail

export HOME="${HOME:-/Users/jintianyouyu}"
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

PROJECT_DIR="$HOME/projects/cls-pipeline"
PYTHON="$HOME/a_stock_ai_selector/.venv/bin/python"

cd "$PROJECT_DIR"
mkdir -p logs

export PYTHONWARNINGS="ignore::Warning"

# 仅本地绑定;远程访问请走 nginx / Tailscale,不要直接改 --host
exec "$PYTHON" -m uvicorn \
    --app-dir src \
    api:app \
    --host 127.0.0.1 \
    --port "${CLS_API_PORT:-8787}" \
    --log-level info
