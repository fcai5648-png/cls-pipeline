#!/bin/bash
# launchd 入口:启动 cls-pipeline 守护进程
# 关键:launchd 默认 PATH 几乎是空的,这里强制设全
set -uo pipefail

export HOME="${HOME:-/Users/jintianyouyu}"
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

PROJECT_DIR="$HOME/projects/cls-pipeline"
PYTHON="$HOME/a_stock_ai_selector/.venv/bin/python"

cd "$PROJECT_DIR"
mkdir -p logs data

# urllib3 ssl 警告比较烦,屏蔽掉(不影响 https 请求)
export PYTHONWARNINGS="ignore::Warning"

exec "$PYTHON" "$PROJECT_DIR/src/daemon.py"
