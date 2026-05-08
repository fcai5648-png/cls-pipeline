#!/bin/bash
# 查询包装:直接转发参数给 cli.py
PROJECT_DIR="$HOME/projects/cls-pipeline"
PYTHON="$HOME/a_stock_ai_selector/.venv/bin/python"
exec "$PYTHON" "$PROJECT_DIR/src/cli.py" "$@"
