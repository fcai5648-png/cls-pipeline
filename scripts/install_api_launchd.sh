#!/bin/bash
# 安装 cls-pipeline-api launchd job(独立于 daemon)
set -euo pipefail

PLIST_SRC="$HOME/projects/cls-pipeline/com.user.cls-pipeline-api.plist"
PLIST_DST="$HOME/Library/LaunchAgents/com.user.cls-pipeline-api.plist"
LABEL="com.user.cls-pipeline-api"

if [ ! -f "$PLIST_SRC" ]; then
    echo "ERROR: $PLIST_SRC not found"
    exit 1
fi

chmod +x "$HOME/projects/cls-pipeline/scripts/run_api.sh"

mkdir -p "$HOME/Library/LaunchAgents"
cp "$PLIST_SRC" "$PLIST_DST"

launchctl unload "$PLIST_DST" 2>/dev/null || true
launchctl load -w "$PLIST_DST"

echo "✅ installed: $PLIST_DST"
echo
echo "状态:"
launchctl list | grep "$LABEL" || echo "(not yet running)"
echo
echo "测试:"
echo "  curl -s http://127.0.0.1:8787/health | python3 -m json.tool"
echo "  curl -s 'http://127.0.0.1:8787/telegraph/latest?n=3' | python3 -m json.tool"
echo "  open http://127.0.0.1:8787/docs"
