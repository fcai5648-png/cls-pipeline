#!/bin/bash
# 卸载 launchd job(数据库保留)
set -uo pipefail

PLIST_DST="$HOME/Library/LaunchAgents/com.user.cls-pipeline.plist"

if [ -f "$PLIST_DST" ]; then
    launchctl unload "$PLIST_DST" 2>/dev/null || true
    rm -f "$PLIST_DST"
    echo "✅ uninstalled launchd job"
else
    echo "(no launchd job found)"
fi

echo "数据库未删除:~/projects/cls-pipeline/data/cls.db"
