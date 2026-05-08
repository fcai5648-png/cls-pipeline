#!/bin/bash
# 卸载 cls-pipeline-api launchd job
set -uo pipefail

PLIST_DST="$HOME/Library/LaunchAgents/com.user.cls-pipeline-api.plist"

if [ -f "$PLIST_DST" ]; then
    launchctl unload "$PLIST_DST" 2>/dev/null || true
    rm -f "$PLIST_DST"
    echo "✅ uninstalled api launchd job"
else
    echo "(no api launchd job found)"
fi
