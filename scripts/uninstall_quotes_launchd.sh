#!/bin/bash
set -uo pipefail
PLIST_DST="$HOME/Library/LaunchAgents/com.user.cls-pipeline-quotes.plist"
if [ -f "$PLIST_DST" ]; then
    launchctl unload "$PLIST_DST" 2>/dev/null || true
    rm -f "$PLIST_DST"
    echo "✅ uninstalled quotes launchd job"
else
    echo "(no quotes launchd job found)"
fi
