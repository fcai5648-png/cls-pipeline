#!/bin/bash
set -uo pipefail

PLIST_DST="$HOME/Library/LaunchAgents/com.user.cls-pipeline-enrich.plist"

if [ -f "$PLIST_DST" ]; then
    launchctl unload "$PLIST_DST" 2>/dev/null || true
    rm -f "$PLIST_DST"
    echo "✅ uninstalled enrich launchd job"
else
    echo "(no enrich launchd job found)"
fi
