#!/bin/bash
set -euo pipefail

PLIST_SRC="$HOME/projects/cls-pipeline/com.user.cls-pipeline-enrich.plist"
PLIST_DST="$HOME/Library/LaunchAgents/com.user.cls-pipeline-enrich.plist"
LABEL="com.user.cls-pipeline-enrich"

if [ ! -f "$PLIST_SRC" ]; then
    echo "ERROR: $PLIST_SRC not found"
    exit 1
fi

chmod +x "$HOME/projects/cls-pipeline/scripts/run_enrich.sh"

mkdir -p "$HOME/Library/LaunchAgents"
cp "$PLIST_SRC" "$PLIST_DST"

launchctl unload "$PLIST_DST" 2>/dev/null || true
launchctl load -w "$PLIST_DST"

echo "✅ installed: $PLIST_DST"
echo
echo "状态:"
launchctl list | grep "$LABEL" || echo "(not yet running)"
echo
echo "查看日志:"
echo "  tail -f ~/projects/cls-pipeline/logs/enrich.log"
