#!/bin/bash
# 安装 launchd job — 装上后立刻开跑,且开机自启 + 挂掉自重启
set -euo pipefail

PLIST_SRC="$HOME/projects/cls-pipeline/com.user.cls-pipeline.plist"
PLIST_DST="$HOME/Library/LaunchAgents/com.user.cls-pipeline.plist"
LABEL="com.user.cls-pipeline"

if [ ! -f "$PLIST_SRC" ]; then
    echo "ERROR: $PLIST_SRC not found"
    exit 1
fi

# 让脚本可执行
chmod +x "$HOME/projects/cls-pipeline/scripts/run_daemon.sh"
chmod +x "$HOME/projects/cls-pipeline/scripts/query.sh"

mkdir -p "$HOME/Library/LaunchAgents"
cp "$PLIST_SRC" "$PLIST_DST"

# 已经装过就先 unload(忽略错误)
launchctl unload "$PLIST_DST" 2>/dev/null || true

launchctl load -w "$PLIST_DST"

echo "✅ installed: $PLIST_DST"
echo
echo "状态:"
launchctl list | grep "$LABEL" || echo "(not yet running — check logs)"
echo
echo "查看日志:"
echo "  tail -f ~/projects/cls-pipeline/logs/daemon.log"
echo "查看状态:"
echo "  bash ~/projects/cls-pipeline/scripts/query.sh stats"
