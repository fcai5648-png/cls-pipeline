#!/bin/bash
# 交互式配置 DeepSeek LLM key,落到 data/llm_config.json (chmod 600)
set -uo pipefail

CONFIG="$HOME/projects/cls-pipeline/data/llm_config.json"
mkdir -p "$(dirname "$CONFIG")"

if [ -f "$CONFIG" ]; then
    echo "已存在配置:$CONFIG"
    echo "当前 provider/model:$(grep -o '"provider":[^,]*' "$CONFIG" 2>/dev/null), $(grep -o '"model":[^,]*' "$CONFIG" 2>/dev/null)"
    read -p "覆盖?[y/N] " confirm
    [ "$confirm" != "y" ] && exit 0
fi

echo
echo "去 https://platform.deepseek.com 创建 API Key 后粘贴在下方(输入不会回显):"
read -s -p "DeepSeek API Key: " API_KEY
echo
if [ -z "$API_KEY" ]; then
    echo "ERROR: key 为空"
    exit 1
fi

read -p "每日 LLM 调用上限 [默认 500]: " LIMIT
LIMIT="${LIMIT:-500}"

cat > "$CONFIG" <<EOF
{
  "provider": "deepseek",
  "model": "deepseek-chat",
  "api_key": "$API_KEY",
  "base_url": "https://api.deepseek.com/v1",
  "daily_call_limit": $LIMIT
}
EOF
chmod 600 "$CONFIG"

echo
echo "✅ 写入:$CONFIG"
echo "   每日上限:$LIMIT 次"
echo
echo "重启 enrich worker 让新配置生效:"
echo "  launchctl kickstart -k gui/\$(id -u)/com.user.cls-pipeline-enrich"
