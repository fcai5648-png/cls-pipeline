#!/bin/bash
# ============================================================
# cls-pipeline 一键引导(新机器迁移用)
#
# 用法:在新 Mac 上,先把 cls-pipeline 整个目录拷贝/clone 到 ~/projects/cls-pipeline,
# 然后运行:
#   bash ~/projects/cls-pipeline/scripts/bootstrap.sh
#
# 脚本会:
#   1. 检查 a_stock_ai_selector venv 是否存在(cls-pipeline 复用它)
#   2. 检查 5 个核心 Python 包(akshare/openai/fastapi/uvicorn/futu-api),缺则装
#   3. 检查富途 OpenD / DeepSeek key / 词典文件 等 secrets / 配置
#   4. 安装 6 个 launchd job(daemon/api/enrich/alerts/signals/quotes)
#   5. 跑 smoke test 确认服务正常
#
# 不会:
#   - 不会 git push / pull
#   - 不会动 a_stock_ai_selector 项目
#   - 不会自动填 secrets — 需要你手动配
# ============================================================
set -uo pipefail

PROJECT_DIR="$HOME/projects/cls-pipeline"
ASTOCK_DIR="$HOME/a_stock_ai_selector"
ASTOCK_VENV="$ASTOCK_DIR/.venv"
PYTHON="$ASTOCK_VENV/bin/python"
PIP="$ASTOCK_VENV/bin/pip"

ok()    { echo "✅ $*"; }
warn()  { echo "⚠️  $*"; }
err()   { echo "❌ $*"; }
header(){ echo; echo "=== $* ==="; }

cd "$PROJECT_DIR" 2>/dev/null || { err "$PROJECT_DIR 不存在 — 先把项目拷贝过来"; exit 1; }

# -----------------------------------------------------------
header "1. 检查 a_stock_ai_selector venv(cls-pipeline 复用它)"
# -----------------------------------------------------------
if [ ! -x "$PYTHON" ]; then
    err "$PYTHON 不存在"
    cat <<EOF
   cls-pipeline 复用 a_stock_ai_selector 的 venv(已装 akshare 等基础包)。
   先在这台机器准备好 a_stock_ai_selector:
     1. 把源机器的 ~/a_stock_ai_selector/ 整个目录拷贝过来(或 git clone)
        (可以排除 .venv、data/cls.db 之类大文件)
     2. cd ~/a_stock_ai_selector
     3. python3.9 -m venv .venv
     4. .venv/bin/pip install -r requirements.txt
   完成后再跑本脚本。
EOF
    exit 1
fi
ok "venv: $PYTHON"

# -----------------------------------------------------------
header "2. 检查 cls-pipeline 必备 Python 包"
# -----------------------------------------------------------
MISSING=$("$PYTHON" - <<'PY'
import importlib.util
needed = {
    "akshare":  "akshare",
    "openai":   "openai",
    "fastapi":  "fastapi",
    "uvicorn":  "uvicorn[standard]",
    "futu":     "futu-api",
}
miss = [pip_name for mod, pip_name in needed.items()
        if importlib.util.find_spec(mod) is None]
print(" ".join(miss))
PY
)
if [ -n "$MISSING" ]; then
    warn "缺少包: $MISSING"
    read -p "    自动装到 $ASTOCK_VENV?[Y/n] " ans
    if [ "${ans:-Y}" != "n" ] && [ "${ans:-Y}" != "N" ]; then
        "$PIP" install $MISSING || { err "pip install 失败"; exit 1; }
        ok "依赖装好"
    else
        warn "跳过自动装 — 你需要手动 $PIP install $MISSING"
    fi
else
    ok "所有依赖齐全"
fi

# -----------------------------------------------------------
header "3. 创建必要目录"
# -----------------------------------------------------------
mkdir -p "$PROJECT_DIR/data" "$PROJECT_DIR/logs"
ok "data/ logs/ 就绪"

# -----------------------------------------------------------
header "4. 检查词典(必须从源机器拷过来)"
# -----------------------------------------------------------
DICT_DIR="$PROJECT_DIR/data/dict"
mkdir -p "$DICT_DIR"
DICT_OK=true
for f in sectors.json companies.json orgs.json event_types.json sentiment.json watchlist.json quote_targets.json; do
    if [ ! -f "$DICT_DIR/$f" ]; then
        err "缺 $DICT_DIR/$f"
        DICT_OK=false
    fi
done
if ! $DICT_OK; then
    cat <<EOF
   词典文件在源机器的 ~/projects/cls-pipeline/data/dict/ 里。
   从源机器跑(把 newmac.local 替换成新机器的 host):
     rsync -av ~/projects/cls-pipeline/data/dict/ newmac.local:~/projects/cls-pipeline/data/dict/
   或者 scp 单文件,或者把 data/dict 目录打包传过来。
EOF
    exit 1
fi
ok "词典齐全"

# -----------------------------------------------------------
header "5. 检查 DeepSeek LLM 配置(可选)"
# -----------------------------------------------------------
LLM_CFG="$PROJECT_DIR/data/llm_config.json"
if [ -f "$LLM_CFG" ]; then
    KEY_LEN=$("$PYTHON" -c "import json; print(len(json.load(open('$LLM_CFG')).get('api_key','')))" 2>/dev/null || echo 0)
    if [ "$KEY_LEN" -gt 10 ]; then
        ok "LLM 已配(api_key 长度 $KEY_LEN)"
    else
        warn "$LLM_CFG 存在但 api_key 为空"
    fi
else
    warn "$LLM_CFG 不存在 — enrich worker 会退回纯规则模式(不影响主流程)"
    cat <<EOF
   想启用 LLM 二次精分类(规则版判错的走 DeepSeek):
     bash $PROJECT_DIR/scripts/setup_llm.sh
   月成本约 \$1,DeepSeek key 在 https://platform.deepseek.com/api_keys 申请
EOF
fi

# -----------------------------------------------------------
header "6. 检查富途 OpenD"
# -----------------------------------------------------------
if lsof -nP -iTCP:11111 -sTCP:LISTEN >/dev/null 2>&1; then
    ok "OpenD 在跑(127.0.0.1:11111)"
else
    warn "OpenD 未运行 — quote_worker 拉不到行情(其他 worker 不受影响)"
    cat <<EOF
   下载:https://www.futunn.com/download/openAPI(免费)
   装好后启动 + 登录富途账号(没开户也能拿 Level 1 港美股实时行情)
EOF
fi

# -----------------------------------------------------------
header "7. 检查告警配置"
# -----------------------------------------------------------
ALERT_CFG="$PROJECT_DIR/data/alert_config.json"
if [ ! -f "$ALERT_CFG" ]; then
    warn "$ALERT_CFG 不存在 — 告警 worker 会用默认(仅 osascript 本地通知)"
    cat <<EOF
   想接 Bark / 微信 Server酱 / 飞书机器人:
     编辑 $ALERT_CFG(可参考源机器的同名文件)
     改完跑 launchctl kickstart -k gui/\$(id -u)/com.user.cls-pipeline-alerts
EOF
else
    ok "告警配置存在"
fi

# -----------------------------------------------------------
header "8. 安装 6 个 launchd job"
# -----------------------------------------------------------
read -p "现在装 6 个 launchd job?[Y/n] " ans
if [ "${ans:-Y}" = "n" ] || [ "${ans:-Y}" = "N" ]; then
    warn "跳过 — 后续手动跑 scripts/install_*_launchd.sh"
else
    chmod +x "$PROJECT_DIR"/scripts/*.sh
    for s in install_launchd.sh install_api_launchd.sh install_enrich_launchd.sh \
             install_alerts_launchd.sh install_signals_launchd.sh install_quotes_launchd.sh; do
        echo "→ $s"
        bash "$PROJECT_DIR/scripts/$s" 2>&1 | grep -E "✅|状态:|^[0-9]"
    done
    ok "6 个 job 已装"
fi

# -----------------------------------------------------------
header "9. Smoke test"
# -----------------------------------------------------------
sleep 5
echo "launchctl 状态:"
launchctl list | grep cls-pipeline | sort

# 等 daemon 拉到第一条电报
echo
echo "等 daemon 抓第一条电报(最多 60s)..."
for i in $(seq 1 30); do
    rows=$("$PYTHON" -c "
import sqlite3, sys
try:
    c = sqlite3.connect('$PROJECT_DIR/data/cls.db')
    print(c.execute('select count(*) from telegraph').fetchone()[0])
except Exception:
    print(0)
" 2>/dev/null)
    if [ "$rows" -gt 0 ]; then
        ok "已抓 $rows 条电报"
        break
    fi
    sleep 2
done

# 等 api 起来
echo
for i in $(seq 1 15); do
    if curl -sf http://127.0.0.1:8787/health >/dev/null 2>&1; then
        ok "API server 通"
        break
    fi
    sleep 2
done

# -----------------------------------------------------------
header "✅ 引导完成"
# -----------------------------------------------------------
cat <<EOF
浏览器打开:  http://127.0.0.1:8787/
查看日志:    tail -f $PROJECT_DIR/logs/daemon.log
看抓取统计:  curl http://127.0.0.1:8787/stats

后续:
  - 没配 DeepSeek key:bash $PROJECT_DIR/scripts/setup_llm.sh
  - 没装 OpenD:下载 https://www.futunn.com/download/openAPI
  - 想接微信 / 飞书告警:编辑 $PROJECT_DIR/data/alert_config.json
EOF
