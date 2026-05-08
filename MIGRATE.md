# 在另一台 Mac 上部署 cls-pipeline

> 把当前机器的 cls-pipeline 完整搬到新 Mac。预计耗时 30 分钟(不含装 OpenD)。

## 前置:必须先装 a_stock_ai_selector(被 cls-pipeline 复用)

cls-pipeline 没有自己的 venv,而是复用 `~/a_stock_ai_selector/.venv`。
所以新 Mac 上必须先有 a_stock_ai_selector,且其 venv 已激活。

```bash
# 在新 Mac 上
mkdir -p ~/a_stock_ai_selector
# 把源机器的 a_stock_ai_selector 拷过来(也可以从你的 git repo clone)
rsync -av --exclude='.venv' --exclude='data/cls.db*' --exclude='logs/' --exclude='*.pyc' \
  oldmac.local:~/a_stock_ai_selector/ ~/a_stock_ai_selector/

cd ~/a_stock_ai_selector
python3.9 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## 1. 拷贝 cls-pipeline(不带 venv / db / logs)

**方式 A:rsync(直接传)**
```bash
# 在源 Mac 跑(替换 newmac.local 为新机器 host 或 IP)
rsync -av \
  --exclude='.venv' \
  --exclude='logs/*.log' --exclude='logs/*.log.*' \
  --exclude='data/cls.db' --exclude='data/cls.db-wal' --exclude='data/cls.db-shm' \
  --exclude='__pycache__' --exclude='*.pyc' \
  ~/projects/cls-pipeline/ newmac.local:~/projects/cls-pipeline/
```

**方式 B:git**
1. 源 Mac 上把 cls-pipeline 推到 GitHub(私有 repo)
2. 新 Mac:`mkdir -p ~/projects && cd ~/projects && git clone <repo> cls-pipeline`
3. 词典 + secrets 不在 git(被 .gitignore),仍需手动传(下面)

## 2. 传 secrets / 词典(必须手动,不入 git)

```bash
# 在源 Mac 跑
scp ~/projects/cls-pipeline/data/llm_config.json     newmac.local:~/projects/cls-pipeline/data/
scp ~/projects/cls-pipeline/data/alert_config.json   newmac.local:~/projects/cls-pipeline/data/
rsync -av ~/projects/cls-pipeline/data/dict/         newmac.local:~/projects/cls-pipeline/data/dict/
```

> 如果你想新机器从零建词典,可以跳过 dict 拷贝 — 但那 4 个 JSON 词典是手工调过的,建议直接复用。

## 3. 装富途 OpenD(港美股行情用,可选但推荐)

下载:https://www.futunn.com/download/openAPI

装完启动,登录你的富途账号(**没开户也能拿 Level 1 港美股实时行情免费**)。

不装的话:`com.user.cls-pipeline-quotes` worker 会一直报错,但其他 5 个 worker 正常跑。

## 4. 一键引导

```bash
bash ~/projects/cls-pipeline/scripts/bootstrap.sh
```

脚本会自动:
1. 检查 a_stock_ai_selector venv 存在
2. 检查 + 装 5 个核心 Python 包(akshare / openai / fastapi / uvicorn / futu-api)
3. 检查 OpenD / DeepSeek key / 词典 / 告警配置
4. 装 6 个 launchd job(daemon / api / enrich / alerts / signals / quotes)
5. 跑 smoke test

任何缺失项会**告诉你具体怎么补**(不会偷偷跳过)。

## 5. 验证

```bash
launchctl list | grep cls-pipeline      # 应该看到 6 个进程
open http://127.0.0.1:8787/             # viewer
curl http://127.0.0.1:8787/stats        # 统计
tail -f ~/projects/cls-pipeline/logs/daemon.log
```

5-30 分钟内,viewer 会逐渐有电报,有了 30 分钟+数据后信号引擎开始产出板块情绪 Top。

## 不要做

- ❌ 不要把源 Mac 的 `data/cls.db` 拷过来 — 数据库会自己重建
- ❌ 不要拷 `.venv/` — 平台特异,新机器自己装
- ❌ 不要把 `data/llm_config.json` 入 git — 这是 DeepSeek API key

## 故障排查

| 现象 | 原因 + 解决 |
|---|---|
| `launchctl list` 没看到 6 个 job | 重跑 `bash scripts/install_*_launchd.sh` |
| daemon 一直报 `ImportError: akshare` | venv 没装 akshare,跑 `~/a_stock_ai_selector/.venv/bin/pip install akshare` |
| api 启动失败 `Unable to evaluate type annotation 'str | None'` | venv 是 Python 3.9,改 src/api.py 把 `str \| None` 换成 `Optional[str]` |
| `quote_worker` 一直报 `Connection refused` | OpenD 没装/没启动 |
| 告警没收到 | 1) macOS 系统设置 → 通知 → 允许"脚本编辑器" 2) 检查 `data/alert_config.json` 的 channels.enabled |

## 完整重置(如果你想从零开始)

```bash
# 卸载 6 个 launchd job
for s in uninstall_launchd uninstall_api_launchd uninstall_enrich_launchd \
         uninstall_alerts_launchd uninstall_signals_launchd uninstall_quotes_launchd; do
    bash ~/projects/cls-pipeline/scripts/$s.sh
done
# 删 db + logs
rm -rf ~/projects/cls-pipeline/data/cls.db* ~/projects/cls-pipeline/logs/*
# 重新跑 bootstrap.sh
bash ~/projects/cls-pipeline/scripts/bootstrap.sh
```
