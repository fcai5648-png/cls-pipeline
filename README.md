# cls-pipeline

7×24 小时抓 [财联社电报](https://www.cls.cn/telegraph) 入库 SQLite,作为下游分析(选股 / 早报 / 告警)的统一数据源。

> 想把这个项目部署到另一台 Mac?看 [MIGRATE.md](./MIGRATE.md)。

## 架构

```
┌──────────────────────────────────────────────────────────┐
│  launchd  (com.user.cls-pipeline,KeepAlive=true)         │
│   ├─ RunAtLoad: 开机即起                                  │
│   ├─ KeepAlive: 进程挂了 30s 后自动重启                   │
└──────────────────────┬───────────────────────────────────┘
                       │ exec
                       ▼
┌──────────────────────────────────────────────────────────┐
│  src/daemon.py  (单进程长跑)                              │
│   ├─ 每 30s → fetcher.fetch_with_retry()                  │
│   │     ↳ akshare.stock_info_global_cls(symbol="全部")    │
│   │     ↳ 失败 3 次退避(2s/4s/8s)                         │
│   ├─ store.insert_batch(rows)  ─ 内容 hash 唯一索引去重    │
│   ├─ store.log_fetch()         ─ 每周期记一条 fetch_log    │
│   ├─ SIGTERM/SIGINT 优雅退出                              │
│   └─ 连续 20 次失败 → 退出 → launchd 重启                 │
└──────────────────────┬───────────────────────────────────┘
                       │ SQLite WAL
                       ▼
┌──────────────────────────────────────────────────────────┐
│  data/cls.db                                              │
│   ├─ telegraph: id, pub_dt, title, content, content_hash, │
│   │             fetched_at, raw_json                      │
│   └─ fetch_log: 拉取元信息(用来排查丢条/失败率)            │
└──────────────────────┬───────────────────────────────────┘
                       │ 任意下游
                       ▼
       命令行查询(query.sh)/ HTTP API / Web 浏览器 / 下游消费
                       ▲
                       │ 共享 SQLite(WAL,多读者无冲突)
   ┌───────────────────┼─────────────────────────┐
   │                   │                         │
┌──┴────────────┐  ┌──┴────────────┐  ┌──────┴────────────┐
│ daemon        │  │ enrich worker │  │ api server         │
│ (cls-pipeline)│  │ (-enrich)     │  │ (-api,uvicorn)    │
│ 30s 拉取写入  │  │ 20s 扫新增条目│  │ 127.0.0.1:8787     │
│               │  │ 规则引擎抽标签│  │ FastAPI+viewer/HTML│
└───────────────┘  └───────────────┘  └───────────────────┘
        ↑                  ↑                    ↑
       launchd KeepAlive=true(三个独立 job,任一挂掉自动重启)
```

## 关键设计

| 项 | 选择 | 原因 |
|---|---|---|
| 调度 | launchd `KeepAlive=true` | 真 7×24,不依赖 cron / Claude Code 调度;挂了自动拉起 |
| 数据源 | akshare `stock_info_global_cls` | 已在 daily-brief 验证可用,字段稳定;一次返 20 条 |
| 频率 | 30s | 电报高峰一分钟 5-10 条,30s 间隔 + 一次 20 条窗口足以不丢 |
| 存储 | SQLite WAL | 单文件、零运维、并发读写无锁问题 |
| 去重 | sha1(`pub_dt + title + content[:300]`) UNIQUE | 财联社无稳定电报 ID,内容哈希足够稳 |
| Python | 复用 `~/a_stock_ai_selector/.venv` | 已含 akshare,不再单独建 venv |
| 保留 | 永久 | 每天 ~1000 条 × ~500 字节 ≈ 0.5 MB/天,1 年 < 200 MB |

## 目录

```
~/projects/cls-pipeline/
├── README.md
├── com.user.cls-pipeline.plist            # launchd(daemon)
├── com.user.cls-pipeline-api.plist        # launchd(api server)
├── com.user.cls-pipeline-enrich.plist     # launchd(enrich worker)
├── src/
│   ├── db.py                       # SQLite schema + insert/query/enrichment
│   ├── fetcher.py                  # akshare 调用 + 重试
│   ├── daemon.py                   # 抓取主循环(入口)
│   ├── enrich.py                   # 规则引擎(实体/情绪/事件)
│   ├── enrich_worker.py            # backfill + 增量抽取守护(入口)
│   ├── cli.py                      # 命令行查询
│   └── api.py                      # FastAPI + 内嵌 HTML viewer
├── scripts/
│   ├── run_daemon.sh / run_api.sh / run_enrich.sh
│   ├── query.sh
│   └── install_*_launchd.sh / uninstall_*_launchd.sh
├── data/
│   ├── cls.db                      # SQLite(WAL,运行时生成)
│   └── dict/                       # ⭐ 词典 — 直接编辑这些 JSON 加自选股/板块
│       ├── sectors.json            # 板块(规范名 → 别名)
│       ├── companies.json          # 公司精确名列表
│       ├── orgs.json               # 政策机构 / 监管层
│       ├── event_types.json        # 事件类型 → 关键词
│       └── sentiment.json          # positive / negative 关键词
├── data/
│   └── cls.db                      # SQLite(WAL 模式,运行后生成)
└── logs/
    ├── daemon.log                  # 守护进程日志(rotating, 10MB×5)
    ├── launchd-stdout.log          # launchd 抓的 stdout
    └── launchd-stderr.log          # launchd 抓的 stderr
```

## 用法

### 一次性安装(开机自启 + 7×24)

```bash
bash ~/projects/cls-pipeline/scripts/install_launchd.sh           # daemon(必装)
bash ~/projects/cls-pipeline/scripts/install_api_launchd.sh       # http api / viewer(可选)
bash ~/projects/cls-pipeline/scripts/install_enrich_launchd.sh    # enrichment(可选,需先有 daemon)
```

### 查询

```bash
# 概览
bash ~/projects/cls-pipeline/scripts/query.sh stats

# 最新 20 条
bash ~/projects/cls-pipeline/scripts/query.sh tail 20

# 关键词搜索
bash ~/projects/cls-pipeline/scripts/query.sh search "央行" 30

# 导出最新 1000 条到 JSON
bash ~/projects/cls-pipeline/scripts/query.sh export /tmp/cls.json 1000
```

### 监控运行

```bash
# 实时日志
tail -f ~/projects/cls-pipeline/logs/daemon.log

# launchd 状态(看 PID / 上次退出码)
launchctl list | grep cls-pipeline

# 健康检查(过去一小时是否还在拉)
bash ~/projects/cls-pipeline/scripts/query.sh stats
# 期望:fetches_last_hour ≈ 120(30s × 60min × 60min/hr ÷ 30s),errors_last_hour 应该是 0 或个位数
```

### 卸载

```bash
bash ~/projects/cls-pipeline/scripts/uninstall_launchd.sh
bash ~/projects/cls-pipeline/scripts/uninstall_api_launchd.sh
bash ~/projects/cls-pipeline/scripts/uninstall_enrich_launchd.sh
# 数据库保留在 data/cls.db,如要清干净:rm -rf data/ logs/
```

## Enrichment(标签抽取)

每条电报由 `enrich_worker` 用规则引擎抽成 5 类标签,落到 `telegraph_enrichment` 表。

| 字段 | 内容 | 例 |
|---|---|---|
| `sectors` | 命中的板块(规范名 + 公司自动映射的板块) | `["半导体", "AI", "光模块"]` |
| `companies` | 命中的公司精确名 | `["宁德时代", "比亚迪"]` |
| `orgs` | 政策机构 / 监管层 | `["央行", "证监会"]` |
| `event_types` | 事件类型 | `["政策", "业绩", "并购"]` |
| `sentiment` | 三态情绪 | `positive` / `negative` / `neutral` |
| `sentiment_score` | 情绪强度 -1.0~1.0 | `0.8`(命中 4 个利好 0 个利空 → 0.8) |

### 公司 → 板块自动映射(rule-v2)

`companies.json` 里每个公司带 `[sectors...]` 标注,规则引擎命中公司时**自动**把对应板块加进 sectors。
例:文中只写了"宁德时代量产合作",规则版会自动归入 `板块=[锂电池, 新能源车]`,无需文中显式提"锂电池"字眼。

```json
{
  "company_to_sectors": {
    "宁德时代": ["锂电池", "新能源车"],
    "中际旭创": ["光模块", "AI", "算力"],
    "特斯拉": ["新能源车", "智能驾驶", "人形机器人"],
    "海康威视": []     // 空数组 = 命中也不归任何板块
  }
}
```

直接编辑 `data/dict/companies.json` 加自选股 + 板块标注 → kickstart enrich worker 即生效。
板块名必须在 `sectors.json` 里有,否则会被静默丢掉(防止笔误污染数据)。

### 怎么改词典

直接编辑 `data/dict/*.json`,worker 每次启动时重新加载。要立刻让词典变更生效:

```bash
launchctl kickstart -k gui/$(id -u)/com.user.cls-pipeline-enrich
```

如果改了 `enrich.py` 逻辑(影响抽取结果),把 `ENRICHER_VERSION` 改一下(例 `rule-v1` → `rule-v2`),worker 启动时会**重抽全部历史数据**(80 条 ~12ms)。

### enrichment 相关 API

| Path | 说明 |
|---|---|
| `GET /telegraph/latest?n=N` | 已含 `enrichment` 字段(可能为 null) |
| `GET /tags?hours=24` | 过去 N 小时 sector / event / sentiment 命中统计(viewer 侧栏用) |
| `GET /telegraph/by_tag?kind=K&name=N&hours=72&limit=100` | 按标签筛选(K = sector / event / company / org / sentiment) |

### Web Viewer

[http://127.0.0.1:8787/](http://127.0.0.1:8787/) 是真正的电报浏览器(不是 Swagger):
- 每条电报下方显示彩色 tag chip:🟦 板块 · 🟧 事件 · 🟪 公司 · 🩷 机构 · 🟩/🟥 情绪
- 左边框颜色按情绪着色(绿/灰/红)
- 点击任意 chip 即按该标签筛选;再点 ✕ 清除
- 右侧栏显示 24 小时热门板块 / 事件 / 情绪分布,点击即筛选
- 自动每 15 秒刷新,新到电报 1.6s 闪光提示

## HTTP API

启动后绑 `127.0.0.1:8787`(仅本地)。Swagger UI 在 [http://127.0.0.1:8787/docs](http://127.0.0.1:8787/docs)。

### Endpoints

| Method | Path | 说明 |
|---|---|---|
| GET | `/health` | 状态检查(过去 1 小时拉取数 / 错误率) |
| GET | `/stats` | 数据库 + 拉取元统计 |
| GET | `/telegraph/latest?n=20` | 最新 N 条(发布时间倒序;1≤n≤500) |
| GET | `/telegraph/since?after=ISO_TS&limit=500` | **增量拉取** — 给下游 ETL 用,正序返回 `pub_dt > after` 的所有条目 |
| GET | `/telegraph/search?q=KW&limit=50` | 标题/内容关键词搜索 |
| GET | `/telegraph/{id}` | 按主键取单条(含 raw_json) |

### 例子

```bash
# 健康检查
curl -s http://127.0.0.1:8787/health

# 最新 5 条
curl -s 'http://127.0.0.1:8787/telegraph/latest?n=5'

# 增量拉取(下游 ETL 模式 — 记下上次最大 pub_dt,下次以它为 after)
curl -s 'http://127.0.0.1:8787/telegraph/since?after=2026-05-07T14:00:00&limit=200'

# 关键词搜索(URL encode 中文)
curl -s 'http://127.0.0.1:8787/telegraph/search?q=央行&limit=20'
```

### 下游接入示例(Python)

```python
import requests, time
BASE = "http://127.0.0.1:8787"

# 1) 增量订阅(轮询)
last_seen = "2026-05-07T00:00:00"
while True:
    r = requests.get(f"{BASE}/telegraph/since",
                     params={"after": last_seen, "limit": 500}).json()
    for row in r["rows"]:
        process(row)                     # 你的下游处理
        last_seen = row["pub_dt"]        # 推进游标
    time.sleep(30)
```

### 设计要点

- **独立 launchd job**:`com.user.cls-pipeline-api` 与 daemon 解耦,任一挂掉不影响另一个。
- **共享 SQLite (WAL)**:多读者天然支持,无锁冲突;daemon 写,api 只读。
- **仅本地绑定**:`--host 127.0.0.1` — 远程访问请走 nginx / Tailscale,**不要直接改成 0.0.0.0**(没鉴权)。
- **UTF-8 直出**:自定义 `UTF8JSONResponse` 让中文以原文返回,不转 `\uXXXX`。
- **端口可改**:`CLS_API_PORT=9999 launchctl ...` — 见 `scripts/run_api.sh`。

### 手动前台跑(调试)

```bash
cd ~/projects/cls-pipeline
CLS_INTERVAL_SEC=15 ~/a_stock_ai_selector/.venv/bin/python src/daemon.py
# Ctrl+C 退出
```

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `CLS_DB_PATH` | `data/cls.db` | SQLite 文件路径 |
| `CLS_INTERVAL_SEC` | `30` | 拉取间隔(秒) |
| `CLS_LOG_PATH` | `logs/daemon.log` | 守护日志路径 |

## 数据 schema

```sql
CREATE TABLE telegraph (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pub_date        TEXT NOT NULL,           -- 2026-05-07
    pub_time        TEXT NOT NULL,           -- 12:53:27
    pub_dt          TEXT NOT NULL,           -- 2026-05-07 12:53:27(查询用)
    title           TEXT NOT NULL,
    content         TEXT NOT NULL,
    content_hash    TEXT NOT NULL UNIQUE,    -- 去重 key
    fetched_at      TEXT NOT NULL,           -- 我们入库时间(ISO)
    raw_json        TEXT                     -- 整行原始 JSON,schema 演化兜底
);

CREATE TABLE fetch_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fetched_at      TEXT NOT NULL,
    rows_returned   INTEGER NOT NULL,        -- API 返回总条数
    rows_new        INTEGER NOT NULL,        -- 去重后新入库条数
    duration_ms     INTEGER NOT NULL,
    error           TEXT                     -- 失败时记 error string
);
```

## 下游接入

任何 Python 项目直接读 SQLite 即可:

```python
import sqlite3
conn = sqlite3.connect("/Users/jintianyouyu/projects/cls-pipeline/data/cls.db")
cur = conn.execute(
    "SELECT pub_dt, title, content FROM telegraph "
    "WHERE pub_dt >= ? ORDER BY pub_dt DESC",
    ("2026-05-07 00:00:00",)
)
for row in cur:
    print(row)
```

或者 daily-brief 项目可以把 `fetch_cls_telegraph()` 切到读本地 db,瞬时返回(免每次拉 akshare),并且能拿到任意时间窗口的电报(目前只能拉最新 20 条)。

## 重要性评分

每条电报抽完标签后立刻按公式打 0-100 分,落 `telegraph_enrichment.importance_score`,默认建索引。
Viewer 顶部"⭐ Top 重要"切换 + `/telegraph/top` API 都基于这个字段。

### 公式

```
score = (
   |sentiment_score| × 25       # 情绪强度    0-25
 + max_event_weight  × 35       # 事件权重    0-35(命中事件中最高)
 + watchlist_boost              # 关注板块+10 / 公司+30,上限 40
) × freshness_decay              # e^(-hours/24): 1h=0.96 / 24h=0.37 / 48h=0.14
                                 # 最终 cap 100
```

### 关注列表(可编辑)

`data/dict/watchlist.json` — 改完立即生效:

```bash
launchctl kickstart -k gui/$(id -u)/com.user.cls-pipeline-enrich
```

worker 启动时会扫描 `scoring_version` 不一致的行**只重新算分**(不重抽 enrichment),81 条 ~80ms。
改 `scoring.py` 公式时把 `SCORING_VERSION` 升号(`score-v1` → `score-v2`)即可全量重算。

### 调试

每条 enrichment 都存了 `scoring_components_json`:

```sql
SELECT t.title, e.importance_score, e.scoring_components_json
FROM telegraph t JOIN telegraph_enrichment e ON e.telegraph_id = t.id
WHERE e.importance_score > 60 ORDER BY e.importance_score DESC LIMIT 5;
```

输出会展示 `sentiment_part / event_part / watchlist_part / freshness / sector_hits / company_hits` 各分量。

## 告警推送

`com.user.cls-pipeline-alerts` 独立 worker,每 30 秒扫一次 `score >= min_score` 且未推过的电报,
分发到所有 enabled channel。`alert_log.telegraph_id UNIQUE` 保证一条电报全生命周期只推一次。

### 滑动窗口设计

每轮只看过去 1 小时内的高分(`lookback_hours_on_start` 控制,默认 1)。
配合评分公式的 freshness decay(1h=0.96 → 24h=0.37),早于 1h 的电报基本不会再过阈值,所以
**1h 窗口既能保证不漏推,也保证重启不刷屏**(已推过的 telegraph_id 直接被 NOT EXISTS 排除)。

### 配置(`data/alert_config.json`)

```json
{
  "min_score": 60,
  "lookback_hours_on_start": 1,
  "poll_interval_sec": 30,
  "channels": {
    "osascript":     {"enabled": true,  "sound": "Submarine"},
    "bark":          {"enabled": false, "url_or_key": ""},
    "serverchan":    {"enabled": false, "sendkey": ""},
    "wecom_webhook": {"enabled": false, "url": ""}
  }
}
```

改完立即生效:
```bash
launchctl kickstart -k gui/$(id -u)/com.user.cls-pipeline-alerts
```

### 渠道说明

| Channel | 配置项 | 注意 |
|---|---|---|
| `osascript` | `sound`(可空表示静音) | 第一次会弹 macOS 通知权限对话框 — 必须允许"脚本编辑器" |
| `bark` | `url_or_key` — 完整 URL 或仅 key | 装 [Bark iOS app](https://apps.apple.com/cn/app/bark-customed-notifications/id1403753865) 拿 key |
| `serverchan` | `sendkey` | 在 [sct.ftqq.com](https://sct.ftqq.com) 申请,微信收推送 |
| `wecom_webhook` | `url` | 企业微信群 → 添加机器人 → 复制 webhook |

### 告警含价格(已上线)

如果 `data/dict/quote_targets.json` 存在 + quote_worker 在跑,告警 body 自动加一行 `💹` 列出该电报相关的港美股实时涨跌(按 |涨跌| 倒序,前 5 个):

```
🔥 40 🟢 · 锂电池 / 新能源车 · 宁德时代与Togg达成磐石底盘量产合作
板块: 锂电池,新能源车
公司: 宁德时代
情绪: positive (+0.75)
💹 ALB +10.53% · SQM +3.60% · TSLA +3.45% · 01211 +1.82% · NIO +1.36%
【宁德时代与 Togg 达成磐石底盘量产合作...】
```

价值:看到通知**就知道信号是否被价格 confirm** — 利好+涨=追,利好+跌=警惕(可能美股先行调整)。

### macOS 通知权限故障排查

如果日志显示 `sent ... channels=['osascript']` 但屏幕没看到弹窗:
1. 打开 **系统设置 → 通知**
2. 找到 **"脚本编辑器"**(英文 "Script Editor")
3. 确保:允许通知 ✅,横幅样式选 "横幅" 或 "提醒"
4. 验证:`osascript -e 'display notification "test" with title "test"'`(终端跑这条应该弹)

## 信号引擎

`com.user.cls-pipeline-signals` 独立 worker,每 15 分钟扫一次 enrichment 表,
产出 4 类结构化信号写入 `signals` 表。下游(选股 / daily-brief / 告警)读这张表。

### 信号类型

| Kind | 算法 | 用途 |
|---|---|---|
| `sector_sentiment` | 板块累计 importance × 频次 × 情绪均值,watchlist 板块 ×1.5 | **板块强度排行** — 选热门板块 |
| `company_heat` | 关注公司频次 × 10 + 累计分 × 0.5 + 事件多样性 × 5 | **个股催化剂** — 哪些自选股最活跃 |
| `event_cluster` | 同一(板块, 事件)对 ≥3 次 → 强催化共振 | **主线题材识别** |
| `sector_anomaly` | 4h 内板块电报数 vs 7d 基线的 z-score(z≥1.5 算异动) | **异动早报** — 抢跑 |

### API

```bash
GET /signals?kind=sector_sentiment&top=20      # 当前快照
GET /signals?kind=company_heat&top=20
GET /signals?kind=event_cluster&top=20
GET /signals/anomalies                         # 当前异动 (z≥1.5)
GET /signals/history?kind=sector_sentiment&target=半导体&days=7   # 时间序列
```

### Viewer 信号面板

顶部"视图"切换到 **📊 信号面板** — 一个页面看全:
- ⚠️ 板块异动(z≥1.5,数据满 7 天后自动开始)
- 📊 板块情绪 24h(score 排序,⭐=watchlist 板块)
  - 每行右侧 **7 天 sparkline**(SVG mini chart):绿=上升趋势 / 红=下降
  - 每行加 **港美股龙头实时涨跌 chip**(信号 confirm)
- 🔥 关注公司热度 24h(同上,带 sparkline + 港美股报价)
- ⚡ 事件共振(板块×事件 ≥3 次)

### 回测

数据攒满 1+ 天后跑:

```bash
~/a_stock_ai_selector/.venv/bin/python ~/projects/cls-pipeline/scripts/backtest_signals.py 7
```

每个历史 sector_sentiment 信号 → 看其后 4h 同板块电报情绪是否同向 → 计算"信号准确率"。
按板块细分 Top/Bottom,识别哪些板块的信号可信、哪些噪声大。

### 下游接入

```python
# 选股项目可以直接读 signals 表
import sqlite3
c = sqlite3.connect('/Users/jintianyouyu/projects/cls-pipeline/data/cls.db')
top_sectors = c.execute("""
  SELECT target, score, direction, components_json
  FROM signals WHERE kind='sector_sentiment'
    AND computed_at = (SELECT MAX(computed_at) FROM signals)
  ORDER BY score DESC LIMIT 5
""").fetchall()
# 直接拿到当前最热的 5 个板块,可作为选股候选池
```

## 富途行情接入(港美股)

`com.user.cls-pipeline-quotes` 独立 worker,90 秒一次拉富途 OpenD 实时行情(港股 + 美股 Level 1,免费),
缓存到 `quotes` 表。Viewer 信号面板每个板块/公司 chip 旁边自动显示对应港美股龙头的实时涨跌幅,
**用价格 confirm cls 信号**。

### 前提

1. **本机装富途 OpenD 桌面客户端**:https://www.futunn.com/download/openAPI(免费,即使没开户也能拿行情)
2. OpenD 启动后默认监听 `127.0.0.1:11111`,**保持开机**(否则 worker 拉空)
3. 富途账号登录 OpenD(空账号即可,无需开户)

### 映射(可编辑)

`data/dict/quote_targets.json`:

```json
{
  "sector_to_codes": {
    "AI":     ["US.NVDA", "US.MSFT", "US.GOOG", "US.PLTR", "US.META"],
    "半导体": ["US.NVDA", "US.TSM", "US.AMD", "US.MU", "HK.00981"],
    "锂电池": ["US.ALB", "US.SQM", "HK.03750"]
  },
  "company_to_code": {
    "腾讯":     "HK.00700",
    "宁德时代": "HK.03750",
    "英伟达":   "US.NVDA"
  }
}
```

修改后:`launchctl kickstart -k gui/$(id -u)/com.user.cls-pipeline-quotes` 立即生效。
**无效代码会被自动二分查找出来标记跳过**(不会因为一个错代码而批量失败)。

### API

```bash
GET /quotes                                # 全部缓存
GET /quotes?codes=HK.00700,US.NVDA         # 指定代码
GET /quotes/sector?name=半导体             # 该板块对应的港美股
GET /quotes/company?name=宁德时代          # 该公司对应的港美股
```

### 红线(我不会做的事)

- ❌ 自动下单 / 撤单 / 转账
- ❌ 直接给"买入 X 股"的指令
- ✅ 只读拉行情数据,辅助你判断
- ✅ 把信号 + 价格放一起,让你**手动决策**

下单永远是你的事。

## 可能的下一步

- ~~**enrichment 层**~~ / ~~**HTTP API**~~ / ~~**重要性评分**~~ / ~~**告警推送**~~ / ~~**公司→板块映射**~~ / ~~**信号工程**~~ / ~~**富途行情接入**~~:✅ 全部完成
- **接 daily-brief**:早报消费 signals + quotes 表
- **多源融合**:加 wallstreetcn / 东财 7×24
- **SSE / WebSocket 推送**:`/telegraph/stream` 让下游订阅而非轮询
- **LLM 配额可视化**:viewer 加每日 LLM 调用 / 累计成本显示
- **个股催化剂时间线**:单公司 N 天内事件序列
- **告警含价格**:cls 高分电报告警时把当前涨跌幅塞进消息体("XX 利好 + 已涨 5%")
