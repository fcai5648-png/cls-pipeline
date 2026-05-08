"""SQLite 存储层 — WAL 模式,支持 7*24 长跑。

表设计:
- telegraph:电报正文 + 内容哈希(去重 key)
- fetch_log:每次拉取的元信息(用来排查丢条 / 失败率)
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, Optional


SCHEMA = """
CREATE TABLE IF NOT EXISTS telegraph (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pub_date        TEXT    NOT NULL,
    pub_time        TEXT    NOT NULL,
    pub_dt          TEXT    NOT NULL,
    title           TEXT    NOT NULL,
    content         TEXT    NOT NULL,
    content_hash    TEXT    NOT NULL UNIQUE,
    fetched_at      TEXT    NOT NULL,
    raw_json        TEXT,
    is_highlight    INTEGER NOT NULL DEFAULT 0     -- cls 编辑精选(头条)
);
-- idx_telegraph_highlight 索引在 _init_db() 里 ALTER 之后建,这里不能引用尚未 ALTER 上的列
CREATE INDEX IF NOT EXISTS idx_telegraph_pub_dt    ON telegraph(pub_dt DESC);
CREATE INDEX IF NOT EXISTS idx_telegraph_fetched   ON telegraph(fetched_at DESC);

CREATE TABLE IF NOT EXISTS fetch_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fetched_at      TEXT    NOT NULL,
    rows_returned   INTEGER NOT NULL,
    rows_new        INTEGER NOT NULL,
    duration_ms     INTEGER NOT NULL,
    error           TEXT
);
CREATE INDEX IF NOT EXISTS idx_fetch_log_fetched   ON fetch_log(fetched_at DESC);

-- enrichment 一对一关联 telegraph;enricher_version 用于将来重抽
CREATE TABLE IF NOT EXISTS telegraph_enrichment (
    telegraph_id        INTEGER PRIMARY KEY REFERENCES telegraph(id) ON DELETE CASCADE,
    sectors_json        TEXT NOT NULL DEFAULT '[]',
    companies_json      TEXT NOT NULL DEFAULT '[]',
    orgs_json           TEXT NOT NULL DEFAULT '[]',
    event_types_json    TEXT NOT NULL DEFAULT '[]',
    sentiment           TEXT NOT NULL DEFAULT 'neutral',
    sentiment_score     REAL NOT NULL DEFAULT 0.0,
    enricher_version    TEXT NOT NULL,
    processed_at        TEXT NOT NULL,
    llm_called          INTEGER NOT NULL DEFAULT 0,   -- 0/1
    llm_cost_usd        REAL    NOT NULL DEFAULT 0.0,
    llm_reasoning       TEXT                          -- LLM 给的一句话理由(可空)
);
CREATE INDEX IF NOT EXISTS idx_enrich_sentiment    ON telegraph_enrichment(sentiment);
CREATE INDEX IF NOT EXISTS idx_enrich_processed    ON telegraph_enrichment(processed_at DESC);

-- LLM 调用审计 — 用来算每日配额 + 月度成本
CREATE TABLE IF NOT EXISTS llm_call_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    called_at       TEXT    NOT NULL,
    telegraph_id    INTEGER,
    model           TEXT    NOT NULL,
    tokens_in       INTEGER NOT NULL DEFAULT 0,
    tokens_out      INTEGER NOT NULL DEFAULT 0,
    cost_usd        REAL    NOT NULL DEFAULT 0.0,
    latency_ms      INTEGER NOT NULL DEFAULT 0,
    error           TEXT
);
CREATE INDEX IF NOT EXISTS idx_llm_called_at ON llm_call_log(called_at DESC);

-- 告警推送日志 — telegraph_id UNIQUE 保证一条电报只推一次
CREATE TABLE IF NOT EXISTS alert_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    telegraph_id    INTEGER NOT NULL UNIQUE REFERENCES telegraph(id),
    score           REAL    NOT NULL,
    channels_json   TEXT    NOT NULL,
    sent_at         TEXT    NOT NULL,
    success         INTEGER NOT NULL,
    error           TEXT
);
CREATE INDEX IF NOT EXISTS idx_alert_sent_at ON alert_log(sent_at DESC);

-- 信号快照表 — 每 15 分钟一次,每个 (kind, target, window) 一行
CREATE TABLE IF NOT EXISTS signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    computed_at     TEXT    NOT NULL,
    window_hours    INTEGER NOT NULL,
    kind            TEXT    NOT NULL,    -- sector_sentiment | company_heat | event_cluster | sector_anomaly
    target          TEXT    NOT NULL,    -- 板块名 / 公司名 / "板块|事件" 等
    score           REAL    NOT NULL,
    direction       TEXT,                -- bullish | bearish | neutral
    components_json TEXT,
    evidence_json   TEXT,                -- 支撑信号的电报 id 列表
    UNIQUE(computed_at, kind, target, window_hours)
);
CREATE INDEX IF NOT EXISTS idx_signals_kind_target ON signals(kind, target, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_signals_computed    ON signals(computed_at DESC);

-- 富途实时报价缓存(只读,quote_worker 定期刷新)
CREATE TABLE IF NOT EXISTS quotes (
    code            TEXT    PRIMARY KEY,    -- HK.00700 / US.NVDA
    name            TEXT,
    last_price      REAL,
    prev_close      REAL,
    change_rate     REAL,                   -- 涨跌幅 %,自算
    turnover        REAL,
    volume          REAL,
    update_time     TEXT,                   -- 富途返回的最新报价时间
    fetched_at      TEXT                    -- 我们入库时间
);
"""


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    """轻量 migration:检查列存在性,缺则 ALTER 加上(用于已部署 db 升级)。"""
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cur.fetchall()}
    for col, ddl in columns.items():
        if col not in existing:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")


_BRACKETED_PREFIX = re.compile(r'^[【\[][^】\]]*[】\]]\s*')


def make_hash(pub_dt: str, title: str, content: str) -> str:
    """财联社去重:同一新闻常被推 2 次(短 title 简版 + 长 title 详版,或快讯+追加版)。
    去掉【...】title 段后,只用正文前 100 字 hash:
    - cls 电报都以"财联社 X 月 X 日电,"开头,真正内容差异在第 13 字起
    - 100 字真实内容差异度足以区分不同新闻
    - 同新闻不同版本前 100 字几乎必然撞 → dedupe
    """
    body = _BRACKETED_PREFIX.sub('', content or '').strip()
    payload = f"{pub_dt}|{body[:100]}".encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


class Store:
    """SQLite 封装。线程安全:用 RLock + per-thread connection 不必要,
    本守护进程是单线程轮询,直接共用一个 connection 即可。"""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self.db_path),
            isolation_level=None,    # autocommit;事务靠手动 BEGIN
            check_same_thread=False,
        )
        self._lock = threading.RLock()
        self._init_db()

    def _init_db(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA synchronous=NORMAL")
            cur.execute("PRAGMA temp_store=MEMORY")
            cur.executescript(SCHEMA)
            # 为已部署的 db 补加新列
            _ensure_columns(self._conn, "telegraph", {
                "is_highlight": "INTEGER NOT NULL DEFAULT 0",
            })
            _ensure_columns(self._conn, "telegraph_enrichment", {
                "llm_called":               "INTEGER NOT NULL DEFAULT 0",
                "llm_cost_usd":             "REAL NOT NULL DEFAULT 0.0",
                "llm_reasoning":            "TEXT",
                "importance_score":         "REAL NOT NULL DEFAULT 0.0",
                "scoring_components_json":  "TEXT",
                "scoring_version":          "TEXT",
            })
            cur.execute("CREATE INDEX IF NOT EXISTS idx_enrich_score ON telegraph_enrichment(importance_score DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_telegraph_highlight ON telegraph(is_highlight, pub_dt DESC)")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Cursor]:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                yield cur
                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise

    def insert_batch(self, rows: Iterable[dict]) -> int:
        """rows: dict with keys pub_date, pub_time, title, content (raw row)。
        可选 _is_highlight=1 标记 cls 编辑精选(头条)。
        返回新插入的条数;已存在但被新标记为 highlight 的会 upgrade(不算新条)。"""
        now_iso = datetime.now().isoformat(timespec="seconds")
        new_count = 0
        with self.transaction() as cur:
            for r in rows:
                pub_date = str(r.get("发布日期") or r.get("pub_date") or "").strip()
                pub_time = str(r.get("发布时间") or r.get("pub_time") or "").strip()
                title = str(r.get("标题") or r.get("title") or "").strip()
                content = str(r.get("内容") or r.get("content") or "").strip()
                if not (pub_date and pub_time and (title or content)):
                    continue
                pub_dt = f"{pub_date} {pub_time}"
                chash = make_hash(pub_dt, title, content)
                is_highlight = 1 if r.get("_is_highlight") else 0
                try:
                    cur.execute(
                        "INSERT INTO telegraph "
                        "(pub_date, pub_time, pub_dt, title, content, content_hash, fetched_at, raw_json, is_highlight) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            pub_date, pub_time, pub_dt,
                            title, content, chash, now_iso,
                            json.dumps(r, ensure_ascii=False, default=str),
                            is_highlight,
                        ),
                    )
                    new_count += 1
                except sqlite3.IntegrityError:
                    # 已存在 — 如果新数据是重点,把已有的 upgrade(0→1,1→1 不变)
                    if is_highlight:
                        cur.execute(
                            "UPDATE telegraph SET is_highlight = 1 "
                            "WHERE content_hash = ? AND is_highlight = 0",
                            (chash,),
                        )
        return new_count

    def log_fetch(self, rows_returned: int, rows_new: int, duration_ms: int, error: Optional[str]) -> None:
        with self.transaction() as cur:
            cur.execute(
                "INSERT INTO fetch_log (fetched_at, rows_returned, rows_new, duration_ms, error) "
                "VALUES (?, ?, ?, ?, ?)",
                (datetime.now().isoformat(timespec="seconds"), rows_returned, rows_new, duration_ms, error),
            )

    # ---------- 查询 ----------
    def total(self) -> int:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT COUNT(*) FROM telegraph")
            return cur.fetchone()[0]

    def latest(self, n: int = 20) -> list[dict]:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT pub_dt, title, content, fetched_at "
                "FROM telegraph ORDER BY pub_dt DESC, id DESC LIMIT ?",
                (n,),
            )
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def search(self, keyword: str, n: int = 50) -> list[dict]:
        with self._lock:
            cur = self._conn.cursor()
            kw = f"%{keyword}%"
            cur.execute(
                "SELECT pub_dt, title, content FROM telegraph "
                "WHERE title LIKE ? OR content LIKE ? "
                "ORDER BY pub_dt DESC LIMIT ?",
                (kw, kw, n),
            )
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def since(self, after: str, limit: int = 500) -> list[dict]:
        """按发布时间增量拉取(after 形如 '2026-05-07 10:00:00' 或 ISO '2026-05-07T10:00:00')。
        SQLite 字符串比较对齐 ISO 风格,'T' 换成空格即可。"""
        normalized = after.replace("T", " ").strip()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT id, pub_dt, title, content, fetched_at "
                "FROM telegraph WHERE pub_dt > ? ORDER BY pub_dt ASC, id ASC LIMIT ?",
                (normalized, limit),
            )
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_by_id(self, tid: int) -> dict | None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT id, pub_dt, title, content, fetched_at, raw_json "
                "FROM telegraph WHERE id = ?",
                (tid,),
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = [c[0] for c in cur.description]
            return dict(zip(cols, row))

    def stats(self) -> dict:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT COUNT(*), MIN(pub_dt), MAX(pub_dt) FROM telegraph")
            total, earliest, latest = cur.fetchone()
            cur.execute(
                "SELECT COUNT(*), SUM(rows_new), AVG(duration_ms), "
                "SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) "
                "FROM fetch_log WHERE fetched_at >= datetime('now', '-1 hour', 'localtime')"
            )
            f_count, f_new, f_avg_ms, f_err = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM telegraph_enrichment")
            enriched = cur.fetchone()[0] or 0
            return {
                "total_telegraph": total or 0,
                "earliest_pub": earliest,
                "latest_pub": latest,
                "fetches_last_hour": f_count or 0,
                "new_rows_last_hour": f_new or 0,
                "avg_fetch_ms_last_hour": round(f_avg_ms or 0, 1),
                "errors_last_hour": f_err or 0,
                "enriched_total": enriched,
                "enrichment_pending": (total or 0) - enriched,
            }

    # ---------- enrichment ----------
    def enrichment(self, telegraph_id: int) -> dict | None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT sectors_json, companies_json, orgs_json, event_types_json, "
                "sentiment, sentiment_score, enricher_version, processed_at "
                "FROM telegraph_enrichment WHERE telegraph_id = ?",
                (telegraph_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "sectors": json.loads(row[0]),
                "companies": json.loads(row[1]),
                "orgs": json.loads(row[2]),
                "event_types": json.loads(row[3]),
                "sentiment": row[4],
                "sentiment_score": row[5],
                "enricher_version": row[6],
                "processed_at": row[7],
            }

    def upsert_enrichment(self, telegraph_id: int, payload: dict, version: str,
                          llm_called: bool = False, llm_cost_usd: float = 0.0,
                          llm_reasoning: str | None = None,
                          importance_score: float = 0.0,
                          scoring_components: dict | None = None,
                          scoring_version: str | None = None) -> None:
        with self.transaction() as cur:
            cur.execute(
                "INSERT INTO telegraph_enrichment "
                "(telegraph_id, sectors_json, companies_json, orgs_json, event_types_json, "
                " sentiment, sentiment_score, enricher_version, processed_at, "
                " llm_called, llm_cost_usd, llm_reasoning, "
                " importance_score, scoring_components_json, scoring_version) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(telegraph_id) DO UPDATE SET "
                "  sectors_json     = excluded.sectors_json, "
                "  companies_json   = excluded.companies_json, "
                "  orgs_json        = excluded.orgs_json, "
                "  event_types_json = excluded.event_types_json, "
                "  sentiment        = excluded.sentiment, "
                "  sentiment_score  = excluded.sentiment_score, "
                "  enricher_version = excluded.enricher_version, "
                "  processed_at     = excluded.processed_at, "
                "  llm_called       = excluded.llm_called, "
                "  llm_cost_usd     = excluded.llm_cost_usd, "
                "  llm_reasoning    = excluded.llm_reasoning, "
                "  importance_score = excluded.importance_score, "
                "  scoring_components_json = excluded.scoring_components_json, "
                "  scoring_version  = excluded.scoring_version",
                (
                    telegraph_id,
                    json.dumps(payload.get("sectors", []), ensure_ascii=False),
                    json.dumps(payload.get("companies", []), ensure_ascii=False),
                    json.dumps(payload.get("orgs", []), ensure_ascii=False),
                    json.dumps(payload.get("event_types", []), ensure_ascii=False),
                    payload.get("sentiment", "neutral"),
                    float(payload.get("sentiment_score", 0.0)),
                    version,
                    datetime.now().isoformat(timespec="seconds"),
                    1 if llm_called else 0,
                    float(llm_cost_usd),
                    llm_reasoning,
                    float(importance_score),
                    json.dumps(scoring_components, ensure_ascii=False) if scoring_components else None,
                    scoring_version,
                ),
            )

    def log_llm_call(self, telegraph_id: int | None, model: str, tokens_in: int,
                     tokens_out: int, cost_usd: float, latency_ms: int,
                     error: str | None) -> None:
        with self.transaction() as cur:
            cur.execute(
                "INSERT INTO llm_call_log "
                "(called_at, telegraph_id, model, tokens_in, tokens_out, cost_usd, latency_ms, error) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (datetime.now().isoformat(timespec="seconds"),
                 telegraph_id, model, tokens_in, tokens_out, cost_usd, latency_ms, error),
            )

    def fetch_pending_scoring(self, version: str, limit: int = 1000) -> list[dict]:
        """已 enriched 但未按 当前 scoring_version 算分的行(供 backfill 重算)。"""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT t.id, t.pub_dt, e.sectors_json, e.companies_json, "
                "       e.orgs_json, e.event_types_json, e.sentiment, e.sentiment_score "
                "FROM telegraph t JOIN telegraph_enrichment e ON e.telegraph_id = t.id "
                "WHERE e.scoring_version IS NULL OR e.scoring_version != ? "
                "ORDER BY t.id ASC LIMIT ?",
                (version, limit),
            )
            rows = []
            for r in cur.fetchall():
                rows.append({
                    "id": r[0], "pub_dt": r[1],
                    "enrichment": {
                        "sectors": json.loads(r[2]),
                        "companies": json.loads(r[3]),
                        "orgs": json.loads(r[4]),
                        "event_types": json.loads(r[5]),
                        "sentiment": r[6],
                        "sentiment_score": r[7],
                    },
                })
            return rows

    def update_score_only(self, telegraph_id: int, score: float,
                          components: dict, version: str) -> None:
        with self.transaction() as cur:
            cur.execute(
                "UPDATE telegraph_enrichment SET "
                "  importance_score = ?, "
                "  scoring_components_json = ?, "
                "  scoring_version = ? "
                "WHERE telegraph_id = ?",
                (float(score), json.dumps(components, ensure_ascii=False), version, telegraph_id),
            )

    def llm_calls_today(self) -> tuple[int, float]:
        """返回 (今日调用次数, 今日成本 USD)。"""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT COUNT(*), COALESCE(SUM(cost_usd), 0) FROM llm_call_log "
                "WHERE called_at >= date('now', 'localtime')"
            )
            return tuple(cur.fetchone())  # type: ignore

    # ---------- alerts ----------
    def fetch_alert_candidates(self, min_score: float, hours: int, limit: int = 50,
                               only_highlight: bool = False) -> list[dict]:
        """高分且未推过的电报。only_highlight=True 时只看 cls 头条。"""
        with self._lock:
            cur = self._conn.cursor()
            extra = "AND t.is_highlight = 1 " if only_highlight else ""
            cur.execute(
                self._JOINED_SELECT
                + f"WHERE COALESCE(e.importance_score, 0) >= ? {extra}"
                  "  AND t.pub_dt >= datetime('now', ?, 'localtime') "
                  "  AND NOT EXISTS (SELECT 1 FROM alert_log a WHERE a.telegraph_id = t.id) "
                  "ORDER BY e.importance_score DESC, t.pub_dt DESC LIMIT ?",
                (float(min_score), f"-{hours} hours", limit),
            )
            return [self._row_to_dict(r) for r in cur.fetchall()]

    def log_alert(self, telegraph_id: int, score: float, channels: list[str],
                  success: bool, error: str | None) -> None:
        with self.transaction() as cur:
            cur.execute(
                "INSERT OR IGNORE INTO alert_log "
                "(telegraph_id, score, channels_json, sent_at, success, error) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (telegraph_id, float(score), json.dumps(channels, ensure_ascii=False),
                 datetime.now().isoformat(timespec="seconds"),
                 1 if success else 0, error),
            )

    def alerts_today(self) -> int:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM alert_log WHERE sent_at >= date('now', 'localtime')"
            )
            return cur.fetchone()[0] or 0

    # ---------- signals ----------
    def insert_signal(self, computed_at: str, window_hours: int, kind: str, target: str,
                      score: float, direction: str | None,
                      components: dict | None, evidence: list | None) -> None:
        with self.transaction() as cur:
            cur.execute(
                "INSERT OR REPLACE INTO signals "
                "(computed_at, window_hours, kind, target, score, direction, components_json, evidence_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (computed_at, window_hours, kind, target, float(score), direction,
                 json.dumps(components, ensure_ascii=False) if components else None,
                 json.dumps(evidence, ensure_ascii=False) if evidence else None),
            )

    def latest_signals(self, kind: str | None = None, top: int = 20,
                       window_hours: int | None = None) -> list[dict]:
        """最新一次计算的信号(每个 target 取最新一条)。"""
        with self._lock:
            cur = self._conn.cursor()
            sql = (
                "SELECT s.computed_at, s.window_hours, s.kind, s.target, s.score, "
                "       s.direction, s.components_json, s.evidence_json "
                "FROM signals s "
                "WHERE s.computed_at = (SELECT MAX(computed_at) FROM signals "
                "                       WHERE kind = s.kind AND target = s.target "
                "                             AND window_hours = s.window_hours) "
            )
            params: list = []
            if kind:
                sql += "AND s.kind = ? "
                params.append(kind)
            if window_hours is not None:
                sql += "AND s.window_hours = ? "
                params.append(window_hours)
            sql += "ORDER BY s.score DESC LIMIT ?"
            params.append(top)
            cur.execute(sql, params)
            return [self._signal_row_to_dict(r) for r in cur.fetchall()]

    def signal_history(self, kind: str, target: str, days: int = 7,
                       window_hours: int | None = None) -> list[dict]:
        """单 (kind, target) 的时间序列,给 sparkline 用。"""
        with self._lock:
            cur = self._conn.cursor()
            sql = (
                "SELECT computed_at, window_hours, kind, target, score, direction, "
                "       components_json, evidence_json "
                "FROM signals WHERE kind = ? AND target = ? "
                "  AND computed_at >= datetime('now', ?, 'localtime') "
            )
            params = [kind, target, f"-{days} days"]
            if window_hours is not None:
                sql += "AND window_hours = ? "
                params.append(window_hours)
            sql += "ORDER BY computed_at ASC"
            cur.execute(sql, params)
            return [self._signal_row_to_dict(r) for r in cur.fetchall()]

    @staticmethod
    def _signal_row_to_dict(row: tuple) -> dict:
        return {
            "computed_at": row[0], "window_hours": row[1],
            "kind": row[2], "target": row[3], "score": row[4],
            "direction": row[5],
            "components": json.loads(row[6]) if row[6] else None,
            "evidence": json.loads(row[7]) if row[7] else None,
        }

    def signals_sparklines(self, kind: str, targets: list[str], days: int = 7,
                           max_points_per_target: int = 50) -> dict[str, list[float]]:
        """一次查询拿到多个 target 的 score 时间序列(降采样到 max_points_per_target 个点)。
        返回 {target: [score 时间正序 列表]}"""
        if not targets:
            return {}
        with self._lock:
            cur = self._conn.cursor()
            placeholders = ",".join(["?"] * len(targets))
            cur.execute(
                f"SELECT target, score, computed_at FROM signals "
                f"WHERE kind = ? AND target IN ({placeholders}) "
                f"  AND computed_at >= datetime('now', ?, 'localtime') "
                f"ORDER BY target, computed_at ASC",
                [kind, *targets, f"-{days} days"],
            )
            by_target: dict[str, list[float]] = {t: [] for t in targets}
            for tgt, score, _ts in cur.fetchall():
                by_target[tgt].append(float(score))

            # 降采样
            for tgt, arr in by_target.items():
                if len(arr) > max_points_per_target:
                    step = len(arr) / max_points_per_target
                    by_target[tgt] = [arr[int(i * step)] for i in range(max_points_per_target)]
            return by_target

    def signals_count(self) -> int:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT COUNT(*) FROM signals")
            return cur.fetchone()[0] or 0

    def prune_old_signals(self, keep_days: int = 30) -> int:
        """删除 keep_days 之前的信号(防止表无限增长)。"""
        with self.transaction() as cur:
            cur.execute(
                "DELETE FROM signals WHERE computed_at < datetime('now', ?, 'localtime')",
                (f"-{keep_days} days",),
            )
            return cur.rowcount

    # ---------- quotes (futu) ----------
    def upsert_quotes(self, rows: list[dict]) -> int:
        with self.transaction() as cur:
            for r in rows:
                cur.execute(
                    "INSERT INTO quotes "
                    "(code, name, last_price, prev_close, change_rate, turnover, volume, update_time, fetched_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(code) DO UPDATE SET "
                    "  name=excluded.name, last_price=excluded.last_price, "
                    "  prev_close=excluded.prev_close, change_rate=excluded.change_rate, "
                    "  turnover=excluded.turnover, volume=excluded.volume, "
                    "  update_time=excluded.update_time, fetched_at=excluded.fetched_at",
                    (r["code"], r.get("name"), r.get("last_price"), r.get("prev_close"),
                     r.get("change_rate"), r.get("turnover"), r.get("volume"),
                     r.get("update_time"),
                     datetime.now().isoformat(timespec="seconds")),
                )
            return len(rows)

    def get_quotes(self, codes: list[str] | None = None) -> dict[str, dict]:
        """返回 {code: {name, last_price, change_rate, turnover, update_time, fetched_at}}"""
        with self._lock:
            cur = self._conn.cursor()
            if codes:
                placeholders = ",".join(["?"] * len(codes))
                cur.execute(
                    f"SELECT code, name, last_price, prev_close, change_rate, "
                    f"       turnover, volume, update_time, fetched_at "
                    f"FROM quotes WHERE code IN ({placeholders})",
                    codes,
                )
            else:
                cur.execute(
                    "SELECT code, name, last_price, prev_close, change_rate, "
                    "       turnover, volume, update_time, fetched_at FROM quotes"
                )
            out = {}
            for r in cur.fetchall():
                out[r[0]] = {
                    "code": r[0], "name": r[1], "last_price": r[2], "prev_close": r[3],
                    "change_rate": r[4], "turnover": r[5], "volume": r[6],
                    "update_time": r[7], "fetched_at": r[8],
                }
            return out

    def fetch_enrichment_window(self, hours: int) -> list[dict]:
        """拉过去 N 小时所有 enrichment 行(给 signals 算法用)。"""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT t.id, t.pub_dt, t.title, "
                "       e.sectors_json, e.companies_json, e.orgs_json, "
                "       e.event_types_json, e.sentiment, e.sentiment_score, e.importance_score "
                "FROM telegraph t JOIN telegraph_enrichment e ON e.telegraph_id = t.id "
                "WHERE t.pub_dt >= datetime('now', ?, 'localtime') "
                "ORDER BY t.pub_dt ASC",
                (f"-{hours} hours",),
            )
            rows = []
            for r in cur.fetchall():
                rows.append({
                    "id": r[0], "pub_dt": r[1], "title": r[2],
                    "sectors": json.loads(r[3]),
                    "companies": json.loads(r[4]),
                    "orgs": json.loads(r[5]),
                    "event_types": json.loads(r[6]),
                    "sentiment": r[7],
                    "sentiment_score": r[8] or 0.0,
                    "importance_score": r[9] or 0.0,
                })
            return rows

    def fetch_pending_enrichment(self, limit: int = 200, version: str | None = None) -> list[dict]:
        """返回还没 enrichment 的 telegraph 行,或者 enricher_version 不匹配的。"""
        with self._lock:
            cur = self._conn.cursor()
            if version is None:
                cur.execute(
                    "SELECT t.id, t.pub_dt, t.title, t.content, t.is_highlight FROM telegraph t "
                    "LEFT JOIN telegraph_enrichment e ON e.telegraph_id = t.id "
                    "WHERE e.telegraph_id IS NULL "
                    "ORDER BY t.id ASC LIMIT ?",
                    (limit,),
                )
            else:
                cur.execute(
                    "SELECT t.id, t.pub_dt, t.title, t.content, t.is_highlight FROM telegraph t "
                    "LEFT JOIN telegraph_enrichment e ON e.telegraph_id = t.id "
                    "WHERE e.telegraph_id IS NULL OR e.enricher_version != ? "
                    "ORDER BY t.id ASC LIMIT ?",
                    (version, limit),
                )
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    _JOINED_SELECT = (
        "SELECT t.id, t.pub_dt, t.title, t.content, t.fetched_at, "
        "       e.sectors_json, e.companies_json, e.orgs_json, "
        "       e.event_types_json, e.sentiment, e.sentiment_score, "
        "       e.importance_score, e.llm_called, e.llm_reasoning, "
        "       t.is_highlight "
        "FROM telegraph t LEFT JOIN telegraph_enrichment e ON e.telegraph_id = t.id "
    )

    @staticmethod
    def _row_to_dict(row: tuple) -> dict:
        return {
            "id": row[0],
            "pub_dt": row[1],
            "title": row[2],
            "content": row[3],
            "fetched_at": row[4],
            "is_highlight": bool(row[14]),
            "enrichment": None if row[5] is None else {
                "sectors": json.loads(row[5]),
                "companies": json.loads(row[6]),
                "orgs": json.loads(row[7]),
                "event_types": json.loads(row[8]),
                "sentiment": row[9],
                "sentiment_score": row[10],
                "importance_score": row[11] or 0.0,
                "llm_called": bool(row[12]),
                "llm_reasoning": row[13],
            },
        }

    def latest_with_enrichment(self, n: int = 50) -> list[dict]:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(self._JOINED_SELECT + "ORDER BY t.pub_dt DESC, t.id DESC LIMIT ?", (n,))
            return [self._row_to_dict(r) for r in cur.fetchall()]

    def top_by_score(self, n: int = 20, hours: int = 24, min_score: float = 0.0) -> list[dict]:
        """按 importance_score 倒序,最近 N 小时。"""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                self._JOINED_SELECT
                + "WHERE t.pub_dt >= datetime('now', ?, 'localtime') "
                  "  AND COALESCE(e.importance_score, 0) >= ? "
                  "ORDER BY e.importance_score DESC, t.pub_dt DESC LIMIT ?",
                (f"-{hours} hours", float(min_score), n),
            )
            return [self._row_to_dict(r) for r in cur.fetchall()]

    def tag_counts(self, hours: int = 24) -> dict:
        """过去 N 小时各类标签的命中次数(用来填 viewer 的过滤栏)。"""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT e.sectors_json, e.event_types_json, e.sentiment "
                "FROM telegraph t JOIN telegraph_enrichment e ON e.telegraph_id = t.id "
                "WHERE t.pub_dt >= datetime('now', ?, 'localtime')",
                (f"-{hours} hours",),
            )
            sectors: dict[str, int] = {}
            events: dict[str, int] = {}
            sentiments: dict[str, int] = {"positive": 0, "negative": 0, "neutral": 0}
            for s_json, e_json, sent in cur.fetchall():
                for s in json.loads(s_json):
                    sectors[s] = sectors.get(s, 0) + 1
                for e in json.loads(e_json):
                    events[e] = events.get(e, 0) + 1
                if sent in sentiments:
                    sentiments[sent] += 1
            def topn(d, n=20):
                return sorted([{"name": k, "count": v} for k, v in d.items()], key=lambda x: -x["count"])[:n]
            return {"sectors": topn(sectors), "event_types": topn(events), "sentiment": sentiments}
