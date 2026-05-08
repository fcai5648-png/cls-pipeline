"""信号计算 worker:每 15 分钟扫一次,产出 4 类信号。

行为:
  - 加载 watchlist
  - 拉过去 24h 的 enrichment(信号 1+2+3 共用窗口)
  - 拉过去 4h 的 enrichment + 7 天历史窗口样本(信号 4 用)
  - 对每个 (kind, target) 写一行 signal(UNIQUE 防重)
  - 每天清一次老于 30d 的信号(防表无限增长)

环境变量:
  CLS_DB_PATH                  默认 ../data/cls.db
  CLS_SIGNAL_INTERVAL_SEC      默认 900 (15 分钟)
"""
from __future__ import annotations

import json
import logging
import logging.handlers
import os
import signal
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import Store  # noqa: E402
import signals as sig_mod  # noqa: E402


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.environ.get("CLS_DB_PATH", PROJECT_ROOT / "data" / "cls.db"))
LOG_PATH = PROJECT_ROOT / "logs" / "signals.log"
INTERVAL_SEC = int(os.environ.get("CLS_SIGNAL_INTERVAL_SEC", "900"))  # 15 min

WINDOW_HOURS_MAIN = 24       # sector_sentiment / company_heat / event_cluster 共用
WINDOW_HOURS_ANOMALY = 4     # 异动检测窗口
BASELINE_DAYS = 7

PRUNE_AFTER_DAYS = 30


def setup_logging() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("cls-signals")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.handlers.RotatingFileHandler(LOG_PATH, maxBytes=5 * 1024 * 1024, backupCount=3)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


_should_stop = False


def _on_signal(signum, frame):
    global _should_stop
    _should_stop = True


def load_watchlist() -> tuple[set[str], set[str]]:
    p = PROJECT_ROOT / "data" / "dict" / "watchlist.json"
    if not p.exists():
        return set(), set()
    data = json.loads(p.read_text(encoding="utf-8"))
    return set(data.get("sectors", [])), set(data.get("companies", []))


def build_anomaly_baseline(store: Store) -> list[dict[str, int]]:
    """从过去 7 天数据采样多个 4h 窗口,得到每个 sector 在 4h 窗口内的频次分布。"""
    samples: list[dict[str, int]] = []
    now = datetime.now()
    # 每天采 6 个互不重叠的 4h 窗口 → 7×6 = 42 个样本
    for day in range(BASELINE_DAYS):
        for slot in range(0, 24, WINDOW_HOURS_ANOMALY):
            slot_end = now - timedelta(days=day, hours=slot)
            slot_start = slot_end - timedelta(hours=WINDOW_HOURS_ANOMALY)
            # 这个 slot 跟现在窗口重叠就跳过(避免拿"未来"做基线)
            if slot_end > now - timedelta(hours=WINDOW_HOURS_ANOMALY):
                continue
            counts: dict[str, int] = {}
            with store._lock:
                cur = store._conn.cursor()
                cur.execute(
                    "SELECT e.sectors_json FROM telegraph t "
                    "JOIN telegraph_enrichment e ON e.telegraph_id = t.id "
                    "WHERE t.pub_dt >= ? AND t.pub_dt < ?",
                    (slot_start.strftime("%Y-%m-%d %H:%M:%S"),
                     slot_end.strftime("%Y-%m-%d %H:%M:%S")),
                )
                for (s_json,) in cur.fetchall():
                    for s in json.loads(s_json):
                        counts[s] = counts.get(s, 0) + 1
            if counts:  # 空窗口(可能晚上)跳过
                samples.append(counts)
    return samples


def compute_cycle(store: Store, log: logging.Logger) -> dict:
    """跑一轮信号计算,返回各类信号数量。"""
    watch_sectors, watch_companies = load_watchlist()
    now_iso = datetime.now().isoformat(timespec="seconds")

    rows_24h = store.fetch_enrichment_window(WINDOW_HOURS_MAIN)
    rows_recent = store.fetch_enrichment_window(WINDOW_HOURS_ANOMALY)

    counts = {"sector_sentiment": 0, "company_heat": 0, "event_cluster": 0, "sector_anomaly": 0}

    if rows_24h:
        for s in sig_mod.compute_sector_sentiment(rows_24h, watch_sectors):
            store.insert_signal(now_iso, WINDOW_HOURS_MAIN, s["kind"], s["target"],
                                s["score"], s["direction"], s["components"], s["evidence"])
            counts["sector_sentiment"] += 1
        for s in sig_mod.compute_company_heat(rows_24h, watch_companies):
            store.insert_signal(now_iso, WINDOW_HOURS_MAIN, s["kind"], s["target"],
                                s["score"], s["direction"], s["components"], s["evidence"])
            counts["company_heat"] += 1
        for s in sig_mod.compute_event_cluster(rows_24h, min_count=3):
            store.insert_signal(now_iso, WINDOW_HOURS_MAIN, s["kind"], s["target"],
                                s["score"], s["direction"], s["components"], s["evidence"])
            counts["event_cluster"] += 1

    # anomaly:需要历史基线
    baseline = build_anomaly_baseline(store)
    if rows_recent and len(baseline) >= 7:
        for s in sig_mod.compute_sector_anomaly(rows_recent, baseline,
                                                 recent_hours=WINDOW_HOURS_ANOMALY,
                                                 baseline_days=BASELINE_DAYS):
            store.insert_signal(now_iso, WINDOW_HOURS_ANOMALY, s["kind"], s["target"],
                                s["score"], s["direction"], s["components"], s["evidence"])
            counts["sector_anomaly"] += 1
    else:
        log.debug("anomaly skipped: baseline samples=%d (need ≥7)", len(baseline))

    return counts


def main() -> int:
    log = setup_logging()
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    log.info("starting cls-signals worker (pid=%d)", os.getpid())
    if not DB_PATH.exists():
        log.error("db not found: %s — daemon must run first", DB_PATH)
        return 1

    log.info("  db_path     = %s", DB_PATH)
    log.info("  interval    = %ds (%.1f min)", INTERVAL_SEC, INTERVAL_SEC / 60)
    log.info("  windows     = %dh main / %dh anomaly", WINDOW_HOURS_MAIN, WINDOW_HOURS_ANOMALY)

    store = Store(DB_PATH)
    log.info("  signals existing = %d rows", store.signals_count())

    consec_errors = 0
    last_prune_day = None
    while not _should_stop:
        cycle_start = time.monotonic()
        try:
            counts = compute_cycle(store, log)
            total = sum(counts.values())
            log.info("computed %d signals (sector=%d company=%d cluster=%d anomaly=%d)",
                     total, counts["sector_sentiment"], counts["company_heat"],
                     counts["event_cluster"], counts["sector_anomaly"])

            today = datetime.now().date()
            if last_prune_day != today:
                pruned = store.prune_old_signals(keep_days=PRUNE_AFTER_DAYS)
                if pruned:
                    log.info("pruned %d old signals (older than %d days)", pruned, PRUNE_AFTER_DAYS)
                last_prune_day = today

            consec_errors = 0
        except Exception as e:
            consec_errors += 1
            log.exception("cycle failed (streak=%d): %s", consec_errors, e)
            if consec_errors >= 5:
                log.error("too many failures, exit for launchd to restart")
                store.close()
                return 1

        elapsed = time.monotonic() - cycle_start
        sleep_s = max(0.0, INTERVAL_SEC - elapsed)
        slept = 0.0
        while slept < sleep_s and not _should_stop:
            time.sleep(min(2.0, sleep_s - slept))
            slept += 2.0

    log.info("received stop signal, exiting cleanly")
    store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
