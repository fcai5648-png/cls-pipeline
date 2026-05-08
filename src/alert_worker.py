"""告警推送 worker:扫高分电报,按配置渠道推送,每条只推一次。

行为:
  - 每 poll_interval_sec 秒查 fetch_alert_candidates(score>=min_score AND 未推过)
  - 启动时只看 lookback_hours_on_start 小时内的(防止重启刷屏)
  - 调度所有 enabled 的 channel,任一成功就算成功(并入 channels_json 列表)
  - 任何 channel 抛异常都回退到 (False, err) 不阻塞其他 channel
  - 推完写 alert_log(telegraph_id UNIQUE 防重)

环境变量:
  CLS_DB_PATH        见 daemon.py
  CLS_ALERT_CONFIG   默认 PROJECT_ROOT/data/alert_config.json
"""
from __future__ import annotations

import json
import logging
import logging.handlers
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import Store  # noqa: E402
from alerts import build_dispatchers, format_alert  # noqa: E402


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.environ.get("CLS_DB_PATH", PROJECT_ROOT / "data" / "cls.db"))
CONFIG_PATH = Path(os.environ.get("CLS_ALERT_CONFIG", PROJECT_ROOT / "data" / "alert_config.json"))
QUOTE_TARGETS_PATH = PROJECT_ROOT / "data" / "dict" / "quote_targets.json"
LOG_PATH = PROJECT_ROOT / "logs" / "alerts.log"

VIEWER_URL = "http://127.0.0.1:8787/"  # 推送里附的查看链接


def load_quote_targets() -> dict:
    if not QUOTE_TARGETS_PATH.exists():
        return {"sector_to_codes": {}, "company_to_code": {}}
    return json.loads(QUOTE_TARGETS_PATH.read_text(encoding="utf-8"))


def quote_summary_for_row(row: dict, targets: dict, store) -> list[str]:
    """根据电报 enrichment 抽相关港美股 quotes,返回 ['CODE ±X.XX%'] 列表(按 |涨跌| 倒序,最多 5 个)。"""
    enr = row.get("enrichment") or {}
    codes: set[str] = set()
    for sec in (enr.get("sectors") or [])[:3]:  # 只取前 3 个板块,避免代码爆炸
        for code in targets.get("sector_to_codes", {}).get(sec, []):
            codes.add(code)
    for co in enr.get("companies") or []:
        code = targets.get("company_to_code", {}).get(co)
        if code:
            codes.add(code)
    if not codes:
        return []
    quotes = store.get_quotes(list(codes))
    out: list[tuple[str, float]] = []
    for code, q in quotes.items():
        cr = q.get("change_rate")
        if cr is None:
            continue
        short = code.replace("HK.", "").replace("US.", "")
        out.append((short, float(cr)))
    out.sort(key=lambda x: -abs(x[1]))  # |涨跌| 大的优先
    return [f"{short} {cr:+.2f}%" for short, cr in out[:5]]


def setup_logging() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("cls-alerts")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.handlers.RotatingFileHandler(LOG_PATH, maxBytes=5 * 1024 * 1024, backupCount=3)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


def load_config(path: Path) -> dict:
    if not path.exists():
        return {
            "min_score": 60, "lookback_hours_on_start": 1, "poll_interval_sec": 30,
            "channels": {"osascript": {"enabled": True, "sound": "Submarine"}},
        }
    return json.loads(path.read_text(encoding="utf-8"))


_should_stop = False


def _on_signal(signum, frame):
    global _should_stop
    _should_stop = True


def dispatch_one(row: dict, dispatchers: list, log: logging.Logger,
                 targets: dict | None = None, store=None) -> tuple[bool, str | None, list[str]]:
    """推一条电报到所有 enabled channel。返回 (任一成功, 综合 error, 成功的 channel 列表)。"""
    quote_summary = []
    if targets and store is not None:
        try:
            quote_summary = quote_summary_for_row(row, targets, store)
        except Exception as e:
            log.warning("quote_summary failed for id=%s: %s", row.get("id"), e)
    title, body = format_alert(row, VIEWER_URL, quote_summary=quote_summary)
    score = (row.get("enrichment") or {}).get("importance_score", 0)
    successes: list[str] = []
    errors: list[str] = []
    for name, fn in dispatchers:
        try:
            ok, err = fn(title, body, score, VIEWER_URL)
            if ok:
                successes.append(name)
            else:
                errors.append(f"{name}: {err}")
        except Exception as e:
            errors.append(f"{name}: {type(e).__name__}: {e}")
    any_ok = bool(successes)
    err_str = " | ".join(errors) if errors else None
    return any_ok, err_str, successes


def main() -> int:
    log = setup_logging()
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    log.info("starting cls-alerts worker (pid=%d)", os.getpid())
    if not DB_PATH.exists():
        log.error("db not found: %s — daemon must run first", DB_PATH)
        return 1

    cfg = load_config(CONFIG_PATH)
    min_score = float(cfg.get("min_score", 60))
    interval = int(cfg.get("poll_interval_sec", 30))
    lookback_hours = int(cfg.get("lookback_hours_on_start", 1))
    channels_cfg = cfg.get("channels", {})

    dispatchers = build_dispatchers(channels_cfg)
    enabled_names = [n for n, _ in dispatchers]
    log.info("  config       = %s", CONFIG_PATH)
    log.info("  min_score    = %.0f", min_score)
    log.info("  poll         = %ds", interval)
    log.info("  lookback     = %dh (sliding window)", lookback_hours)
    log.info("  channels     = %s", enabled_names or "(none enabled — 这台机器不会响)")

    if not dispatchers:
        log.warning("no enabled channels — worker will run but never send anything. 编辑 %s 启用至少一个 channel", CONFIG_PATH)

    targets = load_quote_targets()
    has_quotes = bool(targets.get("sector_to_codes") or targets.get("company_to_code"))
    log.info("  quotes       = %s (告警将含港美股涨跌)" if has_quotes else "  quotes       = none",
             "enabled" if has_quotes else "disabled")

    store = Store(DB_PATH)

    # 滑动窗口:每轮只看 lookback_hours 内的(默认 1h)
    # 评分有 freshness decay,1h 之后即使是大利好也基本不会再上 60 阈值
    # 这个窗口同时是"防止重启刷屏"和"日常推送"的语义
    consec_errors = 0
    while not _should_stop:
        cycle_start = time.monotonic()
        try:
            candidates = store.fetch_alert_candidates(min_score=min_score, hours=lookback_hours, limit=20)
            for row in candidates:
                if _should_stop: break
                tid = row["id"]
                score = (row.get("enrichment") or {}).get("importance_score", 0)
                if dispatchers:
                    ok, err, channels = dispatch_one(row, dispatchers, log,
                                                      targets=targets, store=store)
                    store.log_alert(tid, score, channels, ok, err)
                    log.info("sent id=%s score=%.0f channels=%s%s",
                             tid, score, channels,
                             f" / err: {err}" if err else "")
                else:
                    # 没有 channel:静默 mark 为已处理(避免日后改配置后突然刷屏)
                    store.log_alert(tid, score, [], False, "no enabled channels at send time")
            consec_errors = 0
        except Exception as e:
            consec_errors += 1
            log.exception("alert cycle failed (streak=%d): %s", consec_errors, e)
            if consec_errors >= 10:
                log.error("too many failures, exit for launchd to restart")
                store.close()
                return 1

        elapsed = time.monotonic() - cycle_start
        sleep_s = max(0.0, interval - elapsed)
        slept = 0.0
        while slept < sleep_s and not _should_stop:
            time.sleep(min(1.0, sleep_s - slept))
            slept += 1.0

    log.info("received stop signal, exiting cleanly")
    store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
