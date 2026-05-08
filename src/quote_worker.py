"""富途行情 worker — 90s 拉一次 quote_targets.json 涉及的所有代码,缓存到 quotes 表。

环境变量:
  CLS_DB_PATH                  默认 ../data/cls.db
  CLS_QUOTE_INTERVAL_SEC       默认 90
  CLS_QUOTE_HOST / PORT        默认 127.0.0.1 / 11111
"""
from __future__ import annotations

import json
import logging
import logging.handlers
import os
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import Store  # noqa: E402
from futu_quote import FutuQuote  # noqa: E402


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.environ.get("CLS_DB_PATH", PROJECT_ROOT / "data" / "cls.db"))
LOG_PATH = PROJECT_ROOT / "logs" / "quotes.log"
INTERVAL_SEC = int(os.environ.get("CLS_QUOTE_INTERVAL_SEC", "90"))
HOST = os.environ.get("CLS_QUOTE_HOST", "127.0.0.1")
PORT = int(os.environ.get("CLS_QUOTE_PORT", "11111"))

QUOTE_TARGETS_PATH = PROJECT_ROOT / "data" / "dict" / "quote_targets.json"


def setup_logging() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("cls-quotes")
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


def load_target_codes() -> list[str]:
    """聚合 sector_to_codes 和 company_to_code 的所有代码,去重。"""
    if not QUOTE_TARGETS_PATH.exists():
        return []
    data = json.loads(QUOTE_TARGETS_PATH.read_text(encoding="utf-8"))
    codes: set[str] = set()
    for arr in data.get("sector_to_codes", {}).values():
        if isinstance(arr, list):
            codes.update(c for c in arr if c)
    for c in data.get("company_to_code", {}).values():
        if c:
            codes.add(c)
    return sorted(codes)


def main() -> int:
    log = setup_logging()
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    log.info("starting cls-quotes worker (pid=%d)", os.getpid())
    if not DB_PATH.exists():
        log.error("db not found: %s — daemon must run first", DB_PATH)
        return 1

    log.info("  db_path     = %s", DB_PATH)
    log.info("  interval    = %ds", INTERVAL_SEC)
    log.info("  futu        = %s:%d", HOST, PORT)

    codes = load_target_codes()
    log.info("  codes       = %d (HK %d, US %d)",
             len(codes),
             sum(1 for c in codes if c.startswith("HK.")),
             sum(1 for c in codes if c.startswith("US.")))

    if not codes:
        log.error("no codes loaded from %s — nothing to fetch", QUOTE_TARGETS_PATH)
        return 1

    store = Store(DB_PATH)
    consec_errors = 0

    invalid_codes: set[str] = set()
    while not _should_stop:
        cycle_start = time.monotonic()
        try:
            valid_codes = [c for c in codes if c not in invalid_codes]
            with FutuQuote(HOST, PORT) as fq:
                t0 = time.monotonic()
                rows, invalid = fq.get_snapshot(valid_codes)
                duration_ms = int((time.monotonic() - t0) * 1000)
                store.upsert_quotes(rows)
            if invalid:
                invalid_codes.update(invalid)
                log.warning("dropping invalid codes (will skip from now on): %s", invalid)
            log.info("fetched %d quotes in %dms (skipped %d invalid)",
                     len(rows), duration_ms, len(invalid_codes))
            consec_errors = 0
        except Exception as e:
            consec_errors += 1
            log.exception("fetch failed (streak=%d): %s", consec_errors, e)
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
