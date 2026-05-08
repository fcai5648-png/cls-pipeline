"""7*24 守护进程主循环。

行为:
  - 每 INTERVAL_SEC 秒拉一次财联社电报(默认 30s)
  - 入库 + 去重(content_hash UNIQUE)
  - 每次拉取记 fetch_log
  - 处理 SIGTERM / SIGINT 优雅退出(launchd unload 时会发 SIGTERM)
  - 任意未捕获异常 → 打日志 → 进程退出 → launchd KeepAlive 重启

环境变量:
  CLS_DB_PATH        默认 ~/projects/cls-pipeline/data/cls.db
  CLS_INTERVAL_SEC   默认 30
  CLS_LOG_PATH       默认 ~/projects/cls-pipeline/logs/daemon.log
"""
from __future__ import annotations

import logging
import logging.handlers
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

# 让 src 能 import 兄弟模块(直接 python src/daemon.py 也能跑)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import Store  # noqa: E402
from fetcher import fetch_with_retry  # noqa: E402


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.environ.get("CLS_DB_PATH", PROJECT_ROOT / "data" / "cls.db"))
LOG_PATH = Path(os.environ.get("CLS_LOG_PATH", PROJECT_ROOT / "logs" / "daemon.log"))
INTERVAL_SEC = int(os.environ.get("CLS_INTERVAL_SEC", "30"))


def setup_logging() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("cls-daemon")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    # 滚动日志:10MB x 5 份
    fh = logging.handlers.RotatingFileHandler(LOG_PATH, maxBytes=10 * 1024 * 1024, backupCount=5)
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


def main() -> int:
    log = setup_logging()
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    log.info("starting cls-pipeline daemon")
    log.info("  db_path     = %s", DB_PATH)
    log.info("  interval    = %ds", INTERVAL_SEC)
    log.info("  log_path    = %s", LOG_PATH)
    log.info("  pid         = %d", os.getpid())

    store = Store(DB_PATH)
    initial_total = store.total()
    log.info("  total rows  = %d (existing)", initial_total)

    consec_errors = 0
    seen_any_dedup = initial_total > 0  # 库非空时,新一轮 20/20 才算可疑(冷启动 20/20 是正常)
    while not _should_stop:
        cycle_start = time.monotonic()
        t0 = time.monotonic()
        rows, err = fetch_with_retry(max_attempts=3)
        duration_ms = int((time.monotonic() - t0) * 1000)

        if err:
            consec_errors += 1
            log.warning("fetch failed (attempt streak=%d): %s", consec_errors, err)
            try:
                store.log_fetch(0, 0, duration_ms, err)
            except Exception as e:
                log.error("log_fetch failed: %s", e)
        else:
            consec_errors = 0
            try:
                new_count = store.insert_batch(rows)
                store.log_fetch(len(rows), new_count, duration_ms, None)
                if new_count > 0:
                    log.info("fetched %d, new %d (%.0fms)", len(rows), new_count, duration_ms)
                else:
                    log.debug("fetched %d, new 0 (%.0fms)", len(rows), duration_ms)
                # 警告:库已经有数据后,如果一次 20 条全新,说明上次拉到现在间隔太长丢条了
                if seen_any_dedup and new_count == len(rows) and len(rows) >= 20:
                    log.warning("ALL %d rows new — possible miss; consider lowering interval", len(rows))
                if new_count < len(rows):
                    seen_any_dedup = True
            except Exception as e:
                log.exception("insert/log failed: %s", e)

        # 严重故障保护:连续 20 次失败(~10 分钟)才退出,让 launchd 重启
        # 普通网络抖动能自愈,不用每次都 churn
        if consec_errors >= 20:
            log.error("consecutive errors=%d, exiting for launchd to restart", consec_errors)
            store.close()
            return 1

        # 等到下一次窗口(对齐到 INTERVAL_SEC,但不强制——简单 sleep 余量即可)
        elapsed = time.monotonic() - cycle_start
        sleep_s = max(0.0, INTERVAL_SEC - elapsed)
        # 拆 1 秒粒度 sleep,信号能更快终止
        slept = 0.0
        while slept < sleep_s and not _should_stop:
            time.sleep(min(1.0, sleep_s - slept))
            slept += 1.0

    log.info("received stop signal, exiting cleanly")
    store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
