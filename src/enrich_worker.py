"""Enrichment worker:扫描 telegraph 表里未抽取的行,用 enrich.py 规则引擎填 telegraph_enrichment。

模式:
  - 启动:全量 backfill 一遍(版本号变化时也会重抽)
  - 持续:每 SLEEP 秒扫一次新条目,实时跟进 daemon 拉的新数据

环境变量:
  CLS_DB_PATH        见 daemon.py
  CLS_ENRICH_INTERVAL  默认 20 秒
  CLS_ENRICH_BATCH     默认 200 条/批
"""
from __future__ import annotations

import logging
import logging.handlers
import os
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import Store  # noqa: E402
from enrich import default_enricher, ENRICHER_VERSION  # noqa: E402
from llm_enrich import LLMEnricher, merge_rule_and_llm, needs_llm  # noqa: E402
from scoring import default_scorer, SCORING_VERSION  # noqa: E402


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.environ.get("CLS_DB_PATH", PROJECT_ROOT / "data" / "cls.db"))
LOG_PATH = Path(os.environ.get("CLS_ENRICH_LOG_PATH", PROJECT_ROOT / "logs" / "enrich.log"))
INTERVAL_SEC = int(os.environ.get("CLS_ENRICH_INTERVAL", "20"))
BATCH_SIZE = int(os.environ.get("CLS_ENRICH_BATCH", "200"))


def setup_logging() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("cls-enrich")
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


def process_batch(store: Store, enricher, llm: LLMEnricher | None, scorer,
                  version: str, batch_size: int, log: logging.Logger) -> tuple[int, int, float]:
    """处理一批未抽取的数据。返回 (处理条数, LLM 调用数, LLM 总成本)。"""
    pending = store.fetch_pending_enrichment(limit=batch_size, version=version)
    if not pending:
        return 0, 0, 0.0

    allowed_sectors = {c for c, _ in enricher._sectors}
    allowed_event_types = {c for c, _ in enricher._event_types}
    allowed_orgs = {c for c, _ in enricher._orgs}

    t0 = time.monotonic()
    llm_calls = 0
    llm_cost = 0.0

    for row in pending:
        tid = row.get("id")
        title = row.get("title", "")
        content = row.get("content", "")
        try:
            rule_payload = enricher.enrich(title, content)
            llm_payload = None
            llm_telemetry = None

            if llm is not None and llm.enabled:
                should, _reason = needs_llm(rule_payload, content)
                if should:
                    llm_payload, llm_telemetry = llm.enrich(
                        title, content,
                        allowed_sectors, allowed_event_types, allowed_orgs,
                        telegraph_id=tid,
                    )
                    if llm_telemetry["called"]:
                        llm_calls += 1
                        llm_cost += llm_telemetry["cost_usd"]

            merged = merge_rule_and_llm(rule_payload, llm_payload)

            # 评分(基于 enrichment 结果 + pub_dt 时间衰减)
            score, components = scorer.score(merged, row.get("pub_dt", ""))

            store.upsert_enrichment(
                tid, merged, version,
                llm_called=(llm_payload is not None),
                llm_cost_usd=(llm_telemetry["cost_usd"] if llm_telemetry else 0.0),
                llm_reasoning=(llm_payload.get("reasoning") if llm_payload else None),
                importance_score=score,
                scoring_components=components,
                scoring_version=SCORING_VERSION,
            )
        except Exception as e:
            log.exception("enrich failed for id=%s: %s", tid, e)

    duration_ms = int((time.monotonic() - t0) * 1000)
    avg_ms = duration_ms / max(len(pending), 1)
    if llm_calls > 0:
        log.info("processed %d rows in %dms (avg %.1fms) — LLM called %d times, $%.4f",
                 len(pending), duration_ms, avg_ms, llm_calls, llm_cost)
    else:
        log.info("processed %d rows in %dms (avg %.1fms/row)",
                 len(pending), duration_ms, avg_ms)
    return len(pending), llm_calls, llm_cost


def main() -> int:
    log = setup_logging()
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    log.info("starting cls-enrich worker")
    log.info("  db_path     = %s", DB_PATH)
    log.info("  interval    = %ds", INTERVAL_SEC)
    log.info("  batch       = %d", BATCH_SIZE)
    log.info("  version     = %s", ENRICHER_VERSION)
    log.info("  pid         = %d", os.getpid())

    if not DB_PATH.exists():
        log.error("db not found: %s — daemon must run first", DB_PATH)
        return 1

    store = Store(DB_PATH)
    enricher = default_enricher(PROJECT_ROOT)

    # LLM 配置(可选);文件不存在 / key 缺 → 退化为纯规则模式
    llm_config_path = PROJECT_ROOT / "data" / "llm_config.json"
    llm = LLMEnricher.from_config_file(llm_config_path, store=store)
    if llm is not None and llm.enabled:
        log.info("  llm         = %s/%s, daily_limit=%d (config: %s)",
                 llm.config.provider, llm.config.model, llm.config.daily_call_limit, llm_config_path)
    else:
        log.info("  llm         = disabled (no %s or no api_key)", llm_config_path)

    # 评分引擎
    scorer = default_scorer(PROJECT_ROOT)
    log.info("  scoring     = %s, watchlist=%d sectors / %d companies",
             SCORING_VERSION, len(scorer.sectors), len(scorer.companies))

    # 启动 1:enrichment 缺失的全量抽
    total_backfilled = 0
    total_llm_calls = 0
    total_llm_cost = 0.0
    while not _should_stop:
        n, lc, cost = process_batch(store, enricher, llm, scorer, ENRICHER_VERSION, BATCH_SIZE, log)
        total_backfilled += n
        total_llm_calls += lc
        total_llm_cost += cost
        if n < BATCH_SIZE:
            break
    if total_backfilled > 0:
        log.info("backfill enrichment: %d rows, LLM %d calls $%.4f",
                 total_backfilled, total_llm_calls, total_llm_cost)
    else:
        log.info("nothing to enrich-backfill")

    # 启动 2:scoring 缺失或版本旧的重新算分(不重抽 enrichment,只更新 score 列)
    total_rescored = 0
    while not _should_stop:
        pending = store.fetch_pending_scoring(SCORING_VERSION, limit=1000)
        if not pending:
            break
        for row in pending:
            score, components = scorer.score(row["enrichment"], row.get("pub_dt", ""))
            store.update_score_only(row["id"], score, components, SCORING_VERSION)
        total_rescored += len(pending)
        if len(pending) < 1000:
            break
    if total_rescored > 0:
        log.info("rescored %d rows to %s", total_rescored, SCORING_VERSION)

    # 持续:每 INTERVAL_SEC 扫新条目
    consec_errors = 0
    while not _should_stop:
        cycle_start = time.monotonic()
        try:
            n, _lc, _cost = process_batch(store, enricher, llm, scorer, ENRICHER_VERSION, BATCH_SIZE, log)
            consec_errors = 0
            if n == 0:
                log.debug("no new rows")
        except Exception as e:
            consec_errors += 1
            log.exception("batch failed (streak=%d): %s", consec_errors, e)
            if consec_errors >= 10:
                log.error("too many failures, exit for launchd to restart")
                store.close()
                return 1

        elapsed = time.monotonic() - cycle_start
        sleep_s = max(0.0, INTERVAL_SEC - elapsed)
        slept = 0.0
        while slept < sleep_s and not _should_stop:
            time.sleep(min(1.0, sleep_s - slept))
            slept += 1.0

    log.info("received stop signal, exiting cleanly")
    store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
