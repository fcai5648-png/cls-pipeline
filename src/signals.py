"""信号计算引擎 — 每 15 分钟由 signal_worker 调用。

输入:某时间窗口内所有 enrichment 行(从 db.fetch_enrichment_window 拿)
输出:list[dict],每个 dict 是一个信号快照(kind / target / score / direction / components / evidence)

算法(MVP):
  1. sector_sentiment:每个板块的累计重要性 + 加权情绪 + 提及次数
  2. company_heat:关注公司的活跃度(频次 + 累计分 + 事件多样性)
  3. event_cluster:同一(板块, 事件)对密集出现 → 强催化
  4. sector_anomaly:板块电报频次 vs 7d 基线的 z-score(需要历史数据)

下游消费方式:
  - viewer 信号面板
  - 选股项目按 sector_sentiment 选板块
  - daily-brief 按 event_cluster 写"今日重大事件"段落
"""
from __future__ import annotations

import math
import statistics
from collections import defaultdict
from datetime import datetime
from typing import Iterable


def _direction_from_score(avg_sentiment: float, threshold: float = 0.15) -> str:
    if avg_sentiment > threshold:
        return "bullish"
    if avg_sentiment < -threshold:
        return "bearish"
    return "neutral"


def compute_sector_sentiment(
    rows: list[dict],
    watchlist_sectors: set[str],
    sector_hit_boost: float = 1.5,
    min_count: int = 1,
) -> list[dict]:
    """每个板块算累计分 + 平均情绪。score 主要由累计 importance 主导。"""
    by_sector: dict[str, dict] = defaultdict(
        lambda: {"count": 0, "sentiments": [], "imp_sum": 0.0, "evidence": []}
    )
    for r in rows:
        for sec in r["sectors"]:
            d = by_sector[sec]
            d["count"] += 1
            d["sentiments"].append(r["sentiment_score"])
            d["imp_sum"] += r["importance_score"]
            if len(d["evidence"]) < 5:  # 留 5 条最新作为证据
                d["evidence"].append({"id": r["id"], "title": r["title"][:60], "score": r["importance_score"]})
    out = []
    for sec, d in by_sector.items():
        if d["count"] < min_count:
            continue
        avg_sent = statistics.fmean(d["sentiments"]) if d["sentiments"] else 0.0
        boost = sector_hit_boost if sec in watchlist_sectors else 1.0
        score = d["imp_sum"] * boost
        out.append({
            "kind": "sector_sentiment",
            "target": sec,
            "score": round(score, 1),
            "direction": _direction_from_score(avg_sent),
            "components": {
                "count": d["count"],
                "avg_sentiment": round(avg_sent, 3),
                "imp_sum": round(d["imp_sum"], 1),
                "watchlist": sec in watchlist_sectors,
                "boost": boost,
            },
            "evidence": d["evidence"],
        })
    out.sort(key=lambda x: -x["score"])
    return out


def compute_company_heat(
    rows: list[dict],
    watchlist_companies: set[str],
    only_watchlist: bool = True,
) -> list[dict]:
    """公司活跃度。only_watchlist=True 时只算 watchlist 里的公司(降噪)。"""
    by_co: dict[str, dict] = defaultdict(
        lambda: {"count": 0, "sentiments": [], "imp_sum": 0.0, "events": set(), "evidence": []}
    )
    for r in rows:
        for co in r["companies"]:
            if only_watchlist and co not in watchlist_companies:
                continue
            d = by_co[co]
            d["count"] += 1
            d["sentiments"].append(r["sentiment_score"])
            d["imp_sum"] += r["importance_score"]
            d["events"].update(r["event_types"])
            if len(d["evidence"]) < 5:
                d["evidence"].append({"id": r["id"], "title": r["title"][:60], "events": r["event_types"]})
    out = []
    for co, d in by_co.items():
        if d["count"] < 1:
            continue
        avg_sent = statistics.fmean(d["sentiments"]) if d["sentiments"] else 0.0
        # 热度 = 频次 × 10 + 重要性累计 × 0.5 + 事件多样性 × 5
        score = d["count"] * 10 + d["imp_sum"] * 0.5 + len(d["events"]) * 5
        out.append({
            "kind": "company_heat",
            "target": co,
            "score": round(score, 1),
            "direction": _direction_from_score(avg_sent),
            "components": {
                "count": d["count"],
                "avg_sentiment": round(avg_sent, 3),
                "imp_sum": round(d["imp_sum"], 1),
                "event_diversity": len(d["events"]),
                "events": sorted(d["events"]),
            },
            "evidence": d["evidence"],
        })
    out.sort(key=lambda x: -x["score"])
    return out


def compute_event_cluster(
    rows: list[dict],
    min_count: int = 3,
) -> list[dict]:
    """同一(板块, 事件)对的密集出现。只输出 count >= min_count 的。"""
    by_pair: dict[tuple[str, str], dict] = defaultdict(
        lambda: {"count": 0, "sentiments": [], "imp_sum": 0.0, "evidence": []}
    )
    for r in rows:
        sectors = r["sectors"] or [None]  # 没板块的事件也聚一次("any" 板块)
        for sec in r["sectors"]:
            for ev in r["event_types"]:
                key = (sec, ev)
                d = by_pair[key]
                d["count"] += 1
                d["sentiments"].append(r["sentiment_score"])
                d["imp_sum"] += r["importance_score"]
                if len(d["evidence"]) < 4:
                    d["evidence"].append({"id": r["id"], "title": r["title"][:60]})
    out = []
    for (sec, ev), d in by_pair.items():
        if d["count"] < min_count:
            continue
        avg_sent = statistics.fmean(d["sentiments"]) if d["sentiments"] else 0.0
        # 聚类强度 = 频次 × 8 + 累计重要性 × 0.5
        score = d["count"] * 8 + d["imp_sum"] * 0.5
        out.append({
            "kind": "event_cluster",
            "target": f"{sec}|{ev}",   # 用 | 分隔板块和事件
            "score": round(score, 1),
            "direction": _direction_from_score(avg_sent),
            "components": {
                "sector": sec,
                "event_type": ev,
                "count": d["count"],
                "avg_sentiment": round(avg_sent, 3),
                "imp_sum": round(d["imp_sum"], 1),
            },
            "evidence": d["evidence"],
        })
    out.sort(key=lambda x: -x["score"])
    return out


def compute_sector_anomaly(
    rows_recent: list[dict],
    baseline_history: list[dict],
    recent_hours: int = 4,
    baseline_days: int = 7,
    min_baseline_samples: int = 7,
) -> list[dict]:
    """板块电报频次的 z-score 异动。
    baseline_history:过去 baseline_days × 24/recent_hours 个 recent_hours 窗口的 sector 计数样本。
    每个样本是 {sector: count}。
    """
    if len(baseline_history) < min_baseline_samples:
        return []  # 基线不足

    recent_count: dict[str, int] = defaultdict(int)
    for r in rows_recent:
        for s in r["sectors"]:
            recent_count[s] += 1

    # 统计基线均值/标准差
    sectors_seen = set()
    for sample in baseline_history:
        sectors_seen.update(sample.keys())
    sectors_seen.update(recent_count.keys())

    out = []
    for sec in sectors_seen:
        samples = [s.get(sec, 0) for s in baseline_history]
        if len(samples) < min_baseline_samples:
            continue
        mu = statistics.fmean(samples)
        sigma = statistics.pstdev(samples)
        if sigma < 0.01:  # 几乎没波动,可能没意义
            continue
        cur = recent_count.get(sec, 0)
        z = (cur - mu) / sigma
        if abs(z) < 1.5:  # 不是异动
            continue
        out.append({
            "kind": "sector_anomaly",
            "target": sec,
            "score": round(z, 2),
            "direction": "bullish" if z > 0 else "bearish",
            "components": {
                "current_count": cur,
                "baseline_mean": round(mu, 2),
                "baseline_std": round(sigma, 2),
                "z_score": round(z, 2),
                "window_hours": recent_hours,
                "baseline_days": baseline_days,
                "samples_used": len(samples),
            },
            "evidence": None,
        })
    out.sort(key=lambda x: -abs(x["score"]))
    return out


def compute_all(
    rows: list[dict],
    watchlist_sectors: set[str],
    watchlist_companies: set[str],
) -> list[dict]:
    """一站式 — 计算 1+2+3 三类信号(anomaly 需要历史基线,worker 单独算)。"""
    return (
        compute_sector_sentiment(rows, watchlist_sectors)
        + compute_company_heat(rows, watchlist_companies)
        + compute_event_cluster(rows, min_count=3)
    )
