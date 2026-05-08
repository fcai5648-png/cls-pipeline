#!/usr/bin/env python3
"""信号有效性回测。

逻辑:对每个历史 sector_sentiment 信号(过去 N 天),
   - 比较信号产生时刻的 direction
   - 与该板块在信号发出后 4h 内的电报情绪均值
   - 如果同向 → 信号被验证(有效);反向或转中性 → 失效

输出:
  - 总体准确率(每 kind 分别)
  - 按 direction(bullish / bearish / neutral)细分
  - Top 10 表现最好的板块 / Top 10 最差

用法:
  python scripts/backtest_signals.py [days]   # 默认回测过去 7 天
"""
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

DB = Path(__file__).resolve().parent.parent / "data" / "cls.db"
DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 7
WINDOW_AFTER_HOURS = 4   # 信号后多久看效果

c = sqlite3.connect(str(DB))


def fetch_signal_history(kind: str):
    cur = c.execute(
        "SELECT computed_at, target, direction, components_json "
        "FROM signals WHERE kind = ? "
        "  AND computed_at >= datetime('now', ?, 'localtime') "
        "ORDER BY computed_at",
        (kind, f"-{DAYS} days"),
    )
    return cur.fetchall()


def fetch_post_signal_sentiment(target: str, signal_time: str, hours: int) -> tuple[int, float]:
    """信号发出后 hours 内,该板块电报数 + 平均情绪。"""
    cur = c.execute(
        "SELECT COUNT(*), COALESCE(AVG(e.sentiment_score), 0) "
        "FROM telegraph t JOIN telegraph_enrichment e ON e.telegraph_id = t.id "
        "WHERE t.pub_dt > ? "
        "  AND t.pub_dt <= datetime(?, '+' || ? || ' hours') "
        "  AND e.sectors_json LIKE ?",
        (signal_time, signal_time, hours, f'%"{target}"%'),
    )
    n, avg = cur.fetchone()
    return n or 0, avg or 0.0


def direction_from_avg(avg: float) -> str:
    if avg > 0.15: return "bullish"
    if avg < -0.15: return "bearish"
    return "neutral"


def backtest_kind(kind: str, target_field: str = None):
    print(f"\n{'='*60}")
    print(f"回测:{kind} (过去 {DAYS} 天)")
    print(f"{'='*60}")

    hist = fetch_signal_history(kind)
    if not hist:
        print(f"  无 {kind} 历史信号(可能 worker 还没跑够久)")
        return

    bydir = defaultdict(lambda: {"total": 0, "validated": 0, "reversed": 0})
    bytarget = defaultdict(lambda: {"total": 0, "validated": 0})

    for computed_at, target, direction, comp_json in hist:
        # 跳过最近 4h 内的信号(还没足够数据回测)
        try:
            sig_dt = datetime.strptime(computed_at, "%Y-%m-%dT%H:%M:%S")
            if (datetime.now() - sig_dt).total_seconds() < WINDOW_AFTER_HOURS * 3600:
                continue
        except ValueError:
            continue

        n_after, avg_after = fetch_post_signal_sentiment(target, computed_at, WINDOW_AFTER_HOURS)
        if n_after < 1:
            continue
        post_dir = direction_from_avg(avg_after)

        bydir[direction]["total"] += 1
        bytarget[target]["total"] += 1
        if direction == post_dir:
            bydir[direction]["validated"] += 1
            bytarget[target]["validated"] += 1
        elif {direction, post_dir} == {"bullish", "bearish"}:
            bydir[direction]["reversed"] += 1

    print(f"\n按 direction 统计:")
    print(f"  {'方向':<10} {'样本':>5} {'验证':>5} {'反向':>5} {'准确率':>8}")
    for d, s in bydir.items():
        if s["total"] == 0: continue
        acc = s["validated"] * 100 / s["total"]
        print(f"  {d:<10} {s['total']:>5} {s['validated']:>5} {s['reversed']:>5} {acc:>7.1f}%")

    print(f"\nTop 5 准确(min 3 次):")
    target_acc = [
        (t, s["validated"] / s["total"], s["total"])
        for t, s in bytarget.items() if s["total"] >= 3
    ]
    target_acc.sort(key=lambda x: -x[1])
    for t, acc, n in target_acc[:5]:
        print(f"  {t:<14} {acc*100:>5.1f}%  ({n} samples)")

    if len(target_acc) > 5:
        print(f"\nBottom 3 准确(min 3 次):")
        for t, acc, n in target_acc[-3:]:
            print(f"  {t:<14} {acc*100:>5.1f}%  ({n} samples)")


if __name__ == "__main__":
    backtest_kind("sector_sentiment")
    backtest_kind("event_cluster")

    print(f"\n{'='*60}")
    print(f"说明:")
    print(f"  - 验证 = 信号产生后 {WINDOW_AFTER_HOURS}h 内同板块电报情绪同向")
    print(f"  - 反向 = bullish 信号后 4h 内出现 bearish(或反之)")
    print(f"  - 数据不足时(早期 / 板块冷门)无样本")
