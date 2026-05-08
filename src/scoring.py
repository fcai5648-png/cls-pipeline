"""重要性评分引擎。

公式(0-100):
  score = (
    |sentiment_score| * 25            # 情绪强度 0~25
  + max_event_weight  * 35            # 命中事件中最高权重 × 35,0~35
  + watchlist_part                    # 关注板块 sector_w 分 / 关注公司 company_w 分,上限 cap
  ) * freshness_decay                 # 时间衰减 e^(-hours/24)

权重和 watchlist 都从 data/dict/watchlist.json 加载,改完 worker 重启即生效。
评分用于 viewer / 告警 / 选股信号,落 telegraph_enrichment.importance_score。
版本号 SCORING_VERSION 升号会在下一轮回填里全量重抽。
"""
from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path

SCORING_VERSION = "score-v1"

DEFAULT_EVENT_WEIGHTS = {
    "监管": 0.9, "政策": 0.8, "并购": 0.7, "业绩": 0.7,
    "技术": 0.6, "产能": 0.6, "事故": 0.6,
    "资金": 0.5, "解禁": 0.4, "宏观": 0.4, "评级": 0.3,
}
DEFAULT_WEIGHTS = {"sector_hit": 10, "company_hit": 30, "watchlist_cap": 40}


class Scorer:
    def __init__(self, watchlist_path: Path):
        self.path = Path(watchlist_path)
        self.sectors: set[str] = set()
        self.companies: set[str] = set()
        self.event_weights: dict[str, float] = dict(DEFAULT_EVENT_WEIGHTS)
        self.weights: dict[str, int] = dict(DEFAULT_WEIGHTS)
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return  # 用默认值
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            self.sectors = set(data.get("sectors", []))
            self.companies = set(data.get("companies", []))
            if "event_weights" in data:
                self.event_weights.update(data["event_weights"])
            if "weights" in data:
                self.weights.update({k: int(v) for k, v in data["weights"].items()})
        except Exception:
            pass

    @staticmethod
    def _freshness(pub_dt: str, now: datetime | None = None) -> float:
        """e^(-hours/24)。1h=0.96 / 6h=0.78 / 24h=0.37 / 48h=0.14 / 7d=0.0008。"""
        try:
            pub = datetime.strptime(pub_dt, "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return 0.5  # 解析失败给中等权重
        now = now or datetime.now()
        hours = (now - pub).total_seconds() / 3600
        return max(0.05, math.exp(-max(hours, 0) / 24))

    HIGHLIGHT_BOOST = 15.0   # cls 编辑精选(头条)固定 +15 分

    def score(self, enrichment: dict, pub_dt: str, now: datetime | None = None,
              is_highlight: bool = False) -> tuple[float, dict]:
        """返回 (importance_score 0-100, components dict)。
        is_highlight=True 时(cls 编辑精选)在原始分上 +HIGHLIGHT_BOOST。"""
        sentiment_score = enrichment.get("sentiment_score") or 0.0
        event_types = enrichment.get("event_types") or []
        sectors = enrichment.get("sectors") or []
        companies = enrichment.get("companies") or []

        sentiment_part = abs(float(sentiment_score)) * 25.0   # 0-25
        max_event_w = max((self.event_weights.get(e, 0.3) for e in event_types), default=0.0)
        event_part = max_event_w * 35.0   # 0-35

        sector_hits = sum(1 for s in sectors if s in self.sectors)
        company_hits = sum(1 for c in companies if c in self.companies)
        watchlist_part = min(
            self.weights["watchlist_cap"],
            sector_hits * self.weights["sector_hit"]
            + company_hits * self.weights["company_hit"],
        )

        highlight_boost = self.HIGHLIGHT_BOOST if is_highlight else 0.0
        freshness = self._freshness(pub_dt, now)
        raw = sentiment_part + event_part + watchlist_part + highlight_boost
        final = min(100.0, raw * freshness)

        components = {
            "sentiment_part": round(sentiment_part, 1),
            "event_part": round(event_part, 1),
            "watchlist_part": round(watchlist_part, 1),
            "highlight_boost": round(highlight_boost, 1),
            "raw": round(raw, 1),
            "freshness": round(freshness, 3),
            "final": round(final, 1),
            "sector_hits": sector_hits,
            "company_hits": company_hits,
            "max_event_w": round(max_event_w, 2),
            "is_highlight": bool(is_highlight),
        }
        return round(final, 1), components


def default_scorer(project_root: Path | None = None) -> Scorer:
    if project_root is None:
        project_root = Path(__file__).resolve().parent.parent
    return Scorer(project_root / "data" / "dict" / "watchlist.json")


if __name__ == "__main__":
    # 自检
    s = default_scorer()
    samples = [
        ("宁德时代Q1净利润同比增长30%", "2026-05-07 14:00:00",
         {"sectors": ["锂电池"], "companies": ["宁德时代"], "orgs": [], "event_types": ["业绩"], "sentiment": "positive", "sentiment_score": 0.8}),
        ("证监会对XX公司立案调查", "2026-05-07 14:00:00",
         {"sectors": [], "companies": [], "orgs": ["证监会"], "event_types": ["监管"], "sentiment": "negative", "sentiment_score": -0.7}),
        ("欧洲天然气价格下跌5%", "2026-05-07 14:00:00",
         {"sectors": [], "companies": [], "orgs": [], "event_types": ["宏观"], "sentiment": "neutral", "sentiment_score": 0.0}),
        ("旧消息(48 小时前)", "2026-05-05 14:00:00",
         {"sectors": ["AI"], "companies": ["寒武纪"], "orgs": [], "event_types": ["业绩"], "sentiment": "positive", "sentiment_score": 0.8}),
    ]
    print(f"{'示例':<25} {'final':<7} components")
    for title, pub_dt, enr in samples:
        score, c = s.score(enr, pub_dt)
        print(f"{title[:22]:<25} {score:<7} {c}")
