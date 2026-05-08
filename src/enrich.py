"""规则引擎:把电报文本抽成结构化标签。

- 实体识别:板块(别名→规范名)、公司(精确名)、政策机构(别名→规范名)
- 事件类型:多个(政策/业绩/资金/产能/技术/并购/监管/宏观/事故/评级/解禁)
- 情绪:positive / negative / neutral 三态 + 数值分(-1.0 ~ 1.0)
- 词典从 data/dict/ 外部 JSON 加载,用户可直接编辑

设计取舍:
- 简单子串匹配 — 中文场景没词边界,简单可靠;偶有"中国平安"被"中国"截短问题,但词典里"中国"不在 sectors,影响小
- 无否定语境识别("不会立案" 仍会命中"立案") — 规则版限制,LLM 二次精分类能补
- 大写保留区分:CPO ≠ cpo;但中文不区分
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

ENRICHER_VERSION = "rule-v2"  # v2: 公司→板块自动映射


class Enricher:
    """加载词典,提供 enrich(title, content) -> dict。"""

    def __init__(self, dict_dir: Path):
        self.dict_dir = Path(dict_dir)
        self._sectors: list[tuple[str, list[str]]] = []     # [(规范名, [别名...])]
        self._orgs: list[tuple[str, list[str]]] = []
        self._companies: list[str] = []
        self._company_sectors: dict[str, list[str]] = {}    # 公司 → 自动归属板块
        self._event_types: list[tuple[str, list[str]]] = []
        self._pos_words: list[str] = []
        self._neg_words: list[str] = []
        self._load()

    def _load(self) -> None:
        # sectors / orgs:dict[规范名 -> 别名列表]
        for attr, fname in (("_sectors", "sectors.json"), ("_orgs", "orgs.json")):
            data = json.loads((self.dict_dir / fname).read_text(encoding="utf-8"))
            entries = []
            for canonical, aliases in data.items():
                if canonical.startswith("_"):
                    continue
                # 别名按长度倒序,优先长匹配避免"中国" 命中前于 "中国移动"(虽然 sectors 里没这冲突)
                aliases_sorted = sorted({a for a in aliases if a}, key=len, reverse=True)
                entries.append((canonical, aliases_sorted))
            setattr(self, attr, entries)

        # companies:支持两种格式
        # 新:{"company_to_sectors": {"宁德时代": ["锂电池", "新能源车"], ...}}
        # 旧:{"names": ["宁德时代", "比亚迪", ...]} (兼容)
        comp_data = json.loads((self.dict_dir / "companies.json").read_text(encoding="utf-8"))
        if "company_to_sectors" in comp_data:
            mapping = {k: list(v) for k, v in comp_data["company_to_sectors"].items()
                       if not k.startswith("_") and k}
            self._company_sectors = mapping
            names = list(mapping.keys())
        else:
            names = comp_data.get("names", [])
            self._company_sectors = {}
        # 长度倒序,避免短名(如 "京东")命中前于 "京东物流"
        self._companies = sorted({n for n in names if n}, key=len, reverse=True)

        # event_types
        et_data = json.loads((self.dict_dir / "event_types.json").read_text(encoding="utf-8"))
        et_entries = []
        for canonical, kws in et_data.items():
            if canonical.startswith("_"):
                continue
            et_entries.append((canonical, sorted({k for k in kws if k}, key=len, reverse=True)))
        self._event_types = et_entries

        # sentiment
        sent_data = json.loads((self.dict_dir / "sentiment.json").read_text(encoding="utf-8"))
        self._pos_words = sorted({w for w in sent_data.get("positive", []) if w}, key=len, reverse=True)
        self._neg_words = sorted({w for w in sent_data.get("negative", []) if w}, key=len, reverse=True)

    @staticmethod
    def _match_canonical(text: str, entries: Iterable[tuple[str, list[str]]]) -> list[str]:
        """返回命中的规范名(去重保序)。"""
        out: list[str] = []
        seen: set[str] = set()
        for canonical, aliases in entries:
            for a in aliases:
                if a in text:
                    if canonical not in seen:
                        out.append(canonical)
                        seen.add(canonical)
                    break
        return out

    @staticmethod
    def _match_flat(text: str, names: Iterable[str]) -> list[str]:
        """精确名称命中(列表已按长度倒序,先长后短,本质上 substring)。"""
        out: list[str] = []
        seen: set[str] = set()
        for name in names:
            if name in text and name not in seen:
                out.append(name)
                seen.add(name)
        return out

    @staticmethod
    def _count_hits(text: str, words: Iterable[str]) -> int:
        return sum(1 for w in words if w in text)

    def enrich(self, title: str, content: str) -> dict:
        text = f"{title}\n{content}"

        sectors = self._match_canonical(text, self._sectors)
        orgs = self._match_canonical(text, self._orgs)
        companies = self._match_flat(text, self._companies)
        event_types = self._match_canonical(text, self._event_types)

        # 公司 → 板块自动映射:命中公司时,把对应板块并入 sectors(去重保序)
        if companies and self._company_sectors:
            existing = set(sectors)
            valid_sectors = {c for c, _ in self._sectors}  # sectors.json 里的合法板块
            for c in companies:
                for sec in self._company_sectors.get(c, []):
                    if sec in valid_sectors and sec not in existing:
                        sectors.append(sec)
                        existing.add(sec)

        pos = self._count_hits(text, self._pos_words)
        neg = self._count_hits(text, self._neg_words)
        score = (pos - neg) / (pos + neg + 1)
        if score > 0.15:
            sentiment = "positive"
        elif score < -0.15:
            sentiment = "negative"
        else:
            sentiment = "neutral"

        return {
            "sectors": sectors,
            "companies": companies,
            "orgs": orgs,
            "event_types": event_types,
            "sentiment": sentiment,
            "sentiment_score": round(score, 3),
        }


def default_enricher(project_root: Path | None = None) -> Enricher:
    if project_root is None:
        project_root = Path(__file__).resolve().parent.parent
    return Enricher(project_root / "data" / "dict")


if __name__ == "__main__":
    # 自检:直接 python src/enrich.py 跑一个 demo
    e = default_enricher()
    samples = [
        ("央行决定降准0.5个百分点", "财联社5月7日电,中国人民银行决定下调金融机构存款准备金率0.5个百分点"),
        ("宁德时代Q1净利润同比增长30%", "宁德时代发布一季报,营收和净利润均超预期,扩产计划加速"),
        ("证监会对XX公司立案调查", "因涉嫌信息披露违法违规,证监会决定对XX公司立案调查"),
        ("中际旭创海外订单暴增", "中际旭创公布海外大单,800G光模块出货量同比 +50%"),
        ("特斯拉 Optimus 量产时间确认", "特斯拉 CEO 表示 Optimus 人形机器人有望 2027 年量产"),
    ]
    for title, content in samples:
        print(f"\n[{title}]")
        result = e.enrich(title, content)
        for k, v in result.items():
            print(f"  {k}: {v}")
