"""DeepSeek LLM 二次精分类。

用法:
  enricher = LLMEnricher.from_config_file(Path('data/llm_config.json'))
  if enricher.enabled:
      result, telemetry = enricher.enrich(title, content, candidate_sectors=[...], ...)

设计:
  - 走 OpenAI 兼容协议(DeepSeek API),response_format json_object 强制 JSON 输出
  - 候选词典作为 system prompt 约束(LLM 不能瞎造板块名)
  - DeepSeek 自动 prompt caching:固定 system prompt 后续命中缓存,实际成本极低
  - 每日预算硬上限,超了拒绝调用
  - 任何错误都不抛异常,返回 (None, telemetry) 让上游回退到规则版
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# DeepSeek-chat (V3) 实际定价(2026 年初参考):
# 见 https://api-docs.deepseek.com/quick_start/pricing
PRICE_INPUT_PER_M = 0.27       # cache miss
PRICE_INPUT_CACHED_PER_M = 0.07  # cache hit
PRICE_OUTPUT_PER_M = 1.10

LLM_VERSION_TAG = "rule-v1+llm-deepseek-chat"

SYSTEM_TEMPLATE = """你是中文金融电报标签抽取助手。从给定的电报中,严格按 JSON schema 返回。
不要解释,只返回 JSON。

【候选板块】(只能从下面列表里选,不要造新词):
{sectors}

【候选事件类型】(只能从下面列表里选):
{event_types}

【候选政策机构】(只能从下面列表里选):
{orgs}

【情绪判断标准】对中国 A 股 / 中国相关市场的影响:
- positive: 利好(订单、扩产、政策支持、业绩超预期、降准降息、技术突破等)
- negative: 利空(亏损、监管处罚、退市、产能过剩、贸易摩擦等)
- neutral: 中性(纯行情数据、海外消息无 A 股直接关联、模糊事件)
注意识别否定语境("不会立案" 不是利空,"未达成" 是利空)。

【输出 JSON Schema】(严格遵守):
{{
  "sectors": ["..."],
  "companies": ["..."],     // 提到的公司精确名(电报里出现的中文公司名)
  "orgs": ["..."],
  "event_types": ["..."],
  "sentiment": "positive" | "negative" | "neutral",
  "sentiment_score": <-1 到 1 的浮点数>,
  "reasoning": "<不超过 30 字的一句话理由>"
}}"""


@dataclass
class LLMConfig:
    provider: str
    model: str
    api_key: str
    base_url: str
    daily_call_limit: int = 500


class LLMEnricher:
    def __init__(self, config: LLMConfig, store=None):
        self.config = config
        self.store = store
        self._client = None
        self._system_prompt: str | None = None
        self.enabled = bool(config.api_key)

    @classmethod
    def from_config_file(cls, path: Path, store=None) -> "LLMEnricher | None":
        """加载配置;文件不存在 / api_key 缺失 → 返回 None(纯规则模式)。"""
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            cfg = LLMConfig(
                provider=data.get("provider", "deepseek"),
                model=data.get("model", "deepseek-chat"),
                api_key=data.get("api_key", ""),
                base_url=data.get("base_url", "https://api.deepseek.com/v1"),
                daily_call_limit=int(data.get("daily_call_limit", 500)),
            )
            if not cfg.api_key:
                return None
            return cls(cfg, store=store)
        except Exception:
            return None

    def _get_client(self):
        if self._client is None:
            from openai import OpenAI
            self._client = OpenAI(api_key=self.config.api_key, base_url=self.config.base_url)
        return self._client

    def build_system_prompt(self, sectors: list[str], event_types: list[str], orgs: list[str]) -> str:
        if self._system_prompt is None:
            self._system_prompt = SYSTEM_TEMPLATE.format(
                sectors=", ".join(sectors),
                event_types=", ".join(event_types),
                orgs=", ".join(orgs),
            )
        return self._system_prompt

    def can_call(self) -> tuple[bool, str]:
        """配额检查。返回 (可调用, 原因)。"""
        if not self.enabled:
            return False, "no api key"
        if self.store is not None:
            calls_today, _cost_today = self.store.llm_calls_today()
            if calls_today >= self.config.daily_call_limit:
                return False, f"daily limit reached ({calls_today}/{self.config.daily_call_limit})"
        return True, "ok"

    @staticmethod
    def _calc_cost(usage: dict, model: str) -> float:
        """估算成本,单位 USD。优先用 prompt_cache_hit_tokens 区分 cache hit 价格。"""
        in_total = usage.get("prompt_tokens", 0) or 0
        cache_hit = usage.get("prompt_cache_hit_tokens", 0) or 0
        in_uncached = max(in_total - cache_hit, 0)
        out_total = usage.get("completion_tokens", 0) or 0
        cost = (
            in_uncached * PRICE_INPUT_PER_M / 1_000_000
            + cache_hit * PRICE_INPUT_CACHED_PER_M / 1_000_000
            + out_total * PRICE_OUTPUT_PER_M / 1_000_000
        )
        return cost

    @staticmethod
    def _filter_to_dict_keys(items: list, allowed: set[str]) -> list[str]:
        """过滤 LLM 输出,丢掉不在词典里的(防止幻觉)。"""
        return [x for x in items if isinstance(x, str) and x in allowed]

    def enrich(
        self,
        title: str,
        content: str,
        allowed_sectors: set[str],
        allowed_event_types: set[str],
        allowed_orgs: set[str],
        telegraph_id: int | None = None,
    ) -> tuple[Optional[dict], dict]:
        """返回 (payload | None, telemetry)。
        payload schema 与规则版一致:sectors / companies / orgs / event_types / sentiment / sentiment_score / reasoning
        """
        telemetry = {
            "called": False, "tokens_in": 0, "tokens_out": 0,
            "cost_usd": 0.0, "latency_ms": 0, "error": None,
        }
        ok, reason = self.can_call()
        if not ok:
            telemetry["error"] = reason
            return None, telemetry

        system_prompt = self.build_system_prompt(
            sorted(allowed_sectors),
            sorted(allowed_event_types),
            sorted(allowed_orgs),
        )
        user_msg = f"标题:{title or '(无标题)'}\n内容:{content}"

        t0 = time.monotonic()
        try:
            resp = self._get_client().chat.completions.create(
                model=self.config.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_msg},
                ],
                temperature=0,
                response_format={"type": "json_object"},
                max_tokens=400,
                timeout=20,
            )
        except Exception as e:
            telemetry["latency_ms"] = int((time.monotonic() - t0) * 1000)
            telemetry["error"] = f"{type(e).__name__}: {str(e)[:200]}"
            if self.store is not None:
                self.store.log_llm_call(telegraph_id, self.config.model, 0, 0, 0.0,
                                        telemetry["latency_ms"], telemetry["error"])
            return None, telemetry

        telemetry["latency_ms"] = int((time.monotonic() - t0) * 1000)
        telemetry["called"] = True

        usage = (resp.usage.model_dump() if hasattr(resp.usage, "model_dump") else dict(resp.usage)) if resp.usage else {}
        telemetry["tokens_in"] = usage.get("prompt_tokens", 0) or 0
        telemetry["tokens_out"] = usage.get("completion_tokens", 0) or 0
        telemetry["cost_usd"] = self._calc_cost(usage, self.config.model)

        if self.store is not None:
            self.store.log_llm_call(
                telegraph_id, self.config.model,
                telemetry["tokens_in"], telemetry["tokens_out"],
                telemetry["cost_usd"], telemetry["latency_ms"], None,
            )

        # 解析 JSON
        try:
            content_str = resp.choices[0].message.content or "{}"
            data = json.loads(content_str)
        except Exception as e:
            telemetry["error"] = f"json parse: {e}"
            return None, telemetry

        # 词典过滤(防幻觉)
        payload = {
            "sectors":     self._filter_to_dict_keys(data.get("sectors", []), allowed_sectors),
            "event_types": self._filter_to_dict_keys(data.get("event_types", []), allowed_event_types),
            "orgs":        self._filter_to_dict_keys(data.get("orgs", []), allowed_orgs),
            # companies 不限制(LLM 可能识别词典里没有的小公司)
            "companies":   [x for x in data.get("companies", []) if isinstance(x, str)][:20],
            "sentiment":   data.get("sentiment", "neutral") if data.get("sentiment") in ("positive", "negative", "neutral") else "neutral",
            "sentiment_score": float(data.get("sentiment_score", 0.0)) if isinstance(data.get("sentiment_score"), (int, float)) else 0.0,
            "reasoning":   str(data.get("reasoning", ""))[:80],
        }
        return payload, telemetry


def merge_rule_and_llm(rule: dict, llm: dict | None) -> dict:
    """合并规则版和 LLM 结果。
    - 实体类(sectors/companies/orgs/event_types):并集去重(规则在前)
    - sentiment:用 LLM 的(LLM 懂否定语境)
    - sentiment_score:用 LLM 的
    """
    if llm is None:
        return dict(rule)
    def union(a: list, b: list) -> list:
        seen, out = set(), []
        for x in (a or []) + (b or []):
            if x and x not in seen:
                out.append(x)
                seen.add(x)
        return out
    return {
        "sectors":     union(rule.get("sectors", []),     llm.get("sectors", [])),
        "companies":   union(rule.get("companies", []),   llm.get("companies", [])),
        "orgs":        union(rule.get("orgs", []),        llm.get("orgs", [])),
        "event_types": union(rule.get("event_types", []), llm.get("event_types", [])),
        "sentiment":   llm.get("sentiment", rule.get("sentiment", "neutral")),
        "sentiment_score": llm.get("sentiment_score", rule.get("sentiment_score", 0.0)),
    }


def selftest() -> int:
    """命令行自检:python src/llm_enrich.py
    直接调用一次 DeepSeek API,验证 key 有效 + 打印实际成本。"""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from enrich import default_enricher

    project_root = Path(__file__).resolve().parent.parent
    config_path = project_root / "data" / "llm_config.json"
    llm = LLMEnricher.from_config_file(config_path)
    if llm is None or not llm.enabled:
        print(f"❌ LLM 未配置或 key 缺失:{config_path}")
        print("   先跑:bash scripts/setup_llm.sh")
        return 1

    enricher = default_enricher(project_root)
    allowed_sectors = {c for c, _ in enricher._sectors}
    allowed_event_types = {c for c, _ in enricher._event_types}
    allowed_orgs = {c for c, _ in enricher._orgs}

    title = "央行决定不会对 XX 公司立案调查,符合预期"
    content = "财联社5月7日电,中国人民银行表示,经核查,XX 公司经营合规,不会对其立案调查,此前的市场担忧得到澄清。"
    print(f"测试电报:[{title}]")
    print(f"  → {content[:80]}...")
    print(f"\n模型:{llm.config.model}  endpoint:{llm.config.base_url}")
    print("调用中…")
    payload, telem = llm.enrich(title, content,
                                 allowed_sectors, allowed_event_types, allowed_orgs)
    if not telem["called"]:
        print(f"❌ 调用失败:{telem['error']}")
        return 1
    print(f"\n✅ 成功(latency {telem['latency_ms']}ms,tokens in/out {telem['tokens_in']}/{telem['tokens_out']},cost ${telem['cost_usd']:.5f})")
    print(f"\n抽取结果:")
    for k, v in (payload or {}).items():
        print(f"  {k}: {v}")
    return 0


def needs_llm(rule_payload: dict, content: str) -> tuple[bool, str]:
    """判断这条电报是否需要 LLM 复核(B 策略:低置信度补充)。
    返回 (是否需要, 原因)。
    """
    has_entity = any(rule_payload.get(k) for k in ("sectors", "companies", "orgs"))
    has_event = bool(rule_payload.get("event_types"))
    score = rule_payload.get("sentiment_score", 0.0)

    # 规则:任何 entity / 任何 event 都没抽到 → LLM 补抽
    if not has_entity and not has_event:
        return True, "no_entity_no_event"

    # 规则:命中敏感事件 ['监管','并购','事故','解禁'] 但 sentiment 接近中性 → 可能否定语境,LLM 复核
    sensitive = {"监管", "并购", "事故", "解禁"}
    if any(e in sensitive for e in rule_payload.get("event_types", [])) and abs(score) < 0.2:
        return True, "sensitive_event_neutral_score"

    # 内容很短(<30 字)且没抽到任何东西 — 可能是行情瞬时数据,跳过
    # (上面的 not has_entity and not has_event 已经覆盖,这里不重复)

    return False, "rule_confident"


if __name__ == "__main__":
    import sys
    sys.exit(selftest())
