"""富途行情封装 — 只读,只拉 snapshot 不订阅推送(简化)。

OpenD 必须在本机 127.0.0.1:11111 跑且账号已登录。
没开户也能拿港美股 Level 1 实时报价。

用法:
  with FutuQuote() as q:
      rows = q.get_snapshot(['HK.00700', 'US.NVDA'])
      # rows = [{code, name, last_price, prev_close, change_rate, ...}]
"""
from __future__ import annotations

import math
from typing import Iterable

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 11111
BATCH_SIZE = 200   # 富途 get_market_snapshot 一次最多 200 只


class FutuQuote:
    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT):
        self.host = host
        self.port = port
        self._ctx = None

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, *_):
        self.close()

    def _connect(self):
        from futu import OpenQuoteContext
        self._ctx = OpenQuoteContext(host=self.host, port=self.port)

    def close(self):
        if self._ctx is not None:
            try:
                self._ctx.close()
            except Exception:
                pass
            self._ctx = None

    def get_snapshot(self, codes: Iterable[str], skip_invalid: bool = True) -> tuple[list[dict], list[str]]:
        """返回 (rows, invalid_codes)。
        skip_invalid=True 时,如果整批拉失败(例如某个代码不存在),会用二分法找出无效代码并跳过。
        invalid_codes 包含本次发现的无效代码,调用方可以记录并下次直接排除。"""
        from futu import RET_OK
        if self._ctx is None:
            self._connect()

        codes = list(dict.fromkeys(c for c in codes if c))
        out: list[dict] = []
        invalid: list[str] = []

        def _fetch_chunk(chunk: list[str]) -> None:
            if not chunk:
                return
            ret, data = self._ctx.get_market_snapshot(chunk)
            if ret == RET_OK:
                for _, r in data.iterrows():
                    out.append(self._row_to_dict(r))
                return
            # 失败 — 如果只有一只就标记 invalid,否则二分递归
            if not skip_invalid or len(chunk) == 1:
                if skip_invalid and len(chunk) == 1:
                    invalid.append(chunk[0])
                    return
                raise RuntimeError(f"get_market_snapshot failed: {data}")
            mid = len(chunk) // 2
            _fetch_chunk(chunk[:mid])
            _fetch_chunk(chunk[mid:])

        for i in range(0, len(codes), BATCH_SIZE):
            _fetch_chunk(codes[i:i + BATCH_SIZE])

        return out, invalid

    @staticmethod
    def _row_to_dict(r) -> dict:
        last = r.get("last_price")
        prev = r.get("prev_close_price")
        change_rate = None
        try:
            if last is not None and prev is not None and prev != 0:
                cr = (float(last) - float(prev)) / float(prev) * 100
                if cr == cr:  # 排除 NaN
                    change_rate = round(cr, 2)
        except (TypeError, ValueError):
            change_rate = None
        return {
            "code": r.get("code"),
            "name": r.get("name"),
            "last_price": float(last) if last is not None else None,
            "prev_close": float(prev) if prev is not None else None,
            "change_rate": change_rate,
            "turnover": float(r["turnover"]) if r.get("turnover") is not None else None,
            "volume": float(r["volume"]) if r.get("volume") is not None else None,
            "update_time": str(r.get("update_time") or ""),
        }

    def _legacy_get_snapshot_old(self, codes):  # 保留旧实现作参考
        from futu import RET_OK
        codes = list(dict.fromkeys(c for c in codes if c))
        out: list[dict] = []
        for i in range(0, len(codes), BATCH_SIZE):
            chunk = codes[i:i + BATCH_SIZE]
            ret, data = self._ctx.get_market_snapshot(chunk)
            if ret != RET_OK:
                raise RuntimeError(f"get_market_snapshot failed: {data}")
            for _, r in data.iterrows():
                last = r.get("last_price")
                prev = r.get("prev_close_price")
                # 用 prev_close 自算 change_rate
                change_rate = None
                try:
                    if last is not None and prev is not None and prev != 0:
                        change_rate = (float(last) - float(prev)) / float(prev) * 100
                        if math.isnan(change_rate):
                            change_rate = None
                except (TypeError, ValueError):
                    change_rate = None
                out.append({
                    "code": r.get("code"),
                    "name": r.get("name"),
                    "last_price": float(last) if last is not None else None,
                    "prev_close": float(prev) if prev is not None else None,
                    "change_rate": round(change_rate, 2) if change_rate is not None else None,
                    "turnover": float(r["turnover"]) if r.get("turnover") is not None else None,
                    "volume": float(r["volume"]) if r.get("volume") is not None else None,
                    "update_time": str(r.get("update_time") or ""),
                })
        return out


if __name__ == "__main__":
    # 自检 — 含一个故意错误代码,测试 binary search fallback
    with FutuQuote() as q:
        rows, invalid = q.get_snapshot(['HK.00700', 'US.NVDA', 'US.AAPL', 'US.NOTEXIST_XXX'])
        print(f"valid: {len(rows)}, invalid: {invalid}")
        for r in rows:
            chg = f"{r['change_rate']:+.2f}%" if r['change_rate'] is not None else "?"
            print(f"  {r['code']:<10} {r['name'][:12]:<14} 现价={r['last_price']:<10} {chg}  upd={r['update_time']}")
