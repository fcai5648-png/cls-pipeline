"""财联社电报抓取封装。

底层用 akshare.stock_info_global_cls(symbol='全部'),一次返回最新 20 条。
返回格式:list[dict],每条 dict 含 标题/内容/发布日期/发布时间。
"""
from __future__ import annotations

import time
from typing import List


def fetch_telegraph() -> List[dict]:
    """拉一次最新电报。失败抛异常,由调用方决定重试/记录。"""
    import akshare as ak  # 延迟 import,启动更快
    df = ak.stock_info_global_cls(symbol="全部")
    if df is None or len(df) == 0:
        return []
    return df.to_dict("records")


def fetch_with_retry(max_attempts: int = 3, backoff_base: float = 2.0) -> tuple[List[dict], str | None]:
    """带退避重试。返回 (rows, error_msg)。
    成功:(rows, None)。失败:([], "error string")。"""
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            rows = fetch_telegraph()
            return rows, None
        except Exception as e:  # akshare 偶发网络错误 / DataFrame 解析失败
            last_err = f"{type(e).__name__}: {e}"
            if attempt < max_attempts:
                time.sleep(backoff_base ** attempt)  # 2s, 4s, 8s
    return [], last_err
