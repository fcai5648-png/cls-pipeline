"""财联社电报抓取封装。

底层用 akshare.stock_info_global_cls(symbol='全部'),一次返回最新 20 条。
返回格式:list[dict],每条 dict 含 标题/内容/发布日期/发布时间。
"""
from __future__ import annotations

import time
from typing import List


def fetch_telegraph() -> List[dict]:
    """拉一次最新电报 — 同时拉'全部' + '重点',合并去重并标记 _is_highlight=1。
    失败抛异常,由调用方决定重试/记录。"""
    import akshare as ak  # 延迟 import,启动更快

    df_all = ak.stock_info_global_cls(symbol="全部")
    if df_all is None or len(df_all) == 0:
        df_all_records = []
    else:
        df_all_records = df_all.to_dict("records")

    # 重点(头条)— cls 编辑精选,通常是"全部"的子集,但偶尔会有不在最新窗口的
    try:
        df_hot = ak.stock_info_global_cls(symbol="重点")
        df_hot_records = df_hot.to_dict("records") if df_hot is not None and len(df_hot) > 0 else []
    except Exception:
        df_hot_records = []  # 重点接口失败也别影响主流程

    # 用 (date, time, title) 标识重点
    def _key(r):
        return (str(r.get("发布日期", "")).strip(),
                str(r.get("发布时间", "")).strip(),
                str(r.get("标题", "")).strip())

    hot_keys = {_key(r) for r in df_hot_records}

    out = []
    seen_keys = set()
    for r in df_all_records:
        k = _key(r)
        r["_is_highlight"] = 1 if k in hot_keys else 0
        out.append(r)
        seen_keys.add(k)

    # "重点"里若有不在"全部"的(罕见,但可能时间窗错开),补进来
    for r in df_hot_records:
        if _key(r) not in seen_keys:
            r["_is_highlight"] = 1
            out.append(r)

    return out


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
