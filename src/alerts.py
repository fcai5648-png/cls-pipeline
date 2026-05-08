"""告警 channel 实现:macOS 本地通知 / Bark / Server酱 / 企业微信。

每个 channel 是一个 send(title, body, score, url) → (ok: bool, err: str|None) 函数。
任何错误都回 (False, error_msg),不抛异常。
"""
from __future__ import annotations

import json
import shlex
import subprocess
import urllib.parse
import urllib.request
from typing import Callable


def _http_post_json(url: str, payload: dict, timeout: int = 8) -> tuple[bool, str | None]:
    try:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            url, data=data, method="POST",
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
        return True, None if resp.status == 200 else f"http {resp.status}: {body[:120]}"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:120]}"


def _http_get(url: str, timeout: int = 8) -> tuple[bool, str | None]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
        return resp.status == 200, None if resp.status == 200 else f"http {resp.status}: {body[:120]}"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:120]}"


def osascript_notify(title: str, body: str, score: float, url: str, sound: str | None = None) -> tuple[bool, str | None]:
    """macOS 本地通知,通过 osascript display notification。"""
    safe_title = title.replace('"', "'").replace("\\", "")[:60]
    safe_body = body.replace('"', "'").replace("\\", "").replace("\n", " ")[:200]
    sound_part = f' sound name "{sound}"' if sound else ""
    script = f'display notification "{safe_body}" with title "{safe_title}"{sound_part}'
    try:
        subprocess.run(["osascript", "-e", script], check=True, capture_output=True, timeout=5)
        return True, None
    except subprocess.CalledProcessError as e:
        return False, f"osascript exit {e.returncode}: {e.stderr.decode()[:120]}"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:120]}"


def bark_send(title: str, body: str, score: float, url: str, url_or_key: str) -> tuple[bool, str | None]:
    """Bark iOS 推送。url_or_key 可以是完整 URL 或仅 key。"""
    if not url_or_key:
        return False, "no bark key"
    if url_or_key.startswith("http"):
        base = url_or_key.rstrip("/")
    else:
        base = f"https://api.day.app/{url_or_key.strip('/')}"
    encoded_title = urllib.parse.quote(title)
    encoded_body = urllib.parse.quote(body)
    full_url = f"{base}/{encoded_title}/{encoded_body}?url={urllib.parse.quote(url)}&group=cls-pipeline"
    return _http_get(full_url)


def serverchan_send(title: str, body: str, score: float, url: str, sendkey: str) -> tuple[bool, str | None]:
    """Server酱 微信推送。sendkey 来自 https://sct.ftqq.com。"""
    if not sendkey:
        return False, "no serverchan sendkey"
    api = f"https://sctapi.ftqq.com/{sendkey}.send"
    payload_url = f"https://sctapi.ftqq.com/{sendkey}.send?title={urllib.parse.quote(title)}&desp={urllib.parse.quote(body + chr(10) + url)}"
    return _http_get(payload_url)


def wecom_send(title: str, body: str, score: float, url: str, webhook: str) -> tuple[bool, str | None]:
    """企业微信群机器人。webhook 形如 https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx。"""
    if not webhook:
        return False, "no wecom webhook"
    text = f"{title}\n\n{body}\n\n查看全文: {url}"
    payload = {"msgtype": "text", "text": {"content": text}}
    return _http_post_json(webhook, payload)


def feishu_bot_send(title: str, body: str, score: float, url: str,
                    webhook: str, secret: str = "") -> tuple[bool, str | None]:
    """飞书自定义机器人(富文本 post 格式)。
    webhook 形如 https://open.feishu.cn/open-apis/bot/v2/hook/{uuid}。
    secret 可选 — 设了就发签名;没设就发明文(机器人需配关键词校验)。"""
    if not webhook:
        return False, "no feishu webhook"
    # body 按行拆,每行一个 paragraph;最后加一行链接
    lines = [ln for ln in (body or "").splitlines() if ln.strip()]
    content_paragraphs = [[{"tag": "text", "text": ln}] for ln in lines]
    content_paragraphs.append([{"tag": "a", "text": "📱 浏览器查看", "href": url or "http://127.0.0.1:8787/"}])
    payload: dict = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": title,
                    "content": content_paragraphs,
                }
            }
        },
    }
    # 可选签名(飞书特殊规则:key=timestamp+\n+secret,msg=空)
    if secret:
        import base64, hashlib, hmac, time
        timestamp = str(int(time.time()))
        key = f"{timestamp}\n{secret}".encode("utf-8")
        sign = base64.b64encode(hmac.new(key, b"", hashlib.sha256).digest()).decode("utf-8")
        payload["timestamp"] = timestamp
        payload["sign"] = sign
    ok, err = _http_post_json(webhook, payload)
    if ok and err is None:
        return True, None
    # 飞书 200 也可能含 code != 0(如签名/关键词不通过),检查 body
    return ok, err


# 渠道注册表:name -> (send_callable, args_extractor)
# args_extractor: 从 channels 配置项中拎出 channel 特有的 kwargs
def build_dispatchers(channels_config: dict) -> list[tuple[str, Callable]]:
    """从配置构造调度列表。返回 [(channel_name, callable_with_kwargs)]。"""
    out = []
    for name, cfg in channels_config.items():
        if not cfg.get("enabled"):
            continue
        if name == "osascript":
            sound = cfg.get("sound") or None
            out.append((name, lambda t, b, s, u, _s=sound: osascript_notify(t, b, s, u, sound=_s)))
        elif name == "bark":
            key = cfg.get("url_or_key", "")
            out.append((name, lambda t, b, s, u, _k=key: bark_send(t, b, s, u, _k)))
        elif name == "serverchan":
            sk = cfg.get("sendkey", "")
            out.append((name, lambda t, b, s, u, _sk=sk: serverchan_send(t, b, s, u, _sk)))
        elif name == "wecom_webhook":
            wh = cfg.get("url", "")
            out.append((name, lambda t, b, s, u, _w=wh: wecom_send(t, b, s, u, _w)))
        elif name == "feishu_bot":
            wh = cfg.get("webhook", "")
            sec = cfg.get("secret", "")
            out.append((name, lambda t, b, s, u, _w=wh, _sec=sec: feishu_bot_send(t, b, s, u, _w, _sec)))
    return out


def format_alert(row: dict, viewer_url: str, quote_summary: list[str] | None = None) -> tuple[str, str]:
    """从 telegraph 行构造 (title, body)。
    quote_summary:富途相关港美股涨跌字符串列表,如 ['NVDA +2.68%', 'MSFT +2.35%'],已按重要性排序。"""
    enr = row.get("enrichment") or {}
    score = enr.get("importance_score", 0)
    sectors = enr.get("sectors", [])
    companies = enr.get("companies", [])
    sentiment = enr.get("sentiment", "neutral")
    raw_title = row.get("title") or ""
    content = row.get("content") or ""

    sent_emoji = {"positive": "🟢", "negative": "🔴", "neutral": "⚪"}.get(sentiment, "⚪")
    is_hl = bool(row.get("is_highlight"))
    head = f"⭐头条 🔥 {score:.0f} {sent_emoji}" if is_hl else f"🔥 {score:.0f} {sent_emoji}"
    if sectors:
        head += " · " + " / ".join(sectors[:3])

    if raw_title:
        head_title = raw_title[:50] + ("…" if len(raw_title) > 50 else "")
    else:
        head_title = content[:40] + "…"
    title = f"{head} · {head_title}"

    body_lines = []
    if sectors:    body_lines.append(f"板块:{','.join(sectors[:5])}")
    if companies:  body_lines.append(f"公司:{','.join(companies[:5])}")
    if enr.get("event_types"): body_lines.append(f"事件:{','.join(enr['event_types'])}")
    body_lines.append(f"情绪:{sentiment} ({enr.get('sentiment_score', 0):+.2f})")
    if quote_summary:
        body_lines.append(f"💹 {' · '.join(quote_summary[:5])}")
    body_lines.append(content[:120] + ("…" if len(content) > 120 else ""))
    body = "\n".join(body_lines)
    return title, body


__all__ = ["build_dispatchers", "format_alert"]
