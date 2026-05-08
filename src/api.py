"""cls-pipeline HTTP API (FastAPI)。

独立进程跑,只读 SQLite — daemon 写,api 读,WAL 模式不冲突。
默认绑 127.0.0.1:8787,仅本地访问;远程访问请走 nginx / Tailscale。

启动:
  uvicorn --app-dir src api:app --host 127.0.0.1 --port 8787

或通过 launchd:com.user.cls-pipeline-api(install_api_launchd.sh)
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import Store  # noqa: E402


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.environ.get("CLS_DB_PATH", PROJECT_ROOT / "data" / "cls.db"))


class UTF8JSONResponse(JSONResponse):
    """中文直出,不做 \\uXXXX 转义。"""
    def render(self, content: Any) -> bytes:
        return json.dumps(content, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


app = FastAPI(
    title="cls-pipeline API",
    version="0.1.0",
    description="财联社电报本地 HTTP 接口 — 数据来源 cls-pipeline daemon。",
    default_response_class=UTF8JSONResponse,
)

_store: Store | None = None


def store() -> Store:
    global _store
    if _store is None:
        if not DB_PATH.exists():
            raise HTTPException(503, f"db not found at {DB_PATH} — daemon hasn't run yet")
        _store = Store(DB_PATH)
    return _store


VIEWER_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>财联社电报 · cls-pipeline</title>
<style>
  * { box-sizing: border-box; }
  body { font-family: -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif;
         max-width: 1180px; margin: 0 auto; padding: 18px 16px;
         background: #fafafa; color: #222; line-height: 1.55; }
  header { display: flex; align-items: baseline; justify-content: space-between;
           padding-bottom: 10px; border-bottom: 2px solid #e0e0e0; margin-bottom: 12px; }
  h1 { margin: 0; font-size: 18px; font-weight: 600; }
  .meta { font-size: 12px; color: #888; }
  .meta span { margin-left: 10px; }
  .ok { color: #2e7d32; }
  .degraded { color: #c62828; }
  .layout { display: grid; grid-template-columns: 1fr 240px; gap: 16px; }
  @media (max-width: 880px) { .layout { grid-template-columns: 1fr; } aside { display: none; } }
  .controls { margin: 0 0 12px; font-size: 13px; display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
  .controls input[type=text] { padding: 5px 8px; width: 200px; border: 1px solid #ccc; border-radius: 4px; }
  .controls select { padding: 5px; border: 1px solid #ccc; border-radius: 4px; }
  .controls button { padding: 5px 12px; border: 1px solid #999; background: #fff;
                     border-radius: 4px; cursor: pointer; }
  .controls button:hover { background: #eee; }
  .controls .auto { margin-left: auto; }
  .filter-pill { display: inline-flex; align-items: center; gap: 4px;
                 padding: 3px 8px; background: #1976d2; color: #fff;
                 border-radius: 12px; font-size: 12px; cursor: pointer; }
  .filter-pill::after { content: ' ✕'; opacity: 0.7; }
  .item { background: #fff; padding: 10px 14px 8px; margin-bottom: 7px;
          border-left: 3px solid #ccc; border-radius: 3px;
          box-shadow: 0 1px 2px rgba(0,0,0,0.04); }
  .item.s-positive { border-left-color: #2e7d32; }
  .item.s-negative { border-left-color: #c62828; }
  .item.s-neutral  { border-left-color: #9e9e9e; }
  .item.high-score { box-shadow: 0 0 0 1px #ff9800; }
  .item.new { animation: flash 1.6s ease-out; box-shadow: 0 0 0 2px #ff9800; }
  @keyframes flash { 0% { background: #fff8e1; } 100% { background: #fff; } }
  .item-head { display: flex; align-items: baseline; justify-content: space-between; }
  .ts { color: #888; font-size: 12px; font-variant-numeric: tabular-nums; margin-right: 8px; }
  .score-badge { font-size: 11px; font-weight: 600; font-variant-numeric: tabular-nums;
                 padding: 1px 6px; border-radius: 9px; background: #eee; color: #666; }
  .score-badge.high { background: #ff5722; color: #fff; }
  .score-badge.mid  { background: #ffb74d; color: #4e2700; }
  .score-badge.low  { background: #f5f5f5; color: #888; }
  .llm-mark { font-size: 11px; color: #1976d2; cursor: help; margin-left: 4px; }
  .title { font-weight: 600; color: #111; }
  .content { color: #444; font-size: 14px; margin-top: 3px; }
  .empty { color: #999; padding: 28px; text-align: center; }
  .chips { margin-top: 5px; font-size: 11px; }
  .chip { display: inline-block; padding: 1px 7px; margin: 2px 4px 0 0;
          background: #eef; color: #335; border-radius: 9px; cursor: pointer;
          border: 1px solid transparent; }
  .chip:hover { border-color: #335; }
  .chip.sector  { background: #e3f2fd; color: #1565c0; }
  .chip.event   { background: #fff3e0; color: #ef6c00; }
  .chip.company { background: #f3e5f5; color: #6a1b9a; }
  .chip.org     { background: #fce4ec; color: #ad1457; }
  .chip.sent-positive { background: #e8f5e9; color: #2e7d32; }
  .chip.sent-negative { background: #ffebee; color: #c62828; }
  aside { font-size: 12px; }
  aside h3 { font-size: 12px; color: #888; margin: 0 0 4px; font-weight: 600;
             text-transform: uppercase; letter-spacing: 0.5px; }
  aside .group { background: #fff; padding: 8px 10px; margin-bottom: 8px;
                 border-radius: 4px; border: 1px solid #eee; }
  aside .chip { font-size: 11px; }
  aside .chip .n { color: #999; margin-left: 4px; font-size: 10px; }
  .footer { font-size: 11px; color: #aaa; text-align: center; margin-top: 18px;
            padding-top: 10px; border-top: 1px solid #eee; }
  mark { background: #fff59d; padding: 0 2px; }
</style>
</head>
<body>
<header>
  <h1>财联社电报</h1>
  <div class="meta">
    <span id="health">加载中…</span>
    <span id="total"></span>
    <span id="enriched"></span>
    <span id="updated"></span>
  </div>
</header>
<div class="controls">
  视图 <select id="view">
    <option value="latest" selected>最新电报</option>
    <option value="top">⭐ Top 重要</option>
    <option value="signals">📊 信号面板</option>
  </select>
  搜索 <input type="text" id="q" placeholder="留空看最新" />
  显示 <select id="n"><option>20</option><option selected>50</option><option>100</option><option>200</option></select> 条
  <button onclick="load(true)">刷新</button>
  <span id="active-filter"></span>
  <label class="auto"><input type="checkbox" id="auto" checked /> 每 15 秒自动刷新</label>
</div>
<div class="layout">
  <main>
    <div id="list"><div class="empty">加载中…</div></div>
  </main>
  <aside>
    <div class="group">
      <h3>情绪 · 24h</h3>
      <div id="sent-bar"></div>
    </div>
    <div class="group">
      <h3>板块 · 24h Top</h3>
      <div id="sectors-bar"></div>
    </div>
    <div class="group">
      <h3>事件 · 24h Top</h3>
      <div id="events-bar"></div>
    </div>
  </aside>
</div>
<div class="footer">
  数据源:akshare stock_info_global_cls · daemon 30s · enrich 20s · WAL SQLite ·
  <a href="/docs" target="_blank">API 文档</a>
</div>
<script>
let prevIds = new Set();
let activeFilter = null;  // {kind, name}

function setFilter(kind, name) {
  activeFilter = (activeFilter && activeFilter.kind === kind && activeFilter.name === name) ? null : {kind, name};
  load(true);
}
function clearFilter() { activeFilter = null; load(true); }

function renderActiveFilter() {
  const el = document.getElementById('active-filter');
  if (!activeFilter) { el.innerHTML = ''; return; }
  el.innerHTML = `筛选:<span class="filter-pill" onclick="clearFilter()">${activeFilter.kind}=${activeFilter.name}</span>`;
}

function escapeHtml(s) { return (s || '').replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
function escapeAttr(s) { return escapeHtml(s).replace(/"/g, '&quot;'); }

function chipsFor(enr) {
  if (!enr) return '';
  const out = [];
  if (enr.sentiment && enr.sentiment !== 'neutral') {
    out.push(`<span class="chip sent-${enr.sentiment}" onclick="setFilter('sentiment','${enr.sentiment}')">${enr.sentiment === 'positive' ? '利好' : '利空'} ${enr.sentiment_score}</span>`);
  }
  for (const s of enr.sectors || []) out.push(`<span class="chip sector" onclick="setFilter('sector','${escapeAttr(s)}')">${escapeHtml(s)}</span>`);
  for (const e of enr.event_types || []) out.push(`<span class="chip event" onclick="setFilter('event','${escapeAttr(e)}')">${escapeHtml(e)}</span>`);
  for (const c of enr.companies || []) out.push(`<span class="chip company" onclick="setFilter('company','${escapeAttr(c)}')">${escapeHtml(c)}</span>`);
  for (const o of enr.orgs || []) out.push(`<span class="chip org" onclick="setFilter('org','${escapeAttr(o)}')">${escapeHtml(o)}</span>`);
  return out.length ? `<div class="chips">${out.join('')}</div>` : '';
}

function renderSparkline(scores, width, height) {
  if (!scores || scores.length < 3) return '';
  width = width || 90;
  height = height || 18;
  const min = Math.min(...scores);
  const max = Math.max(...scores);
  const range = (max - min) || 1;
  const stepX = width / (scores.length - 1);
  const points = scores.map((s, i) => {
    const x = i * stepX;
    const y = height - ((s - min) / range) * (height - 2) - 1;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
  const trendUp = scores[scores.length - 1] >= scores[0];
  const color = trendUp ? '#2e7d32' : '#c62828';
  const lastX = (scores.length - 1) * stepX;
  const lastY = height - ((scores[scores.length-1] - min) / range) * (height - 2) - 1;
  return `<svg width="${width}" height="${height}" style="vertical-align:middle;margin-left:6px" title="7d 走势,${scores.length} 个点">
    <polyline fill="none" stroke="${color}" stroke-width="1.3" points="${points}" />
    <circle cx="${lastX.toFixed(1)}" cy="${lastY.toFixed(1)}" r="2" fill="${color}" />
  </svg>`;
}

async function loadSignalsView() {
  const list = document.getElementById('list');
  list.innerHTML = '<div class="empty">加载信号中…</div>';
  try {
    const [sectors, companies, clusters, anomalies, quotesAll] = await Promise.all([
      fetch('/signals?kind=sector_sentiment&top=15').then(r => r.json()),
      fetch('/signals?kind=company_heat&top=15').then(r => r.json()),
      fetch('/signals?kind=event_cluster&top=12').then(r => r.json()),
      fetch('/signals/anomalies').then(r => r.json()),
      fetch('/quotes').then(r => r.json()).catch(() => ({quotes: {}})),
    ]);
    const sentEmoji = (d) => ({bullish:'🟢',bearish:'🔴',neutral:'⚪'}[d] || '⚪');
    const allQuotes = quotesAll.quotes || {};

    // 批量拉 sparklines(板块 + 公司)
    const secTargets = sectors.rows.map(s => s.target).join(',');
    const coTargets = companies.rows.map(s => s.target).join(',');
    const [secSparks, coSparks] = await Promise.all([
      secTargets ? fetch(`/signals/sparklines?kind=sector_sentiment&targets=${encodeURIComponent(secTargets)}&days=7`).then(r => r.json()) : Promise.resolve({sparklines:{}}),
      coTargets ? fetch(`/signals/sparklines?kind=company_heat&targets=${encodeURIComponent(coTargets)}&days=7`).then(r => r.json()) : Promise.resolve({sparklines:{}}),
    ]);
    const secSparkMap = secSparks.sparklines || {};
    const coSparkMap = coSparks.sparklines || {};

    const targetsCache = {sector: {}, company: {}};

    const fmtChange = (q) => {
      if (!q || q.change_rate == null) return '';
      const c = q.change_rate;
      const cls = c > 0 ? 'sent-positive' : c < 0 ? 'sent-negative' : '';
      const sign = c >= 0 ? '+' : '';
      return `<span class="chip ${cls}" style="margin-left:6px;font-size:11px" title="${q.code} ${q.update_time}">${q.code.replace(/^(HK|US)\./,'')} ${sign}${c.toFixed(2)}%</span>`;
    };

    const fetchSectorQuotes = async (sec) => {
      if (sec in targetsCache.sector) return targetsCache.sector[sec];
      try {
        const r = await fetch(`/quotes/sector?name=${encodeURIComponent(sec)}`).then(x => x.json());
        targetsCache.sector[sec] = r;
        return r;
      } catch { return null; }
    };
    const fetchCompanyQuote = async (co) => {
      if (co in targetsCache.company) return targetsCache.company[co];
      try {
        const r = await fetch(`/quotes/company?name=${encodeURIComponent(co)}`).then(x => x.json());
        targetsCache.company[co] = r;
        return r;
      } catch { return null; }
    };

    const renderSec = async (s) => {
      const c = s.components || {};
      const star = c.watchlist ? '⭐' : '';
      const dir = s.direction === 'bullish' ? 'positive' : s.direction === 'bearish' ? 'negative' : 'neutral';
      const qresp = await fetchSectorQuotes(s.target);
      const codes = qresp ? (qresp.codes || []).slice(0, 4) : [];
      const quoteHtml = codes.map(code => fmtChange(qresp.quotes[code])).join('');
      const spark = renderSparkline(secSparkMap[s.target]);
      return `<div class="item s-${dir}">
        <div class="item-head">
          <div><span class="title">${star}${escapeHtml(s.target)}</span> ${sentEmoji(s.direction)}
            <span style="color:#888;font-size:12px;margin-left:8px">${c.count}条 · 情绪 ${(c.avg_sentiment >= 0 ? '+' : '') + c.avg_sentiment}</span>
            ${quoteHtml}</div>
          <div>${spark}<span class="score-badge ${s.score > 500 ? 'high' : s.score > 200 ? 'mid' : 'low'}" style="margin-left:6px">${s.score.toFixed(0)}</span></div>
        </div>
      </div>`;
    };
    const renderCo = async (s) => {
      const c = s.components || {};
      const dir = s.direction === 'bullish' ? 'positive' : s.direction === 'bearish' ? 'negative' : 'neutral';
      const qresp = await fetchCompanyQuote(s.target);
      const quote = qresp && qresp.quote ? qresp.quote : null;
      const quoteHtml = quote ? fmtChange(quote) : '';
      const spark = renderSparkline(coSparkMap[s.target]);
      return `<div class="item s-${dir}">
        <div class="item-head">
          <div><span class="title">${escapeHtml(s.target)}</span> ${sentEmoji(s.direction)}
            <span style="color:#888;font-size:12px;margin-left:8px">${c.count}次 · ${(c.events || []).join(',')}</span>
            ${quoteHtml}</div>
          <div>${spark}<span class="score-badge ${s.score > 100 ? 'high' : s.score > 30 ? 'mid' : 'low'}" style="margin-left:6px">${s.score.toFixed(0)}</span></div>
        </div>
      </div>`;
    };
    const renderCluster = (s) => {
      const c = s.components || {};
      const dir = s.direction === 'bullish' ? 'positive' : s.direction === 'bearish' ? 'negative' : 'neutral';
      return `<div class="item s-${dir}">
        <div class="item-head">
          <div><span class="title">${escapeHtml(c.sector || '')} · ${escapeHtml(c.event_type || '')}</span> ${sentEmoji(s.direction)}
            <span style="color:#888;font-size:12px;margin-left:8px">${c.count}次共振</span></div>
          <span class="score-badge ${s.score > 200 ? 'high' : s.score > 100 ? 'mid' : 'low'}">${s.score.toFixed(0)}</span>
        </div>
      </div>`;
    };

    let html = '';
    const quoteCount = Object.keys(allQuotes).length;
    if (quoteCount > 0) {
      html += `<div style="font-size:11px;color:#888;margin-bottom:6px">💹 富途行情 ${quoteCount} 条缓存中,绿/红 chip 表示同板块港美股龙头实时涨跌</div>`;
    } else {
      html += `<div style="font-size:11px;color:#c62828;margin-bottom:6px">⚠️ 富途行情未连接 — 检查 OpenD 在跑且 quote_worker 已启动</div>`;
    }
    if (anomalies.rows.length) {
      html += `<h3 style="margin:8px 0 4px">⚠️ 板块异动(z-score ≥ 1.5)</h3>`;
      html += anomalies.rows.map(s => `<div class="item s-${s.direction === 'bullish' ? 'positive' : 'negative'}">
        <div class="item-head"><div><b>${escapeHtml(s.target)}</b> 当前${s.components.current_count}条 / 基线均值 ${s.components.baseline_mean}</div>
        <span class="score-badge high">z=${s.score}</span></div></div>`).join('');
    }
    html += `<h3 style="margin:14px 0 4px">📊 板块情绪 + 港美股报价(24h)</h3>`;
    const secHtml = await Promise.all(sectors.rows.map(renderSec));
    html += secHtml.join('');
    html += `<h3 style="margin:14px 0 4px">🔥 关注公司热度 + 报价(24h)</h3>`;
    const coHtml = await Promise.all(companies.rows.map(renderCo));
    html += coHtml.join('') || '<div class="empty">watchlist 公司暂无命中</div>';
    html += `<h3 style="margin:14px 0 4px">⚡ 事件共振(板块×事件,≥3次)</h3>${clusters.rows.map(renderCluster).join('')}`;
    list.innerHTML = html;
  } catch (e) {
    list.innerHTML = `<div class="empty">加载失败:${e}</div>`;
  }
}

async function load(manual) {
  renderActiveFilter();
  const q = document.getElementById('q').value.trim();
  const n = document.getElementById('n').value;
  const view = document.getElementById('view').value;

  if (view === 'signals') {
    await loadSignalsView();
    // 刷新顶部 health/total 等
    try {
      const h = await (await fetch('/health')).json();
      const s = await (await fetch('/stats')).json();
      document.getElementById('health').innerHTML =
        `<span class="${h.status === 'ok' ? 'ok' : 'degraded'}">●</span> ${h.status} · 1h 拉取 ${h.fetches_last_hour} / 错 ${h.errors_last_hour}`;
      document.getElementById('total').textContent = `共 ${s.total_telegraph} 条`;
      document.getElementById('enriched').textContent = `已抽取 ${s.enriched_total}`;
      document.getElementById('updated').textContent = new Date().toTimeString().slice(0,8);
    } catch (e) {}
    return;
  }

  let items;
  try {
    let r;
    if (activeFilter) {
      r = await fetch(`/telegraph/by_tag?kind=${activeFilter.kind}&name=${encodeURIComponent(activeFilter.name)}&hours=72&limit=${n}`);
      items = (await r.json()).rows;
    } else if (q) {
      r = await fetch(`/telegraph/search?q=${encodeURIComponent(q)}&limit=${n}`);
      const rows = (await r.json()).rows;
      items = rows.map(x => ({...x, enrichment: null}));
    } else if (view === 'top') {
      r = await fetch(`/telegraph/top?n=${n}&hours=48&min_score=15`);
      items = (await r.json()).rows;
    } else {
      r = await fetch(`/telegraph/latest?n=${n}`);
      items = (await r.json()).rows;
    }
    const h = await (await fetch('/health')).json();
    const s = await (await fetch('/stats')).json();
    document.getElementById('health').innerHTML =
      `<span class="${h.status === 'ok' ? 'ok' : 'degraded'}">●</span> ${h.status} · 1h 拉取 ${h.fetches_last_hour} / 错 ${h.errors_last_hour}`;
    document.getElementById('total').textContent = `共 ${s.total_telegraph} 条`;
    document.getElementById('enriched').textContent = `已抽取 ${s.enriched_total}/待 ${s.enrichment_pending}`;
    document.getElementById('updated').textContent = new Date().toTimeString().slice(0,8);
  } catch (e) {
    document.getElementById('health').innerHTML = `<span class="degraded">●</span> 连接失败`;
    return;
  }

  const list = document.getElementById('list');
  if (!items.length) { list.innerHTML = '<div class="empty">没有匹配的电报</div>'; return; }
  const newIds = new Set();
  list.innerHTML = items.map(r => {
    const key = r.pub_dt + '|' + (r.title || r.content.slice(0, 40));
    newIds.add(key);
    const isNew = !manual && prevIds.size > 0 && !prevIds.has(key);
    const title = escapeHtml(r.title);
    const content = escapeHtml(r.content);
    const qStr = document.getElementById('q').value.trim();
    const hl = (s) => qStr ? s.replace(new RegExp(qStr.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\\\$&'), 'gi'), m => `<mark>${m}</mark>`) : s;
    const sentClass = r.enrichment ? `s-${r.enrichment.sentiment}` : 's-neutral';
    const score = r.enrichment ? (r.enrichment.importance_score || 0) : 0;
    const scoreCls = score >= 60 ? 'high' : score >= 30 ? 'mid' : 'low';
    const highScoreCls = score >= 60 ? ' high-score' : '';
    const llmMark = r.enrichment && r.enrichment.llm_called
      ? `<span class="llm-mark" title="${escapeAttr(r.enrichment.llm_reasoning || 'LLM 复抽过')}">🤖</span>` : '';
    return `<div class="item ${sentClass}${isNew ? ' new' : ''}${highScoreCls}">
      <div class="item-head">
        <div><span class="ts">${r.pub_dt}</span>${title ? `<span class="title">${hl(title)}</span>` : ''}${llmMark}</div>
        <span class="score-badge ${scoreCls}" title="重要性评分">${score.toFixed(0)}</span>
      </div>
      <div class="content">${hl(content)}</div>
      ${chipsFor(r.enrichment)}
    </div>`;
  }).join('');
  prevIds = newIds;
}

async function loadSidebar() {
  try {
    const t = await (await fetch('/tags?hours=24')).json();
    const sentBar = document.getElementById('sent-bar');
    sentBar.innerHTML =
      `<span class="chip sent-positive" onclick="setFilter('sentiment','positive')">利好 <span class="n">${t.sentiment.positive}</span></span>` +
      `<span class="chip sent-negative" onclick="setFilter('sentiment','negative')">利空 <span class="n">${t.sentiment.negative}</span></span>` +
      `<span class="chip" onclick="setFilter('sentiment','neutral')">中性 <span class="n">${t.sentiment.neutral}</span></span>`;
    document.getElementById('sectors-bar').innerHTML = t.sectors.slice(0, 12).map(x =>
      `<span class="chip sector" onclick="setFilter('sector','${escapeAttr(x.name)}')">${escapeHtml(x.name)}<span class="n">${x.count}</span></span>`).join('') || '<span style="color:#999">暂无</span>';
    document.getElementById('events-bar').innerHTML = t.event_types.slice(0, 10).map(x =>
      `<span class="chip event" onclick="setFilter('event','${escapeAttr(x.name)}')">${escapeHtml(x.name)}<span class="n">${x.count}</span></span>`).join('') || '<span style="color:#999">暂无</span>';
  } catch (e) {}
}

load(true);
loadSidebar();
setInterval(() => { if (document.getElementById('auto').checked) { load(false); loadSidebar(); } }, 15000);
document.getElementById('q').addEventListener('keydown', e => { if (e.key === 'Enter') { activeFilter = null; load(true); } });
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse, summary="电报实时浏览器(网页)")
def viewer() -> HTMLResponse:
    return HTMLResponse(VIEWER_HTML)


@app.get("/health", summary="健康检查")
def health() -> dict:
    """daemon 是否还活着 — 看过去 1 小时拉取次数 + 错误率。"""
    s = store().stats()
    fetches = s["fetches_last_hour"]
    errors = s["errors_last_hour"]
    error_rate = errors / fetches if fetches else 1.0
    healthy = fetches > 0 and error_rate < 0.5
    return {
        "status": "ok" if healthy else "degraded",
        "fetches_last_hour": fetches,
        "errors_last_hour": errors,
        "error_rate": round(error_rate, 3),
        "latest_pub": s["latest_pub"],
    }


@app.get("/stats", summary="数据库 + 拉取统计")
def stats() -> dict:
    return store().stats()


@app.get("/telegraph/latest", summary="最新 N 条电报(发布时间倒序),含 enrichment(可能为 null)")
def latest(n: int = Query(20, ge=1, le=500, description="返回条数,1-500")) -> dict:
    return {"rows": store().latest_with_enrichment(n), "count": n}


@app.get("/tags", summary="过去 N 小时的标签命中统计 — 用于 viewer 过滤栏")
def tags(hours: int = Query(24, ge=1, le=168)) -> dict:
    return store().tag_counts(hours=hours)


@app.get("/telegraph/top", summary="按重要性评分排序的 Top N — 选股 / 告警的入口")
def top(
    n: int = Query(20, ge=1, le=200),
    hours: int = Query(24, ge=1, le=168),
    min_score: float = Query(0.0, ge=0, le=100),
) -> dict:
    rows = store().top_by_score(n=n, hours=hours, min_score=min_score)
    return {"rows": rows, "count": len(rows), "hours": hours, "min_score": min_score}


@app.get("/signals", summary="最新信号快照 — 选股 / 决策入口")
def signals(
    kind: Optional[str] = Query(None, description="sector_sentiment | company_heat | event_cluster | sector_anomaly | None=全部"),
    top: int = Query(20, ge=1, le=200),
) -> dict:
    rows = store().latest_signals(kind=kind, top=top)
    return {"rows": rows, "count": len(rows), "kind": kind or "all"}


@app.get("/signals/history", summary="单 (kind, target) 时间序列 — sparkline 用")
def signal_history(
    kind: str = Query(..., description="信号类型"),
    target: str = Query(..., description="板块名 / 公司名 / 板块|事件"),
    days: int = Query(7, ge=1, le=30),
) -> dict:
    rows = store().signal_history(kind=kind, target=target, days=days)
    return {"rows": rows, "count": len(rows), "kind": kind, "target": target, "days": days}


@app.get("/signals/anomalies", summary="当前异动 — z-score >= 1.5 的板块")
def signal_anomalies() -> dict:
    rows = store().latest_signals(kind="sector_anomaly", top=50)
    return {"rows": rows, "count": len(rows)}


@app.get("/signals/sparklines", summary="批量 sparkline 数据 — viewer 信号面板的 7 天走势图")
def signal_sparklines(
    kind: str = Query(..., description="sector_sentiment | company_heat | event_cluster"),
    targets: str = Query(..., description="逗号分隔的 target 列表"),
    days: int = Query(7, ge=1, le=30),
    max_points: int = Query(50, ge=5, le=200),
) -> dict:
    target_list = [t.strip() for t in targets.split(",") if t.strip()]
    if not target_list:
        return {"sparklines": {}}
    series = store().signals_sparklines(kind, target_list, days=days, max_points_per_target=max_points)
    return {"sparklines": series, "kind": kind, "days": days}


@app.get("/quotes", summary="批量查富途报价(最新缓存值,quote_worker 每 90s 刷)")
def quotes(codes: Optional[str] = Query(None, description="逗号分隔的代码,如 HK.00700,US.NVDA。空=返全部")) -> dict:
    code_list = [c.strip() for c in codes.split(",")] if codes else None
    return {"quotes": store().get_quotes(code_list)}


@app.get("/quotes/sector", summary="板块对应港美股龙头报价")
def quote_sector(name: str = Query(..., description="板块名,如 半导体 / AI")) -> dict:
    import json as _json
    targets_path = PROJECT_ROOT / "data" / "dict" / "quote_targets.json"
    if not targets_path.exists():
        raise HTTPException(404, "quote_targets.json not found")
    targets = _json.loads(targets_path.read_text(encoding="utf-8"))
    codes = targets.get("sector_to_codes", {}).get(name, [])
    if not codes:
        return {"sector": name, "codes": [], "quotes": {}}
    return {"sector": name, "codes": codes, "quotes": store().get_quotes(codes)}


@app.get("/quotes/company", summary="公司对应港美股代码报价")
def quote_company(name: str = Query(..., description="公司名,如 宁德时代 / 腾讯")) -> dict:
    import json as _json
    targets_path = PROJECT_ROOT / "data" / "dict" / "quote_targets.json"
    if not targets_path.exists():
        raise HTTPException(404, "quote_targets.json not found")
    targets = _json.loads(targets_path.read_text(encoding="utf-8"))
    code = targets.get("company_to_code", {}).get(name)
    if not code:
        return {"company": name, "code": None, "quote": None}
    qmap = store().get_quotes([code])
    return {"company": name, "code": code, "quote": qmap.get(code)}


@app.get("/telegraph/by_tag", summary="按标签筛选(sector / event / company / sentiment / org)")
def by_tag(
    kind: str = Query(..., description="标签类型:sector | event | company | org | sentiment"),
    name: str = Query(..., description="标签值(规范名),例 半导体 / 业绩 / 宁德时代 / 央行 / positive"),
    hours: int = Query(48, ge=1, le=168),
    limit: int = Query(100, ge=1, le=500),
) -> dict:
    s = store()
    sql_map = {
        "sector":  ("sectors_json", True),
        "event":   ("event_types_json", True),
        "company": ("companies_json", True),
        "org":     ("orgs_json", True),
        "sentiment": ("sentiment", False),
    }
    if kind not in sql_map:
        raise HTTPException(400, f"unknown kind: {kind}")
    col, is_json = sql_map[kind]
    needle = f'"{name}"' if is_json else name
    op = "LIKE" if is_json else "="
    val = f'%{needle}%' if is_json else name
    with s._lock:
        cur = s._conn.cursor()
        cur.execute(
            s._JOINED_SELECT
            + f"WHERE e.{col} {op} ? AND t.pub_dt >= datetime('now', ?, 'localtime') "
              "ORDER BY e.importance_score DESC, t.pub_dt DESC LIMIT ?",
            (val, f"-{hours} hours", limit),
        )
        rows = [s._row_to_dict(r) for r in cur.fetchall()]
    return {"rows": rows, "count": len(rows), "kind": kind, "name": name, "hours": hours}


@app.get("/telegraph/since", summary="增量拉取 — 给下游 ETL 用")
def since(
    after: str = Query(
        ...,
        description="发布时间下界(不含),形如 '2026-05-07 10:00:00' 或 ISO '2026-05-07T10:00:00'",
        examples=["2026-05-07T10:00:00"],
    ),
    limit: int = Query(500, ge=1, le=2000),
) -> dict:
    rows = store().since(after, limit)
    return {"rows": rows, "count": len(rows), "after": after, "limit": limit}


@app.get("/telegraph/search", summary="标题/内容关键词搜索")
def search(
    q: str = Query(..., min_length=1, description="关键词"),
    limit: int = Query(50, ge=1, le=500),
) -> dict:
    rows = store().search(q, limit)
    return {"rows": rows, "count": len(rows), "query": q}


@app.get("/telegraph/{tid}", summary="按 ID 取单条")
def by_id(tid: int) -> dict:
    row = store().get_by_id(tid)
    if not row:
        raise HTTPException(404, f"telegraph id={tid} not found")
    return row
