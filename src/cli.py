"""命令行查询工具。

用法:
  python src/cli.py stats                 # 概览
  python src/cli.py tail [N]              # 最新 N 条(默认 20)
  python src/cli.py search KEYWORD [N]    # 关键词搜索(默认 50)
  python src/cli.py export FILE.json [N]  # 导出最新 N 条到 JSON
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from db import Store  # noqa: E402


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.environ.get("CLS_DB_PATH", PROJECT_ROOT / "data" / "cls.db"))


def _store() -> Store:
    if not DB_PATH.exists():
        print(f"db not found: {DB_PATH}", file=sys.stderr)
        print("  daemon hasn't run yet — start it with `bash scripts/run_daemon.sh`", file=sys.stderr)
        sys.exit(2)
    return Store(DB_PATH)


def cmd_stats() -> int:
    s = _store().stats()
    print(f"  total telegraph     : {s['total_telegraph']}")
    print(f"  earliest pub        : {s['earliest_pub']}")
    print(f"  latest pub          : {s['latest_pub']}")
    print(f"  fetches last hour   : {s['fetches_last_hour']}")
    print(f"  new rows last hour  : {s['new_rows_last_hour']}")
    print(f"  avg fetch ms        : {s['avg_fetch_ms_last_hour']}")
    print(f"  errors last hour    : {s['errors_last_hour']}")
    return 0


def cmd_tail(argv: list[str]) -> int:
    n = int(argv[0]) if argv else 20
    rows = _store().latest(n)
    for r in rows:
        title = r["title"]
        content = r["content"]
        if len(content) > 200:
            content = content[:200] + "..."
        print(f"[{r['pub_dt']}] {title}")
        if content and content != title:
            print(f"  {content}")
        print()
    return 0


def cmd_search(argv: list[str]) -> int:
    if not argv:
        print("usage: cli.py search KEYWORD [N]", file=sys.stderr)
        return 2
    keyword = argv[0]
    n = int(argv[1]) if len(argv) > 1 else 50
    rows = _store().search(keyword, n)
    print(f"# {len(rows)} 条匹配 '{keyword}'\n")
    for r in rows:
        content = r["content"]
        if len(content) > 200:
            content = content[:200] + "..."
        print(f"[{r['pub_dt']}] {r['title']}")
        if content and content != r["title"]:
            print(f"  {content}")
        print()
    return 0


def cmd_export(argv: list[str]) -> int:
    if not argv:
        print("usage: cli.py export FILE.json [N]", file=sys.stderr)
        return 2
    out = Path(argv[0])
    n = int(argv[1]) if len(argv) > 1 else 1000
    rows = _store().latest(n)
    out.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {len(rows)} rows -> {out}")
    return 0


def main(argv: list[str]) -> int:
    if not argv or argv[0] in {"-h", "--help", "help"}:
        print(__doc__)
        return 0
    cmd, rest = argv[0], argv[1:]
    if cmd == "stats":
        return cmd_stats()
    if cmd == "tail":
        return cmd_tail(rest)
    if cmd == "search":
        return cmd_search(rest)
    if cmd == "export":
        return cmd_export(rest)
    print(f"unknown command: {cmd}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
