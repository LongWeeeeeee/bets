#!/usr/bin/env python3
"""Clamp open cards with max_runtime_seconds above a ceiling.

Default ceiling: 3600s.
Skips currently running tasks whose worker_pid is alive (don't cut live long jobs mid-flight).
Optional --exclude-ids / env KANBAN_MAXRT_CLAMP_EXCLUDE.

Writes: /root/main/runtime/kanban_maxrt_clamp_last.json
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

REPORT = Path("/root/main/runtime/kanban_maxrt_clamp_last.json")
KANBAN_DB = Path("/root/.hermes/kanban.db")
BOARDS = Path("/root/.hermes/kanban/boards")
DEFAULT_CEILING = 3600


def boards() -> list[tuple[str, Path]]:
    out = [("default", KANBAN_DB)]
    if BOARDS.is_dir():
        for p in sorted(BOARDS.glob("*/kanban.db")):
            out.append((p.parent.name, p))
    return out


def pid_alive(pid) -> bool:
    if not pid:
        return False
    try:
        return Path(f"/proc/{int(pid)}").exists()
    except Exception:
        return False


def process(name: str, db: Path, ceiling: int, exclude: set[str], dry: bool) -> dict[str, Any]:
    if not db.exists():
        return {"board": name, "error": "missing"}
    conn = sqlite3.connect(str(db), timeout=60)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id, title, status, max_runtime_seconds, worker_pid, body
        FROM tasks
        WHERE status NOT IN ('done', 'archived')
          AND IFNULL(max_runtime_seconds, 0) > ?
        """,
        (ceiling,),
    ).fetchall()
    actions = []
    for r in rows:
        tid = r["id"]
        if tid in exclude:
            actions.append(
                {
                    "board": name,
                    "task_id": tid,
                    "skipped": "excluded",
                    "maxrt": r["max_runtime_seconds"],
                    "title": (r["title"] or "")[:70],
                }
            )
            continue
        if r["status"] == "running" and pid_alive(r["worker_pid"]):
            actions.append(
                {
                    "board": name,
                    "task_id": tid,
                    "skipped": "live_running",
                    "maxrt": r["max_runtime_seconds"],
                    "pid": r["worker_pid"],
                    "title": (r["title"] or "")[:70],
                }
            )
            continue
        old = int(r["max_runtime_seconds"])
        act = {
            "board": name,
            "task_id": tid,
            "status": r["status"],
            "was": old,
            "now": ceiling,
            "title": (r["title"] or "")[:70],
        }
        if not dry:
            conn.execute(
                "UPDATE tasks SET max_runtime_seconds = ? WHERE id = ? AND IFNULL(max_runtime_seconds,0) > ?",
                (ceiling, tid, ceiling),
            )
            body = r["body"] or ""
            note = f"\n\n<!-- MAXRT_CLAMP {old}->{ceiling} ts={int(time.time())} -->\n"
            if "MAXRT_CLAMP" not in body[-2000:]:
                try:
                    conn.execute(
                        "UPDATE tasks SET body = ? WHERE id = ?",
                        ((body + note)[-50000:], tid),
                    )
                except Exception:
                    pass
            act["applied"] = True
        else:
            act["applied"] = False
        actions.append(act)
    if not dry:
        conn.commit()
    conn.close()
    applied = sum(1 for a in actions if a.get("applied"))
    return {"board": name, "actions": actions, "applied": applied, "seen": len(actions)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ceiling", type=int, default=DEFAULT_CEILING)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--exclude-ids", default="", help="comma task ids")
    ap.add_argument("--json-out", default=str(REPORT))
    args = ap.parse_args()
    exclude = {x.strip() for x in args.exclude_ids.split(",") if x.strip()}
    env_ex = os.environ.get("KANBAN_MAXRT_CLAMP_EXCLUDE", "")
    exclude |= {x.strip() for x in env_ex.split(",") if x.strip()}

    results = []
    applied = 0
    for name, db in boards():
        r = process(name, db, args.ceiling, exclude, args.dry_run)
        results.append(r)
        applied += int(r.get("applied") or 0)
    out = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ceiling": args.ceiling,
        "dry_run": args.dry_run,
        "exclude": sorted(exclude),
        "applied_total": applied,
        "boards": results,
    }
    Path(args.json_out).write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n")
    print(f"kanban_maxrt_clamp applied={applied} ceiling={args.ceiling} dry={args.dry_run}")
    for r in results:
        for a in r.get("actions") or []:
            if a.get("applied"):
                print(f"  CLAMP {a['board']}/{a['task_id']} {a['was']}->{a['now']} {a.get('title')}")
            elif a.get("skipped"):
                print(f"  SKIP {a['board']}/{a['task_id']} {a['skipped']} maxrt={a.get('maxrt')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
