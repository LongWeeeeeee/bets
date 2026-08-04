#!/usr/bin/env python3
"""Read-only blocked-card triage across Hermes boards.

Classifies blocked tasks into actionable buckets for replan/archive/owner.
Does NOT mutate kanban (report only).

Output: /root/main/runtime/blocked_triage_latest.json
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

REPORT = Path("/root/main/runtime/blocked_triage_latest.json")
KANBAN_DB = Path("/root/.hermes/kanban.db")
BOARDS = Path("/root/.hermes/kanban/boards")


def boards() -> list[tuple[str, Path]]:
    out = [("default", KANBAN_DB)]
    if BOARDS.is_dir():
        for p in sorted(BOARDS.glob("*/kanban.db")):
            out.append((p.parent.name, p))
    return out


def classify(title: str, err: str, summary: str, fails: int) -> str:
    blob = f"{title}\n{err}\n{summary}".lower()
    if "retry_enforcer" in blob or "needs_replan" in blob and "protocol" in blob:
        return "protocol_exhausted"
    if "without calling kanban" in blob or "protocol violation" in blob:
        return "protocol_exhausted" if fails >= 2 else "protocol_once"
    if "needs_input" in blob or "need user" in blob or "user decision" in blob:
        return "needs_input"
    if "issues iteration" in blob or "reviewer" in blob and "issues" in blob:
        return "reviewer_issues"
    if "waiting for planner" in blob or "amendment" in blob:
        return "waiting_planner"
    if "parent contract" in blob or "superseded" in blob or "stale integration" in blob:
        return "graph_invalid"
    if "ssh" in blob or "host" in blob and "unavail" in blob:
        return "infra_external"
    if "iteration budget" in blob or "timed_out" in blob or "elapsed" in blob and "limit" in blob:
        return "timeout_exhausted" if fails >= 2 else "timeout_once"
    if "authorized" in blob and "unavailable" in blob:
        return "infra_external"
    if fails >= 3 and not (err or "").strip() and "waiting" in blob:
        return "waiting_external"
    if fails >= 3:
        return "failed_exhausted"
    if not (err or "").strip() and summary:
        return "blocked_clean"  # intentional kanban block with summary
    if not (err or "").strip():
        return "blocked_empty"
    return "other"


def triage_board(name: str, db: Path) -> dict[str, Any]:
    if not db.exists():
        return {"board": name, "error": "missing"}
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    now = int(time.time())
    rows = conn.execute(
        """
        SELECT t.id, t.title, t.assignee, t.consecutive_failures,
               t.last_failure_error, t.created_at, t.started_at, t.completed_at,
               t.max_runtime_seconds,
               (SELECT outcome FROM task_runs r WHERE r.task_id=t.id ORDER BY r.id DESC LIMIT 1) AS last_outcome,
               (SELECT substr(COALESCE(summary,''),1,240) FROM task_runs r WHERE r.task_id=t.id ORDER BY r.id DESC LIMIT 1) AS last_summary,
               (SELECT COUNT(*) FROM task_runs r WHERE r.task_id=t.id) AS nruns
        FROM tasks t
        WHERE t.status = 'blocked'
        ORDER BY COALESCE(t.started_at, t.created_at) ASC
        """
    ).fetchall()
    items = []
    by = Counter()
    for r in rows:
        err = r["last_failure_error"] or ""
        summary = r["last_summary"] or ""
        fails = int(r["consecutive_failures"] or 0)
        cls = classify(r["title"] or "", err, summary, fails)
        by[cls] += 1
        base = r["started_at"] or r["created_at"] or now
        try:
            age_h = (now - int(base)) / 3600.0
        except Exception:
            age_h = None
        items.append(
            {
                "task_id": r["id"],
                "title": (r["title"] or "")[:100],
                "class": cls,
                "fails": fails,
                "nruns": r["nruns"],
                "last_outcome": r["last_outcome"],
                "age_h": round(age_h, 1) if age_h is not None else None,
                "assignee": r["assignee"],
                "maxrt": r["max_runtime_seconds"],
                "err": (err or "")[:120],
                "summary": (summary or "")[:160],
                "suggested_action": {
                    "needs_input": "user digest — do not redispatch",
                    "reviewer_issues": "planner replan open signatures only",
                    "waiting_planner": "ensure planner card exists / unstick",
                    "graph_invalid": "fix parent edges / supersede cleanup",
                    "infra_external": "keep blocked until host/SSH restored",
                    "protocol_exhausted": "split/replan body — no identical retry",
                    "protocol_once": "one salvage retry max then replan",
                    "timeout_exhausted": "fat-job split build+watch; clamp maxrt",
                    "timeout_once": "resume from salvage if pack else split",
                    "failed_exhausted": "replan or archive if stale>30d",
                    "blocked_clean": "read summary — often intentional gate",
                    "blocked_empty": "inspect last run summary/logs",
                    "waiting_external": "owner ping / deadline",
                    "other": "manual inspect",
                }.get(cls, "manual"),
            }
        )
    conn.close()
    return {
        "board": name,
        "blocked_total": len(items),
        "by_class": dict(by),
        "items": items,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json-out", default=str(REPORT))
    ap.add_argument("--top", type=int, default=15)
    args = ap.parse_args()
    board_reports = [triage_board(n, p) for n, p in boards()]
    total = sum(int(b.get("blocked_total") or 0) for b in board_reports)
    all_cls: Counter = Counter()
    for b in board_reports:
        all_cls.update(b.get("by_class") or {})
    out = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "blocked_total": total,
        "by_class": dict(all_cls),
        "boards": board_reports,
    }
    Path(args.json_out).write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n")
    print(f"blocked_triage total={total} classes={dict(all_cls)}")
    for b in board_reports:
        if not b.get("blocked_total"):
            continue
        print(f"  board={b['board']} n={b['blocked_total']} {b.get('by_class')}")
        # show top oldest per board
        items = sorted(b.get("items") or [], key=lambda x: -(x.get("age_h") or 0))
        for it in items[: args.top]:
            print(
                f"    {it['class']:18} fails={it['fails']} age_h={it['age_h']} {it['task_id']} {it['title'][:50]}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
