#!/usr/bin/env python3
"""Stop identical protocol/timeout burns with zero artifact delta.

Policy (anti-stall):
  - protocol_violation ×2 same error fingerprint + 0 salvage/artifact delta
    → block needs_replan (do not redispatch same body)
  - timed_out ×2 + 0 PARTIAL/salvage pack/artifact delta
    → block needs_replan
  - Does NOT touch currently running tasks
  - Does NOT archive; only blocks ready/todo that would burn again

Safe under hygiene cron. Writes:
  /root/main/runtime/kanban_retry_enforcer_last.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

from kanban_lane_paths import discover_boards as discover_lane_boards
from kanban_lane_paths import lane_home, lane_runtime_root, selected_lane

LANE = selected_lane()
HERMES_HOME = lane_home(LANE)
RUNTIME_ROOT = lane_runtime_root(LANE)
REPORT = RUNTIME_ROOT / "kanban_retry_enforcer_last.json"
KANBAN_DB = HERMES_HOME / "kanban.db"
BOARDS = HERMES_HOME / "kanban/boards"
SALVAGE_ROOT = RUNTIME_ROOT / "task_salvage"
PARTIAL_ROOTS = [
    RUNTIME_ROOT / "reviewer_partials",
    RUNTIME_ROOT / "task_partials",
]


def boards() -> list[tuple[str, Path]]:
    return discover_lane_boards(HERMES_HOME)


def fp_err(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\d+", "N", s)
    s = re.sub(r"\s+", " ", s)[:200]
    return hashlib.sha1(s.encode()).hexdigest()[:12]


def has_progress(task_id: str, meta: dict | None = None) -> bool:
    """True if salvage pack / PARTIAL / non-empty staging markers exist."""
    meta = meta or {}
    for k in ("salvage_dir", "salvage_md", "log_path"):
        v = meta.get(k)
        if v and Path(str(v)).exists():
            # log alone is weak; require pack md or partial for "progress"
            if k == "log_path":
                continue
            return True
    sdir = SALVAGE_ROOT / task_id
    if sdir.is_dir():
        for p in sdir.glob("run_*/SALVAGE.md"):
            try:
                if p.stat().st_size > 40:
                    # check artifacts.json nonempty or PARTIAL sibling
                    arts = p.parent / "artifacts.json"
                    if arts.exists():
                        try:
                            data = json.loads(arts.read_text())
                            if data:
                                return True
                        except Exception:
                            pass
                    if (p.parent / "session_excerpt.md").exists():
                        return True
                    # SALVAGE.md alone after crash still counts as recoverable progress
                    # BUT for zero-delta retry breaker we need stronger signal:
                    # treat bare salvage without arts/session as weak → False below
            except Exception:
                pass
    for root in PARTIAL_ROOTS:
        pm = root / task_id / "PARTIAL.md"
        if pm.exists() and pm.stat().st_size > 30:
            return True
    return False


def classify_run(outcome: str | None, error: str | None) -> Optional[str]:
    o = (outcome or "").lower()
    e = (error or "").lower()
    if "protocol" in e or "without calling kanban" in e or o == "protocol_violation":
        return "protocol"
    if o == "timed_out" or "elapsed" in e and "limit" in e or "iteration budget" in e:
        return "timeout"
    return None


def process_board(name: str, db: Path, dry: bool) -> dict[str, Any]:
    if not db.exists():
        return {"board": name, "error": "missing"}
    conn = sqlite3.connect(str(db), timeout=60)
    conn.row_factory = sqlite3.Row
    actions: list[dict] = []
    # Candidates: ready/todo (will block) + blocked without marker (stamp only)
    tasks = conn.execute(
        """
        SELECT id, title, status, body, consecutive_failures, last_failure_error,
               max_runtime_seconds, assignee
        FROM tasks
        WHERE status IN ('ready', 'todo', 'blocked')
          AND IFNULL(consecutive_failures, 0) >= 2
        ORDER BY consecutive_failures DESC
        LIMIT 400
        """
    ).fetchall()

    for t in tasks:
        tid = t["id"]
        runs = conn.execute(
            """
            SELECT id, outcome, error, summary, metadata, started_at, ended_at
            FROM task_runs
            WHERE task_id = ?
            ORDER BY id DESC
            LIMIT 6
            """,
            (tid,),
        ).fetchall()
        fails = int(t["consecutive_failures"] or 0)
        body = t["body"] or ""
        # Skip if already stamped
        if "RETRY_ENFORCER_NEEDS_REPLAN" in body:
            continue

        classes: list[str] = []
        fps: list[str] = []
        metas: list[dict] = []
        for r in runs[:4]:
            cls = classify_run(r["outcome"], r["error"] or t["last_failure_error"])
            if not cls:
                # stop streak on non-matching outcome
                if classes:
                    break
                continue
            classes.append(cls)
            fps.append(fp_err(r["error"] or t["last_failure_error"] or r["outcome"] or ""))
            try:
                m = json.loads(r["metadata"] or "{}")
                if not isinstance(m, dict):
                    m = {}
            except Exception:
                m = {}
            metas.append(m)

        # Fallback: last_failure_error + fails>=2 without two classified runs
        if len(classes) < 2:
            fb = classify_run(None, t["last_failure_error"] or "")
            if fb and fails >= 2:
                classes = [fb, fb]
                fps = [fp_err(t["last_failure_error"] or ""), fp_err(t["last_failure_error"] or "")]
            else:
                continue
        # same class last 2
        if classes[0] != classes[1]:
            continue
        # same fingerprint preferred; if empty both, still count
        if fps[0] and fps[1] and fps[0] != fps[1]:
            continue

        # Protocol: salvage pack without real arts is NOT progress — still stop retries.
        # Timeout: require PARTIAL/arts to consider progress; bare salvage alone is weak.
        progress = False
        if classes[0] == "timeout":
            progress = has_progress(tid, metas[0] if metas else None) or (
                has_progress(tid, metas[1]) if len(metas) > 1 else False
            )
            # bare salvage.md alone: has_progress returns True only with arts/session — OK
        if progress:
            continue

        cls = classes[0]
        reason = (
            f"retry_enforcer: {cls}×{min(len(classes), fails)} identical + 0 artifact delta "
            f"— needs_replan (no more identical body dispatch)"
        )
        act = {
            "board": name,
            "task_id": tid,
            "title": (t["title"] or "")[:80],
            "class": cls,
            "fails": fails,
            "fps": fps[:3],
            "status_from": t["status"],
            "reason": reason,
        }
        if not dry:
            if t["status"] in ("ready", "todo"):
                # block + stamp error; leave body for planner
                conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'blocked',
                        claim_lock = NULL,
                        claim_expires = NULL,
                        worker_pid = NULL,
                        last_failure_error = ?,
                        last_heartbeat_at = NULL
                    WHERE id = ? AND status IN ('ready', 'todo')
                    """,
                    (reason[:1800], tid),
                )
            else:
                # already blocked — only refresh error stamp if protocol/timeout class
                conn.execute(
                    """
                    UPDATE tasks
                    SET last_failure_error = COALESCE(?, last_failure_error)
                    WHERE id = ? AND status = 'blocked'
                    """,
                    (reason[:1800], tid),
                )
            # comment-like event if table exists
            try:
                cols = [r[1] for r in conn.execute("PRAGMA table_info(task_events)")]
                if cols:
                    # kind + payload flexible
                    now = int(time.time())
                    if "created_at" in cols or "ts" in cols:
                        ts_col = "created_at" if "created_at" in cols else "ts"
                        conn.execute(
                            f"INSERT INTO task_events (task_id, kind, payload, {ts_col}) VALUES (?,?,?,?)",
                            (
                                tid,
                                "retry_enforcer",
                                json.dumps(
                                    {"class": cls, "reason": reason, "fps": fps[:3]},
                                    ensure_ascii=False,
                                ),
                                now,
                            ),
                        )
            except Exception:
                pass
            # append marker into body if no REPLAN marker yet
            marker = "\n\n<!-- RETRY_ENFORCER_NEEDS_REPLAN -->\n" + reason + "\n"
            if "RETRY_ENFORCER_NEEDS_REPLAN" not in body:
                try:
                    conn.execute(
                        "UPDATE tasks SET body = ? WHERE id = ?",
                        ((body + marker)[-50000:], tid),
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
    return {"board": name, "actions": actions, "count": len(actions)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--json-out", default=str(REPORT))
    args = ap.parse_args()
    results = []
    total = 0
    for name, db in boards():
        r = process_board(name, db, args.dry_run)
        results.append(r)
        total += int(r.get("count") or 0)
    out = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dry_run": args.dry_run,
        "total_actions": total,
        "boards": results,
    }
    Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.json_out).write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n")
    print(f"kanban_retry_enforcer actions={total} dry={args.dry_run}")
    for r in results:
        for a in r.get("actions") or []:
            print(
                f"  {a.get('board')}/{a.get('task_id')} {a.get('class')} fails={a.get('fails')} applied={a.get('applied')} {(a.get('title') or '')[:60]}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
