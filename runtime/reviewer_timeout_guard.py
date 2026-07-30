#!/usr/bin/env python3
"""Reviewer timeout anti-loop + partial-resume guard (kanban).

Problem:
  On max_runtime timeout the dispatcher flips task → ready and re-spawns the
  SAME body cold. Partial analysis dies with the killed PID. Reviewer then
  loops wide analysis until failure_limit.

This guard (cron-safe, idempotent):
  1) For assignee=reviewer tasks that just timed out / are ready after timeout:
     - ensure staging partial path exists and is referenced
     - rewrite/append a RESUME appendix into the task body (once) pointing at
       prior attempt artifacts so the next spawn continues, not restarts
  2) Same-signature timeout without new artifact delta → block + needs_replan
     instead of burning remaining retries
  3) Clamp reviewer max_retries <= 2 (3 total attempts ceiling already global)
  4) Ensure reviewer cards have a required PARTIAL path contract in body

Also patches Reviewer SOUL with mandatory partial-checkpoint + narrow-scope rules.

Does NOT modify Hermes package sources (survives upgrades). Operates on
/root/.hermes/kanban.db + profile SOUL files.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from kanban_lane_paths import lane_home, lane_runtime_root, selected_lane
LANE = selected_lane()
RUNTIME_ROOT = lane_runtime_root(LANE)
KANBAN_DB = lane_home(LANE) / "kanban.db"
STATE_PATH = RUNTIME_ROOT / "reviewer_timeout_guard_state.json"
LOG_PATH = RUNTIME_ROOT / "reviewer_timeout_guard.log"
STAGING_ROOT = RUNTIME_ROOT / "reviewer_partials"

RESUME_MARK = "<!-- REVIEWER_RESUME_GUARD_v1 -->"
SOUL_MARK = "<!-- REVIEWER_TIMEOUT_RESUME_v1 -->"

REVIEWER_SOULS = [
    Path("/root/.hermes/profiles/reviewer/SOUL.md"),
]

SOUL_APPENDIX = f"""
{SOUL_MARK}
## Reviewer timeout / resume (mandatory, host)

Do **not** do unbounded codebase archaeology. Scope = parent INT `final/` + content hashes + owned paths listed on the card.

### Partial checkpoint (every run)
1. Within the first ~15 tool calls, create/update:
   `runtime/reviewer_partials/<task_id>/PARTIAL.md`
2. PARTIAL.md must contain:
   - goal checklist (pass/fail/unknown)
   - evidence paths already verified (+ hashes if any)
   - remaining checks only
   - draft lean verdict direction (APPROVE / ISSUES) if already clear
3. Update PARTIAL.md whenever a checklist item flips.

### On resume (body contains REVIEWER_RESUME_GUARD or PARTIAL.md exists)
- Read PARTIAL.md **first**
- Do **not** re-verify items marked pass with evidence
- Only execute remaining checks
- End with exactly one terminal line: `APPROVE` or `ISSUES` and `kanban complete` / `kanban block`

### Anti-loop
- Timeout without PARTIAL delta → stop and block/replan, do not widen scope
- Never treat “read more of the repo” as progress
- Prefer ISSUES with missing-outcome-evidence over burning the clock on broad reading
"""


def log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {msg}"
    print(line)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except Exception:
            return {}
    return {}


def save_state(st: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(st, indent=2, sort_keys=True) + "\n")
    tmp.replace(STATE_PATH)


def partial_dir(task_id: str) -> Path:
    return STAGING_ROOT / task_id


def partial_fingerprint(task_id: str) -> str:
    d = partial_dir(task_id)
    if not d.exists():
        return "missing"
    parts = []
    for p in sorted(d.rglob("*")):
        if p.is_file():
            h = hashlib.sha256(p.read_bytes()).hexdigest()[:16]
            parts.append(f"{p.relative_to(d)}:{p.stat().st_size}:{h}")
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()[:24] if parts else "empty"


def ensure_partial_stub(task_id: str, title: str) -> Path:
    d = partial_dir(task_id)
    d.mkdir(parents=True, exist_ok=True)
    p = d / "PARTIAL.md"
    if not p.exists():
        p.write_text(
            f"# Reviewer PARTIAL — {task_id}\n\n"
            f"Title: {title}\n\n"
            f"## Goal checklist\n- [ ] (fill on start)\n\n"
            f"## Verified evidence\n- (none yet)\n\n"
            f"## Remaining\n- (all)\n\n"
            f"## Draft verdict\n- unknown\n"
        )
    return p


def build_resume_appendix(task_id: str, title: str, last_err: str, attempt: int) -> str:
    p = ensure_partial_stub(task_id, title)
    return (
        f"\n\n{RESUME_MARK}\n"
        f"## RESUME / anti-loop (auto-injected, attempt≥{attempt})\n"
        f"Previous attempt ended: `{last_err or 'timed_out'}`\n"
        f"Continue from checkpoint, do **not** restart broad analysis.\n"
        f"- Required partial: `{p}`\n"
        f"- Read PARTIAL.md first; only execute remaining checks.\n"
        f"- Write/update PARTIAL.md before any wide tool use.\n"
        f"- Terminal verdict required: APPROVE or ISSUES + kanban complete/block.\n"
        f"- If no new evidence is possible from this host → ISSUES `unverifiable-from-here`.\n"
    )


def patch_soul() -> list[str]:
    touched = []
    for path in REVIEWER_SOULS:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        if SOUL_MARK in text:
            continue
        # append
        path.write_text(text.rstrip() + "\n" + SOUL_APPENDIX + "\n", encoding="utf-8")
        touched.append(str(path))
    return touched


def process_tasks(conn: sqlite3.Connection, state: dict, dry_run: bool = False) -> dict:
    stats = {
        "resume_injected": [],
        "blocked_no_delta": [],
        "retries_clamped": [],
        "stubs": [],
        "skipped": [],
    }
    rows = conn.execute(
        """
        SELECT id, title, body, status, assignee, consecutive_failures,
               max_retries, max_runtime_seconds, last_failure_error, result,
               workspace_path
        FROM tasks
        WHERE assignee = 'reviewer'
          AND status IN ('ready','todo','blocked','running')
        ORDER BY created_at DESC
        """
    ).fetchall()

    for r in rows:
        tid = r["id"]
        body = r["body"] or ""
        err = (r["last_failure_error"] or "").lower()
        fails = int(r["consecutive_failures"] or 0)
        status = r["status"]
        title = r["title"] or ""

        # Clamp max_retries for reviewer if null or >2
        mr = r["max_retries"]
        if mr is None or int(mr) > 2:
            if not dry_run:
                conn.execute("UPDATE tasks SET max_retries = 2 WHERE id = ?", (tid,))
            stats["retries_clamped"].append(tid)

        # Always ensure stub path exists for active/ready reviewers
        if status in ("ready", "todo", "running") or fails > 0:
            if not dry_run:
                ensure_partial_stub(tid, title)
            stats["stubs"].append(tid)

        timed = ("elapsed" in err and "limit" in err) or "timed_out" in err or "timeout" in err
        protocol = "protocol violation" in err

        # Inject resume appendix once when retrying after timeout/protocol
        if status in ("ready", "todo") and fails >= 1 and (timed or protocol):
            if RESUME_MARK not in body:
                appendix = build_resume_appendix(tid, title, r["last_failure_error"] or "", fails + 1)
                new_body = body.rstrip() + appendix
                if not dry_run:
                    conn.execute("UPDATE tasks SET body = ? WHERE id = ?", (new_body, tid))
                stats["resume_injected"].append(tid)
            else:
                stats["skipped"].append(f"{tid}:resume_present")

        # Same-signature timeout loop breaker: 2+ timeouts, no partial delta
        fp = partial_fingerprint(tid)
        st_key = f"task:{tid}"
        prev = state.get(st_key) or {}
        prev_fp = prev.get("fp")
        prev_fails = int(prev.get("fails") or 0)
        if timed and fails >= 2 and status in ("ready", "todo"):
            if prev_fp == fp and fp in ("missing", "empty") or (prev_fp == fp and fails > prev_fails):
                # no progress → block
                reason = (
                    "reviewer-timeout-guard: same-signature timeout without PARTIAL delta; "
                    "block for REPLAN/split (narrow scope) instead of blind relaunch"
                )
                if not dry_run and status != "blocked":
                    conn.execute(
                        """
                        UPDATE tasks
                        SET status = 'blocked',
                            block_kind = COALESCE(NULLIF(block_kind, ''), 'needs_replan'),
                            last_failure_error = ?,
                            claim_lock = NULL,
                            claim_expires = NULL,
                            worker_pid = NULL
                        WHERE id = ? AND status IN ('ready','todo')
                        """,
                        (reason, tid),
                    )
                    # best-effort event
                    try:
                        conn.execute(
                            """
                            INSERT INTO task_events (task_id, type, payload, created_at)
                            VALUES (?, 'blocked', ?, ?)
                            """,
                            (
                                tid,
                                json.dumps({"by": "reviewer_timeout_guard", "reason": reason, "fp": fp}),
                                int(time.time()),
                            ),
                        )
                    except Exception:
                        pass
                stats["blocked_no_delta"].append(tid)

        state[st_key] = {
            "fp": fp,
            "fails": fails,
            "status": status,
            "ts": int(time.time()),
            "err": (r["last_failure_error"] or "")[:200],
        }

    return stats


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--soul-only", action="store_true")
    args = ap.parse_args(argv)

    touched_soul = patch_soul()
    if touched_soul:
        log(f"soul_patched {touched_soul}")
    else:
        log("soul_ok (already patched or missing)")

    if args.soul_only:
        return 0

    if not KANBAN_DB.exists():
        log(f"FAIL no kanban db {KANBAN_DB}")
        return 2

    state = load_state()
    conn = sqlite3.connect(str(KANBAN_DB))
    conn.row_factory = sqlite3.Row
    try:
        if not args.dry_run:
            conn.execute("BEGIN")
        stats = process_tasks(conn, state, dry_run=args.dry_run)
        if not args.dry_run:
            conn.commit()
            save_state(state)
        log("stats " + json.dumps(stats, ensure_ascii=False))
        # nest into hygiene report side file
        (RUNTIME_ROOT / "reviewer_timeout_guard_last.json").write_text(
            json.dumps({"ts": int(time.time()), "stats": stats, "dry_run": args.dry_run}, indent=2)
            + "\n"
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
