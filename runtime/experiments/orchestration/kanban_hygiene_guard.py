#!/usr/bin/env python3
"""Kanban hygiene + stuck + assignee guard across all Hermes boards.

Scans:
  - /root/.hermes/kanban.db (legacy/default)
  - /root/.hermes/kanban/boards/*/kanban.db

Actions:
  - assignee guard: implement-like cards on assignee=default -> reassign worker
  - stuck: running with dead worker_pid or stale heartbeat -> block (transient)
  - hygiene warnings: blocked>24h, unassigned ready/todo, running on default
  - nested plan-lint: fat mono-cards, protocol×2 streak, max_retries>3
    (max parallel fan-out: staging Workers + single INT)
  - nested task_salvage_guard: timeout/crash packs + resume markers
  - nested kanban_process_logger: live workers / running tasks / process anomalies
  - nested kanban_retry_enforcer: protocol/timeout ×2 + 0Δ → needs_replan
  - quiet notify: drop kanban_notify_subs unless block_kind=needs_input (active)

Report: /root/main/runtime/kanban_hygiene_last.json
        /root/main/runtime/kanban_plan_lint_last.json
        /root/main/runtime/kanban_process_log/latest.json
"""
from __future__ import annotations
# --- bootstrap раскладки: соседние эксперименты живут в runtime/experiments/<тема>/
import sys as _sys, pathlib as _pathlib
_repo_root = next((p for p in _pathlib.Path(__file__).resolve().parents if (p / '.git').exists()), None)
if _repo_root is not None:
    for _exp_dir in sorted((_repo_root / 'runtime' / 'experiments').glob('*')):
        if _exp_dir.is_dir() and str(_exp_dir) not in _sys.path:
            _sys.path.insert(0, str(_exp_dir))

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from kanban_lane_paths import discover_boards as discover_lane_boards
from kanban_lane_paths import lane_home, lane_runtime_root, selected_lane

LANE = selected_lane()
HERMES_HOME = lane_home(LANE)
RUNTIME_ROOT = lane_runtime_root(LANE)
REPORT_PATH = RUNTIME_ROOT / "kanban_hygiene_last.json"
HERMES_BIN = os.environ.get("HERMES_BIN", "/usr/local/lib/hermes-agent/venv/bin/hermes")
STUCK_SECONDS = int(os.environ.get("KANBAN_STUCK_SECONDS", str(45 * 60)))
# Anti-hoarding policy: blocked cards must not live forever. needs_input ages
# out fast (the human is not a queue); everything else gets 30d. Archived cards
# stay in the DB (status=archived) and are mirrored to ARCHIVE_MANIFEST (jsonl)
# so the sweep is fully recoverable. Cards with live children outside the
# archive set are skipped (never orphan a dispatchable dependency edge).
AUTO_ARCHIVE_BLOCKED_DAYS = int(os.environ.get("KANBAN_AUTO_ARCHIVE_BLOCKED_DAYS", "30"))
NEEDS_INPUT_MAX_AGE_DAYS = int(os.environ.get("KANBAN_NEEDS_INPUT_MAX_AGE_DAYS", "7"))
MAX_RETRIES_CAP = int(os.environ.get("KANBAN_MAX_RETRIES_CAP", "3"))
ARCHIVE_MANIFEST = Path(
    os.environ.get(
        "KANBAN_ARCHIVE_MANIFEST", str(RUNTIME_ROOT / "kanban_archive_manifest.jsonl")
    )
)

IMPLEMENT_HINT = re.compile(
    r"(?i)\b(W\d+|implement|fix|REPLAN|patch|code|worker|INT-|build|test)\b"
)


def now_ts() -> float:
    return time.time()


def parse_ts(val: Any) -> Optional[float]:
    if val is None or val == "":
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return None
    try:
        if re.fullmatch(r"\d+(\.\d+)?", s):
            return float(s)
    except ValueError:
        pass
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return None


def pid_alive(pid: Any) -> bool:
    if not pid:
        return False
    try:
        pid_i = int(pid)
    except Exception:
        return False
    try:
        os.kill(pid_i, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def discover_boards() -> list[tuple[str, Path]]:
    return discover_lane_boards(HERMES_HOME)


def run_hermes(board: str, args: list[str], dry: bool) -> tuple[int, str]:
    cmd = [HERMES_BIN, "kanban", "--board", board, *args]
    env = os.environ.copy()
    env["HERMES_HOME"] = str(HERMES_HOME)
    if dry:
        return 0, "DRY_RUN: " + " ".join(cmd)
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=90)
        out = (p.stdout or "") + (p.stderr or "")
        return p.returncode, out.strip()
    except Exception as e:
        return 2, "error: " + str(e)


def is_implement_card(title: str, body: str) -> bool:
    if re.search(r"(?i)^PLAN\b", title or ""):
        return False
    if re.search(r"(?i)^REVIEW\b", title or ""):
        return False
    text = (title or "") + "\n" + (body or "")
    if IMPLEMENT_HINT.search(text):
        return True
    if re.search(
        r"(?i)EXCLUSIVE OWNERSHIP|REQUIRED BEHAVIOR|acceptance evidence",
        body or "",
    ):
        return True
    return False


def load_tasks(db: Path) -> list[dict[str, Any]]:
    con = sqlite3.connect(str(db))
    con.row_factory = sqlite3.Row
    cols = [r[1] for r in con.execute("pragma table_info(tasks)")]
    if not cols:
        return []
    rows = con.execute(
        "select * from tasks where status not in ('done','archived')"
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        d: dict[str, Any] = {}
        for c in cols:
            d[c] = r[c]
        out.append(d)
    con.close()
    return out



def load_links(db: Path) -> list[tuple[str, str]]:
    try:
        con = sqlite3.connect(str(db))
        rows = [
            (str(p), str(c))
            for p, c in con.execute("select parent_id, child_id from task_links")
        ]
        con.close()
        return rows
    except Exception:
        return []


def stale_archive_sweep(
    board: str, db: Path, tasks: list[dict[str, Any]], dry: bool
) -> list[dict[str, Any]]:
    """Archive stale blocked cards: needs_input > NEEDS_INPUT_MAX_AGE_DAYS,
    all other block kinds > AUTO_ARCHIVE_BLOCKED_DAYS.

    Safety: a card whose active (non-done/archived) children are not all in the
    archive set is skipped, so no live child keeps a dependency edge into an
    archived parent. Children are archived before parents (pass loop). Every
    archived card is mirrored to ARCHIVE_MANIFEST (jsonl, append-only).
    """
    actions: list[dict[str, Any]] = []
    active_status = {str(t.get("id")): str(t.get("status")) for t in tasks}
    children: dict[str, list[str]] = defaultdict(list)
    for p, c in load_links(db):
        children[p].append(c)

    candidates: dict[str, tuple[str, float]] = {}
    for t in tasks:
        if str(t.get("status")) != "blocked":
            continue
        tid = str(t.get("id"))
        anchor = parse_ts(t.get("updated_at")) or parse_ts(t.get("created_at"))
        if anchor is None:
            continue
        age_d = (now_ts() - anchor) / 86400.0
        bk = str(t.get("block_kind") or "").strip()
        if bk == "needs_input" and age_d > NEEDS_INPUT_MAX_AGE_DAYS:
            candidates[tid] = (
                "needs_input stale %.0fd > %dd" % (age_d, NEEDS_INPUT_MAX_AGE_DAYS),
                age_d,
            )
        elif bk != "needs_input" and age_d > AUTO_ARCHIVE_BLOCKED_DAYS:
            candidates[tid] = (
                "blocked stale %.0fd > %dd" % (age_d, AUTO_ARCHIVE_BLOCKED_DAYS),
                age_d,
            )

    # fixpoint: drop candidates with an active child outside the candidate set
    changed = True
    while changed:
        changed = False
        for tid in list(candidates):
            for ch in children.get(tid, []):
                st = active_status.get(ch)
                if ch not in candidates and st not in (None, "done", "archived"):
                    del candidates[tid]
                    changed = True
                    break

    title_by_id = {str(t.get("id")): str(t.get("title") or "") for t in tasks}
    assignee_by_id = {
        str(t.get("id")): (str(t.get("assignee")) if t.get("assignee") else None)
        for t in tasks
    }
    bk_by_id = {str(t.get("id")): str(t.get("block_kind") or "") for t in tasks}

    archived: set[str] = set()
    remaining = dict(candidates)
    passes = 0
    while remaining and passes < 10:
        passes += 1
        progressed = False
        for tid, (reason, age_d) in list(remaining.items()):
            live_children = [
                ch
                for ch in children.get(tid, [])
                if ch not in archived
                and active_status.get(ch) not in (None, "done", "archived")
            ]
            if live_children:
                continue
            comment = (
                "hygiene-guard: auto-archive (%s). Recoverable via manifest %s"
                % (reason, ARCHIVE_MANIFEST)
            )
            run_hermes(board, ["comment", tid, comment], dry)
            rc, out = run_hermes(board, ["archive", tid], dry)
            entry = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "board": board,
                "task_id": tid,
                "title": title_by_id.get(tid, "")[:200],
                "assignee": assignee_by_id.get(tid),
                "block_kind": bk_by_id.get(tid),
                "age_days": round(age_d, 1),
                "reason": reason,
                "dry_run": dry,
                "archive_rc": rc,
            }
            if not dry:
                try:
                    ARCHIVE_MANIFEST.parent.mkdir(parents=True, exist_ok=True)
                    with ARCHIVE_MANIFEST.open("a") as f:
                        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                except Exception:
                    pass
            archived.add(tid)
            del remaining[tid]
            progressed = True
            actions.append(
                {
                    "type": "auto_archive",
                    "board": board,
                    "task_id": tid,
                    "title": title_by_id.get(tid, "")[:120],
                    "reason": reason,
                    "rc": rc,
                    "out": out[:300],
                    "dry_run": dry,
                }
            )
        if not progressed:
            break
    for tid, (reason, age_d) in remaining.items():
        actions.append(
            {
                "type": "auto_archive_skip_active_children",
                "board": board,
                "task_id": tid,
                "title": title_by_id.get(tid, "")[:120],
                "reason": reason,
                "dry_run": dry,
            }
        )
    return actions


def clamp_max_retries(
    board: str, db: Path, tasks: list[dict[str, Any]], dry: bool
) -> list[dict[str, Any]]:
    """Anti-stall policy: active cards must not carry max_retries > MAX_RETRIES_CAP.

    No kanban CLI verb exists for max_retries; direct sqlite UPDATE follows the
    same precedent as quiet_notify_cleanup's direct DELETE on kanban_notify_subs.
    """
    ids: list[tuple[str, int, str]] = []
    for t in tasks:
        try:
            mr_i = int(t.get("max_retries"))
        except Exception:
            continue
        if mr_i > MAX_RETRIES_CAP and str(t.get("status")) not in ("done", "archived"):
            ids.append((str(t.get("id")), mr_i, str(t.get("title") or "")[:120]))
    if not ids:
        return []
    if not dry:
        try:
            con = sqlite3.connect(str(db))
            con.executemany(
                "update tasks set max_retries=? where id=?",
                [(MAX_RETRIES_CAP, tid) for tid, _, _ in ids],
            )
            con.commit()
            con.close()
        except Exception as e:
            return [{"type": "max_retries_clamp_error", "board": board, "error": str(e)}]
    return [
        {
            "type": "max_retries_clamp",
            "board": board,
            "task_id": tid,
            "from": mr,
            "to": MAX_RETRIES_CAP,
            "title": title,
            "dry_run": dry,
        }
        for tid, mr, title in ids
    ]


def _notify_home_target() -> tuple[str, str, str]:
    """Return (platform, chat_id, notifier_profile) for human kanban asks."""
    # Prefer default gateway home — it owns dispatcher + notifier loop.
    env_paths = [
        HERMES_HOME / ".env",
        HERMES_HOME / "profiles" / "worker" / ".env",
    ]
    chat = "7543801207"
    for ep in env_paths:
        if not ep.exists():
            continue
        for line in ep.read_text(errors="replace").splitlines():
            if line.startswith("TELEGRAM_HOME_CHANNEL="):
                chat = line.split("=", 1)[1].strip() or chat
                break
    return "telegram", chat, "default"


def quiet_notify_cleanup(dry: bool) -> list[dict[str, Any]]:
    """Quiet non-human kanban notify subs AND repair missing needs_input asks.

    User preference: Telegram push only for needs_input / true human decision,
    not intermediate auto-retry / done / archived / transient blocks.

    Keep only: status not in {done, archived} AND block_kind == needs_input.

    Repair path (why asks may never arrive):
      1) card becomes needs_input blocked without a notify sub (worker/API block,
         not /kanban create auto-subscribe)
      2) quiet_notify previously dropped ALL non-needs_input — including before
         block_kind was set / race with hygiene
      3) sub missing → gateway notifier has nothing to deliver

    For each active needs_input without a home sub: INSERT sub with
    last_event_id = max(0, last_blocked_event_id-1) so the latest blocked
    event is reclaimed once by the gateway notifier.
    """
    actions: list[dict[str, Any]] = []
    platform, home_chat, notifier_profile = _notify_home_target()
    dbs = [HERMES_HOME / "kanban.db"]
    boards = HERMES_HOME / "kanban" / "boards"
    if boards.is_dir():
        dbs.extend(sorted(boards.glob("*/kanban.db")))
    for db in dbs:
        if not db.exists():
            continue
        con = sqlite3.connect(str(db))
        con.row_factory = sqlite3.Row
        try:
            tables = {
                r[0]
                for r in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if "kanban_notify_subs" not in tables or "tasks" not in tables:
                continue
            rows = list(
                con.execute(
                    """
                    SELECT s.task_id, s.platform, s.chat_id, ifnull(s.thread_id,'') AS thread_id,
                           ifnull(t.status,'') AS status, ifnull(t.block_kind,'') AS block_kind,
                           ifnull(t.title,'') AS title
                    FROM kanban_notify_subs s
                    LEFT JOIN tasks t ON t.id = s.task_id
                    """
                )
            )
            dropped = 0
            kept = 0
            for r in rows:
                status = (r["status"] or "").strip()
                bk = (r["block_kind"] or "").strip()
                # Keep human gates: needs_input (and capability treated as human when blocked)
                keep = status not in {"done", "archived"} and bk == "needs_input"
                if keep:
                    kept += 1
                    continue
                dropped += 1
                if not dry:
                    con.execute(
                        """
                        DELETE FROM kanban_notify_subs
                        WHERE task_id=? AND platform=? AND chat_id=? AND ifnull(thread_id,'')=?
                        """,
                        (r["task_id"], r["platform"], r["chat_id"], r["thread_id"]),
                    )
                actions.append(
                    {
                        "type": "quiet_notify_drop_sub",
                        "board_db": str(db),
                        "task_id": r["task_id"],
                        "status": status,
                        "block_kind": bk,
                        "title": (r["title"] or "")[:120],
                        "platform": r["platform"],
                        "chat_id": r["chat_id"],
                    }
                )

            # Repair: needs_input + capability blocked cards must have home sub
            repaired = 0
            human_rows = list(
                con.execute(
                    """
                    SELECT id, status, ifnull(block_kind,'') AS block_kind,
                           substr(ifnull(title,''),1,120) AS title,
                           substr(ifnull(last_failure_error,''),1,160) AS err
                    FROM tasks
                    WHERE status = 'blocked'
                      AND ifnull(block_kind,'') = 'needs_input'
                    """
                )
            )
            existing = {
                (
                    r["task_id"],
                    (r["platform"] or "").lower(),
                    str(r["chat_id"]),
                    r["thread_id"] or "",
                )
                for r in rows
            }
            # also reload after drops
            if not dry and dropped:
                con.commit()
                existing = {
                    (
                        r["task_id"],
                        (r["platform"] or "").lower(),
                        str(r["chat_id"]),
                        (r["thread_id"] or ""),
                    )
                    for r in con.execute(
                        "SELECT task_id, platform, chat_id, ifnull(thread_id,'') AS thread_id "
                        "FROM kanban_notify_subs"
                    )
                }

            now = int(time.time())
            for t in human_rows:
                key = (t["id"], platform, str(home_chat), "")
                if key in existing:
                    continue
                # cursor just before latest blocked event so notifier re-delivers ask
                last_ev = con.execute(
                    """
                    SELECT id FROM task_events
                    WHERE task_id = ? AND kind IN ('blocked','gave_up','crashed','timed_out')
                    ORDER BY id DESC LIMIT 1
                    """,
                    (t["id"],),
                ).fetchone()
                # Pin at latest terminal event: future blocks notify; no historical flood.
                cursor = int(last_ev["id"]) if last_ev is not None else 0
                if not dry:
                    con.execute(
                        """
                        INSERT OR IGNORE INTO kanban_notify_subs
                            (task_id, platform, chat_id, thread_id, user_id,
                             notifier_profile, created_at, last_event_id)
                        VALUES (?, ?, ?, '', ?, ?, ?, ?)
                        """,
                        (
                            t["id"],
                            platform,
                            str(home_chat),
                            str(home_chat),
                            notifier_profile,
                            now,
                            cursor,
                        ),
                    )
                    # If row existed with different profile/cursor empty — ensure cursor
                    con.execute(
                        """
                        UPDATE kanban_notify_subs
                           SET last_event_id = CASE
                                 WHEN last_event_id = 0 OR last_event_id IS NULL THEN ?
                                 WHEN last_event_id >= ? THEN ?
                                 ELSE last_event_id
                               END,
                               notifier_profile = COALESCE(NULLIF(notifier_profile,''), ?)
                         WHERE task_id = ? AND platform = ? AND chat_id = ?
                           AND ifnull(thread_id,'') = ''
                        """,
                        (
                            cursor,
                            int(last_ev["id"]) if last_ev else 0,
                            cursor,
                            notifier_profile,
                            t["id"],
                            platform,
                            str(home_chat),
                        ),
                    )
                repaired += 1
                existing.add(key)
                actions.append(
                    {
                        "type": "quiet_notify_repair_sub",
                        "board_db": str(db),
                        "task_id": t["id"],
                        "block_kind": t["block_kind"],
                        "title": t["title"],
                        "platform": platform,
                        "chat_id": str(home_chat),
                        "last_event_id": cursor,
                        "dry_run": dry,
                    }
                )

            if not dry and (dropped or repaired):
                con.commit()
            actions.append(
                {
                    "type": "quiet_notify_summary",
                    "board_db": str(db),
                    "kept": kept,
                    "dropped": dropped,
                    "repaired": repaired,
                    "dry_run": dry,
                }
            )
        finally:
            con.close()
    return actions


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--stuck-seconds", type=int, default=STUCK_SECONDS)
    ap.add_argument("--json-out", default=str(REPORT_PATH))
    ap.add_argument("--skip-plan-lint", action="store_true", help="skip nested kanban_plan_lint.py")
    ap.add_argument(
        "--plan-lint-no-blocks",
        action="store_true",
        help="plan-lint comments only (no auto-block of fat/protocol cards)",
    )
    args = ap.parse_args()
    dry = bool(args.dry_run)

    boards = discover_boards()
    actions: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    all_counts: Counter = Counter()
    board_counts: dict[str, dict[str, int]] = {}

    for board, db in boards:
        tasks = load_tasks(db)
        c = Counter(str(t.get("status")) for t in tasks)
        board_counts[board] = dict(c)
        all_counts.update(c)

        for t in tasks:
            tid = str(t.get("id") or "")
            title = str(t.get("title") or "")
            body = str(t.get("body") or "")
            status = str(t.get("status") or "")
            assignee_raw = t.get("assignee")
            assignee = str(assignee_raw).strip() if assignee_raw else None
            if assignee == "":
                assignee = None

            # assignee guard
            if (
                status in ("todo", "ready", "triage", "running", "review", "blocked")
                and assignee == "default"
                and is_implement_card(title, body)
            ):
                if status == "running":
                    rc, out = run_hermes(
                        board,
                        [
                            "reassign",
                            tid,
                            "worker",
                            "--reclaim",
                            "--reason",
                            "assignee-guard: implement card must not run on default/sol",
                        ],
                        dry,
                    )
                else:
                    rc, out = run_hermes(
                        board,
                        [
                            "reassign",
                            tid,
                            "worker",
                            "--reason",
                            "assignee-guard: implement card -> worker",
                        ],
                        dry,
                    )
                actions.append(
                    {
                        "type": "assignee_guard",
                        "board": board,
                        "task_id": tid,
                        "title": title[:120],
                        "from": "default",
                        "to": "worker",
                        "rc": rc,
                        "out": out[:500],
                    }
                )
                if not dry:
                    run_hermes(
                        board,
                        [
                            "comment",
                            tid,
                            "assignee-guard: reassigned default->worker (implement uses Grok worker)",
                        ],
                        dry,
                    )

            # stuck
            if status == "running":
                hb = parse_ts(t.get("last_heartbeat_at"))
                started = parse_ts(t.get("started_at"))
                created = parse_ts(t.get("created_at"))
                anchor = hb if hb is not None else (
                    started if started is not None else created
                )
                age: Optional[float] = (now_ts() - anchor) if anchor is not None else None
                wpid = t.get("worker_pid")
                dead = wpid not in (None, "", 0, "0") and (not pid_alive(wpid))
                stale = age is not None and age > float(args.stuck_seconds)
                if dead or stale:
                    parts: list[str] = []
                    if dead:
                        parts.append("dead worker_pid=%s" % wpid)
                    if stale and age is not None:
                        parts.append(
                            "stale heartbeat age=%ss > %ss"
                            % (int(age), args.stuck_seconds)
                        )
                    why = "stuck-guard: " + ", ".join(parts)
                    rc, out = run_hermes(
                        board,
                        ["block", tid, why, "--kind", "transient"],
                        dry,
                    )
                    actions.append(
                        {
                            "type": "stuck",
                            "board": board,
                            "task_id": tid,
                            "title": title[:120],
                            "assignee": assignee,
                            "worker_pid": wpid,
                            "age_s": int(age) if age is not None else None,
                            "rc": rc,
                            "out": out[:500],
                            "reason": why,
                        }
                    )

            if status == "blocked":
                # use completed_at? no - created/started; comments last?
                updated = parse_ts(t.get("last_heartbeat_at")) or parse_ts(
                    t.get("started_at")
                ) or parse_ts(t.get("created_at"))
                if updated is not None and now_ts() - updated > 24 * 3600:
                    warnings.append(
                        {
                            "type": "blocked_stale_24h",
                            "board": board,
                            "task_id": tid,
                            "title": title[:120],
                            "assignee": assignee,
                        }
                    )
            if status in ("todo", "ready") and not assignee:
                warnings.append(
                    {
                        "type": "unassigned_ready_or_todo",
                        "board": board,
                        "task_id": tid,
                        "title": title[:120],
                        "status": status,
                    }
                )
            if status == "running" and assignee == "default":
                warnings.append(
                    {
                        "type": "running_on_default",
                        "board": board,
                        "task_id": tid,
                        "title": title[:120],
                    }
                )

        # auto-archive stale blocked (needs_input 7d, others 30d) + manifest
        actions.extend(stale_archive_sweep(board, db, tasks, dry))
        # anti-stall: clamp max_retries above cap on active cards
        actions.extend(clamp_max_retries(board, db, tasks, dry))

    # Quiet Telegram notify: drop intermediate / non-needs_input subs
    quiet_actions = quiet_notify_cleanup(dry)
    actions.extend(quiet_actions)
    for a in quiet_actions:
        if a.get("type") == "quiet_notify_summary":
            db_name = str(a.get("board_db", "")).rstrip("/").split("/")[-1]
            all_counts[f"quiet_notify_kept:{db_name}"] = a.get("kept", 0)
            all_counts[f"quiet_notify_dropped:{db_name}"] = a.get("dropped", 0)

    # Nested reviewer timeout/resume anti-loop guard (PARTIAL checkpoint inject)
    reviewer_guard_summary: dict[str, Any] | None = None
    guard_py = Path(__file__).resolve().parent / "reviewer_timeout_guard.py"
    if guard_py.exists() and not dry:
        try:
            gcmd = [sys.executable, str(guard_py)]
            g = subprocess.run(gcmd, capture_output=True, text=True, timeout=120)
            last_path = RUNTIME_ROOT / "reviewer_timeout_guard_last.json"
            if last_path.exists():
                try:
                    reviewer_guard_summary = json.loads(last_path.read_text())
                except Exception:
                    reviewer_guard_summary = {"raw": last_path.read_text()[:500]}
            actions.append(
                {
                    "type": "reviewer_timeout_guard",
                    "rc": g.returncode,
                    "stdout_tail": (g.stdout or "")[-500:],
                    "stderr_tail": (g.stderr or "")[-300:],
                    "summary": reviewer_guard_summary,
                }
            )
            if g.returncode != 0:
                warnings.append(
                    {
                        "type": "reviewer_timeout_guard_failed",
                        "rc": g.returncode,
                        "stderr": (g.stderr or "")[:300],
                    }
                )
        except Exception as e:
            warnings.append({"type": "reviewer_timeout_guard_error", "error": str(e)[:300]})

    # Nested salvage guard: preserve logs/sessions/artifacts across timeout retries
    salvage_guard_summary: dict[str, Any] | None = None
    salvage_py = Path(__file__).resolve().parent / "task_salvage_guard.py"
    if salvage_py.exists() and not dry:
        try:
            scmd = [sys.executable, str(salvage_py)]
            s = subprocess.run(scmd, capture_output=True, text=True, timeout=300)
            slast = RUNTIME_ROOT / "task_salvage_guard_last.json"
            if slast.exists():
                try:
                    salvage_guard_summary = json.loads(slast.read_text())
                except Exception:
                    salvage_guard_summary = {"raw": slast.read_text()[:500]}
            actions.append(
                {
                    "type": "task_salvage_guard",
                    "rc": s.returncode,
                    "stdout_tail": (s.stdout or "")[-800:],
                    "stderr_tail": (s.stderr or "")[-300:],
                    "summary": salvage_guard_summary,
                }
            )
            if s.returncode != 0:
                warnings.append(
                    {
                        "type": "task_salvage_guard_failed",
                        "rc": s.returncode,
                        "stderr": (s.stderr or "")[:300],
                    }
                )
        except Exception as e:
            warnings.append({"type": "task_salvage_guard_error", "error": str(e)[:300]})

    # Nested process logger: durable snapshots of live workers / running tasks / anomalies
    process_log_summary: dict[str, Any] | None = None
    process_log_py = Path(__file__).resolve().parent / "kanban_process_logger.py"
    if process_log_py.exists() and not dry:
        try:
            plcmd = [sys.executable, str(process_log_py), "--quiet"]
            pl = subprocess.run(plcmd, capture_output=True, text=True, timeout=90)
            pl_latest = RUNTIME_ROOT / "kanban_process_log/latest.json"
            if pl_latest.exists():
                try:
                    process_log_summary = json.loads(pl_latest.read_text()).get("counts")
                except Exception:
                    process_log_summary = {"raw": pl_latest.read_text()[:300]}
            actions.append(
                {
                    "type": "kanban_process_logger",
                    "rc": pl.returncode,
                    "stdout_tail": (pl.stdout or "")[-500:],
                    "stderr_tail": (pl.stderr or "")[-200:],
                    "counts": process_log_summary,
                }
            )
            if pl.returncode != 0:
                warnings.append(
                    {
                        "type": "kanban_process_logger_failed",
                        "rc": pl.returncode,
                        "stderr": (pl.stderr or "")[:300],
                    }
                )
            # Surface critical process anomalies into hygiene warnings (no kills).
            # Ignore session-dirt kinds — those are logged in process log only.
            _surface = {
                "worker_long_running",
                "running_pid_dead",
                "stale_heartbeat",
                "over_max_runtime",
                "running_task_long",
                "running_without_process",
                "orphan_worker_process",
                "board_read_error",
            }
            try:
                latest_full = json.loads(pl_latest.read_text()) if pl_latest.exists() else {}
                for a in (latest_full.get("anomalies") or [])[:40]:
                    if a.get("level") != "critical":
                        continue
                    if a.get("kind") not in _surface:
                        continue
                    warnings.append(
                        {
                            "type": "process_" + str(a.get("kind") or "anomaly"),
                            "task_id": a.get("task_id"),
                            "board": a.get("board"),
                            "title": str(a)[:120],
                            **{
                                k: a.get(k)
                                for k in ("pid", "age_s", "heartbeat_ago_s", "worker_pid")
                                if a.get(k) is not None
                            },
                        }
                    )
            except Exception:
                pass
        except Exception as e:
            warnings.append({"type": "kanban_process_logger_error", "error": str(e)[:300]})


    # Nested session_id collision cleanup: origin chat session_id is shared
    # across a whole wave; multiple open cards on the same origin id confuse
    # ops/process_logger and can trigger false SALVAGE_RESUME. NULL those on
    # open collision groups every tick so they never accumulate.
    session_clear_summary: dict[str, Any] | None = None
    session_clear_py = Path(__file__).resolve().parent / "kanban_clear_shared_session_ids.py"
    if session_clear_py.exists() and not dry:
        try:
            scmd = [sys.executable, str(session_clear_py), "--write"]
            sc = subprocess.run(scmd, capture_output=True, text=True, timeout=60)
            try:
                session_clear_summary = json.loads(sc.stdout.strip().splitlines()[-1])
            except Exception:
                session_clear_summary = {"raw": (sc.stdout or "")[:300]}
            actions.append(
                {
                    "type": "session_id_collision_clear",
                    "rc": sc.returncode,
                    "stdout_tail": (sc.stdout or "")[-300:],
                    "stderr_tail": (sc.stderr or "")[-200:],
                    "summary": session_clear_summary,
                }
            )
            if sc.returncode != 0:
                warnings.append(
                    {
                        "type": "session_id_collision_clear_failed",
                        "rc": sc.returncode,
                        "stderr": (sc.stderr or "")[:300],
                    }
                )
        except Exception as e:
            warnings.append({"type": "session_id_collision_clear_error", "error": str(e)[:300]})

    # Nested retry enforcer: protocol/timeout ×2 + 0 delta → block needs_replan
    retry_enforcer_summary: dict[str, Any] | None = None
    enforcer_py = Path(__file__).resolve().parent / "kanban_retry_enforcer.py"
    if enforcer_py.exists() and not dry:
        try:
            ecmd = [sys.executable, str(enforcer_py)]
            er = subprocess.run(ecmd, capture_output=True, text=True, timeout=120)
            elast = RUNTIME_ROOT / "kanban_retry_enforcer_last.json"
            if elast.exists():
                try:
                    retry_enforcer_summary = json.loads(elast.read_text())
                except Exception:
                    retry_enforcer_summary = {"raw": elast.read_text()[:400]}
            actions.append(
                {
                    "type": "kanban_retry_enforcer",
                    "rc": er.returncode,
                    "stdout_tail": (er.stdout or "")[-600:],
                    "stderr_tail": (er.stderr or "")[-200:],
                    "total_actions": (retry_enforcer_summary or {}).get("total_actions"),
                }
            )
            if er.returncode != 0:
                warnings.append(
                    {
                        "type": "kanban_retry_enforcer_failed",
                        "rc": er.returncode,
                        "stderr": (er.stderr or "")[:300],
                    }
                )
        except Exception as e:
            warnings.append({"type": "kanban_retry_enforcer_error", "error": str(e)[:300]})

    hygiene = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stuck_seconds": args.stuck_seconds,
        "boards_scanned": [b for b, _ in boards],
        "counts": dict(all_counts),
        "boards": board_counts,
        "actions": actions,
        "warnings": warnings,
        "action_count": len(actions),
        "warning_count": len(warnings),
        "dry_run": dry,
        "reviewer_timeout_guard": reviewer_guard_summary,
        "task_salvage_guard": salvage_guard_summary,
        "process_log": process_log_summary,
        "retry_enforcer": retry_enforcer_summary,
        "session_id_collision_clear": session_clear_summary,
    }
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    Path(args.json_out).write_text(json.dumps(hygiene, ensure_ascii=False, indent=2) + "\n")

    print(
        "kanban_hygiene actions=%s warnings=%s counts=%s dry=%s boards=%s"
        % (len(actions), len(warnings), dict(all_counts), dry, [b for b, _ in boards])
    )
    for a in actions:
        print(
            "  ACTION %s %s/%s %s"
            % (a["type"], a.get("board"), a.get("task_id"), str(a.get("title", ""))[:80])
        )
    for w in warnings[:25]:
        print(
            "  WARN %s %s/%s %s"
            % (w["type"], w.get("board"), w.get("task_id"), str(w.get("title", ""))[:80])
        )

    # Nested plan-lint / protocol-streak / fan-out enforcement
    plan_lint_rc = 0
    plan_lint_summary = None
    if not args.skip_plan_lint:
        lint_py = Path(__file__).resolve().parent / "kanban_plan_lint.py"
        if lint_py.exists():
            cmd = [sys.executable, str(lint_py)]
            if dry:
                cmd.append("--dry-run")
            if args.plan_lint_no_blocks:
                cmd.append("--no-blocks")
            try:
                r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                plan_lint_rc = r.returncode
                out = ((r.stdout or "") + (r.stderr or "")).strip()
                print("--- plan_lint ---")
                print(out[-2500:] if len(out) > 2500 else out)
                report_path = RUNTIME_ROOT / "kanban_plan_lint_last.json"
                if report_path.exists():
                    try:
                        plan_lint_summary = json.loads(report_path.read_text())
                    except Exception:
                        plan_lint_summary = {"raw_tail": out[-500:]}
            except Exception as e:
                print("plan_lint failed:", e)
                plan_lint_rc = 1
        else:
            print("plan_lint skipped: missing", lint_py)

    hygiene["plan_lint_rc"] = plan_lint_rc
    if plan_lint_summary:
        hygiene["plan_lint"] = {
            "finding_count": plan_lint_summary.get("finding_count"),
            "critical": plan_lint_summary.get("critical"),
            "error": plan_lint_summary.get("error"),
            "warning": plan_lint_summary.get("warning"),
        }
    Path(args.json_out).write_text(json.dumps(hygiene, ensure_ascii=False, indent=2) + "\n")
    return 0 if plan_lint_rc in (0, 2) else plan_lint_rc


if __name__ == "__main__":
    raise SystemExit(main())
