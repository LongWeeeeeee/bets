#!/usr/bin/env python3
"""Kanban / Hermes process logger — durable snapshots for long-run forensics.

Every tick writes:
  - JSONL append: runtime/kanban_process_log/events.jsonl
  - latest snapshot: runtime/kanban_process_log/latest.json
  - optional anomaly-only: runtime/kanban_process_log/anomalies.jsonl

Captures:
  * live `hermes … chat -q work kanban task …` workers (pid, etime, cpu, mem, profile, task, salvage)
  * gateway PIDs
  * board `running` rows vs live PID / heartbeat lag / max_runtime remaining
  * OPEN sessions with no matching live worker (zombie session signal)

Read-only w.r.t. kanban DB. Safe under cron every 5m (nested from hygiene).
"""
from __future__ import annotations
# --- bootstrap раскладки: соседние эксперименты живут в runtime/experiments/<тема>/
import sys as _sys, pathlib as _pathlib
_repo_root = next((p for p in _pathlib.Path(__file__).resolve().parents if (p / '.git').exists()), None)
if _repo_root is not None:
    for _exp_dir in sorted((_repo_root / 'runtime' / 'experiments').glob('*')):
        if _exp_dir.is_dir() and str(_exp_dir) not in _sys.path:
            _sys.path.insert(0, str(_exp_dir))

import json
import os
import re
import sqlite3
import subprocess
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

from kanban_lane_paths import discover_boards as discover_lane_boards
from kanban_lane_paths import lane_home, lane_runtime_root, selected_lane

LANE = selected_lane()
HERMES_HOME = lane_home(LANE)
ROOT = lane_runtime_root(LANE) / "kanban_process_log"
EVENTS = ROOT / "events.jsonl"
ANOMALIES = ROOT / "anomalies.jsonl"
LATEST = ROOT / "latest.json"
STATE = ROOT / "state.json"

KANBAN_DB = HERMES_HOME / "kanban.db"
BOARDS_ROOT = HERMES_HOME / "kanban/boards"

# Thresholds (seconds) — logging only, does not kill
WARN_AGE_S = int(os.environ.get("KANBAN_PROCLOG_WARN_AGE_S", str(30 * 60)))
CRIT_AGE_S = int(os.environ.get("KANBAN_PROCLOG_CRIT_AGE_S", str(2 * 3600)))
STALE_HB_S = int(os.environ.get("KANBAN_PROCLOG_STALE_HB_S", str(10 * 60)))
ZOMBIE_SESSION_S = int(os.environ.get("KANBAN_PROCLOG_ZOMBIE_SESSION_S", str(45 * 60)))

TASK_RE = re.compile(r"work kanban task (t_[a-f0-9]+)")
PROFILE_RE = re.compile(r"(?:--profile|-p)\s+(\S+)")


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"{ts} {msg}", flush=True)


def boards() -> list[tuple[str, Path]]:
    return discover_lane_boards(HERMES_HOME)


def _etime_to_seconds(etime: str) -> int:
    """ps etime → seconds. Formats: mm:ss | hh:mm:ss | d-hh:mm:ss"""
    etime = (etime or "").strip()
    days = 0
    if "-" in etime:
        d, etime = etime.split("-", 1)
        days = int(d)
    parts = [int(x) for x in etime.split(":")]
    if len(parts) == 3:
        h, m, s = parts
    elif len(parts) == 2:
        h, m, s = 0, parts[0], parts[1]
    else:
        h, m, s = 0, 0, parts[0] if parts else 0
    return days * 86400 + h * 3600 + m * 60 + s


def snapshot_live_workers() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    try:
        raw = subprocess.check_output(
            ["bash", "-lc", "ps -eo pid,etime,pcpu,pmem,args --no-headers"],
            text=True,
            timeout=30,
        )
    except Exception as e:
        return [{"error": f"ps_failed:{e}"}], []

    workers: list[dict[str, Any]] = []
    gateways: list[dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 4)
        if len(parts) < 5:
            continue
        pid_s, etime, pcpu, pmem, args = parts
        if "hermes_cli.main" in args and "gateway run" in args:
            m = PROFILE_RE.search(args)
            gateways.append(
                {
                    "pid": int(pid_s),
                    "etime": etime,
                    "age_s": _etime_to_seconds(etime),
                    "pcpu": float(pcpu),
                    "pmem": float(pmem),
                    "profile": (m.group(1) if m else "default"),
                    "kind": "gateway",
                }
            )
            continue
        if "chat -q" not in args or "work kanban task" not in args:
            # also match without exact spacing
            if not ("chat" in args and "kanban task" in args and "hermes" in args):
                continue
        m_t = TASK_RE.search(args)
        if not m_t:
            m_t = re.search(r"kanban task (t_[a-f0-9]+)", args)
        m_p = PROFILE_RE.search(args)
        workers.append(
            {
                "pid": int(pid_s),
                "etime": etime,
                "age_s": _etime_to_seconds(etime),
                "pcpu": float(pcpu),
                "pmem": float(pmem),
                "profile": (m_p.group(1) if m_p else "?"),
                "task_id": (m_t.group(1) if m_t else None),
                "salvage_inject": "[SALVAGE_RESUME]" in args or "SALVAGE pack" in args,
                "cmdline_tail": args[-220:],
                "kind": "kanban_worker",
            }
        )
    return workers, gateways


def _pid_alive(pid: Optional[int]) -> bool:
    if not pid:
        return False
    try:
        return Path(f"/proc/{int(pid)}").exists()
    except Exception:
        return False


def snapshot_running_tasks() -> list[dict[str, Any]]:
    now = int(time.time())
    rows: list[dict[str, Any]] = []
    for board, db in boards():
        if not db.exists():
            continue
        try:
            conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=30)
            conn.row_factory = sqlite3.Row
            q = """
            SELECT t.id, t.title, t.assignee, t.status, t.worker_pid,
                   t.max_runtime_seconds, t.consecutive_failures,
                   t.started_at, t.last_heartbeat_at, t.claim_lock,
                   t.session_id,
                   r.id AS run_id, r.started_at AS run_started,
                   r.last_heartbeat_at AS run_hb, r.outcome AS run_outcome
            FROM tasks t
            LEFT JOIN task_runs r ON r.id = t.current_run_id
            WHERE t.status = 'running'
            """
            for r in conn.execute(q):
                rs = r["run_started"] or r["started_at"]
                hb = r["run_hb"] or r["last_heartbeat_at"]
                age_s = (now - int(rs)) if rs else None
                hb_ago = (now - int(hb)) if hb else None
                maxrt = r["max_runtime_seconds"]
                remain = None
                if maxrt is not None and age_s is not None:
                    remain = int(maxrt) - int(age_s)
                pid = r["worker_pid"]
                rows.append(
                    {
                        "board": board,
                        "task_id": r["id"],
                        "title": (r["title"] or "")[:80],
                        "assignee": r["assignee"],
                        "worker_pid": pid,
                        "pid_alive": _pid_alive(pid),
                        "max_runtime_seconds": maxrt,
                        "age_s": age_s,
                        "heartbeat_ago_s": hb_ago,
                        "runtime_remaining_s": remain,
                        "consecutive_failures": r["consecutive_failures"],
                        "run_id": r["run_id"],
                        "session_id": r["session_id"],
                        "claim_lock": r["claim_lock"],
                    }
                )
            conn.close()
        except Exception as e:
            rows.append({"board": board, "error": str(e)})
    return rows


def snapshot_open_sessions() -> list[dict[str, Any]]:
    """OPEN sessions from profile state DBs (zombie detector input)."""
    now = time.time()
    paths = [
        ("default", Path("/root/.hermes/state.db")),
        ("worker", Path("/root/.hermes/profiles/worker/state.db")),
        ("orchestration1", Path("/root/.hermes/profiles/orchestration1/state.db")),
        ("orchestration2", Path("/root/.hermes/profiles/orchestration2/state.db")),
    ]
    out: list[dict[str, Any]] = []
    for prof, db in paths:
        if not db.exists():
            continue
        try:
            conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=30)
            conn.row_factory = sqlite3.Row
            # started_at may be float unix or string — handle both
            rows = conn.execute(
                """
                SELECT id, title, source, message_count, tool_call_count,
                       started_at, ended_at, end_reason
                FROM sessions
                WHERE ended_at IS NULL
                ORDER BY started_at DESC
                LIMIT 40
                """
            ).fetchall()
            for r in rows:
                sa = r["started_at"]
                try:
                    sa_f = float(sa)
                except Exception:
                    sa_f = None
                age_s = (now - sa_f) if sa_f else None
                out.append(
                    {
                        "profile": prof,
                        "session_id": r["id"],
                        "title": (r["title"] or "")[:80],
                        "source": r["source"],
                        "messages": r["message_count"],
                        "tools": r["tool_call_count"],
                        "age_s": int(age_s) if age_s is not None else None,
                    }
                )
            conn.close()
        except Exception as e:
            out.append({"profile": prof, "error": str(e)})
    return out


def classify_anomalies(
    workers: list[dict[str, Any]],
    running: list[dict[str, Any]],
    sessions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    anoms: list[dict[str, Any]] = []
    live_pids = {w["pid"] for w in workers if "pid" in w}
    live_tasks = {w.get("task_id") for w in workers if w.get("task_id")}

    for w in workers:
        if "error" in w:
            continue
        age = int(w.get("age_s") or 0)
        level = None
        if age >= CRIT_AGE_S:
            level = "critical"
        elif age >= WARN_AGE_S:
            level = "warn"
        if level:
            anoms.append(
                {
                    "kind": "worker_long_running",
                    "level": level,
                    "task_id": w.get("task_id"),
                    "pid": w.get("pid"),
                    "age_s": age,
                    "profile": w.get("profile"),
                    "salvage_inject": w.get("salvage_inject"),
                    "pcpu": w.get("pcpu"),
                    "pmem": w.get("pmem"),
                }
            )

    for r in running:
        if r.get("error"):
            anoms.append(
                {
                    "kind": "board_read_error",
                    "level": "warn",
                    "board": r.get("board"),
                    "error": r.get("error"),
                }
            )
            continue
        tid = r.get("task_id")
        age = r.get("age_s")
        hb = r.get("heartbeat_ago_s")
        remain = r.get("runtime_remaining_s")
        pid = r.get("worker_pid")
        alive = r.get("pid_alive")

        if pid and not alive:
            anoms.append(
                {
                    "kind": "running_pid_dead",
                    "level": "critical",
                    "board": r.get("board"),
                    "task_id": tid,
                    "worker_pid": pid,
                    "age_s": age,
                    "run_id": r.get("run_id"),
                }
            )
        if hb is not None and hb >= STALE_HB_S and alive:
            anoms.append(
                {
                    "kind": "stale_heartbeat",
                    "level": "critical" if hb >= STALE_HB_S * 3 else "warn",
                    "board": r.get("board"),
                    "task_id": tid,
                    "heartbeat_ago_s": hb,
                    "worker_pid": pid,
                    "age_s": age,
                }
            )
        if remain is not None and remain <= 0 and alive:
            anoms.append(
                {
                    "kind": "over_max_runtime",
                    "level": "critical",
                    "board": r.get("board"),
                    "task_id": tid,
                    "age_s": age,
                    "max_runtime_seconds": r.get("max_runtime_seconds"),
                    "runtime_remaining_s": remain,
                    "worker_pid": pid,
                }
            )
        if age is not None and age >= WARN_AGE_S:
            anoms.append(
                {
                    "kind": "running_task_long",
                    "level": "critical" if age >= CRIT_AGE_S else "warn",
                    "board": r.get("board"),
                    "task_id": tid,
                    "age_s": age,
                    "max_runtime_seconds": r.get("max_runtime_seconds"),
                    "assignee": r.get("assignee"),
                    "pid_alive": alive,
                }
            )
        # DB running but no live process with that task id
        if tid and tid not in live_tasks and not alive:
            anoms.append(
                {
                    "kind": "running_without_process",
                    "level": "critical",
                    "board": r.get("board"),
                    "task_id": tid,
                    "worker_pid": pid,
                    "age_s": age,
                }
            )

    # orphan live worker: process exists but task not running in any board
    running_ids = {r.get("task_id") for r in running if r.get("task_id")}
    for w in workers:
        tid = w.get("task_id")
        if tid and tid not in running_ids:
            anoms.append(
                {
                    "kind": "orphan_worker_process",
                    "level": "warn",
                    "task_id": tid,
                    "pid": w.get("pid"),
                    "age_s": w.get("age_s"),
                    "profile": w.get("profile"),
                }
            )

    # zombie OPEN sessions — only recent windows matter for ops.
    # Ancient OPEN rows are DB dirt → single aggregate. Cap per-item noise.
    old_open = 0
    recent_open_items: list[dict[str, Any]] = []
    for s in sessions:
        if s.get("error"):
            continue
        age = int(s.get("age_s") or 0)
        if age < ZOMBIE_SESSION_S:
            continue
        if age >= 12 * 3600:
            old_open += 1
            continue
        recent_open_items.append(s)

    # Prefer sessions that look abandoned: 0 tools for a long time, or age>3h.
    def _sess_score(s: dict[str, Any]) -> tuple:
        age = int(s.get("age_s") or 0)
        tools = int(s.get("tools") or 0)
        return (-age, tools)

    recent_open_items.sort(key=_sess_score)
    for s in recent_open_items[:8]:
        age = int(s.get("age_s") or 0)
        anoms.append(
            {
                "kind": "open_session_old",
                "level": "warn" if age < 3 * 3600 else "critical",
                "profile": s.get("profile"),
                "session_id": s.get("session_id"),
                "age_s": age,
                "source": s.get("source"),
                "messages": s.get("messages"),
                "tools": s.get("tools"),
                "title": s.get("title"),
            }
        )
    if len(recent_open_items) > 8:
        anoms.append(
            {
                "kind": "open_session_old_aggregate",
                "level": "warn",
                "count": len(recent_open_items) - 8,
                "note": "additional OPEN sessions 45m–12h not listed individually",
            }
        )
    if old_open:
        anoms.append(
            {
                "kind": "open_session_dirt_aggregate",
                "level": "warn",
                "count": old_open,
                "note": "OPEN sessions older than 12h still marked open in state.db",
            }
        )

    return anoms


def board_status_counts() -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for board, db in boards():
        if not db.exists():
            continue
        try:
            conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=30)
            rows = conn.execute(
                "SELECT status, COUNT(*) FROM tasks GROUP BY status"
            ).fetchall()
            out[board] = {str(s): int(n) for s, n in rows}
            conn.close()
        except Exception as e:
            out[board] = {"_error": 1, "_msg": str(e)}  # type: ignore[dict-item]
    return out


def append_jsonl(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, default=str) + "\n")


def load_state() -> dict[str, Any]:
    if STATE.exists():
        try:
            return json.loads(STATE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_state(st: dict[str, Any]) -> None:
    STATE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(st, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(STATE)


def rotate_jsonl_if_huge(path: Path, max_bytes: int = 40_000_000) -> None:
    """Simple rotate: rename to .1 if oversized (keep one gen)."""
    try:
        if path.exists() and path.stat().st_size >= max_bytes:
            bak = path.with_suffix(path.suffix + ".1")
            if bak.exists():
                bak.unlink()
            path.rename(bak)
    except Exception:
        pass


def run(*, quiet: bool = False) -> dict[str, Any]:
    ROOT.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    workers, gateways = snapshot_live_workers()

    running = snapshot_running_tasks()
    sessions = snapshot_open_sessions()
    anoms = classify_anomalies(workers, running, sessions)
    statuses = board_status_counts()

    snap: dict[str, Any] = {
        "ts": ts,
        "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts)),
        "gateways": gateways,
        "workers": workers,
        "running_tasks": running,
        "open_sessions": sessions,
        "anomalies": anoms,
        "board_statuses": statuses,
        "counts": {
            "gateways": len(gateways),
            "workers": len(workers),
            "running_tasks": len([r for r in running if not r.get("error")]),
            "open_sessions": len([s for s in sessions if not s.get("error")]),
            "anomalies": len(anoms),
            "anomalies_critical": sum(1 for a in anoms if a.get("level") == "critical"),
            "anomalies_warn": sum(1 for a in anoms if a.get("level") == "warn"),
        },
        "thresholds": {
            "warn_age_s": WARN_AGE_S,
            "crit_age_s": CRIT_AGE_S,
            "stale_hb_s": STALE_HB_S,
            "zombie_session_s": ZOMBIE_SESSION_S,
        },
    }

    # compact event line (not full session dump every time if huge)
    event = {
        "ts": ts,
        "ts_iso": snap["ts_iso"],
        "counts": snap["counts"],
        "workers": [
            {
                "pid": w.get("pid"),
                "task_id": w.get("task_id"),
                "profile": w.get("profile"),
                "age_s": w.get("age_s"),
                "pcpu": w.get("pcpu"),
                "pmem": w.get("pmem"),
                "salvage": w.get("salvage_inject"),
            }
            for w in workers
            if "pid" in w
        ],
        "running": [
            {
                "board": r.get("board"),
                "task_id": r.get("task_id"),
                "age_s": r.get("age_s"),
                "hb_ago": r.get("heartbeat_ago_s"),
                "remain": r.get("runtime_remaining_s"),
                "pid": r.get("worker_pid"),
                "alive": r.get("pid_alive"),
                "maxrt": r.get("max_runtime_seconds"),
                "fails": r.get("consecutive_failures"),
            }
            for r in running
            if not r.get("error")
        ],
        "anomalies": anoms,
        "board_statuses": statuses,
        "gateways": [
            {"pid": g.get("pid"), "profile": g.get("profile"), "age_s": g.get("age_s")}
            for g in gateways
        ],
    }

    rotate_jsonl_if_huge(EVENTS)
    rotate_jsonl_if_huge(ANOMALIES)
    append_jsonl(EVENTS, event)
    if anoms:
        append_jsonl(
            ANOMALIES,
            {"ts": ts, "ts_iso": snap["ts_iso"], "anomalies": anoms, "counts": snap["counts"]},
        )

    tmp = LATEST.with_suffix(".tmp")
    tmp.write_text(json.dumps(snap, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    tmp.replace(LATEST)

    st = load_state()
    st["last_ts"] = ts
    st["last_counts"] = snap["counts"]
    st["ticks"] = int(st.get("ticks") or 0) + 1
    # track first-seen for long tasks
    seen = st.setdefault("task_first_seen", {})
    for w in workers:
        tid = w.get("task_id")
        if tid and tid not in seen:
            seen[tid] = {"ts": ts, "pid": w.get("pid")}
    # prune first_seen for tasks not live and older than 2d
    live = {w.get("task_id") for w in workers}
    for tid in list(seen.keys()):
        if tid in live:
            continue
        if ts - int(seen[tid].get("ts") or ts) > 2 * 86400:
            del seen[tid]
    save_state(st)

    crit = snap["counts"]["anomalies_critical"]
    warn = snap["counts"]["anomalies_warn"]
    if not quiet:
        log(
            f"workers={snap['counts']['workers']} running={snap['counts']['running_tasks']} "
            f"sessions_open={snap['counts']['open_sessions']} "
            f"anomalies={snap['counts']['anomalies']} (crit={crit} warn={warn})"
        )
        for a in anoms[:20]:
            detail = {k: a.get(k) for k in a if k not in ("kind", "level")}
            log(
                f"  ANOM {a.get('level')} {a.get('kind')} "
                f"{a.get('task_id') or a.get('session_id') or ''} {detail}"
            )
    return snap


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Kanban process logger")
    ap.add_argument("--print-latest", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)
    snap = run(quiet=args.quiet and not args.print_latest)
    if args.print_latest:
        print(json.dumps(snap["counts"], indent=2))
        if snap["anomalies"]:
            print(json.dumps(snap["anomalies"][:15], indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
