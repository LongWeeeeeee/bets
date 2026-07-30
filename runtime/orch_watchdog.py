#!/usr/bin/env python3
"""Orchestration stall watchdog — every ~20m.

Collects:
  - gateway / worker process health
  - running/ready/todo/blocked counts per board
  - why todo is not ready (parent blocked/todo/missing)
  - top parent blockers (fan-in)
  - recent worker log tails for running tasks
  - hygiene/process_log anomalies
  - stall signals (0 running + spawnable-looking work, sticky blocked roots)

Writes lane-local ``orch_watch/{latest.json,latest.md,history/*}``.

Does NOT mutate kanban. Safe under cron.
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from kanban_lane_paths import discover_boards as discover_lane_boards
from kanban_lane_paths import lane_home, lane_runtime_root, selected_lane

LANE = selected_lane()
HERMES_HOME = lane_home(LANE)
RUNTIME_ROOT = lane_runtime_root(LANE)
ROOT = RUNTIME_ROOT / "orch_watch"
LATEST_JSON = ROOT / "latest.json"
LATEST_MD = ROOT / "latest.md"
HIST = ROOT / "history"
KANBAN_DB = HERMES_HOME / "kanban.db"
BOARDS = HERMES_HOME / "kanban/boards"
PROCESS_LOG = RUNTIME_ROOT / "kanban_process_log/latest.json"
HYGIENE = RUNTIME_ROOT / "kanban_hygiene_last.json"


def boards() -> list[tuple[str, Path]]:
    """Boards to monitor.

    Default: **default only** (the 3-bot dispatcher board). Stale epic boards
    like telemt-proxy create false fan-in "stalls" after product work is done.

    Override with env ``ORCH_WATCH_BOARDS``:
      - ``default`` (same as unset)
      - ``all``
      - comma list: ``default,speech-awareness``
    """
    import os
    sel = (os.environ.get("ORCH_WATCH_BOARDS") or "default").strip().lower()
    all_boards = discover_lane_boards(HERMES_HOME)
    if sel in {"all", "*"}:
        return all_boards
    want = {x.strip() for x in sel.split(",") if x.strip()}
    out = [(n, path) for n, path in all_boards if n in want]
    return out or [("default", KANBAN_DB)]


def sh(cmd: str) -> str:
    try:
        return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT, timeout=30)
    except Exception as e:
        return f"ERR:{e}"


def pid_alive(pid) -> bool:
    try:
        return bool(pid) and Path(f"/proc/{int(pid)}").exists()
    except Exception:
        return False


def analyze_board(name: str, db: Path) -> dict[str, Any]:
    if not db.exists():
        return {"board": name, "error": "missing"}
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    parents: dict[str, list[str]] = defaultdict(list)
    try:
        for r in conn.execute("SELECT parent_id, child_id FROM task_links"):
            parents[r["child_id"]].append(r["parent_id"])
    except Exception:
        pass

    tasks = {
        r["id"]: r
        for r in conn.execute(
            """
            SELECT id, title, status, assignee, consecutive_failures, max_retries,
                   worker_pid, last_heartbeat_at, started_at, max_runtime_seconds,
                   block_kind, substr(COALESCE(last_failure_error,''),1,160) err,
                   substr(title,1,90) tshort
            FROM tasks
            """
        )
    }
    counts = Counter(t["status"] for t in tasks.values())
    now = int(time.time())

    running = []
    for t in tasks.values():
        if t["status"] != "running":
            continue
        hb = t["last_heartbeat_at"]
        st = t["started_at"]
        running.append(
            {
                "id": t["id"],
                "title": t["tshort"],
                "assignee": t["assignee"],
                "pid": t["worker_pid"],
                "pid_alive": pid_alive(t["worker_pid"]),
                "age_s": (now - int(st)) if st else None,
                "hb_age_s": (now - int(hb)) if hb else None,
                "maxrt": t["max_runtime_seconds"],
                "fails": t["consecutive_failures"],
            }
        )

    why = Counter()
    samples = []
    child_wait: Counter = Counter()
    for t in tasks.values():
        if t["status"] not in ("todo", "ready"):
            continue
        pars = parents.get(t["id"], [])
        if not pars:
            key = "ready_no_parents" if t["status"] == "ready" else "todo_no_parents"
            why[key] += 1
            samples.append(
                {
                    "id": t["id"],
                    "status": t["status"],
                    "assignee": t["assignee"],
                    "why": key,
                    "title": t["tshort"],
                }
            )
            continue
        open_p = []
        for p in pars:
            st = tasks.get(p)
            ps = st["status"] if st else "MISSING"
            if ps not in ("done", "archived"):
                open_p.append(
                    {
                        "parent": p,
                        "status": ps,
                        "assignee": st["assignee"] if st else None,
                        "title": st["tshort"] if st else "?",
                        "fails": st["consecutive_failures"] if st else None,
                        "block_kind": st["block_kind"] if st else None,
                        "err": (st["err"] if st else "")[:100],
                    }
                )
        if open_p:
            why[f"wait_parent_{open_p[0]['status']}"] += 1
            for op in open_p:
                child_wait[op["parent"]] += 1
            samples.append(
                {
                    "id": t["id"],
                    "status": t["status"],
                    "assignee": t["assignee"],
                    "why": "parents_open",
                    "title": t["tshort"],
                    "parents": open_p[:6],
                }
            )
        else:
            why["parents_done_not_promoted"] += 1
            samples.append(
                {
                    "id": t["id"],
                    "status": t["status"],
                    "assignee": t["assignee"],
                    "why": "parents_done_not_promoted",
                    "title": t["tshort"],
                    "parents": pars,
                }
            )

    top_blockers = []
    for pid, n in child_wait.most_common(15):
        st = tasks.get(pid)
        top_blockers.append(
            {
                "id": pid,
                "children_waiting": n,
                "status": st["status"] if st else None,
                "assignee": st["assignee"] if st else None,
                "title": st["tshort"] if st else None,
                "fails": st["consecutive_failures"] if st else None,
                "block_kind": st["block_kind"] if st else None,
                "err": (st["err"] if st else "")[:120],
            }
        )

    # sticky blocked roots with protocol/retry markers
    blocked_hot = []
    for t in tasks.values():
        if t["status"] != "blocked":
            continue
        err = (t["err"] or "").lower()
        if any(
            k in err
            for k in (
                "protocol",
                "retry_enforcer",
                "needs_replan",
                "timed_out",
                "iteration budget",
                "without calling kanban",
            )
        ) or int(t["consecutive_failures"] or 0) >= 2:
            blocked_hot.append(
                {
                    "id": t["id"],
                    "title": t["tshort"],
                    "assignee": t["assignee"],
                    "fails": t["consecutive_failures"],
                    "block_kind": t["block_kind"],
                    "err": t["err"],
                    "children_waiting": child_wait.get(t["id"], 0),
                }
            )
    blocked_hot.sort(key=lambda x: (-x["children_waiting"], -(x["fails"] or 0)))

    # log tails for running
    log_tails = []
    log_dirs = [
        HERMES_HOME / "kanban" / "boards" / name / "logs"
        if name != "default"
        else HERMES_HOME / "kanban" / "logs",
        HERMES_HOME / "kanban" / "logs",
    ]
    for r in running:
        path = None
        for d in log_dirs:
            cand = d / f"{r['id']}.log"
            if cand.exists():
                path = cand
                break
        tail = ""
        if path and path.exists():
            try:
                data = path.read_text(errors="replace").splitlines()
                tail = "\n".join(data[-25:])
            except Exception as e:
                tail = f"read_err:{e}"
        log_tails.append({"task_id": r["id"], "log": str(path) if path else None, "tail": tail[-2500:]})

    conn.close()
    ready_n = int(counts.get("ready") or 0)
    run_n = int(counts.get("running") or 0)
    todo_n = int(counts.get("todo") or 0)
    stall_hints = []
    if run_n == 0 and ready_n == 0 and todo_n > 0:
        stall_hints.append("idle_no_ready_all_todo_gated_by_parents")
    if run_n == 0 and ready_n > 0:
        stall_hints.append("ready_but_not_spawned_check_dispatcher_profiles")
    if why.get("parents_done_not_promoted"):
        stall_hints.append("recompute_ready_stuck")
    if any(b["children_waiting"] >= 2 and b["status"] == "blocked" for b in top_blockers):
        stall_hints.append("blocked_parent_fanin")

    return {
        "board": name,
        "status_counts": dict(counts),
        "running": running,
        "todo_ready_why": dict(why),
        "top_parent_blockers": top_blockers,
        "blocked_hot": blocked_hot[:20],
        "samples": samples[:30],
        "log_tails": log_tails,
        "stall_hints": stall_hints,
    }


def main() -> int:
    ROOT.mkdir(parents=True, exist_ok=True)
    HIST.mkdir(parents=True, exist_ok=True)
    now = time.time()
    gateways = sh("ps -eo pid,etime,cmd | grep 'hermes_cli.main' | grep 'gateway run' | grep -v grep || true")
    workers = sh("ps -eo pid,etime,cmd | grep 'work kanban task' | grep -v grep || true")
    lock = sh(f"fuser -v {HERMES_HOME / 'kanban/.dispatcher.lock'} 2>&1 | tail -6 || true")
    units = sh(
        "for u in hermes-default-gateway-oneshot hermes-gateway-worker "
        "hermes-gateway-orchestration1 hermes-gateway-orchestration2; do "
        "systemctl show ${u}.service -p Id -p ActiveState -p MainPID --no-pager 2>/dev/null | tr '\\n' ' '; echo; done"
    )

    board_reports = [analyze_board(n, p) for n, p in boards()]
    process_log = None
    hygiene = None
    try:
        if PROCESS_LOG.exists():
            process_log = json.loads(PROCESS_LOG.read_text())
            process_log = {
                "age_s": int(now - PROCESS_LOG.stat().st_mtime),
                "counts": process_log.get("counts"),
                "anomalies_n": len(process_log.get("anomalies") or []),
            }
    except Exception as e:
        process_log = {"error": str(e)}
    try:
        if HYGIENE.exists():
            h = json.loads(HYGIENE.read_text())
            hygiene = {
                "age_s": int(now - HYGIENE.stat().st_mtime),
                "action_types": [a.get("type") for a in (h.get("actions") or []) if isinstance(a, dict)],
                "process_log": h.get("process_log"),
                "retry_enforcer_total": (h.get("retry_enforcer") or {}).get("total_actions"),
            }
    except Exception as e:
        hygiene = {"error": str(e)}

    total_running = sum(len(b.get("running") or []) for b in board_reports)
    total_ready = sum(int((b.get("status_counts") or {}).get("ready") or 0) for b in board_reports)
    total_todo = sum(int((b.get("status_counts") or {}).get("todo") or 0) for b in board_reports)
    total_blocked = sum(int((b.get("status_counts") or {}).get("blocked") or 0) for b in board_reports)
    all_hints: list[str] = []
    for b in board_reports:
        all_hints.extend(b.get("stall_hints") or [])

    # global top blockers across boards
    global_blockers = []
    for b in board_reports:
        for tb in b.get("top_parent_blockers") or []:
            global_blockers.append({**tb, "board": b.get("board")})
    global_blockers.sort(key=lambda x: -int(x.get("children_waiting") or 0))

    snap = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
        "totals": {
            "running": total_running,
            "ready": total_ready,
            "todo": total_todo,
            "blocked": total_blocked,
            "gateway_lines": len([l for l in gateways.splitlines() if l.strip()]),
            "worker_proc_lines": len([l for l in workers.splitlines() if l.strip()]),
        },
        "stall_hints": sorted(set(all_hints)),
        "units": units,
        "gateways": gateways,
        "workers": workers,
        "dispatcher_lock": lock,
        "process_log": process_log,
        "hygiene": hygiene,
        "global_top_blockers": global_blockers[:20],
        "boards": board_reports,
    }

    ts = time.strftime("%Y%m%d_%H%M%S", time.gmtime(now))
    hist_path = HIST / f"{ts}.json"
    payload = json.dumps(snap, ensure_ascii=False, indent=2) + "\n"
    hist_path.write_text(payload)
    LATEST_JSON.write_text(payload)

    # markdown summary
    lines = [
        f"# Orch watchdog {snap['generated_at']}",
        "",
        f"- running={total_running} ready={total_ready} todo={total_todo} blocked={total_blocked}",
        f"- gateway_procs={snap['totals']['gateway_lines']} worker_procs={snap['totals']['worker_proc_lines']}",
        f"- stall_hints: {', '.join(snap['stall_hints']) or 'none'}",
        "",
        "## Top parent blockers (fan-in)",
    ]
    for b in global_blockers[:12]:
        lines.append(
            f"- `{b.get('board')}` **x{b.get('children_waiting')}** `{b.get('status')}` "
            f"fails={b.get('fails')} asg={b.get('assignee')} `{b.get('id')}` {(b.get('title') or '')[:60]}"
        )
        if b.get("err"):
            lines.append(f"  - err: `{b['err'][:120]}`")
    lines.append("")
    lines.append("## Per board")
    for br in board_reports:
        sc = br.get("status_counts") or {}
        if not sc:
            continue
        lines.append(
            f"### {br.get('board')}  running={sc.get('running',0)} ready={sc.get('ready',0)} "
            f"todo={sc.get('todo',0)} blocked={sc.get('blocked',0)}"
        )
        if br.get("todo_ready_why"):
            lines.append(f"- why_todo: `{br['todo_ready_why']}`")
        if br.get("stall_hints"):
            lines.append(f"- hints: {br['stall_hints']}")
        for r in br.get("running") or []:
            lines.append(
                f"- RUN `{r['id']}` pid={r['pid']} alive={r['pid_alive']} age_s={r['age_s']} "
                f"hb_age={r['hb_age_s']} {(r.get('title') or '')[:50]}"
            )
        for lt in br.get("log_tails") or []:
            if lt.get("tail"):
                lines.append(f"  - log `{lt.get('log')}` tail:")
                lines.append("```")
                lines.append(lt["tail"][-1200:])
                lines.append("```")
    lines.append("")
    lines.append("## Processes")
    lines.append("```")
    lines.append((gateways or "").strip() or "(no gateways)")
    lines.append((workers or "").strip() or "(no workers)")
    lines.append("```")

    LATEST_MD.write_text("\n".join(lines) + "\n")

    # stdout short
    print(
        f"orch_watch running={total_running} ready={total_ready} todo={total_todo} "
        f"blocked={total_blocked} hints={snap['stall_hints']}"
    )
    for b in global_blockers[:8]:
        print(
            f"  BLOCK x{b['children_waiting']} {b.get('board')}/{b['id']} {b.get('status')} "
            f"{(b.get('title') or '')[:50]}"
        )
    for br in board_reports:
        for r in br.get("running") or []:
            print(
                f"  RUN {br.get('board')}/{r['id']} alive={r['pid_alive']} age={r['age_s']} {(r.get('title') or '')[:40]}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
