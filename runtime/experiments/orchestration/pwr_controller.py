#!/usr/bin/env python3
"""Durable controller for isolated Plan -> Work -> Review -> Replan lanes.

The gateway dispatcher executes cards.  This controller only advances explicit
``PWR_WORKFLOW_ID`` graphs and records every transition/failure in a lane-local
SQLite journal plus the lane's Ruflo AgentDB namespace.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from kanban_lane_paths import lane_home, lane_runtime_root

LANES = ("default", "orchestration1", "orchestration2")
TERMINAL = {"done", "blocked", "archived", "triage"}
SUCCESS = {"done"}


@dataclass(frozen=True)
class Lane:
    name: str
    profile: str
    home: Path
    db: Path
    runtime: Path
    ruflo_root: Path
    namespace: str
    roles: dict[str, str]
    max_fix_iters: int


def load_lane(name: str) -> Lane:
    if name not in LANES:
        raise ValueError(name)
    home = lane_home(name)
    cfg = yaml.safe_load((home / "config.yaml").read_text()) or {}
    data = cfg.get("orchestration_lane") or {}
    return Lane(
        name=name,
        profile=name,
        home=home,
        db=home / "kanban.db",
        runtime=lane_runtime_root(name) / "pwr-controller",
        ruflo_root=Path(data["ruflo_root"]),
        namespace=str(data["ruflo_namespace"]),
        roles=dict(data["roles"]),
        max_fix_iters=int(data.get("max_fix_iters", 3)),
    )


def env_for(lane: Lane) -> dict[str, str]:
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("HERMES_SESSION_") or key in {"HERMES_HOME", "HERMES_PROFILE"}:
            env.pop(key, None)
    env["HERMES_KANBAN_HOME"] = str(lane.home)
    return env


def run_hermes(lane: Lane, *args: str) -> str:
    proc = subprocess.run(
        ["hermes", "--profile", lane.profile, *args],
        env=env_for(lane), text=True, capture_output=True, timeout=120,
    )
    if proc.returncode:
        raise RuntimeError((proc.stdout + proc.stderr).strip()[-2000:])
    return proc.stdout.strip()


def connect(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(path, timeout=30)
    con.row_factory = sqlite3.Row
    return con


def journal(lane: Lane) -> sqlite3.Connection:
    lane.runtime.mkdir(parents=True, exist_ok=True)
    con = connect(lane.runtime / "state.db")
    con.execute(
        """CREATE TABLE IF NOT EXISTS workflows(
        workflow_id TEXT PRIMARY KEY, title TEXT NOT NULL, goal TEXT NOT NULL,
        workspace TEXT NOT NULL, state TEXT NOT NULL, root_task_id TEXT,
        current_task_id TEXT, iteration INTEGER NOT NULL DEFAULT 0,
        open_signatures TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
        terminal_reason TEXT)"""
    )
    con.execute(
        """CREATE TABLE IF NOT EXISTS events(
        id INTEGER PRIMARY KEY AUTOINCREMENT, workflow_id TEXT NOT NULL,
        kind TEXT NOT NULL, task_id TEXT, payload TEXT, created_at INTEGER NOT NULL)"""
    )
    con.commit()
    return con


def event(j: sqlite3.Connection, workflow_id: str, kind: str, task_id: str | None, payload: Any) -> None:
    body = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False, sort_keys=True)
    j.execute("INSERT INTO events(workflow_id,kind,task_id,payload,created_at) VALUES(?,?,?,?,?)",
              (workflow_id, kind, task_id, body, int(time.time())))
    j.execute("UPDATE workflows SET updated_at=? WHERE workflow_id=?", (int(time.time()), workflow_id))
    j.commit()


def marker(workflow_id: str, phase: str, iteration: int) -> str:
    return f"PWR_WORKFLOW_ID: {workflow_id}\nPWR_PHASE: {phase}\nPWR_ITERATION: {iteration}"


def create_card(
    lane: Lane, *, title: str, body: str, assignee: str, workspace: str,
    parent: str | None = None, max_runtime: str = "30m",
) -> str:
    args = ["kanban", "create", title, "--body", body, "--assignee", assignee,
            "--workspace", workspace, "--max-runtime", max_runtime,
            "--max-retries", "3", "--json"]
    if parent:
        args.extend(["--parent", parent])
    return str(json.loads(run_hermes(lane, *args))["id"])


def task(con: sqlite3.Connection, tid: str) -> dict[str, Any] | None:
    row = con.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone()
    return dict(row) if row else None


def comments(con: sqlite3.Connection, tid: str) -> list[dict[str, Any]]:
    return [dict(r) for r in con.execute(
        "SELECT author,body,created_at FROM task_comments WHERE task_id=? ORDER BY id", (tid,)
    )]


def evidence(con: sqlite3.Connection, t: dict[str, Any]) -> dict[str, Any]:
    runs = [dict(r) for r in con.execute(
        "SELECT status,outcome,summary,error,metadata,started_at,ended_at FROM task_runs WHERE task_id=? ORDER BY id",
        (t["id"],),
    )]
    return {
        "id": t["id"], "title": t["title"], "status": t["status"],
        "result": t.get("result"), "last_failure_error": t.get("last_failure_error"),
        "consecutive_failures": t.get("consecutive_failures"),
        "comments": comments(con, t["id"]), "runs": runs,
    }


def text_evidence(ev: dict[str, Any]) -> str:
    return json.dumps(ev, ensure_ascii=False, sort_keys=True)


def verdict(ev: dict[str, Any]) -> tuple[str, list[str]]:
    text = text_evidence(ev)
    upper = text.upper()
    if re.search(r"(^|\n|\b)APPROVE(D)?\b", upper) and "ISSUES" not in upper:
        return "APPROVE", []
    sigs: list[str] = []
    for line in text.splitlines():
        if "ISSUES" in line.upper() or "SIGNATURE" in line.upper() or ":" in line:
            s = re.sub(r"\s+", " ", line).strip()[:400]
            if s and s not in sigs:
                sigs.append(s)
    if not sigs:
        digest = hashlib.sha256(text.encode()).hexdigest()[:16]
        sigs = [f"reviewer:no-approve:{digest}"]
    return "ISSUES", sigs[:20]


def ruflo_store(lane: Lane, key: str, value: dict[str, Any]) -> dict[str, Any]:
    payload = json.dumps({
        "key": key,
        "value": json.dumps(value, ensure_ascii=False, sort_keys=True),
        "namespace": lane.namespace,
        "upsert": True,
    }, ensure_ascii=False)
    proc = subprocess.run(
        ["ruflo", "mcp", "exec", "-t", "memory_store", "-p", payload],
        cwd=lane.ruflo_root, text=True, capture_output=True, timeout=60,
    )
    return {"rc": proc.returncode, "output": (proc.stdout + proc.stderr)[-1500:]}


def durable_event(lane: Lane, j: sqlite3.Connection, wid: str, kind: str,
                  tid: str | None, payload: dict[str, Any]) -> None:
    event(j, wid, kind, tid, payload)
    stored = ruflo_store(lane, f"pwr:{wid}:{int(time.time())}:{kind}", {
        "lane": lane.name, "workflow_id": wid, "kind": kind, "task_id": tid,
        "payload": payload, "recorded_at": int(time.time()),
    })
    event(j, wid, "ruflo_store", tid, stored)


def create_workflow(lane: Lane, title: str, goal: str, workspace: str,
                    max_runtime: str) -> dict[str, Any]:
    wid = "pwr_" + uuid.uuid4().hex[:12]
    j = journal(lane)
    now = int(time.time())
    j.execute(
        "INSERT INTO workflows(workflow_id,title,goal,workspace,state,iteration,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
        (wid, title, goal, workspace, "PLANNING", 0, now, now),
    )
    j.commit()
    body = f"""MODE: EVIDENCE_TRIAGE or IMPLEMENTATION_PLAN.
{marker(wid, 'PLAN', 0)}
ORIGINAL_GOAL:
{goal}

First decide whether facts required for a correct plan are missing. If so, create focused RESEARCH Worker cards and a fresh dependent Planner continuation. Otherwise create exact implementation/integration cards. Do not implement or review. All children must carry the same PWR_WORKFLOW_ID and terminate via kanban complete/block.
ISOLATED_LANE: {lane.name}
RUFLO_ROOT: {lane.ruflo_root}
RUFLO_NAMESPACE: {lane.namespace}
"""
    root = create_card(lane, title=f"PLAN: {title}", body=body,
                       assignee=lane.roles["planner"], workspace=workspace,
                       max_runtime=max_runtime)
    j.execute("UPDATE workflows SET root_task_id=?,current_task_id=? WHERE workflow_id=?", (root, root, wid))
    j.commit()
    durable_event(lane, j, wid, "workflow_created", root, {"title": title, "goal": goal})
    return {"workflow_id": wid, "task_id": root, "lane": lane.name}


def workflow_tasks(k: sqlite3.Connection, wid: str) -> list[dict[str, Any]]:
    rows = k.execute("SELECT * FROM tasks WHERE body LIKE ? ORDER BY created_at,id", (f"%PWR_WORKFLOW_ID: {wid}%",)).fetchall()
    return [dict(r) for r in rows]


def phase_of(t: dict[str, Any]) -> str:
    m = re.search(r"^PWR_PHASE:\s*([A-Z_]+)\s*$", t.get("body") or "", re.M)
    if m:
        return m.group(1)
    a = (t.get("assignee") or "").lower()
    if "reviewer" in a:
        return "REVIEW"
    if "planner" in a:
        return "PLAN"
    return "WORK"


def children(k: sqlite3.Connection, tid: str) -> list[str]:
    return [r[0] for r in k.execute("SELECT child_id FROM task_links WHERE parent_id=?", (tid,))]


def create_review(lane: Lane, wf: sqlite3.Row, producer: dict[str, Any], max_runtime: str = "30m") -> str:
    wid = wf["workflow_id"]
    iteration = int(wf["iteration"])
    body = f"""{marker(wid, 'REVIEW', iteration)}
ORIGINAL_GOAL:
{wf['goal']}

PRODUCER: {producer['id']}
Read parent evidence and re-check the real outcome. First line and terminal result must be exactly APPROVE or ISSUES. For ISSUES emit stable signatures. Read-only; never implement fixes. End via kanban complete/block.
ISOLATED_LANE: {lane.name}
RUFLO_NAMESPACE: {lane.namespace}
"""
    return create_card(lane, title=f"REVIEW[{iteration}]: {wf['title']}", body=body,
                       assignee=lane.roles["reviewer"], workspace=wf["workspace"],
                       parent=producer["id"], max_runtime=max_runtime)


def create_replan(lane: Lane, wf: sqlite3.Row, review_ev: dict[str, Any], sigs: list[str],
                  max_runtime: str = "30m") -> str:
    wid = wf["workflow_id"]
    iteration = int(wf["iteration"]) + 1
    issue_json = json.dumps({"signatures": sigs, "review": review_ev}, ensure_ascii=False)
    body = f"""MODE: REPLAN.
{marker(wid, 'REPLAN', iteration)}
ORIGINAL_GOAL:
{wf['goal']}

OPEN_REVIEWER_ISSUES:
{issue_json}

Address only open issue signatures, preserve successful/approved work, and create focused correction Worker/INT cards followed by a Reviewer only after producer SUCCESS. Do not repeat unchanged work. All children must carry PWR_WORKFLOW_ID. End via kanban complete/block.
ISOLATED_LANE: {lane.name}
RUFLO_NAMESPACE: {lane.namespace}
"""
    return create_card(lane, title=f"REPLAN[{iteration}]: {wf['title']}", body=body,
                       assignee=lane.roles["planner"], workspace=wf["workspace"],
                       parent=review_ev["id"], max_runtime=max_runtime)


def tick_lane(lane: Lane) -> dict[str, Any]:
    j = journal(lane)
    k = connect(lane.db)
    changed: list[dict[str, Any]] = []
    workflows = j.execute("SELECT * FROM workflows WHERE state NOT IN ('APPROVED','GAVE_UP','NEEDS_INPUT') ORDER BY created_at").fetchall()
    for wf in workflows:
        wid = wf["workflow_id"]
        tasks = workflow_tasks(k, wid)
        if not tasks:
            continue
        terminal = [t for t in tasks if t["status"] in TERMINAL]
        nonterminal = [t for t in tasks if t["status"] not in TERMINAL]
        # Persist every newly observed terminal card, including failures.
        for t in terminal:
            key = f"observed:{t['id']}:{t['status']}"
            if not j.execute("SELECT 1 FROM events WHERE workflow_id=? AND kind=?", (wid, key)).fetchone():
                durable_event(lane, j, wid, key, t["id"], evidence(k, t))
        if nonterminal:
            continue
        latest = tasks[-1]
        phase = phase_of(latest)
        ev = evidence(k, latest)
        if latest["status"] != "done":
            state = "NEEDS_INPUT" if latest.get("block_kind") == "needs_input" else "GAVE_UP"
            j.execute("UPDATE workflows SET state=?,current_task_id=?,terminal_reason=?,updated_at=? WHERE workflow_id=?",
                      (state, latest["id"], text_evidence(ev)[:2000], int(time.time()), wid)); j.commit()
            durable_event(lane, j, wid, "workflow_terminal_failure", latest["id"], ev)
            changed.append({"workflow_id": wid, "state": state})
            continue
        if phase == "REVIEW":
            v, sigs = verdict(ev)
            if v == "APPROVE":
                j.execute("UPDATE workflows SET state='APPROVED',current_task_id=?,updated_at=? WHERE workflow_id=?",
                          (latest["id"], int(time.time()), wid)); j.commit()
                durable_event(lane, j, wid, "workflow_approved", latest["id"], ev)
                changed.append({"workflow_id": wid, "state": "APPROVED"})
                continue
            sig_json = json.dumps(sigs, ensure_ascii=False, sort_keys=True)
            prev = wf["open_signatures"] or ""
            iteration = int(wf["iteration"])
            repeated = prev == sig_json
            if iteration >= lane.max_fix_iters or (repeated and iteration >= 2):
                reason = "limit" if iteration >= lane.max_fix_iters else "stuck"
                j.execute("UPDATE workflows SET state='GAVE_UP',terminal_reason=?,open_signatures=?,updated_at=? WHERE workflow_id=?",
                          (reason, sig_json, int(time.time()), wid)); j.commit()
                durable_event(lane, j, wid, "workflow_gave_up", latest["id"], {"reason": reason, "signatures": sigs, "review": ev})
                changed.append({"workflow_id": wid, "state": "GAVE_UP", "reason": reason})
                continue
            rep = create_replan(lane, wf, ev, sigs)
            j.execute("UPDATE workflows SET state='REPLANNING',current_task_id=?,iteration=?,open_signatures=?,updated_at=? WHERE workflow_id=?",
                      (rep, iteration + 1, sig_json, int(time.time()), wid)); j.commit()
            durable_event(lane, j, wid, "replan_created", rep, {"signatures": sigs, "review_task": latest["id"]})
            changed.append({"workflow_id": wid, "state": "REPLANNING", "task_id": rep})
            continue
        # Planner/worker cards may create a downstream graph themselves. If all are done and no Reviewer exists, gate one Reviewer on the latest successful non-Planner producer.
        producers = [t for t in terminal if t["status"] == "done" and phase_of(t) == "WORK"]
        if not producers:
            # Planner cards may either create an executable child graph or contain
            # a complete one-card plan.  Materialize the latter as a Worker card.
            if phase in {"PLAN", "REPLAN"}:
                worker_body = f"""{marker(wid, 'WORK', int(wf['iteration']))}
ORIGINAL_GOAL:
{wf['goal']}

PARENT_PLAN: {latest['id']}
Read the completed parent plan/evidence, implement it fully, run objective checks, and end SUCCESS/FAILED via kanban complete/block. Record files, tests, commit SHA where applicable, and re-check commands.
ISOLATED_LANE: {lane.name}
RUFLO_NAMESPACE: {lane.namespace}
"""
                work = create_card(lane, title=f"WORK[{wf['iteration']}]: {wf['title']}",
                                   body=worker_body, assignee=lane.roles["worker"],
                                   workspace=wf["workspace"], parent=latest["id"])
                j.execute("UPDATE workflows SET state='WORKING',current_task_id=?,updated_at=? WHERE workflow_id=?",
                          (work, int(time.time()), wid)); j.commit()
                durable_event(lane, j, wid, "worker_created", work, {"plan_task": latest["id"]})
                changed.append({"workflow_id": wid, "state": "WORKING", "task_id": work})
                continue
            j.execute("UPDATE workflows SET state='GAVE_UP',terminal_reason=?,updated_at=? WHERE workflow_id=?",
                      ("no_successful_producer", int(time.time()), wid)); j.commit()
            durable_event(lane, j, wid, "workflow_gave_up", latest["id"], {"reason": "no_successful_producer", "evidence": ev})
            changed.append({"workflow_id": wid, "state": "GAVE_UP"})
            continue
        producer = producers[-1]
        review = create_review(lane, wf, producer)
        j.execute("UPDATE workflows SET state='REVIEWING',current_task_id=?,updated_at=? WHERE workflow_id=?",
                  (review, int(time.time()), wid)); j.commit()
        durable_event(lane, j, wid, "review_created", review, {"producer": producer["id"]})
        changed.append({"workflow_id": wid, "state": "REVIEWING", "task_id": review})
    k.close(); j.close()
    return {"lane": lane.name, "changed": changed}


def status(lane: Lane, wid: str | None = None) -> list[dict[str, Any]]:
    j = journal(lane)
    q = "SELECT * FROM workflows"
    params: tuple[Any, ...] = ()
    if wid:
        q += " WHERE workflow_id=?"; params = (wid,)
    rows = [dict(r) for r in j.execute(q + " ORDER BY created_at", params)]
    for r in rows:
        r["events"] = [dict(e) for e in j.execute(
            "SELECT kind,task_id,payload,created_at FROM events WHERE workflow_id=? ORDER BY id", (r["workflow_id"],)
        )]
    j.close(); return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    sp = ap.add_subparsers(dest="cmd", required=True)
    c = sp.add_parser("create")
    c.add_argument("--lane", choices=LANES, required=True); c.add_argument("--title", required=True)
    c.add_argument("--goal", required=True); c.add_argument("--workspace", default="dir:/root/main")
    c.add_argument("--max-runtime", default="30m")
    t = sp.add_parser("tick"); t.add_argument("--lane", choices=(*LANES, "all"), default="all")
    s = sp.add_parser("status"); s.add_argument("--lane", choices=LANES, required=True); s.add_argument("--workflow-id")
    args = ap.parse_args()
    if args.cmd == "create": out = create_workflow(load_lane(args.lane), args.title, args.goal, args.workspace, args.max_runtime)
    elif args.cmd == "tick":
        names = LANES if args.lane == "all" else (args.lane,)
        out = [tick_lane(load_lane(n)) for n in names]
    else: out = status(load_lane(args.lane), args.workflow_id)
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
