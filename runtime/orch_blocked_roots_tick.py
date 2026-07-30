#!/usr/bin/env python3
"""One-shot blocked-root triage for orchestration stall (read-only).

Focus: parents that gate todo/ready children (fan-in), classify block reason,
propose action. Writes orch_watch/blocked_roots_tick.{json,md}.
"""
from __future__ import annotations

import json
import sqlite3
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

OUT_DIR = Path("/root/main/runtime/orch_watch")
KANBAN_DB = Path("/root/.hermes/kanban.db")
BOARDS = Path("/root/.hermes/kanban/boards")


def boards() -> list[tuple[str, Path]]:
    out = [("default", KANBAN_DB)]
    if BOARDS.is_dir():
        for p in sorted(BOARDS.glob("*/kanban.db")):
            out.append((p.parent.name, p))
    return out


def classify(
    err: str | None,
    body: str | None,
    block_kind: str | None,
    fails: Any,
    blobs: list[str],
) -> list[str]:
    text = " ".join(
        [
            err or "",
            (body or "")[:2500],
            block_kind or "",
            " ".join(blobs or []),
        ]
    ).lower()
    reasons: list[str] = []
    if "retry_enforcer" in text or "needs_replan" in text:
        reasons.append("retry_enforcer_stamp")
    if (
        "protocol" in text
        or "without calling kanban" in text
        or "kanban_complete" in text
    ):
        reasons.append("protocol")
    if "timed_out" in text or "timeout" in text or "max_runtime" in text:
        reasons.append("timeout")
    if (
        "iteration budget" in text
        or "max_turns" in text
        or "budget exhausted" in text
    ):
        reasons.append("iteration_budget")
    if "needs_input" in text or block_kind == "needs_input":
        reasons.append("needs_input")
    if "issues" in text or (
        "reviewer" in text and "approve" not in text and "approval" not in text
    ):
        reasons.append("reviewer_issues")
    if "parent" in text and (
        "invalid" in text or "missing" in text or "not done" in text
    ):
        reasons.append("graph_parent")
    if (
        "ssh" in text
        or "host unreachable" in text
        or "connection refused" in text
    ):
        reasons.append("infra")
    if (
        "human" in text
        or "waiting for user" in text
        or "ask the user" in text
        or "e2e message" in text
    ):
        reasons.append("human_gate")
    try:
        if fails is not None and int(fails) >= 3:
            reasons.append("fails_exhausted")
    except Exception:
        pass
    if not reasons:
        if block_kind:
            reasons.append(f"block_kind:{block_kind}")
        else:
            reasons.append("unclear_sticky")
    return reasons


def propose_action(
    status: str,
    classes: list[str],
    upstream_open: list[dict],
    children_waiting: int,
) -> str:
    if status == "todo":
        if children_waiting > 0 and not upstream_open:
            return "promote_or_dispatch_parent_todo"
        if upstream_open:
            return "unblock_upstream_first"
        return "todo_no_fanin"
    if status == "ready":
        return "dispatch_ready_parent"
    if status == "blocked":
        if "needs_input" in classes or "human_gate" in classes:
            return "needs_human_input"
        if "infra" in classes:
            return "fix_infra_then_unblock"
        if any(
            c in classes
            for c in (
                "protocol",
                "timeout",
                "iteration_budget",
                "retry_enforcer_stamp",
                "fails_exhausted",
            )
        ):
            return "replan_slice_not_redispatch"
        if "reviewer_issues" in classes:
            return "replan_open_review_signatures_only"
        if "graph_parent" in classes:
            return "fix_graph_edges"
        return "read_block_reason_then_decide"
    if status == "MISSING":
        return "fix_missing_parent_edge"
    return "inspect"


def analyze_board(name: str, db: Path, now: int) -> dict[str, Any]:
    c = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=60)
    c.row_factory = sqlite3.Row
    parents: dict[str, list[str]] = defaultdict(list)
    children: dict[str, list[str]] = defaultdict(list)
    try:
        for r in c.execute("SELECT parent_id, child_id FROM task_links"):
            parents[r["child_id"]].append(r["parent_id"])
            children[r["parent_id"]].append(r["child_id"])
    except Exception:
        pass

    tasks: dict[str, dict] = {}
    for r in c.execute(
        """
        SELECT id, title, status, assignee, consecutive_failures, max_retries,
               block_kind, body, result, last_failure_error, started_at,
               created_at, worker_pid, max_runtime_seconds, session_id
        FROM tasks
        """
    ):
        tasks[r["id"]] = dict(r)

    def last_comments(tid: str, n: int = 5) -> list[str]:
        try:
            rows = c.execute(
                "SELECT substr(body,1,320) b, created_at FROM task_comments "
                "WHERE task_id=? ORDER BY created_at DESC LIMIT ?",
                (tid, n),
            ).fetchall()
            return [f"[{r['created_at']}] {r['b']}" for r in rows]
        except Exception:
            return []

    def last_events(tid: str, n: int = 8) -> list[str]:
        try:
            rows = c.execute(
                "SELECT * FROM task_events WHERE task_id=? ORDER BY rowid DESC LIMIT ?",
                (tid, n),
            ).fetchall()
            out = []
            for r in rows:
                d = dict(r)
                kind = d.get("kind") or d.get("event_type") or d.get("type") or ""
                msg = (
                    d.get("message")
                    or d.get("payload")
                    or d.get("data")
                    or d.get("body")
                    or ""
                )
                if isinstance(msg, bytes):
                    msg = msg.decode("utf-8", "replace")
                out.append(f"{kind}: {str(msg)[:220]}")
            return out
        except Exception as e:
            return [f"events_err:{e}"]

    def last_runs(tid: str, n: int = 3) -> list[dict]:
        try:
            rows = c.execute(
                """
                SELECT id, outcome, exit_code, started_at, ended_at,
                       substr(COALESCE(summary,''),1,220) summary
                FROM task_runs WHERE task_id=? ORDER BY id DESC LIMIT ?
                """,
                (tid, n),
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            return [{"error": str(e)}]

    waiting_on: Counter = Counter()
    for tid, t in tasks.items():
        if t["status"] not in ("todo", "ready"):
            continue
        for p in parents.get(tid, []):
            pt = tasks.get(p)
            ps = pt["status"] if pt else "MISSING"
            if ps not in ("done", "archived"):
                waiting_on[p] += 1

    candidate_ids = set(waiting_on.keys())
    for tid, t in tasks.items():
        if t["status"] == "blocked" and int(t["consecutive_failures"] or 0) >= 2:
            candidate_ids.add(tid)
        if t["status"] == "blocked" and waiting_on.get(tid, 0) > 0:
            candidate_ids.add(tid)

    roots: list[dict] = []
    for pid in candidate_ids:
        t = tasks.get(pid)
        if not t:
            roots.append(
                {
                    "id": pid,
                    "board": name,
                    "status": "MISSING",
                    "children_waiting": waiting_on.get(pid, 0),
                    "classes": ["missing_parent"],
                    "proposed_action": "fix_missing_parent_edge",
                    "sole_gate_children": 0,
                    "multi_gate_children": 0,
                }
            )
            continue

        comments = last_comments(pid)
        events = last_events(pid)
        runs = last_runs(pid)
        blobs = comments + events + [
            str(r.get("summary") or "") for r in runs if isinstance(r, dict)
        ]
        classes = classify(
            t.get("last_failure_error"),
            t.get("body"),
            t.get("block_kind"),
            t.get("consecutive_failures"),
            blobs,
        )

        direct_children = children.get(pid, [])
        sole_gate = 0
        multi_gate = 0
        child_info = []
        for ch in direct_children:
            ct = tasks.get(ch)
            if not ct or ct["status"] not in ("todo", "ready", "blocked"):
                continue
            open_pars = []
            for p2 in parents.get(ch, []):
                pt = tasks.get(p2)
                if not pt or pt["status"] not in ("done", "archived"):
                    open_pars.append(p2)
            if ct["status"] in ("todo", "ready") and open_pars:
                if len(open_pars) == 1 and open_pars[0] == pid:
                    sole_gate += 1
                else:
                    multi_gate += 1
            child_info.append(
                {
                    "id": ch,
                    "status": ct["status"],
                    "assignee": ct["assignee"],
                    "title": (ct["title"] or "")[:70],
                    "open_parents": open_pars,
                }
            )

        upstream_open = []
        for p2 in parents.get(pid, []):
            pt = tasks.get(p2)
            if not pt or pt["status"] not in ("done", "archived"):
                upstream_open.append(
                    {
                        "id": p2,
                        "status": pt["status"] if pt else "MISSING",
                        "title": (pt["title"][:50] if pt else "?"),
                        "assignee": pt["assignee"] if pt else None,
                    }
                )

        action = propose_action(
            t["status"], classes, upstream_open, waiting_on.get(pid, 0)
        )

        age = None
        for key in ("started_at", "created_at"):
            if t.get(key):
                try:
                    age = now - int(t[key])
                    break
                except Exception:
                    pass

        roots.append(
            {
                "id": pid,
                "board": name,
                "status": t["status"],
                "assignee": t["assignee"],
                "title": (t["title"] or "")[:100],
                "fails": t["consecutive_failures"],
                "max_retries": t["max_retries"],
                "block_kind": t["block_kind"],
                "err": (t.get("last_failure_error") or "")[:240],
                "result_head": (t.get("result") or "")[:240],
                "body_head": (t.get("body") or "")[:450],
                "classes": classes,
                "children_waiting": waiting_on.get(pid, 0),
                "sole_gate_children": sole_gate,
                "multi_gate_children": multi_gate,
                "direct_children_open": child_info[:12],
                "upstream_open": upstream_open,
                "comments": comments[:5],
                "events": events[:6],
                "runs": runs,
                "proposed_action": action,
                "age_s": age,
            }
        )

    roots.sort(
        key=lambda r: (
            -int(r.get("sole_gate_children") or 0),
            -int(r.get("children_waiting") or 0),
            -int(r.get("fails") or 0),
        )
    )

    class_counts: Counter = Counter()
    action_counts: Counter = Counter()
    for r in roots:
        for cl in r.get("classes") or ["?"]:
            class_counts[cl] += 1
        action_counts[r.get("proposed_action") or "?"] += 1

    c.close()
    return {
        "status_counts": dict(Counter(t["status"] for t in tasks.values())),
        "roots_with_fanin_or_hot": roots[:50],
        "class_counts": dict(class_counts),
        "action_counts": dict(action_counts),
    }


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    now = int(time.time())
    report: dict[str, Any] = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
        "boards": {},
        "priority_actions": [],
    }

    for name, db in boards():
        if not db.exists():
            continue
        b = analyze_board(name, db, now)
        report["boards"][name] = b
        for r in b.get("roots_with_fanin_or_hot") or []:
            if int(r.get("sole_gate_children") or 0) >= 1 or int(
                r.get("children_waiting") or 0
            ) >= 2:
                report["priority_actions"].append(
                    {
                        "board": name,
                        "id": r["id"],
                        "title": r.get("title"),
                        "status": r.get("status"),
                        "assignee": r.get("assignee"),
                        "classes": r.get("classes"),
                        "action": r.get("proposed_action"),
                        "sole_gate": r.get("sole_gate_children"),
                        "waiting": r.get("children_waiting"),
                        "fails": r.get("fails"),
                        "err": r.get("err"),
                        "block_kind": r.get("block_kind"),
                        "upstream_open": r.get("upstream_open"),
                        "comment0": (r.get("comments") or [None])[0],
                        "last_run": (r.get("runs") or [None])[0],
                    }
                )

    report["priority_actions"].sort(
        key=lambda x: (-int(x.get("sole_gate") or 0), -int(x.get("waiting") or 0))
    )

    out_json = OUT_DIR / "blocked_roots_tick.json"
    out_md = OUT_DIR / "blocked_roots_tick.md"
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")

    lines = [
        f"# Blocked roots triage {report['generated_at']}",
        "",
        "## Priority (unlock fan-in first)",
    ]
    for i, a in enumerate(report["priority_actions"][:30], 1):
        lines.append(
            f"{i}. **`{a['board']}/{a['id']}`** `{a['status']}` asg={a['assignee']} "
            f"sole_gate={a['sole_gate']} waiting={a['waiting']} fails={a['fails']}"
        )
        lines.append(f"   - {(a.get('title') or '')[:90]}")
        lines.append(f"   - classes: `{', '.join(a.get('classes') or [])}`")
        lines.append(f"   - **action:** `{a['action']}`")
        if a.get("block_kind"):
            lines.append(f"   - block_kind: `{a['block_kind']}`")
        if a.get("err"):
            lines.append(f"   - err: `{(a.get('err') or '')[:180]}`")
        if a.get("comment0"):
            lines.append(f"   - comment: `{(a.get('comment0') or '')[:180]}`")
        lr = a.get("last_run")
        if isinstance(lr, dict) and not lr.get("error"):
            lines.append(
                f"   - last_run: outcome={lr.get('outcome')} "
                f"summary=`{(lr.get('summary') or '')[:140]}`"
            )
        if a.get("upstream_open"):
            lines.append(f"   - upstream_open: `{a['upstream_open']}`")

    lines.append("")
    lines.append("## Per-board class / action mix")
    for name, b in report["boards"].items():
        lines.append(f"### {name}")
        lines.append(f"- status: `{b.get('status_counts')}`")
        lines.append(f"- classes: `{b.get('class_counts')}`")
        lines.append(f"- actions: `{b.get('action_counts')}`")
        for r in (b.get("roots_with_fanin_or_hot") or [])[:10]:
            lines.append(
                f"- `{r['id']}` {r['status']} sole={r.get('sole_gate_children')} "
                f"wait={r.get('children_waiting')} → **{r.get('proposed_action')}** | "
                f"{', '.join(r.get('classes') or [])}"
            )
            lines.append(f"  - {r.get('title')}")
            if r.get("err"):
                lines.append(f"  - err: `{r['err'][:150]}`")
            if r.get("comments"):
                lines.append(f"  - comment0: `{r['comments'][0][:170]}`")
            if r.get("runs") and isinstance(r["runs"][0], dict):
                rr = r["runs"][0]
                lines.append(
                    f"  - last_run: outcome={rr.get('outcome')} "
                    f"summary=`{(rr.get('summary') or '')[:130]}`"
                )

    # executive recommendation buckets
    buckets: Counter = Counter(a["action"] for a in report["priority_actions"])
    lines.append("")
    lines.append("## Recommended next moves (no auto-mutate)")
    lines.append(f"- priority roots: **{len(report['priority_actions'])}**")
    lines.append(f"- by action: `{dict(buckets)}`")
    lines.append(
        "- Do **not** blind redispatch protocol/timeout roots — replan slice only."
    )
    lines.append(
        "- Parent still `todo` with no upstream and fan-in → check why not `ready` "
        "(recompute_ready / sticky / assignee profile)."
    )
    lines.append(
        "- `needs_human_input` / human_gate → digest to user, keep blocked."
    )

    out_md.write_text("\n".join(lines) + "\n")
    print(out_md.read_text()[:7000])
    print("---")
    print("priority_n", len(report["priority_actions"]))
    print("buckets", dict(buckets))
    print("wrote", out_json, out_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
