#!/usr/bin/env python3
"""Orchestration completion-confirm + related-board close helper (safe-by-default).

Problem
-------
Product work may be finished (Xray migrated, telemt epic done) while kanban still
holds open todo/blocked tails. Watchdogs then treat those tails as "what stalls
the 3 bots". User wants an **explicit confirmation** before closing boards.

Also: needs_input asks sometimes never reach Telegram because cards blocked via
worker/API never got ``kanban_notify_subs``, and quiet_notify only dropped noise
without repairing missing human-gate subs (fixed in hygiene).

Modes
-----
  scan (default)
      List open boards/tails + candidates that look "product-done but graph open".
      Optionally send ONE Telegram digest asking which boards to close.
      Writes pending prompt under runtime/orch_watch/completion_pending.json

  confirm --yes --boards default,telemt-proxy[,...]
      After user said yes: archive open non-running tasks on those boards
      (or --board-ids). Refuses without --yes. Dry-run without --apply.

  apply-pending --yes
      Close boards listed in completion_pending.json after user confirmation.

  status
      Show pending prompt + last close report.

Never force-kills running workers. Never deletes DBs. Archive only.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

HERMES_HOME = Path("/root/.hermes")  # absolute multi-profile root; ignore process HERMES_HOME
OUT = Path("/root/main/runtime/orch_watch")
PENDING = OUT / "completion_pending.json"
LAST_CLOSE = OUT / "completion_last_close.json"
DEFAULT_HOME_CHAT = "7543801207"


def boards() -> list[tuple[str, Path]]:
    out = [("default", HERMES_HOME / "kanban.db")]
    b = HERMES_HOME / "kanban" / "boards"
    if b.is_dir():
        for p in sorted(b.glob("*/kanban.db")):
            out.append((p.parent.name, p))
    return out


def load_default_token_chat() -> tuple[str, str]:
    env = HERMES_HOME / ".env"
    token = ""
    chat = DEFAULT_HOME_CHAT
    if env.exists():
        for line in env.read_text(errors="replace").splitlines():
            if line.startswith("TELEGRAM_BOT_TOKEN="):
                token = line.split("=", 1)[1].strip()
            elif line.startswith("TELEGRAM_HOME_CHANNEL="):
                chat = line.split("=", 1)[1].strip() or chat
    return token, chat


def tg_send(text: str, disable_notification: bool = False) -> dict[str, Any]:
    token, chat = load_default_token_chat()
    if not token:
        return {"ok": False, "error": "no TELEGRAM_BOT_TOKEN in default .env"}
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode(
        {
            "chat_id": chat,
            "text": text[:3900],
            "disable_notification": "true" if disable_notification else "false",
        }
    ).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode())
            return body
    except Exception as e:
        return {"ok": False, "error": str(e)}


def board_snapshot(name: str, db: Path) -> dict[str, Any]:
    if not db.exists():
        return {"board": name, "error": "missing"}
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    counts = {
        r["status"]: r["c"]
        for r in con.execute(
            "SELECT status, count(*) c FROM tasks GROUP BY status"
        )
    }
    open_rows = list(
        con.execute(
            """
            SELECT id, status, assignee, ifnull(block_kind,'') bk,
                   consecutive_failures fails,
                   substr(title,1,90) title
            FROM tasks
            WHERE status NOT IN ('done','archived')
            ORDER BY CASE status
              WHEN 'running' THEN 0 WHEN 'ready' THEN 1
              WHEN 'todo' THEN 2 ELSE 3 END, id
            LIMIT 80
            """
        )
    )
    done_recent = list(
        con.execute(
            """
            SELECT id, substr(title,1,80) title, completed_at
            FROM tasks WHERE status IN ('done','archived')
            ORDER BY COALESCE(completed_at,0) DESC LIMIT 8
            """
        )
    )
    ni = [
        dict(r)
        for r in open_rows
        if r["status"] == "blocked" and r["bk"] in ("needs_input", "capability")
    ]
    running = [dict(r) for r in open_rows if r["status"] == "running"]
    con.close()
    return {
        "board": name,
        "db": str(db),
        "counts": counts,
        "open_n": sum(
            counts.get(s, 0)
            for s in ("todo", "ready", "running", "blocked", "review", "triage")
        ),
        "running": running,
        "needs_input": ni[:20],
        "open_sample": [dict(r) for r in open_rows[:25]],
        "done_recent": [dict(r) for r in done_recent],
    }


def scan(send_tg: bool) -> dict[str, Any]:
    snaps = [board_snapshot(n, p) for n, p in boards()]
    # Heuristic candidates: boards with 0 running and many open tails,
    # or name-matched product epics user often finishes outside graph.
    candidates = []
    for s in snaps:
        if s.get("error"):
            continue
        run_n = len(s.get("running") or [])
        open_n = int(s.get("open_n") or 0)
        if run_n == 0 and open_n > 0:
            candidates.append(
                {
                    "board": s["board"],
                    "open_n": open_n,
                    "blocked": (s.get("counts") or {}).get("blocked", 0),
                    "todo": (s.get("counts") or {}).get("todo", 0),
                    "needs_input_n": len(s.get("needs_input") or []),
                    "reason": "no_running_but_open_tails",
                }
            )
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "boards": snaps,
        "candidates_to_confirm_close": candidates,
        "instructions": {
            "ask": "Confirm which boards are product-complete so open tails can be archived.",
            "confirm_cmd": (
                "python3 /root/main/runtime/orch_completion_confirm.py "
                "confirm --yes --apply --boards <board1,board2>"
            ),
            "or": (
                "python3 /root/main/runtime/orch_completion_confirm.py "
                "apply-pending --yes --apply"
            ),
        },
    }
    OUT.mkdir(parents=True, exist_ok=True)
    PENDING.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")

    # Human digest
    lines = [
        "Kanban completion confirm",
        f"UTC {report['generated_at']}",
        "",
        "Are these product tasks finished? If YES, reply which boards to close",
        "(archive open tails). Running tasks are never touched.",
        "",
    ]
    for c in candidates:
        lines.append(
            f"- {c['board']}: open={c['open_n']} blocked={c['blocked']} "
            f"todo={c['todo']} needs_input={c['needs_input_n']}"
        )
    if not candidates:
        lines.append("- (no idle boards with open tails)")
    lines.append("")
    lines.append("Examples to close after your YES:")
    lines.append("  boards: telemt-proxy")
    lines.append("  boards: telemt-proxy,default  (only if default epic done)")
    lines.append("")
    lines.append("needs_input open on default (asks should reach you):")
    for s in snaps:
        if s.get("board") != "default":
            continue
        for t in (s.get("needs_input") or [])[:8]:
            lines.append(f"  • {t['id']} {t['title'][:60]}")
    text = "\n".join(lines)
    (OUT / "completion_ask.md").write_text(text + "\n")
    tg_result = None
    if send_tg:
        # Terminal user decision → notification allowed
        tg_result = tg_send(text, disable_notification=False)
    report["telegram"] = tg_result
    PENDING.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(text)
    if tg_result is not None:
        print("telegram:", json.dumps(tg_result, ensure_ascii=False)[:300])
    return report


def archive_open_on_board(
    name: str, db: Path, apply: bool
) -> dict[str, Any]:
    con = sqlite3.connect(str(db))
    con.row_factory = sqlite3.Row
    open_tasks = list(
        con.execute(
            """
            SELECT id, status, assignee, substr(title,1,100) title, worker_pid
            FROM tasks
            WHERE status NOT IN ('done','archived')
            """
        )
    )
    archived = []
    skipped_running = []
    now = int(time.time())
    for t in open_tasks:
        if t["status"] == "running":
            skipped_running.append(dict(t))
            continue
        archived.append({"id": t["id"], "was": t["status"], "title": t["title"]})
        if apply:
            con.execute(
                """
                UPDATE tasks
                   SET status='archived',
                       completed_at=COALESCE(completed_at, ?),
                       claim_lock=NULL,
                       claim_expires=NULL,
                       worker_pid=NULL
                 WHERE id=? AND status NOT IN ('done','archived','running')
                """,
                (now, t["id"]),
            )
            try:
                con.execute(
                    """
                    INSERT INTO task_events (task_id, kind, payload, created_at)
                    VALUES (?, 'archived', ?, ?)
                    """,
                    (
                        t["id"],
                        json.dumps(
                            {
                                "reason": "completion_confirm_user_yes",
                                "prev_status": t["status"],
                            }
                        ),
                        now,
                    ),
                )
            except Exception:
                # events schema may differ slightly
                pass
            # drop notify subs for archived
            try:
                con.execute(
                    "DELETE FROM kanban_notify_subs WHERE task_id=?", (t["id"],)
                )
            except Exception:
                pass
    if apply:
        con.commit()
    con.close()
    return {
        "board": name,
        "db": str(db),
        "apply": apply,
        "archived_n": len(archived),
        "archived_sample": archived[:40],
        "skipped_running": skipped_running,
    }


def confirm(board_names: list[str], apply: bool, yes: bool) -> dict[str, Any]:
    if not yes:
        raise SystemExit("refusing: pass --yes after explicit user confirmation")
    if not board_names:
        raise SystemExit("no boards specified")
    known = {n: p for n, p in boards()}
    results = []
    for name in board_names:
        name = name.strip()
        if name not in known:
            results.append({"board": name, "error": "unknown board"})
            continue
        results.append(archive_open_on_board(name, known[name], apply=apply))
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "yes": yes,
        "apply": apply,
        "boards": board_names,
        "results": results,
    }
    OUT.mkdir(parents=True, exist_ok=True)
    LAST_CLOSE.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    # notify user of close outcome (terminal)
    lines = [
        f"Completion close {'APPLIED' if apply else 'DRY-RUN'}",
        f"boards: {', '.join(board_names)}",
    ]
    for r in results:
        if r.get("error"):
            lines.append(f"- {r['board']}: ERROR {r['error']}")
        else:
            lines.append(
                f"- {r['board']}: archived={r['archived_n']} "
                f"skipped_running={len(r.get('skipped_running') or [])}"
            )
    text = "\n".join(lines)
    print(text)
    if apply:
        print("telegram:", tg_send(text, disable_notification=False))
    return report


def apply_pending(apply: bool, yes: bool) -> dict[str, Any]:
    if not PENDING.exists():
        raise SystemExit("no completion_pending.json — run scan first")
    data = json.loads(PENDING.read_text())
    names = [c["board"] for c in data.get("candidates_to_confirm_close") or []]
    if not names:
        raise SystemExit("pending has no candidate boards")
    return confirm(names, apply=apply, yes=yes)


def status() -> None:
    print("pending", PENDING, "exists", PENDING.exists())
    if PENDING.exists():
        d = json.loads(PENDING.read_text())
        print(" generated", d.get("generated_at"))
        print(" candidates", d.get("candidates_to_confirm_close"))
    print("last_close", LAST_CLOSE, "exists", LAST_CLOSE.exists())
    if LAST_CLOSE.exists():
        print(LAST_CLOSE.read_text()[:1500])


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_scan = sub.add_parser("scan")
    p_scan.add_argument("--send-tg", action="store_true")

    p_conf = sub.add_parser("confirm")
    p_conf.add_argument("--yes", action="store_true")
    p_conf.add_argument("--apply", action="store_true")
    p_conf.add_argument(
        "--boards",
        required=True,
        help="comma-separated board names, e.g. telemt-proxy or telemt-proxy,default",
    )

    p_ap = sub.add_parser("apply-pending")
    p_ap.add_argument("--yes", action="store_true")
    p_ap.add_argument("--apply", action="store_true")

    sub.add_parser("status")

    args = ap.parse_args()
    if args.cmd == "scan":
        scan(send_tg=bool(args.send_tg))
    elif args.cmd == "confirm":
        names = [x.strip() for x in args.boards.split(",") if x.strip()]
        confirm(names, apply=bool(args.apply), yes=bool(args.yes))
    elif args.cmd == "apply-pending":
        apply_pending(apply=bool(args.apply), yes=bool(args.yes))
    elif args.cmd == "status":
        status()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
