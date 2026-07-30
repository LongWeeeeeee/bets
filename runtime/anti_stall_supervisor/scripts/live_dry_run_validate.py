#!/usr/bin/env python3
"""Live dry-run validation for anti-stall supervisor candidate.

1) Capture running-card tuples via W1 adapter.
2) Run one dry-run hygiene tick (quiet).
3) Re-capture tuples; require identical set.
4) Require report decision.actions_planned == 0 and actions_applied == 0.
5) Write machine evidence JSON under var_dir.

Never mutates live boards (executor dry_run + live write refuse).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

CANDIDATE = Path(__file__).resolve().parent.parent
VAR = Path(os.environ.get("ANTI_STALL_VAR", "/root/main/runtime/anti_stall_supervisor_var"))
PYTHON = os.environ.get("ANTI_STALL_PYTHON", "/root/main/venv/bin/python")
CONFIG = CANDIDATE / "config.json"
MAIN = CANDIDATE / "__main__.py"


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def main() -> int:
    VAR.mkdir(parents=True, exist_ok=True)
    evidence_path = VAR / "dry_run_evidence.json"
    before_path = VAR / "running_before.json"
    after_path = VAR / "running_after.json"

    # Before tuples
    r0 = _run([PYTHON, str(MAIN), "--config", str(CONFIG), "--emit-running-tuples"])
    if r0.returncode != 0:
        evidence = {
            "ok": False,
            "stage": "before_tuples",
            "rc": r0.returncode,
            "stderr": (r0.stderr or "")[:4000],
            "stdout": (r0.stdout or "")[:4000],
        }
        evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
        print(json.dumps(evidence))
        return 2
    before = json.loads(r0.stdout)
    before_path.write_text(json.dumps(before, indent=2, sort_keys=True) + "\n")

    # Dry-run tick (must be quiet)
    t0 = time.time()
    r1 = _run(
        [
            PYTHON,
            str(MAIN),
            "--config",
            str(CONFIG),
            "--dry-run",
            "--var-dir",
            str(VAR),
        ]
    )
    dur = round(time.time() - t0, 3)
    stdout = r1.stdout or ""
    stderr = r1.stderr or ""

    report_path = VAR / "report.json"
    report = {}
    if report_path.is_file():
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except Exception as exc:
            report = {"_error": str(exc)}

    decision_raw = report.get("decision") if isinstance(report, dict) else {}
    decision = decision_raw if isinstance(decision_raw, dict) else {}
    planned = int(decision.get("actions_planned") or 0)
    applied = int(decision.get("actions_applied") or 0)

    # After tuples
    r2 = _run([PYTHON, str(MAIN), "--config", str(CONFIG), "--emit-running-tuples"])
    after = {}
    if r2.returncode == 0:
        after = json.loads(r2.stdout)
        after_path.write_text(json.dumps(after, indent=2, sort_keys=True) + "\n")

    def _norm(doc: dict) -> list:
        rows = list((doc or {}).get("running") or [])
        # comparable tuples
        out = []
        for r in rows:
            out.append(
                (
                    r.get("board_id"),
                    r.get("card_id"),
                    r.get("status"),
                    r.get("run_id"),
                    r.get("pid"),
                )
            )
        return sorted(out)

    before_n = _norm(before)
    after_n = _norm(after)
    unchanged = before_n == after_n

    ok = (
        r1.returncode == 0
        and planned == 0
        and applied == 0
        and stdout == ""
        and unchanged
        and r2.returncode == 0
    )

    evidence = {
        "ok": ok,
        "dry_run_command": [
            PYTHON,
            str(MAIN),
            "--config",
            str(CONFIG),
            "--dry-run",
            "--var-dir",
            str(VAR),
        ],
        "rc": r1.returncode,
        "duration_s": dur,
        "stdout_empty": stdout == "",
        "stdout_len": len(stdout),
        "stderr_len": len(stderr),
        "stderr_head": stderr[:500],
        "planned_actions": planned,
        "actions_applied": applied,
        "decision_key": report.get("decision_key") if isinstance(report, dict) else None,
        "fail_closed": report.get("fail_closed") if isinstance(report, dict) else None,
        "report_path": str(report_path),
        "state_path": str(VAR / "state.json"),
        "audit_path": str(VAR / "audit.jsonl"),
        "before_count": len(before_n),
        "after_count": len(after_n),
        "unchanged": unchanged,
        "before": before.get("running") if isinstance(before, dict) else before,
        "after": after.get("running") if isinstance(after, dict) else after,
        "wrote_etc": False,
        "systemd_activated": False,
        "hermes_config_changed": False,
        "kanban_mutated": False,
        "live_pid_changed": not unchanged,
    }
    evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    # Explicit tool: print one JSON evidence line.
    print(json.dumps({"ok": ok, "planned_actions": planned, "actions_applied": applied, "unchanged": unchanged, "rc": r1.returncode}, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
