#!/usr/bin/env python3
"""Run hygiene and read-only watchdog independently for all commander lanes."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LANES = ("default", "orchestration1", "orchestration2")


def run(script: str, lane: str) -> int:
    env = os.environ.copy()
    env["HERMES_ORCHESTRATION_LANE"] = lane
    proc = subprocess.run(
        [sys.executable, str(ROOT / script)],
        env=env,
        text=True,
        capture_output=True,
        timeout=360,
    )
    out = ((proc.stdout or "") + (proc.stderr or "")).strip()
    print(f"[{lane}] {script} rc={proc.returncode}")
    if out:
        print(out[-3000:])
    return proc.returncode


def main() -> int:
    failures = 0
    for lane in LANES:
        failures += int(run("kanban_hygiene_guard.py", lane) != 0)
        failures += int(run("orch_watchdog.py", lane) != 0)
    # One deterministic tick advances only explicit PWR_WORKFLOW_ID graphs.
    # It creates no recursive schedules and records all transitions durably.
    proc = subprocess.run(
        [sys.executable, str(ROOT / "pwr_controller.py"), "tick", "--lane", "all"],
        text=True,
        capture_output=True,
        timeout=360,
    )
    out = ((proc.stdout or "") + (proc.stderr or "")).strip()
    print(f"[all] pwr_controller.py rc={proc.returncode}")
    if out:
        print(out[-5000:])
    failures += int(proc.returncode != 0)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
