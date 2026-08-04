#!/usr/bin/env python3
"""Winline REPLAN6 observer watchdog — deterministic no_agent cron script.

Watches the bounded observer run under
  /root/main/runtime/winline_current_map_polling_replan6/staging/observe/
plus production service identity. Read-only: never restarts the observer,
the service, or launches browsers/parsers; never touches kanban cards.

Semantics (Hermes no_agent cron):
  - empty stdout  => SILENT
  - stdout text   => delivered verbatim as the alert
  - non-zero exit => error alert (watchdog-internal failure only)

State machine:
  - observer process alive + PROGRESS.json fresh  => healthy run, SILENT
  - observer alive + PROGRESS stale >10m          => ALERT (hung observer)
  - observer alive past its 100-min ceiling        => ALERT (ceiling breach)
  - observer gone + TERMINAL present               => terminal state, SILENT
  - observer gone + no TERMINAL                    => ALERT (no terminal evidence)
  - service identity violations                    => ALERT (same rules as
                                                      winline_shadow_watchdog)
"""
import json
import subprocess
import sys
import time
from pathlib import Path

OBS_DIR = Path("/root/main/runtime/winline_current_map_polling_replan6/staging/observe")
PROGRESS = OBS_DIR / "PROGRESS.json"
TERMINAL = OBS_DIR / "TERMINAL"
STATUS = OBS_DIR / "STATUS.json"
OBS_PATTERN = "observe_live_r6.py"
OBS_CEILING_SECONDS = 100 * 60
PROGRESS_STALE_SECONDS = 10 * 60
SERVICE = "cyberscore.service"


def sh(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=20)


def service_problems() -> list[str]:
    problems: list[str] = []
    r = sh(["systemctl", "is-active", SERVICE])
    active = (r.stdout or "").strip()
    if active != "active":
        return [f"{SERVICE} is-active={active!r} (expected active)"]
    r2 = sh(["systemctl", "show", SERVICE, "-p", "MainPID,NRestarts"])
    props = dict(
        line.split("=", 1) for line in (r2.stdout or "").splitlines() if "=" in line
    )
    try:
        pid = int(props.get("MainPID") or 0)
    except ValueError:
        pid = 0
    if pid == 0:
        problems.append(f"{SERVICE} MainPID=0 while active")
    if props.get("NRestarts", "?") not in ("0",):
        problems.append(f"{SERVICE} NRestarts={props.get('NRestarts')} (crash loop)")
    cmdline_path = Path(f"/proc/{pid}/cmdline")
    if pid and cmdline_path.exists():
        cmdline = cmdline_path.read_bytes().replace(b"\0", b" ").decode(
            "utf-8", "replace"
        )
        for frag in ("--dltv-source", "sourcetv", "--no-odds"):
            if frag not in cmdline:
                problems.append(f"{SERVICE} argv missing {frag!r}: {cmdline[:200]}")
    r3 = sh(["pgrep", "-f", "base/cyberscore_try.py"])
    prod = sorted({p.strip() for p in (r3.stdout or "").splitlines() if p.strip()})
    if len(prod) > 1:
        problems.append(f"multiple cyberscore_try.py producers: {prod}")
    return problems


def observer_pids() -> list[int]:
    r = sh(["pgrep", "-f", OBS_PATTERN])
    out: list[int] = []
    for line in (r.stdout or "").splitlines():
        line = line.strip()
        if line.isdigit():
            out.append(int(line))
    return out


def main() -> int:
    problems = service_problems()
    now = time.time()

    pids = observer_pids()
    if pids:
        # active run: freshness via PROGRESS.json mtime, age via oldest process etime
        if PROGRESS.exists():
            stale = now - PROGRESS.stat().st_mtime
            if stale > PROGRESS_STALE_SECONDS:
                problems.append(
                    f"observer pids={pids} alive but PROGRESS.json stale {int(stale)}s "
                    f"> {PROGRESS_STALE_SECONDS}s (hung observer?)"
                )
        else:
            problems.append(f"observer pids={pids} alive but no PROGRESS.json yet")
        r = sh(["ps", "-o", "etimes=", "-p", ",".join(map(str, pids))])
        etimes = [int(x) for x in (r.stdout or "").split() if x.strip().isdigit()]
        if etimes and max(etimes) > OBS_CEILING_SECONDS:
            problems.append(
                f"observer runtime {max(etimes)}s exceeds ceiling {OBS_CEILING_SECONDS}s "
                "without exiting (do not restart blindly; inspect logs/)"
            )
    else:
        if TERMINAL.exists():
            # terminal state: nothing to watch; surface verdict change only via
            # explicit human request, stay silent here
            pass
        elif OBS_DIR.exists():
            problems.append(
                "observer exited without TERMINAL marker; inspect "
                f"{OBS_DIR}/logs and STATUS.json before any re-run"
            )
        # OBS_DIR missing entirely => feature not deployed here, silent

    if problems:
        print("WINLINE REPLAN6 ALERT")
        for p in problems:
            print("-", p)
        if STATUS.exists():
            try:
                s = json.loads(STATUS.read_text())
                print(f"last STATUS verdict={s.get('verdict')} terminal={s.get('terminal')}")
            except Exception:
                pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
