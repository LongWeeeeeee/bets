#!/usr/bin/env python3
"""Winline live shadow watchdog — deterministic no_agent cron script.

Semantics (Hermes no_agent cron):
  - empty stdout  => SILENT (healthy, nothing delivered)
  - stdout text   => delivered verbatim as the alert
  - non-zero exit => error alert (use only for watchdog-internal failures)

Alert conditions (anything else stays silent):
  - cyberscore.service not active, MainPID=0, or NRestarts>0 (crash loop)
  - cmdline missing `--dltv-source sourcetv` or `--no-odds`
  - more than one cyberscore_try.py producer process
  - runtime/winline-shadow/latest.json exists but is invalid JSON
  - latest.json producer_pid != current cyberscore MainPID

Absence of latest.json / STAR / markets is NOT an alert.
"""
import json
import subprocess
import sys
from pathlib import Path

LATEST = Path("/root/main/runtime/winline-shadow/latest.json")
SERVICE = "cyberscore.service"


def sh(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=20)


def main() -> int:
    problems: list[str] = []
    pid = 0

    r = sh(["systemctl", "is-active", SERVICE])
    active = (r.stdout or "").strip()
    if active != "active":
        problems.append(f"{SERVICE} is-active={active!r} (expected active)")
    else:
        r2 = sh(["systemctl", "show", SERVICE, "-p", "MainPID,NRestarts"])
        props = dict(
            line.split("=", 1) for line in (r2.stdout or "").splitlines() if "=" in line
        )
        try:
            pid = int(props.get("MainPID") or 0)
        except ValueError:
            pid = 0
        nrestarts = props.get("NRestarts", "?")
        if pid == 0:
            problems.append(f"{SERVICE} MainPID=0 while active")
        if nrestarts not in ("0",):
            problems.append(f"{SERVICE} NRestarts={nrestarts} (crash-restart loop)")
        cmdline_path = Path(f"/proc/{pid}/cmdline")
        if pid and cmdline_path.exists():
            cmdline = cmdline_path.read_bytes().replace(b"\0", b" ").decode(
                "utf-8", "replace"
            )
            for frag in ("--dltv-source", "sourcetv", "--no-odds"):
                if frag not in cmdline:
                    problems.append(
                        f"{SERVICE} argv missing {frag!r}: {cmdline[:200]}"
                    )

    # exactly one producer (pgrep excludes itself; match full pattern)
    r3 = sh(["pgrep", "-f", "base/cyberscore_try.py"])
    prod_pids = sorted({p.strip() for p in (r3.stdout or "").splitlines() if p.strip()})
    if len(prod_pids) > 1:
        problems.append(f"multiple cyberscore_try.py producers: {prod_pids}")

    # shadow latest.json: absence is fine, invalid/mismatched is not
    if LATEST.exists():
        try:
            data = json.loads(LATEST.read_text())
            pp = data.get("producer_pid")
            if pp and pid and int(pp) != pid:
                problems.append(
                    f"latest.json producer_pid={pp} != current MainPID={pid}"
                )
        except Exception as e:
            problems.append(f"latest.json unreadable/invalid: {e}")

    if problems:
        print("WINLINE SHADOW ALERT")
        for p in problems:
            print("-", p)
    return 0


if __name__ == "__main__":
    sys.exit(main())
