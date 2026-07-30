#!/usr/bin/env python3
"""Hermes orchestration drift watchdog.

Detects accidental config drift against the user-agreed baseline.
Does NOT rewrite config (except optional --restore for known safe flips).
Writes report to /root/main/runtime/hermes_drift_last.json
Optional Telegram alert via BOT token from default profile (only on NEW drift).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml

# Always the multi-profile root. Do NOT use process HERMES_HOME — worker/orch
# sessions set HERMES_HOME to their own profile dir and would false-positive.
ROOT = Path("/root/.hermes")
REPORT = Path("/root/main/runtime/hermes_drift_last.json")
STATE = Path("/root/main/runtime/hermes_drift_state.json")
ALERT_USER = os.environ.get("HERMES_DRIFT_ALERT_USER", "7543801207")

# Agreed baseline (orchestration freeze)
BASELINE = {
    "default": {
        "path": ROOT / "config.yaml",
        "model": "cx/gpt-5.6-sol-xhigh",
        "dispatch_in_gateway": True,
        "default_assignee": "worker",
        "failure_limit": 3,
        "max_turns": 100,
        "child_timeout_seconds": 1200,
        "cli0": "terminal",  # lean, not hermes-cli
    },
    "worker": {
        "path": ROOT / "profiles" / "worker" / "config.yaml",
        "model": "grok-cli/grok-4.5",
        "dispatch_in_gateway": False,
        "default_assignee": "worker",
        "failure_limit": 3,
        "max_turns": 100,
        "child_timeout_seconds": 1200,
        "cli0": "terminal",
    },
    "orchestration1": {
        "path": ROOT / "profiles" / "orchestration1" / "config.yaml",
        "model": "cx/gpt-5.6-sol-xhigh",
        "dispatch_in_gateway": False,
        "default_assignee": "worker",
        "failure_limit": 3,
        "max_turns": 100,
        "child_timeout_seconds": 1200,
        "cli0": "terminal",
    },
    "orchestration2": {
        "path": ROOT / "profiles" / "orchestration2" / "config.yaml",
        "model": "cx/gpt-5.6-sol-xhigh",
        "dispatch_in_gateway": False,
        "default_assignee": "worker",
        "failure_limit": 3,
        "max_turns": 100,
        "child_timeout_seconds": 1200,
        "cli0": "terminal",
    },
    "planner": {
        "path": ROOT / "profiles" / "planner" / "config.yaml",
        "model": "cx/gpt-5.6-sol-xhigh",
        "dispatch_in_gateway": False,
        "default_assignee": "worker",
        "failure_limit": 3,
        "max_turns": 40,
        "child_timeout_seconds": 1200,
        "cli0": "terminal",
    },
    "reviewer": {
        "path": ROOT / "profiles" / "reviewer" / "config.yaml",
        "model": "cx/gpt-5.6-sol-xhigh",
        "dispatch_in_gateway": False,
        "default_assignee": "worker",
        "failure_limit": 3,
        "max_turns": 100,
        "child_timeout_seconds": 1200,
        "cli0": "terminal",
    },
}


def load_cfg(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text()) or {}


def check_profile(name: str, exp: dict[str, Any]) -> list[dict[str, Any]]:
    drifts: list[dict[str, Any]] = []
    path: Path = exp["path"]
    if not path.exists():
        drifts.append({"profile": name, "field": "path", "expected": str(path), "actual": "MISSING"})
        return drifts
    c = load_cfg(path)
    model = (c.get("model") or {}).get("default")
    if model != exp["model"]:
        drifts.append({"profile": name, "field": "model.default", "expected": exp["model"], "actual": model})

    kanban = c.get("kanban") or {}
    if bool(kanban.get("dispatch_in_gateway")) != bool(exp["dispatch_in_gateway"]):
        drifts.append({
            "profile": name,
            "field": "kanban.dispatch_in_gateway",
            "expected": exp["dispatch_in_gateway"],
            "actual": kanban.get("dispatch_in_gateway"),
        })
    
    fl = kanban.get("failure_limit")
    if exp.get("failure_limit") is not None and fl != exp["failure_limit"]:
        drifts.append({
            "profile": name,
            "field": "kanban.failure_limit",
            "expected": exp["failure_limit"],
            "actual": fl,
        })

    if kanban.get("default_assignee") != exp["default_assignee"]:
        drifts.append({
            "profile": name,
            "field": "kanban.default_assignee",
            "expected": exp["default_assignee"],
            "actual": kanban.get("default_assignee"),
        })

    max_turns = (c.get("agent") or {}).get("max_turns")
    if max_turns != exp["max_turns"]:
        drifts.append({
            "profile": name,
            "field": "agent.max_turns",
            "expected": exp["max_turns"],
            "actual": max_turns,
        })

    timeout = (c.get("delegation") or {}).get("child_timeout_seconds")
    if timeout != exp["child_timeout_seconds"]:
        drifts.append({
            "profile": name,
            "field": "delegation.child_timeout_seconds",
            "expected": exp["child_timeout_seconds"],
            "actual": timeout,
        })

    cli = (c.get("platform_toolsets") or {}).get("cli") or []
    cli0 = cli[0] if cli else None
    if exp.get("cli0") and cli0 != exp["cli0"]:
        # hermes-cli blob is the main anti-pattern for default/orch
        if cli0 == "hermes-cli" or (name != "worker" and cli0 != exp["cli0"]):
            drifts.append({
                "profile": name,
                "field": "platform_toolsets.cli[0]",
                "expected": exp["cli0"],
                "actual": cli0,
            })
    return drifts


def fingerprint(drifts: list[dict[str, Any]]) -> str:
    blob = json.dumps(drifts, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def load_token() -> Optional[str]:
    env = ROOT / ".env"
    if not env.exists():
        return None
    for line in env.read_text().splitlines():
        if line.startswith("TELEGRAM_BOT_TOKEN="):
            return line.split("=", 1)[1].strip() or None
    return None


def tg_send(token: str, text: str) -> tuple[bool, str]:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({
        "chat_id": ALERT_USER,
        "text": text,
        "disable_notification": False,
    }).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read().decode()
        return True, body[:200]
    except Exception as e:
        return False, str(e)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--alert", action="store_true", help="Telegram alert on NEW drift only")
    ap.add_argument("--quiet-ok", action="store_true", help="print nothing if clean")
    args = ap.parse_args()

    drifts: list[dict[str, Any]] = []
    for name, exp in BASELINE.items():
        drifts.extend(check_profile(name, exp))

    # Global invariant: only default may dispatch
    dispatchers = []
    for name, exp in BASELINE.items():
        path = exp["path"]
        if not path.exists():
            continue
        c = load_cfg(path)
        if (c.get("kanban") or {}).get("dispatch_in_gateway"):
            dispatchers.append(name)
    if set(dispatchers) != {"default"}:
        drifts.append({
            "profile": "*",
            "field": "dispatchers",
            "expected": ["default"],
            "actual": dispatchers,
        })

    fp = fingerprint(drifts) if drifts else "clean"
    prev = {}
    if STATE.exists():
        try:
            prev = json.loads(STATE.read_text())
        except Exception:
            prev = {}

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "clean": len(drifts) == 0,
        "drift_count": len(drifts),
        "fingerprint": fp,
        "drifts": drifts,
        "baseline_note": "orchestration freeze + lean CLI + timeout=1200 + planner max_turns=40 + failure_limit=3",
    }
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")

    alert_sent = False
    if drifts and args.alert:
        # only alert if fingerprint changed (new/changed drift)
        if prev.get("fingerprint") != fp:
            token = load_token()
            if token:
                lines = ["⚠️ Hermes drift watchdog"]
                for d in drifts[:12]:
                    lines.append(
                        f"• {d['profile']}.{d['field']}: expected={d['expected']} actual={d['actual']}"
                    )
                if len(drifts) > 12:
                    lines.append(f"… +{len(drifts)-12} more")
                ok, msg = tg_send(token, "\n".join(lines))
                alert_sent = ok
                report["alert"] = {"ok": ok, "msg": msg[:200]}
            else:
                report["alert"] = {"ok": False, "msg": "no TELEGRAM_BOT_TOKEN"}
        else:
            report["alert"] = {"ok": False, "msg": "suppressed_same_fingerprint"}

    STATE.write_text(json.dumps({
        "fingerprint": fp,
        "updated_at": report["generated_at"],
        "clean": report["clean"],
        "alert_sent": alert_sent,
    }, indent=2) + "\n")
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")

    if report["clean"]:
        if not args.quiet_ok:
            print("drift_watchdog CLEAN")
        return 0
    print(f"drift_watchdog DRIFT count={len(drifts)} fp={fp}")
    for d in drifts:
        print(f"  {d['profile']}.{d['field']}: expected={d['expected']} actual={d['actual']}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
