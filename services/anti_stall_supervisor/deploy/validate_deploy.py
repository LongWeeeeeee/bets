#!/usr/bin/env python3
"""W5 deploy validator for hermes-anti-stall-supervisor (staging only).

Implements fingerprint / dry-run / unit / post-enable gates used by INT and the
post-APPROVE deploy card. This module never installs, enables, starts, stops, or
edits live systemd units when invoked as --self-test or library import.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Optional, Sequence

SCHEMA_VERSION = "live_fingerprint_v1"
REPORT_SCHEMA_VERSION = "supervisor_report_v1"
STATE_SCHEMA_VERSION = "state_v1"

# Status-changing action kinds (executor/decision contract). Diagnostic/comment-only
# kinds are NOT status-changing per Planner correction on t_23457e9d.
STATUS_CHANGING_ACTIONS = frozenset(
    {
        "unblock",
        "auto_unblock",
        "retry",
        "requeue",
        "reclaim",
        "complete",
        "kanban_complete",
        "block",
        "kanban_block",
        "route_needs_replan",
        "needs_replan",
        "fail",
        "cancel",
        "archive",
        "set_status",
        "change_status",
        "spawn",
        "dispatch",
        "create_card",
        "duplicate",
        "reset_attempts",
        "clear_block",
        "force_ready",
        "promote",
        "demote",
        "kill_worker",
        "signal_worker",
        "touch_run",
        "advance_run",
        "mutate",
        "write_status",
    }
)
DIAGNOSTIC_ONLY_ACTIONS = frozenset(
    {
        "diagnostic",
        "diagnose",
        "comment",
        "comment_once",
        "comment_only",
        "noop",
        "log",
        "observe",
        "record_audit",
        "note",
    }
)

REQUIRED_SERVICE_DIRECTIVES = {
    "Type": "oneshot",
    "WorkingDirectory": "/root/main",
    "ExecStart": (
        "/root/main/venv/bin/python -m services.anti_stall_supervisor "
        "--config /root/main/services/anti_stall_supervisor/config.json"
    ),
    "Restart": "no",
    "NoNewPrivileges": "yes",
    "PrivateTmp": "yes",
    "PrivateDevices": "yes",
    "ProtectSystem": "strict",
    "ProtectHome": "read-only",
    "RestrictAddressFamilies": "AF_UNIX",
}

REQUIRED_TIMER_DIRECTIVES = {
    "OnCalendar": "*:0/5",
    "Persistent": "true",
    "Unit": "hermes-anti-stall-supervisor.service",
}

FORBIDDEN_UNIT_PATTERNS = [
    re.compile(r"(?im)^\s*Restart\s*=\s*always\b"),
    re.compile(r"(?im)^\s*Restart\s*=\s*on-failure\b"),
    re.compile(r"(?im)^\s*RandomizedDelaySec\s*="),
    re.compile(r"(?im)^\s*RestrictAddressFamilies\s*=\s*.*\bAF_INET\b"),
    re.compile(r"(?im)^\s*RestrictAddressFamilies\s*=\s*.*\bAF_INET6\b"),
    re.compile(r"(?im)^\s*ExecStart\s*=.*/bin/(rm|dd|mkfs)\b"),
    re.compile(r"(?im)^\s*PermissionStartOnly\s*="),  # obsolete footgun marker
]


class ValidationError(Exception):
    """Fail-closed validation failure with machine-readable payload."""

    def __init__(self, message: str, *, details: Optional[Mapping[str, Any]] = None):
        super().__init__(message)
        self.details = dict(details or {})


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _is_secretish_path(path: str) -> bool:
    low = path.lower()
    needles = (
        "/.env",
        "credentials",
        "secret",
        "api_key",
        "apikey",
        "keys.py",
        "auth.json",
        "id_rsa",
        ".pem",
    )
    return any(n in low for n in needles)


def discover_board_dbs(hermes_root: Path) -> list[Path]:
    """Discover kanban board DBs under a Hermes root (live or fixture)."""
    root = Path(hermes_root).resolve()
    found: list[Path] = []
    candidates = [
        root / "kanban" / "boards",
        root / "kanban.db",
        root / "boards",
    ]
    # Common layouts: ~/.hermes/kanban/boards/<board>/kanban.db and ~/.hermes/kanban.db
    boards_dir = root / "kanban" / "boards"
    if boards_dir.is_dir():
        for child in sorted(boards_dir.iterdir()):
            db = child / "kanban.db"
            if db.is_file():
                found.append(db.resolve())
    legacy = root / "kanban.db"
    if legacy.is_file():
        found.append(legacy.resolve())
    # Dedup preserve order
    seen: set[str] = set()
    out: list[Path] = []
    for p in found:
        key = str(p)
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return {r[1] for r in rows}


def _safe_query_tasks(con: sqlite3.Connection) -> list[dict[str, Any]]:
    cols = _table_columns(con, "tasks")
    if not cols or "id" not in cols:
        return []
    want = [
        "id",
        "status",
        "current_run_id",
        "worker_pid",
        "started_at",
        "last_heartbeat_at",
        "claim_lock",
        "assignee",
        "block_kind",
    ]
    select = [c for c in want if c in cols]
    # Non-archived only when status column exists
    sql = f"SELECT {', '.join(select)} FROM tasks"
    if "status" in cols:
        sql += " WHERE status IS NULL OR status != 'archived'"
    sql += " ORDER BY id"
    out: list[dict[str, Any]] = []
    for row in con.execute(sql):
        out.append({select[i]: row[i] for i in range(len(select))})
    return out


def _safe_query_run(
    con: sqlite3.Connection, run_id: Any
) -> Optional[dict[str, Any]]:
    if run_id is None:
        return None
    cols = _table_columns(con, "task_runs")
    if not cols or "id" not in cols:
        return None
    want = [
        "id",
        "task_id",
        "worker_pid",
        "started_at",
        "status",
        "outcome",
        "last_heartbeat_at",
        "claim_lock",
    ]
    select = [c for c in want if c in cols]
    row = con.execute(
        f"SELECT {', '.join(select)} FROM task_runs WHERE id = ?", (run_id,)
    ).fetchone()
    if not row:
        return None
    return {select[i]: row[i] for i in range(len(select))}


def _pid_start_ticks(pid: Optional[int]) -> Optional[str]:
    """Return process start ticks from /proc/<pid>/stat field 22, or None."""
    if pid is None:
        return None
    try:
        pid_i = int(pid)
    except (TypeError, ValueError):
        return None
    if pid_i <= 0:
        return None
    stat_path = Path(f"/proc/{pid_i}/stat")
    try:
        text = stat_path.read_text(encoding="utf-8", errors="replace")
    except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
        return None
    # comm may contain spaces/parens — split after last ')'
    try:
        rparen = text.rfind(")")
        if rparen < 0:
            return None
        fields = text[rparen + 2 :].split()
        # fields[0] is state; starttime is field index 19 in this slice (man proc 22nd)
        # After comm: state(1) ppid(2) ... starttime is 20th field overall => index 19 in full after pid/comm
        # full: pid + (comm) + rest; rest index 19 = starttime
        starttime = fields[19]
        return str(starttime)
    except (IndexError, ValueError):
        return None


def capture_live_fingerprint(hermes_root: Path | str) -> dict[str, Any]:
    """Fingerprint every non-archived task: id/status/current_run_id + run PID/start ticks.

    Read-only. Never mutates boards. Suitable for before/after deploy comparison.
    """
    root = Path(hermes_root)
    boards = discover_board_dbs(root)
    board_payloads: list[dict[str, Any]] = []
    for db_path in boards:
        entry: dict[str, Any] = {
            "db_path": str(db_path),
            "ok": False,
            "tasks": [],
            "diagnostic": None,
        }
        uri = db_path.resolve().as_uri() + "?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True, timeout=5.0)
            try:
                con.execute("PRAGMA query_only=ON")
                tasks_out: list[dict[str, Any]] = []
                for t in _safe_query_tasks(con):
                    run = None
                    run_id = t.get("current_run_id")
                    if run_id is not None:
                        run = _safe_query_run(con, run_id)
                    pid = None
                    if run and run.get("worker_pid") is not None:
                        pid = run.get("worker_pid")
                    elif t.get("worker_pid") is not None:
                        pid = t.get("worker_pid")
                    start_ticks = _pid_start_ticks(pid)
                    run_started = None if not run else run.get("started_at")
                    tasks_out.append(
                        {
                            "task_id": t.get("id"),
                            "status": t.get("status"),
                            "current_run_id": run_id,
                            "run_worker_pid": pid,
                            "run_started_at": run_started,
                            "pid_start_ticks": start_ticks,
                            "task_started_at": t.get("started_at"),
                            "last_heartbeat_at": t.get("last_heartbeat_at"),
                        }
                    )
                entry["tasks"] = tasks_out
                entry["ok"] = True
            finally:
                con.close()
        except sqlite3.Error as exc:
            entry["diagnostic"] = f"sqlite_error:{type(exc).__name__}:{exc}"
        except OSError as exc:
            entry["diagnostic"] = f"os_error:{type(exc).__name__}:{exc}"
        board_payloads.append(entry)

    payload = {
        "schema": SCHEMA_VERSION,
        "hermes_root": str(root),
        "captured_at_unix": time.time(),
        "boards": board_payloads,
    }
    payload["fingerprint_sha256"] = hashlib.sha256(
        _canonical_json(
            {
                "boards": [
                    {
                        "db_path": b["db_path"],
                        "tasks": [
                            {
                                "task_id": t["task_id"],
                                "status": t["status"],
                                "current_run_id": t["current_run_id"],
                                "run_worker_pid": t["run_worker_pid"],
                                "pid_start_ticks": t["pid_start_ticks"],
                                "run_started_at": t["run_started_at"],
                            }
                            for t in b.get("tasks") or []
                        ],
                    }
                    for b in board_payloads
                    if b.get("ok")
                ]
            }
        ).encode("utf-8")
    ).hexdigest()
    return payload


def _iter_planned_actions(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Normalize planned actions from a supervisor dry-run report."""
    actions: list[dict[str, Any]] = []
    if not isinstance(report, Mapping):
        return actions
    # Common keys
    for key in ("planned_actions", "actions", "plan_actions"):
        raw = report.get(key)
        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, Mapping):
                    actions.append(dict(item))
                elif isinstance(item, str):
                    actions.append({"kind": item})
    plan = report.get("plan") or report.get("decision_plan") or report.get("decision_plan_v1")
    if isinstance(plan, Mapping):
        for key in ("actions", "planned_actions", "items"):
            raw = plan.get(key)
            if isinstance(raw, list):
                for item in raw:
                    if isinstance(item, Mapping):
                        actions.append(dict(item))
    per_task = report.get("per_task") or report.get("tasks") or report.get("task_plans")
    if isinstance(per_task, list):
        for item in per_task:
            if not isinstance(item, Mapping):
                continue
            tid = item.get("task_id") or item.get("id")
            nested = item.get("planned_actions") or item.get("actions") or []
            if isinstance(nested, list):
                for a in nested:
                    if isinstance(a, Mapping):
                        d = dict(a)
                        d.setdefault("task_id", tid)
                        actions.append(d)
                    elif isinstance(a, str):
                        actions.append({"kind": a, "task_id": tid})
            count = item.get("planned_actions_count")
            if isinstance(count, int) and count > 0 and not nested:
                actions.append(
                    {
                        "kind": item.get("action_kind") or item.get("kind") or "mutate",
                        "task_id": tid,
                        "planned_actions_count": count,
                    }
                )
    return actions


def _action_kind(action: Mapping[str, Any]) -> str:
    for k in ("kind", "action", "type", "name", "op"):
        v = action.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip().lower()
    return ""


def _is_status_changing(action: Mapping[str, Any]) -> bool:
    kind = _action_kind(action)
    if not kind:
        # Unknown non-empty action object counts as status-changing (fail closed)
        # unless explicitly marked diagnostic
        if action.get("diagnostic_only") is True or action.get("status_changing") is False:
            return False
        if action.get("status_changing") is True:
            return True
        # planned_actions_count without kind
        if int(action.get("planned_actions_count") or 0) > 0:
            return True
        return True
    if kind in DIAGNOSTIC_ONLY_ACTIONS:
        return False
    if kind in STATUS_CHANGING_ACTIONS:
        return True
    if action.get("diagnostic_only") is True or action.get("status_changing") is False:
        return False
    # Unknown kind: fail closed as status-changing
    return True


def _running_task_ids(baseline: Mapping[str, Any]) -> set[str]:
    ids: set[str] = set()
    for board in baseline.get("boards") or []:
        if not isinstance(board, Mapping):
            continue
        for t in board.get("tasks") or []:
            if not isinstance(t, Mapping):
                continue
            if str(t.get("status") or "").lower() == "running":
                tid = t.get("task_id")
                if tid is not None:
                    ids.add(str(tid))
    # Also allow explicit list on baseline
    extra = baseline.get("running_task_ids")
    if isinstance(extra, list):
        ids.update(str(x) for x in extra)
    return ids


def validate_dry_run(
    report: Mapping[str, Any], baseline: Mapping[str, Any]
) -> dict[str, Any]:
    """Validate pre-enable dry-run report against live fingerprint baseline.

    Requirements (Planner correction authoritative):
    - planned_actions=0 for every currently running card
    - no status-changing action that would alter any pre-existing task
    - diagnostic/comment-only plans on non-running cards may be reported and must
      not be miscounted as status-changing actions
    """
    if not isinstance(report, Mapping):
        raise ValidationError("report_not_mapping")
    if not isinstance(baseline, Mapping):
        raise ValidationError("baseline_not_mapping")

    errors: list[str] = []
    actions = _iter_planned_actions(report)
    running_ids = _running_task_ids(baseline)

    # Index pre-existing task ids
    preexisting: set[str] = set()
    for board in baseline.get("boards") or []:
        for t in (board or {}).get("tasks") or []:
            if t.get("task_id") is not None:
                preexisting.add(str(t["task_id"]))

    per_running_counts: dict[str, int] = {rid: 0 for rid in running_ids}
    status_changing: list[dict[str, Any]] = []
    diagnostic_only: list[dict[str, Any]] = []

    for action in actions:
        tid = action.get("task_id") or action.get("id")
        tid_s = str(tid) if tid is not None else None
        changing = _is_status_changing(action)
        if not changing:
            diagnostic_only.append(action)
            continue
        status_changing.append(action)
        if tid_s and tid_s in running_ids:
            per_running_counts[tid_s] = per_running_counts.get(tid_s, 0) + 1
        # Any status-changing action on a pre-existing task is forbidden pre-enable
        if tid_s and tid_s in preexisting:
            errors.append(f"status_changing_on_preexisting:{tid_s}:{_action_kind(action)}")
        elif tid_s is None:
            errors.append(f"status_changing_missing_task_id:{_action_kind(action)}")
        else:
            # Action targeting unknown/new id still blocked pre-enable (no create)
            errors.append(f"status_changing_on_nonbaseline:{tid_s}:{_action_kind(action)}")

    for rid, count in sorted(per_running_counts.items()):
        if count != 0:
            errors.append(f"running_card_planned_actions_nonzero:{rid}:{count}")

    # Explicit per-task map in report if present
    per_task = report.get("per_task") or report.get("task_plans")
    if isinstance(per_task, list):
        for item in per_task:
            if not isinstance(item, Mapping):
                continue
            tid = str(item.get("task_id") or item.get("id") or "")
            if tid in running_ids:
                pac = item.get("planned_actions")
                if isinstance(pac, list):
                    # count only status-changing
                    n = sum(1 for a in pac if isinstance(a, Mapping) and _is_status_changing(a))
                    n += sum(1 for a in pac if isinstance(a, str) and a.lower() not in DIAGNOSTIC_ONLY_ACTIONS)
                    if n != 0:
                        errors.append(f"running_card_planned_actions_nonzero:{tid}:{n}")
                elif isinstance(item.get("planned_actions_count"), int):
                    # If report gives a raw count, require 0 for running
                    if item["planned_actions_count"] != 0:
                        # unless all nested are diagnostic — already handled; raw count must be 0
                        errors.append(
                            f"running_card_planned_actions_nonzero:{tid}:{item['planned_actions_count']}"
                        )

    # Overall mutating planned actions must be zero pre-enable
    if status_changing:
        errors.append(f"mutating_planned_actions_nonzero:{len(status_changing)}")

    ok = not errors
    result = {
        "ok": ok,
        "errors": errors,
        "running_task_ids": sorted(running_ids),
        "status_changing_count": len(status_changing),
        "diagnostic_only_count": len(diagnostic_only),
        "per_running_status_changing": per_running_counts,
    }
    if not ok:
        raise ValidationError("dry_run_validation_failed", details=result)
    return result


def compare_live_fingerprint(
    before: Mapping[str, Any], after: Mapping[str, Any]
) -> dict[str, Any]:
    """Require no pre-existing task status/run-id/PID/start-tick change."""
    if not isinstance(before, Mapping) or not isinstance(after, Mapping):
        raise ValidationError("fingerprint_not_mapping")

    def index(fp: Mapping[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
        out: dict[tuple[str, str], dict[str, Any]] = {}
        for board in fp.get("boards") or []:
            if not isinstance(board, Mapping) or not board.get("ok"):
                continue
            dbp = str(board.get("db_path") or "")
            for t in board.get("tasks") or []:
                if not isinstance(t, Mapping):
                    continue
                tid = str(t.get("task_id") or "")
                out[(dbp, tid)] = {
                    "status": t.get("status"),
                    "current_run_id": t.get("current_run_id"),
                    "run_worker_pid": t.get("run_worker_pid"),
                    "pid_start_ticks": t.get("pid_start_ticks"),
                    "run_started_at": t.get("run_started_at"),
                }
        return out

    b = index(before)
    a = index(after)
    changes: list[dict[str, Any]] = []
    missing_after: list[str] = []
    for key, bval in b.items():
        aval = a.get(key)
        if aval is None:
            missing_after.append(f"{key[0]}::{key[1]}")
            changes.append({"task": key[1], "db": key[0], "change": "missing_after"})
            continue
        for field in (
            "status",
            "current_run_id",
            "run_worker_pid",
            "pid_start_ticks",
            "run_started_at",
        ):
            if bval.get(field) != aval.get(field):
                changes.append(
                    {
                        "task": key[1],
                        "db": key[0],
                        "field": field,
                        "before": bval.get(field),
                        "after": aval.get(field),
                    }
                )

    ok = not changes
    result = {
        "ok": ok,
        "preexisting_tasks": len(b),
        "changes": changes,
        "missing_after": missing_after,
        "before_sha256": before.get("fingerprint_sha256"),
        "after_sha256": after.get("fingerprint_sha256"),
    }
    if not ok:
        raise ValidationError("fingerprint_delta_nonzero", details=result)
    return result


def _parse_unit_file(text: str) -> dict[str, dict[str, str]]:
    """Minimal systemd unit parser (last assignment wins per key in section)."""
    sections: dict[str, dict[str, str]] = {}
    current = None
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith(";"):
            continue
        if line.startswith("[") and line.endswith("]"):
            current = line[1:-1]
            sections.setdefault(current, {})
            continue
        if current is None or "=" not in line:
            continue
        key, _, val = line.partition("=")
        sections[current][key.strip()] = val.strip()
    return sections


def validate_units(paths: Sequence[Path | str]) -> dict[str, Any]:
    """Validate staged unit files for required hardening and forbidden directives."""
    path_list = [Path(p) for p in paths]
    errors: list[str] = []
    warnings: list[str] = []
    details: dict[str, Any] = {}

    service_path = None
    timer_path = None
    for p in path_list:
        name = p.name
        if name.endswith(".service"):
            service_path = p
        elif name.endswith(".timer"):
            timer_path = p

    if service_path is None:
        errors.append("missing_service_unit")
    if timer_path is None:
        errors.append("missing_timer_unit")

    def check_file(p: Optional[Path], kind: str) -> Optional[dict[str, dict[str, str]]]:
        if p is None:
            return None
        if not p.is_file():
            errors.append(f"not_a_file:{p}")
            return None
        text = p.read_text(encoding="utf-8")
        if _is_secretish_path(str(p)):
            warnings.append(f"secretish_path_name:{p}")
        # No embedded secrets (heuristic)
        for pat in (
            r"(?i)api[_-]?key\s*=\s*\S+",
            r"(?i)password\s*=\s*\S+",
            r"-----BEGIN [A-Z ]*PRIVATE KEY-----",
            r"(?i)token\s*=\s*[A-Za-z0-9_\-]{20,}",
        ):
            if re.search(pat, text):
                errors.append(f"possible_secret_in_unit:{p.name}")
        for cre in FORBIDDEN_UNIT_PATTERNS:
            if cre.search(text):
                errors.append(f"forbidden_directive:{p.name}:{cre.pattern}")
        parsed = _parse_unit_file(text)
        details[str(p)] = {
            "sha256": _sha256_file(p),
            "sections": {k: dict(v) for k, v in parsed.items()},
        }
        return parsed

    svc = check_file(service_path, "service")
    tmr = check_file(timer_path, "timer")

    if svc is not None:
        section = svc.get("Service") or {}
        for key, expected in REQUIRED_SERVICE_DIRECTIVES.items():
            got = section.get(key)
            if got is None:
                errors.append(f"service_missing:{key}")
            elif key == "ExecStart":
                # Allow minor whitespace normalization
                if " ".join(got.split()) != " ".join(expected.split()):
                    errors.append(f"service_mismatch:{key}:{got!r}")
            else:
                if got.lower() != expected.lower() and got != expected:
                    # Restart=no exact; others case-insensitive where safe
                    if key in {"Type", "Restart"} and got.lower() == expected.lower():
                        pass
                    elif got != expected:
                        errors.append(f"service_mismatch:{key}:{got!r}!={expected!r}")
        # TimeoutStartSec must be <= 240
        ts = section.get("TimeoutStartSec")
        if ts is None:
            errors.append("service_missing:TimeoutStartSec")
        else:
            try:
                # support "240" or "240s"
                n = int(str(ts).rstrip("sS"))
                if n <= 0 or n > 240:
                    errors.append(f"service_timeout_out_of_bounds:{ts}")
            except ValueError:
                errors.append(f"service_timeout_unparseable:{ts}")
        # ReadWritePaths must include required paths (order-independent)
        rwp = section.get("ReadWritePaths") or ""
        parts = rwp.split()
        for req in (
            "/root/.hermes/kanban/boards",
            "/root/main/runtime",
        ):
            if req not in parts:
                errors.append(f"service_missing_readwritepath:{req}")
        # Hardening extras recommended
        for key in (
            "ProtectKernelTunables",
            "ProtectKernelModules",
            "ProtectControlGroups",
            "RestrictSUIDSGID",
            "LockPersonality",
        ):
            if key not in section:
                warnings.append(f"service_recommended_missing:{key}")

    if tmr is not None:
        section = tmr.get("Timer") or {}
        for key, expected in REQUIRED_TIMER_DIRECTIVES.items():
            got = section.get(key)
            if got is None:
                errors.append(f"timer_missing:{key}")
            elif got != expected and got.lower() != expected.lower():
                # OnCalendar must be exact
                errors.append(f"timer_mismatch:{key}:{got!r}!={expected!r}")
        if "RandomizedDelaySec" in section:
            errors.append("timer_has_randomized_delay")
        # AccuracySec optional but if huge warn
        acc = section.get("AccuracySec")
        if acc:
            try:
                n = int(str(acc).rstrip("sS"))
                if n > 60:
                    warnings.append(f"timer_accuracy_loose:{acc}")
            except ValueError:
                warnings.append(f"timer_accuracy_unparseable:{acc}")

    # Optional: systemd-analyze verify if available (does not install)
    analyze_out = None
    analyze_rc = None
    if service_path and timer_path and service_path.is_file() and timer_path.is_file():
        try:
            proc = subprocess.run(
                ["systemd-analyze", "verify", str(service_path), str(timer_path)],
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            analyze_rc = proc.returncode
            analyze_out = {
                "stdout": (proc.stdout or "")[-4000:],
                "stderr": (proc.stderr or "")[-4000:],
            }
            # systemd-analyze verify often returns non-zero on dependency warnings for
            # staged units outside /etc. Record warnings; only hard-fail on clear unit
            # syntax errors for OUR units.
            combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
            details["systemd_analyze"] = {
                "rc": analyze_rc,
                "output": analyze_out,
            }
            # Hard errors: "Failed to prepare" / parse failures naming our files
            hard = []
            for line in combined.splitlines():
                low = line.lower()
                if "failed to parse" in low or "invalid" in low and (
                    service_path.name in line or timer_path.name in line
                ):
                    hard.append(line)
                if "assignment outside of section" in low:
                    hard.append(line)
            if hard:
                errors.extend(f"systemd_analyze_hard:{h}" for h in hard)
            elif analyze_rc != 0:
                # Dependency warnings unrelated to these units — record, do not hide
                warnings.append(
                    f"systemd_analyze_rc_nonzero:{analyze_rc}:see_details"
                )
        except FileNotFoundError:
            warnings.append("systemd_analyze_not_found")
        except subprocess.TimeoutExpired:
            errors.append("systemd_analyze_timeout")

    ok = not errors
    result = {
        "ok": ok,
        "errors": errors,
        "warnings": warnings,
        "details": details,
        "systemd_analyze_rc": analyze_rc,
    }
    if not ok:
        raise ValidationError("unit_validation_failed", details=result)
    return result


def validate_report_state_schema(
    report: Mapping[str, Any] | None,
    state: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Lightweight schema gate for supervisor report/state JSON."""
    errors: list[str] = []
    if report is None:
        errors.append("report_missing")
    else:
        if not isinstance(report, Mapping):
            errors.append("report_not_object")
        else:
            # Accept either explicit schema field or known content markers
            sch = str(report.get("schema") or report.get("schema_version") or "")
            if sch and "report" not in sch and sch not in {
                REPORT_SCHEMA_VERSION,
                "report_v1",
                "supervisor_report_v1",
            }:
                # still allow if required keys present
                pass
            if "rc" not in report and "exit_code" not in report and "result" not in report:
                # dry-run reports may use ok/planned_actions only
                if "planned_actions" not in report and "actions" not in report and "plan" not in report:
                    errors.append("report_missing_rc_or_plan")
    if state is None:
        errors.append("state_missing")
    else:
        if not isinstance(state, Mapping):
            errors.append("state_not_object")
        else:
            sch = str(state.get("schema") or state.get("schema_version") or "")
            if sch and sch not in {STATE_SCHEMA_VERSION, "state_v1", "supervisor_state_v1"}:
                pass
            # minimal keys for state_v1
            # last_completed_tick or similar optional on first run
    ok = not errors
    result = {"ok": ok, "errors": errors}
    if not ok:
        raise ValidationError("report_state_schema_invalid", details=result)
    return result


def validate_post_enable_gates(
    *,
    before_fp: Mapping[str, Any],
    after_fp: Mapping[str, Any],
    report: Mapping[str, Any] | None,
    state: Mapping[str, Any] | None,
    last_service_rc: int,
    timer_enabled: bool,
    timer_active: bool,
    lock_path: Path | str,
) -> dict[str, Any]:
    """Post-enable acceptance: zero fingerprint delta, rc=0, timer up, lock free."""
    errors: list[str] = []
    parts: dict[str, Any] = {}
    try:
        parts["fingerprint"] = compare_live_fingerprint(before_fp, after_fp)
    except ValidationError as exc:
        errors.append(str(exc))
        parts["fingerprint"] = exc.details
    try:
        parts["schema"] = validate_report_state_schema(report, state)
    except ValidationError as exc:
        errors.append(str(exc))
        parts["schema"] = exc.details
    if int(last_service_rc) != 0:
        errors.append(f"last_service_rc_nonzero:{last_service_rc}")
    if not timer_enabled:
        errors.append("timer_not_enabled")
    if not timer_active:
        errors.append("timer_not_active")
    lock_p = Path(lock_path)
    lock_info = {"path": str(lock_p), "exists": lock_p.exists()}
    # Lock released/acquirable: nonblocking exclusive create or flock
    acquired = False
    acquire_error = None
    try:
        lock_p.parent.mkdir(parents=True, exist_ok=True)
        # Prefer POSIX flock via fcntl
        import fcntl

        fd = os.open(
            str(lock_p),
            os.O_RDWR | os.O_CREAT,
            0o600,
        )
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                fcntl.flock(fd, fcntl.LOCK_UN)
            except BlockingIOError:
                acquired = False
                acquire_error = "lock_held"
        finally:
            os.close(fd)
    except OSError as exc:
        acquire_error = f"os_error:{exc}"
        acquired = False
    lock_info["acquirable"] = acquired
    lock_info["error"] = acquire_error
    parts["lock"] = lock_info
    if not acquired:
        errors.append(f"lock_not_acquirable:{acquire_error}")

    ok = not errors
    result = {"ok": ok, "errors": errors, "parts": parts}
    if not ok:
        raise ValidationError("post_enable_gates_failed", details=result)
    return result


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    data = json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    with tmp.open("w", encoding="utf-8") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _build_fixture_hermes(root: Path) -> Path:
    """Create a temporary Hermes root with one board DB matching lifecycle schema."""
    board = root / "kanban" / "boards" / "fixture-board"
    board.mkdir(parents=True, exist_ok=True)
    db = board / "kanban.db"
    con = sqlite3.connect(str(db))
    try:
        con.executescript(
            """
            CREATE TABLE tasks (
              id TEXT PRIMARY KEY,
              title TEXT,
              body TEXT,
              assignee TEXT,
              status TEXT,
              priority INTEGER,
              created_by TEXT,
              created_at INTEGER,
              started_at INTEGER,
              completed_at INTEGER,
              result TEXT,
              worker_pid INTEGER,
              last_heartbeat_at INTEGER,
              current_run_id INTEGER,
              block_kind TEXT
            );
            CREATE TABLE task_runs (
              id INTEGER PRIMARY KEY,
              task_id TEXT,
              profile TEXT,
              step_key TEXT,
              status TEXT,
              claim_lock TEXT,
              claim_expires INTEGER,
              worker_pid INTEGER,
              max_runtime_seconds INTEGER,
              last_heartbeat_at INTEGER,
              started_at INTEGER,
              ended_at INTEGER,
              outcome TEXT,
              summary TEXT,
              metadata TEXT,
              error TEXT
            );
            """
        )
        # running task bound to this process for start-ticks
        pid = os.getpid()
        now = int(time.time())
        con.execute(
            "INSERT INTO task_runs (id, task_id, profile, status, worker_pid, started_at, last_heartbeat_at) "
            "VALUES (1, 't_run_1', 'worker', 'running', ?, ?, ?)",
            (pid, now, now),
        )
        con.execute(
            "INSERT INTO tasks (id, title, status, assignee, created_at, started_at, "
            "worker_pid, last_heartbeat_at, current_run_id) "
            "VALUES ('t_run_1', 'running fixture', 'running', 'worker', ?, ?, ?, ?, 1)",
            (now, now, pid, now),
        )
        con.execute(
            "INSERT INTO tasks (id, title, status, assignee, created_at, current_run_id) "
            "VALUES ('t_todo_1', 'todo fixture', 'todo', 'worker', ?, NULL)",
            (now,),
        )
        con.execute(
            "INSERT INTO tasks (id, title, status, assignee, created_at, current_run_id) "
            "VALUES ('t_arch_1', 'archived fixture', 'archived', 'worker', ?, NULL)",
            (now,),
        )
        con.commit()
    finally:
        con.close()
    return root


def run_self_test() -> int:
    """CLI --self-test using temporary fixture roots/reports only (no live mutation)."""
    failures: list[str] = []
    deploy_dir = Path(__file__).resolve().parent
    unit_paths = [
        deploy_dir / "hermes-anti-stall-supervisor.service",
        deploy_dir / "hermes-anti-stall-supervisor.timer",
    ]

    # 1) unit validation on staged files
    try:
        ures = validate_units(unit_paths)
        print("validate_units: OK", json.dumps({
            "warnings": ures.get("warnings"),
            "systemd_analyze_rc": ures.get("systemd_analyze_rc"),
        }, sort_keys=True))
    except ValidationError as exc:
        failures.append(f"validate_units:{exc}:{exc.details}")
        print("validate_units: FAIL", exc, exc.details)

    with tempfile.TemporaryDirectory(prefix="anti_stall_deploy_selftest_") as td:
        tdp = Path(td)
        hermes = _build_fixture_hermes(tdp / "hermes")
        fp1 = capture_live_fingerprint(hermes)
        # archived excluded, running+todo present
        task_ids = []
        for b in fp1["boards"]:
            task_ids.extend(t["task_id"] for t in b["tasks"])
        if "t_arch_1" in task_ids:
            failures.append("fingerprint_included_archived")
        if "t_run_1" not in task_ids or "t_todo_1" not in task_ids:
            failures.append(f"fingerprint_missing_expected:{task_ids}")
        # running task should have pid + start ticks for this process
        running = None
        for b in fp1["boards"]:
            for t in b["tasks"]:
                if t["task_id"] == "t_run_1":
                    running = t
        if not running or running.get("run_worker_pid") != os.getpid():
            failures.append(f"fingerprint_pid_mismatch:{running}")
        if not running or not running.get("pid_start_ticks"):
            failures.append(f"fingerprint_missing_start_ticks:{running}")
        print("capture_live_fingerprint: OK", {
            "boards": len(fp1["boards"]),
            "tasks": len(task_ids),
            "sha": fp1.get("fingerprint_sha256"),
        })

        # dry-run good: only diagnostic on non-running
        good_report = {
            "schema": REPORT_SCHEMA_VERSION,
            "rc": 0,
            "planned_actions": [
                {
                    "kind": "comment_once",
                    "task_id": "t_todo_1",
                    "diagnostic_only": True,
                }
            ],
            "per_task": [
                {
                    "task_id": "t_run_1",
                    "planned_actions": [],
                    "planned_actions_count": 0,
                },
                {
                    "task_id": "t_todo_1",
                    "planned_actions": [{"kind": "comment_once"}],
                },
            ],
        }
        try:
            dres = validate_dry_run(good_report, fp1)
            print("validate_dry_run_good: OK", dres)
        except ValidationError as exc:
            failures.append(f"dry_run_good:{exc}:{exc.details}")
            print("validate_dry_run_good: FAIL", exc.details)

        # dry-run bad: status change on running
        bad_report = {
            "schema": REPORT_SCHEMA_VERSION,
            "rc": 0,
            "planned_actions": [
                {"kind": "retry", "task_id": "t_run_1"},
            ],
            "per_task": [
                {
                    "task_id": "t_run_1",
                    "planned_actions": [{"kind": "retry"}],
                    "planned_actions_count": 1,
                }
            ],
        }
        try:
            validate_dry_run(bad_report, fp1)
            failures.append("dry_run_bad_should_fail")
            print("validate_dry_run_bad: FAIL (accepted)")
        except ValidationError as exc:
            print("validate_dry_run_bad: OK (rejected)", exc.details.get("errors"))

        # fingerprint compare identical
        fp2 = capture_live_fingerprint(hermes)
        try:
            cres = compare_live_fingerprint(fp1, fp2)
            print("compare_live_fingerprint_same: OK", {
                "ok": cres["ok"],
                "sha": cres.get("before_sha256"),
            })
        except ValidationError as exc:
            failures.append(f"compare_same:{exc}")
            print("compare_live_fingerprint_same: FAIL", exc.details)

        # fingerprint compare changed status
        # mutate fixture DB (temp only)
        db = next(Path(hermes).joinpath("kanban/boards").rglob("kanban.db"))
        con = sqlite3.connect(str(db))
        try:
            con.execute("UPDATE tasks SET status='blocked' WHERE id='t_run_1'")
            con.commit()
        finally:
            con.close()
        fp3 = capture_live_fingerprint(hermes)
        try:
            compare_live_fingerprint(fp1, fp3)
            failures.append("compare_changed_should_fail")
            print("compare_live_fingerprint_changed: FAIL (accepted)")
        except ValidationError as exc:
            print(
                "compare_live_fingerprint_changed: OK (rejected)",
                exc.details.get("changes"),
            )

        # restore for lock/post gates
        con = sqlite3.connect(str(db))
        try:
            con.execute(
                "UPDATE tasks SET status='running' WHERE id='t_run_1'"
            )
            con.commit()
        finally:
            con.close()
        fp4 = capture_live_fingerprint(hermes)

        state = {
            "schema": STATE_SCHEMA_VERSION,
            "last_completed_tick": {"unix": time.time(), "rc": 0},
            "action_keys": [],
        }
        lock_path = tdp / "var" / "hygiene.lock"
        try:
            pres = validate_post_enable_gates(
                before_fp=fp1,
                after_fp=fp4,
                report=good_report,
                state=state,
                last_service_rc=0,
                timer_enabled=True,
                timer_active=True,
                lock_path=lock_path,
            )
            print("validate_post_enable_gates: OK", {"ok": pres["ok"]})
        except ValidationError as exc:
            failures.append(f"post_enable:{exc}:{exc.details}")
            print("validate_post_enable_gates: FAIL", exc.details)

        # lock held should fail
        import fcntl

        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                validate_post_enable_gates(
                    before_fp=fp1,
                    after_fp=fp4,
                    report=good_report,
                    state=state,
                    last_service_rc=0,
                    timer_enabled=True,
                    timer_active=True,
                    lock_path=lock_path,
                )
                failures.append("post_enable_lock_held_should_fail")
                print("lock_held_gate: FAIL (accepted)")
            except ValidationError as exc:
                print("lock_held_gate: OK (rejected)", exc.details.get("errors"))
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    if failures:
        print("SELF_TEST_FAIL", failures)
        return 1
    print("SELF_TEST_OK")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Validate anti-stall supervisor deploy gates")
    parser.add_argument("--self-test", action="store_true", help="Run fixture-only self tests")
    parser.add_argument(
        "--validate-units",
        nargs="+",
        default=None,
        help="Paths to staged unit files",
    )
    parser.add_argument(
        "--fingerprint",
        metavar="HERMES_ROOT",
        default=None,
        help="Capture live fingerprint under HERMES_ROOT (read-only) and print JSON",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Optional path to write JSON output",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.self_test:
        return run_self_test()

    if args.validate_units:
        try:
            res = validate_units(args.validate_units)
        except ValidationError as exc:
            print(json.dumps({"ok": False, "error": str(exc), "details": exc.details}, indent=2))
            return 2
        text = json.dumps(res, indent=2, sort_keys=True)
        print(text)
        if args.out:
            _write_json(args.out, res)
        return 0

    if args.fingerprint:
        fp = capture_live_fingerprint(args.fingerprint)
        text = json.dumps(fp, indent=2, sort_keys=True)
        print(text)
        if args.out:
            _write_json(args.out, fp)
        return 0

    parser.print_help()
    return 64


if __name__ == "__main__":
    sys.exit(main())
