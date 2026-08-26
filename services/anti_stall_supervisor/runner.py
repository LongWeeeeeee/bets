#!/usr/bin/env python3
"""W4: single-lock five-minute hygiene tick runner (staging).

Bounded orchestrator that:
  - acquires one nonblocking process lock for the whole tick
  - chains the existing plan linter under that lock
  - runs injected snapshot / decision / executor adapters
  - atomically commits state / report / audit

No live board deployment. INT wires real lane modules later.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, Mapping, MutableMapping, Optional

# Frozen outer lock — plan_lint has no lock of its own (observed 2026-07-19).
DEFAULT_LOCK_PATH = Path("/root/main/runtime/anti_stall_supervisor_var/hygiene.lock")
DEFAULT_LINTER = Path("/root/main/runtime/kanban_plan_lint.py")
DEFAULT_STATE_SCHEMA = "state_v1"
DEFAULT_REPORT_SCHEMA = "report_v1"
DEFAULT_TICK_DEADLINE_S = 240
DEFAULT_LINTER_TIMEOUT_S = 60
DEFAULT_AUDIT_MAX_BYTES = 1_000_000
DEFAULT_AUDIT_KEEP = 3
SECRET_KEY_RE = re.compile(
    r"(password|passwd|secret|token|api[_-]?key|authorization|cookie|jwt|bearer)",
    re.I,
)
SECRET_VALUE_RE = re.compile(
    r"(?i)(bearer\s+[a-z0-9._\-]+|sk-[a-z0-9]{10,}|ghp_[a-z0-9]{20,}|xox[baprs]-[a-z0-9-]{10,})"
)


class TickLock:
    """Nonblocking exclusive flock held for the duration of a tick."""

    def __init__(self, lock_path: Path):
        self.lock_path = Path(lock_path)
        self._fh: Any = None
        self.acquired = False

    def acquire(self) -> bool:
        import fcntl

        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self.lock_path, "a+", encoding="utf-8")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            fh.close()
            self.acquired = False
            return False
        except OSError:
            fh.close()
            self.acquired = False
            return False
        self._fh = fh
        try:
            fh.seek(0)
            fh.truncate()
            fh.write(f"pid={os.getpid()} ts={time.time():.6f}\n")
            fh.flush()
            os.fsync(fh.fileno())
        except Exception:
            pass
        self.acquired = True
        return True

    def release(self) -> None:
        import fcntl

        if self._fh is None:
            return
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            self._fh.close()
        except Exception:
            pass
        self._fh = None
        self.acquired = False

    def __enter__(self) -> "TickLock":
        self.acquire()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.release()


def acquire_tick_lock(lock_path: str | Path) -> TickLock:
    """Acquire nonblocking tick lock. Caller must release() or use as context."""
    lock = TickLock(Path(lock_path))
    lock.acquire()
    return lock


def atomic_write_json(path: str | Path, value: Any) -> None:
    """Write JSON via <target>.tmp + fsync + atomic replace."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    data = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    # exclusive create/truncate
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(str(tmp), flags, 0o644)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
    except Exception:
        try:
            os.close(fd)
        except Exception:
            pass
        raise
    os.replace(str(tmp), str(path))
    # best-effort dir fsync
    try:
        dir_fd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except Exception:
        pass


def _now_iso(clock: Optional[Callable[[], float]] = None) -> str:
    ts = (clock or time.time)()
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def redact_value(obj: Any, key_hint: str = "") -> Any:
    """Recursively redact secret-looking keys/values. Bounded, pure."""
    if isinstance(obj, Mapping):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            ks = str(k)
            if SECRET_KEY_RE.search(ks):
                out[ks] = "***REDACTED***"
            else:
                out[ks] = redact_value(v, ks)
        return out
    if isinstance(obj, list):
        return [redact_value(x, key_hint) for x in obj[:200]]
    if isinstance(obj, str):
        if SECRET_KEY_RE.search(key_hint) or SECRET_VALUE_RE.search(obj):
            return "***REDACTED***"
        if len(obj) > 2000:
            return obj[:2000] + "…[truncated]"
        return obj
    return obj


def empty_state_v1() -> Dict[str, Any]:
    return {
        "schema": DEFAULT_STATE_SCHEMA,
        "version": 1,
        "previous_evidence_digests": {},
        "consecutive_stall_counters": {},
        "action_keys": {},
        "cooldowns": {},
        # Decision-native containers (W2 prior_state). Preserved across ticks.
        "tasks": {},
        "action_cooldowns": {},
        "last_tick_ns": None,
        "policy_digest": None,
        "schema_digest": None,
        "last_completed_tick": None,
        "last_tick_started_at": None,
        "last_decision_key": None,
        "linter_last": None,
        "tick_count": 0,
        "notes": [],
    }


def load_state(path: Path) -> tuple[Dict[str, Any], Optional[str]]:
    """Load state_v1. On corruption: rename aside, return empty + diagnostic."""
    if not path.exists():
        return empty_state_v1(), None
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict) or data.get("schema") not in (
            DEFAULT_STATE_SCHEMA,
            "state_v1",
            None,
        ):
            # allow missing schema only if dict-like prior; still validate keys lightly
            if not isinstance(data, dict):
                raise ValueError("state is not an object")
        # ensure required containers
        base = empty_state_v1()
        base.update({k: data.get(k, base[k]) for k in base})
        # type guards
        if not isinstance(base.get("previous_evidence_digests"), dict):
            raise ValueError("previous_evidence_digests not object")
        if not isinstance(base.get("consecutive_stall_counters"), dict):
            raise ValueError("consecutive_stall_counters not object")
        if not isinstance(base.get("action_keys"), dict):
            raise ValueError("action_keys not object")
        if not isinstance(base.get("cooldowns"), dict):
            raise ValueError("cooldowns not object")
        # Optional decision-native containers (may be absent in older state files).
        if base.get("tasks") is None:
            base["tasks"] = {}
        if not isinstance(base.get("tasks"), dict):
            raise ValueError("tasks not object")
        if base.get("action_cooldowns") is None:
            base["action_cooldowns"] = {}
        if not isinstance(base.get("action_cooldowns"), dict):
            raise ValueError("action_cooldowns not object")
        return base, None
    except Exception as exc:
        aside = path.with_name(
            f"{path.name}.corrupt.{int(time.time())}.{os.getpid()}"
        )
        try:
            os.replace(str(path), str(aside))
            diag = f"corrupt_state_renamed:{aside.name}:{type(exc).__name__}:{exc}"
        except Exception as ren_exc:
            diag = f"corrupt_state_unreadable:{type(exc).__name__}:{exc};rename_failed:{ren_exc}"
        st = empty_state_v1()
        st["notes"] = [diag]
        return st, diag


def validate_report(report: Mapping[str, Any]) -> list[str]:
    """Minimal schema validation for report_v1. Returns list of errors."""
    errs: list[str] = []
    if not isinstance(report, Mapping):
        return ["report_not_object"]
    if report.get("schema") != DEFAULT_REPORT_SCHEMA:
        errs.append("schema_must_be_report_v1")
    for key in (
        "generated_at",
        "decision",
        "decision_key",
        "rc",
        "dry_run",
        "lock",
        "linter",
        "tick",
    ):
        if key not in report:
            errs.append(f"missing:{key}")
    dec = report.get("decision")
    if not isinstance(dec, Mapping):
        errs.append("decision_not_object")
    else:
        if "key" not in dec or "reason" not in dec:
            errs.append("decision_missing_key_or_reason")
    return errs


def validate_state(state: Mapping[str, Any]) -> list[str]:
    errs: list[str] = []
    if not isinstance(state, Mapping):
        return ["state_not_object"]
    if state.get("schema") != DEFAULT_STATE_SCHEMA:
        errs.append("schema_must_be_state_v1")
    for key in (
        "previous_evidence_digests",
        "consecutive_stall_counters",
        "action_keys",
        "cooldowns",
    ):
        if not isinstance(state.get(key), dict):
            errs.append(f"{key}_not_object")
    return errs


def append_audit(
    audit_path: Path,
    record: Mapping[str, Any],
    *,
    max_bytes: int = DEFAULT_AUDIT_MAX_BYTES,
    keep: int = DEFAULT_AUDIT_KEEP,
) -> None:
    """Append one redacted JSONL audit line; rotate by rename (no deletion)."""
    audit_path = Path(audit_path)
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    safe = redact_value(dict(record))
    line = json.dumps(safe, ensure_ascii=False, sort_keys=True) + "\n"
    # rotation without deletion
    if audit_path.exists() and audit_path.stat().st_size + len(line) > max_bytes:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        rotated = audit_path.with_name(f"{audit_path.name}.{ts}")
        try:
            os.replace(str(audit_path), str(rotated))
        except Exception:
            pass
        # bounded retention: rename overflow older files with .overflow suffix, never unlink
        try:
            siblings = sorted(
                audit_path.parent.glob(audit_path.name + ".*"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            # keep newest `keep` rotated; mark older as .retained_overflow (still no delete)
            for i, sib in enumerate(siblings):
                if i >= keep and not str(sib).endswith(".retained_overflow"):
                    overflow = Path(str(sib) + ".retained_overflow")
                    if not overflow.exists():
                        try:
                            os.replace(str(sib), str(overflow))
                        except Exception:
                            pass
        except Exception:
            pass
    with audit_path.open("a", encoding="utf-8") as fh:
        fh.write(line)
        fh.flush()
        os.fsync(fh.fileno())


_PLAN_LINT_SUMMARY_RE = re.compile(
    r"plan_lint\s+findings=(?P<findings>\d+)\s+"
    r"critical=(?P<critical>\d+)\s+"
    r"error=(?P<error>\d+)\s+"
    r"warning=(?P<warning>\d+)",
    re.I,
)


def _parse_plan_lint_summary(text: str) -> dict[str, int]:
    """Extract finding counts from kanban_plan_lint human summary line."""
    if not text:
        return {}
    m = _PLAN_LINT_SUMMARY_RE.search(str(text))
    if not m:
        return {}
    return {
        "finding_count": int(m.group("findings")),
        "critical_count": int(m.group("critical")),
        "error_count": int(m.group("error")),
        "warning_count": int(m.group("warning")),
    }


def _normalize_plan_lint_result(raw: Mapping[str, Any] | None) -> dict:
    """Normalize plan-lint process outcome into gate semantics.

    Distinction:
      - process executed (rc known, including Critical rc=2)
      - safety gate passed only when rc==0 (finding-free)

    rc=2 means execution completed with Critical findings: gate fails.
    Unparseable / non-mapping results are fail-closed.
    """
    if not isinstance(raw, Mapping):
        return {
            "ok": False,
            "gate_ok": False,
            "executed": False,
            "rc": None,
            "error": "linter_result_unparseable",
            "reason": "linter_result_unparseable",
            "critical_count": None,
            "finding_count": None,
            "stdout": "",
            "stderr": "",
            "duration_s": 0.0,
            "cmd": [],
            "timed_out": False,
        }

    out = dict(raw)
    stdout = str(out.get("stdout") or "")
    stderr = str(out.get("stderr") or "")
    parsed = _parse_plan_lint_summary(stdout) or _parse_plan_lint_summary(stderr)

    # Prefer explicit counts from runner/fake payloads, else parse stdout.
    for src_key, dst_key in (
        ("critical_count", "critical_count"),
        ("finding_count", "finding_count"),
        ("error_count", "error_count"),
        ("warning_count", "warning_count"),
        ("critical", "critical_count"),
        ("finding_count", "finding_count"),
    ):
        if out.get(dst_key) is None and out.get(src_key) is not None:
            try:
                out[dst_key] = int(out.get(src_key))
            except (TypeError, ValueError):
                out[dst_key] = None
    for k, v in parsed.items():
        if out.get(k) is None:
            out[k] = v

    rc_raw = out.get("rc")
    try:
        rc = int(rc_raw) if rc_raw is not None else None
    except (TypeError, ValueError):
        rc = None
    out["rc"] = rc

    timed_out = bool(out.get("timed_out"))
    # Process executed if we have a concrete rc or an explicit executed flag,
    # excluding pure timeout/missing cases that never completed.
    executed = bool(out.get("executed"))
    if rc is not None and not timed_out:
        executed = True
    out["executed"] = executed

    critical_count = out.get("critical_count")
    try:
        critical_count_i = int(critical_count) if critical_count is not None else None
    except (TypeError, ValueError):
        critical_count_i = None
        out["critical_count"] = None
    else:
        out["critical_count"] = critical_count_i

    # Safety gate: only finding-free rc=0 is a pass.
    gate_ok = False
    reason = out.get("reason")
    error = out.get("error")

    if timed_out:
        gate_ok = False
        error = error or f"linter_timeout"
        reason = reason or error
    elif rc == 0:
        # rc=0 is finding-free pass; if critical_count contradicts, fail-close.
        if critical_count_i is not None and critical_count_i > 0:
            gate_ok = False
            reason = "critical_plan_lint_findings"
            error = error or "linter_critical_findings"
        else:
            gate_ok = True
            if critical_count_i is None:
                out["critical_count"] = 0
    elif rc == 2:
        gate_ok = False
        reason = "critical_plan_lint_findings"
        error = error or "linter_critical_findings"
        if critical_count_i is None:
            # Unable to parse Critical count from rc=2 output → still fail-closed.
            error = "linter_critical_findings_unparsed"
            reason = "critical_plan_lint_findings"
    else:
        gate_ok = False
        if rc is None:
            error = error or "linter_result_unparseable"
            reason = reason or "linter_result_unparseable"
        else:
            error = error or f"linter_rc_{rc}"
            reason = reason or error

    out["gate_ok"] = gate_ok
    # `ok` is the safety-gate signal consumed by run_tick (not mere process completion).
    out["ok"] = bool(gate_ok)
    out["error"] = error
    out["reason"] = reason
    return out


def _action_identity(action: Any) -> Optional[str]:
    """Stable action identity used for planned/applied corroboration."""
    if isinstance(action, Mapping):
        for key in ("action_key", "key", "id"):
            val = action.get(key)
            if val is None:
                continue
            s = str(val).strip()
            if s:
                return s
        return None
    if action is None:
        return None
    s = str(action).strip()
    return s or None


def _validate_executor_result(
    exec_result: Any,
    planned_actions: Any,
    *,
    dry_run: bool = False,
) -> tuple[bool, dict, list[str], str]:
    """Validate executor envelope before treating the tick as successful.

    Success requires:
      - result is a dict
      - literal ok is True
      - no denied / errors buckets with entries
      - exact one-to-one planned vs applied identity parity
        (dry-run: applied must be empty; planned identities need not appear in applied)

    Returns (ok, normalized_result, error_details, reason_code).
    """
    details: list[str] = []
    if not isinstance(exec_result, dict):
        norm = {
            "schema": "executor_result_v1",
            "ok": False,
            "applied": [],
            "denied": [],
            "errors": [{"code": "invalid_executor_result", "message": "non_dict"}],
            "raw_type": type(exec_result).__name__,
            "dry_run": dry_run,
            "reason": "executor_result_non_dict",
        }
        return False, norm, ["executor_result_non_dict"], "executor_result_non_dict"

    norm = dict(exec_result)
    norm.setdefault("schema", "executor_result_v1")
    norm["dry_run"] = bool(norm.get("dry_run", dry_run))

    if norm.get("ok") is not True:
        details.append("executor_ok_not_true")
        reason = "executor_ok_not_true"
        # Preserve structured failure details from executor contract.
        for bucket in ("denied", "errors"):
            items = norm.get(bucket) or []
            if isinstance(items, list):
                for it in items:
                    if isinstance(it, Mapping):
                        code = it.get("code") or it.get("status") or bucket
                        details.append(f"executor_{bucket}:{code}")
                    else:
                        details.append(f"executor_{bucket}:{it}")
        if any(
            isinstance(it, Mapping) and str(it.get("code") or "") == "locked_db"
            for it in (norm.get("errors") or [])
            if isinstance((norm.get("errors") or []), list)
        ):
            reason = "executor_locked_db"
        norm["ok"] = False
        norm["reason"] = norm.get("reason") or reason
        return False, norm, details, reason

    denied = norm.get("denied") or []
    errors = norm.get("errors") or []
    if not isinstance(denied, list):
        details.append("executor_denied_not_list")
        denied = []
    if not isinstance(errors, list):
        details.append("executor_errors_not_list")
        errors = []
    if denied:
        details.append(f"executor_denied_count:{len(denied)}")
        for it in denied:
            if isinstance(it, Mapping):
                details.append(
                    f"executor_denied:{it.get('code') or it.get('status') or 'denied'}"
                )
        norm["ok"] = False
        reason = "executor_denied"
        norm["reason"] = norm.get("reason") or reason
        return False, norm, details, reason
    if errors:
        details.append(f"executor_errors_count:{len(errors)}")
        reason = "executor_errors"
        for it in errors:
            if isinstance(it, Mapping):
                code = str(it.get("code") or "error")
                details.append(f"executor_errors:{code}")
                if code == "locked_db":
                    reason = "executor_locked_db"
        norm["ok"] = False
        norm["reason"] = norm.get("reason") or reason
        return False, norm, details, reason

    planned_list = list(planned_actions or []) if isinstance(planned_actions, list) else []
    applied_list = norm.get("applied") or []
    if not isinstance(applied_list, list):
        details.append("executor_applied_not_list")
        norm["ok"] = False
        norm["applied"] = []
        reason = "executor_applied_not_list"
        norm["reason"] = reason
        return False, norm, details, reason

    planned_ids = [_action_identity(a) for a in planned_list]
    applied_ids = [_action_identity(a) for a in applied_list]

    if any(i is None for i in planned_ids):
        details.append("executor_planned_missing_identity")
        norm["ok"] = False
        reason = "executor_planned_missing_identity"
        norm["reason"] = reason
        return False, norm, details, reason
    if any(i is None for i in applied_ids):
        details.append("executor_applied_missing_identity")
        norm["ok"] = False
        reason = "executor_applied_missing_identity"
        norm["reason"] = reason
        return False, norm, details, reason

    planned_ids_s = [str(i) for i in planned_ids]
    applied_ids_s = [str(i) for i in applied_ids]

    if dry_run:
        # Dry-run contract: outer report actions_applied stays 0. Real adapters
        # already empty applied; tolerate test/adapters that still echo planned
        # identities in applied by moving them to planned (no fail-close).
        if applied_ids_s:
            planned_bucket = list(norm.get("planned") or [])
            planned_bucket.extend(list(applied_list))
            norm["planned"] = planned_bucket
            norm["applied"] = []
            details.append("executor_dry_run_applied_normalized")
        norm["ok"] = True
        norm["reason"] = norm.get("reason") or "ok"
        return True, norm, [], "ok"

    # Live path: exact one-to-one multiset parity between planned and applied identities.
    if sorted(planned_ids_s) != sorted(applied_ids_s):
        from collections import Counter

        pc = Counter(planned_ids_s)
        ac = Counter(applied_ids_s)
        missing = sorted((pc - ac).elements())
        extra = sorted((ac - pc).elements())
        # Detect pure duplicates when counts differ but sets equal.
        if missing:
            details.append(f"executor_applied_missing:{','.join(missing)}")
        if extra:
            details.append(f"executor_applied_extra:{','.join(extra)}")
        if not missing and not extra and len(planned_ids_s) != len(set(planned_ids_s)):
            details.append("executor_applied_duplicate")
        if len(applied_ids_s) != len(set(applied_ids_s)) and not extra:
            # duplicate applied of a planned id while planned had one
            details.append("executor_applied_duplicate")
        reason = "executor_planned_applied_mismatch"
        norm["ok"] = False
        norm["reason"] = reason
        norm["planned_ids"] = planned_ids_s
        norm["applied_ids"] = applied_ids_s
        return False, norm, details, reason

    # Reject duplicate identities even if multisets somehow matched unevenly — sorted equal
    # already implies multiset equality, which allows intentional multi-apply only if planned
    # also duplicated. That is exact parity; accept.
    norm["ok"] = True
    norm["reason"] = norm.get("reason") or "ok"
    return True, norm, [], "ok"


def run_plan_lint(config: Mapping[str, Any]) -> dict:
    """Invoke existing kanban_plan_lint.py once under caller-held lock.

    config keys:
      linter_path, linter_timeout_s, dry_run, python_executable,
      linter_args (optional list), capture_limit (int)
      linter_runner (optional callable for tests) -> dict

    Return contract:
      ok / gate_ok — safety gate passed (rc==0 only)
      rc=2 — process executed with Critical findings; ok=False
      critical_count / reason exposed for reporting
    """
    if "linter_runner" in config and callable(config["linter_runner"]):
        return _normalize_plan_lint_result(dict(config["linter_runner"](config)))

    linter = Path(config.get("linter_path") or DEFAULT_LINTER)
    timeout = float(config.get("linter_timeout_s") or DEFAULT_LINTER_TIMEOUT_S)
    timeout = min(max(timeout, 1.0), 60.0)  # hard cap <= 60s
    py = config.get("python_executable") or sys.executable
    dry = bool(config.get("dry_run", False))
    extra = list(config.get("linter_args") or [])
    capture_limit = int(config.get("capture_limit") or 8000)

    if not linter.is_file():
        return _normalize_plan_lint_result(
            {
                "ok": False,
                "rc": 127,
                "error": f"linter_missing:{linter}",
                "stdout": "",
                "stderr": "",
                "duration_s": 0.0,
                "cmd": [],
                "timed_out": False,
                "executed": False,
            }
        )

    cmd = [str(py), str(linter)]
    if dry and "--dry-run" not in extra:
        cmd.append("--dry-run")
    cmd.extend(extra)

    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        dur = time.monotonic() - t0
        stdout = (proc.stdout or "")[:capture_limit]
        stderr = (proc.stderr or "")[:capture_limit]
        rc = int(proc.returncode)
        raw = {
            "rc": rc,
            "stdout": stdout,
            "stderr": stderr,
            "duration_s": round(dur, 4),
            "cmd": cmd,
            "timed_out": False,
            "executed": True,
        }
        if rc not in (0, 2):
            raw["error"] = f"linter_rc_{rc}"
        return _normalize_plan_lint_result(raw)
    except subprocess.TimeoutExpired as exc:
        dur = time.monotonic() - t0
        stdout = ((exc.stdout or "") if isinstance(exc.stdout, str) else "")[:capture_limit]
        stderr = ((exc.stderr or "") if isinstance(exc.stderr, str) else "")[:capture_limit]
        return _normalize_plan_lint_result(
            {
                "ok": False,
                "rc": 124,
                "error": f"linter_timeout:{timeout}s",
                "stdout": stdout,
                "stderr": stderr,
                "duration_s": round(dur, 4),
                "cmd": cmd,
                "timed_out": True,
                "executed": False,
            }
        )
    except Exception as exc:
        dur = time.monotonic() - t0
        return _normalize_plan_lint_result(
            {
                "ok": False,
                "rc": 1,
                "error": f"linter_exception:{type(exc).__name__}:{exc}",
                "stdout": "",
                "stderr": "",
                "duration_s": round(dur, 4),
                "cmd": cmd,
                "timed_out": False,
                "executed": False,
            }
        )


class _Deadline:
    def __init__(self, seconds: float, clock: Callable[[], float]):
        self.deadline = clock() + seconds
        self.clock = clock

    def remaining(self) -> float:
        return self.deadline - self.clock()

    def expired(self) -> bool:
        return self.remaining() <= 0


def _safe_call(name: str, fn: Callable, *args: Any, **kwargs: Any) -> tuple[bool, Any, Optional[str]]:
    try:
        return True, fn(*args, **kwargs), None
    except Exception as exc:
        tb = traceback.format_exc(limit=5)
        return False, None, f"{name}_failed:{type(exc).__name__}:{exc}"


def _default_adapters() -> Dict[str, Callable]:
    """No-op adapters for self-test / dry structural runs."""

    def snapshot(config: Mapping[str, Any], state: Mapping[str, Any]) -> dict:
        return {
            "schema": "snapshot_v1",
            "boards": [],
            "tasks": [],
            "digest": _sha256_text("empty"),
            "collected_at": _now_iso(),
        }

    def decide(
        snapshot: Mapping[str, Any],
        state: Mapping[str, Any],
        config: Mapping[str, Any],
    ) -> dict:
        return {
            "schema": "decision_plan_v1",
            "actions": [],
            "decision_key": "healthy_noop",
            "reason": "no_tasks",
            "fail_closed": False,
        }

    def execute(
        plan: Mapping[str, Any],
        config: Mapping[str, Any],
        *,
        dry_run: bool,
    ) -> dict:
        return {
            "schema": "executor_result_v1",
            "applied": [],
            "skipped": list(plan.get("actions") or []),
            "dry_run": dry_run,
            "ok": True,
        }

    return {
        "snapshot": snapshot,
        "decide": decide,
        "execute": execute,
    }


def run_tick(
    config: Mapping[str, Any],
    *,
    adapters: Optional[Mapping[str, Callable]] = None,
    clock: Optional[Callable[[], float]] = None,
) -> int:
    """Run one hygiene tick. Returns process rc.

    Quiet by default: empty stdout/stderr on routine/healthy/lock-contention.
    Lock contention -> rc=0, no board/state mutation, one report decision.
    """
    clock = clock or time.time
    cfg = dict(config)
    dry_run = bool(cfg.get("dry_run", False))
    quiet = bool(cfg.get("quiet", True))
    lock_path = Path(cfg.get("lock_path") or DEFAULT_LOCK_PATH)
    state_path = Path(cfg.get("state_path") or (Path(cfg.get("var_dir") or ".") / "state.json"))
    report_path = Path(
        cfg.get("report_path") or (Path(cfg.get("var_dir") or ".") / "report.json")
    )
    audit_path = Path(
        cfg.get("audit_path") or (Path(cfg.get("var_dir") or ".") / "audit.jsonl")
    )
    deadline_s = float(cfg.get("tick_deadline_s") or DEFAULT_TICK_DEADLINE_S)
    deadline_s = min(max(deadline_s, 1.0), float(DEFAULT_TICK_DEADLINE_S))
    ads = dict(_default_adapters())
    if adapters:
        ads.update(dict(adapters))

    started = clock()
    started_iso = datetime.fromtimestamp(started, tz=timezone.utc).isoformat()
    dl = _Deadline(deadline_s, clock)

    # --- lock ---
    lock = acquire_tick_lock(lock_path)
    if not lock.acquired:
        # contention: one report decision, no state mutation, rc=0, quiet
        report = {
            "schema": DEFAULT_REPORT_SCHEMA,
            "generated_at": _now_iso(clock),
            "decision": {
                "key": "lock_contention",
                "reason": f"lock_busy:{lock_path}",
                "actions_planned": 0,
                "actions_applied": 0,
            },
            "decision_key": "lock_contention",
            "rc": 0,
            "dry_run": dry_run,
            "lock": {"path": str(lock_path), "acquired": False},
            "linter": {"invoked": False},
            "tick": {
                "started_at": started_iso,
                "finished_at": _now_iso(clock),
                "duration_s": round(clock() - started, 4),
                "deadline_s": deadline_s,
            },
            "fail_closed": False,
            "errors": [],
        }
        # write report only (no state advance) — still atomic
        try:
            errs = validate_report(report)
            if not errs:
                atomic_write_json(report_path, report)
            append_audit(
                audit_path,
                {
                    "ts": _now_iso(clock),
                    "decision_key": "lock_contention",
                    "action_key": None,
                    "reason": f"lock_busy:{lock_path}",
                    "dry_run": dry_run,
                    "rc": 0,
                },
                max_bytes=int(cfg.get("audit_max_bytes") or DEFAULT_AUDIT_MAX_BYTES),
                keep=int(cfg.get("audit_keep") or DEFAULT_AUDIT_KEEP),
            )
        except Exception:
            pass
        return 0

    linter_result: Dict[str, Any] = {"invoked": False}
    errors: list[str] = []
    fail_closed = False
    decision_key = "uninitialized"
    decision_reason = ""
    plan: Dict[str, Any] = {}
    snap: Dict[str, Any] = {}
    exec_result: Dict[str, Any] = {}
    state: Dict[str, Any] = empty_state_v1()
    corrupt_diag: Optional[str] = None
    actions_planned = 0
    actions_applied = 0
    rc = 0

    try:
        # load state under lock
        state, corrupt_diag = load_state(state_path)
        if corrupt_diag:
            fail_closed = True
            errors.append(corrupt_diag)
            decision_key = "corrupt_state"
            decision_reason = corrupt_diag
            # no actions on this tick
            plan = {
                "schema": "decision_plan_v1",
                "actions": [],
                "decision_key": decision_key,
                "reason": decision_reason,
                "fail_closed": True,
            }
        elif dl.expired():
            fail_closed = True
            errors.append("tick_deadline_before_work")
            decision_key = "deadline_exceeded"
            decision_reason = "tick_deadline_before_work"
            plan = {
                "schema": "decision_plan_v1",
                "actions": [],
                "decision_key": decision_key,
                "reason": decision_reason,
                "fail_closed": True,
            }
        else:
            # 1) plan linter under same outer lock (exactly once)
            lint_cfg = {
                "linter_path": cfg.get("linter_path") or DEFAULT_LINTER,
                "linter_timeout_s": min(
                    float(cfg.get("linter_timeout_s") or DEFAULT_LINTER_TIMEOUT_S),
                    60.0,
                    max(0.1, dl.remaining() - 1.0),
                ),
                "dry_run": dry_run or bool(cfg.get("linter_dry_run", dry_run)),
                "python_executable": cfg.get("python_executable") or sys.executable,
                "linter_args": cfg.get("linter_args") or [],
                "capture_limit": cfg.get("capture_limit") or 8000,
            }
            if "linter_runner" in cfg:
                lint_cfg["linter_runner"] = cfg["linter_runner"]
            linter_result = run_plan_lint(lint_cfg)
            linter_result["invoked"] = True
            if not linter_result.get("ok"):
                fail_closed = True
                lint_err = (
                    linter_result.get("error")
                    or linter_result.get("reason")
                    or "linter_failed"
                )
                errors.append(str(lint_err))
                if linter_result.get("timed_out"):
                    decision_key = "linter_timeout"
                    decision_reason = str(lint_err)
                elif (
                    linter_result.get("rc") == 2
                    or linter_result.get("reason") == "critical_plan_lint_findings"
                    or str(linter_result.get("error") or "").startswith(
                        "linter_critical"
                    )
                ):
                    decision_key = "linter_critical_findings"
                    decision_reason = str(
                        linter_result.get("reason")
                        or "critical_plan_lint_findings"
                    )
                    crit = linter_result.get("critical_count")
                    if crit is not None:
                        errors.append(f"critical_count:{crit}")
                else:
                    decision_key = "linter_failure"
                    decision_reason = str(lint_err)
                # fail closed: no supervisor actions
                plan = {
                    "schema": "decision_plan_v1",
                    "actions": [],
                    "decision_key": decision_key,
                    "reason": decision_reason,
                    "fail_closed": True,
                }
            elif dl.expired():
                fail_closed = True
                errors.append("tick_deadline_after_linter")
                decision_key = "deadline_exceeded"
                decision_reason = "tick_deadline_after_linter"
                plan = {
                    "schema": "decision_plan_v1",
                    "actions": [],
                    "decision_key": decision_key,
                    "reason": decision_reason,
                    "fail_closed": True,
                }
            else:
                # 2) snapshot
                ok, snap_val, err = _safe_call(
                    "snapshot", ads["snapshot"], cfg, state
                )
                if not ok:
                    fail_closed = True
                    errors.append(err or "snapshot_failed")
                    decision_key = "adapter_failure"
                    decision_reason = err or "snapshot_failed"
                    snap = {}
                    plan = {
                        "schema": "decision_plan_v1",
                        "actions": [],
                        "decision_key": decision_key,
                        "reason": decision_reason,
                        "fail_closed": True,
                    }
                else:
                    snap = snap_val if isinstance(snap_val, dict) else {"raw": snap_val}
                    if dl.expired():
                        fail_closed = True
                        errors.append("tick_deadline_after_snapshot")
                        decision_key = "deadline_exceeded"
                        decision_reason = "tick_deadline_after_snapshot"
                        plan = {
                            "schema": "decision_plan_v1",
                            "actions": [],
                            "decision_key": decision_key,
                            "reason": decision_reason,
                            "fail_closed": True,
                        }
                    else:
                        # 3) decide
                        ok, plan_val, err = _safe_call(
                            "decide", ads["decide"], snap, state, cfg
                        )
                        if not ok:
                            fail_closed = True
                            errors.append(err or "decide_failed")
                            decision_key = "adapter_failure"
                            decision_reason = err or "decide_failed"
                            plan = {
                                "schema": "decision_plan_v1",
                                "actions": [],
                                "decision_key": decision_key,
                                "reason": decision_reason,
                                "fail_closed": True,
                            }
                        else:
                            plan = (
                                plan_val
                                if isinstance(plan_val, dict)
                                else {
                                    "schema": "decision_plan_v1",
                                    "actions": [],
                                    "decision_key": "invalid_plan",
                                    "reason": "decide_returned_non_dict",
                                    "fail_closed": True,
                                }
                            )
                            decision_key = str(
                                plan.get("decision_key")
                                or plan.get("key")
                                or "planned"
                            )
                            decision_reason = str(
                                plan.get("reason") or decision_key
                            )
                            if plan.get("fail_closed"):
                                fail_closed = True
                            actions_planned = len(plan.get("actions") or [])

                            # 4) execute (skipped on fail_closed or dry_run for mutation)
                            if fail_closed:
                                exec_result = {
                                    "schema": "executor_result_v1",
                                    "applied": [],
                                    "skipped": list(plan.get("actions") or []),
                                    "dry_run": dry_run,
                                    "ok": True,
                                    "reason": "fail_closed",
                                }
                            elif dl.expired():
                                fail_closed = True
                                errors.append("tick_deadline_before_execute")
                                decision_key = "deadline_exceeded"
                                decision_reason = "tick_deadline_before_execute"
                                exec_result = {
                                    "schema": "executor_result_v1",
                                    "applied": [],
                                    "skipped": list(plan.get("actions") or []),
                                    "dry_run": dry_run,
                                    "ok": False,
                                    "reason": decision_reason,
                                }
                            else:
                                ok, ex_val, err = _safe_call(
                                    "execute",
                                    ads["execute"],
                                    plan,
                                    cfg,
                                    dry_run=dry_run,
                                )
                                if not ok:
                                    fail_closed = True
                                    errors.append(err or "execute_failed")
                                    decision_key = "adapter_failure"
                                    decision_reason = err or "execute_failed"
                                    exec_result = {
                                        "schema": "executor_result_v1",
                                        "applied": [],
                                        "skipped": list(plan.get("actions") or []),
                                        "dry_run": dry_run,
                                        "ok": False,
                                        "reason": decision_reason,
                                    }
                                    actions_applied = 0
                                else:
                                    exec_ok, exec_norm, exec_details, exec_reason = (
                                        _validate_executor_result(
                                            ex_val,
                                            plan.get("actions") or [],
                                            dry_run=dry_run,
                                        )
                                    )
                                    exec_result = exec_norm
                                    if not exec_ok:
                                        # Fail-close; do not retry inside the tick.
                                        # Do not advance uncorroborated / failed actions.
                                        fail_closed = True
                                        decision_key = "executor_failure"
                                        decision_reason = exec_reason
                                        for d in exec_details:
                                            errors.append(str(d))
                                        if exec_reason not in errors:
                                            errors.append(exec_reason)
                                        # Structured failure details stay on exec_result.
                                        applied = exec_result.get("applied") or []
                                        # Count only corroborated applied identities that
                                        # also appear in the plan (mixed-result contract).
                                        # State still will not advance because fail_closed.
                                        planned_ids = {
                                            i
                                            for i in (
                                                _action_identity(a)
                                                for a in (plan.get("actions") or [])
                                            )
                                            if i
                                        }
                                        corroborated = []
                                        if isinstance(applied, list):
                                            for a in applied:
                                                aid = _action_identity(a)
                                                if aid and aid in planned_ids:
                                                    corroborated.append(a)
                                        # Report applied count stays 0 on fail-closed tick
                                        # for outer safety (no successful tick application).
                                        actions_applied = 0
                                        exec_result["ok"] = False
                                        exec_result["reason"] = exec_reason
                                        exec_result["failure_details"] = list(
                                            exec_details
                                        )
                                        exec_result["corroborated_applied"] = (
                                            corroborated
                                        )
                                    else:
                                        applied = exec_result.get("applied") or []
                                        actions_applied = (
                                            len(applied)
                                            if isinstance(applied, list)
                                            else 0
                                        )
                                        if dry_run:
                                            # dry-run must not advance cooldowns/action state
                                            actions_applied = 0
                                            # Normalize applied empty for report semantics.
                                            if exec_result.get("applied"):
                                                exec_result = dict(exec_result)
                                                planned_bucket = list(
                                                    exec_result.get("planned") or []
                                                )
                                                planned_bucket.extend(
                                                    exec_result.get("applied") or []
                                                )
                                                exec_result["planned"] = planned_bucket
                                                exec_result["applied"] = []

        finished = clock()
        finished_iso = datetime.fromtimestamp(finished, tz=timezone.utc).isoformat()

        # --- commit state (not advanced on pure lock contention; we hold lock here) ---
        new_state = dict(state)
        new_state["schema"] = DEFAULT_STATE_SCHEMA
        # evidence digests
        digests = dict(new_state.get("previous_evidence_digests") or {})
        if isinstance(snap, dict) and snap.get("digest"):
            digests["snapshot"] = str(snap.get("digest"))
        if linter_result.get("invoked"):
            digests["linter_stdout"] = _sha256_text(
                str(linter_result.get("stdout") or "")
            )
        digests["decision_key"] = _sha256_text(decision_key)
        new_state["previous_evidence_digests"] = digests
        new_state["last_tick_started_at"] = started_iso
        new_state["last_decision_key"] = decision_key
        new_state["linter_last"] = {
            "ok": linter_result.get("ok"),
            "gate_ok": linter_result.get("gate_ok", linter_result.get("ok")),
            "rc": linter_result.get("rc"),
            "timed_out": bool(linter_result.get("timed_out")),
            "error": linter_result.get("error"),
            "reason": linter_result.get("reason"),
            "critical_count": linter_result.get("critical_count"),
            "finding_count": linter_result.get("finding_count"),
            "duration_s": linter_result.get("duration_s"),
        }
        new_state["tick_count"] = int(new_state.get("tick_count") or 0) + 1

        # Always keep decision-native containers present (even on dry-run/fail-closed).
        if not isinstance(new_state.get("tasks"), dict):
            new_state["tasks"] = {}
        if not isinstance(new_state.get("action_cooldowns"), dict):
            new_state["action_cooldowns"] = {}

        # dry-run: do not advance cooldowns / action_keys / decision observations
        if not dry_run and not fail_closed:
            # merge action keys from exec if provided
            for a in exec_result.get("applied") or []:
                if isinstance(a, dict) and a.get("action_key"):
                    aks = dict(new_state.get("action_keys") or {})
                    aks[str(a["action_key"])] = {
                        "ts": finished_iso,
                        "decision_key": decision_key,
                    }
                    new_state["action_keys"] = aks
            # optional cooldown updates from plan
            for ck, cv in (plan.get("cooldown_updates") or {}).items():
                cds = dict(new_state.get("cooldowns") or {})
                cds[str(ck)] = cv
                new_state["cooldowns"] = cds
            for sk, sv in (plan.get("stall_counter_updates") or {}).items():
                scs = dict(new_state.get("consecutive_stall_counters") or {})
                scs[str(sk)] = sv
                new_state["consecutive_stall_counters"] = scs
            # Merge W2 state_observations into state.tasks for same-sig breaker continuity.
            obs = plan.get("state_observations") or {}
            if isinstance(obs, dict) and obs:
                tasks_map = dict(new_state.get("tasks") or {})
                for key, rec in obs.items():
                    if not isinstance(rec, dict):
                        continue
                    prev = dict(tasks_map.get(str(key)) or {})
                    prev_obs = list(prev.get("observations") or [])
                    if isinstance(rec.get("observations"), list):
                        prev_obs = list(rec.get("observations") or [])
                    else:
                        prev_obs.append(dict(rec))
                    prev_obs = prev_obs[-8:]
                    merged = dict(prev)
                    merged.update(rec)
                    merged["observations"] = prev_obs
                    tasks_map[str(key)] = merged
                new_state["tasks"] = tasks_map
            # Mirror decision cooldowns for W2 prior_state.action_cooldowns
            acd = dict(new_state.get("action_cooldowns") or {})
            for a in plan.get("actions") or []:
                if not isinstance(a, dict) or not a.get("action_key"):
                    continue
                try:
                    cd_s = int(a.get("cooldown_seconds") or 0)
                except (TypeError, ValueError):
                    cd_s = 0
                # Approximate now_ns from finished wall clock.
                try:
                    now_ns_approx = int(finished * 1_000_000_000)
                except Exception:
                    now_ns_approx = 0
                acd[str(a["action_key"])] = {
                    "last_ns": now_ns_approx,
                    "until_ns": now_ns_approx + cd_s * 1_000_000_000 if cd_s > 0 else now_ns_approx,
                    "action": a.get("action"),
                    "task_id": a.get("task_id"),
                }
            new_state["action_cooldowns"] = acd
            try:
                new_state["last_tick_ns"] = int(finished * 1_000_000_000)
            except Exception:
                pass
            new_state["last_completed_tick"] = finished_iso
        elif dry_run:
            # preserve prior cooldowns/action_keys; still record last tick meta
            new_state["last_completed_tick"] = finished_iso
            new_state["notes"] = list(new_state.get("notes") or []) + [
                "dry_run_no_action_state_advance"
            ]
        else:
            # fail_closed: still record completion meta, no action advance
            new_state["last_completed_tick"] = finished_iso

        if cfg.get("policy_digest"):
            new_state["policy_digest"] = cfg.get("policy_digest")
        if cfg.get("schema_digest"):
            new_state["schema_digest"] = cfg.get("schema_digest")

        report = {
            "schema": DEFAULT_REPORT_SCHEMA,
            "generated_at": finished_iso,
            "decision": {
                "key": decision_key,
                "reason": decision_reason,
                "actions_planned": actions_planned,
                "actions_applied": actions_applied,
                "fail_closed": fail_closed,
            },
            "decision_key": decision_key,
            "rc": rc,
            "dry_run": dry_run,
            "lock": {"path": str(lock_path), "acquired": True},
            "linter": {
                "invoked": bool(linter_result.get("invoked")),
                "ok": linter_result.get("ok"),
                "gate_ok": linter_result.get("gate_ok", linter_result.get("ok")),
                "rc": linter_result.get("rc"),
                "timed_out": bool(linter_result.get("timed_out")),
                "error": linter_result.get("error"),
                "reason": linter_result.get("reason"),
                "critical_count": linter_result.get("critical_count"),
                "finding_count": linter_result.get("finding_count"),
                "executed": linter_result.get("executed"),
                "duration_s": linter_result.get("duration_s"),
                "cmd": linter_result.get("cmd") if cfg.get("include_linter_cmd") else None,
            },
            "critical_count": linter_result.get("critical_count"),
            "tick": {
                "started_at": started_iso,
                "finished_at": finished_iso,
                "duration_s": round(finished - started, 4),
                "deadline_s": deadline_s,
            },
            "fail_closed": fail_closed,
            "errors": errors[:20],
            "snapshot_digest": (snap or {}).get("digest"),
            "executor": {
                "ok": exec_result.get("ok"),
                "applied_count": actions_applied,
                "dry_run": dry_run,
                "reason": exec_result.get("reason"),
                "failure_details": exec_result.get("failure_details"),
                "denied": exec_result.get("denied"),
                "errors": exec_result.get("errors"),
            },
            "state_corrupt_recovered": bool(corrupt_diag),
        }

        # validate then atomic write state+report only after complete tick
        state_errs = validate_state(new_state)
        report_errs = validate_report(report)
        if state_errs or report_errs:
            errors.extend(state_errs + report_errs)
            report["errors"] = errors[:20]
            report["fail_closed"] = True
            # still try to write report for diagnostics; skip invalid state
            try:
                if not report_errs:
                    atomic_write_json(report_path, report)
            except Exception as wexc:
                errors.append(f"report_write_failed:{wexc}")
            rc = 1
        else:
            try:
                atomic_write_json(state_path, new_state)
                atomic_write_json(report_path, report)
            except Exception as wexc:
                errors.append(f"commit_failed:{wexc}")
                rc = 1

        append_audit(
            audit_path,
            {
                "ts": finished_iso,
                "decision_key": decision_key,
                "action_key": (
                    (exec_result.get("applied") or [{}])[0].get("action_key")
                    if (exec_result.get("applied") or [])
                    else None
                ),
                "reason": decision_reason,
                "dry_run": dry_run,
                "fail_closed": fail_closed,
                "rc": rc,
                "actions_planned": actions_planned,
                "actions_applied": actions_applied,
                "linter_ok": linter_result.get("ok"),
                "errors": errors[:5],
                # deliberate secret-looking field to prove redaction in tests when injected
                **(
                    {"token": cfg["audit_probe_token"]}
                    if cfg.get("audit_probe_token")
                    else {}
                ),
            },
            max_bytes=int(cfg.get("audit_max_bytes") or DEFAULT_AUDIT_MAX_BYTES),
            keep=int(cfg.get("audit_keep") or DEFAULT_AUDIT_KEEP),
        )
    finally:
        lock.release()

    # quiet routine stdout/stderr
    if not quiet and cfg.get("print_report"):
        # only explicit non-quiet CLI modes
        pass
    return rc


def load_config(path: Optional[str | Path]) -> Dict[str, Any]:
    if not path:
        return {}
    p = Path(path)
    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("config must be a JSON object")
    return data


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Anti-stall hygiene tick runner (staging W4)")
    ap.add_argument("--config", default="", help="JSON config path")
    ap.add_argument("--dry-run", action="store_true", help="read-only boards; no action state advance")
    ap.add_argument("--self-test", action="store_true", help="run built-in self checks and exit")
    ap.add_argument("--var-dir", default="", help="override var/state/report/audit directory")
    ap.add_argument("--lock-path", default="", help="override lock path")
    ap.add_argument("--print-success", action="store_true", help="emit terminal SUCCESS line (explicit)")
    return ap


def self_test() -> int:
    """Minimal built-in checks without pytest (used by --self-test)."""
    import tempfile

    failures = 0
    with tempfile.TemporaryDirectory(prefix="hygiene_runner_selftest_") as td:
        td_path = Path(td)
        lock_a = td_path / "hygiene.lock"
        # lock acquire/release
        l1 = acquire_tick_lock(lock_a)
        if not l1.acquired:
            print("FAIL acquire", file=sys.stderr)
            failures += 1
        l2 = acquire_tick_lock(lock_a)
        if l2.acquired:
            print("FAIL contention should not acquire", file=sys.stderr)
            failures += 1
            l2.release()
        l1.release()
        l3 = acquire_tick_lock(lock_a)
        if not l3.acquired:
            print("FAIL reacquire", file=sys.stderr)
            failures += 1
        l3.release()

        # atomic write
        p = td_path / "x.json"
        atomic_write_json(p, {"a": 1})
        if json.loads(p.read_text()) != {"a": 1}:
            print("FAIL atomic_write", file=sys.stderr)
            failures += 1

        # run_tick happy path with fake linter
        calls = {"lint": 0}

        def fake_lint(cfg: Mapping[str, Any]) -> dict:
            calls["lint"] += 1
            return {
                "ok": True,
                "rc": 0,
                "error": None,
                "stdout": "plan_lint findings=0",
                "stderr": "",
                "duration_s": 0.01,
                "cmd": ["fake"],
                "timed_out": False,
            }

        cfg = {
            "var_dir": str(td_path / "var"),
            "lock_path": str(td_path / "var" / "hygiene.lock"),
            "state_path": str(td_path / "var" / "state.json"),
            "report_path": str(td_path / "var" / "report.json"),
            "audit_path": str(td_path / "var" / "audit.jsonl"),
            "dry_run": True,
            "linter_runner": fake_lint,
            "tick_deadline_s": 30,
            "quiet": True,
        }
        rc = run_tick(cfg, adapters=None, clock=time.time)
        if rc != 0 or calls["lint"] != 1:
            print(f"FAIL run_tick rc={rc} lint_calls={calls['lint']}", file=sys.stderr)
            failures += 1
        if not Path(cfg["report_path"]).is_file() or not Path(cfg["state_path"]).is_file():
            print("FAIL missing state/report", file=sys.stderr)
            failures += 1

    if failures:
        return 1
    # explicit terminal success only for --self-test
    print("SELF_TEST_OK")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    ap = build_arg_parser()
    args = ap.parse_args(argv)
    if args.self_test:
        return self_test()

    cfg = load_config(args.config) if args.config else {}
    if args.dry_run:
        cfg["dry_run"] = True
    if args.var_dir:
        cfg["var_dir"] = args.var_dir
        cfg.setdefault("state_path", str(Path(args.var_dir) / "state.json"))
        cfg.setdefault("report_path", str(Path(args.var_dir) / "report.json"))
        cfg.setdefault("audit_path", str(Path(args.var_dir) / "audit.jsonl"))
        cfg.setdefault("lock_path", str(Path(args.var_dir) / "hygiene.lock"))
    if args.lock_path:
        cfg["lock_path"] = args.lock_path
    cfg.setdefault("quiet", True)

    rc = run_tick(cfg)
    if args.print_success and rc == 0:
        # explicit terminal SUCCESS requested by CLI only
        print("SUCCESS")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
