#!/usr/bin/env python3
"""Bounded process-external any-terminal Winline shadow event watchdog.

Polls the atomic shadow evidence artifact for the first NEW eligible terminal
rewrite produced by the current cyberscore.service MainPID generation, then
writes one trigger JSON and exits.

stdout is always empty. Diagnostics go to stderr only.
Exit codes:
  0 — eligible rewrite observed; trigger + durable state written
  2 — deadline with no new eligible rewrite
  3 — hard health failure (service inactive / no MainPID / generation change)
  4 — invalid args or invalid durable-state schema
  5 — atomic write failure (hard; never report success)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Production defaults (CLI may override for bounded tests)
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_PATH = (
    _REPO_ROOT / ".hermes" / "runtime" / "winline-shadow" / "latest.json"
)
DEFAULT_STATE_PATH = (
    _REPO_ROOT / ".hermes" / "runtime" / "winline-shadow" / "watchdog_state.json"
)
DEFAULT_TRIGGER_PATH = (
    _REPO_ROOT
    / ".hermes"
    / "staging"
    / "winline-shadow-live-trigger"
    / "trigger.json"
)
DEFAULT_POLL_INTERVAL = 15.0
DEFAULT_DEADLINE_SECONDS = 1200.0
DEFAULT_UNIT = "cyberscore.service"

ARTIFACT_SCHEMA = "winline_shadow_probe.v1"
STATE_SCHEMA = "winline_shadow_watchdog_state.v1"
TRIGGER_SCHEMA = "winline_shadow_watchdog_trigger.v1"

EXIT_OK = 0
EXIT_TIMEOUT = 2
EXIT_HEALTH = 3
EXIT_BAD_ARGS = 4
EXIT_WRITE_FAIL = 5

_PARSER_ERROR_TOKENS = frozenset(
    {
        "acquisition_failed",
        "acquisition_error",
        "evidence_missing",
        "seam_exception",
        "seam_rc_missing",
        "observation_missing_or_malformed",
        "schema_version_mismatch",
        "verdict_not_pass",
        "not_pass_verdict",
    }
)
_MARKET_REASON_MARKERS = (
    "market_closed",
    "odds_missing",
    "p1_odds_missing",
    "p2_odds_missing",
    "p1_odds_invalid",
    "p2_odds_invalid",
    "p1_odds_missing_or_malformed",
    "p2_odds_missing_or_malformed",
)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ServiceSnapshot:
    active: bool
    main_pid: Optional[int]
    start_snapshot: Optional[str]


@dataclass(frozen=True)
class ArtifactIdentity:
    producer_pid: int
    canonical_key: str
    attempt_finished_at: float
    artifact_sha256: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "producer_pid": int(self.producer_pid),
            "canonical_key": str(self.canonical_key),
            "attempt_finished_at": float(self.attempt_finished_at),
            "artifact_sha256": str(self.artifact_sha256),
        }

    def key(self) -> Tuple[int, str, float, str]:
        return (
            int(self.producer_pid),
            str(self.canonical_key),
            float(self.attempt_finished_at),
            str(self.artifact_sha256),
        )


@dataclass(frozen=True)
class WatchConfig:
    artifact_path: Path
    state_path: Path
    trigger_path: Path
    poll_interval: float
    deadline_seconds: float
    unit: str = DEFAULT_UNIT


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def _eprint(msg: str) -> None:
    sys.stderr.write(msg.rstrip() + "\n")
    sys.stderr.flush()


def sha256_full_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Atomic write via adjacent temp + fsync + os.replace."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=str(target.parent),
    )
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, target)
    except Exception:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except OSError:
            pass
        raise


def atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    data = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
    atomic_write_bytes(path, data)


def read_artifact_atomic(
    path: Path,
) -> Tuple[Optional[bytes], Optional[Dict[str, Any]], Optional[str]]:
    """Read full final-path bytes then decode JSON.

    Returns (raw_bytes, dict, error_token).
    Missing / replace / decode races return (None, None, token) — never raise.
    """
    p = Path(path)
    try:
        if not p.exists():
            return None, None, "missing"
    except OSError:
        return None, None, "stat_race"
    try:
        raw = p.read_bytes()
    except OSError:
        return None, None, "read_race"
    if not raw:
        return None, None, "empty"
    try:
        text = raw.decode("utf-8")
    except UnicodeError:
        return None, None, "unicode"
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        return None, None, "json"
    if not isinstance(obj, dict):
        return None, None, "not_dict"
    return raw, obj, None


# ---------------------------------------------------------------------------
# Identity / eligibility / classification
# ---------------------------------------------------------------------------


def identity_from_artifact(rec: Dict[str, Any], raw: bytes) -> ArtifactIdentity:
    return ArtifactIdentity(
        producer_pid=int(rec["producer_pid"]),
        canonical_key=str(rec["canonical_key"]),
        attempt_finished_at=float(rec["attempt_finished_at"]),
        artifact_sha256=sha256_full_bytes(raw),
    )


def _is_finite_number(value: Any) -> bool:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(f)


def _is_finite_odds(value: Any) -> bool:
    if not _is_finite_number(value):
        return False
    return float(value) > 1.0


def _reasons_list(rec: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for key in ("failure_reasons", "controller_failure_reasons"):
        val = rec.get(key)
        if isinstance(val, list):
            for item in val:
                if item is not None:
                    out.append(str(item))
        elif val is not None:
            out.append(str(val))
    return out


def structural_eligible(
    rec: Dict[str, Any],
    *,
    main_pid: int,
) -> bool:
    """True when observation is a genuine parsed current map for this PID.

    Classification (fresh / market_closed / parser_error) does NOT gate this.
    STAR / selected_side do NOT gate this.
    """
    if not isinstance(rec, dict):
        return False
    if str(rec.get("schema_version") or "") != ARTIFACT_SCHEMA:
        return False
    key = rec.get("canonical_key")
    if key is None or not str(key).strip():
        return False
    try:
        map_num = int(rec.get("map_num"))
    except (TypeError, ValueError):
        return False
    if not (1 <= map_num <= 5):
        return False
    p1 = str(rec.get("p1_team") if rec.get("p1_team") is not None else rec.get("team1") or "").strip()
    p2 = str(rec.get("p2_team") if rec.get("p2_team") is not None else rec.get("team2") or "").strip()
    # Prefer explicit p1_team/p2_team when present (even empty → fail)
    if "p1_team" in rec:
        p1 = str(rec.get("p1_team") or "").strip()
    if "p2_team" in rec:
        p2 = str(rec.get("p2_team") or "").strip()
    if not p1 or not p2 or p1 == p2:
        return False
    if not _is_finite_number(rec.get("attempt_finished_at")):
        return False
    try:
        prod = int(rec.get("producer_pid"))
    except (TypeError, ValueError):
        return False
    if prod != int(main_pid):
        return False
    return True


def classify_observation(rec: Dict[str, Any]) -> str:
    """Non-gating classification of a structurally eligible terminal rewrite."""
    if _is_finite_odds(rec.get("p1_odds")) and _is_finite_odds(rec.get("p2_odds")):
        return "fresh_p1p2"

    reasons = [r.lower() for r in _reasons_list(rec)]
    source = str(rec.get("source") or "").lower()
    market_closed = rec.get("market_closed") is True
    marketish = market_closed or "market" in source or "missing" in source
    if not marketish:
        for r in reasons:
            if any(m in r for m in _MARKET_REASON_MARKERS):
                marketish = True
                break
    # Prefer market label when odds are simply missing/closed without parser tokens
    parserish = False
    if rec.get("acquisition_error") not in (None, "", False):
        parserish = True
    try:
        seam_rc = rec.get("seam_rc")
        if seam_rc is not None and int(seam_rc) not in (0,):
            # nonzero seam may still be market path; only force parser if
            # acquisition-like tokens appear or odds not simply market-closed
            pass
    except (TypeError, ValueError):
        pass
    for r in reasons:
        token = r.split(":", 1)[0]
        if token in _PARSER_ERROR_TOKENS or token.startswith("seam_rc_"):
            parserish = True
        if "acquisition" in r or "evidence_missing" in r or "seam_exception" in r:
            parserish = True

    if marketish and not parserish:
        return "market_closed_or_missing"
    if parserish:
        return "parser_error"
    # Closed/missing odds without explicit parser → market class
    if not _is_finite_odds(rec.get("p1_odds")) or not _is_finite_odds(rec.get("p2_odds")):
        return "market_closed_or_missing"
    return "fresh_p1p2"


# ---------------------------------------------------------------------------
# Durable state
# ---------------------------------------------------------------------------


def parse_state(payload: Any) -> Tuple[Optional[ArtifactIdentity], Optional[str]]:
    """Validate durable state schema. Returns (identity_or_None, error_or_None)."""
    if not isinstance(payload, dict):
        return None, "state_not_dict"
    if str(payload.get("schema_version") or "") != STATE_SCHEMA:
        return None, "state_schema"
    last = payload.get("last_identity")
    if last is None:
        return None, None
    if not isinstance(last, dict):
        return None, "identity_not_dict"
    required = ("producer_pid", "canonical_key", "attempt_finished_at", "artifact_sha256")
    for k in required:
        if k not in last:
            return None, f"identity_missing_{k}"
    try:
        pid = int(last["producer_pid"])
    except (TypeError, ValueError):
        return None, "identity_pid"
    key = last.get("canonical_key")
    if key is None or not str(key).strip():
        return None, "identity_key"
    if not _is_finite_number(last.get("attempt_finished_at")):
        return None, "identity_finish"
    sha = last.get("artifact_sha256")
    if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{64}", sha.lower() if isinstance(sha, str) else ""):
        # allow uppercase hex too
        if not (isinstance(sha, str) and re.fullmatch(r"[0-9a-fA-F]{64}", sha)):
            return None, "identity_sha"
    return (
        ArtifactIdentity(
            producer_pid=pid,
            canonical_key=str(key),
            attempt_finished_at=float(last["attempt_finished_at"]),
            artifact_sha256=str(sha).lower(),
        ),
        None,
    )


def load_baseline_state(path: Path) -> Tuple[Optional[ArtifactIdentity], Optional[str]]:
    p = Path(path)
    if not p.exists():
        return None, None
    raw, obj, err = read_artifact_atomic(p)
    if err is not None:
        return None, f"state_read_{err}"
    return parse_state(obj)


def build_state_payload(identity: ArtifactIdentity, updated_at: float) -> Dict[str, Any]:
    return {
        "schema_version": STATE_SCHEMA,
        "updated_at": float(updated_at),
        "last_identity": identity.as_dict(),
    }


def build_trigger_payload(
    *,
    baseline: Optional[ArtifactIdentity],
    observed: ArtifactIdentity,
    main_pid: int,
    start_snapshot: Optional[str],
    classification: str,
    watch_started_at: float,
    observed_at: float,
    unit: str,
) -> Dict[str, Any]:
    return {
        "schema_version": TRIGGER_SCHEMA,
        "unit": unit,
        "main_pid": int(main_pid),
        "main_pid_start_snapshot": start_snapshot,
        "classification": classification,
        "baseline_identity": None if baseline is None else baseline.as_dict(),
        "observed_identity": observed.as_dict(),
        "watch_started_at": float(watch_started_at),
        "observed_at": float(observed_at),
    }


# ---------------------------------------------------------------------------
# Service probe
# ---------------------------------------------------------------------------


def probe_cyberscore_service(unit: str = DEFAULT_UNIT) -> ServiceSnapshot:
    """Readonly systemctl show for ActiveState + MainPID + start timestamp."""
    try:
        proc = subprocess.run(
            [
                "systemctl",
                "show",
                unit,
                "-p",
                "ActiveState",
                "-p",
                "MainPID",
                "-p",
                "ExecMainStartTimestampMonotonic",
                "-p",
                "ExecMainStartTimestamp",
                "--no-pager",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        _eprint(f"service_probe_error:{exc}")
        return ServiceSnapshot(active=False, main_pid=None, start_snapshot=None)

    fields: Dict[str, str] = {}
    for line in (proc.stdout or "").splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            fields[k.strip()] = v.strip()
    active = fields.get("ActiveState", "") == "active"
    try:
        main_pid = int(fields.get("MainPID") or "0")
    except ValueError:
        main_pid = 0
    if main_pid <= 0:
        main_pid_opt: Optional[int] = None
    else:
        main_pid_opt = main_pid
    snap = (
        fields.get("ExecMainStartTimestampMonotonic")
        or fields.get("ExecMainStartTimestamp")
        or None
    )
    if not active:
        return ServiceSnapshot(active=False, main_pid=main_pid_opt, start_snapshot=snap)
    if main_pid_opt is None:
        return ServiceSnapshot(active=True, main_pid=None, start_snapshot=snap)
    return ServiceSnapshot(active=True, main_pid=main_pid_opt, start_snapshot=snap)


def bind_service_probe(
    main_pid: int,
    start_snapshot: Optional[str],
) -> Callable[[], ServiceSnapshot]:
    """Test/CLI override: fixed generation unless PID is forced to 0/inactive."""

    def _probe() -> ServiceSnapshot:
        if int(main_pid) <= 0:
            return ServiceSnapshot(active=False, main_pid=None, start_snapshot=None)
        return ServiceSnapshot(
            active=True,
            main_pid=int(main_pid),
            start_snapshot=start_snapshot,
        )

    return _probe


# ---------------------------------------------------------------------------
# Core loop
# ---------------------------------------------------------------------------


def run_watch(
    cfg: WatchConfig,
    *,
    service_probe: Optional[Callable[[], ServiceSnapshot]] = None,
    time_fn: Callable[[], float] = time.time,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> int:
    probe = service_probe or (lambda: probe_cyberscore_service(cfg.unit))
    started = float(time_fn())
    deadline = started + float(cfg.deadline_seconds)

    baseline_id, state_err = load_baseline_state(cfg.state_path)
    if state_err is not None:
        _eprint(f"invalid_state:{state_err}")
        return EXIT_BAD_ARGS

    snap0 = probe()
    if (not snap0.active) or snap0.main_pid is None or int(snap0.main_pid) <= 0:
        _eprint("health:service_inactive_or_no_mainpid")
        return EXIT_HEALTH
    bind_pid = int(snap0.main_pid)
    bind_start = snap0.start_snapshot

    while True:
        now = float(time_fn())
        if now >= deadline:
            _eprint("timeout:no_eligible_rewrite")
            return EXIT_TIMEOUT

        snap = probe()
        if (not snap.active) or snap.main_pid is None or int(snap.main_pid) <= 0:
            _eprint("health:service_inactive_or_no_mainpid")
            return EXIT_HEALTH
        if int(snap.main_pid) != bind_pid:
            _eprint(f"health:pid_rollover:{bind_pid}->{snap.main_pid}")
            return EXIT_HEALTH
        if (
            bind_start is not None
            and snap.start_snapshot is not None
            and str(snap.start_snapshot) != str(bind_start)
        ):
            _eprint("health:start_snapshot_changed")
            return EXIT_HEALTH

        raw, rec, err = read_artifact_atomic(cfg.artifact_path)
        if err is not None or raw is None or rec is None:
            # transient — re-poll, never stdout
            remaining = deadline - float(time_fn())
            if remaining <= 0:
                _eprint("timeout:no_eligible_rewrite")
                return EXIT_TIMEOUT
            sleep_fn(min(float(cfg.poll_interval), max(remaining, 0.0)))
            continue

        if not structural_eligible(rec, main_pid=bind_pid):
            remaining = deadline - float(time_fn())
            if remaining <= 0:
                _eprint("timeout:no_eligible_rewrite")
                return EXIT_TIMEOUT
            sleep_fn(min(float(cfg.poll_interval), max(remaining, 0.0)))
            continue

        try:
            observed = identity_from_artifact(rec, raw)
        except (TypeError, ValueError, KeyError):
            remaining = deadline - float(time_fn())
            if remaining <= 0:
                _eprint("timeout:no_eligible_rewrite")
                return EXIT_TIMEOUT
            sleep_fn(min(float(cfg.poll_interval), max(remaining, 0.0)))
            continue

        if baseline_id is not None and observed.key() == baseline_id.key():
            remaining = deadline - float(time_fn())
            if remaining <= 0:
                _eprint("timeout:no_eligible_rewrite")
                return EXIT_TIMEOUT
            sleep_fn(min(float(cfg.poll_interval), max(remaining, 0.0)))
            continue

        # NEW eligible terminal rewrite
        classification = classify_observation(rec)
        observed_at = float(time_fn())
        trigger = build_trigger_payload(
            baseline=baseline_id,
            observed=observed,
            main_pid=bind_pid,
            start_snapshot=bind_start,
            classification=classification,
            watch_started_at=started,
            observed_at=observed_at,
            unit=cfg.unit,
        )
        state_payload = build_state_payload(observed, observed_at)
        try:
            # trigger first, then durable state (both atomic). If state fails
            # after trigger, still fail hard — do not report success.
            atomic_write_json(cfg.trigger_path, trigger)
            atomic_write_json(cfg.state_path, state_payload)
        except Exception as exc:
            _eprint(f"write_fail:{exc}")
            return EXIT_WRITE_FAIL
        return EXIT_OK


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="winline_shadow_event_watchdog",
        description="Bounded any-terminal Winline shadow event watcher",
    )
    p.add_argument(
        "--artifact",
        default=str(DEFAULT_ARTIFACT_PATH),
        help="Read-only path to latest shadow JSON",
    )
    p.add_argument(
        "--state",
        default=str(DEFAULT_STATE_PATH),
        help="Durable watchdog state JSON (atomic write)",
    )
    p.add_argument(
        "--trigger",
        default=str(DEFAULT_TRIGGER_PATH),
        help="One-shot trigger JSON output (atomic write)",
    )
    p.add_argument(
        "--poll-interval",
        type=float,
        default=DEFAULT_POLL_INTERVAL,
        help=f"Poll interval seconds (default {DEFAULT_POLL_INTERVAL})",
    )
    p.add_argument(
        "--deadline-seconds",
        type=float,
        default=DEFAULT_DEADLINE_SECONDS,
        help=f"Total watch budget seconds (default {DEFAULT_DEADLINE_SECONDS})",
    )
    p.add_argument(
        "--unit",
        default=DEFAULT_UNIT,
        help="systemd unit to bind MainPID against",
    )
    p.add_argument(
        "--bind-main-pid",
        type=int,
        default=None,
        help="Test override: bind to this MainPID instead of systemctl",
    )
    p.add_argument(
        "--bind-start-snapshot",
        default=None,
        help="Test override: start snapshot string paired with --bind-main-pid",
    )
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Sequence[str]] = None) -> int:
    # Keep stdout empty under all paths.
    try:
        args = parse_args(argv)
    except SystemExit as se:
        # argparse already wrote to stderr; map to exit 4
        code = se.code
        if code in (0, None):
            return EXIT_OK
        return EXIT_BAD_ARGS
    except Exception as exc:
        _eprint(f"args_error:{exc}")
        return EXIT_BAD_ARGS

    try:
        poll = float(args.poll_interval)
        deadline = float(args.deadline_seconds)
    except (TypeError, ValueError) as exc:
        _eprint(f"args_error:{exc}")
        return EXIT_BAD_ARGS
    if not math.isfinite(poll) or poll <= 0:
        _eprint("args_error:poll_interval_must_be_positive")
        return EXIT_BAD_ARGS
    if not math.isfinite(deadline) or deadline <= 0:
        _eprint("args_error:deadline_must_be_positive")
        return EXIT_BAD_ARGS

    cfg = WatchConfig(
        artifact_path=Path(args.artifact),
        state_path=Path(args.state),
        trigger_path=Path(args.trigger),
        poll_interval=poll,
        deadline_seconds=deadline,
        unit=str(args.unit),
    )

    service_probe: Optional[Callable[[], ServiceSnapshot]] = None
    if args.bind_main_pid is not None:
        service_probe = bind_service_probe(
            int(args.bind_main_pid),
            args.bind_start_snapshot,
        )

    try:
        return int(
            run_watch(
                cfg,
                service_probe=service_probe,
            )
        )
    except Exception as exc:
        _eprint(f"fatal:{exc}")
        return EXIT_HEALTH


if __name__ == "__main__":
    # Never print to stdout — exit code is the only success signal.
    raise SystemExit(main())
