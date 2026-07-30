"""Deterministic no-sleep Winline shadow activation controller.

Owns per-canonical-key eligibility, injected-seam invocation, evidence
validation, and atomic enrichment. Never imports the production main module.
Never sleeps, spawns, threads, or blocks waiting. Fail-closed for shadow only:
never raises into its caller.
"""
from __future__ import annotations

import json
import math
import os
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

PathLike = Union[str, Path]

# Default terminal evidence path expected by activation tests / live INT.
WINLINE_SHADOW_ACTIVATION_EVIDENCE_PATH: Path = (
    Path(__file__).resolve().parents[1]
    / ".hermes"
    / "runtime"
    / "winline-shadow"
    / "latest.json"
)

# Per-canonical-key in-process state.
_winline_shadow_activation_state: Dict[str, Dict[str, Any]] = {}

# Pending queue for queue/flush production shape.
_winline_shadow_activation_queue: List[Dict[str, Any]] = []

# Backoff ladder (seconds): failure n → delay; capped at 300.
_BACKOFF_LADDER = (30, 60, 120, 240, 300)
_CANDIDATE_ENRICH_DELAY = 30.0
_SCHEMA_VERSION = "winline_shadow_probe.v1"
_SOURCE_EXPECTED = "Winline"


def reset_winline_shadow_activation_state() -> None:
    """Test/helper seam: clear in-process state and queue."""
    _winline_shadow_activation_state.clear()
    _winline_shadow_activation_queue.clear()


def _winline_shadow_activation_backoff_seconds(failure_count: int) -> float:
    if failure_count <= 0:
        return 0.0
    idx = min(int(failure_count), len(_BACKOFF_LADDER)) - 1
    return float(_BACKOFF_LADDER[idx])


def _strip_series(match_key: Any) -> Optional[str]:
    """Stable series portion: strip kills/score suffix and |mapN if present."""
    if match_key is None:
        return None
    raw = str(match_key).strip()
    if not raw:
        return None
    # Already a lifecycle key: series|mapN → series portion only.
    if "|map" in raw:
        series = raw.rsplit("|map", 1)[0].strip()
        return series or None
    # Strip trailing .N score/kills suffix when present (e.g. url.12).
    # Only strip a pure integer tail after the last dot when the base looks like a URL path.
    if "." in raw:
        base, tail = raw.rsplit(".", 1)
        if tail.isdigit() and base:
            return base
    return raw


def _winline_shadow_activation_key(match_key: Any, map_num: Any) -> Optional[str]:
    """Canonical stable-series key: stripped nonempty match_key + |mapN, N in 1..5."""
    try:
        n = int(map_num) if map_num is not None else None
    except (TypeError, ValueError):
        n = None
    if n is None or not (1 <= n <= 5):
        return None
    series = _strip_series(match_key)
    if not series:
        return None
    return f"{series}|map{n}"


def _winline_shadow_activation_selected_side(
    selected_side: Any,
    team1: Any,
    team2: Any,
) -> Optional[str]:
    """Normalize candidate to P1/P2 from ordered teams or explicit P1/P2."""
    if selected_side is None:
        return None
    s = str(selected_side).strip()
    if not s:
        return None
    up = s.upper()
    if up in {"P1", "P2"}:
        return up
    t1 = str(team1 or "").strip()
    t2 = str(team2 or "").strip()
    if t1 and s == t1:
        return "P1"
    if t2 and s == t2:
        return "P2"
    # case-insensitive team match
    if t1 and s.lower() == t1.lower():
        return "P1"
    if t2 and s.lower() == t2.lower():
        return "P2"
    return None


def _state_for(key: str) -> Dict[str, Any]:
    st = _winline_shadow_activation_state.get(key)
    if st is None:
        st = {
            "in_flight": False,
            "terminal_success": False,
            "failure_count": 0,
            "next_eligible_at": 0.0,
            "last_selected_side": None,
            "had_p1p2_pass": False,
            "candidate_enrich_done": False,
            "p1p2_pass_at": None,
        }
        _winline_shadow_activation_state[key] = st
    return st


def _is_finite_odds(value: Any) -> bool:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(f) and f > 1.0


def _atomic_write_json(path: PathLike, payload: Dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    tmp = target.with_suffix(target.suffix + ".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, target)
    except Exception:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass
        raise


def _read_json(path: PathLike) -> Optional[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def _validate_evidence(
    record: Dict[str, Any],
    *,
    map_num: int,
    team1: str,
    team2: str,
    selected_side: Optional[str],
    now: float,
    freshness_limit_seconds: float,
) -> List[str]:
    reasons: List[str] = []
    if record.get("schema_version") != _SCHEMA_VERSION:
        reasons.append("schema_version_mismatch")
    if str(record.get("source") or "") != _SOURCE_EXPECTED:
        reasons.append("source_not_winline")
    try:
        rec_map = int(record.get("map_num"))
    except (TypeError, ValueError):
        rec_map = None
    if rec_map != int(map_num):
        reasons.append("map_num_mismatch")
    if str(record.get("team1") or "") != str(team1):
        reasons.append("team1_mismatch")
    if str(record.get("team2") or "") != str(team2):
        reasons.append("team2_mismatch")
    if not _is_finite_odds(record.get("p1_odds")):
        reasons.append("p1_odds_invalid")
    if not _is_finite_odds(record.get("p2_odds")):
        reasons.append("p2_odds_invalid")

    rec_side_raw = record.get("selected_side")
    rec_side = str(rec_side_raw).strip().upper() if rec_side_raw not in (None, "") else ""
    expected_side = selected_side or ""
    if expected_side:
        if rec_side != expected_side:
            reasons.append("selected_side_mismatch")
        else:
            # exact candidate selected_side -> selected_odds correspondence
            try:
                so = float(record.get("selected_odds"))
            except (TypeError, ValueError):
                so = None
            p1 = record.get("p1_odds")
            p2 = record.get("p2_odds")
            try:
                p1f = float(p1) if p1 is not None else None
            except (TypeError, ValueError):
                p1f = None
            try:
                p2f = float(p2) if p2 is not None else None
            except (TypeError, ValueError):
                p2f = None
            if so is None or not math.isfinite(so):
                reasons.append("selected_odds_invalid")
            elif expected_side == "P1" and (p1f is None or so != p1f):
                reasons.append("selected_odds_mismatch")
            elif expected_side == "P2" and (p2f is None or so != p2f):
                reasons.append("selected_odds_mismatch")
    else:
        # No candidate: selected_odds should be absent/None; empty side ok.
        pass

    # Freshness timestamps must be present and finite. Absolute staleness is
    # enforced by the probe seam; controller only requires usable clocks.
    try:
        observed = float(record.get("observed_at"))
    except (TypeError, ValueError):
        observed = None
    try:
        collected = float(record.get("collected_at"))
    except (TypeError, ValueError):
        collected = None
    if observed is None or not math.isfinite(observed):
        reasons.append("observed_at_invalid")
    if collected is None or not math.isfinite(collected):
        reasons.append("collected_at_invalid")
    _ = freshness_limit_seconds  # probe-owned; kept for signature symmetry
    _ = now
    return reasons


def _winline_shadow_activation_write_fail(
    output_path: PathLike,
    *,
    map_num: Optional[int],
    team1: str,
    team2: str,
    selected_side: Optional[str],
    canonical_key: Optional[str],
    reasons: List[str],
    acquisition_error: Optional[str] = None,
    base: Optional[Dict[str, Any]] = None,
    attempt_started_at: Optional[float] = None,
    attempt_finished_at: Optional[float] = None,
    next_eligible_at: Optional[float] = None,
    controller_outcome: str = "FAIL",
    seam_rc: Optional[int] = None,
) -> None:
    record: Dict[str, Any] = dict(base or {})
    record.setdefault("schema_version", _SCHEMA_VERSION)
    if map_num is not None:
        record["map_num"] = int(map_num)
    record["team1"] = team1
    record["team2"] = team2
    record.setdefault("p1_team", team1)
    record.setdefault("p2_team", team2)
    record.setdefault("source", _SOURCE_EXPECTED)
    record["selected_side"] = selected_side or ""
    record["verdict"] = "FAIL"
    existing_reasons = list(record.get("failure_reasons") or [])
    merged = existing_reasons + [r for r in reasons if r not in existing_reasons]
    record["failure_reasons"] = merged
    if acquisition_error is not None:
        record["acquisition_error"] = acquisition_error
    # controller enrichment (never discard capsule fields)
    if canonical_key is not None:
        record["canonical_key"] = canonical_key
    record["producer_pid"] = os.getpid()
    record["controller_outcome"] = controller_outcome
    if attempt_started_at is not None:
        record["attempt_started_at"] = float(attempt_started_at)
    if attempt_finished_at is not None:
        record["attempt_finished_at"] = float(attempt_finished_at)
    if next_eligible_at is not None:
        record["next_eligible_at"] = float(next_eligible_at)
    record["controller_failure_reasons"] = list(reasons)
    if seam_rc is not None:
        record["seam_rc"] = int(seam_rc)
    if "collected_at" not in record:
        record["collected_at"] = float(attempt_finished_at or time.time())
    if "observed_at" not in record:
        record["observed_at"] = record["collected_at"]
    _atomic_write_json(output_path, record)


def _enrich_pass(
    record: Dict[str, Any],
    *,
    canonical_key: str,
    attempt_started_at: float,
    attempt_finished_at: float,
    next_eligible_at: Optional[float] = None,
) -> Dict[str, Any]:
    out = dict(record)
    out["canonical_key"] = canonical_key
    out["producer_pid"] = os.getpid()
    out["controller_outcome"] = "PASS"
    out["attempt_started_at"] = float(attempt_started_at)
    out["attempt_finished_at"] = float(attempt_finished_at)
    out["validation_reasons"] = []
    out["controller_failure_reasons"] = []
    if next_eligible_at is not None:
        out["next_eligible_at"] = float(next_eligible_at)
    return out


def maybe_run_winline_shadow_activation(
    *,
    match_key: Any,
    map_num: Any,
    team1: Any,
    team2: Any,
    selected_side: Any = None,
    ordinary_path_completed: bool = True,
    now_monotonic: Optional[float] = None,
    run_winline_shadow_request: Optional[Callable[..., int]] = None,
    output_path: Optional[PathLike] = None,
    freshness_limit_seconds: float = 15.0,
    now: Optional[float] = None,
    no_odds_active: bool = True,
) -> Optional[int]:
    """Invoke injected shadow seam once when eligible; never raise.

    Returns:
      int rc-like outcome (0 PASS, nonzero FAIL) when an attempt was made;
      None when skipped (ineligible / dedup / backoff / missing seam).
    """
    try:
        return _maybe_run_winline_shadow_activation_impl(
            match_key=match_key,
            map_num=map_num,
            team1=team1,
            team2=team2,
            selected_side=selected_side,
            ordinary_path_completed=ordinary_path_completed,
            now_monotonic=now_monotonic,
            run_winline_shadow_request=run_winline_shadow_request,
            output_path=output_path,
            freshness_limit_seconds=freshness_limit_seconds,
            now=now,
            no_odds_active=no_odds_active,
        )
    except Exception:
        # Absolute fail-closed: never raise into caller.
        return 2


def _maybe_run_winline_shadow_activation_impl(
    *,
    match_key: Any,
    map_num: Any,
    team1: Any,
    team2: Any,
    selected_side: Any,
    ordinary_path_completed: bool,
    now_monotonic: Optional[float],
    run_winline_shadow_request: Optional[Callable[..., int]],
    output_path: Optional[PathLike],
    freshness_limit_seconds: float,
    now: Optional[float],
    no_odds_active: bool,
) -> Optional[int]:
    if not ordinary_path_completed:
        return None
    if not no_odds_active:
        return None
    if run_winline_shadow_request is None or not callable(run_winline_shadow_request):
        return None

    key = _winline_shadow_activation_key(match_key, map_num)
    if key is None:
        return None

    try:
        resolved_map = int(map_num)
    except (TypeError, ValueError):
        return None

    t1 = str(team1 or "")
    t2 = str(team2 or "")
    side = _winline_shadow_activation_selected_side(selected_side, t1, t2)
    mono = float(now_monotonic if now_monotonic is not None else time.monotonic())
    wall = float(now if now is not None else time.time())
    out_path = Path(output_path) if output_path is not None else Path(WINLINE_SHADOW_ACTIVATION_EVIDENCE_PATH)

    st = _state_for(key)

    if st.get("in_flight"):
        return None

    # Terminal success dedup with one delayed candidate enrichment window.
    if st.get("terminal_success"):
        if (
            st.get("had_p1p2_pass")
            and not st.get("candidate_enrich_done")
            and side in {"P1", "P2"}
            and st.get("last_selected_side") in (None, "")
        ):
            p1p2_at = float(st.get("p1p2_pass_at") or 0.0)
            if mono >= p1p2_at + _CANDIDATE_ENRICH_DELAY:
                # allow one enrichment attempt
                pass
            else:
                return None
        else:
            return None

    next_at = float(st.get("next_eligible_at") or 0.0)
    if mono < next_at:
        return None

    # Claim in-flight
    st["in_flight"] = True
    attempt_started = wall
    seam_rc: Optional[int] = None
    acquisition_error: Optional[str] = None
    record: Optional[Dict[str, Any]] = None

    try:
        try:
            seam_rc = int(
                run_winline_shadow_request(
                    match_key=str(match_key),
                    map_num=resolved_map,
                    team1=t1,
                    team2=t2,
                    selected_side=side if side is not None else "",
                    output_path=out_path,
                    freshness_limit_seconds=float(freshness_limit_seconds),
                    no_odds_active=True,
                    now=wall,
                )
            )
        except Exception as exc:
            acquisition_error = f"{type(exc).__name__}: {exc}"
            seam_rc = 2
            record = None
        else:
            record = _read_json(out_path)

        attempt_finished = float(now if now is not None else time.time())
        # If wall clock not injected, keep wall-based finish close to start for tests.
        if now is not None:
            attempt_finished = wall

        fail_reasons: List[str] = []
        controller_pass = False

        if acquisition_error is not None:
            fail_reasons.append("seam_exception")
            fail_reasons.append(acquisition_error)
        elif seam_rc is None:
            fail_reasons.append("seam_rc_missing")
        elif int(seam_rc) != 0:
            fail_reasons.append(f"seam_rc_{int(seam_rc)}")
            if record is None:
                fail_reasons.append("evidence_missing")
        elif record is None:
            fail_reasons.append("evidence_missing")
        else:
            # rc==0: require terminal PASS record satisfying all checks
            if str(record.get("verdict") or "") != "PASS":
                fail_reasons.append("verdict_not_pass")
            v_reasons = _validate_evidence(
                record,
                map_num=resolved_map,
                team1=t1,
                team2=t2,
                selected_side=side,
                now=wall,
                freshness_limit_seconds=float(freshness_limit_seconds),
            )
            fail_reasons.extend(v_reasons)
            if not fail_reasons:
                controller_pass = True

        if controller_pass and record is not None:
            enriched = _enrich_pass(
                record,
                canonical_key=key,
                attempt_started_at=attempt_started,
                attempt_finished_at=attempt_finished,
            )
            try:
                _atomic_write_json(out_path, enriched)
            except Exception as write_exc:
                # write failure → shadow FAIL + backoff
                fail_reasons = [f"evidence_write_failed: {write_exc}"]
                controller_pass = False
                st["failure_count"] = int(st.get("failure_count") or 0) + 1
                delay = _winline_shadow_activation_backoff_seconds(st["failure_count"])
                st["next_eligible_at"] = mono + delay
                st["terminal_success"] = False
                try:
                    _winline_shadow_activation_write_fail(
                        out_path,
                        map_num=resolved_map,
                        team1=t1,
                        team2=t2,
                        selected_side=side,
                        canonical_key=key,
                        reasons=fail_reasons,
                        base=record,
                        attempt_started_at=attempt_started,
                        attempt_finished_at=attempt_finished,
                        next_eligible_at=st["next_eligible_at"],
                        seam_rc=seam_rc,
                    )
                except Exception:
                    pass
                return int(seam_rc) if seam_rc not in (None, 0) else 1

            # success state
            was_enrich = bool(st.get("had_p1p2_pass") and side in {"P1", "P2"})
            if side in {"P1", "P2"}:
                st["terminal_success"] = True
                st["last_selected_side"] = side
                if was_enrich or st.get("had_p1p2_pass"):
                    st["candidate_enrich_done"] = True
                else:
                    # direct candidate PASS also terminal
                    st["candidate_enrich_done"] = True
                    st["had_p1p2_pass"] = False
            else:
                # P1/P2-only PASS (no candidate)
                st["terminal_success"] = True
                st["had_p1p2_pass"] = True
                st["p1p2_pass_at"] = mono
                st["last_selected_side"] = None
                st["candidate_enrich_done"] = False
            st["failure_count"] = 0
            st["next_eligible_at"] = 0.0
            return 0

        # FAIL path
        st["failure_count"] = int(st.get("failure_count") or 0) + 1
        delay = _winline_shadow_activation_backoff_seconds(st["failure_count"])
        st["next_eligible_at"] = mono + delay
        # do not mark terminal_success
        try:
            _winline_shadow_activation_write_fail(
                out_path,
                map_num=resolved_map,
                team1=t1,
                team2=t2,
                selected_side=side,
                canonical_key=key,
                reasons=fail_reasons or ["controller_fail"],
                acquisition_error=acquisition_error,
                base=record,
                attempt_started_at=attempt_started,
                attempt_finished_at=attempt_finished,
                next_eligible_at=st["next_eligible_at"],
                seam_rc=seam_rc,
            )
        except Exception:
            pass
        if seam_rc is not None and int(seam_rc) != 0:
            return int(seam_rc)
        return 1
    finally:
        st["in_flight"] = False


def queue_winline_shadow_activation(
    *,
    match_key: Any,
    map_num: Any,
    team1: Any,
    team2: Any,
    selected_side: Any = None,
    run_winline_shadow_request: Optional[Callable[..., int]] = None,
    output_path: Optional[PathLike] = None,
    freshness_limit_seconds: float = 15.0,
    now: Optional[float] = None,
) -> None:
    """Queue a shadow activation request; does not invoke the seam yet."""
    _winline_shadow_activation_queue.append(
        {
            "match_key": match_key,
            "map_num": map_num,
            "team1": team1,
            "team2": team2,
            "selected_side": selected_side,
            "run_winline_shadow_request": run_winline_shadow_request,
            "output_path": output_path,
            "freshness_limit_seconds": freshness_limit_seconds,
            "now": now,
        }
    )


def flush_winline_shadow_activation(
    *,
    now_monotonic: Optional[float] = None,
    run_winline_shadow_request: Optional[Callable[..., int]] = None,
) -> List[Optional[int]]:
    """Flush queued activations after ordinary path; returns per-item outcomes."""
    pending = list(_winline_shadow_activation_queue)
    _winline_shadow_activation_queue.clear()
    results: List[Optional[int]] = []
    for item in pending:
        seam = item.get("run_winline_shadow_request") or run_winline_shadow_request
        results.append(
            maybe_run_winline_shadow_activation(
                match_key=item.get("match_key"),
                map_num=item.get("map_num"),
                team1=item.get("team1"),
                team2=item.get("team2"),
                selected_side=item.get("selected_side"),
                ordinary_path_completed=True,
                now_monotonic=now_monotonic,
                run_winline_shadow_request=seam,
                output_path=item.get("output_path"),
                freshness_limit_seconds=float(item.get("freshness_limit_seconds") or 15.0),
                now=item.get("now"),
                no_odds_active=True,
            )
        )
    return results
