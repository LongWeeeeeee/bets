"""Winline shadow probe — non-sending, injected shared-session only.

ONE acquisition path: the caller-injected ``submit_shared_job``. This module
has no browser, session, thread, process, delivery, or odds-gate capability of
its own. INT wiring injects the production process-wide shared job runner.
"""
from __future__ import annotations

import inspect
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

SCHEMA_VERSION = "winline_shadow_probe.v1"

PathLike = Union[str, Path]


def _normalize_side(value: Any) -> str:
    return str(value or "").strip().upper()


def _as_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:  # NaN
        return None
    return number


def _as_int(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    try:
        if isinstance(value, float) and not value.is_integer():
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_epoch(value: Any) -> Optional[float]:
    number = _as_float(value)
    if number is None:
        return None
    # Accept millisecond timestamps.
    if number > 1e12:
        number = number / 1000.0
    return number


def _teams_equal(a: Any, b: Any) -> bool:
    left = " ".join(str(a or "").strip().lower().split())
    right = " ".join(str(b or "").strip().lower().split())
    return bool(left) and left == right


def _context_fields(context: Any) -> Dict[str, Any]:
    if not isinstance(context, dict):
        return {}
    match_id = context.get("match_id")
    if match_id is None:
        match_id = context.get("series_id")
    team1 = context.get("team1")
    team2 = context.get("team2")
    if team1 is None and isinstance(context.get("teams"), (list, tuple)) and len(context["teams"]) >= 2:
        team1, team2 = context["teams"][0], context["teams"][1]
    return {
        "match_id": match_id,
        "map_num": context.get("map_num"),
        "team1": team1,
        "team2": team2,
        "selected_side": context.get("selected_side"),
        "freshness_limit_seconds": context.get("freshness_limit_seconds", 15.0),
        "job": context.get("job"),
    }


def _observation_fields(obs: Any) -> Dict[str, Any]:
    if not isinstance(obs, dict):
        return {}
    match_id = obs.get("match_id")
    if match_id is None:
        match_id = obs.get("series_id")
    team1 = obs.get("team1")
    team2 = obs.get("team2")
    if team1 is None and isinstance(obs.get("teams"), (list, tuple)) and len(obs["teams"]) >= 2:
        team1, team2 = obs["teams"][0], obs["teams"][1]
    p1_odds = obs.get("p1_odds")
    p2_odds = obs.get("p2_odds")
    odds = obs.get("odds")
    if (p1_odds is None or p2_odds is None) and isinstance(odds, (list, tuple)) and len(odds) >= 2:
        if p1_odds is None:
            p1_odds = odds[0]
        if p2_odds is None:
            p2_odds = odds[1]
    source = obs.get("source")
    # Accept only explicit Winline; normalize common casing.
    return {
        "source": source,
        "match_id": match_id,
        "map_num": obs.get("map_num"),
        "team1": team1,
        "team2": team2,
        "p1_odds": p1_odds,
        "p2_odds": p2_odds,
        "observed_at": obs.get("observed_at"),
    }


def _source_is_winline(source: Any) -> bool:
    return str(source or "").strip().lower() == "winline"


async def _call_submit(submit_shared_job: Any, job: Any) -> Any:
    """Invoke injected runner exactly once (sync or async)."""
    if inspect.iscoroutinefunction(submit_shared_job):
        return await submit_shared_job(job)
    result = submit_shared_job(job)
    if inspect.isawaitable(result):
        return await result
    return result


def _validate(
    *,
    ctx: Dict[str, Any],
    obs: Dict[str, Any],
    collected_at: float,
    now: float,
) -> Tuple[str, List[str], Optional[float]]:
    """Return (verdict, failure_reasons, selected_odds)."""
    reasons: List[str] = []

    selected_side = _normalize_side(ctx.get("selected_side"))
    # Absent / None / empty / whitespace selected_side is non-gating.
    # Only an explicitly supplied non-blank value must be P1 or P2.
    if selected_side and selected_side not in {"P1", "P2"}:
        reasons.append("invalid_selected_side")

    ctx_match = ctx.get("match_id")
    if ctx_match is None or str(ctx_match).strip() == "":
        reasons.append("context_match_id_missing")

    ctx_map = _as_int(ctx.get("map_num"))
    if ctx_map is None:
        reasons.append("context_map_num_missing")

    ctx_team1 = ctx.get("team1")
    ctx_team2 = ctx.get("team2")
    if not str(ctx_team1 or "").strip() or not str(ctx_team2 or "").strip():
        reasons.append("context_teams_missing")
    elif _teams_equal(ctx_team1, ctx_team2):
        reasons.append("ambiguous_team_order")

    freshness = _as_float(ctx.get("freshness_limit_seconds"))
    if freshness is None or freshness < 0:
        reasons.append("invalid_freshness_limit")
        freshness = 0.0

    if not obs:
        reasons.append("observation_missing_or_malformed")
        return "FAIL", reasons, None

    if not _source_is_winline(obs.get("source")):
        reasons.append("source_not_winline")

    obs_match = obs.get("match_id")
    if ctx_match is not None and str(obs_match) != str(ctx_match):
        reasons.append("match_id_mismatch")

    obs_map = _as_int(obs.get("map_num"))
    if ctx_map is not None and obs_map != ctx_map:
        reasons.append("map_num_mismatch")

    obs_team1 = obs.get("team1")
    obs_team2 = obs.get("team2")
    if not str(obs_team1 or "").strip() or not str(obs_team2 or "").strip():
        reasons.append("observation_teams_missing")
    elif _teams_equal(obs_team1, obs_team2):
        reasons.append("ambiguous_team_order")
    elif not (
        _teams_equal(obs_team1, ctx_team1) and _teams_equal(obs_team2, ctx_team2)
    ):
        # Team order / identity must match context P1/P2 meaning exactly.
        reasons.append("team_order_mismatch")

    p1_odds = _as_float(obs.get("p1_odds"))
    p2_odds = _as_float(obs.get("p2_odds"))
    if p1_odds is None:
        reasons.append("p1_odds_missing_or_malformed")
    elif p1_odds <= 0:
        reasons.append("p1_odds_not_positive")
    if p2_odds is None:
        reasons.append("p2_odds_missing_or_malformed")
    elif p2_odds <= 0:
        reasons.append("p2_odds_not_positive")

    observed_at = _as_epoch(obs.get("observed_at"))
    if observed_at is None:
        reasons.append("observed_at_missing")
    else:
        age = float(now) - float(observed_at)
        if age < 0:
            # Future clock skew: treat as invalid freshness provenance.
            reasons.append("observed_at_in_future")
        elif freshness is not None and age > float(freshness):
            reasons.append("observation_stale")

    selected_odds: Optional[float] = None
    if selected_side == "P1" and p1_odds is not None and p1_odds > 0:
        selected_odds = p1_odds
    elif selected_side == "P2" and p2_odds is not None and p2_odds > 0:
        selected_odds = p2_odds
    elif selected_side in {"P1", "P2"}:
        reasons.append("selected_odds_unavailable")

    # collected_at must be present (caller supplies); ensure non-empty.
    if collected_at is None:
        reasons.append("collected_at_missing")

    if reasons:
        return "FAIL", reasons, selected_odds
    return "PASS", [], selected_odds


def _atomic_write_json(path: PathLike, payload: Dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2)
    data = data + "\n"
    fd, tmp_name = _mkstemp_adjacent(target)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, target)
    except Exception:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _mkstemp_adjacent(target: Path) -> Tuple[int, str]:
    import tempfile

    directory = str(target.parent)
    fd, name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=directory,
    )
    return fd, name


def _build_record(
    *,
    ctx: Dict[str, Any],
    obs: Dict[str, Any],
    verdict: str,
    reasons: List[str],
    selected_odds: Optional[float],
    collected_at: float,
    acquisition_error: Optional[str] = None,
) -> Dict[str, Any]:
    selected_side = _normalize_side(ctx.get("selected_side"))
    p1_odds = _as_float(obs.get("p1_odds")) if obs else None
    p2_odds = _as_float(obs.get("p2_odds")) if obs else None
    observed_at = _as_epoch(obs.get("observed_at")) if obs else None
    record: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "match_id": ctx.get("match_id"),
        "map_num": _as_int(ctx.get("map_num")),
        "team1": ctx.get("team1"),
        "team2": ctx.get("team2"),
        "p1_team": ctx.get("team1"),
        "p2_team": ctx.get("team2"),
        "source": (obs.get("source") if obs else None),
        "observed_at": observed_at,
        "collected_at": collected_at,
        "p1_odds": p1_odds,
        "p2_odds": p2_odds,
        "selected_side": selected_side if selected_side in {"P1", "P2"} else str(ctx.get("selected_side") or ""),
        "selected_odds": selected_odds,
        "verdict": verdict,
        "failure_reasons": list(reasons),
    }
    if acquisition_error is not None:
        record["acquisition_error"] = acquisition_error
    return record


async def run_winline_shadow_probe(
    *,
    submit_shared_job: Any,
    context: Any,
    output_path: Any,
    now: Optional[float] = None,
) -> int:
    """Obtain one Winline observation via injected shared job and write evidence.

    Returns 0 only for a PASS evidence record. Nonzero for every validation,
    acquisition, or write failure. Never raises a false PASS.
    """
    clock = float(now if now is not None else time.time())
    collected_at = clock
    ctx = _context_fields(context)
    job = ctx.get("job")
    if job is None and isinstance(context, dict):
        job = {
            "label": "winline-shadow",
            "match_id": ctx.get("match_id"),
            "map_num": ctx.get("map_num"),
            "team1": ctx.get("team1"),
            "team2": ctx.get("team2"),
        }

    obs_raw: Any = None
    acquisition_error: Optional[str] = None
    try:
        obs_raw = await _call_submit(submit_shared_job, job)
    except Exception as exc:  # acquisition failure — never PASS
        acquisition_error = f"{type(exc).__name__}: {exc}"
        record = _build_record(
            ctx=ctx,
            obs={},
            verdict="FAIL",
            reasons=["acquisition_failed", acquisition_error],
            selected_odds=None,
            collected_at=collected_at,
            acquisition_error=acquisition_error,
        )
        try:
            _atomic_write_json(output_path, record)
        except Exception:
            return 3
        return 2

    obs = _observation_fields(obs_raw)
    verdict, reasons, selected_odds = _validate(
        ctx=ctx,
        obs=obs,
        collected_at=collected_at,
        now=clock,
    )
    record = _build_record(
        ctx=ctx,
        obs=obs,
        verdict=verdict,
        reasons=reasons,
        selected_odds=selected_odds,
        collected_at=collected_at,
    )
    try:
        _atomic_write_json(output_path, record)
    except Exception:
        return 3

    return 0 if verdict == "PASS" else 1
