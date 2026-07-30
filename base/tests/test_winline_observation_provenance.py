"""FIX-B: bind current-map observation to match_key + explicit live status.

Canonical reservation may accept an observation only when it is fresh,
exact-map, same expected match, and explicitly live/in-progress.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402


# Existing-contract live vocabulary (see cyberscore_try status_looks_live / status_rank).
_LIVE_STATUSES = ("live", "in_progress", "online", "running", "inprogress")
# Existing finished terminal set already owned by the validator.
_FINISHED_STATUSES = ("finished", "ended", "complete", "completed")


def _open_winline_sites(map_num: int = 2, odds: Optional[List[float]] = None) -> Dict[str, Any]:
    return {
        "winline": {
            "match_found": True,
            "odds": list(odds or [1.55, 2.40]),
            "match_odds": [1.30, 3.15],
            "market_closed": False,
            "market_kind": "current_map_winner",
            "map_num": map_num,
            "p1_team": "team1",
            "p2_team": "team2",
            "source": "deeplink_map_market",
        }
    }


def _seed_prefetch(
    match_key: str,
    *,
    map_num: int,
    sites: Dict[str, Any],
    odds_refreshed_at: Optional[float] = None,
) -> None:
    refreshed_at = float(odds_refreshed_at if odds_refreshed_at is not None else time.time())
    with cs.bookmaker_prefetch_condition:
        cs.bookmaker_prefetch_results[match_key] = {
            "status": "done",
            "finished_at": refreshed_at,
            "submitted_at": refreshed_at,
            "odds_refreshed_at": refreshed_at,
            "odds_refresh_ready": True,
            "map_num": map_num,
            "mode": "live",
            "sites": sites,
        }


def _obs(
    *,
    match_key: Optional[str] = None,
    map_num: int = 2,
    status: Any = "live",
    observed_at: Optional[float] = None,
    include_match_key: bool = True,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "map_num": map_num,
        "status": status,
        "observed_at": float(observed_at if observed_at is not None else time.time()),
    }
    if include_match_key:
        out["match_key"] = match_key
    return out


def _resolve(
    match_key: str,
    observation: Any,
    *,
    map_num: int = 2,
    pending: Optional[Dict[str, Any]] = None,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    if now is not None:
        # Caller patches cs.time.time; this is only for seed freshness alignment.
        pass
    pending_state = pending if pending is not None else {}
    return cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending_state,
        map_num=map_num,
        current_map_observation=observation,
    )


def _assert_no_reservation(decision: Dict[str, Any], pending: Dict[str, Any], match_key: str) -> None:
    assert decision.get("should_send") is False
    assert decision.get("token") in (None, "")
    res = decision.get("reservation_context")
    assert res in (None, {}) or not (isinstance(res, dict) and res.get("token"))
    entry = pending.get(match_key) or {}
    assert entry.get("token") in (None, "")
    assert entry.get("state") != "prepared"
    assert decision.get("state") in {"temporarily_closed_wait", "terminal_skip"}


def test_valid_same_match_exact_map_fresh_live_reserves(monkeypatch) -> None:
    """Baseline: same match_key + live + exact map + fresh => may reserve."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_010_000.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    match_key = "https://cyberscore.live/matches/prov-valid-9501"
    pending: Dict[str, Any] = {}
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    obs = _obs(match_key=match_key, map_num=2, status="live", observed_at=clock["now"])
    decision = _resolve(match_key, obs, pending=pending, map_num=2)
    assert decision.get("should_send") is True or decision.get("state") == "prepared"
    assert decision.get("token") or (decision.get("reservation_context") or {}).get("token")
    entry = pending.get(match_key) or {}
    assert entry.get("state") == "prepared"
    assert entry.get("token")


@pytest.mark.parametrize("live_status", list(_LIVE_STATUSES))
def test_explicit_live_vocabulary_accepted(monkeypatch, live_status: str) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)

    clock = {"now": 1_700_010_100.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])
    match_key = f"https://cyberscore.live/matches/prov-live-{live_status}-9502"
    pending: Dict[str, Any] = {}
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    obs = _obs(match_key=match_key, status=live_status, observed_at=clock["now"])
    decision = _resolve(match_key, obs, pending=pending)
    assert decision.get("state") == "prepared"
    assert (pending.get(match_key) or {}).get("token")


def test_foreign_match_key_fails_closed_no_reservation(monkeypatch) -> None:
    """Foreign observation match_key must not reserve/send."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_010_200.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    match_key = "https://cyberscore.live/matches/prov-foreign-9503"
    foreign = "https://cyberscore.live/matches/OTHER-MATCH"
    pending: Dict[str, Any] = {}
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    obs = _obs(match_key=foreign, status="live", observed_at=clock["now"])
    decision = _resolve(match_key, obs, pending=pending)
    _assert_no_reservation(decision, pending, match_key)
    # Fail-closed reason under existing vocabulary (wait or terminal skip).
    assert decision.get("reason") in {
        "current_map_unavailable",
        "current_map_mismatch",
        "current_map_observation_stale",
        "temporarily_closed_wait",
    } or decision.get("state") in {"temporarily_closed_wait", "terminal_skip"}


def test_absent_match_key_identity_fails_closed(monkeypatch) -> None:
    """Missing match_key field on observation must fail closed (no token)."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_010_240.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])
    match_key = "https://cyberscore.live/matches/prov-absent-key-9504a"
    pending: Dict[str, Any] = {}
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    obs = {
        "map_num": 2,
        "status": "live",
        "observed_at": clock["now"],
    }
    assert "match_key" not in obs
    decision = _resolve(match_key, obs, pending=pending)
    _assert_no_reservation(decision, pending, match_key)
    assert decision.get("reason") in {
        "current_map_unavailable",
        "current_map_mismatch",
        "current_map_observation_stale",
        "temporarily_closed_wait",
    } or decision.get("state") in {"temporarily_closed_wait", "terminal_skip"}


def test_none_match_key_identity_fails_closed(monkeypatch) -> None:
    """match_key=None on observation must fail closed (no token)."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_010_245.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])
    match_key = "https://cyberscore.live/matches/prov-none-key-9504b"
    pending: Dict[str, Any] = {}
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    obs = _obs(match_key=None, status="live", observed_at=clock["now"])
    assert obs.get("match_key") is None
    decision = _resolve(match_key, obs, pending=pending)
    _assert_no_reservation(decision, pending, match_key)
    assert decision.get("reason") in {
        "current_map_unavailable",
        "current_map_mismatch",
        "current_map_observation_stale",
        "temporarily_closed_wait",
    } or decision.get("state") in {"temporarily_closed_wait", "terminal_skip"}


def test_empty_match_key_identity_fails_closed(monkeypatch) -> None:
    """Empty-string match_key on observation must fail closed (no token)."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_010_250.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])
    match_key = "https://cyberscore.live/matches/prov-empty-key-9504c"
    pending: Dict[str, Any] = {}
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    obs = _obs(match_key="", status="live", observed_at=clock["now"])
    assert obs.get("match_key") == ""
    decision = _resolve(match_key, obs, pending=pending)
    _assert_no_reservation(decision, pending, match_key)
    assert decision.get("reason") in {
        "current_map_unavailable",
        "current_map_mismatch",
        "current_map_observation_stale",
        "temporarily_closed_wait",
    } or decision.get("state") in {"temporarily_closed_wait", "terminal_skip"}


@pytest.mark.parametrize(
    "bad_status",
    [
        "scheduled",
        "pre_match",
        "prematch",
        "",
        None,
        "unknown",
        "canceled",
        "cancelled",
    ],
)
def test_non_live_or_empty_status_fails_closed(monkeypatch, bad_status: Any) -> None:
    """scheduled/empty/unknown/canceled must not reserve (scheduled/empty currently pass — RED)."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_010_300.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])
    match_key = f"https://cyberscore.live/matches/prov-status-{bad_status!r}-9505"
    pending: Dict[str, Any] = {}
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    obs = _obs(match_key=match_key, status=bad_status, observed_at=clock["now"])
    if bad_status is None:
        obs.pop("status", None)
        # Force missing status key path
        obs = {
            "match_key": match_key,
            "map_num": 2,
            "observed_at": clock["now"],
        }
    decision = _resolve(match_key, obs, pending=pending)
    _assert_no_reservation(decision, pending, match_key)


@pytest.mark.parametrize("finished_status", list(_FINISHED_STATUSES))
def test_finished_status_terminal_skip_no_token(monkeypatch, finished_status: str) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)

    clock = {"now": 1_700_010_400.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])
    match_key = f"https://cyberscore.live/matches/prov-fin-{finished_status}-9506"
    pending: Dict[str, Any] = {}
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    obs = _obs(match_key=match_key, status=finished_status, observed_at=clock["now"])
    decision = _resolve(match_key, obs, pending=pending)
    assert decision.get("should_send") is False
    assert decision.get("token") in (None, "")
    assert decision.get("state") == "terminal_skip"
    assert decision.get("reason") == "match_finished"
    assert (pending.get(match_key) or {}).get("token") in (None, "")


def test_map_mismatch_terminal_skip(monkeypatch) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)

    clock = {"now": 1_700_010_500.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])
    match_key = "https://cyberscore.live/matches/prov-map-mismatch-9507"
    pending: Dict[str, Any] = {}
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    obs = _obs(match_key=match_key, map_num=3, status="live", observed_at=clock["now"])
    decision = _resolve(match_key, obs, pending=pending, map_num=2)
    assert decision.get("state") == "terminal_skip"
    assert decision.get("reason") == "current_map_mismatch"
    assert decision.get("should_send") is False
    assert decision.get("token") in (None, "")


def test_stale_observation_waits_tokenless(monkeypatch) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_010_600.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])
    match_key = "https://cyberscore.live/matches/prov-stale-9508"
    pending: Dict[str, Any] = {}
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    obs = _obs(
        match_key=match_key,
        status="live",
        observed_at=clock["now"] - 60.0,  # older than max age
    )
    decision = _resolve(match_key, obs, pending=pending)
    _assert_no_reservation(decision, pending, match_key)
    assert decision.get("state") == "temporarily_closed_wait"
    assert decision.get("reason") == "current_map_observation_stale"


def test_future_observation_timestamp_waits_tokenless(monkeypatch) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_010_700.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])
    match_key = "https://cyberscore.live/matches/prov-future-9509"
    pending: Dict[str, Any] = {}
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    obs = _obs(
        match_key=match_key,
        status="live",
        observed_at=clock["now"] + 30.0,
    )
    decision = _resolve(match_key, obs, pending=pending)
    _assert_no_reservation(decision, pending, match_key)
    assert decision.get("reason") == "current_map_observation_stale"


def test_missing_observation_waits_tokenless(monkeypatch) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_010_800.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])
    match_key = "https://cyberscore.live/matches/prov-none-9510"
    pending: Dict[str, Any] = {}
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    decision = _resolve(match_key, None, pending=pending)
    _assert_no_reservation(decision, pending, match_key)
    assert decision.get("reason") == "current_map_unavailable"


def test_validator_unit_foreign_and_scheduled_currently_documented() -> None:
    """Direct validator boundary: foreign key / non-live status must not return ok."""
    now = 1_700_011_000.0
    expected = "https://cyberscore.live/matches/unit-9511"
    foreign_obs = {
        "match_key": "https://cyberscore.live/matches/FOREIGN",
        "map_num": 2,
        "status": "live",
        "observed_at": now,
    }
    scheduled_obs = {
        "match_key": expected,
        "map_num": 2,
        "status": "scheduled",
        "observed_at": now,
    }
    empty_status_obs = {
        "match_key": expected,
        "map_num": 2,
        "status": "",
        "observed_at": now,
    }
    # After fix these must not be ok. Signature of helper may include expected_match_key.
    kwargs = {"expected_map_num": 2, "now": now}
    # Prefer new kw if present; also try positional plumbing via inspect.
    import inspect

    sig = inspect.signature(cs._bookmaker_validate_current_map_observation)
    if "expected_match_key" in sig.parameters:
        kwargs["expected_match_key"] = expected

    r_foreign = cs._bookmaker_validate_current_map_observation(foreign_obs, **kwargs)
    r_sched = cs._bookmaker_validate_current_map_observation(scheduled_obs, **kwargs)
    r_empty = cs._bookmaker_validate_current_map_observation(empty_status_obs, **kwargs)
    assert r_foreign != "ok", "foreign match_key must fail closed"
    assert r_sched != "ok", "scheduled status must fail closed"
    assert r_empty != "ok", "empty status must fail closed"


def test_validator_unit_identity_absent_none_empty_foreign_fail_closed() -> None:
    """Direct validator: absent/None/empty/foreign match_key fail closed vs expected key."""
    now = 1_700_011_050.0
    expected = "https://cyberscore.live/matches/unit-identity-9511b"
    kwargs: Dict[str, Any] = {
        "expected_map_num": 2,
        "now": now,
        "expected_match_key": expected,
    }
    base = {"map_num": 2, "status": "live", "observed_at": now}
    r_absent = cs._bookmaker_validate_current_map_observation(dict(base), **kwargs)
    r_none = cs._bookmaker_validate_current_map_observation(
        {**base, "match_key": None}, **kwargs
    )
    r_empty = cs._bookmaker_validate_current_map_observation(
        {**base, "match_key": ""}, **kwargs
    )
    r_foreign = cs._bookmaker_validate_current_map_observation(
        {**base, "match_key": "https://cyberscore.live/matches/FOREIGN"}, **kwargs
    )
    assert r_absent != "ok", "absent match_key must fail closed"
    assert r_none != "ok", "None match_key must fail closed"
    assert r_empty != "ok", "empty match_key must fail closed"
    assert r_foreign != "ok", "foreign match_key must fail closed"


def test_validator_accepts_live_same_match() -> None:
    now = 1_700_011_100.0
    expected = "https://cyberscore.live/matches/unit-ok-9512"
    obs = {
        "match_key": expected,
        "map_num": 2,
        "status": "live",
        "observed_at": now,
    }
    import inspect

    kwargs: Dict[str, Any] = {"expected_map_num": 2, "now": now}
    sig = inspect.signature(cs._bookmaker_validate_current_map_observation)
    if "expected_match_key" in sig.parameters:
        kwargs["expected_match_key"] = expected
    assert cs._bookmaker_validate_current_map_observation(obs, **kwargs) == "ok"
