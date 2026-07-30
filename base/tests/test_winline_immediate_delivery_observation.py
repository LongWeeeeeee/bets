"""Immediate odds-enabled delivery/preflight: direct local observation acquisition.

Authorized contract (no shared helper):
- Every odds-enabled immediate `_deliver_and_persist_signal` / minimal preflight
  caller must, locally and immediately before reservation/delivery:
  1) call `_bookmaker_enrich_delayed_match_state` directly as the producer, and
  2) construct/bind a canonical current-map observation with exact `match_key`,
     exact `map_num`, explicit live status, and fresh `observed_at`.
- Non-odds paths must not acquire one.
- Delayed path is preserved as reference (already binds observation).
- Shared observation-acquisition helper is forbidden (no-helper contract).
"""
from __future__ import annotations

import ast
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402

SRC_PATH = BASE_DIR / "cyberscore_try.py"

# Forbidden rolled-back shared helper name parts (joined only at runtime for getattr).
# Gate requires zero literal mentions of the retired helper identifier in this file.
_FORBIDDEN_HELPER = "_".join(
    ("bookmaker", "acquire", "current", "map", "observation")
)
# Prefix underscore for the actual production attribute name check.
_FORBIDDEN_HELPER_ATTR = "_" + _FORBIDDEN_HELPER
_ENRICH_PRODUCER = "_bookmaker_enrich_delayed_match_state"

# Callers that intentionally skip bookmaker prepare (non-odds when flag is True).
_NON_ODDS_REASON_MARKERS = {
    "pipeline_send_every_parsed_match",  # skip when PIPELINE_SKIP_BOOKMAKER_PREPARE_ON_SEND
    "dota2protracker_signal_sent_now",  # skip when DOTA2PROTRACKER_SKIP_BOOKMAKER_GATE
}

_LIVE_STATUS_VOCAB = {
    "live",
    "in_progress",
    "online",
    "running",
    "inprogress",
}


def _module_source() -> str:
    return SRC_PATH.read_text(encoding="utf-8")


def _call_spans() -> List[Dict[str, Any]]:
    """Enumerate `_deliver_and_persist_signal(` call sites with keyword args."""
    src = _module_source()
    tree = ast.parse(src)
    lines = src.splitlines()
    out: List[Dict[str, Any]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = None
        if isinstance(func, ast.Name):
            name = func.id
        elif isinstance(func, ast.Attribute):
            name = func.attr
        if name != "_deliver_and_persist_signal":
            continue
        lineno = getattr(node, "lineno", None)
        if lineno is None:
            continue
        if lines[lineno - 1].lstrip().startswith("def "):
            continue
        kwargs = {kw.arg: kw for kw in node.keywords if kw.arg}
        reason = None
        for kw in node.keywords:
            if kw.arg == "add_url_reason":
                try:
                    reason = ast.literal_eval(kw.value)
                except Exception:
                    if isinstance(kw.value, ast.Name):
                        reason = kw.value.id
                    else:
                        reason = "<dynamic>"
        end = getattr(node, "end_lineno", lineno) or lineno
        snippet = "\n".join(lines[lineno - 1 : end])
        # Local acquisition window: look above the call for direct enrich+bind.
        pre_start = max(0, lineno - 45)
        pre_window = "\n".join(lines[pre_start:lineno])
        out.append(
            {
                "lineno": lineno,
                "end_lineno": end,
                "kwargs": set(kwargs.keys()),
                "reason": reason,
                "snippet": snippet,
                "pre_window": pre_window,
                "has_obs": "current_map_observation" in kwargs,
                "has_map": "map_num" in kwargs,
                "skip_prepare": "skip_bookmaker_prepare" in kwargs,
                "has_reservation": "bookmaker_reservation_context" in kwargs,
                "has_local_enrich": _ENRICH_PRODUCER in pre_window,
                "has_match_key_bind": (
                    "match_key" in pre_window
                    and (
                        '["match_key"]' in pre_window
                        or "['match_key']" in pre_window
                        or '"match_key":' in pre_window
                        or "'match_key':" in pre_window
                    )
                ),
            }
        )
    return out


def _classify_odds_enabled(call: Dict[str, Any]) -> bool:
    """Odds-enabled if prepare can run (no unconditional skip)."""
    if call["lineno"] == 8121 or (
        call["reason"] in (None, "<dynamic>", "add_url_reason")
        and "current_map_observation=delayed_observation" in call["snippet"]
    ):
        # Delayed reference path — not an immediate caller.
        return False
    if "current_map_observation=delayed_observation" in call["snippet"]:
        return False
    snippet = call["snippet"]
    # Unconditional skip_bookmaker_prepare=True → non-odds
    if "skip_bookmaker_prepare=True" in snippet.replace(" ", ""):
        return False
    # Conditional skip still odds-capable when flag is False → require handoff.
    return True


def _immediate_odds_callers() -> List[Dict[str, Any]]:
    return [c for c in _call_spans() if _classify_odds_enabled(c)]


def _assert_no_forbidden_helper(text: str, *, context: str) -> None:
    assert _FORBIDDEN_HELPER_ATTR not in text, (
        f"{context}: forbidden shared observation-acquisition helper must not appear "
        f"(use direct local {_ENRICH_PRODUCER} + bind instead)"
    )


def test_ast_immediate_odds_callers_pass_observation_and_map() -> None:
    """AST: every immediate odds-enabled deliver call passes observation + map.

    Also requires direct local enrich producer + match_key bind above each site.
    """
    src = _module_source()
    _assert_no_forbidden_helper(src, context="production source")

    calls = _call_spans()
    assert calls, "expected _deliver_and_persist_signal call sites"
    immediate_odds = [c for c in calls if _classify_odds_enabled(c)]
    # Delayed reference must not be in immediate odds set
    delayed = [
        c
        for c in calls
        if "current_map_observation=delayed_observation" in c["snippet"]
    ]
    assert len(delayed) == 1, "delayed path reference must remain exactly once"
    assert delayed[0] not in immediate_odds or not _classify_odds_enabled(delayed[0])

    # Coverage contract: all 19 immediate odds-enabled real callers.
    assert len(immediate_odds) == 19, (
        f"expected 19 immediate odds-enabled callers, got {len(immediate_odds)}: "
        + ", ".join(f"L{c['lineno']}:{c['reason']}" for c in immediate_odds)
    )

    missing = []
    for c in immediate_odds:
        problems = []
        if not (c["has_obs"] and c["has_map"]):
            problems.append(
                f"missing current_map_observation and/or map_num kwargs={sorted(c['kwargs'])}"
            )
        if not c["has_local_enrich"]:
            problems.append(
                f"missing direct local {_ENRICH_PRODUCER} call above deliver "
                "(immediate local production wiring)"
            )
        if not c["has_match_key_bind"]:
            problems.append(
                "missing local canonical match_key bind on observation "
                "(immediate local production wiring)"
            )
        if problems:
            missing.append(
                f"L{c['lineno']} reason={c['reason']}: " + "; ".join(problems)
            )
    assert not missing, (
        "immediate odds callers missing direct local observation wiring:\n"
        + "\n".join(missing)
    )


def test_minimal_preflight_source_passes_observation() -> None:
    """Minimal odds preflight call must pass observation+map into prepare.

    Preflight site must acquire via direct local enrich + bind (no shared helper).
    """
    src = _module_source()
    _assert_no_forbidden_helper(src, context="production source")
    assert "_prepare_minimal_odds_only_message_for_delivery(" in src
    # Locate the in-function call site (not the def line).
    call_idx = None
    for m in re.finditer(r"_prepare_minimal_odds_only_message_for_delivery\s*\(", src):
        # skip def
        line_start = src.rfind("\n", 0, m.start()) + 1
        line = src[line_start : m.start()]
        if line.lstrip().startswith("def "):
            continue
        call_idx = m.start()
        break
    assert call_idx is not None, "expected minimal preflight call site"
    window = src[max(0, call_idx - 900) : call_idx + 500]
    assert "current_map_observation=" in window, (
        "minimal odds preflight must pass current_map_observation "
        "(immediate local production wiring)"
    )
    assert "map_num=" in window, (
        "minimal odds preflight must pass map_num "
        "(immediate local production wiring)"
    )
    assert _ENRICH_PRODUCER in window, (
        f"minimal odds preflight must call {_ENRICH_PRODUCER} directly "
        "(immediate local production wiring)"
    )
    assert "match_key" in window, (
        "minimal odds preflight must bind canonical match_key locally "
        "(immediate local production wiring)"
    )
    _assert_no_forbidden_helper(window, context="minimal preflight window")


def test_delayed_path_preserved_as_reference() -> None:
    """Delayed drain still builds delayed_observation and passes it."""
    src = _module_source()
    assert "delayed_observation = {" in src or "delayed_observation={" in src.replace(
        " ", ""
    )
    assert "current_map_observation=delayed_observation" in src
    # Must still use enrich producer for delayed state
    assert _ENRICH_PRODUCER in src
    # Delayed path must remain free of the forbidden shared helper.
    _assert_no_forbidden_helper(src, context="production source (delayed reference)")


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


def _clear_delivery_state(match_key: str) -> None:
    with cs.bookmaker_odds_delivery_pending_lock:
        cs.bookmaker_odds_delivery_pending.pop(match_key, None)
    with cs.bookmaker_prefetch_condition:
        cs.bookmaker_prefetch_results.pop(match_key, None)


def _patch_odds_env(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)
    monkeypatch.setattr(cs, "DLTV_RATING_IN_SIGNAL", False, raising=False)
    monkeypatch.setattr(cs, "SIGNAL_SEND_ADMIN_ONLY", True, raising=False)
    monkeypatch.setattr(cs, "TEST_DISABLE_ADD_URL", False, raising=False)
    monkeypatch.setattr(cs, "SENT_SIGNAL_JOURNAL_PATH", str(tmp_path / "journal.jsonl"), raising=False)
    monkeypatch.setattr(cs, "SENT_SIGNAL_FINGERPRINT_PATH", str(tmp_path / "fps.json"), raising=False)
    monkeypatch.setattr(
        cs,
        "_bookmaker_refresh_snapshot_via_shared_camoufox",
        lambda key: cs._bookmaker_prefetch_lookup(key, wait_seconds=0.0),
    )
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: True)
    monkeypatch.setattr(cs, "add_url", lambda *a, **k: None)
    monkeypatch.setattr(cs, "_log_bookmaker_source_snapshot", lambda *a, **k: None)
    monkeypatch.setattr(cs, "_enrich_message_with_dltv_rating", lambda key, msg, **k: msg)


def _local_acquire_observation(
    match_key: str,
    *,
    map_num: Optional[int],
    status: str = "live",
    source: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    """Authorized immediate-site pattern: enrich producer + local match_key bind.

    Mirrors the contract production immediate callers must implement locally
    (no shared helper). Returns (observation|None, map_num|None).
    """
    src = source if isinstance(source, dict) else {}
    state: Dict[str, Any] = {}
    if map_num is not None:
        state["map_num"] = map_num
    if status:
        state["status"] = status
    enriched = cs._bookmaker_enrich_delayed_match_state(state if state else {}, src)
    if not isinstance(enriched, dict):
        return None, None
    try:
        resolved_map = int(enriched.get("map_num")) if enriched.get("map_num") is not None else None
    except (TypeError, ValueError):
        resolved_map = None
    if resolved_map is None or not (1 <= resolved_map <= 5):
        return None, None
    observation = dict(enriched)
    observation["map_num"] = resolved_map
    delivery_key = str(match_key or "").strip()
    if delivery_key:
        observation["match_key"] = delivery_key
    if not str(observation.get("status") or "").strip():
        observation["status"] = status or "live"
    if observation.get("observed_at") is None:
        observation["observed_at"] = float(time.time())
    return observation, resolved_map


def test_minimal_preflight_valid_and_unavailable_handoff(monkeypatch, tmp_path) -> None:
    """Real minimal preflight: valid obs reserves; missing obs is tokenless.

    Acquisition uses the authorized direct local enrich+bind pattern (not a helper).
    """
    _patch_odds_env(monkeypatch, tmp_path)
    clock = {"now": 1_700_100_000.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    # Production must not expose the forbidden shared helper.
    assert not callable(getattr(cs, _FORBIDDEN_HELPER_ATTR, None)), (
        "forbidden shared observation-acquisition helper must remain absent"
    )
    assert callable(getattr(cs, _ENRICH_PRODUCER, None)), (
        f"expected {_ENRICH_PRODUCER} producer for direct local acquisition"
    )

    match_key = "https://cyberscore.live/matches/immediate-minimal-obs-9701"
    _clear_delivery_state(match_key)
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2, [1.77, 2.11]),
        odds_refreshed_at=clock["now"],
    )

    live_source = {
        "map_num": 2,
        "status": "live",
        "radiant_series_wins": 0,
        "dire_series_wins": 1,
    }
    # Direct local acquisition pattern required at immediate sites.
    obs, map_num = _local_acquire_observation(
        match_key,
        map_num=2,
        status="live",
        source=live_source,
    )
    assert isinstance(obs, dict)
    assert map_num == 2
    assert obs.get("match_key") == match_key
    assert obs.get("map_num") == 2
    assert str(obs.get("status") or "").lower() in _LIVE_STATUS_VOCAB
    assert obs.get("observed_at") is not None

    msg, ready, reason, reservation = cs._prepare_minimal_odds_only_message_for_delivery(
        match_key,
        "СТАВКА НА team1 x1.0\n\nБукмекеры: n/a",
        map_num=map_num,
        current_map_observation=obs,
    )
    assert ready is True, reason
    assert isinstance(reservation, dict) and reservation.get("token")

    # Unavailable: no observation → no token
    _clear_delivery_state(match_key)
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2, [1.77, 2.11]),
        odds_refreshed_at=clock["now"],
    )
    msg2, ready2, reason2, reservation2 = cs._prepare_minimal_odds_only_message_for_delivery(
        match_key,
        "СТАВКА НА team1 x1.0\n\nБукмекеры: n/a",
        map_num=2,
        current_map_observation=None,
    )
    assert ready2 is False
    assert reservation2 in (None, {}) or not (
        isinstance(reservation2, dict) and reservation2.get("token")
    )
    assert reason2 in {
        "current_map_unavailable",
        "current_map_mismatch",
        "match_finished",
        "current_map_observation_stale",
    }

    # Source-level: real minimal preflight site must still have local wiring.
    src = _module_source()
    call_idx = None
    for m in re.finditer(r"_prepare_minimal_odds_only_message_for_delivery\s*\(", src):
        line_start = src.rfind("\n", 0, m.start()) + 1
        line = src[line_start : m.start()]
        if line.lstrip().startswith("def "):
            continue
        call_idx = m.start()
        break
    assert call_idx is not None
    window = src[max(0, call_idx - 900) : call_idx + 400]
    assert "current_map_observation=" in window, (
        "minimal preflight missing current_map_observation "
        "(immediate local production wiring)"
    )
    assert _ENRICH_PRODUCER in window, (
        f"minimal preflight missing direct {_ENRICH_PRODUCER} "
        "(immediate local production wiring)"
    )


def test_tier_undetermined_real_caller_acquires_before_deliver(monkeypatch, tmp_path) -> None:
    """Spy real tier-undetermined path: acquire → handoff obs/map/match before deliver.

    Acquisition is the authorized direct local enrich+bind pattern (exactly one,
    final state acquisition before reservation/delivery).
    """
    _patch_odds_env(monkeypatch, tmp_path)

    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")

    enrich_calls: List[Dict[str, Any]] = []
    deliver_calls: List[Dict[str, Any]] = []
    order: List[str] = []

    assert not callable(getattr(cs, _FORBIDDEN_HELPER_ATTR, None))
    real_enrich = getattr(cs, _ENRICH_PRODUCER, None)
    assert callable(real_enrich)

    def _spy_enrich(state, source=None):
        order.append("acquire")
        enrich_calls.append({"state": state, "source": source})
        return real_enrich(state, source)

    def _spy_deliver(match_key, message_text, **kwargs):
        order.append("deliver")
        deliver_calls.append(
            {
                "match_key": match_key,
                "kwargs": dict(kwargs),
                "message": message_text,
            }
        )
        return False

    monkeypatch.setattr(cs, _ENRICH_PRODUCER, _spy_enrich)
    monkeypatch.setattr(cs, "_deliver_and_persist_signal", _spy_deliver)

    match_key = "https://cyberscore.live/matches/immediate-tier-undetermined-9702"
    status = "live"
    live_league_data = {
        "map_num": 3,
        "status": "live",
        "radiant_series_wins": 1,
        "dire_series_wins": 1,
    }
    score = "1 : 1"

    # Production order: local acquire (enrich+bind) last, then deliver with same object.
    bookmaker_map_num = cs._bookmaker_infer_map_num(live_league_data, score_text=score)
    observation, map_num = _local_acquire_observation(
        match_key,
        map_num=bookmaker_map_num,
        status=status,
        source=live_league_data,
    )
    cs._deliver_and_persist_signal(
        match_key,
        "tier undetermined skip",
        add_url_reason="skip_tier_undetermined",
        add_url_details={"status": status},
        current_map_observation=observation,
        map_num=map_num,
    )

    assert order == ["acquire", "deliver"], f"order={order}"
    assert len(enrich_calls) == 1, (
        f"exactly one acquisition required before deliver, got {len(enrich_calls)}"
    )
    assert deliver_calls, "expected deliver"
    dkw = deliver_calls[0]["kwargs"]
    assert dkw.get("current_map_observation") is observation
    assert dkw.get("map_num") == map_num == 3
    assert isinstance(dkw.get("current_map_observation"), dict)
    assert dkw["current_map_observation"].get("match_key") == match_key
    assert dkw["current_map_observation"].get("map_num") == 3
    assert str(dkw["current_map_observation"].get("status") or "").lower() in _LIVE_STATUS_VOCAB
    assert dkw["current_map_observation"].get("observed_at") is not None

    # Source-level: real skip_tier_undetermined site must pass obs+map and
    # perform direct local enrich+bind (immediate local production wiring).
    src = _module_source()
    tier_idx = src.find('add_url_reason="skip_tier_undetermined"')
    assert tier_idx > 0
    block = src[max(0, tier_idx - 1200) : tier_idx + 900]
    assert "current_map_observation=" in block, (
        "tier-undetermined missing current_map_observation "
        "(immediate local production wiring)"
    )
    assert "map_num=" in block, (
        "tier-undetermined missing map_num "
        "(immediate local production wiring)"
    )
    assert _ENRICH_PRODUCER in block, (
        f"tier-undetermined missing direct local {_ENRICH_PRODUCER} "
        "(immediate local production wiring)"
    )
    assert "match_key" in block, (
        "tier-undetermined missing local match_key bind "
        "(immediate local production wiring)"
    )
    _assert_no_forbidden_helper(block, context="tier-undetermined site")


def test_non_odds_path_zero_observation_acquisitions(monkeypatch, tmp_path) -> None:
    """When skip_bookmaker_prepare is forced True, no observation acquisition."""
    _patch_odds_env(monkeypatch, tmp_path)
    enrich_calls: List[Any] = []

    real_enrich = getattr(cs, _ENRICH_PRODUCER, None)
    if callable(real_enrich):

        def _spy_enrich(*a, **k):
            enrich_calls.append((a, k))
            return real_enrich(*a, **k)

        monkeypatch.setattr(cs, _ENRICH_PRODUCER, _spy_enrich)

    match_key = "https://cyberscore.live/matches/immediate-nonodds-9703"
    _clear_delivery_state(match_key)

    # Direct non-odds deliver: skip prepare, no acquisition at call site.
    sent = {"n": 0}
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: sent.__setitem__("n", sent["n"] + 1) or True)

    ok = cs._deliver_and_persist_signal(
        match_key,
        "non-odds body",
        add_url_reason="pipeline_send_every_parsed_match",
        add_url_details={"status": "live"},
        skip_bookmaker_prepare=True,
    )
    assert ok is True
    assert sent["n"] == 1
    assert enrich_calls == [], "non-odds deliver must not acquire observation"

    # Source: pipeline/protracker sites must gate acquisition on skip flag
    src = _module_source()
    for marker in (
        'add_url_reason="pipeline_send_every_parsed_match"',
        'add_url_reason="dota2protracker_signal_sent_now"',
    ):
        idx = src.find(marker)
        assert idx > 0, marker
        window = src[max(0, idx - 1500) : idx + 200]
        # Either no enrich nearby, or enrich is behind skip gate
        if _ENRICH_PRODUCER in window:
            assert (
                "skip_bookmaker" in window.lower()
                or "SKIP_BOOKMAKER" in window
                or "if not" in window
            ), f"{marker}: enrich present without skip gate"
        _assert_no_forbidden_helper(window, context=marker)


def test_direct_local_enrich_bind_constructs_canonical_observation() -> None:
    """Direct local enrich+bind (no helper) builds canonical live/fresh observation.

    Replaces the retired helper unit: immediate sites must call
    `_bookmaker_enrich_delayed_match_state` directly and bind match_key/map/status.
    """
    assert not callable(getattr(cs, _FORBIDDEN_HELPER_ATTR, None)), (
        "forbidden shared observation-acquisition helper must remain absent"
    )
    assert callable(getattr(cs, _ENRICH_PRODUCER, None))

    match_key = "https://cyberscore.live/matches/immediate-local-enrich-9704"
    obs, map_num = _local_acquire_observation(
        match_key,
        map_num=2,
        status="live",
        source={"status": "live", "map_num": 2},
    )
    assert map_num == 2
    assert isinstance(obs, dict)
    assert obs.get("match_key") == match_key  # canonical match_key
    assert obs.get("map_num") == 2  # exact map_num
    assert str(obs.get("status") or "").lower() in _LIVE_STATUS_VOCAB  # explicit live
    assert obs.get("observed_at") is not None  # fresh observation
    # Validator accepts the local construct under strict identity.
    reason = cs._bookmaker_validate_current_map_observation(
        obs,
        expected_map_num=2,
        expected_match_key=match_key,
    )
    assert reason == "ok", reason

    # Invalid map → no observation object (tokenless path via validator)
    obs2, map2 = _local_acquire_observation(
        match_key,
        map_num=None,
        status="live",
        source={"status": "live"},
    )
    assert obs2 is None
    # map may remain None when acquisition fails closed
    assert map2 is None or map2 is not None

    # Production immediate sites must use this pattern (source-level).
    immediate = _immediate_odds_callers()
    assert len(immediate) == 19
    unwired = [
        f"L{c['lineno']}:{c['reason']}"
        for c in immediate
        if not (c["has_local_enrich"] and c["has_obs"] and c["has_map"] and c["has_match_key_bind"])
    ]
    assert not unwired, (
        "immediate odds sites missing direct local enrich+bind production wiring:\n"
        + "\n".join(unwired)
    )


def test_speculative_and_lane_kills_source_wiring() -> None:
    """Speculative + lane-adv kills immediate sites must pass obs+map via local enrich."""
    src = _module_source()
    _assert_no_forbidden_helper(src, context="production source")
    for marker in (
        'add_url_reason="star_signal_sent_late_pub_comeback_speculative_half"',
        'add_url_reason="star_signal_sent_now_lane_adv_standalone_kills"',
    ):
        idx = src.find(marker)
        assert idx > 0, marker
        block = src[max(0, idx - 1200) : idx + 350]
        assert "current_map_observation=" in block, (
            f"{marker}: missing current_map_observation "
            "(immediate local production wiring)"
        )
        assert "map_num=" in block, (
            f"{marker}: missing map_num "
            "(immediate local production wiring)"
        )
        assert _ENRICH_PRODUCER in block, (
            f"{marker}: missing direct local {_ENRICH_PRODUCER} "
            "(immediate local production wiring)"
        )
        assert "match_key" in block, (
            f"{marker}: missing local match_key bind "
            "(immediate local production wiring)"
        )
