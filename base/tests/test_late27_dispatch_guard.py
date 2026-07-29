"""27+ late-гейт: late-driven отправка требует >=2 late-хитов, WR >= порога и
отсутствия противоположных star-хитов в блоке All.

Референс-кейс (прод, 27:39): late star = Counterpick_1vs1 +7 (WR65, один хит),
в All — Protracker_1vs1 -5.0 (WR65) на другую команду. Такой сигнал уходить
не должен ни из основного пути, ни из delayed watcher'а.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from test_networth_dispatch_gates import BranchScenario, _run_branch_scenario, runtime  # noqa: E402
from test_same_sign_lane_adv_dispatch import _patch_early_late_wr  # noqa: E402


OPPOSITE_ALL_HITS = [{"metric": "dota2protracker_cp1vs1", "value": -5.0, "wr_level": 65}]
SAME_SIGN_ALL_HITS = [{"metric": "counterpick_1vs1", "value": 7.0, "wr_level": 65}]


def _snapshot(
    *,
    late_wr_pct: Optional[float] = 65.0,
    late_hit_count: Optional[int] = 1,
    late_sign: Optional[int] = 1,
    has_late_star: bool = True,
    has_early_star: bool = True,
    early_sign: Optional[int] = -1,
    all_star_hits: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    return runtime._build_late27_dispatch_guard_snapshot(
        has_selected_late_star=has_late_star,
        selected_late_sign=late_sign,
        late_wr_pct=late_wr_pct,
        late_star_hit_count=late_hit_count,
        has_selected_early_star=has_early_star,
        selected_early_sign=early_sign,
        all_star_hits=OPPOSITE_ALL_HITS if all_star_hits is None else all_star_hits,
    )


def _evaluate(snapshot: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
    kwargs.setdefault("target_side", "radiant")
    kwargs.setdefault("game_time_seconds", 27 * 60 + 39)
    return runtime._evaluate_late27_dispatch_guard(snapshot, **kwargs)


def test_guard_thresholds_are_pinned() -> None:
    assert runtime.LATE27_DISPATCH_MIN_LATE_HITS == 2
    assert runtime.LATE27_DISPATCH_MIN_LATE_WR == 65.0
    assert runtime.LATE27_DISPATCH_GUARD_REJECT_REASON == "star_signal_rejected_late27_dispatch_guard"


def test_opposite_sign_star_hit_metrics_filters_by_sign() -> None:
    hits = OPPOSITE_ALL_HITS + SAME_SIGN_ALL_HITS + [{"metric": "solo", "value": 0.0}]

    assert runtime._opposite_sign_star_hit_metrics(hits, 1) == ["dota2protracker_cp1vs1"]
    assert runtime._opposite_sign_star_hit_metrics(hits, -1) == ["counterpick_1vs1"]
    assert runtime._opposite_sign_star_hit_metrics(hits, None) == []
    assert runtime._opposite_sign_star_hit_metrics(None, 1) == []


def test_production_case_is_blocked() -> None:
    """Late WR65 с одним хитом + противоположный star-хит в All → отказ."""

    guard = _evaluate(_snapshot())

    assert guard["active"] is True
    assert guard["blocked"] is True
    assert guard["reasons"] == ["late_hits_below_min", "all_opposite_star_hit"]
    assert guard["all_opposite_hit_metrics"] == ["dota2protracker_cp1vs1"]


def test_two_hits_wr65_without_opposite_all_passes() -> None:
    guard = _evaluate(_snapshot(late_hit_count=2, all_star_hits=SAME_SIGN_ALL_HITS))

    assert guard["active"] is True
    assert guard["blocked"] is False
    assert guard["reasons"] == []


def test_two_hits_with_opposite_all_hit_is_blocked() -> None:
    guard = _evaluate(_snapshot(late_hit_count=2, late_wr_pct=75.0))

    assert guard["blocked"] is True
    assert guard["reasons"] == ["all_opposite_star_hit"]


def test_single_hit_is_blocked_even_at_high_wr() -> None:
    guard = _evaluate(_snapshot(late_hit_count=1, late_wr_pct=80.0, all_star_hits=[]))

    assert guard["blocked"] is True
    assert guard["reasons"] == ["late_hits_below_min"]


def test_wr_below_min_is_blocked_with_two_hits() -> None:
    guard = _evaluate(_snapshot(late_hit_count=2, late_wr_pct=60.0, all_star_hits=[]))

    assert guard["blocked"] is True
    assert guard["reasons"] == ["late_wr_below_min"]


def test_unknown_hit_count_or_wr_is_blocked_on_complete_snapshot() -> None:
    """В полном снимке (текущая версия кода) неизвестные late-данные = отказ."""

    assert _evaluate(_snapshot(late_hit_count=None, all_star_hits=[]))["reasons"] == [
        "late_hits_unknown"
    ]
    assert _evaluate(_snapshot(late_hit_count=2, late_wr_pct=None, all_star_hits=[]))["reasons"] == [
        "late_wr_unknown"
    ]


def test_guard_is_inactive_when_valid_early_supports_late() -> None:
    guard = _evaluate(_snapshot(has_early_star=True, early_sign=1))

    assert guard["early_supports_late"] is True
    assert guard["active"] is False
    assert guard["blocked"] is False


def test_guard_is_inactive_before_27_00() -> None:
    guard = _evaluate(_snapshot(), game_time_seconds=27 * 60 - 1)

    assert guard["active"] is False
    assert guard["blocked"] is False


def test_guard_is_inactive_for_other_target_side() -> None:
    guard = _evaluate(_snapshot(), target_side="dire")

    assert guard["active"] is False
    assert guard["blocked"] is False


def test_guard_is_inactive_without_late_star() -> None:
    guard = _evaluate(_snapshot(has_late_star=False))

    assert guard["active"] is False
    assert guard["blocked"] is False


def test_force_odds_signal_test_bypasses_guard() -> None:
    guard = _evaluate(_snapshot(), force_odds_signal_test_active=True)

    assert guard["active"] is True
    assert guard["blocked"] is False


def test_snapshot_from_stake_multiplier_context_roundtrip() -> None:
    context = runtime._build_stake_multiplier_context(
        stake_team_name="Radiant Team",
        target_side="radiant",
        team_elo_meta=None,
        radiant_team_name="Radiant Team",
        dire_team_name="Dire Team",
        selected_early_sign=-1,
        selected_late_sign=1,
        has_selected_early_star=True,
        has_selected_late_star=True,
        early_wr_pct=70.0,
        late_wr_pct=65.0,
        late_star_hit_count=1,
        selected_all_sign=-1,
        has_selected_all_star=False,
        all_wr_pct=65.0,
        all_star_hits=OPPOSITE_ALL_HITS,
    )

    assert context["all_star_hits"] == OPPOSITE_ALL_HITS

    guard = _evaluate(runtime._late27_dispatch_guard_snapshot_from_context(context))

    assert guard["blocked"] is True
    assert guard["all_star_hits_known"] is True
    assert guard["reasons"] == ["late_hits_below_min", "all_opposite_star_hit"]


def test_legacy_context_without_all_hits_skips_all_check() -> None:
    """Delayed-записи, созданные до внедрения гейта, проверяются только по late."""

    legacy_context = {
        "has_selected_late_star": True,
        "selected_late_sign": 1,
        "late_wr_pct": 65.0,
        "late_star_hit_count": 2,
        "has_selected_early_star": False,
        "selected_early_sign": None,
    }

    guard = _evaluate(runtime._late27_dispatch_guard_snapshot_from_context(legacy_context))

    assert guard["all_star_hits_known"] is False
    assert guard["blocked"] is False

    legacy_one_hit = dict(legacy_context, late_star_hit_count=1)
    guard_one_hit = _evaluate(
        runtime._late27_dispatch_guard_snapshot_from_context(legacy_one_hit)
    )

    assert guard_one_hit["reasons"] == ["late_hits_below_min"]

    # В legacy-записи без данных о late-хитах гейт не блокирует: нечем
    # подтвердить нарушение, а WR известен и проходит порог.
    legacy_without_hits = {
        key: value for key, value in legacy_context.items() if key != "late_star_hit_count"
    }
    guard_legacy = _evaluate(
        runtime._late27_dispatch_guard_snapshot_from_context(legacy_without_hits)
    )

    assert guard_legacy["active"] is True
    assert guard_legacy["blocked"] is False


def _patch_comeback_table(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "late_pub_comeback_table_thresholds_by_wr",
        {65: {27: -2000.0, 28: -2500.0}},
        raising=False,
    )


def test_late_only_27_dispatch_is_rejected_end_to_end(monkeypatch) -> None:
    _patch_comeback_table(monkeypatch)
    _patch_early_late_wr(monkeypatch, early_level=65, late_level=65, all_level=65)
    case = BranchScenario(
        name="late27_guard_reject",
        game_time_seconds=27 * 60 + 39,
        target_side="radiant",
        target_networth_diff=1500,
        has_early_star=False,
        early_sign=-1,
        has_late_star=True,
        late_sign=1,
        has_all_star=False,
        expected_send_calls=0,
        raw_early_output={"counterpick_1vs1": -14},
        raw_mid_output={"counterpick_1vs1": 7},
        # All-блок невалиден как STAR, но содержит WR60+ хит на другую команду.
        raw_post_lane_output={"counterpick_1vs1": -8, "synergy_duo": 4},
    )

    result = _run_branch_scenario(monkeypatch, case)

    assert result.sent_messages == []
    assert result.queued_payload is None
    assert result.add_url_calls[-1]["reason"] == runtime.LATE27_DISPATCH_GUARD_REJECT_REASON
    details = result.add_url_calls[-1]["details"]
    assert details["dispatch_status_label"] == runtime.LATE27_DISPATCH_GUARD_STATUS_LABEL
    assert "late_hits_below_min" in details["late27_guard_reasons"]
    assert "all_opposite_star_hit" in details["late27_guard_reasons"]
    assert details["late27_guard_all_opposite_hit_metrics"] == ["counterpick_1vs1"]


def _late27_watcher_payload(
    *,
    late_hit_count: int,
    all_star_hits: Optional[List[Dict[str, Any]]],
) -> Dict[str, Any]:
    return {
        "message": "СТАВКА НА Radiant Team x0.5\nRadiant Team VS Dire Team\n",
        "stake_multiplier_context": {
            "stake_team_name": "Radiant Team",
            "target_side": "radiant",
            "has_selected_late_star": True,
            "selected_late_sign": 1,
            "late_wr_pct": 65.0,
            "late_star_hit_count": late_hit_count,
            "has_selected_early_star": False,
            "selected_early_sign": None,
            "all_star_hits": all_star_hits,
        },
        "reason": "late_star_pub_comeback_table_monitor",
        "json_url": "https://dltv.org/live/test.json",
        "target_game_time": float(runtime.LATE_PUB_COMEBACK_TABLE_START_SECONDS),
        "queued_at": 1_700_000_000.0,
        "queued_game_time": 26 * 60,
        "last_game_time": 26 * 60,
        "last_progress_at": 1_700_000_000.0,
        "dispatch_status_label": runtime.NETWORTH_STATUS_LATE_PUB_TABLE_WAIT,
        "add_url_reason": "star_signal_sent_delayed",
        "add_url_details": {
            "dispatch_status_label": runtime.NETWORTH_STATUS_LATE_PUB_TABLE_WAIT,
            "target_side": "radiant",
            "networth_target_side": "radiant",
        },
        "fallback_send_status_label": runtime.NETWORTH_STATUS_LATE_PUB_TABLE_WAIT,
        "send_on_target_game_time": False,
        "allow_live_recheck": False,
        "retry_attempt_count": 0,
        "next_retry_at": 0.0,
        "late_pub_comeback_table_active": True,
        "late_pub_comeback_table_wr_level": 65,
        "networth_target_side": "radiant",
    }


def _run_late27_delayed_worker(monkeypatch, payload: Dict[str, Any]) -> Dict[str, Any]:
    deliveries: List[Dict[str, Any]] = []
    add_url_calls: List[Dict[str, Any]] = []
    dropped: List[str] = []
    match_key = "dltv.org/matches/test-match.0"

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
        runtime.monitored_matches[match_key] = dict(payload)

    _patch_comeback_table(monkeypatch)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_120.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _match_key: False)
    monkeypatch.setattr(
        runtime,
        "_fetch_delayed_match_state",
        lambda _json_url: {"game_time": 27 * 60 + 39, "radiant_lead": 3000.0},
    )
    monkeypatch.setattr(runtime, "_maybe_refresh_stale_cyberscore_delayed_state", lambda *_a, **_k: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_a, **_k: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_a, **_k: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_a, **_k: None)
    monkeypatch.setattr(runtime, "_refresh_stake_multiplier_message", lambda message, **_k: message)
    monkeypatch.setattr(runtime, "_refresh_message_bookmaker_block_for_dispatch", lambda _key, message: message)
    monkeypatch.setattr(runtime, "_print_star_metrics_snapshot", lambda *_a, **_k: None)

    def _record_add_url(url: str, reason: str = "unspecified", details: Any = None):
        add_url_calls.append(
            {
                "url": url,
                "reason": reason,
                "details": dict(details) if isinstance(details, dict) else details,
            }
        )

    def _record_drop(match_key_arg: str, reason: str = "") -> bool:
        dropped.append(str(reason))
        with runtime.monitored_matches_lock:
            runtime.monitored_matches.pop(str(match_key_arg), None)
        return True

    def _record_delivery(match_key_arg, message_text, *, add_url_reason, add_url_details, **_kwargs):
        deliveries.append(
            {
                "match_key": match_key_arg,
                "reason": add_url_reason,
                "details": dict(add_url_details or {}),
            }
        )
        return True

    monkeypatch.setattr(runtime, "add_url", _record_add_url)
    monkeypatch.setattr(runtime, "_drop_delayed_match", _record_drop)
    monkeypatch.setattr(runtime, "_deliver_and_persist_signal", _record_delivery)

    try:
        runtime._drain_due_delayed_signals_once()
    finally:
        with runtime.monitored_matches_lock:
            runtime.monitored_matches.clear()

    return {"deliveries": deliveries, "add_url_calls": add_url_calls, "dropped": dropped}


def test_delayed_watcher_rejects_blocked_late27_signal(monkeypatch) -> None:
    result = _run_late27_delayed_worker(
        monkeypatch,
        _late27_watcher_payload(late_hit_count=1, all_star_hits=OPPOSITE_ALL_HITS),
    )

    assert result["deliveries"] == []
    assert result["dropped"] == ["late27_dispatch_guard"]
    assert result["add_url_calls"][-1]["reason"] == runtime.LATE27_DISPATCH_GUARD_REJECT_REASON
    details = result["add_url_calls"][-1]["details"]
    assert details["dispatch_status_label"] == runtime.LATE27_DISPATCH_GUARD_STATUS_LABEL
    assert details["dispatch_mode"] == "rejected_late27_dispatch_guard_watcher"
    assert "late_hits_below_min" in details["late27_guard_reasons"]
    assert "all_opposite_star_hit" in details["late27_guard_reasons"]


def test_delayed_watcher_drops_restored_valid_late_opposite_all(monkeypatch) -> None:
    payload = _late27_watcher_payload(
        late_hit_count=2,
        all_star_hits=OPPOSITE_ALL_HITS,
    )
    payload["stake_multiplier_context"].update(
        {
            "has_selected_all_star": True,
            "selected_all_sign": -1,
            "all_wr_pct": 80.0,
            "late_wr_pct": 70.0,
            "late_star_hit_metrics": ["counterpick_1vs2", "solo"],
        }
    )

    result = _run_late27_delayed_worker(monkeypatch, payload)

    assert result["deliveries"] == []
    assert result["dropped"] == ["late_opposite_all"]
    assert (
        result["add_url_calls"][-1]["reason"]
        == runtime.LATE_OPPOSITE_ALL_REJECT_REASON
    )
    details = result["add_url_calls"][-1]["details"]
    assert details["dispatch_mode"] == "rejected_late_opposite_all_delayed"
    assert details["late_wr_pct"] == 70.0
    assert details["all_wr_pct"] == 80.0


def test_delayed_watcher_keeps_sending_valid_late27_signal(monkeypatch) -> None:
    result = _run_late27_delayed_worker(
        monkeypatch,
        _late27_watcher_payload(late_hit_count=2, all_star_hits=SAME_SIGN_ALL_HITS),
    )

    assert result["dropped"] == []
    assert len(result["deliveries"]) == 1
    assert result["deliveries"][0]["details"]["dispatch_status_label"] == (
        runtime.NETWORTH_STATUS_LATE_PUB_TABLE_SEND
    )
