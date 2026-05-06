from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path


TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from test_networth_dispatch_gates import BranchScenario, _run_branch_scenario, runtime  # noqa: E402


ALIGNED_LANE_OUTPUT = ("Top: win 70%", "Bot: win 70%", "Mid: win 70%")
OPPOSITE_LANE_OUTPUT = ("Top: lose 70%", "Bot: lose 70%", "Mid: lose 70%")
WEAK_OPPOSITE_LANE_OUTPUT = ("Top: lose 48%", "Bot: win 39%", "Mid: win 39%")
ALIGNED_LANE_ADV = {
    "pro_lane_advantage": 5.0,
}

OPPOSITE_LANE_ADV = {
    "pro_lane_advantage": -5.0,
}


def test_same_sign_lane_adv_guard_ignores_weak_values() -> None:
    guard = runtime._same_sign_lane_adv_guard(
        star_sign=1,
        lane_adv_dict_value=-4.99,
        lane_adv_protracker_value=-2.99,
    )

    assert guard["lane_adv_dict_sign"] is None
    assert guard["lane_adv_protracker_sign"] is None
    assert guard["opposes_target"] is False
    assert guard["aligned"] is False


def test_same_sign_lane_adv_guard_keeps_strong_values() -> None:
    guard = runtime._same_sign_lane_adv_guard(
        star_sign=1,
        lane_adv_dict_value=-5.0,
        lane_adv_protracker_value=3.0,
    )

    assert guard["lane_adv_dict_sign"] == -1
    assert guard["lane_adv_protracker_sign"] == 1
    assert guard["opposes_target"] is True
    assert guard["opposing_sources"] == ["lane_adv_dict"]


def _same_sign_case(*, game_time_seconds: int, target_networth_diff: int, metrics_extra: dict) -> BranchScenario:
    return BranchScenario(
        name="same_sign_lane_adv",
        game_time_seconds=game_time_seconds,
        target_side="radiant",
        target_networth_diff=target_networth_diff,
        has_early_star=True,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=0,
        raw_early_output={"solo": 3},
        raw_mid_output={"solo": 3},
        metrics_extra=metrics_extra,
    )


def test_same_sign_star_dispatches_immediately_when_lane_adv_matches(monkeypatch) -> None:
    result = _run_branch_scenario(
        monkeypatch,
        _same_sign_case(
            game_time_seconds=2 * 60,
            target_networth_diff=-1200,
            metrics_extra=ALIGNED_LANE_ADV,
        ),
        lane_output=ALIGNED_LANE_OUTPUT,
    )

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None
    assert result.add_url_calls[-1]["reason"] == "star_signal_sent_now"
    details = result.add_url_calls[-1]["details"]
    assert details["dispatch_mode"] == "immediate_star_rule"


def test_same_sign_star_dispatches_immediately_when_only_lane_adv_dict_matches(monkeypatch) -> None:
    result = _run_branch_scenario(
        monkeypatch,
        _same_sign_case(
            game_time_seconds=2 * 60,
            target_networth_diff=-1200,
            metrics_extra={},
        ),
        lane_output=ALIGNED_LANE_OUTPUT,
    )

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None
    assert result.add_url_calls[-1]["reason"] == "star_signal_sent_now"


def test_same_sign_star_dispatches_immediately_when_only_protracker_lane_adv_matches(monkeypatch) -> None:
    result = _run_branch_scenario(
        monkeypatch,
        _same_sign_case(
            game_time_seconds=2 * 60,
            target_networth_diff=-1200,
            metrics_extra=ALIGNED_LANE_ADV,
        ),
        lane_output=("", "", ""),
    )

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None
    assert result.add_url_calls[-1]["reason"] == "star_signal_sent_now"


def test_late_all_same_sign_dispatches_immediately_when_lane_adv_matches(monkeypatch) -> None:
    case = replace(
        _same_sign_case(
            game_time_seconds=2 * 60,
            target_networth_diff=-1200,
            metrics_extra=ALIGNED_LANE_ADV,
        ),
        has_early_star=False,
        has_all_star=True,
        all_sign=1,
    )

    result = _run_branch_scenario(
        monkeypatch,
        case,
        lane_output=ALIGNED_LANE_OUTPUT,
    )

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None
    assert result.add_url_calls[-1]["reason"] == "star_signal_sent_now"


def test_same_sign_star_waits_until_four_when_lane_adv_not_matching(monkeypatch) -> None:
    result = _run_branch_scenario(
        monkeypatch,
        _same_sign_case(
            game_time_seconds=(3 * 60) + 30,
            target_networth_diff=5000,
            metrics_extra=OPPOSITE_LANE_ADV,
        ),
        lane_output=OPPOSITE_LANE_OUTPUT,
    )

    assert result.sent_messages == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "same_sign_lane_adv_wait_4_10"
    assert result.queued_payload["dynamic_monitor_profile"] == "same_sign_lane_adv_wait_4_10"
    assert float(result.queued_payload["target_game_time"]) == float(runtime.NETWORTH_GATE_SAME_SIGN_LANE_ADV_FALLBACK_SECONDS)
    assert result.queued_payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_SAME_SIGN_LANE_ADV_PRE4_WAIT
    assert float(result.queued_payload["networth_monitor_hold_seconds"]) == 0.0


def test_same_sign_star_sends_after_four_when_target_leads_by_800(monkeypatch) -> None:
    result = _run_branch_scenario(
        monkeypatch,
        _same_sign_case(
            game_time_seconds=5 * 60,
            target_networth_diff=800,
            metrics_extra=OPPOSITE_LANE_ADV,
        ),
        lane_output=OPPOSITE_LANE_OUTPUT,
    )

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None
    assert result.add_url_calls[-1]["reason"] == "star_signal_sent_now_networth_gate"
    details = result.add_url_calls[-1]["details"]
    assert details["dispatch_status_label"] == runtime.NETWORTH_STATUS_4_10_SEND_800
    assert details["release_reason"] == runtime.NETWORTH_STATUS_4_10_SEND_800
    assert float(details["networth_monitor_hold_seconds"]) == 0.0


def test_same_sign_star_queues_until_ten_when_four_to_ten_lead_is_below_800(monkeypatch) -> None:
    result = _run_branch_scenario(
        monkeypatch,
        _same_sign_case(
            game_time_seconds=5 * 60,
            target_networth_diff=799,
            metrics_extra=OPPOSITE_LANE_ADV,
        ),
        lane_output=OPPOSITE_LANE_OUTPUT,
    )

    assert result.sent_messages == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "same_sign_lane_adv_wait_4_10"
    assert result.queued_payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_SAME_SIGN_LANE_ADV_WAIT_800
    assert float(result.queued_payload["networth_monitor_threshold"]) == 800.0
    assert float(result.queued_payload["networth_monitor_hold_seconds"]) == 0.0


def test_same_sign_star_sends_at_ten_even_without_800_lead(monkeypatch) -> None:
    result = _run_branch_scenario(
        monkeypatch,
        _same_sign_case(
            game_time_seconds=10 * 60,
            target_networth_diff=-2000,
            metrics_extra=OPPOSITE_LANE_ADV,
        ),
        lane_output=OPPOSITE_LANE_OUTPUT,
    )

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None
    assert result.add_url_calls[-1]["reason"] == "star_signal_sent_now_target_reached"
    details = result.add_url_calls[-1]["details"]
    assert details["dispatch_status_label"] == runtime.NETWORTH_STATUS_SAME_SIGN_LANE_ADV_FALLBACK_10_SEND
    assert details["release_reason"] == runtime.NETWORTH_STATUS_SAME_SIGN_LANE_ADV_FALLBACK_10_SEND


def test_early_only_star_with_aligned_lane_adv_still_dispatches_immediately(monkeypatch) -> None:
    result = _run_branch_scenario(
        monkeypatch,
        BranchScenario(
            name="early_only_playtime_like",
            game_time_seconds=(11 * 60) + 58,
            target_side="dire",
            target_networth_diff=1752,
            has_early_star=True,
            early_sign=-1,
            has_late_star=False,
            late_sign=1,
            expected_send_calls=0,
            raw_early_output={"solo": -3},
            raw_mid_output={"solo": 0},
            raw_post_lane_output={"dota2protracker_cp1vs1": -1.88},
            metrics_extra={"pro_lane_advantage": -1.96},
        ),
        lane_output=OPPOSITE_LANE_OUTPUT,
    )

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None
    assert result.add_url_calls[-1]["reason"] == "star_signal_sent_now"
    assert result.add_url_calls[-1]["details"]["dispatch_mode"] == "immediate_star_rule"


def test_no_late_immediate_star_waits_when_lane_adv_dict_opposes_target(monkeypatch) -> None:
    result = _run_branch_scenario(
        monkeypatch,
        BranchScenario(
            name="yellow_submarine_mouz_map3_lane_dict_guard",
            game_time_seconds=38,
            target_side="radiant",
            target_networth_diff=647,
            has_early_star=True,
            early_sign=1,
            has_late_star=False,
            late_sign=-1,
            has_all_star=True,
            all_sign=1,
            expected_send_calls=0,
            raw_early_output={"counterpick_1vs1": 6, "solo": 4},
            raw_mid_output={"counterpick_1vs1": 3, "solo": 1},
            raw_post_lane_output={"counterpick_1vs1": 8, "counterpick_1vs2": 12, "synergy_duo": 8},
        ),
        lane_output=OPPOSITE_LANE_OUTPUT,
    )

    assert result.sent_messages == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "same_sign_lane_adv_wait_4_10"
    assert result.queued_payload["dynamic_monitor_profile"] == "same_sign_lane_adv_wait_4_10"
    assert result.queued_payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_SAME_SIGN_LANE_ADV_PRE4_WAIT
    assert result.queued_payload["lane_adv_dict_sign"] == -1
    assert result.queued_payload["lane_adv_protracker_sign"] is None


def test_no_late_immediate_star_ignores_weak_lane_adv_dict_opposition(monkeypatch) -> None:
    result = _run_branch_scenario(
        monkeypatch,
        BranchScenario(
            name="weak_lane_dict_guard_neutral",
            game_time_seconds=38,
            target_side="radiant",
            target_networth_diff=647,
            has_early_star=True,
            early_sign=1,
            has_late_star=False,
            late_sign=-1,
            has_all_star=True,
            all_sign=1,
            expected_send_calls=0,
            raw_early_output={"counterpick_1vs1": 6, "solo": 4},
            raw_mid_output={"counterpick_1vs1": 3, "solo": 1},
            raw_post_lane_output={"counterpick_1vs1": 8, "counterpick_1vs2": 12, "synergy_duo": 8},
        ),
        lane_output=WEAK_OPPOSITE_LANE_OUTPUT,
    )

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None
    assert result.add_url_calls[-1]["reason"] == "star_signal_sent_now"


def test_no_late_early_and_all_opposite_signs_are_rejected(monkeypatch) -> None:
    result = _run_branch_scenario(
        monkeypatch,
        BranchScenario(
            name="no_late_early_all_opposite_signs_reject",
            game_time_seconds=12 * 60,
            target_side="radiant",
            target_networth_diff=3200,
            has_early_star=True,
            early_sign=1,
            has_late_star=False,
            late_sign=1,
            has_all_star=True,
            all_sign=-1,
            expected_send_calls=0,
            raw_early_output={"solo": 3},
            raw_mid_output={"solo": 0},
            raw_post_lane_output={"dota2protracker_cp1vs1": -3},
        ),
    )

    assert result.sent_messages == []
    assert result.queued_payload is None
    assert result.add_url_calls
    assert result.add_url_calls[-1]["reason"] == "star_signal_rejected_no_late_early_all_opposite_signs"
    details = result.add_url_calls[-1]["details"]
    assert isinstance(details, dict)
    assert details["selected_early_star"] is True
    assert details["selected_late_star"] is False
    assert details["selected_all_star"] is True
    assert details["selected_early_sign"] == 1
    assert details["selected_all_sign"] == -1
    assert details["star_dispatch_flags"]["no_late_early_all_opposite_signs"] is True
    assert details["star_dispatch_flags"]["send_now_no_late_early_or_all"] is False


def test_no_late_immediate_star_waits_when_protracker_lane_adv_opposes_target(monkeypatch) -> None:
    result = _run_branch_scenario(
        monkeypatch,
        BranchScenario(
            name="no_late_protracker_lane_adv_guard",
            game_time_seconds=(3 * 60) + 30,
            target_side="radiant",
            target_networth_diff=5000,
            has_early_star=True,
            early_sign=1,
            has_late_star=False,
            late_sign=-1,
            has_all_star=True,
            all_sign=1,
            expected_send_calls=0,
            metrics_extra=OPPOSITE_LANE_ADV,
        ),
        lane_output=ALIGNED_LANE_OUTPUT,
    )

    assert result.sent_messages == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "same_sign_lane_adv_wait_4_10"
    assert result.queued_payload["lane_adv_dict_sign"] == 1
    assert result.queued_payload["lane_adv_protracker_sign"] == -1


def test_no_late_lane_adv_guard_sends_after_four_when_target_leads_by_800(monkeypatch) -> None:
    result = _run_branch_scenario(
        monkeypatch,
        BranchScenario(
            name="no_late_lane_adv_guard_send_800",
            game_time_seconds=5 * 60,
            target_side="radiant",
            target_networth_diff=800,
            has_early_star=True,
            early_sign=1,
            has_late_star=False,
            late_sign=-1,
            has_all_star=True,
            all_sign=1,
            expected_send_calls=0,
        ),
        lane_output=OPPOSITE_LANE_OUTPUT,
    )

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None
    assert result.add_url_calls[-1]["reason"] == "star_signal_sent_now_networth_gate"
    details = result.add_url_calls[-1]["details"]
    assert details["dispatch_status_label"] == runtime.NETWORTH_STATUS_4_10_SEND_800
    assert details["release_reason"] == runtime.NETWORTH_STATUS_4_10_SEND_800
    assert details["lane_adv_dict_sign"] == -1


def _same_sign_delayed_payload() -> dict:
    return {
        "message": "test message",
        "reason": "same_sign_lane_adv_wait_4_10",
        "json_url": "https://dltv.org/live/test.json",
        "target_game_time": float(runtime.NETWORTH_GATE_SAME_SIGN_LANE_ADV_FALLBACK_SECONDS),
        "queued_at": 1_700_000_000.0,
        "queued_game_time": 210.0,
        "last_game_time": 210.0,
        "last_progress_at": 1_700_000_000.0,
        "dispatch_status_label": runtime.NETWORTH_STATUS_SAME_SIGN_LANE_ADV_PRE4_WAIT,
        "add_url_reason": "star_signal_sent_delayed",
        "add_url_details": {
            "status": "live",
            "dispatch_mode": "delayed_same_sign_lane_adv_wait_4_10",
            "delay_reason": "same_sign_lane_adv_wait_4_10",
        },
        "fallback_send_status_label": runtime.NETWORTH_STATUS_SAME_SIGN_LANE_ADV_FALLBACK_10_SEND,
        "send_on_target_game_time": True,
        "dynamic_monitor_profile": "same_sign_lane_adv_wait_4_10",
        "networth_monitor_threshold_4_to_10": 800.0,
        "networth_monitor_status_4_to_10": runtime.NETWORTH_STATUS_SAME_SIGN_LANE_ADV_WAIT_800,
        "networth_target_side": "radiant",
        "networth_monitor_hold_seconds": 0.0,
        "retry_attempt_count": 0,
        "next_retry_at": 0.0,
    }


def test_same_sign_lane_adv_dynamic_monitor_snapshot_transitions() -> None:
    payload = _same_sign_delayed_payload()

    pre4 = runtime._dynamic_monitor_snapshot_for_payload(payload, (3 * 60) + 59)
    assert pre4["threshold"] is None
    assert pre4["status_label"] == runtime.NETWORTH_STATUS_SAME_SIGN_LANE_ADV_PRE4_WAIT

    wait_800 = runtime._dynamic_monitor_snapshot_for_payload(payload, 5 * 60)
    assert float(wait_800["threshold"]) == 800.0
    assert wait_800["status_label"] == runtime.NETWORTH_STATUS_SAME_SIGN_LANE_ADV_WAIT_800

    fallback = runtime._dynamic_monitor_snapshot_for_payload(payload, 10 * 60)
    assert fallback["threshold"] is None
    assert fallback["status_label"] == runtime.NETWORTH_STATUS_SAME_SIGN_LANE_ADV_FALLBACK_10_SEND


def _run_same_sign_delayed_worker(monkeypatch, *, game_time: float, radiant_lead: float) -> list[dict]:
    deliveries: list[dict] = []
    match_key = "dltv.org/matches/test-match.0"
    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
        runtime.monitored_matches[match_key] = _same_sign_delayed_payload()

    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_120.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _match_key: False)
    monkeypatch.setattr(runtime, "_fetch_delayed_match_state", lambda _json_url: {"game_time": game_time, "radiant_lead": radiant_lead})
    monkeypatch.setattr(runtime, "_maybe_refresh_stale_cyberscore_delayed_state", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_refresh_stake_multiplier_message", lambda message, **_kwargs: message)
    monkeypatch.setattr(runtime, "_refresh_message_bookmaker_block_for_dispatch", lambda _match_key, message: message)
    monkeypatch.setattr(runtime, "_print_star_metrics_snapshot", lambda *_args, **_kwargs: None)

    def _record_delivery(match_key_arg, message_text, *, add_url_reason, add_url_details, **_kwargs):
        deliveries.append(
            {
                "match_key": match_key_arg,
                "message": message_text,
                "reason": add_url_reason,
                "details": dict(add_url_details or {}),
            }
        )
        return True

    monkeypatch.setattr(runtime, "_deliver_and_persist_signal", _record_delivery)

    try:
        runtime._drain_due_delayed_signals_once()
    finally:
        with runtime.monitored_matches_lock:
            runtime.monitored_matches.clear()
    return deliveries


def test_same_sign_delayed_worker_sends_after_four_when_target_leads_by_800(monkeypatch) -> None:
    deliveries = _run_same_sign_delayed_worker(monkeypatch, game_time=5 * 60, radiant_lead=800.0)

    assert len(deliveries) == 1
    assert deliveries[0]["reason"] == "star_signal_sent_delayed"
    details = deliveries[0]["details"]
    assert details["dispatch_status_label"] == runtime.NETWORTH_STATUS_4_10_SEND_800
    assert details["release_reason"] == runtime.NETWORTH_STATUS_4_10_SEND_800
    assert float(details["networth_monitor_hold_seconds"]) == 0.0


def test_same_sign_delayed_worker_sends_at_ten_without_800_lead(monkeypatch) -> None:
    deliveries = _run_same_sign_delayed_worker(monkeypatch, game_time=10 * 60, radiant_lead=-1500.0)

    assert len(deliveries) == 1
    details = deliveries[0]["details"]
    assert details["dispatch_status_label"] == runtime.NETWORTH_STATUS_SAME_SIGN_LANE_ADV_FALLBACK_10_SEND
    assert details["release_reason"] == runtime.NETWORTH_STATUS_SAME_SIGN_LANE_ADV_FALLBACK_10_SEND
    assert float(details["target_networth_diff"]) == -1500.0
