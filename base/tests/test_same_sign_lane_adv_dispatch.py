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
ALIGNED_LANE_ADV = {
    "pro_lane_advantage": 5.0,
}

OPPOSITE_LANE_ADV = {
    "pro_lane_advantage": -5.0,
}


def test_late_pre27_watcher_config_uses_late_all_average_wr(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "late_pre27_watcher_thresholds_by_group_wr",
        {
            "late_all": {
                70: {10: -900.0, 11: -1200.0},
                75: {10: -1100.0, 11: -1400.0},
            }
        },
        raising=False,
    )

    config = runtime._late_pre27_watcher_monitor_config(
        signal_group="late_all",
        target_sign=1,
        late_wr_pct=70.0,
        all_wr_pct=80.0,
        selected_star_wr=60,
    )

    assert config is not None
    assert config["profile"] == runtime.LATE_PRE27_WATCHER_PROFILE
    assert config["target_side"] == "radiant"
    assert config["wr_level"] == 75
    assert config["thresholds_by_minute"] == {10: -1100.0, 11: -1400.0}


def test_late_pre27_watcher_snapshot_uses_latest_elapsed_minute() -> None:
    payload = {
        "dynamic_monitor_profile": runtime.LATE_PRE27_WATCHER_PROFILE,
        "target_game_time": 27 * 60,
        "networth_monitor_thresholds_by_minute": {
            10: -900.0,
            11: -1200.0,
            12: -1500.0,
        },
        "networth_monitor_status": runtime.NETWORTH_STATUS_LATE_PRE27_WATCHER_WAIT,
    }

    before_10 = runtime._dynamic_monitor_snapshot_for_payload(payload, (9 * 60) + 59)
    at_11 = runtime._dynamic_monitor_snapshot_for_payload(payload, (11 * 60) + 30)
    at_27 = runtime._dynamic_monitor_snapshot_for_payload(payload, 27 * 60)

    assert before_10["threshold"] is None
    assert at_11["threshold"] == -1200.0
    assert at_11["source_minute"] == 11
    assert at_11["status_label"] == runtime.NETWORTH_STATUS_LATE_PRE27_WATCHER_WAIT
    assert at_27["threshold"] is None


def test_late_only_no_early_queues_pre27_watcher(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "late_pre27_watcher_thresholds_by_group_wr",
        {"late_only": {90: {10: -900.0, 11: -1200.0, 12: -1500.0}}},
        raising=False,
    )
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, phase: (
            {"level": 90, "min_odds": 1.11, "wr_pct": 90.0}
            if phase == "late"
            else None
        ),
    )

    result = _run_branch_scenario(
        monkeypatch,
        BranchScenario(
            name="late_only_pre27_watcher",
            game_time_seconds=12 * 60,
            target_side="radiant",
            target_networth_diff=-2000,
            has_early_star=False,
            early_sign=1,
            has_late_star=True,
            late_sign=1,
            expected_send_calls=0,
            expected_queue=True,
            raw_early_output={"counterpick_1vs1": 1, "solo": 1},
            raw_mid_output={"counterpick_1vs1": 10, "solo": 8},
        ),
    )

    assert result.sent_messages == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "late_only_no_early_star_pre27_watcher"
    assert result.queued_payload["dynamic_monitor_profile"] == runtime.LATE_PRE27_WATCHER_PROFILE
    assert result.queued_payload["late_pre27_watcher_group"] == "late_only"
    assert int(result.queued_payload["late_pre27_watcher_wr_level"]) == 90
    assert float(result.queued_payload["networth_monitor_threshold"]) == -1500.0
    assert result.queued_payload["send_on_target_game_time"] is False


def test_same_sign_lane_adv_guard_uses_dict_only_threshold_for_direction() -> None:
    guard = runtime._same_sign_lane_adv_guard(
        star_sign=1,
        lane_adv_dict_value=3.0,
        lane_adv_protracker_value=-5.0,
    )

    assert guard["lane_adv_dict_sign"] == 1
    assert guard["lane_adv_protracker_sign"] == -1
    assert guard["lane_adv_pair_sign"] is None
    assert guard["opposing_sources"] == []
    assert guard["opposes_target"] is False
    assert guard["aligned"] is True


def test_same_sign_lane_adv_guard_rejects_weak_dict_even_if_protracker_matches() -> None:
    guard = runtime._same_sign_lane_adv_guard(
        star_sign=1,
        lane_adv_dict_value=2.99,
        lane_adv_protracker_value=5.0,
    )

    assert guard["lane_adv_dict_sign"] is None
    assert guard["lane_adv_protracker_sign"] == 1
    assert guard["lane_adv_pair_sign"] is None
    assert guard["opposes_target"] is False
    assert guard["aligned"] is False


def test_same_sign_lane_adv_guard_marks_dict_opposition() -> None:
    guard = runtime._same_sign_lane_adv_guard(
        star_sign=1,
        lane_adv_dict_value=-3.0,
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


def test_same_sign_star_waits_when_only_protracker_lane_adv_matches(monkeypatch) -> None:
    result = _run_branch_scenario(
        monkeypatch,
        _same_sign_case(
            game_time_seconds=2 * 60,
            target_networth_diff=-1200,
            metrics_extra=ALIGNED_LANE_ADV,
        ),
        lane_output=("", "", ""),
    )

    assert result.sent_messages == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "same_sign_lane_adv_wait_4_10"
    assert result.queued_payload["lane_adv_dict_sign"] is None
    assert result.queued_payload["lane_adv_protracker_sign"] == 1


def test_late_all_no_early_uses_pre27_watcher_even_when_lane_adv_matches(monkeypatch) -> None:
    _patch_early_late_wr(monkeypatch, early_level=60, late_level=65, all_level=75)
    monkeypatch.setattr(
        runtime,
        "late_pre27_watcher_thresholds_by_group_wr",
        {"late_all": {70: {10: -1000.0, 11: -1200.0}}},
        raising=False,
    )
    case = replace(
        _same_sign_case(
            game_time_seconds=(10 * 60) + 50,
            target_networth_diff=-2656,
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

    assert result.sent_messages == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "late_all_no_early_star_pre27_watcher"
    assert result.queued_payload["dynamic_monitor_profile"] == runtime.LATE_PRE27_WATCHER_PROFILE
    assert result.queued_payload["late_pre27_watcher_group"] == "late_all"
    assert int(result.queued_payload["late_pre27_watcher_wr_level"]) == 70
    assert float(result.queued_payload["networth_monitor_threshold"]) == -1000.0
    assert result.queued_payload["send_on_target_game_time"] is False


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
    _patch_early_wr(monkeypatch, 65.0)

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
            raw_mid_output={"counterpick_1vs1": -1, "counterpick_1vs2": -1, "solo": -1},
            raw_post_lane_output={"dota2protracker_cp1vs1": -1.88},
            metrics_extra={"pro_lane_advantage": -1.96},
        ),
        lane_output=OPPOSITE_LANE_OUTPUT,
    )

    assert len(result.sent_messages) == 1
    assert result.sent_messages[0].startswith("СТАВКА НА Dire Team x0.5\n")
    assert result.queued_payload is None
    assert result.add_url_calls[-1]["reason"] == "star_signal_sent_now"
    assert result.add_url_calls[-1]["details"]["dispatch_mode"] == "immediate_early_only_target_half"


def _patch_early_wr(monkeypatch, wr_pct: float) -> None:
    def _recommend(_data, phase):
        if str(phase) == "early":
            return {"wr_pct": float(wr_pct), "min_odds": 1.67}
        return None

    monkeypatch.setattr(runtime, "_recommend_odds_for_block", _recommend)


def _patch_early_late_wr(monkeypatch, *, early_level: int, late_level: int, all_level=None) -> None:
    def _recommend(_data, phase):
        phase_name = str(phase)
        if phase_name == "early":
            return {
                "level": int(early_level),
                "wr_pct": float(early_level),
                "min_odds": round(100.0 / float(early_level), 2),
            }
        if phase_name in {"late", "mid"}:
            return {
                "level": int(late_level),
                "wr_pct": float(late_level),
                "min_odds": round(100.0 / float(late_level), 2),
            }
        if phase_name == "all" and all_level is not None:
            return {
                "level": int(all_level),
                "wr_pct": float(all_level),
                "min_odds": round(100.0 / float(all_level), 2),
            }
        return None

    monkeypatch.setattr(runtime, "_recommend_odds_for_block", _recommend)


def test_late_pre27_dominance_grid_keeps_zero_threshold_with_hold(monkeypatch) -> None:
    _patch_early_late_wr(monkeypatch, early_level=60, late_level=80)

    result = _run_branch_scenario(
        monkeypatch,
        BranchScenario(
            name="late_pre27_dominance_zero_threshold",
            game_time_seconds=18 * 60,
            target_side="radiant",
            target_networth_diff=0,
            has_early_star=True,
            early_sign=-1,
            has_late_star=True,
            late_sign=1,
            expected_send_calls=0,
            raw_early_output={"solo": -3},
            raw_mid_output={"solo": 3},
        ),
    )

    assert result.sent_messages == []
    assert result.queued_payload is not None
    assert result.queued_payload["dynamic_monitor_profile"] == runtime.LATE_PRE27_DOMINANCE_PROFILE
    assert float(result.queued_payload["networth_monitor_threshold"]) == 0.0
    assert result.queued_payload["networth_monitor_hold_allow_zero_threshold"] is True
    assert float(result.queued_payload["networth_monitor_hold_seconds"]) == float(runtime.NETWORTH_MONITOR_HOLD_SECONDS)
    assert float(result.queued_payload["networth_monitor_hold_started_game_time"]) == 18 * 60
    assert int(result.queued_payload["late_pre27_early_wr_level"]) == 60
    assert int(result.queued_payload["late_pre27_late_wr_level"]) == 80
    assert int(result.queued_payload["late_pre27_delta_level"]) == 20


def test_late_all_weak_early_uses_watcher_instead_of_dominance_grid(monkeypatch) -> None:
    _patch_early_late_wr(monkeypatch, early_level=60, late_level=65, all_level=75)
    monkeypatch.setattr(
        runtime,
        "late_pre27_watcher_thresholds_by_group_wr",
        {"late_all": {70: {10: -1000.0, 23: -4000.0}}},
        raising=False,
    )

    result = _run_branch_scenario(
        monkeypatch,
        BranchScenario(
            name="late_all_weak_early_pre27_watcher_not_dominance",
            game_time_seconds=(23 * 60) + 51,
            target_side="radiant",
            target_networth_diff=-5000,
            has_early_star=True,
            early_sign=-1,
            has_late_star=True,
            late_sign=1,
            has_all_star=True,
            all_sign=1,
            expected_send_calls=0,
            raw_early_output={"counterpick_1vs2": -4},
            raw_mid_output={"counterpick_1vs1": 6, "counterpick_1vs2": 8, "solo": 3},
            raw_post_lane_output={"counterpick_1vs1": 5, "counterpick_1vs2": 6},
        ),
    )

    assert result.sent_messages == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "late_all_same_weak_early_pre27_watcher"
    assert result.queued_payload["dynamic_monitor_profile"] == runtime.LATE_PRE27_WATCHER_PROFILE
    assert result.queued_payload["late_pre27_watcher_group"] == "late_all"
    assert int(result.queued_payload["late_pre27_watcher_wr_level"]) == 70
    assert float(result.queued_payload["networth_monitor_threshold"]) == -4000.0
    assert "late_pre27_delta_level" not in result.queued_payload


def test_late_pre27_dominance_dynamic_snapshot_uses_wr_delta_grid() -> None:
    payload = {
        "dynamic_monitor_profile": runtime.LATE_PRE27_DOMINANCE_PROFILE,
        "target_game_time": float(runtime.LATE_PUB_COMEBACK_TABLE_START_SECONDS),
        "networth_monitor_threshold_10_to_16": 4000.0,
        "networth_monitor_threshold_17_to_19": 2500.0,
        "networth_monitor_threshold_20_to_26": 2500.0,
        "networth_monitor_status_10_to_16": runtime.NETWORTH_STATUS_LATE_PRE27_DOMINANCE_WAIT,
        "networth_monitor_status_17_to_19": runtime.NETWORTH_STATUS_LATE_PRE27_DOMINANCE_WAIT,
        "networth_monitor_status_20_to_26": runtime.NETWORTH_STATUS_LATE_PRE27_DOMINANCE_WAIT,
    }

    pre10 = runtime._dynamic_monitor_snapshot_for_payload(payload, (9 * 60) + 59)
    assert pre10["threshold"] is None

    ten_to_16 = runtime._dynamic_monitor_snapshot_for_payload(payload, 12 * 60)
    assert float(ten_to_16["threshold"]) == 4000.0

    seventeen_to_19 = runtime._dynamic_monitor_snapshot_for_payload(payload, 18 * 60)
    assert float(seventeen_to_19["threshold"]) == 2500.0

    twenty_to_26 = runtime._dynamic_monitor_snapshot_for_payload(payload, 22 * 60)
    assert float(twenty_to_26["threshold"]) == 2500.0

    after_27 = runtime._dynamic_monitor_snapshot_for_payload(payload, 27 * 60)
    assert after_27["threshold"] is None


def test_zero_threshold_hold_can_require_confirmation() -> None:
    started = runtime._networth_monitor_hold_check(
        current_game_time=18 * 60,
        target_networth_diff=0.0,
        monitor_threshold=0.0,
        hold_started_game_time=None,
        hold_seconds=60.0,
        allow_zero_threshold=True,
    )
    assert started["enabled"] is True
    assert started["ready"] is False
    assert float(started["hold_started_game_time"]) == 18 * 60

    ready = runtime._networth_monitor_hold_check(
        current_game_time=(18 * 60) + 60,
        target_networth_diff=0.0,
        monitor_threshold=0.0,
        hold_started_game_time=started["hold_started_game_time"],
        hold_seconds=60.0,
        allow_zero_threshold=True,
    )
    assert ready["ready"] is True


def test_early_only_signal_rejects_when_wr_below_65(monkeypatch) -> None:
    _patch_early_wr(monkeypatch, 60.0)

    result = _run_branch_scenario(
        monkeypatch,
        BranchScenario(
            name="mouz_l1ga_early_only_wr60_reject",
            game_time_seconds=(10 * 60) + 33,
            target_side="radiant",
            target_networth_diff=-2305,
            has_early_star=True,
            early_sign=1,
            has_late_star=False,
            late_sign=-1,
            has_all_star=False,
            expected_send_calls=0,
            raw_early_output={"counterpick_1vs2": 5},
            raw_mid_output={"counterpick_1vs1": 0, "counterpick_1vs2": -3, "solo": -1},
            raw_post_lane_output={"dota2protracker_cp1vs1": 1.74},
        ),
        lane_output=("Top: draw 32%", "Mid: win 47%", "Bot: lose 43%"),
    )

    assert result.sent_messages == []
    assert result.queued_payload is None
    assert result.add_url_calls
    assert result.add_url_calls[-1]["reason"] == "star_signal_rejected_early_only_wr_below_65"
    details = result.add_url_calls[-1]["details"]
    assert details["early_only_no_late_all_gate"]["min_wr_ok"] is False
    assert details["early_only_no_late_all_gate"]["early_wr_pct"] == 60.0


def test_early_only_signal_sends_target_half_when_late_core_same_sign(monkeypatch) -> None:
    _patch_early_wr(monkeypatch, 65.0)

    result = _run_branch_scenario(
        monkeypatch,
        BranchScenario(
            name="early_only_late_core_same_sign_target_half",
            game_time_seconds=2 * 60,
            target_side="radiant",
            target_networth_diff=-1200,
            has_early_star=True,
            early_sign=1,
            has_late_star=False,
            late_sign=1,
            has_all_star=False,
            expected_send_calls=0,
            raw_early_output={"counterpick_1vs2": 5},
            raw_mid_output={"counterpick_1vs1": 1, "counterpick_1vs2": 3, "solo": 1},
        ),
        lane_output=ALIGNED_LANE_OUTPUT,
    )

    assert len(result.sent_messages) == 1
    assert result.sent_messages[0].startswith("СТАВКА НА Radiant Team x0.5\n")
    assert result.queued_payload is None
    assert result.add_url_calls[-1]["details"]["dispatch_mode"] == "immediate_early_only_target_half"
    gate = result.add_url_calls[-1]["details"]["early_only_no_late_all_gate"]
    assert gate["signal_mode"] == "target_half"
    assert gate["late_core_same_sign"] is True


def test_early_only_signal_sends_target_half_when_late_cp1v2_missing(monkeypatch) -> None:
    _patch_early_wr(monkeypatch, 65.0)

    result = _run_branch_scenario(
        monkeypatch,
        BranchScenario(
            name="early_only_late_core_cp1v2_missing_target_half",
            game_time_seconds=2 * 60,
            target_side="radiant",
            target_networth_diff=-1200,
            has_early_star=True,
            early_sign=1,
            has_late_star=False,
            late_sign=1,
            has_all_star=False,
            expected_send_calls=0,
            raw_early_output={"counterpick_1vs2": 5},
            raw_mid_output={"counterpick_1vs1": 1, "solo": 1},
        ),
        lane_output=ALIGNED_LANE_OUTPUT,
    )

    assert len(result.sent_messages) == 1
    assert result.sent_messages[0].startswith("СТАВКА НА Radiant Team x0.5\n")
    assert result.queued_payload is None
    gate = result.add_url_calls[-1]["details"]["early_only_no_late_all_gate"]
    assert gate["signal_mode"] == "target_half"
    assert gate["late_core_same_sign"] is True
    assert gate["late_core_missing_metrics"] == ["counterpick_1vs2"]


def test_early_only_signal_sends_kills_header_when_late_core_zero_or_opposite(monkeypatch) -> None:
    _patch_early_wr(monkeypatch, 65.0)

    result = _run_branch_scenario(
        monkeypatch,
        BranchScenario(
            name="early_only_late_core_opposite_kills_from",
            game_time_seconds=2 * 60,
            target_side="radiant",
            target_networth_diff=-1200,
            has_early_star=True,
            early_sign=1,
            has_late_star=False,
            late_sign=-1,
            has_all_star=False,
            expected_send_calls=0,
            raw_early_output={"counterpick_1vs2": 5},
            raw_mid_output={"counterpick_1vs1": 0, "counterpick_1vs2": -3, "solo": -1},
        ),
        lane_output=ALIGNED_LANE_OUTPUT,
    )

    assert len(result.sent_messages) == 1
    first_line = result.sent_messages[0].splitlines()[0]
    assert first_line == "СТАВКА НА килы от Radiant Team"
    assert " x" not in first_line
    assert result.queued_payload is None
    assert result.add_url_calls[-1]["details"]["dispatch_mode"] == "immediate_early_only_kills_from"
    gate = result.add_url_calls[-1]["details"]["early_only_no_late_all_gate"]
    assert gate["signal_mode"] == "kills_from"
    assert gate["late_core_same_sign"] is False


def test_early_only_kills_waits_when_lane_adv_dict_is_weak(monkeypatch) -> None:
    _patch_early_wr(monkeypatch, 65.0)

    result = _run_branch_scenario(
        monkeypatch,
        BranchScenario(
            name="early_only_kills_weak_lane_adv_wait",
            game_time_seconds=2 * 60,
            target_side="radiant",
            target_networth_diff=-1200,
            has_early_star=True,
            early_sign=1,
            has_late_star=False,
            late_sign=-1,
            has_all_star=False,
            expected_send_calls=0,
            raw_early_output={"counterpick_1vs2": 5},
            raw_mid_output={"counterpick_1vs1": 0, "counterpick_1vs2": -3, "solo": -1},
        ),
        lane_output=("Top: draw 32%", "Mid: win 47%", "Bot: lose 43%"),
    )

    assert result.sent_messages == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "same_sign_lane_adv_wait_4_10"
    assert result.queued_payload["dynamic_monitor_profile"] == "same_sign_lane_adv_wait_4_10"
    assert result.queued_payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_SAME_SIGN_LANE_ADV_PRE4_WAIT
    assert result.queued_payload["lane_adv_dict_sign"] is None


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


def test_no_late_immediate_star_waits_when_lane_adv_dict_is_weak(monkeypatch) -> None:
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
        lane_output=("Top: lose 47%", "Bot: win 39%", "Mid: win 39%"),
    )

    assert result.sent_messages == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "same_sign_lane_adv_wait_4_10"
    assert result.queued_payload["lane_adv_dict_sign"] is None
    assert result.queued_payload["lane_adv_protracker_sign"] is None


def test_no_late_immediate_star_waits_when_soft_pair_lane_adv_opposes_target(monkeypatch) -> None:
    result = _run_branch_scenario(
        monkeypatch,
        BranchScenario(
            name="dfamily_5sillymice_map2_soft_pair_lane_guard",
            game_time_seconds=0,
            target_side="dire",
            target_networth_diff=0,
            has_early_star=True,
            early_sign=-1,
            has_late_star=False,
            late_sign=1,
            has_all_star=True,
            all_sign=-1,
            expected_send_calls=0,
            raw_early_output={"solo": -3},
            raw_mid_output={"solo": 0},
            raw_post_lane_output={"dota2protracker_cp1vs1": -3},
            metrics_extra={"pro_lane_advantage": 0.61},
        ),
        lane_output=("Top: win 61%", "Bot: win 39%", "Mid: lose 48%"),
    )

    assert result.sent_messages == []
    assert result.queued_payload is not None
    assert result.queued_payload["reason"] == "same_sign_lane_adv_wait_4_10"
    assert result.queued_payload["dynamic_monitor_profile"] == "same_sign_lane_adv_wait_4_10"
    assert result.queued_payload["dispatch_status_label"] == runtime.NETWORTH_STATUS_SAME_SIGN_LANE_ADV_PRE4_WAIT
    assert result.queued_payload["lane_adv_dict_sign"] == 1
    assert result.queued_payload["lane_adv_protracker_sign"] is None
    assert result.queued_payload["lane_adv_pair_sign"] is None
    assert result.queued_payload["lane_adv_opposing_sources"] == ["lane_adv_dict"]
    assert float(result.queued_payload["target_game_time"]) == float(runtime.NETWORTH_GATE_SAME_SIGN_LANE_ADV_FALLBACK_SECONDS)


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


def test_no_late_immediate_star_ignores_protracker_lane_adv_opposition(monkeypatch) -> None:
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

    assert len(result.sent_messages) == 1
    assert result.queued_payload is None
    assert result.add_url_calls[-1]["reason"] == "star_signal_sent_now"


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


def _run_same_sign_delayed_worker(
    monkeypatch,
    *,
    game_time: float,
    radiant_lead: float,
    payload=None,
) -> list[dict]:
    deliveries: list[dict] = []
    match_key = "dltv.org/matches/test-match.0"
    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
        runtime.monitored_matches[match_key] = dict(payload or _same_sign_delayed_payload())

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


def test_late_pre27_delayed_worker_labels_zero_release_as_pre27_monitor(monkeypatch) -> None:
    payload = {
        "message": "test message",
        "reason": "late_star_pub_comeback_table_monitor",
        "json_url": "https://dltv.org/live/test.json",
        "target_game_time": float(runtime.LATE_PUB_COMEBACK_TABLE_START_SECONDS),
        "queued_at": 1_700_000_000.0,
        "queued_game_time": 17 * 60,
        "last_game_time": 17 * 60,
        "last_progress_at": 1_700_000_000.0,
        "dispatch_status_label": runtime.NETWORTH_STATUS_LATE_PUB_TABLE_WAIT,
        "add_url_reason": "star_signal_sent_delayed",
        "add_url_details": {
            "status": "live",
            "dispatch_mode": "delayed_late_pre27_dominance_grid",
            "delay_reason": "late_star_pub_comeback_table_monitor",
            "dispatch_status_label": runtime.NETWORTH_STATUS_LATE_PUB_TABLE_WAIT,
        },
        "fallback_send_status_label": runtime.NETWORTH_STATUS_LATE_PUB_TABLE_WAIT,
        "send_on_target_game_time": False,
        "dynamic_monitor_profile": runtime.LATE_PRE27_DOMINANCE_PROFILE,
        "networth_monitor_threshold_10_to_16": 800.0,
        "networth_monitor_threshold_17_to_19": 0.0,
        "networth_monitor_threshold_20_to_26": 0.0,
        "networth_monitor_status_10_to_16": runtime.NETWORTH_STATUS_LATE_PRE27_DOMINANCE_WAIT,
        "networth_monitor_status_17_to_19": runtime.NETWORTH_STATUS_LATE_PRE27_DOMINANCE_WAIT,
        "networth_monitor_status_20_to_26": runtime.NETWORTH_STATUS_LATE_PRE27_DOMINANCE_WAIT,
        "networth_monitor_hold_allow_zero_threshold": True,
        "networth_monitor_hold_seconds": 60.0,
        "networth_monitor_hold_started_game_time": (17 * 60),
        "networth_target_side": "radiant",
        "late_pub_comeback_table_active": True,
        "late_pub_comeback_table_wr_level": 80,
        "timeout_add_url_reason": "star_signal_rejected_late_pub_comeback_table_timeout",
        "timeout_status_label": runtime.NETWORTH_STATUS_LATE_COMEBACK_TIMEOUT_NO_SEND,
        "retry_attempt_count": 0,
        "next_retry_at": 0.0,
    }

    deliveries = _run_same_sign_delayed_worker(
        monkeypatch,
        game_time=18 * 60,
        radiant_lead=0.0,
        payload=payload,
    )

    assert len(deliveries) == 1
    details = deliveries[0]["details"]
    assert details["dispatch_status_label"] == runtime.NETWORTH_STATUS_LATE_PRE27_DOMINANCE_WAIT
    assert details["release_reason"] == "networth_monitor_0"
    assert details["networth_monitor_early_release"] is True
    assert "late_pub_comeback_table_reached" not in details
