from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path


TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from test_networth_dispatch_gates import BranchScenario, _run_branch_scenario, runtime  # noqa: E402
from test_same_sign_lane_adv_dispatch import _patch_early_late_wr  # noqa: E402


def test_all_only_watcher_monitor_config_picks_nearest_wr_level(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "all_only_watcher_thresholds_by_wr",
        {
            60: {10: -800.0, 11: -1200.0},
            70: {10: -1500.0, 11: -1800.0},
        },
        raising=False,
    )

    config = runtime._all_only_watcher_monitor_config(
        target_sign=1,
        all_wr_pct=72.0,
    )

    assert config is not None
    assert config["enabled"] is True
    assert config["profile"] == runtime.ALL_ONLY_WATCHER_PROFILE
    assert config["target_side"] == "radiant"
    assert config["wr_level"] == 70
    assert config["thresholds_by_minute"] == {10: -1500.0, 11: -1800.0}
    assert float(config["target_game_time"]) == float(
        runtime.ALL_ONLY_WATCHER_TARGET_GAME_TIME_SECONDS
    )


def test_all_only_watcher_monitor_config_returns_none_when_thresholds_missing(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "all_only_watcher_thresholds_by_wr",
        {},
        raising=False,
    )

    config = runtime._all_only_watcher_monitor_config(
        target_sign=-1,
        all_wr_pct=80.0,
    )

    assert config is None


def test_all_only_watcher_snapshot_uses_latest_elapsed_minute() -> None:
    payload = {
        "dynamic_monitor_profile": runtime.ALL_ONLY_WATCHER_PROFILE,
        "target_game_time": runtime.ALL_ONLY_WATCHER_TARGET_GAME_TIME_SECONDS,
        "networth_monitor_thresholds_by_minute": {
            10: -800.0,
            11: -1200.0,
            12: -1500.0,
        },
        "networth_monitor_status": runtime.NETWORTH_STATUS_ALL_ONLY_WATCHER_WAIT,
    }

    before_10 = runtime._dynamic_monitor_snapshot_for_payload(payload, (9 * 60) + 59)
    at_11 = runtime._dynamic_monitor_snapshot_for_payload(payload, (11 * 60) + 30)

    assert before_10["threshold"] is None
    assert at_11["threshold"] == -1200.0
    assert at_11["source_minute"] == 11
    assert at_11["status_label"] == runtime.NETWORTH_STATUS_ALL_ONLY_WATCHER_WAIT


def test_all_only_watcher_snapshot_drops_after_target() -> None:
    payload = {
        "dynamic_monitor_profile": runtime.ALL_ONLY_WATCHER_PROFILE,
        "target_game_time": runtime.ALL_ONLY_WATCHER_TARGET_GAME_TIME_SECONDS,
        "networth_monitor_thresholds_by_minute": {10: -800.0, 11: -1200.0},
        "timeout_status_label": runtime.NETWORTH_STATUS_ALL_ONLY_WATCHER_TIMEOUT_NO_SEND,
    }

    at_target = runtime._all_only_watcher_snapshot(
        payload,
        float(runtime.ALL_ONLY_WATCHER_TARGET_GAME_TIME_SECONDS),
    )
    after_target = runtime._all_only_watcher_snapshot(
        payload,
        float(runtime.ALL_ONLY_WATCHER_TARGET_GAME_TIME_SECONDS) + 30,
    )

    for snap in (at_target, after_target):
        assert snap["threshold"] is None
        assert snap["status_label"] == runtime.NETWORTH_STATUS_ALL_ONLY_WATCHER_TIMEOUT_NO_SEND
        assert snap["drop_without_fallback"] is True


def test_all_only_no_early_no_late_queues_watcher(monkeypatch) -> None:
    _patch_early_late_wr(monkeypatch, early_level=60, late_level=65, all_level=70)
    monkeypatch.setattr(
        runtime,
        "all_only_watcher_thresholds_by_wr",
        {70: {10: -1000.0, 11: -1200.0, 12: -1500.0}},
        raising=False,
    )

    case = BranchScenario(
        name="all_only_no_early_no_late_queues_watcher",
        game_time_seconds=(10 * 60) + 30,
        target_side="radiant",
        target_networth_diff=-2000,
        has_early_star=False,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        has_all_star=True,
        all_sign=1,
        expected_send_calls=0,
        expected_queue=True,
        raw_early_output={"solo": 0},
        raw_mid_output={"solo": 0},
        raw_post_lane_output={"synergy_duo": 5},
    )

    result = _run_branch_scenario(monkeypatch, case)

    assert result.sent_messages == []
    assert result.queued_payload is not None
    payload = result.queued_payload
    assert payload["reason"] == "all_only_watcher_no_early_no_late"
    assert payload["dynamic_monitor_profile"] == runtime.ALL_ONLY_WATCHER_PROFILE
    assert int(payload["all_only_watcher_wr_level"]) == 70
    assert float(payload["target_game_time"]) == float(
        runtime.ALL_ONLY_WATCHER_TARGET_GAME_TIME_SECONDS
    )
    assert payload["send_on_target_game_time"] is False
    assert payload["all_only_watcher_drop_without_fallback"] is True
    assert (
        payload["timeout_status_label"]
        == runtime.NETWORTH_STATUS_ALL_ONLY_WATCHER_TIMEOUT_NO_SEND
    )
    assert (
        payload["timeout_add_url_reason"]
        == "star_signal_rejected_all_only_watcher_timeout"
    )
    assert float(payload["networth_monitor_threshold"]) == -1000.0
    assert (
        payload["dispatch_status_label"]
        == runtime.NETWORTH_STATUS_ALL_ONLY_WATCHER_WAIT
    )


def test_all_only_watcher_timeout_drops_url(monkeypatch) -> None:
    _patch_early_late_wr(monkeypatch, early_level=60, late_level=65, all_level=70)
    monkeypatch.setattr(
        runtime,
        "all_only_watcher_thresholds_by_wr",
        {70: {10: -1000.0, 11: -1200.0}},
        raising=False,
    )

    case = BranchScenario(
        name="all_only_watcher_timeout",
        game_time_seconds=int(runtime.ALL_ONLY_WATCHER_TARGET_GAME_TIME_SECONDS) + 5,
        target_side="radiant",
        target_networth_diff=-3000,
        has_early_star=False,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        has_all_star=True,
        all_sign=1,
        expected_send_calls=0,
        raw_early_output={"solo": 0},
        raw_mid_output={"solo": 0},
        raw_post_lane_output={"synergy_duo": 5},
    )

    result = _run_branch_scenario(monkeypatch, case)

    assert result.sent_messages == []
    assert result.queued_payload is None
    assert result.add_url_calls
    last_call = result.add_url_calls[-1]
    assert last_call["reason"] == "star_signal_rejected_all_only_watcher_timeout"
    details = last_call["details"]
    assert (
        details["dispatch_status_label"]
        == runtime.NETWORTH_STATUS_ALL_ONLY_WATCHER_TIMEOUT_NO_SEND
    )
    assert details["all_only_watcher_wr_level"] == 70
