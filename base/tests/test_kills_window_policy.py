from __future__ import annotations

import sys
from pathlib import Path

import pytest


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime


def _fake_gate(ed_by_label):
    def _gate(*, radiant_heroes_and_pos, dire_heroes_and_pos, target_sign, min_abs_ed=0.0):
        return {
            "valid": True,
            "status": "ok",
            "matching_windows": [],
            "ed_by_label": dict(ed_by_label),
            "min_abs_ed": float(min_abs_ed),
            "target_sign": int(target_sign),
        }

    return _gate


def _select(monkeypatch, *, gt, ed_by_label, lane_kills=None, lead=None, required=None):
    monkeypatch.setattr(
        runtime, "_kills_window_direction_gate_for_target", _fake_gate(ed_by_label)
    )
    return runtime._kills_window_policy_select(
        game_time_seconds=gt,
        radiant_heroes_and_pos={"pos1": {"hero_id": 1}},
        dire_heroes_and_pos={"pos1": {"hero_id": 2}},
        lane_kills_adv=lane_kills,
        radiant_lead=lead,
        required_target_sign=required,
    )


def test_515_combo_passes_at_draft(monkeypatch):
    result = _select(
        monkeypatch,
        gt=30.0,
        ed_by_label={"5_15": 0.45, "10_20": 0.1},
        lane_kills={"expected_diff": 0.35, "coverage": 12},
    )
    assert result["valid"] is True
    assert result["window_label"] == "5_15"
    assert result["target_side"] == "radiant"
    assert result["target_sign"] == 1


def test_515_blocked_when_lane_kills_opposite(monkeypatch):
    result = _select(
        monkeypatch,
        gt=30.0,
        ed_by_label={"5_15": 0.45},
        lane_kills={"expected_diff": -0.35, "coverage": 12},
    )
    assert result["valid"] is False
    assert result["status"] == "lane_kills_gate_failed"


def test_515_blocked_when_lane_kills_weak(monkeypatch):
    result = _select(
        monkeypatch,
        gt=30.0,
        ed_by_label={"5_15": 0.45},
        lane_kills={"expected_diff": 0.15, "coverage": 12},
    )
    assert result["valid"] is False
    assert result["status"] == "lane_kills_gate_failed"


def test_515_blocked_when_ed_below_0_3(monkeypatch):
    result = _select(
        monkeypatch,
        gt=30.0,
        ed_by_label={"5_15": 0.25},
        lane_kills={"expected_diff": 0.35, "coverage": 12},
    )
    assert result["valid"] is False
    assert result["status"] == "ed_below_min"


def test_1020_band_requires_nw_nonnegative(monkeypatch):
    passed = _select(
        monkeypatch, gt=240.0, ed_by_label={"10_20": 0.2}, lead=100.0
    )
    assert passed["valid"] is True
    assert passed["window_label"] == "10_20"
    assert passed["target_side"] == "radiant"
    blocked = _select(
        monkeypatch, gt=240.0, ed_by_label={"10_20": 0.2}, lead=-100.0
    )
    assert blocked["valid"] is False
    assert blocked["status"] == "nw_lead_below_min"


def test_1020_dire_target_uses_inverted_lead(monkeypatch):
    result = _select(
        monkeypatch, gt=240.0, ed_by_label={"10_20": -0.2}, lead=-400.0
    )
    assert result["valid"] is True
    assert result["target_side"] == "dire"
    assert result["target_networth_diff"] == pytest.approx(400.0)


def test_1525_band_requires_nw_plus_500(monkeypatch):
    blocked = _select(
        monkeypatch, gt=8 * 60.0, ed_by_label={"15_25": 0.4}, lead=400.0
    )
    assert blocked["valid"] is False
    assert blocked["status"] == "nw_lead_below_min"
    passed = _select(
        monkeypatch, gt=8 * 60.0, ed_by_label={"15_25": 0.4}, lead=600.0
    )
    assert passed["valid"] is True
    assert passed["window_label"] == "15_25"


def test_2030_band_passes_with_zero_lead(monkeypatch):
    result = _select(
        monkeypatch, gt=14 * 60.0, ed_by_label={"20_30": 0.2}, lead=0.0
    )
    assert result["valid"] is True
    assert result["window_label"] == "20_30"


@pytest.mark.parametrize(
    "gt",
    [150.0, 5.5 * 60, 6 * 60, 12 * 60, 16 * 60, 20 * 60],
)
def test_outside_policy_bands_never_fire(monkeypatch, gt):
    result = _select(
        monkeypatch,
        gt=gt,
        ed_by_label={
            "5_15": 3.0,
            "10_20": 3.0,
            "15_25": 3.0,
            "20_30": 3.0,
        },
        lane_kills={"expected_diff": 3.0, "coverage": 99},
        lead=10000.0,
    )
    assert result["valid"] is False
    assert result["status"] == "outside_policy_band"


def test_required_target_sign_filters_wrong_side(monkeypatch):
    result = _select(
        monkeypatch,
        gt=240.0,
        ed_by_label={"10_20": 0.3},
        lead=500.0,
        required=-1,
    )
    assert result["valid"] is False
    assert result["status"] == "sign_mismatch"


def test_band_boundaries_half_open(monkeypatch):
    # 10-20 полоса: [180, 300) — старт включён, конец исключён.
    on_start = _select(monkeypatch, gt=180.0, ed_by_label={"10_20": 0.3}, lead=0.0)
    assert on_start["valid"] is True
    on_end = _select(monkeypatch, gt=300.0, ed_by_label={"10_20": 0.3}, lead=0.0)
    assert on_end["valid"] is False
    assert on_end["status"] == "outside_policy_band"


def test_kills_header_contains_window_label():
    header = runtime._format_signal_header(
        stake_team_name="Vici Gaming",
        stake_multiplier=1.0,
        special_header_mode="early_kills",
        kills_window_label="10_20",
    )
    assert header == "СТАВКА НА Ранние килы 10-20 Vici Gaming"
