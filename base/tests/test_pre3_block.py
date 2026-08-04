import importlib
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

runtime = importlib.import_module("cyberscore_try")
import test_networth_dispatch_gates
from test_networth_dispatch_gates import BranchScenario, _run_branch_scenario


def test_pre3_block_status_label() -> None:
    assert runtime.NETWORTH_STATUS_PRE3_BLOCK == "pre3_block"


def _patch_early_wr(monkeypatch, wr_pct: float) -> None:
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda data, phase: {"level": int(wr_pct), "min_odds": 1.67, "wr_pct": wr_pct},
    )


def test_early_kills_no_bet_outside_policy_band(monkeypatch, capsys) -> None:
    # Kills-window policy: at 4:30 we sit in the gap between the 5-15 band
    # (ends 03:00) and the 10-20 band (starts 06:00) — tier1_early_kills_mode
    # is gated off, so no "Ранние килы" bet is dispatched.
    case = BranchScenario(
        name="early_kills_outside_policy_band",
        game_time_seconds=(4 * 60) + 30,
        target_side="radiant",
        target_networth_diff=5000,
        has_early_star=True,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        expected_send_calls=0,
        raw_early_output={"counterpick_1vs1": 6, "solo": 3},  # 2 hits
        raw_mid_output={"solo": 0},
    )
    _patch_early_wr(monkeypatch, 70.0)

    # Monkeypatch the mock utility function in test_networth_dispatch_gates to return correct hit_metrics
    def _my_star_diagnostics_for_case(case_obj, section):
        if section == "early_output":
            hits = list(case_obj.raw_early_output.keys()) if case_obj.raw_early_output else ["solo"]
            return {
                "valid": True,
                "status": "ok",
                "sign": 1,
                "hit_metrics": hits,
                "conflict_metric": None,
            }
        return {
            "valid": False,
            "status": "no_hits",
            "sign": None,
            "hit_metrics": [],
            "conflict_metric": None,
        }

    monkeypatch.setattr(test_networth_dispatch_gates, "_star_diagnostics_for_case", _my_star_diagnostics_for_case)
    # Direction gate валиден, чтобы main-путь дошёл до policy-селектора: в
    # 2:30 ни одна связка не активна → outside_policy_band.
    monkeypatch.setattr(
        runtime,
        "_kills_window_direction_gate_for_target",
        lambda **_kwargs: {
            "valid": True,
            "status": "ok",
            "matching_windows": ["5_15"],
            "ed_by_label": {"5_15": 0.5},
            "min_abs_ed": 0.0,
            "target_sign": 1,
        },
    )

    result = _run_branch_scenario(monkeypatch, case)
    output = capsys.readouterr().out

    kills_msgs = [m for m in result.sent_messages if m.startswith("СТАВКА НА Ранние килы")]
    assert kills_msgs == [], "вне полосы kills-window policy килов быть не должно"
    assert "kills-window policy не сошлась" in output


def test_early_kills_policy_release_in_band(monkeypatch) -> None:
    # At 3:30 the 10-20 band is active; with a valid policy combo the kills
    # bet releases immediately (no legacy prep6 ladder).
    case = BranchScenario(
        name="early_kills_policy_release_10_20",
        game_time_seconds=(3 * 60) + 30,
        target_side="radiant",
        target_networth_diff=800,
        has_early_star=True,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        expected_send_calls=1,
        raw_early_output={"counterpick_1vs1": 6, "solo": 3},
        raw_mid_output={"solo": 0},
    )
    _patch_early_wr(monkeypatch, 70.0)

    def _my_star_diagnostics_for_case(case_obj, section):
        if section == "early_output":
            hits = list(case_obj.raw_early_output.keys()) if case_obj.raw_early_output else ["solo"]
            return {
                "valid": True,
                "status": "ok",
                "sign": 1,
                "hit_metrics": hits,
                "conflict_metric": None,
            }
        return {
            "valid": False,
            "status": "no_hits",
            "sign": None,
            "hit_metrics": [],
            "conflict_metric": None,
        }

    monkeypatch.setattr(test_networth_dispatch_gates, "_star_diagnostics_for_case", _my_star_diagnostics_for_case)
    # Main-path direction gate: kills_window same-sign block exists.
    monkeypatch.setattr(
        runtime,
        "_kills_window_direction_gate_for_target",
        lambda **_kwargs: {
            "valid": True,
            "status": "ok",
            "matching_windows": ["10_20"],
            "ed_by_label": {"10_20": 0.3},
            "min_abs_ed": 0.0,
            "target_sign": 1,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_kills_window_policy_select",
        lambda **_kwargs: {
            "valid": True,
            "status": "ok",
            "window_label": "10_20",
            "target_sign": 1,
            "target_side": "radiant",
            "expected_diff": 0.3,
            "min_ed": 0.2,
            "lane_kills_adv_ed": None,
            "target_networth_diff": 800.0,
            "band_start": 180.0,
            "band_end": 300.0,
        },
    )

    result = _run_branch_scenario(monkeypatch, case)

    assert len(result.sent_messages) == 1
    assert result.sent_messages[0].startswith("СТАВКА НА Ранние килы 10-20 ")
    assert result.add_url_calls
    assert result.add_url_calls[-1]["details"]["release_reason"] == runtime.NETWORTH_STATUS_KILLS_WINDOW_POLICY_SEND


def test_early_kills_suppressed_when_no_tier1_team(monkeypatch) -> None:
    # Same shape as test_early_kills_allowed_after_three_minutes (which fires a
    # kills release), but neither team is Tier-1 -> tier1_early_kills_mode is
    # gated off and NO "Ранние килы" kills bet is dispatched.
    case = BranchScenario(
        name="early_kills_suppressed_no_tier1",
        game_time_seconds=(3 * 60) + 30,
        target_side="radiant",
        target_networth_diff=800,
        has_early_star=True,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        expected_send_calls=0,
        raw_early_output={"counterpick_1vs1": 6, "solo": 3},
        raw_mid_output={"solo": 0},
    )
    _patch_early_wr(monkeypatch, 70.0)

    def _my_star_diagnostics_for_case(case_obj, section):
        if section == "early_output":
            hits = list(case_obj.raw_early_output.keys()) if case_obj.raw_early_output else ["solo"]
            return {
                "valid": True,
                "status": "ok",
                "sign": 1,
                "hit_metrics": hits,
                "conflict_metric": None,
            }
        return {
            "valid": False,
            "status": "no_hits",
            "sign": None,
            "hit_metrics": [],
            "conflict_metric": None,
        }

    monkeypatch.setattr(test_networth_dispatch_gates, "_star_diagnostics_for_case", _my_star_diagnostics_for_case)

    result = _run_branch_scenario(monkeypatch, case, match_has_tier1_team=False)

    kills_msgs = [m for m in result.sent_messages if m.startswith("СТАВКА НА Ранние килы")]
    assert kills_msgs == [], "early-kills release must be suppressed when no team is Tier-1"


def test_early_kills_gate_uses_early_side_networth(monkeypatch, capsys) -> None:
    # Regression for Zero Tenacity vs PuckChamp: Late/All selected radiant,
    # Early selected dire, and radiant led by 243. The kills policy must read
    # the Early (dire) networth as -243 — the 15-25 combo (полоса 11:00–13:00)
    # needs dire NW >= +1000, so the kills bet is suppressed instead of
    # releasing on radiant +243.
    case = BranchScenario(
        name="early_kills_uses_early_side_networth",
        game_time_seconds=(12 * 60),
        target_side="radiant",
        target_networth_diff=243,
        has_early_star=True,
        early_sign=-1,
        has_late_star=True,
        late_sign=1,
        has_all_star=True,
        all_sign=1,
        expected_send_calls=0,
        raw_early_output={
            "counterpick_1vs1": -9,
            "counterpick_1vs2": -9,
            "solo": -5,
        },
        raw_mid_output={
            "counterpick_1vs1": 15,
            "counterpick_1vs2": 13,
            "solo": 8,
        },
        raw_post_lane_output={
            "counterpick_1vs1": 5,
            "counterpick_1vs2": 8,
            "synergy_duo": 7,
        },
    )
    _patch_early_wr(monkeypatch, 70.0)
    default_diagnostics = test_networth_dispatch_gates._star_diagnostics_for_case

    def _diagnostics_with_real_early_hits(case_obj, section):
        diagnostics = default_diagnostics(case_obj, section)
        if section == "early_output" and diagnostics.get("valid"):
            diagnostics = dict(diagnostics)
            diagnostics["hit_metrics"] = list(case_obj.raw_early_output or {})
            diagnostics["hit_count"] = len(diagnostics["hit_metrics"])
        return diagnostics

    monkeypatch.setattr(
        test_networth_dispatch_gates,
        "_star_diagnostics_for_case",
        _diagnostics_with_real_early_hits,
    )
    # kills_window dict: 15-25 ed points dire (early side), |ed| >= 0.2.
    monkeypatch.setattr(
        runtime,
        "_kills_window_direction_gate_for_target",
        lambda **_kwargs: {
            "valid": True,
            "status": "ok",
            "matching_windows": ["15_25"],
            "ed_by_label": {"15_25": -0.5},
            "min_abs_ed": 0.0,
            "target_sign": int(_kwargs.get("target_sign") or 1),
        },
    )

    result = _run_branch_scenario(monkeypatch, case)
    output = capsys.readouterr().out

    kills_msgs = [m for m in result.sent_messages if m.startswith("СТАВКА НА Ранние килы")]
    assert kills_msgs == [], "NW-гейт обязан читаться со стороны early star"
    assert "nw_lead_below_min" in output


def test_zero_tenacity_is_tier2_only() -> None:
    zero_tenacity_id = 9600141

    assert runtime._get_team_tier(zero_tenacity_id) == 2
