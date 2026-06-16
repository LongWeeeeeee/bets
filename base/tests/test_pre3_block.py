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


def test_early_kills_pre3_block(monkeypatch, capsys) -> None:
    # Under 3:00 (e.g., 2:30), in tier1_early_kills_mode (>= 2 hits) with no lane_adv hits,
    # we expect it to be blocked by pre3_block and return return_status immediately.
    case = BranchScenario(
        name="early_kills_pre3_block_wait",
        game_time_seconds=(2 * 60) + 30,
        target_side="radiant",
        target_networth_diff=5000,
        has_early_star=True,
        early_sign=1,
        has_late_star=False,
        late_sign=1,
        expected_send_calls=0,
        raw_early_output={"counterpick_1vs1": 6, "solo": 3},  # 2 hits => tier1_early_kills_mode is True
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

    result = _run_branch_scenario(monkeypatch, case)
    output = capsys.readouterr().out

    assert result.sent_messages == []
    # Verify that the pre3_block warning/wait statement was printed in stdout
    assert "pre3_block" in output


def test_early_kills_allowed_after_three_minutes(monkeypatch) -> None:
    # At 3:30 (above 3:00 boundary), in tier1_early_kills_mode with target limit >= 600,
    # it bypasses/sends early kills signal.
    case = BranchScenario(
        name="early_kills_3_6_lead_bypass_send",
        game_time_seconds=(3 * 60) + 30,
        target_side="radiant",
        target_networth_diff=800,  # exceeds NETWORTH_GATE_TIER1_EARLY_KILLS_EARLY_LEAD_MIN_DIFF = 600.0
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

    result = _run_branch_scenario(monkeypatch, case)

    assert len(result.sent_messages) == 1
    assert result.add_url_calls
    # Should be released under tier1_early_kills_3_6_lead_send label
    assert result.add_url_calls[-1]["details"]["release_reason"] == runtime.NETWORTH_STATUS_TIER1_EARLY_KILLS_3_6_LEAD_SEND


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
