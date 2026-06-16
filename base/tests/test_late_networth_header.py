"""Regression: late networth/STAR dispatch must not inherit the early-kills header.

These tests live in their own module (not inside test_networth_dispatch_gates.py)
because that module is currently guarded by a REQUIRED_STATUS_CONSTS skip and is
fully skipped in this environment. They reuse the same dispatch harness via import,
exactly like test_pre3_block.py.
"""

import importlib
import sys
from pathlib import Path
from typing import Any, Dict

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

runtime = importlib.import_module("cyberscore_try")
import test_networth_dispatch_gates  # noqa: E402
from test_networth_dispatch_gates import (  # noqa: E402
    BranchScenario,
    _patch_team_elo_summary,
    _run_branch_scenario,
)

# Radiant-dominated lanes -> lane_adv_dict aligned with the radiant target, so
# the same-sign lane_adv guard does NOT force a wait and the immediate STAR
# branch (dispatch_mode "immediate_star_rule") fires at any game_time.
ALIGNED_LANE_OUTPUT = ("Top: win 70%", "Bot: win 70%", "Mid: win 70%")


def _patch_all_phase_wr(monkeypatch, wr_pct: float) -> None:
    monkeypatch.setattr(
        runtime,
        "_recommend_odds_for_block",
        lambda _data, _phase: {"level": int(wr_pct), "min_odds": 1.67, "wr_pct": wr_pct},
    )


def _two_hit_diagnostics(case_obj: BranchScenario, section: str) -> Dict[str, Any]:
    """Force >=2 hit_metrics on BOTH early and late blocks so
    tier1_early_kills_mode (needs >=2 early hits) fires alongside a valid late
    star (the default harness diagnostics return only a single 'solo' hit)."""
    if section == "early_output" and case_obj.has_early_star:
        return {
            "valid": True,
            "status": "ok",
            "sign": case_obj.early_sign,
            "hit_metrics": list((case_obj.raw_early_output or {}).keys()),
            "conflict_metric": None,
        }
    if section == "mid_output" and case_obj.has_late_star:
        return {
            "valid": True,
            "status": "ok",
            "sign": case_obj.late_sign,
            "hit_metrics": list((case_obj.raw_mid_output or {}).keys()),
            "conflict_metric": None,
        }
    return {
        "valid": False,
        "status": "no_hits",
        "sign": None,
        "hit_metrics": [],
        "conflict_metric": None,
    }


def test_late_networth_dispatch_strips_early_kills_header(monkeypatch) -> None:
    # A LATE (24:58) immediate STAR / networth dispatch that ALSO has
    # tier1_early_kills_mode=True (early star, early WR>=65, >=2 early hits) must
    # render the regular "СТАВКА НА <team> x<mult>" header — NOT the
    # "Ранние килы <team>" placeholder baked into the base message_text. The bug:
    # the immediate-send paths fed the raw stake_multiplier_context (still
    # carrying special_header_mode="early_kills") into
    # _refresh_stake_multiplier_message, so the late networth bet inherited the
    # early-kills header. Mirrors the reported case: Time 24:58, Networth +3483.
    case = BranchScenario(
        name="late_networth_strips_early_kills_header",
        game_time_seconds=(24 * 60) + 58,
        target_side="radiant",
        target_networth_diff=3483,
        has_early_star=True,
        early_sign=1,
        has_late_star=True,
        late_sign=1,
        expected_send_calls=1,
        expected_add_url_reason="star_signal_sent_now",
        raw_early_output={"counterpick_1vs1": 6, "solo": 3},  # 2 early hits
        raw_mid_output={"counterpick_1vs1": 6, "solo": 3},  # 2 late hits
    )
    _patch_all_phase_wr(monkeypatch, 70.0)  # early & late WR 70 (>=65 -> kills mode)
    _patch_team_elo_summary(monkeypatch, radiant_wr=56.0, dire_wr=44.0)
    monkeypatch.setattr(
        test_networth_dispatch_gates, "_star_diagnostics_for_case", _two_hit_diagnostics
    )

    result = _run_branch_scenario(monkeypatch, case, lane_output=ALIGNED_LANE_OUTPUT)

    assert len(result.sent_messages) == 1
    # Confirm we exercised the immediate STAR branch (one of the patched sites).
    assert result.add_url_calls[-1]["details"].get("dispatch_mode") == "immediate_star_rule"
    header = result.sent_messages[0].splitlines()[0]
    assert "Ранние килы" not in header, header
    assert header.startswith("СТАВКА НА Radiant Team x"), header
    # Sanity: this really is the late networth signal from the report.
    assert "Time: 24:58" in result.sent_messages[0]
    assert "Networth: Radiant Team +3483" in result.sent_messages[0]
