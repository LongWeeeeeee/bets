"""E-350: the ML WIN price floor uses a pro-calibrated map-win probability.

Input is the captured serv1 journal row for OG (Radiant) vs BETBOOM TEAM,
dltv.org/matches/9026301227 map 2 (02.10.2026), the tick that delivered
"СТАВКА НА BETBOOM TEAM x1 / Ставить от кэфа 1.74" (see the ``_capture`` block
of base/tests/fixtures/ml_dispatch_og_betboom_20261002.json for the exact
capture command).  Assertions are made at the delivery boundary: the Decision
floor AND the line the bet message builder prints.

Platt parameters are the all-league map-win calibrators of
runtime/artifacts/draft-cp/pro_calibration_20261003/calibrators.json
(docs/experiments/E-350-pro-calibration-phase-models.md).
"""
from __future__ import annotations

import json
import math
from pathlib import Path
import sys

import pytest

from base import ml_dispatch as md

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Same ImportError-only keys stub as test_ml_dispatch_lane_kills.py.
try:
    import keys  # noqa: F401,E402
except ImportError:
    import types

    _test_keys = types.ModuleType("keys")
    _test_keys.api_to_proxy = {}
    _test_keys.BOOKMAKER_PROXY_URL = None
    _test_keys.BOOKMAKER_PROXY_POOL = []
    _test_keys.DLTV_PROXY_POOL = []
    sys.modules["keys"] = _test_keys

import cyberscore_try as C  # noqa: E402

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "ml_dispatch_og_betboom_20261002.json").read_text()
)
RECORD = FIXTURE["record"]

PLATT = {
    "late": (0.0087, 0.9017),
    "all": (-0.0071, 0.9383),
    "early_win": (-0.0347, 0.8355),
    "early_nw": (0.0602, 0.5848),
}


def _cal_side_conf(model, side, conf):
    """Independent re-derivation: side probability after the Platt map."""
    a, b = PLATT[model]
    p_r = conf if side == "Radiant" else 1.0 - conf
    p_r = min(max(p_r, 1e-6), 1 - 1e-6)
    p_cal = 1.0 / (1.0 + math.exp(-(a + b * math.log(p_r / (1.0 - p_r)))))
    return p_cal if side == "Radiant" else 1.0 - p_cal


def _verdict(name, record=RECORD, **override):
    value = record["verdicts"].get(name)
    if value is None:
        return None
    value = dict(value, **override)
    return md.ModelVerdict(**value)


def _fixture_ctx(**overrides):
    record = RECORD
    kwargs = dict(
        match_key=record["match_key"], base_url=record["base_url"],
        map_num=record["map_num"], game_time=record["game_time"],
        radiant_team=record["teams"]["radiant"], dire_team=record["teams"]["dire"],
        heroes=record["heroes"], elo_radiant=record["elo_r"], elo_dire=record["elo_d"],
        early_nw=_verdict("early_nw"), early_win=_verdict("early_win"),
        late=_verdict("late"), all=_verdict("all"), lane=_verdict("lane"),
        prematch=None, already_sent=set(),
        radiant_networth_lead=record["radiant_networth_lead"],
        lane_adv_dict=record["lane_adv_dict"],
    )
    kwargs.update(overrides)
    return md.Ctx(**kwargs)


def _win(result):
    wins = [d for d in result.decisions if d.market == "win"]
    assert len(wins) == 1
    return wins[0]


def _cfg(**env):
    return md.Config.from_env(env)


def _message_floor_line(decision):
    text = C._build_prematch_model_bet_message(
        radiant_team_name="OG", dire_team_name="BETBOOM TEAM",
        target_team_name=decision.target_team, live_league=None,
        top=None, mid=None, bot=None, protracker_payload=None,
        team_elo_block="", game_time_seconds=-15.0, radiant_lead=471.0,
        model_line="", min_odds=decision.min_odds,
    )
    lines = [line for line in text.splitlines() if line.startswith("Ставить от кэфа")]
    assert len(lines) == 1, text
    return lines[0], text


def test_fixture_is_the_delivered_1_74_tick():
    delivered = RECORD["decisions"][0]
    assert (delivered["market"], delivered["target_team"], delivered["min_odds"]) == (
        "win", "BETBOOM TEAM", 1.74)
    assert delivered["models_for"] == ["late", "early_nw"]


def test_raw_path_reproduces_the_delivered_1_74_floor_and_message():
    result = md.evaluate(_fixture_ctx(), _cfg(ML_DISPATCH_FLOOR_CALIBRATION="0"))
    win = _win(result)
    assert win.target_side == "Dire"
    assert win.models_for == ["late", "early_nw"]
    assert win.expected_wr == pytest.approx(0.6933)
    assert win.min_odds == 1.74
    line, _ = _message_floor_line(win)
    assert line == "Ставить от кэфа 1.74"


def test_calibrated_path_floor_uses_best_calibrated_side_probability():
    result = md.evaluate(_fixture_ctx(), _cfg())
    win = _win(result)
    # Same side, same models: the decision itself is unchanged.
    assert win.target_side == "Dire"
    assert win.rule == "win_single_model_confirm"
    assert win.models_for == ["late", "early_nw"]
    # Dire side: late 0.64 -> 0.62483 beats early_nw 0.6933 -> 0.60271
    # (E-350: raw 0.6933 is a marker-direction probability, ~0.60 map win).
    late_cal = _cal_side_conf("late", "Dire", 0.64)
    nw_cal = _cal_side_conf("early_nw", "Dire", 0.6933)
    assert late_cal == pytest.approx(0.62483, abs=1e-5)
    assert nw_cal == pytest.approx(0.60271, abs=1e-5)
    assert win.expected_wr == pytest.approx(max(late_cal, nw_cal))
    assert win.min_odds == round(1 / (max(late_cal, nw_cal) - 0.12), 2)
    assert win.min_odds == 1.98
    line, text = _message_floor_line(win)
    assert line == "Ставить от кэфа 1.98"
    assert "1.74" not in text


def test_calibrated_floor_from_early_nw_alone_is_2_07():
    # The E-350 headline number: with only the Early NW voice for BetBoom
    # (late below threshold) the honest floor is 1/(0.6027-0.12) = 2.07.
    # A second support keeps the early-solo block from firing.
    ctx = _fixture_ctx(late=md.ModelVerdict("Dire", 0.5), all=md.ModelVerdict("Dire", 0.60))
    win = _win(md.evaluate(ctx, _cfg()))
    assert sorted(win.models_for) == ["all", "early_nw"]
    all_cal = _cal_side_conf("all", "Dire", 0.60)
    nw_cal = _cal_side_conf("early_nw", "Dire", 0.6933)
    assert max(all_cal, nw_cal) == pytest.approx(nw_cal)
    assert win.min_odds == round(1 / (nw_cal - 0.12), 2) == 2.07


def test_radiant_side_conversion_is_not_mirrored():
    # Same raw confidences but voting for Radiant: probabilities are mapped in
    # Radiant space, so the Radiant result differs from the Dire one above.
    ctx = _fixture_ctx(
        early_nw=md.ModelVerdict("Radiant", 0.6933), late=md.ModelVerdict("Radiant", 0.64),
        all=None, early_win=None, lane=None,
    )
    win = _win(md.evaluate(ctx, _cfg()))
    assert win.target_side == "Radiant"
    nw_cal = _cal_side_conf("early_nw", "Radiant", 0.6933)
    late_cal = _cal_side_conf("late", "Radiant", 0.64)
    assert nw_cal == pytest.approx(0.63115, abs=1e-5)
    assert late_cal == pytest.approx(0.62890, abs=1e-5)
    assert win.expected_wr == pytest.approx(max(nw_cal, late_cal))
    assert win.min_odds == round(1 / (nw_cal - 0.12), 2) == 1.96
    # Raw rollback gives the old number for the same input.
    raw = _win(md.evaluate(ctx, _cfg(ML_DISPATCH_FLOOR_CALIBRATION="0")))
    assert raw.min_odds == 1.74


def test_non_calibrated_model_is_identity():
    # prematch is not one of the four calibrated draft-phase models.
    ctx = _fixture_ctx(
        early_nw=None, early_win=None, late=None, all=None, lane=None,
        prematch=md.ModelVerdict("Dire", 0.72),
    )
    cfg = _cfg(PREMATCH_ML_ENABLED="1")
    win = _win(md.evaluate(ctx, cfg))
    assert win.models_for == ["prematch"]
    assert win.expected_wr == 0.72
    assert win.min_odds == round(1 / (0.72 - 0.12), 2) == 1.67


def test_late_conflict_after_wait_path_is_calibrated_too():
    # early_win (Radiant) vs late (Dire), after the 1860 s wait: floor from late.
    ctx = _fixture_ctx(
        game_time=1860.0, early_nw=None, all=None, lane=None,
        early_win=md.ModelVerdict("Radiant", 0.65), late=md.ModelVerdict("Dire", 0.70),
    )
    win = _win(md.evaluate(ctx, _cfg()))
    assert win.rule == md.RULE_WIN_LATE_AFTER_WAIT
    late_cal = _cal_side_conf("late", "Dire", 0.70)
    assert win.expected_wr == pytest.approx(late_cal)
    assert win.min_odds == round(1 / (late_cal - 0.12), 2)
    raw = _win(md.evaluate(ctx, _cfg(ML_DISPATCH_FLOOR_CALIBRATION="0")))
    assert raw.min_odds == round(1 / (0.70 - 0.12), 2) == 1.72
    assert win.min_odds != raw.min_odds


def test_env_flag_parsing_and_default():
    assert _cfg().floor_calibration is True
    assert _cfg(ML_DISPATCH_FLOOR_CALIBRATION="1").floor_calibration is True
    for off in ("0", "false", "off"):
        assert _cfg(ML_DISPATCH_FLOOR_CALIBRATION=off).floor_calibration is False


def test_probability_clip_keeps_extreme_confidence_finite():
    assert md._calibrate_side_confidence("late", "Dire", 1.0) < 1.0
    assert md._calibrate_side_confidence("late", "Radiant", 1.0) < 1.0
    assert md._calibrate_side_confidence("late", "Radiant", 0.0) > 0.0
