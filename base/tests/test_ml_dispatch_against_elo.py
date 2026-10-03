"""Owner decision 03.10.2026: no ML WIN decision on the ELO underdog.

Inputs are verbatim serv1 journal rows (runtime/ml_dispatch_decisions.jsonl,
captured 2026-10-03) stored in fixtures/ml_dispatch_against_elo_20261003.json;
see its ``_capture`` block for the capture command and the line numbers.
Assertions are made on what ``ml_dispatch.evaluate`` returns: ``_ml_dispatch_tick``
delivers exactly the ``timing == "now"`` decisions of that result
(cyberscore_try.py, ``if decision.timing != "now": continue``), so a missing
Decision is a missing bet.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from base import ml_dispatch as md

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "ml_dispatch_against_elo_20261003.json").read_text()
)
CASES = FIXTURE["cases"]
# Literal on purpose: the tests must fail on behaviour, not on a missing constant.
REASON_AGAINST_ELO = "win_against_elo_blocked"


def _record(name):
    return CASES[name]["record"]


def _verdict(record, name):
    value = record["verdicts"].get(name)
    return md.ModelVerdict(**value) if value else None


def _ctx(name, **overrides):
    """Rebuild the tick's Ctx from the journaled fields (floor-calibration test style)."""
    record = _record(name)
    kills30 = record["verdicts"].get("kills30") or {}
    kwargs = dict(
        match_key=record["match_key"], base_url=record["base_url"],
        map_num=record["map_num"], game_time=record["game_time"],
        radiant_team=record["teams"]["radiant"], dire_team=record["teams"]["dire"],
        heroes=record["heroes"], elo_radiant=record["elo_r"], elo_dire=record["elo_d"],
        early_nw=_verdict(record, "early_nw"), early_win=_verdict(record, "early_win"),
        late=_verdict(record, "late"), all=_verdict(record, "all"),
        lane=_verdict(record, "lane"), prematch=None, already_sent=set(),
        radiant_networth_lead=record.get("radiant_networth_lead"),
        lane_adv_dict=record.get("lane_adv_dict"),
        kills30_radiant=kills30.get("radiant"), kills30_dire=kills30.get("dire"),
    )
    kwargs.update(overrides)
    return md.Ctx(**kwargs)


def _cfg(**env):
    return md.Config.from_env(env)


def _wins(result):
    return [d for d in result.decisions if d.market == "win"]


def _win_skips(result, reason):
    return [s for s in result.skipped if s.market == "win" and s.reason == reason]


def _target_deficit(name):
    """ELO of the opponent minus ELO of the journaled target (computed, not trusted)."""
    record = _record(name)
    side = record["decisions"][0]["target_side"]
    elo_r, elo_d = record["elo_r"], record["elo_d"]
    return side, (elo_d - elo_r) if side == "Radiant" else (elo_r - elo_d)


UNDERDOG_CASES = [
    "underdog_single_model_delivered",
    "underdog_late_after_wait_delivered",
    "underdog_wait_600_pending",
    "underdog_win_and_kills_window",
]
FAVORITE_CASES = ["favorite_single_model_delivered", "favorite_late_after_wait_delivered"]

# Current calibrated (E-350) floor of the journaled decision; the journal itself
# holds the raw floor that was delivered before E-350.
CALIBRATED_MIN_ODDS = {
    "underdog_single_model_delivered": 2.04,
    "underdog_late_after_wait_delivered": 1.96,
    "underdog_wait_600_pending": 1.98,
    "underdog_win_and_kills_window": 2.02,
    "favorite_single_model_delivered": 2.09,
    "favorite_late_after_wait_delivered": 2.06,
}


# -- fixture sanity ---------------------------------------------------------

def test_fixture_rows_are_what_they_claim():
    expected = {
        "underdog_single_model_delivered": ("win_single_model_confirm", "now", True),
        "underdog_late_after_wait_delivered": ("win_late_after_wait", "now", True),
        "favorite_single_model_delivered": ("win_single_model_confirm", "now", True),
        "favorite_late_after_wait_delivered": ("win_late_after_wait", "now", True),
        "underdog_wait_600_pending": ("win_single_model_confirm", "wait_600", False),
        "underdog_win_and_kills_window": ("win_single_model_confirm", "wait_600", False),
    }
    for name, (rule, timing, delivered) in expected.items():
        win = [d for d in _record(name)["decisions"] if d["market"] == "win"][0]
        assert (win["rule"], win["timing"]) == (rule, timing), name
        was_delivered = any(
            d["market"] == "win" and d["status"] == "delivered"
            for d in _record(name)["delivered"]
        )
        assert was_delivered is delivered, name
    for name in UNDERDOG_CASES:
        side, deficit = _target_deficit(name)
        assert deficit >= 50, (name, side, deficit)
    for name in FAVORITE_CASES:
        side, deficit = _target_deficit(name)
        assert deficit <= -50, (name, side, deficit)


# -- the gate ---------------------------------------------------------------

@pytest.mark.parametrize("name", UNDERDOG_CASES)
def test_default_env_blocks_win_on_the_elo_underdog(name):
    record = _record(name)
    journaled = [d for d in record["decisions"] if d["market"] == "win"][0]
    side = journaled["target_side"]
    result = md.evaluate(_ctx(name), _cfg())

    assert _wins(result) == []
    skips = _win_skips(result, REASON_AGAINST_ELO)
    assert [s.side for s in skips] == [side]
    detail = skips[0].detail
    side_deficit = _target_deficit(name)[1]
    assert f"elo_radiant={record['elo_r']:.1f}" in detail
    assert f"elo_dire={record['elo_d']:.1f}" in detail
    assert f"deficit={side_deficit:.1f}" in detail
    # No side flipping: the other side stays what it was (no decision for it).
    other = "Dire" if side == "Radiant" else "Radiant"
    assert all(d.target_side != other for d in _wins(result))


@pytest.mark.parametrize("name", UNDERDOG_CASES + FAVORITE_CASES)
def test_gate_off_returns_the_journaled_decision_under_the_current_floor(name):
    record = _record(name)
    journaled = [d for d in record["decisions"] if d["market"] == "win"][0]
    result = md.evaluate(_ctx(name), _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK="0"))
    wins = _wins(result)
    assert len(wins) == 1
    win = wins[0]
    assert (win.target_side, win.rule, win.timing, win.models_for) == (
        journaled["target_side"], journaled["rule"], journaled["timing"],
        journaled["models_for"])
    assert win.min_odds == CALIBRATED_MIN_ODDS[name]
    assert _win_skips(result, REASON_AGAINST_ELO) == []
    # The raw floor reproduces the journaled (pre-E-350) number exactly.
    raw = _wins(md.evaluate(
        _ctx(name),
        _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK="0", ML_DISPATCH_FLOOR_CALIBRATION="0"),
    ))[0]
    assert raw.min_odds == journaled["min_odds"]


@pytest.mark.parametrize("off", ["0", "false", "off", "OFF"])
def test_env_spellings_disable_the_gate(off):
    result = md.evaluate(
        _ctx("underdog_single_model_delivered"), _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK=off))
    assert len(_wins(result)) == 1


def test_reason_constant_is_the_journaled_string():
    assert md.REASON_WIN_AGAINST_ELO == REASON_AGAINST_ELO


def test_hand_built_config_keeps_the_old_behaviour():
    assert md.Config().win_underdog_block is False
    assert md.Config.from_env({}).win_underdog_block is True
    assert md.Config.from_env({}).win_underdog_block_min_diff == 50.0
    result = md.evaluate(_ctx("underdog_single_model_delivered"), md.Config())
    assert len(_wins(result)) == 1


@pytest.mark.parametrize("name", FAVORITE_CASES)
def test_elo_favorite_win_is_unchanged_by_the_gate(name):
    on = md.evaluate(_ctx(name), _cfg())
    off = md.evaluate(_ctx(name), _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK="0"))
    assert len(_wins(on)) == 1
    assert _wins(on) == _wins(off)
    assert _win_skips(on, REASON_AGAINST_ELO) == []


def test_boundary_49_allowed_50_blocked_for_both_win_paths():
    for name, side in (("underdog_single_model_delivered", "Radiant"),
                       ("underdog_late_after_wait_delivered", "Radiant")):
        # Target Radiant; keep the journaled models, only move the ratings.
        assert _record(name)["decisions"][0]["target_side"] == side
        allowed = md.evaluate(_ctx(name, elo_radiant=1800.0, elo_dire=1849.0), _cfg())
        blocked = md.evaluate(_ctx(name, elo_radiant=1800.0, elo_dire=1850.0), _cfg())
        assert len(_wins(allowed)) == 1, name
        assert _win_skips(allowed, REASON_AGAINST_ELO) == [], name
        assert _wins(blocked) == [], name
        assert len(_win_skips(blocked, REASON_AGAINST_ELO)) == 1, name


def test_boundary_uses_the_win_knob_not_the_kills_underdog_knob():
    name = "underdog_single_model_delivered"  # deficit 82.8
    # Kills knob raised above the deficit: the win gate still blocks.
    assert _wins(md.evaluate(_ctx(name), _cfg(ML_DISPATCH_UNDERDOG_MIN_DIFF="500"))) == []
    # Win knob raised above the deficit: the win goes through.
    assert len(_wins(md.evaluate(
        _ctx(name), _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK_MIN_DIFF="100")))) == 1
    # Win knob lowered: a 30-point underdog is blocked too.
    assert _wins(md.evaluate(
        _ctx(name, elo_radiant=1800.0, elo_dire=1830.0),
        _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK_MIN_DIFF="25"))) == []


@pytest.mark.parametrize("name", ["underdog_single_model_delivered",
                                   "underdog_late_after_wait_delivered"])
@pytest.mark.parametrize("override", [
    {"elo_radiant": None},
    {"elo_dire": None},
    {"elo_radiant": None, "elo_dire": None},
    {"elo_radiant": float("nan")},
    {"elo_dire": float("inf")},
])
def test_missing_or_nonfinite_elo_keeps_the_decision(name, override):
    result = md.evaluate(_ctx(name, **override), _cfg())
    assert len(_wins(result)) == 1
    assert _win_skips(result, REASON_AGAINST_ELO) == []


def test_late_conflict_wait_is_unchanged_before_the_deadline():
    name = "underdog_late_after_wait_delivered"
    result = md.evaluate(_ctx(name, game_time=1000.0), _cfg())
    assert _wins(result) == []
    assert len(_win_skips(result, md.REASON_LATE_CONFLICT_WAIT)) == 2
    assert _win_skips(result, REASON_AGAINST_ELO) == []


def test_late_conflict_gate_keeps_the_veto_skip_of_the_other_side():
    result = md.evaluate(_ctx("underdog_late_after_wait_delivered"), _cfg())
    assert [s.side for s in _win_skips(result, REASON_AGAINST_ELO)] == ["Radiant"]
    assert [s.side for s in _win_skips(result, md.REASON_VETO)] == ["Dire"]


def test_wait_600_decision_is_dropped_too_not_only_the_released_one():
    name = "underdog_wait_600_pending"
    off = md.evaluate(_ctx(name), _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK="0"))
    assert [d.timing for d in _wins(off)] == ["wait_600"]
    on = md.evaluate(_ctx(name), _cfg())
    assert _wins(on) == []
    # The same pending situation once the 10th minute releases it: still blocked.
    released = md.evaluate(_ctx(name, game_time=650.0), _cfg())
    assert _wins(released) == []
    assert len(_win_skips(released, REASON_AGAINST_ELO)) == 1


def test_kills_underdog_decision_is_still_produced_for_the_blocked_side():
    name = "underdog_win_and_kills_window"
    record = _record(name)
    journaled_kills = [d for d in record["decisions"] if d["market"] == "kills_window"][0]
    window = [r for r in journaled_kills["reasons"] if r.startswith("window=")][0].split("=")[1]
    ctx = _ctx(name, kills_windows_open=[window])
    on = md.evaluate(ctx, _cfg())
    off = md.evaluate(ctx, _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK="0"))

    assert _wins(on) == []
    kills_on = [d for d in on.decisions if d.market == "kills_window"]
    assert len(kills_on) == 1
    assert (kills_on[0].target_side, kills_on[0].rule) == (
        journaled_kills["target_side"], journaled_kills["rule"])
    # Kills decisions are byte-identical with and without the gate.
    assert kills_on == [d for d in off.decisions if d.market == "kills_window"]
    # And the underdog side of the kills bet is the side the win gate blocked.
    assert on.underdog_side == journaled_kills["target_side"] == (
        [s.side for s in _win_skips(on, REASON_AGAINST_ELO)][0])


def test_dedup_still_reported_before_the_gate():
    name = "underdog_single_model_delivered"
    record = _record(name)
    key = (record["base_url"], record["map_num"], "win", "Radiant")
    result = md.evaluate(_ctx(name, already_sent={key}), _cfg())
    assert _wins(result) == []
    assert len(_win_skips(result, md.REASON_DEDUP)) == 1
    assert _win_skips(result, REASON_AGAINST_ELO) == []


def test_gate_detail_numbers_are_finite_and_signed_from_the_ratings():
    name = "underdog_single_model_delivered"
    skip = _win_skips(md.evaluate(_ctx(name), _cfg()), REASON_AGAINST_ELO)[0]
    record = _record(name)
    deficit = record["elo_d"] - record["elo_r"]  # target is Radiant
    assert math.isfinite(deficit) and deficit >= 50
    assert f"deficit={deficit:.1f}" in skip.detail


def test_dire_underdog_mirror_boundary():
    # Journaled Dire-target win (ELO favorite); move only the ratings so Dire
    # becomes the underdog: 49 points behind passes, 50 behind is blocked.
    name = "favorite_single_model_delivered"
    assert _record(name)["decisions"][0]["target_side"] == "Dire"
    allowed = md.evaluate(_ctx(name, elo_radiant=1849.0, elo_dire=1800.0), _cfg())
    blocked = md.evaluate(_ctx(name, elo_radiant=1850.0, elo_dire=1800.0), _cfg())
    assert len(_wins(allowed)) == 1
    assert _wins(blocked) == []
    assert len(_win_skips(blocked, REASON_AGAINST_ELO)) == 1


@pytest.mark.parametrize("raw", ["nan", "inf", "-inf", "-5", "garbage"])
def test_invalid_threshold_falls_back_to_50(raw):
    assert _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK_MIN_DIFF=raw).win_underdog_block_min_diff == 50.0
    name = "underdog_single_model_delivered"  # deficit 82.8 >= 50: still blocked
    assert _wins(md.evaluate(_ctx(name), _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK_MIN_DIFF=raw))) == []
    # The ELO favorite is never blocked whatever the threshold string.
    fav = "favorite_single_model_delivered"
    assert len(_wins(md.evaluate(_ctx(fav), _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK_MIN_DIFF=raw)))) == 1


def test_zero_threshold_blocks_strictly_lower_elo_only():
    name = "underdog_single_model_delivered"
    cfg = _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK_MIN_DIFF="0")
    assert len(_wins(md.evaluate(_ctx(name, elo_radiant=1800.0, elo_dire=1800.0), cfg))) == 1
    assert _wins(md.evaluate(_ctx(name, elo_radiant=1800.0, elo_dire=1800.5), cfg)) == []
