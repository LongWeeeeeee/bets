"""Owner decision 07.10.2026 (research E-357): the 31st-minute Late-conflict bet
``win_late_after_wait`` is skipped when the Late side trails by >= 11000 net worth
at the tick (``ML_DISPATCH_LATE_WAIT_NW_GATE``, <= 0 disables).

Inputs are 7 verbatim serv1 journal ticks (``runtime/ml_dispatch_decisions.jsonl``,
12.09-06.10.2026) in fixtures/ml_dispatch_late_nw_gate_ticks_20261007.jsonl, see its README.
Assertions are made on what ``ml_dispatch.evaluate`` returns: the dispatcher delivers exactly
the ``timing == "now"`` decisions of that result, so a missing Decision is a missing bet.
The deficit used by the tests is recomputed here from the tick's own
``radiant_networth_lead`` (the corpus ``late_side_deficit`` field is NOT the gate input).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from base import ml_dispatch as md

FIXTURE = Path(__file__).parent / "fixtures" / "ml_dispatch_late_nw_gate_ticks_20261007.jsonl"
ROWS = [json.loads(line) for line in FIXTURE.read_text().splitlines() if line.strip()]
# Literals on purpose: the tests must fail on behaviour, not on a missing constant.
REASON = "late_conflict_nw_gate"
RULE = "win_late_after_wait"
GATE = 11000.0

# row index -> (late side, tick NW deficit of the late side, expected: gated?)
# 0,1: Dire 15.7k / 16.1k behind; 2: NW not logged; 3: Dire 10.7k behind (just under);
# 4: Dire 8.5k behind; 5: Radiant (late) 11.9k behind -> sign trap, lead is -11881;
# 6: Dire AHEAD (Radiant lead -32468).
EXPECTED = {
    0: ("Dire", 15661.0, True),
    1: ("Dire", 16133.0, True),
    2: ("Dire", None, False),
    3: ("Dire", 10728.0, False),
    4: ("Dire", 8538.0, False),
    5: ("Radiant", 11881.0, True),
    6: ("Dire", -32468.0, False),
}


def _verdict(tick, name):
    value = tick["verdicts"].get(name)
    return md.ModelVerdict(**value) if value else None


def _ctx(index, **overrides):
    tick = ROWS[index]["tick"]
    kills30 = tick["verdicts"].get("kills30") or {}
    kwargs = dict(
        match_key=tick["match_key"], base_url=tick["base_url"],
        map_num=tick["map_num"], game_time=tick["game_time"],
        radiant_team=tick["teams"]["radiant"], dire_team=tick["teams"]["dire"],
        heroes=tick["heroes"], elo_radiant=tick["elo_r"], elo_dire=tick["elo_d"],
        early_nw=_verdict(tick, "early_nw"), early_win=_verdict(tick, "early_win"),
        late=_verdict(tick, "late"), all=_verdict(tick, "all"),
        lane=_verdict(tick, "lane"), prematch=None, already_sent=set(),
        radiant_networth_lead=tick.get("radiant_networth_lead"),
        lane_adv_dict=tick.get("lane_adv_dict"),
        kills30_radiant=kills30.get("radiant"), kills30_dire=kills30.get("dire"),
    )
    kwargs.update(overrides)
    return md.Ctx(**kwargs)


def _cfg(**env):
    # The 03.10.2026 against-ELO gate is a different gate: switch it off so these
    # ticks (several are ELO-underdog Late bets journaled before it) isolate the NW gate.
    env.setdefault("ML_DISPATCH_WIN_UNDERDOG_BLOCK", "0")
    return md.Config.from_env(env)


def _wins(result):
    return [d for d in result.decisions if d.market == "win"]


def _win_skips(result, reason):
    return [s for s in result.skipped if s.market == "win" and s.reason == reason]


def _tick_deficit(index, side):
    """Deficit of ``side`` from the tick's own NW lead (+ = side behind); None if unknown."""
    lead = ROWS[index]["tick"].get("radiant_networth_lead")
    if lead is None:
        return None
    return -lead if side == "Radiant" else lead


def test_fixture_rows_are_what_they_claim():
    assert len(ROWS) == 7
    for index, (side, deficit, _gated) in EXPECTED.items():
        tick = ROWS[index]["tick"]
        win = [d for d in tick["decisions"] if d["market"] == "win"]
        assert [(d["rule"], d["target_side"], d["timing"]) for d in win] == [(RULE, side, "now")], index
        assert tick["game_time"] >= 1860.0, index
        assert ROWS[index]["late_side"] == side, index
        # the expectation table is derived from the tick's own NW, not from the corpus field
        assert _tick_deficit(index, side) == deficit, index


def test_nw_missing_row_has_no_nw_key():
    assert "radiant_networth_lead" not in ROWS[2]["tick"]


@pytest.mark.parametrize("index", sorted(EXPECTED))
def test_default_gate_on_real_ticks(index):
    side, deficit, gated = EXPECTED[index]
    result = md.evaluate(_ctx(index), _cfg())
    if gated:
        assert deficit >= GATE
        assert _wins(result) == [], index
        skips = _win_skips(result, REASON)
        assert len(skips) == 1 and skips[0].side == side, index
        assert str(int(deficit)) in skips[0].detail and "11000" in skips[0].detail, skips[0].detail
    else:
        # the real late-conflict path, not a re-derived single-model branch
        wins = _wins(result)
        assert [(d.rule, d.target_side, d.timing) for d in wins] == [(RULE, side, "now")], index
        assert _win_skips(result, REASON) == [], index


def test_nw_unknown_keeps_the_bet_and_is_marked():
    wins = _wins(md.evaluate(_ctx(2), _cfg()))
    assert [d.rule for d in wins] == [RULE]
    assert "nw=unknown" in wins[0].reasons


@pytest.mark.parametrize("bad", [None, float("nan"), float("inf"), float("-inf")])
def test_nonfinite_nw_keeps_the_bet(bad):
    # tick 0 has a 15.7k Dire deficit; with the lead unusable the gate must not fire
    result = md.evaluate(_ctx(0, radiant_networth_lead=bad), _cfg())
    assert [d.rule for d in _wins(result)] == [RULE]
    assert _win_skips(result, REASON) == []
    assert "nw=unknown" in _wins(result)[0].reasons


@pytest.mark.parametrize("off", ["0", "-1"])
def test_env_off_restores_the_old_bet(off):
    cfg = _cfg(ML_DISPATCH_LATE_WAIT_NW_GATE=off)
    for index, (side, _deficit, _gated) in EXPECTED.items():
        result = md.evaluate(_ctx(index), cfg)
        assert [(d.rule, d.target_side) for d in _wins(result)] == [(RULE, side)], index
        assert _win_skips(result, REASON) == [], index
        assert "nw=unknown" not in _wins(result)[0].reasons, index


def test_env_changes_the_threshold():
    # tick 5 (deficit 11881) passes under a 12000 gate and is blocked under 11881
    assert [d.rule for d in _wins(md.evaluate(_ctx(5), _cfg(ML_DISPATCH_LATE_WAIT_NW_GATE="12000")))] == [RULE]
    assert _wins(md.evaluate(_ctx(5), _cfg(ML_DISPATCH_LATE_WAIT_NW_GATE="11881"))) == []


def test_boundary_is_inclusive_and_sign_correct_for_both_sides():
    # tick 0: Dire is the late side; Radiant lead 11000 = exactly the gate -> blocked, 10999 -> bet
    assert _wins(md.evaluate(_ctx(0, radiant_networth_lead=11000.0), _cfg())) == []
    assert len(_wins(md.evaluate(_ctx(0, radiant_networth_lead=10999.0), _cfg()))) == 1
    # a Dire lead (negative radiant lead) never blocks the Dire bet
    assert len(_wins(md.evaluate(_ctx(0, radiant_networth_lead=-20000.0), _cfg()))) == 1
    # tick 5: Radiant is the late side; Radiant lead -11000 -> blocked, -10999 -> bet, +20000 -> bet
    assert _wins(md.evaluate(_ctx(5, radiant_networth_lead=-11000.0), _cfg())) == []
    assert len(_wins(md.evaluate(_ctx(5, radiant_networth_lead=-10999.0), _cfg()))) == 1
    assert len(_wins(md.evaluate(_ctx(5, radiant_networth_lead=20000.0), _cfg()))) == 1


def test_gate_applies_every_tick_after_the_deadline_and_fires_later_when_deficit_shrinks():
    cfg = _cfg()
    blocked = md.evaluate(_ctx(0, game_time=1861.0, radiant_networth_lead=12000.0), cfg)
    assert _wins(blocked) == [] and len(_win_skips(blocked, REASON)) == 1
    later = md.evaluate(_ctx(0, game_time=1921.0, radiant_networth_lead=9000.0), cfg)
    assert [d.rule for d in _wins(later)] == [RULE]


def test_before_the_deadline_the_wait_skip_is_unchanged():
    result = md.evaluate(_ctx(0, game_time=1859.0), _cfg())
    assert _wins(result) == []
    assert len(_win_skips(result, "late_conflict_wait")) == 2
    assert _win_skips(result, REASON) == []


def test_dedup_is_checked_before_the_gate():
    ctx = _ctx(0)
    key = md._dedup_key(ctx, "win", "Dire")
    result = md.evaluate(_ctx(0, already_sent={key}), _cfg())
    assert _wins(result) == []
    assert len(_win_skips(result, "dedup")) == 1
    assert _win_skips(result, REASON) == []


def test_nw_gate_comes_before_the_against_elo_gate():
    # tick 3: Dire is the ELO underdog by 106 (against-ELO gate would block). With a 12k Dire
    # deficit set on top of the captured tick, the NW gate must be the reported reason.
    cfg = md.Config.from_env({})  # production defaults: both gates on
    result = md.evaluate(_ctx(3, radiant_networth_lead=12000.0), cfg)
    assert _wins(result) == []
    assert len(_win_skips(result, REASON)) == 1
    assert _win_skips(result, "win_against_elo_blocked") == []
    # NW fine -> the old against-ELO gate still reports as before
    result = md.evaluate(_ctx(3), cfg)
    assert _wins(result) == []
    assert len(_win_skips(result, "win_against_elo_blocked")) == 1


def test_the_other_side_veto_skip_is_kept():
    result = md.evaluate(_ctx(0), _cfg())
    vetoes = _win_skips(result, "veto")
    assert [s.side for s in vetoes] == ["Radiant"]


def test_config_default_and_env_parsing():
    assert md.Config().late_wait_nw_gate == 11000.0
    assert md.Config.from_env({}).late_wait_nw_gate == 11000.0
    assert md.Config.from_env({"ML_DISPATCH_LATE_WAIT_NW_GATE": "15000"}).late_wait_nw_gate == 15000.0
    assert md.Config.from_env({"ML_DISPATCH_LATE_WAIT_NW_GATE": "garbage"}).late_wait_nw_gate == 11000.0
    assert md.REASON_LATE_CONFLICT_NW_GATE == REASON
