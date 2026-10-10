"""Lead decision 10.10.2026 (E-374 follow-up, card ingame-vmwo): an ML WIN decision on
the ELO underdog is kept only when Early Win, Early NW and All each favour the TARGET
side with confidence >= ``ML_DISPATCH_WIN_UNDERDOG_TRIO_MIN`` (default 0.60).

Inputs are verbatim serv1 journal rows (ml_dispatch_decisions.jsonl, copied 2026-10-10),
stored in fixtures/ml_dispatch_underdog_trio_20261010.json; see its ``_capture`` block for
the capture command and line numbers. Assertions are on what ``ml_dispatch.evaluate``
returns: ``_ml_dispatch_tick`` delivers exactly the ``timing == "now"`` decisions of that
result, so a missing Decision is a missing bet. Reason strings are literals on purpose.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from base import ml_dispatch as md

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "ml_dispatch_underdog_trio_20261010.json").read_text()
)
CASES = FIXTURE["cases"]
TRIO_REASON = "win_underdog_trio_unsupported"
BLOCK_REASON = "win_against_elo_blocked"

KEPT = "underdog_trio_supported_single"            # journal 2320
EARLY_NW_AGAINST = "underdog_early_nw_against_single"  # journal 2445
LATE_AFTER_WAIT = "underdog_late_after_wait_early_against"  # journal 2408
FAV_SINGLE = "favorite_single_delivered"           # journal 3458
FAV_LATE = "favorite_late_after_wait_delivered"    # journal 3482
KILLS_ONLY = "underdog_kills_window_no_win"        # journal 428


def _record(name):
    return CASES[name]["record"]


def _verdict(record, name):
    value = record["verdicts"].get(name)
    return md.ModelVerdict(**value) if value else None


def _ctx(name, **overrides):
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
    """Production ``from_env`` (block off since 09.10, trio gate 0.60) plus overrides."""
    return md.Config.from_env(dict(env))


def _wins(result):
    return [d for d in result.decisions if d.market == "win"]


def _skips(result, reason):
    return [s for s in result.skipped if s.market == "win" and s.reason == reason]


def _target(name):
    return [d for d in _record(name)["decisions"] if d["market"] == "win"][0]["target_side"]


def _deficit(name, side):
    record = _record(name)
    elo_r, elo_d = record["elo_r"], record["elo_d"]
    return (elo_d - elo_r) if side == "Radiant" else (elo_r - elo_d)


# -- fixture sanity ----------------------------------------------------------

def test_fixture_rows_are_what_they_claim():
    for name in (KEPT, EARLY_NW_AGAINST, LATE_AFTER_WAIT):
        side = _target(name)
        assert _deficit(name, side) >= 50, name
        assert _record(name)["underdog_side"] == side, name
    for name in (FAV_SINGLE, FAV_LATE):
        assert _deficit(name, _target(name)) <= -50, name
    rules = {n: [d for d in _record(n)["decisions"] if d["market"] == "win"][0]["rule"]
             for n in (KEPT, EARLY_NW_AGAINST, LATE_AFTER_WAIT, FAV_SINGLE, FAV_LATE)}
    assert rules[KEPT] == rules[EARLY_NW_AGAINST] == rules[FAV_SINGLE] == "win_single_model_confirm"
    assert rules[LATE_AFTER_WAIT] == rules[FAV_LATE] == "win_late_after_wait"
    # The kept row has all three trio models >= 0.60 for the target; the two blocked rows do not.
    kept_side = _target(KEPT)
    for model in ("early_win", "early_nw", "all"):
        v = _record(KEPT)["verdicts"][model]
        assert v["side"] == kept_side and v["confidence"] >= 0.60
    v = _record(EARLY_NW_AGAINST)["verdicts"]["early_nw"]
    assert v["side"] != _target(EARLY_NW_AGAINST)
    late_side = _target(LATE_AFTER_WAIT)
    for model in ("early_win", "early_nw", "all"):
        assert _record(LATE_AFTER_WAIT)["verdicts"][model]["side"] != late_side


# -- (a) default blocks unsupported underdog WINs -----------------------------

@pytest.mark.parametrize("name", [EARLY_NW_AGAINST, LATE_AFTER_WAIT])
def test_default_skips_underdog_win_without_trio_support(name):
    record = _record(name)
    side = _target(name)
    result = md.evaluate(_ctx(name), _cfg())

    assert _wins(result) == []
    skips = _skips(result, TRIO_REASON)
    assert [s.side for s in skips] == [side]          # exactly one record, on the target
    detail = skips[0].detail
    for model in ("early_win", "early_nw", "all"):
        v = record["verdicts"][model]
        assert f"{model}={v['side']}:{v['confidence']:.4f}" in detail, (model, detail)
    assert f"elo_radiant={record['elo_r']:.1f}" in detail
    assert f"elo_dire={record['elo_d']:.1f}" in detail
    assert f"deficit={_deficit(name, side):.1f}" in detail
    assert "trio_min=0.60" in detail
    # The old block did not fire (block is off) and no decision exists for the other side.
    assert _skips(result, BLOCK_REASON) == []
    other = "Dire" if side == "Radiant" else "Radiant"
    assert all(d.target_side != other for d in _wins(result))


# -- (b) TRIO_MIN=0 is the 09.10 behaviour -----------------------------------

@pytest.mark.parametrize("name", [EARLY_NW_AGAINST, LATE_AFTER_WAIT])
@pytest.mark.parametrize("off", ["0", "off", "false", "OFF", ""])
def test_gate_off_keeps_the_09_10_win_decision(name, off):
    journaled = [d for d in _record(name)["decisions"] if d["market"] == "win"][0]
    result = md.evaluate(_ctx(name), _cfg(ML_DISPATCH_WIN_UNDERDOG_TRIO_MIN=off))
    wins = _wins(result)
    assert len(wins) == 1
    assert (wins[0].target_side, wins[0].rule, wins[0].timing, wins[0].models_for) == (
        journaled["target_side"], journaled["rule"], journaled["timing"], journaled["models_for"])
    assert _skips(result, TRIO_REASON) == []


# -- (c) supported underdog stays delivered ----------------------------------

def test_default_keeps_underdog_win_with_full_trio_support():
    journaled = [d for d in _record(KEPT)["decisions"] if d["market"] == "win"][0]
    result = md.evaluate(_ctx(KEPT), _cfg())
    wins = _wins(result)
    assert len(wins) == 1
    win = wins[0]
    assert (win.target_side, win.rule, win.timing) == (
        journaled["target_side"], journaled["rule"], "now")        # "now" = delivered
    assert _skips(result, TRIO_REASON) == []
    # The decision is byte-identical to the one with the gate switched off.
    off = md.evaluate(_ctx(KEPT), _cfg(ML_DISPATCH_WIN_UNDERDOG_TRIO_MIN="0"))
    assert _wins(off) == wins


# -- (d) the full ban is untouched -------------------------------------------

@pytest.mark.parametrize("name", [KEPT, EARLY_NW_AGAINST, LATE_AFTER_WAIT])
def test_full_ban_behaves_exactly_as_before(name):
    side = _target(name)
    banned = md.evaluate(_ctx(name), _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK="1"))
    old = md.evaluate(
        _ctx(name), _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK="1", ML_DISPATCH_WIN_UNDERDOG_TRIO_MIN="0"))
    assert banned == old
    assert _wins(banned) == []
    assert [s.side for s in _skips(banned, BLOCK_REASON)] == [side]
    assert _skips(banned, TRIO_REASON) == []


def test_realized_release_under_the_full_ban_is_untouched_by_the_trio_gate():
    # Dire (the ELO underdog of the captured late_after_wait tick) is ahead by 3000 NW
    # at 1910 s: the E-365 release lifts the full ban; the trio gate must not run there.
    ctx_kwargs = dict(radiant_networth_lead=-3000.0)
    banned = md.evaluate(_ctx(LATE_AFTER_WAIT, **ctx_kwargs), _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK="1"))
    old = md.evaluate(
        _ctx(LATE_AFTER_WAIT, **ctx_kwargs),
        _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK="1", ML_DISPATCH_WIN_UNDERDOG_TRIO_MIN="0"))
    assert banned == old
    wins = _wins(banned)
    assert len(wins) == 1
    assert any(r.startswith("underdog_realized_release") for r in wins[0].reasons)
    assert _skips(banned, TRIO_REASON) == []


# -- (e) favourites and kills markets are untouched --------------------------

@pytest.mark.parametrize("name", [FAV_SINGLE, FAV_LATE])
def test_elo_favorite_win_is_unchanged(name):
    on = md.evaluate(_ctx(name), _cfg())
    off = md.evaluate(_ctx(name), _cfg(ML_DISPATCH_WIN_UNDERDOG_TRIO_MIN="0"))
    assert len(_wins(on)) == 1
    assert on == off
    assert _skips(on, TRIO_REASON) == []


def test_underdog_kills_window_decision_is_unchanged():
    record = _record(KILLS_ONLY)
    kills = [d for d in record["decisions"] if d["market"] == "kills_window"][0]
    window = [r for r in kills["reasons"] if r.startswith("window=")][0].split("=")[1]
    ctx = _ctx(KILLS_ONLY, kills_windows_open=[window])
    env = {"ML_DISPATCH_UNDERDOG_KILLS_WINDOW": "1"}
    on = md.evaluate(ctx, _cfg(**env))
    off = md.evaluate(ctx, _cfg(ML_DISPATCH_WIN_UNDERDOG_TRIO_MIN="0", **env))
    kills_on = [d for d in on.decisions if d.market == "kills_window"]
    assert len(kills_on) == 1
    assert kills_on == [d for d in off.decisions if d.market == "kills_window"]
    assert _skips(on, TRIO_REASON) == []


def test_kills_decisions_survive_a_trio_skip_on_the_same_tick():
    # Captured 2445 also carries a kills_total decision: it must survive the win skip.
    on = md.evaluate(_ctx(EARLY_NW_AGAINST), _cfg())
    off = md.evaluate(_ctx(EARLY_NW_AGAINST), _cfg(ML_DISPATCH_WIN_UNDERDOG_TRIO_MIN="0"))
    assert [d for d in on.decisions if d.market != "win"] == [
        d for d in off.decisions if d.market != "win"]
    assert any(d.market == "kills_total" for d in on.decisions)


# -- (f) env parsing ---------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("0.65", 0.65), ("0.60", 0.60), ("0.7", 0.7),
    ("off", 0.0), ("OFF", 0.0), ("false", 0.0), ("0", 0.0), ("", 0.0), ("0.0", 0.0), ("-1", 0.0),
    ("abc", 0.60), ("nan", 0.60), ("inf", 0.60), ("-inf", 0.60),
    ("0.99", 0.95), ("0.95", 0.95), ("5", 0.95),
])
def test_env_parsing(raw, expected):
    assert _cfg(ML_DISPATCH_WIN_UNDERDOG_TRIO_MIN=raw).win_underdog_trio_min == pytest.approx(expected)


def test_defaults_dataclass_off_from_env_on():
    assert md.Config().win_underdog_trio_min == 0.0
    assert md.Config.from_env({}).win_underdog_trio_min == pytest.approx(0.60)
    # Hand-built Config() keeps the 09.10 behaviour.
    assert len(_wins(md.evaluate(_ctx(EARLY_NW_AGAINST), md.Config()))) == 1


def test_reason_constant_is_the_journaled_string():
    assert md.REASON_WIN_UNDERDOG_TRIO_UNSUPPORTED == TRIO_REASON


# -- thresholds, sides, missing data ------------------------------------------

def test_threshold_is_inclusive_and_env_driven():
    # KEPT: min trio confidence is All 0.7494.
    assert len(_wins(md.evaluate(_ctx(KEPT), _cfg(ML_DISPATCH_WIN_UNDERDOG_TRIO_MIN="0.7494")))) == 1
    blocked = md.evaluate(_ctx(KEPT), _cfg(ML_DISPATCH_WIN_UNDERDOG_TRIO_MIN="0.78"))
    assert _wins(blocked) == []
    assert [s.side for s in _skips(blocked, TRIO_REASON)] == ["Radiant"]
    assert "trio_min=0.78" in _skips(blocked, TRIO_REASON)[0].detail


@pytest.mark.parametrize("model", ["early_win", "early_nw", "all"])
def test_each_trio_model_is_required(model):
    side = _target(KEPT)
    other = "Dire" if side == "Radiant" else "Radiant"
    # low confidence for the target
    low = md.ModelVerdict(side=side, confidence=0.59)
    res = md.evaluate(_ctx(KEPT, **{model: low}), _cfg())
    assert _wins(res) == [] and len(_skips(res, TRIO_REASON)) == 1
    # the verdict side is the opponent (below the veto threshold, so only the trio gate fires)
    flipped = md.ModelVerdict(side=other, confidence=0.55)
    res = md.evaluate(_ctx(KEPT, **{model: flipped}), _cfg())
    assert _wins(res) == [] and len(_skips(res, TRIO_REASON)) == 1
    # missing verdict
    res = md.evaluate(_ctx(KEPT, **{model: None}), _cfg())
    assert _wins(res) == [] and len(_skips(res, TRIO_REASON)) == 1
    assert f"{model}=none" in _skips(res, TRIO_REASON)[0].detail
    # exactly at the threshold the model supports the target
    edge = md.ModelVerdict(side=side, confidence=0.60)
    res = md.evaluate(_ctx(KEPT, **{model: edge}), _cfg())
    assert len(_wins(res)) == 1 and _skips(res, TRIO_REASON) == []


def test_confidence_is_p_of_the_verdict_side_not_p_radiant():
    # Dire-underdog tick: all supports Dire with 0.62 -> kept even though P(Radiant)=0.38.
    name = LATE_AFTER_WAIT
    side = _target(name)
    assert side == "Dire"
    supported = {m: md.ModelVerdict(side="Dire", confidence=0.62) for m in ("early_win", "early_nw", "all")}
    res = md.evaluate(_ctx(name, **supported), _cfg())
    assert _skips(res, TRIO_REASON) == []
    assert [d.target_side for d in _wins(res)] == ["Dire"]


def test_missing_or_nonfinite_elo_keeps_the_decision():
    for override in ({"elo_radiant": None}, {"elo_dire": None},
                     {"elo_radiant": float("nan")}, {"elo_dire": float("inf")}):
        res = md.evaluate(_ctx(EARLY_NW_AGAINST, **override), _cfg())
        assert len(_wins(res)) == 1, override
        assert _skips(res, TRIO_REASON) == [], override


def test_underdog_boundary_uses_the_win_diff_knob():
    name = EARLY_NW_AGAINST  # target Radiant, journaled deficit 96.9
    ok = md.evaluate(_ctx(name, elo_radiant=1800.0, elo_dire=1849.0), _cfg())
    assert len(_wins(ok)) == 1 and _skips(ok, TRIO_REASON) == []
    cut = md.evaluate(_ctx(name, elo_radiant=1800.0, elo_dire=1850.0), _cfg())
    assert _wins(cut) == [] and len(_skips(cut, TRIO_REASON)) == 1
    # equal ratings are never an underdog
    eq = md.evaluate(_ctx(name, elo_radiant=1800.0, elo_dire=1800.0),
                     _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK_MIN_DIFF="0"))
    assert len(_wins(eq)) == 1
    # win knob raised above the deficit
    hi = md.evaluate(_ctx(name), _cfg(ML_DISPATCH_WIN_UNDERDOG_BLOCK_MIN_DIFF="100"))
    assert len(_wins(hi)) == 1


def test_dedup_is_reported_before_the_trio_gate_and_once():
    name = EARLY_NW_AGAINST
    record = _record(name)
    key = (record["base_url"], record["map_num"], "win", "Radiant")
    res = md.evaluate(_ctx(name, already_sent={key}), _cfg())
    assert _wins(res) == []
    assert len([s for s in res.skipped if s.reason == md.REASON_DEDUP]) == 1
    assert _skips(res, TRIO_REASON) == []


def test_late_conflict_wait_is_unchanged_before_the_deadline():
    res = md.evaluate(_ctx(LATE_AFTER_WAIT, game_time=1000.0), _cfg())
    assert _wins(res) == []
    assert _skips(res, TRIO_REASON) == []
    assert len(_skips(res, md.REASON_LATE_CONFLICT_WAIT)) == 2


def test_detail_numbers_are_finite():
    skip = _skips(md.evaluate(_ctx(EARLY_NW_AGAINST), _cfg()), TRIO_REASON)[0]
    assert math.isfinite(_deficit(EARLY_NW_AGAINST, "Radiant"))
    assert "nan" not in skip.detail.lower()


# -- threshold <= 0.5: the gate compares P(target), not "the verdict side is the target" ------

def test_opposite_side_verdict_is_converted_to_p_target_below_one_half_threshold():
    # Regression for the independent verifier finding (Codex astra run d92c2c71): the gate
    # required ``verdict.side == target`` and rejected an opposite-side verdict without the
    # 1-P conversion. The measured E-374 definition is min over the three of P(underdog).
    # Producers emit conf = max(p, 1-p) >= 0.5, so the default 0.60 never exposes the
    # difference; only a threshold <= 0.5 does. No such threshold exists in captured data,
    # so the ctx is HAND-BUILT on top of the captured KEPT tick (Radiant = ELO underdog,
    # deficit >= 50): Early Win / All stay the captured Radiant >= 0.60 verdicts.
    side = _target(KEPT)
    assert side == "Radiant" and _deficit(KEPT, side) >= 50
    verdicts = _record(KEPT)["verdicts"]
    assert verdicts["early_win"]["side"] == "Radiant" and verdicts["early_win"]["confidence"] >= 0.60
    assert verdicts["all"]["side"] == "Radiant" and verdicts["all"]["confidence"] >= 0.60
    cfg = _cfg(ML_DISPATCH_WIN_UNDERDOG_TRIO_MIN="0.40")
    assert cfg.win_underdog_trio_min == pytest.approx(0.40)

    # Early NW says Dire 0.55 -> P(Radiant) = 0.45 >= 0.40: supported, decision KEPT.
    kept = md.evaluate(_ctx(KEPT, early_nw=md.ModelVerdict(side="Dire", confidence=0.55)), cfg)
    assert [d.target_side for d in _wins(kept)] == ["Radiant"]
    assert _skips(kept, TRIO_REASON) == []

    # Early NW says Dire 0.65 -> P(Radiant) = 0.35 < 0.40: unsupported, skipped. Dire >= 0.60
    # also trips the late-conflict wait in ``evaluate`` before any win decision exists, so
    # this half drives the gate function itself with the Decision that ``evaluate`` built
    # for the 0.55 tick (the gate sees finished win decisions).
    decision = _wins(kept)[0]
    ctx65 = _ctx(KEPT, early_nw=md.ModelVerdict(side="Dire", confidence=0.65))
    skip = md._win_underdog_trio_skip(ctx65, cfg, decision)
    assert skip is not None
    assert (skip.market, skip.side, skip.reason) == ("win", "Radiant", TRIO_REASON)
    assert "early_nw=Dire:0.6500(p_target=0.3500)" in skip.detail
    assert "trio_min=0.40" in skip.detail
    kept_list, skipped_list = md._apply_win_underdog_trio_gate(ctx65, cfg, [decision], [])
    assert kept_list == [] and [s.reason for s in skipped_list] == [TRIO_REASON]
    # ... and the 0.55 context passes the same function unchanged.
    ctx55 = _ctx(KEPT, early_nw=md.ModelVerdict(side="Dire", confidence=0.55))
    assert md._win_underdog_trio_skip(ctx55, cfg, decision) is None


def test_skip_detail_shows_p_target_per_model():
    # EARLY_NW_AGAINST: Early NW is Dire 0.5405 against the Radiant target (fixture
    # P(target) 0.4595); the other two models are printed with their own p_target.
    record = _record(EARLY_NW_AGAINST)
    skip = _skips(md.evaluate(_ctx(EARLY_NW_AGAINST), _cfg()), TRIO_REASON)[0]
    side = skip.side
    for model in ("early_win", "early_nw", "all"):
        v = record["verdicts"][model]
        p_target = v["confidence"] if v["side"] == side else 1.0 - v["confidence"]
        assert f"{model}={v['side']}:{v['confidence']:.4f}(p_target={p_target:.4f})" in skip.detail
    assert "early_nw=Dire:0.5405(p_target=0.4595)" in skip.detail
