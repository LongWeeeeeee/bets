"""Owner decision 05.10.2026 (card ingame-h9b5): the ELO-underdog ``kills_window``
bet (rule ``kills_underdog_early_window``) is OFF by default.

Inputs are three captured serv1 ticks (see the README next to the fixture); the
panel verdict and the kills30 override in two cases are added by the test and are
marked as such. Config is built with ``Config.from_env`` so the env switch itself
is under test.
"""
from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import sys

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from base import ml_dispatch as md  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "ml_dispatch_underdog_kills_ticks_20261005.jsonl"
ROWS = [json.loads(line) for line in FIXTURE.read_text().splitlines() if line.strip()]
ROW_PANEL, ROW_TOTAL, ROW_LATE_CONFLICT = ROWS

ENV_NAME = "ML_DISPATCH_UNDERDOG_KILLS_WINDOW"
DISABLED = "underdog_kills_window_disabled"
UNDERDOG_RULE = "kills_underdog_early_window"


def _cfg(value=None):
    env = {} if value is None else {ENV_NAME: value}
    return md.Config.from_env(env)


def _ctx(row, **overrides):
    verdicts = row["verdicts"]

    def verdict(name):
        value = verdicts.get(name)
        return md.ModelVerdict(**value) if value else None

    kills30 = verdicts.get("kills30") or {}
    ctx = md.Ctx(
        match_key=row["match_key"], base_url=row["base_url"],
        map_num=row["map_num"], game_time=row["game_time"],
        radiant_team=row["teams"]["radiant"], dire_team=row["teams"]["dire"],
        heroes=row["heroes"], elo_radiant=row["elo_r"], elo_dire=row["elo_d"],
        early_nw=verdict("early_nw"), early_win=verdict("early_win"),
        late=verdict("late"), all=verdict("all"), lane=verdict("lane"),
        prematch=verdict("prematch"),
        kills_windows_open=["5_15"],
        kills30_radiant=kills30.get("radiant"), kills30_dire=kills30.get("dire"),
        already_sent=set(),
    )
    return replace(ctx, **overrides)


def _windows(result):
    return [d for d in result.decisions if d.market == "kills_window"]


def _totals(result):
    return [d for d in result.decisions if d.market == "kills_total"]


def _captured_window(row):
    (decision,) = [d for d in row["decisions"] if d["rule"] == UNDERDOG_RULE]
    return decision


def test_default_is_off_for_env_and_hand_built_config():
    assert md.Config().underdog_kills_window_enabled is False
    assert _cfg().underdog_kills_window_enabled is False
    for value in ("0", "false", "off", "OFF"):
        assert _cfg(value).underdog_kills_window_enabled is False
    for value in ("1", "true", "on"):
        assert _cfg(value).underdog_kills_window_enabled is True


@pytest.mark.parametrize("row", ROWS, ids=["dire_underdog", "radiant_underdog", "late_conflict_b"])
def test_default_env_emits_no_underdog_window_only_the_skip(row):
    result = md.evaluate(_ctx(row), _cfg())
    assert [d for d in result.decisions if d.rule == UNDERDOG_RULE] == []
    assert _windows(result) == []
    disabled = [s for s in result.skipped if s.reason == DISABLED]
    assert len(disabled) == 1
    assert disabled[0].market == "kills_window"
    assert disabled[0].side == row["underdog_side"]


@pytest.mark.parametrize("row", ROWS, ids=["dire_underdog", "radiant_underdog", "late_conflict_b"])
def test_env_on_restores_the_captured_underdog_window_exactly(row):
    captured = _captured_window(row)
    result = md.evaluate(_ctx(row), _cfg("1"))
    (decision,) = [d for d in result.decisions if d.rule == UNDERDOG_RULE]
    assert decision.market == "kills_window"
    assert decision.target_side == captured["target_side"]
    assert decision.target_team == captured["target_team"]
    assert decision.reasons[-1] == "window=5_15"
    assert [s for s in result.skipped if s.reason == DISABLED] == []


@pytest.mark.parametrize("row", ROWS, ids=["dire_underdog", "radiant_underdog", "late_conflict_b"])
def test_everything_but_the_window_is_identical_to_the_env_on_run(row):
    for ctx in (_ctx(row), _ctx(row, kills30_radiant=0.9, kills30_dire=0.9)):
        off = md.evaluate(ctx, _cfg())
        on = md.evaluate(ctx, _cfg("1"))
        assert off.decisions == [d for d in on.decisions if d.market != "kills_window"]
        # skips: the only difference is the new window skip
        assert [s for s in off.skipped if s.reason != DISABLED] == on.skipped


def test_kills_total_exists_and_is_identical_when_the_underdog_total_fires():
    # Synthetic input on a captured tick: kills30 for both sides set to 0.9 so the
    # E-281 gate lets the underdog kills_total through.
    ctx = _ctx(ROW_TOTAL, kills30_radiant=0.9, kills30_dire=0.9)
    off = md.evaluate(ctx, _cfg())
    on = md.evaluate(ctx, _cfg("1"))
    assert [d.rule for d in _totals(on)] == ["kills_underdog_total"]
    assert _totals(off) == _totals(on)


def test_late_conflict_b_keeps_its_output_when_the_early_side_is_the_underdog():
    ctx = _ctx(ROW_LATE_CONFLICT)
    cfg = _cfg()
    conflict = md._detect_late_conflict(ctx, cfg)
    assert conflict is not None and conflict.subcase == "b"
    assert conflict.early_side == ROW_LATE_CONFLICT["underdog_side"]
    result = md.evaluate(ctx, cfg)
    # No late-conflict kills_window is created in the underdog's place (it was
    # pre-empted by the underdog decision before the switch-off).
    assert _windows(result) == []
    assert [s.reason for s in result.skipped if s.market == "kills_window"] == [DISABLED]


def test_panel_bet_is_not_pre_empted_when_it_backs_the_other_side():
    row = ROW_PANEL
    underdog = row["underdog_side"]
    other = "Radiant" if underdog == "Dire" else "Dire"
    panel = md.ModelVerdict(side=other, confidence=0.65)  # not in the capture
    ctx = _ctx(row, panel_w_5_15=panel)

    off = md.evaluate(ctx, _cfg())
    (window,) = _windows(off)
    assert window.rule == "kills_panel_window"
    assert window.target_side == other
    # the underdog side's skip stays in the journal next to the panel bet
    assert [s.side for s in off.skipped if s.reason == DISABLED] == [underdog]

    on = md.evaluate(ctx, _cfg("1"))
    (window_on,) = _windows(on)
    assert window_on.rule == UNDERDOG_RULE
    assert window_on.target_side == underdog
