"""Owner decision 05.10.2026 (E-342 addendum, card ingame-h9b5): one ``kills_window``
5_15 bet on the side of the panel model ``w_5_15`` when its confidence is >= 0.60;
the ML Laning rule ``kills_lane_early_window`` is OFF by default.

Inputs are real ``w_5_15`` entries of the serv1 panel journal, captured 05.10.2026
19:11 MSK (``fixtures/ml_panel_w_5_15_rows_20261005.jsonl``, see its README). They are
driven through the production path (prod runs PREMATCH_ML_ENABLED=0, so there is no
prematch index): real ``win_model_veto.win_prediction_ex`` -> ``_off_auxiliary_panels``
-> the card-attachment tail of ``functions.synergy_and_counterpick`` (executed from its
own source) -> the DETAILS_KEY blocks -> real ``_ml_dispatch_extract_index_details`` ->
``_ml_dispatch_tick`` -> ``ml_dispatch.evaluate`` -> decision log / delivered payload.
Only the panel model (``evaluate_map``), the kills30 forecaster and the draft fallback
verdicts are stubbed: none of them is under test.
"""
from __future__ import annotations

import ast
import json
from pathlib import Path
import re
import sys
import types

import pytest

from base import ml_dispatch as md

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Same ImportError-only stub as test_ml_dispatch_lane_kills.py.
try:
    import keys  # noqa: F401,E402
except ImportError:
    _test_keys = types.ModuleType("keys")
    _test_keys.api_to_proxy = {}
    _test_keys.BOOKMAKER_PROXY_URL = None
    _test_keys.BOOKMAKER_PROXY_POOL = []
    _test_keys.DLTV_PROXY_POOL = []
    sys.modules["keys"] = _test_keys

import cyberscore_try as C  # noqa: E402
import ml_panel  # noqa: E402
import win_model_veto  # noqa: E402
from base import laning_serving  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"
ROWS = [json.loads(line) for line in
        (FIXTURES / "ml_panel_w_5_15_rows_20261005.jsonl").read_text().splitlines()]
LANE_CAPTURES = {
    row["case"]: row for row in (
        json.loads(line) for line in
        (FIXTURES / "ml_dispatch_lane_kills_20260926.jsonl").read_text().splitlines())
}

# name -> (index of the captured row, expected side, expected confidence)
B_DIRE = ROWS[5]       # B_kv3, Dire 0.724205, ok True
B_RADIANT = ROWS[2]    # B_kv3, Radiant 0.652443, ok True
B_LOW = ROWS[3]        # B_kv3, Radiant 0.569136 (< 0.60)
B_NOT_OK = ROWS[4]     # B_kv3, Radiant 0.629512, ok False (B's own flag is conf >= 0.65)
A_BLOCKED = ROWS[1]    # model A, Dire 0.785025, ok False, blocked "драфт против"
A_PLAIN = ROWS[0]      # model A, Radiant 0.632836

PANEL_ENVS = (
    "ML_DISPATCH_PANEL_KILLS", "ML_DISPATCH_PANEL_KILLS_MIN_CONF",
    "ML_DISPATCH_PANEL_KILLS_WINDOWS", "ML_DISPATCH_LANE_KILLS",
    "ML_DISPATCH_LANE_KILLS_WINDOWS", "ML_DISPATCH_KILLS_WINDOW_ONE_SIDE",
    "PREMATCH_ML_ENABLED", "DISPATCH_MODE",
    "ML_DISPATCH_KILLS_FLOOR", "ML_DISPATCH_KILLS_MIN_ODDS_MARGIN",
)
BLOCK_KEYS = ("early_output", "early_end_output", "mid_output", "post_lane_output")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in PANEL_ENVS:
        monkeypatch.delenv(name, raising=False)
    yield


def _entry(row):
    return next(m for m in row["models"] if m["key"] == "w_5_15")


def _verdicts(row):
    """The ``ml_panel.ModelVerdict`` list the live panel returns for this journal row."""
    out = []
    for m in row["models"]:
        out.append(ml_panel.ModelVerdict(
            key=m["key"], title=m["key"], side=m["side"], probability=m["p"],
            threshold=m["threshold"], fill=m["fill"], ok=m["ok"],
            missing=tuple(m.get("missing") or ()), raw=m.get("raw"),
            draft_share=m.get("draft_share"), parts=m.get("parts"),
            band_hit=m.get("band_hit"), band_n=m.get("band_n") or 0, odds=m.get("odds"),
            blocked=m.get("blocked"), metadata=m.get("metadata")))
    return out


def _draft(base_id):
    return {f"pos{i}": {"hero_id": base_id + i, "account_id": 1000 * base_id + i}
            for i in range(1, 6)}


RADIANT_DRAFT, DIRE_DRAFT = _draft(10), _draft(50)



def _produce_blocks(monkeypatch, verdicts_or_exc, radiant=None, dire=None):
    """The card blocks prod builds with PREMATCH_ML_ENABLED=0.

    ``win_prediction_ex`` runs for real; so does the attachment tail of
    ``functions.synergy_and_counterpick`` (exec'd from its own source, like
    test_prematch_refusal_fallback.py), so the keys that really reach the blocks are
    the keys under test.
    """
    import prematch_panel_live
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    if isinstance(verdicts_or_exc, Exception):
        def _evaluate(*a, **k):
            raise verdicts_or_exc
    else:
        def _evaluate(*a, **k):
            return list(verdicts_or_exc)
    monkeypatch.setattr(prematch_panel_live, "evaluate_map", _evaluate)
    monkeypatch.setattr(ml_panel, "append_journal", lambda *a, **k: None)
    stub = types.ModuleType("kills_transfer_serving")

    def _boom(*a, **k):
        raise RuntimeError("kills30 not under test")

    stub.forecast_probabilities = _boom
    stub.manifest_history_date = lambda: ""
    stub.render = lambda *a, **k: ""
    monkeypatch.setitem(sys.modules, "kills_transfer_serving", stub)
    monkeypatch.setattr(laning_serving, "fallback_verdicts", lambda *a, **k: {})

    tree = ast.parse((BASE_DIR / "functions.py").read_text())
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
              and n.name == "synergy_and_counterpick")
    start = next(i for i, n in enumerate(fn.body) if isinstance(n, ast.Try)
                 and any(isinstance(c, ast.Attribute) and c.attr == "win_prediction_ex"
                         for c in ast.walk(n)))
    assert isinstance(fn.body[-1], ast.Return)
    blocks = {key: {} for key in BLOCK_KEYS}
    env = dict(win_model_veto=win_model_veto,
               radiant_heroes_and_pos=radiant or RADIANT_DRAFT,
               dire_heroes_and_pos=dire or DIRE_DRAFT,
               radiant_team_name="Alpha", dire_team_name="Beta",
               match={"match_id": 9000000001}, return_dict=blocks)
    exec(compile(ast.Module(body=fn.body[start:-1], type_ignores=[]), "<card-tail>", "exec"), env)
    return blocks


def _details(blocks):
    """What the dispatch tick reads: the REAL extractor over the real blocks."""
    index, details = C._ml_dispatch_extract_index_details(
        blocks["early_output"], blocks["mid_output"], blocks["post_lane_output"])
    assert index is None
    return details


def _prod_pair(monkeypatch, row):
    details = _details(_produce_blocks(monkeypatch, _verdicts(row)))
    assert "panel_w_5_15" in details          # the key itself must travel, not only the value
    return details["panel_w_5_15"]


def _ctx(pair, game_time=100.0, already_sent=None, **extra):
    return md.Ctx(
        match_key="m", base_url="https://example/m", map_num=1, game_time=game_time,
        radiant_team="Alpha", dire_team="Beta", heroes=None,
        elo_radiant=None, elo_dire=None,
        panel_w_5_15=C._ml_dispatch_verdict_from_pair(pair),
        kills_windows_open=C._ml_dispatch_open_kills_windows(game_time),
        already_sent=set() if already_sent is None else already_sent, **extra)


def _windows(result):
    return [d for d in result.decisions if d.market == "kills_window"]


def _expect(row):
    entry = _entry(row)
    return entry["side"], entry["confidence"]


# --------------------------------------------------------------- production path

@pytest.mark.parametrize("row", [B_DIRE, B_RADIANT, B_NOT_OK, A_BLOCKED, A_PLAIN],
                         ids=["B_dire", "B_radiant", "B_ge60_ok_false", "A_blocked", "A_plain"])
def test_prod_off_path_panel_entry_becomes_one_5_15_decision(monkeypatch, row):
    pair = _prod_pair(monkeypatch, row)
    side, confidence = _expect(row)
    assert pair["side"] == side
    assert pair["confidence"] == pytest.approx(confidence)
    result = md.evaluate(_ctx(pair), md.Config.from_env())
    windows = _windows(result)
    assert len(windows) == 1
    decision = windows[0]
    assert decision.rule == "kills_panel_window"
    assert decision.target_side == side
    assert decision.target_team == ("Alpha" if side == "Radiant" else "Beta")
    assert decision.models_for == ["panel_w_5_15"] and decision.models_against == []
    assert decision.timing == "now"
    assert "window=5_15" in decision.reasons
    assert decision.expected_wr == pytest.approx(confidence)
    # E-359: informational floor 1/(conf - 0.04) from the kills probability, no WIN
    # calibration applied.
    assert decision.min_odds == round(1.0 / (confidence - 0.04), 2)
    assert decision.floor_informational is True
    assert md.evaluate(_ctx(pair), md.Config.from_env()) == result  # idempotent


def test_prod_off_path_below_threshold_is_skipped_not_bet(monkeypatch):
    pair = _prod_pair(monkeypatch, B_LOW)
    result = md.evaluate(_ctx(pair), md.Config.from_env())
    assert _windows(result) == []
    skips = [s for s in result.skipped
             if s.market == "kills_window" and s.reason == "panel_kills_low_conf"]
    assert len(skips) == 1 and skips[0].side == "Radiant"
    assert "0.569" in skips[0].detail


@pytest.mark.parametrize("case", ["empty", "raises", "no_w_5_15", "missing_hero"])
def test_failed_or_empty_panel_gives_none_on_every_return_path(monkeypatch, case):
    radiant, dire = RADIANT_DRAFT, DIRE_DRAFT
    verdicts = _verdicts(B_DIRE)
    if case == "empty":
        verdicts = []
    elif case == "raises":
        verdicts = RuntimeError("panel down")
    elif case == "no_w_5_15":
        verdicts = [v for v in verdicts if v.key != "w_5_15"]
    else:
        radiant = {**RADIANT_DRAFT, "pos3": {"hero_id": 0, "account_id": 0}}
    details = _details(_produce_blocks(monkeypatch, verdicts, radiant, dire))
    assert "panel_w_5_15" in details and details["panel_w_5_15"] is None
    assert md.evaluate(_ctx(details["panel_w_5_15"]), md.Config.from_env()).decisions == []


def test_producer_exception_fallback_carries_the_key(monkeypatch):
    # win_prediction_ex's own except-fallback (broken positions) must carry the key too.
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    monkeypatch.setattr(win_model_veto, "_off_auxiliary_panels",
                        lambda *a, **k: (_ for _ in ()).throw(ValueError("broken")))
    index, source, details = win_model_veto.win_prediction_ex(
        RADIANT_DRAFT, DIRE_DRAFT, "Alpha", "Beta", {})
    assert (index, source) == (None, None)
    assert "panel_w_5_15" in details and details["panel_w_5_15"] is None


def test_two_live_maps_keep_their_own_panel_verdict(monkeypatch):
    first = _details(_produce_blocks(monkeypatch, _verdicts(B_DIRE)))
    second = _details(_produce_blocks(monkeypatch, _verdicts(B_RADIANT),
                                      _draft(70), _draft(90)))
    assert (first["panel_w_5_15"]["side"], second["panel_w_5_15"]["side"]) == (
        "Dire", "Radiant")
    # The first map's snapshot is card-owned: the second call did not touch it.
    assert first["panel_w_5_15"]["side"] == "Dire"


def test_index_keyed_record_follows_the_kills30_pattern():
    entry = win_model_veto._panel_w_5_15_entry(_verdicts(B_DIRE))
    assert entry == {"side": "Dire", "confidence": pytest.approx(0.724205),
                     "p": pytest.approx(0.275795), "model": "B_kv3"}
    saved = dict(win_model_veto._LAST_FILL)
    try:
        win_model_veto._LAST_FILL.update(index=12.345, panel_w_5_15=entry)
        win_model_veto._remember_fill()
        win_model_veto._LAST_FILL.update(index=-3.21, panel_w_5_15=None)
        win_model_veto._remember_fill()
        assert win_model_veto.last_panel_w_5_15(12.345)["side"] == "Dire"
        assert win_model_veto.last_panel_w_5_15(-3.21) is None      # failed panel: no bet
        assert win_model_veto.last_panel_w_5_15(55.5) is None       # unknown index
    finally:
        win_model_veto._LAST_FILL.clear()
        win_model_veto._LAST_FILL.update(saved)
        win_model_veto._FILL_HISTORY.pop(12.345, None)
        win_model_veto._FILL_HISTORY.pop(-3.21, None)
    pair = win_model_veto._panel_w_5_15_entry(_verdicts(B_RADIANT))
    result = md.evaluate(_ctx(pair), md.Config.from_env())
    assert [(d.rule, d.target_side) for d in _windows(result)] == [
        ("kills_panel_window", "Radiant")]


def test_non_sided_or_missing_w_5_15_gives_none():
    assert win_model_veto._panel_w_5_15_entry([]) is None
    assert win_model_veto._panel_w_5_15_entry(None) is None
    others = [v for v in _verdicts(B_DIRE) if v.key != "w_5_15"]
    assert win_model_veto._panel_w_5_15_entry(others) is None


# ------------------------------------------------------------------ rule behaviour

def _pair(row):
    entry = _entry(row)
    return {"side": entry["side"], "confidence": entry["confidence"]}


def test_window_closed_gives_no_decision_and_a_skip():
    result = md.evaluate(_ctx(_pair(B_DIRE), game_time=200.0), md.Config.from_env())
    assert "5_15" not in C._ml_dispatch_open_kills_windows(200.0)
    assert _windows(result) == []
    assert any(s.market == "kills_window" and s.side == "Dire"
               and s.reason == "panel_kills_window_closed" for s in result.skipped)


@pytest.mark.parametrize("sent_side,reason", [
    ("Dire", md.REASON_DEDUP), ("Radiant", "kills_window_sent_other_side"),
])
def test_ledger_dedup_both_sides(sent_side, reason):
    ctx = _ctx(_pair(B_DIRE))
    ctx.already_sent.add(md._dedup_key(ctx, "kills_window", sent_side))
    result = md.evaluate(ctx, md.Config.from_env())
    assert _windows(result) == []
    assert any(s.market == "kills_window" and s.side == "Dire" and s.reason == reason
               for s in result.skipped)


def test_other_side_check_ignores_the_one_side_rollback_env(monkeypatch):
    monkeypatch.setenv("ML_DISPATCH_KILLS_WINDOW_ONE_SIDE", "0")
    ctx = _ctx(_pair(B_DIRE))
    ctx.already_sent.add(md._dedup_key(ctx, "kills_window", "Radiant"))
    result = md.evaluate(ctx, md.Config.from_env())
    assert _windows(result) == []
    assert any(s.reason == "kills_window_sent_other_side" for s in result.skipped)


@pytest.mark.parametrize("value", ["0", "false", "off"])
def test_panel_toggle_off_gives_no_decision(monkeypatch, value):
    monkeypatch.setenv("ML_DISPATCH_PANEL_KILLS", value)
    result = md.evaluate(_ctx(_pair(B_DIRE)), md.Config.from_env())
    assert _windows(result) == []
    assert not any(s.reason.startswith("panel_kills") for s in result.skipped)


def test_min_conf_and_windows_envs(monkeypatch):
    monkeypatch.setenv("ML_DISPATCH_PANEL_KILLS_MIN_CONF", "0.70")
    assert _windows(md.evaluate(_ctx(_pair(B_RADIANT)), md.Config.from_env())) == []
    assert len(_windows(md.evaluate(_ctx(_pair(B_DIRE)), md.Config.from_env()))) == 1
    monkeypatch.delenv("ML_DISPATCH_PANEL_KILLS_MIN_CONF")
    monkeypatch.setenv("ML_DISPATCH_PANEL_KILLS_WINDOWS", "10_20")
    windows = _windows(md.evaluate(_ctx(_pair(B_DIRE)), md.Config.from_env()))
    assert len(windows) == 1 and "window=10_20" in windows[0].reasons


def test_exactly_at_threshold_fires():
    result = md.evaluate(_ctx({"side": "Dire", "confidence": 0.60}), md.Config.from_env())
    assert len(_windows(result)) == 1


def test_defaults_of_from_env():
    cfg = md.Config.from_env()
    assert (cfg.panel_kills_enabled, cfg.panel_kills_min_conf, cfg.panel_kills_windows) == (
        True, 0.60, ("5_15",))
    assert cfg.lane_kills_enabled is False


# --------------------------------------------- the ML Laning rule is off by default

def _lane_ctx(case):
    record = LANE_CAPTURES[case]["record"]
    verdicts = record["verdicts"]

    def verdict(name):
        value = verdicts.get(name)
        return md.ModelVerdict(**value) if value else None

    kills30 = verdicts.get("kills30") or {}
    return md.Ctx(
        match_key=record["match_key"], base_url=record["base_url"],
        map_num=record["map_num"], game_time=record["game_time"],
        radiant_team=record["teams"]["radiant"], dire_team=record["teams"]["dire"],
        heroes=record["heroes"], elo_radiant=record["elo_r"], elo_dire=record["elo_d"],
        early_nw=verdict("early_nw"), early_win=verdict("early_win"),
        late=verdict("late"), all=verdict("all"), lane=verdict("lane"),
        kills_windows_open=C._ml_dispatch_open_kills_windows(record["game_time"]),
        kills30_radiant=kills30.get("radiant"), kills30_dire=kills30.get("dire"),
        already_sent=set())


def test_lane_rule_does_not_fire_under_default_env():
    ctx = _lane_ctx("fires_both_early_star")
    assert ctx.lane is not None and ctx.panel_w_5_15 is None
    result = md.evaluate(ctx, md.Config.from_env())
    assert not any(d.rule == "kills_lane_early_window" for d in result.decisions)
    assert _windows(result) == []


def test_lane_rule_fires_again_with_rollback_env(monkeypatch):
    monkeypatch.setenv("ML_DISPATCH_LANE_KILLS", "1")
    result = md.evaluate(_lane_ctx("fires_both_early_star"), md.Config.from_env())
    assert [d.rule for d in _windows(result)] == ["kills_lane_early_window"]


def test_panel_wins_over_lane_when_both_enabled(monkeypatch):
    monkeypatch.setenv("ML_DISPATCH_LANE_KILLS", "1")
    ctx = _lane_ctx("fires_both_early_star")
    opposite = "Dire" if ctx.lane.side == "Radiant" else "Radiant"
    ctx.panel_w_5_15 = md.ModelVerdict(opposite, 0.66)
    windows = _windows(md.evaluate(ctx, md.Config.from_env()))
    assert [(d.rule, d.target_side) for d in windows] == [("kills_panel_window", opposite)]



# ------------------------------------------------------- the whole tick, end to end

class _Ledger:
    def __init__(self):
        self.keys = set()

    def as_set(self):
        return set(self.keys)

    def add(self, key):
        self.keys.add(tuple(key))

    def save(self):
        pass


def _drive_tick(monkeypatch, blocks, *, tier1=True, game_time=100.0,
                match_key="https://example/m", captured=None, pin_underdog_block=True):
    """``_ml_dispatch_tick`` the way prod runs it: prematch ML off (no index), the
    real card blocks, the real details extractor.

    ``pin_underdog_block`` (captured ticks only): run under the rollback env
    ``ML_DISPATCH_WIN_UNDERDOG_BLOCK=1`` so the kills assertions are not mixed with the
    ELO-underdog WIN the 09.10.2026 default now lets through; ``False`` = production default.

    ``captured`` (a journal row of ``ml_dispatch_panel_tier1_ticks_20261006.jsonl``) replaces
    the card blocks by the verdicts, ELO, team names/ids and game_time of that real tick:
    the details extractor and the laning verdicts are then stubbed with its values."""
    delivered, logged = [], []
    names, ids = ("Alpha", "Beta"), (1, 2)
    elo = (None, None)
    monkeypatch.setenv("DISPATCH_MODE", "ml")
    if captured is None:
        monkeypatch.setattr(laning_serving, "verdicts",
                            lambda *a, **k: {"all": None, "lane": None})
    else:
        # These 06.10 ticks predate the 09.10.2026 lift of the win-underdog ban (card
        # ingame-8ht2): with the new default their ELO-underdog Radiant win would now be
        # delivered next to the kills rule under test. Run them under the rollback env so
        # the assertions stay on the kills path.
        if pin_underdog_block:
            monkeypatch.setenv("ML_DISPATCH_WIN_UNDERDOG_BLOCK", "1")
        v = captured["verdicts"]
        details = {k: v[k] for k in ("early_nw", "early_win", "late", "panel_w_5_15", "kills30")}
        monkeypatch.setattr(C, "_ml_dispatch_extract_index_details",
                            lambda *a, **k: (None, dict(details)))
        monkeypatch.setattr(laning_serving, "verdicts",
                            lambda *a, **k: {"all": v["all"], "lane": v["lane"]})
        names = (captured["teams"]["radiant"], captured["teams"]["dire"])
        ids = _captured_team_ids(captured)
        elo = (captured["elo_r"], captured["elo_d"])
        game_time = captured["game_time"]
        match_key = captured["match_key"]
        blocks = {"early_output": {}, "mid_output": {}, "post_lane_output": {}}
    monkeypatch.setattr(C, "_team_elo_base_rating_for_side",
                        lambda meta, side: elo[0] if side == "radiant" else elo[1])
    monkeypatch.setattr(C, "_bookmaker_infer_map_num", lambda *a, **k: 1)
    monkeypatch.setattr(C, "_match_has_tier1_team", lambda *a: tier1)
    monkeypatch.setattr(C, "_ml_dispatch_sent_ledger", lambda: _Ledger())
    monkeypatch.setattr(C, "_ml_dispatch_record_decisions",
                        lambda row, *, dedup_view: logged.append(row))
    monkeypatch.setattr(C, "_deliver_and_persist_signal",
                        lambda *a, **k: delivered.append((a, k)) or True)
    C._ml_dispatch_tick(
        match_key=match_key, radiant_team_name=names[0], dire_team_name=names[1],
        live_league={}, top="", mid="", bot="", protracker_payload=None,
        team_elo_block="", team_elo_meta={}, game_time_seconds=game_time, radiant_lead=0,
        early_output=blocks["early_output"], mid_output=blocks["mid_output"],
        all_output=blocks["post_lane_output"],
        radiant_heroes_and_pos=RADIANT_DRAFT, dire_heroes_and_pos=DIRE_DRAFT,
        full_message_text="СТАВКА НА x\n🤖 ML:\n  окно 5-15: Dire 72%",
        radiant_team_id=ids[0], dire_team_id=ids[1])
    return delivered, logged


TIER1_TICKS = [json.loads(line) for line in
               (FIXTURES / "ml_dispatch_panel_tier1_ticks_20261006.jsonl").read_text().splitlines()]


def _captured_team_ids(row):
    """The journal row has no id field: they are in the Tier-1 skip's detail string."""
    detail = next(s["detail"] for s in row["skipped"] if s["reason"] == "kills_requires_tier1_team")
    return tuple(int(x) for x in re.findall(r"team_id=(\d+)", detail))


def _window_calls(delivered):
    return [(a, k) for a, k in delivered
            if k["stake_multiplier_context"]["ml_market"] == "kills_window"]


def test_tick_delivers_panel_bet_and_logs_the_verdict(monkeypatch):
    blocks = _produce_blocks(monkeypatch, _verdicts(B_DIRE))
    delivered, logged = _drive_tick(monkeypatch, blocks)
    window_calls = _window_calls(delivered)
    assert len(window_calls) == 1
    args, kwargs = window_calls[0]
    context = kwargs["stake_multiplier_context"]
    assert (context["origin"], context["ml_rule"], context["target_side"]) == (
        "ml_dispatch", "kills_panel_window", "dire")
    lines = args[1].splitlines()
    assert lines[0] == C._format_signal_header(
        stake_team_name="Beta", stake_multiplier=None,
        special_header_mode="early_kills", kills_window_label="5_15")
    assert any("по панели ML: Beta (Dire) 72%" in line for line in lines[1:3])
    row = logged[0]
    assert row["verdicts"]["panel_w_5_15"] == {"side": "Dire", "confidence": 0.7242}
    assert [(d["market"], d["rule"], d["target_side"]) for d in row["decisions"]] == [
        ("kills_window", "kills_panel_window", "Dire")]
    assert row["delivered"][-1]["status"] == "delivered"


def test_tick_two_maps_each_bet_on_their_own_panel_side(monkeypatch):
    dire_blocks = _produce_blocks(monkeypatch, _verdicts(B_DIRE))
    radiant_blocks = _produce_blocks(monkeypatch, _verdicts(B_RADIANT),
                                     _draft(70), _draft(90))
    # Both maps are live at the same time: tick map 1 AFTER map 2's panel ran.
    delivered_1, logged_1 = _drive_tick(monkeypatch, dire_blocks, match_key="https://example/m1")
    delivered_2, logged_2 = _drive_tick(monkeypatch, radiant_blocks, match_key="https://example/m2")
    sides = [[k["stake_multiplier_context"]["target_side"] for a, k in _window_calls(d)]
             for d in (delivered_1, delivered_2)]
    assert sides == [["dire"], ["radiant"]]
    assert logged_1[0]["verdicts"]["panel_w_5_15"]["side"] == "Dire"
    assert logged_2[0]["verdicts"]["panel_w_5_15"]["side"] == "Radiant"


@pytest.mark.parametrize("case", ["empty", "raises"])
def test_tick_failed_panel_sends_nothing_and_logs_none(monkeypatch, case):
    verdicts = [] if case == "empty" else RuntimeError("panel down")
    blocks = _produce_blocks(monkeypatch, verdicts)
    delivered, logged = _drive_tick(monkeypatch, blocks)
    assert _window_calls(delivered) == []
    assert logged[0]["verdicts"]["panel_w_5_15"] is None


def test_tick_below_threshold_sends_nothing_and_logs_the_skip(monkeypatch):
    blocks = _produce_blocks(monkeypatch, _verdicts(B_LOW))
    delivered, logged = _drive_tick(monkeypatch, blocks)
    assert _window_calls(delivered) == []
    assert any(s["reason"] == "panel_kills_low_conf" for s in logged[0]["skipped"])
    assert logged[0]["verdicts"]["panel_w_5_15"] == {"side": "Radiant", "confidence": 0.5691}


# ---- owner decision 06.10.2026 ("Снять для панели", card ingame-h9b5): the Tier-1 kills
# gate no longer drops the panel rule unless ML_DISPATCH_PANEL_KILLS_REQUIRE_TIER1=1.
# The ticks are the two real serv1 journal rows of non-Tier-1 matches that the gate dropped.

def _tier1_skips(logged):
    return [s for s in logged[0]["skipped"] if s["reason"] == "kills_requires_tier1_team"]


@pytest.mark.parametrize("row", TIER1_TICKS, ids=["DIREBORN_Xipto", "CloudDawning_Yangon"])
def test_tick_non_tier1_panel_bet_is_delivered_by_default(monkeypatch, row):
    assert C.PANEL_KILLS_REQUIRE_TIER1 is False          # the shipped default
    delivered, logged = _drive_tick(monkeypatch, None, tier1=False, captured=row)
    window_calls = _window_calls(delivered)
    assert len(window_calls) == 1
    args, kwargs = window_calls[0]
    context = kwargs["stake_multiplier_context"]
    assert (context["origin"], context["ml_rule"], context["target_side"]) == (
        "ml_dispatch", "kills_panel_window", "dire")
    assert args[1].splitlines()[0] == C._format_signal_header(
        stake_team_name=row["teams"]["dire"], stake_multiplier=None,
        special_header_mode="early_kills", kills_window_label="5_15")
    entry = logged[0]
    assert [(d["market"], d["rule"], d["target_side"]) for d in entry["decisions"]] == [
        ("kills_window", "kills_panel_window", "Dire")]
    assert entry["verdicts"]["panel_w_5_15"] == row["verdicts"]["panel_w_5_15"]
    assert entry["delivered"][-1]["status"] == "delivered"
    assert _tier1_skips(logged) == []


@pytest.mark.parametrize("row", TIER1_TICKS, ids=["DIREBORN_Xipto", "CloudDawning_Yangon"])
def test_tick_rollback_env_applies_the_tier1_gate_to_the_panel_rule(monkeypatch, row):
    monkeypatch.setattr(C, "PANEL_KILLS_REQUIRE_TIER1", True)
    delivered, logged = _drive_tick(monkeypatch, None, tier1=False, captured=row)
    assert delivered == []
    assert [(s["market"], s["side"]) for s in _tier1_skips(logged)] == [("kills_window", "Dire")]
    assert logged[0]["decisions"] == []


def test_tick_synthetic_non_tier1_panel_bet_default_and_rollback(monkeypatch):
    blocks = _produce_blocks(monkeypatch, _verdicts(B_DIRE))
    delivered, logged = _drive_tick(monkeypatch, blocks, tier1=False)
    assert len(_window_calls(delivered)) == 1 and _tier1_skips(logged) == []
    monkeypatch.setattr(C, "PANEL_KILLS_REQUIRE_TIER1", True)
    delivered, logged = _drive_tick(monkeypatch, blocks, tier1=False)
    assert delivered == [] and len(_tier1_skips(logged)) == 1


def test_tick_production_default_delivers_the_underdog_win_next_to_the_panel_bet(monkeypatch):
    """09.10.2026 (ingame-8ht2): under the production default (no BLOCK=1 pin) the captured
    DIREBORN_Xipto tick delivers the kills 5-15 Dire panel bet unchanged AND the WIN on the
    ELO-underdog Radiant side; BLOCK=1 (rollback) delivers only the kills bet."""
    row = TIER1_TICKS[0]
    monkeypatch.delenv("ML_DISPATCH_WIN_UNDERDOG_BLOCK", raising=False)
    delivered, logged = _drive_tick(monkeypatch, None, tier1=False, captured=row,
                                    pin_underdog_block=False)
    markets = sorted(k["stake_multiplier_context"]["ml_market"] for _a, k in delivered)
    assert markets == ["kills_window", "win"]
    (kw_args, kw_kwargs), = _window_calls(delivered)
    assert kw_kwargs["stake_multiplier_context"]["ml_rule"] == "kills_panel_window"
    assert kw_kwargs["stake_multiplier_context"]["target_side"] == "dire"
    assert kw_args[1].splitlines()[0] == C._format_signal_header(
        stake_team_name=row["teams"]["dire"], stake_multiplier=None,
        special_header_mode="early_kills", kills_window_label="5_15")
    (win_args, win_kwargs), = [(a, k) for a, k in delivered
                               if k["stake_multiplier_context"]["ml_market"] == "win"]
    context = win_kwargs["stake_multiplier_context"]
    assert (context["origin"], context["ml_rule"], context["target_side"]) == (
        "ml_dispatch", "win_single_model_confirm", "radiant")
    assert any(line.startswith("Ставить от кэфа") for line in win_args[1].splitlines())
    assert [(d["market"], d["rule"], d["target_side"]) for d in logged[0]["decisions"]] == [
        ("win", "win_single_model_confirm", "Radiant"),
        ("kills_window", "kills_panel_window", "Dire")]
    assert not any(s["reason"] == "win_against_elo_blocked" for s in logged[0]["skipped"])
    # rollback: the same tick loses the win and keeps the kills bet
    delivered, logged = _drive_tick(monkeypatch, None, tier1=False, captured=row)
    assert [k["stake_multiplier_context"]["ml_market"] for _a, k in delivered] == ["kills_window"]
    assert any(s["reason"] == "win_against_elo_blocked" for s in logged[0]["skipped"])


def test_tick_exemption_is_rule_specific_the_underdog_window_stays_gated(monkeypatch):
    """Same non-Tier-1 tick with the panel exemption ON (the default) and the underdog rule
    switched on: ``evaluate`` then emits the underdog Radiant window instead of the panel's
    Dire one, and the Tier-1 gate still drops it - the exemption is not market-wide."""
    row = TIER1_TICKS[0]
    monkeypatch.setenv("ML_DISPATCH_UNDERDOG_KILLS_WINDOW", "1")
    assert C.PANEL_KILLS_REQUIRE_TIER1 is False
    # Control: with a Tier-1 team the underdog rule really emits its Radiant window.
    delivered, logged = _drive_tick(monkeypatch, None, tier1=True, captured=row)
    assert [(d["rule"], d["target_side"]) for d in logged[0]["decisions"]] == [
        ("kills_underdog_early_window", "Radiant")]
    assert len(_window_calls(delivered)) == 1
    # The non-Tier-1 tick: dropped by the gate, nothing delivered.
    delivered, logged = _drive_tick(monkeypatch, None, tier1=False, captured=row)
    assert delivered == []
    assert [(s["market"], s["side"]) for s in _tier1_skips(logged)] == [("kills_window", "Radiant")]
    assert logged[0]["decisions"] == []


def test_panel_tier1_flag_is_an_env_flag_defaulting_to_off():
    tree = ast.parse((BASE_DIR / "cyberscore_try.py").read_text())
    assign = next(n for n in tree.body if isinstance(n, ast.Assign)
                  and any(isinstance(t, ast.Name) and t.id == "PANEL_KILLS_REQUIRE_TIER1"
                          for t in n.targets))
    call = assign.value
    assert isinstance(call, ast.Call) and call.func.id == "_env_flag"
    assert [a.value for a in call.args] == ["ML_DISPATCH_PANEL_KILLS_REQUIRE_TIER1", "0"]
    # the deploy check: the startup config line prints the flag
    assert "panel_tier1=" in (BASE_DIR / "cyberscore_try.py").read_text()


def test_tick_closed_window_logs_the_skip(monkeypatch):
    blocks = _produce_blocks(monkeypatch, _verdicts(B_DIRE))
    delivered, logged = _drive_tick(monkeypatch, blocks, game_time=200.0)
    assert delivered == []
    assert any(s["reason"] == "panel_kills_window_closed" for s in logged[0]["skipped"])


# ------------------------------------------- review round 1: range check, index collision

def _w515(probability, confidence, side="Dire"):
    return types.SimpleNamespace(key="w_5_15", side=side, probability=probability,
                                 confidence=confidence, metadata={"model": "B_kv3"})


@pytest.mark.parametrize("probability,confidence", [
    (1.2, 1.2), (-0.1, 0.9), (0.9, 1.2), (0.3, 0.4), (0.7, 0.4),
    (float("nan"), 0.7), (0.3, float("inf")),
], ids=["p_and_conf_gt_1", "p_below_0", "conf_gt_1", "conf_below_half", "conf_below_half_p_hi",
        "p_nan", "conf_inf"])
def test_panel_entry_refuses_out_of_range_verdict(probability, confidence):
    assert win_model_veto._panel_w_5_15_entry([_w515(probability, confidence)]) is None


@pytest.mark.parametrize("probability,confidence", [(0.275795, 0.724205), (0.5, 0.5), (1.0, 1.0)])
def test_panel_entry_keeps_in_range_verdict(probability, confidence):
    entry = win_model_veto._panel_w_5_15_entry([_w515(probability, confidence)])
    assert entry is not None and entry["confidence"] == confidence


@pytest.mark.parametrize("confidence", [1.2, float("nan"), float("inf")])
def test_evaluate_fails_closed_on_out_of_range_confidence(confidence):
    result = md.evaluate(_ctx({"side": "Dire", "confidence": confidence}), md.Config.from_env())
    assert _windows(result) == []
    bad = [s for s in result.skipped if s.reason == "panel_kills_bad_verdict"]
    assert len(bad) == 1 and bad[0].market == "kills_window" and bad[0].side == "Dire"
    assert md.REASON_PANEL_KILLS_BAD_VERDICT == "panel_kills_bad_verdict"


def test_evaluate_confidence_exactly_one_still_fires():
    result = md.evaluate(_ctx({"side": "Dire", "confidence": 1.0}), md.Config.from_env())
    assert [d.rule for d in _windows(result)] == ["kills_panel_window"]


def test_on_path_snapshot_carries_the_panel_key():
    # PREMATCH_ML_ENABLED=1: win_prediction_ex returns last_prediction_details(index),
    # a deepcopy of the _LAST_FILL record that win_model_veto.py:1401 filled.
    saved = dict(win_model_veto._LAST_FILL)
    try:
        entry = win_model_veto._panel_w_5_15_entry(_verdicts(B_DIRE))
        win_model_veto._LAST_FILL.update(index=21.5, panel_w_5_15=entry)
        win_model_veto._remember_fill()
        snapshot = win_model_veto.last_prediction_details(21.5)
        assert "panel_w_5_15" in snapshot and snapshot["panel_w_5_15"]["side"] == "Dire"
    finally:
        win_model_veto._LAST_FILL.clear()
        win_model_veto._LAST_FILL.update(saved)
        win_model_veto._FILL_HISTORY.pop(21.5, None)


def _on_path_blocks(index, details):
    block = {win_model_veto.INDEX_KEY: index, win_model_veto.SOURCE_KEY: "prematch",
             win_model_veto.DETAILS_KEY: details}
    return {key: dict(block) for key in BLOCK_KEYS}


def test_tick_on_path_same_index_failed_panel_does_not_read_another_maps_verdict(monkeypatch):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "1")
    saved = dict(win_model_veto._LAST_FILL)
    try:
        # Map B (same rounded index) ran its panel later and left a Dire 0.72 verdict
        # in the global per-index history.
        win_model_veto._LAST_FILL.update(
            index=33.25, panel_w_5_15=win_model_veto._panel_w_5_15_entry(_verdicts(B_DIRE)))
        win_model_veto._remember_fill()
        # Map A: card-owned snapshot with a FAILED panel (None) under the same index.
        blocks = _on_path_blocks(33.25, {"index": 33.25, "panel_w_5_15": None})
        delivered, logged = _drive_tick(monkeypatch, blocks)
        assert _window_calls(delivered) == []
        assert logged[0]["verdicts"]["panel_w_5_15"] is None
        # Control: legacy snapshot WITHOUT the key still falls back to the index record.
        blocks = _on_path_blocks(33.25, {"index": 33.25})
        delivered, logged = _drive_tick(monkeypatch, blocks)
        assert logged[0]["verdicts"]["panel_w_5_15"]["side"] == "Dire"
    finally:
        win_model_veto._LAST_FILL.clear()
        win_model_veto._LAST_FILL.update(saved)
        win_model_veto._FILL_HISTORY.pop(33.25, None)


# ---------------------------------------------------------------- E-359 informational floor
# Card ingame-iu1v: the outgoing Telegram text of a kills_panel_window bet carries
# "Ставить от кэфа X" with X = round(1/(conf - 0.04), 2), right under the header like
# the WIN message. Informational only: no block, no price check for kills.

def _floor_text(args):
    return args[1]


def test_tick_panel_bet_text_carries_the_floor_line_under_the_header(monkeypatch):
    blocks = _produce_blocks(monkeypatch, _verdicts(B_DIRE))
    delivered, logged = _drive_tick(monkeypatch, blocks)
    (args, kwargs), = _window_calls(delivered)
    lines = _floor_text(args).splitlines()
    expected = round(1.0 / (0.724205 - 0.04), 2)          # B_DIRE confidence 0.724205
    assert expected == 1.46
    assert lines[1] == "Ставить от кэфа 1.46"
    assert "по панели ML: Beta (Dire) 72%" in lines[2]
    calibration = kwargs["stake_multiplier_context"]["calibration"]
    assert calibration["min_odds"] == pytest.approx(expected)
    assert logged[0]["decisions"][0]["min_odds"] == pytest.approx(expected)


@pytest.mark.parametrize("row,floor", zip(TIER1_TICKS, ["1.78", "1.70"]),
                         ids=["DIREBORN_Xipto", "CloudDawning_Yangon"])
def test_captured_tick_text_carries_the_floor_line(monkeypatch, row, floor):
    conf = row["verdicts"]["panel_w_5_15"]["confidence"]
    assert f"{1.0 / (conf - 0.04):.2f}" == floor
    delivered, _ = _drive_tick(monkeypatch, None, tier1=False, captured=row)
    (args, _kwargs), = _window_calls(delivered)
    lines = args[1].splitlines()
    assert lines[1] == f"Ставить от кэфа {floor}"
    assert "по панели ML" in lines[2]


def test_floor_off_env_gives_todays_text_and_floor(monkeypatch):
    monkeypatch.setenv("ML_DISPATCH_KILLS_FLOOR", "0")
    blocks = _produce_blocks(monkeypatch, _verdicts(B_DIRE))
    delivered, logged = _drive_tick(monkeypatch, blocks)
    (args, kwargs), = _window_calls(delivered)
    assert "Ставить от кэфа" not in args[1]
    assert "по панели ML: Beta (Dire) 72%" in args[1].splitlines()[1]
    # exactly today's decision: the unshown 0.12-margin floor is unchanged.
    assert kwargs["stake_multiplier_context"]["calibration"]["min_odds"] == \
        md._min_odds(0.724205, md.Config.from_env())


def test_margin_env_moves_the_floor(monkeypatch):
    monkeypatch.setenv("ML_DISPATCH_KILLS_MIN_ODDS_MARGIN", "0.10")
    blocks = _produce_blocks(monkeypatch, _verdicts(B_DIRE))
    delivered, _ = _drive_tick(monkeypatch, blocks)
    (args, _kwargs), = _window_calls(delivered)
    assert args[1].splitlines()[1] == f"Ставить от кэфа {1.0 / (0.724205 - 0.10):.2f}"


def test_no_floor_when_margin_swallows_the_confidence(monkeypatch):
    monkeypatch.setenv("ML_DISPATCH_KILLS_MIN_ODDS_MARGIN", "0.80")
    pair = _prod_pair(monkeypatch, B_DIRE)
    decision, = _windows(md.evaluate(_ctx(pair), md.Config.from_env()))
    assert decision.floor_informational is False
    assert decision.min_odds == md._min_odds(0.724205, md.Config.from_env())


def test_kills_floor_is_never_a_delivery_block(monkeypatch):
    """Even with a Winline price far below the printed floor, no ml_min_odds_below_floor."""
    blocks = _produce_blocks(monkeypatch, _verdicts(B_DIRE))
    delivered, _ = _drive_tick(monkeypatch, blocks)
    (args, kwargs), = _window_calls(delivered)
    monkeypatch.setattr(C, "_ml_dispatch_fresh_winline_price", lambda *a, **k: 1.05)
    assert C._ml_dispatch_min_odds_reject_for_delivery(
        args[1], kwargs["stake_multiplier_context"], match_key="m", map_num=1) is None


def test_other_kills_window_rules_get_no_floor(monkeypatch):
    monkeypatch.setenv("ML_DISPATCH_LANE_KILLS", "1")
    decision, = _windows(md.evaluate(_lane_ctx("fires_both_early_star"), md.Config.from_env()))
    assert decision.rule == "kills_lane_early_window"
    assert decision.floor_informational is False


def test_win_floor_margin_is_unchanged_by_the_kills_margin(monkeypatch):
    monkeypatch.setenv("ML_DISPATCH_KILLS_MIN_ODDS_MARGIN", "0.01")
    cfg = md.Config.from_env()
    assert cfg.min_odds_margin == 0.12
    assert md._min_odds(0.74, cfg) == round(1.0 / (0.74 - 0.12), 2)


@pytest.mark.parametrize("raw", ["-1", "-inf", "nan", "inf", "abc", "", "1.5", "1"])
def test_unusable_margin_env_falls_back_to_the_default_margin(monkeypatch, raw):
    """Negative / non-finite / garbage / >= 1 margins use 0.04 (not 'no line')."""
    monkeypatch.setenv("ML_DISPATCH_KILLS_MIN_ODDS_MARGIN", raw)
    cfg = md.Config.from_env()
    assert cfg.kills_floor_margin == 0.04 and cfg.kills_floor_margin_defaulted is True
    blocks = _produce_blocks(monkeypatch, _verdicts(B_DIRE))
    delivered, _ = _drive_tick(monkeypatch, blocks)
    (args, _kwargs), = _window_calls(delivered)
    assert args[1].splitlines()[1] == "Ставить от кэфа 1.46"


def test_valid_margin_env_is_not_flagged_as_defaulted(monkeypatch):
    for raw, flagged in (("0", False), ("0.10", False)):
        monkeypatch.setenv("ML_DISPATCH_KILLS_MIN_ODDS_MARGIN", raw)
        cfg = md.Config.from_env()
        assert cfg.kills_floor_margin == float(raw) and cfg.kills_floor_margin_defaulted is flagged
