"""Pure-logic tests for base.ml_dispatch.evaluate (no network, no models).

Every test builds a Ctx by hand and checks either the produced Decision(s)
or the Skipped(...) reasons. See base/ml_dispatch.py module docstring for
the two flagged design decisions (support-vs-veto interaction, conflict
checked before veto) that these tests pin down.
"""
import pytest

from base import ml_dispatch as md
from base.ml_dispatch import Ctx, Config, ModelVerdict, SentLedger, evaluate


def base_ctx(**overrides):
    defaults = dict(
        match_key="m1",
        base_url="https://example.test/m1",
        map_num=1,
        game_time=100.0,
        radiant_team="Radiant Team",
        dire_team="Dire Team",
        heroes=tuple(range(1, 11)),
        elo_radiant=1500.0,
        elo_dire=1500.0,
    )
    defaults.update(overrides)
    return Ctx(**defaults)


def cfg(**overrides):
    return Config(**overrides)


# --- Underdog / favorite / equal ELO -----------------------------------

def test_underdog_side_when_diff_at_least_50():
    ctx = base_ctx(elo_radiant=1400.0, elo_dire=1450.0)
    result = evaluate(ctx, cfg())
    assert result.underdog_side == "Radiant"
    assert result.elo_diff == -50.0
    assert result.mode_hint == "underdog"


def test_diff_49_has_no_underdog_split():
    ctx = base_ctx(elo_radiant=1401.0, elo_dire=1450.0)
    result = evaluate(ctx, cfg())
    assert result.underdog_side is None
    assert result.mode_hint == "even"


def test_diff_50_exact_is_underdog():
    ctx = base_ctx(elo_radiant=1400.0, elo_dire=1450.0)
    result = evaluate(ctx, cfg())
    assert result.underdog_side == "Radiant"


def test_equal_elo_both_sides_are_win_candidates_without_no_underdog():
    ctx = base_ctx(elo_radiant=1500.0, elo_dire=1500.0,
                    late=ModelVerdict("Radiant", 0.70))
    result = evaluate(ctx, cfg())
    assert result.underdog_side is None
    win_decisions = [d for d in result.decisions if d.market == "win"]
    assert len(win_decisions) == 1
    assert win_decisions[0].target_side == "Radiant"
    # Kills are unaffected by "no underdog" only for the kills markets,
    # which is exactly what no_underdog reports:
    assert any(s.reason == md.REASON_NO_UNDERDOG for s in result.skipped)


# --- Threshold 0.59 vs 0.60 ---------------------------------------------

def test_confidence_059_does_not_back_a_side():
    ctx = base_ctx(late=ModelVerdict("Radiant", 0.59))
    result = evaluate(ctx, cfg())
    assert result.decisions == []
    assert any(s.market == "win" and s.side == "Radiant"
               and s.reason == md.REASON_BELOW_THRESHOLD for s in result.skipped)


def test_confidence_060_backs_a_side():
    ctx = base_ctx(late=ModelVerdict("Radiant", 0.60))
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"]
    assert len(win) == 1 and win[0].target_side == "Radiant"
    assert win[0].expected_wr == 0.60


# --- Veto vs conflict -----------------------------------------------------

def test_late_veto_blocks_a_lone_early_win_candidate():
    # win_models excludes late/all from *supporting* a side, but veto
    # always checks late/all regardless of that config (see module
    # docstring, design decision 1): this isolates "veto" from "conflict".
    ctx = base_ctx(
        early_win=ModelVerdict("Dire", 0.65),
        late=ModelVerdict("Radiant", 0.70),
    )
    result = evaluate(ctx, cfg(win_models=("early_win",)))
    assert result.decisions == []
    dire_skip = next(s for s in result.skipped if s.market == "win" and s.side == "Dire")
    assert dire_skip.reason == md.REASON_VETO


def test_all_veto_blocks_a_lone_early_win_candidate():
    ctx = base_ctx(
        early_win=ModelVerdict("Radiant", 0.65),
        all=ModelVerdict("Dire", 0.70),
    )
    result = evaluate(ctx, cfg(win_models=("early_win",)))
    assert result.decisions == []
    radiant_skip = next(s for s in result.skipped if s.market == "win" and s.side == "Radiant")
    assert radiant_skip.reason == md.REASON_VETO


def test_veto_wins_over_conflict_when_one_side_is_vetoed():
    # Owner's example (plan, "правка 0"): Late confirms Radiant at 0.62,
    # Early Win confirms Dire at 0.65 -- under the default win_models
    # (which counts Late as support for Radiant too) this used to be a
    # conflict, but veto is now resolved PER SIDE FIRST: Late (a veto
    # model) vetoes Dire, only Radiant survives -> bet on Radiant, not a
    # conflict (see module docstring design decision 2).
    ctx = base_ctx(
        late=ModelVerdict("Radiant", 0.62),
        early_win=ModelVerdict("Dire", 0.65),
    )
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"]
    assert len(win) == 1
    assert win[0].target_side == "Radiant"
    assert not any(s.market == "win" and s.reason == md.REASON_CONFLICT for s in result.skipped)
    dire_skip = next(s for s in result.skipped if s.market == "win" and s.side == "Dire")
    assert dire_skip.reason == md.REASON_VETO


def test_conflict_when_both_sides_survive_veto():
    # Genuine conflict: both sides have support and NEITHER is vetoed,
    # because late/all (the only veto models) never voted at all here.
    ctx = base_ctx(
        early_win=ModelVerdict("Radiant", 0.65),
        early_nw=ModelVerdict("Dire", 0.65),
    )
    result = evaluate(ctx, cfg(win_models=("early_win", "early_nw")))
    assert result.decisions == []
    conflict = [s for s in result.skipped if s.market == "win" and s.reason == md.REASON_CONFLICT]
    assert len(conflict) == 1
    assert conflict[0].side is None


# --- "Any one of the four" wins, for both F and U -------------------------

def test_owner_example_8995161253_map2_backs_dire_with_all_four_stars():
    # Shadow log on serv1, 12.09.2026 (dltv.org/matches/8995161253 map 2,
    # game_time 1597): Early NW Dire 0.7324 ★, Early Win Dire 0.6086 ★,
    # All Dire 0.6033 ★, Late Dire 0.5838 (no star), Lane Dire 0.6296.
    # Owner rule: any of the four starred -> the signal goes. Under the
    # default win_models early_nw now counts as support too, so
    # expected_wr is its 0.7324 (the max among voting models), not 0.6086.
    # Input is the captured shadow record (see the .README.md next to it);
    # the record's own "decisions" field is what the OLD default produced.
    import json
    from pathlib import Path
    fixture_path = (Path(__file__).parent / "fixtures"
                    / "ml_dispatch_shadow_8995161253_map2_tick61.json")
    rec = json.loads(fixture_path.read_text())
    assert rec["mode"] == "shadow"
    assert rec["decisions"][0]["models_for"] == ["all", "early_win"]  # old default
    verdicts = {name: (ModelVerdict(v["side"], v["confidence"]) if v else None)
                for name, v in rec["verdicts"].items()}
    assert verdicts["early_nw"].confidence >= 0.60 > verdicts["late"].confidence
    ctx = base_ctx(
        game_time=rec["game_time"], elo_radiant=rec["elo_r"], elo_dire=rec["elo_d"],
        radiant_team=rec["teams"]["radiant"], dire_team=rec["teams"]["dire"],
        **verdicts,
    )
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"]
    assert len(win) == 1
    assert win[0].target_side == "Dire"
    assert win[0].timing == "now"
    assert sorted(win[0].models_for) == ["all", "early_nw", "early_win"]
    assert win[0].expected_wr == 0.7324
    assert win[0].min_odds == round(1 / 0.7324, 2)


def test_lone_early_nw_star_backs_the_side_under_default_config():
    # Only Early NW is starred; Late/All/Early Win are present but below
    # the threshold on the SAME side (no veto). Previously early_nw was
    # not in DEFAULT_WIN_MODELS and this map produced no decision.
    ctx = base_ctx(
        game_time=900.0,
        early_nw=ModelVerdict("Radiant", 0.66),
        early_win=ModelVerdict("Radiant", 0.55),
        late=ModelVerdict("Radiant", 0.52),
        all=ModelVerdict("Radiant", 0.58),
    )
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"]
    assert len(win) == 1 and win[0].target_side == "Radiant"
    assert win[0].models_for == ["early_nw"]


def test_any_one_of_four_win_models_confirms_the_favorite():
    for model_name in ("late", "all", "early_win", "early_nw"):
        ctx = base_ctx(elo_radiant=1550.0, elo_dire=1500.0,
                        **{model_name: ModelVerdict("Radiant", 0.61)})
        result = evaluate(ctx, cfg())
        win = [d for d in result.decisions if d.market == "win"]
        assert len(win) == 1 and win[0].target_side == "Radiant", model_name


def test_any_one_of_four_win_models_confirms_the_underdog():
    for model_name in ("late", "all", "early_win", "early_nw"):
        ctx = base_ctx(elo_radiant=1400.0, elo_dire=1500.0,
                        **{model_name: ModelVerdict("Radiant", 0.61)})
        result = evaluate(ctx, cfg())
        assert result.underdog_side == "Radiant"
        win = [d for d in result.decisions if d.market == "win"]
        assert len(win) == 1 and win[0].target_side == "Radiant", model_name


# --- Kills: only with an underdog, both markets, KILLS_REQUIRE_ALL --------

def test_kills_require_an_underdog():
    ctx = base_ctx(elo_radiant=1500.0, elo_dire=1500.0,
                    early_nw=ModelVerdict("Radiant", 0.70))
    result = evaluate(ctx, cfg())
    assert not [d for d in result.decisions if d.market.startswith("kills")]
    reasons = {s.reason for s in result.skipped if s.market.startswith("kills")}
    assert reasons == {md.REASON_NO_UNDERDOG}


def test_kills_both_markets_when_underdog_and_early_model_confirm():
    ctx = base_ctx(elo_radiant=1400.0, elo_dire=1500.0,
                    early_nw=ModelVerdict("Radiant", 0.70),
                    kills_windows_open=["0-10"])
    result = evaluate(ctx, cfg())
    kills = {d.market: d for d in result.decisions if d.market.startswith("kills")}
    assert set(kills) == {"kills_window", "kills_total"}
    assert kills["kills_window"].target_side == "Radiant"
    assert kills["kills_window"].reasons[-1] == "window=0-10"
    assert kills["kills_total"].target_side == "Radiant"


def test_kills_window_absent_without_an_open_window():
    ctx = base_ctx(elo_radiant=1400.0, elo_dire=1500.0,
                    early_nw=ModelVerdict("Radiant", 0.70),
                    kills_windows_open=[])
    result = evaluate(ctx, cfg())
    # early_nw >= 0.60 is also win-market support since 12.09.2026 (owner
    # rule "any starred model"), so restrict the assertion to kills markets.
    kills = {d.market for d in result.decisions if d.market.startswith("kills")}
    assert kills == {"kills_total"}


def test_kills_require_all_blocks_without_all_confirmation():
    ctx = base_ctx(elo_radiant=1400.0, elo_dire=1500.0,
                    early_nw=ModelVerdict("Radiant", 0.70),
                    all=None,
                    kills_windows_open=["0-10"])
    result = evaluate(ctx, cfg(kills_require_all=True))
    assert not [d for d in result.decisions if d.market.startswith("kills")]


def test_kills_require_all_passes_with_all_confirmation():
    ctx = base_ctx(elo_radiant=1400.0, elo_dire=1500.0,
                    early_nw=ModelVerdict("Radiant", 0.70),
                    all=ModelVerdict("Radiant", 0.65),
                    kills_windows_open=["0-10"])
    result = evaluate(ctx, cfg(kills_require_all=True))
    kills = {d.market: d for d in result.decisions if d.market.startswith("kills")}
    assert set(kills) == {"kills_window", "kills_total"}
    assert "all" in kills["kills_total"].models_for


# --- Timing ---------------------------------------------------------------

def test_lane_at_target_gives_now_regardless_of_game_time():
    ctx = base_ctx(game_time=5.0, late=ModelVerdict("Radiant", 0.70),
                    lane=ModelVerdict("Radiant", 0.65))
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"][0]
    assert win.timing == "now"


def test_lane_below_threshold_and_early_game_waits():
    ctx = base_ctx(game_time=5.0, late=ModelVerdict("Radiant", 0.70),
                    lane=ModelVerdict("Radiant", 0.40))
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"][0]
    assert win.timing == "wait_600"


def test_no_lane_and_early_game_waits():
    ctx = base_ctx(game_time=5.0, late=ModelVerdict("Radiant", 0.70), lane=None)
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"][0]
    assert win.timing == "wait_600"


def test_game_time_past_timing_seconds_gives_now_even_without_lane():
    ctx = base_ctx(game_time=600.0, late=ModelVerdict("Radiant", 0.70), lane=None)
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"][0]
    assert win.timing == "now"


# --- Dedup: in-memory set and SentLedger persistence -----------------------

def test_already_sent_set_skips_repeat_decision():
    ctx = base_ctx(late=ModelVerdict("Radiant", 0.70),
                    already_sent={("https://example.test/m1", 1, "win", "Radiant")})
    result = evaluate(ctx, cfg())
    assert not [d for d in result.decisions if d.market == "win"]
    assert any(s.market == "win" and s.reason == md.REASON_DEDUP for s in result.skipped)


def test_sent_ledger_persists_across_reload(tmp_path):
    path = tmp_path / "ml_dispatch_sent.json"
    ledger = SentLedger(path)
    ledger.load()
    key = ("https://example.test/m1", 1, "win", "Radiant")
    assert not ledger.contains(key)
    ledger.add(key)
    ledger.save()

    reloaded = SentLedger(path).load()
    assert reloaded.contains(key)
    assert not reloaded.contains(("other", 1, "win", "Radiant"))
    # Rebuild-then-replace: the .tmp file must not survive a successful save.
    assert not path.with_suffix(path.suffix + ".tmp").exists()


def test_sent_ledger_missing_file_loads_empty(tmp_path):
    ledger = SentLedger(tmp_path / "does_not_exist.json").load()
    assert not ledger.contains(("a", 1, "win", "Radiant"))


# --- expected_wr / min_odds with margin ------------------------------------

def test_expected_wr_is_max_confidence_among_models_for():
    ctx = base_ctx(late=ModelVerdict("Radiant", 0.65), all=ModelVerdict("Radiant", 0.80))
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"][0]
    assert win.expected_wr == 0.80
    assert win.min_odds == round(1 / 0.80, 2)


def test_min_odds_applies_margin():
    ctx = base_ctx(late=ModelVerdict("Radiant", 0.65))
    result = evaluate(ctx, cfg(min_odds_margin=0.05))
    win = [d for d in result.decisions if d.market == "win"][0]
    assert win.min_odds == round(1 / (0.65 - 0.05), 2)


# --- model_missing ----------------------------------------------------------

def test_model_missing_when_no_win_model_present():
    ctx = base_ctx()  # early_nw/early_win/late/all/lane all default None
    result = evaluate(ctx, cfg())
    win_skips = [s for s in result.skipped if s.market == "win"]
    assert len(win_skips) == 2
    assert all(s.reason == md.REASON_MODEL_MISSING for s in win_skips)


def test_model_missing_for_kills_when_both_early_models_absent():
    ctx = base_ctx(elo_radiant=1400.0, elo_dire=1500.0)
    result = evaluate(ctx, cfg())
    kills_skips = [s for s in result.skipped if s.market.startswith("kills")]
    assert len(kills_skips) == 2
    assert all(s.reason == md.REASON_MODEL_MISSING for s in kills_skips)


# --- Config.from_env ---------------------------------------------------------

def test_config_from_env_reads_all_knobs():
    env = {
        "ML_DISPATCH_MIN_CONF": "0.55",
        "ML_DISPATCH_UNDERDOG_MIN_DIFF": "40",
        "ML_DISPATCH_WIN_MODELS": "late,early_win",
        "ML_DISPATCH_KILLS_REQUIRE_ALL": "1",
        "ML_DISPATCH_TIMING_SECONDS": "300",
        "ML_DISPATCH_MIN_ODDS_MARGIN": "0.02",
        "ML_DISPATCH_SENT_PATH": "runtime/custom_sent.json",
    }
    result = Config.from_env(env)
    assert result.min_conf == 0.55
    assert result.underdog_min_diff == 40.0
    assert result.win_models == ("late", "early_win")
    assert result.kills_require_all is True
    assert result.timing_seconds == 300.0
    assert result.min_odds_margin == 0.02
    assert result.sent_path == "runtime/custom_sent.json"


def test_config_from_env_defaults_when_unset():
    result = Config.from_env({})
    assert result.min_conf == 0.60
    assert result.underdog_min_diff == 50.0
    assert result.win_models == ("late", "all", "early_win", "early_nw")
    assert result.kills_require_all is False
    assert result.timing_seconds == 600.0
    assert result.min_odds_margin == 0.0


# --- laning_serving.verdicts() and the ★ marker ----------------------------

def test_laning_verdicts_matches_panel_lines_all_and_lane():
    from types import SimpleNamespace
    from base import laning_serving as serving

    heroes = tuple(range(1, 11))
    draft = SimpleNamespace(_heroes_vector=lambda *args: heroes,
                            win_index_draft=lambda *args: -5.4)
    teams = [{f'pos{i}': {'account_id': i + offset} for i in range(1, 6)}
             for offset in (0, 5)]

    class Predictor:
        @staticmethod
        def predict(h, a, t):
            return [.39, .01, .60]

    original_service = serving._SERVICE
    serving._SERVICE = Predictor()
    try:
        lines = serving.panel_lines(*teams, 12345, draft_model=draft)
        result = serving.verdicts(*teams, 12345, draft_model=draft)
    finally:
        serving._SERVICE = original_service

    assert lines["all_model_line"] == "🌐 All ML-модель: Dire 55.4%"
    assert result["all"]["side"] == "Dire"
    assert result["all"]["confidence"] == pytest.approx(0.554)
    assert result["lane"] == {"side": "Radiant", "confidence": 0.60, "p_tie": 0.01}
    # ml_laning_line hits the star threshold (60.0%); verdicts() confidence
    # for lane matches the same underlying probability used in that line.
    assert lines["ml_laning_line"].endswith("★")


def test_laning_verdicts_lane_side_ignores_tie_being_the_argmax():
    from types import SimpleNamespace
    from base import laning_serving as serving

    draft = SimpleNamespace(_heroes_vector=lambda *args: tuple(range(1, 11)),
                            win_index_draft=lambda *args: None)

    class Predictor:
        @staticmethod
        def predict(*args):
            return [.2, .6, .2]

    original_service = serving._SERVICE
    serving._SERVICE = Predictor()
    try:
        result = serving.verdicts({}, {}, 12345, draft_model=draft)
    finally:
        serving._SERVICE = original_service

    # Tie (.6) is the argmax, but verdicts() always resolves to Radiant/Dire.
    assert result["lane"]["side"] in ("Radiant", "Dire")
    assert result["lane"]["confidence"] == 0.2
    assert result["lane"]["p_tie"] == 0.6


def test_laning_verdicts_returns_none_on_missing_data():
    from types import SimpleNamespace
    from base import laning_serving as serving

    draft = SimpleNamespace(_heroes_vector=lambda *args: None,
                            win_index_draft=lambda *args: None)
    result = serving.verdicts({}, {}, 12345, draft_model=draft)
    assert result == {"all": None, "lane": None}


# --- ML_DISPATCH_MAX_GAME_TIME (default off) ----------------------------

def test_max_game_time_unset_behaves_as_before():
    ctx = base_ctx(game_time=9999.0, late=ModelVerdict("Radiant", 0.70))
    result = evaluate(ctx, cfg())
    win_decisions = [d for d in result.decisions if d.market == "win"]
    assert len(win_decisions) == 1
    assert win_decisions[0].target_side == "Radiant"
    assert not any(s.reason == md.REASON_TOO_LATE for s in result.skipped)


def test_max_game_time_950_over_900_cap_skips_too_late():
    ctx = base_ctx(game_time=950.0, late=ModelVerdict("Radiant", 0.70))
    result = evaluate(ctx, cfg(max_game_time=900.0))
    win_decisions = [d for d in result.decisions if d.market == "win"]
    assert win_decisions == []
    assert any(
        s.market == "win" and s.side is None and s.reason == md.REASON_TOO_LATE
        for s in result.skipped
    )


def test_max_game_time_650_under_900_cap_still_decides():
    ctx = base_ctx(game_time=650.0, late=ModelVerdict("Radiant", 0.70))
    result = evaluate(ctx, cfg(max_game_time=900.0))
    win_decisions = [d for d in result.decisions if d.market == "win"]
    assert len(win_decisions) == 1
    assert win_decisions[0].target_side == "Radiant"
    assert not any(s.reason == md.REASON_TOO_LATE for s in result.skipped)


def test_config_from_env_max_game_time_empty_or_zero_means_no_cap():
    assert Config.from_env({}).max_game_time is None
    assert Config.from_env({"ML_DISPATCH_MAX_GAME_TIME": ""}).max_game_time is None
    assert Config.from_env({"ML_DISPATCH_MAX_GAME_TIME": "0"}).max_game_time is None
    assert Config.from_env({"ML_DISPATCH_MAX_GAME_TIME": "900"}).max_game_time == 900.0
