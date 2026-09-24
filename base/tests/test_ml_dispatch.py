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
    # early_win vs late on opposite sides is now a late-conflict wait
    # candidate under the 13.09.2026 default -> pin the pre-13.09 veto
    # mode explicitly (see the "late conflict wait" section below for the
    # wait-mode analog of this exact scenario).
    ctx = base_ctx(
        early_win=ModelVerdict("Dire", 0.65),
        late=ModelVerdict("Radiant", 0.70),
    )
    result = evaluate(ctx, cfg(win_models=("early_win",), late_conflict_mode="veto"))
    assert result.decisions == []
    dire_skip = next(s for s in result.skipped if s.market == "win" and s.side == "Dire")
    assert dire_skip.reason == md.REASON_VETO


def test_all_veto_blocks_a_lone_early_win_candidate():
    # early_win vs all on opposite sides is also a late-conflict wait
    # candidate under the 13.09.2026 default -> pin veto mode explicitly.
    ctx = base_ctx(
        early_win=ModelVerdict("Radiant", 0.65),
        all=ModelVerdict("Dire", 0.70),
    )
    result = evaluate(ctx, cfg(win_models=("early_win",), late_conflict_mode="veto"))
    assert result.decisions == []
    radiant_skip = next(s for s in result.skipped if s.market == "win" and s.side == "Radiant")
    assert radiant_skip.reason == md.REASON_VETO


def test_veto_wins_over_conflict_when_one_side_is_vetoed():
    # Owner's example (plan, "правка 0"): Late confirms Radiant at 0.62,
    # Early Win confirms Dire at 0.65 -- under the default win_models
    # (which counts Late as support for Radiant too) this used to be a
    # conflict, but veto is now resolved PER SIDE FIRST: Late (a veto
    # model) vetoes Dire, only Radiant survives -> bet on Radiant, not a
    # conflict (see module docstring design decision 2). This is also a
    # late-conflict wait candidate under the 13.09.2026 default (Early Win
    # stars Dire, Late stars the other side) -> pin veto mode explicitly.
    ctx = base_ctx(
        late=ModelVerdict("Radiant", 0.62),
        early_win=ModelVerdict("Dire", 0.65),
    )
    result = evaluate(ctx, cfg(late_conflict_mode="veto"))
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
    assert win[0].min_odds == round(1 / (0.7324 - 0.12), 2)


def test_lone_early_nw_star_backs_the_side_under_default_config():
    # E-291 (owner decision 15.09.2026, adapted from the pre-E-291 test of
    # the same name): only Early NW is starred; Late/All/Early Win are
    # present but below the threshold on the SAME side (no veto). Before
    # 15.09.2026 this produced a Decision with models_for == ["early_nw"];
    # now a solo early_nw/early_win star (no all/late support) is blocked
    # as early_solo_blocked instead (offline 50-57%, worse than ELO).
    ctx = base_ctx(
        game_time=900.0,
        early_nw=ModelVerdict("Radiant", 0.66),
        early_win=ModelVerdict("Radiant", 0.55),
        late=ModelVerdict("Radiant", 0.52),
        all=ModelVerdict("Radiant", 0.58),
    )
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"]
    assert win == []
    skip = next(s for s in result.skipped if s.market == "win" and s.side == "Radiant")
    assert skip.reason == md.REASON_EARLY_SOLO_BLOCKED


def test_any_one_of_four_win_models_confirms_the_favorite():
    # E-291: late/all alone still confirm a Decision; early_win/early_nw
    # alone are now blocked (early_solo_blocked) since they have no
    # all/late support in models_for -- adapted from the pre-15.09.2026
    # version of this test, which expected a Decision for all four.
    for model_name in ("late", "all"):
        ctx = base_ctx(elo_radiant=1550.0, elo_dire=1500.0,
                        **{model_name: ModelVerdict("Radiant", 0.61)})
        result = evaluate(ctx, cfg())
        win = [d for d in result.decisions if d.market == "win"]
        assert len(win) == 1 and win[0].target_side == "Radiant", model_name
    for model_name in ("early_win", "early_nw"):
        ctx = base_ctx(elo_radiant=1550.0, elo_dire=1500.0,
                        **{model_name: ModelVerdict("Radiant", 0.61)})
        result = evaluate(ctx, cfg())
        win = [d for d in result.decisions if d.market == "win"]
        assert win == [], model_name
        skip = next(s for s in result.skipped
                    if s.market == "win" and s.side == "Radiant")
        assert skip.reason == md.REASON_EARLY_SOLO_BLOCKED, model_name


def test_any_one_of_four_win_models_confirms_the_underdog():
    # E-291: same split as the favorite test above.
    for model_name in ("late", "all"):
        ctx = base_ctx(elo_radiant=1400.0, elo_dire=1500.0,
                        **{model_name: ModelVerdict("Radiant", 0.61)})
        result = evaluate(ctx, cfg())
        assert result.underdog_side == "Radiant"
        win = [d for d in result.decisions if d.market == "win"]
        assert len(win) == 1 and win[0].target_side == "Radiant", model_name
    for model_name in ("early_win", "early_nw"):
        ctx = base_ctx(elo_radiant=1400.0, elo_dire=1500.0,
                        **{model_name: ModelVerdict("Radiant", 0.61)})
        result = evaluate(ctx, cfg())
        assert result.underdog_side == "Radiant"
        win = [d for d in result.decisions if d.market == "win"]
        assert win == [], model_name
        skip = next(s for s in result.skipped
                    if s.market == "win" and s.side == "Radiant")
        assert skip.reason == md.REASON_EARLY_SOLO_BLOCKED, model_name


# --- E-291: solo early_nw/early_win win-decisions are blocked -------------

def test_early_solo_block_a_only_early_win_star_is_blocked():
    # (a) Only early_win is starred for Radiant; nothing else clears the
    # threshold -> no win Decision, Skipped early_solo_blocked for Radiant.
    ctx = base_ctx(early_win=ModelVerdict("Radiant", 0.70))
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"]
    assert win == []
    skip = next(s for s in result.skipped if s.market == "win" and s.side == "Radiant")
    assert skip.reason == md.REASON_EARLY_SOLO_BLOCKED


def test_early_solo_block_b_both_early_models_still_blocked_without_all_or_late():
    # (b) early_nw + early_win both star Radiant; all/late are present but
    # below threshold -> still blocked (models_for has no all/late).
    ctx = base_ctx(
        early_nw=ModelVerdict("Radiant", 0.70),
        early_win=ModelVerdict("Radiant", 0.65),
        all=ModelVerdict("Radiant", 0.55),
        late=ModelVerdict("Radiant", 0.50),
    )
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"]
    assert win == []
    skip = next(s for s in result.skipped if s.market == "win" and s.side == "Radiant")
    assert skip.reason == md.REASON_EARLY_SOLO_BLOCKED


def test_early_solo_block_c_early_plus_all_still_confirms():
    # (c) early_win + all both star Radiant -> Decision as before, with
    # both models in models_for (paired early+all/late is unaffected).
    ctx = base_ctx(
        early_win=ModelVerdict("Radiant", 0.65),
        all=ModelVerdict("Radiant", 0.70),
    )
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"]
    assert len(win) == 1 and win[0].target_side == "Radiant"
    assert sorted(win[0].models_for) == ["all", "early_win"]


def test_early_solo_block_d_lone_late_and_lone_all_are_unaffected():
    # (d) A lone Late star and a lone All star still produce Decisions
    # (owner: Late/All-only behavior is unchanged by E-291).
    ctx_late = base_ctx(late=ModelVerdict("Radiant", 0.65))
    win_late = [d for d in evaluate(ctx_late, cfg()).decisions if d.market == "win"]
    assert len(win_late) == 1 and win_late[0].target_side == "Radiant"

    ctx_all = base_ctx(all=ModelVerdict("Radiant", 0.65))
    win_all = [d for d in evaluate(ctx_all, cfg()).decisions if d.market == "win"]
    assert len(win_all) == 1 and win_all[0].target_side == "Radiant"


def test_early_solo_block_e_env_toggle_restores_pre_e291_behavior():
    # (e) ML_DISPATCH_EARLY_SOLO_BLOCK=0 is the rollback: a solo early_win
    # star produces a Decision again, like before 15.09.2026.
    ctx = base_ctx(early_win=ModelVerdict("Radiant", 0.70))
    config = Config.from_env({"ML_DISPATCH_EARLY_SOLO_BLOCK": "0"})
    assert config.early_solo_block is False
    result = evaluate(ctx, config)
    win = [d for d in result.decisions if d.market == "win"]
    assert len(win) == 1 and win[0].target_side == "Radiant"
    assert win[0].models_for == ["early_win"]


# --- 🤖 Prematch as a sixth win model (owner decision 15.09.2026) ---------

def test_prematch_solo_star_backs_a_side(monkeypatch):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "1")
    # (a) 🤖 Prematch wired in 15.09.2026 as a sixth win model -- unlike
    # early_nw/early_win, it is NOT in EARLY_ONLY_BLOCK_MODELS, so a solo
    # prematch star is a plain Decision, not early_solo_blocked (E-291).
    ctx = base_ctx(prematch=ModelVerdict("Dire", 0.671))
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"]
    assert len(win) == 1
    assert win[0].target_side == "Dire"
    assert win[0].models_for == ["prematch"]
    assert win[0].expected_wr == 0.671


def test_prematch_plus_both_early_stars_not_early_solo_blocked(monkeypatch):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "1")
    # (b) early_nw + early_win + prematch all star the same side; all/late
    # are present but below threshold. prematch in models_for is enough to
    # avoid early_solo_blocked (only early_nw/early_win alone would block).
    ctx = base_ctx(
        early_nw=ModelVerdict("Radiant", 0.70),
        early_win=ModelVerdict("Radiant", 0.65),
        prematch=ModelVerdict("Radiant", 0.66),
        all=ModelVerdict("Radiant", 0.55),
        late=ModelVerdict("Radiant", 0.50),
    )
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"]
    assert len(win) == 1 and win[0].target_side == "Radiant"
    assert not any(s.reason == md.REASON_EARLY_SOLO_BLOCKED for s in result.skipped)
    assert sorted(win[0].models_for) == ["early_nw", "early_win", "prematch"]


def test_prematch_below_threshold_is_skipped(monkeypatch):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "1")
    # (c)
    ctx = base_ctx(prematch=ModelVerdict("Radiant", 0.58))
    result = evaluate(ctx, cfg())
    assert result.decisions == []
    skip = next(s for s in result.skipped if s.market == "win" and s.side == "Radiant")
    assert skip.reason == md.REASON_BELOW_THRESHOLD


def test_prematch_radiant_vs_late_dire_vetoes_radiant(monkeypatch):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "1")
    # (d) prematch is not in KILLS_EARLY_MODELS, so it never triggers the
    # 13.09.2026 late-conflict wait branch (that only watches
    # early_nw/early_win) -- plain Late veto applies directly: Late backs
    # Dire, vetoing Radiant's prematch support; Dire is backed off Late.
    ctx = base_ctx(
        prematch=ModelVerdict("Radiant", 0.671),
        late=ModelVerdict("Dire", 0.65),
    )
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"]
    assert len(win) == 1 and win[0].target_side == "Dire"
    radiant_skip = next(s for s in result.skipped if s.market == "win" and s.side == "Radiant")
    assert radiant_skip.reason == md.REASON_VETO


def test_prematch_ignored_when_win_models_env_excludes_it():
    # (e) explicit ML_DISPATCH_WIN_MODELS without prematch (the pre-15.09.2026
    # rollback value) -- a solo prematch star gives no support.
    ctx = base_ctx(prematch=ModelVerdict("Radiant", 0.90))
    config = Config.from_env({"ML_DISPATCH_WIN_MODELS": "late,all,early_win,early_nw"})
    assert "prematch" not in config.win_models
    result = evaluate(ctx, config)
    assert result.decisions == []


# --- Kills: only with an underdog, both markets, KILLS_REQUIRE_ALL --------

def test_kills_require_an_underdog():
    ctx = base_ctx(elo_radiant=1500.0, elo_dire=1500.0,
                    early_nw=ModelVerdict("Radiant", 0.70))
    # kills_early (19.09.2026) is independent of underdog and would add a
    # kills30_missing skip here (kills30_radiant unset); pin it off so this
    # test still isolates the no-underdog reason alone.
    result = evaluate(ctx, cfg(kills_early_enabled=False))
    assert not [d for d in result.decisions if d.market.startswith("kills")]
    reasons = {s.reason for s in result.skipped if s.market.startswith("kills")}
    assert reasons == {md.REASON_NO_UNDERDOG}


def test_kills_both_markets_when_underdog_and_early_model_confirm():
    ctx = base_ctx(elo_radiant=1400.0, elo_dire=1500.0,
                    early_nw=ModelVerdict("Radiant", 0.70),
                    kills_windows_open=["0-10"],
                    kills30_radiant=0.99)  # clears the E-281 kills_total gate
    result = evaluate(ctx, cfg())
    kills = {d.market: d for d in result.decisions if d.market.startswith("kills")}
    assert set(kills) == {"kills_window", "kills_total"}
    assert kills["kills_window"].target_side == "Radiant"
    assert kills["kills_window"].reasons[-1] == "window=0-10"
    assert kills["kills_total"].target_side == "Radiant"


def test_kills_window_absent_without_an_open_window():
    ctx = base_ctx(elo_radiant=1400.0, elo_dire=1500.0,
                    early_nw=ModelVerdict("Radiant", 0.70),
                    kills_windows_open=[],
                    kills30_radiant=0.99)  # clears the E-281 kills_total gate
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
                    kills_windows_open=["0-10"],
                    kills30_radiant=0.99)  # clears the E-281 kills_total gate
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


@pytest.mark.parametrize("side,lead", [("Radiant", 1000), ("Dire", -1000)])
@pytest.mark.parametrize("time,expected", [(239, "wait_600"), (240, "now"), (599, "now")])
def test_early_nw_releases_exact_threshold_only_in_target_direction(side, lead, time, expected):
    ctx = base_ctx(late=ModelVerdict(side, .70), game_time=time,
                   radiant_networth_lead=lead)
    win = next(d for d in evaluate(ctx, cfg()).decisions if d.market == "win")
    assert win.timing == expected
    assert any("early_nw_release" in r for r in win.reasons) == (expected == "now")


@pytest.mark.parametrize("lead", [None, 0, 999, -1000, float("nan"), float("inf"), "bad"])
def test_early_nw_missing_small_or_opposing_lead_keeps_wait(lead):
    ctx = base_ctx(late=ModelVerdict("Radiant", .70), game_time=240,
                   radiant_networth_lead=lead)
    win = next(d for d in evaluate(ctx, cfg()).decisions if d.market == "win")
    assert win.timing == "wait_600"


def test_early_nw_config_disable_and_nonfinite_game_clock_fail_closed():
    ctx = base_ctx(late=ModelVerdict("Radiant", .70), game_time=240,
                   radiant_networth_lead=1000)
    config = Config.from_env({"ML_DISPATCH_EARLY_NW": "0"})
    assert not config.early_nw_enabled
    assert next(d for d in evaluate(ctx, config).decisions if d.market == "win").timing == "wait_600"
    ctx.game_time = float("nan")
    assert not md._early_nw_release(ctx, cfg(), "Radiant")
    defaults = Config.from_env({})
    assert (defaults.early_nw_enabled, defaults.early_nw_start_seconds,
            defaults.early_nw_min_lead) == (True, 240, 1000)


def test_early_nw_does_not_override_late_conflict_wait_or_dedup():
    ctx = base_ctx(early_nw=ModelVerdict("Radiant", .75),
                   late=ModelVerdict("Dire", .70), game_time=300,
                   radiant_networth_lead=-5000)
    result = evaluate(ctx, cfg())
    assert not any(d.market == "win" for d in result.decisions)
    assert any(s.reason == md.REASON_LATE_CONFLICT_WAIT for s in result.skipped)
    ctx.early_nw = None
    ctx.already_sent = {(ctx.base_url, ctx.map_num, "win", "Dire")}
    result = evaluate(ctx, cfg())
    assert not any(d.market == "win" for d in result.decisions)
    assert any(s.reason == md.REASON_DEDUP for s in result.skipped)


# --- Owner 20.09.2026: ELO favorite + lanes release the wait at 00 --------

@pytest.mark.parametrize("side,elo_radiant,elo_dire", [
    ("Radiant", 1520.0, 1500.0),
    ("Dire", 1500.0, 1520.0),
])
def test_lane_elo_release_via_lane_confidence(side, elo_radiant, elo_dire):
    ctx = base_ctx(elo_radiant=elo_radiant, elo_dire=elo_dire, game_time=5.0,
                   late=ModelVerdict(side, .70), lane=ModelVerdict(side, .55))
    win = next(d for d in evaluate(ctx, cfg()).decisions if d.market == "win")
    assert win.timing == "now"
    assert any(r.startswith("lane_elo_release") and "via=lane" in r and "via=lane_adv_dict" not in r
               for r in win.reasons)


@pytest.mark.parametrize("side,adv,elo_radiant,elo_dire", [
    ("Radiant", 8.0, 1501.0, 1500.0),
    ("Dire", -8.0, 1500.0, 1501.0),
])
def test_lane_elo_release_via_lane_adv_dict(side, adv, elo_radiant, elo_dire):
    ctx = base_ctx(elo_radiant=elo_radiant, elo_dire=elo_dire, game_time=5.0,
                   late=ModelVerdict(side, .70), lane=None, lane_adv_dict=adv)
    win = next(d for d in evaluate(ctx, cfg()).decisions if d.market == "win")
    assert win.timing == "now"
    assert any("via=lane_adv_dict" in r for r in win.reasons)


@pytest.mark.parametrize("elo_radiant,elo_dire,lane,adv", [
    (1500.0, 1500.0, ModelVerdict("Radiant", .58), 9.0),      # equal ELO
    (1499.0, 1500.0, ModelVerdict("Radiant", .58), 9.0),      # underdog
    (None, 1500.0, ModelVerdict("Radiant", .58), 9.0),        # ELO missing
    (1520.0, 1500.0, ModelVerdict("Radiant", .549), 7.99),    # both below threshold
    (1520.0, 1500.0, ModelVerdict("Dire", .58), -9.0),        # lanes point at the other side
    (1520.0, 1500.0, None, None),
    (1520.0, 1500.0, None, float("nan")),
    (1520.0, 1500.0, None, "bad"),
])
def test_lane_elo_release_keeps_wait_when_conditions_not_met(elo_radiant, elo_dire, lane, adv):
    ctx = base_ctx(elo_radiant=elo_radiant, elo_dire=elo_dire, game_time=5.0,
                   late=ModelVerdict("Radiant", .70), lane=lane, lane_adv_dict=adv)
    win = next(d for d in evaluate(ctx, cfg()).decisions if d.market == "win")
    assert win.timing == "wait_600"
    assert not any("lane_elo_release" in r for r in win.reasons)


def test_lane_elo_release_config_and_direct_helper():
    ctx = base_ctx(elo_radiant=1520.0, elo_dire=1500.0, game_time=5.0,
                   late=ModelVerdict("Radiant", .70), lane=ModelVerdict("Radiant", .56))

    disabled = Config.from_env({"ML_DISPATCH_LANE_ELO_RELEASE": "0"})
    assert disabled.lane_elo_release_enabled is False
    win = next(d for d in evaluate(ctx, disabled).decisions if d.market == "win")
    assert win.timing == "wait_600"

    defaults = Config.from_env({})
    assert (defaults.lane_elo_release_enabled, defaults.lane_elo_release_lane_conf,
            defaults.lane_elo_release_lane_adv) == (True, 0.55, 8.0)

    custom = Config.from_env({"ML_DISPATCH_LANE_ELO_RELEASE_LANE_CONF": "0.57",
                              "ML_DISPATCH_LANE_ELO_RELEASE_LANE_ADV": "5"})
    assert (custom.lane_elo_release_lane_conf, custom.lane_elo_release_lane_adv) == (0.57, 5.0)

    assert isinstance(md._lane_elo_release(ctx, cfg(), "Radiant"), str)

    ctx_zero_threshold = base_ctx(elo_radiant=1520.0, elo_dire=1500.0, game_time=5.0,
                                  late=ModelVerdict("Radiant", .70), lane=None,
                                  lane_adv_dict=50.0)
    assert md._lane_elo_release(ctx_zero_threshold, cfg(lane_elo_release_lane_adv=0.0),
                                "Radiant") is None


def test_lane_elo_release_does_not_touch_late_conflict_wait():
    ctx = base_ctx(early_nw=ModelVerdict("Radiant", .75),
                   late=ModelVerdict("Dire", .70), game_time=300,
                   elo_radiant=1500.0, elo_dire=1550.0,
                   lane=ModelVerdict("Dire", .58), lane_adv_dict=-12.0)
    result = evaluate(ctx, cfg())
    assert not any(d.market == "win" for d in result.decisions)
    assert any(s.reason == md.REASON_LATE_CONFLICT_WAIT for s in result.skipped)


def test_lane_elo_release_reason_absent_when_star_already_fires():
    ctx = base_ctx(elo_radiant=1500.0, elo_dire=1500.0, game_time=5.0,
                   late=ModelVerdict("Radiant", .70), lane=ModelVerdict("Radiant", .65))
    win = next(d for d in evaluate(ctx, cfg()).decisions if d.market == "win")
    assert win.timing == "now"
    assert not any("lane_elo_release" in r for r in win.reasons)


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
    assert win.min_odds == round(1 / (0.80 - 0.12), 2)


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
    assert result.min_odds_margin == 0.12
    assert result.early_solo_block is True


def test_min_odds_margin_zero_env_restores_previous_floor():
    assert Config.from_env({"ML_DISPATCH_MIN_ODDS_MARGIN": "0"}).min_odds_margin == 0.0


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
    # The line now ends with a data-freshness suffix (" | данные до ...",
    # c82120ac), so the star is present but no longer the last character.
    assert "★" in lines["ml_laning_line"]


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


# --- Late-conflict wait branches (owner decisions, 13.09.2026) -------------
# See docs/experiments/E-288-early-vs-late-disagreement-branches.md,
# addendum "Решение владельца 13.09 и реализация", for the data behind
# these rules.

def test_late_conflict_4_1_waits_then_bets_the_late_side():
    ctx_early = base_ctx(
        game_time=700.0,
        early_win=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
    )
    result_early = evaluate(ctx_early, cfg())
    assert not [d for d in result_early.decisions if d.market == "win"]
    waits = [s for s in result_early.skipped
             if s.market == "win" and s.reason == md.REASON_LATE_CONFLICT_WAIT]
    assert {s.side for s in waits} == {"Radiant", "Dire"}

    ctx_late = base_ctx(
        game_time=1860.0,
        early_win=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
    )
    result_late = evaluate(ctx_late, cfg())
    win = [d for d in result_late.decisions if d.market == "win"]
    assert len(win) == 1
    assert win[0].target_side == "Dire"
    assert win[0].rule == md.RULE_WIN_LATE_AFTER_WAIT
    assert win[0].models_for == ["late"]
    assert win[0].models_against == ["early_win"]


def test_late_conflict_4_2_late_and_all_agree_on_b():
    ctx = base_ctx(
        game_time=1860.0,
        early_win=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
        all=ModelVerdict("Dire", 0.65),
    )
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"]
    assert len(win) == 1
    assert win[0].target_side == "Dire"
    assert sorted(win[0].models_for) == ["all", "late"]
    assert win[0].expected_wr == 0.70


def test_late_conflict_all_alone_backs_b_without_late_star():
    # E-288 corpus has n=0 for this exact pairing (All stars B, Late does
    # not) -- implemented per the owner's rule, unverified in production.
    ctx = base_ctx(
        game_time=1860.0,
        early_win=ModelVerdict("Radiant", 0.65),
        all=ModelVerdict("Dire", 0.65),
    )
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"]
    assert len(win) == 1
    assert win[0].target_side == "Dire"
    assert win[0].models_for == ["all"]


def test_late_conflict_4_3_kills_fire_immediately_for_early_side():
    ctx_early = base_ctx(
        game_time=700.0,
        elo_radiant=1500.0, elo_dire=1500.0,
        early_win=ModelVerdict("Radiant", 0.65),
        all=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
        kills_windows_open=["0-10"],
        kills30_radiant=0.99,  # clears the E-281 kills_total gate
    )
    result_early = evaluate(ctx_early, cfg())
    assert not [d for d in result_early.decisions if d.market == "win"]
    kills = {d.market: d for d in result_early.decisions if d.market.startswith("kills")}
    assert set(kills) == {"kills_window", "kills_total"}
    assert kills["kills_total"].target_side == "Radiant"
    assert kills["kills_total"].rule == md.RULE_KILLS_LATE_CONFLICT_EARLY_SIDE
    # "kills30" is the E-281 gate's own addition (owner rule 13.09.2026),
    # appended on top of the original early/all support.
    assert sorted(kills["kills_total"].models_for) == ["all", "early_win", "kills30"]

    ctx_late = base_ctx(
        game_time=1860.0,
        elo_radiant=1500.0, elo_dire=1500.0,
        early_win=ModelVerdict("Radiant", 0.65),
        all=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
    )
    result_late = evaluate(ctx_late, cfg())
    win = [d for d in result_late.decisions if d.market == "win"]
    assert len(win) == 1
    assert win[0].target_side == "Dire"
    assert "tiebreak_ignored_all_for_A" in win[0].reasons


def test_late_conflict_4_3_kills_fire_even_when_early_side_is_favorite():
    ctx = base_ctx(
        game_time=700.0,
        elo_radiant=1700.0, elo_dire=1500.0,  # Radiant (A) favorite, Dire underdog
        early_win=ModelVerdict("Radiant", 0.65),
        all=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
        kills30_radiant=0.99,  # clears the E-281 kills_total gate
    )
    result = evaluate(ctx, cfg())
    kills_total = [d for d in result.decisions if d.market == "kills_total"]
    assert len(kills_total) == 1
    assert kills_total[0].target_side == "Radiant"
    assert not any(s.reason == md.REASON_NO_UNDERDOG for s in result.skipped)


def test_late_conflict_4_3_no_duplicate_when_early_side_is_also_underdog():
    ctx = base_ctx(
        game_time=700.0,
        elo_radiant=1300.0, elo_dire=1500.0,  # Radiant (A) is the underdog
        early_win=ModelVerdict("Radiant", 0.65),
        all=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
        kills_windows_open=["0-10"],
        kills30_radiant=0.99,  # clears the E-281 kills_total gate
    )
    result = evaluate(ctx, cfg())
    kills_total = [d for d in result.decisions if d.market == "kills_total"]
    kills_window = [d for d in result.decisions if d.market == "kills_window"]
    assert len(kills_total) == 1 and kills_total[0].target_side == "Radiant"
    assert len(kills_window) == 1 and kills_window[0].target_side == "Radiant"
    assert kills_total[0].rule == "kills_underdog_total"


def test_late_conflict_4_3_no_underdog_skip_left_for_resolved_kills_markets():
    # Equal ELO (underdog_side=None) would normally leave a REASON_NO_UNDERDOG
    # skip on both kills markets; once the 4.3 early-side path resolves both
    # markets for A, that stale skip must not remain alongside the decision.
    ctx = base_ctx(
        game_time=700.0,
        elo_radiant=1500.0, elo_dire=1500.0,
        early_win=ModelVerdict("Radiant", 0.65),
        all=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
        kills_windows_open=["0-10"],
        kills30_radiant=0.99,  # clears the E-281 kills_total gate
    )
    result = evaluate(ctx, cfg())
    kills_markets_resolved = {d.market for d in result.decisions if d.market.startswith("kills")}
    assert kills_markets_resolved == {"kills_window", "kills_total"}
    assert not any(
        s.market.startswith("kills") and s.reason == md.REASON_NO_UNDERDOG
        for s in result.skipped
    )


def test_late_conflict_max_game_time_cap_does_not_block_wait_branch():
    ctx = base_ctx(
        game_time=1900.0,
        early_win=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
    )
    result = evaluate(ctx, cfg(max_game_time=900.0))
    win = [d for d in result.decisions if d.market == "win"]
    assert len(win) == 1
    assert win[0].target_side == "Dire"
    assert not any(s.reason == md.REASON_TOO_LATE for s in result.skipped)


def test_late_conflict_max_game_time_cap_still_applies_without_conflict():
    ctx = base_ctx(game_time=1000.0, late=ModelVerdict("Radiant", 0.70))
    result = evaluate(ctx, cfg(max_game_time=900.0))
    assert not [d for d in result.decisions if d.market == "win"]
    assert any(s.market == "win" and s.reason == md.REASON_TOO_LATE for s in result.skipped)


def test_late_conflict_lane_star_does_not_skip_the_wait():
    ctx = base_ctx(
        game_time=700.0,
        early_win=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
        lane=ModelVerdict("Dire", 0.70),
    )
    result = evaluate(ctx, cfg())
    assert not [d for d in result.decisions if d.market == "win"]
    assert any(s.market == "win" and s.reason == md.REASON_LATE_CONFLICT_WAIT
               for s in result.skipped)


def test_late_conflict_veto_mode_reproduces_pre_13_09_behavior():
    # (1) 4.1 scenario: pre-13.09 veto -> immediate bet on the late side.
    ctx1 = base_ctx(game_time=700.0,
                     early_win=ModelVerdict("Radiant", 0.65),
                     late=ModelVerdict("Dire", 0.70))
    result1 = evaluate(ctx1, cfg(late_conflict_mode="veto"))
    win1 = [d for d in result1.decisions if d.market == "win"]
    assert len(win1) == 1 and win1[0].target_side == "Dire"
    assert win1[0].rule == "win_single_model_confirm"

    # (2) 4.2 scenario: pre-13.09 veto -> immediate bet on late+all side.
    ctx2 = base_ctx(game_time=700.0,
                     early_win=ModelVerdict("Radiant", 0.65),
                     late=ModelVerdict("Dire", 0.70),
                     all=ModelVerdict("Dire", 0.65))
    result2 = evaluate(ctx2, cfg(late_conflict_mode="veto"))
    win2 = [d for d in result2.decisions if d.market == "win"]
    assert len(win2) == 1 and win2[0].target_side == "Dire"

    # (4) 4.3 scenario: mutual veto -> no bet on either side, no kills --
    # matches the documented E-288 prod behavior ("код не ставит").
    ctx4 = base_ctx(game_time=700.0,
                     elo_radiant=1500.0, elo_dire=1500.0,
                     early_win=ModelVerdict("Radiant", 0.65),
                     all=ModelVerdict("Radiant", 0.65),
                     late=ModelVerdict("Dire", 0.70))
    result4 = evaluate(ctx4, cfg(late_conflict_mode="veto"))
    assert result4.decisions == []


def test_config_from_env_late_conflict_defaults_and_overrides():
    default = Config.from_env({})
    assert default.late_conflict_mode == "wait"
    assert default.late_wait_seconds == 1860.0

    overridden = Config.from_env({
        "ML_DISPATCH_LATE_CONFLICT_MODE": "veto",
        "ML_DISPATCH_LATE_WAIT_SECONDS": "1200",
    })
    assert overridden.late_conflict_mode == "veto"
    assert overridden.late_wait_seconds == 1200.0

    invalid = Config.from_env({"ML_DISPATCH_LATE_CONFLICT_MODE": "bogus"})
    assert invalid.late_conflict_mode == "wait"


def test_late_conflict_game_time_none_waits():
    ctx = base_ctx(
        game_time=None,
        early_win=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
    )
    result = evaluate(ctx, cfg())
    assert not [d for d in result.decisions if d.market == "win"]
    assert any(s.reason == md.REASON_LATE_CONFLICT_WAIT for s in result.skipped)


def test_late_conflict_dedup_after_bet_on_late_side_is_sent():
    ctx = base_ctx(
        game_time=1900.0,
        early_win=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
        already_sent={("https://example.test/m1", 1, "win", "Dire")},
    )
    result = evaluate(ctx, cfg())
    assert not [d for d in result.decisions if d.market == "win"]
    dire_dedup = [s for s in result.skipped
                  if s.market == "win" and s.side == "Dire" and s.reason == md.REASON_DEDUP]
    assert len(dire_dedup) == 1


def test_late_conflict_mixed_early_models_resolves_unambiguously():
    ctx = base_ctx(
        game_time=1860.0,
        early_win=ModelVerdict("Radiant", 0.65),
        early_nw=ModelVerdict("Dire", 0.65),
        late=ModelVerdict("Dire", 0.70),
    )
    result = evaluate(ctx, cfg())
    win = [d for d in result.decisions if d.market == "win"]
    assert len(win) == 1
    assert win[0].target_side == "Dire"
    assert win[0].models_for == ["late"]


def test_late_conflict_ambiguous_pairing_falls_back_to_old_veto():
    ctx = base_ctx(
        game_time=700.0,
        elo_radiant=1500.0, elo_dire=1500.0,
        early_win=ModelVerdict("Radiant", 0.65),
        early_nw=ModelVerdict("Dire", 0.65),
        late=ModelVerdict("Dire", 0.70),
        all=ModelVerdict("Radiant", 0.65),
    )
    result = evaluate(ctx, cfg())
    assert result.decisions == []
    ambiguous_skips = [s for s in result.skipped
                       if s.market == "win" and "late_conflict_ambiguous" in s.detail]
    assert len(ambiguous_skips) >= 1


def test_late_conflict_evaluate_is_idempotent():
    ctx = base_ctx(
        game_time=1860.0,
        early_win=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
    )
    result1 = evaluate(ctx, cfg())
    result2 = evaluate(ctx, cfg())
    assert result1.decisions == result2.decisions
    assert result1.skipped == result2.skipped


# --- kills_total E-281 gate (owner rule, 13.09.2026) ------------------------

def test_kills_total_gate_favorite_060_passes_059_fails():
    # 4.3 late-conflict path is the only one where target_side can be the
    # ELO favorite: the underdog path always targets the underdog itself.
    ctx_pass = base_ctx(
        game_time=700.0,
        elo_radiant=1700.0, elo_dire=1500.0,  # Radiant (A) favorite, Dire underdog
        early_win=ModelVerdict("Radiant", 0.65),
        all=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
        kills_windows_open=["0-10"],
        kills30_radiant=0.60,
    )
    # kills_early (19.09.2026) fires independently at >=0.60 and would add
    # a duplicate below/kills_total path in the 0.59 fail leg; pin it off
    # so this test still isolates the 13.09 gate's favorite/other split.
    result_pass = evaluate(ctx_pass, cfg(kills_early_enabled=False))
    kills_total = [d for d in result_pass.decisions if d.market == "kills_total"]
    assert len(kills_total) == 1 and kills_total[0].target_side == "Radiant"
    assert "kills30" in kills_total[0].models_for
    assert any(r.startswith("kills30 p=0.600") and "favorite" in r for r in kills_total[0].reasons)
    assert any(d.market == "kills_window" for d in result_pass.decisions)

    ctx_fail = base_ctx(
        game_time=700.0,
        elo_radiant=1700.0, elo_dire=1500.0,
        early_win=ModelVerdict("Radiant", 0.65),
        all=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
        kills_windows_open=["0-10"],
        kills30_radiant=0.59,
    )
    result_fail = evaluate(ctx_fail, cfg(kills_early_enabled=False))
    assert not [d for d in result_fail.decisions if d.market == "kills_total"]
    assert any(d.market == "kills_window" for d in result_fail.decisions)
    below = [s for s in result_fail.skipped
             if s.market == "kills_total" and s.reason == md.REASON_KILLS30_BELOW]
    assert len(below) == 1 and below[0].side == "Radiant" and "favorite" in below[0].detail


def test_kills_total_gate_underdog_070_passes_069_fails():
    ctx_pass = base_ctx(
        elo_radiant=1400.0, elo_dire=1500.0,  # Radiant underdog
        early_nw=ModelVerdict("Radiant", 0.70),
        kills_windows_open=["0-10"],
        kills30_radiant=0.70,
    )
    # kills_early (19.09.2026) fires independently at >=0.60 and would mask
    # the 0.69 fail leg's below-threshold skip with its own; pin it off so
    # this test still isolates the 13.09 gate's underdog threshold.
    result_pass = evaluate(ctx_pass, cfg(kills_early_enabled=False))
    kills_total = [d for d in result_pass.decisions if d.market == "kills_total"]
    assert len(kills_total) == 1 and kills_total[0].target_side == "Radiant"
    assert "kills30" in kills_total[0].models_for

    ctx_fail = base_ctx(
        elo_radiant=1400.0, elo_dire=1500.0,
        early_nw=ModelVerdict("Radiant", 0.70),
        kills_windows_open=["0-10"],
        kills30_radiant=0.69,
    )
    result_fail = evaluate(ctx_fail, cfg(kills_early_enabled=False))
    assert not [d for d in result_fail.decisions if d.market == "kills_total"]
    assert any(d.market == "kills_window" for d in result_fail.decisions)
    below = [s for s in result_fail.skipped
             if s.market == "kills_total" and s.reason == md.REASON_KILLS30_BELOW]
    assert len(below) == 1 and "underdog" in below[0].detail


def test_kills_total_gate_4_3_even_070_passes_069_fails():
    ctx_pass = base_ctx(
        game_time=700.0,
        elo_radiant=1500.0, elo_dire=1500.0,
        early_win=ModelVerdict("Radiant", 0.65),
        all=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
        kills_windows_open=["0-10"],
        kills30_radiant=0.70,
    )
    # kills_early (19.09.2026) fires independently at >=0.60 (early_win
    # Radiant here) and would refire kills_total in the 0.69 fail leg; pin
    # it off so this test still isolates the 13.09 gate's even-ELO split.
    result_pass = evaluate(ctx_pass, cfg(kills_early_enabled=False))
    kills_total = [d for d in result_pass.decisions if d.market == "kills_total"]
    assert len(kills_total) == 1 and kills_total[0].target_side == "Radiant"

    ctx_fail = base_ctx(
        game_time=700.0,
        elo_radiant=1500.0, elo_dire=1500.0,
        early_win=ModelVerdict("Radiant", 0.65),
        all=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
        kills_windows_open=["0-10"],
        kills30_radiant=0.69,
    )
    result_fail = evaluate(ctx_fail, cfg(kills_early_enabled=False))
    assert not [d for d in result_fail.decisions if d.market == "kills_total"]
    assert any(d.market == "kills_window" for d in result_fail.decisions)
    below = [s for s in result_fail.skipped
             if s.market == "kills_total" and s.reason == md.REASON_KILLS30_BELOW]
    assert len(below) == 1 and "even" in below[0].detail


def test_kills_total_gate_missing_probability_is_fail_closed():
    ctx = base_ctx(
        elo_radiant=1400.0, elo_dire=1500.0,
        early_nw=ModelVerdict("Radiant", 0.70),
        kills_windows_open=["0-10"],
        # kills30_radiant left at Ctx default (None)
    )
    # kills_early (19.09.2026) fires independently at >=0.60 (early_nw
    # Radiant here) and would add a duplicate kills30_missing skip; pin it
    # off so this test still isolates the 13.09 gate's fail-closed path.
    result = evaluate(ctx, cfg(kills_early_enabled=False))
    assert not [d for d in result.decisions if d.market == "kills_total"]
    assert any(d.market == "kills_window" for d in result.decisions)
    missing = [s for s in result.skipped
               if s.market == "kills_total" and s.reason == md.REASON_KILLS30_MISSING]
    assert len(missing) == 1 and missing[0].side == "Radiant"


def test_kills_total_gate_disabled_restores_old_behavior():
    ctx = base_ctx(
        elo_radiant=1400.0, elo_dire=1500.0,
        early_nw=ModelVerdict("Radiant", 0.70),
        kills_windows_open=["0-10"],
        kills30_radiant=0.01,  # would fail every threshold if the gate were on
    )
    result = evaluate(ctx, cfg(kills_total_gate_enabled=False))
    kills_total = [d for d in result.decisions if d.market == "kills_total"]
    assert len(kills_total) == 1
    assert "kills30" not in kills_total[0].models_for
    assert not any(s.reason in (md.REASON_KILLS30_MISSING, md.REASON_KILLS30_BELOW)
                   for s in result.skipped)


def test_kills_total_gate_evaluate_is_idempotent():
    ctx = base_ctx(
        elo_radiant=1400.0, elo_dire=1500.0,
        early_nw=ModelVerdict("Radiant", 0.70),
        kills_windows_open=["0-10"],
        kills30_radiant=0.70,
    )
    result1 = evaluate(ctx, cfg())
    result2 = evaluate(ctx, cfg())
    assert result1.decisions == result2.decisions
    assert result1.skipped == result2.skipped


def test_config_from_env_kills_total_gate_reads_knobs_and_defaults():
    result = Config.from_env({
        "ML_DISPATCH_KILLS_TOTAL_GATE": "0",
        "ML_DISPATCH_KILLS_TOTAL_GATE_FAVORITE": "0.55",
        "ML_DISPATCH_KILLS_TOTAL_GATE_OTHER": "0.75",
    })
    assert result.kills_total_gate_enabled is False
    assert result.kills_total_gate_favorite == 0.55
    assert result.kills_total_gate_other == 0.75

    defaults = Config.from_env({})
    assert defaults.kills_total_gate_enabled is True
    assert defaults.kills_total_gate_favorite == 0.60
    assert defaults.kills_total_gate_other == 0.70


# --- kills_early (owner rule 19.09.2026) ---------------------------------

def _nemesis_ctx(**overrides):
    """Real case: elo 2282/2247 (no underdog), early_nw/early_win both
    Radiant, Late Dire (against Radiant), All Radiant (not starred),
    kills30_radiant 0.604 -> bet Radiant despite Late★ opposing it."""
    defaults = dict(
        elo_radiant=2282.0, elo_dire=2247.0,
        game_time=857.0,
        early_nw=ModelVerdict("Radiant", 0.769),
        early_win=ModelVerdict("Radiant", 0.795),
        late=ModelVerdict("Dire", 0.617),
        all=ModelVerdict("Radiant", 0.524),
        kills30_radiant=0.604,
        kills30_dire=0.441,
    )
    defaults.update(overrides)
    return base_ctx(**defaults)


def test_kills_early_win_plus_nw_same_side_nemesis():
    ctx = _nemesis_ctx()
    result = evaluate(ctx, cfg())
    kills_total = [d for d in result.decisions if d.market == "kills_total"]
    assert len(kills_total) == 1
    decision = kills_total[0]
    assert decision.target_side == "Radiant"
    assert decision.rule == md.RULE_KILLS_EARLY_WIN_KILLS30
    assert sorted(decision.models_for) == ["early_nw", "early_win", "kills30"]
    assert decision.expected_wr == 0.604
    assert decision.min_odds == round(1 / (0.604 - 0.12), 2)
    assert not [d for d in result.decisions if d.market == "win"]
    assert not [s for s in result.skipped
                if s.market == "kills_total" and s.side in (None, "Radiant")]
    assert any(s.market == "kills_window" and s.reason == md.REASON_NO_UNDERDOG
               for s in result.skipped)


def test_kills_early_win_alone_nemiga():
    # Real case: elo 1860/2183 (Radiant underdog), early_nw Dire 0.695,
    # early_win Radiant 0.632 -> bet Radiant; early_nw AGAINST Radiant does
    # not block it. Also exercises the underdog path's own kills_total
    # (gated at kills30 >= 0.70 for an underdog) being dropped at 0.658 and
    # kills_early picking the market back up at its own 0.60 threshold.
    ctx = base_ctx(
        elo_radiant=1860.0, elo_dire=2183.0,
        early_nw=ModelVerdict("Dire", 0.695),
        early_win=ModelVerdict("Radiant", 0.632),
        late=ModelVerdict("Dire", 0.542),
        all=ModelVerdict("Dire", 0.507),
        prematch=ModelVerdict("Dire", 0.796),
        kills30_radiant=0.658,
        kills30_dire=0.464,
    )
    result = evaluate(ctx, cfg())
    kills_total = [d for d in result.decisions if d.market == "kills_total"]
    assert len(kills_total) == 1
    decision = kills_total[0]
    assert decision.target_side == "Radiant"
    assert decision.rule == md.RULE_KILLS_EARLY_WIN_KILLS30
    assert sorted(decision.models_for) == ["early_win", "kills30"]
    assert not [s for s in result.skipped
                if s.market == "kills_total" and s.reason == md.REASON_KILLS30_BELOW]


def test_kills_early_threshold_is_inclusive_at_060():
    ctx_below = _nemesis_ctx(kills30_radiant=0.599)
    result_below = evaluate(ctx_below, cfg())
    assert not [d for d in result_below.decisions if d.market == "kills_total"]
    below = [s for s in result_below.skipped
             if s.market == "kills_total" and s.side == "Radiant"
             and s.reason == md.REASON_KILLS30_BELOW]
    assert len(below) == 1 and "(kills_early)" in below[0].detail

    ctx_at = _nemesis_ctx(kills30_radiant=0.60)
    result_at = evaluate(ctx_at, cfg())
    fired = [d for d in result_at.decisions if d.market == "kills_total"]
    assert len(fired) == 1 and fired[0].target_side == "Radiant"


def test_kills_early_nw_alone():
    ctx = base_ctx(
        elo_radiant=1500.0, elo_dire=1500.0,
        early_nw=ModelVerdict("Radiant", 0.70),
        kills30_radiant=0.65,
    )
    result = evaluate(ctx, cfg())
    kills_total = [d for d in result.decisions if d.market == "kills_total"]
    assert len(kills_total) == 1
    assert kills_total[0].rule == md.RULE_KILLS_EARLY_NW_KILLS30
    assert sorted(kills_total[0].models_for) == ["early_nw", "kills30"]

    # Early Win present but below cfg.min_conf must not change the outcome.
    ctx_weak_win = base_ctx(
        elo_radiant=1500.0, elo_dire=1500.0,
        early_nw=ModelVerdict("Radiant", 0.70),
        early_win=ModelVerdict("Radiant", 0.55),
        kills30_radiant=0.65,
    )
    result_weak_win = evaluate(ctx_weak_win, cfg())
    kills_total_weak = [d for d in result_weak_win.decisions if d.market == "kills_total"]
    assert len(kills_total_weak) == 1
    assert kills_total_weak[0].rule == md.RULE_KILLS_EARLY_NW_KILLS30
    assert sorted(kills_total_weak[0].models_for) == ["early_nw", "kills30"]


def test_kills_early_win_side_overrides_opposite_nw_side():
    ctx = base_ctx(
        elo_radiant=1500.0, elo_dire=1500.0,
        early_nw=ModelVerdict("Radiant", 0.70),
        early_win=ModelVerdict("Dire", 0.65),
        kills30_radiant=0.90,
        kills30_dire=0.65,
    )
    result = evaluate(ctx, cfg())
    kills_total = [d for d in result.decisions if d.market == "kills_total"]
    assert len(kills_total) == 1
    assert kills_total[0].target_side == "Dire"
    assert kills_total[0].rule == md.RULE_KILLS_EARLY_WIN_KILLS30
    assert not [d for d in result.decisions
                if d.market == "kills_total" and d.target_side == "Radiant"]


def test_kills_early_missing_kills30_is_fail_closed():
    ctx = base_ctx(
        elo_radiant=1500.0, elo_dire=1500.0,
        early_win=ModelVerdict("Radiant", 0.65),
        # kills30_radiant left at Ctx default (None)
    )
    result = evaluate(ctx, cfg())
    assert not [d for d in result.decisions if d.market == "kills_total"]
    missing = [s for s in result.skipped
               if s.market == "kills_total" and s.side == "Radiant"
               and s.reason == md.REASON_KILLS30_MISSING]
    assert len(missing) == 1 and "(kills_early)" in missing[0].detail


def test_kills_early_disabled_and_env_knobs():
    ctx = _nemesis_ctx()
    result = evaluate(ctx, cfg(kills_early_enabled=False))
    assert not [d for d in result.decisions if d.market == "kills_total"]
    assert any(s.market == "kills_total" and s.reason == md.REASON_NO_UNDERDOG
               for s in result.skipped)

    assert Config.from_env({"ML_DISPATCH_KILLS_EARLY": "0"}).kills_early_enabled is False
    defaults = Config.from_env({})
    assert defaults.kills_early_enabled is True
    assert defaults.kills_early_min_kills30 == 0.60
    assert Config.from_env(
        {"ML_DISPATCH_KILLS_EARLY_MIN_KILLS30": "0.65"}
    ).kills_early_min_kills30 == 0.65
    assert Config.from_env(
        {"ML_DISPATCH_KILLS_EARLY_MIN_KILLS30": "garbage"}
    ).kills_early_min_kills30 == 0.60


def test_kills_early_no_duplicate_with_4_3_late_conflict():
    # Existing 4.3 scenario: the early-side path already resolves
    # kills_total for Radiant -- kills_early must be a no-op, not a second
    # decision or a rule override.
    ctx = base_ctx(
        game_time=700.0,
        elo_radiant=1500.0, elo_dire=1500.0,
        early_win=ModelVerdict("Radiant", 0.65),
        all=ModelVerdict("Radiant", 0.65),
        late=ModelVerdict("Dire", 0.70),
        kills_windows_open=["0-10"],
        kills30_radiant=0.99,
    )
    result = evaluate(ctx, cfg())
    kills_total = [d for d in result.decisions if d.market == "kills_total"]
    assert len(kills_total) == 1
    assert kills_total[0].rule == md.RULE_KILLS_LATE_CONFLICT_EARLY_SIDE


def test_kills_early_dedup():
    ctx = _nemesis_ctx()
    key = (ctx.base_url, ctx.map_num, "kills_total", "Radiant")
    ctx.already_sent = {key}
    result = evaluate(ctx, cfg())
    assert not [d for d in result.decisions if d.market == "kills_total"]
    dedup = [s for s in result.skipped
             if s.market == "kills_total" and s.side == "Radiant"
             and s.reason == md.REASON_DEDUP]
    assert len(dedup) == 1


def test_kills_early_one_kills_total_per_map_when_underdog_path_wins():
    # Dire is the ELO underdog and already clears its own gate (0.75 >=
    # 0.70); Early Win independently favors Radiant at kills30=0.90, which
    # would qualify for kills_early on its own -- but only one kills_total
    # per map is allowed, and the underdog path got there first.
    ctx = base_ctx(
        elo_radiant=1600.0, elo_dire=1500.0,  # Dire is the underdog
        early_nw=ModelVerdict("Dire", 0.65),
        early_win=ModelVerdict("Radiant", 0.65),
        kills30_radiant=0.90,
        kills30_dire=0.75,
    )
    result = evaluate(ctx, cfg())
    kills_total = [d for d in result.decisions if d.market == "kills_total"]
    assert len(kills_total) == 1
    assert kills_total[0].target_side == "Dire"
    assert kills_total[0].rule == "kills_underdog_total"
    assert not [d for d in result.decisions
                if d.market == "kills_total" and d.target_side == "Radiant"]
