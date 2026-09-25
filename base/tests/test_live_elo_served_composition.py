"""Telegram and dispatch consume the same served team composition."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402
from base import ml_dispatch  # noqa: E402
from ELO import live_team_strength as live  # noqa: E402
from ELO.config import HybridEloConfig  # noqa: E402
from ELO.domain import LeagueTier, MatchRecord  # noqa: E402
from ELO.models import HybridPlayerRosterEloModel  # noqa: E402
from ELO.replay import result_record  # noqa: E402


def test_invalid_composition_warns_once_and_serves_k24(monkeypatch, caplog):
    monkeypatch.setenv("ELO_SERVED_COMPOSITION", "  UNKNOWN  ")
    monkeypatch.setattr(live, "_INVALID_COMPOSITION_LOGGED", False, raising=False)
    assert live._served_composition() == "k24"
    assert live._served_composition() == "k24"
    assert caplog.text.count("ELO_SERVED_COMPOSITION") == 1


def test_composition_normalizes_case_and_spaces(monkeypatch):
    monkeypatch.setenv("ELO_SERVED_COMPOSITION", "  K24  ")
    assert live._served_composition() == "k24"


def test_composition_normalizes_padded_a(monkeypatch, caplog):
    # "  K24  " falls back to k24 anyway, so only a padded "A" proves that
    # the surrounding whitespace is stripped rather than the value rejected.
    monkeypatch.setenv("ELO_SERVED_COMPOSITION", "  A  ")
    monkeypatch.setattr(live, "_INVALID_COMPOSITION_LOGGED", False, raising=False)
    assert live._served_composition() == "a"
    assert "ELO_SERVED_COMPOSITION" not in caplog.text


@pytest.mark.parametrize("setting,composition,label", [(None, "a", "A"), ("k24", "k24", "K24")])
def test_telegram_and_dispatch_read_served_ratings(tmp_path, monkeypatch, setting, composition, label):
    row = json.loads((BASE_DIR.parent / "ELO/tests/variant_a_real_maps_20260925.json").read_text())["maps"][0]
    match = MatchRecord(
        match_id=row["match_id"], timestamp=row["start"], radiant_win=row["radiant_win"],
        radiant_team_id=1, radiant_team_name="Radiant", dire_team_id=2, dire_team_name="Dire",
        radiant_player_ids=tuple(row["radiant_player_ids"]),
        dire_player_ids=tuple(row["dire_player_ids"]), league_id=1, league_name="fixture",
        source_league_tier=None, series_id=None, series_type=None,
        derived_league_tier=LeagueTier.TIER1, duration_seconds=row["duration_seconds"],
    )
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    model.process_match(result_record(match, row["end"]), duration_seconds=row["duration_seconds"])
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(json.dumps({
        "meta": {**live._rating_replay_meta(), "reference_timestamp": row["end"]},
        "teams_by_org_key": {}, "model_state": model.export_state(),
    }))
    if setting is None:
        monkeypatch.delenv("ELO_SERVED_COMPOSITION", raising=False)
    else:
        monkeypatch.setenv("ELO_SERVED_COMPOSITION", setting)
    monkeypatch.setattr(cs, "ELO_LIVE_SNAPSHOT_AVAILABLE", True)
    monkeypatch.setattr(cs, "_elo_live_get_matchup_summary", lambda **kw: live.get_matchup_summary(
        **{**kw, "snapshot_path": snapshot, "data_dir": tmp_path, "rebuild_if_missing": False,
           "runtime_model_state_path": tmp_path / "absent.json"}))
    summary = cs._build_team_elo_matchup_summary_from_live_snapshot(
        1, 2, "Radiant", "Dire", list(match.radiant_player_ids), list(match.dire_player_ids),
        timestamp=row["end"] + 1)
    assert summary is not None and summary["composition"] == composition
    block, meta = cs._format_team_elo_block(summary, radiant_team_name="Radiant", dire_team_name="Dire")
    assert block.startswith(f"ELO состава ({label}):")
    assert block.splitlines()[1:3] == [
        f"Radiant: {summary['radiant']['base_rating']:.0f}",
        f"Dire: {summary['dire']['base_rating']:.0f}",
    ]
    baseline = (BASE_DIR / "tests/fixtures/elo_delivery_boundary_k24_20260925.txt").read_text()
    if composition == "k24":
        assert block == baseline
    else:
        assert block != baseline
    assert meta["radiant_base_rating"] == pytest.approx(summary["radiant"]["base_rating"])
    assert meta["dire_base_rating"] == pytest.approx(summary["dire"]["base_rating"])

    captured = []
    monkeypatch.setattr(cs, "dispatch_mode", lambda: "audit")
    monkeypatch.setattr(cs, "_ml_dispatch_sent_ledger", lambda: SimpleNamespace(as_set=lambda: set()))
    monkeypatch.setattr(cs, "_ml_dispatch_record_decisions", lambda *args, **kwargs: None)
    monkeypatch.setattr(ml_dispatch, "evaluate", lambda ctx, cfg: (
        captured.append(ctx) or SimpleNamespace(decisions=[], skipped=[], elo_diff=0, underdog_side=None)))
    cs._ml_dispatch_tick(
        match_key="fixture", radiant_team_name="Radiant", dire_team_name="Dire",
        live_league={}, top=None, mid=None, bot=None, protracker_payload=None,
        team_elo_block=block, team_elo_meta=meta, game_time_seconds=61, radiant_lead=0,
    )
    assert len(captured) == 1
    assert captured[0].elo_radiant == pytest.approx(summary["radiant"]["base_rating"])
    assert captured[0].elo_dire == pytest.approx(summary["dire"]["base_rating"])


def test_score_duration_wrappers_stay_cache_only(tmp_path, monkeypatch):
    """Both score-advance wrappers must pass a cache-only duration lookup.

    The live betting loop never makes a synchronous Stratz call
    (`stratz_map_result._post` walks proxy pairs at 5 s each); the duration
    for a score advance comes from the background-warmed cache, even stale.
    """
    from base import stratz_map_result as stratz

    radiant_id, dire_id = 7119388, 9572001
    match_id, duration = 8960577698, 2400
    row = {"id": match_id, "seriesId": 1133004, "startDateTime": 1787464800,
           "endDateTime": 1787467200, "durationSeconds": duration,
           "radiantTeamId": radiant_id, "direTeamId": dire_id, "didRadiantWin": True}
    cache = tmp_path / "stratz.json"
    # Seed once, then read much later: only a STALE entry is present, so a
    # network query would be the only other way to answer.
    stratz.team_matches(radiant_id, now=1787473000, cache_path=cache,
                        query=lambda team_id, since: [row])
    monkeypatch.setattr(stratz, "DEFAULT_CACHE_PATH", cache)
    monkeypatch.setitem(sys.modules, "stratz_map_result", stratz)

    post_calls, query_calls, requests_calls = [], [], []

    def _no_post(query):
        post_calls.append(query)
        raise AssertionError("no Stratz network on the score-advance path")

    def _no_query(team_id, since):
        query_calls.append(team_id)
        raise AssertionError("no Stratz network on the score-advance path")

    def _no_requests_post(*args, **kwargs):
        requests_calls.append((args, kwargs))
        raise AssertionError("no Stratz network on the score-advance path")

    monkeypatch.setattr(stratz, "_post", _no_post)
    monkeypatch.setattr(stratz, "_query", _no_query)
    import requests
    monkeypatch.setattr(requests, "post", _no_requests_post)

    captured = {}

    def _fake_register(**kwargs):
        captured["register"] = kwargs
        return {}

    def _fake_finalize(**kwargs):
        captured["finalize"] = kwargs
        return {}

    monkeypatch.setattr(cs, "ELO_LIVE_SNAPSHOT_AVAILABLE", True)
    monkeypatch.setattr(cs, "_elo_live_register_map_context", _fake_register)
    monkeypatch.setattr(cs, "_elo_live_finalize_series_from_scores", _fake_finalize)

    cs._register_completed_live_map_for_elo(
        series_key="425663", series_url="dltv.org/matches/425663",
        map_key="dltv.org/matches/425663.1", first_team_score=0, second_team_score=0,
        first_team_is_radiant=True, map_match_id=match_id, observed_timestamp=1787467000,
        radiant_team_id=radiant_id, dire_team_id=dire_id,
        radiant_team_name="Radiant", dire_team_name="Dire",
        radiant_account_ids=[11, 12, 13, 14, 15], dire_account_ids=[21, 22, 23, 24, 25],
        league_id=1, league_name="fixture", series_type="bo3", match_tier=1)
    cs._finalize_finished_live_series_for_elo(
        series_key="425663", series_url="dltv.org/matches/425663",
        first_team_score=0, second_team_score=1)
    assert set(captured) == {"register", "finalize"}

    pending = {"map_key": "dltv.org/matches/425663.1",
               "match_record": live._serialize_match_record(
                   captured["register"]["match_record"]),
               "first_team_is_radiant": True}
    for name in ("register", "finalize"):
        lookup = captured[name]["score_duration_lookup"]
        # The way ELO calls it in _drain_pending_map_queue.looked_up.
        assert lookup(str(pending.get("map_key") or ""), pending) == {
            "radiant_won": True, "duration_seconds": duration}
    assert post_calls == [] and query_calls == [] and requests_calls == []
