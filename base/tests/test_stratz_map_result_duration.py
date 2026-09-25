"""Stratz's duration survives both cache publication paths."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from base import stratz_map_result as stratz
from ELO import live_team_strength as live
from ELO.config import HybridEloConfig
from ELO.domain import LeagueTier, MatchRecord
from ELO.models import HybridPlayerRosterEloModel


def test_series_history_keeps_duration_from_its_own_finished_map(tmp_path):
    # The transport shape matches the project's _QUERY; ids and values exercise
    # adjacent maps without claiming this constructed response was captured live.
    rows = [
        {"id": 8960577698, "seriesId": 1133004, "startDateTime": 1787464800,
         "endDateTime": 1787467200, "durationSeconds": 2400,
         "radiantTeamId": 7119388, "direTeamId": 9572001, "didRadiantWin": True},
        {"id": 8960655084, "seriesId": 1133004, "startDateTime": 1787470560,
         "endDateTime": 1787472437, "durationSeconds": 1877,
         "radiantTeamId": 9572001, "direTeamId": 7119388, "didRadiantWin": True},
    ]
    cache = tmp_path / "stratz.json"
    query = lambda team_id, since: rows
    history = stratz.series_history(9572001, 7119388, now=1787473000,
                                    cache_path=cache, query=query)
    assert [m["duration_seconds"] for m in history] == [2400, 1877]
    assert history[1]["match_id"] == 8960655084

    rows[1]["durationSeconds"] = 0
    stratz.refresh([9572001], now=1787473200, cache_path=cache, query=query)
    refreshed = stratz.team_matches(9572001, now=1787473200, cache_path=cache, query=query)
    assert refreshed[1]["duration_seconds"] is None


@pytest.mark.parametrize("winner_slot,expected_duration", [("first", 2400), ("second", None)])
def test_score_advance_uses_stale_cache_without_query(tmp_path, monkeypatch, winner_slot, expected_duration):
    base_dir = Path(__file__).resolve().parents[1]
    if str(base_dir) not in sys.path:
        sys.path.insert(0, str(base_dir))
    import cyberscore_try as cs

    row = {"id": 8960577698, "seriesId": 1133004, "startDateTime": 1787464800,
           "endDateTime": 1787467200, "durationSeconds": 2400,
           "radiantTeamId": 7119388, "direTeamId": 9572001, "didRadiantWin": True}
    cache = tmp_path / "stratz.json"
    monkeypatch.setattr(stratz, "resolve_team_id", lambda team_id, **kw: team_id)
    stratz.team_matches(7119388, now=1787473000, cache_path=cache,
                        query=lambda team_id, since: [row])
    calls = []

    def counted_query(team_id, since):
        calls.append(team_id)
        return [row]

    original_history = stratz.series_history
    monkeypatch.setattr(stratz, "series_history", lambda rad, dire, **kw: original_history(
        rad, dire, now=1787473121, cache_path=cache,
        query=kw.get("query", counted_query)))
    monkeypatch.setitem(sys.modules, "stratz_map_result", stratz)
    pending = {"map_key": "map-1", "first_team_is_radiant": True,
               "match_record": {"match_id": row["id"], "radiant_team_id": row["radiantTeamId"],
                                "dire_team_id": row["direTeamId"]}}
    monkeypatch.setattr(live, "_apply_one_pending_map", lambda **kw: kw)

    _, updates = live._drain_pending_map_queue(
        pending_maps=[pending], previous_scores={"first": 0, "second": 0},
        current_scores={"first": int(winner_slot == "first"),
                        "second": int(winner_slot == "second")},
        applied_maps={}, snapshot={}, model_getter=lambda: None,
        normalized_series_key="series", series_url="series",
        winner_lookup=lambda key, pm, *, cache_only=False: cs._live_elo_winner_lookup(
            key, pm, with_duration=True, **({"cache_only": True} if cache_only else {})),
        score_duration_lookup=lambda key, pm: cs._live_elo_winner_lookup(
            key, pm, with_duration=True, cache_only=True),
    )
    assert len(updates) == 1
    assert updates[0]["duration_seconds"] == expected_duration
    assert calls == []


@pytest.mark.parametrize("duration_case", ["positive", "missing", "zero"])
def test_live_between_map_update_uses_the_matched_maps_duration(tmp_path, monkeypatch, duration_case):
    """A raw captured Stratz response drives lookup and the A update."""
    base_dir = Path(__file__).resolve().parents[1]
    if str(base_dir) not in sys.path:
        sys.path.insert(0, str(base_dir))
    import cyberscore_try as cs

    # The raw response contains no player accounts; a separate real lineup
    # supplies them for the rating update.
    accounts = json.loads((base_dir.parent / "ELO/tests/variant_a_real_maps_20260925.json").read_text())["maps"][0]
    captured = json.loads((base_dir / "tests/fixtures/stratz_series_history_9572001_20260925.json").read_text())
    raw_rows = captured["team"]["matches"]
    first_raw = next(m for m in raw_rows if m["id"] == 8960577698)
    next_raw = next(m for m in raw_rows if m["id"] == 8960655084)
    row = {
        "match_id": first_raw["id"], "start": first_raw["startDateTime"],
        "end": first_raw["endDateTime"],
        "radiant_win": first_raw["didRadiantWin"],
        "radiant_player_ids": accounts["radiant_player_ids"],
        "dire_player_ids": accounts["dire_player_ids"],
    }
    duration = first_raw["durationSeconds"] if duration_case == "positive" else None
    if duration_case == "missing":
        first_raw.pop("durationSeconds")
    elif duration_case == "zero":
        first_raw["durationSeconds"] = 0
    data_dir = tmp_path / "source"
    data_dir.mkdir()
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(json.dumps({
        "meta": {**live._rating_replay_meta(), "reference_timestamp": row["start"] - 1},
        "teams_by_org_key": {}, "model_state": HybridPlayerRosterEloModel(HybridEloConfig()).export_state(),
    }))
    common = dict(
        series_key="425663", series_url="dltv.org/matches/425663",
        first_team_score=0, second_team_score=0, first_team_is_radiant=True,
        snapshot_path=snapshot, data_dir=data_dir, rebuild_if_missing=False,
        progress_path=tmp_path / "progress.json", runtime_model_state_path=tmp_path / "state.json",
        runtime_lock_path=tmp_path / "lock",
    )
    def record(raw):
        return MatchRecord(
            match_id=raw["id"], timestamp=raw["startDateTime"], radiant_win=False,
            radiant_team_id=raw["radiantTeamId"], radiant_team_name="Radiant",
            dire_team_id=raw["direTeamId"], dire_team_name="Dire",
            radiant_player_ids=tuple(row["radiant_player_ids"]),
            dire_player_ids=tuple(row["dire_player_ids"]), league_id=1,
            league_name="fixture", source_league_tier=None, series_id=raw["seriesId"],
            series_type="3", derived_league_tier=LeagueTier.TIER1)

    queries = []
    def query(team_id, since):
        queries.append(team_id)
        return captured["team"]["matches"]
    original_history = stratz.series_history
    monkeypatch.setattr(stratz, "series_history", lambda rad, dire: original_history(
        rad, dire, now=next_raw["startDateTime"], cache_path=tmp_path / "stratz.json",
        query=query))
    monkeypatch.setattr(stratz, "resolve_team_id", lambda team_id, **kw: team_id)
    monkeypatch.setitem(sys.modules, "stratz_map_result", stratz)

    first = live.register_live_map_context(map_key=f"dltv.org/matches/{row['match_id']}.0",
                                           match_record=record(first_raw), **common)
    assert first is not None
    result = live.register_live_map_context(
        map_key=f"dltv.org/matches/{next_raw['id']}.0",
        match_record=record(next_raw),
        winner_lookup=lambda key, pending: cs._live_elo_winner_lookup(
            key, pending, with_duration=True), **common)
    assert result is not None
    assert result["applied_update"] is not None
    assert queries, "score-independent winner fallback must still query Stratz"
    state = json.loads((tmp_path / "state.json").read_text())["model_state"]
    model = HybridPlayerRosterEloModel.from_state(state)
    multiplier = max(0.6, min(1.6, 1981 / max(duration, 600))) if duration else 1.0
    expected = 1500 + (36 if row["radiant_win"] else -36) * multiplier
    assert model.player_a[row["radiant_player_ids"][0]] == pytest.approx(expected)
    applied = json.loads((tmp_path / "progress.json").read_text())["applied_maps"]
    assert applied[f"dltv.org/matches/{row['match_id']}.0"]["duration_seconds"] == duration
