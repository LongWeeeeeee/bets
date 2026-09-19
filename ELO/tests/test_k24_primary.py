from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from ELO import array_model, state_overlay
from ELO import live_team_strength as live
from ELO.config import HybridEloConfig
from ELO.domain import LeagueTier, MatchRecord
from ELO.models import K24_HISTORY_RETENTION_SECONDS, K24_SCHEMA_VERSION, HybridPlayerRosterEloModel, k24_lineup_summary
from ELO.replay import result_record


def _match(*, timestamp: int = 1_000, radiant_win: bool = True) -> MatchRecord:
    return MatchRecord(
        match_id=timestamp,
        timestamp=timestamp - 100,
        radiant_win=radiant_win,
        radiant_team_id=1,
        radiant_team_name="Radiant",
        dire_team_id=2,
        dire_team_name="Dire",
        radiant_player_ids=(1, 2, 3, 4, 5),
        dire_player_ids=(6, 7, 8, 9, 10),
        league_id=1,
        league_name="fixture",
        source_league_tier=None,
        series_id=None,
        series_type=None,
        derived_league_tier=LeagueTier.TIER1,
    )


def _summary(model, timestamp=2_000):
    return k24_lineup_summary(
        model,
        radiant_team_name="Radiant",
        dire_team_name="Dire",
        radiant_account_ids=[1, 2, 3, 4, 5],
        dire_account_ids=[6, 7, 8, 9, 10],
        timestamp=timestamp,
    )


def test_k24_formula_is_independent_of_hybrid_tiers_and_bonuses():
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    assert _summary(model)["elo_diff"] == 0.0
    # K24 changes only on the model's completed-result update path.
    model.preview_team_strength(
        team_id=1, team_name="Radiant", player_ids=(1, 2, 3, 4, 5),
        tier=LeagueTier.TIER1, timestamp=900,
    )
    assert _summary(model)["elo_diff"] == 0.0

    model.process_match(result_record(_match(), 1_000))
    summary = _summary(model)
    assert summary["source"] == "elo_composition_k24"
    assert summary["elo_diff"] == pytest.approx(24.0)
    assert summary["radiant_win_prob"] == pytest.approx(1 / (1 + 10 ** (-24 / 400)))
    assert model.player_k24[1] == pytest.approx(1512.0)
    assert model.player_k24[6] == pytest.approx(1488.0)

    nondefault = HybridPlayerRosterEloModel(replace(HybridEloConfig(), initial_rating=777.0))
    assert _summary(nondefault)["radiant"]["rating"] == pytest.approx(1500.0)


def test_k24_asof_is_strict_and_refuses_unretained_state():
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    model.process_match(result_record(_match(), 1_000))
    assert _summary(model, 1_000)["elo_diff"] == 0.0  # result at query is excluded
    assert _summary(model, 1_001)["elo_diff"] == pytest.approx(24.0)
    assert _summary(model, -1) is None

    # A result older than retained history cannot be inserted safely.
    latest = 1_000 + K24_HISTORY_RETENTION_SECONDS
    model.process_match(result_record(_match(timestamp=latest), latest))
    model.process_match(result_record(_match(timestamp=900), 900))
    assert model.k24_available is False
    assert _summary(model, 2_000) is None


def test_k24_late_result_recomputes_suffix_like_chronological_replay(tmp_path: Path):
    first = result_record(_match(), 1_000)
    late = result_record(replace(_match(timestamp=1_100),
        radiant_player_ids=(6, 7, 8, 9, 10), dire_player_ids=(11, 12, 13, 14, 15)), 1_100)
    last = result_record(_match(timestamp=1_200, radiant_win=False), 1_200)
    chronological = HybridPlayerRosterEloModel(HybridEloConfig())
    staged = HybridPlayerRosterEloModel(HybridEloConfig())
    for match in (first, late, last):
        chronological.process_match(match)
    for match in (first, last):
        staged.process_match(match)
    state = staged.export_state()
    path = tmp_path / "late_snapshot.json"
    path.write_text(json.dumps({"model_state": state}))
    restored = HybridPlayerRosterEloModel.from_state(state)
    overlay = array_model.build_overlay_model(path, None)
    for candidate in (staged, restored, overlay):
        candidate.process_match(late)
        assert candidate.k24_available
        for player in range(1, 16):
            assert candidate.player_k24[player] == pytest.approx(chronological.player_k24[player], abs=1e-9)
        for timestamp in (1_000, 1_100, 1_101, 1_200, 1_201):
            assert _summary(candidate, timestamp)["elo_diff"] == pytest.approx(
                _summary(chronological, timestamp)["elo_diff"], abs=1e-9)
        assert candidate.validate_k24_state()


def test_k24_legacy_or_invalid_roster_never_defaults_to_1500():
    legacy = HybridPlayerRosterEloModel.from_state({"config": {}})
    assert legacy.k24_available is False
    assert _summary(legacy) is None

    model = HybridPlayerRosterEloModel(HybridEloConfig())
    assert k24_lineup_summary(
        model, radiant_team_name="A", dire_team_name="B", radiant_account_ids=[1, 2, 3, 4],
        dire_account_ids=[6, 7, 8, 9, 10], timestamp=1,
    ) is None
    assert k24_lineup_summary(
        model, radiant_team_name="A", dire_team_name="B", radiant_account_ids=[1, 2, 3, 4, 5],
        dire_account_ids=[5, 6, 7, 8, 9], timestamp=1,
    ) is None

    malformed = model.export_state()
    malformed["k24_highwater_timestamp"] = 10
    malformed["k24_history"] = [{"timestamp": "bad"}]
    assert HybridPlayerRosterEloModel.from_state(malformed).k24_available is False


def test_k24_corrupt_full_or_array_state_is_unavailable_not_defaulted(tmp_path: Path):
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    model.process_match(result_record(_match(), 1_000))

    missing_highwater = model.export_state()
    missing_highwater["k24_highwater_timestamp"] = None
    restored = HybridPlayerRosterEloModel.from_state(missing_highwater)
    assert restored.k24_available is False
    assert _summary(restored, 1_000) is None

    for invalid_scalar in (1.9, True, "1"):
        fractional_id = model.export_state()
        fractional_id["k24_history"][0]["radiant_player_ids"][0] = invalid_scalar
        assert HybridPlayerRosterEloModel.from_state(fractional_id).k24_available is False
        corrupt_path = tmp_path / f"history_id_{invalid_scalar}.json"
        corrupt_path.write_text(json.dumps({"model_state": fractional_id}))
        assert array_model.build_read_model(corrupt_path).k24_available is False
    fractional_time = model.export_state()
    fractional_time["k24_history"][0]["timestamp"] = 1_000.9
    assert HybridPlayerRosterEloModel.from_state(fractional_time).k24_available is False
    corrupt_path = tmp_path / "history_time.json"
    corrupt_path.write_text(json.dumps({"model_state": fractional_time}))
    assert array_model.build_read_model(corrupt_path).k24_available is False

    contradictory = model.export_state()
    contradictory["k24_history"][0]["radiant_win"] = False
    assert HybridPlayerRosterEloModel.from_state(contradictory).k24_available is False

    missing_players = model.export_state()
    del missing_players["player_k24"]
    assert HybridPlayerRosterEloModel.from_state(missing_players).k24_available is False

    snapshot_path = tmp_path / "missing_players.json"
    snapshot_path.write_text(json.dumps({"meta": {"reference_timestamp": 1_000}, "model_state": missing_players}))
    array_restored = array_model.build_read_model(snapshot_path)
    assert array_restored.k24_available is False
    assert _summary(array_restored, 2_000) is None

    missing_history_player = model.export_state()
    del missing_history_player["player_k24"]["1"]
    assert HybridPlayerRosterEloModel.from_state(missing_history_player).k24_available is False

    for broken_players in ({"bad": 1_500.0}, {"1": float("nan")},
                           {"1": True}, {"1": None}, {"1": []}, {"1": {}}):
        broken = HybridPlayerRosterEloModel(HybridEloConfig()).export_state()
        broken["player_k24"] = broken_players
        assert HybridPlayerRosterEloModel.from_state(broken).k24_available is False
        broken_path = tmp_path / f"broken_{len(broken_players)}_{str(next(iter(broken_players)))}.json"
        broken_path.write_text(json.dumps({"meta": {"reference_timestamp": 1_000}, "model_state": broken}))
        assert array_model.build_read_model(broken_path).k24_available is False

    forged_empty_history = HybridPlayerRosterEloModel(HybridEloConfig()).export_state()
    forged_empty_history["player_k24"] = {"1": 1_600.0}
    assert HybridPlayerRosterEloModel.from_state(forged_empty_history).k24_available is False


def test_k24_overlay_rejects_corrupt_temporal_metadata(tmp_path: Path):
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    model.process_match(result_record(_match(), 1_000))
    state = model.export_state()
    snapshot_path = tmp_path / "snapshot.json"
    delta_path = tmp_path / "delta.json"
    signature = live._model_config_signature(state)
    snapshot_path.write_text(json.dumps({
        "meta": {"reference_timestamp": 1_000, "model_config_signature": signature},
        "model_state": state,
    }))
    broken_parts = state_overlay.collect_small_parts(model)
    broken_parts["k24_highwater_timestamp"] = None
    state_overlay.save_delta(
        delta_path, base_reference_timestamp=1_000, base_model_config_signature=signature,
        changes={}, resets={}, small_parts=broken_parts, updated_at=1_001,
    )
    overlay = array_model.build_overlay_model(snapshot_path, delta_path)
    assert overlay.k24_available is False
    assert _summary(overlay, 2_000) is None


def test_full_state_converter_preserves_live_k24_asof_history(tmp_path: Path):
    from ELO.convert_state_to_delta import main as convert

    model = HybridPlayerRosterEloModel(HybridEloConfig())
    model.process_match(result_record(_match(), 1_000))
    base_state = model.export_state()
    signature = live._model_config_signature(base_state)
    snapshot = tmp_path / "snapshot.json"
    runtime = tmp_path / "runtime.json"
    delta = tmp_path / "delta.json"
    snapshot.write_text(json.dumps({
        "meta": {"reference_timestamp": 1_000, "model_config_signature": signature},
        "model_state": base_state,
    }))
    model.process_match(result_record(_match(timestamp=1_100, radiant_win=False), 1_100))
    runtime.write_text(json.dumps({
        "base_reference_timestamp": 1_000,
        "base_model_config_signature": signature,
        "model_state": model.export_state(),
    }))
    assert convert(["--snapshot", str(snapshot), "--state", str(runtime), "--delta", str(delta)]) == 0
    overlay = array_model.build_overlay_model(snapshot, delta)
    assert overlay.k24_available
    assert overlay.k24_highwater_timestamp == 1_100
    assert list(overlay.k24_history) == list(model.k24_history)
    for timestamp in (1_000, 1_001, 1_100, 1_101):
        assert _summary(overlay, timestamp)["elo_diff"] == pytest.approx(_summary(model, timestamp)["elo_diff"])


def test_k24_side_swap_reverses_sign_only():
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    model.process_match(result_record(_match(), 1_000))
    normal = _summary(model)
    swapped = k24_lineup_summary(
        model, radiant_team_name="Dire", dire_team_name="Radiant",
        radiant_account_ids=[6, 7, 8, 9, 10], dire_account_ids=[1, 2, 3, 4, 5], timestamp=2_000,
    )
    assert normal["elo_diff"] == pytest.approx(-swapped["elo_diff"])
    assert normal["radiant_win_prob"] == pytest.approx(1 - swapped["radiant_win_prob"])


def test_k24_state_array_and_overlay_roundtrip_do_not_alias_history(tmp_path: Path):
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    model.process_match(result_record(_match(), 1_000))
    exported = model.export_state()
    exported["k24_history"][0]["delta"] = 0.0
    assert model.k24_history[0]["delta"] == pytest.approx(12.0)

    state = model.export_state()
    restored = HybridPlayerRosterEloModel.from_state(state)
    assert restored.player_k24 == model.player_k24
    restored.k24_history[0]["delta"] = 0.0
    assert model.k24_history[0]["delta"] == pytest.approx(12.0)

    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_text(json.dumps({"meta": {"reference_timestamp": 1_000}, "model_state": state}))
    array_model.save_state_arrays(snapshot_path)
    array_restored = array_model.build_read_model(snapshot_path)
    assert array_restored.k24_schema_version == K24_SCHEMA_VERSION
    assert array_restored.player_k24[1] == pytest.approx(1512.0)

    wrappers = state_overlay.wrap_model(array_restored)
    array_restored.player_k24[1] += 3.0
    parts = state_overlay.collect_small_parts(array_restored)
    changes = state_overlay.collect_changes(wrappers)
    assert changes["player_k24"] == [[1, pytest.approx(1515.0)]]
    parts["k24_history"][0]["delta"] = 0.0
    assert array_restored.k24_history[0]["delta"] == pytest.approx(12.0)


def test_live_summary_refuses_legacy_or_invalid_live_source_and_marks_live_metadata(monkeypatch):
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    snapshot = {
        "meta": {"reference_timestamp": 1_000}, "teams_by_org_key": {},
        "model_state": model.export_state(),
    }
    monkeypatch.setattr(live, "ensure_snapshot", lambda **kwargs: snapshot)
    monkeypatch.setattr(live, "_restore_model_from_snapshot", lambda current: model)
    monkeypatch.setattr(live, "_snapshot_with_runtime_model_state", lambda current, **kwargs: current)
    summary = live.get_matchup_summary(
        radiant_team_id=1, dire_team_id=2, radiant_team_name="Radiant", dire_team_name="Dire",
        radiant_account_ids=[1, 2, 3, 4, 5], dire_account_ids=[6, 7, 8, 9, 10], timestamp=1_000,
    )
    assert summary["rating_source_metadata"]["kind"] == "base_snapshot"
    assert summary["radiant"]["leaderboard_rank"] is None  # no causal rank at snapshot time

    monkeypatch.setattr(
        live, "_snapshot_with_runtime_model_state",
        lambda current, **kwargs: {**current, live.LIVE_RUNTIME_UNAVAILABLE_MARKER: True},
    )
    assert live.get_matchup_summary(
        radiant_team_id=1, dire_team_id=2, radiant_team_name="Radiant", dire_team_name="Dire",
        radiant_account_ids=[1, 2, 3, 4, 5], dire_account_ids=[6, 7, 8, 9, 10], timestamp=1_001,
    ) is None


def test_k24_rank_map_uses_k24_strength_not_hybrid_team_strength():
    snapshot = {"teams_by_org_key": {
        "hybrid_first": {"team_name": "A", "current_strength": 2_000.0, "k24_strength": 1_400.0},
        "k24_first": {"team_name": "B", "current_strength": 1_000.0, "k24_strength": 1_600.0},
    }}
    assert live._k24_leaderboard_rank_map(snapshot) == {"k24_first": 1, "hybrid_first": 2}


def test_array_runtime_accepts_snapshot_history_signature_not_config_only(tmp_path: Path):
    base = HybridPlayerRosterEloModel(HybridEloConfig())
    runtime = HybridPlayerRosterEloModel(HybridEloConfig())
    runtime.process_match(result_record(_match(), 1_000))
    snapshot_path = tmp_path / "snapshot.json"
    runtime_path = tmp_path / "runtime.json"
    snapshot_path.write_text(json.dumps({
        "meta": {"reference_timestamp": 1_000, "model_config_signature": "history-bound-signature"},
        "model_state": base.export_state(),
    }))
    runtime_path.write_text(json.dumps({
        "base_reference_timestamp": 1_000,
        "base_model_config_signature": "history-bound-signature",
        "model_state": runtime.export_state(),
    }))
    restored = array_model.build_read_model(snapshot_path, runtime_path)
    assert restored.player_k24[1] == pytest.approx(1512.0)
