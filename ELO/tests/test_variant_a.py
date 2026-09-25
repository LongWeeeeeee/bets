"""Variant A against captured real maps; reference math is intentionally local."""
from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from ELO import array_model, live_team_strength as live, state_overlay
from ELO.config import HybridEloConfig
from ELO.domain import LeagueTier, MatchRecord
from ELO.models import HybridPlayerRosterEloModel, a_lineup_summary, k24_lineup_summary
from ELO.replay import result_record


FIXTURE = json.loads((Path(__file__).parent / "variant_a_real_maps_20260925.json").read_text())


def _record(row):
    return MatchRecord(
        match_id=row["match_id"], timestamp=row["start"], radiant_win=row["radiant_win"],
        radiant_team_id=1, radiant_team_name="Radiant",
        dire_team_id=2, dire_team_name="Dire",
        radiant_player_ids=tuple(row["radiant_player_ids"]),
        dire_player_ids=tuple(row["dire_player_ids"]),
        league_id=1, league_name="fixture", source_league_tier=None,
        series_id=None, series_type=None, derived_league_tier=LeagueTier.TIER1,
        duration_seconds=row["duration_seconds"],
    )


def _reference(rows):
    ratings, games = {}, {}
    for row in rows:
        radiant, dire = row["radiant_player_ids"], row["dire_player_ids"]
        diff = (sum(ratings.get(i, 1500.0) for i in radiant)
                - sum(ratings.get(i, 1500.0) for i in dire)) / 5.0
        p = 1.0 / (1.0 + 10.0 ** (-diff / 400.0))
        duration = row["duration_seconds"]
        mult = max(0.6, min(1.6, 1981.0 / max(duration, 600.0))) if duration and duration > 0 else 1.0
        edge = diff if row["radiant_win"] else -diff
        k = 24.0 * mult * 2.2 / (2.2 + 0.001 * max(edge, -1000.0))
        d = (1.0 if row["radiant_win"] else 0.0) - p
        for ids, sign in ((radiant, 1), (dire, -1)):
            for player_id in ids:
                old = ratings.get(player_id, 1500.0)
                count = games.get(player_id, 0)
                ratings[player_id] = old + sign * k * (1.0 + 2.0 / (1.0 + count / 30.0)) * d
                games[player_id] = count + 1
    return ratings, games


def test_real_corpus_a_and_frozen_k24():
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    for row in FIXTURE["maps"]:
        match = _record(row)
        model.process_match(result_record(match, row["end"]), duration_seconds=row["duration_seconds"])
    ratings, games = _reference(FIXTURE["maps"])
    assert model.player_k24 == {int(k): v for k, v in FIXTURE["baseline_k24"].items()}
    assert model.player_a_games == games
    assert model.player_a.keys() == ratings.keys()
    for player_id, expected in ratings.items():
        assert math.isclose(model.player_a[player_id], expected, rel_tol=0, abs_tol=1e-10)


def _lineup(model, fn, timestamp, row):
    return fn(
        model, radiant_team_name="Radiant", dire_team_name="Dire",
        radiant_account_ids=row["radiant_player_ids"], dire_account_ids=row["dire_player_ids"],
        timestamp=timestamp,
    )


def test_a_asof_and_late_rewind_preserve_event_order():
    # Maps 0 and 42 share five real accounts, so rewinding map 42 changes A.
    rows = [FIXTURE["maps"][i] for i in (0, 42, 43)]
    chronological = HybridPlayerRosterEloModel(HybridEloConfig())
    late = HybridPlayerRosterEloModel(HybridEloConfig())
    for row in rows:
        chronological.process_match(result_record(_record(row), row["end"]),
                                    duration_seconds=row["duration_seconds"])
    for row in (rows[1], rows[2], rows[0]):
        late.process_match(result_record(_record(row), row["end"]),
                           duration_seconds=row["duration_seconds"])
    assert late.player_a_games == chronological.player_a_games
    for player_id, rating in chronological.player_a.items():
        assert late.player_a[player_id] == pytest.approx(rating, abs=1e-10)
    assert _lineup(late, a_lineup_summary, rows[0]["end"], rows[0])["elo_diff"] == 0.0
    assert _lineup(late, a_lineup_summary, rows[2]["end"] + 1, rows[2]) == _lineup(
        chronological, a_lineup_summary, rows[2]["end"] + 1, rows[2])


def test_a_sidecar_delta_and_old_snapshot_fallback(tmp_path, monkeypatch, caplog):
    rows = FIXTURE["maps"][:3]
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    for row in rows[:2]:
        model.process_match(result_record(_record(row), row["end"]),
                            duration_seconds=row["duration_seconds"])
    state = model.export_state()
    snapshot_path = tmp_path / "snapshot.json"
    snapshot = {"meta": {"reference_timestamp": rows[1]["end"]},
                "teams_by_org_key": {}, "model_state": state}
    snapshot_path.write_text(json.dumps(snapshot))
    array_model.save_state_arrays(snapshot_path)
    read_model = array_model.build_read_model(snapshot_path)
    assert read_model.a_available
    assert dict(read_model.player_a.items()) == pytest.approx(model.player_a)
    assert dict(read_model.player_a_games.items()) == model.player_a_games
    overlay = array_model.build_overlay_model(snapshot_path, None)
    overlay.process_match(result_record(_record(rows[2]), rows[2]["end"]),
                          duration_seconds=rows[2]["duration_seconds"])
    assert overlay.a_available
    changes = state_overlay.collect_changes(overlay._overlay_wrappers)
    parts = state_overlay.collect_small_parts(overlay)
    assert changes["player_a"] and changes["player_a_games"] and parts["a_history"]
    delta_path = tmp_path / "delta.json"
    state_overlay.save_delta(delta_path, base_reference_timestamp=rows[1]["end"],
                             base_model_config_signature=live._model_config_signature(state),
                             changes=changes, resets={}, small_parts=parts, updated_at=rows[2]["end"])
    snapshot["meta"]["model_config_signature"] = live._model_config_signature(state)
    snapshot_path.write_text(json.dumps(snapshot))
    array_model.save_state_arrays(snapshot_path)
    restored = array_model.build_overlay_model(snapshot_path, delta_path)
    assert restored.a_available
    assert _lineup(restored, a_lineup_summary, rows[2]["end"] + 1, rows[2])["radiant_win_prob"] == pytest.approx(
        _lineup(overlay, a_lineup_summary, rows[2]["end"] + 1, rows[2])["radiant_win_prob"])

    # A pre-migration state has neither A maps nor metadata. New code serves K24.
    legacy = dict(state)
    for key in list(legacy):
        if key.startswith("a_") or key.startswith("player_a"):
            del legacy[key]
    old = HybridPlayerRosterEloModel.from_state(legacy)
    assert not old.a_available and old.k24_available
    legacy_path = tmp_path / "legacy_snapshot.json"
    legacy_path.write_text(json.dumps({"meta": snapshot["meta"], "model_state": legacy}))
    array_model.save_state_arrays(legacy_path)
    old_array = array_model.build_read_model(legacy_path)
    assert not old_array.a_available and old_array.k24_available
    monkeypatch.setenv("ELO_SERVED_COMPOSITION", "a")
    monkeypatch.setattr(live, "ensure_snapshot", lambda **kwargs: {**snapshot, "model_state": legacy})
    monkeypatch.setattr(live, "_restore_model_from_snapshot", lambda _: old)
    monkeypatch.setattr(live, "_snapshot_with_runtime_model_state", lambda current, **kwargs: current)
    monkeypatch.setattr(live, "_A_MISSING_LOGGED", False)
    for _ in range(2):
        served = live.get_matchup_summary(
            radiant_team_id=1, dire_team_id=2, radiant_team_name="Radiant", dire_team_name="Dire",
            radiant_account_ids=rows[0]["radiant_player_ids"], dire_account_ids=rows[0]["dire_player_ids"],
            timestamp=rows[1]["end"] + 1,
        )
        assert served["composition"] == "k24"
        assert served["radiant_win_prob"] == _lineup(old, k24_lineup_summary, rows[1]["end"] + 1, rows[0])["radiant_win_prob"]
    assert caplog.text.count("A composition state is missing") == 1


def test_served_switch_selects_a_and_k24(tmp_path, monkeypatch):
    row = FIXTURE["maps"][0]
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    model.process_match(result_record(_record(row), row["end"]), duration_seconds=row["duration_seconds"])
    snapshot = {"meta": {"reference_timestamp": row["end"]}, "teams_by_org_key": {},
                "model_state": model.export_state()}
    monkeypatch.setattr(live, "ensure_snapshot", lambda **kwargs: snapshot)
    monkeypatch.setattr(live, "_restore_model_from_snapshot", lambda _: model)
    monkeypatch.setattr(live, "_snapshot_with_runtime_model_state", lambda current, **kwargs: current)
    kwargs = dict(radiant_team_id=1, dire_team_id=2, radiant_team_name="Radiant", dire_team_name="Dire",
                  radiant_account_ids=row["radiant_player_ids"], dire_account_ids=row["dire_player_ids"],
                  timestamp=row["end"] + 1)
    for composition, fn in (("a", a_lineup_summary), ("k24", k24_lineup_summary)):
        monkeypatch.setenv("ELO_SERVED_COMPOSITION", composition)
        served = live.get_matchup_summary(**kwargs)
        assert served["composition"] == composition
        assert served["radiant_win_prob"] == _lineup(model, fn, row["end"] + 1, row)["radiant_win_prob"]


def test_live_apply_duration_parameter_controls_only_a():
    row = FIXTURE["maps"][0]
    outputs = []
    for duration in (None, 0, row["duration_seconds"]):
        model = HybridPlayerRosterEloModel(HybridEloConfig())
        live._build_live_applied_update(
            snapshot={"meta": {}}, model=model, match=_record(row), map_key="test-map",
            series_key="test-series", series_url="test", winner_slot="first" if row["radiant_win"] else "second",
            radiant_win=row["radiant_win"], previous_scores={"first": 0, "second": 0},
            current_scores={"first": 1 if row["radiant_win"] else 0,
                            "second": 0 if row["radiant_win"] else 1},
            first_team_is_radiant=True, result_timestamp=row["end"], duration_seconds=duration,
        )
        outputs.append(model)
    assert outputs[0].player_k24 == outputs[1].player_k24 == outputs[2].player_k24
    assert outputs[0].player_a == outputs[1].player_a
    assert outputs[0].player_a != outputs[2].player_a
    assert outputs[0].a_history[0]["duration_seconds"] is None
    assert outputs[1].a_history[0]["duration_seconds"] is None
    assert outputs[2].a_history[0]["duration_seconds"] == row["duration_seconds"]
    assert HybridPlayerRosterEloModel.from_state(outputs[1].export_state()).a_available


def test_old_k24_snapshot_is_readable_without_rebuild(tmp_path, monkeypatch):
    row = FIXTURE["maps"][0]
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    model.process_match(result_record(_record(row), row["end"]))
    state = model.export_state()
    for key in list(state):
        if key.startswith("a_") or key.startswith("player_a"):
            del state[key]
    del state["k24_contract"]
    meta = live._rating_replay_meta()
    del meta["a_schema_version"]
    meta["rating_replay_version"] = "finished_results_v1+k24_v1"
    meta["reference_timestamp"] = row["end"]
    path = tmp_path / "old_snapshot.json"
    path.write_text(json.dumps({"meta": meta, "teams_by_org_key": {},
                                "team_kills_history_by_team_id": {}, "model_state": state}))
    monkeypatch.setenv("ELO_SERVED_COMPOSITION", "a")
    assert live.ensure_snapshot(snapshot_path=path, data_dir=tmp_path, rebuild_if_missing=False) is not None
    served = live.get_matchup_summary(
        radiant_team_id=1, dire_team_id=2, radiant_team_name="Radiant", dire_team_name="Dire",
        radiant_account_ids=row["radiant_player_ids"], dire_account_ids=row["dire_player_ids"],
        timestamp=row["end"] + 1, snapshot_path=path, data_dir=tmp_path,
        rebuild_if_missing=False, runtime_model_state_path=tmp_path / "absent.json",
    )
    assert served is not None and served["composition"] == "k24"


def test_full_state_converter_carries_a_ratings_games_and_history(tmp_path):
    from ELO.convert_state_to_delta import main as convert

    rows = FIXTURE["maps"][:2]
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    model.process_match(result_record(_record(rows[0]), rows[0]["end"]),
                        duration_seconds=rows[0]["duration_seconds"])
    base_state = model.export_state()
    signature = live._model_config_signature(base_state)
    snapshot, runtime, delta = [tmp_path / name for name in ("snapshot.json", "runtime.json", "delta.json")]
    snapshot.write_text(json.dumps({"meta": {"reference_timestamp": rows[0]["end"],
                                            "model_config_signature": signature},
                                    "model_state": base_state}))
    model.process_match(result_record(_record(rows[1]), rows[1]["end"]),
                        duration_seconds=rows[1]["duration_seconds"])
    runtime.write_text(json.dumps({"base_reference_timestamp": rows[0]["end"],
                                   "base_model_config_signature": signature,
                                   "model_state": model.export_state()}))
    assert convert(["--snapshot", str(snapshot), "--state", str(runtime), "--delta", str(delta)]) == 0
    restored = array_model.build_overlay_model(snapshot, delta)
    assert restored.a_available and list(restored.a_history) == list(model.a_history)
    assert dict(restored.player_a.items()) == pytest.approx(model.player_a)
    assert dict(restored.player_a_games.items()) == model.player_a_games
