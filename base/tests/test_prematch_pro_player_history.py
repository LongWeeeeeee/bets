from __future__ import annotations

import json
import math

import numpy as np
import pytest

from base.tools.prematch_pro_player_history import JUNE_1_2026, JULY_15_2026, build_history, feature_names


def _dataset(path, rows):
    n = len(rows)
    accounts = np.zeros((n, 10), dtype=np.int64)
    heroes = np.zeros((n, 10), dtype=np.int32)
    for index, row in enumerate(rows):
        accounts[index, 0], heroes[index, 0] = row.get("account", 1), row.get("hero", 7)
        accounts[index, 5], heroes[index, 5] = 99, 19
    payload = {
        "X": np.arange(n * 2, dtype=np.float32).reshape(n, 2),
        "feature_names": np.array(["base_a", "base_b"]),
        "mid": np.array([row["mid"] for row in rows], dtype=np.int64),
        "ts": np.array([row["ts"] for row in rows], dtype=np.int64),
        "end": np.array([row["end"] for row in rows], dtype=np.int64),
        "y": np.array([row.get("y", 1) for row in rows], dtype=np.int8),
        "series": np.zeros(n, dtype=np.int64), "league": np.ones(n, dtype=np.int64),
        "heroes": heroes, "accounts": accounts, "k24_diff": np.zeros(n, dtype=np.float32),
        "elo_eligible": np.ones(n, dtype=bool), "label_side": np.asarray("radiant"),
        "hero_slots": np.asarray("radiant_1_5,dire_1_5"),
    }
    np.savez_compressed(path, **payload)
    return payload


def _raw(mid, start, duration, won, players):
    return {"id": mid, "startDateTime": start, "durationSeconds": duration,
            "didRadiantWin": won, "players": players}


def _player(account=1, hero=7, radiant=True, *, xp=0, position=None):
    value = {"isRadiant": radiant, "steamAccount": {"id": account}, "heroId": hero,
             "dotaPlusHeroXp": xp}
    if position is not None:
        value["position"] = position
    return value


def _run(tmp_path, rows, raw_files):
    raw_dir, out, dataset = tmp_path / "raw", tmp_path / "out", tmp_path / "dataset.npz"
    raw_dir.mkdir()
    for name, payload in raw_files.items():
        (raw_dir / name).write_text(json.dumps(payload), encoding="utf-8")
    original = _dataset(dataset, rows)
    metadata = build_history(raw_dir, dataset, out)
    return original, metadata, np.load(out / "dataset.npz", allow_pickle=False), np.load(out / "history.npz", allow_pickle=False)


def _feature(data, name, row=0):
    return data["X"][row, list(data["feature_names"]).index(name)]


def test_june_coverage_window_uses_utc_midnight_boundaries():
    assert JUNE_1_2026 == 1_780_272_000
    assert JULY_15_2026 == 1_784_073_600


def test_missing_positions_are_accepted_and_source_prefix_is_unchanged(tmp_path):
    original, metadata, augmented, history = _run(
        tmp_path, [{"mid": 2, "ts": 200, "end": 250, "account": 1, "hero": 7}],
        {"a.json": {"first": _raw(1, 100, 20, True, [_player(position=None)])}},
    )
    assert _feature(augmented, "pro_history_hero_games_log1p_R1") == pytest.approx(math.log(2.0))
    assert metadata["source_population"] == 1 and metadata["counters"]["valid_candidates"] == 1
    assert np.array_equal(augmented["X"][:, :original["X"].shape[1]], original["X"])
    for name, value in original.items():
        if name not in {"X", "feature_names"}:
            assert np.array_equal(augmented[name], value)
    assert tuple(augmented["feature_names"][-len(feature_names()):]) == feature_names()
    assert np.array_equal(history["mid"], original["mid"])


def test_player_ids_distinguish_the_same_hero_and_xp_is_only_prior(tmp_path):
    _, _, augmented, _ = _run(
        tmp_path, [
            {"mid": 2, "ts": 200, "end": 250, "account": 1, "hero": 7},
            {"mid": 3, "ts": 300, "end": 350, "account": 2, "hero": 7},
        ], {"a.json": {"first": _raw(1, 100, 20, True, [_player(1, 7, xp=50)])}},
    )
    assert _feature(augmented, "pro_history_hero_xp_known_R1", 0) == 1.0
    assert _feature(augmented, "pro_history_hero_xp_log1p_R1", 0) == pytest.approx(math.log(51.0))
    assert _feature(augmented, "pro_history_hero_games_log1p_R1", 1) == 0.0
    assert _feature(augmented, "pro_history_hero_xp_known_R1", 1) == 0.0


def test_duplicate_raw_maps_are_canonicalized_once_and_end_boundary_is_strict(tmp_path):
    _, metadata, augmented, _ = _run(
        tmp_path, [
            {"mid": 2, "ts": 120, "end": 150, "account": 1, "hero": 7},
            {"mid": 3, "ts": 121, "end": 150, "account": 1, "hero": 7},
        ], {
            "a.json": {"one": _raw(1, 100, 20, True, [_player()])},
            "b.json": {"copy": _raw(1, 100, 20, True, [_player()])},
        },
    )
    assert metadata["counters"]["duplicate_records"] == 1
    assert _feature(augmented, "pro_history_games_log1p_R1", 0) == 0.0
    assert _feature(augmented, "pro_history_games_log1p_R1", 1) == pytest.approx(math.log(2.0))


def test_empty_roster_cannot_win_canonical_dedup_over_a_valid_duplicate(tmp_path):
    _, metadata, augmented, _ = _run(
        tmp_path, [{"mid": 2, "ts": 200, "end": 250, "account": 1, "hero": 7}],
        {"a.json": {"empty": _raw(1, 10, 20, True, [])},
         "b.json": {"valid": _raw(1, 100, 20, True, [_player()])}},
    )
    assert metadata["counters"]["valid_candidates"] == 1
    assert _feature(augmented, "pro_history_games_log1p_R1") == pytest.approx(math.log(2.0))


def test_ambiguous_overlap_including_corrupt_own_timestamp_fails(tmp_path):
    raw_dir, out, dataset = tmp_path / "raw", tmp_path / "out", tmp_path / "dataset.npz"
    raw_dir.mkdir()
    _dataset(dataset, [{"mid": 1, "ts": 200, "end": 250, "account": 1, "hero": 7}])
    (raw_dir / "a.json").write_text(json.dumps({"same": _raw(1, 100, 20, True, [_player()])}), encoding="utf-8")
    with pytest.raises(ValueError, match="ambiguous raw/query overlap"):
        build_history(raw_dir, dataset, out)


def test_conflicting_duplicate_count_and_identity_mismatch_are_not_silenced(tmp_path):
    raw_dir, out, dataset = tmp_path / "raw", tmp_path / "out", tmp_path / "dataset.npz"
    raw_dir.mkdir()
    _dataset(dataset, [{"mid": 1, "ts": 100, "end": 120, "account": 1, "hero": 7}])
    # Same MID/time/outcome but a different account cannot be allowed to validate the query map.
    (raw_dir / "a.json").write_text(json.dumps({"same": _raw(1, 100, 20, True, [_player(2, 7)])}), encoding="utf-8")
    with pytest.raises(ValueError, match="ambiguous raw/query overlap"):
        build_history(raw_dir, dataset, out)
