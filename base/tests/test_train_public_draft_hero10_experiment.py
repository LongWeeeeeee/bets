from pathlib import Path

import numpy as np
import pytest

from base.draft_features import KIND_PAIR, KIND_ROLE, DraftFeatureEncoder, KillSaturationScale
from base.train_public_draft_hero10_experiment import (
    FEATURE_NAMES,
    canonicalize_match,
    chronological_split,
    feature_frame,
    BoundedRidgeRegressor,
    train_experiment,
)


def sample(map_id=1, start=100, win=True, kills=(1, 2)):
    players = [
        {"isRadiant": side, "position": f"POSITION_{pos}", "heroId": hero}
        for side, base in ((True, 10), (False, 20))
        for pos, hero in enumerate(range(base, base + 5), 1)
    ]
    return {"id": map_id, "startDateTime": start, "durationSeconds": 1800,
            "didRadiantWin": win, "players": players, "radiantKills": list(kills), "direKills": [0, 1]}


def varied(map_id, start, win, kills, duration, pool_offset):
    """Rotating hero pool so pair columns are not degenerate."""
    heroes = [1 + (pool_offset + i) % 12 for i in range(10)]
    players = [
        {"isRadiant": i < 5, "position": f"POSITION_{(i % 5) + 1}", "heroId": hero}
        for i, hero in enumerate(heroes)
    ]
    return {"id": map_id, "startDateTime": start, "durationSeconds": duration,
            "didRadiantWin": win, "players": players, "radiantKills": [kills], "direKills": [0]}


def corpus(n=60):
    raw = [varied(i, i, i % 3 == 0, 5 + (i % 17), 1500 + 13 * (i % 40), i) for i in range(1, n + 1)]
    matches = [canonicalize_match(r)[0] for r in raw]
    return [m for m in matches if m is not None]


def test_canonicalize_is_strict_and_sums_kills():
    match, reason = canonicalize_match(sample())
    assert reason is None
    assert match is not None
    assert match.heroes == tuple(range(10, 15)) + tuple(range(20, 25))
    assert match.total_kills == 4
    assert match.duration_seconds == 1800
    bad = sample(); bad["players"] = bad["players"][:-1]
    assert canonicalize_match(bad)[1] == "invalid_player_count"


def test_features_are_exactly_ten_fixed_columns():
    match, _ = canonicalize_match(sample())
    assert match is not None
    assert feature_frame([match]).shape == (1, 10)
    assert len(FEATURE_NAMES) == 10
    assert FEATURE_NAMES == tuple(f"hero_{s}_{p}" for s in ("R", "D") for p in range(1, 6))


def test_split_is_chronological_60_20_20():
    matches = [canonicalize_match(sample(i, i))[0] for i in range(1, 11)]
    train, validation, test = chronological_split([m for m in matches if m is not None])
    assert [len(train), len(validation), len(test)] == [6, 2, 2]
    assert train[-1].start_time < validation[0].start_time < test[0].start_time


def test_split_boundary_uses_map_id_for_ties():
    matches = [canonicalize_match(sample(i, 100))[0] for i in range(1, 11)]
    ordered = sorted((m for m in matches if m is not None), key=lambda m: (m.start_time, m.map_id))
    train, validation, test = chronological_split(ordered)
    assert (train[-1].start_time, train[-1].map_id) < (validation[0].start_time, validation[0].map_id)
    assert (validation[-1].start_time, validation[-1].map_id) < (test[0].start_time, test[0].map_id)


def test_persisted_ridge_wrapper_bounds_extreme_predictions():
    model = BoundedRidgeRegressor(__import__("sklearn.linear_model", fromlist=["Ridge"]).Ridge(alpha=0.01)).fit(
        np.array([[0.0], [1.0]]), np.array([0.0, 1.0])
    )
    assert np.all((0.0 <= model.predict(np.array([[-100.0], [100.0]]))) & (model.predict(np.array([[-100.0], [100.0]])) <= 1.0))


def test_duration_is_required():
    bad = sample(); bad.pop("durationSeconds")
    assert canonicalize_match(bad)[1] == "invalid_duration_seconds"


# --------------------------------------------------------------------- encoders
def test_win_design_negates_when_sides_swap():
    heroes = feature_frame(corpus(40))
    encoder = DraftFeatureEncoder.fit(heroes, KIND_PAIR, signed=True, pair_min_support=1)
    swapped = np.hstack([heroes[:, 5:], heroes[:, :5]])
    assert abs(encoder.transform(heroes) + encoder.transform(swapped)).nnz == 0


def test_level_design_is_unchanged_when_sides_swap():
    heroes = feature_frame(corpus(40))
    encoder = DraftFeatureEncoder.fit(heroes, KIND_ROLE, signed=False)
    swapped = np.hstack([heroes[:, 5:], heroes[:, :5]])
    assert abs(encoder.transform(heroes) - encoder.transform(swapped)).nnz == 0


def test_every_column_comes_from_the_ten_hero_ids():
    heroes = feature_frame(corpus(40))
    encoder = DraftFeatureEncoder.fit(heroes, KIND_PAIR, signed=True, pair_min_support=1)
    matrix = encoder.transform(heroes)
    assert matrix.shape[1] == encoder.n_columns
    # 10 hero + 10 role + 2*C(5,2) synergy + 5*5 counter entries, all from the same ten IDs
    assert matrix.getrow(0).nnz == 10 + 10 + 20 + 25
    assert encoder.n_columns == 6 * encoder.n_heroes + int(encoder.synergy_lut.max()) + 1 + int(encoder.counter_lut.max()) + 1


def test_unknown_hero_contributes_nothing_instead_of_raising():
    heroes = feature_frame(corpus(40))
    encoder = DraftFeatureEncoder.fit(heroes, KIND_ROLE, signed=False)
    unseen = heroes[:1].copy()
    unseen[0, 0] = 60000
    row = encoder.transform(unseen)
    known = encoder.transform(heroes[:1])
    assert row.shape == (1, encoder.n_columns)
    # the unknown hero is zeroed out; its hero and role entries stop contributing
    assert abs(row).sum() == abs(known).sum() - 2


def test_saturation_puts_the_median_map_at_one_half():
    kills = np.arange(6, 232, dtype=float)
    scale = KillSaturationScale.fit(kills)
    assert scale.saturation([float(np.median(kills))])[0] == pytest.approx(0.5, abs=0.01)
    assert scale.saturation([0.0])[0] == 0.0 or scale.saturation([0.0])[0] < 0.01
    assert scale.saturation([1e6])[0] == pytest.approx(1.0, abs=0.01)
    assert np.all(np.diff(scale.saturation(kills)) > 0)
    assert scale.kills(scale.saturation([100.0]))[0] == pytest.approx(100.0, abs=1.0)


def test_saturation_stays_inside_unit_interval():
    scale = KillSaturationScale.fit(np.array([10.0, 20.0, 30.0]))
    values = scale.saturation(np.array([-500.0, 20.0, 500.0]))
    assert np.all((values >= 0.0) & (values <= 1.0))


# --------------------------------------------------------------------- training
def test_training_writes_v4_artifacts_without_overwrite(tmp_path: Path):
    matches = corpus(60)
    result = train_experiment(matches, tmp_path / "out", pair_min_support=1)
    assert result["counts"] == {"all": 60, "train": 36, "validation": 12, "test": 12, "shipped_fit_rows": 48}
    for name in ("manifest.json", "results.json", "radiant_win_model.joblib",
                 "duration_seconds_regression_model.joblib", "total_kills_regression_model.joblib",
                 "total_kills_over_median_model.joblib", "win_feature_encoder.joblib",
                 "level_feature_encoder.joblib", "kills_saturation_scale.joblib"):
        assert (tmp_path / "out" / name).exists(), name
    assert result["schema"]["uses_ingame_or_third_party_stats"] is False
    assert result["schema"]["win_design"]["kind"] == KIND_PAIR
    assert result["schema"]["level_design"]["kind"] == KIND_ROLE
    assert set(result["evaluation"]) == {"radiant_win", "total_kills_over_median",
                                         "total_kills_regression", "duration_seconds_regression"}
    saturation = result["models"]["kill_saturation"]
    assert saturation["bounds"] == [0.0, 1.0]
    assert 0.0 <= saturation["prediction_min"] <= saturation["prediction_max"] <= 1.0
    with pytest.raises(FileExistsError):
        train_experiment(matches, tmp_path / "out", pair_min_support=1)


def test_persisted_encoder_reproduces_training_matrix(tmp_path: Path):
    import joblib
    matches = corpus(60)
    train_experiment(matches, tmp_path / "out", pair_min_support=1)
    encoder = joblib.load(tmp_path / "out/win_feature_encoder.joblib")
    heroes = feature_frame(matches[:5])
    assert abs(encoder.transform(heroes) - encoder.transform(heroes)).nnz == 0
    assert encoder.transform(heroes).shape == (5, encoder.n_columns)


def test_atomic_joblib_dump_preserves_destination_on_failure(tmp_path: Path, monkeypatch):
    from base import train_public_draft_hero10_experiment as experiment
    destination = tmp_path / "model.joblib"
    destination.write_bytes(b"original")
    def fail_dump(value, handle):
        handle.write(b"partial")
        raise RuntimeError("dump failed")
    monkeypatch.setattr(experiment.joblib, "dump", fail_dump)
    with pytest.raises(RuntimeError):
        experiment.atomic_joblib_dump({"x": 1}, destination)
    assert destination.read_bytes() == b"original"
    assert list(tmp_path.glob("*.tmp")) == []
