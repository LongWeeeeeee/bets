from pathlib import Path

import numpy as np
import pytest

from base.train_public_draft_hero10_experiment import (
    FEATURE_NAMES,
    canonicalize_match,
    chronological_split,
    feature_frame,
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


def test_duration_is_required():
    bad = sample(); bad.pop("durationSeconds")
    assert canonicalize_match(bad)[1] == "invalid_duration_seconds"


def test_training_writes_artifacts_without_overwrite(tmp_path: Path):
    matches = [canonicalize_match(sample(i, i, i % 2 == 0, (i, 1)))[0] for i in range(1, 31)]
    result = train_experiment([m for m in matches if m is not None], tmp_path / "out")
    assert result["counts"] == {"all": 30, "train": 18, "validation": 6, "test": 6}
    assert (tmp_path / "out/manifest.json").exists()
    assert (tmp_path / "out/duration_seconds_regression_model.joblib").exists()
    assert 0.0 <= result["models"]["total_kills_regression"]["normalized_prediction_min"] <= 1.0
    assert 0.0 <= result["models"]["total_kills_regression"]["normalized_prediction_max"] <= 1.0
    with pytest.raises(FileExistsError):
        train_experiment([m for m in matches if m is not None], tmp_path / "out")


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
