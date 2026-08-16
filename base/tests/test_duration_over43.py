from pathlib import Path

import numpy as np
import pytest

from base.duration_over43 import (
    THRESHOLD_MINUTES,
    THRESHOLD_SECONDS,
    heroes_matrix,
    predict_over43,
    predict_proba,
    reload,
)
from base.draft_features import KIND_ROLE, DraftFeatureEncoder
from base.train_duration_over43 import fit_logistic, chrono_idx
from base.train_public_draft_hero10_experiment import atomic_joblib_dump


def test_threshold_is_43_minutes():
    assert THRESHOLD_MINUTES == 43
    assert THRESHOLD_SECONDS == 43 * 60


def test_heroes_matrix_shapes():
    row = np.arange(10, dtype=np.int64)
    assert heroes_matrix(row).shape == (1, 10)
    batch = np.vstack([row, row + 1])
    assert heroes_matrix(batch).shape == (2, 10)
    with pytest.raises(ValueError):
        heroes_matrix(np.arange(9))


def test_chrono_idx_60_20_20():
    tr, va, te = chrono_idx(10)
    assert [len(tr), len(va), len(te)] == [6, 2, 2]
    assert tr[-1] < va[0] < te[0]


def test_scorer_roundtrip(tmp_path: Path):
    n = 80
    heroes = np.zeros((n, 10), dtype=np.int64)
    y = np.zeros(n, dtype=np.int32)
    for i in range(n):
        # hero 1 on mid → long game; hero 2 → short
        long = i % 2 == 0
        heroes[i] = [1 + int(long)] + [10 + (i % 5)] * 9
        y[i] = int(long)
    enc = DraftFeatureEncoder.fit(heroes, KIND_ROLE, signed=False)
    model = fit_logistic(enc.transform(heroes), y, 1.0)
    atomic_joblib_dump(enc, tmp_path / "encoder.joblib")
    atomic_joblib_dump(model, tmp_path / "model.joblib")
    from base import duration_over43 as mod

    previous = mod.MODEL_DIR
    try:
        reload(tmp_path)
        p = predict_proba(heroes)
        assert p is not None
        assert p.shape == (n,)
        acc = float(np.mean((p >= 0.5) == y))
        assert acc >= 0.8
        one = predict_over43(heroes[0])
        assert one is not None
        assert one["over43"] is True
        assert one["threshold_minutes"] == 43
    finally:
        reload(previous)
