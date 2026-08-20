"""Разложение вердикта по компонентам точное, а не оценочное.

Вклад компонента — это частичная сумма логита по его колонкам. Сумма частей
плюс сдвиг ветки обязана давать сам логит: если не даёт, разложение считается
не по тем колонкам, и панель показывает человеку выдумку.

Почему именно так, а не четыре независимо обученные модели: независимое
обучение стоит 0.0072 AUC на боевой конфигурации (0.7114 против 0.7186), а
частичные суммы — ровно ноль.
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from test_prematch_ladder import ACCOUNTS, _artifact, _call  # noqa: E402


def _logit(p: float) -> float:
    return math.log(p / (1.0 - p))


def test_parts_sum_to_the_logit(tmp_path):
    m = _artifact(tmp_path, known_accounts=ACCOUNTS)
    r = _call(m)
    total = sum(r.parts.values()) + m.branches[r.branch].intercept
    assert total == pytest.approx(_logit(r.probability), abs=1e-9)


def test_parts_sum_to_the_logit_on_the_short_branch(tmp_path):
    m = _artifact(tmp_path, known_accounts=[])
    r = _call(m)
    total = sum(r.parts.values()) + m.branches[r.branch].intercept
    assert total == pytest.approx(_logit(r.probability), abs=1e-9)


def test_parts_only_lists_components_present_in_the_branch(tmp_path):
    """Компонента, которого в ветке нет, не должно быть и в разложении."""
    m = _artifact(tmp_path, known_accounts=[])
    r = _call(m)
    assert "players" not in r.parts
    assert set(r.parts) <= {"elo", "draft", "h2h"}


def test_full_branch_lists_every_component(tmp_path):
    m = _artifact(tmp_path, known_accounts=ACCOUNTS)
    r = _call(m)
    assert set(r.parts) == {"elo", "draft", "h2h"}


def test_legacy_artifact_reports_no_parts(tmp_path):
    """Без веток разложение не считается: веса лежат ансамблем, а не одним набором."""
    import numpy as np

    import prematch_scorer as ps
    from test_prematch_ladder import FEATURES

    n = len(FEATURES)
    z = {
        "snapshot_ts": np.array([1700000000], dtype=np.int64),
        "mu": np.zeros((1, n)), "sd": np.ones((1, n)),
        "coef": np.full((1, n), 0.1), "intercept": np.zeros(1),
        "accounts": np.array([[a] + [1500.0] + [1.0] * 18 for a in ACCOUNTS]),
        "acc_hero": np.zeros((0, 6)), "acc_pos": np.zeros((0, 3)),
        "hero_wr30": np.array([[h, 0.5] for h in range(1, 21)]),
        "hero_farm": np.array([[h, 0.4] for h in range(1, 21)]),
        "vs_pairs": np.zeros((0, 4)), "h2h": np.array([[11.0, 22.0, 0.05]]),
        "feature_names": np.array(FEATURES),
    }
    p = tmp_path / "legacy.npz"
    np.savez_compressed(p, **z)
    r = _call(ps.PrematchModel(p))
    assert r.parts == {}
    assert r.branch == "full"
