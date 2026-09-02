"""`PrematchModel.__init__` отказывается грузить артефакт со старой `accounts`.

Раньше `_acc_side` в `prematch_scorer.py` (строки 869-871) при ширине < 16
молча подставляла `A[:, 6]`/`A[:, 10]` вместо `imp_recent10`/`imp30_resid`/
`lh30_resid` — те же числа под чужим именем, масштаб расходится в 100 раз
(E-177). Ночная цепочка уже требует >= 19 колонок
(`scripts/run/rebuild_prematch_snapshot.sh:106`), но ручной/старый артефакт
проходил мимо неё. Загрузчик обязан явно отказать, а не тихо испортить скор.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import prematch_scorer as ps  # noqa: E402


def _artifact(tmp_path, n_acc_cols: int):
    """Минимальный артефакт, которого хватает конструктору, с заданной
    шириной `accounts` (мимикрирует ключи, которые читает `PrematchModel`)."""
    z = {
        "snapshot_ts": np.array([1700000000], dtype=np.int64),
        "mu": np.zeros((1, 2)), "sd": np.ones((1, 2)),
        "coef": np.zeros((1, 2)), "intercept": np.zeros(1),
        "accounts": np.zeros((0, n_acc_cols)), "acc_hero": np.zeros((0, 6)),
        "acc_pos": np.zeros((0, 3)), "hero_wr30": np.zeros((0, 2)),
        "vs_pairs": np.zeros((0, 4)), "h2h": np.zeros((0, 3)),
        "hero_farm": np.zeros((0, 2)),
        "feature_names": np.array(["draft_logit", "elo"]),
    }
    p = tmp_path / "artifact.npz"
    np.savez_compressed(p, **z)
    return p


def test_14_column_accounts_table_is_rejected(tmp_path):
    """Старый (до E-177) формат — явный отказ, а не молчаливая подмена колонки."""
    with pytest.raises(ValueError, match=r"14.*19|19.*14"):
        ps.PrematchModel(_artifact(tmp_path, 14))


def test_19_column_accounts_table_loads(tmp_path):
    """Актуальная ширина проходит без ошибок."""
    m = ps.PrematchModel(_artifact(tmp_path, 19))
    assert m.acc is not None
