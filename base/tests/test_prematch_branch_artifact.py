"""Ветки кладутся в артефакт плоскими массивами и читаются обратно без потерь.

`PrematchModel.__init__` зовёт `np.load` БЕЗ `allow_pickle`, поэтому рваные
данные (у веток разное число колонок) нельзя хранить object-массивом — чтение
упало бы уже в бою. Хранятся длины и один плоский массив на каждую величину.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import prematch_scorer as ps  # noqa: E402


def _minimal_artifact(tmp_path, extra):
    """Артефакт, которого хватает конструктору: пустые таблицы плюс веса."""
    z = {
        "snapshot_ts": np.array([1700000000], dtype=np.int64),
        "mu": np.zeros((1, 2)), "sd": np.ones((1, 2)),
        "coef": np.zeros((1, 2)), "intercept": np.zeros(1),
        "accounts": np.zeros((0, 14)), "acc_hero": np.zeros((0, 6)),
        "acc_pos": np.zeros((0, 3)), "hero_wr30": np.zeros((0, 2)),
        "vs_pairs": np.zeros((0, 4)), "h2h": np.zeros((0, 3)),
        "hero_farm": np.zeros((0, 2)),
        "feature_names": np.array(["draft_logit", "elo"]),
    }
    z.update(extra)
    p = tmp_path / "artifact.npz"
    np.savez_compressed(p, **z)
    return p


def test_artifact_without_branches_still_loads(tmp_path):
    """Обратная совместимость: на проде лежит артефакт без веток."""
    m = ps.PrematchModel(_minimal_artifact(tmp_path, {}))
    assert m.branches == {}


def test_pack_and_read_round_trip(tmp_path):
    branches = {
        "full": ps.Branch(cols=["draft_logit", "elo"],
                          mu=np.array([0.1, 0.2]), sd=np.array([1.0, 2.0]),
                          coef=np.array([0.5, -0.5]), intercept=0.25),
        "no_account": ps.Branch(cols=["draft_logit"],
                                mu=np.array([0.3]), sd=np.array([3.0]),
                                coef=np.array([0.7]), intercept=-0.1),
    }
    packed = ps.pack_branches(branches, ["draft_logit", "elo"])
    m = ps.PrematchModel(_minimal_artifact(tmp_path, packed))
    assert set(m.branches) == {"full", "no_account"}
    got = m.branches["no_account"]
    assert got.cols == ["draft_logit"]
    assert got.intercept == -0.1
    np.testing.assert_allclose(got.mu, [0.3])
    np.testing.assert_allclose(got.sd, [3.0])
    np.testing.assert_allclose(got.coef, [0.7])
    np.testing.assert_allclose(m.branches["full"].coef, [0.5, -0.5])


def test_unknown_column_is_rejected(tmp_path):
    """Колонка не из боевых признаков и не из известных добавок — ошибка."""
    branches = {"full": ps.Branch(cols=["выдумка"], mu=np.zeros(1), sd=np.ones(1),
                                  coef=np.zeros(1), intercept=0.0)}
    with pytest.raises(ValueError, match="не боевые признаки"):
        ps.pack_branches(branches, ["draft_logit", "elo"])


def test_extra_columns_are_allowed(tmp_path):
    """Добавочные колонки коротких веток в feature_names не лежат — и не должны."""
    branches = {"no_account_no_org": ps.Branch(
        cols=["draft_logit", "org_rating_diff"], mu=np.zeros(2), sd=np.ones(2),
        coef=np.zeros(2), intercept=0.0)}
    packed = ps.pack_branches(branches, ["draft_logit", "elo"])
    m = ps.PrematchModel(_minimal_artifact(tmp_path, packed))
    assert m.branches["no_account_no_org"].cols == ["draft_logit", "org_rating_diff"]


def test_packed_arrays_are_not_object_dtype(tmp_path):
    """Иначе чтение упрётся в allow_pickle=False и упадёт в бою."""
    branches = {"full": ps.Branch(cols=["elo"], mu=np.zeros(1), sd=np.ones(1),
                                  coef=np.zeros(1), intercept=0.0)}
    for k, v in ps.pack_branches(branches, ["draft_logit", "elo"]).items():
        assert v.dtype != object, k


def test_corrupted_branch_lengths_are_caught(tmp_path):
    """Молча брать первые N колонок нельзя: веса встанут не на свои места."""
    branches = {"full": ps.Branch(cols=["draft_logit", "elo"], mu=np.zeros(2),
                                  sd=np.ones(2), coef=np.zeros(2), intercept=0.0)}
    packed = ps.pack_branches(branches, ["draft_logit", "elo"])
    packed["branch_lens"] = np.array([1], dtype=np.int64)
    with pytest.raises(ValueError, match="повреждены"):
        ps.PrematchModel(_minimal_artifact(tmp_path, packed))
