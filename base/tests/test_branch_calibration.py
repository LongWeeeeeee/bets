"""Каждая ветка продаёт свой винрейт, а неоткалиброванная — не продаёт никакого.

75% от восьмиколоночной ветки и 75% от полной — разные вещи. Одна таблица на
все ветки означала бы ставку по коэффициенту, который не покрывает риск: из
винрейта получается безубыточный кэф, и завышенный винрейт даёт заниженный кэф.

Наследовать таблицу полной ветки для короткой тоже нельзя — опасность
односторонняя: короткая ветка слабее, её настоящий винрейт при той же
уверенности ниже. Поэтому полоса без данных винрейта не получает вовсе, и
автоматическая ставка по такой ветке не выставляется.
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import prematch_scorer as ps  # noqa: E402
from test_prematch_ladder import ACCOUNTS, FEATURES, NO_ACC, _artifact, _call  # noqa: E402


def _with_calibration(tmp_path, rows):
    """Тот же синтетический артефакт, но с таблицей винрейта по веткам."""
    m = _artifact(tmp_path, known_accounts=[])
    m.cal = {}
    for name, lo, hi, wr, n in rows:
        m.cal.setdefault(name, []).append((lo, hi, wr, n))
    return m


def test_full_branch_keeps_the_general_table(tmp_path):
    """`LAN_ODDS_GRID` снята на честных forward-окнах (E-142) — её не трогаем."""
    m = _artifact(tmp_path, known_accounts=ACCOUNTS)
    wr, src = m.branch_winrate("full", 0.75)
    assert src == "общая таблица"
    assert wr == ps.lan_winrate(0.75)


def test_short_branch_uses_its_own_number(tmp_path):
    m = _with_calibration(tmp_path, [("no_account", 72, 79, 0.641, 1181)])
    wr, src = m.branch_winrate("no_account", 0.75)
    assert wr == 0.641
    assert "своя таблица ветки" in src and "1181" in src


def test_uncalibrated_band_sells_nothing(tmp_path):
    """Полосы нет — винрейта нет. Не ноль и не значение соседа, а `nan`."""
    m = _with_calibration(tmp_path, [("no_account", 50, 58, 0.476, 98)])
    wr, src = m.branch_winrate("no_account", 0.85)
    assert math.isnan(wr)
    assert src == "полоса ветки не откалибрована"


def test_branch_without_any_table_falls_back_to_general(tmp_path):
    """Ветки нет в таблице вовсе — поведение прежнее, чтобы ничего не сломать."""
    m = _with_calibration(tmp_path, [("no_org", 50, 58, 0.5, 100)])
    wr, src = m.branch_winrate("no_account", 0.75)
    assert src == "общая таблица"
    assert wr == ps.lan_winrate(0.75)


def test_score_reports_the_source_of_the_winrate(tmp_path):
    m = _with_calibration(tmp_path, [("no_account", 50, 101, 0.611, 212)])
    r = _call(m)
    assert r.branch == "no_account"
    assert r.lan_winrate == 0.611
    assert any("винрейт: своя таблица ветки" in n for n in r.notes)


def test_score_returns_nan_winrate_on_an_uncalibrated_band(tmp_path):
    """Вероятность отдаётся, винрейт — нет: ставить по нему нельзя."""
    m = _with_calibration(tmp_path, [("no_account", 50, 51, 0.5, 99)])
    r = _call(m)
    assert 0.0 < r.probability < 1.0
    assert math.isnan(r.lan_winrate)
    assert any("не откалибрована" in n for n in r.notes)


def test_calibration_survives_the_artifact_round_trip(tmp_path):
    """Таблица обязана читаться из npz без allow_pickle, как и веса веток."""
    n = len(FEATURES)
    branches = {
        "no_account": ps.Branch(cols=list(NO_ACC), mu=np.zeros(len(NO_ACC)),
                                sd=np.ones(len(NO_ACC)),
                                coef=np.full(len(NO_ACC), 0.2), intercept=0.0),
    }
    z = {
        "snapshot_ts": np.array([1700000000], dtype=np.int64),
        "mu": np.zeros((1, n)), "sd": np.ones((1, n)),
        "coef": np.zeros((1, n)), "intercept": np.zeros(1),
        "accounts": np.zeros((0, 20)), "acc_hero": np.zeros((0, 6)),
        "acc_pos": np.zeros((0, 3)),
        "hero_wr30": np.array([[h, 0.5] for h in range(1, 21)]),
        "hero_farm": np.array([[h, 0.4] for h in range(1, 21)]),
        "vs_pairs": np.zeros((0, 4)), "h2h": np.array([[11.0, 22.0, 0.05]]),
        "feature_names": np.array(FEATURES),
        "cal_branch": np.array(["no_account", "no_account"], dtype="<U32"),
        "cal_lo": np.array([50, 58], dtype=np.int64),
        "cal_hi": np.array([58, 65], dtype=np.int64),
        "cal_wr": np.array([0.476, 0.624]),
        "cal_n": np.array([98, 4040], dtype=np.int64),
    }
    z.update(ps.pack_branches(branches, FEATURES))
    p = tmp_path / "cal.npz"
    np.savez_compressed(p, **z)
    m = ps.PrematchModel(p)
    assert m.cal["no_account"] == [(50, 58, 0.476, 98), (58, 65, 0.624, 4040)]
    assert m.branch_winrate("no_account", 0.60)[0] == 0.624
