"""Таблица (аккаунт, герой) на массивах ведёт себя как словарь, но не ест память.

Словарём эта таблица стоила 2 005 МБ при 311 МБ полезных данных: 6.78 млн
ячеек, на каждую кортеж-ключ из двух Python-int, объект-представление numpy и
слот словаря, причём значения были видами в исходный массив — в памяти жили и
он, и словарь. Процесс на боевой машине из-за этого перешагнул `MemoryHigh`.

Интерфейс обязан остаться словарным: обращения есть и в коде на боевой машине.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import prematch_scorer as ps  # noqa: E402

ROWS = np.array([
    [500, 7, 1.0, 2.0, 3.0, 4.0],
    [100, 3, 5.0, 6.0, 7.0, 8.0],
    [100, 9, 9.0, 10.0, 11.0, 12.0],
    [900, 1, 13.0, 14.0, 15.0, 16.0],
])


def test_get_returns_the_same_row_as_a_dict_would():
    ref = {(int(r[0]), int(r[1])): r[2:] for r in ROWS}
    t = ps.PairTable(ROWS)
    for key, want in ref.items():
        np.testing.assert_allclose(t.get(key), want)


def test_missing_key_returns_default():
    t = ps.PairTable(ROWS)
    assert t.get((100, 4)) is None
    assert t.get((12345, 6), "нет") == "нет"


def test_membership_matches_the_keys():
    t = ps.PairTable(ROWS)
    assert (100, 3) in t
    assert (100, 4) not in t
    assert (500, 7) in t


def test_len_counts_rows():
    assert len(ps.PairTable(ROWS)) == len(ROWS)


def test_empty_table_is_valid():
    t = ps.PairTable(np.zeros((0, 6)))
    assert len(t) == 0
    assert t.get((1, 2)) is None
    assert (1, 2) not in t


def test_key_that_does_not_fit_the_packing_is_rejected():
    """Молча перепутать ячейки нельзя: id героя обязан влезать в сдвиг."""
    bad = np.array([[1, 1 << 20, 0.0, 0.0, 0.0, 0.0]])
    with pytest.raises(ValueError, match="не влезают в упаковку"):
        ps.PairTable(bad)


def test_neighbouring_keys_do_not_collide():
    """Упаковка `аккаунт << 20 | герой` не должна склеивать соседние ключи."""
    rows = np.array([
        [1, 0, 1.0], [1, 1, 2.0], [2, 0, 3.0],
        [(1 << 20) - 1, 5, 4.0], [1 << 20, 5, 5.0],
    ])
    t = ps.PairTable(rows)
    assert t.get((1, 0))[0] == 1.0
    assert t.get((1, 1))[0] == 2.0
    assert t.get((2, 0))[0] == 3.0
    assert t.get(((1 << 20) - 1, 5))[0] == 4.0
    assert t.get((1 << 20, 5))[0] == 5.0
    assert t.get((1, 2)) is None


def test_values_survive_reordering():
    """Строки внутри таблицы сортируются — значения обязаны ехать вместе с ключом."""
    t = ps.PairTable(ROWS)
    np.testing.assert_allclose(t.get((900, 1)), [13.0, 14.0, 15.0, 16.0])
    np.testing.assert_allclose(t.get((100, 9)), [9.0, 10.0, 11.0, 12.0])
