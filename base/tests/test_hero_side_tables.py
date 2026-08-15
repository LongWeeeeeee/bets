"""Тесты симметричного блока карточки героя.

Этим модулем строятся 742 колонки из 822 в предматчевой панели, и он общий для
офлайн-обучения и живого пути. Проверяется то, что при расхождении сломает
модель молча: антисимметрия блока, независимость от размера куска, откат
позиционной ячейки к «все позиции» и отпечаток карточки.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hero_side_tables import (  # noqa: E402
    card_fingerprint, hero_tables, side_matrix, sym_block,
)


class FakeCard:
    """Минимальная карточка: два числовых поля, одно списочное, один baseline."""

    hero_numeric_fields = ("attack", "armor")
    hero_list_fields = ("abilities",)
    baseline_metrics = {"pub": ("gpm", "xpm")}

    def __init__(self, heroes):
        self.heroes = heroes


def cell(m, reliable=True):
    return {"reliable_mean": reliable, "m": {k: [v] for k, v in m.items()}}


def make_card():
    return FakeCard({
        1: {"attack": 50.0, "armor": 2.0, "abilities": ["a", "b"],
            "baselines": {"pub": {"all": cell({"gpm": 400, "xpm": 500}),
                                  "pos": {"1": cell({"gpm": 600, "xpm": 700})}}}},
        2: {"attack": 30.0, "armor": 1.0, "abilities": ["a"],
            "baselines": {"pub": {"all": cell({"gpm": 300, "xpm": 350}),
                                  # позиция 1 НЕ надёжна → откат к «все»
                                  "pos": {"1": cell({"gpm": 999, "xpm": 999}, False)}}}},
    })


@pytest.fixture
def tables():
    return hero_tables(make_card())


class TestHeroTables:
    def test_numeric_and_list_count_columns(self, tables):
        C, _B, cnames, _bn = tables
        assert cnames == ["attack", "armor", "abilities_count"]
        assert list(C[1]) == [50.0, 2.0, 2.0]
        assert list(C[2]) == [30.0, 1.0, 1.0]

    def test_baseline_names_prefixed_by_corpus(self, tables):
        _C, _B, _cn, bnames = tables
        assert bnames == ["bl_pub_gpm", "bl_pub_xpm"]

    def test_position_cell_used_when_reliable(self, tables):
        _C, B, _cn, _bn = tables
        assert list(B[1, 1]) == [600.0, 700.0]

    def test_falls_back_to_all_when_position_unreliable(self, tables):
        """Ненадёжная позиционная ячейка должна откатываться, а не давать 999."""
        _C, B, _cn, _bn = tables
        assert list(B[2, 1]) == [300.0, 350.0]

    def test_empty_card_rejected(self):
        with pytest.raises(ValueError):
            hero_tables(FakeCard({}))


class TestSideMatrix:
    def test_sums_over_five_slots(self, tables):
        C, B, _cn, _bn = tables
        her = np.array([[1, 1, 1, 1, 1]])
        out = side_matrix(her, C, B)
        assert out[0, 0] == pytest.approx(5 * 50.0)

    def test_chunking_does_not_change_result(self, tables):
        C, B, _cn, _bn = tables
        rng = np.random.default_rng(3)
        her = rng.integers(1, 3, size=(500, 5))
        assert np.array_equal(side_matrix(her, C, B, chunk=10),
                              side_matrix(her, C, B, chunk=100_000))

    def test_wrong_shape_rejected(self, tables):
        C, B, _cn, _bn = tables
        with pytest.raises(ValueError):
            side_matrix(np.zeros((3, 10), dtype=int), C, B)


class TestSymBlock:
    def test_width_is_double_side_width(self, tables):
        C, B, _cn, _bn = tables
        out = sym_block(np.array([[1, 2, 1, 2, 1, 2, 1, 2, 1, 2]]), C, B)
        assert out.shape[1] == 2 * (C.shape[1] + B.shape[2])

    def test_swapping_sides_negates_diff_and_keeps_sum(self, tables):
        """Обмен сторон обязан переворачивать разность и не трогать сумму."""
        C, B, _cn, _bn = tables
        her = np.array([[1, 1, 2, 2, 1, 2, 2, 1, 1, 2]])
        mirror = her[:, [5, 6, 7, 8, 9, 0, 1, 2, 3, 4]]
        a = sym_block(her, C, B)
        b = sym_block(mirror, C, B)
        half = a.shape[1] // 2
        assert np.allclose(a[:, :half], -b[:, :half])
        assert np.allclose(a[:, half:], b[:, half:])

    def test_identical_sides_give_zero_diff(self, tables):
        C, B, _cn, _bn = tables
        out = sym_block(np.array([[1, 2, 1, 2, 1, 1, 2, 1, 2, 1]]), C, B)
        assert np.allclose(out[:, :out.shape[1] // 2], 0.0)

    def test_wrong_shape_rejected(self, tables):
        C, B, _cn, _bn = tables
        with pytest.raises(ValueError):
            sym_block(np.zeros((2, 5), dtype=int), C, B)


class TestFingerprint:
    def test_stable_and_short(self, tmp_path):
        p = tmp_path / "card.json"
        p.write_bytes(b'{"a": 1}')
        first = card_fingerprint(p)
        assert len(first) == 12
        assert first == card_fingerprint(p)

    def test_changes_with_content(self, tmp_path):
        a, b = tmp_path / "a.json", tmp_path / "b.json"
        a.write_bytes(b'{"a": 1}')
        b.write_bytes(b'{"a": 2}')
        assert card_fingerprint(a) != card_fingerprint(b)
