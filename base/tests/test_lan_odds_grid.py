"""Инварианты пороговой сетки «уверенность → винрейт → безубыточный кэф».

Таблица правится редко и вручную, а её поломка не видна: модель продолжает
считать, ставки продолжают уходить, просто по неверной цене. Поэтому здесь
закреплены свойства, нарушение которых означает опечатку, а не новое знание.
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import prematch_scorer as ps  # noqa: E402

PCTS = list(range(50, 100))


def test_every_confidence_has_an_entry():
    assert sorted(ps.LAN_ODDS_GRID) == PCTS


def test_winrate_never_decreases_with_confidence():
    wr = [ps.LAN_ODDS_GRID[p][0] for p in PCTS]
    for a, b, p in zip(wr, wr[1:], PCTS[1:]):
        assert a <= b + 1e-12, f"на {p}% винрейт упал: {a} -> {b}"


def test_min_odds_never_increases_with_confidence():
    """Более уверенной ставке не может требоваться БОЛЬШИЙ кэф."""
    od = [ps.LAN_ODDS_GRID[p][1] for p in PCTS]
    for a, b, p in zip(od, od[1:], PCTS[1:]):
        assert a >= b - 1e-12, f"на {p}% требуемый кэф вырос: {a} -> {b}"


def test_min_odds_matches_the_winrate():
    """Кэф — это округлённый вверх 1/винрейт, иначе ставка не окупается."""
    for p in PCTS:
        wr, odds = ps.LAN_ODDS_GRID[p]
        assert odds >= 1.0 / wr - 1e-9, f"{p}%: кэф {odds} ниже безубыточного {1/wr:.4f}"
        assert odds <= math.ceil(100.0 / wr) / 100.0 + 1e-9, (
            f"{p}%: кэф {odds} завышен против {math.ceil(100/wr)/100:.2f}")


def test_values_above_the_reliable_ceiling_are_frozen():
    """Выше 85% данных нет — там значение обязано быть заморожено (E-142)."""
    top = ps.LAN_ODDS_GRID[ps.LAST_RELIABLE_CONF]
    for p in range(ps.LAST_RELIABLE_CONF, 100):
        assert ps.LAN_ODDS_GRID[p] == top, f"{p}% расходится с потолком {top}"


def test_the_two_groups_raised_on_20_08_keep_their_values():
    """Правка по сквозному аудиту: две группы подняты, остальные не тронуты.

    58-63: 0.5817 -> 0.600 (2 319 карт, факт 0.613, нижняя граница 0.600)
    77-80: 0.7541 -> 0.760 (  651 карта, факт 0.782, нижняя граница 0.760)
    """
    for p in range(58, 64):
        assert ps.LAN_ODDS_GRID[p] == (0.6, 1.67), p
    for p in range(77, 81):
        assert ps.LAN_ODDS_GRID[p] == (0.76, 1.32), p
    # соседи слева и справа остались прежними — правка групповая, не сплошная
    assert ps.LAN_ODDS_GRID[57] == (0.5387, 1.86)
    assert ps.LAN_ODDS_GRID[64] == (0.6267, 1.6)
    assert ps.LAN_ODDS_GRID[76] == (0.7324, 1.37)
    assert ps.LAN_ODDS_GRID[81] == (0.7763, 1.29)


def test_helpers_agree_with_the_table():
    for p in (50, 58, 63, 64, 77, 80, 85, 99):
        c = p / 100.0
        assert ps.lan_expected_wr(c) == ps.LAN_ODDS_GRID[p][0]
        assert ps.lan_min_odds(c) == ps.LAN_ODDS_GRID[p][1]


def test_confidence_outside_the_table_is_clamped():
    assert ps.lan_expected_wr(0.10) == ps.LAN_ODDS_GRID[50][0]
    assert ps.lan_expected_wr(1.50) == ps.LAN_ODDS_GRID[99][0]
