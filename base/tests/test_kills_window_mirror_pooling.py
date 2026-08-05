"""Зеркальная ориентация vs-ключа в kills_window.

Билдер пишет матчап один раз с якорем на radiant, поэтому `A_vs_B` и `B_vs_A` —
разные наборы матчей. Читатель исторически брал только прямой ключ и терял
половину выборки; под флагом `KILLS_WINDOW_MIRROR_POOLING` он складывает обе
стороны, переворачивая зеркальную.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import functions  # noqa: E402


def _row(leads: int, draws: int, games: int, diff_sum: float) -> list:
    """Строка словаря kills_window для окна 5_15 (первая пятёрка полей)."""
    return [leads, draws, games, diff_sum, 0.0] + [0, 0, 0, 0.0, 0.0] * 3


def _sides():
    radiant = [{"hero_id": i, "pos": i} for i in range(1, 6)]
    dire = [{"hero_id": 10 + i, "pos": i} for i in range(1, 6)]
    return radiant, dire


def _advantage(data, monkeypatch, *, mirror: str, min_games: str = "10"):
    monkeypatch.setenv("KILLS_WINDOW_MIRROR_POOLING", mirror)
    monkeypatch.setenv("KILLS_WINDOW_MIN_GAMES", min_games)
    monkeypatch.setenv("KILLS_WINDOW_LAYER_POLICY", "1v1")
    radiant, dire = _sides()
    return functions.calculate_kills_window_advantage(radiant, dire, data, window="5_15")


def test_mirror_key_is_ignored_when_flag_off(monkeypatch) -> None:
    """Без флага читается только прямой ключ — поведение прода не меняется."""
    data = {
        "1pos1_vs_11pos1": _row(leads=30, draws=0, games=40, diff_sum=80.0),
        "11pos1_vs_1pos1": _row(leads=0, draws=0, games=40, diff_sum=-400.0),
    }
    got = _advantage(data, monkeypatch, mirror="0")

    assert got is not None
    assert got["games"] == 40
    assert got["expected_diff"] == 2.0


def test_mirror_key_is_pooled_with_inverted_sign(monkeypatch) -> None:
    """С флагом зеркало складывается, но с перевёрнутым знаком и лидами."""
    data = {
        "1pos1_vs_11pos1": _row(leads=30, draws=0, games=40, diff_sum=80.0),
        # Зеркало: та же пара, но якорь на другой стороне — 10 лидов из 40
        # означают, что 1pos1 вёл в 30 матчах, а diff_sum надо перевернуть.
        "11pos1_vs_1pos1": _row(leads=10, draws=0, games=40, diff_sum=-40.0),
    }
    got = _advantage(data, monkeypatch, mirror="1")

    assert got is not None
    assert got["games"] == 80
    # diff_sum: 80 + 40 = 120 на 80 игр
    assert got["expected_diff"] == 1.5
    # лиды: 30 прямых + (40 - 10 - 0) зеркальных = 60 из 80
    assert got["lead_probability"] == 0.75


def test_pooled_cell_passes_threshold_that_neither_side_reaches(monkeypatch) -> None:
    """Порог применяется к объединённой ячейке — иначе добор бесполезен там,
    где он и нужен: на редких матчапах."""
    data = {
        "1pos1_vs_11pos1": _row(leads=5, draws=0, games=6, diff_sum=12.0),
        "11pos1_vs_1pos1": _row(leads=1, draws=0, games=6, diff_sum=-6.0),
    }

    assert _advantage(data, monkeypatch, mirror="0") is None
    pooled = _advantage(data, monkeypatch, mirror="1")
    assert pooled is not None
    assert pooled["games"] == 12


def test_pooled_cell_still_respects_threshold_when_both_sides_are_tiny(monkeypatch) -> None:
    data = {
        "1pos1_vs_11pos1": _row(leads=2, draws=0, games=3, diff_sum=4.0),
        "11pos1_vs_1pos1": _row(leads=1, draws=0, games=3, diff_sum=-2.0),
    }

    assert _advantage(data, monkeypatch, mirror="1") is None


def test_missing_mirror_leaves_direct_cell_intact(monkeypatch) -> None:
    """Если зеркала нет, ячейка читается как раньше — без штрафа."""
    data = {"1pos1_vs_11pos1": _row(leads=30, draws=0, games=40, diff_sum=80.0)}

    off = _advantage(data, monkeypatch, mirror="0")
    on = _advantage(data, monkeypatch, mirror="1")

    assert off is not None and on is not None
    assert on["games"] == off["games"] == 40
    assert on["expected_diff"] == off["expected_diff"] == 2.0
