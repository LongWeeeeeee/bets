"""cp1vs1: шринкедж к ролевой оценке вместо жёсткой отсечки.

Тонкая ячейка больше не выбрасывается, а подтягивается к пулу по ролевому
семейству врага с весом CP1VS1_SHRINKAGE_K. Своя позиция при этом не меняется —
прод-инвариант ролевого добора сохранён.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import functions


def _dict(**pairs):
    """{'1pos1_vs_2pos2': (wins, games)} -> формат сырого словаря."""
    return {k: {"wins": w, "draws": 0, "games": g} for k, (w, g) in pairs.items()}


def test_thin_cell_is_no_longer_dropped():
    """Раньше ячейка с 4 играми при min_matches=30 давала отказ по покрытию."""
    data = _dict(**{
        "1pos1_vs_2pos1": (4, 4),      # тонкая точная: 100% на 4 играх
        "1pos1_vs_2pos2": (300, 600),  # ролевой сосед: 50% на 600
        "1pos1_vs_2pos3": (300, 600),
    })
    value, games = functions._lookup_counterpick_1vs1_winrate(data, "1pos1", "2pos1", 30)

    assert value is not None
    assert games >= 30, "ячейка должна проходить гейт покрытия"
    # 4 игры по 100% против пула 1204 игр с ~50.3% при k=100
    assert 0.50 < value < 0.56, value


def test_dense_cell_stays_close_to_exact():
    """На плотной ячейке шринкедж почти не сдвигает оценку."""
    data = _dict(**{
        "1pos1_vs_2pos1": (3500, 5000),  # 70% на 5000 игр
        "1pos1_vs_2pos2": (250, 500),    # 50%
        "1pos1_vs_2pos3": (250, 500),
    })
    value, _games = functions._lookup_counterpick_1vs1_winrate(data, "1pos1", "2pos1", 30)
    assert abs(value - 0.70) < 0.01, value


def test_own_position_is_never_pooled():
    """Пул идёт только по позициям врага; свои позиции не подмешиваются."""
    data = _dict(**{
        "1pos1_vs_2pos1": (10, 20),
        "1pos2_vs_2pos1": (900, 1000),   # своя другая позиция — не должна влиять
    })
    value, _games = functions._lookup_counterpick_1vs1_winrate(data, "1pos1", "2pos1", 30)
    assert value < 0.6, "оценка уехала к чужой своей позиции — пул слишком широкий"


def test_flag_zero_restores_legacy_cutoff(monkeypatch):
    monkeypatch.setattr(functions, "CP1VS1_SHRINKAGE_K", 0.0)
    data = _dict(**{"1pos1_vs_2pos1": (4, 4)})
    value, games = functions._lookup_counterpick_1vs1_winrate(data, "1pos1", "2pos1", 30)
    assert games == 4, "при выключенном флаге games остаются сырыми"
    assert value == 1.0


def test_no_data_returns_nothing():
    value, games = functions._lookup_counterpick_1vs1_winrate({}, "1pos1", "2pos1", 30)
    assert value is None and games == 0


def test_role_pool_includes_exact_position():
    data = _dict(**{
        "1pos1_vs_2pos1": (100, 100),
        "1pos1_vs_2pos2": (0, 100),
    })
    value, games = functions._counterpick_1vs1_role_pool(data, "1pos1", "2", "pos1")
    assert games == 200
    assert abs(value - 0.5) < 1e-9
