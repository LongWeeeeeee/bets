"""Вес надёжности вместо жёсткого гейта у драфт-метрик.

По умолчанию режим выключен и `_draft_cell_admit` повторяет прод: порог
min_matches, вес = число игр. При `DRAFT_CELL_RELIABILITY_K>0` порога нет, а
редкая ячейка входит с малым весом games/(games+K).
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import functions  # noqa: E402


def _reloaded(monkeypatch, k: str):
    monkeypatch.setenv("DRAFT_CELL_RELIABILITY_K", k)
    return importlib.reload(functions)


def test_default_is_hard_gate(monkeypatch) -> None:
    mod = _reloaded(monkeypatch, "0")

    assert mod.DRAFT_CELL_RELIABILITY_K == 0
    assert mod.GET_DIFF_WEIGHT_POWER == 0.0
    assert mod._draft_cell_admit(99, 100) == (False, 99)
    assert mod._draft_cell_admit(100, 100) == (True, 100)


def test_reliability_mode_admits_sparse_cell_with_small_weight(monkeypatch) -> None:
    mod = _reloaded(monkeypatch, "50")

    admit, weight = mod._draft_cell_admit(20, 100)
    assert admit is True
    assert abs(weight - 20 / 70) < 1e-9

    admit, weight = mod._draft_cell_admit(300, 100)
    assert admit is True
    assert abs(weight - 300 / 350) < 1e-9


def test_reliability_weight_is_honoured_by_get_diff(monkeypatch) -> None:
    """Вес должен доезжать до агрегатора: при power=0 он схлопнулся бы в 1."""
    mod = _reloaded(monkeypatch, "50")
    assert mod.GET_DIFF_WEIGHT_POWER == 1.0


def test_empty_cell_is_never_admitted(monkeypatch) -> None:
    mod = _reloaded(monkeypatch, "50")

    assert mod._draft_cell_admit(0, 100) == (False, 0.0)
    assert mod._draft_cell_admit(None, 100) == (False, 0.0)


def test_broken_games_value_is_rejected(monkeypatch) -> None:
    mod = _reloaded(monkeypatch, "50")

    assert mod._draft_cell_admit("много", 100)[0] is False


def test_weight_and_games_live_in_separate_fields(monkeypatch) -> None:
    """Ячейка = (значение, СЫРЫЕ игры, позиция врага, вес).

    Смысл развода: диагностика `*_games` читает индекс 1 и не должна зависеть
    от того, какой схемой взвешивания мы пользуемся. Раньше вес занимал то же
    поле, и включение любого взвешивания портило цифры в сообщении.
    """
    mod = _reloaded(monkeypatch, "50")

    radiant = {f"pos{i}": {"hero_id": i} for i in range(1, 6)}
    dire = {f"pos{i}": {"hero_id": 10 + i} for i in range(1, 6)}
    data = {}
    for i in range(1, 6):
        for j in range(1, 6):
            data[f"{i}pos{i}_vs_{10 + j}pos{j}"] = {"wins": 16, "games": 20}

    output = {}
    mod.counterpick_team(radiant, dire, output, "radiant_counterpick", data)

    cells = output["radiant_counterpick_1vs1"]["pos1"]
    assert cells, "ячейки должны собираться: в режиме надёжности гейта нет"
    for value, games, enemy_pos, weight in cells:
        assert games == 20, "в поле игр обязаны лежать сырые игры"
        assert isinstance(enemy_pos, str)
        assert abs(weight - 20 / 70) < 1e-9, "вес — отдельным полем"
        assert value == 0.8

    # Диагностика читает именно игры, а не вес.
    assert mod._diagnostic_support_from_entry(cells[0]) == 20


def teardown_module(_module) -> None:
    """Возвращаем модуль в дефолт, иначе следующие тесты увидят чужой режим."""
    import os

    os.environ.pop("DRAFT_CELL_RELIABILITY_K", None)
    importlib.reload(functions)
