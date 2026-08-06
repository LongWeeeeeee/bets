"""Отбраковка подтверждённых подмен позиций при сборке словарей.

Замер (docs/EXPERIMENTS.md E-24): подмена портит ячейку незаметно только когда
герой РЕАЛЬНО играется на обеих позициях — иначе редкую ячейку отсечёт
min_games. Поэтому три условия обязаны выполняться одновременно, и тест
проверяет, что снятие любого из них отменяет отбраковку: иначе фильтр начнёт
портить верные метки офф-мета пиков (их 34% среди кандидатов).

Проверяются ВСЕ пары позиций: существующая починка в check_match_quality знает
лишь pos1<->pos5 и pos3<->pos4, что покрывает четверть случаев.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import explore_database as ed  # noqa: E402

FLEX = ed.SWAP_FLEX_MIN_GAMES
# Оба героя гибкие: много игр на обеих позициях пары.
CELLS = {
    "10|POSITION_2": {"n": FLEX * 2},
    "10|POSITION_4": {"n": FLEX * 2},
    "20|POSITION_4": {"n": FLEX * 2},
    "20|POSITION_2": {"n": FLEX * 2},
}


def _player(hero, pos, networth, radiant=True):
    return {"heroId": hero, "position": pos, "networth": networth, "isRadiant": radiant}


def _match(nw_core=5000, nw_other=20000):
    """Герой 10 стоит на pos2 (корный слот) и БЕДНЕЕ героя 20 на pos4 —
    нетворт противоречит меткам."""
    return {"players": [
        _player(10, "POSITION_2", nw_core),
        _player(20, "POSITION_4", nw_other),
        _player(30, "POSITION_1", 25000),
        _player(40, "POSITION_3", 15000),
        _player(50, "POSITION_5", 8000),
    ]}


@pytest.fixture(autouse=True)
def _catalog(monkeypatch):
    """Каталог: герой 10 не умеет pos2 (но умеет pos4), герой 20 наоборот."""
    def valid(hero, pos):
        if hero == 10:
            return pos != "POSITION_2"
        if hero == 20:
            return pos != "POSITION_4"
        return True

    monkeypatch.setattr(ed, "_position_is_valid_for_hero", valid)


def test_confirmed_swap_is_detected_outside_lane_pairs() -> None:
    """pos2<->pos4 — пара, которую текущая починка не знает."""
    got = ed._confirmed_position_swap(_match(), CELLS)

    assert got == ("POSITION_2", "POSITION_4")


def test_networth_agreeing_with_labels_is_not_a_swap() -> None:
    """Если «кор» богаче — метки скорее верны, это офф-мета пик."""
    got = ed._confirmed_position_swap(_match(nw_core=25000, nw_other=6000), CELLS)

    assert got is None


def test_rare_hero_position_is_not_worth_dropping() -> None:
    """Герой не играется на обеих позициях — ячейка редкая, её отсечёт
    min_games, и выбрасывать матч незачем."""
    thin = {k: {"n": 10} for k in CELLS}

    assert ed._confirmed_position_swap(_match(), thin) is None


def test_no_swap_when_catalog_forbids_the_exchange(monkeypatch) -> None:
    """Обмен не чинит: герой 20 не умеет pos2, значит подмены не было."""
    monkeypatch.setattr(ed, "_position_is_valid_for_hero",
                        lambda hero, pos: False if hero == 10 and pos == "POSITION_2" else
                        (False if hero == 20 and pos == "POSITION_2" else True))

    assert ed._confirmed_position_swap(_match(), CELLS) is None


def test_clean_match_yields_nothing() -> None:
    clean = {"players": [
        _player(30, "POSITION_1", 25000),
        _player(40, "POSITION_3", 15000),
        _player(50, "POSITION_5", 8000),
    ]}

    assert ed._confirmed_position_swap(clean, CELLS) is None


def test_missing_baselines_disable_detection() -> None:
    assert ed._confirmed_position_swap(_match(), {}) is None


def test_all_gates_are_off_by_default() -> None:
    assert ed.DROP_POSITION_SWAPS is False
    assert ed.STRICT_POSITIONS_ENABLED is True, "дефолт обязан сохранять прежнее поведение"
