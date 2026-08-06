"""Метка `positions_repaired` при перестановке позиций.

Зачем она нужна (docs/EXPERIMENTS.md E-24): починка мутирует матч ДО сохранения
в корпус, исходные позиции нигде не остаются — поэтому отличить починенный матч
от изначально чистого невозможно, и сравнить починку с отбраковкой не на чем.
Метка даёт выборку на будущее и позволяет проверить главный риск: что
перестановка иногда делается неверно и матч заезжает в словарь с чужими
позиционными ключами.

Метка обязана появляться ТОЛЬКО когда перестановка реально произошла — иначе
она бесполезна как признак.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import maps_research  # noqa: E402


def _p(hero, pos, networth, radiant=True):
    return {
        "heroId": hero,
        "position": pos,
        "networth": networth,
        "isRadiant": radiant,
        "intentionalFeeding": False,
        "deaths": 3,
        "steamAccount": {"id": hero, "smurfFlag": 0, "isAnonymous": False},
    }


def _match(players):
    return {"players": players, "radiantNetworthLeads": [0] * 30}


@pytest.fixture()
def swap_catalog(monkeypatch):
    """Каталог, при котором pos1 и pos5 у radiant невалидны, но обмен их чинит."""
    def valid(hero, pos):
        norm = str(pos).lower().replace("position_", "pos")
        if hero == 1:   # «кор», на деле саппорт
            return norm != "pos1"
        if hero == 5:   # «саппорт», на деле кор
            return norm != "pos5"
        return True

    monkeypatch.setattr(maps_research, "_has_position_catalog", lambda: True)
    monkeypatch.setattr(maps_research, "_position_is_valid_for_hero", valid)


def _swappable_match(core_networth=4000, support_networth=20000):
    """У «pos1» меньше нетворта, чем у «pos5» — признак подмены."""
    return _match([
        _p(1, "pos1", core_networth),
        _p(5, "pos5", support_networth),
        _p(2, "pos2", 15000),
        _p(3, "pos3", 12000),
        _p(4, "pos4", 7000),
        _p(11, "pos1", 22000, radiant=False),
        _p(12, "pos2", 16000, radiant=False),
        _p(13, "pos3", 13000, radiant=False),
        _p(14, "pos4", 8000, radiant=False),
        _p(15, "pos5", 6000, radiant=False),
    ])


def test_marker_appears_when_swap_happens(swap_catalog) -> None:
    match = _swappable_match()

    ok, reason = maps_research.check_match_quality(match)

    assert ok is True, reason
    marks = match.get("positions_repaired")
    assert marks, "починка произошла, а следа не осталось"
    mark = marks[0]
    assert mark["core_pos"] == "pos1" and mark["support_pos"] == "pos5"
    assert mark["core_hero"] == 1 and mark["support_hero"] == 5
    # Нетворт, по которому принято решение, сохраняется — иначе решение
    # потом не перепроверить.
    assert mark["core_networth"] < mark["support_networth"]


def test_positions_are_actually_swapped(swap_catalog) -> None:
    match = _swappable_match()

    maps_research.check_match_quality(match)

    by_hero = {p["heroId"]: p["position"] for p in match["players"]}
    assert by_hero[1] == "pos5" and by_hero[5] == "pos1"


def test_no_marker_on_a_clean_match(monkeypatch) -> None:
    monkeypatch.setattr(maps_research, "_has_position_catalog", lambda: True)
    monkeypatch.setattr(maps_research, "_position_is_valid_for_hero", lambda h, p: True)
    match = _swappable_match()

    ok, reason = maps_research.check_match_quality(match)

    assert ok is True, reason
    assert "positions_repaired" not in match


def test_no_marker_when_match_is_rejected(monkeypatch) -> None:
    """Нетворт подтверждает текущие метки — перестановки нет, значит нет и метки."""
    def valid(hero, pos):
        norm = str(pos).lower().replace("position_", "pos")
        if hero == 1:
            return norm != "pos1"
        if hero == 5:
            return norm != "pos5"
        return True

    monkeypatch.setattr(maps_research, "_has_position_catalog", lambda: True)
    monkeypatch.setattr(maps_research, "_position_is_valid_for_hero", valid)
    # «Кор» богаче саппорта — путь уходит в 'invalid positions networth order'.
    match = _swappable_match(core_networth=20000, support_networth=4000)

    ok, reason = maps_research.check_match_quality(match)

    assert ok is False
    assert reason == "invalid positions networth order"
    assert "positions_repaired" not in match
