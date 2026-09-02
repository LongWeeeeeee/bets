"""Идентичность серии Winline: одна физическая серия — один ключ.

Ключ серии строится из `league_id` и пары team id моста SourceTV
(`_winline_sourcetv_series_key`). 02.09.2026 в серии Yangon Galacticos —
InterActive Philippines мост дал Yangon РАЗНЫЕ id на разных картах: 7653080 на
карте 1 и 8944230 на карте 2. Серия расщепилась на два ключа, и карту 2
одновременно опрашивали два поллера — настоящий (такт 3.5 с, `match_id`
8978758321) и сирота из ожидания следующей карты (такт 60 с, `match_id=None`).
Оба читали одну карточку Winline с зеркальными ценами, а сирота после смерти
записи реестра ушёл в бесконечный зомби-цикл «⏹️ опрос остановлен»
(`test_winline_next_map_polling.py`).

Контракт:
- team_id приводится к одному значению на имя команды — карточку Winline мы
  всё равно ищем по имени;
- id из моста, уже известный справочнику tier1/2 под этим именем, остаётся
  собой: ключи в логах и истории кэфов читаемы;
- решение принимается один раз на имя, иначе рост справочника расщепил бы ключ
  точно так же, как флип id у моста;
- имена-плейсхолдеры («Dire», «Radiant») не сводятся: под ними ходят разные
  команды.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402

YANGON = "yangongalacticos"
INTERACTIVE = "interactivephilippines"


@pytest.fixture(autouse=True)
def _clean_identity_state(monkeypatch):
    monkeypatch.setattr(cs, "_winline_team_id_canonical", {}, raising=False)
    monkeypatch.setattr(
        cs, "_winline_team_id_index_cache", {"at": 0.0, "by_name": {}}, raising=False)
    yield


def _row(radiant_id: Any, radiant: str, dire_id: Any, dire: str,
         *, league: int = 18865, game_number: int = 1) -> Dict[str, Any]:
    return {
        "league_id": league,
        "radiant_team_id": radiant_id,
        "radiant_team_name": radiant,
        "dire_team_id": dire_id,
        "dire_team_name": dire,
        "series_game_number": game_number,
        "status": "live",
    }


def _dict_of(monkeypatch, known: Dict[str, Any]) -> None:
    """Подменить справочник известных id, не трогая настоящий tier1/2."""
    monkeypatch.setattr(cs, "_winline_known_team_ids_by_name",
                        lambda name_key: known.get(name_key) or frozenset())


def test_team_id_flip_inside_a_series_keeps_one_key(monkeypatch) -> None:
    """Кейс инцидента: 7653080 на карте 1 и 8944230 на карте 2 — одна серия."""
    _dict_of(monkeypatch, {YANGON: {8944230, 9546449}, INTERACTIVE: {7917893}})
    map1 = _row(7653080, "Yangon Galacticos", 7917893, "InterActive Philippines",
                game_number=1)
    map2 = _row(7917893, "InterActive Philippines", 8944230, "Yangon Galacticos",
                game_number=2)

    assert cs._winline_sourcetv_series_key(map1) == cs._winline_sourcetv_series_key(map2)
    assert cs._winline_sourcetv_series_key(map1) == (
        "sourcetv:league:18865|id:7917893|id:8944230")


def test_flip_between_two_known_ids_does_not_split_the_series(monkeypatch) -> None:
    """Справочник знает два id одного имени — выбор не должен плавать."""
    _dict_of(monkeypatch, {YANGON: {8944230, 9546449}, INTERACTIVE: {7917893}})
    first = _row(9546449, "Yangon Galacticos", 7917893, "InterActive Philippines")
    second = _row(7917893, "InterActive Philippines", 8944230, "Yangon Galacticos",
                  game_number=2)

    assert cs._winline_sourcetv_series_key(first) == cs._winline_sourcetv_series_key(second)


def test_known_bridge_id_is_kept_readable(monkeypatch) -> None:
    """Известный справочнику id из моста остаётся собой, а не заменяется минимумом.

    Иначе ключи в `runtime/winline_odds_history.jsonl` и в логах теряли бы
    связь с тем id, который реально прислал мост.
    """
    _dict_of(monkeypatch, {"puckchamp": {457, 10164236}})
    row = _row(10164236, "PuckChamp", 10232231, "Klim Sani4")

    assert cs._winline_sourcetv_series_key(row) == (
        "sourcetv:league:18865|id:10164236|id:10232231")


def test_name_unknown_to_the_dict_keeps_the_bridge_id(monkeypatch) -> None:
    _dict_of(monkeypatch, {})
    row = _row(10232231, "Klim Sani4", 10232570, "Team Syntax")

    assert cs._winline_sourcetv_series_key(row) == (
        "sourcetv:league:18865|id:10232231|id:10232570")


def test_placeholder_names_are_not_merged(monkeypatch) -> None:
    """Под «Dire» ходят разные команды: сводить их id в один нельзя."""
    _dict_of(monkeypatch, {"dire": {111}})
    one = _row(222, "Dire", 7917893, "InterActive Philippines")
    other = _row(333, "Dire", 7917893, "InterActive Philippines")

    assert cs._winline_canonical_team_id(222, "Dire") == "222"
    assert cs._winline_sourcetv_series_key(one) != cs._winline_sourcetv_series_key(other)


def test_missing_id_still_falls_back_to_names(monkeypatch) -> None:
    """Без пары id ключ строится из имён — как и до канонизации."""
    _dict_of(monkeypatch, {YANGON: {8944230}})
    row = _row(0, "Yangon Galacticos", None, "InterActive Philippines")

    assert cs._winline_sourcetv_series_key(row) == (
        "sourcetv:league:18865|name:interactive philippines|name:yangon galacticos")


def test_incident_series_merges_with_the_real_tier_dict() -> None:
    """Тот же кейс на настоящем справочнике tier1/2, без подмены."""
    known = set(cs._winline_known_team_ids_by_name(YANGON))
    if not {8944230, 9546449} <= known:
        pytest.skip(f"справочник не знает id Yangon Galacticos: {sorted(known)}")
    map1 = _row(7653080, "Yangon Galacticos", 7917893, "InterActive Philippines")
    map2 = _row(7917893, "InterActive Philippines", 8944230, "Yangon Galacticos",
                game_number=2)

    assert cs._winline_sourcetv_series_key(map1) == cs._winline_sourcetv_series_key(map2)
