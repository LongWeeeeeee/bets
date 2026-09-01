"""Смена сторон между картами серии — не смена карты.

24.08.2026, RE ARISE — Team Lynx. Опрос второй карты стартовал из ожидания
следующей карты, то есть в порядке ПЕРВОЙ (RE ARISE, Team Lynx). Когда вторая
карта началась с обменом сторон, `_winline_current_map_is_current` сравнил
`team1`/`team2` ПОЗИЦИОННО, вернул `map_rollover`, и в админ-чат ушло

    🏁 Winline · карта 2 / RE ARISE — Team Lynx / карта завершена   20:36:18
    🆕 Winline · карта 2 / Team Lynx — RE ARISE / 2.55 | 1.42       20:36:45

то есть конец карты, которая только началась, и следом её же открытие. В
`winline_odds_history.jsonl` та же карта лежит под двумя ключами с зеркальными
ценами (1.42/2.55 и 2.55/1.42).

Ключ серии порядок команд уже игнорирует — `_winline_sourcetv_series_key`
сортирует id. Идентичность карты должна вести себя так же.

Контракт:
- пара команд сравнивается как МНОЖЕСТВО: перестановка не роняет карту;
- разные команды при том же номере карты по-прежнему `map_rollover`;
- опрос, заведённый в старом порядке, усыновляется вместе с состоянием чата,
  а цены в нём меняются местами — иначе стрелки движения соврут.
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

SERIES = "sourcetv:league:19944|id:10163973|id:9928636"


@pytest.fixture
def registry(monkeypatch):
    monkeypatch.setattr(cs, "_winline_current_map_registry", {}, raising=False)
    monkeypatch.setattr(cs, "_winline_current_map_pollers", {}, raising=False)
    monkeypatch.setattr(cs, "_winline_pending_next_maps", {}, raising=False)
    monkeypatch.setattr(cs, "_winline_odds_notify_state", {}, raising=False)
    monkeypatch.setattr(cs, "_winline_odds_orientation_state", {}, raising=False)
    return cs._winline_current_map_registry


def _live(**overrides: Any) -> Dict[str, Any]:
    """Реестр: карта 2 идёт, radiant — Team Lynx (стороны обменяли)."""
    entry = {
        "map_num": 2,
        "team1": "Team Lynx",
        "team2": "RE ARISE",
        "active": True,
        "series_last_map": False,
    }
    entry.update(overrides)
    return entry


class _Poller:
    """Минимальный двойник: только то, что читает усыновление."""

    def __init__(self, series: str, map_num: int, team1: str, team2: str):
        self._identity = {"series": series, "map_num": map_num,
                          "team1": team1, "team2": team2}
        self._canonical_key = f"{series}|map{map_num}|{team1}|{team2}"
        self._active = True

    def is_active(self) -> bool:
        return self._active


class TestPredicate:
    def test_side_swap_keeps_map_current(self, registry):
        registry[SERIES] = _live()
        # Опрос помнит порядок первой карты.
        got = cs._winline_current_map_is_current(
            identity={"series": SERIES, "map_num": 2,
                      "team1": "RE ARISE", "team2": "Team Lynx"})
        assert got["current"] is True and got["confirmed"] is True

    def test_same_order_still_current(self, registry):
        registry[SERIES] = _live()
        assert cs._winline_current_map_is_current(
            identity={"series": SERIES, "map_num": 2,
                      "team1": "Team Lynx", "team2": "RE ARISE"})["current"] is True

    def test_other_teams_still_rollover(self, registry):
        registry[SERIES] = _live()
        got = cs._winline_current_map_is_current(
            identity={"series": SERIES, "map_num": 2,
                      "team1": "Team Spirit", "team2": "Team Lynx"})
        assert isinstance(got, dict) and got["reason"] == "map_rollover"

    def test_next_map_is_awaited_not_declared_finished(self, registry):
        """Реестр показывает карту РАНЬШЕ нашей — это перерыв, а не смена карты.

        Опрос карты 3 заводится, пока в мосте ещё карта 2: рынок следующей
        карты Winline открывает раньше. Считать это сменой карты нельзя —
        01.09.2026 такой ответ отправил в чат «🏁 карта завершена» по карте,
        которая не начиналась. Опрос продолжается, но подтверждением это не
        считается.
        """
        registry[SERIES] = _live()
        got = cs._winline_current_map_is_current(
            identity={"series": SERIES, "map_num": 3,
                      "team1": "Team Lynx", "team2": "RE ARISE"})
        assert got["current"] is True
        assert got["reason"] == "awaiting_map_start"
        assert got["confirmed"] is False

    def test_previous_map_is_rolled_over_by_the_next_one(self, registry):
        """Обратный ход: мост показывает карту ПОЗЖЕ нашей — наша кончилась."""
        registry[SERIES] = _live()
        got = cs._winline_current_map_is_current(
            identity={"series": SERIES, "map_num": 1,
                      "team1": "Team Lynx", "team2": "RE ARISE"})
        assert isinstance(got, dict) and got["reason"] == "map_rollover"
        assert got["current"] is False

    def test_missing_name_does_not_fabricate_match(self, registry):
        # Пустое имя схлопывает множество; тогда решает номер карты, а не пара.
        registry[SERIES] = _live(team2="")
        assert cs._winline_current_map_is_current(
            identity={"series": SERIES, "map_num": 2,
                      "team1": "Team Lynx", "team2": ""})["current"] is True


class TestSlot:
    def test_slot_ignores_order(self):
        a = cs._winline_map_slot(SERIES, 2, "RE ARISE", "Team Lynx")
        b = cs._winline_map_slot(SERIES, 2, "Team Lynx", "RE ARISE")
        assert a == b

    def test_slot_separates_maps_and_teams(self):
        base = cs._winline_map_slot(SERIES, 2, "RE ARISE", "Team Lynx")
        assert base != cs._winline_map_slot(SERIES, 3, "RE ARISE", "Team Lynx")
        assert base != cs._winline_map_slot(SERIES, 2, "RE ARISE", "Team Spirit")


class TestAdoption:
    def test_adopts_poller_and_swaps_prices(self, registry):
        old_key = f"{SERIES}|map2|RE ARISE|Team Lynx"
        new_key = f"{SERIES}|map2|Team Lynx|RE ARISE"
        poller = _Poller(SERIES, 2, "RE ARISE", "Team Lynx")
        cs._winline_current_map_pollers[old_key] = poller
        cs._winline_odds_notify_state[old_key] = {"p1": 1.42, "p2": 2.55,
                                                  "kind": "change"}
        cs._winline_odds_orientation_state[old_key] = {"p1": 1.42, "p2": 2.55}

        got = cs._winline_adopt_transposed_poller(
            new_key, SERIES, 2, "Team Lynx", "RE ARISE")

        assert got is poller
        assert old_key not in cs._winline_current_map_pollers
        assert cs._winline_current_map_pollers[new_key] is poller
        assert poller._canonical_key == new_key
        assert poller._identity["team1"] == "Team Lynx"
        # Цены привязаны к подписи: без обмена «1.42» встало бы напротив
        # Team Lynx и следующее сообщение показало бы движение на пустом месте.
        assert cs._winline_odds_notify_state[new_key]["p1"] == 2.55
        assert cs._winline_odds_notify_state[new_key]["p2"] == 1.42
        assert cs._winline_odds_orientation_state[new_key]["p1"] == 2.55

    def test_does_not_adopt_other_map(self, registry):
        other = f"{SERIES}|map1|RE ARISE|Team Lynx"
        cs._winline_current_map_pollers[other] = _Poller(
            SERIES, 1, "RE ARISE", "Team Lynx")
        assert cs._winline_adopt_transposed_poller(
            f"{SERIES}|map2|Team Lynx|RE ARISE", SERIES, 2,
            "Team Lynx", "RE ARISE") is None

    def test_does_not_adopt_inactive(self, registry):
        old_key = f"{SERIES}|map2|RE ARISE|Team Lynx"
        poller = _Poller(SERIES, 2, "RE ARISE", "Team Lynx")
        poller._active = False
        cs._winline_current_map_pollers[old_key] = poller
        assert cs._winline_adopt_transposed_poller(
            f"{SERIES}|map2|Team Lynx|RE ARISE", SERIES, 2,
            "Team Lynx", "RE ARISE") is None
        assert old_key in cs._winline_current_map_pollers
