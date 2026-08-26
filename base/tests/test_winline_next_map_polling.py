"""Опрос коэффициентов следующей карты серии, пока её нет в мосте.

Раньше опрос стартовал только из обработки живой карты SourceTV
(`ensure_winline_current_map_polling` под `is_sourcetv_card`), поэтому карта,
не попавшая в мост, коэффициентов не получала вовсе. 22.08.2026 у серии
BoomBoys — Team Spirit на TI по карте 1 прошло 294 опроса, по карте 2 — ни
одного: Valve не поднял по ней SourceTV, и карта в мост не пришла. Рынок
следующей карты Winline при этом открывает раньше, чем карта появляется у
Valve.

Контракт:
- когда серия ушла из моста доказанно, а сыгранная карта не была решающей,
  реестр помечает ожидаемую следующую карту и предикат «карта текущая»
  пропускает её, иначе поллер не стартовал бы;
- ожидание ограничено окном (по корпусу пауза между картами — медиана 30 мин,
  90-й перцентиль 47), после него опрос гаснет сам;
- такт ожидания редкий, а когда карта приходит в мост, он возвращается к
  обычному.
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


def _reg(**overrides: Any) -> Dict[str, Any]:
    """Запись реестра для серии, чья карта 1 только что ушла из моста."""
    entry = {
        "map_num": 1,
        "team1": "BoomBoys",
        "team2": "Team Spirit",
        "series_last_map": False,
        "active": False,
        "inactive_reason": "source_absent",
        "inactive_proven": True,
        "inactive_since": 1_000.0,
        # Bo3, счёт 0:0 — после первой карты вторая будет при любом исходе.
        "series_type": 1,
        "wins": (0, 0),
    }
    entry.update(overrides)
    return entry


@pytest.fixture
def registry(monkeypatch):
    """Пустой реестр серий, набор поллеров и список ожиданий на время теста."""
    monkeypatch.setattr(cs, "_winline_current_map_registry", {}, raising=False)
    monkeypatch.setattr(cs, "_winline_current_map_pollers", {}, raising=False)
    monkeypatch.setattr(cs, "_winline_pending_next_maps", {}, raising=False)
    return cs._winline_current_map_registry


SERIES = "sourcetv:series:1"


def _await(map_num: int = 2, since: Any = None) -> None:
    """Поставить ожидание карты так, как это делает reconcile."""
    cs._winline_pending_next_maps[SERIES] = {
        "map_num": map_num,
        "since": cs.time.time() if since is None else since,
        "team1": "BoomBoys",
        "team2": "Team Spirit",
    }


# ── пометка ожидания ────────────────────────────────────────────────────────

def test_next_map_is_noted_after_a_non_decider_map(registry) -> None:
    assert cs._winline_note_pending_next_map(SERIES, _reg(), now=2_000.0) == 2
    state = cs._winline_pending_next_maps[SERIES]
    assert state["map_num"] == 2
    assert state["since"] == 2_000.0
    assert state["team1"] == "BoomBoys" and state["team2"] == "Team Spirit"


def test_repeat_reconcile_does_not_restart_the_wait(registry) -> None:
    """Мост читается каждый цикл — отсчёт окна не должен обнуляться."""
    cs._winline_note_pending_next_map(SERIES, _reg(), now=2_000.0)
    assert cs._winline_note_pending_next_map(SERIES, _reg(), now=2_600.0) == 2
    assert cs._winline_pending_next_maps[SERIES]["since"] == 2_000.0


def test_running_wait_is_not_advanced_by_our_own_registration(registry) -> None:
    """Запуск опроса пишет ожидаемую карту в реестр как активную.

    Следующий проход reconcile увидит её там и без защиты начал бы ждать карту
    через одну, перескочив ту, которую мы ещё ждём.
    """
    cs._winline_note_pending_next_map(SERIES, _reg(), now=2_000.0)
    assert cs._winline_note_pending_next_map(
        SERIES, _reg(map_num=2, wins=(1, 0)), now=2_100.0) == 2
    assert cs._winline_pending_next_maps[SERIES]["map_num"] == 2


def test_expired_wait_is_dropped_and_not_renewed(registry) -> None:
    cs._winline_note_pending_next_map(SERIES, _reg(), now=2_000.0)
    assert cs._winline_note_pending_next_map(
        SERIES, _reg(map_num=2, wins=(1, 0)), now=2_000.0 + 10 * 3600) is None
    assert SERIES not in cs._winline_pending_next_maps


def test_decider_map_ends_the_series(registry) -> None:
    """После решающей карты следующей нет — ждать нечего."""
    assert cs._winline_note_pending_next_map(SERIES, _reg(series_last_map=True)) is None


def test_unproven_absence_is_not_a_finished_map(registry) -> None:
    """Мост может просто молчать: это ещё не конец карты."""
    assert cs._winline_note_pending_next_map(
        SERIES, _reg(inactive_proven=False, inactive_reason="source_stale")) is None


def test_series_cannot_grow_past_five_maps(registry) -> None:
    """Пятая карта Bo5 неизбежной не бывает: после четвёртой счёт уже 2:1.

    Ждать её можно только зная исход четвёртой, а его у нас нет.
    """
    assert cs._winline_note_pending_next_map(
        SERIES, _reg(map_num=4, series_type=2, wins=(2, 1))) is None
    cs._winline_pending_next_maps.clear()
    assert cs._winline_note_pending_next_map(SERIES, _reg(map_num=5)) is None
    assert cs._winline_note_pending_next_map(SERIES, _reg(map_num=None)) is None


# ── предикат «карта текущая» ────────────────────────────────────────────────

def test_predicate_lets_the_awaited_map_be_polled(registry) -> None:
    """Без этого `begin()` отказал бы: серии в мосте нет, карта «не текущая»."""
    registry[SERIES] = _reg()
    _await(2)
    verdict = cs._winline_current_map_is_current(
        series=SERIES, map_num=2, team1="BoomBoys", team2="Team Spirit")
    assert verdict is True


def test_predicate_still_refuses_a_map_nobody_awaits(registry) -> None:
    registry[SERIES] = _reg()
    _await(2)
    verdict = cs._winline_current_map_is_current(
        series=SERIES, map_num=3, team1="BoomBoys", team2="Team Spirit")
    assert verdict is not True
    assert verdict["current"] is False


def test_waiting_window_expires(registry, monkeypatch) -> None:
    _await(2, since=2_000.0)
    monkeypatch.setattr(cs, "_winline_next_map_wait_seconds", lambda: 45 * 60.0)
    assert cs._winline_awaiting_next_map(SERIES, 2, now=2_000.0 + 44 * 60) is True
    assert cs._winline_awaiting_next_map(SERIES, 2, now=2_000.0 + 46 * 60) is False


def test_bridge_snapshot_clears_the_wait(registry) -> None:
    """Карта пришла — reconcile перезаписывает запись серии целиком."""
    registry[SERIES] = _reg()
    _await(2)
    cs._reconcile_winline_sourcetv_polling(
        {
            "8959362208": {
                "series_id": "1",
                "radiant_team_name": "BoomBoys",
                "dire_team_name": "Team Spirit",
                "series_game_number": 2,
                "series_type": 1,
                "radiant_series_wins": 1,
                "dire_series_wins": 0,
            }
        },
        authoritative=True,
    )
    entry = cs._winline_current_map_registry[SERIES]
    assert SERIES not in cs._winline_pending_next_maps
    assert entry["active"] is True
    assert entry["map_num"] == 2


# ── запуск опроса ───────────────────────────────────────────────────────────

def test_polling_starts_for_the_awaited_map(registry, monkeypatch) -> None:
    calls: list = []

    def _fake_ensure(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return object()

    monkeypatch.setattr(cs, "ensure_winline_current_map_polling", _fake_ensure)
    _await(2)
    assert cs._winline_start_pending_next_map_polling() == 1
    assert calls[0]["map_num"] == 2
    assert calls[0]["team1"] == "BoomBoys" and calls[0]["team2"] == "Team Spirit"
    # Такт ожидания заметно реже обычного: рынка до начала карты нет.
    assert calls[0]["poll_interval_seconds"] >= 30.0
    # Решающей эту карту мы не знаем — счёт серии после пропавшей карты неизвестен.
    assert calls[0]["series_last_map"] is False


def test_polling_is_not_started_twice_for_one_map(registry, monkeypatch) -> None:
    class _Live:
        def is_active(self) -> bool:
            return True

    monkeypatch.setattr(
        cs, "ensure_winline_current_map_polling",
        lambda **kw: pytest.fail("повторный запуск опроса той же карты"))
    _await(2)
    cs._winline_current_map_pollers[
        f"{SERIES}|map2|BoomBoys|Team Spirit"] = _Live()
    assert cs._winline_start_pending_next_map_polling() == 0


def test_expired_wait_starts_nothing(registry, monkeypatch) -> None:
    monkeypatch.setattr(
        cs, "ensure_winline_current_map_polling",
        lambda **kw: pytest.fail("опрос за пределами окна ожидания"))
    _await(2, since=cs.time.time() - 10 * 3600)
    assert cs._winline_start_pending_next_map_polling() == 0


# ── такт ────────────────────────────────────────────────────────────────────

def test_confirmed_map_returns_to_the_normal_cadence() -> None:
    class _Poller:
        _poll_interval = 60.0

    poller = _Poller()
    assert cs._winline_apply_poll_interval(poller, None) is True
    assert poller._poll_interval == 5.0
    # Повторный вызов с тем же значением ничего не меняет.
    assert cs._winline_apply_poll_interval(poller, None) is False
    assert cs._winline_apply_poll_interval(poller, 60.0) is True
    assert poller._poll_interval == 60.0


def test_bad_interval_is_ignored() -> None:
    class _Poller:
        _poll_interval = 5.0

    poller = _Poller()
    assert cs._winline_apply_poll_interval(poller, 0) is False
    assert cs._winline_apply_poll_interval(poller, -1) is False
    assert cs._winline_apply_poll_interval(poller, "быстрее") is False
    assert poller._poll_interval == 5.0


def test_waiting_poller_never_reloads_the_shared_page(registry, monkeypatch) -> None:
    """Страница у всех опросов одна: ожидание не должно её перезагружать.

    Пока карта не началась, рынка нет и после перезагрузки, а сама она на
    десятки секунд лишает разметки живые карты соседних матчей.
    """
    calls: list = []
    monkeypatch.setattr(cs, "ensure_winline_current_map_polling",
                        lambda **kw: calls.append(kw) or object())
    _await(2)
    assert cs._winline_start_pending_next_map_polling() == 1
    assert calls[0]["reload_after_consecutive_misses"] > 10_000


def test_reconcile_starts_waiting_when_the_series_leaves_the_bridge(
    registry, monkeypatch,
) -> None:
    """Полный путь: карта ушла из моста → ожидание → опрос следующей карты."""
    calls: list = []
    monkeypatch.setattr(cs, "ensure_winline_current_map_polling",
                        lambda **kw: calls.append(kw) or object())
    # Карта 1 идёт.
    cs._reconcile_winline_sourcetv_polling(
        {
            "8959222564": {
                "series_id": "1",
                "radiant_team_name": "BoomBoys",
                "dire_team_name": "Team Spirit",
                "series_game_number": 1,
                "series_type": 1,
                "radiant_series_wins": 0,
                "dire_series_wins": 0,
            }
        },
        authoritative=True,
    )
    assert calls == []
    # Карта кончилась, мост пуст.
    cs._reconcile_winline_sourcetv_polling({}, authoritative=True)
    assert cs._winline_pending_next_maps[SERIES]["map_num"] == 2
    assert len(calls) == 1 and calls[0]["map_num"] == 2


def test_series_that_ended_on_its_decider_starts_no_waiting(
    registry, monkeypatch,
) -> None:
    """Bo3 при 1:1 — карта 3 решающая, после неё ждать нечего."""
    monkeypatch.setattr(
        cs, "ensure_winline_current_map_polling",
        lambda **kw: pytest.fail("опрос после решающей карты серии"))
    cs._reconcile_winline_sourcetv_polling(
        {
            "8959222564": {
                "series_id": "1",
                "radiant_team_name": "BoomBoys",
                "dire_team_name": "Team Spirit",
                "series_game_number": 3,
                "series_type": 1,
                "radiant_series_wins": 1,
                "dire_series_wins": 1,
            }
        },
        authoritative=True,
    )
    cs._reconcile_winline_sourcetv_polling({}, authoritative=True)
    assert SERIES not in cs._winline_pending_next_maps


def test_waiting_does_not_change_the_producer_generation(registry, monkeypatch) -> None:
    """Смена поколения продюсера терминальна для ВСЕХ живых опросов.

    Регистрация опроса умеет назначить поколение сама, по `os.getpid()`. Если
    бы ожидание следующей карты этим пользовалось, оно на ровном месте гасило
    бы опрос карты, которая идёт прямо сейчас.
    """
    calls: list = []
    monkeypatch.setattr(cs, "ensure_winline_current_map_polling",
                        lambda **kw: calls.append(kw) or object())
    monkeypatch.setattr(cs, "_winline_current_map_service_gen",
                        {"pid": 4242, "gen": "pid-4242"}, raising=False)
    _await(2)
    assert cs._winline_start_pending_next_map_polling() == 1
    assert calls[0]["producer_pid"] == 4242
    assert calls[0]["producer_start_generation"] == "pid-4242"


def test_waiting_does_not_overwrite_the_registry(registry, monkeypatch) -> None:
    """Реестр описывает карту, идущую на самом деле.

    Запись туда ещё не начавшейся карты выглядела бы для опроса текущей как
    смена карты, и он терминалился бы с причиной `map_rollover` вместо
    честного `source_absent`.
    """
    calls: list = []
    monkeypatch.setattr(cs, "ensure_winline_current_map_polling",
                        lambda **kw: calls.append(kw) or object())
    _await(2)
    cs._winline_start_pending_next_map_polling()
    assert calls[0]["update_registry"] is False


# ── следующая карта обязана быть неизбежной ─────────────────────────────────
#
# 23.08.2026 ожидание карты 3 в серии Team Spirit — Team Yandex запустило опрос
# карты, которой не существовало: при счёте 1:0 в Bo3 серия кончается на второй
# карте, если ведущий её выиграет. Исход только что сыгранной карты нам
# неизвестен, поэтому ждать можно лишь то, что состоится при ЛЮБОМ её исходе.

def _reg_score(series_type: int, wins: tuple, map_num: int) -> dict:
    return _reg(map_num=map_num, series_type=series_type, wins=wins)


def test_bo3_second_map_is_certain_but_third_is_not(registry) -> None:
    # После карты 1 при 0:0 вторая карта в Bo3 будет всегда.
    assert cs._winline_note_pending_next_map(
        SERIES, _reg_score(1, (0, 0), 1), now=2_000.0) == 2
    cs._winline_pending_next_maps.clear()
    # После карты 2 при 1:0 серия может кончиться 2:0 — ждать нельзя.
    assert cs._winline_note_pending_next_map(
        SERIES, _reg_score(1, (1, 0), 2), now=2_000.0) is None
    assert SERIES not in cs._winline_pending_next_maps


def test_bo5_waits_while_the_lead_cannot_close_the_series(registry) -> None:
    for wins, map_num, expected in (((0, 0), 1, 2), ((1, 0), 2, 3), ((1, 1), 3, 4)):
        cs._winline_pending_next_maps.clear()
        assert cs._winline_note_pending_next_map(
            SERIES, _reg_score(2, wins, map_num), now=2_000.0) == expected
    # 2:0 в Bo5 — третья карта может стать последней.
    cs._winline_pending_next_maps.clear()
    assert cs._winline_note_pending_next_map(
        SERIES, _reg_score(2, (2, 0), 3), now=2_000.0) is None


def test_bo2_plays_both_maps(registry) -> None:
    assert cs._winline_note_pending_next_map(
        SERIES, _reg_score(3, (0, 0), 1), now=2_000.0) == 2
    cs._winline_pending_next_maps.clear()
    assert cs._winline_note_pending_next_map(
        SERIES, _reg_score(3, (1, 0), 2), now=2_000.0) is None


def test_unreadable_score_or_format_stops_the_wait(registry) -> None:
    """Гадать нельзя: неизвестный формат и нечитаемый счёт — повод молчать."""
    for reg in (_reg_score(0, (0, 0), 1),          # Bo1
                _reg_score(1, (None, 0), 1),       # счёт не прочитан
                _reg(map_num=1, series_type=None, wins=None),   # полей нет
                _reg_score(1, (0, 0), 2)):         # счёт не сходится с номером
        cs._winline_pending_next_maps.clear()
        assert cs._winline_note_pending_next_map(SERIES, reg, now=2_000.0) is None


def test_finish_is_not_announced_for_a_map_nobody_saw(monkeypatch) -> None:
    """«Карта завершена» — закрытие темы, которой в чате не было."""
    monkeypatch.setattr(cs, "_winline_odds_notify_state", {}, raising=False)
    sent: list = []
    out = cs._winline_odds_telegram_notify(
        {"p1_odds": None, "p2_odds": None, "market_status": "missing",
         "team1": "Team Spirit", "team2": "Team Yandex", "map_num": 3},
        "sourcetv:series:9|map3|Team Spirit|Team Yandex",
        is_terminal=True, map_end_proven=True,
        send_fn=lambda text: sent.append(text))
    assert out is None
    assert sent == []
