"""Победитель карты: мост → Steam WebAPI → OpenDota.

03.09.2026 api.opendota.com лёг (522 от Cloudflare и HTTP=000 на 12-15 с с
прод-машины и со второй независимой сети при том, что winline.ru отвечал 200 за
0.35 с, example.com за 0.08 с, www.opendota.com за 0.6 с). OpenDota был
единственным источником имени победителя, поэтому карта 1 PuckChamp — Team
Spirit Academy ушла в чат как «🏁 карта завершена · 🕐 30:47» без строки
победителя, а догоняющее «🏆» не пришло вовсе. В тот же момент Steam WebAPI
`GetLiveLeagueGames` по лиге 19944 отдавал `wins 0:1` по этой серии — то есть
имя победителя знал, и не один: те же числа probe пишет в снапшот моста.

Контракт:
- счёт серии держится ПО КОМАНДАМ, а не по сторонам (между картами команды
  меняются сторонами);
- победитель по счёту называется только когда доказательство полное: счёт
  сходится с номером карты, ровно одна команда прибавила одну победу, вторая не
  двинулась;
- базовый счёт — только согласованный с номером карты (`sum == N - 1`) и только
  до начала новой серии той же пары;
- источники идут от бесплатного к платному: снапшот моста (ноль сети), Steam
  (один лёгкий HTTP), OpenDota (один HTTP);
- в тестах сеть не трогается: Steam либо подменён, либо выключен env'ом
  (autouse-фикстура в conftest).
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402

SERIES = "sourcetv:league:19944|id:10164236|id:9948367"
KEY_MAP1 = f"{SERIES}|map1|PuckChamp|Team Spirit Academy"
# Между картами серии команды меняются сторонами: та же серия, обратный порядок.
KEY_MAP2 = f"{SERIES}|map2|Team Spirit Academy|PuckChamp"
PUCK = "puckchamp"
ACAD = "team spirit academy"

# Настоящий ответ Steam WebAPI, снят 03.09.2026 16:07 MSK во время аварии
# OpenDota:   GetLiveLeagueGames/v1/?league_id=19944
STEAM_LIVE_GAMES = {
    "result": {
        "games": [
            {
                "match_id": 8980569936,
                "league_id": 19944,
                "league_node_id": 0,
                "lobby_id": 1234,
                "spectators": 77,
                "stream_delay_s": 120,
                "series_type": 1,
                "scoreboard": True,
                "radiant_series_wins": 0,
                "dire_series_wins": 1,
                "radiant_team": {"team_name": "PuckChamp", "team_id": 10164236,
                                 "team_logo": 1, "complete": False},
                "dire_team": {"team_name": "Team Spirit Academy",
                              "team_id": 9948367, "team_logo": 2,
                              "complete": False},
                "players": [],
            }
        ]
    }
}


class Sender:
    def __init__(self):
        self.messages: List[str] = []

    def __call__(self, message, **kwargs):
        self.messages.append(message)
        return True


class Clock:
    def __init__(self, start: float = 1000.0):
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += float(seconds)


def _boom(*_args: Any, **_kwargs: Any) -> Any:
    raise AssertionError("источник не должен был быть спрошен")


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    monkeypatch.setattr(cs, "_winline_series_wins_seen", {}, raising=False)
    monkeypatch.setattr(cs, "_winline_series_wins_baseline", {}, raising=False)
    monkeypatch.setattr(cs, "_winline_odds_notify_state", {}, raising=False)
    monkeypatch.setattr(cs, "_winline_pending_map_winners", {}, raising=False)
    monkeypatch.setattr(cs, "_winline_series_winner_matches", {}, raising=False)
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_ENABLED_ENV, "1")
    monkeypatch.delenv(cs.WINLINE_MAP_WINNER_ENABLED_ENV, raising=False)
    yield


# ── счёт серии по командам ────────────────────────────────────────────────────

def test_wins_are_keyed_by_team_not_by_side() -> None:
    assert cs._winline_row_wins_by_team(
        "PuckChamp", "Team Spirit Academy", 0, 1) == {PUCK: 0, ACAD: 1}


@pytest.mark.parametrize("radiant,dire,rwins,dwins", [
    ("Radiant", "Team Spirit Academy", 0, 1),      # плейсхолдер не приписать
    ("PuckChamp", "", 0, 1),                        # имени нет
    ("PuckChamp", "Team Spirit Academy", None, 1),  # счёт не прочитан
    ("PuckChamp", "Team Spirit Academy", -1, 1),    # счёт бессмысленный
    ("PuckChamp", "PuckChamp", 1, 0),               # одна команда на обе стороны
])
def test_unreadable_wins_are_not_guessed(radiant, dire, rwins, dwins) -> None:
    assert cs._winline_row_wins_by_team(radiant, dire, rwins, dwins) == {}


def test_baseline_is_only_a_score_consistent_with_the_map() -> None:
    """Строка перерыва несёт номер СДВИНУТЫМ, а счёт ещё не обновлённым."""
    cs._winline_note_series_wins(SERIES, 1, {PUCK: 0, ACAD: 0}, now=100.0)
    assert cs._winline_series_wins_baseline[SERIES][1] == {
        "wins": {PUCK: 0, ACAD: 0}, "at": 100.0}

    # Перерыв после карты 1: номер уже 2, счёт ещё 0:0 — базовым не становится.
    cs._winline_note_series_wins(SERIES, 2, {PUCK: 0, ACAD: 0}, now=200.0)
    assert 2 not in cs._winline_series_wins_baseline[SERIES]
    # Последнее наблюдение при этом обновляется.
    assert cs._winline_series_wins_seen[SERIES]["map_num"] == 2

    # Карта 2 началась при счёте 1:0 — вот теперь базовый счёт согласован.
    cs._winline_note_series_wins(SERIES, 2, {ACAD: 1, PUCK: 0}, now=300.0)
    assert cs._winline_series_wins_baseline[SERIES][2]["wins"] == {ACAD: 1, PUCK: 0}


def test_new_series_of_the_same_pair_resets_baselines() -> None:
    """Ключ серии — лига и пара команд: та же пара играет под ним снова."""
    cs._winline_note_series_wins(SERIES, 1, {PUCK: 0, ACAD: 0}, now=100.0)
    cs._winline_note_series_wins(SERIES, 2, {ACAD: 1, PUCK: 0}, now=200.0)
    assert set(cs._winline_series_wins_baseline[SERIES]) == {1, 2}

    # Новая серия началась: карта 1 при счёте 0:0.
    cs._winline_note_series_wins(SERIES, 1, {PUCK: 0, ACAD: 0}, now=90000.0)
    assert set(cs._winline_series_wins_baseline[SERIES]) == {1}


# ── вывод победителя по приращению счёта ─────────────────────────────────────

def test_delta_names_the_team_that_grew() -> None:
    assert cs._winline_winner_from_wins_delta(
        KEY_MAP1, {PUCK: 0, ACAD: 0}, {PUCK: 0, ACAD: 1}) == "Team Spirit Academy"


def test_delta_survives_the_side_swap() -> None:
    """Счёт следующих карт принадлежит ДРУГОЙ radiant: держимся имён."""
    assert cs._winline_winner_from_wins_delta(
        KEY_MAP2, {ACAD: 1, PUCK: 0}, {ACAD: 1, PUCK: 1}) == "PuckChamp"


def test_first_map_is_readable_without_a_baseline() -> None:
    assert cs._winline_winner_from_wins_delta(
        KEY_MAP1, {}, {PUCK: 1, ACAD: 0}) == "PuckChamp"
    # Для карты 2 одного счёта мало: 1:1 не говорит, кто взял вторую.
    assert cs._winline_winner_from_wins_delta(KEY_MAP2, {}, {PUCK: 1, ACAD: 1}) is None


@pytest.mark.parametrize("before,now", [
    ({PUCK: 0, ACAD: 0}, {PUCK: 1, ACAD: 1}),   # прибавили обе — не наша карта
    ({PUCK: 0, ACAD: 0}, {PUCK: 0, ACAD: 0}),   # счёт ещё не обновился
    ({PUCK: 0, ACAD: 0}, {PUCK: 0, ACAD: 2}),   # скачок через карту
    ({PUCK: 1}, {PUCK: 1, ACAD: 1}),            # в базовом счёте нет команды
    ({PUCK: 0, ACAD: 0}, {PUCK: 0}),            # в новом счёте нет команды
    ({PUCK: 0, ACAD: 0}, {"other": 1}),         # строка не про нашу серию
])
def test_delta_refuses_everything_unproven(before: Dict[str, int],
                                           now: Dict[str, int]) -> None:
    assert cs._winline_winner_from_wins_delta(KEY_MAP1, before, now) is None


def test_delta_refuses_a_stale_baseline() -> None:
    """Базовый счёт прошлой серии той же пары победителя не называет.

    Проверяется на карте 2: для карты 1 счёт 1:0 однозначен и без базового,
    а вот кто взял вторую карту при 1:1 — без счёта ДО неё не узнать.
    """
    old = cs.time.time() - cs._WINLINE_SERIES_WINS_BASELINE_TTL_S - 60
    cs._winline_series_wins_baseline[SERIES] = {
        2: {"wins": {PUCK: 1, ACAD: 0}, "at": old}}
    cs._winline_series_wins_seen[SERIES] = {
        "map_num": 2, "wins": {PUCK: 1, ACAD: 1}, "at": cs.time.time()}

    # Счёт по протухшему базовому не выводится — цепочка уходит к OpenDota,
    # а тот молчит: имени нет, и это честнее, чем назвать наугад.
    asked: List[Any] = []

    def _opendota(match_id):
        asked.append(match_id)
        return None

    assert cs._winline_resolve_map_winner(
        KEY_MAP2, 8980463789, fetch_fn=_opendota, steam_fn=lambda _l: []) is None
    assert asked == [8980463789]


# ── Steam WebAPI ─────────────────────────────────────────────────────────────

def test_league_is_taken_from_the_series_key() -> None:
    assert cs._winline_steam_league_from_key(KEY_MAP1) == 19944
    assert cs._winline_steam_league_from_key(
        "sourcetv:series:7|map1|PuckChamp|Team Spirit Academy") is None
    assert cs._winline_steam_league_from_key(
        "dltv.org/matches/8980463789|map1|A|B") is None


def test_steam_response_is_parsed_into_wins_by_team(monkeypatch) -> None:
    monkeypatch.setenv(cs.WINLINE_MAP_WINNER_STEAM_ENV, "1")
    calls: List[Dict[str, Any]] = []

    class _Resp:
        status_code = 200

        @staticmethod
        def json():
            return STEAM_LIVE_GAMES

    def _get(url, params=None, timeout=None):
        calls.append({"url": url, "params": params, "timeout": timeout})
        return _Resp()

    monkeypatch.setattr(cs, "requests", SimpleNamespace(get=_get))

    assert cs._winline_fetch_steam_live_series_wins(19944) == [{PUCK: 0, ACAD: 1}]
    assert calls[0]["params"]["league_id"] == 19944
    assert calls[0]["timeout"] <= 5
    assert "GetLiveLeagueGames" in calls[0]["url"]
    # Ключ боевой: в журнале и в логе его быть не должно.
    assert calls[0]["params"]["key"]


def test_steam_is_not_called_when_switched_off(monkeypatch) -> None:
    monkeypatch.setenv(cs.WINLINE_MAP_WINNER_STEAM_ENV, "0")
    monkeypatch.setattr(cs, "requests", SimpleNamespace(get=_boom))

    assert cs._winline_fetch_steam_live_series_wins(19944) == []


def test_steam_survives_a_dead_endpoint(monkeypatch) -> None:
    monkeypatch.setenv(cs.WINLINE_MAP_WINNER_STEAM_ENV, "1")

    class _Dead:
        status_code = 500

        @staticmethod
        def json():
            raise ValueError("нет тела")

    monkeypatch.setattr(cs, "requests",
                        SimpleNamespace(get=lambda *a, **k: _Dead()))
    assert cs._winline_fetch_steam_live_series_wins(19944) == []

    def _raise(*_a, **_k):
        raise OSError("connection reset")

    monkeypatch.setattr(cs, "requests", SimpleNamespace(get=_raise))
    assert cs._winline_fetch_steam_live_series_wins(19944) == []


# ── порядок источников ───────────────────────────────────────────────────────

def test_bridge_score_names_the_winner_without_any_network() -> None:
    """Ноль сети: счёт уже лежит в снапшоте моста."""
    cs._winline_note_series_wins(SERIES, 1, {PUCK: 0, ACAD: 0}, now=100.0)
    cs._winline_note_series_wins(SERIES, 2, {PUCK: 0, ACAD: 1}, now=200.0)

    assert cs._winline_resolve_map_winner(
        KEY_MAP1, None, fetch_fn=_boom, steam_fn=_boom) == "Team Spirit Academy"


def test_steam_answers_when_the_bridge_is_silent() -> None:
    """Серия ушла из снапшота, но в живых списках Steam она ещё есть."""
    winner = cs._winline_resolve_map_winner(
        KEY_MAP1, 8980463789,
        fetch_fn=_boom,
        steam_fn=lambda league: [{PUCK: 0, ACAD: 1}] if league == 19944 else [])

    assert winner == "Team Spirit Academy"


def test_opendota_stays_the_last_resort() -> None:
    winner = cs._winline_resolve_map_winner(
        KEY_MAP1, 8980463789,
        fetch_fn=lambda _mid: {"radiant_win": True, "name": "PuckChamp"},
        steam_fn=lambda _league: [])

    assert winner == "PuckChamp"


def test_terminal_card_names_the_winner_while_opendota_is_down() -> None:
    """Ровно инцидент 03.09.2026: OpenDota молчит, имя есть в счёте серии."""
    cs._winline_note_series_wins(SERIES, 1, {PUCK: 0, ACAD: 0}, now=100.0)
    cs._winline_note_series_wins(SERIES, 2, {PUCK: 0, ACAD: 1}, now=200.0)
    sender, clock = Sender(), Clock()
    cs._winline_odds_telegram_notify(
        {"p1_odds": 6.00, "p2_odds": 1.09, "market_status": "open",
         "accepted": True},
        KEY_MAP1, send_fn=sender, monotonic_fn=clock, stamp_fn=lambda: "30:47")

    clock.advance(600.0)
    message = cs._winline_odds_telegram_notify(
        {"p1_odds": None, "p2_odds": None, "market_status": "missing",
         "finished_at": 1.0},
        KEY_MAP1, is_terminal=True, map_end_proven=True, map_confirmed_live=True,
        match_id=8980463789, send_fn=sender, monotonic_fn=clock,
        stamp_fn=lambda: "30:47", winner_fn=lambda _mid: None,
        steam_fn=lambda _league: [])

    assert message is not None
    assert "🏁 Winline · карта 1" in message
    assert "🏆 победа: Team Spirit Academy" in message
    # OpenDota не ответил — догонять нечего.
    assert cs._winline_pending_map_winners == {}


def test_pending_winner_without_match_id_still_gets_a_name() -> None:
    """Опрос, заведённый в перерыве, match_id не имеет — имя всё равно придёт."""
    cs._winline_note_pending_map_winner(KEY_MAP1, 1000.0, None)
    assert KEY_MAP1 in cs._winline_pending_map_winners

    sender, clock = Sender(), Clock(1000.0)
    clock.advance(61.0)
    cs._winline_note_series_wins(SERIES, 1, {PUCK: 0, ACAD: 0}, now=100.0)
    cs._winline_note_series_wins(SERIES, 2, {PUCK: 0, ACAD: 1}, now=200.0)

    sent = cs._winline_flush_pending_map_winners(
        monotonic_fn=clock, stamp_fn=lambda: "30:47", send_fn=sender,
        winner_fn=lambda _mid: None, steam_fn=lambda _league: [])

    assert len(sent) == 1
    assert "🏆 Winline · карта 1" in sent[0]
    assert "🏆 победа: Team Spirit Academy" in sent[0]
    assert cs._winline_pending_map_winners == {}
