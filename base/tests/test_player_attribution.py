"""Подтверждение анонимных карт по игрокам (E-269).

Контракт: SourceTV часто отдаёт карты анонимно — без team_id и с
плейсхолдером вместо имени. Когда 4/5 игроков стороны тегированы одной
известной организацией (OpenDota proPlayers), probe пишет в мост только
EVIDENCE (`player_hint`, без переименований — решение пира, E-267: теги
устаревают). Подтверждает пару cyberscore ЖИВОЙ карточкой Winline: если
карточка содержит оба имени (имя моста или team_key хинта), пара идёт
обычным путём с именами/id из подтверждения. Без карточки — прежний путь.

Вход — синтетические GC-записи задокументированной формы GetLiveLeagueGames
(поле players с team/account_id) и захваченные строки ленты Winline
(см. test_winline_first_admission.py). Паттерн фикстур — из E-267.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import sourcetv_probe as probe  # noqa: E402
import cyberscore_try as runtime  # noqa: E402

DOTA_LIVE_CARD = (
    "DOTA 2 | EPL Season MOUZ KLIM SANI4 2карта 16' +68 1 0 16 5 2К "
    "Матч 1.53 2.40 2 карта 1.52 2.42"
)
DOTA_PREMATCH_CARD = (
    "DOTA 2 | EPL Season MOUZ KLIM SANI4 Завтра 15:00 +9 "
    "Матч 2.46 1.48 1.50 + 1.5 - 2.43 1.75 2.5 1.96 1 карта 2.23 1.58"
)


def _players(side: int, aids) -> list:
    return [{"team": side, "account_id": aid} for aid in aids]


def _lookup(pairs) -> dict:
    return {aid: {"team": tag} for aid, tag in pairs}


def _anon_game(radiant_tags, dire_tags):
    aids = list(range(1, 11))
    tags = dict(zip(aids, radiant_tags + dire_tags))
    return {
        "radiant_team": None,
        "dire_team": None,
        "players": _players(0, aids[:5]) + _players(1, aids[5:]),
    }, _lookup(tags.items())


def test_hint_marks_anonymous_sides_without_renaming(monkeypatch) -> None:
    """Хинт — evidence only: стороны опознаны, но игра не переименована."""
    monkeypatch.setattr(
        probe,
        "_player_attribution_tier_index",
        lambda: (set(), {"alpha": {11}, "beta": {22}}),
    )
    game, lookup = _anon_game(
        ["Alpha"] * 4 + ["Nope"], ["Beta"] * 5
    )
    out = probe._player_hint_for_game(game, lookup)
    assert out["radiant"]["team_key"] == "alpha"
    assert out["radiant"]["players"] == 4
    assert out["radiant"]["team_ids"] == [11]
    assert out["dire"]["team_key"] == "beta"
    assert out["dire"]["players"] == 5


def test_hint_skips_named_and_id_sides(monkeypatch) -> None:
    """Хинт только анонимным: именованным и id-шным сторонам не пишется."""
    monkeypatch.setattr(
        probe, "_player_attribution_tier_index", lambda: (set(), {"alpha": {11}})
    )
    lookup = _lookup([(aid, "Alpha") for aid in range(1, 11)])
    game = {
        "radiant_team": {"team_name": "Klim Sani4", "team_id": 0},
        "dire_team": {"team_name": "Someone", "team_id": 999},
        "players": _players(0, [1, 2, 3, 4, 5]) + _players(1, [6, 7, 8, 9, 10]),
    }
    assert probe._player_hint_for_game(game, lookup) == {
        "radiant": None,
        "dire": None,
    }


def test_hint_collision_disables_both(monkeypatch) -> None:
    monkeypatch.setattr(
        probe, "_player_attribution_tier_index", lambda: (set(), {"alpha": {11}})
    )
    lookup = _lookup([(aid, "Alpha") for aid in range(1, 11)])
    game = {
        "radiant_team": None,
        "dire_team": None,
        "players": _players(0, [1, 2, 3, 4, 5]) + _players(1, [6, 7, 8, 9, 10]),
    }
    assert probe._player_hint_for_game(game, lookup) == {
        "radiant": None,
        "dire": None,
    }


def test_threshold_three_of_five_is_not_enough(monkeypatch) -> None:
    monkeypatch.setattr(
        probe, "_player_attribution_tier_index", lambda: (set(), {"alpha": {11}})
    )
    lookup = _lookup([(1, "Alpha"), (2, "Alpha"), (3, "Alpha"), (4, "No"), (5, "No")])
    game = {
        "radiant_team": None,
        "dire_team": None,
        "players": _players(0, [1, 2, 3, 4, 5]),
    }
    assert probe._player_hint_for_game(game, lookup) == {
        "radiant": None,
        "dire": None,
    }


def test_build_target_keeps_names_but_carries_hint(monkeypatch) -> None:
    """Мост НЕ переименовывается (тест пира), хинт едет отдельным полем."""
    monkeypatch.setattr(
        probe, "_player_attribution_tier_index", lambda: (set(), {"alpha": {11}})
    )
    lookup = _lookup([(1, "Alpha"), (2, "Alpha"), (3, "Alpha"), (4, "Alpha"), (5, "Zz")])
    game = {
        "lobby_id": 0,
        "league_id": 19944,
        "radiant_team": None,
        "dire_team": {"team_name": "Someone", "team_id": 999},
        "radiant_series_wins": 0,
        "dire_series_wins": 0,
        "game_number": 1,
        "series_type": 1,
        "series_id": 111,
        "players": _players(0, [1, 2, 3, 4, 5]),
    }
    target = probe._build_target(game, [19944], lookup)
    assert target["rad"] == "Radiant"
    assert target["rad_id"] == 0
    assert target["dire"] == "Someone"
    assert target["dire_id"] == 999
    hint = target["player_hint"]["radiant"]
    assert hint["team_key"] == "alpha"
    assert hint["players"] == 4
    assert hint["account_ids"] == [1, 2, 3, 4]


def _enable_winline_first(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "WINLINE_FIRST_ENABLED", True, raising=False)
    monkeypatch.setattr(runtime, "PURE_DLTV_MODE", False, raising=False)
    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_GATE_MODE", "odds", raising=False)
    monkeypatch.setattr(runtime, "BOOKMAKER_CAMOUFOX_ENABLED", True, raising=False)
    monkeypatch.setattr(runtime, "BOOKMAKER_CAMOUFOX_IMPORTED", True, raising=False)
    runtime._winline_first_parser_fns_cache = None  # noqa: SLF001
    runtime._winline_overview_inject_for_tests("")  # noqa: SLF001


def test_confirm_needs_live_card_for_hinted_pair(monkeypatch) -> None:
    """Живая карточка с обеими командами подтверждает анонимную пару."""
    _enable_winline_first(monkeypatch)
    runtime._winline_overview_inject_for_tests(DOTA_LIVE_CARD)  # noqa: SLF001
    hint = {
        "radiant": {
            "team_key": "mouz",
            "players": 4,
            "team_ids": [1],
            "account_ids": [1, 2, 3, 4],
        },
        "dire": {
            "team_key": "klim sani4",
            "players": 5,
            "team_ids": [2],
            "account_ids": [6, 7, 8, 9, 10],
        },
    }
    hit = runtime._winline_player_confirm("Radiant", "Dire", hint)  # noqa: SLF001
    assert hit is not None
    assert hit["confirmed_names"] == ("mouz", "klim sani4")
    assert hit["confirmed_ids"] == {"radiant": 1, "dire": 2}
    assert hit["league"] == "EPL Season"
    assert runtime._winline_first_bypass_active(hit) is True  # noqa: SLF001


def test_confirm_rejects_prematch_card(monkeypatch) -> None:
    """Карточка линии («Завтра») пару не подтверждает."""
    _enable_winline_first(monkeypatch)
    runtime._winline_overview_inject_for_tests(DOTA_PREMATCH_CARD)  # noqa: SLF001
    hint = {
        "radiant": {"team_key": "mouz", "players": 4, "team_ids": [1], "account_ids": []},
        "dire": {"team_key": "klim sani4", "players": 5, "team_ids": [2], "account_ids": []},
    }
    assert runtime._winline_player_confirm("Radiant", "Dire", hint) is None  # noqa: SLF001


def test_confirm_rejects_placeholder_team_key(monkeypatch) -> None:
    """Тег уровня radiant/dire в join не идёт: такое слово есть везде."""
    _enable_winline_first(monkeypatch)
    runtime._winline_overview_inject_for_tests(DOTA_LIVE_CARD)  # noqa: SLF001
    hint = {
        "radiant": {"team_key": "radiant", "players": 5, "team_ids": [1], "account_ids": []},
        "dire": {"team_key": "klim sani4", "players": 5, "team_ids": [2], "account_ids": []},
    }
    assert runtime._winline_player_confirm("Radiant", "Dire", hint) is None  # noqa: SLF001
