"""Справка об исходе обязана отвечать про НАШУ карту, а не про соседнюю.

23.08.2026, серия Team Spirit — TEAM VISION. Исход карты 1 (`8960577698`,
конец 10:02) применился к карте 2 (`8960655084`, регистрация 10:36): рейтинг
TEAM VISION упал 2070.5 → 2056.1, хотя карту 2 они выиграли, а Team Spirit
получил те же 14.5 пункта. Проверка по OpenDota: `radiant_win: True`.

Первая попытка чинить — отсев «карта завершилась раньше регистрации» — сломала
обратное: ключ карты в проде меняется по десятку раз за карту (.33, .35, .41,
.44), `registered_at` ползёт вместе с ним и к концу карты оказывается ПОЗЖЕ её
конца. Карта 4 той же серии (`8960882635`, конец 14:33, последняя регистрация
14:50) так и осталась неприменённой к моменту начала карты 5.

Контракт: сопоставление идёт по НОМЕРУ карты. Он есть и у отложенной записи, и
у Stratz, и не зависит ни от churn ключей, ни от задержки публикации. Нет
совпадения по номеру — возвращается None: молчание дешевле догадки.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402

REG = 1_787_470_560          # регистрация карты 2, 10:36
SPIRIT, VISION = 7119388, 9572001


MAP2, MAP1 = 8960655084, 8960577698


def _pending(match_id: int = MAP2, registered_at: int = REG) -> dict:
    return {"map_key": f"dltv.org/matches/{match_id}.0",
            "registered_at": registered_at,
            "match_record": {"radiant_team_id": VISION, "dire_team_id": SPIRIT,
                             "match_id": match_id}}


def _map(match_id: int, start: int, end: int, radiant: int, won: bool) -> dict:
    return {"match_id": match_id, "start": start, "end": end,
            "radiant_team_id": radiant,
            "dire_team_id": SPIRIT if radiant == VISION else VISION,
            "radiant_won": won, "series_id": 1133004}


def _with_history(monkeypatch, maps: list, aliases: dict | None = None) -> None:
    module = type(sys)("stratz_map_result")
    module.series_history = lambda rad, dire, **kw: list(maps)
    table = {int(k): int(v) for k, v in (aliases or {}).items()}
    # `resolve_team_id` есть и в бою: именно он переводит наши id в id-пространство
    # Stratz, и справка отвечает уже переведёнными id.
    module.resolve_team_id = lambda tid, **kw: table.get(int(tid), int(tid))
    monkeypatch.setitem(sys.modules, "stratz_map_result", module)


# Инцидент 03.09.2026: Pipsqueak + 4 у нас 9872667, у Stratz та же команда
# 10150633 (связка лежит в runtime/stratz_team_matches.json), DYNASTY — 10225542.
PIPSQUEAK_OURS, PIPSQUEAK_STRATZ, DYNASTY = 9872667, 10150633, 10225542
PIPSQUEAK_MAP = 8980769413


def _pipsqueak_pending() -> dict:
    return {"map_key": f"dltv.org/matches/{PIPSQUEAK_MAP}.0",
            "registered_at": REG,
            "match_record": {"radiant_team_id": PIPSQUEAK_OURS,
                             "dire_team_id": DYNASTY,
                             "match_id": PIPSQUEAK_MAP}}


def test_stratz_id_translation_is_not_read_as_a_side_swap(monkeypatch) -> None:
    """Перевод id — это НЕ обмен сторонами: исход инвертировать нельзя.

    Карта 1 Pipsqueak + 4 — DYNASTY (`8980769413`): Stratz ответил
    radiant_team_id=10150633 и radiant_won=True, а сравнение с нашим сырым
    9872667 не совпало — и в рейтинг ушло «radiant проиграл»: Pipsqueak + 4
    1861.9 → 1849.0 (−12.9), DYNASTY 1793.8 → 1804.6 (+10.8), хотя Pipsqueak
    выиграл карту 28:8, а Winline закрывал рынок на 1.04 против 8.00.
    """
    _with_history(
        monkeypatch,
        [{"match_id": PIPSQUEAK_MAP, "start": REG, "end": REG + 2400,
          "radiant_team_id": PIPSQUEAK_STRATZ, "dire_team_id": DYNASTY,
          "radiant_won": True, "series_id": 1}],
        aliases={PIPSQUEAK_OURS: PIPSQUEAK_STRATZ},
    )

    assert cs._live_elo_winner_lookup(
        f"dltv.org/matches/{PIPSQUEAK_MAP}.0", _pipsqueak_pending()) is True


def test_translated_id_of_the_other_side_is_still_a_swap(monkeypatch) -> None:
    """Настоящий обмен сторонами перевод id прятать не должен."""
    _with_history(
        monkeypatch,
        [{"match_id": PIPSQUEAK_MAP, "start": REG, "end": REG + 2400,
          "radiant_team_id": DYNASTY, "dire_team_id": PIPSQUEAK_STRATZ,
          "radiant_won": True, "series_id": 1}],
        aliases={PIPSQUEAK_OURS: PIPSQUEAK_STRATZ},
    )

    assert cs._live_elo_winner_lookup(
        f"dltv.org/matches/{PIPSQUEAK_MAP}.0", _pipsqueak_pending()) is False


def test_id_unknown_on_both_sides_is_silent_not_inverted(monkeypatch) -> None:
    """Совсем чужой id — это «не знаем», а не повод перевернуть исход."""
    _with_history(
        monkeypatch,
        [{"match_id": PIPSQUEAK_MAP, "start": REG, "end": REG + 2400,
          "radiant_team_id": 424242, "dire_team_id": 525252,
          "radiant_won": True, "series_id": 1}],
        aliases={PIPSQUEAK_OURS: PIPSQUEAK_STRATZ},
    )

    assert cs._live_elo_winner_lookup(
        f"dltv.org/matches/{PIPSQUEAK_MAP}.0", _pipsqueak_pending()) is None


def test_previous_map_of_the_series_is_rejected(monkeypatch) -> None:
    """В истории только карта 1 — про карту 2 ответить нечего."""
    _with_history(monkeypatch, [
        _map(MAP1, start=REG - 4800, end=REG - 2040, radiant=SPIRIT, won=True),
    ])
    assert cs._live_elo_winner_lookup("dltv.org/matches/8960655084.0", _pending()) is None


def test_our_map_is_accepted_and_oriented_to_the_pending_sides(monkeypatch) -> None:
    """Наша карта опознаётся по номеру, соседняя не мешает."""
    _with_history(monkeypatch, [
        _map(MAP1, start=REG - 4800, end=REG - 2040, radiant=SPIRIT, won=True),
        _map(MAP2, start=REG + 120, end=REG + 3840, radiant=VISION, won=True),
    ])
    assert cs._live_elo_winner_lookup(
        "dltv.org/matches/8960655084.0", _pending()) is True


def test_sides_are_flipped_when_stratz_lists_them_the_other_way(monkeypatch) -> None:
    """Ориентация приводится к сторонам ОТЛОЖЕННОЙ карты, а не к порядку Stratz."""
    _with_history(monkeypatch, [
        _map(MAP2, start=REG + 120, end=REG + 3840, radiant=SPIRIT, won=True),
    ])
    assert cs._live_elo_winner_lookup(
        "dltv.org/matches/8960655084.0", _pending()) is False


def test_late_registration_still_finds_the_map(monkeypatch) -> None:
    """Ключ карты ползёт, и регистрация оказывается ПОЗЖЕ конца карты.

    Ровно на этом сломалась первая попытка чинить: карта 4 серии не применилась
    к началу карты 5. Сопоставление по номеру от времени не зависит.
    """
    _with_history(monkeypatch, [
        _map(MAP2, start=REG - 3600, end=REG - 1020, radiant=VISION, won=True),
    ])
    assert cs._live_elo_winner_lookup(
        "dltv.org/matches/8960655084.44", _pending(registered_at=REG)) is True


def test_empty_history_and_missing_teams_are_silent(monkeypatch) -> None:
    _with_history(monkeypatch, [])
    assert cs._live_elo_winner_lookup("dltv.org/matches/8960655084.0", _pending()) is None
    assert cs._live_elo_winner_lookup("x", {"match_record": {}}) is None
    # Без номера карты сопоставлять нечем.
    _with_history(monkeypatch, [_map(MAP2, REG, REG + 60, VISION, True)])
    assert cs._live_elo_winner_lookup("x", {
        "match_record": {"radiant_team_id": VISION, "dire_team_id": SPIRIT}}) is None
    assert cs._live_elo_winner_lookup("x", None) is None
