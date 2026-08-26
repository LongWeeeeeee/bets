"""Промоция рынка «Матч» на решающей карте — проводка до живого пути.

Парсер умел подставлять рынок «Матч» вместо отсутствующего рынка карты
(`_winline_promote_last_map_match_market`), но флаг `series_last_map` в прод-путь
никто не передавал, поэтому промоция не срабатывала ни разу: 05.08.2026 у
`No Hoodwink — Lynx` (Bo3, счёт 1:1, карта 3) в карточке был только `Матч 2.55 1.42`,
а поллер отдавал `market missing`.

Контракт:
- «последняя карта» считается по срезу моста SourceTV (кэш по mtime) либо по уже
  прочитанному payload матча, который главнее файла;
- Bo3 1:1 → карта 3, Bo5 2:2 → карта 5, Bo2 → карта 2; неизвестный формат и
  матч не из среза — False;
- флаг доезжает от регистрации поллера до парсера через реестр серий, а в
  evidence видно и флаг, и факт промоции.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402


def _row(**overrides: Any) -> Dict[str, Any]:
    # Снимок моста 05.08.2026: Team Lynx vs No Hoodwink, карта 3 при счёте 1:1.
    row = {
        "radiant_team_name": "Team Lynx",
        "dire_team_name": "No Hoodwink",
        "series_type": 1,  # Bo3
        "series_game_number": 3,
        "radiant_series_wins": 1,
        "dire_series_wins": 1,
    }
    row.update(overrides)
    return row


@pytest.fixture()
def bridge(monkeypatch, tmp_path):
    """Срез моста в виде словаря match_id -> row (реальный формат файла)."""

    def _write(rows) -> Path:
        path = tmp_path / "sourcetv_matches.json"
        payload = rows if isinstance(rows, list) else rows
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        monkeypatch.setattr(cs, "SOURCETV_MATCHES_PATH", str(path))
        monkeypatch.setattr(cs, "_winline_series_rows_cache", {"mtime": None, "rows": []})
        return path

    return _write


def test_bo3_decider_from_bridge(bridge) -> None:
    bridge({"8930690456": _row()})
    assert cs._winline_series_last_map(3, "Team Lynx", "No Hoodwink") is True
    assert cs._winline_series_last_map(3, "No Hoodwink", "Team Lynx") is True
    assert cs._winline_series_last_map(2, "Team Lynx", "No Hoodwink") is False


def test_bridge_accepts_list_form(bridge) -> None:
    bridge([_row()])
    assert cs._winline_series_last_map(3, "Team Lynx", "No Hoodwink") is True


def test_bo5_and_bo2(bridge) -> None:
    bridge(
        {
            "1": _row(series_type=2, radiant_series_wins=2, dire_series_wins=2),
            "2": _row(
                radiant_team_name="OG",
                dire_team_name="Vici Gaming",
                series_type=3,  # Bo2 — играются ровно две карты
                radiant_series_wins=0,
                dire_series_wins=1,
                series_game_number=2,
            ),
        }
    )
    assert cs._winline_series_last_map(5, "Team Lynx", "No Hoodwink") is True
    assert cs._winline_series_last_map(4, "Team Lynx", "No Hoodwink") is False
    assert cs._winline_series_last_map(2, "OG", "Vici Gaming") is True
    assert cs._winline_series_last_map(1, "OG", "Vici Gaming") is False


@pytest.mark.parametrize(
    "row",
    [
        _row(radiant_series_wins=1, dire_series_wins=0),  # Bo3 1:0 — будет ещё карта
        _row(radiant_series_wins=0, dire_series_wins=0),
        _row(series_type=0),  # Bo1: формат серии не доказан
        _row(series_type=None),
        _row(radiant_series_wins=None),
    ],
)
def test_non_decider_is_not_last_map(bridge, row: Dict[str, Any]) -> None:
    bridge({"1": row})
    assert cs._winline_series_last_map(3, "Team Lynx", "No Hoodwink") is False


def test_unknown_match_is_not_last_map(bridge) -> None:
    bridge({"1": _row()})
    assert cs._winline_series_last_map(3, "Team A", "Team B") is False
    assert cs._winline_series_last_map(3, "Team Lynx", "Team B") is False
    assert cs._winline_series_last_map(3, "", "No Hoodwink") is False


def test_payload_wins_over_bridge(bridge) -> None:
    # В файле счёт устарел (1:0), у вызывающего свежий payload (1:1).
    bridge({"1": _row(radiant_series_wins=1, dire_series_wins=0)})
    assert cs._winline_series_last_map(3, "Team Lynx", "No Hoodwink") is False
    assert cs._winline_series_last_map(3, "Team Lynx", "No Hoodwink", _row()) is True
    # Пустые payload'ы падают обратно на файл.
    assert cs._winline_series_last_map(3, "Team Lynx", "No Hoodwink", None, {}) is False


def test_rows_cache_survives_repeated_calls(bridge) -> None:
    path = bridge({"1": _row()})
    assert cs._winline_series_last_map(3, "Team Lynx", "No Hoodwink") is True
    # Файл исчез, но кэш по mtime уже прогрет — ответ не должен «мигать».
    path.unlink()
    assert cs._winline_series_rows_cache["rows"], "срез обязан кэшироваться"


def test_registry_flag_roundtrip() -> None:
    series = "sourcetv:league:20026|id:10001885|id:9928636"
    with cs._winline_current_map_state_lock:
        cs._winline_current_map_registry.pop(series, None)
    assert cs._winline_registry_series_last_map(series) is False

    with cs._winline_current_map_state_lock:
        cs._winline_current_map_registry[series] = {
            "map_num": 3,
            "team1": "Team Lynx",
            "team2": "No Hoodwink",
            "active": True,
            "series_last_map": True,
        }
    try:
        assert cs._winline_registry_series_last_map(series) is True
        assert cs._winline_registry_series_last_map("") is False
        assert cs._winline_registry_series_last_map("unknown:series") is False
    finally:
        with cs._winline_current_map_state_lock:
            cs._winline_current_map_registry.pop(series, None)


def test_fast_collect_passes_series_last_map(monkeypatch) -> None:
    """Быстрый путь обязан включать промоцию на решающей карте."""
    series = "sourcetv:league:20026|id:10001885|id:9928636"
    with cs._winline_current_map_state_lock:
        cs._winline_current_map_registry[series] = {
            "map_num": 3,
            "team1": "Team Lynx",
            "team2": "No Hoodwink",
            "active": True,
            "series_last_map": True,
        }
    seen: Dict[str, Any] = {}

    class _Extract:
        odds = [2.55, 1.42]
        map_num = 3
        market_closed = False
        market_kind = "current_map_winner"
        promoted_from_match = True
        p1_team = "team1"
        p2_team = "team2"
        # Карточка пишет пару в обратном порядке — провенанс обязан это сохранить.
        card_team_order = "No Hoodwink|Team Lynx"
        card_odds = [1.42, 2.55]
        details = "winline last map of series: map market not offered, match winner promoted"

    def _fake_extract(card, team1, team2, map_num, **kwargs):  # noqa: ANN001
        seen["series_last_map"] = kwargs.get("series_last_map")
        seen["html_passed"] = bool(kwargs.get("html"))
        return _Extract()

    html = (
        "<html><body>" + ("<div>NO HOODWINK LYNX 3карта Матч 2.55 1.42</div>" * 40)
        + "Киберспорт Линия Live сейчас</body></html>"
    )
    monkeypatch.setitem(
        cs.__dict__,
        "_bookmaker_winline_card_context",
        lambda *_a, **_k: "NO HOODWINK LYNX 3карта Матч 2.55 1.42",
    )
    monkeypatch.setitem(cs.__dict__, "_bookmaker_winline_extract", _fake_extract)
    monkeypatch.setitem(cs.__dict__, "_bookmaker_winline_bettable", lambda *_a, **_k: None)
    try:
        out = cs._winline_fast_collect_from_payload(
            {"html": html, "url": "https://winline.ru/stavki/sport/kibersport/dota_2"},
            series=series,
            map_num=3,
            team1="Team Lynx",
            team2="No Hoodwink",
            expected_url="https://winline.ru/stavki/sport/kibersport/dota_2",
        )
    finally:
        with cs._winline_current_map_state_lock:
            cs._winline_current_map_registry.pop(series, None)

    assert seen["series_last_map"] is True, "флаг решающей карты не доехал до парсера"
    assert seen["html_passed"] is True, "промоция работает только по DOM"
    assert out is not None
    assert out["market_status"] == "open"
    assert (out["p1_odds"], out["p2_odds"]) == (2.55, 1.42)
    assert out["odds_promoted_from_match"] is True
    # Провенанс стороны доезжает до поллера в сыром виде карточки.
    assert out["card_team_order"] == "No Hoodwink|Team Lynx"
    assert out["card_odds"] == [1.42, 2.55]


def test_fast_collect_carries_the_promotion_refusal_to_evidence(monkeypatch) -> None:
    """Причина отказа подстановки обязана доехать до evidence поллера.

    До 22.08.2026 отказ выглядел в файлах ровно как отсутствие рынка: девять
    карт-троек остались без цены, и понять, какой предохранитель сработал, было
    нечем.
    """
    series = "sourcetv:league:19719|id:9572001|id:9823272"
    with cs._winline_current_map_state_lock:
        cs._winline_current_map_registry[series] = {
            "map_num": 3,
            "team1": "TEAM VISION",
            "team2": "Team Yandex",
            "active": True,
            "series_last_map": True,
        }

    class _Refused:
        odds: list = []
        map_num = 3
        market_closed = False
        market_kind = "current_map_winner"
        promoted_from_match = False
        p1_team = None
        p2_team = None
        card_team_order = None
        card_odds = None
        details = "winline structured current map winner market unavailable"
        miss_fingerprint = "promotion=card_header_silent"

    # Короткое замыкание быстрого пути разрешено только по ПОЛНОЙ странице:
    # ниже порога `WINLINE_FAST_MIN_PAGE_HTML` он честно отказывается судить.
    filler = "<div>TEAM VISION Team Yandex Матч 1.80 1.95</div>"
    html = (
        "<html><body>"
        + filler * (cs.WINLINE_FAST_MIN_PAGE_HTML // len(filler) + 20)
        + "Киберспорт winline Линия Live сейчас</body></html>"
    )
    monkeypatch.setitem(
        cs.__dict__, "_bookmaker_winline_card_context",
        lambda *_a, **_k: "TEAM VISION Team Yandex Матч 1.80 1.95")
    monkeypatch.setitem(cs.__dict__, "_bookmaker_winline_extract",
                        lambda *_a, **_k: _Refused())
    monkeypatch.setitem(cs.__dict__, "_bookmaker_winline_bettable", lambda *_a, **_k: None)
    try:
        out = cs._winline_fast_collect_from_payload(
            {"html": html, "url": "https://winline.ru/stavki/sport/kibersport/dota_2"},
            series=series,
            map_num=3,
            team1="TEAM VISION",
            team2="Team Yandex",
            expected_url="https://winline.ru/stavki/sport/kibersport/dota_2",
        )
    finally:
        with cs._winline_current_map_state_lock:
            cs._winline_current_map_registry.pop(series, None)

    assert out is not None
    assert out["market_status"] == "missing"
    assert out["miss_fingerprint"] == "promotion=card_header_silent"


def test_proven_bo1_map_is_the_match_itself(bridge) -> None:
    """Bo1: победитель карты и победитель матча — одно событие.

    26.08.2026, квал BLAST Slam: у пары, которую Winline вообще выставил, промах
    шёл с отпечатком `promotion=not_decider` — таблица `_WINLINE_SERIES_WINS_NEEDED`
    знает только Bo3 и Bo5, и карта Bo1 решающей не считалась никогда. Рынок
    карты Winline на этих матчах не выставляет, только «Матч», и подстановка не
    срабатывала.
    """
    proven = _row(series_type=0, cyberscore_best_of=1)
    assert cs._winline_series_last_map(1, "Team Lynx", "No Hoodwink", proven) is True
    # Вторая карта в Bo1 невозможна — доказательство не распространяется на неё.
    assert cs._winline_series_last_map(2, "Team Lynx", "No Hoodwink", proven) is False


def test_unproven_zero_series_type_stays_non_decider(bridge) -> None:
    """У GC `series_type` = 0 означает и Bo1, и «поля нет» — гадать нельзя."""
    bridge({"1": _row(series_type=0)})
    assert cs._winline_series_last_map(1, "Team Lynx", "No Hoodwink") is False
    assert cs._winline_series_last_map(3, "Team Lynx", "No Hoodwink") is False


def test_proof_of_a_longer_format_does_not_promote_first_map(bridge) -> None:
    """Bo3 остаётся Bo3: первая карта решающей не становится."""
    bo3 = _row(series_type=1, cyberscore_best_of=3, radiant_series_wins=0, dire_series_wins=0)
    assert cs._winline_series_last_map(1, "Team Lynx", "No Hoodwink", bo3) is False
