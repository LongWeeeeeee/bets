"""Личность sourcetv-карты из cyberscore, когда Valve не отдал названий.

26.08.2026, квалификация BLAST Slam на тикете Challengermode: карточка ушла в
чат как «СТАВКА НА Dire x0.5 / Radiant VS Dire» с ELO 1500 у обеих сторон —
ставка на сторону, которую мы не опознали. Тот же матч у cyberscore называется
'WhiteSails vs Team Hryvnia' и сшивается точно: `id_steam` строки листинга —
это dota match_id из моста (сверено на четырёх живых картах: совпали и время, и
счёт).
"""

import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


# Сокращённая, но настоящая по форме строка листинга (поля взяты из боевого
# ответа cyberscore.live 26.08.2026).
ROW_HTML = (
    'garbage{"id":185155,"slug":"whitesails-vs-team-hryvnia-185155","id_steam":8966484305,'
    '"status":"online","best_of":1,"game_time":2413,"game_map_number":1,'
    '"score_team_radiant":37,"score_team_dire":23,'
    '"tournament":{"id":46499,"slug":"res-unchained-5","name":"RES Unchained 5: BLAST Slam VIII EU OQ#1","tier":3},'
    '"team_radiant_id":51120,"team_radiant":{"id":51120,"name":"WhiteSails","tag":"WS"},'
    '"team_dire_id":51121,"team_dire":{"id":51121,"name":"Team Hryvnia","tag":"HRV"}}tail'
)


def test_listing_rows_are_keyed_by_dota_match_id() -> None:
    rows = runtime._parse_cyberscore_rows_by_steam_id(ROW_HTML)
    assert set(rows) == {8966484305}
    assert rows[8966484305]["slug"] == "whitesails-vs-team-hryvnia-185155"
    assert runtime._cyberscore_row_team_names(rows[8966484305]) == ("WhiteSails", "Team Hryvnia")
    # Мусор и строки без id_steam молча пропускаются.
    assert runtime._parse_cyberscore_rows_by_steam_id("") == {}
    assert runtime._parse_cyberscore_rows_by_steam_id('{"id":1,"name":"нет id_steam"}') == {}
    assert runtime._parse_cyberscore_rows_by_steam_id('{"id":1,"id_steam":0}') == {}


def test_placeholder_names_are_not_names() -> None:
    assert runtime._is_placeholder_team_name("Radiant") is True
    assert runtime._is_placeholder_team_name("dire") is True
    assert runtime._is_placeholder_team_name("") is True
    assert runtime._is_placeholder_team_name("WhiteSails") is False


def test_anonymous_match_takes_both_names_from_cyberscore() -> None:
    rows = runtime._parse_cyberscore_rows_by_steam_id(ROW_HTML)
    assert runtime._resolve_sourcetv_identity_from_cyberscore(
        8966484305, "Radiant", "Dire", rows=rows
    ) == ("WhiteSails", "Team Hryvnia", "RES Unchained 5: BLAST Slam VIII EU OQ#1")


def test_known_side_confirms_orientation() -> None:
    """Известная сторона совпала с той же стороной cyberscore — берём как есть."""
    rows = runtime._parse_cyberscore_rows_by_steam_id(ROW_HTML)
    radiant, dire, _ = runtime._resolve_sourcetv_identity_from_cyberscore(
        8966484305, "WhiteSails", "Dire", rows=rows
    )
    assert (radiant, dire) == ("WhiteSails", "Team Hryvnia")


def test_swapped_sides_are_flipped_not_trusted_blindly() -> None:
    """Если известная сторона у cyberscore на другой стороне — меняем местами.

    Ставка идёт на СТОРОНУ, поэтому переставить названия обязательно: иначе
    сообщение назовёт победителем чужую команду.
    """
    rows = runtime._parse_cyberscore_rows_by_steam_id(ROW_HTML)
    radiant, dire, _ = runtime._resolve_sourcetv_identity_from_cyberscore(
        8966484305, "Team Hryvnia", "Dire", rows=rows
    )
    assert (radiant, dire) == ("Team Hryvnia", "WhiteSails")


def test_disagreeing_row_is_rejected() -> None:
    """Известная сторона не совпала ни с одной — матч поняли по-разному, отказ."""
    rows = runtime._parse_cyberscore_rows_by_steam_id(ROW_HTML)
    assert runtime._resolve_sourcetv_identity_from_cyberscore(
        8966484305, "Team Spirit", "Dire", rows=rows
    ) is None


def test_unknown_match_id_resolves_to_nothing() -> None:
    rows = runtime._parse_cyberscore_rows_by_steam_id(ROW_HTML)
    assert runtime._resolve_sourcetv_identity_from_cyberscore(
        8966485074, "Radiant", "Dire", rows=rows
    ) is None
    assert runtime._resolve_sourcetv_identity_from_cyberscore(
        0, "Radiant", "Dire", rows=rows
    ) is None
    assert runtime._resolve_sourcetv_identity_from_cyberscore(
        None, "Radiant", "Dire", rows=rows
    ) is None


def test_listing_is_fetched_once_per_window(monkeypatch: pytest.MonkeyPatch) -> None:
    """Один заход в браузер обслуживает весь цикл, а не каждую карту отдельно."""
    calls = {"n": 0}

    def _fake_fetch(_url=None):
        calls["n"] += 1
        return ROW_HTML

    monkeypatch.setattr(runtime, "_get_cyberscore_html_via_camoufox", _fake_fetch)
    monkeypatch.setattr(runtime, "_sourcetv_identity_cache", {"at": 0.0, "rows": {}})

    first = runtime._sourcetv_identity_rows(now=1_000.0)
    assert set(first) == {8966484305}
    assert calls["n"] == 1
    runtime._sourcetv_identity_rows(now=1_000.0 + runtime.SOURCETV_IDENTITY_TTL - 1)
    assert calls["n"] == 1
    runtime._sourcetv_identity_rows(now=1_000.0 + runtime.SOURCETV_IDENTITY_TTL + 1)
    assert calls["n"] == 2


def test_failed_fetch_keeps_the_previous_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    """Отказ браузера не должен стирать уже известные названия."""
    state = {"html": ROW_HTML}

    def _fake_fetch(_url=None):
        if state["html"] is None:
            raise RuntimeError("camoufox упал")
        return state["html"]

    monkeypatch.setattr(runtime, "_get_cyberscore_html_via_camoufox", _fake_fetch)
    monkeypatch.setattr(runtime, "_sourcetv_identity_cache", {"at": 0.0, "rows": {}})

    runtime._sourcetv_identity_rows(now=2_000.0)
    state["html"] = None
    rows = runtime._sourcetv_identity_rows(now=2_000.0 + runtime.SOURCETV_IDENTITY_TTL + 1)
    assert set(rows) == {8966484305}


def test_both_sides_anonymous_is_a_bet_on_nobody() -> None:
    """Правило гейта: обе стороны без имени -> разбирать и отправлять нечего.

    Карточка «СТАВКА НА Dire / Radiant VS Dire» с ELO 1500 у обеих сторон никого
    не называет. Одна безымянная сторона рядом с названной (Kinetix — безымянные
    филиппинцы) остаётся допустимой: там понятно, на кого ставка.
    """
    both_anonymous = (
        runtime._is_placeholder_team_name("Radiant")
        and runtime._is_placeholder_team_name("Dire")
    )
    one_named = (
        runtime._is_placeholder_team_name("Radiant")
        and runtime._is_placeholder_team_name("Team Kinetix")
    )
    assert both_anonymous is True
    assert one_named is False
