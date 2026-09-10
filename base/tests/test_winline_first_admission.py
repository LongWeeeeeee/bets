"""Winline-first admission: живая лента Winline разбирается ПЕРВОЙ.

Контракт (заявка 09.09.2026, кейс MOUZ vs Klim Sani4):
- порядок «сначала матч SourceTV, потом поиск в Winline» ронял матчи с
  неполным опознанием ДО опроса букмекера: team_id-гейт требует id ОБЕИХ
  сторон, league-allowlist режет неизвестные лиги. Ни кэфы, ни сам матч
  не разбирались, хотя Winline матч вёл;
- новый порядок: раз в цикл снимается общий live-снимок Winline (лиги +
  команды), и матч SourceTV, найденный в живой карточке Winline, допускается
  мимо league-allowlist / team_id-гейта / league-denylist. Неизвестная сторона
  остаётся неизвестной (id 0, tier 3 по правилам tier 2, без авто-онбординга
  в tier2 — как явный tier-3 allowlist). Нет снимка или нет совпадения —
  прежний путь без изменений (fail-open).

Захваченные артефакты:
- `base/tests/fixtures/winline_pinned_bar_shadows_card.html` (захват 01.08.2026):
  живая лента, `Counter-Strike | BLAST Bounty, Qualifier 1 FOKUS MOUZ 2карта`;
- строки Dota-ленты ниже — копии захватов 05.08.2026 из
  `test_winline_dota_feed_source.py` (лог winline_parser_monitor + дамп ленты).
Синтетическая карточка MOUZ используется ТОЛЬКО для plumbing bypass-флагов
(формат Winline там не под тестом — он покрыт захваченными артефактами выше).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import bookmaker_selenium_odds as odds_parser  # noqa: E402
import cyberscore_try as runtime  # noqa: E402

FIXTURE_HTML = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "winline_pinned_bar_shadows_card.html"
)

# Копии захватов 05.08.2026 (см. test_winline_dota_feed_source.py).
DOTA_LIVE_CARD = (
    "DOTA 2 | EPL Season MOUZ KLIM SANI4 2карта 16' +68 1 0 16 5 2К "
    "Матч 1.53 2.40 2 карта 1.52 2.42"
)
DOTA_PREMATCH_CARD = (
    "DOTA 2 | EPL Season MOUZ KLIM SANI4 Завтра 15:00 +9 "
    "Матч 2.46 1.48 1.50 + 1.5 - 2.43 1.75 2.5 1.96 1 карта 2.23 1.58"
)


def _enable_winline_first(monkeypatch) -> None:
    monkeypatch.setattr(runtime, "WINLINE_FIRST_ENABLED", True, raising=False)
    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", True, raising=False)
    monkeypatch.setattr(runtime, "PURE_DLTV_MODE", False, raising=False)
    monkeypatch.setattr(
        runtime, "BOOKMAKER_PREFETCH_GATE_MODE", "odds", raising=False
    )
    runtime._winline_first_parser_fns_cache = None  # noqa: SLF001
    runtime._winline_overview_inject_for_tests("")  # noqa: SLF001


def _captured_page_text() -> str:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(FIXTURE_HTML.read_text(encoding="utf-8"), "html.parser")
    return " ".join(soup.stripped_strings)


def test_join_finds_captured_feed_card() -> None:
    """Захват 01.08.2026: живая feed-карточка FOKUS/MOUZ находится и не prematch."""
    card = odds_parser._winline_matched_card_context(
        _captured_page_text(), "FOKUS", "MOUZ"
    )
    assert card, "живая карточка FOKUS/MOUZ обязана находиться в захвате"
    assert "FOKUS" in card and "MOUZ" in card
    assert odds_parser._looks_future_context(card) is False


def test_league_extraction_on_captured_feed_card() -> None:
    """У feed-карточки с заголовком `Counter-Strike | <лига>` лига извлекается.

    Срез — механический, по захваченным маркерам (начало заголовка карточки ..
    конец строки ленты), без ручных правок байтов.
    """
    text = _captured_page_text()
    marker = "Counter-Strike | BLAST Bounty, Qualifier 1 FOKUS MOUZ"
    start = text.index(marker)
    tail = text[start:].split(" NINJAS IN PYJAMAS ", 1)[0]
    assert odds_parser.winline_live_card_league(tail, "FOKUS", "MOUZ") == (
        "BLAST Bounty, Qualifier 1"
    )


def test_join_finds_live_card_and_its_league(monkeypatch) -> None:
    _enable_winline_first(monkeypatch)
    runtime._winline_overview_inject_for_tests(DOTA_LIVE_CARD)  # noqa: SLF001
    hit = runtime._winline_first_join("MOUZ", "Klim Sani4")  # noqa: SLF001
    assert hit is not None, "живая карточка Winline обязана джойниться"
    assert hit["league"] == "EPL Season"
    assert "MOUZ" in hit["card"]


def test_join_rejects_prematch_card(monkeypatch) -> None:
    """Карточка линии (Завтра) — не повод допускать матч, даже если команды те же."""
    _enable_winline_first(monkeypatch)
    runtime._winline_overview_inject_for_tests(DOTA_PREMATCH_CARD)  # noqa: SLF001
    assert runtime._winline_first_join("MOUZ", "Klim Sani4") is None  # noqa: SLF001


def test_join_rejects_placeholder_sides(monkeypatch) -> None:
    """Radiant/Dire без опознания не джойнятся: токены встречаются в любом тексте."""
    _enable_winline_first(monkeypatch)
    runtime._winline_overview_inject_for_tests(DOTA_LIVE_CARD)  # noqa: SLF001
    assert runtime._winline_first_join("Radiant", "Dire") is None  # noqa: SLF001
    assert runtime._winline_first_join("MOUZ", "Dire") is None  # noqa: SLF001


def test_join_fail_open_without_snapshot(monkeypatch) -> None:
    """Нет снимка Winline — нет допуска (прежний путь), а не падение."""
    _enable_winline_first(monkeypatch)
    assert runtime._winline_first_join("MOUZ", "Klim Sani4") is None  # noqa: SLF001


def test_bypass_active_for_fresh_hit(monkeypatch) -> None:
    _enable_winline_first(monkeypatch)
    runtime._winline_overview_inject_for_tests(DOTA_LIVE_CARD)  # noqa: SLF001
    hit = runtime._winline_first_maybe_admit("mid-1", "MOUZ", "Klim Sani4")  # noqa: SLF001
    assert hit is not None
    assert hit["match_id"] == "mid-1"
    assert runtime._winline_first_bypass_active(hit) is True  # noqa: SLF001
    assert runtime._winline_first_bypass_active(None) is False  # noqa: SLF001
    assert runtime._winline_first_bypass_active({}) is False  # noqa: SLF001


def test_bypass_expires(monkeypatch) -> None:
    _enable_winline_first(monkeypatch)
    runtime._winline_overview_inject_for_tests(DOTA_LIVE_CARD)  # noqa: SLF001
    hit = runtime._winline_first_maybe_admit("mid-9", "MOUZ", "Klim Sani4")  # noqa: SLF001
    assert hit is not None
    hit["admitted_at"] -= 10_000.0
    assert runtime._winline_first_bypass_active(hit) is False  # noqa: SLF001


def test_winline_first_disabled_by_flag(monkeypatch) -> None:
    _enable_winline_first(monkeypatch)
    monkeypatch.setattr(runtime, "WINLINE_FIRST_ENABLED", False, raising=False)
    runtime._winline_overview_inject_for_tests(DOTA_LIVE_CARD)  # noqa: SLF001
    assert runtime._winline_first_active() is False  # noqa: SLF001
    assert runtime._winline_first_maybe_admit("mid-1", "MOUZ", "Klim Sani4") is None  # noqa: SLF001
