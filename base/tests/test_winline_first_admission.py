"""Winline-first admission: живая лента Winline разбирается ПЕРВОЙ.

Контракт (заявка 09.09.2026, кейс MOUZ vs Klim Sani4; поправка 10.09.2026:
league allowlist и league-denylist остаются ЖЁСТКИМИ границами — на Winline
есть карты, которые мы не хотим разбирать вовсе):
- порядок «сначала матч SourceTV, потом поиск в Winline» ронял матчи с
  неполным опознанием ДО опроса букмекера: team_id-гейт требует id ОБЕИХ
  сторон. Ни кэфы, ни сам матч не разбирались, хотя Winline матч вёл;
- новый порядок: раз в цикл снимается общий live-снимок Winline (лиги +
  команды), и матч SourceTV ВНУТРИ allowlist-лиг, найденный в живой карточке
  Winline, допускается мимо team_id-гейта. Неизвестная сторона остаётся
  неизвестной (id 0, tier 3 по правилам tier 2, без авто-онбординга в tier2 —
  как явный tier-3 allowlist). Нет снимка или нет совпадения — прежний путь
  без изменений (fail-open).

Захваченные артефакты:
- `base/tests/fixtures/winline_pinned_bar_shadows_card.html` (захват 01.08.2026):
  живая лента, `Counter-Strike | BLAST Bounty, Qualifier 1 FOKUS MOUZ 2карта`;
- строки Dota-ленты ниже — копии захватов 05.08.2026 из
  `test_winline_dota_feed_source.py` (лог winline_parser_monitor + дамп ленты).
Синтетическая карточка MOUZ используется ТОЛЬКО для plumbing bypass-флагов
(формат Winline там не под тестом — он покрыт захваченными артефактами выше).
"""
from __future__ import annotations

import json
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
    monkeypatch.setattr(runtime, "PURE_DLTV_MODE", False, raising=False)
    monkeypatch.setattr(
        runtime, "BOOKMAKER_PREFETCH_GATE_MODE", "odds", raising=False
    )
    monkeypatch.setattr(runtime, "BOOKMAKER_CAMOUFOX_ENABLED", True, raising=False)
    monkeypatch.setattr(runtime, "BOOKMAKER_CAMOUFOX_IMPORTED", True, raising=False)
    runtime._winline_first_parser_fns_cache = None  # noqa: SLF001
    runtime._winline_overview_inject_for_tests("")  # noqa: SLF001


def test_prod_no_odds_shape_stays_active(monkeypatch) -> None:
    """Прод идёт с --no-odds (prefetch OFF): допуск обязан работать и там.

    Регрессия 10.09.2026: `_winline_first_active` требовал включённый
    prefetch, и весь winline-first был dormant именно в прод-режиме.
    """
    _enable_winline_first(monkeypatch)
    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    runtime._winline_overview_inject_for_tests(DOTA_LIVE_CARD)  # noqa: SLF001
    assert runtime._winline_first_active() is True  # noqa: SLF001
    hit = runtime._winline_first_join("MOUZ", "Klim Sani4")  # noqa: SLF001
    assert hit is not None
    assert runtime._winline_first_bypass_active(hit) is True  # noqa: SLF001


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


def test_join_hit_activates_bypass_directly(monkeypatch) -> None:
    """Регрессия 10.09.2026: hit из join обязан сразу проходить bypass_active.

    До фикса штамп `admitted_at` ставил только heads-хелпер, а per-card путь
    (join → bypass_active на team_id-гейте) получал hit без штампа — допуск
    был мёртв, хотя тесты смотрели только heads-путь.
    """
    _enable_winline_first(monkeypatch)
    runtime._winline_overview_inject_for_tests(DOTA_LIVE_CARD)  # noqa: SLF001
    hit = runtime._winline_first_join("MOUZ", "Klim Sani4")  # noqa: SLF001
    assert hit is not None
    assert runtime._winline_first_bypass_active(hit) is True  # noqa: SLF001
    assert runtime._winline_first_bypass_active(None) is False  # noqa: SLF001
    assert runtime._winline_first_bypass_active({}) is False  # noqa: SLF001


def test_bypass_expires(monkeypatch) -> None:
    _enable_winline_first(monkeypatch)
    runtime._winline_overview_inject_for_tests(DOTA_LIVE_CARD)  # noqa: SLF001
    hit = runtime._winline_first_join("MOUZ", "Klim Sani4")  # noqa: SLF001
    assert hit is not None
    hit["admitted_at"] -= 10_000.0
    assert runtime._winline_first_bypass_active(hit) is False  # noqa: SLF001


def test_winline_first_disabled_by_flag(monkeypatch) -> None:
    _enable_winline_first(monkeypatch)
    monkeypatch.setattr(runtime, "WINLINE_FIRST_ENABLED", False, raising=False)
    runtime._winline_overview_inject_for_tests(DOTA_LIVE_CARD)  # noqa: SLF001
    assert runtime._winline_first_active() is False  # noqa: SLF001
    assert runtime._winline_first_join("MOUZ", "Klim Sani4") is None  # noqa: SLF001


def test_non_allowlisted_league_filtered_without_crash(monkeypatch, tmp_path) -> None:
    """Регрессия 10.09.2026: arity вызова фильтра и определения обязаны совпадать.

    В прод уехал вызов `_league_admits_with_known_side` с чужой строкой
    (`_gated_tier12_side`, 4-й аргумент) без парного изменения определения —
    главный цикл падал с TypeError на каждом не-allowlist матче. Тест едет
    через настоящий call site (`get_heads`, SourceTV-ветка) с записью вида
    Mad Dogs League: битый вызов роняет его TypeError, целый отдаёт ([], []).
    """
    import json
    import time

    bridge = {
        "mid-268": {
            "match_id": "mid-268",
            "series_id": "mid-268",
            "league_id": 17911,
            "league_name": "Mad Dogs League",
            "radiant_team_name": "Azure Dragons",
            "radiant_team_id": 0,
            "dire_team_name": "Stormriders",
            "dire_team_id": 0,
            "radiant_score": 0,
            "dire_score": 0,
            "radiant_series_wins": 0,
            "dire_series_wins": 0,
            "series_game_number": 1,
            "series_type": 1,
            "game_time": 100.0,
            "radiant_lead": 0,
            "timestamp": time.time(),
        }
    }
    bridge_path = tmp_path / "sourcetv_matches.json"
    bridge_path.write_text(json.dumps(bridge), encoding="utf-8")
    monkeypatch.setattr(runtime, "SOURCETV_MATCHES_PATH", str(bridge_path))
    monkeypatch.setattr(runtime, "DLTV_SOURCE_MODE", "sourcetv")
    # Герметичность: прогрев снимка Winline здесь не под тестом, а его поток
    # пережил бы тест (daemon) и дёргал бы общую Camoufox-очередь.
    monkeypatch.setattr(
        runtime, "_ensure_winline_overview_refresher", lambda: None
    )
    assert runtime._league_matches_allowlist(17911, "Mad Dogs League") is False
    heads, bodies = runtime.get_heads()
    assert heads == [] and bodies == []


def test_known_side_filter_call_matches_def_arity() -> None:
    """Tripwire 10.09.2026: ни один вызов не передаёт больше, чем принимает деф.

    В прод уехал вызов `_league_admits_with_known_side` с чужой строкой
    (4-й аргумент) без парного изменения определения — главный цикл падал с
    `takes 3 positional arguments but 4 were given` на каждом не-allowlist
    матче (доказано логом serv1; red показан строгой 3-арг подменой дефа:
    битый вызов роняет TypeError). Поведенческий тест выше в этом дереве
    битый вызов не ловит (здесь уже лежит парный 4-арг деф пира), поэтому
    соответствие проверяется статически по исходнику: каждый позиционный
    вызов обязан укладываться в сигнатуру. Числа не захардкожены — парная
    правка дефа и всех вызовов остаётся зелёной.
    """
    import ast
    import inspect
    from pathlib import Path

    target = "_league_admits_with_known_side"
    n_params = len(inspect.signature(getattr(runtime, target)).parameters)
    tree = ast.parse(Path(runtime.__file__).read_text(encoding="utf-8"))
    checked = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = getattr(func, "attr", None) or getattr(func, "id", None)
        if name != target:
            continue
        if any(isinstance(arg, ast.Starred) for arg in node.args):
            continue
        checked += 1
        assert len(node.args) + len(node.keywords) <= n_params, (
            f"вызов {target} передаёт больше аргументов, чем принимает деф "
            f"({len(node.args) + len(node.keywords)} > {n_params})"
        )
    assert checked >= 2, "call sites не найдены — тест ослеп"


# Захват shell-снимка 10.09.2026: холодное чтение сразу после goto (SPA не
# поднялось) — в тексте только заголовок, 149 символов, в DOM нет ни кнопок,
# ни заголовков. Такой снимок затирал хороший, и join не срабатывал НИ РАЗУ
# (в прод-логе ноль `лига Winline`).
SHELL_TEXT = (
    "Ставки на Dota 2 онлайн, сделать ставку в линии ✔️ коэффициенты на Winline "
    "Ставки на Dota 2 онлайн, сделать ставку в линии ✔️ коэффициенты на Winline"
)
SHELL_HTML = "<html><head><title>Ставки на Dota 2</title></head><body></body></html>"
FEED_HTML = (
    '<div class="card"><span class="period-name">1К</span>'
    '<button class="coefficient-button">2.60</button></div>'
)
# Живая лента всегда несёт chrome (меню лиг/дисциплин, ~3.5k в захвате
# 10.09.2026): порог 500 лежит в пустом промежутке между shell (149) и лентой.
FEED_TEXT = (DOTA_LIVE_CARD + " ") * 6


def test_overview_shell_snapshot_rejected() -> None:
    """Shell без признаков ленты отклоняется предикатом."""
    assert (
        odds_parser._winline_overview_payload_looks_like_feed(  # noqa: SLF001
            SHELL_TEXT, SHELL_HTML
        )
        is False
    )
    assert (
        odds_parser._winline_overview_payload_looks_like_feed("", "")  # noqa: SLF001
        is False
    )


def test_overview_feed_snapshot_accepted() -> None:
    """Лента с классами карточек принимается предикатом."""
    assert (
        odds_parser._winline_overview_payload_looks_like_feed(  # noqa: SLF001
            FEED_TEXT, FEED_HTML
        )
        is True
    )


def _fake_overview_fns(payload: dict):
    def fake_collect(page, url):
        return dict(payload)

    fake_collect.__module__ = "bookmaker_selenium_odds"
    return (
        fake_collect,
        lambda *args, **kwargs: "",
        lambda *args, **kwargs: False,
        lambda *args, **kwargs: None,
    )


def _offline_refresh(monkeypatch, payload: dict) -> None:
    _enable_winline_first(monkeypatch)
    monkeypatch.setattr(runtime, "_winline_first_parser_fns", lambda: _fake_overview_fns(payload))
    monkeypatch.setattr(
        runtime, "_bookmaker_urls_for_mode", lambda mode: {"winline": "http://x"}
    )
    monkeypatch.setattr(
        runtime, "_run_shared_camoufox_job", lambda name, job, **kw: job(object())
    )
    monkeypatch.setattr(
        runtime._shared_camoufox_session,  # noqa: SLF001
        "get_or_create_page",
        lambda *args, **kwargs: object(),
    )


def test_refresh_rejects_shell_and_keeps_prior_feed(monkeypatch) -> None:
    """Регрессия 10.09.2026: shell-съём не затирает хороший снимок.

    До фикса refresh складировал любой непустой текст: холодное чтение
    (149 символов заголовка) затирало ленту, и `_winline_first_join` никогда
    не находил карточки в проде.
    """
    _offline_refresh(
        monkeypatch,
        {"text": SHELL_TEXT, "html": SHELL_HTML, "status": "ok", "error": ""},
    )
    runtime._winline_overview_inject_for_tests(FEED_TEXT)  # noqa: SLF001
    assert runtime._winline_overview_refresh_once() is False  # noqa: SLF001
    assert runtime._winline_overview_snapshot_text() == FEED_TEXT  # noqa: SLF001


class _FakeSettlePage:
    """Дабл страницы: скрипт ответов evaluate, подсчёт опросов."""

    def __init__(self, script) -> None:
        self._script = list(script)
        self.calls = 0

    def evaluate(self, js):
        self.calls += 1
        return self._script[min(self.calls - 1, len(self._script) - 1)]


def test_settle_waits_for_feed_markers() -> None:
    """Settle опрашивает DOM, пока не появятся маркеры карточек."""
    import asyncio

    page = _FakeSettlePage([False, False, True])
    assert asyncio.run(odds_parser._settle_winline_overview_feed(page, timeout_s=30)) is True
    assert page.calls == 3


def test_settle_times_out_without_markers() -> None:
    """Без маркеров — быстрый False, дальше решает предикат shell/feed."""
    import asyncio

    page = _FakeSettlePage([False])
    assert asyncio.run(odds_parser._settle_winline_overview_feed(page, timeout_s=0.01)) is False
    # Детерминировано: первый опрос раньше дедлайна, второй — после sleep(2).
    assert page.calls == 2


def test_refresh_stores_feed_snapshot(monkeypatch) -> None:
    """Лента с признаками feed кладётся в состояние и читается join."""
    _offline_refresh(
        monkeypatch,
        {"text": FEED_TEXT, "html": FEED_HTML, "status": "ok", "error": ""},
    )
    runtime._winline_overview_inject_for_tests("")  # noqa: SLF001
    assert runtime._winline_overview_refresh_once() is True  # noqa: SLF001
    assert runtime._winline_overview_snapshot_text() == FEED_TEXT  # noqa: SLF001


# Живой захват 10.09.2026 (прогретый снимок): обе demanded-пары с кэфами.
YS_PT_CARD = (
    "DOTA 2 | BLAST Slam, Qualifier YELLOW SUBMARINE PLAYTIME "
    "1карта 0 0 0 0 1К Матч 2.60 1.40 - - - - - - 1 карта 2.60 1.40 - - - - - - "
)
IC_KAL_CARD = (
    "INNER CIRCLE KALMYCHATA 0 0 0 0 1К Матч 1.30 3.00 - - - - - - "
)
YS_PT_SNAPSHOT = "ГЛАВНАЯ LIVE DOTA 2 " + YS_PT_CARD + IC_KAL_CARD + "DOTA 2 | Mad Dogs League "
# Две соседние live-карточки под одним заголовком лиги: плоский текст их не
# делит (single_card_scope), DOM — делит. Разметка повторяет живую.
YS_PT_HTML = (
    '<div class="feed">'
    '<div class="card"><span>YELLOW SUBMARINE</span><span>PLAYTIME</span>'
    "<span>1карта</span>"
    '<button class="coefficient-button">2.60</button>'
    '<button class="coefficient-button">1.40</button></div>'
    '<div class="card"><span>INNER CIRCLE</span><span>KALMYCHATA</span>'
    "<span>1карта</span>"
    '<button class="coefficient-button">1.30</button>'
    '<button class="coefficient-button">3.00</button></div>'
    "</div>"
)

WEAK_YS_HINT = {
    "radiant": {
        "team_key": "yellowsubmarine",
        "display": "Yellow Submarine",
        "players": 3,
        "team_ids": [2576071],
        "account_ids": [11, 12, 13],
        "weak": True,
    },
    "dire": None,
}


def test_weak_confirm_with_bridge_anchor(monkeypatch) -> None:
    """E-270: weak-хинт + точное имя второй стороны + живая карточка = hit.

    Кейс 10.09.2026: GC `None vs PlayTime` (10877), составы 3/5 YS,
    карточка Winline `YELLOW SUBMARINE PLAYTIME` с кэфами 2.60/1.40.
    """
    _enable_winline_first(monkeypatch)
    runtime._winline_overview_inject_for_tests(YS_PT_SNAPSHOT, YS_PT_HTML)  # noqa: SLF001
    hit = runtime._winline_player_confirm("Radiant", "PlayTime", WEAK_YS_HINT)  # noqa: SLF001
    assert hit is not None
    assert hit["confirmed_names"] == ("Yellow Submarine", "PlayTime")
    assert hit["confirmed_ids"] == {"radiant": 2576071}
    assert runtime._winline_first_bypass_active(hit) is True  # noqa: SLF001


def test_weak_confirm_without_anchor_rejected(monkeypatch) -> None:
    """Weak-хинт без якоря точным именем из моста — отказ (обе стороны хинты)."""
    _enable_winline_first(monkeypatch)
    runtime._winline_overview_inject_for_tests(YS_PT_SNAPSHOT, YS_PT_HTML)  # noqa: SLF001
    hint = {
        "radiant": dict(WEAK_YS_HINT["radiant"]),
        "dire": {
            "team_key": "playtime",
            "display": "PlayTime",
            "players": 3,
            "team_ids": [10207983],
            "account_ids": [21, 22, 23],
            "weak": True,
        },
    }
    assert runtime._winline_player_confirm("Radiant", "Dire", hint) is None  # noqa: SLF001


def test_card_admits_gated_league_but_not_denied_or_foreign(monkeypatch) -> None:
    """E-270: карточка открывает league-фильтр только внутри разрешённого."""
    import time

    _enable_winline_first(monkeypatch)
    runtime._winline_overview_inject_for_tests(YS_PT_SNAPSHOT, YS_PT_HTML)  # noqa: SLF001
    hit = runtime._winline_player_confirm("Radiant", "PlayTime", WEAK_YS_HINT)  # noqa: SLF001
    assert hit is not None
    assert runtime._winline_card_admits_league(hit, 10877) is True  # noqa: SLF001
    assert runtime._winline_card_admits_league(hit, 17911) is False  # noqa: SLF001
    assert runtime._winline_card_admits_league(None, 10877) is False  # noqa: SLF001
    denied_hit = {
        "card": "x",
        "league": "BLAST Slam VII: China Open Qualifier 2",
        "admitted_at": time.time(),
        "admission_ttl": 180.0,
    }
    assert runtime._winline_card_admits_league(denied_hit, 10877) is False  # noqa: SLF001
    no_league_hit = dict(denied_hit, league="")
    assert runtime._winline_card_admits_league(no_league_hit, 10877) is False  # noqa: SLF001


def test_weak_pair_flows_through_league_filter(monkeypatch, tmp_path) -> None:
    """E-270 end-to-end admission: мост None–PlayTime + карточка → mock-нода.

    До фикса: probe не писал такой мост, а league-фильтр ронял его даже при
    наличии (нет известной стороны по id). Здесь — тот же каркас, что в
    Mad-Dogs-тесте выше, но пара подтверждена и нода строится.
    """
    import json
    import time

    _enable_winline_first(monkeypatch)
    runtime._winline_overview_inject_for_tests(YS_PT_SNAPSHOT, YS_PT_HTML)  # noqa: SLF001
    bridge = {
        "8991598414": {
            "match_id": 8991598414,
            "series_id": 8991598414,
            "league_id": 10877,
            "league_name": "",
            "radiant_team_name": "Radiant",
            "radiant_team_id": 0,
            "dire_team_name": "PlayTime",
            "dire_team_id": 10020555,
            "radiant_score": 0,
            "dire_score": 0,
            "radiant_series_wins": 0,
            "dire_series_wins": 0,
            "series_game_number": 1,
            "series_type": 1,
            "game_time": 100.0,
            "radiant_lead": 0,
            "timestamp": time.time(),
            "player_hint": WEAK_YS_HINT,
        }
    }
    bridge_path = tmp_path / "sourcetv_matches.json"
    bridge_path.write_text(json.dumps(bridge), encoding="utf-8")
    monkeypatch.setattr(runtime, "SOURCETV_MATCHES_PATH", str(bridge_path))
    monkeypatch.setattr(runtime, "DLTV_SOURCE_MODE", "sourcetv")
    monkeypatch.setattr(
        runtime, "_ensure_winline_overview_refresher", lambda: None
    )
    heads, bodies = runtime.get_heads()
    assert len(heads) == 1 and len(bodies) == 1
    assert "PlayTime" in bodies[0].get_text()
    assert "Radiant" in bodies[0].get_text()


class TestOverviewSnapshotPersist:
    """Сброс успешного feed-снимка на диск (разработка перечислителя карт)."""

    def test_snapshot_persisted(self, tmp_path, monkeypatch):
        monkeypatch.setenv(
            "WINLINE_OVERVIEW_SNAPSHOT_PATH", str(tmp_path / "snap.json"))
        assert runtime._winline_overview_persist_snapshot(
            "FEEDTEXT", "<html>H</html>") is True
        data = json.loads((tmp_path / "snap.json").read_text(encoding="utf-8"))
        assert data["text"] == "FEEDTEXT"
        assert "<html>" in data["html"]
        assert data["wall"] > 0

    def test_snapshot_fail_open(self, tmp_path, monkeypatch):
        monkeypatch.setenv("WINLINE_OVERVIEW_SNAPSHOT_PATH",
                           "/nonexistent-dir-xyz-abc/snap.json")
        assert runtime._winline_overview_persist_snapshot("t", "h") is False
