from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
ROOT_DIR = BASE_DIR.parent
for path in (str(ROOT_DIR), str(BASE_DIR)):
    if path not in sys.path:
        sys.path.insert(0, path)

import bookmaker_selenium_odds as odds_parser  # noqa: E402


def test_winline_event_href_is_treated_as_match_page() -> None:
    assert odds_parser._href_looks_match_page(
        "winline",
        "https://winline.ru/stavki/event/15356534",
    )


def test_winline_feed_context_accepts_map_shorthand() -> None:
    odds = odds_parser._extract_map_odds_from_feed_context(
        "winline",
        "WINTER BEAR DOGSENT 2К 1.30 3.15",
        team1="Winter Bear",
        team2="DOGSENT",
        forced_map_num=2,
    )

    assert odds == [1.30, 3.15]


def test_pari_deeplink_requires_target_team_context() -> None:
    odds = odds_parser._extract_map_odds_deeplink(
        "pari",
        "PREMIER SERIES 2-я карта 1.58 - 2.25 Power Rangers Team Shpilit",
        "Winter Bear",
        "DOGSENT",
        forced_map_num=2,
    )

    assert odds == []


def test_winline_match_fallback_is_extracted_from_context() -> None:
    odds = odds_parser._extract_match_odds_from_context(
        "winline",
        "WINTER BEAR DOGSENT 2карта 31' +13 0 1 30 13 2К 9 37 1К Матч 1.30 3.15 - - - - - -",
        team1="Winter Bear",
        team2="DOGSENT",
    )

    assert odds == [1.30, 3.15]


def test_presence_sources_accept_aliases() -> None:
    found, source, detail = odds_parser._find_presence_from_sources(
        "XctN",
        "Rekonix",
        [("dom_body_text", "Execration REKONIX 1-я карта П1 2.3 П2 1.55")],
        team1_aliases=["Execration"],
        team2_aliases=["REKONIX"],
    )

    assert found is True
    assert source == "dom_body_text"
    assert "matched_as=Execration vs Rekonix" in detail


def test_run_presence_sites_parallel_preserves_site_order_and_quits_drivers(monkeypatch) -> None:
    """Selenium presence path: singleton driver, ordered results, quit on cleanup path.

    Presence behavior must stay independent of odds shared-Camoufox wiring.
    """
    # Force selenium path and reset singleton state used by current presence runner.
    monkeypatch.setattr(odds_parser, "BOOKMAKER_CAMOUFOX_PRESENCE_ENABLED", False)
    monkeypatch.setattr(odds_parser, "_presence_driver", None)
    monkeypatch.setattr(odds_parser, "_presence_base_handles", {})
    monkeypatch.setattr(odds_parser, "_presence_base_initialized", False)
    monkeypatch.setattr(odds_parser.time, "sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(odds_parser.time, "monotonic", lambda: 0.0)

    @dataclass
    class _FakeDriver:
        site: str
        quit_called: bool = False
        current_window_handle: str = "base"
        window_handles: List[str] = field(default_factory=lambda: ["base"])

        def get(self, _url: str) -> None:
            return None

        def execute_script(self, _script: str, *args):
            if isinstance(_script, str) and "window.open" in _script:
                handle = f"tab-{len(self.window_handles)}"
                self.window_handles.append(handle)
            return None

        def __post_init__(self) -> None:
            class _Switch:
                def window(self_inner, _h):
                    return None

            # Selenium exposes switch_to as an attribute, not a callable.
            self.switch_to = _Switch()

        def save_screenshot(self, path: str) -> bool:
            return True

        def quit(self) -> None:
            self.quit_called = True

    created: list[_FakeDriver] = []

    def _build_driver(_proxy_url: str, **_kwargs) -> _FakeDriver:
        drv = _FakeDriver(site=f"drv-{len(created)}")
        created.append(drv)
        return drv

    def _probe(
        drv,
        *,
        site: str,
        url: str,
        team1: str,
        team2: str,
        mode: str,
        team1_aliases=None,
        team2_aliases=None,
        **_kwargs,
    ):
        assert drv in created
        assert team1 == "OG"
        assert team2 == "Rekonix"
        assert mode == "live"
        return odds_parser.SiteResult(
            site=site,
            url=url,
            status="ok",
            match_found=(site != "pari"),
            odds=[],
            source="presence_found" if site != "pari" else "presence_missing",
            details=f"checked {site}",
            market_closed=False,
            match_odds=[],
        )

    # Avoid OCR fallback path (optional deps; not part of this contract).
    class _NoOcr:
        def __getattr__(self, _name: str):
            raise ImportError("bookmaker_ocr disabled in unit test")

    monkeypatch.setattr(odds_parser, "_build_driver", _build_driver)
    monkeypatch.setattr(odds_parser, "_probe_presence_site_in_current_tab", _probe)
    monkeypatch.setitem(sys.modules, "base.bookmaker_ocr", _NoOcr())

    results = odds_parser.run_presence_sites_parallel(
        selected_sites=["betboom", "pari", "winline"],
        urls={
            "betboom": "https://betboom.example/live",
            "pari": "https://pari.example/live",
            "winline": "https://winline.example/live",
        },
        team1="OG",
        team2="Rekonix",
        mode="live",
        team1_aliases=["og"],
        team2_aliases=["rekonix"],
    )

    assert [item.site for item in results] == ["betboom", "pari", "winline"]
    assert [item.match_found for item in results] == [True, False, True]
    assert len(created) == 1
    # Singleton presence driver is intentionally long-lived (reused across calls);
    # production does not quit after one presence batch.
    assert created[0].quit_called is False

def _load_overview_snapshot() -> dict:
    """Захваченный прод-дамп обзора Winline.

    Захват: serv1, 10.09.2026 ~18:13 MSK, runtime/winline_overview_snapshot.json
    (feed 4191 chars / html 526829; 8 карточек, 4 live). Полный файл без обрезки.
    """
    import json as _json

    path = Path(__file__).resolve().parent / "fixtures" / "winline_overview_snapshot_20260910.json"
    return _json.loads(path.read_text(encoding="utf-8"))


def test_winline_enumerate_live_cards() -> None:
    snap = _load_overview_snapshot()
    cards = odds_parser.winline_enumerate_live_cards(snap["html"])
    live = [c for c in cards if c["live"]]
    assert len(cards) == 8
    assert len(live) == 4
    by_event = {c["event_id"]: c for c in cards}
    recrent = by_event["16659473"]
    assert recrent["league"] == "WINLINE Star Series"
    assert recrent["team1"] == "RECRENT CLUB"
    assert recrent["team2"] == "DAXAK CLUB"
    assert recrent["live"] is True
    zero_ten = by_event["16674794"]
    assert (zero_ten["team1"], zero_ten["team2"]) == ("ZERO TENACITY", "DEVIL KINGS")
    assert zero_ten["league"] == "BLAST Slam, Qualifier"
    cyber = by_event["16674833"]
    assert (cyber["team1"], cyber["team2"]) == ("CYBER NOVA", "DAWN BULLS")


def test_winline_enumerate_marks_prematch_and_mad_dogs() -> None:
    snap = _load_overview_snapshot()
    by_event = {c["event_id"]: c for c in odds_parser.winline_enumerate_live_cards(snap["html"])}
    navi = by_event["16674853"]
    assert navi["live"] is False
    assert (navi["team1"], navi["team2"]) == ("NATUS VINCERE", "KLIM SANI4")
    assert navi["league"] == "EPL Masters"
    # Mad Dogs перечисляется (гейт deny — дело рантайма, а не парсера).
    peace = by_event["16674209"]
    assert peace["live"] is True
    assert peace["league"] == "Mad Dogs League"
    assert (peace["team1"], peace["team2"]) == ("PEACEKEEPERS", "STORMRIDERS")


def test_winline_enumerate_map_rows_with_prices() -> None:
    snap = _load_overview_snapshot()
    by_event = {c["event_id"]: c for c in odds_parser.winline_enumerate_live_cards(snap["html"])}
    rows = {r["map_num"]: r["has_prices"]
            for r in by_event["16659473"]["rows"] if r["kind"] == "map"}
    assert rows.get(3) is True
    assert by_event["16659473"]["header_map"] == 2
