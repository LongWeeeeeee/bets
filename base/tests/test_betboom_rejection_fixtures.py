from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from base import bookmaker_selenium_odds as odds_parser


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"


class _FakeRequests(list):
    def clear(self) -> None:
        super().clear()


class _FakeDriver:
    def __init__(self, *, page_source: str, body_text: str) -> None:
        self.page_source = page_source
        self._body_text = body_text
        self.current_url = "about:blank"
        self.requests = _FakeRequests()
        self.scopes = None

    def get(self, url: str) -> None:
        self.current_url = url

    def execute_script(self, _script: str) -> None:
        return None

    def find_element(self, _by, _value):
        return SimpleNamespace(text=self._body_text)

    def refresh(self) -> None:
        return None


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _patch_no_sleep(monkeypatch) -> None:
    monkeypatch.setattr(odds_parser.time, "sleep", lambda *_args, **_kwargs: None)


def test_betboom_match_level_rejected_fixture(monkeypatch) -> None:
    _patch_no_sleep(monkeypatch)
    fixture = _load_fixture("betboom_match_level_rejected.json")
    driver = _FakeDriver(
        page_source=f"<html><body>{fixture['body_text']}</body></html>",
        body_text=fixture["body_text"],
    )

    monkeypatch.setattr(
        odds_parser,
        "_parse_map_market_on_current_page",
        lambda *_args, **_kwargs: ([], fixture["body_text"]),
    )
    monkeypatch.setattr(
        odds_parser,
        "_is_map_market_closed",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        odds_parser,
        "_iter_request_texts",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        odds_parser,
        "_find_from_sources",
        lambda *_args, **_kwargs: (
            True,
            [2.90, 1.32],
            "dom_visible_text",
            fixture["details_with_match_odds"],
        ),
    )

    result = odds_parser.parse_site(
        driver,
        fixture["site"],
        fixture["url"],
        fixture["team1"],
        fixture["team2"],
        mode="live",
        forced_map_num=fixture["forced_map_num"],
    )

    assert result.match_found is True
    assert result.odds == []
    assert result.source == "betboom_match_level_rejected"
    assert "2-я карта" in result.details


def test_betboom_map_market_closed_fixture(monkeypatch) -> None:
    _patch_no_sleep(monkeypatch)
    fixture = _load_fixture("betboom_map_market_closed.json")
    driver = _FakeDriver(
        page_source=f"<html><body>{fixture['body_text']}</body></html>",
        body_text=fixture["body_text"],
    )

    monkeypatch.setattr(
        odds_parser,
        "_parse_map_market_on_current_page",
        lambda *_args, **_kwargs: ([], fixture["body_text"]),
    )
    monkeypatch.setattr(
        odds_parser,
        "_is_map_market_closed",
        lambda *_args, **_kwargs: True,
    )

    def _unexpected_find(*_args, **_kwargs):
        raise AssertionError("_find_from_sources should not run for map_market_missing path")

    monkeypatch.setattr(odds_parser, "_find_from_sources", _unexpected_find)
    monkeypatch.setattr(
        odds_parser,
        "_iter_request_texts",
        lambda *_args, **_kwargs: [],
    )

    result = odds_parser.parse_site(
        driver,
        fixture["site"],
        fixture["url"],
        fixture["team1"],
        fixture["team2"],
        mode="live",
        forced_map_num=fixture["forced_map_num"],
    )

    assert result.match_found is True
    assert result.odds == []
    assert result.source == "betboom_map_market_closed"
    assert result.market_closed is True


def test_betboom_map_market_missing_fixture(monkeypatch) -> None:
    _patch_no_sleep(monkeypatch)
    fixture = _load_fixture("betboom_map_market_missing.json")
    driver = _FakeDriver(
        page_source=f"<html><body>{fixture['body_text']}</body></html>",
        body_text=fixture["body_text"],
    )

    monkeypatch.setattr(
        odds_parser,
        "_parse_map_market_on_current_page",
        lambda *_args, **_kwargs: ([], fixture["body_text"]),
    )
    monkeypatch.setattr(
        odds_parser,
        "_is_map_market_closed",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        odds_parser,
        "_iter_request_texts",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        odds_parser,
        "_find_from_sources",
        lambda *_args, **_kwargs: (True, [], "dom_visible_text", fixture["body_text"]),
    )

    result = odds_parser.parse_site(
        driver,
        fixture["site"],
        fixture["url"],
        fixture["team1"],
        fixture["team2"],
        mode="live",
        forced_map_num=fixture["forced_map_num"],
    )

    assert result.match_found is True
    assert result.odds == []
    assert result.source == "betboom_map_market_missing"
    assert result.source != "betboom_map_market_closed"
    assert result.market_closed is False
