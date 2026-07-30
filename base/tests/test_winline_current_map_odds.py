from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import bookmaker_selenium_odds as odds_parser  # noqa: E402
import cyberscore_try as cs  # noqa: E402


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _patch_no_sleep(monkeypatch) -> None:
    monkeypatch.setattr(odds_parser.time, "sleep", lambda *_a, **_k: None)


class _FakeLocator:
    def __init__(self, text: str) -> None:
        self._text = text

    def inner_text(self, timeout: int = 0):  # noqa: ARG002
        return self._text


class _FakePage:
    def __init__(self, *, html: str, body_text: str, url: str = "about:blank") -> None:
        self._html = html
        self._body_text = body_text
        self.url = url

    def goto(self, url: str, wait_until: str = "domcontentloaded", timeout: int = 0):  # noqa: ARG002
        self.url = url

    def content(self) -> str:
        return self._html

    def locator(self, selector: str):
        if selector == "body":
            return _FakeLocator(self._body_text)
        raise AssertionError(f"unexpected selector: {selector}")

    def title(self) -> str:
        return ""

    def evaluate(self, script: str, arg=None):  # noqa: ARG002
        if "document.readyState" in script:
            return "complete"
        return False

    def reload(self, wait_until: str = "domcontentloaded", timeout: int = 0):  # noqa: ARG002
        return None


def _run_winline_camoufox_fixture(monkeypatch, fixture: dict) -> odds_parser.SiteResult:
    _patch_no_sleep(monkeypatch)
    body = fixture["body_text"]
    page = _FakePage(
        html=f"<html><body>{body}</body></html>",
        body_text=body,
        url=fixture["url"],
    )
    monkeypatch.setattr(
        odds_parser,
        "_load_site_render_payload_camoufox",
        lambda *_a, **_k: ("ok", "", page.content(), body, body),
    )
    monkeypatch.setattr(
        odds_parser,
        "_parse_map_market_on_current_camoufox_page",
        lambda *_a, **_k: (
            odds_parser._extract_map_odds_deeplink(
                "winline",
                body,
                fixture["team1"],
                fixture["team2"],
                forced_map_num=fixture["forced_map_num"],
            ),
            body,
        ),
    )
    monkeypatch.setattr(odds_parser, "_is_deeplink", lambda *_a, **_k: True)
    return odds_parser.parse_site_in_camoufox_page(
        page,
        fixture["site"],
        fixture["url"],
        fixture["team1"],
        fixture["team2"],
        mode="live",
        forced_map_num=fixture["forced_map_num"],
    )


def _assert_valid_current_map(result: odds_parser.SiteResult, fixture: dict) -> None:
    assert list(result.odds) == [pytest.approx(x) for x in fixture["expected_odds"]]
    assert getattr(result, "market_kind", None) == fixture["expected_market_kind"]
    assert getattr(result, "map_num", None) == fixture["forced_map_num"]
    assert getattr(result, "p1_team", None) == fixture["expected_p1_team"]
    assert getattr(result, "p2_team", None) == fixture["expected_p2_team"]
    assert result.market_closed is False
    # match_odds may exist diagnostically but must never be treated as valid current-map odds
    assert list(result.odds) != list(getattr(result, "match_odds", []) or [])


@pytest.mark.parametrize(
    "fixture_name",
    [
        "winline_current_map_winner_nk.json",
        "winline_current_map_winner_n_karta.json",
        "winline_current_map_winner_pobeditel.json",
        "winline_current_map_winner_reverse_order.json",
    ],
)
def test_winline_current_map_winner_accepted(monkeypatch, fixture_name: str) -> None:
    fixture = _load_fixture(fixture_name)
    result = _run_winline_camoufox_fixture(monkeypatch, fixture)
    assert result.match_found is True
    _assert_valid_current_map(result, fixture)


@pytest.mark.parametrize(
    "fixture_name",
    [
        "winline_current_map_match_only_rejected.json",
        "winline_current_map_match_and_other_map_rejected.json",
        "winline_current_map_wrong_map_rejected.json",
        "winline_current_map_ambiguous_order_rejected.json",
    ],
)
def test_winline_current_map_rejected(monkeypatch, fixture_name: str) -> None:
    fixture = _load_fixture(fixture_name)
    result = _run_winline_camoufox_fixture(monkeypatch, fixture)
    assert list(result.odds or []) == []
    blob = f"{result.source} {result.details}".lower()
    assert fixture["reject_reason_contains"].lower() in blob
    # Never promote match odds into valid current-map odds
    assert not (isinstance(result.odds, list) and len(result.odds) >= 2)


def test_winline_market_closed_fixture(monkeypatch) -> None:
    fixture = _load_fixture("winline_current_map_market_closed.json")
    result = _run_winline_camoufox_fixture(monkeypatch, fixture)
    assert list(result.odds or []) == []
    assert result.market_closed is True
    blob = f"{result.source} {result.details}".lower()
    assert "closed" in blob


def test_winline_reversed_alias_match_found_when_requested_map_missing(monkeypatch) -> None:
    fixture = _load_fixture("winline_ilbirs_zero_tenacity_missing_map1.json")
    result = _run_winline_camoufox_fixture(monkeypatch, fixture)

    assert result.match_found is fixture["expected_match_found"]
    assert result.odds == fixture["expected_odds"]
    assert result.map_num == fixture["expected_map_num"]
    assert result.market_kind is None
    assert fixture["expected_other_map_odds"] != result.odds
    assert fixture["reject_reason_contains"] in f"{result.source} {result.details}".lower()


def test_winline_reversed_alias_requested_map_odds_align_to_input_order() -> None:
    extracted = odds_parser._extract_winline_current_map_winner(
        "ZERO TENACITY ILBIRS 1К 2.15 1.62 2К 1.62 2.15 Матч 1.38 2.80",
        "Ilbirs Esports",
        "Zero Tenacity",
        forced_map_num=1,
    )

    assert extracted.odds == [pytest.approx(1.62), pytest.approx(2.15)]
    assert extracted.map_num == 1
    assert extracted.market_kind == "current_map_winner"
    assert extracted.p1_team == "team1"
    assert extracted.p2_team == "team2"


def test_winline_adjacent_cards_requested_map_odds_stay_on_target_card() -> None:
    body = (
        "DOTA 2 | EPL Masters ZERO TENACITY ILBIRS "
        "1К 2.15 1.62 2К 1.62 2.15 Матч 1.38 2.80 "
        "DOTA 2 | Other League NEXT RADIANT NEXT DIRE "
        "1К 1.91 1.83 Матч 1.70 2.05"
    )
    extracted = odds_parser._extract_winline_current_map_winner(
        body,
        "Ilbirs Esports",
        "Zero Tenacity",
        forced_map_num=1,
    )

    assert extracted.odds == [pytest.approx(1.62), pytest.approx(2.15)]
    assert extracted.map_num == 1
    assert extracted.market_kind == "current_map_winner"
    assert extracted.p1_team == "team1"
    assert extracted.p2_team == "team2"


def test_winline_live_acquisition_uses_explicit_live_feed() -> None:
    assert odds_parser.BOOKMAKER_URLS["live"]["winline"] == (
        "https://winline.ru/stavki/sport/kibersport/live"
    )


def test_winline_live_feed_reversed_alias_keeps_match_when_map1_missing(monkeypatch) -> None:
    fixture = _load_fixture("winline_ilbirs_zero_tenacity_missing_map1.json")
    body = fixture["body_text"]
    html = (
        "<html><body>"
        "<div class='event'>DOTA 2 | EPL Masters ZERO TENACITY ILBIRS "
        "1карта +14 0 0 0 0 1К Матч 1.38 2.80 2К 1.62 2.15</div>"
        "<div class='event'>DOTA 2 | Other League NEXT RADIANT NEXT DIRE "
        "1К 1.91 1.83 Матч 1.70 2.05</div>"
        "</body></html>"
    )
    page = _FakePage(html=html, body_text=body, url=fixture["url"])
    _patch_no_sleep(monkeypatch)
    monkeypatch.setattr(odds_parser, "_is_deeplink", lambda *_a, **_k: False)
    monkeypatch.setattr(
        odds_parser,
        "_load_site_render_payload_camoufox",
        lambda *_a, **_k: ("ok", "", html, body, body),
    )
    monkeypatch.setattr(odds_parser, "_candidate_match_urls_from_html", lambda *_a, **_k: [])

    result = odds_parser.parse_site_in_camoufox_page(
        page,
        site="winline",
        url=fixture["url"],
        team1=fixture["team1"],
        team2=fixture["team2"],
        mode="live",
        forced_map_num=fixture["forced_map_num"],
    )

    assert result.match_found is True
    assert result.odds == []
    assert result.map_num == 1
    assert result.market_kind is None
    assert "map" in f"{result.source} {result.details}".lower()
    assert fixture["expected_other_map_odds"] != result.odds


def test_bookmaker_infer_map_num_explicit_priority() -> None:
    inferred = cs._bookmaker_infer_map_num(
        {
            "game_map_number": 3,
            "radiant_series_wins": 0,
            "dire_series_wins": 0,
        },
        score_text="0:0",
    )
    assert inferred == 3


def test_bookmaker_infer_map_num_series_wins_fallback() -> None:
    inferred = cs._bookmaker_infer_map_num(
        {"radiant_series_wins": 1, "dire_series_wins": 1},
        score_text="",
    )
    assert inferred == 3


def test_bookmaker_infer_map_num_score_text_fallback() -> None:
    inferred = cs._bookmaker_infer_map_num({}, score_text="1:0")
    assert inferred == 2


def test_bookmaker_infer_map_num_invalid_returns_none() -> None:
    assert cs._bookmaker_infer_map_num({"game_map_number": 9}, score_text="") is None
    assert cs._bookmaker_infer_map_num({"radiant_series_wins": 9, "dire_series_wins": 9}, "") is None


def test_odds_mode_sites_are_winline_only() -> None:
    sites = cs._bookmaker_effective_sites_for_mode("odds")
    assert sites == ("winline",)


def test_presence_mode_sites_unchanged() -> None:
    sites = cs._bookmaker_effective_sites_for_mode("presence")
    assert "betboom" in sites
    assert "pari" in sites
    assert "winline" in sites


def _seed_prefetch(match_key: str, *, map_num: int, sites: Dict[str, Any], mode: str = "live") -> None:
    with cs.bookmaker_prefetch_condition:
        cs.bookmaker_prefetch_results[match_key] = {
            "status": "done",
            "finished_at": time.time(),
            "submitted_at": time.time(),
            "map_num": map_num,
            "mode": mode,
            "sites": sites,
        }


def test_gate_ready_only_for_strict_winline_current_map(monkeypatch) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    match_key = "https://cyberscore.live/matches/900001"
    _seed_prefetch(
        match_key,
        map_num=2,
        sites={
            "winline": {
                "match_found": True,
                "status": "ok",
                "odds": [1.52, 2.45],
                "match_odds": [1.30, 3.15],
                "market_closed": False,
                "market_kind": "current_map_winner",
                "map_num": 2,
                "p1_team": "team1",
                "p2_team": "team2",
                "source": "deeplink_map_market",
                "details": "Winter Bear DOGSENT 2К 1.52 2.45",
            }
        },
    )
    block, ready, reason = cs._bookmaker_format_odds_block(match_key)
    assert ready is True
    assert reason == "ok"
    assert "Winline" in block
    assert "1.52" in block and "2.45" in block
    assert "BetBoom" not in block
    assert "Pari" not in block
    assert "(п1/п2)" not in block


def test_gate_rejects_pari_or_betboom_payload(monkeypatch) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    match_key = "https://cyberscore.live/matches/900002"
    _seed_prefetch(
        match_key,
        map_num=2,
        sites={
            "pari": {
                "match_found": True,
                "odds": [1.40, 2.80],
                "market_closed": False,
                "market_kind": "current_map_winner",
                "map_num": 2,
                "p1_team": "team1",
                "p2_team": "team2",
                "source": "pari",
            },
            "betboom": {
                "match_found": True,
                "odds": [1.55, 2.40],
                "market_closed": False,
                "market_kind": "current_map_winner",
                "map_num": 2,
                "p1_team": "team1",
                "p2_team": "team2",
                "source": "betboom",
            },
        },
    )
    block, ready, reason = cs._bookmaker_format_odds_block(match_key)
    assert ready is False
    assert block == ""
    assert reason in {"no_numeric_odds", "no_strict_winline_current_map", "invalid_sites_payload"} or "winline" in reason


def test_gate_rejects_match_odds_fallback(monkeypatch) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    match_key = "https://cyberscore.live/matches/900003"
    _seed_prefetch(
        match_key,
        map_num=2,
        sites={
            "winline": {
                "match_found": True,
                "odds": [],
                "match_odds": [1.30, 3.15],
                "market_closed": False,
                "market_kind": "match_winner",
                "map_num": 2,
                "p1_team": "team1",
                "p2_team": "team2",
                "source": "winline_match_level_rejected",
            }
        },
    )
    block, ready, reason = cs._bookmaker_format_odds_block(match_key)
    assert ready is False
    assert block == ""
    assert "(п1/п2)" not in block


def test_gate_rejects_wrong_map_num(monkeypatch) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    match_key = "https://cyberscore.live/matches/900004"
    _seed_prefetch(
        match_key,
        map_num=2,
        sites={
            "winline": {
                "match_found": True,
                "odds": [1.52, 2.45],
                "market_closed": False,
                "market_kind": "current_map_winner",
                "map_num": 1,
                "p1_team": "team1",
                "p2_team": "team2",
                "source": "deeplink_map_market",
            }
        },
    )
    block, ready, reason = cs._bookmaker_format_odds_block(match_key)
    assert ready is False


def test_closed_market_is_temporarily_closed_wait(monkeypatch) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    match_key = "https://cyberscore.live/matches/900005"
    _seed_prefetch(
        match_key,
        map_num=2,
        sites={
            "winline": {
                "match_found": True,
                "odds": [],
                "match_odds": [1.30, 3.15],
                "market_closed": True,
                "market_kind": "current_map_winner",
                "map_num": 2,
                "p1_team": "team1",
                "p2_team": "team2",
                "source": "winline_map_market_closed",
            }
        },
    )
    state = cs._bookmaker_odds_market_state(match_key)
    assert state == "temporarily_closed_wait"
    block, ready, reason = cs._bookmaker_format_odds_block(match_key)
    assert ready is False
    assert block == ""


def test_closed_then_open_sends_once(monkeypatch) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    match_key = "https://cyberscore.live/matches/900006"
    pending: Dict[str, Any] = {}

    closed_sites = {
        "winline": {
            "match_found": True,
            "odds": [],
            "match_odds": [1.30, 3.15],
            "market_closed": True,
            "market_kind": "current_map_winner",
            "map_num": 2,
            "p1_team": "team1",
            "p2_team": "team2",
            "source": "winline_map_market_closed",
        }
    }
    open_sites = {
        "winline": {
            "match_found": True,
            "odds": [1.52, 2.45],
            "match_odds": [1.30, 3.15],
            "market_closed": False,
            "market_kind": "current_map_winner",
            "map_num": 2,
            "p1_team": "team1",
            "p2_team": "team2",
            "source": "deeplink_map_market",
        }
    }

    _seed_prefetch(match_key, map_num=2, sites=closed_sites)
    obs = _fresh_current_map_observation(match_key=match_key, map_num=2)
    decision1 = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        deadline_ts=time.time() + 60,
        map_num=2,
        current_map_observation=obs,
    )
    assert decision1["state"] == "temporarily_closed_wait"
    assert decision1["should_send"] is False
    assert pending.get(match_key) is not None
    assert "sent_at" not in (pending.get(match_key) or {})

    _seed_prefetch(match_key, map_num=2, sites=open_sites)
    decision2 = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        deadline_ts=time.time() + 60,
        map_num=2,
        current_map_observation=obs,
    )
    # Prepare/reserve only — never marks sent.
    assert decision2["state"] == "prepared"
    assert decision2["should_send"] is True
    assert decision2.get("token")
    assert str((pending.get(match_key) or {}).get("state")) == "prepared"
    assert "sent_at" not in (pending.get(match_key) or {})

    # Same open map re-prepare is an observer and must not receive owner context/token.
    decision3 = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        deadline_ts=time.time() + 60,
        map_num=2,
        current_map_observation=obs,
    )
    assert decision3["state"] == "reservation_inflight"
    assert decision3["should_send"] is False
    assert decision3.get("reason") == "reservation_inflight"
    assert decision3.get("token") is None
    assert decision3.get("reservation_context") is None

    committed = cs._bookmaker_commit_odds_delivery(
        match_key,
        reservation_context=decision2.get("reservation_context"),
        pending_state=pending,
    )
    assert committed is True
    assert str((pending.get(match_key) or {}).get("state")) == "sent"
    assert (pending.get(match_key) or {}).get("sent_at") is not None

    decision4 = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        deadline_ts=time.time() + 60,
        map_num=2,
        current_map_observation=obs,
    )
    assert decision4["should_send"] is False
    assert decision4.get("reason") == "already_sent"


def test_closed_until_deadline_terminal_skip(monkeypatch) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    match_key = "https://cyberscore.live/matches/900007"
    pending: Dict[str, Any] = {}
    _seed_prefetch(
        match_key,
        map_num=2,
        sites={
            "winline": {
                "match_found": True,
                "odds": [],
                "market_closed": True,
                "market_kind": "current_map_winner",
                "map_num": 2,
                "p1_team": "team1",
                "p2_team": "team2",
                "source": "winline_map_market_closed",
            }
        },
    )
    pending_before = dict(pending)
    decision = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        deadline_ts=time.time() - 1,
        map_num=2,
        current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=2),
    )
    assert decision["state"] == "terminal_skip"
    assert decision["should_send"] is False
    # Immutable terminal gate: no create/replace of pending; remains tokenless.
    assert pending == pending_before
    entry = pending.get(match_key) or {}
    assert entry.get("token") in (None, "")
    assert entry.get("state") != "prepared"


def test_map_change_cancels_pending_closed_wait(monkeypatch) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    match_key = "https://cyberscore.live/matches/900008"
    pending: Dict[str, Any] = {
        match_key: {"map_num": 2, "state": "temporarily_closed_wait", "created_at": time.time()}
    }
    _seed_prefetch(
        match_key,
        map_num=3,
        sites={
            "winline": {
                "match_found": True,
                "odds": [1.52, 2.45],
                "market_closed": False,
                "market_kind": "current_map_winner",
                "map_num": 3,
                "p1_team": "team1",
                "p2_team": "team2",
                "source": "deeplink_map_market",
            }
        },
    )
    decision = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        deadline_ts=time.time() + 60,
        map_num=3,
        current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=3),
    )
    # Map changed vs pending map 2 -> cancel wait; open odds for map 3 prepare fresh
    assert decision["state"] in {"prepared", "open_valid_odds", "terminal_skip"}
    if decision["state"] == "prepared":
        assert decision["should_send"] is True
        assert int((pending.get(match_key) or {}).get("map_num") or 0) == 3
        assert str((pending.get(match_key) or {}).get("state")) == "prepared"
    if decision["state"] == "terminal_skip":
        assert match_key not in pending


def test_winline_proxy_candidates_exclude_ru_and_use_live_pool(monkeypatch) -> None:
    live_pool = [
        {"url": "http://user:pass@de1.example:1000", "country": "DE"},
        {"url": "http://user:pass@de2.example:1001", "country": "DE"},
        {"url": "http://user:pass@de3.example:1002", "country": "DE"},
        {"url": "http://user:pass@de4.example:1003", "country": "DE"},
        {"url": "http://user:pass@de5.example:1004", "country": "DE"},
        {"url": "http://user:pass@us1.example:1005", "country": "US"},
        {"url": "http://user:pass@ru1.example:1006", "country": "RU"},
    ]
    monkeypatch.setattr(cs, "_bookmaker_live_proxy_pool", lambda: live_pool)
    candidates = cs._bookmaker_winline_proxy_candidates()
    countries = [c.get("country") for c in candidates]
    assert "RU" not in countries
    assert countries.count("DE") == 5
    assert countries.count("US") == 1
    # never expose raw credentials in repr/str helpers used by logs
    safe = cs._bookmaker_proxy_safe_label(candidates[0])
    assert "pass" not in safe
    assert "user" not in safe


def test_shared_camoufox_singleton_across_sequential_winline_jobs(monkeypatch) -> None:
    launches: List[int] = []
    active = {"count": 0, "max": 0}

    class _FakeBrowser:
        def close(self) -> None:
            active["count"] = max(0, active["count"] - 1)

    class _FakeCM:
        def __enter__(self):
            launches.append(1)
            active["count"] += 1
            active["max"] = max(active["max"], active["count"])
            return _FakeBrowser()

        def __exit__(self, *exc):
            active["count"] = max(0, active["count"] - 1)
            return False

    class _FakeCamoufox:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def __enter__(self):
            return _FakeCM().__enter__()

        def __exit__(self, *exc):
            return False

    # Replace module-level camoufox factory used by shared session
    monkeypatch.setattr(cs, "CAMOUFOX_AVAILABLE", True)
    monkeypatch.setattr(cs, "camoufox", SimpleNamespace(Camoufox=_FakeCamoufox))
    monkeypatch.setattr(cs, "_cyberscore_camoufox_proxy_kwargs", lambda: {})
    monkeypatch.setattr(cs, "_note_proxy_success", lambda *_a, **_k: None)

    session = cs._SharedCamoufoxSession()
    seen_ids: List[int] = []

    def _job(browser):
        seen_ids.append(id(browser))
        return "ok"

    assert session.submit("w1", _job, timeout=5) == "ok"
    assert session.submit("w2", _job, timeout=5) == "ok"
    assert session.submit("w3", _job, timeout=5) == "ok"
    assert len(launches) == 1
    assert len(set(seen_ids)) == 1
    assert active["max"] == 1
    session.close()


def test_shared_camoufox_recovery_keeps_max_active_browsers_one(monkeypatch) -> None:
    launches: List[int] = []
    active = {"count": 0, "max": 0}

    class _FakeBrowser:
        def close(self) -> None:
            active["count"] = max(0, active["count"] - 1)

    class _FakeCamoufox:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def __enter__(self):
            launches.append(1)
            active["count"] += 1
            active["max"] = max(active["max"], active["count"])
            return _FakeBrowser()

        def __exit__(self, *exc):
            active["count"] = max(0, active["count"] - 1)
            return False

    monkeypatch.setattr(cs, "CAMOUFOX_AVAILABLE", True)
    monkeypatch.setattr(cs, "camoufox", SimpleNamespace(Camoufox=_FakeCamoufox))
    monkeypatch.setattr(cs, "_cyberscore_camoufox_proxy_kwargs", lambda: {})
    monkeypatch.setattr(cs, "_note_proxy_success", lambda *_a, **_k: None)

    session = cs._SharedCamoufoxSession()

    def _fail(_browser):
        raise RuntimeError("boom")

    def _ok(_browser):
        return "recovered"

    with pytest.raises(RuntimeError):
        session.submit("fail", _fail, timeout=5, reset_on_error=True)
    # allow reset path to run
    time.sleep(0.05)
    assert session.submit("ok", _ok, timeout=5) == "recovered"
    assert active["max"] == 1
    assert len(launches) >= 2
    session.close()


def test_feed_href_winline_emits_strict_provenance(monkeypatch) -> None:
    """Production non-deeplink href_opened path must emit market provenance for gate."""
    _patch_no_sleep(monkeypatch)
    fixture = _load_fixture("winline_current_map_winner_nk.json")
    body = fixture["body_text"]
    html = f"<html><body>{body}</body></html>"
    page = _FakePage(html=html, body_text=body, url="https://winline.ru/stavki/sport/kibersport")

    monkeypatch.setattr(odds_parser, "_is_deeplink", lambda site, url: False)
    monkeypatch.setattr(
        odds_parser,
        "_load_site_render_payload_camoufox",
        lambda *a, **k: ("ok", "", html, body, body),
    )
    monkeypatch.setattr(
        odds_parser,
        "_candidate_match_urls_from_html",
        lambda *a, **k: ["https://winline.ru/stavki/event/15356534"],
    )
    monkeypatch.setattr(
        odds_parser,
        "_camoufox_find_match_by_urls",
        lambda *a, **k: "https://winline.ru/stavki/event/15356534",
    )
    monkeypatch.setattr(
        odds_parser,
        "_parse_map_market_on_current_camoufox_page",
        lambda *a, **k: ([1.52, 2.45], body),
    )

    result = odds_parser.parse_site_in_camoufox_page(
        page,
        site="winline",
        url="https://winline.ru/stavki/sport/kibersport",
        team1=fixture["team1"],
        team2=fixture["team2"],
        mode="live",
        forced_map_num=2,
    )
    assert result.match_found is True
    assert result.source == "feed_href_map_market"
    assert list(result.odds) == [1.52, 2.45]
    assert result.market_kind == "current_map_winner"
    assert result.map_num == 2
    assert result.p1_team == "team1"
    assert result.p2_team == "team2"

    # Gate accepts exact-map strict provenance from this production path.
    match_key = "https://cyberscore.live/matches/feed-href-9001"
    sites = {
        "winline": {
            "match_found": result.match_found,
            "status": result.status,
            "odds": list(result.odds),
            "match_odds": list(result.match_odds or []),
            "market_closed": result.market_closed,
            "market_kind": result.market_kind,
            "map_num": result.map_num,
            "p1_team": result.p1_team,
            "p2_team": result.p2_team,
            "source": result.source,
            "details": result.details,
        }
    }
    _seed_prefetch(match_key, map_num=2, sites=sites)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    block, ready, reason = cs._bookmaker_format_odds_block(match_key)
    assert ready is True
    assert reason == "ok"
    assert "1.52" in block and "2.45" in block


def test_prepare_message_uses_odds_state_machine_production_path(monkeypatch) -> None:
    """_bookmaker_prepare_message_for_delivery must honor closed/open/deadline/map-change."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True)

    match_key = "https://cyberscore.live/matches/prod-state-9002"
    with cs.bookmaker_odds_delivery_pending_lock:
        cs.bookmaker_odds_delivery_pending.clear()

    refresh_calls: List[str] = []

    def _fake_refresh(key: str):
        refresh_calls.append(key)
        return cs._bookmaker_prefetch_lookup(key, wait_seconds=0.0)

    monkeypatch.setattr(cs, "_bookmaker_refresh_snapshot_via_shared_camoufox", _fake_refresh)
    # Guard: subprocess path must not be used for odds when shared is available.
    monkeypatch.setattr(
        cs,
        "_bookmaker_prefetch_fetch_subprocess",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("subprocess must not run for odds")),
    )

    closed_sites = {
        "winline": {
            "match_found": True,
            "odds": [],
            "match_odds": [1.30, 3.15],
            "market_closed": True,
            "market_kind": "current_map_winner",
            "map_num": 2,
            "p1_team": "team1",
            "p2_team": "team2",
            "source": "winline_map_market_closed",
        }
    }
    open_sites = {
        "winline": {
            "match_found": True,
            "odds": [1.52, 2.45],
            "match_odds": [1.30, 3.15],
            "market_closed": False,
            "market_kind": "current_map_winner",
            "map_num": 2,
            "p1_team": "team1",
            "p2_team": "team2",
            "source": "deeplink_map_market",
        }
    }
    base_msg = "signal body\n\nБукмекеры: n/a"

    _seed_prefetch(match_key, map_num=2, sites=closed_sites)
    obs = _fresh_current_map_observation(match_key=match_key, map_num=2)
    msg, ready, reason, reservation = cs._bookmaker_prepare_message_for_delivery(
        match_key, base_msg, current_map_observation=obs, map_num=2
    )
    assert ready is False
    assert reason == "temporarily_closed_wait"
    assert match_key in refresh_calls
    with cs.bookmaker_odds_delivery_pending_lock:
        pending_closed = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending_closed.get("state") == "temporarily_closed_wait"
    assert "sent_at" not in pending_closed

    _seed_prefetch(match_key, map_num=2, sites=open_sites)
    msg, ready, reason, reservation = cs._bookmaker_prepare_message_for_delivery(
        match_key, base_msg, current_map_observation=obs, map_num=2
    )
    assert ready is True
    assert reason == "ok"
    assert "1.52" in msg and "2.45" in msg
    assert isinstance(reservation, dict) and reservation.get("token")
    with cs.bookmaker_odds_delivery_pending_lock:
        pending_open = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending_open.get("state") == "prepared"
    assert "sent_at" not in pending_open

    # second open before commit is an observer and receives no ownership token
    msg2, ready2, reason2, reservation2 = cs._bookmaker_prepare_message_for_delivery(
        match_key, base_msg, current_map_observation=obs, map_num=2
    )
    assert ready2 is False
    assert reason2 == "reservation_inflight"
    assert reservation2 is None

    # commit after confirmed delivery path would mark sent
    assert cs._bookmaker_commit_odds_delivery(match_key, reservation_context=reservation) is True
    msg2b, ready2b, reason2b, _res2b = cs._bookmaker_prepare_message_for_delivery(
        match_key, base_msg, current_map_observation=obs, map_num=2
    )
    assert ready2b is False
    assert reason2b == "already_sent"

    # map change cleans pending and re-evaluates
    open_map3 = {
        "winline": {
            **open_sites["winline"],
            "map_num": 3,
            "odds": [1.61, 2.30],
        }
    }
    _seed_prefetch(match_key, map_num=3, sites=open_map3)
    obs3 = _fresh_current_map_observation(match_key=match_key, map_num=3)
    msg3, ready3, reason3, reservation3 = cs._bookmaker_prepare_message_for_delivery(
        match_key, base_msg, map_num=3, current_map_observation=obs3
    )
    assert ready3 is True
    assert reason3 == "ok"
    assert "1.61" in msg3
    assert reservation3 and reservation3.get("map_num") == 3

    # stale token from map 2 cannot commit over map 3 reservation
    assert cs._bookmaker_commit_odds_delivery(match_key, reservation_context=reservation) is False

    # deadline terminal while closed
    with cs.bookmaker_odds_delivery_pending_lock:
        cs.bookmaker_odds_delivery_pending.clear()
    _seed_prefetch(match_key, map_num=2, sites=closed_sites)
    msg4, ready4, reason4, _res4 = cs._bookmaker_prepare_message_for_delivery(
        match_key,
        base_msg,
        deadline_ts=time.time() - 5,
        map_num=2,
        current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=2),
    )
    assert ready4 is False
    assert "terminal_skip" in reason4


def test_shared_camoufox_uses_direct_then_de_us_fallback_policy(monkeypatch) -> None:
    """Shared Winline starts direct and retains configured DE/US fallbacks."""
    launches: List[dict] = []

    class _FakeBrowser:
        def close(self) -> None:
            return None

    class _FakeCamoufox:
        def __init__(self, **kwargs):
            launches.append(dict(kwargs))
            self.kwargs = kwargs

        def __enter__(self):
            return _FakeBrowser()

        def __exit__(self, *exc):
            return False

    de_items = [
        {"url": f"http://user:pass@de{i}.example:8000", "country": "DE"} for i in range(5)
    ]
    # No US in inventory — policy must still accept 5 DE.
    monkeypatch.setattr(cs, "CAMOUFOX_AVAILABLE", True)
    monkeypatch.setattr(cs, "camoufox", SimpleNamespace(Camoufox=_FakeCamoufox))
    monkeypatch.setattr(cs, "_bookmaker_winline_proxy_candidates", lambda: list(de_items))
    monkeypatch.setattr(cs, "_cyberscore_camoufox_proxy_kwargs", lambda: {"proxy": {"server": "http://should-not-use"}})
    monkeypatch.setattr(cs, "_note_proxy_success", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_bookmaker_shared_proxy_candidates", [], raising=False)
    monkeypatch.setattr(cs, "_bookmaker_shared_proxy_index", 0, raising=False)

    # Reset module-level rotation state
    cs._bookmaker_shared_proxy_candidates = []
    cs._bookmaker_shared_proxy_index = 0

    cands = cs._bookmaker_winline_proxy_candidates()
    assert len(cands) == 5
    assert all(c.get("country") == "DE" for c in cands)
    assert not any(c.get("country") == "RU" for c in cands)

    direct_session = cs._SharedCamoufoxSession()
    assert direct_session.submit("direct-job", lambda _b: "ok", timeout=5) == "ok"
    assert launches, "browser must launch"
    assert launches[0].get("proxy") is None
    direct_session.close()

    # A recovery rotation advances from direct to the first configured proxy.
    cs._bookmaker_shared_proxy_index = 1
    proxy_session = cs._SharedCamoufoxSession()
    assert proxy_session.submit("proxy-job", lambda _b: "ok", timeout=5) == "ok"
    proxy = launches[-1].get("proxy")
    assert isinstance(proxy, dict)
    server = str(proxy.get("server") or "")
    assert "de0.example" in server or "de" in server
    assert "should-not-use" not in server
    # Safe label never leaks credentials
    label = cs._bookmaker_proxy_safe_label(de_items[0])
    assert "pass" not in label
    assert "user" not in label or "user@" not in label
    proxy_session.close()


def test_valid_fallback_page_restores_shared_winline_route_to_direct(monkeypatch) -> None:
    """A geo-incomplete fallback must not become the permanent shared route."""
    resets: List[bool] = []
    monkeypatch.setattr(
        cs,
        "_bookmaker_shared_proxy_candidates",
        [
            {"url": "", "country": "DIRECT"},
            {"url": "http://proxy.example:8000", "country": "US"},
        ],
        raising=False,
    )
    monkeypatch.setattr(cs, "_bookmaker_shared_proxy_index", 1, raising=False)
    monkeypatch.setattr(
        cs,
        "_shared_camoufox_session",
        SimpleNamespace(request_reset=lambda: resets.append(True)),
        raising=False,
    )

    restored = cs._bookmaker_restore_shared_camoufox_direct_route(
        reason="winline_valid_page"
    )

    assert restored is True
    assert cs._bookmaker_shared_proxy_index == 0
    assert resets == [True]


def test_expanded_selected_event_panel_allows_multiple_markets_without_neighbor_leakage():
    html = """
    <html><body>
      <ww-pinned-card>
        <div>1w Essence</div>
        <div>2 карта</div>
        <div>TEAM FALCONS</div>
        <div>VICI GAMING</div>
      </ww-pinned-card>
      <ww-feature-event-live-center-dsk>
        <section class="event-live-center">
          <header> DOTA 2, 1w Essence TEAM FALCONS 1 : 0 VICI GAMING </header>
          <div>Популярные на матч Победитель 1.36 2.90</div>
          <div>Популярные на карту Победитель 2 карта 1.37 2.93</div>
          <div>Тотал убийств 2 карта 1.85 1.85 М 52.5 Б</div>
        </section>
      </ww-feature-event-live-center-dsk>
      <div class="event-card">
        DOTA 2 | Other League WRONG ONE WRONG TWO Матч 1.10 7.00
        2 карта 1.10 7.00
      </div>
    </body></html>
    """

    context = odds_parser._winline_matched_card_context(
        "",
        "Team Falcons",
        "Vici Gaming",
        html=html,
        map_num=2,
    )
    result = odds_parser._extract_winline_current_map_winner(
        context or "",
        "Team Falcons",
        "Vici Gaming",
        forced_map_num=2,
    )

    assert context is not None
    assert "1.37 2.93" in context
    assert "WRONG ONE" not in context
    assert result.odds == [pytest.approx(1.37), pytest.approx(2.93)]


def test_structured_winline_winner_rejects_neighbor_handicap_prices():
    html = """
    <html><body>
      <article class="event-card">
        <header>DOTA 2 | 1w Essence TEAM LIQUID MOUZ Матч 1.01 15.00</header>
        <div class="card__body card__body--second">
          <div class="period-name">1 карта</div>
          <div class="card__coeffs">
            <ww-feature-event-market-dsk class="card__market">
              <div class="coefficient-button coefficient-button_generic2
                          coefficient-button--is-blank">-</div>
              <div class="coefficient-button coefficient-button_generic2
                          coefficient-button--is-blank">-</div>
            </ww-feature-event-market-dsk>
            <ww-feature-event-market-dsk class="card__market">
              <div class="coefficient-button coefficient-button_handicap2">1.90</div>
              <div class="coefficient-button coefficient-button_handicap2">1.80</div>
            </ww-feature-event-market-dsk>
            <ww-feature-event-market-dsk class="card__market">
              <div class="coefficient-button coefficient-button_total2">1.85</div>
              <div class="coefficient-button coefficient-button_total2">1.85</div>
            </ww-feature-event-market-dsk>
          </div>
        </div>
      </article>
    </body></html>
    """

    result = odds_parser._extract_winline_current_map_winner(
        "TEAM LIQUID MOUZ 1 карта 1.90 1.80 1.85 1.85",
        "Team Liquid",
        "MOUZ",
        forced_map_num=1,
        html=html,
    )

    assert result.odds == []
    assert result.market_closed is True
    assert result.market_kind == "current_map_winner"


def test_structured_winline_winner_uses_generic_buttons_only():
    html = """
    <html><body>
      <article class="event-card">
        <header>DOTA 2 | 1w Essence TEAM LIQUID MOUZ Матч 1.01 15.00</header>
        <div class="card__body card__body--second">
          <div class="period-name">1 карта</div>
          <div class="card__coeffs">
            <ww-feature-event-market-dsk class="card__market">
              <div class="coefficient-button coefficient-button_generic2">1.16</div>
              <div class="coefficient-button coefficient-button_generic2">4.79</div>
            </ww-feature-event-market-dsk>
            <ww-feature-event-market-dsk class="card__market">
              <div class="coefficient-button coefficient-button_handicap2">1.90</div>
              <div class="coefficient-button coefficient-button_handicap2">1.80</div>
            </ww-feature-event-market-dsk>
          </div>
        </div>
      </article>
    </body></html>
    """

    result = odds_parser._extract_winline_current_map_winner(
        "TEAM LIQUID MOUZ 1 карта 1.16 4.79 1.90 1.80",
        "Team Liquid",
        "MOUZ",
        forced_map_num=1,
        html=html,
    )

    assert result.odds == [pytest.approx(1.16), pytest.approx(4.79)]
    assert result.market_closed is False
    assert result.market_kind == "current_map_winner"


def test_html_snapshot_never_falls_back_to_flat_neighbor_prices():
    html = """
    <html><body>
      <article class="event-card">
        <header>DOTA 2 | 1w Essence TEAM LIQUID MOUZ</header>
        <div>1 карта 1.90 1.80</div>
      </article>
    </body></html>
    """

    result = odds_parser._extract_winline_current_map_winner(
        "TEAM LIQUID MOUZ 1 карта 1.90 1.80",
        "Team Liquid",
        "MOUZ",
        forced_map_num=1,
        html=html,
    )

    assert result.odds == []
    assert result.reason == "map"
    assert result.market_kind == "current_map_winner"


def test_structured_expanded_panel_uses_map_winner_not_total():
    html = """
    <html><body>
      <ww-feature-event-live-center-dsk>
        <section class="event-live-center">
          <header>DOTA 2, 1w Essence TEAM LIQUID 1 : 0 MOUZ 2 карта</header>
          <div class="fast-bets__wrapper">
            <div class="fast-bets__title">Популярные на матч</div>
            <div class="bet-line">
              <span class="bet-line__market-name">Победитель</span>
              <span class="bet-line__period"></span>
              <div class="bet-line__coefs-wrapper">
                <div class="odd-btn">1.20</div><div class="odd-btn">4.01</div>
              </div>
            </div>
          </div>
          <div class="fast-bets__wrapper">
            <div class="fast-bets__title">Популярные на карту</div>
            <div class="bet-line">
              <span class="bet-line__market-name">Победитель</span>
              <span class="bet-line__period">2 карта</span>
              <div class="bet-line__coefs-wrapper">
                <div class="odd-btn">1.21</div><div class="odd-btn">4.06</div>
              </div>
            </div>
            <div class="bet-line">
              <span class="bet-line__market-name">Тотал убийств</span>
              <span class="bet-line__period">2 карта</span>
              <div class="bet-line__coefs-wrapper">
                <div class="odd-btn">1.85</div><div class="odd-btn">1.85</div>
              </div>
            </div>
          </div>
        </section>
      </ww-feature-event-live-center-dsk>
    </body></html>
    """

    result = odds_parser._extract_winline_current_map_winner(
        "TEAM LIQUID MOUZ Победитель 2 карта 1.21 4.06 "
        "Тотал убийств 2 карта 1.85 1.85",
        "Team Liquid",
        "MOUZ",
        forced_map_num=2,
        html=html,
    )

    assert result.odds == [pytest.approx(1.21), pytest.approx(4.06)]
    assert result.market_closed is False
    assert result.market_kind == "current_map_winner"


def test_odds_delivery_refresh_uses_shared_camoufox_not_subprocess(monkeypatch) -> None:
    """Production odds refresh must use shared Camoufox job, never spawn Camoufox subprocess."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_USE_SUBPROCESS", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)

    match_key = "https://cyberscore.live/matches/shared-refresh-9003"
    open_sites = {
        "winline": {
            "match_found": True,
            "odds": [1.52, 2.45],
            "match_odds": [1.30, 3.15],
            "market_closed": False,
            "market_kind": "current_map_winner",
            "map_num": 2,
            "p1_team": "team1",
            "p2_team": "team2",
            "source": "deeplink_map_market",
        }
    }
    _seed_prefetch(match_key, map_num=2, sites=open_sites)
    with cs.bookmaker_odds_delivery_pending_lock:
        cs.bookmaker_odds_delivery_pending.pop(match_key, None)

    shared_calls: List[str] = []
    subprocess_calls: List[str] = []

    def _fake_shared_job(label, callback, **kwargs):
        shared_calls.append(str(label))
        return {
            "winline": dict(open_sites["winline"]),
        }

    def _fake_subprocess(*a, **k):
        subprocess_calls.append("subprocess")
        raise AssertionError("Camoufox subprocess must not be used for odds refresh")

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", _fake_shared_job)
    monkeypatch.setattr(cs, "_bookmaker_prefetch_fetch_subprocess", _fake_subprocess)

    # Direct shared refresh path
    refreshed = cs._bookmaker_refresh_snapshot_via_shared_camoufox(match_key)
    assert isinstance(refreshed, dict)
    assert shared_calls, "shared Camoufox job must be used"
    assert not subprocess_calls

    # Production prepare routes through shared refresh, not subprocess.
    shared_calls.clear()
    msg, ready, reason, reservation = cs._bookmaker_prepare_message_for_delivery(
        match_key,
        "body\n\nБукмекеры: n/a",
        map_num=2,
        current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=2),
    )
    assert ready is True
    assert reason == "ok"
    assert isinstance(reservation, dict) and reservation.get("token")
    assert shared_calls
    assert not subprocess_calls
    with cs.bookmaker_odds_delivery_pending_lock:
        pending = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending.get("state") == "prepared"
    assert "sent_at" not in pending

    # Legacy name must also route to shared for odds/camoufox.
    shared_calls.clear()
    out = cs._bookmaker_refresh_snapshot_via_subprocess(match_key)
    assert isinstance(out, dict)
    assert shared_calls
    assert not subprocess_calls


def _open_winline_sites(map_num: int = 2, odds: Optional[List[float]] = None) -> Dict[str, Any]:
    return {
        "winline": {
            "match_found": True,
            "odds": list(odds or [1.52, 2.45]),
            "match_odds": [1.30, 3.15],
            "market_closed": False,
            "market_kind": "current_map_winner",
            "map_num": map_num,
            "p1_team": "team1",
            "p2_team": "team2",
            "source": "deeplink_map_market",
        }
    }


def _clear_delivery_state(match_key: str) -> None:
    with cs.bookmaker_odds_delivery_pending_lock:
        cs.bookmaker_odds_delivery_pending.pop(match_key, None)
    with cs._SENT_SIGNAL_FP_LOCK:
        cs._SENT_SIGNAL_DEDUP_KEYS.clear()
        cs._SIGNAL_DEDUP_FINGERPRINTS.clear()


def test_deliver_and_persist_commits_sent_only_after_confirmed_send(monkeypatch, tmp_path) -> None:
    """Caller-sequence regression: prepare once, send once, commit only after confirmed delivery."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True)
    monkeypatch.setattr(cs, "DLTV_RATING_IN_SIGNAL", False, raising=False)
    monkeypatch.setattr(cs, "SIGNAL_SEND_ADMIN_ONLY", True, raising=False)
    monkeypatch.setattr(cs, "SENT_SIGNAL_JOURNAL_PATH", str(tmp_path / "journal.jsonl"), raising=False)
    monkeypatch.setattr(cs, "SENT_SIGNAL_FINGERPRINT_PATH", str(tmp_path / "fps.json"), raising=False)

    match_key = "https://cyberscore.live/matches/delivery-seq-9100"
    _clear_delivery_state(match_key)
    _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2))

    prepare_calls: List[str] = []
    send_calls: List[str] = []
    add_url_calls: List[str] = []
    real_prepare = cs._bookmaker_prepare_message_for_delivery

    def _counting_prepare(key, message, **kwargs):
        prepare_calls.append(key)
        return real_prepare(key, message, **kwargs)

    def _fake_send(message, **kwargs):
        send_calls.append(str(message))
        # Before send returns, reservation must still be prepared (not sent).
        with cs.bookmaker_odds_delivery_pending_lock:
            pending = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
        assert pending.get("state") == "prepared"
        assert "sent_at" not in pending
        return True

    monkeypatch.setattr(cs, "_bookmaker_prepare_message_for_delivery", _counting_prepare)
    monkeypatch.setattr(cs, "send_message", _fake_send)
    monkeypatch.setattr(cs, "add_url", lambda url, **_k: add_url_calls.append(url))
    monkeypatch.setattr(
        cs,
        "_bookmaker_refresh_snapshot_via_shared_camoufox",
        lambda key: cs._bookmaker_prefetch_lookup(key, wait_seconds=0.0),
    )

    base_msg = "СТАВКА НА team1 x1\n\nБукмекеры: n/a"
    obs = _fresh_current_map_observation(match_key=match_key, map_num=2)
    ok = cs._deliver_and_persist_signal(
        match_key,
        base_msg,
        add_url_reason="unit_delivery_commit",
        add_url_details={"status": "ok"},
        current_map_observation=obs,
        map_num=2,
    )
    assert ok is True
    assert len(prepare_calls) == 1
    assert len(send_calls) == 1
    assert "1.52" in send_calls[0] and "2.45" in send_calls[0]
    assert add_url_calls == [match_key]
    with cs.bookmaker_odds_delivery_pending_lock:
        pending = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending.get("state") == "sent"
    assert pending.get("sent_at") is not None

    # Second delivery attempt: prepare once more, already_sent blocks send.
    prepare_calls.clear()
    send_calls.clear()
    ok2 = cs._deliver_and_persist_signal(
        match_key,
        base_msg,
        add_url_reason="unit_delivery_commit_retry",
        add_url_details={"status": "ok"},
        current_map_observation=obs,
        map_num=2,
    )
    assert ok2 is False
    assert len(prepare_calls) == 1
    assert send_calls == []


def test_concurrent_duplicate_cannot_rollback_owner_bookmaker_reservation(monkeypatch, tmp_path) -> None:
    """An inflight observer must not receive or roll back the sender's lease token."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True)
    monkeypatch.setattr(cs, "DLTV_RATING_IN_SIGNAL", False, raising=False)
    monkeypatch.setattr(cs, "SIGNAL_SEND_ADMIN_ONLY", True, raising=False)
    monkeypatch.setattr(cs, "SENT_SIGNAL_JOURNAL_PATH", str(tmp_path / "journal.jsonl"), raising=False)
    monkeypatch.setattr(cs, "SENT_SIGNAL_FINGERPRINT_PATH", str(tmp_path / "fps.json"), raising=False)

    match_key = "https://cyberscore.live/matches/concurrent-owner-9105"
    _clear_delivery_state(match_key)
    cs._signal_fingerprint_register(match_key, "team1", "team2", 1, 0)
    _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2))
    monkeypatch.setattr(
        cs,
        "_bookmaker_refresh_snapshot_via_shared_camoufox",
        lambda key: cs._bookmaker_prefetch_lookup(key, wait_seconds=0.0),
    )

    owner_in_send = threading.Event()
    release_owner = threading.Event()
    send_calls: List[str] = []
    results: Dict[str, Any] = {}

    def _blocking_send(message, **_kwargs):
        send_calls.append(str(message))
        owner_in_send.set()
        assert release_owner.wait(timeout=5.0)
        return True

    def _deliver(label: str) -> None:
        try:
            results[label] = cs._deliver_and_persist_signal(
                match_key,
                "СТАВКА НА team1 x1\n\nБукмекеры: n/a",
                add_url_reason=f"unit_concurrent_{label}",
                add_url_details={"status": "ok"},
                current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=2),
                map_num=2,
            )
        except BaseException as exc:
            results[label] = exc

    monkeypatch.setattr(cs, "send_message", _blocking_send)
    monkeypatch.setattr(cs, "add_url", lambda *_a, **_k: None)

    owner = threading.Thread(target=_deliver, args=("owner",), daemon=True)
    duplicate = threading.Thread(target=_deliver, args=("duplicate",), daemon=True)
    owner.start()
    assert owner_in_send.wait(timeout=5.0)
    with cs.bookmaker_odds_delivery_pending_lock:
        owner_pending = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    owner_token = owner_pending.get("token")
    assert owner_pending.get("state") == "prepared"
    assert owner_token

    duplicate.start()
    duplicate.join(timeout=5.0)
    assert not duplicate.is_alive()
    assert not isinstance(results.get("duplicate"), BaseException)
    assert len(send_calls) == 1
    with cs.bookmaker_odds_delivery_pending_lock:
        pending_while_owner_blocked = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending_while_owner_blocked.get("state") == "prepared"
    assert pending_while_owner_blocked.get("token") == owner_token
    assert "sent_at" not in pending_while_owner_blocked

    release_owner.set()
    owner.join(timeout=5.0)
    assert not owner.is_alive()
    assert results.get("owner") is True
    with cs.bookmaker_odds_delivery_pending_lock:
        pending_sent = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending_sent.get("state") == "sent"
    assert pending_sent.get("token") == owner_token
    assert pending_sent.get("sent_at") is not None


def test_deliver_and_persist_hard_fail_rolls_back_and_allows_retry(monkeypatch, tmp_path) -> None:
    """Hard Telegram failure must not commit sent; a later confirmed send may commit once."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True)
    monkeypatch.setattr(cs, "DLTV_RATING_IN_SIGNAL", False, raising=False)
    monkeypatch.setattr(cs, "SIGNAL_SEND_ADMIN_ONLY", True, raising=False)
    monkeypatch.setattr(cs, "SENT_SIGNAL_JOURNAL_PATH", str(tmp_path / "journal.jsonl"), raising=False)
    monkeypatch.setattr(cs, "SENT_SIGNAL_FINGERPRINT_PATH", str(tmp_path / "fps.json"), raising=False)

    match_key = "https://cyberscore.live/matches/delivery-retry-9101"
    _clear_delivery_state(match_key)
    _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2, [1.61, 2.30]))

    send_calls: List[str] = []
    fail_once = {"n": 0}

    def _flaky_send(message, **kwargs):
        send_calls.append(str(message))
        fail_once["n"] += 1
        if fail_once["n"] == 1:
            raise cs.TelegramSendError("telegram hard fail", delivery_uncertain=False)
        return True

    monkeypatch.setattr(cs, "send_message", _flaky_send)
    monkeypatch.setattr(cs, "add_url", lambda *_a, **_k: None)
    monkeypatch.setattr(
        cs,
        "_bookmaker_refresh_snapshot_via_shared_camoufox",
        lambda key: cs._bookmaker_prefetch_lookup(key, wait_seconds=0.0),
    )

    base_msg = "СТАВКА НА team1 x1\n\nБукмекеры: n/a"
    with pytest.raises(cs.TelegramSendError):
        cs._deliver_and_persist_signal(
            match_key,
            base_msg,
            add_url_reason="unit_delivery_fail",
            add_url_details={"status": "ok"},
            current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=2),
            map_num=2,
        )
    with cs.bookmaker_odds_delivery_pending_lock:
        pending_after_fail = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending_after_fail.get("state") != "sent"
    assert "sent_at" not in pending_after_fail

    ok = cs._deliver_and_persist_signal(
        match_key,
        base_msg,
        add_url_reason="unit_delivery_retry_ok",
        add_url_details={"status": "ok"},
        current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=2),
        map_num=2,
    )
    assert ok is True
    assert len(send_calls) == 2
    with cs.bookmaker_odds_delivery_pending_lock:
        pending_ok = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending_ok.get("state") == "sent"
    assert pending_ok.get("sent_at") is not None


def test_deliver_and_persist_uncertain_keeps_reservation_uncommitted(monkeypatch, tmp_path) -> None:
    """Uncertain delivery must not mark odds as sent (no false commit)."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True)
    monkeypatch.setattr(cs, "DLTV_RATING_IN_SIGNAL", False, raising=False)
    monkeypatch.setattr(cs, "SIGNAL_SEND_ADMIN_ONLY", True, raising=False)
    monkeypatch.setattr(
        cs,
        "UNCERTAIN_SIGNAL_DELIVERY_PATH",
        str(tmp_path / "uncertain.jsonl"),
        raising=False,
    )
    monkeypatch.setattr(
        cs,
        "UNCERTAIN_SIGNAL_DELIVERY_FALLBACK_PATH",
        str(tmp_path / "uncertain_fb.jsonl"),
        raising=False,
    )
    monkeypatch.setattr(cs, "SENT_SIGNAL_FINGERPRINT_PATH", str(tmp_path / "fps.json"), raising=False)

    match_key = "https://cyberscore.live/matches/delivery-uncertain-9102"
    _clear_delivery_state(match_key)
    _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2))

    monkeypatch.setattr(
        cs,
        "send_message",
        lambda *_a, **_k: (_ for _ in ()).throw(
            cs.TelegramSendError("read timeout", delivery_uncertain=True)
        ),
    )
    monkeypatch.setattr(cs, "add_url", lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("no add_url")))
    monkeypatch.setattr(
        cs,
        "_bookmaker_refresh_snapshot_via_shared_camoufox",
        lambda key: cs._bookmaker_prefetch_lookup(key, wait_seconds=0.0),
    )
    with cs.uncertain_delivery_urls_lock:
        cs.uncertain_delivery_urls_cache.clear()

    delivered = cs._deliver_and_persist_signal(
        match_key,
        "body\n\nБукмекеры: n/a",
        add_url_reason="unit_uncertain",
        add_url_details={"status": "ok"},
        current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=2),
        map_num=2,
    )
    assert delivered is False
    with cs.bookmaker_odds_delivery_pending_lock:
        pending = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending.get("state") == "prepared"
    assert "sent_at" not in pending


def test_minimal_odds_only_hands_explicit_reservation_without_second_prepare(monkeypatch, tmp_path) -> None:
    """Minimal odds-only preflight must hand ownership token into delivery (no naked skip)."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True)
    monkeypatch.setattr(cs, "DLTV_RATING_IN_SIGNAL", False, raising=False)
    monkeypatch.setattr(cs, "SIGNAL_SEND_ADMIN_ONLY", True, raising=False)
    monkeypatch.setattr(cs, "SENT_SIGNAL_JOURNAL_PATH", str(tmp_path / "journal.jsonl"), raising=False)
    monkeypatch.setattr(cs, "SENT_SIGNAL_FINGERPRINT_PATH", str(tmp_path / "fps.json"), raising=False)

    match_key = "https://cyberscore.live/matches/minimal-odds-9103"
    _clear_delivery_state(match_key)
    _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2))
    monkeypatch.setattr(
        cs,
        "_bookmaker_refresh_snapshot_via_shared_camoufox",
        lambda key: cs._bookmaker_prefetch_lookup(key, wait_seconds=0.0),
    )

    msg, ready, reason, reservation = cs._prepare_minimal_odds_only_message_for_delivery(
        match_key,
        "minimal body\n\nБукмекеры: n/a",
        map_num=2,
        current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=2),
    )
    assert ready is True
    assert reason == "ok"
    assert isinstance(reservation, dict) and reservation.get("token")
    with cs.bookmaker_odds_delivery_pending_lock:
        pending = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending.get("state") == "prepared"

    prepare_calls: List[str] = []
    real_prepare = cs._bookmaker_prepare_message_for_delivery

    def _count_prepare(key, message, **kwargs):
        prepare_calls.append(key)
        return real_prepare(key, message, **kwargs)

    send_calls: List[str] = []
    monkeypatch.setattr(cs, "_bookmaker_prepare_message_for_delivery", _count_prepare)
    monkeypatch.setattr(cs, "send_message", lambda message, **_k: send_calls.append(message) or True)
    monkeypatch.setattr(cs, "add_url", lambda *_a, **_k: None)

    ok = cs._deliver_and_persist_signal(
        match_key,
        msg,
        add_url_reason="minimal_odds_only_signal_sent_now",
        add_url_details={"status": "ok"},
        bookmaker_reservation_context=reservation,
        current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=2),
        map_num=2,
    )
    assert ok is True
    assert prepare_calls == []  # ownership token reused; no second prepare
    assert len(send_calls) == 1
    with cs.bookmaker_odds_delivery_pending_lock:
        pending_sent = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending_sent.get("state") == "sent"


def test_stale_minimal_handoff_cannot_send_or_mutate_owner_reservation(monkeypatch) -> None:
    match_key = "https://cyberscore.live/matches/minimal-stale-handoff-9107"
    _clear_delivery_state(match_key)
    with cs.bookmaker_odds_delivery_pending_lock:
        cs.bookmaker_odds_delivery_pending[match_key] = {
            "map_num": 2,
            "state": "prepared",
            "token": "current-owner-token",
            "created_at": time.time(),
            "updated_at": time.time(),
        }

    monkeypatch.setattr(
        cs,
        "_bookmaker_prepare_message_for_delivery",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("invalid handoff must not prepare")),
    )
    monkeypatch.setattr(
        cs,
        "send_message",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("invalid handoff must not send")),
    )

    delivered = cs._deliver_and_persist_signal(
        match_key,
        "minimal body\n\nБукмекеры: n/a",
        add_url_reason="unit_stale_handoff",
        bookmaker_reservation_context={
            "match_key": match_key,
            "map_num": 2,
            "token": "stale-observer-token",
        },
        current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=2),
        map_num=2,
    )
    assert delivered is False
    with cs.bookmaker_odds_delivery_pending_lock:
        pending = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending.get("state") == "prepared"
    assert pending.get("token") == "current-owner-token"


@pytest.mark.parametrize(
    ("camoufox_enabled", "camoufox_imported", "expected_reason"),
    [
        (False, True, "shared_camoufox_disabled"),
        (True, False, "shared_camoufox_import_unavailable"),
    ],
)
def test_odds_mode_shared_unavailable_rejects_valid_cached_odds_without_subprocess(
    monkeypatch,
    camoufox_enabled: bool,
    camoufox_imported: bool,
    expected_reason: str,
) -> None:
    """Unavailable shared refresh makes even strict cached odds gate-ineligible."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_USE_SUBPROCESS", True)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_ENABLED", camoufox_enabled)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", camoufox_imported)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)

    match_key = f"https://cyberscore.live/matches/shared-unavail-{int(camoufox_enabled)}-{int(camoufox_imported)}"
    original_message = "body\n\nБукмекеры: n/a"
    _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2, [1.52, 2.45]))
    with cs.bookmaker_odds_delivery_pending_lock:
        cs.bookmaker_odds_delivery_pending.pop(match_key, None)

    subprocess_calls: List[str] = []
    run_calls: List[str] = []
    legacy_calls: List[str] = []

    def _forbidden_subprocess(*_a, **_k):
        subprocess_calls.append("fetch_subprocess")
        raise AssertionError("odds mode must not call _bookmaker_prefetch_fetch_subprocess")

    def _forbidden_run(*_a, **_k):
        run_calls.append("subprocess.run")
        raise AssertionError("odds mode must not call subprocess.run")

    monkeypatch.setattr(cs, "_bookmaker_prefetch_fetch_subprocess", _forbidden_subprocess)
    monkeypatch.setattr(cs.subprocess, "run", _forbidden_run)
    monkeypatch.setattr(
        cs,
        "_bookmaker_refresh_cached_match_tabs_for_dispatch",
        lambda *_a, **_k: legacy_calls.append("legacy_selenium") or (_ for _ in ()).throw(
            AssertionError("odds mode must not call legacy cached-tab refresh")
        ),
    )

    msg, ready, reason, reservation = cs._bookmaker_prepare_message_for_delivery(
        match_key,
        original_message,
        map_num=2,
        current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=2),
    )
    assert msg == original_message
    assert ready is False
    assert reason == expected_reason
    assert reservation is None
    assert subprocess_calls == []
    assert run_calls == []
    assert legacy_calls == []
    with cs.bookmaker_odds_delivery_pending_lock:
        assert match_key not in cs.bookmaker_odds_delivery_pending
    snapshot = cs._bookmaker_prefetch_lookup(match_key, wait_seconds=0.0)
    assert snapshot is not None
    assert snapshot.get("status") == "error"
    assert snapshot.get("error") == expected_reason
    assert snapshot.get("odds_refresh_ready") is False
    # Preserve old sites for diagnostics only; gate must still reject them.
    assert snapshot["sites"]["winline"]["odds"] == [1.52, 2.45]


def test_odds_mode_shared_refresh_failure_rejects_valid_cached_odds(monkeypatch) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)

    match_key = "https://cyberscore.live/matches/shared-refresh-failed-9106"
    original_message = "body\n\nБукмекеры: n/a"
    _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2, [1.61, 2.30]))
    with cs.bookmaker_odds_delivery_pending_lock:
        cs.bookmaker_odds_delivery_pending.pop(match_key, None)

    monkeypatch.setattr(
        cs,
        "_bookmaker_prefetch_fetch_camoufox_direct",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("shared owner failed")),
    )
    monkeypatch.setattr(cs, "_bookmaker_rotate_shared_camoufox_proxy", lambda **_kwargs: None)

    msg, ready, reason, reservation = cs._bookmaker_prepare_message_for_delivery(
        match_key,
        original_message,
        map_num=2,
        current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=2),
    )
    assert (msg, ready, reason, reservation) == (
        original_message,
        False,
        "shared_camoufox_refresh_failed",
        None,
    )
    with cs.bookmaker_odds_delivery_pending_lock:
        assert match_key not in cs.bookmaker_odds_delivery_pending


def test_production_dispatch_callers_do_not_pre_prepare_before_deliver() -> None:
    """Static regression: four reviewed production blocks must not pre-prepare."""
    src = Path(cs.__file__).read_text(encoding="utf-8")
    # Only the legacy helper definition may mention pre-prepare refresh.
    assert src.count("_refresh_message_bookmaker_block_for_dispatch(") == 1
    # Immediate/delayed delivery call sites must go through _deliver_and_persist_signal.
    for marker in (
        "star_signal_sent_late_pub_comeback_speculative_half",
        "star_signal_sent_now",
        "minimal_odds_only_signal_sent_now",
    ):
        assert marker in src
    # Minimal odds-only must hand reservation context, not naked skip_bookmaker_prepare.
    assert "bookmaker_reservation_context=minimal_odds_reservation" in src
    assert "skip_bookmaker_prepare=True" not in src.split("SIGNAL_MINIMAL_ODDS_ONLY_MODE")[1].split(
        "return return_status"
    )[0]


def test_delayed_production_caller_commits_sent_only_after_confirmed_delivery(
    monkeypatch, tmp_path
) -> None:
    """Real delayed production path: prepare once inside deliver, send once, commit after persist."""
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    map_id_check_path = tmp_path / "map_id_check.txt"
    monkeypatch.setattr(cs, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(cs, "MAP_ID_CHECK_PATH", str(map_id_check_path), raising=False)
    monkeypatch.setattr(cs, "TEST_DISABLE_ADD_URL", False, raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True)
    monkeypatch.setattr(cs, "DLTV_RATING_IN_SIGNAL", False, raising=False)
    monkeypatch.setattr(cs, "SIGNAL_SEND_ADMIN_ONLY", True, raising=False)
    monkeypatch.setattr(cs, "SENT_SIGNAL_JOURNAL_PATH", str(tmp_path / "journal.jsonl"), raising=False)
    monkeypatch.setattr(cs, "SENT_SIGNAL_FINGERPRINT_PATH", str(tmp_path / "fps.json"), raising=False)
    monkeypatch.setattr(cs.time, "time", lambda: 1_700_000_000.0)

    match_key = "https://cyberscore.live/matches/delayed-delivery-9200"
    _clear_delivery_state(match_key)
    _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2, [1.71, 2.11]))

    prepare_calls: List[str] = []
    send_calls: List[str] = []
    add_url_calls: List[str] = []
    real_prepare = cs._bookmaker_prepare_message_for_delivery
    real_add_url = cs.add_url

    def _counting_prepare(key, message, **kwargs):
        prepare_calls.append(key)
        return real_prepare(key, message, **kwargs)

    def _tracking_add_url(url, **kwargs):
        add_url_calls.append(url)
        return real_add_url(url, **kwargs)

    def _fake_send(message, **kwargs):
        send_calls.append(str(message))
        with cs.bookmaker_odds_delivery_pending_lock:
            pending = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
        assert pending.get("state") == "prepared"
        assert "sent_at" not in pending
        return True

    monkeypatch.setattr(cs, "_bookmaker_prepare_message_for_delivery", _counting_prepare)
    monkeypatch.setattr(cs, "send_message", _fake_send)
    monkeypatch.setattr(cs, "add_url", _tracking_add_url)
    monkeypatch.setattr(cs, "_acquire_signal_send_slot", lambda *_a, **_k: True)
    monkeypatch.setattr(cs, "_release_signal_send_slot", lambda *_a, **_k: None)
    monkeypatch.setattr(
        cs,
        "_fetch_delayed_match_state",
        lambda _json_url: {
            "game_time": float(cs.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "radiant_lead": 0.0,
        },
    )
    monkeypatch.setattr(
        cs,
        "_bookmaker_refresh_snapshot_via_shared_camoufox",
        lambda key: cs._bookmaker_prefetch_lookup(key, wait_seconds=0.0),
    )
    monkeypatch.setattr(cs, "_refresh_stake_multiplier_message", lambda message, **_k: message)
    monkeypatch.setattr(cs, "_log_bookmaker_source_snapshot", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_maybe_strip_early_kills_header_late", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_bookmaker_release_match_tabs", lambda *_a, **_k: None)

    with cs.monitored_matches_lock:
        cs.monitored_matches.clear()
    # Ensure map_id_check / processed state is isolated for this match.
    with cs.map_id_check_lock:
        pass
    if hasattr(cs, "_mark_url_processed"):
        # Clear any process-local processed markers if present.
        with getattr(cs, "map_id_check_lock"):
            pass

    # Authoritative structured selected side P1 (radiant/team1) from same observation.
    selected_side = "radiant"
    expected_line = "Кэф Winline: 1.71"
    opposite_decimal = "2.11"
    cs._set_delayed_match(
        match_key,
        {
            "message": "СТАВКА НА radiant x1\n\nБукмекеры: n/a",
            "reason": "late_only",
            "json_url": "https://dltv.org/live/delayed-delivery-9200.json",
            "target_game_time": float(cs.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 1100.0,
            "last_game_time": 1100.0,
            "last_progress_at": 1_699_999_000.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {
                "status": "ok",
                "dispatch_mode": "delayed_unit",
                "target_side": selected_side,
            },
            "fallback_send_status_label": "late_fallback_20_20_send",
            "allow_live_recheck": False,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
            "current_map_observation": _fresh_current_map_observation(match_key=match_key, map_num=2),
            "stake_multiplier_context": {"target_side": selected_side},
            "networth_target_side": selected_side,
        },
    )

    cs._drain_due_delayed_signals_once()

    assert prepare_calls == [match_key]
    assert len(send_calls) == 1
    _assert_exact_selected_winline_coefficient(
        send_calls[0], expected_line=expected_line, opposite_decimal=opposite_decimal
    )
    assert add_url_calls == [match_key]
    with cs.bookmaker_odds_delivery_pending_lock:
        pending = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending.get("state") == "sent"
    assert pending.get("sent_at") is not None
    with cs.monitored_matches_lock:
        assert match_key not in cs.monitored_matches

    # Direct second delivery attempt: already_sent blocks send; no double delivery.
    prepare_calls.clear()
    send_calls.clear()
    add_url_calls.clear()
    try:
        ok2 = cs._deliver_and_persist_signal(
            match_key,
            "СТАВКА НА radiant x1\n\nБукмекеры: n/a",
            add_url_reason="star_signal_sent_delayed_retry",
            add_url_details={"status": "ok", "target_side": selected_side},
            current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=2),
            map_num=2,
            selected_side=selected_side,
        )
    except TypeError:
        ok2 = cs._deliver_and_persist_signal(
            match_key,
            "СТАВКА НА radiant x1\n\nБукмекеры: n/a",
            add_url_reason="star_signal_sent_delayed_retry",
            add_url_details={"status": "ok", "target_side": selected_side},
            current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=2),
            map_num=2,
        )
    assert ok2 is False
    assert prepare_calls == [match_key]
    assert send_calls == []
    assert add_url_calls == []


def test_delayed_production_caller_missing_side_fails_closed(monkeypatch, tmp_path) -> None:
    """Concrete delayed payload without structured selected side must not send."""
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    map_id_check_path = tmp_path / "map_id_check.txt"
    monkeypatch.setattr(cs, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(cs, "MAP_ID_CHECK_PATH", str(map_id_check_path), raising=False)
    monkeypatch.setattr(cs, "TEST_DISABLE_ADD_URL", False, raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True)
    monkeypatch.setattr(cs, "DLTV_RATING_IN_SIGNAL", False, raising=False)
    monkeypatch.setattr(cs, "SIGNAL_SEND_ADMIN_ONLY", True, raising=False)
    monkeypatch.setattr(cs, "SENT_SIGNAL_JOURNAL_PATH", str(tmp_path / "journal.jsonl"), raising=False)
    monkeypatch.setattr(cs, "SENT_SIGNAL_FINGERPRINT_PATH", str(tmp_path / "fps.json"), raising=False)
    monkeypatch.setattr(cs.time, "time", lambda: 1_700_000_000.0)

    match_key = "https://cyberscore.live/matches/delayed-missing-side-9201"
    _clear_delivery_state(match_key)
    _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2, [1.71, 2.11]))

    send_calls: List[str] = []
    prepare_reasons: List[str] = []
    real_prepare = cs._bookmaker_prepare_message_for_delivery

    def _tracking_prepare(key, message, **kwargs):
        out = real_prepare(key, message, **kwargs)
        # reason is the third tuple element from prepare
        prepare_reasons.append(str(out[2] if len(out) > 2 else ""))
        return out

    monkeypatch.setattr(cs, "_bookmaker_prepare_message_for_delivery", _tracking_prepare)
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else "")) or True)
    monkeypatch.setattr(cs, "add_url", lambda *a, **k: None)
    monkeypatch.setattr(cs, "_acquire_signal_send_slot", lambda *_a, **_k: True)
    monkeypatch.setattr(cs, "_release_signal_send_slot", lambda *_a, **_k: None)
    monkeypatch.setattr(
        cs,
        "_fetch_delayed_match_state",
        lambda _json_url: {
            "game_time": float(cs.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "radiant_lead": 0.0,
        },
    )
    monkeypatch.setattr(
        cs,
        "_bookmaker_refresh_snapshot_via_shared_camoufox",
        lambda key: cs._bookmaker_prefetch_lookup(key, wait_seconds=0.0),
    )
    monkeypatch.setattr(cs, "_refresh_stake_multiplier_message", lambda message, **_k: message)
    monkeypatch.setattr(cs, "_log_bookmaker_source_snapshot", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_maybe_strip_early_kills_header_late", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_bookmaker_release_match_tabs", lambda *_a, **_k: None)

    with cs.monitored_matches_lock:
        cs.monitored_matches.clear()

    # No structured selected-side fields: production must fail closed, not dual-footer send.
    cs._set_delayed_match(
        match_key,
        {
            "message": "СТАВКА НА team1 x1\n\nБукмекеры: n/a",
            "reason": "late_only",
            "json_url": "https://dltv.org/live/delayed-missing-side-9201.json",
            "target_game_time": float(cs.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 1100.0,
            "last_game_time": 1100.0,
            "last_progress_at": 1_699_999_000.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {"status": "ok", "dispatch_mode": "delayed_unit"},
            "fallback_send_status_label": "late_fallback_20_20_send",
            "allow_live_recheck": False,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
            "current_map_observation": _fresh_current_map_observation(match_key=match_key, map_num=2),
        },
    )

    cs._drain_due_delayed_signals_once()

    assert send_calls == [], (
        "missing structured selected side must not deliver; "
        f"got forbidden message(s): {send_calls!r}"
    )
    assert any("selected_side_missing" in reason for reason in prepare_reasons), (
        f"expected observable reason selected_side_missing; got {prepare_reasons!r}"
    )


def _closed_winline_sites(map_num: int = 2) -> Dict[str, Any]:
    return {
        "winline": {
            "match_found": True,
            "odds": [],
            "match_odds": [1.30, 3.15],
            "market_closed": True,
            "market_kind": "current_map_winner",
            "map_num": map_num,
            "p1_team": "team1",
            "p2_team": "team2",
            "source": "winline_map_market_closed",
        }
    }


def _fresh_current_map_observation(
    *,
    match_key: Optional[str] = None,
    map_num: int = 2,
    status: str = "live",
    observed_at: Optional[float] = None,
    include_match_key: bool = True,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "map_num": map_num,
        "status": status,
        "observed_at": float(observed_at if observed_at is not None else time.time()),
    }
    if include_match_key:
        out["match_key"] = match_key
    return out


def _obs(map_num: int = 2, **kwargs) -> Dict[str, Any]:
    return _fresh_current_map_observation(map_num=map_num, **kwargs)


def _seed_prefetch_with_refresh(
    match_key: str,
    *,
    map_num: int,
    sites: Dict[str, Any],
    odds_refreshed_at: Optional[float] = None,
    mode: str = "live",
) -> None:
    refreshed_at = float(odds_refreshed_at if odds_refreshed_at is not None else time.time())
    with cs.bookmaker_prefetch_condition:
        cs.bookmaker_prefetch_results[match_key] = {
            "status": "done",
            "finished_at": refreshed_at,
            "submitted_at": refreshed_at,
            "odds_refreshed_at": refreshed_at,
            "odds_refresh_ready": True,
            "map_num": map_num,
            "mode": mode,
            "sites": sites,
        }


def _patch_production_delivery_env(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True)
    monkeypatch.setattr(cs, "DLTV_RATING_IN_SIGNAL", False, raising=False)
    monkeypatch.setattr(cs, "SIGNAL_SEND_ADMIN_ONLY", True, raising=False)
    monkeypatch.setattr(cs, "SENT_SIGNAL_JOURNAL_PATH", str(tmp_path / "journal.jsonl"), raising=False)
    monkeypatch.setattr(cs, "SENT_SIGNAL_FINGERPRINT_PATH", str(tmp_path / "fps.json"), raising=False)
    monkeypatch.setattr(
        cs,
        "_bookmaker_refresh_snapshot_via_shared_camoufox",
        lambda key: cs._bookmaker_prefetch_lookup(key, wait_seconds=0.0),
    )


def test_production_closed_wait_keeps_original_deadline_and_no_reservation(
    monkeypatch, tmp_path
) -> None:
    """Closed market on the production deliver path retains deadline_at; no token/send."""
    _patch_production_delivery_env(monkeypatch, tmp_path)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_000_100.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    match_key = "https://cyberscore.live/matches/lifecycle-closed-deadline-9301"
    _clear_delivery_state(match_key)
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_closed_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )

    send_calls: List[str] = []
    add_url_calls: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else "")))
    monkeypatch.setattr(cs, "add_url", lambda url, **_k: add_url_calls.append(url))

    observation = _fresh_current_map_observation(match_key=match_key, map_num=2, observed_at=clock["now"])
    base_msg = "СТАВКА НА team1 x1\n\nБукмекеры: n/a"

    ok1 = cs._deliver_and_persist_signal(
        match_key,
        base_msg,
        add_url_reason="unit_closed_wait",
        add_url_details={"status": "ok"},
        current_map_observation=observation,
    )
    assert ok1 is False
    assert send_calls == []
    assert add_url_calls == []

    with cs.bookmaker_odds_delivery_pending_lock:
        pending1 = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending1.get("state") == "temporarily_closed_wait"
    assert pending1.get("token") in (None, "")
    deadline1 = pending1.get("deadline_at")
    assert isinstance(deadline1, (int, float))
    assert float(deadline1) == pytest.approx(clock["now"] + 90.0)

    # Second poll later must keep the original deadline and still not reserve/send.
    clock["now"] = 1_700_000_130.0
    observation2 = _fresh_current_map_observation(match_key=match_key, map_num=2, observed_at=clock["now"])
    ok2 = cs._deliver_and_persist_signal(
        match_key,
        base_msg,
        add_url_reason="unit_closed_wait_retry",
        add_url_details={"status": "ok"},
        current_map_observation=observation2,
    )
    assert ok2 is False
    assert send_calls == []
    assert add_url_calls == []

    with cs.bookmaker_odds_delivery_pending_lock:
        pending2 = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending2.get("state") == "temporarily_closed_wait"
    assert pending2.get("token") in (None, "")
    assert float(pending2.get("deadline_at")) == pytest.approx(float(deadline1))


def test_closed_then_fresh_open_sends_once_with_observation(monkeypatch, tmp_path) -> None:
    """Closed wait then fresh open exact-map odds: one prepare/reserve/send/commit."""
    _patch_production_delivery_env(monkeypatch, tmp_path)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_000_200.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    match_key = "https://cyberscore.live/matches/lifecycle-closed-open-9302"
    _clear_delivery_state(match_key)
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_closed_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )

    send_calls: List[str] = []
    add_url_calls: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else "")))
    monkeypatch.setattr(cs, "add_url", lambda url, **_k: add_url_calls.append(url))

    base_msg = "СТАВКА НА team1 x1\n\nБукмекеры: n/a"
    obs = _fresh_current_map_observation(match_key=match_key, map_num=2, observed_at=clock["now"])
    ok_closed = cs._deliver_and_persist_signal(
        match_key,
        base_msg,
        add_url_reason="unit_closed_open",
        current_map_observation=obs,
    )
    assert ok_closed is False
    assert send_calls == []

    with cs.bookmaker_odds_delivery_pending_lock:
        pending_closed = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    deadline_at = pending_closed.get("deadline_at")
    assert isinstance(deadline_at, (int, float))

    clock["now"] = 1_700_000_210.0
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2, [1.61, 2.22]),
        odds_refreshed_at=clock["now"],
    )
    obs_open = _fresh_current_map_observation(match_key=match_key, map_num=2, observed_at=clock["now"])
    ok_open = cs._deliver_and_persist_signal(
        match_key,
        base_msg,
        add_url_reason="unit_closed_open_send",
        current_map_observation=obs_open,
    )
    assert ok_open is True
    assert len(send_calls) == 1
    assert "1.61" in send_calls[0] and "2.22" in send_calls[0]
    assert add_url_calls == [match_key]

    with cs.bookmaker_odds_delivery_pending_lock:
        pending_sent = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending_sent.get("state") == "sent"
    assert "sent_at" in pending_sent

    # Second attempt must not send again.
    send_calls.clear()
    add_url_calls.clear()
    ok_again = cs._deliver_and_persist_signal(
        match_key,
        base_msg,
        add_url_reason="unit_closed_open_retry",
        current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=2, observed_at=clock["now"]),
    )
    assert ok_again is False
    assert send_calls == []
    assert add_url_calls == []


def test_terminal_skips_never_reserve_or_send(monkeypatch, tmp_path) -> None:
    """Map mismatch / finished / stale-after-deadline / future / missing-after-deadline skip cleanly."""
    _patch_production_delivery_env(monkeypatch, tmp_path)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_000_300.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    send_calls: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else "")))
    monkeypatch.setattr(cs, "add_url", lambda *a, **k: None)
    base_msg = "СТАВКА НА team1 x1\n\nБукмекеры: n/a"

    cases = [
        (
            "mismatch",
            "https://cyberscore.live/matches/lifecycle-term-mismatch-9303",
            _open_winline_sites(2),
            _fresh_current_map_observation(
                match_key="https://cyberscore.live/matches/lifecycle-term-mismatch-9303",
                map_num=3,
                observed_at=1_700_000_300.0,
            ),
            "current_map_mismatch",
        ),
        (
            "finished",
            "https://cyberscore.live/matches/lifecycle-term-finished-9304",
            _open_winline_sites(2),
            _fresh_current_map_observation(
                match_key="https://cyberscore.live/matches/lifecycle-term-finished-9304",
                map_num=2,
                status="finished",
                observed_at=1_700_000_300.0,
            ),
            "match_finished",
        ),
        (
            "future_odds",
            "https://cyberscore.live/matches/lifecycle-term-future-9305",
            _open_winline_sites(2),
            _fresh_current_map_observation(
                match_key="https://cyberscore.live/matches/lifecycle-term-future-9305",
                map_num=2,
                observed_at=1_700_000_300.0,
            ),
            "odds_timestamp_future",
            1_700_000_400.0,  # odds refreshed in the future
        ),
    ]

    for case in cases:
        label, match_key, sites, observation, expected_reason = case[:5]
        odds_ts = case[5] if len(case) > 5 else clock["now"]
        _clear_delivery_state(match_key)
        _seed_prefetch_with_refresh(
            match_key, map_num=2, sites=sites, odds_refreshed_at=odds_ts
        )
        send_calls.clear()
        ok = cs._deliver_and_persist_signal(
            match_key,
            base_msg,
            add_url_reason=f"unit_terminal_{label}",
            current_map_observation=observation,
            map_num=2,
        )
        assert ok is False, label
        assert send_calls == [], label
        with cs.bookmaker_odds_delivery_pending_lock:
            pending = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
        # Terminal skip must not leave a reservation token.
        assert pending.get("token") in (None, ""), label
        if label in {"mismatch", "finished"}:
            assert pending == {} or pending.get("state") != "prepared", label

    # Stale open odds after retained deadline: terminal, no send.
    match_key = "https://cyberscore.live/matches/lifecycle-term-stale-deadline-9306"
    _clear_delivery_state(match_key)
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_closed_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    obs = _fresh_current_map_observation(match_key=match_key, map_num=2, observed_at=clock["now"])
    assert (
        cs._deliver_and_persist_signal(
            match_key,
            base_msg,
            add_url_reason="unit_stale_seed",
            current_map_observation=obs,
        )
        is False
    )
    with cs.bookmaker_odds_delivery_pending_lock:
        deadline_at = float(cs.bookmaker_odds_delivery_pending[match_key]["deadline_at"])

    clock["now"] = deadline_at + 1.0
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2),
        odds_refreshed_at=clock["now"] - 60.0,  # stale beyond max age
    )
    send_calls.clear()
    ok_stale = cs._deliver_and_persist_signal(
        match_key,
        base_msg,
        add_url_reason="unit_stale_after_deadline",
        current_map_observation=_fresh_current_map_observation(
            match_key=match_key,
            map_num=2, observed_at=clock["now"]
        ),
    )
    assert ok_stale is False
    assert send_calls == []
    with cs.bookmaker_odds_delivery_pending_lock:
        pending_stale = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending_stale.get("token") in (None, "")
    assert pending_stale.get("state") != "prepared"


def test_stale_open_odds_before_deadline_waits_tokenless(monkeypatch, tmp_path) -> None:
    """Stale open odds before retained deadline wait without reservation/send."""
    _patch_production_delivery_env(monkeypatch, tmp_path)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_000_400.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    match_key = "https://cyberscore.live/matches/lifecycle-stale-wait-9307"
    _clear_delivery_state(match_key)
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2),
        odds_refreshed_at=clock["now"] - 30.0,  # older than max age
    )

    send_calls: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else "")))
    monkeypatch.setattr(cs, "add_url", lambda *a, **k: None)

    ok = cs._deliver_and_persist_signal(
        match_key,
        "СТАВКА НА team1 x1\n\nБукмекеры: n/a",
        add_url_reason="unit_stale_wait",
        current_map_observation=_fresh_current_map_observation(
            match_key=match_key,
            map_num=2, observed_at=clock["now"]
        ),
    )
    assert ok is False
    assert send_calls == []
    with cs.bookmaker_odds_delivery_pending_lock:
        pending = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending.get("state") == "temporarily_closed_wait"
    assert pending.get("token") in (None, "")
    assert isinstance(pending.get("deadline_at"), (int, float))
    assert float(pending["deadline_at"]) == pytest.approx(clock["now"] + 90.0)

    # Retained deadline on second stale poll.
    clock["now"] = 1_700_000_420.0
    first_deadline = float(pending["deadline_at"])
    ok2 = cs._deliver_and_persist_signal(
        match_key,
        "СТАВКА НА team1 x1\n\nБукмекеры: n/a",
        add_url_reason="unit_stale_wait_2",
        current_map_observation=_fresh_current_map_observation(
            match_key=match_key,
            map_num=2, observed_at=clock["now"]
        ),
    )
    assert ok2 is False
    with cs.bookmaker_odds_delivery_pending_lock:
        pending2 = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert float(pending2.get("deadline_at")) == pytest.approx(first_deadline)
    assert pending2.get("token") in (None, "")


def test_map_change_between_fetch_and_reserve_skips(monkeypatch, tmp_path) -> None:
    """If live observation map diverges from odds map at reserve time, no send."""
    _patch_production_delivery_env(monkeypatch, tmp_path)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)

    clock = {"now": 1_700_000_500.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    match_key = "https://cyberscore.live/matches/lifecycle-map-change-9308"
    _clear_delivery_state(match_key)
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )

    send_calls: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else "")))
    monkeypatch.setattr(cs, "add_url", lambda *a, **k: None)

    # Observation says map 3 while odds are for map 2.
    ok = cs._deliver_and_persist_signal(
        match_key,
        "СТАВКА НА team1 x1\n\nБукмекеры: n/a",
        add_url_reason="unit_map_change",
        map_num=2,
        current_map_observation=_fresh_current_map_observation(
            match_key=match_key,
            map_num=3, observed_at=clock["now"]
        ),
    )
    assert ok is False
    assert send_calls == []
    with cs.bookmaker_odds_delivery_pending_lock:
        pending = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending.get("token") in (None, "")
    assert pending.get("state") != "prepared"


def test_finished_status_clears_pending_odds_wait(monkeypatch) -> None:
    """status=='finished' cleanup boundary drops pending wait for the match key."""
    match_key = "https://cyberscore.live/matches/lifecycle-finished-cleanup-9309"
    with cs.bookmaker_odds_delivery_pending_lock:
        cs.bookmaker_odds_delivery_pending[match_key] = {
            "map_num": 2,
            "state": "temporarily_closed_wait",
            "created_at": 1.0,
            "updated_at": 1.0,
            "deadline_at": 100.0,
        }
    cs._bookmaker_clear_odds_delivery_pending(match_key, map_num=2)
    with cs.bookmaker_odds_delivery_pending_lock:
        assert match_key not in cs.bookmaker_odds_delivery_pending


def test_delayed_sender_refreshes_observation_before_delivery(monkeypatch, tmp_path) -> None:
    """Delayed drain must refresh current_map_observation and pass it into deliver."""
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    map_id_check_path = tmp_path / "map_id_check.txt"
    monkeypatch.setattr(cs, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(cs, "MAP_ID_CHECK_PATH", str(map_id_check_path), raising=False)
    monkeypatch.setattr(cs, "TEST_DISABLE_ADD_URL", False, raising=False)
    _patch_production_delivery_env(monkeypatch, tmp_path)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_000_600.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    match_key = "https://cyberscore.live/matches/lifecycle-delayed-obs-9310"
    _clear_delivery_state(match_key)
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2, [1.88, 1.99]),
        odds_refreshed_at=clock["now"],
    )

    deliver_kwargs: List[Dict[str, Any]] = []
    real_deliver = cs._deliver_and_persist_signal

    def _capturing_deliver(key, message, **kwargs):
        deliver_kwargs.append(dict(kwargs))
        return real_deliver(key, message, **kwargs)

    monkeypatch.setattr(cs, "_deliver_and_persist_signal", _capturing_deliver)
    monkeypatch.setattr(
        cs,
        "_fetch_delayed_match_state",
        lambda _json_url: {
            "game_time": 720.0,
            "radiant_lead": 1200.0,
            "map_num": 2,
            "status": "live",
            "observed_at": clock["now"],
        },
    )
    # Force due immediately.
    monkeypatch.setattr(cs, "_is_url_processed", lambda *_a, **_k: False)
    send_calls: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else "")))
    monkeypatch.setattr(cs, "add_url", lambda *a, **k: None)

    # Seed delayed queue with stale observation that must be refreshed.
    # Shape mirrors production delayed payload used by _drain_due_delayed_signals_once.
    # Authoritative structured selected side P2 (dire/team2) from same observation.
    selected_side = "dire"
    expected_line = "Кэф Winline: 1.99"
    opposite_decimal = "1.88"
    monkeypatch.setattr(cs, "_acquire_signal_send_slot", lambda *_a, **_k: True)
    monkeypatch.setattr(cs, "_release_signal_send_slot", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_refresh_stake_multiplier_message", lambda message, **_k: message)
    monkeypatch.setattr(cs, "_log_bookmaker_source_snapshot", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_maybe_strip_early_kills_header_late", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_bookmaker_release_match_tabs", lambda *_a, **_k: None)
    with cs.monitored_matches_lock:
        cs.monitored_matches.clear()
    cs._set_delayed_match(
        match_key,
        {
            "message": "СТАВКА НА dire x1\n\nБукмекеры: n/a",
            "reason": "late_only",
            "json_url": "https://dltv.org/live/lifecycle-delayed-obs-9310.json",
            "target_game_time": 720.0,
            "queued_at": clock["now"] - 100.0,
            "queued_game_time": 600.0,
            "last_game_time": 600.0,
            "last_progress_at": clock["now"] - 100.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {
                "status": "ok",
                "dispatch_mode": "delayed_unit",
                "target_side": selected_side,
            },
            "fallback_send_status_label": "late_fallback_20_20_send",
            "allow_live_recheck": False,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
            "current_map_observation": {
                "match_key": match_key,
                "map_num": 2,
                "status": "live",
                "observed_at": clock["now"] - 1000.0,  # stale
            },
            "stake_multiplier_context": {"target_side": selected_side},
            "networth_target_side": selected_side,
        },
    )

    cs._drain_due_delayed_signals_once(only_match_key=match_key)

    assert deliver_kwargs, "delayed drain must call deliver"
    obs = deliver_kwargs[0].get("current_map_observation")
    assert isinstance(obs, dict)
    assert int(obs.get("map_num")) == 2
    assert str(obs.get("status")).lower() == "live"
    # Must not keep the ancient stored observed_at.
    assert float(obs.get("observed_at")) == pytest.approx(clock["now"])
    assert len(send_calls) == 1
    _assert_exact_selected_winline_coefficient(
        send_calls[0], expected_line=expected_line, opposite_decimal=opposite_decimal
    )


def test_closed_wait_to_fresh_open_after_retained_deadline_is_terminal_tokenless(
    monkeypatch,
) -> None:
    """After retained deadline, closed->fresh open must stay terminal/tokenless."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)

    clock = {"now": 1_700_001_000.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    match_key = "https://cyberscore.live/matches/w2r1-deadline-9401"
    pending: Dict[str, Any] = {}
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_closed_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    obs = _fresh_current_map_observation(match_key=match_key, map_num=2, observed_at=clock["now"])
    d1 = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        map_num=2,
        current_map_observation=obs,
    )
    assert d1["state"] == "temporarily_closed_wait"
    assert d1["should_send"] is False
    assert pending[match_key].get("token") in (None, "")
    deadline_at = float(pending[match_key]["deadline_at"])
    retained_deadline = deadline_at

    # Still before deadline: open may prepare (control that open path works).
    clock["now"] = deadline_at - 10.0
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2, [1.55, 2.40]),
        odds_refreshed_at=clock["now"],
    )
    # Keep closed-wait path for the actual regression: restore closed, advance past deadline.
    clock["now"] = 1_700_001_000.0
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_closed_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    pending.clear()
    d_seed = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        map_num=2,
        current_map_observation=_fresh_current_map_observation(
            match_key=match_key,
            map_num=2, observed_at=clock["now"]
        ),
    )
    assert d_seed["state"] == "temporarily_closed_wait"
    deadline_at = float(pending[match_key]["deadline_at"])
    assert deadline_at == pytest.approx(retained_deadline)

    clock["now"] = deadline_at + 1.0
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2, [1.55, 2.40]),
        odds_refreshed_at=clock["now"],
    )
    d2 = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        map_num=2,
        current_map_observation=_fresh_current_map_observation(
            match_key=match_key,
            map_num=2, observed_at=clock["now"]
        ),
    )
    assert d2["state"] == "terminal_skip"
    assert d2["should_send"] is False
    assert d2.get("token") in (None, "")
    assert d2.get("reservation_context") in (None, {})
    assert match_key not in pending or pending[match_key].get("token") in (None, "")
    assert pending.get(match_key) is None or pending[match_key].get("state") != "prepared"

    # Fresh open later must not refresh/extend retained deadline or create ownership.
    clock["now"] = deadline_at + 30.0
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2, [1.60, 2.30]),
        odds_refreshed_at=clock["now"],
    )
    d3 = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        map_num=2,
        current_map_observation=_fresh_current_map_observation(
            match_key=match_key,
            map_num=2, observed_at=clock["now"]
        ),
    )
    assert d3["state"] == "terminal_skip"
    assert d3["should_send"] is False
    assert d3.get("token") in (None, "")
    assert pending.get(match_key) is None or pending[match_key].get("token") in (None, "")


def test_missing_observation_direct_resolver_waits_tokenless_then_terminal(
    monkeypatch,
) -> None:
    """current_map_observation=None is invalid for W2: wait then terminal, never reserve."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)

    clock = {"now": 1_700_001_100.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    match_key = "https://cyberscore.live/matches/w2r1-missing-obs-9402"
    pending: Dict[str, Any] = {}
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2, [1.70, 2.10]),
        odds_refreshed_at=clock["now"],
    )

    d1 = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        map_num=2,
        current_map_observation=None,
    )
    assert d1["should_send"] is False
    assert d1.get("token") in (None, "")
    assert d1.get("reservation_context") in (None, {})
    assert d1["state"] in {"temporarily_closed_wait", "terminal_skip"} or d1.get(
        "reason"
    ) in {"current_map_unavailable", "temporarily_closed_wait"}
    entry1 = pending.get(match_key) or {}
    assert entry1.get("token") in (None, "")
    assert entry1.get("state") != "prepared"
    # Before deadline: wait/tokenless with retained deadline.
    assert d1["state"] == "temporarily_closed_wait"
    deadline_at = float(entry1["deadline_at"])
    assert deadline_at == pytest.approx(clock["now"] + 90.0)

    clock["now"] = deadline_at - 5.0
    d2 = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        map_num=2,
        current_map_observation=None,
    )
    assert d2["state"] == "temporarily_closed_wait"
    assert d2["should_send"] is False
    assert (pending.get(match_key) or {}).get("token") in (None, "")
    assert float((pending.get(match_key) or {})["deadline_at"]) == pytest.approx(
        deadline_at
    )

    clock["now"] = deadline_at + 1.0
    d3 = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        map_num=2,
        current_map_observation=None,
    )
    assert d3["state"] == "terminal_skip"
    assert d3["should_send"] is False
    assert d3.get("token") in (None, "")
    assert pending.get(match_key) is None or (pending.get(match_key) or {}).get(
        "token"
    ) in (None, "")


def test_missing_observation_delayed_path_never_reserves_or_sends(
    monkeypatch, tmp_path
) -> None:
    """Canonical delayed production path with missing observation must not reserve/send."""
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    map_id_check_path = tmp_path / "map_id_check.txt"
    monkeypatch.setattr(cs, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(cs, "MAP_ID_CHECK_PATH", str(map_id_check_path), raising=False)
    monkeypatch.setattr(cs, "TEST_DISABLE_ADD_URL", False, raising=False)
    _patch_production_delivery_env(monkeypatch, tmp_path)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_001_200.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    match_key = "https://cyberscore.live/matches/w2r1-missing-obs-delayed-9403"
    _clear_delivery_state(match_key)
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2, [1.81, 2.05]),
        odds_refreshed_at=clock["now"],
    )

    send_calls: List[str] = []
    add_url_calls: List[str] = []
    monkeypatch.setattr(
        cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else ""))
    )
    monkeypatch.setattr(cs, "add_url", lambda url, **_k: add_url_calls.append(url))
    monkeypatch.setattr(cs, "_acquire_signal_send_slot", lambda *_a, **_k: True)
    monkeypatch.setattr(cs, "_release_signal_send_slot", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_is_url_processed", lambda *_a, **_k: False)
    monkeypatch.setattr(cs, "_refresh_stake_multiplier_message", lambda message, **_k: message)
    monkeypatch.setattr(cs, "_log_bookmaker_source_snapshot", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_maybe_strip_early_kills_header_late", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_bookmaker_release_match_tabs", lambda *_a, **_k: None)
    # Force missing observation on the delayed production path: no map/status enrichment.
    monkeypatch.setattr(
        cs,
        "_fetch_delayed_match_state",
        lambda _json_url: {
            "game_time": 720.0,
            "radiant_lead": 100.0,
            # intentionally no map_num/status/observed_at
        },
    )
    # Prevent stored observation from being reattached.
    monkeypatch.setattr(
        cs,
        "_bookmaker_enrich_delayed_match_state",
        lambda state, source=None: state,
    )

    with cs.monitored_matches_lock:
        cs.monitored_matches.clear()
    cs._set_delayed_match(
        match_key,
        {
            "message": "СТАВКА НА team1 x1\n\nБукмекеры: n/a",
            "reason": "late_only",
            "json_url": "https://dltv.org/live/w2r1-missing-obs-delayed-9403.json",
            "target_game_time": 720.0,
            "queued_at": clock["now"] - 100.0,
            "queued_game_time": 600.0,
            "last_game_time": 600.0,
            "last_progress_at": clock["now"] - 100.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {"status": "ok", "dispatch_mode": "delayed_unit"},
            "fallback_send_status_label": "late_fallback_20_20_send",
            "allow_live_recheck": False,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
            # Explicitly no current_map_observation in delayed payload.
        },
    )

    cs._drain_due_delayed_signals_once(only_match_key=match_key)

    assert send_calls == []
    assert add_url_calls == []
    with cs.bookmaker_odds_delivery_pending_lock:
        pending = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending.get("token") in (None, "")
    assert pending.get("state") != "prepared"
    # Still tracked as wait or empty; never ownership reservation.
    if pending:
        assert pending.get("state") in {
            "temporarily_closed_wait",
            None,
            "",
        } or pending.get("token") in (None, "")


def test_old_map_finished_cleanup_preserves_new_map_pending_and_owner(
    monkeypatch,
) -> None:
    """Finished cleanup from old map/lease must not drop newer-map pending+owner."""
    match_key = "https://cyberscore.live/matches/w2r1-cleanup-map-scope-9404"
    owner_token = "owner-token-map3-aabbcc"
    with cs.bookmaker_odds_delivery_pending_lock:
        cs.bookmaker_odds_delivery_pending[match_key] = {
            "map_num": 3,
            "state": "prepared",
            "token": owner_token,
            "created_at": 1_700_001_300.0,
            "updated_at": 1_700_001_300.0,
            "deadline_at": 1_700_001_390.0,
        }
        snapshot_before = dict(cs.bookmaker_odds_delivery_pending[match_key])

    # Old map/lease completion against newer-map pending must be a no-op.
    cs._bookmaker_clear_odds_delivery_pending(
        match_key,
        map_num=2,
        token="stale-old-map-token",
    )
    with cs.bookmaker_odds_delivery_pending_lock:
        after_old = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert after_old == snapshot_before
    assert after_old.get("map_num") == 3
    assert after_old.get("token") == owner_token
    assert after_old.get("state") == "prepared"

    # Exact matching current-map/current-owner completion still cleans up.
    cs._bookmaker_clear_odds_delivery_pending(
        match_key,
        map_num=3,
        token=owner_token,
    )
    with cs.bookmaker_odds_delivery_pending_lock:
        assert match_key not in cs.bookmaker_odds_delivery_pending


def test_fresh_open_exact_map_happy_path_sends_once(monkeypatch, tmp_path) -> None:
    """Fresh/open exact-map observation: exactly one send."""
    _patch_production_delivery_env(monkeypatch, tmp_path)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_001_400.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    match_key = "https://cyberscore.live/matches/w2r1-happy-path-9405"
    _clear_delivery_state(match_key)
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2, [1.66, 2.18]),
        odds_refreshed_at=clock["now"],
    )

    send_calls: List[str] = []
    add_url_calls: List[str] = []
    monkeypatch.setattr(
        cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else ""))
    )
    monkeypatch.setattr(cs, "add_url", lambda url, **_k: add_url_calls.append(url))

    base_msg = "СТАВКА НА team1 x1\n\nБукмекеры: n/a"
    obs = _fresh_current_map_observation(match_key=match_key, map_num=2, observed_at=clock["now"])
    ok = cs._deliver_and_persist_signal(
        match_key,
        base_msg,
        add_url_reason="unit_happy_path",
        current_map_observation=obs,
        map_num=2,
    )
    assert ok is True
    assert len(send_calls) == 1
    assert "1.66" in send_calls[0] and "2.18" in send_calls[0]
    assert add_url_calls == [match_key]

    send_calls.clear()
    add_url_calls.clear()
    ok2 = cs._deliver_and_persist_signal(
        match_key,
        base_msg,
        add_url_reason="unit_happy_path_retry",
        current_map_observation=_fresh_current_map_observation(
            match_key=match_key,
            map_num=2, observed_at=clock["now"]
        ),
        map_num=2,
    )
    assert ok2 is False
    assert send_calls == []
    assert add_url_calls == []



# ---------------------------------------------------------------------------
# W2 ITERATION 2D: inclusive retained deadline + observation-safe finished cleanup
# ---------------------------------------------------------------------------


def _snapshot_pending(match_key: str) -> Dict[str, Any]:
    with cs.bookmaker_odds_delivery_pending_lock:
        entry = cs.bookmaker_odds_delivery_pending.get(match_key)
        return dict(entry) if isinstance(entry, dict) else {}


def _seed_pending(match_key: str, entry: Dict[str, Any]) -> Dict[str, Any]:
    with cs.bookmaker_odds_delivery_pending_lock:
        cs.bookmaker_odds_delivery_pending[match_key] = dict(entry)
        return dict(cs.bookmaker_odds_delivery_pending[match_key])


def _build_legacy_finished_listing(
    *,
    slug: str,
    score_left: int = 1,
    score_right: int = 1,
):
    html = f"""
    <div class="head">
      <div class="event__info-info__time">finished</div>
    </div>
    <div class="body">
      <div class="match__item-team__score">{score_left}</div>
      <div class="match__item-team__score">{score_right}</div>
      <a href="https://dltv.org/matches/{slug}"></a>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    head = soup.find("div", class_="head")
    body = soup.find("div", class_="body")
    assert head is not None and body is not None
    return [head], [body]


def _patch_check_head_finished_harness(monkeypatch, *, applied_update: Optional[Dict[str, Any]] = None):
    """Keep check_head finished branches free of side effects outside bookmaker cleanup."""
    monkeypatch.setattr(cs, "_should_emit_verbose_match_log", lambda *_a, **_k: False)
    monkeypatch.setattr(cs, "_dispatch_block_reason", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_drop_delayed_match", lambda *_a, **_k: True)
    monkeypatch.setattr(cs, "_emit_live_elo_applied_log", lambda *_a, **_k: None)

    def _fake_finalize(**_kwargs):
        if applied_update is None:
            return None
        return {"applied_update": dict(applied_update)}

    monkeypatch.setattr(cs, "_finalize_finished_live_series_for_elo", _fake_finalize)


def test_retained_deadline_no_mutation_at_equality_and_later(monkeypatch) -> None:
    """now == and now > retained deadline: terminal/tokenless without mutating pending."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)

    clock = {"now": 1_700_002_000.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    match_key = "https://cyberscore.live/matches/w2-i3-deadline-immutable-9601"
    pending: Dict[str, Any] = {}
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_closed_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    obs = _fresh_current_map_observation(match_key=match_key, map_num=2, observed_at=clock["now"])
    d_wait = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        map_num=2,
        current_map_observation=obs,
    )
    assert d_wait["state"] == "temporarily_closed_wait"
    assert d_wait["should_send"] is False
    assert pending[match_key].get("token") in (None, "")
    deadline_at = float(pending[match_key]["deadline_at"])
    waiting_snapshot = dict(pending[match_key])
    # Full-dict reservation identity is the pending entry itself (no separate store).
    reservation_snapshot_wait = dict(pending[match_key])

    # Control: before deadline remains waiting/open-capable path unchanged.
    clock["now"] = deadline_at - 1.0
    d_before = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        map_num=2,
        current_map_observation=_fresh_current_map_observation(
            match_key=match_key,
            map_num=2, observed_at=clock["now"]
        ),
    )
    assert d_before["state"] == "temporarily_closed_wait"
    assert d_before["should_send"] is False
    assert pending[match_key].get("token") in (None, "")
    # Waiting refresh may update timestamps under the same wait state; capture terminal baseline after.
    baseline_before_terminal = dict(pending[match_key])
    assert baseline_before_terminal.get("state") == "temporarily_closed_wait"
    assert float(baseline_before_terminal["deadline_at"]) == pytest.approx(deadline_at)

    # Equality boundary: market can be fresh open, but retained deadline is terminal.
    clock["now"] = float(deadline_at)
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2, [1.55, 2.40]),
        odds_refreshed_at=clock["now"],
    )
    pending_before_eq = dict(pending[match_key])
    reservation_before_eq = dict(pending[match_key])
    d_eq = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        map_num=2,
        current_map_observation=_fresh_current_map_observation(
            match_key=match_key,
            map_num=2, observed_at=clock["now"]
        ),
    )
    assert d_eq["state"] == "terminal_skip"
    assert d_eq["should_send"] is False
    assert d_eq.get("token") in (None, "")
    assert d_eq.get("reservation_context") in (None, {})
    # Complete pending + reservation snapshot equality: no rewrite of wait entry.
    assert dict(pending[match_key]) == pending_before_eq
    assert dict(pending[match_key]) == reservation_before_eq
    assert pending[match_key].get("state") == "temporarily_closed_wait"
    assert pending[match_key].get("updated_at") == pending_before_eq.get("updated_at")
    assert float(pending[match_key]["deadline_at"]) == pytest.approx(deadline_at)
    assert pending[match_key].get("token") in (None, "")
    assert waiting_snapshot.get("token") in (None, "")
    assert reservation_snapshot_wait.get("token") in (None, "")

    # Later than deadline: still terminal/tokenless and still no mutation.
    clock["now"] = float(deadline_at) + 5.0
    _seed_prefetch_with_refresh(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2, [1.60, 2.30]),
        odds_refreshed_at=clock["now"],
    )
    pending_before_later = dict(pending[match_key])
    reservation_before_later = dict(pending[match_key])
    d_later = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        map_num=2,
        current_map_observation=_fresh_current_map_observation(
            match_key=match_key,
            map_num=2, observed_at=clock["now"]
        ),
    )
    assert d_later["state"] == "terminal_skip"
    assert d_later["should_send"] is False
    assert d_later.get("token") in (None, "")
    assert d_later.get("reservation_context") in (None, {})
    assert dict(pending[match_key]) == pending_before_later
    assert dict(pending[match_key]) == reservation_before_later
    assert pending[match_key].get("state") == "temporarily_closed_wait"
    assert pending[match_key].get("updated_at") == pending_before_later.get("updated_at")
    assert float(pending[match_key]["deadline_at"]) == pytest.approx(deadline_at)


def test_exact_retained_deadline_equality_is_terminal_tokenless_no_mutation(
    monkeypatch,
) -> None:
    """Compatibility alias covering equality boundary immutability for preserved suite."""
    test_retained_deadline_no_mutation_at_equality_and_later(monkeypatch)


def test_clear_pending_without_map_identity_is_noop(monkeypatch) -> None:
    """Helper: pending object with no map_num is never eligible for finished cleanup."""
    match_key = "https://cyberscore.live/matches/w2-2d-clear-no-map-9502"
    seed = {
        "state": "temporarily_closed_wait",
        "created_at": 1_700_002_100.0,
        "updated_at": 1_700_002_100.0,
        "deadline_at": 1_700_002_190.0,
        # intentionally no map_num
    }
    before = _seed_pending(match_key, seed)
    cs._bookmaker_clear_odds_delivery_pending(match_key, map_num=2)
    after = _snapshot_pending(match_key)
    assert after == before
    assert "map_num" not in after
    with cs.bookmaker_odds_delivery_pending_lock:
        assert match_key in cs.bookmaker_odds_delivery_pending


def _assert_finished_cleanup_waiting_only_matrix(
    *,
    run_site,
    match_key: str,
    exact_map: int,
) -> None:
    """Shared finished-cleanup contract: only exact-map tokenless wait deletes."""
    tokenless_waiting = {
        "map_num": exact_map,
        "state": "temporarily_closed_wait",
        "created_at": 1_700_002_200.0,
        "updated_at": 1_700_002_200.0,
        "deadline_at": 1_700_002_290.0,
    }
    owner_prepared = {
        "map_num": exact_map,
        "state": "prepared",
        "token": "owner-token-w2-2d-aabb",
        "created_at": 1_700_002_201.0,
        "updated_at": 1_700_002_201.0,
        "deadline_at": 1_700_002_291.0,
    }
    missing_map_pending = {
        "state": "temporarily_closed_wait",
        "created_at": 1_700_002_202.0,
        "updated_at": 1_700_002_202.0,
        "deadline_at": 1_700_002_292.0,
    }
    other_map_pending = {
        "map_num": exact_map + 1 if exact_map < 5 else exact_map - 1,
        "state": "temporarily_closed_wait",
        "created_at": 1_700_002_203.0,
        "updated_at": 1_700_002_203.0,
        "deadline_at": 1_700_002_293.0,
    }
    tokenless_non_wait_states = {
        "prepared": {
            "map_num": exact_map,
            "state": "prepared",
            "created_at": 1_700_002_204.0,
            "updated_at": 1_700_002_204.0,
            "deadline_at": 1_700_002_294.0,
        },
        "reserved": {
            "map_num": exact_map,
            "state": "reserved",
            "created_at": 1_700_002_205.0,
            "updated_at": 1_700_002_205.0,
            "deadline_at": 1_700_002_295.0,
        },
        "sent": {
            "map_num": exact_map,
            "state": "sent",
            "created_at": 1_700_002_206.0,
            "updated_at": 1_700_002_206.0,
            "deadline_at": 1_700_002_296.0,
        },
        "terminal_skip": {
            "map_num": exact_map,
            "state": "terminal_skip",
            "created_at": 1_700_002_207.0,
            "updated_at": 1_700_002_207.0,
            "deadline_at": 1_700_002_297.0,
        },
        # fail-closed for any other non-wait state
        "open_valid_odds": {
            "map_num": exact_map,
            "state": "open_valid_odds",
            "created_at": 1_700_002_208.0,
            "updated_at": 1_700_002_208.0,
            "deadline_at": 1_700_002_298.0,
        },
    }

    # 1) exact-map tokenless waiting: cleanup succeeds
    _seed_pending(match_key, tokenless_waiting)
    run_site(exact_map=exact_map)
    with cs.bookmaker_odds_delivery_pending_lock:
        assert match_key not in cs.bookmaker_odds_delivery_pending

    # 2) owner-token prepared: preserve complete pending+reservation snapshot
    before = _seed_pending(match_key, owner_prepared)
    run_site(exact_map=exact_map)
    assert _snapshot_pending(match_key) == before

    # 3) pending without map: preserve
    before = _seed_pending(match_key, missing_map_pending)
    run_site(exact_map=exact_map)
    assert _snapshot_pending(match_key) == before

    # 4) mismatched map: preserve
    before = _seed_pending(match_key, other_map_pending)
    run_site(exact_map=exact_map)
    assert _snapshot_pending(match_key) == before

    # 5) exact-map tokenless non-wait states must fail closed (not deleted)
    for state_name, seed in tokenless_non_wait_states.items():
        before = _seed_pending(match_key, seed)
        run_site(exact_map=exact_map)
        after = _snapshot_pending(match_key)
        assert after == before, f"tokenless non-wait state {state_name!r} must be preserved"
        assert after.get("state") == state_name
        with cs.bookmaker_odds_delivery_pending_lock:
            assert match_key in cs.bookmaker_odds_delivery_pending


def _assert_finished_cleanup_four_cases(
    *,
    run_site,
    match_key: str,
    exact_map: int,
) -> None:
    """Backward-compatible alias used by existing branch tests."""
    _assert_finished_cleanup_waiting_only_matrix(
        run_site=run_site,
        match_key=match_key,
        exact_map=exact_map,
    )


def test_clear_pending_waiting_only_helper_matrix(monkeypatch) -> None:
    """Direct helper: only exact-map tokenless temporarily_closed_wait is deleted."""
    match_key = "https://cyberscore.live/matches/w2-i3-clear-helper-9602"
    exact_map = 2

    def _run_site(*, exact_map: int) -> None:
        cs._bookmaker_clear_odds_delivery_pending(match_key, map_num=exact_map)

    _assert_finished_cleanup_waiting_only_matrix(
        run_site=_run_site,
        match_key=match_key,
        exact_map=exact_map,
    )


def test_delayed_finished_waiting_only_cleanup(monkeypatch, tmp_path) -> None:
    """Actual delayed drain finished branch: only exact-map tokenless waiting clears."""
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    map_id_check_path = tmp_path / "map_id_check.txt"
    monkeypatch.setattr(cs, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(cs, "MAP_ID_CHECK_PATH", str(map_id_check_path), raising=False)
    monkeypatch.setattr(cs, "TEST_DISABLE_ADD_URL", False, raising=False)
    _patch_production_delivery_env(monkeypatch, tmp_path)

    clock = {"now": 1_700_002_300.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])
    monkeypatch.setattr(cs, "_is_url_processed", lambda *_a, **_k: False)
    monkeypatch.setattr(cs, "_acquire_signal_send_slot", lambda *_a, **_k: True)
    monkeypatch.setattr(cs, "_release_signal_send_slot", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_refresh_stake_multiplier_message", lambda message, **_k: message)
    monkeypatch.setattr(cs, "_log_bookmaker_source_snapshot", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_maybe_strip_early_kills_header_late", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_bookmaker_release_match_tabs", lambda *_a, **_k: None)
    send_calls: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else "")))
    monkeypatch.setattr(cs, "add_url", lambda *a, **k: None)

    match_key = "https://cyberscore.live/matches/w2-2d-delayed-finished-9503"
    exact_map = 2

    def _run_site(*, exact_map: int) -> None:
        send_calls.clear()
        with cs.monitored_matches_lock:
            cs.monitored_matches.clear()
        monkeypatch.setattr(
            cs,
            "_fetch_delayed_match_state",
            lambda _json_url: {
                "game_time": 720.0,
                "radiant_lead": 100.0,
                "map_num": exact_map,
                "status": "finished",
                "observed_at": clock["now"],
            },
        )
        cs._set_delayed_match(
            match_key,
            {
                "message": "СТАВКА НА team1 x1\n\nБукмекеры: n/a",
                "reason": "late_only",
                "json_url": "https://dltv.org/live/w2-2d-delayed-finished-9503.json",
                "target_game_time": 720.0,
                "queued_at": clock["now"] - 100.0,
                "queued_game_time": 600.0,
                "last_game_time": 600.0,
                "last_progress_at": clock["now"] - 100.0,
                "add_url_reason": "star_signal_sent_delayed",
                "add_url_details": {"status": "ok", "dispatch_mode": "delayed_unit"},
                "fallback_send_status_label": "late_fallback_20_20_send",
                "allow_live_recheck": False,
                "retry_attempt_count": 0,
                "next_retry_at": 0.0,
            },
        )
        cs._drain_due_delayed_signals_once(only_match_key=match_key)
        assert send_calls == []

    _assert_finished_cleanup_four_cases(
        run_site=_run_site,
        match_key=match_key,
        exact_map=exact_map,
    )


def test_listing_finished_waiting_only_cleanup(monkeypatch) -> None:
    """Actual check_head listing-finished branch: observation identity only, fail-closed."""
    slug = "w2-2d-listing-finished-9504"
    # score 1:1 => finished_map inferred as 2
    exact_map = 2
    heads, bodies = _build_legacy_finished_listing(
        slug=slug, score_left=1, score_right=1
    )
    listing_match_key = f"dltv.org/matches/{slug}.{exact_map}"
    _patch_check_head_finished_harness(monkeypatch, applied_update=None)

    def _run_site(*, exact_map: int) -> None:  # noqa: ARG001
        cs.check_head(heads, bodies, 0, set())

    _assert_finished_cleanup_four_cases(
        run_site=_run_site,
        match_key=listing_match_key,
        exact_map=exact_map,
    )


def test_applied_map_finished_waiting_only_cleanup(monkeypatch) -> None:
    """Actual check_head applied-map-finished branch: observation identity only, fail-closed."""
    slug = "w2-2d-applied-finished-9505"
    exact_map = 3
    heads, bodies = _build_legacy_finished_listing(
        slug=slug, score_left=2, score_right=1
    )
    # listing uniq_score = 3; applied path uses applied_update map_key
    applied_match_key = f"dltv.org/matches/{slug}.applied-map"
    _patch_check_head_finished_harness(
        monkeypatch,
        applied_update={
            "map_key": applied_match_key,
            "map_num": exact_map,
        },
    )

    def _run_site(*, exact_map: int) -> None:  # noqa: ARG001
        cs.check_head(heads, bodies, 0, set())

    _assert_finished_cleanup_four_cases(
        run_site=_run_site,
        match_key=applied_match_key,
        exact_map=exact_map,
    )



# ---------------------------------------------------------------------------
# Selected-side Winline coefficient product contract (strict RED until plumbing)
# ---------------------------------------------------------------------------
# Product line target: exactly one "Кэф Winline: <decimal>" from the same
# observation that authorizes reserve/send. Mapping: team1/radiant -> odds[0],
# team2/dire -> odds[1]. Dual "Winline П1/П2" must not coexist for concrete bets.


_SELECTED_WINLINE_PREFIX = "Кэф Winline:"
_LEGACY_DUAL_MARKERS = ("Winline П1", "Winline П2", "П1 ", " / П2 ")


def _assert_exact_selected_winline_coefficient(message: str, *, expected_line: str, opposite_decimal: str) -> None:
    text = str(message)
    assert text.count(_SELECTED_WINLINE_PREFIX) == 1, text
    assert text.count(expected_line) == 1, text
    assert expected_line in text
    # Opposite decimal must not appear as the selected coefficient line.
    opposite_line = f"{_SELECTED_WINLINE_PREFIX} {opposite_decimal}"
    assert opposite_line not in text
    # Legacy dual footer must not be present for the concrete selected-side bet.
    assert "Winline П1" not in text
    assert "Winline П2" not in text
    assert " / П2 " not in text


def _selected_side_aliases(side: str) -> tuple:
    """Canonical side vocab used by concrete-bet routes (radiant|dire|team1|team2|p1|p2)."""
    s = str(side).strip().lower()
    if s in {"team1", "radiant", "p1", "1"}:
        return ("team1", "radiant", "p1")
    if s in {"team2", "dire", "p2", "2"}:
        return ("team2", "dire", "p2")
    raise AssertionError(f"unexpected selected side fixture value: {side!r}")


def _invoke_selected_side_delivery(
    *,
    match_key: str,
    base_msg: str,
    selected_side: str,
    observation: Dict[str, Any],
    map_num: int = 2,
    add_url_reason: str = "unit_selected_winline_coefficient",
) -> bool:
    """Call deliver with selected_side if production accepts it; else fall back.

    The contract requires selected-side plumbing into prepare/format. Until that
    exists TypeError/unexpected kwargs means the product line cannot be selected.
    """
    kwargs = dict(
        add_url_reason=add_url_reason,
        add_url_details={"status": "ok", "target_side": selected_side},
        current_map_observation=observation,
        map_num=map_num,
        selected_side=selected_side,
    )
    try:
        return cs._deliver_and_persist_signal(match_key, base_msg, **kwargs)
    except TypeError:
        # Production currently has no selected_side parameter (EVIDENCE-A).
        # Call without it so the suite exercises the real dual-footer path and
        # fails the product-line assertions below rather than erroring on kwargs.
        kwargs.pop("selected_side", None)
        return cs._deliver_and_persist_signal(match_key, base_msg, **kwargs)


def _invoke_selected_side_prepare(
    *,
    match_key: str,
    base_msg: str,
    selected_side: str,
    observation: Dict[str, Any],
    map_num: int = 2,
):
    kwargs = dict(
        current_map_observation=observation,
        map_num=map_num,
        selected_side=selected_side,
    )
    try:
        return cs._bookmaker_prepare_message_for_delivery(match_key, base_msg, **kwargs)
    except TypeError:
        kwargs.pop("selected_side", None)
        return cs._bookmaker_prepare_message_for_delivery(match_key, base_msg, **kwargs)


@pytest.mark.parametrize(
    ("selected_side", "expected_line", "opposite_decimal"),
    [
        ("team1", "Кэф Winline: 1.52", "2.45"),
        ("radiant", "Кэф Winline: 1.52", "2.45"),
        ("team2", "Кэф Winline: 2.45", "1.52"),
        ("dire", "Кэф Winline: 2.45", "1.52"),
    ],
)
def test_selected_winline_coefficient_immediate_exact_line(
    monkeypatch, tmp_path, selected_side: str, expected_line: str, opposite_decimal: str
) -> None:
    """Immediate deliver seam: exact one selected-side coefficient from authorizing observation."""
    _patch_production_delivery_env(monkeypatch, tmp_path)
    match_key = f"https://cyberscore.live/matches/sel-winline-imm-{selected_side}-9401"
    _clear_delivery_state(match_key)
    _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2, [1.52, 2.45]))

    send_calls: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else "")) or True)
    monkeypatch.setattr(cs, "add_url", lambda *a, **k: None)

    obs = _fresh_current_map_observation(match_key=match_key, map_num=2)
    base_msg = f"СТАВКА НА {selected_side} x1\n\nБукмекеры: n/a"
    ok = _invoke_selected_side_delivery(
        match_key=match_key,
        base_msg=base_msg,
        selected_side=selected_side,
        observation=obs,
        map_num=2,
        add_url_reason="unit_selected_winline_immediate",
    )
    assert ok is True
    assert len(send_calls) == 1
    _assert_exact_selected_winline_coefficient(
        send_calls[0], expected_line=expected_line, opposite_decimal=opposite_decimal
    )
    with cs.bookmaker_odds_delivery_pending_lock:
        pending = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending.get("state") == "sent"


@pytest.mark.parametrize(
    ("selected_side", "expected_line", "opposite_decimal", "odds"),
    [
        ("radiant", "Кэф Winline: 1.71", "2.11", [1.71, 2.11]),
        ("dire", "Кэф Winline: 2.11", "1.71", [1.71, 2.11]),
    ],
)
def test_selected_winline_coefficient_delayed_exact_line(
    monkeypatch, tmp_path, selected_side: str, expected_line: str, opposite_decimal: str, odds: List[float]
) -> None:
    """Delayed queue/flush seam: coefficient from the observation authorizing that flush send."""
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    map_id_check_path = tmp_path / "map_id_check.txt"
    monkeypatch.setattr(cs, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(cs, "MAP_ID_CHECK_PATH", str(map_id_check_path), raising=False)
    monkeypatch.setattr(cs, "TEST_DISABLE_ADD_URL", False, raising=False)
    _patch_production_delivery_env(monkeypatch, tmp_path)
    monkeypatch.setattr(cs.time, "time", lambda: 1_700_000_500.0)

    match_key = f"https://cyberscore.live/matches/sel-winline-del-{selected_side}-9402"
    _clear_delivery_state(match_key)
    _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2, list(odds)))

    send_calls: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else "")) or True)
    monkeypatch.setattr(cs, "add_url", lambda *a, **k: None)
    monkeypatch.setattr(cs, "_acquire_signal_send_slot", lambda *_a, **_k: True)
    monkeypatch.setattr(cs, "_release_signal_send_slot", lambda *_a, **_k: None)
    monkeypatch.setattr(
        cs,
        "_fetch_delayed_match_state",
        lambda _json_url: {
            "game_time": float(cs.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "radiant_lead": 0.0,
        },
    )
    monkeypatch.setattr(cs, "_refresh_stake_multiplier_message", lambda message, **_k: message)
    monkeypatch.setattr(cs, "_log_bookmaker_source_snapshot", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_maybe_strip_early_kills_header_late", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_bookmaker_release_match_tabs", lambda *_a, **_k: None)

    # Intercept deliver to inject selected_side if production accepts it.
    real_deliver = cs._deliver_and_persist_signal

    def _deliver_with_side(match_key_arg, message_text, **kwargs):
        kwargs = dict(kwargs)
        kwargs.setdefault("selected_side", selected_side)
        details = dict(kwargs.get("add_url_details") or {})
        details.setdefault("target_side", selected_side)
        kwargs["add_url_details"] = details
        try:
            return real_deliver(match_key_arg, message_text, **kwargs)
        except TypeError:
            kwargs.pop("selected_side", None)
            return real_deliver(match_key_arg, message_text, **kwargs)

    monkeypatch.setattr(cs, "_deliver_and_persist_signal", _deliver_with_side)

    with cs.monitored_matches_lock:
        cs.monitored_matches.clear()

    obs = _fresh_current_map_observation(match_key=match_key, map_num=2)
    cs._set_delayed_match(
        match_key,
        {
            "message": f"СТАВКА НА {selected_side} x1\n\nБукмекеры: n/a",
            "reason": "late_only",
            "json_url": f"https://dltv.org/live/sel-winline-del-{selected_side}-9402.json",
            "target_game_time": float(cs.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 1_699_999_500.0,
            "queued_game_time": 1100.0,
            "last_game_time": 1100.0,
            "last_progress_at": 1_699_999_500.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {"status": "ok", "dispatch_mode": "delayed_unit", "target_side": selected_side},
            "fallback_send_status_label": "late_fallback_20_20_send",
            "allow_live_recheck": False,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
            "current_map_observation": obs,
            "stake_multiplier_context": {"target_side": selected_side},
            "networth_target_side": selected_side,
        },
    )
    cs._drain_due_delayed_signals_once()
    assert len(send_calls) == 1
    _assert_exact_selected_winline_coefficient(
        send_calls[0], expected_line=expected_line, opposite_decimal=opposite_decimal
    )


def test_selected_winline_coefficient_uses_authorizing_observation_not_later_snapshot(
    monkeypatch, tmp_path
) -> None:
    """Coefficient must come from the observation that reserved the send, not a later seed."""
    _patch_production_delivery_env(monkeypatch, tmp_path)
    match_key = "https://cyberscore.live/matches/sel-winline-auth-obs-9403"
    _clear_delivery_state(match_key)

    # Authorizing observation odds (what prepare/reserve should bind).
    _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2, [1.52, 2.45]))

    send_calls: List[str] = []
    real_prepare = cs._bookmaker_prepare_message_for_delivery

    def _prepare_then_mutate(key, message, **kwargs):
        out = real_prepare(key, message, **kwargs)
        # After reservation, mutate snapshot to different decimals — delivered
        # message must still reflect the authorizing observation (1.52), not 9.99.
        _seed_prefetch(key, map_num=2, sites=_open_winline_sites(2, [9.99, 8.88]))
        return out

    monkeypatch.setattr(cs, "_bookmaker_prepare_message_for_delivery", _prepare_then_mutate)
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else "")) or True)
    monkeypatch.setattr(cs, "add_url", lambda *a, **k: None)

    obs = _fresh_current_map_observation(match_key=match_key, map_num=2)
    ok = _invoke_selected_side_delivery(
        match_key=match_key,
        base_msg="СТАВКА НА radiant x1\n\nБукмекеры: n/a",
        selected_side="radiant",
        observation=obs,
        add_url_reason="unit_selected_winline_auth_obs",
    )
    assert ok is True
    assert len(send_calls) == 1
    _assert_exact_selected_winline_coefficient(
        send_calls[0], expected_line="Кэф Winline: 1.52", opposite_decimal="2.45"
    )
    assert "9.99" not in send_calls[0]
    assert "8.88" not in send_calls[0]


@pytest.mark.parametrize(
    "case_name",
    [
        "missing_snapshot",
        "stale_open",
        "closed_market",
        "wrong_map",
        "wrong_match",
        "unmappable_side",
        "observation_side_mismatch_status_finished",
    ],
)
def test_selected_winline_coefficient_fail_closed_no_send(
    monkeypatch, tmp_path, case_name: str
) -> None:
    """Invalid/stale/closed/wrong-map/wrong-match/unmappable must not send a fabricated coeff."""
    _patch_production_delivery_env(monkeypatch, tmp_path)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_000_600.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    match_key = f"https://cyberscore.live/matches/sel-winline-fc-{case_name}-9404"
    _clear_delivery_state(match_key)

    send_calls: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else "")) or True)
    monkeypatch.setattr(cs, "add_url", lambda *a, **k: None)

    selected_side = "radiant"
    base_msg = "СТАВКА НА radiant x1\n\nБукмекеры: n/a"
    obs = _fresh_current_map_observation(match_key=match_key, map_num=2, observed_at=clock["now"])
    pending_before = None

    if case_name == "missing_snapshot":
        with cs.bookmaker_prefetch_condition:
            cs.bookmaker_prefetch_results.pop(match_key, None)
    elif case_name == "stale_open":
        _seed_prefetch_with_refresh(
            match_key,
            map_num=2,
            sites=_open_winline_sites(2, [1.52, 2.45]),
            odds_refreshed_at=clock["now"] - 60.0,
        )
    elif case_name == "closed_market":
        _seed_prefetch(match_key, map_num=2, sites=_closed_winline_sites(2))
    elif case_name == "wrong_map":
        _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2, [1.52, 2.45]))
        obs = _fresh_current_map_observation(match_key=match_key, map_num=3, observed_at=clock["now"])
    elif case_name == "wrong_match":
        other_key = match_key + "-other"
        _seed_prefetch(other_key, map_num=2, sites=_open_winline_sites(2, [1.52, 2.45]))
        # observation claims a different match_key than the delivery key
        obs = _fresh_current_map_observation(match_key=other_key, map_num=2, observed_at=clock["now"])
    elif case_name == "unmappable_side":
        _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2, [1.52, 2.45]))
        selected_side = "midlane"  # not radiant|dire|team1|team2
        base_msg = "СТАВКА НА midlane x1\n\nБукмекеры: n/a"
    elif case_name == "observation_side_mismatch_status_finished":
        _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2, [1.52, 2.45]))
        obs = _fresh_current_map_observation(
            match_key=match_key, map_num=2, status="finished", observed_at=clock["now"]
        )
    else:
        raise AssertionError(case_name)

    with cs.bookmaker_odds_delivery_pending_lock:
        pending_before = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})

    ok = _invoke_selected_side_delivery(
        match_key=match_key,
        base_msg=base_msg,
        selected_side=selected_side,
        observation=obs,
        map_num=int(obs.get("map_num") or 2),
        add_url_reason=f"unit_selected_winline_fc_{case_name}",
    )
    assert ok is False
    assert send_calls == []
    # Must not fabricate selected coefficient anywhere in prepare path either.
    msg, ready, reason, reservation = _invoke_selected_side_prepare(
        match_key=match_key,
        base_msg=base_msg,
        selected_side=selected_side,
        observation=obs,
        map_num=int(obs.get("map_num") or 2),
    )
    assert ready is False
    assert _SELECTED_WINLINE_PREFIX not in str(msg)
    assert "Кэф Winline: 2.45" not in str(msg)  # opposite / fabricated
    # Reservation must not progress to prepared/sent for these fail-closed cases.
    with cs.bookmaker_odds_delivery_pending_lock:
        pending_after = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending_after.get("state") != "sent"
    if case_name in {"wrong_map", "wrong_match", "observation_side_mismatch_status_finished", "missing_snapshot"}:
        assert pending_after.get("token") in (None, "")
        assert pending_after.get("state") != "prepared"


def test_selected_winline_coefficient_absent_side_fail_closed_no_send(
    monkeypatch, tmp_path
) -> None:
    """Concrete team-side bet without selected_side must not send dual/guessed coefficient."""
    _patch_production_delivery_env(monkeypatch, tmp_path)
    match_key = "https://cyberscore.live/matches/sel-winline-absent-side-9405"
    _clear_delivery_state(match_key)
    _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2, [1.52, 2.45]))

    send_calls: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else "")) or True)
    monkeypatch.setattr(cs, "add_url", lambda *a, **k: None)

    obs = _fresh_current_map_observation(match_key=match_key, map_num=2)
    base_msg = "СТАВКА НА team1 x1\n\nБукмекеры: n/a"

    try:
        ok = cs._deliver_and_persist_signal(
            match_key,
            base_msg,
            add_url_reason="unit_selected_winline_absent",
            add_url_details={"status": "ok"},
            current_map_observation=obs,
            map_num=2,
            selected_side=None,
        )
    except TypeError:
        # No selected_side param yet: current production dual-sends without side.
        ok = cs._deliver_and_persist_signal(
            match_key,
            base_msg,
            add_url_reason="unit_selected_winline_absent",
            add_url_details={"status": "ok"},
            current_map_observation=obs,
            map_num=2,
        )

    # Contract: concrete bet without reliable selected_side must not reach sender
    # with dual footer or fabricated line. Today dual-send still happens → RED.
    if ok is True and send_calls:
        text = send_calls[0]
        assert text.count(_SELECTED_WINLINE_PREFIX) == 1, (
            "absent selected_side must not deliver dual/legacy footer; "
            f"got: {text!r}"
        )
        assert "Winline П1" not in text
        assert " / П2 " not in text
    else:
        assert ok is False
        assert send_calls == []


def test_selected_winline_coefficient_disabled_and_no_odds_modes_unchanged(
    monkeypatch, tmp_path
) -> None:
    """Winline-disabled / no-odds modes: no product line, preserve current send/withhold."""
    # 1) Prefetch disabled: prepare early-returns ready=True, message byte-identical, no product line.
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    match_key = "https://cyberscore.live/matches/sel-winline-disabled-9406"
    original = "body without bookmaker\n\nБукмекеры: n/a"
    msg, ready, reason, reservation = cs._bookmaker_prepare_message_for_delivery(
        match_key,
        original,
        current_map_observation=_fresh_current_map_observation(match_key=match_key, map_num=2),
        map_num=2,
    )
    assert ready is True
    assert reason == "disabled"
    assert reservation is None
    assert msg == original
    assert _SELECTED_WINLINE_PREFIX not in msg
    assert "Winline П1" not in msg

    # 2) Presence gate mode: odds product line must not appear (format path is presence, not selected coeff).
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "presence")
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    match_key2 = "https://cyberscore.live/matches/sel-winline-presence-9407"
    _seed_prefetch(
        match_key2,
        map_num=2,
        sites={
            "winline": {
                "match_found": True,
                "odds": [1.52, 2.45],
                "market_closed": False,
                "market_kind": "current_map_winner",
                "map_num": 2,
                "p1_team": "team1",
                "p2_team": "team2",
                "source": "deeplink_map_market",
            }
        },
    )
    msg2, ready2, reason2, reservation2 = cs._bookmaker_prepare_message_for_delivery(
        match_key2,
        original,
        current_map_observation=_fresh_current_map_observation(match_key=match_key2, map_num=2),
        map_num=2,
    )
    # presence mode is treated as disabled for odds prepare contract
    assert ready2 is True
    assert reason2 == "disabled"
    assert reservation2 is None
    assert msg2 == original
    assert _SELECTED_WINLINE_PREFIX not in msg2

    # 3) Deliver path with skip_bookmaker_prepare (non-bet / diagnostic style): no product line added.
    _patch_production_delivery_env(monkeypatch, tmp_path)
    match_key3 = "https://cyberscore.live/matches/sel-winline-skip-prepare-9408"
    _clear_delivery_state(match_key3)
    _seed_prefetch(match_key3, map_num=2, sites=_open_winline_sites(2, [1.52, 2.45]))
    send_calls: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else "")) or True)
    monkeypatch.setattr(cs, "add_url", lambda *a, **k: None)
    non_bet = "pipeline diagnostic message\n\nБукмекеры: n/a"
    ok = cs._deliver_and_persist_signal(
        match_key3,
        non_bet,
        add_url_reason="pipeline_send_every_parsed_match",
        add_url_details={"status": "ok"},
        skip_bookmaker_prepare=True,
        current_map_observation=_fresh_current_map_observation(match_key=match_key3, map_num=2),
        map_num=2,
    )
    assert ok is True
    assert len(send_calls) == 1
    assert send_calls[0] == non_bet or _SELECTED_WINLINE_PREFIX not in send_calls[0]
    assert _SELECTED_WINLINE_PREFIX not in send_calls[0]
    assert "Winline П1" not in send_calls[0]


def test_selected_winline_coefficient_prepare_exact_line_radiant_and_dire(
    monkeypatch, tmp_path
) -> None:
    """Prepare seam alone must render exact selected line (no dual footer)."""
    _patch_production_delivery_env(monkeypatch, tmp_path)
    for side, expected, opposite in (
        ("radiant", "Кэф Winline: 1.52", "2.45"),
        ("dire", "Кэф Winline: 2.45", "1.52"),
        ("team1", "Кэф Winline: 1.52", "2.45"),
        ("team2", "Кэф Winline: 2.45", "1.52"),
    ):
        match_key = f"https://cyberscore.live/matches/sel-winline-prep-{side}-9409"
        _clear_delivery_state(match_key)
        _seed_prefetch(match_key, map_num=2, sites=_open_winline_sites(2, [1.52, 2.45]))
        base_msg = f"СТАВКА НА {side} x1\n\nБукмекеры: n/a"
        msg, ready, reason, reservation = _invoke_selected_side_prepare(
            match_key=match_key,
            base_msg=base_msg,
            selected_side=side,
            observation=_fresh_current_map_observation(match_key=match_key, map_num=2),
            map_num=2,
        )
        assert ready is True, (side, reason, msg)
        assert reason == "ok"
        assert reservation is not None
        _assert_exact_selected_winline_coefficient(
            msg, expected_line=expected, opposite_decimal=opposite
        )
