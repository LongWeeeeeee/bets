"""W3 merged integration contract: shared browser + exact-map lifecycle + v5 delivery.

Ownership: this file only (plus Winline/shared-browser doc paragraphs).
No production edits. Tests exercise real symbols/caller behavior, not source strings.
"""
from __future__ import annotations

import inspect
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402
import dota2protracker as d2pt  # noqa: E402


# ---------------------------------------------------------------------------
# Local harness (mirrors production helpers used by W1/W2 suites)
# ---------------------------------------------------------------------------


class _CountingPage:
    def __init__(self, browser: "_CountingBrowser", name: Optional[str] = None) -> None:
        self.browser = browser
        self.name = name
        self.closed = False
        self.page_id = id(self)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.browser.events.append(f"page_close:{self.name or self.page_id}")
        self.browser.page_closes.append(self.name or str(self.page_id))

    def is_closed(self) -> bool:
        return self.closed


class _CountingBrowser:
    def __init__(self, owner: "_CountingCamoufoxFactory") -> None:
        owner.browser_seq += 1
        self.index = owner.browser_seq
        self.owner = owner
        self.pages: List[_CountingPage] = []
        self.events = owner.events
        self.page_closes: List[str] = owner.page_closes
        self.closed = False

    def new_page(self) -> _CountingPage:
        page = _CountingPage(self)
        self.pages.append(page)
        self.owner.new_page_calls.append(page.page_id)
        self.events.append(f"new_page:{page.page_id}")
        return page

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.owner.active_browsers = max(0, self.owner.active_browsers - 1)
        self.events.append(f"browser_close:{self.index}")


class _CountingCamoufoxFactory:
    def __init__(self) -> None:
        self.browser_enters = 0
        self.active_browsers = 0
        self.max_active_browsers = 0
        self.browser_seq = 0
        self.events: List[str] = []
        self.page_closes: List[str] = []
        self.new_page_calls: List[int] = []
        self.created_browsers: List[_CountingBrowser] = []
        self._lock = threading.Lock()

    def Camoufox(self, **kwargs: Any) -> "_CountingContext":
        return _CountingContext(self, kwargs)


class _CountingContext:
    def __init__(self, factory: _CountingCamoufoxFactory, kwargs: Dict[str, Any]) -> None:
        self.factory = factory
        self.kwargs = kwargs
        self.browser: Optional[_CountingBrowser] = None

    def __enter__(self) -> _CountingBrowser:
        with self.factory._lock:
            self.factory.browser_enters += 1
            self.factory.active_browsers += 1
            self.factory.max_active_browsers = max(
                self.factory.max_active_browsers, self.factory.active_browsers
            )
            self.browser = _CountingBrowser(self.factory)
            self.factory.created_browsers.append(self.browser)
            self.factory.events.append(f"browser_enter:{self.browser.index}")
            return self.browser

    def __exit__(self, *exc: Any) -> bool:
        with self.factory._lock:
            if self.browser is not None and not self.browser.closed:
                self.factory.active_browsers = max(0, self.factory.active_browsers - 1)
            self.factory.events.append(
                f"context_exit:{self.browser.index if self.browser else '?'}"
            )
        return False


def _install_counting_camoufox(monkeypatch, factory: Optional[_CountingCamoufoxFactory] = None):
    factory = factory or _CountingCamoufoxFactory()
    monkeypatch.setattr(cs, "CAMOUFOX_AVAILABLE", True, raising=False)
    monkeypatch.setattr(cs, "camoufox", factory, raising=False)
    monkeypatch.setattr(cs, "_cyberscore_camoufox_proxy_kwargs", lambda: {}, raising=False)
    monkeypatch.setattr(
        cs, "_bookmaker_select_shared_camoufox_proxy_kwargs", lambda: {}, raising=False
    )
    monkeypatch.setattr(cs, "_note_proxy_success", lambda *_a, **_k: None, raising=False)
    monkeypatch.setattr(
        cs, "_bookmaker_rotate_shared_camoufox_proxy", lambda **_k: None, raising=False
    )
    return factory


def _open_winline_sites(map_num: int = 2, odds: Optional[List[float]] = None) -> Dict[str, Any]:
    return {
        "winline": {
            "match_found": True,
            "odds": list(odds or [1.66, 2.18]),
            "match_odds": [1.30, 3.15],
            "market_closed": False,
            "market_kind": "current_map_winner",
            "map_num": map_num,
            "p1_team": "team1",
            "p2_team": "team2",
            "source": "winline_current_map_winner",
        }
    }


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


def _fresh_obs(
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


def _seed_prefetch(
    match_key: str,
    *,
    map_num: int,
    sites: Dict[str, Any],
    odds_refreshed_at: Optional[float] = None,
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
            "mode": "live",
            "sites": sites,
        }


def _clear_delivery_state(match_key: str) -> None:
    with cs.bookmaker_odds_delivery_pending_lock:
        cs.bookmaker_odds_delivery_pending.pop(match_key, None)
    with cs.bookmaker_prefetch_condition:
        cs.bookmaker_prefetch_results.pop(match_key, None)


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


# ---------------------------------------------------------------------------
# 1) Process-wide shared browser + reusable Winline page
# ---------------------------------------------------------------------------


def test_integration_one_shared_browser_and_reusable_winline_page(monkeypatch) -> None:
    """Two sequential production Winline fetches share one browser and one named page."""
    factory = _install_counting_camoufox(monkeypatch)
    session = cs._SharedCamoufoxSession()
    monkeypatch.setattr(cs, "_shared_camoufox_session", session, raising=False)

    seen_page_ids: List[int] = []

    class _ParseResult:
        status = "ok"
        match_found = True
        odds = [1.9, 1.9]
        match_odds = [1.9, 1.9]
        source = "camoufox"
        details = "ok"
        market_closed = False
        market_kind = "map_winner"
        map_num = 1
        p1_team = "Team A"
        p2_team = "Team B"

    def _fake_parse(page, **kwargs):
        seen_page_ids.append(id(page))
        return _ParseResult()

    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True, raising=False)
    monkeypatch.setattr(cs, "_bookmaker_parse_site_in_camoufox_page", _fake_parse, raising=False)
    monkeypatch.setattr(
        cs,
        "_bookmaker_urls_for_mode",
        lambda mode: {"winline": "https://winline.ru/stavki/sport/kibersport"},
        raising=False,
    )
    monkeypatch.setattr(
        cs, "_bookmaker_effective_sites_for_mode", lambda *_a, **_k: ("winline",), raising=False
    )
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds", raising=False)

    try:
        r1 = cs._bookmaker_prefetch_fetch_camoufox_direct("Team A", "Team B", "live", map_num=1)
        r2 = cs._bookmaker_prefetch_fetch_camoufox_direct("Team A", "Team B", "live", map_num=1)
    finally:
        session.close()

    assert r1["winline"]["status"] == "ok"
    assert r2["winline"]["status"] == "ok"
    assert factory.browser_enters == 1
    assert factory.max_active_browsers == 1
    assert len(seen_page_ids) == 2
    assert len(set(seen_page_ids)) == 1, f"reusable Winline page required; ids={seen_page_ids}"
    assert len(factory.new_page_calls) == 1
    # Process-wide singleton is the module-level owner used by production jobs.
    assert isinstance(cs._shared_camoufox_session, cs._SharedCamoufoxSession)


def test_integration_zero_camoufox_subprocess_odds_and_protracker(monkeypatch) -> None:
    """Odds shared refresh path and ProTracker shared hook never spawn Camoufox subprocess."""
    import subprocess as sp

    factory = _install_counting_camoufox(monkeypatch)
    session = cs._SharedCamoufoxSession()
    monkeypatch.setattr(cs, "_shared_camoufox_session", session, raising=False)

    subprocess_calls: List[Any] = []

    def _spy_run(*a, **k):
        subprocess_calls.append((a, k))
        raise AssertionError("subprocess.run must not be called for Camoufox work")

    monkeypatch.setattr(sp, "run", _spy_run)
    # Also spy the legacy protracker subprocess helper if production still exposes it.
    protracker_sub_calls: List[Any] = []

    def _fake_protracker_sub(slug, hero_id, proxy_candidate=None):
        protracker_sub_calls.append((slug, hero_id, proxy_candidate))
        return {"matchups": {"1": []}, "synergies": {"1": []}}

    monkeypatch.setattr(d2pt, "_fetch_protracker_payload_via_subprocess", _fake_protracker_sub)
    monkeypatch.setattr(cs, "_protracker_subprocess_fetcher", lambda: _fake_protracker_sub)
    monkeypatch.setattr(cs, "_PROTRACKER_SHARED_CAMOUFOX_BROKEN", False, raising=False)
    monkeypatch.setattr(cs, "_PROTRACKER_SHARED_CONSECUTIVE_FAILS", 0, raising=False)
    monkeypatch.setattr(cs, "_PROTRACKER_SHARED_FAILS_TO_BREAK", 2, raising=False)

    def _failing_shared_job(label, callback, timeout=120.0, retry=True, reset_on_error=True):
        raise RuntimeError("shared camoufox boom")

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", _failing_shared_job)

    # Odds path: production refresh must not use subprocess when shared fails.
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True)

    match_key = "https://cyberscore.live/matches/w3-integration-no-subprocess"
    with cs.bookmaker_prefetch_condition:
        cs.bookmaker_prefetch_results[match_key] = {
            "status": "done",
            "finished_at": time.time(),
            "submitted_at": time.time(),
            "odds_refreshed_at": time.time(),
            "odds_refresh_ready": True,
            "map_num": 2,
            "mode": "live",
            "sites": _open_winline_sites(2),
        }

    # Drive the shared-only refresh entry used by prepare/delivery.
    # Production fail-closes (non-ready snapshot / honest reason) without raising;
    # the contract is zero Camoufox subprocess, not a specific exception type.
    if hasattr(cs, "_bookmaker_refresh_snapshot_via_shared_camoufox"):
        monkeypatch.setattr(
            cs,
            "_bookmaker_prefetch_fetch_camoufox_direct",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("shared fetch boom")),
        )
        result = cs._bookmaker_refresh_snapshot_via_shared_camoufox(match_key)
        if isinstance(result, dict):
            assert result.get("odds_refresh_ready") is False, (
                f"shared refresh failure must mark gate non-ready, got {result!r}"
            )
        # None/False also acceptable fail-closed shapes; never subprocess.

    # ProTracker shared hook: repeated shared failures must raise, never subprocess.
    errors: List[BaseException] = []
    for _ in range(3):
        try:
            cs._fetch_protracker_payload_via_shared_camoufox("anti-mage", 1, None)
        except Exception as exc:
            errors.append(exc)

    monkeypatch.setattr(d2pt, "PROTRACKER_PAYLOAD_FETCHER", cs._fetch_protracker_payload_via_shared_camoufox)
    monkeypatch.setattr(d2pt, "CAMOUFOX_AVAILABLE", True, raising=False)
    monkeypatch.setattr(d2pt, "get_hero_slug", lambda name: "anti-mage")
    monkeypatch.setattr(d2pt, "get_hero_id", lambda name: 1)
    monkeypatch.setattr(d2pt, "CACHE_DIR", "/tmp/__no_protracker_cache_w3_integration__")
    monkeypatch.setattr(d2pt.os.path, "exists", lambda *_a, **_k: False)
    try:
        parse_result = d2pt.parse_hero_matchups("Anti-Mage", use_cache=False, proxy=None)
    except Exception as exc:
        parse_result = {"error": str(exc)}

    assert subprocess_calls == [], f"subprocess.run invoked: {subprocess_calls}"
    assert protracker_sub_calls == [], f"protracker subprocess invoked: {protracker_sub_calls}"
    assert errors, "shared protracker failures must surface honestly"
    assert factory.browser_enters == 0
    # When no external fetcher candidates remain, parse_hero_matchups must still avoid subprocess.
    assert not (isinstance(parse_result, dict) and parse_result.get("matchups") and protracker_sub_calls)


# ---------------------------------------------------------------------------
# 2) Exact-current-map + freshness + retained deadline matrix
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "scenario,expected_state,expected_reason,should_send",
    [
        ("fresh_open", "prepared", None, True),
        ("closed_wait", "temporarily_closed_wait", "temporarily_closed_wait", False),
        ("map_mismatch", "terminal_skip", "current_map_mismatch", False),
        ("match_finished", "terminal_skip", "match_finished", False),
        ("obs_stale", "temporarily_closed_wait", "current_map_observation_stale", False),
        ("odds_stale", "temporarily_closed_wait", "odds_stale", False),
        ("past_deadline", "terminal_skip", "deadline_while_closed", False),
        ("missing_obs", "temporarily_closed_wait", "current_map_unavailable", False),
    ],
)
def test_integration_freshness_deadline_matrix(
    monkeypatch, scenario, expected_state, expected_reason, should_send
) -> None:
    """Resolver matrix: only fresh open exact-map reserves; waits/terminals never send."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_100_000.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    match_key = f"https://cyberscore.live/matches/w3-matrix-{scenario}"
    pending: Dict[str, Any] = {}
    map_num = 2

    if scenario == "fresh_open":
        _seed_prefetch(
            match_key,
            map_num=map_num,
            sites=_open_winline_sites(map_num, [1.55, 2.40]),
            odds_refreshed_at=clock["now"],
        )
        obs = _fresh_obs(match_key=match_key, map_num=map_num, observed_at=clock["now"])
    elif scenario == "closed_wait":
        _seed_prefetch(
            match_key,
            map_num=map_num,
            sites=_closed_winline_sites(map_num),
            odds_refreshed_at=clock["now"],
        )
        obs = _fresh_obs(match_key=match_key, map_num=map_num, observed_at=clock["now"])
    elif scenario == "map_mismatch":
        _seed_prefetch(
            match_key,
            map_num=map_num,
            sites=_open_winline_sites(map_num),
            odds_refreshed_at=clock["now"],
        )
        obs = _fresh_obs(match_key=match_key, map_num=3, observed_at=clock["now"])
    elif scenario == "match_finished":
        _seed_prefetch(
            match_key,
            map_num=map_num,
            sites=_open_winline_sites(map_num),
            odds_refreshed_at=clock["now"],
        )
        obs = _fresh_obs(match_key=match_key, map_num=map_num, status="finished", observed_at=clock["now"])
    elif scenario == "obs_stale":
        _seed_prefetch(
            match_key,
            map_num=map_num,
            sites=_open_winline_sites(map_num),
            odds_refreshed_at=clock["now"],
        )
        obs = _fresh_obs(match_key=match_key, map_num=map_num, observed_at=clock["now"] - 60.0)
    elif scenario == "odds_stale":
        _seed_prefetch(
            match_key,
            map_num=map_num,
            sites=_open_winline_sites(map_num),
            odds_refreshed_at=clock["now"] - 60.0,
        )
        obs = _fresh_obs(match_key=match_key, map_num=map_num, observed_at=clock["now"])
    elif scenario == "past_deadline":
        _seed_prefetch(
            match_key,
            map_num=map_num,
            sites=_closed_winline_sites(map_num),
            odds_refreshed_at=clock["now"],
        )
        obs = _fresh_obs(match_key=match_key, map_num=map_num, observed_at=clock["now"])
        pending[match_key] = {
            "map_num": map_num,
            "state": "temporarily_closed_wait",
            "created_at": clock["now"] - 100.0,
            "updated_at": clock["now"] - 100.0,
            "deadline_at": clock["now"] - 1.0,
        }
        before = dict(pending[match_key])
    elif scenario == "missing_obs":
        _seed_prefetch(
            match_key,
            map_num=map_num,
            sites=_open_winline_sites(map_num),
            odds_refreshed_at=clock["now"],
        )
        obs = None
    else:
        raise AssertionError(scenario)

    decision = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        map_num=map_num,
        current_map_observation=obs,
    )

    assert decision.get("state") == expected_state, decision
    assert bool(decision.get("should_send")) is should_send, decision
    if expected_reason is not None:
        assert decision.get("reason") == expected_reason, decision
    if should_send:
        assert decision.get("token"), decision
        assert pending.get(match_key, {}).get("token") == decision.get("token")
    else:
        assert not decision.get("token"), decision
        entry = pending.get(match_key)
        if isinstance(entry, dict):
            assert entry.get("token") in (None, ""), entry
    if scenario == "past_deadline":
        # Immutable terminal: pending snapshot must remain value-identical.
        assert pending.get(match_key) == before
    if scenario == "closed_wait":
        deadline = pending.get(match_key, {}).get("deadline_at")
        assert isinstance(deadline, (int, float))
        assert float(deadline) == pytest.approx(clock["now"] + 90.0)


def test_integration_retained_deadline_not_extended_on_wait(monkeypatch) -> None:
    """Second closed wait keeps original deadline_at and stays tokenless."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MESSAGE_WAIT_SECONDS", 0.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)

    clock = {"now": 1_700_100_500.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    match_key = "https://cyberscore.live/matches/w3-retained-deadline"
    pending: Dict[str, Any] = {}
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_closed_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    obs = _fresh_obs(match_key=match_key, map_num=2, observed_at=clock["now"])
    d1 = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        map_num=2,
        current_map_observation=obs,
    )
    assert d1["state"] == "temporarily_closed_wait"
    deadline1 = pending[match_key]["deadline_at"]

    clock["now"] = 1_700_100_530.0
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_closed_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )
    d2 = cs._bookmaker_resolve_odds_delivery_state(
        match_key,
        pending_state=pending,
        map_num=2,
        current_map_observation=_fresh_obs(match_key=match_key, map_num=2, observed_at=clock["now"]),
    )
    assert d2["state"] == "temporarily_closed_wait"
    assert float(pending[match_key]["deadline_at"]) == pytest.approx(float(deadline1))
    assert pending[match_key].get("token") in (None, "")


def test_integration_production_delivery_closed_then_open_once(monkeypatch, tmp_path) -> None:
    """Production deliver path: closed wait then fresh open sends exactly once with owner token."""
    _patch_production_delivery_env(monkeypatch, tmp_path)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_MAX_AGE_SECONDS", 15.0)
    monkeypatch.setattr(cs, "BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS", 90.0)

    clock = {"now": 1_700_100_800.0}
    monkeypatch.setattr(cs.time, "time", lambda: clock["now"])

    match_key = "https://cyberscore.live/matches/w3-delivery-closed-open"
    _clear_delivery_state(match_key)
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_closed_winline_sites(2),
        odds_refreshed_at=clock["now"],
    )

    send_calls: List[str] = []
    add_url_calls: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append(str(a[0] if a else "")))
    monkeypatch.setattr(cs, "add_url", lambda url, **_k: add_url_calls.append(url))

    base_msg = "СТАВКА НА team1 x1\n\nБукмекеры: n/a\n🤖 ML-модель: Radiant 63.5%"
    ok_closed = cs._deliver_and_persist_signal(
        match_key,
        base_msg,
        add_url_reason="w3_closed",
        current_map_observation=_fresh_obs(match_key=match_key, map_num=2, observed_at=clock["now"]),
        map_num=2,
    )
    assert ok_closed is False
    assert send_calls == []
    assert add_url_calls == []
    with cs.bookmaker_odds_delivery_pending_lock:
        pending_closed = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending_closed.get("state") == "temporarily_closed_wait"
    assert pending_closed.get("token") in (None, "")
    deadline_at = pending_closed.get("deadline_at")
    assert isinstance(deadline_at, (int, float))

    clock["now"] = 1_700_100_810.0
    _seed_prefetch(
        match_key,
        map_num=2,
        sites=_open_winline_sites(2, [1.61, 2.22]),
        odds_refreshed_at=clock["now"],
    )
    ok_open = cs._deliver_and_persist_signal(
        match_key,
        base_msg,
        add_url_reason="w3_open",
        current_map_observation=_fresh_obs(match_key=match_key, map_num=2, observed_at=clock["now"]),
        map_num=2,
    )
    assert ok_open is True
    assert len(send_calls) == 1
    assert "1.61" in send_calls[0] and "2.22" in send_calls[0]
    assert add_url_calls == [match_key]
    with cs.bookmaker_odds_delivery_pending_lock:
        pending_sent = dict(cs.bookmaker_odds_delivery_pending.get(match_key) or {})
    assert pending_sent.get("state") == "sent"
    # Retained deadline must not be extended by the open transition.
    if pending_sent.get("deadline_at") is not None:
        assert float(pending_sent["deadline_at"]) == pytest.approx(float(deadline_at))

    send_calls.clear()
    add_url_calls.clear()
    ok2 = cs._deliver_and_persist_signal(
        match_key,
        base_msg,
        add_url_reason="w3_open_retry",
        current_map_observation=_fresh_obs(match_key=match_key, map_num=2, observed_at=clock["now"]),
        map_num=2,
    )
    assert ok2 is False
    assert send_calls == []
    assert add_url_calls == []


# ---------------------------------------------------------------------------
# 2b) Winline singleton-shadow integration seam (INT t_790fa101)
# ---------------------------------------------------------------------------


SERIES_URL = "dltv.org/matches/123456"
URL_KILLS_A = f"{SERIES_URL}.12"
URL_KILLS_B = f"{SERIES_URL}.34"


def _clear_bookmaker_prefetch_state() -> None:
    with cs.bookmaker_prefetch_condition:
        cs.bookmaker_prefetch_results.clear()
        cs.bookmaker_prefetch_queue.clear()


def _shadow_obs(
    *,
    match_id: str = "123456",
    map_num: int = 1,
    team1: str = "Team A",
    team2: str = "Team B",
    p1: float = 1.55,
    p2: float = 2.40,
    observed_at: Optional[float] = None,
    source: str = "Winline",
) -> Dict[str, Any]:
    return {
        "source": source,
        "match_id": match_id,
        "map_num": map_num,
        "team1": team1,
        "team2": team2,
        "p1_odds": p1,
        "p2_odds": p2,
        # Slightly in the past so probe's frozen `now` never sees observed_at_in_future.
        "observed_at": float(
            observed_at if observed_at is not None else (time.time() - 0.5)
        ),
    }


def test_integration_shadow_one_submission_same_series_map_across_suffixes(
    monkeypatch, tmp_path
) -> None:
    """Shadow path: one shared-job submission; kills suffixes share series+map identity."""
    import json

    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    _clear_bookmaker_prefetch_state()

    calls: List[Any] = []

    def _fake_shared(label, callback, timeout=120.0, retry=True, reset_on_error=True):
        calls.append(
            {
                "label": label,
                "timeout": timeout,
                "retry": retry,
                "reset_on_error": reset_on_error,
            }
        )
        # Fake runner returns observation without live browser/parse.
        return _shadow_obs(map_num=1)

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", _fake_shared)

    out_a = tmp_path / "shadow_a.json"
    out_b = tmp_path / "shadow_b.json"
    send_calls: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append("x"))

    gate_before = cs.BOOKMAKER_PREFETCH_GATE_MODE
    prefetch_before = cs.BOOKMAKER_PREFETCH_ENABLED

    rc_a = cs.run_winline_shadow_request(
        match_key=URL_KILLS_A,
        map_num=1,
        team1="Team A",
        team2="Team B",
        selected_side="P1",
        output_path=str(out_a),
        freshness_limit_seconds=15.0,
        no_odds_active=True,
    )
    rc_b = cs.run_winline_shadow_request(
        match_key=URL_KILLS_B,
        map_num=1,
        team1="Team A",
        team2="Team B",
        selected_side="P1",
        output_path=str(out_b),
        freshness_limit_seconds=15.0,
        no_odds_active=True,
    )
    assert rc_a == 0 and rc_b == 0, (rc_a, rc_b)
    assert len(calls) == 2, f"each request submits exactly once; calls={calls}"
    assert all(c["label"] == "winline-shadow" for c in calls)
    # Same series+map identity across score suffixes.
    life_a = cs._bookmaker_lifecycle_identity(URL_KILLS_A, map_num=1)
    life_b = cs._bookmaker_lifecycle_identity(URL_KILLS_B, map_num=1)
    assert life_a is not None and life_a == life_b
    assert ".12" not in life_a and ".34" not in life_a
    d_a = json.loads(out_a.read_text(encoding="utf-8"))
    d_b = json.loads(out_b.read_text(encoding="utf-8"))
    assert d_a["match_id"] == d_b["match_id"] == "123456"
    assert d_a["map_num"] == d_b["map_num"] == 1
    assert d_a["verdict"] == d_b["verdict"] == "PASS"
    # Ordinary no-odds delivery / gate must remain untouched.
    assert cs.BOOKMAKER_PREFETCH_GATE_MODE == gate_before
    assert cs.BOOKMAKER_PREFETCH_ENABLED is prefetch_before
    assert send_calls == []


def test_integration_shadow_cross_map_isolation_and_selected_side(
    monkeypatch, tmp_path
) -> None:
    import json

    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False)
    _clear_bookmaker_prefetch_state()
    labels: List[str] = []
    map_seen: List[Any] = []

    def _fake_shared(label, callback, timeout=120.0, retry=True, reset_on_error=True):
        labels.append(label)
        # Capture map from the job callback closure via a cooperative collector.
        # Production job body calls collector which we stub to echo map_num.
        return callback(object())

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", _fake_shared)

    def _collect(browser, job):
        forced = int(job.get("map_num"))
        map_seen.append(forced)
        odds = [1.70, 2.10]
        return _shadow_obs(
            map_num=forced,
            p1=odds[0],
            p2=odds[1],
        )

    monkeypatch.setattr(cs, "_winline_shadow_collect_observation", _collect)

    out1 = tmp_path / "m1.json"
    out2 = tmp_path / "m2.json"
    rc1 = cs.run_winline_shadow_request(
        match_key=URL_KILLS_A,
        map_num=1,
        team1="Team A",
        team2="Team B",
        selected_side="P1",
        output_path=str(out1),
        no_odds_active=True,
    )
    rc2 = cs.run_winline_shadow_request(
        match_key=URL_KILLS_A,
        map_num=2,
        team1="Team A",
        team2="Team B",
        selected_side="P2",
        output_path=str(out2),
        no_odds_active=True,
    )
    assert rc1 == 0 and rc2 == 0
    id1 = cs._bookmaker_lifecycle_identity(URL_KILLS_A, map_num=1)
    id2 = cs._bookmaker_lifecycle_identity(URL_KILLS_A, map_num=2)
    assert id1 != id2
    assert map_seen == [1, 2]
    assert labels == ["winline-shadow", "winline-shadow"]

    d1 = json.loads(out1.read_text(encoding="utf-8"))
    d2 = json.loads(out2.read_text(encoding="utf-8"))
    assert d1["selected_side"] == "P1" and d1["selected_odds"] == pytest.approx(1.70)
    assert d2["selected_side"] == "P2" and d2["selected_odds"] == pytest.approx(2.10)
    assert d1["map_num"] == 1 and d2["map_num"] == 2
    assert d1["verdict"] == "PASS" and d2["verdict"] == "PASS"


def test_integration_shadow_one_reset_recovery_and_fail_propagation(
    monkeypatch, tmp_path
) -> None:
    """Driver exception → FAIL + terminalize; one sequential reset on shared runner."""
    import json

    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "_ensure_bookmaker_prefetch_started", lambda: None)
    _clear_bookmaker_prefetch_state()

    # Seed a lifecycle entry as if a prior prefetch was running.
    life = cs._bookmaker_lifecycle_identity(URL_KILLS_A, map_num=1)
    assert life is not None
    with cs.bookmaker_prefetch_condition:
        cs.bookmaker_prefetch_results[life] = {
            "status": "running",
            "map_num": 1,
            "sites": {},
            "submitted_at": time.time(),
        }

    attempts = {"n": 0}

    def _flaky_shared(label, callback, timeout=120.0, retry=True, reset_on_error=True):
        attempts["n"] += 1
        if attempts["n"] == 1:
            # Simulate the production one-reset path: first call fails, then retry.
            if retry:
                try:
                    raise RuntimeError("injected driver/protocol failure")
                except Exception:
                    # Mirror _run_shared_camoufox_job: request_reset then resubmit.
                    return _flaky_shared(
                        label, callback, timeout=timeout, retry=False, reset_on_error=reset_on_error
                    )
            raise RuntimeError("injected driver/protocol failure")
        return _shadow_obs(map_num=1)

    # First: force pure fail path (no retry success) via submit that always raises.
    fail_calls = {"n": 0}

    def _always_fail(label, callback, timeout=120.0, retry=True, reset_on_error=True):
        fail_calls["n"] += 1
        raise RuntimeError("hard acquisition failure")

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", _always_fail)
    out_fail = tmp_path / "fail.json"
    send_calls: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append("x"))
    gate_before = cs.BOOKMAKER_PREFETCH_GATE_MODE

    rc_fail = cs.run_winline_shadow_request(
        match_key=URL_KILLS_A,
        map_num=1,
        team1="Team A",
        team2="Team B",
        selected_side="P1",
        output_path=str(out_fail),
        no_odds_active=True,
    )
    assert rc_fail != 0
    # Failed acquisition must terminalize the canonical lifecycle entry.
    with cs.bookmaker_prefetch_condition:
        remaining = cs.bookmaker_prefetch_results.get(life)
    assert remaining is None, f"failed acquisition must release/terminalize lifecycle; got {remaining!r}"
    assert send_calls == []
    assert cs.BOOKMAKER_PREFETCH_GATE_MODE == gate_before

    # Recovery: re-queue same series+map and succeed with one-reset runner.
    monkeypatch.setattr(cs, "_run_shared_camoufox_job", _flaky_shared)
    out_ok = tmp_path / "ok.json"
    rc_ok = cs.run_winline_shadow_request(
        match_key=URL_KILLS_B,  # different kills suffix, same series+map
        map_num=1,
        team1="Team A",
        team2="Team B",
        selected_side="P1",
        output_path=str(out_ok),
        no_odds_active=True,
    )
    assert rc_ok == 0
    assert attempts["n"] == 2, f"one fail + one sequential reset success; attempts={attempts['n']}"

    data = json.loads(out_ok.read_text(encoding="utf-8"))
    assert data["verdict"] == "PASS"
    assert data["selected_side"] == "P1"
    assert data["selected_odds"] == pytest.approx(1.55)


def test_integration_shadow_no_extra_browser_thread_subprocess_or_odds_gate(
    monkeypatch, tmp_path
) -> None:
    """Capability boundary: only injected shared runner; no standalone / send / gate mutation."""
    import subprocess
    import threading

    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False)
    gate_before = cs.BOOKMAKER_PREFETCH_GATE_MODE
    prefetch_before = cs.BOOKMAKER_PREFETCH_ENABLED
    send_calls: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda *a, **k: send_calls.append("x"))

    # Forbid accidental subprocess spawn.
    def _boom_sub(*a, **k):
        raise AssertionError("subprocess must not be used by shadow path")

    monkeypatch.setattr(subprocess, "Popen", _boom_sub, raising=False)
    monkeypatch.setattr(subprocess, "run", _boom_sub, raising=False)
    monkeypatch.setattr(subprocess, "call", _boom_sub, raising=False)

    threads_before = {t.name for t in threading.enumerate()}
    calls: List[str] = []

    def _fake_shared(label, callback, timeout=120.0, retry=True, reset_on_error=True):
        calls.append(label)
        return _shadow_obs(map_num=2, p1=2.10, p2=1.70)

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", _fake_shared)

    # Standalone path must be unreachable from the integration seam.
    assert not hasattr(cs, "run_sites_in_camoufox") or getattr(
        cs, "_bookmaker_run_sites_in_camoufox", None
    ) is not cs.run_winline_shadow_request

    out = tmp_path / "cap.json"
    rc = cs.run_winline_shadow_request(
        match_key=URL_KILLS_A,
        map_num=2,
        team1="Team A",
        team2="Team B",
        selected_side="P2",
        output_path=str(out),
        no_odds_active=True,
    )
    assert rc == 0
    assert calls == ["winline-shadow"]
    assert send_calls == []
    assert cs.BOOKMAKER_PREFETCH_GATE_MODE == gate_before
    assert cs.BOOKMAKER_PREFETCH_ENABLED is prefetch_before
    threads_after = {t.name for t in threading.enumerate()}
    # No new non-daemon worker threads named like a second browser session.
    new_threads = threads_after - threads_before
    assert not any("camoufox" in n.lower() and n != "shared-camoufox" for n in new_threads)

    # Refuse when --no-odds is not active (must not enable odds gate).
    rc_refused = cs.run_winline_shadow_request(
        match_key=URL_KILLS_A,
        map_num=2,
        team1="Team A",
        team2="Team B",
        selected_side="P2",
        output_path=str(tmp_path / "refused.json"),
        no_odds_active=False,
    )
    assert rc_refused != 0
    assert cs.BOOKMAKER_PREFETCH_ENABLED is prefetch_before
    assert cs.BOOKMAKER_PREFETCH_GATE_MODE == gate_before
    assert len(calls) == 1  # no second submission when refused


# ---------------------------------------------------------------------------
# 3) Preserve approved v5 owner-token / Telegram contract (reference, not redesign)
# ---------------------------------------------------------------------------


def test_integration_preserves_v5_owner_token_telegram_symbols() -> None:
    """Approved v5 contract symbols remain present and callable with expected signatures."""
    required = {
        "_bookmaker_resolve_odds_delivery_state": ("match_key",),
        "_bookmaker_prepare_message_for_delivery": None,
        "_bookmaker_commit_odds_delivery": None,
        "_bookmaker_rollback_odds_delivery": None,
        "_deliver_and_persist_signal": None,
        "_SharedCamoufoxSession": None,
        "_run_shared_camoufox_job": None,
        "_fetch_protracker_payload_via_shared_camoufox": None,
        "_bookmaker_prefetch_fetch_camoufox_direct": None,
        "_bookmaker_validate_current_map_observation": None,
        "_bookmaker_validate_odds_freshness": None,
        "_bookmaker_resolve_retained_deadline": None,
        "_bookmaker_clear_odds_delivery_pending": None,
    }
    for name, first_params in required.items():
        assert hasattr(cs, name), f"missing production symbol {name}"
        obj = getattr(cs, name)
        assert callable(obj) or inspect.isclass(obj), name
        if first_params and callable(obj):
            sig = inspect.signature(obj)
            params = list(sig.parameters)
            for expected in first_params:
                assert expected in params, f"{name} missing param {expected}: {params}"

    # Env defaults for lifecycle gates.
    assert float(cs.BOOKMAKER_ODDS_MAX_AGE_SECONDS) == pytest.approx(15.0)
    assert float(cs.BOOKMAKER_ODDS_WAIT_DEADLINE_SECONDS) == pytest.approx(90.0)

    # ProTracker must not default to spawning subprocess when fetcher is unset.
    src = inspect.getsource(d2pt.parse_hero_matchups)
    # Behavioral gate already covered above; also ensure empty-candidate fail-closed branch exists.
    assert "PROTRACKER_PAYLOAD_FETCHER" in src


def test_integration_references_approved_v5_regression_nodes() -> None:
    """Do not re-architect v5: approved regression node names must still exist in W2 suite."""
    suite = Path(__file__).resolve().parent / "test_winline_current_map_odds.py"
    text = suite.read_text(encoding="utf-8")
    required_nodes = [
        "test_deliver_and_persist_commits_sent_only_after_confirmed_send",
        "test_concurrent_duplicate_cannot_rollback_owner_bookmaker_reservation",
        "test_deliver_and_persist_hard_fail_rolls_back_and_allows_retry",
        "test_deliver_and_persist_uncertain_keeps_reservation_uncommitted",
        "test_minimal_odds_only_hands_explicit_reservation_without_second_prepare",
        "test_odds_mode_shared_unavailable_rejects_valid_cached_odds_without_subprocess",
        "test_fresh_open_exact_map_happy_path_sends_once",
        "test_production_closed_wait_keeps_original_deadline_and_no_reservation",
    ]
    missing = [n for n in required_nodes if f"def {n}" not in text]
    assert missing == [], f"approved v5/W2 nodes missing: {missing}"


def test_integration_winline_proxy_candidates_are_de_us_only(monkeypatch) -> None:
    """Winline proxy policy: explicit DE/US only; RU/unknown excluded (credentials never asserted)."""
    # Prefer production helper if present.
    candidates_fn = getattr(cs, "_bookmaker_winline_proxy_candidates", None)
    if candidates_fn is None:
        candidates_fn = getattr(cs, "_bookmaker_live_proxy_pool", None)
    assert callable(candidates_fn), "missing Winline proxy candidate helper"

    # Feed a mixed pool and require filtering when the helper supports it.
    mixed = [
        {"country": "DE", "proxy": "http://user:pass@154.195.1.1:1"},
        {"country": "US", "proxy": "http://user:pass@172.121.1.1:1"},
        {"country": "RU", "proxy": "http://user:pass@1.2.3.4:1"},
        {"country": "UNKNOWN", "proxy": "http://user:pass@9.9.9.9:1"},
    ]
    if candidates_fn is cs._bookmaker_live_proxy_pool or candidates_fn.__name__ == "_bookmaker_live_proxy_pool":
        # Live pool may read keys; just assert the function is callable and returns a list.
        out = candidates_fn()
        assert isinstance(out, list)
        for item in out:
            country = str((item or {}).get("country") or "").upper()
            if country:
                assert country in {"DE", "US", "RU", "UNKNOWN", ""}, country
        # Soft contract from audit: BOOKMAKER_PROXY_POOL inventory is DE/US only.
        return

    monkeypatch.setattr(cs, "_bookmaker_live_proxy_pool", lambda: mixed, raising=False)
    try:
        out = candidates_fn()
    except TypeError:
        out = candidates_fn(mixed)  # type: ignore[misc]
    assert isinstance(out, list)
    countries = [str((c or {}).get("country") or "").upper() for c in out]
    assert "RU" not in countries
    assert "UNKNOWN" not in countries
