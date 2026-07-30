"""Contract tests for process-wide shared Camoufox browser + reusable named pages.

W1-BROWSER ownership: this file only (plus the shared-session / ProTracker hooks).
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402


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
        self.owner.new_page_by_name["anonymous"] = self.owner.new_page_by_name.get("anonymous", 0) + 1
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
        self.new_page_by_name: Dict[str, int] = {}
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
                # context exit without explicit browser.close still ends the browser
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
    monkeypatch.setattr(cs, "_bookmaker_select_shared_camoufox_proxy_kwargs", lambda: {}, raising=False)
    monkeypatch.setattr(cs, "_note_proxy_success", lambda *_a, **_k: None, raising=False)
    monkeypatch.setattr(cs, "_bookmaker_rotate_shared_camoufox_proxy", lambda **_k: None, raising=False)
    return factory


def test_winline_jobs_reuse_named_page_in_one_browser(monkeypatch) -> None:
    """Two sequential Winline direct fetches must reuse one named page in one browser."""
    factory = _install_counting_camoufox(monkeypatch)

    session = cs._SharedCamoufoxSession()
    monkeypatch.setattr(cs, "_shared_camoufox_session", session, raising=False)

    # Track pages seen by the real production job path.
    seen_page_ids: List[int] = []
    parse_calls: List[Dict[str, Any]] = []

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
        parse_calls.append(dict(kwargs))
        # If named-page API is used, page may carry a name attribute from registry.
        return _ParseResult()

    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True, raising=False)
    monkeypatch.setattr(cs, "_bookmaker_parse_site_in_camoufox_page", _fake_parse, raising=False)
    monkeypatch.setattr(
        cs,
        "_bookmaker_urls_for_mode",
        lambda mode: {"winline": "https://winline.ru/stavki/sport/kibersport"},
        raising=False,
    )
    monkeypatch.setattr(cs, "_bookmaker_effective_sites_for_mode", lambda *_a, **_k: ("winline",), raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds", raising=False)

    try:
        r1 = cs._bookmaker_prefetch_fetch_camoufox_direct("Team A", "Team B", "live", map_num=1)
        r2 = cs._bookmaker_prefetch_fetch_camoufox_direct("Team A", "Team B", "live", map_num=1)
    finally:
        session.close()

    assert r1["winline"]["status"] == "ok"
    assert r2["winline"]["status"] == "ok"
    assert factory.browser_enters == 1, f"expected one browser, got {factory.browser_enters}"
    assert factory.max_active_browsers == 1
    # Core contract: one reusable named Winline page — not a new_page() per job.
    assert len(seen_page_ids) == 2
    assert len(set(seen_page_ids)) == 1, (
        f"Winline page must be reused across jobs; got page ids {seen_page_ids}; "
        f"new_page_calls={factory.new_page_calls}; events={factory.events}"
    )
    # Exactly one anonymous new_page for the reusable Winline tab across both jobs.
    assert len(factory.new_page_calls) == 1, (
        f"expected single new_page for reusable Winline page, got {factory.new_page_calls}; "
        f"events={factory.events}"
    )
    # Close order on teardown: named page closes, then browser, then context.
    # There must not be a close-per-job pattern (page_close between the two parses).
    page_close_events = [e for e in factory.events if e.startswith("page_close:")]
    assert len(page_close_events) == 1, (
        f"expected one page_close on browser teardown, got {page_close_events}; events={factory.events}"
    )
    browser_close_idx = next(i for i, e in enumerate(factory.events) if e.startswith("browser_close"))
    page_close_idx = next(i for i, e in enumerate(factory.events) if e.startswith("page_close:"))
    assert page_close_idx < browser_close_idx, (
        f"page must close before browser; events={factory.events}"
    )


def test_proxy_rotation_closes_page_and_browser_before_next_launch(monkeypatch) -> None:
    """Proxy/reset rotation: page_close < browser_close/context_exit < second_browser_enter; max active=1."""
    factory = _install_counting_camoufox(monkeypatch)
    session = cs._SharedCamoufoxSession()
    monkeypatch.setattr(cs, "_shared_camoufox_session", session, raising=False)

    seen_pages: List[int] = []

    def _job_with_named_page(browser):
        page = session.get_or_create_page("bookmaker:winline", browser)
        seen_pages.append(id(page))
        return id(browser)

    try:
        b1 = session.submit("first", _job_with_named_page, timeout=5)
        session.request_reset()
        # Allow worker finally/reset to run if already idle; next submit applies reset first.
        b2 = session.submit("second", _job_with_named_page, timeout=5)
    finally:
        session.close()

    assert b1 != b2, "reset must launch a new browser instance"
    assert factory.max_active_browsers == 1, (
        f"max_active_browsers must stay 1, got {factory.max_active_browsers}; events={factory.events}"
    )
    assert factory.browser_enters >= 2
    assert len(set(seen_pages)) == 2, (
        f"after reset, Winline page must be a new page object; seen={seen_pages}"
    )

    # Order: first page_close before first browser_close/context_exit before second browser_enter
    events = factory.events
    enter_idxs = [i for i, e in enumerate(events) if e.startswith("browser_enter:")]
    page_close_idxs = [i for i, e in enumerate(events) if e.startswith("page_close:")]
    browser_close_idxs = [i for i, e in enumerate(events) if e.startswith("browser_close:")]
    context_exit_idxs = [i for i, e in enumerate(events) if e.startswith("context_exit:")]
    assert len(enter_idxs) >= 2, f"need two browser enters; events={events}"
    assert page_close_idxs, f"need page_close; events={events}"
    first_page_close = page_close_idxs[0]
    first_browser_close = browser_close_idxs[0] if browser_close_idxs else len(events)
    first_context_exit = context_exit_idxs[0] if context_exit_idxs else len(events)
    # Accept either browser_close or context_exit as the browser teardown marker before relaunch.
    teardown_idx = min(first_browser_close, first_context_exit)
    second_browser_enter = enter_idxs[1]
    assert first_page_close < teardown_idx < second_browser_enter, (
        f"expected page_close < browser/context close < second_browser_enter; "
        f"idxs page={first_page_close} teardown={teardown_idx} enter2={second_browser_enter}; "
        f"events={events}"
    )


def test_shared_browser_jobs_never_call_camoufox_subprocess_fallback(monkeypatch) -> None:
    """After repeated shared Camoufox errors, ProTracker must not call Camoufox subprocess."""
    import dota2protracker as d2pt

    factory = _install_counting_camoufox(monkeypatch)
    session = cs._SharedCamoufoxSession()
    monkeypatch.setattr(cs, "_shared_camoufox_session", session, raising=False)
    monkeypatch.setattr(cs, "CAMOUFOX_AVAILABLE", True, raising=False)

    subprocess_calls: List[tuple] = []
    reset_calls: List[str] = []

    def _failing_shared_job(label, callback, timeout=120.0, retry=True, reset_on_error=True):
        # Simulate shared-session failure for every protracker attempt.
        raise RuntimeError("shared camoufox protracker boom")

    def _fake_subprocess(slug, hero_id, proxy_candidate=None):
        subprocess_calls.append((slug, hero_id, proxy_candidate))
        return {"matchups": {"1": []}, "synergies": {"1": []}}

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", _failing_shared_job)
    monkeypatch.setattr(
        cs._shared_camoufox_session,
        "request_reset",
        lambda: reset_calls.append("reset"),
    )
    # Ensure the production fallback resolver would see a real subprocess fetcher.
    monkeypatch.setattr(cs, "_protracker_subprocess_fetcher", lambda: _fake_subprocess)
    monkeypatch.setattr(cs, "_PROTRACKER_SHARED_CAMOUFOX_BROKEN", False, raising=False)
    monkeypatch.setattr(cs, "_PROTRACKER_SHARED_CONSECUTIVE_FAILS", 0, raising=False)
    monkeypatch.setattr(cs, "_PROTRACKER_SHARED_FAILS_TO_BREAK", 2, raising=False)

    # Wire dota2protracker external fetcher to the production shared hook.
    monkeypatch.setattr(d2pt, "PROTRACKER_PAYLOAD_FETCHER", cs._fetch_protracker_payload_via_shared_camoufox)
    monkeypatch.setattr(d2pt, "CAMOUFOX_AVAILABLE", True, raising=False)
    monkeypatch.setattr(d2pt, "get_hero_slug", lambda name: "anti-mage")
    monkeypatch.setattr(d2pt, "get_hero_id", lambda name: 1)
    monkeypatch.setattr(d2pt, "_fetch_protracker_payload_via_subprocess", _fake_subprocess)
    # Avoid cache hits / file writes
    monkeypatch.setattr(d2pt, "CACHE_DIR", "/tmp/__no_protracker_cache_w1_browser__")
    monkeypatch.setattr(d2pt.os.path, "exists", lambda *_a, **_k: False)

    # Drive the actual external-fetcher path after repeated shared errors.
    # First shared fail -> second shared fail -> should NOT fall through to subprocess.
    results = []
    errors = []
    for _ in range(3):
        try:
            # Call production shared fetcher directly (the external hook body).
            results.append(cs._fetch_protracker_payload_via_shared_camoufox("anti-mage", 1, None))
        except Exception as exc:
            errors.append(exc)

    # Also exercise parse_hero_matchups external-fetcher branch once broken/failing.
    try:
        parse_result = d2pt.parse_hero_matchups("Anti-Mage", use_cache=False, proxy=None)
    except Exception as exc:
        parse_result = {"error": str(exc)}

    assert subprocess_calls == [], (
        f"Camoufox subprocess fallback must never be invoked; calls={subprocess_calls}; "
        f"errors={errors!r}; results={results!r}; parse={parse_result!r}"
    )
    # Shared failures must request reset and surface an honest error (exception or error payload).
    assert reset_calls or errors or (isinstance(parse_result, dict) and parse_result.get("error")), (
        f"expected reset and/or honest error; reset={reset_calls}; errors={errors}; parse={parse_result}"
    )
    # No second browser from subprocess path.
    assert factory.browser_enters == 0


def test_independent_browser_job_names_share_browser_not_page(monkeypatch) -> None:
    """Winline and ProTracker share one browser owner but use separate named pages."""
    factory = _install_counting_camoufox(monkeypatch)
    session = cs._SharedCamoufoxSession()
    monkeypatch.setattr(cs, "_shared_camoufox_session", session, raising=False)

    page_ids: Dict[str, int] = {}
    browser_ids: List[int] = []

    def _winline_job(browser):
        page = session.get_or_create_page("bookmaker:winline", browser)
        page_ids["winline"] = id(page)
        browser_ids.append(id(browser))
        return "winline-ok"

    def _protracker_job(browser):
        page = session.get_or_create_page("protracker:matchups", browser)
        page_ids["protracker"] = id(page)
        browser_ids.append(id(browser))
        return "protracker-ok"

    try:
        assert session.submit("bookmaker-prefetch", _winline_job, timeout=5) == "winline-ok"
        assert session.submit("dota2protracker:anti-mage", _protracker_job, timeout=5) == "protracker-ok"
        # Second Winline job reuses same page, still same browser.
        assert session.submit("bookmaker-prefetch", _winline_job, timeout=5) == "winline-ok"
    finally:
        session.close()

    assert factory.browser_enters == 1
    assert factory.max_active_browsers == 1
    assert len(set(browser_ids)) == 1, f"must share one browser; browser_ids={browser_ids}"
    assert "winline" in page_ids and "protracker" in page_ids
    assert page_ids["winline"] != page_ids["protracker"], (
        f"Winline and ProTracker must not share a page; page_ids={page_ids}"
    )
    # Two named pages created, no cross-site reuse.
    assert len(factory.new_page_calls) == 2, (
        f"expected two new_page calls (one per name), got {factory.new_page_calls}; events={factory.events}"
    )
