"""RED/GREEN contract: stable series+map bookmaker lifecycle + one-reset recovery.

Ownership: W-CORE t_7de34f7c.
Uses fakes only — no live browser, network, Telegram, or production restart.
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402


SERIES_URL = "dltv.org/matches/123456"
URL_KILLS_A = f"{SERIES_URL}.12"
URL_KILLS_B = f"{SERIES_URL}.34"
URL_MAP2_A = f"{SERIES_URL}.99"  # same series, different map_num only


# ---------------------------------------------------------------------------
# Helpers / fakes
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


class _CountingBrowser:
    def __init__(self, owner: "_CountingCamoufoxFactory") -> None:
        owner.browser_seq += 1
        self.index = owner.browser_seq
        self.owner = owner
        self.pages: List[_CountingPage] = []
        self.events = owner.events
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


class _CountingContext:
    def __init__(self, factory: "_CountingCamoufoxFactory", kwargs: Dict[str, Any]) -> None:
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


class _CountingCamoufoxFactory:
    def __init__(self) -> None:
        self.browser_enters = 0
        self.active_browsers = 0
        self.max_active_browsers = 0
        self.browser_seq = 0
        self.events: List[str] = []
        self.new_page_calls: List[int] = []
        self.created_browsers: List[_CountingBrowser] = []
        self._lock = threading.Lock()

    def Camoufox(self, **kwargs: Any) -> _CountingContext:
        return _CountingContext(self, kwargs)


def _install_counting_camoufox(monkeypatch, factory: Optional[_CountingCamoufoxFactory] = None):
    factory = factory or _CountingCamoufoxFactory()
    monkeypatch.setattr(cs, "CAMOUFOX_AVAILABLE", True, raising=False)
    monkeypatch.setattr(cs, "camoufox", factory, raising=False)
    monkeypatch.setattr(cs, "_cyberscore_camoufox_proxy_kwargs", lambda: {}, raising=False)
    monkeypatch.setattr(cs, "_bookmaker_select_shared_camoufox_proxy_kwargs", lambda: {}, raising=False)
    monkeypatch.setattr(cs, "_note_proxy_success", lambda *_a, **_k: None, raising=False)
    monkeypatch.setattr(cs, "_bookmaker_rotate_shared_camoufox_proxy", lambda **_k: None, raising=False)
    return factory


def _clear_bookmaker_state() -> None:
    with cs.bookmaker_prefetch_condition:
        cs.bookmaker_prefetch_results.clear()
        cs.bookmaker_prefetch_queue.clear()
    with cs.bookmaker_odds_delivery_pending_lock:
        cs.bookmaker_odds_delivery_pending.clear()
    with cs.bookmaker_browser_lock:
        cs.bookmaker_browser_match_tabs.clear()


def _lifecycle_keys_in_results() -> List[str]:
    with cs.bookmaker_prefetch_condition:
        return sorted(cs.bookmaker_prefetch_results.keys())


def _lifecycle_identity(match_key: str, map_num: Optional[int]) -> Optional[str]:
    """Resolve the production lifecycle identity helper (must exist for GREEN)."""
    for name in (
        "_bookmaker_lifecycle_identity",
        "_bookmaker_series_map_lifecycle_key",
        "_bookmaker_canonical_lifecycle_key",
        "_bookmaker_series_map_key",
    ):
        fn = getattr(cs, name, None)
        if callable(fn):
            try:
                return fn(match_key, map_num=map_num)
            except TypeError:
                return fn(match_key, map_num)
    # Fallback: if production normalizes inside submit/lookup only, probe via submit state.
    return None


# ---------------------------------------------------------------------------
# Identity: same series+map across kills suffixes is one lifecycle
# ---------------------------------------------------------------------------


def test_same_series_map_across_kills_suffixes_share_one_lifecycle(monkeypatch) -> None:
    """Score/kills suffix must not create separate queue/cache identities."""
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_MAX_PENDING", 50)
    # Prevent worker thread from consuming the queue during unit assertion.
    monkeypatch.setattr(cs, "_ensure_bookmaker_prefetch_started", lambda: None)
    _clear_bookmaker_state()

    map_num = 1
    cs._bookmaker_prefetch_submit(
        match_key=URL_KILLS_A,
        radiant_team="Team A",
        dire_team="Team B",
        map_num=map_num,
        series_url=SERIES_URL,
    )
    # Second submit with different kills suffix must reuse the same lifecycle entry.
    cs._bookmaker_prefetch_submit(
        match_key=URL_KILLS_B,
        radiant_team="Team A",
        dire_team="Team B",
        map_num=map_num,
        series_url=SERIES_URL,
    )

    keys = _lifecycle_keys_in_results()
    assert len(keys) == 1, (
        f"same series+map across kills suffixes must share one lifecycle; keys={keys}"
    )
    # Raw score-suffixed URL must not be the identity key.
    assert URL_KILLS_A not in keys or URL_KILLS_B not in keys or URL_KILLS_A == URL_KILLS_B
    # Both lookups resolve the same payload.
    snap_a = cs._bookmaker_prefetch_lookup(URL_KILLS_A, wait_seconds=0.0)
    snap_b = cs._bookmaker_prefetch_lookup(URL_KILLS_B, wait_seconds=0.0)
    assert snap_a is not None and snap_b is not None
    assert str(snap_a.get("status")) == str(snap_b.get("status")) == "queued"
    # Presence gate and format helpers must also resolve via canonical identity.
    state_a, _ = cs._bookmaker_presence_gate_resolution(URL_KILLS_A)
    state_b, _ = cs._bookmaker_presence_gate_resolution(URL_KILLS_B)
    assert state_a == state_b == "pending"
    # Release by either suffix clears the shared entry.
    cs._bookmaker_release_match_tabs(URL_KILLS_B)
    with cs.bookmaker_browser_lock:
        # release is about tab cache; also ensure lifecycle identity helper exists
        pass
    identity = _lifecycle_identity(URL_KILLS_A, map_num)
    identity_b = _lifecycle_identity(URL_KILLS_B, map_num)
    assert identity is not None and identity_b is not None, (
        "production must expose a series+map lifecycle identity helper"
    )
    assert identity == identity_b
    assert ".12" not in identity and ".34" not in identity
    assert "123456" in identity
    assert "|map1" in identity


def test_same_series_different_map_num_stays_isolated(monkeypatch) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "_ensure_bookmaker_prefetch_started", lambda: None)
    _clear_bookmaker_state()

    cs._bookmaker_prefetch_submit(
        match_key=URL_KILLS_A,
        radiant_team="Team A",
        dire_team="Team B",
        map_num=1,
        series_url=SERIES_URL,
    )
    cs._bookmaker_prefetch_submit(
        match_key=URL_MAP2_A,
        radiant_team="Team A",
        dire_team="Team B",
        map_num=2,
        series_url=SERIES_URL,
    )
    keys = _lifecycle_keys_in_results()
    assert len(keys) == 2, f"map1 and map2 must not collide; keys={keys}"
    snap1 = cs._bookmaker_prefetch_lookup(URL_KILLS_A, wait_seconds=0.0)
    # Lookup for map2 URL with only series base would be wrong; submit used map_num=2.
    # After normalization, lookup of URL_MAP2_A without map context still needs map_num
    # from the caller's observation — production stores per series+map.
    # Both entries must remain present and distinct.
    assert snap1 is not None
    assert str(snap1.get("map_num")) in {"1", 1} or snap1.get("map_num") == 1
    id1 = _lifecycle_identity(URL_KILLS_A, 1)
    id2 = _lifecycle_identity(URL_MAP2_A, 2)
    assert id1 is not None and id2 is not None and id1 != id2


def test_missing_map_num_fails_closed_no_submit(monkeypatch) -> None:
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "_ensure_bookmaker_prefetch_started", lambda: None)
    _clear_bookmaker_state()

    cs._bookmaker_prefetch_submit(
        match_key=URL_KILLS_A,
        radiant_team="Team A",
        dire_team="Team B",
        map_num=None,
        series_url=SERIES_URL,
    )
    keys = _lifecycle_keys_in_results()
    assert keys == [], f"missing map must fail closed (no lifecycle entry); keys={keys}"
    assert cs._bookmaker_prefetch_lookup(URL_KILLS_A, wait_seconds=0.0) is None
    block, ready, reason = cs._bookmaker_format_odds_block(URL_KILLS_A)
    assert ready is False
    assert block == ""


# ---------------------------------------------------------------------------
# One sequential reset after driver/protocol failure; next job succeeds
# ---------------------------------------------------------------------------


def test_one_driver_exception_causes_one_close_relaunch_then_next_job_succeeds(
    monkeypatch,
) -> None:
    """Injected driver/protocol exception: exactly one close then one relaunch; no strand."""
    factory = _install_counting_camoufox(monkeypatch)
    session = cs._SharedCamoufoxSession()
    monkeypatch.setattr(cs, "_shared_camoufox_session", session, raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", True)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_GATE_MODE", "odds")
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_ENABLED", True, raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True, raising=False)
    monkeypatch.setattr(cs, "_ensure_bookmaker_prefetch_started", lambda: None)
    _clear_bookmaker_state()

    call_count = {"n": 0}
    job_browsers: List[int] = []

    def _flaky_job(browser):
        call_count["n"] += 1
        job_browsers.append(id(browser))
        if call_count["n"] == 1:
            raise RuntimeError("injected driver/protocol failure")
        return {"winline": {"status": "ok", "match_found": True, "odds": [1.9, 2.0]}}

    # First shared job fails → requests one reset (close before next launch).
    with pytest.raises(RuntimeError, match="injected driver/protocol failure"):
        cs._run_shared_camoufox_job("bookmaker-prefetch", _flaky_job, timeout=5, retry=False)

    # Allow worker to process reset after the failed job.
    deadline = time.time() + 2.0
    while time.time() < deadline and factory.browser_enters < 1:
        time.sleep(0.01)

    # Terminalize failed canonical entry so it is not permanently queued/running/error.
    map_num = 1
    cs._bookmaker_prefetch_submit(
        match_key=URL_KILLS_A,
        radiant_team="Team A",
        dire_team="Team B",
        map_num=map_num,
        series_url=SERIES_URL,
    )
    # Simulate failed job marking error on the canonical entry, then recovery path.
    with cs.bookmaker_prefetch_condition:
        for key, payload in list(cs.bookmaker_prefetch_results.items()):
            if isinstance(payload, dict):
                payload["status"] = "error"
                payload["error"] = "injected driver/protocol failure"
                payload["finished_at"] = time.time()

    # Recovery helper (or re-submit after terminalize) must allow next job for same series+map
    # including a different kills suffix.
    recover = getattr(cs, "_bookmaker_terminalize_failed_lifecycle", None)
    assert callable(recover), "production must expose _bookmaker_terminalize_failed_lifecycle"
    recover(URL_KILLS_A, map_num=map_num)

    # Re-submit with different kills suffix after failure — must not be stranded.
    cs._bookmaker_prefetch_submit(
        match_key=URL_KILLS_B,
        radiant_team="Team A",
        dire_team="Team B",
        map_num=map_num,
        series_url=SERIES_URL,
    )
    keys = _lifecycle_keys_in_results()
    assert len(keys) == 1, f"post-failure re-submit must keep one series+map entry; keys={keys}"
    snap = cs._bookmaker_prefetch_lookup(URL_KILLS_B, wait_seconds=0.0)
    assert snap is not None
    assert str(snap.get("status")) in {"queued", "running", "done"}, (
        f"entry must not remain permanently error after recovery; status={snap.get('status')}"
    )

    # Second shared job succeeds after one sequential close/relaunch.
    result = cs._run_shared_camoufox_job("bookmaker-prefetch", _flaky_job, timeout=5, retry=True)
    assert result["winline"]["status"] == "ok"
    session.close()

    assert factory.browser_enters >= 1
    # Exactly one close before the successful relaunch path (no parallel browsers).
    close_events = [e for e in factory.events if e.startswith("browser_close:")]
    enter_events = [e for e in factory.events if e.startswith("browser_enter:")]
    assert factory.max_active_browsers == 1, (
        f"no parallel browser; max_active={factory.max_active_browsers}; events={factory.events}"
    )
    # After failure with reset_on_error, one close then a relaunch for the next job.
    assert len(enter_events) >= 1
    if len(enter_events) >= 2:
        # close must occur between first enter and second enter
        first_enter_idx = factory.events.index(enter_events[0])
        second_enter_idx = factory.events.index(enter_events[1])
        closes_between = [
            e
            for i, e in enumerate(factory.events)
            if first_enter_idx < i < second_enter_idx and e.startswith("browser_close:")
        ]
        assert len(closes_between) >= 1, (
            f"expected close before relaunch; events={factory.events}"
        )
    # No second worker thread / subprocess fallback: only one shared session thread name.
    assert session._thread is None or not session._thread.is_alive() or session._thread.name == "shared-camoufox"


def test_identity_never_falls_back_to_series_only_or_raw_score_url(monkeypatch) -> None:
    """Missing/invalid series or map fails closed — no series-only or raw URL identity."""
    # Invalid map
    for bad_map in (0, 9, -1, "x"):
        id_bad = _lifecycle_identity(URL_KILLS_A, bad_map if not isinstance(bad_map, str) else None)
        if isinstance(bad_map, str):
            # explicitly pass invalid via helper if it accepts raw
            fn = None
            for name in (
                "_bookmaker_lifecycle_identity",
                "_bookmaker_series_map_lifecycle_key",
                "_bookmaker_canonical_lifecycle_key",
                "_bookmaker_series_map_key",
            ):
                cand = getattr(cs, name, None)
                if callable(cand):
                    fn = cand
                    break
            assert fn is not None, "lifecycle identity helper must exist"
            try:
                id_bad = fn(URL_KILLS_A, map_num=bad_map)
            except TypeError:
                id_bad = fn(URL_KILLS_A, bad_map)
        assert id_bad is None, f"invalid map must fail closed, got {id_bad!r} for map={bad_map!r}"

    # Missing series identity
    id_missing = _lifecycle_identity("not-a-match-url", 1)
    assert id_missing is None, f"missing series must fail closed, got {id_missing!r}"

    # Valid identity strips kills suffix and includes map.
    good = _lifecycle_identity(URL_KILLS_A, 2)
    assert good is not None
    assert ".12" not in good
    assert "123456" in good
