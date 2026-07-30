"""REPLAN9-INT: cross-file Winline cadence / page policy / publication E2E.

Exclusive ownership of this file + runtime/replan9/staging/int/.

Proves together (deterministic fakes, no network/live browser):
- prior-start +5s cadence with synchronous serialization
- overrun coalesces to one immediate follow-up (no burst/overlap), noncompliant
- every browser op uses _run_shared_camoufox_job + one bookmaker:winline page
- correct-URL dynamic_dom: 0 goto / 0 reload / 0 sleep
- blank/root/wrong URL: exactly one repair navigation
- controlled_reload after 3 stable eligible misses with >=60s spacing, reload once
- concurrent publishers leave valid whole JSON, zero collisions/writer errors
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import bookmaker_selenium_odds as odds_parser  # noqa: E402
import cyberscore_try as cs  # noqa: E402

SERIES = "dltv.org/matches/8900882416"
TEAM1 = "BoomBoys"
TEAM2 = "Nigma Galaxy"
MAP_NUM = 2
CANONICAL = f"{SERIES}|map{MAP_NUM}|{TEAM1}|{TEAM2}"
WINLINE_LIVE = "https://winline.ru/stavki/sport/kibersport/match/12345"
WINLINE_ROOT = "https://winline.ru/"
WINLINE_WRONG = "https://example.com/odds"
NAMED_PAGE = "bookmaker:winline"
POLL_INTERVAL = 5.0
RELOAD_SPACING = 60.0

# Deterministic assertion IDs (also recorded in staging manifest).
SIG_CADENCE_PRIOR_START = "INT.cadence.prior_start_plus_5s"
SIG_OVERRUN_COALESCE = "INT.cadence.overrun_coalesce_one_immediate"
SIG_SERIAL_NO_OVERLAP = "INT.cadence.synchronous_serialization"
SIG_SHARED_RUNNER = "INT.browser.shared_camoufox_only"
SIG_NAMED_PAGE = "INT.browser.named_page_bookmaker_winline"
SIG_DYNAMIC_DOM_LIVE = "INT.page.dynamic_dom_live_zero_nav"
SIG_DYNAMIC_DOM_REPAIR = "INT.page.dynamic_dom_wrong_one_goto"
SIG_CONTROLLED_RELOAD = "INT.mode.controlled_reload_after_3_misses_60s"
SIG_PUBLICATION_RACE = "INT.pub.concurrent_valid_json_zero_collisions"
SIG_EVIDENCE_FIELDS = "INT.evidence.attempt_contract_fields"


class FakeClock:
    def __init__(self, mono: float = 1000.0, wall: float = 1_700_000_000.0) -> None:
        self.mono = float(mono)
        self.wall = float(wall)
        self._lock = threading.Lock()

    def monotonic(self) -> float:
        with self._lock:
            return self.mono

    def time(self) -> float:
        with self._lock:
            return self.wall

    def advance(self, seconds: float) -> None:
        seconds = float(seconds)
        with self._lock:
            self.mono += seconds
            self.wall += seconds


class _SleepCounter:
    def __init__(self) -> None:
        self.calls: List[float] = []

    def __call__(self, seconds: float = 0, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        self.calls.append(float(seconds))


class _FakeLocator:
    def __init__(self, text: str) -> None:
        self._text = text

    def inner_text(self, timeout: int = 0):  # noqa: ARG002
        return self._text


class _CountingPage:
    def __init__(
        self,
        *,
        html: str,
        body_text: str,
        url: str = "about:blank",
        name: Optional[str] = None,
    ) -> None:
        self._html = html
        self._body_text = body_text
        self.url = url
        self.name = name
        self.goto_calls: List[Dict[str, Any]] = []
        self.reload_calls: List[Dict[str, Any]] = []
        self.content_calls = 0
        self.evaluate_calls = 0
        self.page_id = id(self)
        self.closed = False

    def goto(self, url: str, wait_until: str = "domcontentloaded", timeout: int = 0):  # noqa: ARG002
        self.goto_calls.append({"url": url, "wait_until": wait_until, "timeout": timeout})
        self.url = url
        return None

    def reload(self, wait_until: str = "domcontentloaded", timeout: int = 0):  # noqa: ARG002
        self.reload_calls.append({"wait_until": wait_until, "timeout": timeout})
        return None

    def content(self) -> str:
        self.content_calls += 1
        return self._html

    def locator(self, selector: str):
        if selector == "body":
            return _FakeLocator(self._body_text)
        raise AssertionError(f"unexpected selector: {selector}")

    def title(self) -> str:
        return "Winline"

    def evaluate(self, script: str, arg=None):  # noqa: ARG002
        self.evaluate_calls += 1
        if "document.readyState" in str(script):
            return "complete"
        return False

    def close(self) -> None:
        self.closed = True


class _NamedPageRegistry:
    def __init__(self) -> None:
        self._pages: Dict[str, _CountingPage] = {}
        self.create_calls: List[str] = []

    def get_or_create_page(self, name: str, browser=None, **_kw: Any) -> _CountingPage:
        if name in self._pages:
            return self._pages[name]
        self.create_calls.append(name)
        page = _CountingPage(
            html=_html("seed"),
            body_text="seed",
            url="about:blank",
            name=name,
        )
        self._pages[name] = page
        return page


def _html(text: str) -> str:
    return f"<html><body>{text}</body></html>"


def _clear_state() -> None:
    fn = getattr(cs, "reset_winline_current_map_polling_state", None)
    if callable(fn):
        try:
            fn()
        except Exception:
            pass
    for attr in (
        "_winline_current_map_pollers",
        "_winline_current_map_registry",
        "_winline_map_lifecycle_registry",
    ):
        st = getattr(cs, attr, None)
        if isinstance(st, dict):
            st.clear()


def _missing_payload(**overrides: Any) -> Dict[str, Any]:
    base = {
        "market_status": "missing",
        "source": "winline_current_map_winner",
        "p1_odds": None,
        "p2_odds": None,
        "map_num": MAP_NUM,
        "team1": TEAM1,
        "team2": TEAM2,
        "series": SERIES,
        "current_url": WINLINE_LIVE,
        "dom_signature": "stable-sig",
        "dom_hash": "stable-hash",
        "parser_failure_reasons": [],
        "error": None,
        "acquisition_error": None,
        "page_valid": True,
    }
    base.update(overrides)
    return base


def _attempt_of(tick_out: Any) -> Dict[str, Any]:
    if not isinstance(tick_out, dict):
        return {}
    # tick_winline returns list of outs; poller.tick returns one dict
    if "attempt" in tick_out or "status" in tick_out or "acquisition_mode" in tick_out:
        return tick_out.get("attempt") or tick_out
    return tick_out


def _drive_tick(clock: FakeClock, *, advance: Optional[float] = None) -> List[Dict[str, Any]]:
    if advance is not None:
        clock.advance(advance)
    return cs.tick_winline_current_map_polling(
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
    )


# ---------------------------------------------------------------------------
# SIG: prior-start cadence + serialization + overrun coalesce (cross-file)
# ---------------------------------------------------------------------------


def test_int_prior_start_cadence_and_overrun_coalesce(tmp_path, monkeypatch) -> None:
    """INT.cadence.* — prior-start +5s, overrun one immediate, no burst/overlap."""
    _clear_state()
    clock = FakeClock(mono=1000.0, wall=50_000.0)
    evidence = tmp_path / "latest.json"
    start_monos: List[float] = []
    in_flight_max = {"n": 0}
    active = {"n": 0}
    latencies = [1.5, 12.0, 0.2, 0.2]
    call_i = {"n": 0}

    def collector(**kwargs: Any) -> Dict[str, Any]:
        active["n"] += 1
        in_flight_max["n"] = max(in_flight_max["n"], active["n"])
        try:
            start_monos.append(float(clock.mono))
            lat = latencies[min(call_i["n"], len(latencies) - 1)]
            call_i["n"] += 1
            # Simulate work latency via fake clock (no real sleep).
            clock.advance(lat)
            payload = _missing_payload(
                latency_seconds=lat,
                observed_at=clock.wall,
                acquisition_mode_echo=kwargs.get("acquisition_mode"),
            )
            return payload
        finally:
            active["n"] -= 1

    # Prevent real scheduler thread from racing fake clock.
    monkeypatch.setattr(
        cs,
        "start_winline_current_map_polling_scheduler",
        lambda **_k: True,
        raising=False,
    )
    monkeypatch.setattr(cs, "send_message", lambda *_a, **_k: None)

    cs.ensure_winline_current_map_polling(
        series=SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=4242,
        producer_start_generation="int-gen-1",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=collector,
        evidence_path=evidence,
    )

    # Attempt 1: latency 1.5 → next due = start+5
    outs1 = _drive_tick(clock)
    assert outs1, f"{SIG_CADENCE_PRIOR_START}: expected first attempt"
    a1 = _attempt_of(outs1[0])
    s1 = float(a1["attempt_started_at_monotonic"])
    assert s1 == pytest.approx(1000.0)
    next1 = float(a1.get("next_poll_at_monotonic") or a1["next_poll_monotonic"])
    assert next1 == pytest.approx(s1 + POLL_INTERVAL), (
        f"{SIG_CADENCE_PRIOR_START}: due must be prior-start+5s, got {next1} vs {s1 + POLL_INTERVAL}"
    )
    assert a1.get("cadence_overrun") is False
    assert a1.get("cadence_compliant") is True
    assert float(a1.get("attempt_latency_seconds") or a1["latency_seconds"]) == pytest.approx(1.5)
    assert a1.get("attempt_start_delta_seconds") is None
    assert in_flight_max["n"] == 1, f"{SIG_SERIAL_NO_OVERLAP}: max concurrency must be 1"

    # Not due yet (finish was 1001.5; start+5 = 1005.0 → need +3.5 from finish; advance 3.4)
    outs_early = _drive_tick(clock, advance=3.4)
    early_attempts = [o for o in outs_early if isinstance(o, dict) and o.get("attempt")]
    assert not early_attempts, f"{SIG_CADENCE_PRIOR_START}: must not fire before start+5"
    assert call_i["n"] == 1

    # Exactly at start+5
    outs2 = _drive_tick(clock, advance=0.1)
    assert outs2 and call_i["n"] == 2
    a2 = _attempt_of(outs2[0])
    # Attempt 2 latency 12s → overrun path
    assert a2.get("cadence_overrun") is True or a2.get("overrun") is True
    assert a2.get("cadence_compliant") is False, f"{SIG_OVERRUN_COALESCE}: overrun noncompliant"
    assert int(a2.get("coalesced_missed_intervals") or 0) >= 2
    assert float(a2["attempt_start_delta_seconds"]) == pytest.approx(POLL_INTERVAL)
    next2 = float(a2.get("next_poll_at_monotonic") or a2["next_poll_monotonic"])
    assert next2 == pytest.approx(float(clock.mono)), (
        f"{SIG_OVERRUN_COALESCE}: next due must be immediate at finish"
    )
    assert float(a2["attempt_latency_seconds"]) == pytest.approx(12.0)

    # Immediate single follow-up (no clock advance) — coalesce, not burst
    outs3 = _drive_tick(clock)
    assert outs3 and call_i["n"] == 3, f"{SIG_OVERRUN_COALESCE}: one immediate follow-up"
    a3 = _attempt_of(outs3[0])
    assert float(a3["attempt_start_delta_seconds"]) == pytest.approx(12.0)

    # Capture attempt evidence immediately after a real attempt write (before any
    # not_due tick which may also publish a lightweight status blob).
    assert evidence.is_file(), f"{SIG_EVIDENCE_FIELDS}: evidence path must be written"
    doc_after_attempt = json.loads(evidence.read_text(encoding="utf-8"))
    for field in (
        "attempt_started_at_monotonic",
        "attempt_start_delta_seconds",
        "attempt_latency_seconds",
        "cadence_overrun",
        "cadence_compliant",
        "acquisition_mode",
        "canonical_key",
        "map_num",
    ):
        assert field in doc_after_attempt, (
            f"{SIG_EVIDENCE_FIELDS}: missing {field} in {sorted(doc_after_attempt)}"
        )
    # Also prove attempt payload itself carries the contract (tick return path).
    for field in (
        "attempt_started_at_monotonic",
        "attempt_start_delta_seconds",
        "attempt_latency_seconds",
        "cadence_overrun",
        "cadence_compliant",
        "acquisition_mode",
        "canonical_key",
        "map_num",
    ):
        assert field in a3, f"{SIG_EVIDENCE_FIELDS}: attempt missing {field}"

    # No third burst without advance past start3+5
    outs_mid = _drive_tick(clock)
    mid_attempts = [o for o in outs_mid if isinstance(o, dict) and o.get("attempt")]
    assert not mid_attempts, f"{SIG_OVERRUN_COALESCE}: must not burst-replay missed ticks"
    assert call_i["n"] == 3
    assert in_flight_max["n"] == 1, f"{SIG_SERIAL_NO_OVERLAP}: never overlapped"


# ---------------------------------------------------------------------------
# SIG: shared runner + named page + page policy modes via production collector
# ---------------------------------------------------------------------------


def test_int_shared_runner_named_page_and_mode_matrix(tmp_path, monkeypatch) -> None:
    """INT.browser.* + INT.page.* + INT.mode.* — full collector path with fakes."""
    _clear_state()
    clock = FakeClock(mono=0.0, wall=0.0)
    evidence = tmp_path / "latest.json"
    sleep = _SleepCounter()
    monkeypatch.setattr(odds_parser.time, "sleep", sleep)

    registry = _NamedPageRegistry()
    shared_labels: List[str] = []
    page_names: List[str] = []
    parse_modes: List[str] = []
    concurrency = {"n": 0, "max": 0}
    lock = threading.Lock()

    # Seed named page already on live URL for first dynamic_dom after initial.
    live_body = f"{TEAM1} {TEAM2} missing market DOM"

    class _FakeSession:
        def get_or_create_page(self, name, browser=None, **_kw):
            page_names.append(name)
            page = registry.get_or_create_page(name, browser)
            return page

    def fake_run_shared(label, callback, timeout=120.0, retry=True, reset_on_error=True):  # noqa: ARG001
        with lock:
            concurrency["n"] += 1
            concurrency["max"] = max(concurrency["max"], concurrency["n"])
            shared_labels.append(str(label))
        try:
            return callback(object())
        finally:
            with lock:
                concurrency["n"] -= 1

    def fake_parse(page, site, url, team1, team2, mode, forced_map_num=None, acquisition_mode=None):
        # Route through real page policy loader so goto/reload/sleep counters apply.
        parse_modes.append(str(acquisition_mode or ""))
        assert site == "winline"
        assert page is not None
        # Keep DOM body stable eligible-miss style after load.
        page._html = _html(live_body)
        page._body_text = live_body
        load_status, load_error, html, visible, body_text, diag = (
            odds_parser._load_site_render_payload_camoufox(
                page,
                url,
                initial_wait_seconds=0.0,
                scroll_wait_seconds=0.0,
                acquisition_mode=acquisition_mode,
            )
        )

        class _R:
            pass

        r = _R()
        r.source = "Winline"
        r.odds = []
        r.map_num = forced_map_num if forced_map_num is not None else MAP_NUM
        r.p1_team = team1
        r.p2_team = team2
        r.market_closed = False
        r.market_kind = "current_map_winner"
        r.status = "ok" if load_status == "ok" else load_status
        r.match_found = False  # eligible miss on valid page
        r.acquisition_mode = acquisition_mode
        r.dom_signature = "stable-sig"
        r.dom_hash = "stable-hash"
        r.page_url = getattr(page, "url", url)
        r.current_url = getattr(page, "url", url)
        r.body_text = body_text or visible or live_body
        r.details = body_text or visible or live_body
        r.acquisition_error = diag.get("acquisition_error") or load_error or None
        r.error = load_error or None
        r.load_error = load_error or None
        r.load_status = load_status
        r.parser_failure_reasons = []
        r.page_valid = load_status == "ok"
        return r

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", fake_run_shared)
    monkeypatch.setattr(cs, "_shared_camoufox_session", _FakeSession(), raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True, raising=False)
    monkeypatch.setattr(cs, "_bookmaker_parse_site_in_camoufox_page", fake_parse, raising=False)
    monkeypatch.setattr(
        cs,
        "_bookmaker_urls_for_mode",
        lambda _mode: {"winline": WINLINE_LIVE},
        raising=False,
    )
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(
        cs,
        "start_winline_current_map_polling_scheduler",
        lambda **_k: True,
        raising=False,
    )
    monkeypatch.setattr(cs, "send_message", lambda *_a, **_k: None)
    sent: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda m, **_k: sent.append(str(m)))

    # Start page blank → first acquisition initial_goto repairs via production collector.
    cs.ensure_winline_current_map_polling(
        series=SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=7,
        producer_start_generation="int-b",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        # No injected collector → production _winline_current_map_poller_collect
        evidence_path=evidence,
        reload_min_spacing_seconds=RELOAD_SPACING,
    )

    page = registry.get_or_create_page(NAMED_PAGE)
    # Ensure first tick sees blank named page (fresh create).
    page.url = "about:blank"
    page.goto_calls.clear()
    page.reload_calls.clear()
    page.content_calls = 0

    modes_seen: List[str] = []
    attempts: List[Dict[str, Any]] = []

    def _one_tick(*, advance: float = 0.0) -> Optional[Dict[str, Any]]:
        if advance:
            clock.advance(advance)
        outs = cs.tick_winline_current_map_polling(
            monotonic_fn=clock.monotonic,
            wall_fn=clock.time,
        )
        for o in outs:
            att = o.get("attempt") if isinstance(o, dict) else None
            if isinstance(att, dict):
                attempts.append(att)
                modes_seen.append(str(att.get("acquisition_mode") or ""))
                return att
            if isinstance(o, dict) and o.get("acquisition_mode"):
                attempts.append(o)
                modes_seen.append(str(o.get("acquisition_mode") or ""))
                return o
        return None

    # --- Attempt 1: initial_goto from blank ---
    a1 = _one_tick()
    assert a1 is not None
    assert modes_seen[0] == "initial_goto"
    assert shared_labels, f"{SIG_SHARED_RUNNER}: must call _run_shared_camoufox_job"
    assert all("winline" in str(x).lower() for x in shared_labels)
    assert page_names and all(n == NAMED_PAGE for n in page_names), (
        f"{SIG_NAMED_PAGE}: expected only {NAMED_PAGE}, got {page_names}"
    )
    assert registry.create_calls == [NAMED_PAGE], (
        f"{SIG_NAMED_PAGE}: create once, got {registry.create_calls}"
    )
    assert concurrency["max"] == 1, f"{SIG_SERIAL_NO_OVERLAP}: shared job concurrency"

    # After initial_goto page is on live URL. Reset nav counters for dynamic_dom probe.
    page.url = WINLINE_LIVE
    page.goto_calls.clear()
    page.reload_calls.clear()
    page.content_calls = 0
    sleep.calls.clear()

    # --- Attempts 2-3: dynamic_dom on correct live URL (eligible misses) ---
    a2 = _one_tick(advance=POLL_INTERVAL)
    assert a2 is not None
    assert modes_seen[-1] == "dynamic_dom"
    assert page.goto_calls == [], f"{SIG_DYNAMIC_DOM_LIVE}: goto must be 0 on live URL"
    assert page.reload_calls == [], f"{SIG_DYNAMIC_DOM_LIVE}: reload must be 0 on live URL"
    assert sleep.calls == [], f"{SIG_DYNAMIC_DOM_LIVE}: sleep must be 0 on live dynamic_dom"
    assert page.content_calls >= 1

    a3 = _one_tick(advance=POLL_INTERVAL)
    assert a3 is not None
    assert modes_seen[-1] == "dynamic_dom"
    # Still zero nav on live
    assert page.goto_calls == []
    assert page.reload_calls == []
    assert sleep.calls == []

    # --- dynamic_dom repair: blank / root / wrong (direct page-policy, same page) ---
    for start_url, case_id in (
        ("about:blank", "blank"),
        (WINLINE_ROOT, "root"),
        (WINLINE_WRONG, "wrong_host"),
    ):
        page.url = start_url
        before_goto = len(page.goto_calls)
        before_reload = len(page.reload_calls)
        sleep.calls.clear()
        load_status, load_error, html, visible, body_text, diag = (
            odds_parser._load_site_render_payload_camoufox(
                page,
                WINLINE_LIVE,
                initial_wait_seconds=0.0,
                scroll_wait_seconds=0.0,
                acquisition_mode="dynamic_dom",
            )
        )
        assert len(page.goto_calls) == before_goto + 1, (
            f"{SIG_DYNAMIC_DOM_REPAIR}:{case_id} expected exactly one goto"
        )
        assert page.goto_calls[-1]["url"] == WINLINE_LIVE
        assert len(page.reload_calls) == before_reload, (
            f"{SIG_DYNAMIC_DOM_REPAIR}:{case_id} must not reload"
        )
        assert sleep.calls == [], f"{SIG_DYNAMIC_DOM_REPAIR}:{case_id} must not sleep"
        assert page.url == WINLINE_LIVE
        assert load_status == "ok"
        assert diag.get("acquisition_mode") == "dynamic_dom"

    # Restore stable live page for miss streak continuation (3 consecutive already
    # from a1-a3 if eligible; a1 was initial_goto miss too).
    page.url = WINLINE_LIVE
    page.goto_calls.clear()
    page.reload_calls.clear()
    sleep.calls.clear()

    # After 3 consecutive eligible misses, controlled_reload is due if spacing ok.
    # a1,a2,a3 were misses → next should be controlled_reload (last_reload None).
    a4 = _one_tick(advance=POLL_INTERVAL)
    assert a4 is not None, f"{SIG_CONTROLLED_RELOAD}: expected attempt after 3 misses"
    assert modes_seen[-1] == "controlled_reload", (
        f"{SIG_CONTROLLED_RELOAD}: expected controlled_reload, modes={modes_seen}"
    )
    assert page.goto_calls == [], f"{SIG_CONTROLLED_RELOAD}: no goto on controlled_reload"
    assert len(page.reload_calls) == 1, (
        f"{SIG_CONTROLLED_RELOAD}: exactly one reload, got {page.reload_calls}"
    )
    assert sleep.calls == [], f"{SIG_CONTROLLED_RELOAD}: no settle sleep"
    reload_mono_1 = float(
        a4.get("last_reload_at_monotonic") or a4.get("attempt_finished_at_monotonic") or clock.mono
    )

    # Immediate next ticks within <60s must NOT reload again (dynamic_dom).
    page.reload_calls.clear()
    page.goto_calls.clear()
    sleep.calls.clear()
    a5 = _one_tick(advance=POLL_INTERVAL)
    assert a5 is not None
    assert modes_seen[-1] == "dynamic_dom", (
        f"{SIG_CONTROLLED_RELOAD}: within 60s spacing must stay dynamic_dom, got {modes_seen[-1]}"
    )
    assert page.reload_calls == [], f"{SIG_CONTROLLED_RELOAD}: no reload inside 60s window"
    assert page.goto_calls == []
    assert sleep.calls == []

    # Advance remaining to satisfy >=60s since last reload, keep miss streak.
    # Need 2 more stable misses to re-hit 3 consecutive, then spacing gate.
    # After reload, consecutive_misses typically resets or continues per poller;
    # drive enough misses + time to observe second controlled_reload only after 60s.
    for _ in range(6):
        page.reload_calls.clear()
        att = _one_tick(advance=POLL_INTERVAL)
        if att is None:
            continue
        if modes_seen[-1] == "controlled_reload":
            break
    # If second reload happened, spacing from first must be >= 60s.
    reload_attempts = [a for a in attempts if a.get("acquisition_mode") == "controlled_reload"]
    assert len(reload_attempts) >= 1, f"{SIG_CONTROLLED_RELOAD}: at least one reload mode"
    if len(reload_attempts) >= 2:
        t0 = float(reload_attempts[0]["attempt_started_at_monotonic"])
        t1 = float(reload_attempts[1]["attempt_started_at_monotonic"])
        assert (t1 - t0) >= RELOAD_SPACING - 1e-6, (
            f"{SIG_CONTROLLED_RELOAD}: reload spacing {t1 - t0} < {RELOAD_SPACING}"
        )

    # Same page object throughout
    assert registry.get_or_create_page(NAMED_PAGE) is page
    assert registry.create_calls == [NAMED_PAGE]
    assert concurrency["max"] == 1
    assert sent == [], "collector path must not send messages / bets"
    assert evidence.is_file()
    doc = json.loads(evidence.read_text(encoding="utf-8"))
    assert doc.get("canonical_key") == CANONICAL or CANONICAL in str(doc.get("canonical_key") or "")
    assert "acquisition_mode" in doc


def test_int_dynamic_dom_live_and_repair_direct_policy(monkeypatch) -> None:
    """Focused page-policy cross-check used by OBSERVE field mapping."""
    sleep = _SleepCounter()
    monkeypatch.setattr(odds_parser.time, "sleep", sleep)
    body = f"{TEAM1} {TEAM2} live"

    # Live URL
    page = _CountingPage(html=_html(body), body_text=body, url=WINLINE_LIVE, name=NAMED_PAGE)
    st, err, html, vis, bt, diag = odds_parser._load_site_render_payload_camoufox(
        page, WINLINE_LIVE, initial_wait_seconds=0.0, scroll_wait_seconds=0.0, acquisition_mode="dynamic_dom"
    )
    assert page.goto_calls == [] and page.reload_calls == [] and sleep.calls == []
    assert st == "ok" and diag.get("acquisition_mode") == "dynamic_dom"

    # Repair cases
    for start in ("about:blank", WINLINE_ROOT, WINLINE_WRONG):
        page = _CountingPage(html=_html(body), body_text=body, url=start, name=NAMED_PAGE)
        sleep.calls.clear()
        st, err, html, vis, bt, diag = odds_parser._load_site_render_payload_camoufox(
            page,
            WINLINE_LIVE,
            initial_wait_seconds=0.0,
            scroll_wait_seconds=0.0,
            acquisition_mode="dynamic_dom",
        )
        assert len(page.goto_calls) == 1
        assert page.reload_calls == []
        assert sleep.calls == []
        assert page.url == WINLINE_LIVE

    # controlled_reload once
    page = _CountingPage(html=_html(body), body_text=body, url=WINLINE_LIVE, name=NAMED_PAGE)
    sleep.calls.clear()
    st, err, html, vis, bt, diag = odds_parser._load_site_render_payload_camoufox(
        page,
        WINLINE_LIVE,
        initial_wait_seconds=0.0,
        scroll_wait_seconds=0.0,
        acquisition_mode="controlled_reload",
    )
    assert page.goto_calls == []
    assert len(page.reload_calls) == 1
    assert sleep.calls == []
    assert diag.get("acquisition_mode") == "controlled_reload"


# ---------------------------------------------------------------------------
# SIG: concurrent publication race-safety
# ---------------------------------------------------------------------------


def test_int_concurrent_publishers_valid_json_zero_collisions(tmp_path) -> None:
    """INT.pub.* — concurrent writers, whole JSON, no collisions/errors."""
    _clear_state()
    evidence = tmp_path / "latest.json"
    evidence.write_text('{"seed": true}\n', encoding="utf-8")
    errors: List[str] = []
    barrier = threading.Barrier(12)
    lock = threading.Lock()

    def worker(i: int) -> None:
        payload = {
            "worker": i,
            "blob": ("Z" * 400) + str(i),
            "ok": True,
            "n": list(range(80)),
            "canonical_key": CANONICAL,
        }
        try:
            barrier.wait(timeout=5)
        except Exception as exc:
            with lock:
                errors.append(f"barrier:{exc}")
            return
        try:
            cs._winline_write_current_map_evidence(payload, path=evidence)
            if payload.get("_evidence_write_error"):
                with lock:
                    errors.append(str(payload["_evidence_write_error"]))
        except Exception as exc:
            with lock:
                errors.append(f"raise:{type(exc).__name__}:{exc}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        futs = [pool.submit(worker, i) for i in range(12)]
        for f in concurrent.futures.as_completed(futs):
            f.result(timeout=15)

    assert not errors, f"{SIG_PUBLICATION_RACE}: writer errors {errors}"
    assert evidence.is_file()
    data = json.loads(evidence.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    assert data.get("ok") is True or data.get("seed") is True
    # Leftover temps must be full JSON if present
    leftovers = list(tmp_path.glob(".latest.json.*.tmp")) + list(tmp_path.glob("*.tmp"))
    for p in leftovers:
        if p.is_file() and p.stat().st_size > 0:
            json.loads(p.read_text(encoding="utf-8"))

    # Source contract: dedicated lock + unique temp
    src = (BASE_DIR / "cyberscore_try.py").read_text(encoding="utf-8")
    assert "_winline_current_map_evidence_lock" in src
    write_fn_src = src.split("def _winline_write_current_map_evidence", 1)[1].split("\ndef ", 1)[0]
    assert "_winline_current_map_evidence_lock" in write_fn_src
    assert "token_hex" in write_fn_src or "uuid" in write_fn_src


def test_int_evidence_path_constant_for_observe() -> None:
    """Document exact production evidence path OBSERVE must tail."""
    path = getattr(cs, "WINLINE_CURRENT_MAP_POLLING_EVIDENCE_PATH", None)
    assert path is not None
    path_s = str(path)
    assert "winline-current-map" in path_s
    assert path_s.endswith("latest.json")
    # Relative from PROJECT_ROOT
    assert str(cs.PROJECT_ROOT / ".hermes" / "runtime" / "winline-current-map" / "latest.json") == path_s or (
        Path(path_s).name == "latest.json"
    )


def test_int_send_bet_counters_untouched(tmp_path, monkeypatch) -> None:
    """Wiring must not invoke send_message / mutate BOOKMAKER_PREFETCH."""
    _clear_state()
    clock = FakeClock()
    sent: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda m, **_k: sent.append(str(m)))
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(
        cs,
        "start_winline_current_map_polling_scheduler",
        lambda **_k: True,
        raising=False,
    )
    before = bool(getattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False))

    def collector(**kwargs):
        return _missing_payload(
            market_status="open",
            p1_odds=1.9,
            p2_odds=2.1,
            page_valid=True,
        )

    cs.ensure_winline_current_map_polling(
        series=SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        producer_pid=1,
        producer_start_generation="g",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=collector,
        evidence_path=tmp_path / "e.json",
    )
    cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    assert sent == []
    assert bool(getattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False)) is before
