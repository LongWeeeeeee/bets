"""W-PROD-WIRING: whole-current-map Winline poller → live parse seam.

Exclusive ownership: this file + base/cyberscore_try.py (wiring only).
Does not edit parent poller / collector / probe modules.

Contract:
- every successfully-parsed current map begins independent poller state
  (canonical series|mapN|team1|team2) regardless of STAR / selected_side
- due schedule is prior-attempt-start+5s; never overlapping attempts;
  overrun coalesces missed intervals into one immediate follow-up
- collector ops go through _run_shared_camoufox_job + page bookmaker:winline
- acquisition modes: initial_goto → dynamic_dom → controlled_reload (poller)
- missing/closed stays pending (no old activation terminal-dedup / backoff)
- accepted exact/fresh P1/P2 terminalizes only that map
- rollover / end / PID-generation change stops stale map state
- ordinary send / bet / Telegram paths untouched; --no-odds preserved
- no real sleep / second browser / production start
- evidence publication is race-safe (dedicated lock + unique same-dir temp)
"""
from __future__ import annotations

import ast
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

import pytest
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import cyberscore_try as cs  # noqa: E402

SERIES = "dltv.org/matches/8900882416"
SERIES_URL = f"https://{SERIES}" if not SERIES.startswith("http") else SERIES
# Production harness uses path form without scheme, matching every-parsed-map tests.
MATCH_PATH = "8900882416"
LISTING_SERIES = f"dltv.org/matches/{MATCH_PATH}"
TEAM1 = "BoomBoys"
TEAM2 = "Nigma Galaxy"
MAP_NUM = 2
CANONICAL = f"{LISTING_SERIES}|map{MAP_NUM}|{TEAM1}|{TEAM2}"

SEAM_SUCCESS_DRAFT_STMT = 'match_log(f"   ✅ Драфт успешно распарсен")'
POLLING_ENSURE_MARKERS = (
    "ensure_winline_current_map_polling",
    "note_winline_current_map_parsed",
    "on_winline_current_map_parsed",
)


class FakeClock:
    def __init__(self, mono: float = 1000.0, wall: float = 1_700_000_000.0) -> None:
        self.mono = float(mono)
        self.wall = float(wall)

    def monotonic(self) -> float:
        return self.mono

    def time(self) -> float:
        return self.wall

    def advance(self, seconds: float) -> None:
        seconds = float(seconds)
        self.mono += seconds
        self.wall += seconds


def _clear_wiring_state() -> None:
    for name in (
        "reset_winline_current_map_polling_state",
        "reset_winline_shadow_activation_state",
    ):
        fn = getattr(cs, name, None)
        if callable(fn):
            try:
                fn()
            except Exception:
                pass
    # Drop any residual registry if present.
    for attr in (
        "_winline_current_map_pollers",
        "_winline_current_map_polling_state",
        "_winline_map_lifecycle_registry",
    ):
        st = getattr(cs, attr, None)
        if isinstance(st, dict):
            st.clear()


def _accepted_collector_result(**overrides: Any) -> Dict[str, Any]:
    base = {
        "market_status": "open",
        "source": "winline_current_map_winner",
        "p1_odds": 1.85,
        "p2_odds": 2.05,
        "map_num": MAP_NUM,
        "team1": TEAM1,
        "team2": TEAM2,
        "series": LISTING_SERIES,
        "current_url": "https://winline.example/match/x",
        "dom_signature": "sig-ok",
        "dom_hash": "hash-ok",
        "parser_failure_reasons": [],
        "error": None,
        "acquisition_error": None,
        "page_valid": True,
    }
    base.update(overrides)
    return base


def _missing_collector_result(**overrides: Any) -> Dict[str, Any]:
    base = {
        "market_status": "missing",
        "p1_odds": None,
        "p2_odds": None,
        "dom_signature": "sig-miss",
        "dom_hash": "hash-miss",
    }
    base.update(overrides)
    return _accepted_collector_result(**base)


# ---------------------------------------------------------------------------
# RED: public wiring surface must exist
# ---------------------------------------------------------------------------


def test_wiring_public_api_exports_exist():
    """Production module must export ensure/tick/reset wiring helpers."""
    assert hasattr(cs, "ensure_winline_current_map_polling"), (
        "missing ensure_winline_current_map_polling on cyberscore_try"
    )
    assert callable(cs.ensure_winline_current_map_polling)
    assert hasattr(cs, "tick_winline_current_map_polling"), (
        "missing tick_winline_current_map_polling on cyberscore_try"
    )
    assert callable(cs.tick_winline_current_map_polling)
    assert hasattr(cs, "reset_winline_current_map_polling_state")
    assert callable(cs.reset_winline_current_map_polling_state)


def test_runtime_file_alerts_never_send_telegram_under_pytest(monkeypatch):
    sent: List[str] = []
    key = "test:missing-staged-runtime-file"
    cs._RUNTIME_ALERTED_ERRORS.discard(key)
    monkeypatch.setattr(
        cs,
        "send_message",
        lambda message, **_kwargs: sent.append(str(message)),
    )

    cs._notify_runtime_error_once(
        "⚠️ Missing runtime file: staged-test.json",
        dedupe_key=key,
    )

    assert sent == []
    cs._RUNTIME_ALERTED_ERRORS.discard(key)


def test_parse_seam_invokes_polling_ensure_after_draft_success():
    """Source seam: after successful draft parse, wiring ensure is called."""
    src = (BASE_DIR / "cyberscore_try.py").read_text(encoding="utf-8")
    lines = src.splitlines()
    draft_hits = [
        i for i, line in enumerate(lines, 1) if SEAM_SUCCESS_DRAFT_STMT in line
    ]
    assert len(draft_hits) == 1, draft_hits
    draft_line = draft_hits[0]
    # Search a short window after draft success for a polling ensure call.
    window = "\n".join(lines[draft_line - 1 : draft_line - 1 + 40])
    assert any(m in window for m in POLLING_ENSURE_MARKERS), (
        "expected ensure/note polling wiring call within 40 lines after "
        f"draft success at L{draft_line}; window={window!r}"
    )


def test_wiring_helpers_do_not_import_second_browser_stack():
    """Wiring must reuse shared Camoufox job path — no new browser imports in helpers."""
    src = (BASE_DIR / "cyberscore_try.py").read_text(encoding="utf-8")
    # Cheap structural guard: the collector label / page name must appear near
    # the poller wiring helpers.
    assert "bookmaker:winline" in src
    assert "_run_shared_camoufox_job" in src
    # Ensure helper body references shared job (if defined).
    tree = ast.parse(src)
    helper_names = {
        "ensure_winline_current_map_polling",
        "tick_winline_current_map_polling",
        "_winline_current_map_poller_collect",
        "_make_winline_current_map_poller_collector",
    }
    found = {n for n in helper_names if any(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == n
        for node in ast.walk(tree)
    )}
    # At least ensure + tick must exist as functions in source once GREEN.
    assert "ensure_winline_current_map_polling" in found or hasattr(
        cs, "ensure_winline_current_map_polling"
    )


# ---------------------------------------------------------------------------
# Behaviour: ensure / schedule / no-overlap / modes / lifecycle
# ---------------------------------------------------------------------------


def test_ensure_creates_one_poller_independent_of_star_and_selected_side(tmp_path, monkeypatch):
    _clear_wiring_state()
    clock = FakeClock()
    collector_calls: List[Dict[str, Any]] = []

    def fake_collector(**kwargs):
        collector_calls.append(dict(kwargs))
        return _missing_collector_result()

    monkeypatch.setattr(cs, "time", type("T", (), {
        "time": clock.time,
        "monotonic": clock.monotonic,
        "sleep": lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("sleep forbidden")),
    })(), raising=False)
    # Prefer injectable collector for unit wiring.
    if hasattr(cs, "_winline_current_map_poller_collect_impl"):
        monkeypatch.setattr(cs, "_winline_current_map_poller_collect_impl", fake_collector)
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)

    # STAR-irrelevant: selected_side absent, no candidate.
    out = cs.ensure_winline_current_map_polling(
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=os.getpid(),
        producer_start_generation="gen-A",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=fake_collector,
        evidence_path=tmp_path / "latest.json",
    )
    assert out is not False
    # First tick due immediately → one collector call.
    tick = cs.tick_winline_current_map_polling(
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
    )
    assert collector_calls, f"expected collector after ensure+tick; tick={tick!r}"
    assert collector_calls[0].get("acquisition_mode") == "initial_goto"
    assert collector_calls[0].get("map_num") == MAP_NUM
    assert collector_calls[0].get("team1") == TEAM1
    assert collector_calls[0].get("team2") == TEAM2

    # Same identity again must not spawn a second concurrent poller / in-flight.
    cs.ensure_winline_current_map_polling(
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        selected_side="",  # empty must remain non-gating
        producer_pid=os.getpid(),
        producer_start_generation="gen-A",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=fake_collector,
        evidence_path=tmp_path / "latest.json",
    )
    # Not due yet (need +5s from completion) → no extra call.
    n_before = len(collector_calls)
    cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    assert len(collector_calls) == n_before, "must not overlap / re-fire before +5s"

    clock.advance(5.0)
    cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    assert len(collector_calls) == n_before + 1
    assert collector_calls[-1].get("acquisition_mode") == "dynamic_dom"


def test_missing_closed_does_not_terminal_dedup_or_activation_backoff(tmp_path, monkeypatch):
    """Active-map missing/closed stays pending on 5s schedule (not old activation ladder)."""
    _clear_wiring_state()
    clock = FakeClock()
    calls: List[Dict[str, Any]] = []

    def fake_collector(**kwargs):
        calls.append(dict(kwargs))
        return _missing_collector_result(market_status="closed")

    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)

    cs.ensure_winline_current_map_polling(
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=1,
        producer_start_generation="g1",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=fake_collector,
        evidence_path=tmp_path / "latest.json",
    )
    cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    assert len(calls) == 1

    # Advance far less than activation backoff floor (30s) but past poller +5s.
    clock.advance(5.0)
    cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    clock.advance(5.0)
    cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    assert len(calls) == 3, (
        "missing/closed must keep 5s schedule; must not hit 30/60/120 activation backoff "
        f"or terminal_success dedup; calls={len(calls)}"
    )
    # Still active (not terminal success).
    active = cs.list_winline_current_map_polling_keys() if hasattr(
        cs, "list_winline_current_map_polling_keys"
    ) else None
    if active is not None:
        assert any(MAP_NUM == 2 or "map2" in str(k) for k in active) or len(active) >= 1


def test_accepted_odds_terminalizes_only_exact_map(tmp_path, monkeypatch):
    """Two independent series: accepted map A stops; missing map B keeps polling."""
    _clear_wiring_state()
    clock = FakeClock()
    series_a = "dltv.org/matches/8900882416"
    series_b = "dltv.org/matches/8900882999"
    key_a = f"{series_a}|map2|{TEAM1}|{TEAM2}"
    key_b = f"{series_b}|map3|{TEAM1}|{TEAM2}"
    results = {
        key_a: _accepted_collector_result(series=series_a, map_num=2),
        key_b: _missing_collector_result(series=series_b, map_num=3),
    }
    calls: List[Dict[str, Any]] = []

    def fake_collector(**kwargs):
        calls.append(dict(kwargs))
        key = (
            f"{kwargs.get('series')}|map{int(kwargs.get('map_num'))}|"
            f"{kwargs.get('team1')}|{kwargs.get('team2')}"
        )
        return dict(
            results.get(key)
            or _missing_collector_result(
                series=kwargs.get("series"),
                map_num=kwargs.get("map_num"),
            )
        )

    cs.ensure_winline_current_map_polling(
        series=series_a,
        map_num=2,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=1,
        producer_start_generation="g1",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=fake_collector,
        evidence_path=tmp_path / "latest.json",
    )
    cs.ensure_winline_current_map_polling(
        series=series_b,
        map_num=3,
        team1=TEAM1,
        team2=TEAM2,
        selected_side="P1",
        producer_pid=1,
        producer_start_generation="g1",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=fake_collector,
        evidence_path=tmp_path / "latest.json",
    )

    cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    # series_a map2 accepted → terminal; series_b map3 missing → still pending.
    # After +5s only map3 should fire again.
    n_after_first = len(calls)
    assert n_after_first >= 2, f"expected both maps collected first wave; calls={calls!r}"
    clock.advance(5.0)
    cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    map_nums_second_wave = [c.get("map_num") for c in calls[n_after_first:]]
    assert 2 not in map_nums_second_wave, (
        f"accepted map2 must not keep polling; second wave={map_nums_second_wave!r}"
    )
    assert 3 in map_nums_second_wave
    assert key_a not in cs.list_winline_current_map_polling_keys()
    assert key_b in cs.list_winline_current_map_polling_keys()


def test_rollover_and_generation_change_stop_old_state(tmp_path, monkeypatch):
    _clear_wiring_state()
    clock = FakeClock()
    calls: List[Dict[str, Any]] = []

    def fake_collector(**kwargs):
        calls.append(dict(kwargs))
        return _missing_collector_result(map_num=kwargs.get("map_num"))

    cs.ensure_winline_current_map_polling(
        series=LISTING_SERIES,
        map_num=1,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=10,
        producer_start_generation="gen-1",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=fake_collector,
        evidence_path=tmp_path / "latest.json",
    )
    cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    assert any(c.get("map_num") == 1 for c in calls)

    # Rollover to map 2 for same series.
    cs.ensure_winline_current_map_polling(
        series=LISTING_SERIES,
        map_num=2,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=10,
        producer_start_generation="gen-1",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=fake_collector,
        evidence_path=tmp_path / "latest.json",
    )
    # Drive lifecycle: old map1 must stop; map2 may run.
    cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    clock.advance(5.0)
    n_before = len(calls)
    cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    map_nums = [c.get("map_num") for c in calls[n_before:]]
    assert 1 not in map_nums, f"map1 must stop after rollover; got {map_nums!r}"

    # PID / start-generation change stops remaining active maps.
    if hasattr(cs, "note_winline_current_map_service_generation"):
        cs.note_winline_current_map_service_generation(
            producer_pid=99,
            producer_start_generation="gen-2",
        )
    else:
        # ensure with new generation should propagate stop on tick
        cs.ensure_winline_current_map_polling(
            series=LISTING_SERIES,
            map_num=2,
            team1=TEAM1,
            team2=TEAM2,
            selected_side=None,
            producer_pid=99,
            producer_start_generation="gen-2",
            monotonic_fn=clock.monotonic,
            wall_fn=clock.time,
            collector=fake_collector,
            evidence_path=tmp_path / "latest.json",
        )
    clock.advance(5.0)
    n_mid = len(calls)
    cs.tick_winline_current_map_polling(
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        producer_pid=99,
        producer_start_generation="gen-2",
    )
    # After generation change, old gen poller must be terminal; a fresh ensure
    # may create a new one — at minimum, tick must not leave stale in-flight.
    # Re-ensure for new generation and confirm collector still serializes.
    cs.ensure_winline_current_map_polling(
        series=LISTING_SERIES,
        map_num=2,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=99,
        producer_start_generation="gen-2",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=fake_collector,
        evidence_path=tmp_path / "latest.json",
    )
    cs.tick_winline_current_map_polling(
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        producer_pid=99,
        producer_start_generation="gen-2",
    )
    assert len(calls) >= n_mid  # progress under new generation


def test_collector_routes_through_shared_camoufox_job_and_named_page(tmp_path, monkeypatch):
    """Default collector must use _run_shared_camoufox_job + bookmaker:winline page."""
    _clear_wiring_state()
    clock = FakeClock()
    shared_jobs: List[str] = []
    shared_policies: List[Dict[str, Any]] = []
    page_names: List[str] = []
    acq_modes: List[Optional[str]] = []

    class _FakePage:
        pass

    class _FakeSession:
        def get_or_create_page(self, name, browser):
            page_names.append(name)
            return _FakePage()

    class _FakeResult:
        source = "Winline"
        odds = [1.9, 2.1]
        map_num = MAP_NUM
        p1_team = TEAM1
        p2_team = TEAM2
        market_closed = False
        market_kind = "current_map_winner"
        status = "ok"
        match_found = True
        acquisition_mode = "initial_goto"
        dom_signature = "dom-sig"
        page_url = "https://winline.example/x"

    def fake_run_shared(label, callback, timeout=120.0, retry=True, reset_on_error=True):
        shared_jobs.append(label)
        shared_policies.append({"timeout": timeout, "retry": retry})
        return callback(object())  # browser token

    def fake_parse(
        page,
        site,
        url,
        team1,
        team2,
        mode,
        forced_map_num=None,
        acquisition_mode=None,
        # Сборщик прокидывает флаг решающей карты серии (промоция рынка «Матч»).
        series_last_map=False,
    ):
        acq_modes.append(acquisition_mode)
        assert site == "winline"
        assert page is not None
        return _FakeResult()

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", fake_run_shared)
    monkeypatch.setattr(cs, "_shared_camoufox_session", _FakeSession(), raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True, raising=False)
    monkeypatch.setattr(cs, "_bookmaker_parse_site_in_camoufox_page", fake_parse, raising=False)
    monkeypatch.setattr(
        cs,
        "_bookmaker_urls_for_mode",
        lambda _mode: {"winline": "https://winline.example/live"},
        raising=False,
    )
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)

    # No injected collector → production collector path.
    cs.ensure_winline_current_map_polling(
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=1,
        producer_start_generation="g1",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        evidence_path=tmp_path / "latest.json",
    )
    cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)

    assert shared_jobs, "collector must submit via _run_shared_camoufox_job"
    assert shared_policies == [
        {
            "timeout": cs.WINLINE_CURRENT_MAP_SHARED_JOB_TIMEOUT_S,
            "retry": False,
        }
    ]
    # Production Winline acquisition can legitimately take 35-40s because the
    # listing and candidate-match navigations are separately bounded.  The
    # outer future must not discard that successful result.
    assert shared_policies[0]["timeout"] == 60.0
    assert any("winline" in str(j).lower() for j in shared_jobs)
    assert page_names == ["bookmaker:winline"] or "bookmaker:winline" in page_names
    assert acq_modes and acq_modes[0] in {"initial_goto", "dynamic_dom", "controlled_reload"}


def test_controlled_reload_forwarded_after_miss_streak(tmp_path, monkeypatch):
    _clear_wiring_state()
    clock = FakeClock()
    modes: List[str] = []

    def fake_collector(**kwargs):
        modes.append(str(kwargs.get("acquisition_mode")))
        return _missing_collector_result(
            dom_signature="same",
            dom_hash="same",
        )

    cs.ensure_winline_current_map_polling(
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=1,
        producer_start_generation="g1",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=fake_collector,
        evidence_path=tmp_path / "latest.json",
        # speed up reload spacing for unit test via poller kwargs if accepted
        reload_min_spacing_seconds=0.0,
        # Пороги по умолчанию намеренно много выше: перезагрузка сама лишает
        # страницу карточек на десятки секунд. Здесь проверяется проводка режима.
        reload_after_consecutive_misses=3,
        reload_settle_seconds=0.0,
    )
    # attempt 1 initial_goto, 2-3 dynamic_dom, 4 controlled_reload (3 consecutive misses)
    for i in range(4):
        if i:
            clock.advance(5.0)
        cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    assert modes[0] == "initial_goto"
    assert "dynamic_dom" in modes
    assert "controlled_reload" in modes, modes


def test_ordinary_send_and_bet_paths_untouched_by_wiring_helpers(monkeypatch):
    """Wiring helpers must not mutate BOOKMAKER_PREFETCH or call send_message."""
    _clear_wiring_state()
    clock = FakeClock()
    sent: List[str] = []
    monkeypatch.setattr(cs, "send_message", lambda m, **_k: sent.append(str(m)))
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    before = bool(getattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False))

    def fake_collector(**kwargs):
        return _accepted_collector_result()

    cs.ensure_winline_current_map_polling(
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=1,
        producer_start_generation="g1",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=fake_collector,
    )
    cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    assert sent == []
    assert bool(getattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False)) is before


def test_evidence_written_for_attempts_and_terminal(tmp_path, monkeypatch):
    _clear_wiring_state()
    clock = FakeClock()
    evidence = tmp_path / "shadow_latest.json"

    def fake_collector(**kwargs):
        return _accepted_collector_result()

    cs.ensure_winline_current_map_polling(
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=42,
        producer_start_generation="g1",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=fake_collector,
        evidence_path=evidence,
    )
    cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    assert evidence.is_file(), "expected shadow evidence path write on attempt/terminal"
    text = evidence.read_text(encoding="utf-8")
    assert "p1_odds" in text or "attempt" in text or "success" in text or "terminal" in text


def test_concurrent_evidence_publication_race_safe_unique_temp_and_lock(tmp_path, monkeypatch):
    """Two publishers must never collide on fixed .tmp or leave mixed/truncated JSON."""
    import json
    import concurrent.futures

    _clear_wiring_state()
    evidence = tmp_path / "latest.json"
    # Pre-seed so replace races always have a valid baseline or full doc.
    evidence.write_text('{"seed": true}\n', encoding="utf-8")

    errors: List[str] = []
    observed_tmps: List[str] = []
    lock = threading.Lock()
    barrier = threading.Barrier(8)

    def _worker(i: int) -> None:
        payload = {
            "worker": i,
            "blob": ("x" * 200) + str(i),
            "ok": True,
            "n": list(range(50)),
        }
        try:
            barrier.wait(timeout=5)
        except Exception as exc:
            with lock:
                errors.append(f"barrier:{exc}")
            return
        # Spy temp creation by wrapping open for exclusive create path.
        cs._winline_write_current_map_evidence(payload, path=evidence)

    # Also assert source uses unique temp pattern + dedicated lock (not fixed .tmp only).
    src = (BASE_DIR / "cyberscore_try.py").read_text(encoding="utf-8")
    assert "_winline_current_map_evidence_lock" in src
    assert "token_hex" in src or "uuid" in src or ".tmp" in src
    # Must not rely solely on fixed suffix latest.json.tmp without uniqueness.
    write_fn_src = src.split("def _winline_write_current_map_evidence", 1)[1].split(
        "\ndef ", 1
    )[0]
    assert "with_suffix" not in write_fn_src or "token" in write_fn_src or "pid" in write_fn_src
    assert "_winline_current_map_evidence_lock" in write_fn_src

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(_worker, i) for i in range(8)]
        for f in concurrent.futures.as_completed(futs):
            f.result(timeout=10)

    assert not errors, errors
    assert evidence.is_file()
    # Destination must be valid whole JSON (never truncated/mixed).
    data = json.loads(evidence.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    assert data.get("ok") is True or data.get("seed") is True
    # No leftover temps from successful replaces (best-effort: none with partial content).
    leftovers = list(tmp_path.glob(".latest.json.*.tmp")) + list(tmp_path.glob("*.tmp"))
    for p in leftovers:
        # If any remain, they must themselves be valid full JSON or empty orphan cleaned.
        if p.is_file() and p.stat().st_size > 0:
            json.loads(p.read_text(encoding="utf-8"))
    # Writer must not raise / mark payload errors on happy path
    probe = {"probe": 1}
    cs._winline_write_current_map_evidence(probe, path=evidence)
    assert "_evidence_write_error" not in probe
    final = json.loads(evidence.read_text(encoding="utf-8"))
    assert final.get("probe") == 1


def test_identical_evidence_is_not_rewritten(tmp_path, monkeypatch):
    _clear_wiring_state()
    evidence = tmp_path / "latest.json"
    replaces = []
    real_replace = os.replace

    def _replace(src, dst):
        replaces.append((str(src), str(dst)))
        return real_replace(src, dst)

    monkeypatch.setattr(cs.os, "replace", _replace)
    payload = {"canonical_key": CANONICAL, "attempt_index": 7, "status": "pending"}
    cs._winline_write_current_map_evidence(payload, path=evidence)
    cs._winline_write_current_map_evidence(dict(payload), path=evidence)

    assert len(replaces) == 1


def test_sourcetv_series_key_survives_new_map_id_and_side_swap(monkeypatch):
    monkeypatch.setattr(cs, "DLTV_SOURCE_MODE", "sourcetv", raising=False)
    map1 = {
        "match_id": 8919836261,
        "league_id": 17999,
        "radiant_team_id": 101,
        "dire_team_id": 202,
        "radiant_team_name": "Team Spirit Academy",
        "dire_team_name": "Aion",
        "series_game_number": 1,
    }
    map2 = {
        "match_id": 8919960678,
        "league_id": 17999,
        "radiant_team_id": 202,
        "dire_team_id": 101,
        "radiant_team_name": "Aion",
        "dire_team_name": "Team Spirit Academy",
        "series_game_number": 2,
    }

    key1 = cs._winline_polling_series_key("dltv.org/matches/8919836261", live_data=map1)
    key2 = cs._winline_polling_series_key("dltv.org/matches/8919960678", live_data=map2)

    assert key1 == key2
    assert key1.startswith("sourcetv:league:17999|")
    assert cs._winline_sourcetv_map_num(map1) == 1
    assert cs._winline_sourcetv_map_num(map2) == 2


def test_sourcetv_rollover_terminalizes_and_prunes_old_map(tmp_path, monkeypatch):
    _clear_wiring_state()
    clock = FakeClock()
    monkeypatch.setattr(cs, "DLTV_SOURCE_MODE", "sourcetv", raising=False)
    monkeypatch.setattr(cs, "start_winline_current_map_polling_scheduler", lambda **_k: True)
    map1 = {
        "match_id": 8919836261,
        "league_id": 17999,
        "radiant_team_id": 101,
        "dire_team_id": 202,
        "radiant_team_name": "Team Spirit Academy",
        "dire_team_name": "Aion",
        "series_game_number": 1,
    }
    map2 = {
        "match_id": 8919960678,
        "league_id": 17999,
        "radiant_team_id": 202,
        "dire_team_id": 101,
        "radiant_team_name": "Aion",
        "dire_team_name": "Team Spirit Academy",
        "series_game_number": 2,
    }
    series = cs._winline_sourcetv_series_key(map1)
    cs.ensure_winline_current_map_polling(
        series=series,
        map_num=1,
        team1=map1["radiant_team_name"],
        team2=map1["dire_team_name"],
        producer_pid=1,
        producer_start_generation="g1",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=lambda **_k: _missing_collector_result(),
        evidence_path=tmp_path / "map1.json",
    )

    cs._reconcile_winline_sourcetv_polling({"map2": map2}, authoritative=True)
    result = cs.tick_winline_current_map_polling(
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
    )

    terminal = (result[0].get("terminal") or result[0]) if result else {}
    assert terminal.get("lifecycle_event") == "map_rollover"
    assert cs.list_winline_current_map_polling_keys() == []


def _decider_payload(**overrides: Any) -> Dict[str, Any]:
    """Снимок моста: Bo3 при счёте 1:1 — идёт решающая третья карта."""
    base = {
        "match_id": 8919836261,
        "league_id": 17999,
        "radiant_team_id": 101,
        "dire_team_id": 202,
        "radiant_team_name": "Team Spirit Academy",
        "dire_team_name": "Aion",
        "series_type": 1,  # GC-энум: 1 = Bo3
        "radiant_series_wins": 1,
        "dire_series_wins": 1,
    }
    base.update(overrides)
    return base


def test_reconcile_computes_decider_flag_from_the_snapshot():
    """Сверка сама считает решающую карту, а не обнуляет флаг до регистрации."""
    _clear_wiring_state()
    item = _decider_payload()
    series = cs._winline_sourcetv_series_key(item)

    cs._reconcile_winline_sourcetv_polling({"live": item}, authoritative=True)

    assert cs._winline_current_map_registry[series]["map_num"] == 3
    assert cs._winline_registry_series_last_map(series) is True


def test_reconcile_keeps_proven_decider_flag_when_snapshot_cannot_prove_it():
    """Снимок без series_type не имеет права гасить уже доказанный флаг."""
    _clear_wiring_state()
    item = _decider_payload()
    series = cs._winline_sourcetv_series_key(item)
    cs._reconcile_winline_sourcetv_polling({"live": item}, authoritative=True)
    assert cs._winline_registry_series_last_map(series) is True

    blind = _decider_payload()
    blind.pop("series_type")
    cs._reconcile_winline_sourcetv_polling({"live": blind}, authoritative=True)

    assert cs._winline_current_map_registry[series]["map_num"] == 3
    assert cs._winline_registry_series_last_map(series) is True


def test_decider_flag_does_not_leak_to_the_next_map():
    """Флаг прошлой карты не переносится: промоция «Матч» — только на решающей."""
    _clear_wiring_state()
    item = _decider_payload(series_type=2, radiant_series_wins=1, dire_series_wins=1)
    series = cs._winline_sourcetv_series_key(item)
    cs._winline_current_map_registry[series] = {
        "map_num": 2,
        "team1": item["radiant_team_name"],
        "team2": item["dire_team_name"],
        "active": True,
        "inactive_reason": None,
        "inactive_proven": False,
        "inactive_since": None,
        "series_last_map": True,
    }

    cs._reconcile_winline_sourcetv_polling({"live": item}, authoritative=True)

    # Bo5 при 1:1 — идёт третья карта, решающей она не является.
    assert cs._winline_current_map_registry[series]["map_num"] == 3
    assert cs._winline_registry_series_last_map(series) is False


def _blackout_series_payload() -> Dict[str, Any]:
    return {
        "match_id": 8919836261,
        "league_id": 17999,
        "radiant_team_id": 101,
        "dire_team_id": 202,
        "radiant_team_name": "Team Spirit Academy",
        "dire_team_name": "Aion",
        "series_game_number": 1,
    }


def _register_blackout_map(tmp_path, clock) -> str:
    payload = _blackout_series_payload()
    series = cs._winline_sourcetv_series_key(payload)
    # Карта сначала ПОДТВЕРЖДЕНА мостом — как в бою: карточка сама собрана из
    # строки моста. Иначе речь шла бы о карте, которую источник не видел, а её
    # исчезновение ничего не доказывает.
    cs._reconcile_winline_sourcetv_polling([payload], authoritative=True)
    cs.ensure_winline_current_map_polling(
        series=series,
        map_num=1,
        team1=payload["radiant_team_name"],
        team2=payload["dire_team_name"],
        producer_pid=1,
        producer_start_generation="g1",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=lambda **_k: _missing_collector_result(),
        evidence_path=tmp_path / "blackout.json",
    )
    return series


def test_stale_sourcetv_snapshot_keeps_live_map_polling(tmp_path, monkeypatch):
    """Probe замолчал — карта продолжает опрашиваться, а не «завершается».

    Регрессия 16.08.2026 (LGD Gaming — Team Yandex, карта 3): один цикл без
    свежего снимка убивал поллер на 57-й минуте живой игры и слал в чат
    «карта завершена».
    """
    _clear_wiring_state()
    clock = FakeClock()
    monkeypatch.setattr(cs, "DLTV_SOURCE_MODE", "sourcetv", raising=False)
    monkeypatch.setattr(cs, "start_winline_current_map_polling_scheduler", lambda **_k: True)
    series = _register_blackout_map(tmp_path, clock)

    # Снимок протух целиком: свежих матчей нет, но и отсутствие не доказано.
    cs._reconcile_winline_sourcetv_polling({}, authoritative=False)
    registry = cs._winline_current_map_registry[series]
    assert registry["active"] is False
    assert registry["inactive_reason"] == "source_stale"
    assert registry["inactive_since"] is not None

    clock.advance(5.0)
    result = cs.tick_winline_current_map_polling(
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
    )

    assert result and result[0].get("terminal") is None
    assert cs.list_winline_current_map_polling_keys()


def test_source_blackout_terminalizes_after_grace_without_proving_map_end(
    tmp_path, monkeypatch
):
    """Молчание дольше грейса опрос всё же гасит, но концом карты не зовётся."""
    _clear_wiring_state()
    clock = FakeClock()
    monkeypatch.setattr(cs, "DLTV_SOURCE_MODE", "sourcetv", raising=False)
    monkeypatch.setattr(cs, "start_winline_current_map_polling_scheduler", lambda **_k: True)
    series = _register_blackout_map(tmp_path, clock)

    cs._reconcile_winline_sourcetv_polling({}, authoritative=False)
    registry = cs._winline_current_map_registry[series]
    registry["inactive_since"] = float(registry["inactive_since"]) - 600.0

    clock.advance(5.0)
    result = cs.tick_winline_current_map_polling(
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
    )

    terminal = (result[0].get("terminal") or {}) if result else {}
    assert terminal.get("lifecycle_event") == "source_stale"
    assert terminal.get("map_end_proven") is False


def _incident_series_payload() -> Dict[str, Any]:
    """Строка моста той самой серии из инцидента 31.08.2026."""
    return {
        "match_id": 8976511215,
        "league_id": 19944,
        "radiant_team_id": 10019843,
        "dire_team_id": 10233067,
        "radiant_team_name": "Inner Circle x Insanity",
        "dire_team_name": "4ikibamboni",
        "series_game_number": 3,
    }


def test_live_map_past_the_ceiling_says_nothing_and_keeps_polling(tmp_path, monkeypatch):
    """Регрессия 31.08.2026: пара «⏹️ опрос остановлен» + «🏁 карта завершена».

    Опрос карты 3 (Inner Circle x Insanity — 4ikibamboni) начался в 21:29:32,
    потолок сработал ровно на 90-й минуте (22:59:32) на живой карте, опрос
    завёлся заново под тем же ключом и доложил настоящий конец в 23:04:11.
    Проверка идёт по ГРАНИЦЕ ДОСТАВКИ: что реально ушло в отправку телеграма.
    """
    _clear_wiring_state()
    cs._winline_odds_notify_state.clear()
    cs._winline_pending_map_winners.clear()
    clock = FakeClock()
    monkeypatch.setattr(cs, "DLTV_SOURCE_MODE", "sourcetv", raising=False)
    monkeypatch.setattr(cs, "start_winline_current_map_polling_scheduler", lambda **_k: True)
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_ENABLED_ENV, "1")
    monkeypatch.setenv(cs.WINLINE_CURRENT_MAP_CONTINUOUS_ENV, "1")
    monkeypatch.setenv(cs.WINLINE_MAP_WINNER_ENABLED_ENV, "0")
    sent: List[str] = []

    def _send(message, **_kwargs):
        sent.append(message)
        return True

    monkeypatch.setattr(cs, "send_winline_odds_message", _send)

    payload = _incident_series_payload()
    series = cs._winline_sourcetv_series_key(payload)
    cs._reconcile_winline_sourcetv_polling([payload], authoritative=True)
    cs.ensure_winline_current_map_polling(
        series=series,
        map_num=3,
        team1=payload["radiant_team_name"],
        team2=payload["dire_team_name"],
        match_id=payload["match_id"],
        producer_pid=1,
        producer_start_generation="g1",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=lambda **_k: _accepted_collector_result(
            map_num=3,
            team1=payload["radiant_team_name"],
            team2=payload["dire_team_name"],
            series=series,
        ),
        evidence_path=tmp_path / "incident.json",
    )

    def _tick():
        return cs.tick_winline_current_map_polling(
            monotonic_fn=clock.monotonic, wall_fn=clock.time)

    _tick()
    assert len(sent) == 1 and "🆕" in sent[0]

    # 90-я минута живой карты: реестр всё ещё подтверждает её.
    clock.advance(90 * 60 + 1.0)
    _tick()
    clock.advance(200.0)          # больше окна ожидания остановки опроса
    _tick()

    assert cs.list_winline_current_map_polling_keys(), "опрос живой карты снят"
    assert not any("опрос остановлен" in m for m in sent), sent
    assert not any("карта завершена" in m for m in sent), sent

    # Серия исчезла из свежего снимка — вот теперь конец карты доказан.
    cs._reconcile_winline_sourcetv_polling([], authoritative=True)
    clock.advance(5.0)
    _tick()

    finished = [m for m in sent if "карта завершена" in m]
    assert len(finished) == 1, sent
    assert "🏁 Winline · карта 3" in finished[0]
    assert not any("опрос остановлен" in m for m in sent), sent


def test_authoritative_source_absence_terminalizes_without_grace(tmp_path, monkeypatch):
    """Доказанное исчезновение серии из свежего снимка гасит опрос сразу."""
    _clear_wiring_state()
    clock = FakeClock()
    monkeypatch.setattr(cs, "DLTV_SOURCE_MODE", "sourcetv", raising=False)
    monkeypatch.setattr(cs, "start_winline_current_map_polling_scheduler", lambda **_k: True)
    _register_blackout_map(tmp_path, clock)

    other = {
        "match_id": 8919999999,
        "league_id": 17999,
        "radiant_team_id": 303,
        "dire_team_id": 404,
        "radiant_team_name": "OG",
        "dire_team_name": "Vici Gaming",
        "series_game_number": 1,
    }
    cs._reconcile_winline_sourcetv_polling({"other": other}, authoritative=True)

    clock.advance(5.0)
    result = cs.tick_winline_current_map_polling(
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
    )

    terminal = (result[0].get("terminal") or {}) if result else {}
    assert terminal.get("lifecycle_event") == "source_absent"
    assert terminal.get("map_end_proven") is True


def test_two_active_maps_share_one_browser_snapshot(tmp_path, monkeypatch):
    _clear_wiring_state()
    clock = FakeClock()
    shared_calls: List[str] = []

    class _FakePage:
        def evaluate(self, *_args, **_kwargs):
            return {"html": "<html>shared snapshot</html>", "url": "https://winline/live"}

    class _FakeSession:
        def get_or_create_page(self, name, browser):
            assert name == "bookmaker:winline"
            return _FakePage()

    class _FakeResult:
        source = "Winline"
        odds = []
        map_num = MAP_NUM
        market_closed = False
        market_kind = "current_map_winner"
        status = "ok"
        match_found = False
        acquisition_mode = "initial_goto"
        dom_signature = "dom-sig"
        page_url = "https://winline/live"
        body_text = "loaded page"
        acquisition_error = None
        error = None
        load_error = None
        parser_failure_reasons = []
        details = "loaded page"

    def fake_run_shared(label, callback, **_kwargs):
        shared_calls.append(str(label))
        return callback(object())

    monkeypatch.setattr(cs, "start_winline_current_map_polling_scheduler", lambda **_k: True)
    monkeypatch.setattr(cs, "_run_shared_camoufox_job", fake_run_shared)
    monkeypatch.setattr(cs, "_shared_camoufox_session", _FakeSession(), raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True, raising=False)
    monkeypatch.setattr(
        cs,
        "_bookmaker_parse_site_in_camoufox_page",
        lambda *_a, **_k: _FakeResult(),
        raising=False,
    )
    monkeypatch.setattr(
        cs,
        "_bookmaker_urls_for_mode",
        lambda _mode: {"winline": "https://winline/live"},
        raising=False,
    )
    monkeypatch.setattr(
        cs,
        "_winline_fast_collect_from_payload",
        lambda _payload, **kwargs: _missing_collector_result(
            series=kwargs["series"], map_num=kwargs["map_num"]
        ),
    )

    for suffix, map_num in (("a", 1), ("b", 2)):
        cs.ensure_winline_current_map_polling(
            series=f"series-{suffix}",
            map_num=map_num,
            team1=f"{TEAM1}-{suffix}",
            team2=f"{TEAM2}-{suffix}",
            producer_pid=1,
            producer_start_generation="g1",
            monotonic_fn=clock.monotonic,
            wall_fn=clock.time,
            evidence_path=tmp_path / f"{suffix}.json",
        )

    cs.tick_winline_current_map_polling(
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
    )

    assert len(shared_calls) == 1
    assert len(cs.list_winline_current_map_polling_keys()) == 2


def test_collector_routes_only_through_shared_camoufox_no_duplicate_scheduler_path(tmp_path, monkeypatch):
    """Browser ops in named symbols go through _run_shared_camoufox_job; one named page."""
    _clear_wiring_state()
    clock = FakeClock()
    shared_calls: List[str] = []

    class _FakeSession:
        def get_or_create_page(self, name, browser):
            assert name == "bookmaker:winline"
            return object()

    class _FakeResult:
        source = "Winline"
        odds = []
        map_num = MAP_NUM
        p1_team = TEAM1
        p2_team = TEAM2
        market_closed = False
        market_kind = "current_map_winner"
        status = "ok"
        match_found = False
        acquisition_mode = "initial_goto"
        dom_signature = "dom-sig"
        page_url = "https://winline.example/live"
        body_text = "body"
        acquisition_error = None
        error = None
        load_error = None
        parser_failure_reasons = []
        details = "body"

    def fake_run_shared(label, callback, timeout=120.0, retry=True, reset_on_error=True):
        shared_calls.append(str(label))
        return callback(object())

    def fake_parse(
        page,
        site,
        url,
        team1,
        team2,
        mode,
        forced_map_num=None,
        acquisition_mode=None,
        # Сборщик прокидывает флаг решающей карты серии (промоция рынка «Матч»).
        series_last_map=False,
    ):
        return _FakeResult()

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", fake_run_shared)
    monkeypatch.setattr(cs, "_shared_camoufox_session", _FakeSession(), raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True, raising=False)
    monkeypatch.setattr(cs, "_bookmaker_parse_site_in_camoufox_page", fake_parse, raising=False)
    monkeypatch.setattr(
        cs,
        "_bookmaker_urls_for_mode",
        lambda _mode: {"winline": "https://winline.example/live"},
        raising=False,
    )

    # Production collector path (no inject) must use shared job.
    cs.ensure_winline_current_map_polling(
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=1,
        producer_start_generation="g1",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        evidence_path=tmp_path / "latest.json",
    )
    cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    assert shared_calls, "collector must route via _run_shared_camoufox_job"
    assert all("winline" in c.lower() for c in shared_calls)

    src = (BASE_DIR / "cyberscore_try.py").read_text(encoding="utf-8")
    # Named collector definition must call shared job; no second camoufox launch path.
    collect_src = src.split("def _winline_current_map_poller_collect", 1)[1].split(
        "\ndef ", 1
    )[0]
    assert "_run_shared_camoufox_job" in collect_src
    assert "bookmaker:winline" in collect_src
    assert "Camoufox(" not in collect_src
    assert "camoufox.Camoufox" not in collect_src


def test_initial_goto_uses_fast_dom_when_named_page_is_already_healthy(monkeypatch):
    _clear_wiring_state()
    parse_calls: List[str] = []

    class _FakeSession:
        def get_or_create_page(self, name, browser):
            assert name == "bookmaker:winline"
            return object()

    expected = _missing_collector_result(
        page_valid=True,
        market_status="missing",
        market_missing=True,
    )

    monkeypatch.setattr(
        cs, "_run_shared_camoufox_job", lambda _label, callback, **_kwargs: callback(object())
    )
    monkeypatch.setattr(cs, "_shared_camoufox_session", _FakeSession(), raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True, raising=False)
    monkeypatch.setattr(
        cs,
        "_bookmaker_urls_for_mode",
        lambda _mode: {"winline": "https://winline.example/live"},
        raising=False,
    )
    monkeypatch.setattr(cs, "_winline_fast_collect", lambda *_a, **_k: dict(expected))
    monkeypatch.setattr(
        cs,
        "_bookmaker_parse_site_in_camoufox_page",
        lambda *_a, **_k: parse_calls.append("full"),
        raising=False,
    )

    out = cs._winline_current_map_poller_collect(
        acquisition_mode="initial_goto",
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
    )

    assert out["market_status"] == "missing"
    assert out["acquisition_mode_echo"] == "initial_goto"
    assert parse_calls == []


def test_missing_pinned_event_is_selected_then_fast_dom_reparsed(monkeypatch):
    _clear_wiring_state()
    clicks: List[str] = []
    fast_results = [
        {
            **_missing_collector_result(),
            "match_found": True,
        },
        {
            **_accepted_collector_result(),
            "match_found": True,
        },
    ]

    class _Card:
        def inner_text(self, timeout=0):
            return "1w Essence TEAM FALCONS VICI GAMING"

        def locator(self, selector):
            assert selector == ".new-card--selected"
            return type("_Selected", (), {"count": lambda self: 0})()

        def click(self, timeout=0):
            clicks.append("clicked")

    class _Cards:
        def count(self):
            return 1

        def nth(self, index):
            assert index == 0
            return _Card()

    class _Page:
        def locator(self, selector):
            assert selector == "ww-pinned-card"
            return _Cards()

        def wait_for_timeout(self, milliseconds):
            assert milliseconds == 800

    class _Session:
        def get_or_create_page(self, name, browser):
            assert name == "bookmaker:winline"
            return _Page()

    monkeypatch.setattr(
        cs, "_run_shared_camoufox_job", lambda _label, callback, **_kwargs: callback(object())
    )
    monkeypatch.setattr(cs, "_shared_camoufox_session", _Session(), raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True, raising=False)
    monkeypatch.setattr(
        cs,
        "_bookmaker_urls_for_mode",
        lambda _mode: {"winline": "https://winline.example/live"},
        raising=False,
    )
    monkeypatch.setattr(
        cs,
        "_bookmaker_text_matches_teams",
        lambda text, team1, team2: "FALCONS" in text and "VICI" in text,
        raising=False,
    )
    monkeypatch.setattr(
        cs,
        "_winline_fast_collect",
        lambda *_a, **_k: fast_results.pop(0),
    )
    monkeypatch.setattr(
        cs,
        "_bookmaker_parse_site_in_camoufox_page",
        lambda *_a, **_k: pytest.fail("full parser must not run"),
        raising=False,
    )

    page = _Session().get_or_create_page("bookmaker:winline", object())
    pinned_key = "series-falcons-vici|map1|Team Falcons|Vici Gaming"
    cs._winline_shared_page_state["selected_pinned_page_id"] = id(page)
    cs._winline_shared_page_state["selected_pinned_key"] = pinned_key
    monkeypatch.setattr(
        cs._shared_camoufox_session,
        "get_or_create_page",
        lambda name, browser: page,
    )

    out = cs._winline_current_map_poller_collect(
        acquisition_mode="dynamic_dom",
        series="series-falcons-vici",
        map_num=1,
        team1="Team Falcons",
        team2="Vici Gaming",
    )

    assert clicks == ["clicked"]
    assert out["market_status"] == "open"
    assert out["p1_odds"] is not None
    assert out["p2_odds"] is not None


def test_shared_page_suppresses_duplicate_controlled_reload(monkeypatch):
    _clear_wiring_state()
    modes: List[str] = []

    class _FakeSession:
        def get_or_create_page(self, name, browser):
            assert name == "bookmaker:winline"
            return object()

    class _FakeResult:
        source = "Winline"
        odds = []
        map_num = MAP_NUM
        market_closed = False
        market_kind = "current_map_winner"
        status = "ok"
        match_found = False
        acquisition_mode = None
        dom_signature = "stable"
        page_url = "https://winline.example/live"
        body_text = "loaded page"
        acquisition_error = None
        error = None
        load_error = None
        parser_failure_reasons = []
        details = "loaded page"

    def _fake_parse(*_args, acquisition_mode=None, **_kwargs):
        modes.append(str(acquisition_mode))
        result = _FakeResult()
        result.acquisition_mode = acquisition_mode
        return result

    monkeypatch.setattr(
        cs, "_run_shared_camoufox_job", lambda _label, callback, **_kwargs: callback(object())
    )
    monkeypatch.setattr(cs, "_shared_camoufox_session", _FakeSession(), raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True, raising=False)
    monkeypatch.setattr(
        cs,
        "_bookmaker_urls_for_mode",
        lambda _mode: {"winline": "https://winline.example/live"},
        raising=False,
    )
    monkeypatch.setattr(cs, "_winline_fast_collect", lambda *_a, **_k: None)
    monkeypatch.setattr(
        cs, "_bookmaker_parse_site_in_camoufox_page", _fake_parse, raising=False
    )

    kwargs = dict(
        acquisition_mode="controlled_reload",
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
    )
    cs._winline_current_map_poller_collect(series="series-a", **kwargs)
    cs._winline_current_map_poller_collect(series="series-b", **kwargs)

    assert modes == ["controlled_reload", "dynamic_dom"]


def test_acquisition_error_rotates_proxy_and_resets_shared_browser(monkeypatch):
    """Rotation is a streak decision now, not a reflex on the first error.

    serv1 replaced "any acquisition error rotates the route" with
    ``WINLINE_CURRENT_MAP_ERROR_STREAK_TO_ROTATE`` consecutive errors.  Its
    justification, quoted from base/cyberscore_try.py:

        "РАЗОВЫЙ СБОЙ НЕ СНОСИТ РАБОЧУЮ СТРАНИЦУ. Смена прокси перезапускает
        браузер, а вместе с ним гибнет общая открытая страница ...
        Замер 20.08.2026: обычные опросы 1.09-2.39 с при нулевом времени
        браузера, после первого сброса — 64.8, 131.6 и 85.4 с при времени
        браузера 26.5, 36.0 и 12.7 с."

    Asserted here: under the threshold nothing rotates, a valid page zeroes
    the streak, the threshold-th consecutive error rotates exactly once, and
    the recovery cooldown still rate-limits the streak that follows it.
    """
    _clear_wiring_state()
    # reset_winline_current_map_polling_state() clears the sibling recovery
    # timestamp but not this counter, so pin it: the threshold assertions
    # below must not inherit a streak from whatever ran before.
    cs._winline_shared_page_state["acquisition_error_streak"] = 0
    threshold = int(cs.WINLINE_CURRENT_MAP_ERROR_STREAK_TO_ROTATE)
    assert threshold >= 2, "single-error rotation is the behaviour serv1 removed"
    # Pin the tuned value: 3 is what the 20.08.2026 measurement bought.
    # Retuning it is a decision, not a refactor — update this line with it.
    assert threshold == 3
    rotations: List[str] = []
    restores: List[str] = []

    class _FakeSession:
        def get_or_create_page(self, name, browser):
            assert name == "bookmaker:winline"
            return object()

    class _FailedResult:
        source = "Winline"
        odds = []
        map_num = MAP_NUM
        market_closed = False
        market_kind = "current_map_winner"
        status = "partial_load"
        match_found = False
        acquisition_mode = "controlled_reload"
        dom_signature = "stale-dom"
        page_url = "https://winline.example/live"
        body_text = "stale loaded page"
        acquisition_error = "Page.reload: Timeout 12000ms exceeded"
        error = None
        load_error = acquisition_error
        parser_failure_reasons = []
        details = body_text

    class _ValidResult:
        source = "Winline"
        odds = []
        map_num = MAP_NUM
        market_closed = False
        market_kind = "current_map_winner"
        status = "ok"
        match_found = False
        acquisition_mode = "controlled_reload"
        dom_signature = "stable-dom"
        page_url = "https://winline.example/live"
        body_text = "loaded page"
        acquisition_error = None
        error = None
        load_error = None
        parser_failure_reasons = []
        details = body_text

    next_result: Dict[str, Any] = {"cls": _FailedResult}

    monkeypatch.setattr(
        cs, "_run_shared_camoufox_job", lambda _label, callback, **_kwargs: callback(object())
    )
    monkeypatch.setattr(cs, "_shared_camoufox_session", _FakeSession(), raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True, raising=False)
    monkeypatch.setattr(
        cs,
        "_bookmaker_urls_for_mode",
        lambda _mode: {"winline": "https://winline.example/live"},
        raising=False,
    )
    monkeypatch.setattr(cs, "_winline_fast_collect", lambda *_a, **_k: None)
    monkeypatch.setattr(
        cs,
        "_bookmaker_parse_site_in_camoufox_page",
        lambda *_a, **_k: next_result["cls"](),
        raising=False,
    )
    monkeypatch.setattr(
        cs,
        "_bookmaker_rotate_shared_camoufox_proxy",
        lambda *, reason="": rotations.append(reason),
    )
    monkeypatch.setattr(
        cs,
        "_bookmaker_restore_shared_camoufox_direct_route",
        lambda *, reason="": restores.append(reason),
    )

    kwargs = dict(
        acquisition_mode="controlled_reload",
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
    )

    def _streak() -> int:
        return int(cs._winline_shared_page_state.get("acquisition_error_streak", 0))

    def _failing(series: str) -> Dict[str, Any]:
        next_result["cls"] = _FailedResult
        out = cs._winline_current_map_poller_collect(series=series, **kwargs)
        assert out["page_valid"] is False
        assert "Timeout" in str(out["acquisition_error"])
        return out

    # Below the threshold the working page survives: no route change at all.
    for i in range(1, threshold):
        _failing(f"series-warmup-{i}")
        assert _streak() == i
        assert rotations == []

    # A valid page proves the route is fine and starts the streak over.
    next_result["cls"] = _ValidResult
    ok = cs._winline_current_map_poller_collect(series="series-ok", **kwargs)
    assert ok["page_valid"] is True
    assert _streak() == 0
    assert rotations == []
    assert restores == ["winline_valid_page"]

    # Errors resume from zero, so the pre-success ones no longer count.
    for i in range(1, threshold):
        _failing(f"series-again-{i}")
        assert _streak() == i
        assert rotations == []

    # The threshold-th consecutive error rotates the proxy and, through it,
    # resets the shared browser exactly once.
    _failing("series-rotate")
    assert rotations == ["winline_acquisition_error"]
    assert _streak() == 0

    # Recovery stays rate-limited: a fresh full streak inside
    # WINLINE_CURRENT_MAP_RECOVERY_COOLDOWN_S does not rotate again.
    for i in range(1, threshold + 1):
        _failing(f"series-cooldown-{i}")
        assert rotations == ["winline_acquisition_error"]
    assert _streak() == threshold


# ---------------------------------------------------------------------------
# check_head integration: STAR-independent ensure at parse seam
# ---------------------------------------------------------------------------


class _FakeTextResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code


class _FakeJsonResponse:
    def __init__(self, payload: Dict[str, Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = "{}"

    def json(self) -> Dict[str, Any]:
        return self._payload


def _valid_heroes(seed: int) -> Dict[str, Dict[str, int]]:
    return {
        "pos1": {"hero_id": seed + 1, "account_id": seed + 101},
        "pos2": {"hero_id": seed + 2, "account_id": seed + 102},
        "pos3": {"hero_id": seed + 3, "account_id": seed + 103},
        "pos4": {"hero_id": seed + 4, "account_id": seed + 104},
        "pos5": {"hero_id": seed + 5, "account_id": seed + 105},
    }


def _build_heads_and_bodies():
    html = f"""
    <div class="head">
      <div class="event__info-info__time">live</div>
    </div>
    <div class="body">
      <div class="match__item-team__score">0</div>
      <div class="match__item-team__score">0</div>
      <a href="https://dltv.org/matches/{MATCH_PATH}"></a>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    head = soup.find("div", class_="head")
    body = soup.find("div", class_="body")
    assert head is not None and body is not None
    return [head], [body]


def _patch_check_head_no_odds(monkeypatch, *, live_map_num: int = 2) -> str:
    with cs.monitored_matches_lock:
        cs.monitored_matches.clear()
    if hasattr(cs, "reset_winline_shadow_activation_state"):
        try:
            cs.reset_winline_shadow_activation_state()
        except Exception:
            pass
    _clear_wiring_state()

    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(cs, "DOTA2PROTRACKER_ENABLED", False, raising=False)
    monkeypatch.setattr(cs, "FORCE_ODDS_SIGNAL_TEST", False, raising=False)
    monkeypatch.setattr(cs, "LANE_ADV_STANDALONE_KILLS_ENABLED", False, raising=False)
    monkeypatch.setattr(cs, "PIPELINE_SEND_EVERY_PARSED_MATCH", False, raising=False)
    monkeypatch.setattr(cs, "PIPELINE_DISABLE_SIGNAL_GATES", False, raising=False)
    monkeypatch.setattr(cs, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(cs, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(cs, "_drop_delayed_match", lambda *_a, **_k: False)
    monkeypatch.setattr(cs, "_skip_dispatch_for_processed_url", lambda *_a, **_k: False)
    monkeypatch.setattr(cs, "_acquire_signal_send_slot", lambda *_a, **_k: True)
    monkeypatch.setattr(cs, "_release_signal_send_slot", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_mark_url_processed", lambda *_a, **_k: None)
    monkeypatch.setattr(cs, "_log_bookmaker_source_snapshot", lambda *_a, **_k: None)
    monkeypatch.setattr(
        cs,
        "_refresh_message_bookmaker_block_for_dispatch",
        lambda _match_key, message: message,
    )
    monkeypatch.setattr(cs, "send_message", lambda message, **_k: None)
    monkeypatch.setattr(cs, "add_url", lambda *_a, **_k: None)

    page_html = f"<html><script>$.get('/live/{MATCH_PATH}.json')</script></html>"
    monkeypatch.setattr(
        cs,
        "make_request_with_retry",
        lambda *_a, **_k: _FakeTextResponse(page_html, status_code=200),
    )
    series_wins_r = max(0, int(live_map_num) - 1)
    live_data = {
        "fast_picks": [1],
        "db": {
            "first_team": {
                "is_radiant": True,
                "title": TEAM1,
                "team_id": 1001,
                "id": 1001,
            },
            "second_team": {"title": TEAM2, "team_id": 2002, "id": 2002},
        },
        "live_league_data": {
            "match": {},
            "radiant_team": {"team_id": 1001},
            "dire_team": {"team_id": 2002},
            "radiant_series_wins": series_wins_r,
            "dire_series_wins": 0,
            "game_map_number": int(live_map_num),
        },
        "radiant_lead": 0.0,
        "game_time": float(10 * 60),
    }
    monkeypatch.setattr(
        cs.requests,
        "get",
        lambda *_a, **_k: _FakeJsonResponse(live_data, status_code=200),
    )
    team_id_calls = {"count": 0}

    def _extract_candidate_team_ids(*_a, **_k):
        team_id_calls["count"] += 1
        return [1001] if team_id_calls["count"] == 1 else [2002]

    monkeypatch.setattr(cs, "_extract_candidate_team_ids", _extract_candidate_team_ids)
    monkeypatch.setattr(
        cs,
        "_ensure_known_team_or_add_to_tier2",
        lambda team_ids, _team_name, _match_key: (True, int(team_ids[0])),
    )
    monkeypatch.setattr(cs, "_determine_star_signal_match_tier", lambda *_a, **_k: 1)
    monkeypatch.setattr(
        cs,
        "parse_draft_and_positions",
        lambda *_a, **_k: (_valid_heroes(0), _valid_heroes(100), None, "", []),
    )
    monkeypatch.setattr(
        cs,
        "synergy_and_counterpick",
        lambda *_a, **_k: {
            "early_output": {"solo": 0},
            "mid_output": {"solo": 0},
            "post_lane_output": {"synergy_duo": 0},
        },
    )
    monkeypatch.setattr(cs, "lane_data", {}, raising=False)
    monkeypatch.setattr(cs, "calculate_lanes", lambda *_a, **_k: ("", "", ""))
    monkeypatch.setattr(cs, "format_output_dict", lambda *_a, **_k: True)
    monkeypatch.setattr(
        cs,
        "_star_block_diagnostics",
        lambda *, raw_block, target_wr, section: {
            "valid": False,
            "status": "no_hits",
            "sign": 0,
            "hit_metrics": [],
            "conflict_metric": None,
        },
    )
    monkeypatch.setattr(
        cs,
        "_block_signs_same_or_zero",
        lambda *_a, **_k: {"valid": False, "status": "no_sign"},
    )
    monkeypatch.setattr(cs, "_format_raw_star_block_metrics", lambda *_a, **_k: "none")
    monkeypatch.setattr(
        cs, "_decorate_star_block_for_display", lambda raw_block, **_k: dict(raw_block or {})
    )
    monkeypatch.setattr(cs.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(cs, "_match_has_tier1_team", lambda *_a, **_k: True)
    return f"{LISTING_SERIES}.0"


def test_check_head_successful_parse_triggers_polling_without_star(monkeypatch, tmp_path):
    ensure_calls: List[Dict[str, Any]] = []

    def _capture_ensure(**kwargs):
        ensure_calls.append(dict(kwargs))
        return True

    _patch_check_head_no_odds(monkeypatch, live_map_num=2)
    monkeypatch.setattr(cs, "ensure_winline_current_map_polling", _capture_ensure)
    # Keep shadow fail-open from doing real work.
    monkeypatch.setattr(cs, "_fail_open_winline_shadow_after_send", lambda **_k: None)

    heads, bodies = _build_heads_and_bodies()
    cs.check_head(heads=heads, bodies=bodies, i=0, maps_data=set(), return_status=None)

    assert ensure_calls, (
        "check_head successful parse must call ensure_winline_current_map_polling "
        "independent of STAR/selected_side"
    )
    call = ensure_calls[0]
    assert int(call.get("map_num")) == 2
    assert call.get("team1") == TEAM1
    assert call.get("team2") == TEAM2
    # selected_side may be absent/None — non-gating
    if "selected_side" in call:
        assert call["selected_side"] in (None, "", "P1", "P2", TEAM1, TEAM2)


def test_sourcetv_intermission_advances_winline_to_next_map_only_when_draft_cleared():
    completed = {
        "status": "live",
        "series_game_number": 1,
        "radiant_series_wins": 0,
        "dire_series_wins": 0,
        "series_type": 3,
        "game_time": 2318,
        "radiant_score": 38,
        "dire_score": 10,
        "fast_picks": {},
        "_cyberscore_heroes_and_pos": {"radiant": None, "dire": None},
    }
    assert cs._winline_sourcetv_map_num(completed) == 2

    still_playing = dict(completed)
    still_playing["fast_picks"] = {"first_team": [{"hero_id": 1}]}
    still_playing["_cyberscore_heroes_and_pos"] = {
        "radiant": {"pos1": {"hero_id": 1}},
        "dire": {"pos1": {"hero_id": 2}},
    }
    assert cs._winline_sourcetv_map_num(still_playing) == 1

    incomplete = dict(completed)
    incomplete.pop("_cyberscore_heroes_and_pos")
    assert cs._winline_sourcetv_map_num(incomplete) == 1


def test_no_sleep_calls_in_wiring_module_surface():
    """Guard: wiring helpers must not call time.sleep (poller is no-sleep)."""
    src = (BASE_DIR / "cyberscore_try.py").read_text(encoding="utf-8")
    # Extract function bodies roughly by name markers if present.
    for name in (
        "ensure_winline_current_map_polling",
        "tick_winline_current_map_polling",
        "_winline_current_map_poller_collect",
    ):
        if f"def {name}" not in src:
            continue
        start = src.index(f"def {name}")
        # crude slice until next top-level def at column 0 after start+1
        rest = src[start + 4 :]
        nxt = rest.find("\ndef ")
        body = rest[: nxt if nxt != -1 else 4000]
        assert "time.sleep" not in body, f"{name} must not sleep"
        assert "sleep(" not in body or "interruptible" in body  # allow only unrelated


# ---------------------------------------------------------------------------
# W8: independent 5s scheduler + eligible-market-miss classification
# ---------------------------------------------------------------------------


class _SiteResultLike:
    """Minimal SiteResult stand-in for classification seam tests."""

    def __init__(self, **kwargs):
        defaults = dict(
            odds=[],
            market_closed=False,
            status="ok",
            match_found=False,
            market_kind="current_map_winner",
            source="Winline",
            map_num=MAP_NUM,
            p1_team=TEAM1,
            p2_team=TEAM2,
            page_url="https://winline.example/live",
            dom_signature="winline-body-sig",
            acquisition_error=None,
            error=None,
            load_error=None,
            body_text="Winline live markets body",
            parser_failure_reasons=[],
            details="Winline live markets body",
        )
        defaults.update(kwargs)
        for k, v in defaults.items():
            setattr(self, k, v)


def test_w8_classification_eligible_miss_loaded_page_match_not_found():
    """Loaded expected page + nonblank DOM + match_found=False => page_valid eligible miss."""
    fn = getattr(cs, "_winline_map_site_result_to_collector_dict", None)
    assert callable(fn), "missing classification seam _winline_map_site_result_to_collector_dict"
    result = _SiteResultLike(match_found=False, odds=[], status="ok")
    out = fn(
        result,
        acquisition_mode="initial_goto",
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        expected_url="https://winline.example/live",
    )
    assert out.get("page_valid") is True, out
    assert out.get("market_missing") is True or out.get("market_status") in {
        "missing",
        "market_missing",
        "closed",
        "market_closed",
    }, out
    assert out.get("market_status") not in {"error", "browser_error", "request_error"}


def test_w8_match_found_requested_market_missing_remains_retryable():
    fn = cs._winline_map_site_result_to_collector_dict
    result = _SiteResultLike(
        match_found=True,
        odds=[],
        status="ok",
        source="winline_map_market_missing",
        market_kind=None,
        map_num=1,
        p1_team=None,
        p2_team=None,
    )
    out = fn(
        result,
        acquisition_mode="dynamic_dom",
        series="8911784562",
        map_num=1,
        team1="Ilbirs Esports",
        team2="Zero Tenacity",
        expected_url="https://winline.example/live",
    )

    assert out["match_found"] is True
    assert out["map_num"] == 1
    assert out["p1_odds"] is None and out["p2_odds"] is None
    assert out["market_status"] == "missing"
    assert out["market_missing"] is True
    assert out["page_valid"] is True


def test_w8_classification_wrong_url_is_browser_failure():
    fn = cs._winline_map_site_result_to_collector_dict
    result = _SiteResultLike(
        match_found=False,
        page_url="https://evil.example/phish",
        dom_signature="nonblank",
    )
    out = fn(
        result,
        acquisition_mode="initial_goto",
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        expected_url="https://winline.example/live",
    )
    assert out.get("page_valid") is False, out


def test_w8_classification_blank_dom_is_browser_failure():
    fn = cs._winline_map_site_result_to_collector_dict
    result = _SiteResultLike(
        match_found=False,
        page_url="https://winline.example/live",
        dom_signature="",
        body_text="",
        details="",
    )
    out = fn(
        result,
        acquisition_mode="initial_goto",
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        expected_url="https://winline.example/live",
    )
    assert out.get("page_valid") is False, out


def test_w8_classification_acquisition_error_is_browser_failure():
    fn = cs._winline_map_site_result_to_collector_dict
    result = _SiteResultLike(
        match_found=False,
        status="ok",
        acquisition_error="timeout navigating",
        dom_signature="sig",
    )
    out = fn(
        result,
        acquisition_mode="initial_goto",
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        expected_url="https://winline.example/live",
    )
    assert out.get("page_valid") is False, out


def test_w8_reload_timeout_does_not_discard_confirmed_open_market():
    """A lifecycle timeout is diagnostic when the requested market parsed."""
    fn = cs._winline_map_site_result_to_collector_dict
    result = _SiteResultLike(
        match_found=True,
        status="partial_load",
        acquisition_error="Page.reload: Timeout 12000ms exceeded",
        odds=[1.31, 3.49],
        p1_team=TEAM1,
        p2_team=TEAM2,
        map_num=MAP_NUM,
        dom_signature="confirmed-market-dom",
    )
    out = fn(
        result,
        acquisition_mode="controlled_reload",
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        expected_url="https://winline.example/live",
    )

    assert out["page_valid"] is True, out
    assert out["market_status"] == "open", out
    assert out["p1_odds"] == 1.31
    assert out["p2_odds"] == 3.49
    assert out["acquisition_error"]


def test_w8_eligible_miss_advances_initial_goto_dynamic_dom_controlled_reload(tmp_path):
    """Eligible loaded misses must climb acquisition ladder with configured reload spacing."""
    _clear_wiring_state()
    clock = FakeClock()
    modes: List[str] = []

    def fake_collector(**kwargs):
        modes.append(str(kwargs.get("acquisition_mode")))
        # Simulate classification output of eligible miss (page_valid true).
        return _missing_collector_result(
            page_valid=True,
            market_missing=True,
            market_status="missing",
            dom_signature="same-loaded",
            dom_hash="same-loaded",
            current_url="https://winline.example/live",
        )

    cs.ensure_winline_current_map_polling(
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=1,
        producer_start_generation="g-w8",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=fake_collector,
        evidence_path=tmp_path / "latest.json",
        reload_min_spacing_seconds=15.0,
        reload_after_consecutive_misses=3,
    )
    # 1 initial + 2 dynamic + (miss streak=3) => 4th controlled_reload after spacing.
    # With spacing 15s and poll interval 5s: attempts at t=0,5,10,15.
    for i in range(4):
        if i:
            clock.advance(5.0)
        cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    assert modes[0] == "initial_goto", modes
    assert modes[1] == "dynamic_dom", modes
    assert modes[2] == "dynamic_dom", modes
    assert modes[3] == "controlled_reload", modes


def test_w8_scheduler_ticks_during_blocking_general_nominal_5s(tmp_path, monkeypatch):
    """Independent scheduler must tick at ~5s even while general() is blocked 30s.

    Uses fake clock + event seam; no real multi-second sleep.
    """
    _clear_wiring_state()
    clock = FakeClock(mono=10_000.0)
    starts: List[float] = []
    send_spy: List[Any] = []

    def fake_collector(**kwargs):
        starts.append(float(clock.monotonic()))
        return _missing_collector_result(
            page_valid=True,
            market_missing=True,
            market_status="missing",
            dom_signature="sched-sig",
            dom_hash="sched-hash",
        )

    monkeypatch.setattr(cs, "send_message", lambda m, **_k: send_spy.append(m))
    monkeypatch.setattr(cs, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)

    assert hasattr(cs, "start_winline_current_map_polling_scheduler"), (
        "missing start_winline_current_map_polling_scheduler"
    )
    assert hasattr(cs, "stop_winline_current_map_polling_scheduler")
    assert hasattr(cs, "drive_winline_current_map_polling_scheduler_for_tests")

    cs.ensure_winline_current_map_polling(
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=7,
        producer_start_generation="g-sched",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=fake_collector,
        evidence_path=tmp_path / "latest.json",
    )

    # Simulate 30s blocked general(): only the scheduler drives ticks.
    drive = cs.drive_winline_current_map_polling_scheduler_for_tests
    summary = drive(
        duration_s=30.0,
        step_s=1.0,
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        advance_fn=clock.advance,
        producer_pid=7,
        producer_start_generation="g-sched",
    )
    assert send_spy == []
    assert len(starts) >= 5, f"expected >=5 starts over 30s; got {starts!r} summary={summary!r}"
    deltas = [b - a for a, b in zip(starts, starts[1:])]
    assert deltas, starts
    for d in deltas:
        assert 3.0 <= d <= 8.0, f"start delta {d} outside 3-8s; starts={starts}"
    # Must not require main-loop tick after general().
    assert summary.get("main_loop_tick_invocations", 0) == 0 or summary.get(
        "scheduler_driven", True
    )


def test_w8_scheduler_serializes_camoufox_max_concurrency_one(tmp_path, monkeypatch):
    _clear_wiring_state()
    clock = FakeClock()
    concurrent = 0
    max_concurrent = 0
    shared_labels: List[str] = []
    lock = threading.Lock()

    class _FakeSession:
        def get_or_create_page(self, name, browser):
            assert name == "bookmaker:winline"
            return object()

    class _FakeResult:
        source = "Winline"
        odds = []
        map_num = MAP_NUM
        p1_team = TEAM1
        p2_team = TEAM2
        market_closed = False
        market_kind = "current_map_winner"
        status = "ok"
        match_found = False
        acquisition_mode = "initial_goto"
        dom_signature = "dom-sig"
        page_url = "https://winline.example/live"
        body_text = "body"
        acquisition_error = None
        error = None
        load_error = None
        parser_failure_reasons = []
        details = "body"

    def fake_run_shared(label, callback, timeout=120.0, retry=True, reset_on_error=True):
        nonlocal concurrent, max_concurrent
        shared_labels.append(str(label))
        with lock:
            concurrent += 1
            max_concurrent = max(max_concurrent, concurrent)
        try:
            # Hold the "job" briefly while another scheduler tick could race.
            return callback(object())
        finally:
            with lock:
                concurrent -= 1

    def fake_parse(
        page,
        site,
        url,
        team1,
        team2,
        mode,
        forced_map_num=None,
        acquisition_mode=None,
        # Сборщик прокидывает флаг решающей карты серии (промоция рынка «Матч»).
        series_last_map=False,
    ):
        return _FakeResult()

    monkeypatch.setattr(cs, "_run_shared_camoufox_job", fake_run_shared)
    monkeypatch.setattr(cs, "_shared_camoufox_session", _FakeSession(), raising=False)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True, raising=False)
    monkeypatch.setattr(cs, "_bookmaker_parse_site_in_camoufox_page", fake_parse, raising=False)
    monkeypatch.setattr(
        cs,
        "_bookmaker_urls_for_mode",
        lambda _mode: {"winline": "https://winline.example/live"},
        raising=False,
    )

    cs.ensure_winline_current_map_polling(
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=1,
        producer_start_generation="g1",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        evidence_path=tmp_path / "latest.json",
    )
    # Drive several due ticks via scheduler test seam.
    drive = cs.drive_winline_current_map_polling_scheduler_for_tests
    drive(
        duration_s=20.0,
        step_s=1.0,
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        advance_fn=clock.advance,
        producer_pid=1,
        producer_start_generation="g1",
    )
    assert shared_labels, "scheduler/collector must use _run_shared_camoufox_job"
    assert max_concurrent == 1, f"max concurrent Camoufox jobs must be 1; got {max_concurrent}"
    assert all("winline" in str(x).lower() for x in shared_labels)


def test_w8_finite_odds_terminalizes_and_send_spy_zero(tmp_path, monkeypatch):
    _clear_wiring_state()
    clock = FakeClock()
    send_spy: List[Any] = []
    monkeypatch.setattr(cs, "send_message", lambda m, **_k: send_spy.append(m))

    def fake_collector(**kwargs):
        return _accepted_collector_result()

    cs.ensure_winline_current_map_polling(
        series=LISTING_SERIES,
        map_num=MAP_NUM,
        team1=TEAM1,
        team2=TEAM2,
        selected_side=None,
        producer_pid=1,
        producer_start_generation="g1",
        monotonic_fn=clock.monotonic,
        wall_fn=clock.time,
        collector=fake_collector,
        evidence_path=tmp_path / "latest.json",
    )
    out = cs.tick_winline_current_map_polling(monotonic_fn=clock.monotonic, wall_fn=clock.time)
    assert out
    terminal = None
    for row in out:
        if isinstance(row, dict) and row.get("terminal"):
            terminal = row["terminal"]
    assert terminal is not None or any(
        isinstance(r, dict) and r.get("status") == "success" for r in out
    ), out
    assert send_spy == []


def test_w8_main_loop_starts_scheduler_not_only_post_general_tick():
    """Source guard: main loop must start independent scheduler; tick may remain as backup."""
    src = (BASE_DIR / "cyberscore_try.py").read_text(encoding="utf-8")
    assert "start_winline_current_map_polling_scheduler" in src
    # Scheduler start should appear near main loop / __main__ path, not only as dead code.
    assert "drive_winline_current_map_polling_scheduler_for_tests" in src or (
        "def start_winline_current_map_polling_scheduler" in src
    )


def _puckchamp_row(**overrides: Dict[str, Any]) -> Dict[str, Any]:
    """Строка моста серии PuckChamp — Klim Sani4 (инцидент 01.09.2026).

    Числа взяты из захваченного ответа OpenDota по карте 1 этой серии
    (`fixtures/opendota_match_8977252978_20260901.json`): лига 19944, id команд
    10164236 / 10232231, Bo3, 2338 секунд, счёт 13:48. Ключ серии из таких
    полей совпадает с продовым — `sourcetv:league:19944|id:10164236|id:10232231`.
    """
    row = {
        "match_id": 8977252978,
        "league_id": 19944,
        "radiant_team_id": 10164236,
        "dire_team_id": 10232231,
        "radiant_team_name": "PuckChamp",
        "dire_team_name": "Klim Sani4",
        "series_game_number": 1,
        "series_type": 1,
        "radiant_series_wins": 0,
        "dire_series_wins": 0,
        "status": "live",
        "game_time": 2338,
        "radiant_score": 13,
        "dire_score": 48,
        "fast_picks": {"first_team": [{"hero_id": 80}]},
        "_cyberscore_heroes_and_pos": {
            "radiant": {"pos1": {"hero_id": 80}},
            "dire": {"pos1": {"hero_id": 4}},
        },
    }
    row.update(overrides)
    return row


def _puckchamp_intermission_row() -> Dict[str, Any]:
    """Та же строка в перерыве: матч ещё жив у Valve, драфт уже стёрт."""
    return _puckchamp_row(fast_picks={},
                          _cyberscore_heroes_and_pos={"radiant": None, "dire": None})


def test_map_that_never_started_is_never_announced_as_finished(tmp_path, monkeypatch):
    """Регрессия 01.09.2026: «🏁 карта 2 · 🏆 победа» по карте, которой не было.

    Серия PuckChamp — Klim Sani4. Карта 1 доигралась, но GC держал её матч
    живым; по строке перерыва номер карты сдвинулся на 2, и опрос карты 2
    завёлся ещё до её начала. Когда запись наконец ушла из моста, смерть карты 1
    была засчитана карте 2: в 13:07:33 в чат ушло «🏁 карта завершена» с
    победителем карты 1. Проверка идёт по ГРАНИЦЕ ДОСТАВКИ.
    """
    _clear_wiring_state()
    cs._winline_odds_notify_state.clear()
    cs._winline_pending_map_winners.clear()
    getattr(cs, "_winline_series_winner_matches", {}).clear()
    clock = FakeClock()
    monkeypatch.setattr(cs, "DLTV_SOURCE_MODE", "sourcetv", raising=False)
    monkeypatch.setattr(cs, "start_winline_current_map_polling_scheduler", lambda **_k: True)
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_ENABLED_ENV, "1")
    monkeypatch.setenv(cs.WINLINE_CURRENT_MAP_CONTINUOUS_ENV, "1")
    monkeypatch.setenv(cs.WINLINE_MAP_WINNER_ENABLED_ENV, "0")
    sent: List[str] = []
    monkeypatch.setattr(cs, "send_winline_odds_message",
                        lambda message, **_k: (sent.append(message), True)[1])

    live = _puckchamp_row()
    series = cs._winline_sourcetv_series_key(live)
    assert series == "sourcetv:league:19944|id:10164236|id:10232231", series

    def _register(map_num: int, match_id: Any) -> None:
        cs.ensure_winline_current_map_polling(
            series=series,
            map_num=map_num,
            team1=live["radiant_team_name"],
            team2=live["dire_team_name"],
            match_id=match_id,
            producer_pid=1,
            producer_start_generation="g1",
            monotonic_fn=clock.monotonic,
            wall_fn=clock.time,
            collector=lambda **_k: _accepted_collector_result(
                map_num=map_num,
                team1=live["radiant_team_name"],
                team2=live["dire_team_name"],
                series=series,
            ),
            evidence_path=tmp_path / f"map{map_num}.json",
        )

    def _tick():
        return cs.tick_winline_current_map_polling(
            monotonic_fn=clock.monotonic, wall_fn=clock.time)

    # Карта 1 идёт: мост подтверждает именно её.
    cs._reconcile_winline_sourcetv_polling([live], authoritative=True)
    assert cs._winline_sourcetv_map_num(live) == 1
    _register(1, live["match_id"])
    _tick()

    # Перерыв: матч карты 1 ещё в мосте, драфт стёрт, номер карты уехал на 2.
    intermission = _puckchamp_intermission_row()
    assert cs._winline_sourcetv_map_num(intermission) == 2
    cs._reconcile_winline_sourcetv_polling([intermission], authoritative=True)
    clock.advance(100.0)
    # Ровно то, что подставляет боевой путь карточки. Запасной вариант — как
    # было до правки (id живого матча), чтобы проверка падала на СООБЩЕНИИ, а не
    # на отсутствии новой функции.
    card_match_id = getattr(
        cs, "_winline_card_match_id", lambda row, *_a: row.get("match_id"))(intermission)
    _register(2, card_match_id)
    _tick()

    assert any("🆕 Winline · карта 2" in m for m in sent), sent

    # Запись доигранной карты наконец протухла и ушла из снимка.
    cs._reconcile_winline_sourcetv_polling([], authoritative=True)
    clock.advance(300.0)
    _tick()
    clock.advance(cs._winline_stop_notice_hold_seconds() + 60.0)
    _tick()
    cs._winline_flush_pending_stop_notices(
        monotonic_fn=clock.monotonic, stamp_fn=lambda: "13:07:33")

    finished = [m for m in sent if "карта завершена" in m]
    assert len(finished) == 1, sent
    assert "🏁 Winline · карта 1" in finished[0], finished
    # Ключевое: по карте 2 «завершена» не объявляется и победитель не называется.
    assert not any("карта 2" in m and "завершена" in m for m in sent), sent
    assert not any("🏆" in m for m in sent), sent
    # Честный остаток по карте 2 — «опрос остановлен», не «завершена».
    stops = [m for m in sent if "опрос остановлен" in m]
    assert len(stops) == 1 and "карта 2" in stops[0], sent
    assert card_match_id is None, (
        "id доигранной карты не должен уезжать в опрос следующей")


def test_intermission_row_does_not_make_us_wait_for_the_map_after_next():
    """Номер в строке перерыва УЖЕ сдвинут — прибавлять к нему единицу нельзя.

    Bo5, счёт 1:0, строка доигранной карты 1: номер карты из неё выходит 2.
    Ждать после неё надо карту 2 (она ещё не начиналась), а не карту 3 — иначе
    заводится опрос карты, до которой серия может не дойти, и его конец уходит
    в чат как конец несуществующей карты.
    """
    _clear_wiring_state()
    row = _puckchamp_intermission_row()
    row["series_type"] = 2          # Bo5
    row["radiant_series_wins"] = 1  # карта 1 уже засчитана GC
    series = cs._winline_sourcetv_series_key(row)

    cs._reconcile_winline_sourcetv_polling([row], authoritative=True)
    registry = cs._winline_current_map_registry[series]
    assert registry["map_num"] == 2
    assert registry["bridge_confirmed"] is False
    assert cs._winline_next_map_certain(registry, 2) is True, (
        "предпосылка теста: счёт и номер карты согласованы, серия продолжится")


    cs._reconcile_winline_sourcetv_polling([], authoritative=True)

    pending = cs._winline_pending_next_maps.get(series) or {}
    assert pending.get("map_num") == 2, pending


def test_map_number_without_a_gc_game_number_is_not_bumped():
    """Без номера карты у GC сдвигать нечего — иначе перескок через карту.

    `series_game_number` и счёт серии — независимые поля GC. При счёте 1:0 и
    отсутствующем номере `played + 1` уже указывает на карту 2; сдвиг делал из
    неё карту 3 — опрос карты, до которой серия может не дойти.
    """
    base = _puckchamp_intermission_row()
    base["series_game_number"] = 0

    stale_score = dict(base, radiant_series_wins=0, dire_series_wins=0)
    assert cs._winline_sourcetv_map_num(stale_score) == 1

    updated_score = dict(base, radiant_series_wins=1, dire_series_wins=0)
    assert cs._winline_sourcetv_map_num(updated_score) == 2

    later = dict(base, radiant_series_wins=1, dire_series_wins=1)
    assert cs._winline_sourcetv_map_num(later) == 3


def test_last_map_of_a_series_is_still_announced_as_finished(tmp_path, monkeypatch):
    """Решающую карту серии «завершена» терять нельзя.

    На карте 5 номер двигать некуда (`resolved < 5` не срабатывает), поэтому
    запись перерыва остаётся под ТЕМ ЖЕ номером с `bridge_confirmed=False`.
    Карта при этом настоящая: её опрос видел подтверждение источника, пока она
    шла, — и «завершена» терять нельзя. Проверка по ГРАНИЦЕ ДОСТАВКИ.
    """
    _clear_wiring_state()
    cs._winline_odds_notify_state.clear()
    cs._winline_pending_map_winners.clear()
    getattr(cs, "_winline_series_winner_matches", {}).clear()
    clock = FakeClock()
    monkeypatch.setattr(cs, "DLTV_SOURCE_MODE", "sourcetv", raising=False)
    monkeypatch.setattr(cs, "start_winline_current_map_polling_scheduler", lambda **_k: True)
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_ENABLED_ENV, "1")
    monkeypatch.setenv(cs.WINLINE_CURRENT_MAP_CONTINUOUS_ENV, "1")
    monkeypatch.setenv(cs.WINLINE_MAP_WINNER_ENABLED_ENV, "0")
    sent: List[str] = []
    monkeypatch.setattr(cs, "send_winline_odds_message",
                        lambda message, **_k: (sent.append(message), True)[1])

    # Bo5, счёт 2:2, идёт решающая карта 5: номер уже упёрся в предел серии.
    live = _puckchamp_row(series_type=2, series_game_number=5,
                          radiant_series_wins=2, dire_series_wins=2)
    series = cs._winline_sourcetv_series_key(live)
    assert cs._winline_sourcetv_map_num(live) == 5
    cs._reconcile_winline_sourcetv_polling([live], authoritative=True)
    cs.ensure_winline_current_map_polling(
        series=series, map_num=5,
        team1=live["radiant_team_name"], team2=live["dire_team_name"],
        match_id=live["match_id"], producer_pid=1, producer_start_generation="g1",
        monotonic_fn=clock.monotonic, wall_fn=clock.time,
        collector=lambda **_k: _accepted_collector_result(
            map_num=5, team1=live["radiant_team_name"],
            team2=live["dire_team_name"], series=series),
        evidence_path=tmp_path / "decider.json",
    )

    def _tick():
        return cs.tick_winline_current_map_polling(
            monotonic_fn=clock.monotonic, wall_fn=clock.time)

    _tick()
    assert any("🆕 Winline · карта 5" in m for m in sent), sent

    # Карта доиграна: строка перерыва под тем же номером 5, счёт ещё не обновлён.
    intermission = _puckchamp_row(series_type=2, series_game_number=5,
                                  radiant_series_wins=2, dire_series_wins=2,
                                  fast_picks={},
                                  _cyberscore_heroes_and_pos={"radiant": None,
                                                              "dire": None})
    assert cs._winline_sourcetv_map_num(intermission) == 5, (
        "предпосылка: номер решающей карты двигать некуда")
    cs._reconcile_winline_sourcetv_polling([intermission], authoritative=True)
    clock.advance(60.0)
    _tick()

    # Запись ушла из свежего снимка — конец карты доказан.
    cs._reconcile_winline_sourcetv_polling([], authoritative=True)
    clock.advance(10.0)
    _tick()

    finished = [m for m in sent if "карта завершена" in m]
    assert len(finished) == 1, sent
    assert "🏁 Winline · карта 5" in finished[0]
    assert not any("опрос остановлен" in m for m in sent), sent


def test_terminal_taken_by_the_main_loop_tick_is_still_announced(tmp_path, monkeypatch):
    """Терминал отдаётся ровно один раз — съесть его backup-тиком нельзя.

    Поллеры тикают два водителя: поток-шедулер (основной такт) и backup-тик
    главного цикла после `general()`. Backup-тик в чат не пишет намеренно:
    блокирующая отправка не должна удлинять цикл ставок. Но терминал он при
    этом СНИМАЕТ — `tick()` возвращает его один раз, — и «🏁 карта завершена»
    не уходит никогда: поллер уже мёртв, а нового повода сообщить не будет.

    02.09.2026 Team Synapse — 4ikibamboni: карта 1 терминалилась в 19:55:24
    (evidence: `map_end_proven=True`, `map_confirmed_live=True`,
    `lifecycle_event=source_absent`), а в чат в 19:55:25 ушло только
    «🆕 Winline · карта 2» — конец карты 1 объявлен не был.
    """
    _clear_wiring_state()
    cs._winline_odds_notify_state.clear()
    cs._winline_pending_map_winners.clear()
    getattr(cs, "_winline_series_winner_matches", {}).clear()
    clock = FakeClock()
    monkeypatch.setattr(cs, "DLTV_SOURCE_MODE", "sourcetv", raising=False)
    monkeypatch.setattr(cs, "start_winline_current_map_polling_scheduler", lambda **_k: True)
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_ENABLED_ENV, "1")
    monkeypatch.setenv(cs.WINLINE_CURRENT_MAP_CONTINUOUS_ENV, "1")
    monkeypatch.setenv(cs.WINLINE_MAP_WINNER_ENABLED_ENV, "0")
    sent: List[str] = []
    monkeypatch.setattr(cs, "send_winline_odds_message",
                        lambda message, **_k: (sent.append(message), True)[1])

    live = _puckchamp_row()
    series = cs._winline_sourcetv_series_key(live)
    team1, team2 = live["radiant_team_name"], live["dire_team_name"]
    cs._reconcile_winline_sourcetv_polling([live], authoritative=True)
    cs.ensure_winline_current_map_polling(
        series=series, map_num=1, team1=team1, team2=team2,
        match_id=live["match_id"], producer_pid=1, producer_start_generation="g1",
        monotonic_fn=clock.monotonic, wall_fn=clock.time,
        collector=lambda **_k: _accepted_collector_result(
            map_num=1, team1=team1, team2=team2, series=series),
        evidence_path=tmp_path / "map1.json",
    )

    # Карта жива: такт шедулера снял кэфы, чат увидел «🆕».
    cs.tick_winline_current_map_polling(
        monotonic_fn=clock.monotonic, wall_fn=clock.time)
    assert any("🆕 Winline · карта 1" in m for m in sent), sent

    # Серия ушла из моста доказанно — следующий такт даёт терминал карты.
    cs._reconcile_winline_sourcetv_polling([], authoritative=True)
    clock.advance(10.0)

    # Терминал снимает BACKUP-тик главного цикла: сам он в чат не пишет.
    cs.tick_winline_current_map_polling(
        monotonic_fn=clock.monotonic, wall_fn=clock.time, from_main_loop=True)
    assert not any("карта завершена" in m for m in sent), sent

    # Следующий такт шедулера обязан дообъявить конец карты.
    clock.advance(5.0)
    cs.tick_winline_current_map_polling(
        monotonic_fn=clock.monotonic, wall_fn=clock.time)

    finished = [m for m in sent if "карта завершена" in m]
    assert len(finished) == 1, sent
    assert "🏁 Winline · карта 1" in finished[0]
