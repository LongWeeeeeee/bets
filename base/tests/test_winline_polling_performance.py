"""Regressions for DOM reuse, browser ownership, scheduling and evidence retention."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bookmaker_selenium_odds as bk
import cyberscore_try as cs
from test_winline_fast_path import CARD_HTML, TEAM1, TEAM2, URL

_start_scheduler = cs.start_winline_current_map_polling_scheduler


@pytest.fixture(autouse=True)
def isolated_polling(monkeypatch):
    cs.reset_winline_current_map_polling_state()
    monkeypatch.setattr(cs, "start_winline_current_map_polling_scheduler", lambda **kw: True)
    monkeypatch.setattr(cs, "_bookmaker_restore_shared_camoufox_direct_route", lambda **kw: False)
    yield
    cs.reset_winline_current_map_polling_state()


def collect(payload, team1=TEAM1, team2=TEAM2):
    return cs._winline_fast_collect_from_payload(
        payload, series="performance-fixture", map_num=2,
        team1=team1, team2=team2, expected_url=URL,
    )


def test_one_soup_per_payload_preserves_orientation_and_invalidates_on_html_change(monkeypatch):
    calls = []
    original = bk.BeautifulSoup

    def parse(*args, **kwargs):
        calls.append(args[0])
        return original(*args, **kwargs)

    monkeypatch.setattr(bk, "BeautifulSoup", parse)
    payload = {"html": CARD_HTML, "url": URL}
    direct = collect(payload)
    reverse = collect(payload, TEAM2, TEAM1)
    assert (direct["p1_odds"], direct["p2_odds"]) == (3.8, 1.2)
    assert (reverse["p1_odds"], reverse["p2_odds"]) == (1.2, 3.8)
    assert len(calls) == 1
    payload["html"] = CARD_HTML.replace("3.80", "3.90")
    assert collect(payload)["p1_odds"] == 3.9
    assert len(calls) == 2


@pytest.mark.parametrize("path", sorted((Path(__file__).parent / "fixtures").glob("winline*.html")))
def test_shared_parse_matches_independent_parse_for_captured_cards(path):
    html = path.read_text()
    snapshot = bk._WinlineDOMSnapshot(html)
    for t1, t2 in [(TEAM1, TEAM2), ("MOUZ", "Dawn Bulls"), ("Team Falcons", "Vici Gaming")]:
        for map_num in (2, 3):
            args = ("", t1, t2)
            card = bk._winline_matched_card_context(*args, html=html, map_num=map_num)
            assert bk._winline_matched_card_context(*args, html=html, map_num=map_num, _snapshot=snapshot) == card
            plain = bk._extract_winline_current_map_winner(card or "", t1, t2, map_num, html=html)
            shared = bk._extract_winline_current_map_winner(card or "", t1, t2, map_num, html=html, _snapshot=snapshot)
            assert vars(shared) == vars(plain)


def test_fast_parse_runs_outside_shared_worker_and_pinned_refresh_invalidates_snapshot(monkeypatch):
    caller = threading.get_ident()
    worker_ids, parser_ids = [], []
    state = {"clicked": False}
    before = CARD_HTML.replace("2 карта", "3 карта")
    # Full-page markers make absent-map evidence authoritative.
    padding = '<header>Киберспорт Winline</header>' + '<!--' + 'x' * 70000 + '-->'

    class Page:
        def evaluate(self, *args):
            worker_ids.append(threading.get_ident())
            return {"html": (CARD_HTML if state["clicked"] else before) + padding, "url": URL}

    page = Page()
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True)
    monkeypatch.setattr(cs, "_bookmaker_urls_for_mode", lambda mode: {"winline": URL})
    monkeypatch.setattr(cs, "_shared_camoufox_session", SimpleNamespace(get_or_create_page=lambda *a: page))
    monkeypatch.setattr(cs, "_bookmaker_parse_site_in_camoufox_page", lambda *a, **kw: pytest.fail("unexpected full fallback"))
    batch = {"payload": None}
    monkeypatch.setattr(cs, "_winline_current_map_batch_context", batch)
    original = cs._winline_fast_collect_from_payload

    def parse(*args, **kwargs):
        parser_ids.append(threading.get_ident())
        return original(*args, **kwargs)

    def select(*args, **kwargs):
        worker_ids.append(threading.get_ident())
        state["clicked"] = True
        return True

    monkeypatch.setattr(cs, "_winline_fast_collect_from_payload", parse)
    monkeypatch.setattr(cs, "_winline_select_matching_pinned_card", select)
    with ThreadPoolExecutor(max_workers=1) as worker:
        monkeypatch.setattr(cs, "_run_shared_camoufox_job", lambda label, callback, **kw: worker.submit(callback, object()).result())
        out = cs._winline_current_map_poller_collect(acquisition_mode="dynamic_dom", series="fixture", map_num=2, team1=TEAM1, team2=TEAM2)
    assert out["p1_odds"] == 3.8
    assert parser_ids == [caller, caller]
    assert worker_ids and all(tid != caller for tid in worker_ids)
    assert "2 карта" in batch["payload"]["_parsed_dom"].html


@pytest.mark.parametrize("status", ["not_due", "in_flight"])
def test_transient_tick_preserves_last_attempt_files(tmp_path, monkeypatch, status):
    path, latest = tmp_path / "map.json", tmp_path / "latest.json"
    for target in (path, latest):
        target.write_text('{"p1_odds":1.85,"p2_odds":2.05}')
    before = [(p.read_bytes(), p.stat().st_mtime_ns) for p in (path, latest)]
    poller = SimpleNamespace(tick=lambda **kw: {"status": status}, is_active=lambda: True, _evidence_path=path)
    monkeypatch.setattr(cs, "_winline_current_map_pollers", {"fixture": poller})
    monkeypatch.setattr(cs, "WINLINE_CURRENT_MAP_POLLING_EVIDENCE_PATH", latest)
    cs.tick_winline_current_map_polling(from_main_loop=True)
    assert [(p.read_bytes(), p.stat().st_mtime_ns) for p in (path, latest)] == before


def test_scheduler_uses_remaining_deadline_and_avoids_spin_during_another_tick(monkeypatch):
    clock = [100.0]
    poller = SimpleNamespace(is_active=lambda: True, _mono=lambda: clock[0], _next_poll_mono=100.3)
    monkeypatch.setattr(cs, "_winline_current_map_pollers", {"fixture": poller})
    monkeypatch.setitem(cs._winline_current_map_scheduler_meta, "interval_s", 1.0)
    assert cs._winline_scheduler_delay() == pytest.approx(0.3)
    clock[0] = 101.0
    assert cs._winline_scheduler_delay() == 0
    with cs._winline_current_map_tick_lock:
        assert cs._winline_scheduler_delay() == 1.0


def test_latest_evidence_written_once_per_sweep_and_errors_remain_visible(tmp_path, monkeypatch):
    writes = []
    latest = tmp_path / "latest.json"
    monkeypatch.setattr(cs, "WINLINE_CURRENT_MAP_POLLING_EVIDENCE_PATH", latest)
    monkeypatch.setattr(cs, "_winline_write_current_map_evidence", lambda payload, *, path: writes.append((path, payload)))

    def fail(**kw):
        raise RuntimeError("collector unavailable")

    pollers = {
        "first": SimpleNamespace(tick=lambda **kw: {"attempt": {"p1_odds": 1.85}}, is_active=lambda: True, _evidence_path=tmp_path / "first.json"),
        "second": SimpleNamespace(tick=fail, is_active=lambda: True, _evidence_path=tmp_path / "second.json"),
    }
    monkeypatch.setattr(cs, "_winline_current_map_pollers", pollers)
    cs.tick_winline_current_map_polling(from_main_loop=True)
    assert [path for path, _ in writes] == [tmp_path / "first.json", tmp_path / "second.json", latest]
    assert writes[-1][1]["status"] == "error"
    assert "collector unavailable" in writes[-1][1]["error"]


def test_new_and_accelerated_poller_wake_scheduler(tmp_path):
    clock = [100.0]
    cs._winline_current_map_scheduler_wake.clear()
    poller = cs.ensure_winline_current_map_polling(
        series="performance-fixture", map_num=2, team1=TEAM1, team2=TEAM2,
        monotonic_fn=lambda: clock[0], wall_fn=lambda: 1700000000 + clock[0],
        collector=lambda **kw: {}, evidence_path=tmp_path / "map.json",
    )
    assert cs._winline_current_map_scheduler_wake.is_set()
    cs._winline_current_map_scheduler_wake.clear()
    poller._next_poll_mono = 160.0
    assert cs.accelerate_winline_current_map_polling("performance-fixture")
    assert poller._next_poll_mono == 100.0 + poller._accelerated_interval
    assert cs._winline_current_map_scheduler_wake.is_set()


def test_scheduler_does_not_add_a_second_after_collector_work(monkeypatch):
    clock, starts, sleeps = [100.0], [], []
    poller = SimpleNamespace(is_active=lambda: True, _mono=lambda: clock[0], _next_poll_mono=100.0)
    monkeypatch.setattr(cs, "_winline_current_map_pollers", {"fixture": poller})
    monkeypatch.setitem(cs._winline_current_map_scheduler_meta, "interval_s", 1.0)

    def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += seconds

    def tick(**kw):
        if clock[0] >= poller._next_poll_mono:
            starts.append(clock[0])
            poller._next_poll_mono = clock[0] + 3.5
            clock[0] += 0.8
        if len(starts) == 2:
            cs._winline_current_map_scheduler_stop.set()

    monkeypatch.setattr(cs, "_winline_scheduler_sleep", sleep)
    monkeypatch.setattr(cs, "tick_winline_current_map_polling", tick)
    cs._winline_current_map_scheduler_stop.clear()
    cs._winline_current_map_scheduler_loop()
    assert starts == pytest.approx([100.0, 103.5])
    assert sleeps == pytest.approx([0.0, 1.0, 1.0, 0.7])


def test_live_scheduler_interval_update_wakes_existing_wait(monkeypatch):
    thread = SimpleNamespace(is_alive=lambda: True, join=lambda **kw: None)
    monkeypatch.setattr(cs, "_winline_current_map_scheduler_thread", thread)
    cs._winline_current_map_scheduler_wake.clear()
    assert _start_scheduler(interval_s=0.2)
    assert cs._winline_current_map_scheduler_meta["interval_s"] == 0.2
    assert cs._winline_current_map_scheduler_wake.is_set()


def test_overview_defers_html_fallback_and_skips_soup_when_body_text_exists(monkeypatch):
    calls = []
    original = bk.BeautifulSoup
    monkeypatch.setattr(bk, "BeautifulSoup", lambda *a, **kw: (calls.append(a[0]), original(*a, **kw))[1])
    raw = {"text": "BODY", "html": "<b>HTML</b>", "status": "ok", "_deferred_visible_text": False}
    assert bk._winline_finalize_overview(raw)["text"] == "BODY"
    assert calls == []
    raw.update(text="", _deferred_visible_text=True)
    assert bk._winline_finalize_overview(raw)["text"] == "HTML"
    assert calls == ["<b>HTML</b>"]
