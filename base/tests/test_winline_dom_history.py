"""Historical replay and bounded-writer regressions for missing odds evidence."""
import asyncio
import gzip
import hashlib
import json
from pathlib import Path
import sys
import threading
import time

import pytest

BASE = Path(__file__).resolve().parents[1]
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

import bookmaker_selenium_odds as bk
import cyberscore_try as cs
import winline_dom_history as history


URL = "https://winline.ru/stavki/sport/kibersport/dota_2"
KEY = "series|map1|Execration|Team Bored"
HTML = """<ww-feature-block-event-dsk>
<div class="card card--live"><div class="card__competitors">
<div class="name">Execration</div><div class="name">Team Bored</div>
<span>09:42</span></div><div class="card__body">
<div class="match-row-label">1 карта</div><div class="card__coeffs">
<ww-feature-event-market-dsk>
<div class="coefficient-button coefficient-button_generic2"><span>2.15</span></div>
<div class="coefficient-button coefficient-button_generic2"><span>1.62</span></div>
</ww-feature-event-market-dsk></div></div></div>
</ww-feature-block-event-dsk>"""


def _inputs(html=HTML):
    return dict(html=html, body_text="", visible_text="", url=URL,
                captured_wall=1789279655.0, page_instance_id=77,
                parser_path="fast_html")


def _attempt(index=172, status="open"):
    return dict(canonical_key=KEY, attempt_index=index, market_status=status,
                attempt_finished_at=1789279655.0 + (index - 172) * 3.5,
                producer_pid=123, producer_start_generation="test-123")


def _result(status="open"):
    return dict(market_status=status, p1_odds=2.15, p2_odds=1.62,
                odds_bettable=status == "open", source="winline_current_map_winner",
                card_odds=[2.15, 1.62])


def _records(root):
    index = [json.loads(s) for s in (root / "index.jsonl").read_text().splitlines()]
    records = []
    for row in index:
        packed = (root / row["file"]).read_bytes()
        assert hashlib.sha256(packed).hexdigest() == row["sha256"]
        record = json.loads(gzip.decompress(packed))
        assert hashlib.sha256(record["inputs"]["html"].encode()).hexdigest() == row["html_sha256"]
        records.append(record)
    return records


def test_stable_prices_are_sampled_and_five_second_closure_is_preserved(tmp_path, monkeypatch):
    clock = [100.0]
    # Replace the module binding, not the process-wide time.monotonic function.
    monkeypatch.setattr(history, "time", type("Clock", (), {
        "monotonic": staticmethod(lambda: clock[0]),
        "time": staticmethod(lambda: 1789279655.0 + clock[0]),
    }))
    writer = history.WinlineDOMHistory(tmp_path, interval_s=15)
    try:
        assert writer.submit(_attempt(), _inputs(), _result())
        clock[0] += 5
        assert writer.submit(_attempt(173), _inputs(HTML.replace("09:42", "09:47")), _result()) is None
        clock[0] += 10
        assert writer.submit(_attempt(176), _inputs(HTML.replace("09:42", "09:57")), _result())
        clock[0] += 3.5
        assert writer.submit(_attempt(177, "closed"), _inputs(), _result("closed"))
        clock[0] += 5
        result = _result()
        assert writer.submit(_attempt(178), _inputs(), result)
        result["card_odds"][0] = 99  # queue owns its metadata
    finally:
        assert writer.close(timeout=3)
    records = _records(tmp_path)
    assert [r["attempt"]["attempt_index"] for r in records] == [172, 176, 177, 178]
    assert "09:57" in records[1]["inputs"]["html"]
    assert records[-1]["result"]["card_odds"] == [2.15, 1.62]
    replay = bk._extract_winline_current_map_winner(
        "", "Execration", "Team Bored", 1, html=records[1]["inputs"]["html"],
    )
    assert replay.odds == [2.15, 1.62]


def test_queue_overflow_does_not_wait_for_writer(tmp_path, monkeypatch):
    writer = history.WinlineDOMHistory(tmp_path, interval_s=0, queue_size=1)
    entered, release = threading.Event(), threading.Event()
    original = writer._write_record

    def blocked(record):
        entered.set()
        assert release.wait(3)
        original(record)

    monkeypatch.setattr(writer, "_write_record", blocked)
    try:
        assert writer.submit(_attempt(), _inputs(), _result())
        assert entered.wait(2)
        assert writer.submit(_attempt(173), _inputs(), _result())
        before = time.monotonic()
        assert writer.submit(_attempt(174), _inputs(), _result()) is None
        assert time.monotonic() - before < 0.25
        assert writer.status()["dropped"] == 1
    finally:
        release.set()
        assert writer.close(timeout=3)
    assert len(_records(tmp_path)) == 2


def test_budget_counts_existing_evidence_and_never_deletes_it(tmp_path):
    old = tmp_path / "older.json.gz"
    old.write_bytes(b"existing evidence" * 5000)
    before = old.read_bytes()
    writer = history.WinlineDOMHistory(tmp_path, max_bytes=80000)
    assert writer.submit(_attempt(), _inputs(), _result())
    assert writer.close(timeout=3)
    status = json.loads((tmp_path / "status.json").read_text())
    assert status["budget_exhausted"] and status["disabled"]
    assert status["written"] == 0
    assert old.read_bytes() == before
    assert list(tmp_path.glob("*.json.gz")) == [old]


def test_write_error_is_visible_and_next_attempt_can_be_written(tmp_path, monkeypatch):
    writer = history.WinlineDOMHistory(tmp_path, interval_s=15)
    original = writer._write_record
    calls = []

    def fail_once(record):
        calls.append(record)
        if len(calls) == 1:
            raise OSError("simulated unavailable disk")
        original(record)

    monkeypatch.setattr(writer, "_write_record", fail_once)
    assert writer.submit(_attempt(), _inputs(), _result())
    writer._queue.join()  # fail + invalidate sample before the next equal result
    assert writer.submit(_attempt(173), _inputs(), _result())
    assert writer.close(timeout=3)
    assert writer.status()["errors"] == 1
    assert [r["attempt"]["attempt_index"] for r in _records(tmp_path)] == [173]


def test_close_reports_timeout_then_drains_already_queued_records(tmp_path, monkeypatch):
    writer = history.WinlineDOMHistory(tmp_path, interval_s=0)
    entered, release = threading.Event(), threading.Event()
    original = writer._write_record

    def blocked(record):
        entered.set()
        assert release.wait(3)
        original(record)

    monkeypatch.setattr(writer, "_write_record", blocked)
    try:
        assert writer.submit(_attempt(), _inputs(), _result())
        assert entered.wait(2)
        assert writer.submit(_attempt(173), _inputs(), _result())
        assert writer.close(timeout=0) is False
        assert writer.submit(_attempt(174), _inputs(), _result()) is None
    finally:
        release.set()
        assert writer.close(timeout=3)
    assert len(_records(tmp_path)) == 2


def test_archive_lock_prevents_a_second_writer(tmp_path):
    first = history.WinlineDOMHistory(tmp_path)
    second = history.WinlineDOMHistory(tmp_path)
    try:
        assert first.submit(_attempt(), _inputs(), _result())
        first._queue.join()
        assert second.submit(_attempt(173), _inputs(), _result())
        assert second.close(timeout=3)
        assert second.status()["disabled"]
        assert second.status()["errors"] == 1
    finally:
        assert first.close(timeout=3)
    assert len(_records(tmp_path)) == 1


def test_browser_failure_records_an_explicit_absent_dom(tmp_path, monkeypatch):
    monkeypatch.setenv("WINLINE_DOM_HISTORY_ENABLED", "1")
    writer = history.WinlineDOMHistory(tmp_path)
    monkeypatch.setattr(history, "_recorder", writer)
    assert history.record_attempt(_attempt(status="error"), {"market_status": "error"})
    assert writer.close(timeout=3)
    record = _records(tmp_path)[0]
    assert record["inputs"]["parser_path"] == "unavailable"
    assert record["inputs"]["captured_wall"] is None


def test_disabled_observer_creates_no_writer_or_files(tmp_path, monkeypatch):
    monkeypatch.setenv("WINLINE_DOM_HISTORY_ENABLED", "0")
    monkeypatch.setenv("WINLINE_DOM_HISTORY_DIR", str(tmp_path))
    monkeypatch.setattr(history, "_recorder", None)
    result = dict(_result(), _dom_history_payload=_inputs())
    assert history.record_attempt(_attempt(), result) is None
    assert "_dom_history_payload" not in result
    assert history._recorder is None
    assert list(tmp_path.iterdir()) == []


def test_poller_observer_binds_final_attempt_without_retaining_raw_html(tmp_path, monkeypatch):
    monkeypatch.setenv("WINLINE_DOM_HISTORY_ENABLED", "1")
    writer = history.WinlineDOMHistory(tmp_path)
    monkeypatch.setattr(history, "_recorder", writer)
    mod = cs._load_winline_current_map_poller_module()
    result = dict(_result(), page_valid=True, map_num=1,
                  _dom_history_payload=_inputs())
    poller = mod.WinlineCurrentMapOddsPoller(
        collector=lambda **kw: result, is_map_current=lambda **kw: True,
        attempt_observer=history.record_attempt, continuous=True,
    )
    assert poller.begin(series="series", map_num=1, team1="Execration", team2="Team Bored")
    out = poller.tick()
    assert writer.close(timeout=3)
    saved = _records(tmp_path)[0]
    assert saved["attempt"]["attempt_index"] == out["attempt"]["attempt_index"]
    assert saved["capture_id"] == out["attempt"]["dom_history_queued_id"]
    assert out["attempt"]["accepted"]
    assert HTML not in json.dumps(poller._attempts)
    assert "_dom_history_payload" not in saved["result"]


def test_observer_failure_does_not_change_quote_acceptance():
    def broken(*args):
        raise OSError("archive unavailable")

    mod = cs._load_winline_current_map_poller_module()
    poller = mod.WinlineCurrentMapOddsPoller(
        collector=lambda **kw: dict(_result(), page_valid=True, map_num=1),
        is_map_current=lambda **kw: True, attempt_observer=broken, continuous=True,
    )
    poller.begin(series="series", map_num=1, team1="Execration", team2="Team Bored")
    out = poller.tick()
    assert out["attempt"]["accepted"]
    assert out["attempt"]["dom_history_error"] == "OSError"


def test_fast_and_shared_batch_attach_the_actual_input_without_another_read(monkeypatch):
    monkeypatch.setenv("WINLINE_DOM_HISTORY_ENABLED", "1")
    monkeypatch.setattr(cs, "_winline_current_map_poller_collect_impl", None)
    monkeypatch.setattr(cs, "BOOKMAKER_CAMOUFOX_IMPORTED", True)
    monkeypatch.setattr(cs, "_bookmaker_urls_for_mode", lambda mode: {"winline": URL})
    monkeypatch.setattr(cs, "_winline_registry_series_last_map", lambda series: False)
    monkeypatch.setattr(cs, "_bookmaker_restore_shared_camoufox_direct_route", lambda **kw: None)
    calls = []

    class Page:
        def evaluate(self, *args):
            calls.append("DOM")
            return dict(html=HTML, url=URL)

    page = Page()
    monkeypatch.setattr(cs, "_shared_camoufox_session", type("Session", (), {
        "get_or_create_page": lambda self, *a: page,
    })())
    monkeypatch.setattr(cs, "_run_shared_camoufox_job", lambda label, callback, **kw: callback(object()))
    monkeypatch.setattr(cs, "_winline_current_map_batch_context", {"payload": None})
    first = cs._winline_current_map_poller_collect(
        acquisition_mode="dynamic_dom", series="s1", map_num=1,
        team1="Execration", team2="Team Bored",
    )
    second = cs._winline_current_map_poller_collect(
        acquisition_mode="dynamic_dom", series="s2", map_num=1,
        team1="Execration", team2="Team Bored",
    )
    assert calls == ["DOM"]
    assert first["p1_odds"] == second["p1_odds"] == 2.15
    assert first["_dom_history_payload"] == second["_dom_history_payload"]
    assert first["_dom_history_payload"]["html"] == HTML
    assert first["_dom_history_payload"]["page_instance_id"] == id(page)
    assert second["acquisition_mode_echo"] == "shared_dom_batch"


def test_full_parser_captures_exact_inputs_without_a_second_page_read(monkeypatch):
    text = "Execration Team Bored 1 карта 2.15 1.62"

    async def load(*args, **kwargs):
        return "ok", "", HTML, text, text, {}

    monkeypatch.setattr(bk, "_load_site_render_payload_camoufox_async", load)
    monkeypatch.setattr(bk, "WINLINE_OPEN_REQUESTED_MATCH", False)
    page = type("Page", (), {"url": URL})()  # no content/evaluate capability
    result = asyncio.run(bk.parse_site_in_camoufox_page_async(
        page, "winline", URL, "Execration", "Team Bored", "odds",
        forced_map_num=1, acquisition_mode="controlled_reload", capture_dom=True,
    ))
    assert result._dom_history_payload["html"] == HTML
    assert result._dom_history_payload["body_text"] == text
    assert result._dom_history_payload["page_instance_id"] == id(page)
    assert result._dom_history_payload["parser_path"] == "full_parser"
