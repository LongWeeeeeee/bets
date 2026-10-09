"""Boundaries of the 08.10 STRATZ follow-ups: CLI, visited teams and workers."""
import asyncio
import json
import os
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pytest

from test_stratz_dead_keys import (
    EXPIRED, VALID, URL, clock, event_loop, mr, ok,
)

CAPTURE = json.loads((Path(__file__).parent / "fixtures" /
                     "stratz_auth_403_20261008.json").read_text())
CAPTURED_403 = CAPTURE["body"]


def install_http(monkeypatch, handler):
    """Mock only physical HTTP; pool selection, parsing and retry remain real."""
    from types import SimpleNamespace

    def post(url, proxies=None, **kwargs):
        data, headers = handler(url, (proxies or {}).get("https"), **kwargs)
        status = 403 if data == CAPTURED_403 else (
            429 if data.get("message") == "API rate limit exceeded" else 200)
        return SimpleNamespace(status_code=status, headers=headers,
                               json=lambda: data, text=json.dumps(data))

    monkeypatch.setattr(mr.cf_requests, "post", post)


@pytest.fixture
def pro_corpus(monkeypatch, tmp_path):
    import id_to_names
    import tier_dynamic_overlay

    monkeypatch.chdir(tmp_path)
    corpus = tmp_path / "pro"
    corpus.mkdir()
    monkeypatch.setattr(mr, "PRO_HEROES_DIR", corpus)
    monkeypatch.setattr(mr, "STRATZ_PROXY_MAP", {"http://test:1": VALID})
    monkeypatch.setattr(mr, "proxy_pool", None)
    monkeypatch.setattr(id_to_names, "tier_one_teams", {"test": set(range(1, 8))})
    monkeypatch.setattr(id_to_names, "tier_two_teams", {})
    monkeypatch.setenv(tier_dynamic_overlay.ENV_PATH_VAR, str(tmp_path / "overlay.json"))
    mr._save_visited_teams(str(corpus / mr.PRO_VISITED_TEAMS_FILE), {99})
    return corpus


@pytest.mark.parametrize("mode, rc, summary", [
    ("revoked", 4, "ok=0 failed=2 auth_failed=2"),
    ("expired", 4, "ok=0 failed=2 auth_failed=2"),
    ("expired_empty", 4, "ok=0 failed=0 auth_failed=0"),
    ("partial", 0, "ok=6 failed=1 auth_failed=0"),
    ("failed", 4, "ok=0 failed=2 auth_failed=0"),
    ("auth_after_success", 4, "ok=1 failed=1 auth_failed=1"),
    ("good", 0, "ok=2 failed=0 auth_failed=0"),
    ("backfill_auth", 4, "ok=2 failed=0 auth_failed=0"),
    ("empty", 0, "ok=0 failed=0 auth_failed=0"),
    ("locked", 0, "ok=0 failed=0 auth_failed=0"),
])
def test_topup_cli_batch_summary_and_exit_status(tmp_path, mode, rc, summary):
    # Execute the real topup entrypoint; only configuration, HTTP and time are fake.
    root = Path(__file__).resolve().parents[2]
    corpus = tmp_path / "pro"
    corpus.mkdir()
    (corpus / mr.PRO_VISITED_TEAMS_FILE).write_text("[]")
    code = rf"""
import keys  # preloaded from the test stub, never the real base/keys.py
import runpy, time, asyncio, json, re, io, urllib.request, sys
from types import SimpleNamespace
import maps_research as m
import id_to_names, tier_dynamic_overlay, os
time.time = lambda: {1_791_400_000.0!r}
time.sleep = lambda delay: None
sys.path.insert(0, {str(root / 'scripts/pro_chain')!r})
m.PRO_HEROES_DIR = {str(corpus)!r}
m.STRATZ_PROXY_MAP = {{'http://test:1': {(EXPIRED if mode.startswith('expired') else VALID)!r},
                      'http://test:2': {(EXPIRED if mode.startswith('expired') else VALID + 'b')!r}}}
m.proxy_pool = None
id_to_names.tier_one_teams = {{'test': set(range(1, 32 if {mode!r} == 'partial' else 8))}} if {mode!r} not in ('empty', 'expired_empty') else {{}}
id_to_names.tier_two_teams = {{}}
os.environ[tier_dynamic_overlay.ENV_PATH_VAR] = {str(tmp_path / 'overlay.json')!r}
def post(url, **kwargs):
    match = re.search(r'teamIds: (\[[^]]*\])', kwargs['json']['query'])
    ids = json.loads(match[1]) if match else []
    if {mode!r} == 'failed' or ({mode!r} == 'partial' and 6 in ids):
        raise asyncio.CancelledError()
    revoked = ({mode!r} == 'revoked' or ({mode!r} == 'auth_after_success' and 6 in ids)
               or ({mode!r} == 'backfill_auth' and not match))
    data = {CAPTURED_403!r} if revoked else {{'data': {{'teams': []}}}}
    return SimpleNamespace(status_code=403 if revoked else 200,
                           headers={{}}, json=lambda: data, text=json.dumps(data))
m.cf_requests.post = post
pages = iter([[{{'match_id': 100, 'start_time': int(time.time())}}], []]
             if {mode!r} == 'backfill_auth' else [[]])
urllib.request.urlopen = lambda *a, **k: io.BytesIO(json.dumps(next(pages)).encode())
if {mode!r} == 'locked':
    from pathlib import Path
    lock = Path({str(tmp_path / 'runtime/topup_pro_corpus.lock')!r})
    lock.parent.mkdir()
    lock.write_text('existing-owner')
runpy.run_path({str(root / 'scripts/pro_chain/topup_pro_corpus.py')!r}, run_name='__main__')
"""
    env = dict(os.environ, DRAFT_ROOT=str(tmp_path))
    env["PYTHONPATH"] = env.get("PYTHONPATH", "") + os.pathsep + str(root / "base")
    result = subprocess.run([sys.executable, "-c", code], cwd=tmp_path, env=env,
                            capture_output=True, text=True, timeout=5)
    assert result.returncode == rc, result.stdout + result.stderr
    summaries = [line for line in result.stdout.splitlines() if line.startswith("STRATZ batches:")]
    assert summaries == ["STRATZ batches: " + summary], result.stdout + result.stderr
    assert mr._load_visited_teams(str(corpus / mr.PRO_VISITED_TEAMS_FILE)) == (
        set(range(1, 8)) if mode in ("good", "backfill_auth") else
        set(range(1, 32)) - set(range(6, 11)) if mode == "partial" else
        {1, 2, 3, 4, 5} if mode == "auth_after_success" else set())


@pytest.mark.parametrize("failure", ["auth", "cancelled"])
def test_pros_failed_batch_stays_unvisited_and_retries_next_run(
        monkeypatch, clock, pro_corpus, capsys, failure):
    batches = []

    def post(url, proxy, **kwargs):
        ids = json.loads(re.search(r"teamIds: (\[[^]]*\])", kwargs["json"]["query"])[1])
        batches.append(ids)
        if 6 in ids:
            if failure == "auth":
                return CAPTURED_403, {}
            # A cancelled HTTP operation is a failed gather batch, not an auth error.
            raise asyncio.CancelledError()
        return ok()

    install_http(monkeypatch, post)
    with pytest.raises(mr.StratzAuthError if failure == "auth" else RuntimeError):
        mr.get_pros()
    visited_path = str(pro_corpus / mr.PRO_VISITED_TEAMS_FILE)
    assert mr._load_visited_teams(visited_path) == {1, 2, 3, 4, 5, 99}
    assert batches == [[1, 2, 3, 4, 5], [6, 7]], "no retry loop inside this run"
    assert sum("ОШИБКА" in line for line in capsys.readouterr().out.splitlines()) == 1

    batches.clear()
    monkeypatch.setattr(mr, "proxy_pool", None)

    def recovered(url, proxy, **kwargs):
        batches.append(json.loads(re.search(r"teamIds: (\[[^]]*\])", kwargs["json"]["query"])[1]))
        return ok()

    install_http(monkeypatch, recovered)
    mr.get_pros()
    assert batches == [[6, 7]]
    assert mr._load_visited_teams(visited_path) == set(range(1, 8)) | {99}


async def collect(kind, out_dir, ids, pace=0):
    if kind == "playback":
        return await mr.get_playback_new(ids, str(out_dir), pace=pace, show_prints=False)
    return await mr.get_batched(ids, str(out_dir), "{ id }", lambda row: row,
                                batch=1, pace=pace, show_prints=False)


@pytest.mark.parametrize("kind", ["playback", "batched"])
@pytest.mark.parametrize("html", [False, True], ids=["json403", "html403"])
def test_revoked_workers_terminate_and_all_dead_raise(
        monkeypatch, clock, event_loop, tmp_path, kind, html):
    import keys
    monkeypatch.setattr(keys, "STRATZ_PAIRS", [("http://r1:1", VALID),
                                               ("http://r2:1", VALID + "b")], raising=False)
    calls = Counter()

    def post(url, proxy, **kwargs):
        calls[proxy] += 1
        if html:
            raise RuntimeError("HTTP 403: <html>A bearer token is required for a request.</html>")
        return CAPTURED_403, {}

    install_http(monkeypatch, post)
    with pytest.raises(mr.StratzAuthError):
        event_loop.run_until_complete(asyncio.wait_for(collect(kind, tmp_path, [1, 2]), 4))
    assert calls == {"http://r1:1": 1, "http://r2:1": 1}
    assert not clock.sleeps, "auth failures must not enter the 900-second backoff"


@pytest.mark.parametrize("kind", ["playback", "batched"])
def test_live_worker_drains_requeued_batch_after_revoked_worker_stops(
        monkeypatch, clock, event_loop, tmp_path, kind):
    import keys
    import threading
    import gzip

    monkeypatch.setattr(keys, "STRATZ_PAIRS", [("http://revoked:1", VALID),
                                               ("http://live:1", VALID + "b")], raising=False)
    live_served = threading.Event()
    calls = Counter()
    live_times = []

    def post(url, proxy, **kwargs):
        calls[proxy] += 1
        if proxy == "http://revoked:1":
            assert live_served.wait(2), "live worker did not start"
            return CAPTURED_403, {}
        query = kwargs["json"]
        live_times.append(clock.now)
        mid = query["variables"]["id"] if kind == "playback" else int(
            re.search(r"match\(id: (\d+)\)", query["query"])[1])
        live_served.set()
        row = {"id": mid, "parsedDateTime": 1}
        return {"data": {"match" if kind == "playback" else "m0": row}}, {}

    install_http(monkeypatch, post)
    count = event_loop.run_until_complete(asyncio.wait_for(collect(kind, tmp_path, [1, 2], pace=2.7), 4))
    assert count == 2
    assert calls == {"http://revoked:1": 1, "http://live:1": 2}
    with gzip.open(next(tmp_path.glob("*.jsonl.gz")), "rt") as fh:
        assert {json.loads(line)["id"] for line in fh} == {1, 2}
    assert live_times[1] - live_times[0] == pytest.approx(2.7)


def test_dead_tracker_does_not_exhaust_retries_while_live_key_is_rate_limited(
        monkeypatch, clock, event_loop):
    pool = mr.ProxyAPIPool([("http://revoked:1", VALID), ("http://live:1", VALID + "b")])
    calls = Counter()

    def post(url, proxy, **kwargs):
        calls[proxy] += 1
        if proxy == "http://revoked:1":
            return CAPTURED_403, {}
        if calls[proxy] <= 4:
            return {"message": "API rate limit exceeded"}, {"retry-after": "1"}
        return ok()

    install_http(monkeypatch, post)
    assert event_loop.run_until_complete(pool.make_request(URL)) == ok()[0]
    assert calls == {"http://revoked:1": 1, "http://live:1": 5}
    assert len(clock.sleeps) == 4


@pytest.mark.parametrize("kind", ["playback", "batched"])
def test_revoked_worker_stops_while_429_worker_retries(
        monkeypatch, clock, event_loop, tmp_path, kind):
    import keys
    monkeypatch.setattr(keys, "STRATZ_PAIRS", [("http://revoked:1", VALID),
                                               ("http://live:1", VALID + "b")], raising=False)
    calls = Counter()

    def post(url, proxy, **kwargs):
        calls[proxy] += 1
        if proxy == "http://revoked:1":
            return CAPTURED_403, {}
        if calls[proxy] <= 4:
            return {"message": "API rate limit exceeded"}, {"retry-after": "1"}
        query = kwargs["json"]
        mid = query["variables"]["id"] if kind == "playback" else int(
            re.search(r"match\(id: (\d+)\)", query["query"])[1])
        return {"data": {"match" if kind == "playback" else "m0":
                         {"id": mid, "parsedDateTime": 1}}}, {}

    install_http(monkeypatch, post)
    assert event_loop.run_until_complete(asyncio.wait_for(
        collect(kind, tmp_path, [1, 2, 3]), 4)) == 3
    # Existing K=1 is stricter than K=3: repeated 403 must never be scheduled.
    assert calls == {"http://revoked:1": 1, "http://live:1": 7}
    assert len(clock.sleeps) == 4


@pytest.mark.parametrize("failure", ["json503", "html503", "timeout"])
def test_transient_transport_failure_does_not_kill_key(
        monkeypatch, clock, event_loop, failure):
    from types import SimpleNamespace
    pool = mr.ProxyAPIPool([("http://test:1", VALID)])
    calls = []

    def post(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            if failure == "timeout":
                raise mr.cf_requests.exceptions.Timeout("offline test timeout")

            def payload():
                if failure == "html503":
                    raise ValueError("HTML gateway response")
                return CAPTURED_403

            return SimpleNamespace(status_code=503, headers={}, json=payload,
                                   text="<html>A bearer token is required</html>")
        return SimpleNamespace(status_code=200, headers={}, json=lambda: ok()[0])

    monkeypatch.setattr(mr.cf_requests, "post", post)
    assert event_loop.run_until_complete(pool.make_request(URL)) == ok()[0]
    assert len(calls) == 2
    assert not pool.trackers[0].auth_dead


@pytest.mark.parametrize("status", [401, 403])
def test_auth_http_status_drops_key(monkeypatch, clock, event_loop, status):
    from types import SimpleNamespace
    pool = mr.ProxyAPIPool([("http://test:1", VALID)])
    monkeypatch.setattr(mr.cf_requests, "post", lambda *a, **k: SimpleNamespace(
        status_code=status, headers={}, json=lambda: CAPTURED_403))
    with pytest.raises(mr.StratzAuthError):
        event_loop.run_until_complete(pool.make_request(URL))
    assert pool.trackers[0].auth_dead
