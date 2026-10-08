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
    CAPTURED_403, EXPIRED, VALID, URL, clock, event_loop, install_http, mr, ok,
)


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


@pytest.mark.parametrize("expired", [False, True], ids=["revoked", "expired"])
def test_topup_cli_dead_keys_exit_nonzero_with_error_line(tmp_path, expired):
    # Execute the real topup entrypoint; only configuration, HTTP and time are fake.
    root = Path(__file__).resolve().parents[2]
    corpus = tmp_path / "pro"
    corpus.mkdir()
    (corpus / mr.PRO_VISITED_TEAMS_FILE).write_text("[]")
    code = f"""
import runpy, time
import maps_research as m
import id_to_names, tier_dynamic_overlay, os
time.time = lambda: {1_791_400_000.0!r}
m.PRO_HEROES_DIR = {str(corpus)!r}
m.STRATZ_PROXY_MAP = {{'http://test:1': {(EXPIRED if expired else VALID)!r}}}
m.proxy_pool = None
id_to_names.tier_one_teams = {{'test': 1}}
id_to_names.tier_two_teams = {{}}
os.environ[tier_dynamic_overlay.ENV_PATH_VAR] = {str(tmp_path / 'overlay.json')!r}
m.ProxyAPIPool._post_json_with_requests = staticmethod(lambda *a, **k: ({CAPTURED_403!r}, {{}}))
runpy.run_path({str(root / 'scripts/pro_chain/topup_pro_corpus.py')!r}, run_name='__main__')
"""
    env = dict(os.environ, DRAFT_ROOT=str(tmp_path))
    env["PYTHONPATH"] = str(root / "base") + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run([sys.executable, "-c", code], cwd=tmp_path, env=env,
                            capture_output=True, text=True, timeout=5)
    assert result.returncode != 0, result.stdout
    errors = [line for line in result.stdout.splitlines() if "ОШИБКА" in line]
    assert len(errors) == 1, result.stdout + result.stderr
    assert "StratzAuthError" in result.stderr


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
