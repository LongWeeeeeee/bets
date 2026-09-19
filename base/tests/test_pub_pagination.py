"""Реальные async-проходы bounded pub-пагинатора без сети."""
from __future__ import annotations

import asyncio
import json
import re
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import maps_research


def _match(match_id, timestamp):
    return {"id": match_id, "startDateTime": timestamp, "players": []}


def _response(pages):
    return {"data": {"players": [
        {"steamAccount": {"id": pid}, "matches": pages.get(pid, [])}
        for pid in pages
    ]}}


class _Pool:
    def __init__(self, responder):
        self.responder = responder
        self.calls = []

    async def make_request(self, *, json, **_kwargs):
        query = json["query"]
        ids = tuple(int(value) for value in re.search(r"steamAccountIds: \[([^]]*)\]", query).group(1).split(",") if value.strip())
        skip = int(re.search(r"skip: (\d+)", query).group(1))
        self.calls.append((skip, ids))
        return self.responder(ids, skip)


async def _no_wait_retry(func, *args, **kwargs):
    kwargs.pop("max_retries", None)
    kwargs.pop("sleep_time", None)
    kwargs.pop("non_retryable_exceptions", None)
    kwargs.pop("raise_last_error", None)
    return await func(*args, **kwargs)


async def _collect(**kwargs):
    return [event async for event in maps_research.iter_pub_pages(**kwargs)]


@pytest.fixture
def fake_transport(monkeypatch):
    monkeypatch.setattr(maps_research, "retry_request_with_proxy_rotation", _no_wait_retry)

    def install(responder):
        pool = _Pool(responder)
        monkeypatch.setattr(maps_research, "get_proxy_pool", lambda: pool)
        return pool

    return install


def test_bfs_refills_skip_bucket_across_more_than_five_players(fake_transport):
    cutoff = 1_700_000_000

    def responder(ids, skip):
        if skip == 0:
            return _response({pid: [_match(pid * 1000 + n, cutoff + 1) for n in range(100)]
                              if pid <= 6 else [] for pid in ids})
        assert skip == 100
        return _response({pid: [] for pid in ids})

    pool = fake_transport(responder)
    events = asyncio.run(_collect(player_ids=range(1, 9), start_date_time=cutoff,
                                  batch_size=5, batch_concurrency=1))

    assert pool.calls == [(0, (1, 2, 3, 4, 5)), (0, (6, 7, 8)),
                          (100, (1, 2, 3, 4, 5)), (100, (6,))]
    assert set().union(*(event[0] for event in events)) == set(range(1, 9))


def test_ordered_old_match_stops_but_known_fresh_page_continues(fake_transport):
    cutoff = 1_700_000_000
    pages = {
        1: [_match(n, cutoff + 1) for n in range(99)] + [_match(100, cutoff - 1)],
        2: [_match(1000 + n, cutoff + 1) for n in range(100)],
    }

    def responder(ids, skip):
        if skip == 100:
            return _response({pid: [] for pid in ids})
        return _response({pid: pages[pid] for pid in ids})

    pool = fake_transport(responder)
    existing = set(range(1000, 1100))
    events = asyncio.run(_collect(player_ids=[1, 2], start_date_time=cutoff,
                                  existing_match_ids=existing))

    assert pool.calls == [(0, (1, 2)), (100, (2,))]
    assert {1, 2} == set().union(*(event[0] for event in events))
    assert 100 not in existing  # old page does not add an out-of-window match


def test_timestamp_anomalies_keep_player_walking_until_short_page(fake_transport):
    cutoff = 1_700_000_000

    def responder(ids, skip):
        pid = ids[0]
        if skip == 0:
            return _response({pid: [_match(1, None)] + [_match(n, cutoff + 5) for n in range(2, 101)]})
        if skip == 100:
            # Old then newer is out-of-order; sticky unsafe state forces skip=200.
            return _response({pid: [_match(101, cutoff - 1), _match(102, cutoff + 1)]
                              + [_match(n, cutoff - 2) for n in range(103, 201)]})
        return _response({pid: []})

    pool = fake_transport(responder)
    events = asyncio.run(_collect(player_ids=[7], start_date_time=cutoff))

    assert [skip for skip, _ids in pool.calls] == [0, 100, 200]
    assert events[-1][0] == {7}


def test_ineligible_queried_player_does_not_expand_participant_ids(fake_transport):
    cutoff = 1_700_000_000
    match = _match(1, cutoff + 1)
    match["players"] = [{"intentionalFeeding": False,
                        "steamAccount": {"id": 99, "smurfFlag": 0, "isAnonymous": False}}]
    fake_transport(lambda _ids, _skip: {"data": {"players": [{
        "steamAccount": {"id": 1, "smurfFlag": 1, "isAnonymous": False},
        "matches": [match],
    }]}})

    events = asyncio.run(_collect(player_ids=[1], start_date_time=cutoff, player_ids_check=True))

    assert events[0][2] == set()


@pytest.mark.parametrize("payload", [
    {"errors": [{"message": "bad query"}]},
    {"data": {"players": []}},
    {"data": {"players": [{"steamAccount": {"id": 1}, "matches": None}]}},
])
def test_malformed_or_partial_response_raises_without_processing(fake_transport, payload):
    fake_transport(lambda _ids, _skip: payload)

    with pytest.raises(maps_research.PubPaginationError):
        asyncio.run(_collect(player_ids=[1, 2], start_date_time=1_700_000_000))


def test_failed_second_page_does_not_complete_player_or_drop_first_page_ids(monkeypatch):
    cutoff = 1_700_000_000
    calls = []

    async def retry_three_times(func, *args, **kwargs):
        kwargs.pop("max_retries", None)
        kwargs.pop("sleep_time", None)
        kwargs.pop("non_retryable_exceptions", None)
        kwargs.pop("raise_last_error", None)
        last_error = None
        for _ in range(3):
            try:
                return await func(*args, **kwargs)
            except OSError as error:
                last_error = error
        raise last_error

    def responder(ids, skip):
        calls.append(skip)
        if skip == 0:
            return _response({ids[0]: [_match(n, cutoff + 1) for n in range(1, 101)]})
        raise OSError("temporary proxy failure")

    pool = _Pool(responder)
    monkeypatch.setattr(maps_research, "get_proxy_pool", lambda: pool)
    monkeypatch.setattr(maps_research, "retry_request_with_proxy_rotation", retry_three_times)
    existing = set()
    events = asyncio.run(_collect(player_ids=[1], start_date_time=cutoff,
                                  existing_match_ids=existing))

    assert calls == [0, 100, 100, 100]
    assert events[0][0] == set() and events[-1][3] == {1}
    assert existing == set(range(1, 101))


def test_proceed_wrapper_stages_shared_dedup_until_all_pages_succeed(monkeypatch):
    cutoff = 1_700_000_000
    phase = {"fail": True}

    def responder(ids, skip):
        if skip == 0:
            return _response({ids[0]: [_match(n, cutoff + 1) for n in range(1, 101)]})
        if phase["fail"]:
            raise OSError("second page failed")
        return _response({ids[0]: []})

    pool = _Pool(responder)
    monkeypatch.setattr(maps_research, "get_proxy_pool", lambda: pool)
    monkeypatch.setattr(maps_research, "retry_request_with_proxy_rotation", _no_wait_retry)
    shared = set()

    with pytest.raises(RuntimeError, match="pub page failed"):
        asyncio.run(maps_research.proceed_get_maps_with_data(
            ids_to_graph=[1], start_date_time=cutoff, existing_match_ids=shared))
    assert shared == set()

    phase["fail"] = False
    matches, _player_ids = asyncio.run(maps_research.proceed_get_maps_with_data(
        ids_to_graph=[1], start_date_time=cutoff, existing_match_ids=shared))
    assert {match["id"] for match in matches} == set(range(1, 101))
    assert shared == set(range(1, 101))


def test_shared_existing_ids_dedup_between_completed_page_tasks(fake_transport):
    cutoff = 1_700_000_000
    fake_transport(lambda ids, _skip: _response({pid: [_match(55, cutoff + 1)] for pid in ids}))
    existing = set()

    first = asyncio.run(_collect(player_ids=[1], start_date_time=cutoff, existing_match_ids=existing))
    second = asyncio.run(_collect(player_ids=[2], start_date_time=cutoff, existing_match_ids=existing))

    assert [match["id"] for event in first for match in event[1]] == [55]
    assert [match["id"] for event in second for match in event[1]] == []
    assert existing == {55}


def test_pagination_cap_is_explicit(fake_transport, monkeypatch):
    cutoff = 1_700_000_000
    fake_transport(lambda ids, _skip: _response({pid: [_match(n, cutoff + 1) for n in range(100)] for pid in ids}))
    monkeypatch.setattr(maps_research, "PUB_PAGINATION_MAX_SKIP", 0)

    with pytest.raises(maps_research.PubPaginationCapError):
        asyncio.run(_collect(player_ids=[1], start_date_time=cutoff))


def test_get_maps_new_marks_only_terminal_pub_players_and_keeps_watermark(monkeypatch, tmp_path):
    cutoff = 1_700_000_000
    pool = _Pool(lambda ids, skip: (
        _response({1: [_match(n, cutoff + 1) for n in range(100)], 2: []})
        if skip == 0 else (_ for _ in ()).throw(OSError("second page failed"))
    ))
    saved_pub = []
    cleared_state = []
    monkeypatch.setattr(maps_research, "get_proxy_pool", lambda: pool)
    monkeypatch.setattr(maps_research, "retry_request_with_proxy_rotation", _no_wait_retry)
    monkeypatch.setattr(maps_research, "check_match_quality", lambda _match: (True, "ok"))
    monkeypatch.setattr(maps_research, "merge_temp_files_by_patch", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(maps_research, "load_get_maps_state", lambda: None)
    monkeypatch.setattr(maps_research, "save_get_maps_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(maps_research, "clear_get_maps_state", lambda *_args, **_kwargs: cleared_state.append(True))
    monkeypatch.setattr(maps_research, "load_pub_player_ids", lambda: None)
    monkeypatch.setattr(maps_research, "save_pub_player_ids", lambda *args, **_kwargs: saved_pub.append(args))

    with pytest.raises(RuntimeError, match="pub crawl incomplete"):
        asyncio.run(maps_research.get_maps_new(
            ids=[1, 2], mkdir=str(tmp_path), show_prints=False, start_date_time=cutoff))

    assert pool.calls == [(0, (1, 2)), (100, (1,))]
    assert json.loads((tmp_path / "processed_ids_to_graph.txt").read_text()) == [2]
    assert saved_pub == []
    assert cleared_state == []


def test_pro_wrapper_remains_compatible_for_empty_team_response(monkeypatch):
    class ProPool:
        async def make_request(self, **_kwargs):
            return {"data": {"teams": []}}

    monkeypatch.setattr(maps_research, "get_proxy_pool", lambda: ProPool())
    matches, player_ids = asyncio.run(maps_research.proceed_get_maps_with_data(
        ids_to_graph=[1], pro=True, start_date_time=1_700_000_000))

    assert matches == [] and player_ids == set()
