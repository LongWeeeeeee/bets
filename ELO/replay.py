"""Shared start/result event ordering for offline Elo and snapshot rebuilds."""
from __future__ import annotations

import heapq
from dataclasses import replace
from typing import Iterable, Iterator

from ELO.domain import MatchRecord

REPLAY_VERSION = "finished_results_v1"


def replay_events(matches: Iterable[MatchRecord]) -> Iterator[tuple[str, int, MatchRecord]]:
    """Predict every start at t before observing any result at t.

    Input is unique maps sorted by (start, id). Unknown durations still permit
    prediction but never update ratings. Heap size is concurrent matches, not
    the entire corpus. Result updates recompute expectation from the state at
    observation time, as the live process does when a result arrives.
    """
    pending = []
    previous = None
    for match in matches:
        key = match.timestamp, match.match_id
        if previous is not None and key <= previous:
            raise ValueError("replay requires unique maps sorted by (timestamp, match_id)")
        previous = key
        while pending and pending[0][0] < match.timestamp:
            end, _, finished = heapq.heappop(pending)
            yield "result", end, finished
        yield "start", match.timestamp, match
        end = match.result_timestamp
        if end is not None:
            heapq.heappush(pending, (end, match.match_id, match))
    while pending:
        end, _, finished = heapq.heappop(pending)
        yield "result", end, finished


def result_record(match: MatchRecord, observed_at: int | None = None) -> MatchRecord:
    timestamp = match.result_timestamp if observed_at is None else observed_at
    if timestamp is None or timestamp < match.timestamp:
        raise ValueError("result needs a valid observation time")
    return replace(match, timestamp=int(timestamp), duration_seconds=None)


def prepare_prediction(model, timestamp: int) -> None:
    reset = getattr(model, "_maybe_apply_patch_local_reset", None)
    if callable(reset):
        reset(timestamp)
