"""Append-only audit journal for pre-match predictions and outcomes.

The live scorer is deliberately not imported here.  This module only gives
callers a small, failure-tolerant persistence boundary for audit evidence.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Optional

try:
    import fcntl
except ImportError:  # pragma: no cover - the runtime is POSIX
    fcntl = None

_LOG = logging.getLogger(__name__)
_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PREDICTION_PATH = _ROOT / "runtime" / "prematch_model_eval.jsonl"
DEFAULT_OUTCOME_PATH = _ROOT / "runtime" / "prematch_model_outcomes.jsonl"
MAX_RECENT_KEYS = 20_000
_LOCAL_LOCK = threading.Lock()
_POLL_SUFFIX_RE = re.compile(r"(?:[?&#]|[_:-])poll(?:[_=-]?\d+)?$", re.IGNORECASE)
_DLTV_MATCH_KILLS_RE = re.compile(r"(/matches/\d+)\.\d+(?=$|[?#])")


@dataclass
class _PredictionState:
    identity: Optional[tuple[int, int]] = None
    cursor: int = 0
    ids: deque[str] = field(default_factory=lambda: deque(maxlen=MAX_RECENT_KEYS))
    seen: set[str] = field(default_factory=set)

    def reset(self, identity: Optional[tuple[int, int]]) -> None:
        self.identity = identity
        self.cursor = 0
        self.ids.clear()
        self.seen.clear()

    def add(self, prediction_id: str) -> None:
        if prediction_id in self.seen:
            return
        if len(self.ids) == self.ids.maxlen:
            self.seen.discard(self.ids.popleft())
        self.ids.append(prediction_id)
        self.seen.add(prediction_id)


@dataclass
class _OutcomeState:
    identity: Optional[tuple[int, int]] = None
    cursor: int = 0
    found: dict[int, bool] = field(default_factory=dict)

    def reset(self, identity: Optional[tuple[int, int]]) -> None:
        self.identity = identity
        self.cursor = 0
        self.found.clear()


_PREDICTION_STATES: dict[Path, _PredictionState] = {}
_OUTCOME_STATES: dict[Path, _OutcomeState] = {}


def _path(path: Optional[os.PathLike[str] | str], env_name: str,
          default: Path) -> Path:
    if path is not None:
        return Path(path)
    configured = os.getenv(env_name)
    return Path(configured) if configured else default


def _finite(value: Any) -> Any:
    """Make a JSON-safe copy, replacing NaN/Infinity with null."""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, Mapping):
        return {str(k): _finite(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_finite(v) for v in value]
    if isinstance(value, tuple):
        return [_finite(v) for v in value]
    return value


def _json(value: Any) -> str:
    return json.dumps(_finite(value), ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _map_identity(rec: Mapping[str, Any]) -> Any:
    stable = rec.get("stable_match_id")
    if stable in (None, ""):
        stable = rec.get("match_id")
    if stable not in (None, ""):
        return stable
    value = str(rec.get("map_key") or "")
    if not value:
        # Failed maps have no durable id or key.  Keep their distinct teams in
        # the audit identity instead of collapsing every such prediction.
        return {"team1": rec.get("team1") or rec.get("radiant_team"),
                "team2": rec.get("team2") or rec.get("dire_team")}
    value = value.split("#", 1)[0].split("?", 1)[0]
    # DLTV presents the changing kills sum after its series URL number.  It is
    # presentation data, not a Dota match id, and only this exact URL shape is
    # normalized; other dotted map keys remain intact.
    value = _DLTV_MATCH_KILLS_RE.sub(r"\1", value)
    return _POLL_SUFFIX_RE.sub("", value)


def _prediction_id(rec: Mapping[str, Any]) -> str:
    # Deliberately excludes poll timestamps/suffixes and evaluation index.  A
    # map id remains part of the key, so two maps at the same index survive.
    identity = {
        "map": _map_identity(rec),
        "artifact_sha256": rec.get("artifact_sha256"),
        "branch": rec.get("branch"),
        "feature_names": rec.get("feature_names"),
        "features": rec.get("features"),
        "raw_probability": rec.get("raw_probability"),
        "probability": rec.get("probability"),
        "calibration": rec.get("calibration"),
        "reason": rec.get("reason"),
    }
    return hashlib.sha256(_json(identity).encode("utf-8")).hexdigest()


def _locked(path: Path) -> Iterator[Any]:
    """Yield a lock file while holding both process and OS-level locks."""
    class _Lock:
        def __enter__(self):
            _LOCAL_LOCK.acquire()
            self.handle = None
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                self.handle = path.with_name(path.name + ".lock").open("a+", encoding="utf-8")
                if fcntl is not None:
                    fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)
                return self.handle
            except Exception:
                if self.handle is not None:
                    self.handle.close()
                _LOCAL_LOCK.release()
                raise

        def __exit__(self, exc_type, exc, tb):
            try:
                if self.handle is not None and fcntl is not None:
                    fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
                if self.handle is not None:
                    self.handle.close()
            finally:
                _LOCAL_LOCK.release()

    return _Lock()


def _cache_path(path: Path) -> Path:
    return path.resolve()


def _identity(stat: os.stat_result) -> tuple[int, int]:
    return stat.st_dev, stat.st_ino


def _read_appended_rows(path: Path, cursor: int,
                        consume: Callable[[Mapping[str, Any]], None]) -> tuple[int, bool]:
    """Read complete JSONL rows after cursor, leaving a torn trailing row out."""
    with path.open("rb") as fh:
        fh.seek(cursor)
        while True:
            row_start = fh.tell()
            raw = fh.readline()
            if not raw:
                return row_start, False
            if not raw.endswith(b"\n"):
                # A completed JSON object may have lost only its final newline.
                # Ingest it before adding the separator, so a retry cannot add
                # a duplicate record.
                try:
                    row = json.loads(raw)
                except (TypeError, ValueError, UnicodeDecodeError):
                    row = None
                if isinstance(row, Mapping):
                    consume(row)
                return row_start, True
            try:
                row = json.loads(raw)
            except (TypeError, ValueError, UnicodeDecodeError):
                continue
            if isinstance(row, Mapping):
                consume(row)


def _terminate_torn_tail(path: Path) -> None:
    """Separate an unterminated append without mutating its audit bytes."""
    with path.open("ab") as fh:
        fh.write(b"\n")
        fh.flush()
        os.fsync(fh.fileno())


def _sync_state(path: Path, state: _PredictionState | _OutcomeState,
                consume: Callable[[Mapping[str, Any]], None]) -> None:
    """Bring one process-local state forward while the caller holds _locked()."""
    try:
        stat = path.stat()
    except FileNotFoundError:
        state.reset(None)
        return
    identity = _identity(stat)
    if state.identity != identity or stat.st_size < state.cursor:
        state.reset(identity)
    if stat.st_size == state.cursor:
        return
    cursor, torn = _read_appended_rows(path, state.cursor, consume)
    state.cursor = cursor
    if torn:
        _terminate_torn_tail(path)
        _mark_written(path, state)


def _mark_written(path: Path, state: _PredictionState | _OutcomeState) -> None:
    stat = path.stat()
    state.identity = _identity(stat)
    state.cursor = stat.st_size


def _prediction_state(path: Path) -> _PredictionState:
    return _PREDICTION_STATES.setdefault(_cache_path(path), _PredictionState())


def _outcome_state(path: Path) -> _OutcomeState:
    return _OUTCOME_STATES.setdefault(_cache_path(path), _OutcomeState())


def _consume_prediction(state: _PredictionState, row: Mapping[str, Any]) -> None:
    prediction_id = row.get("prediction_id") or row.get("stable_prediction_id")
    if isinstance(prediction_id, str) and prediction_id:
        state.add(prediction_id)


def _consume_outcome(state: _OutcomeState, row: Mapping[str, Any]) -> None:
    match_id = row.get("match_id")
    if isinstance(match_id, bool):
        return
    try:
        mid = int(match_id)
    except (TypeError, ValueError):
        return
    winner = row.get("radiant_win")
    if mid > 0 and isinstance(winner, bool):
        state.found.setdefault(mid, winner)


def _write_row(path: Path, row: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(_json(row) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def record_prediction(rec: Mapping[str, Any], path: Optional[os.PathLike[str] | str] = None) -> bool:
    """Append one prediction unless it is among the recent persisted records."""
    if not isinstance(rec, Mapping):
        return False
    row = dict(_finite(dict(rec)))
    prediction_id = _prediction_id(row)
    row["schema_version"] = 2
    row["prediction_id"] = prediction_id
    row["stable_prediction_id"] = prediction_id
    row["ts"] = time.time()
    target = _path(path, "PREMATCH_EVAL_JOURNAL", DEFAULT_PREDICTION_PATH)
    state = _prediction_state(target)
    try:
        with _locked(target):
            _sync_state(target, state, lambda existing: _consume_prediction(state, existing))
            if prediction_id in state.seen:
                return False
            _write_row(target, row)
            _mark_written(target, state)
            state.add(prediction_id)
            return True
    except Exception as exc:
        _LOG.warning("prematch journal write failed (%s)", type(exc).__name__)
        return False


def record_outcome(match_id: Any, radiant_win: Any, start: Any, end: Any,
                   source: Any, path: Optional[os.PathLike[str] | str] = None) -> bool:
    """Append a completed outcome; reject duplicates and conflicting labels."""
    if isinstance(match_id, bool):
        return False
    try:
        mid = int(match_id)
        start_i, end_i = int(start), int(end)
    except (TypeError, ValueError):
        return False
    if (mid <= 0 or start_i <= 0 or end_i <= 0 or end_i < start_i
            or not isinstance(radiant_win, bool)):
        return False
    if not isinstance(source, str) or not source.strip():
        return False
    target = _path(path, "PREMATCH_OUTCOME_JOURNAL", DEFAULT_OUTCOME_PATH)
    row = {"schema_version": 1, "match_id": mid, "radiant_win": radiant_win,
           "start": start_i, "end": end_i, "source": source,
           "observed_ts": time.time()}
    state = _outcome_state(target)
    try:
        with _locked(target):
            _sync_state(target, state, lambda existing: _consume_outcome(state, existing))
            if mid in state.found:
                if state.found[mid] != radiant_win:
                    _LOG.warning("prematch outcome conflict for match_id=%s", mid)
                return False
            _write_row(target, row)
            _mark_written(target, state)
            state.found[mid] = radiant_win
            return True
    except Exception as exc:
        _LOG.warning("prematch journal write failed (%s)", type(exc).__name__)
        return False


__all__ = ["record_prediction", "record_outcome"]
