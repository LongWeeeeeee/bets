"""Frozen kills-v3 history snapshot and radiant T+P prematch features.

Only team IDs, ten account IDs, and query start affect T/P. League, series and
heroes affect G/H and are neutral in features_for_map. Source snapshots must be
the same immutable inputs used by the offline dataset for parity.
"""
from __future__ import annotations

import argparse
from collections import deque
import ctypes
import json
import os
from pathlib import Path
import sys
import warnings

import numpy as np

from base.tools import kills_v3_research as v3


SCHEMA = "kills-v3-serving-state-v2"
CODE_VERSION = {"builder_sha256": v3.sha256(Path(v3.__file__)),
                "serving_sha256": v3.sha256(Path(__file__))}


def _rss_bytes() -> int:
    if sys.platform == "darwin":
        libc = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
        libc.mach_task_self.restype = ctypes.c_uint32
        libc.task_info.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_void_p,
                                   ctypes.POINTER(ctypes.c_uint32)]
        info = (ctypes.c_uint32 * 12)()
        count = ctypes.c_uint32(12)
        if libc.task_info(libc.mach_task_self(), 20, info, ctypes.byref(count)) != 0:
            raise RuntimeError("mach_task_basic_info failed")
        return int(ctypes.cast(info, ctypes.POINTER(ctypes.c_uint64))[1])
    if sys.platform.startswith("linux"):
        return int(Path("/proc/self/statm").read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    raise RuntimeError("RSS measurement unsupported on this platform")


def build_state(rich_path: Path, db_paths: list[Path], *, history_start: int,
                cutoff: int, visibility_delay: int = 0, **builder_params) -> v3.History:
    """Replay the same deduplicated end-order prefix as a query at cutoff.

    Strict visibility is end < min(cutoff - delay, cutoff). For a frozen dataset
    queried later with delay > 0, a newer event may become visible; parity then
    detects that difference rather than silently treating this state as current.
    """
    if history_start < 0 or cutoff < history_start or visibility_delay < 0:
        raise ValueError("require 0 <= history_start <= cutoff and visibility_delay >= 0")
    allowed = {"half_life_days", "pseudo_games", "poisson_lr", "elo_k"}
    if set(builder_params) - allowed:
        raise TypeError(f"unsupported builder parameters: {sorted(set(builder_params) - allowed)}")
    defaults = {"half_life_days": 90, "pseudo_games": 5, "poisson_lr": 0.03, "elo_k": 20}
    defaults.update(builder_params)
    if defaults["half_life_days"] <= 0 or defaults["pseudo_games"] < 0 or defaults["poisson_lr"] < 0 or defaults["elo_k"] < 0:
        raise ValueError("invalid history parameters")
    rich_path = Path(rich_path).resolve()
    db_paths = [Path(p).resolve() for p in db_paths]
    source_hashes = {str(p): v3.sha256(p) for p in [rich_path] + db_paths if p.exists()}
    events, audit = v3.load_events(rich_path, db_paths, int(history_start))
    used = [rich_path] + [p for p in db_paths if str(p) not in audit["db_paths_missing"]]
    if any(str(p) not in source_hashes for p in used):
        raise RuntimeError("source appeared while loading; retry on a stable snapshot")
    state = v3.History(**defaults)
    end_order = np.argsort(events["end"], kind="stable")
    visible = min(int(cutoff) - int(visibility_delay), int(cutoff))
    applied = 0
    for i in end_order:
        if int(events["end"][i]) >= visible:
            break
        state.apply(events, int(i))
        applied += 1
    pending_order = end_order[applied:]
    pending_order = pending_order[events["end"][pending_order] < cutoff]
    pending_keys = ("end", "final", "duration", "teams", "timeline", "win",
                    "league", "accounts", "heroes", "pmetrics")
    state.serving_pending = {key: events[key][pending_order].copy() for key in pending_keys}
    state.serving_pending_at = 0
    state.serving_last_query_start = int(cutoff)
    for p in used:
        if v3.sha256(p) != source_hashes[str(p)]:
            raise RuntimeError(f"source changed during replay: {p}")
    names, blocks = v3.feature_layout()
    state.serving_meta = {"schema": SCHEMA, "numpy_version": np.__version__, "cutoff": int(cutoff),
                          "history_start": int(history_start), "visibility_delay": int(visibility_delay),
                          "builder_params": defaults, "source_hashes": {str(p): source_hashes[str(p)] for p in used},
                          "source_order": [str(p) for p in used],
                          "rich_path": str(rich_path), "db_paths": [str(p) for p in db_paths],
                          "feature_names": names, "tp_names": blocks["T"] + blocks["P"],
                          "events_loaded": len(events["mid"]), "events_replayed": applied,
                          "events_pending": len(pending_order),
                          "code_version": dict(CODE_VERSION)}
    return state


class _DecayedMap:
    """Sorted columnar backing; materialize only entities touched by queries."""
    def __init__(self, ids, sums, counts, last_time, last_event, width):
        self.ids, self.sums, self.counts = ids, sums, counts
        self.last_time, self.last_event, self.width = last_time, last_event, width
        self.overlay = {}

    def get(self, key, default=None):
        key = int(key)
        if key in self.overlay:
            return self.overlay[key]
        pos = int(np.searchsorted(self.ids, key))
        if pos == len(self.ids) or self.ids[pos] != key:
            return default
        obj = v3.Decayed(self.width)
        obj.sums = self.sums[pos].copy()
        obj.counts = self.counts[pos].copy()
        obj.last_time = int(self.last_time[pos])
        obj.last_event = int(self.last_event[pos])
        self.overlay[key] = obj
        return obj

    def __getitem__(self, key):
        obj = self.get(key)
        if obj is None:
            obj = v3.Decayed(self.width)
            self.overlay[int(key)] = obj
        return obj

    def setdefault(self, key, default):
        obj = self.get(key)
        if obj is None:
            self.overlay[int(key)] = default
            return default
        return obj

    def items(self):
        for key in self.ids:
            yield int(key), self.get(int(key))
        for key, obj in self.overlay.items():
            if not _contains(self.ids, key):
                yield key, obj


def _contains(ids, key):
    pos = int(np.searchsorted(ids, key))
    return pos < len(ids) and ids[pos] == key


class _ScalarMap:
    def __init__(self, ids, values, default, f32=None):
        self.ids, self.values, self.default = ids, values, default
        if f32 is None:
            self.f32 = frozenset()
        else:
            flags = np.asarray(f32, dtype=bool)
            self.f32 = frozenset(int(key) for key, flag in zip(ids, flags) if flag)
        self.overlay = {}

    def __getitem__(self, key):
        key = int(key)
        if key in self.overlay:
            return self.overlay[key]
        pos = int(np.searchsorted(self.ids, key))
        if pos >= len(self.ids) or self.ids[pos] != key:
            return self.default
        return np.float32(self.values[pos]) if key in self.f32 else float(self.values[pos])

    def __setitem__(self, key, value):
        self.overlay[int(key)] = value

    def items(self):
        for key in self.ids:
            yield int(key), self[int(key)]
        for key, value in self.overlay.items():
            if not _contains(self.ids, key):
                yield key, value


class _QueueMap:
    def __init__(self, ids, offsets, ends, values):
        self.ids, self.offsets, self.ends, self.values = ids, offsets, ends, values
        self.overlay = {}

    def __getitem__(self, key):
        key = int(key)
        if key not in self.overlay:
            pos = int(np.searchsorted(self.ids, key))
            if pos < len(self.ids) and self.ids[pos] == key:
                lo, hi = int(self.offsets[pos]), int(self.offsets[pos + 1])
                self.overlay[key] = deque((int(self.ends[j]), self.values[j].copy()) for j in range(lo, hi))
            else:
                self.overlay[key] = deque()
        return self.overlay[key]

    def items(self):
        for key in self.ids:
            yield int(key), self[int(key)]
        for key, value in self.overlay.items():
            if not _contains(self.ids, key):
                yield key, value


def _pack_decayed(data, width, prefix, arrays):
    rows = sorted(data.items())
    arrays[prefix + "_ids"] = np.asarray([key for key, _ in rows], np.int64)
    arrays[prefix + "_sums"] = np.asarray([obj.sums for _, obj in rows], np.float64).reshape(-1, width)
    arrays[prefix + "_counts"] = np.asarray([obj.counts for _, obj in rows], np.float64).reshape(-1, width)
    arrays[prefix + "_last_time"] = np.asarray([obj.last_time for _, obj in rows], np.int64)
    arrays[prefix + "_last_event"] = np.asarray([obj.last_event for _, obj in rows], np.int64)


def _pack_scalar(data, prefix, arrays):
    rows = sorted(data.items())
    arrays[prefix + "_ids"] = np.asarray([key for key, _ in rows], np.int64)
    arrays[prefix + "_values"] = np.asarray([value for _, value in rows], np.float64)
    arrays[prefix + "_f32"] = np.asarray([isinstance(value, np.float32) for _, value in rows],
                                         dtype=bool)


def save_state(state: v3.History, path: Path) -> None:
    path = Path(path)
    meta = state.serving_meta
    if meta["schema"] != SCHEMA:
        raise ValueError("unrecognized state schema")
    arrays = {"metadata": np.asarray(json.dumps(meta, sort_keys=True)),
              "pending_at": np.asarray(state.serving_pending_at, np.int64),
              "last_query_start": np.asarray(state.serving_last_query_start, np.int64)}
    for key, values in state.serving_pending.items():
        arrays["pending_" + key] = values
    for prefix, data, width in (("team", state.team, len(v3.TEAM_METRICS)),
                                ("player", state.player, len(v3.PLAYER_METRICS)),
                                ("hero", state.hero, len(v3.HERO_METRICS)),
                                ("league_stats", state.league_stats, 3)):
        _pack_decayed(data, width, prefix, arrays)
    for prefix, data in (("attack", state.attack), ("defense", state.defense),
                         ("elo", state.elo), ("player_attack", state.player_attack)):
        _pack_scalar(data, prefix, arrays)
    for name in ("team_pop", "player_pop", "hero_pop", "global_total"):
        obj = getattr(state, name)
        arrays[name + "_sums"] = obj.sums
        arrays[name + "_counts"] = obj.counts
        arrays[name + "_times"] = np.asarray([obj.last_time, obj.last_event], np.int64)
    rows = sorted(state.leagues.items())
    arrays["leagues_ids"] = np.asarray([key for key, _ in rows], np.int64)
    arrays["leagues_offsets"] = np.asarray([0] + list(np.cumsum([len(q) for _, q in rows])), np.int64)
    arrays["leagues_ends"] = np.asarray([end for _, q in rows for end, _ in q], np.int64)
    arrays["leagues_values"] = np.asarray([values for _, q in rows for _, values in q], np.float64).reshape(-1, 3)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("wb") as fh:
        np.savez_compressed(fh, **arrays)
    tmp.replace(path)


def load_state(path: Path) -> v3.History:
    with np.load(path, allow_pickle=False) as data:
        meta = json.loads(str(data["metadata"]))
        if meta["schema"] != SCHEMA:
            raise ValueError("unrecognized state schema")
        if (not isinstance(meta.get("source_order"), list)
                or len(meta["source_order"]) != len(meta["source_hashes"])
                or set(meta["source_order"]) != set(meta["source_hashes"])):
            raise ValueError("state source order missing or invalid; rebuild the snapshot")
        stored_numpy = meta.get("numpy_version")
        if stored_numpy is not None and str(stored_numpy).split(".")[0] != np.__version__.split(".")[0]:
            raise ValueError(f"state numpy major version {stored_numpy} differs from running "
                             f"numpy {np.__version__}; rebuild the snapshot with the running numpy")
        names, blocks = v3.feature_layout()
        if meta["feature_names"] != names or meta["tp_names"] != blocks["T"] + blocks["P"]:
            raise ValueError("feature layout changed since snapshot")
        if meta["code_version"] != CODE_VERSION:
            raise ValueError("code version changed since snapshot")
        state = v3.History(**meta["builder_params"])
        for prefix, width in (("team", len(v3.TEAM_METRICS)), ("player", len(v3.PLAYER_METRICS)),
                              ("hero", len(v3.HERO_METRICS)), ("league_stats", 3)):
            setattr(state, prefix, _DecayedMap(*(data[prefix + suffix] for suffix in
                      ("_ids", "_sums", "_counts", "_last_time", "_last_event")), width))
        for prefix, default in (("attack", 0.0), ("defense", 0.0), ("elo", 1500.0),
                                ("player_attack", 0.0)):
            flags = data[prefix + "_f32"] if prefix + "_f32" in data else None
            setattr(state, prefix, _ScalarMap(data[prefix + "_ids"], data[prefix + "_values"],
                                             default, flags))
        for name in ("team_pop", "player_pop", "hero_pop", "global_total"):
            obj = getattr(state, name)
            obj.sums = data[name + "_sums"]
            obj.counts = data[name + "_counts"]
            obj.last_time, obj.last_event = map(int, data[name + "_times"])
        state.leagues = _QueueMap(data["leagues_ids"], data["leagues_offsets"],
                                  data["leagues_ends"], data["leagues_values"])
        state.serving_pending = {key: data["pending_" + key] for key in
                                 ("end", "final", "duration", "teams", "timeline", "win",
                                  "league", "accounts", "heroes", "pmetrics")}
        state.serving_pending_at = int(data["pending_at"])
        state.serving_last_query_start = int(data["last_query_start"])
        state.serving_meta = meta
    return state


def features_for_map(state: v3.History, radiant_team_id: int, dire_team_id: int,
                     radiant_accounts, dire_accounts, start_ts: int, *,
                     strict: bool = False) -> tuple[list[str], np.ndarray]:
    min_id, max_id = np.iinfo(np.int64).min, np.iinfo(np.int64).max
    if (not isinstance(start_ts, (int, np.integer)) or isinstance(start_ts, (bool, np.bool_))
            or not state.serving_meta["cutoff"] < start_ts < 4_102_444_800):
        raise ValueError("query start must be an integer after cutoff and before 2100")
    if any(not isinstance(team, (int, np.integer)) or isinstance(team, (bool, np.bool_))
           or not min_id <= team <= max_id for team in (radiant_team_id, dire_team_id)):
        raise ValueError("team IDs must be int64 integers")
    try:
        radiant_accounts, dire_accounts = list(radiant_accounts), list(dire_accounts)
        if len(radiant_accounts) != 5 or len(dire_accounts) != 5:
            raise ValueError("five accounts per side required")
        radiant_accounts = [int(account) for account in radiant_accounts]
        dire_accounts = [int(account) for account in dire_accounts]
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("five integer-convertible accounts per side required") from exc
    if any(not 0 <= account <= max_id for account in radiant_accounts + dire_accounts):
        raise ValueError("account IDs must be nonnegative int64 integers")
    pending = state.serving_pending
    visible = min(int(start_ts) - state.serving_meta["visibility_delay"], state.serving_meta["cutoff"])
    if strict and state.serving_pending_at > 0 and int(pending["end"][state.serving_pending_at - 1]) >= visible:
        raise ValueError("delayed snapshot queries must be in nondecreasing start order")
    while state.serving_pending_at < len(pending["end"]) and int(pending["end"][state.serving_pending_at]) < visible:
        state.apply(pending, state.serving_pending_at)
        state.serving_pending_at += 1
    last_end = int(pending["end"][state.serving_pending_at - 1]) if state.serving_pending_at else None
    state.serving_last_overvisible_seconds = max(0, last_end - visible + 1) if last_end is not None else 0
    state.serving_out_of_order_queries = (getattr(state, "serving_out_of_order_queries", 0)
                                          + int(start_ts < state.serving_last_query_start))
    state.serving_last_query_start = int(start_ts)
    query = {"start": int(start_ts), "league": 0, "stype": 0, "series_number": 0,
             "teams": np.asarray([radiant_team_id, dire_team_id], np.int64),
             "accounts": np.asarray(radiant_accounts + dire_accounts, np.int64),
             "heroes": np.zeros(10, np.int64)}
    x = state.query_features(query)
    blocks = state.feature_blocks
    lo, hi = len(blocks["G"]), len(blocks["G"]) + len(blocks["T"]) + len(blocks["P"])
    result = x[0, lo:hi].copy()
    assert result.dtype == np.float32 and len(result) == 152
    return state.serving_meta["tp_names"], result


def parity(state: v3.History, dataset: Path, n: int = 2000) -> tuple[float, int, int]:
    if n <= 0:
        raise ValueError("n must be positive")
    meta = json.loads((Path(dataset).parent / "metadata.json").read_text())
    params = dict(meta["parameters"])
    sm = state.serving_meta
    if "history_start" not in params or "visibility_delay" not in params:
        if meta.get("causality") != "history event applied only if end < query start; series game number counts starts < query start":
            raise ValueError("dataset omits history/visibility parameters without known legacy contract")
        params.setdefault("history_start", v3.HISTORY_START)
        params.setdefault("visibility_delay", 0)
        warnings.warn("legacy dataset omits history_start and visibility_delay; assuming original defaults, then checking numeric parity", stacklevel=2)
    for key, expected in (("history_cutoff", sm["cutoff"]),
                          ("history_start", sm["history_start"]),
                          ("visibility_delay", sm["visibility_delay"])):
        if params[key] != expected:
            raise ValueError(f"dataset {key} differs from state")
    for key, expected in sm["builder_params"].items():
        if params[key] != expected:
            raise ValueError(f"dataset {key} differs from state")
    # A retained immutable source may be mounted at a different path. Preserve
    # source priority by comparing hashes in the builder's input order.
    if list(meta["source_hashes"].values()) != [sm["source_hashes"][path] for path in sm["source_order"]]:
        raise ValueError("dataset source hashes differ from state")
    for path, expected in sm["source_hashes"].items():
        if v3.sha256(Path(path)) != expected:
            raise ValueError(f"source changed since snapshot: {path}")
    events, _ = v3.load_events(Path(sm["rich_path"]), [Path(p) for p in sm["db_paths"]], sm["history_start"])
    for path, expected in sm["source_hashes"].items():
        if v3.sha256(Path(path)) != expected:
            raise ValueError(f"source changed during parity loading: {path}")
    order = np.argsort(events["mid"])
    sorted_mids = events["mid"][order]
    with np.load(dataset, allow_pickle=False) as ds:
        if ds["feature_names"].tolist() != sm["feature_names"]:
            raise ValueError("dataset feature layout differs")
        selected = np.flatnonzero(ds["starts"] > sm["cutoff"])[:n]
        if not len(selected):
            raise ValueError("dataset has no maps after state cutoff")
        mids = ds["mids"][selected]
        positions = np.searchsorted(sorted_mids, mids)
        if np.any(positions >= len(sorted_mids)) or np.any(sorted_mids[positions] != mids):
            raise ValueError("dataset map absent from original source union")
        indices = order[positions]
        x = ds["X"][selected, 0]
        names = sm["tp_names"]
        columns = [sm["feature_names"].index(name) for name in names]
        expected = x[:, columns]
        got = np.empty_like(expected)
        for row, i in enumerate(indices):
            teams, accounts = events["teams"][i], events["accounts"][i]
            _, got[row] = features_for_map(state, int(teams[0]), int(teams[1]),
                                            accounts[:5], accounts[5:], int(events["start"][i]),
                                            strict=True)
    mismatch = int(np.count_nonzero(np.isnan(got) != np.isnan(expected)))
    if np.isinf(got).any() or np.isinf(expected).any():
        raise ValueError("nonfinite infinity in parity features")
    finite = np.isfinite(got) & np.isfinite(expected)
    max_diff = float(np.max(np.abs(got[finite] - expected[finite]))) if finite.any() else 0.0
    return max_diff, mismatch, len(selected)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("build-state")
    build.add_argument("--rich", type=Path, default=v3.RICH)
    build.add_argument("--db", type=Path, action="append")
    build.add_argument("--cutoff", type=int, required=True)
    build.add_argument("--history-start", type=int, required=True)
    build.add_argument("--visibility-delay", type=int, default=0)
    build.add_argument("--half-life-days", type=float, default=90)
    build.add_argument("--pseudo-games", type=float, default=5)
    build.add_argument("--poisson-lr", type=float, default=0.03)
    build.add_argument("--elo-k", type=float, default=20)
    build.add_argument("--output", type=Path, required=True)
    check = sub.add_parser("parity")
    check.add_argument("--state", type=Path, required=True)
    check.add_argument("--dataset", type=Path, required=True)
    check.add_argument("--n", type=int, default=2000)
    args = parser.parse_args()
    if args.command == "build-state":
        state = build_state(args.rich, args.db if args.db is not None else [v3.OLD_DB, v3.NEW_DB],
                            history_start=args.history_start, cutoff=args.cutoff,
                            visibility_delay=args.visibility_delay, half_life_days=args.half_life_days,
                            pseudo_games=args.pseudo_games, poisson_lr=args.poisson_lr, elo_k=args.elo_k)
        save_state(state, args.output)
        loaded = load_state(args.output)
        print(json.dumps({"events_replayed": loaded.serving_meta["events_replayed"],
                          "file_bytes": args.output.stat().st_size, "rss_after_load_bytes": _rss_bytes()}))
    else:
        state = load_state(args.state)
        max_diff, nan_mismatches, rows = parity(state, args.dataset, args.n)
        print(json.dumps({"rows": rows, "max_abs_diff": max_diff, "nan_pattern_mismatches": nan_mismatches}))
        if max_diff != 0 or nan_mismatches:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
