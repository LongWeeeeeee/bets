#!/usr/bin/env python3
"""Build one chronological public-draft corpus for phase-model experiments.

The input is deliberately parsed as a stream: the public archive is much too
large to load as one JSON object.  Source shards are cached only after a fully
successful parse.  A cache key includes both the source identity and the exact
Early-NW labelling rule, so a changed threshold cannot reuse old labels.
"""
from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import multiprocessing
import os
import tempfile
import zipfile
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

import numpy as np

try:
    import ijson  # type: ignore
except ImportError:  # Small fixtures and minimal installations still work.
    ijson = None

try:
    from base import analise_database as production
except ImportError:  # Direct script execution.
    import analise_database as production  # type: ignore


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT_DIR / "bets_data/analise_pub_matches/json_parts_split_from_object"
DEFAULT_OUTPUT = ROOT_DIR / "data/draft_phase_corpus"
SCHEMA_VERSION = 2
MIN_DURATION_SECONDS = 20 * 60
MARKER_START_MINUTE = 20
MARKER_END_MINUTE = 28
SKIP_FILENAMES = frozenset({"merge_patch_summary.json", "scan_manifest.json"})
ROWS_NAME = "rows.npz"


class SourceBuildError(RuntimeError):
    """A source was unreadable or had an unsupported top-level JSON shape."""


@dataclass(frozen=True)
class Row:
    mid: int
    ts: int
    heroes: tuple[int, ...]
    duration: int
    win: int
    early_nw: int
    marker_minute: int


def _strict_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        converted = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return converted if str(converted) == str(value).strip() or isinstance(value, int) else None


def _finite_lead_window(leads: list[Any], start_minute: int, end_minute: int) -> bool:
    """The label is valid only when every claimed minute was observed."""
    if end_minute < start_minute or len(leads) < end_minute:
        return False
    for value in leads[start_minute - 1:end_minute]:
        if isinstance(value, bool):
            return False
        try:
            if not np.isfinite(float(value)):
                return False
        except (TypeError, ValueError, OverflowError):
            return False
    return True


def _canonical_heroes(players: Any) -> tuple[tuple[int, ...] | None, str | None]:
    if not isinstance(players, list) or len(players) != 10:
        return None, "invalid_player_count"
    sides: dict[bool, dict[int, int]] = {True: {}, False: {}}
    used: set[int] = set()
    for player in players:
        if not isinstance(player, Mapping):
            return None, "invalid_player"
        side = player.get("isRadiant")
        raw_position = player.get("position")
        if not isinstance(side, bool) or not isinstance(raw_position, str) or not raw_position.startswith("POSITION_"):
            return None, "invalid_position"
        position = _strict_int(raw_position.removeprefix("POSITION_"))
        hero = _strict_int(player.get("heroId"))
        if position not in range(1, 6):
            return None, "invalid_position"
        if hero is None or hero <= 0 or hero > np.iinfo(np.int32).max:
            return None, "invalid_hero_id"
        if position in sides[side]:
            return None, "duplicate_position"
        if hero in used:
            return None, "duplicate_hero"
        sides[side][position] = hero
        used.add(hero)
    if any(set(sides[side]) != set(range(1, 6)) for side in (True, False)):
        return None, "incomplete_positions"
    return tuple(sides[side][position] for side in (True, False) for position in range(1, 6)), None


def canonicalize_match(raw: Any, object_key: Any = None) -> tuple[Row | None, str | None]:
    """Validate one public record and label its Early-NW phase without gates."""
    if not isinstance(raw, Mapping):
        return None, "not_object"
    mid = _strict_int(raw.get("id", object_key))
    ts = _strict_int(raw.get("startDateTime"))
    duration = _strict_int(raw.get("durationSeconds"))
    winner = raw.get("didRadiantWin")
    if mid is None or mid <= 0:
        return None, "invalid_map_id"
    if ts is None or ts <= 0:
        return None, "invalid_start_time"
    if duration is None or duration < MIN_DURATION_SECONDS or duration > np.iinfo(np.int32).max:
        return None, "invalid_duration_seconds"
    if not isinstance(winner, bool):
        return None, "invalid_outcome"
    heroes, reason = _canonical_heroes(raw.get("players"))
    if heroes is None:
        return None, reason

    leads = raw.get("radiantNetworthLeads")
    early_nw, marker_minute = -1, -1
    if isinstance(leads, list):
        observed_end = min(MARKER_END_MINUTE, duration // 60)
        # A corrupt row sometimes carries leads beyond actual map duration.
        # Production's helper is therefore deliberately given only observed
        # minutes; it remains the source of truth for thresholds and ordering.
        observed_leads = leads[:observed_end]
        # This is the production threshold function itself.  Do not call
        # is_early_nw_match: it has optional fast-finish and minute-10 gates.
        dominator, minute = production._first_dynamic_threshold_reach(
            raw, observed_leads, MARKER_START_MINUTE, MARKER_END_MINUTE
        )
        if dominator == "radiant" and _finite_lead_window(leads, MARKER_START_MINUTE, int(minute)):
            early_nw, marker_minute = 1, int(minute)
        elif dominator == "dire" and _finite_lead_window(leads, MARKER_START_MINUTE, int(minute)):
            early_nw, marker_minute = 0, int(minute)
        elif _finite_lead_window(leads, MARKER_START_MINUTE, observed_end):
            early_nw = 2
    return Row(mid, ts, heroes, duration, int(winner), early_nw, marker_minute), None


def iter_json_objects(path: Path) -> Iterator[tuple[Any, Any]]:
    """Yield root dict values or list elements without materialising raw data."""
    if ijson is None:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, Mapping):
            yield from payload.items()
        elif isinstance(payload, list):
            yield from enumerate(payload)
        else:
            raise SourceBuildError("top-level JSON must be an object or list")
        return
    with path.open("rb") as handle:
        try:
            event = next(ijson.parse(handle))
        except StopIteration as exc:
            raise SourceBuildError("empty JSON") from exc
    prefix, kind, _value = event
    if prefix or kind not in {"start_map", "start_array"}:
        raise SourceBuildError("top-level JSON must be an object or list")
    with path.open("rb") as handle:
        if kind == "start_map":
            yield from ijson.kvitems(handle, "")
        else:
            yield from enumerate(ijson.items(handle, "item"))


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def rule_fingerprint() -> str:
    threshold_path = Path(production.EARLY_DOMINATOR_THRESHOLDS_PATH)
    try:
        threshold_bytes = threshold_path.read_bytes()
    except OSError as exc:
        raise SourceBuildError(f"cannot fingerprint threshold file {threshold_path}: {exc}") from exc
    config = {
        "schema": SCHEMA_VERSION,
        "min_duration_seconds": MIN_DURATION_SECONDS,
        "marker_window": [MARKER_START_MINUTE, MARKER_END_MINUTE],
        "threshold_sha256": _sha256_bytes(threshold_bytes),
        "threshold_function_sha256": _sha256_bytes(inspect.getsource(production._first_dynamic_threshold_reach).encode()),
        "threshold_selector_sha256": _sha256_bytes(inspect.getsource(production._early_threshold_for).encode()),
        "canonicalizer_sha256": _sha256_bytes(inspect.getsource(canonicalize_match).encode()),
        "finite_window_sha256": _sha256_bytes(inspect.getsource(_finite_lead_window).encode()),
    }
    return _sha256_bytes(json.dumps(config, sort_keys=True, separators=(",", ":")).encode())


def _source_stat(path: Path, source_root: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "relative_path": str(path.relative_to(source_root)),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def source_fingerprint(path: Path, source_root: Path, rules: str) -> dict[str, Any]:
    return {
        "schema": SCHEMA_VERSION,
        **_source_stat(path, source_root),
        "source_sha256": _file_sha256(path),
        "rules": rules,
    }


def _atomic_npz(path: Path, **arrays: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".tmp", delete=False) as handle:
        temporary = Path(handle.name)
        try:
            np.savez_compressed(handle, **arrays)
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    os.replace(temporary, path)


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, suffix=".tmp", delete=False) as handle:
        temporary = Path(handle.name)
        try:
            json.dump(value, handle, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    os.replace(temporary, path)


def _cache_path(cache_dir: Path, fingerprint: Mapping[str, Any]) -> Path:
    token = _sha256_bytes(json.dumps(fingerprint, sort_keys=True, separators=(",", ":")).encode())
    return cache_dir / f"{token}.npz"


def _rows_to_arrays(rows: Sequence[Row]) -> dict[str, np.ndarray]:
    return {
        "heroes": np.asarray([row.heroes for row in rows], dtype=np.int32).reshape((-1, 10)),
        "mid": np.asarray([row.mid for row in rows], dtype=np.int64),
        "ts": np.asarray([row.ts for row in rows], dtype=np.int64),
        "duration": np.asarray([row.duration for row in rows], dtype=np.int32),
        "wins": np.asarray([row.win for row in rows], dtype=np.int8),
        "early_nw": np.asarray([row.early_nw for row in rows], dtype=np.int8),
        "marker_minute": np.asarray([row.marker_minute for row in rows], dtype=np.int8),
    }


ARRAY_KEYS = ("heroes", "mid", "ts", "duration", "wins", "early_nw", "marker_minute")


def _validate_arrays(arrays: Mapping[str, np.ndarray]) -> dict[str, np.ndarray]:
    lengths = {len(arrays[key]) for key in ("mid", "ts", "duration", "wins", "early_nw", "marker_minute")}
    heroes = arrays["heroes"]
    if len(lengths) != 1 or heroes.ndim != 2 or heroes.shape[1:] != (10,) or len(heroes) != next(iter(lengths)):
        raise SourceBuildError("corrupt cache array shapes")
    if heroes.dtype != np.int32:
        raise SourceBuildError("corrupt cache hero dtype")
    return {key: arrays[key] for key in ARRAY_KEYS}


def _load_cache(path: Path, fingerprint: Mapping[str, Any]) -> tuple[dict[str, np.ndarray], dict[str, int]] | None:
    if not path.exists():
        return None
    try:
        with np.load(path, allow_pickle=False) as archive:
            metadata = json.loads(str(archive["metadata_json"].item()))
            if metadata != dict(fingerprint):
                return None
            arrays = {key: archive[key].copy() for key in ARRAY_KEYS}
            counts = json.loads(str(archive["counts_json"].item()))
        arrays = _validate_arrays(arrays)
        normalized_counts = {str(key): int(value) for key, value in counts.items()}
        if normalized_counts.get("accepted_before_global_dedup") != len(arrays["mid"]):
            return None
        return arrays, normalized_counts
    except (OSError, KeyError, ValueError, TypeError, json.JSONDecodeError, zipfile.BadZipFile):
        return None


def _parse_source(path: Path) -> tuple[dict[str, np.ndarray], dict[str, int]]:
    mids: list[int] = []
    timestamps: list[int] = []
    heroes: list[tuple[int, ...]] = []
    durations: list[int] = []
    wins: list[int] = []
    early_nw: list[int] = []
    marker_minutes: list[int] = []
    counts: Counter[str] = Counter()
    for key, raw in iter_json_objects(path):
        row, reason = canonicalize_match(raw, key)
        if row is None:
            counts[reason or "invalid"] += 1
        else:
            mids.append(row.mid); timestamps.append(row.ts); heroes.append(row.heroes)
            durations.append(row.duration); wins.append(row.win); early_nw.append(row.early_nw)
            marker_minutes.append(row.marker_minute)
    counts["accepted_before_global_dedup"] = len(mids)
    arrays = {
        "heroes": np.asarray(heroes, dtype=np.int32).reshape((-1, 10)),
        "mid": np.asarray(mids, dtype=np.int64), "ts": np.asarray(timestamps, dtype=np.int64),
        "duration": np.asarray(durations, dtype=np.int32), "wins": np.asarray(wins, dtype=np.int8),
        "early_nw": np.asarray(early_nw, dtype=np.int8), "marker_minute": np.asarray(marker_minutes, dtype=np.int8),
    }
    return arrays, dict(sorted(counts.items()))


def _load_or_build_source(path: Path, source_root: Path, cache_dir: Path, rules: str) -> tuple[dict[str, np.ndarray], dict[str, Any]]:
    fingerprint = source_fingerprint(path, source_root, rules)
    cache = _cache_path(cache_dir, fingerprint)
    cached = _load_cache(cache, fingerprint)
    if cached is not None:
        rows, counts = cached
        return rows, {"source": fingerprint["relative_path"], "cache": "hit", "fingerprint": fingerprint, "counts": counts}
    arrays, counts = _parse_source(path)
    if _source_stat(path, source_root) != {key: fingerprint[key] for key in ("relative_path", "size", "mtime_ns")}:
        # Do not cache a partial/ambiguous read when an archive writer rotated
        # or appended this source while it was being streamed.
        raise SourceBuildError(f"source changed during parse; retry later: {path}")
    _atomic_npz(cache, **arrays, metadata_json=np.asarray(json.dumps(fingerprint, sort_keys=True)), counts_json=np.asarray(json.dumps(counts, sort_keys=True)))
    return arrays, {"source": fingerprint["relative_path"], "cache": "rebuilt", "fingerprint": fingerprint, "counts": counts}


def _consolidate(arrays_by_source: list[tuple[str, dict[str, np.ndarray]]]) -> tuple[dict[str, np.ndarray], Counter[str]]:
    """Deduplicate entirely in NumPy, then return chronological rows.

    Sorting by map id makes every duplicate contiguous.  A conflict in any
    adjacent pair rejects that whole id group, including A,B,A across shards.
    """
    counts: Counter[str] = Counter()
    if not arrays_by_source:
        return _rows_to_arrays([]), counts
    arrays = {key: np.concatenate([item[1][key] for item in arrays_by_source]) for key in ARRAY_KEYS}
    arrays_by_source.clear()  # Release shard arrays before the two global sorts.
    order = np.argsort(arrays["mid"], kind="stable")
    by_mid = {key: arrays[key][order] for key in ARRAY_KEYS}
    del arrays, order
    mid = by_mid["mid"]
    if len(mid) == 0:
        return by_mid, counts
    same_mid = np.empty(len(mid), dtype=bool)
    same_mid[0] = False
    same_mid[1:] = mid[1:] == mid[:-1]
    same_payload = same_mid.copy()
    for key in ("ts", "duration", "wins", "early_nw", "marker_minute"):
        same_payload[1:] &= by_mid[key][1:] == by_mid[key][:-1]
    for column in range(10):
        same_payload[1:] &= by_mid["heroes"][1:, column] == by_mid["heroes"][:-1, column]
    conflict_adjacent = same_mid & ~same_payload
    bad_mids = np.unique(mid[conflict_adjacent])
    bad = np.isin(mid, bad_mids, assume_unique=False)
    first_of_mid = ~same_mid
    keep = first_of_mid & ~bad
    counts["duplicate_identical"] = int(np.count_nonzero(same_mid & same_payload))
    counts["duplicate_conflict"] = int(len(bad_mids))
    counts["conflicting_map_ids_rejected"] = int(len(bad_mids))
    unique_rows = {key: by_mid[key][keep] for key in ARRAY_KEYS}
    del by_mid, same_mid, same_payload, conflict_adjacent, bad_mids, bad, first_of_mid, keep
    chronological = np.lexsort((unique_rows["mid"], unique_rows["ts"]))
    result = {key: unique_rows[key][chronological] for key in ARRAY_KEYS}
    return result, counts


def chronological_split_indices(ts: np.ndarray, fractions: tuple[int, int] = (60, 80)) -> tuple[int, int]:
    """Return 60/20/20 cut points without placing equal timestamps in two sets."""
    if ts.ndim != 1 or (len(ts) > 1 and np.any(ts[1:] < ts[:-1])):
        raise ValueError("timestamps must be one-dimensional and sorted")
    first, second = (len(ts) * fraction // 100 for fraction in fractions)
    while first < len(ts) and first > 0 and ts[first - 1] == ts[first]:
        first += 1
    while second < len(ts) and second > 0 and ts[second - 1] == ts[second]:
        second += 1
    second = max(second, first)
    return first, second


def _source_paths(source: Path) -> tuple[list[Path], list[str]]:
    if source.is_file():
        return ([source] if source.name not in SKIP_FILENAMES else []), ([source.name] if source.name in SKIP_FILENAMES else [])
    if not source.is_dir():
        raise SourceBuildError(f"source does not exist: {source}")
    paths, skipped = [], []
    for path in sorted(source.glob("*.json")):
        if path.name in SKIP_FILENAMES:
            skipped.append(path.name)
        else:
            paths.append(path)
    return paths, skipped


def build_corpus(source: Path | str = DEFAULT_SOURCE, output_dir: Path | str = DEFAULT_OUTPUT, workers: int = 2) -> dict[str, Any]:
    """Build rows.npz atomically and return its manifest.  `workers` is kept for CLI compatibility.

    Parsing remains deterministic and source-failure-safe: a bad source aborts
    before the consolidated file is replaced.  The cache makes a restart avoid
    reparsing already completed sources.
    """
    if workers < 1:
        raise ValueError("workers must be >= 1")
    source_path, destination = Path(source).resolve(), Path(output_dir).resolve()
    paths, skipped_metadata = _source_paths(source_path)
    if not paths:
        raise SourceBuildError("no raw JSON sources found")
    source_root = source_path if source_path.is_dir() else source_path.parent
    rules = rule_fingerprint()
    cache_dir = destination / "cache"
    source_results: list[tuple[str, dict[str, np.ndarray]]] = []
    source_manifest: list[dict[str, Any]] = []
    try:
        # CPU-bound canonicalization is isolated into spawned workers.  The
        # one-worker path stays direct for deterministic unit-test monkeypatches.
        completed: list[tuple[dict[str, np.ndarray], dict[str, Any]] | None] = [None] * len(paths)
        if workers == 1:
            for index, path in enumerate(paths):
                arrays, item = _load_or_build_source(path, source_root, cache_dir, rules)
                completed[index] = (arrays, item)
                accepted = item["counts"].get("accepted_before_global_dedup", 0)
                print(f"[{index + 1}/{len(paths)}] {path.name}: {item['cache']} raw_accepted={accepted}", flush=True)
        else:
            with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn")) as executor:
                futures = {
                    executor.submit(_load_or_build_source, path, source_root, cache_dir, rules): (index, path)
                    for index, path in enumerate(paths)
                }
                for future in as_completed(futures):
                    index, path = futures[future]
                    arrays, item = future.result()
                    completed[index] = (arrays, item)
                    accepted = item["counts"].get("accepted_before_global_dedup", 0)
                    print(f"[{index + 1}/{len(paths)}] {path.name}: {item['cache']} raw_accepted={accepted}", flush=True)
                futures.clear()
                del future
        for result in completed:
            assert result is not None
            arrays, item = result
            source_results.append((item["source"], arrays))
            source_manifest.append(item)
        completed.clear()
        del result, arrays, item
    except Exception as exc:
        failed = {"complete": False, "error": f"{type(exc).__name__}: {exc}", "sources": source_manifest, "skipped_metadata": skipped_metadata}
        _atomic_json(destination / "manifest.json", failed)
        raise SourceBuildError(f"source build failed; consolidated rows were not replaced: {exc}") from exc

    arrays, global_counts = _consolidate(source_results)
    cuts = chronological_split_indices(arrays["ts"])
    early_labels, early_counts = np.unique(arrays["early_nw"], return_counts=True)
    manifest = {
        "complete": True,
        "schema_version": SCHEMA_VERSION,
        "rule_fingerprint": rules,
        "rows_file": ROWS_NAME,
        "rows": len(arrays["mid"]),
        "first_ts": int(arrays["ts"][0]) if len(arrays["ts"]) else None,
        "last_ts": int(arrays["ts"][-1]) if len(arrays["ts"]) else None,
        "early_nw_class_counts": {str(int(label)): int(count) for label, count in zip(early_labels, early_counts)},
        "hero_dtype": str(arrays["heroes"].dtype),
        "global_counts": dict(sorted(global_counts.items())),
        "sources": source_manifest,
        "skipped_metadata": skipped_metadata,
        "chronological_split_indices": {"train_end": cuts[0], "validation_end": cuts[1]},
    }
    _atomic_npz(destination / ROWS_NAME, **arrays)
    _atomic_json(destination / "manifest.json", manifest)
    return manifest


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args(argv)
    manifest = build_corpus(args.source, args.output_dir, args.workers)
    print(json.dumps({"rows": manifest["rows"], "output": str(args.output_dir / ROWS_NAME)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
