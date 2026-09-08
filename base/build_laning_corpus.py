#!/usr/bin/env python3
"""Build a compact, chronological laning corpus from public match JSON shards."""
from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import multiprocessing
import os
import tempfile
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

import numpy as np

from base.build_draft_phase_corpus import (
    SourceBuildError,
    _canonical_heroes,
    _strict_int,
    iter_json_objects,
)

SCHEMA_VERSION = 1
MIN_DURATION_SECONDS = 600
ROWS_NAME = "rows.npz"
SKIP_FILENAMES = frozenset({"merge_patch_summary.json", "scan_manifest.json"})
LANE_CODES = {
    "DIRE_STOMP": 0,
    "DIRE_VICTORY": 1,
    "TIE": 2,
    "RADIANT_VICTORY": 3,
    "RADIANT_STOMP": 4,
}
ARRAY_KEYS = ("mid", "ts", "duration", "heroes", "accounts", "lane_labels", "team_nw10")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _source_fingerprint(path: Path, root: Path) -> dict[str, Any]:
    stat = path.stat()
    return {"relative_path": str(path.relative_to(root)), "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns, "source_sha256": _sha256(path),
            "schema": SCHEMA_VERSION, "code_sha256": _rules_fingerprint()}


def _rules_fingerprint() -> str:
    source = repr((SCHEMA_VERSION, MIN_DURATION_SECONDS, LANE_CODES)) + "\n" + "\n".join(
        inspect.getsource(func) for func in (
            canonicalize_match, _lane_code, _parse_source,
            _canonical_heroes, _strict_int, iter_json_objects,
        )
    )
    return hashlib.sha256(source.encode()).hexdigest()


def _lane_code(value: Any) -> int:
    if not isinstance(value, str):
        return -1
    return LANE_CODES.get(value.upper(), -1)


def canonicalize_match(raw: Any, object_key: Any = None) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(raw, Mapping):
        return None, "not_object"
    mid = _strict_int(raw.get("id", object_key))
    ts = _strict_int(raw.get("startDateTime"))
    duration = _strict_int(raw.get("durationSeconds"))
    if mid is None or mid <= 0:
        return None, "invalid_map_id"
    if ts is None or ts <= 0:
        return None, "invalid_start_time"
    if duration is None or duration < MIN_DURATION_SECONDS or duration > np.iinfo(np.int32).max:
        return None, "invalid_duration_seconds"
    heroes, reason = _canonical_heroes(raw.get("players"))
    if heroes is None:
        return None, reason
    players = raw["players"]
    ordered = sorted(players, key=lambda p: (not p["isRadiant"], _strict_int(p["position"].removeprefix("POSITION_"))))
    def account_id(player: Mapping[str, Any]) -> int:
        steam_account = player.get("steamAccount")
        if not isinstance(steam_account, Mapping):
            return 0
        account = _strict_int(steam_account.get("id"))
        if account is None or account <= 0 or account == 4294967295:
            return 0
        return account

    accounts = tuple(account_id(player) for player in ordered)
    positive_accounts = [account for account in accounts if account > 0]
    if len(positive_accounts) != len(set(positive_accounts)):
        return None, "duplicate_account_id"
    leads = raw.get("radiantNetworthLeads")
    nw10 = np.nan
    nw_reason = None
    if not isinstance(leads, list) or len(leads) <= 10:
        nw_reason = "missing_team_nw10"
    else:
        candidate = leads[10]
        if isinstance(candidate, bool) or not isinstance(candidate, (int, float)) or not np.isfinite(float(candidate)):
            nw_reason = "invalid_team_nw10"
        else:
            nw10 = float(candidate)
    lane_labels = tuple(_lane_code(raw.get(name)) for name in ("bottomLaneOutcome", "midLaneOutcome", "topLaneOutcome"))
    return {"mid": mid, "ts": ts, "duration": duration, "heroes": heroes,
            "accounts": accounts, "lane_labels": lane_labels, "team_nw10": nw10,
            "_nw_reason": nw_reason}, None


def _empty_arrays() -> dict[str, np.ndarray]:
    return {"mid": np.empty(0, np.int64), "ts": np.empty(0, np.int64),
            "duration": np.empty(0, np.int32), "heroes": np.empty((0, 10), np.int32),
            "accounts": np.empty((0, 10), np.int64), "lane_labels": np.empty((0, 3), np.int8),
            "team_nw10": np.empty(0, np.float64)}


def _parse_source(path: Path) -> tuple[dict[str, np.ndarray], dict[str, int]]:
    rows, counts = [], Counter()
    for key, raw in iter_json_objects(path):
        row, reason = canonicalize_match(raw, key)
        if row is None:
            counts[reason or "invalid"] += 1
        else:
            if row.get("_nw_reason"):
                counts[row["_nw_reason"]] += 1
            rows.append(row)
    if not rows:
        return _empty_arrays(), {"accepted_before_global_dedup": 0, **dict(counts)}
    arrays = {key: np.asarray([row[key] for row in rows], dtype=dtype) for key, dtype in
              (("mid", np.int64), ("ts", np.int64), ("duration", np.int32), ("heroes", np.int32),
               ("accounts", np.int64), ("lane_labels", np.int8), ("team_nw10", np.float64))}
    arrays["heroes"] = arrays["heroes"].reshape(-1, 10)
    arrays["accounts"] = arrays["accounts"].reshape(-1, 10)
    arrays["lane_labels"] = arrays["lane_labels"].reshape(-1, 3)
    counts["accepted_before_global_dedup"] = len(rows)
    return arrays, dict(sorted(counts.items()))


def _atomic_npz(path: Path, **arrays: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".tmp", delete=False) as handle:
        temporary = Path(handle.name)
        try:
            np.savez_compressed(handle, **arrays); handle.flush(); os.fsync(handle.fileno())
        except BaseException:
            raise
    os.replace(temporary, path)


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, suffix=".tmp", delete=False) as handle:
        temporary = Path(handle.name)
        try:
            json.dump(value, handle, sort_keys=True, indent=2); handle.write("\n"); handle.flush(); os.fsync(handle.fileno())
        except BaseException:
            raise
    os.replace(temporary, path)


def _cache_path(cache_dir: Path, fingerprint: Mapping[str, Any]) -> Path:
    token = hashlib.sha256(json.dumps(fingerprint, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return cache_dir / f"{token}.npz"


def _load_or_build(path: Path, root: Path, cache_dir: Path) -> tuple[dict[str, np.ndarray], dict[str, Any]]:
    fingerprint = _source_fingerprint(path, root)
    cache = _cache_path(cache_dir, fingerprint)
    if cache.exists():
        try:
            with np.load(cache, allow_pickle=False) as archive:
                if json.loads(str(archive["metadata_json"].item())) == fingerprint:
                    arrays = {key: archive[key].copy() for key in ARRAY_KEYS}
                    counts = json.loads(str(archive["counts_json"].item()))
                    return arrays, {"source": fingerprint, "cache": "hit", "counts": counts}
        except (OSError, ValueError, KeyError, json.JSONDecodeError):
            pass
    arrays, counts = _parse_source(path)
    if _source_fingerprint(path, root) != fingerprint:
        raise SourceBuildError(f"source changed during parse: {path}")
    _atomic_npz(cache, **arrays, metadata_json=np.asarray(json.dumps(fingerprint, sort_keys=True)),
                counts_json=np.asarray(json.dumps(counts, sort_keys=True)))
    return arrays, {"source": fingerprint, "cache": "rebuilt", "counts": counts}


def _consolidate(parts: list[dict[str, np.ndarray]]) -> tuple[dict[str, np.ndarray], dict[str, int]]:
    if not parts:
        return _empty_arrays(), {"duplicate_identical": 0, "duplicate_conflict": 0}
    all_rows = {key: np.concatenate([part[key] for part in parts]) for key in ARRAY_KEYS}
    order = np.argsort(all_rows["mid"], kind="stable")
    all_rows = {key: value[order] for key, value in all_rows.items()}
    mid = all_rows["mid"]
    if len(mid) == 0:
        return all_rows, {"duplicate_identical": 0, "duplicate_conflict": 0}
    bad_ids: set[int] = set(); identical = 0
    i = 0
    while i < len(mid):
        j = i + 1
        while j < len(mid) and mid[j] == mid[i]: j += 1
        if j - i > 1:
            identical_row = all(
                np.array_equal(
                    all_rows[key][i:j],
                    np.broadcast_to(all_rows[key][i], all_rows[key][i:j].shape),
                    equal_nan=True,
                )
                for key in ARRAY_KEYS if key != "mid"
            )
            if identical_row: identical += j - i - 1
            else: bad_ids.add(int(mid[i]))
        i = j
    keep = np.array([int(value) not in bad_ids for value in mid], dtype=bool)
    first = np.r_[True, mid[1:] != mid[:-1]]
    keep &= first
    result = {key: value[keep] for key, value in all_rows.items()}
    chronological = np.lexsort((result["mid"], result["ts"]))
    return {key: value[chronological] for key, value in result.items()}, {"duplicate_identical": identical, "duplicate_conflict": len(bad_ids)}


def _source_paths(source: Path) -> tuple[list[Path], list[str]]:
    if source.is_file(): return ([source] if source.name not in SKIP_FILENAMES else []), ([source.name] if source.name in SKIP_FILENAMES else [])
    if not source.is_dir(): raise SourceBuildError(f"source does not exist: {source}")
    paths = sorted(source.glob("*.json")); return [p for p in paths if p.name not in SKIP_FILENAMES], [p.name for p in paths if p.name in SKIP_FILENAMES]


def build_corpus(source: Path | str, output_dir: Path | str, workers: int = 2) -> dict[str, Any]:
    if workers < 1: raise ValueError("workers must be >= 1")
    source, output = Path(source).resolve(), Path(output_dir).resolve(); paths, skipped = _source_paths(source)
    if not paths: raise SourceBuildError("no raw JSON sources found")
    root = source if source.is_dir() else source.parent; cache_dir = output / "cache"
    completed: list[tuple[dict[str, np.ndarray], dict[str, Any]] | None] = [None] * len(paths)
    if workers == 1:
        for index, path in enumerate(paths):
            completed[index] = _load_or_build(path, root, cache_dir)
            print(f"[{index + 1}/{len(paths)}] {path.name}: {completed[index][1]['cache']}", flush=True)
    else:
        with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn")) as executor:
            futures = {executor.submit(_load_or_build, path, root, cache_dir): index for index, path in enumerate(paths)}
            for progress, future in enumerate(as_completed(futures), 1):
                index = futures[future]
                completed[index] = future.result()
                print(f"[{progress}/{len(paths)}] {paths[index].name}: {completed[index][1]['cache']}", flush=True)
    ready = [item for item in completed if item is not None]
    arrays, counts = _consolidate([item[0] for item in ready])
    manifest = {"complete": True, "schema_version": SCHEMA_VERSION, "rows_file": ROWS_NAME, "rows": len(arrays["mid"]),
                "sources": [item[1]["source"] | {"cache": item[1]["cache"], "counts": item[1]["counts"]} for item in ready],
                "skipped_metadata": skipped, "global_counts": counts}
    _atomic_npz(output / ROWS_NAME, **arrays); _atomic_json(output / "manifest.json", manifest)
    return manifest


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--source", type=Path, required=True); parser.add_argument("--output-dir", type=Path, required=True); parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args(argv); manifest = build_corpus(args.source, args.output_dir, args.workers); print(json.dumps({"rows": manifest["rows"], "output": str(args.output_dir / ROWS_NAME)})); return 0


if __name__ == "__main__": raise SystemExit(main())
