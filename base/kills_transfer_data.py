"""Extract immutable, missing-aware public/pro rows for offline kills transfer.

No current-match outcome is a prediction feature. These rows are observations
for building earlier history and labels. Run with the project interpreter.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import time

import ijson
import numpy as np

from kills_relative_profiles import parse_match

ROOT = Path(__file__).resolve().parents[1]
STAT_NAMES = ("kills", "deaths", "assists", "networth", "xp", "gpm")


def timestamp(value):
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp())


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    os.replace(temp, path)


def atomic_npz(path, **arrays):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("wb") as stream:
        np.savez_compressed(stream, **arrays)
    os.replace(temp, path)


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _team(match, side):
    team = match.get(side + "Team") or {}
    return int(match.get(side + "TeamId") or (team.get("id") if isinstance(team, dict) else 0) or 0)


def _arrays(records):
    keys = ("mids", "ts", "ends", "durations", "heroes", "accounts", "teams", "sids", "stats")
    dtypes = (np.int64, np.int64, np.int64, np.int32, np.int32, np.int64, np.int64, np.int64, np.float32)
    return {key: np.asarray([row[i] for row in records], dtype=dtype)
            for i, (key, dtype) in enumerate(zip(keys, dtypes))}


def extract(args):
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    lower, upper = timestamp(args.start), timestamp(args.before)
    source = ROOT / ("bets_data/analise_pub_matches/json_parts_split_from_object"
                     if args.corpus == "public" else "pro_heroes_data/json_parts_split_from_object")
    paths = sorted(source.glob("*_part*.json"))
    if args.max_files:
        paths = paths[:args.max_files]
    totals = Counter()
    chunks, source_manifest = [], []
    begin = time.monotonic()
    code_hash = sha256(__file__) + sha256(ROOT / "base/kills_relative_profiles.py")
    for index, path in enumerate(paths):
        stamp = path.stat()
        fingerprint = sha256(path)
        source_manifest.append({"path": str(path.relative_to(ROOT)), "sha256": fingerprint})
        checkpoint = out / (path.stem + ".json")
        signature = {"source_sha256": fingerprint, "code": code_hash, "start": lower, "before": upper}
        if checkpoint.exists():
            saved = json.loads(checkpoint.read_text())
            if saved["signature"] != signature:
                raise ValueError(f"Input changed: {checkpoint}; use a new extraction directory")
            for chunk in saved["chunks"]:
                if sha256(out / chunk["path"]) != chunk["sha256"]:
                    raise ValueError(f"Corrupt checkpoint chunk: {chunk['path']}")
            chunks.extend(saved["chunks"])
            totals.update(saved["counts"])
            continue
        records, file_chunks, counts = [], [], Counter()

        def flush():
            if not records:
                return
            dest = out / f"{path.stem}.{len(file_chunks):03d}.npz"
            atomic_npz(dest, **_arrays(records))
            file_chunks.append({"path": dest.name, "rows": len(records), "sha256": sha256(dest)})
            records.clear()

        with path.open("rb") as stream:
            for key, match in ijson.kvitems(stream, "", use_float=True):
                counts["seen"] += 1
                if not isinstance(match, dict):
                    counts["invalid"] += 1
                    continue
                if not isinstance(match.get("didRadiantWin"), bool):
                    counts["unfinished"] += 1
                    continue
                start = match.get("startDateTime")
                if not isinstance(start, (int, float)) or not lower <= start < upper:
                    counts["outside_dates"] += 1
                    continue
                if not match.get("id"):
                    match["id"] = key
                row = parse_match(match)
                if row is None:
                    counts["invalid"] += 1
                    continue
                if row["end"] >= upper:
                    counts["end_embargo"] += 1
                    continue
                values = np.stack([row["stats"][name] for name in STAT_NAMES], axis=1)
                # Missing K is an unknown label, never a low-kill game.
                if not np.isfinite(values[:, 0]).all():
                    counts["missing_label"] += 1
                    continue
                counts["missing_stat_cells"] += int(np.isnan(values).sum())
                series = match.get("series") or {}
                series_id = int(series.get("id") or 0) if isinstance(series, dict) else 0
                records.append((row["id"], row["start"], row["end"], row["duration"],
                                row["heroes"], [int(x or 0) for x in row["account_ids"]],
                                [_team(match, "radiant"), _team(match, "dire")], series_id, values))
                counts["kept"] += 1
                if len(records) >= args.chunk_size:
                    flush()
                if args.max_rows and totals["kept"] + counts["kept"] >= args.max_rows:
                    break
        flush()
        if (path.stat().st_size, path.stat().st_mtime_ns) != (stamp.st_size, stamp.st_mtime_ns):
            raise ValueError(f"Source changed during extraction: {path}")
        atomic_json(checkpoint, {"signature": signature, "counts": dict(counts), "chunks": file_chunks})
        totals.update(counts)
        chunks.extend(file_chunks)
        print(f"{args.corpus} {index + 1}/{len(paths)} {path.name}: kept={counts['kept']} "
              f"total={totals['kept']} elapsed={time.monotonic()-begin:.0f}s", flush=True)
        if args.max_rows and totals["kept"] >= args.max_rows:
            break
    result = {"schema": "kills-transfer-rows-v1", "corpus": args.corpus,
              "start": lower, "before": upper, "counts": dict(totals),
              "stat_names": list(STAT_NAMES), "chunks": chunks, "sources": source_manifest,
              "code": code_hash, "bounded_pilot": bool(args.max_files or args.max_rows)}
    atomic_json(out / "summary.json", result)
    print(f"DONE {args.corpus}: {totals['kept']} rows", flush=True)


def load_rows(directory, start=0, before=np.inf):
    directory = Path(directory)
    manifest = json.loads((directory / "summary.json").read_text())
    if manifest["stat_names"] != list(STAT_NAMES):
        raise ValueError("Unexpected stat order")
    blocks = {}
    for chunk in manifest["chunks"]:
        path = directory / chunk["path"]
        if sha256(path) != chunk["sha256"]:
            raise ValueError(f"Corrupt input: {path}")
        with np.load(path) as z:
            keep = (z["ts"] >= start) & (z["ends"] < before)
            for key in z.files:
                blocks.setdefault(key, []).append(z[key][keep])
    if not blocks:
        raise ValueError("No extracted rows")
    data = {key: np.concatenate(values) for key, values in blocks.items()}
    _, first = np.unique(data["mids"], return_index=True)
    # Exact duplicate rows are one observation; conflicting duplicates fail.
    if len(first) != len(data["mids"]):
        order = np.argsort(data["mids"], kind="stable")
        a, b = order[:-1], order[1:]
        dup = data["mids"][a] == data["mids"][b]
        for key in data:
            if not np.array_equal(data[key][a[dup]], data[key][b[dup]], equal_nan=True):
                raise ValueError(f"Conflicting duplicate match rows: {key}")
    order = first[np.argsort(data["ts"][first], kind="stable")]
    return {key: values[order] for key, values in data.items()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", choices=("public", "pro"), required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--start", default="2026-01-01")
    parser.add_argument("--before", default="2026-09-12")
    parser.add_argument("--chunk-size", type=int, default=25000)
    parser.add_argument("--max-files", type=int, default=0)
    parser.add_argument("--max-rows", type=int, default=0)
    extract(parser.parse_args())
