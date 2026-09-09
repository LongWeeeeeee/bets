#!/usr/bin/env python3
"""Export a completed laning corpus into an immutable mmap history directory."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from base.laning_model import PLAYER_LANES


def _sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path, value):
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, sort_keys=True, indent=2)
        handle.write("\n")
    os.replace(temporary, path)


def _atomic_npy(path, array):
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("xb") as handle:
        np.save(handle, array, allow_pickle=False)
    os.replace(temporary, path)


def _load_corpus(corpus):
    corpus = Path(corpus)
    manifest_path, rows_path = corpus / "manifest.json", corpus / "rows.npz"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("invalid corpus manifest") from exc
    if not isinstance(manifest, dict) or manifest.get("complete") is not True:
        raise ValueError("corpus is incomplete")
    if manifest.get("rows_file", "rows.npz") != "rows.npz":
        raise ValueError("unsupported corpus rows file")
    try:
        with np.load(rows_path, allow_pickle=False) as archive:
            required = ("ts", "duration", "heroes", "accounts", "lane_labels")
            if any(name not in archive for name in required):
                raise ValueError("corpus rows are missing required arrays")
            arrays = {name: archive[name] for name in required}
    except (OSError, ValueError) as exc:
        raise ValueError("invalid corpus rows") from exc
    count = len(arrays["ts"])
    if (arrays["ts"].shape != (count,) or arrays["duration"].shape != (count,) or
            arrays["heroes"].shape != (count, 10) or arrays["accounts"].shape != (count, 10) or
            arrays["lane_labels"].shape != (count, 3) or manifest.get("rows") != count):
        raise ValueError("invalid corpus row shapes or count")
    try:
        ts = arrays["ts"].astype(np.int64, casting="safe", copy=False)
        duration = arrays["duration"].astype(np.int64, casting="safe", copy=False)
        heroes = arrays["heroes"].astype(np.int64, casting="safe", copy=False)
        accounts = arrays["accounts"].astype(np.int64, casting="safe", copy=False)
        labels = arrays["lane_labels"].astype(np.int8, casting="safe", copy=False)
    except TypeError as exc:
        raise ValueError("invalid corpus array dtypes") from exc
    if (np.any(ts <= 0) or np.any(duration < 0) or np.any(ts > np.iinfo(np.int64).max - duration) or
            np.any(heroes <= 0) or np.any(heroes >= 1024) or np.any(accounts < 0) or
            np.any((labels < -1) | (labels > 4))):
        raise ValueError("invalid corpus values")
    return {"ts": ts, "duration": duration, "heroes": heroes, "accounts": accounts,
            "lane_labels": labels}, {"manifest_sha256": _sha256(manifest_path),
                                        "rows_sha256": _sha256(rows_path)}


def _role_events(corpus, role):
    slots = (role, role + 5)
    accounts = corpus["accounts"][:, slots].reshape(-1)
    heroes = corpus["heroes"][:, slots].reshape(-1)
    labels = corpus["lane_labels"][:, PLAYER_LANES[list(slots)]].reshape(-1)
    scores = labels.astype(np.int8) - 2
    scores[1::2] *= -1  # Dire's lane label is converted to the player's own side.
    end_ts = np.repeat(corpus["ts"] + corpus["duration"], 2)
    valid = (accounts > 0) & (labels >= 0)
    accounts, end_ts = accounts[valid].astype(np.uint64), end_ts[valid].astype(np.int64)
    heroes, scores = heroes[valid].astype(np.uint16), scores[valid].astype(np.int8)
    order = np.lexsort((end_ts, accounts))
    accounts, end_ts, heroes, scores = (value[order] for value in (accounts, end_ts, heroes, scores))
    unique, starts = np.unique(accounts, return_index=True)
    offsets = np.empty(len(unique) + 1, dtype=np.int64)
    offsets[:-1], offsets[-1] = starts, len(accounts)
    return {"accounts": unique, "offsets": offsets, "end_ts": end_ts,
            "heroes": heroes, "scores": scores}


def export_laning_history(corpus, output_dir):
    """Stage a new export beside its final path, then publish it atomically."""
    corpus, output = Path(corpus), Path(output_dir)
    if output.exists():
        raise FileExistsError(f"laning history output already exists: {output}")
    arrays, source_fingerprint = _load_corpus(corpus)
    output.parent.mkdir(parents=True, exist_ok=True)
    # Do not reuse or remove interrupted staging directories: each retry gets a
    # fresh sibling, and readers can only ever observe the final complete tree.
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.staging.", dir=output.parent))
    initial = {"schema_version": 1, "complete": False, "source_fingerprint": source_fingerprint}
    _atomic_json(staging / "manifest.json", initial)
    roles = []
    output_files = {}
    max_end_ts = None
    for role in range(5):
        events = _role_events(arrays, role)
        for name, value in events.items():
            filename = f"role_{role + 1}_{name}.npy"
            path = staging / filename
            _atomic_npy(path, value)
            output_files[filename] = _sha256(path)
        role_max = int(events["end_ts"].max()) if len(events["end_ts"]) else None
        if role_max is not None:
            max_end_ts = role_max if max_end_ts is None else max(max_end_ts, role_max)
        roles.append({"role": role + 1, "accounts": int(len(events["accounts"])),
                      "events": int(len(events["end_ts"])), "max_end_ts": role_max})
    manifest = {"schema_version": 1, "complete": True, "source_fingerprint": source_fingerprint,
                "counts": {"rows": int(len(arrays["ts"])),
                           "events": int(sum(role["events"] for role in roles))},
                "max_end_ts": max_end_ts, "roles": roles, "files_sha256": output_files}
    _atomic_json(staging / "manifest.json", manifest)
    if output.exists():
        raise FileExistsError(f"laning history output already exists: {output}")
    os.rename(staging, output)
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    manifest = export_laning_history(args.corpus, args.output_dir)
    print(json.dumps({"output": str(args.output_dir), "counts": manifest["counts"],
                      "max_end_ts": manifest["max_end_ts"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
