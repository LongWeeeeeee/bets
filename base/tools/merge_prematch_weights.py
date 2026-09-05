#!/usr/bin/env python3
"""Merge checked weights into a prematch NPZ while streaming unchanged tables."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import zipfile

import numpy as np

REPLACE = {"mu", "sd", "coef", "intercept", "branch_mu", "branch_sd",
           "branch_coef", "branch_intercept"}
FROZEN = {"ctx_mu", "ctx_sd", "feature_names", "branch_names", "branch_lens", "branch_cols"}


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def member_hashes(path):
    result = {}
    with zipfile.ZipFile(path) as archive:
        for name in archive.namelist():
            h = hashlib.sha256()
            with archive.open(name) as stream:
                for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                    h.update(chunk)
            result[name] = h.hexdigest()
    return result


def merge(snapshot, weights, output, report, expected_snapshot_sha256):
    paths = [Path(p).resolve() for p in (snapshot, weights, output, report)]
    snapshot, weights, output, report = paths
    if len(set(paths)) != 4 or output.exists() or report.exists():
        raise ValueError("distinct input/output paths and new outputs required")
    before = sha256(snapshot)
    if before != expected_snapshot_sha256:
        raise ValueError("snapshot identity changed")
    with np.load(snapshot) as old, np.load(weights) as new:
        for key in FROZEN:
            if not np.array_equal(old[key], new[key]):
                raise ValueError(f"frozen metadata changed: {key}")
        for key in REPLACE:
            if old[key].shape != new[key].shape or not np.isfinite(new[key]).all():
                raise ValueError(f"invalid replacement: {key}")
        if (new["sd"] <= 0).any() or (new["branch_sd"] <= 0).any():
            raise ValueError("nonpositive standard deviation")
        i = list(new["branch_names"]).index("full")
        offset = int(new["branch_lens"][:i].sum())
        width = int(new["branch_lens"][i])
        for key in ("mu", "sd", "coef"):
            if not np.array_equal(new[key][0], new["branch_" + key][offset:offset + width]):
                raise ValueError("top weights do not equal full branch")
        if float(new["intercept"][0]) != float(new["branch_intercept"][i]):
            raise ValueError("top intercept does not equal full branch")
    output.parent.mkdir(parents=True, exist_ok=True)
    report.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_name(output.name + ".tmp")
    original_hashes = {}
    with zipfile.ZipFile(snapshot) as src, zipfile.ZipFile(weights) as updates, \
            zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as dest:
        for name in src.namelist():
            replacement = name.endswith(".npy") and name[:-4] in REPLACE
            origin = updates if replacement else src
            digest = hashlib.sha256()
            with origin.open(name) as reader, dest.open(name, "w", force_zip64=True) as writer:
                for chunk in iter(lambda: reader.read(8 * 1024 * 1024), b""):
                    writer.write(chunk)
                    digest.update(chunk)
            original_hashes[name] = digest.hexdigest()
    actual_hashes = member_hashes(tmp)
    if actual_hashes != original_hashes or sha256(snapshot) != before:
        raise ValueError("merged member validation failed or snapshot changed during merge")
    os.replace(tmp, output)
    result = {"status": "PASS", "snapshot_sha256": before, "weights_sha256": sha256(weights),
              "output_sha256": sha256(output), "replaced": sorted(REPLACE),
              "unchanged_member_count": len(actual_hashes) - len(REPLACE),
              "output_member_sha256": actual_hashes}
    tmp = report.with_name(report.name + ".tmp")
    tmp.write_text(json.dumps(result, indent=2) + "\n")
    os.replace(tmp, report)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("snapshot", "weights", "output", "report"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--expected-snapshot-sha256", required=True)
    result = merge(**vars(parser.parse_args()))
    print(json.dumps({k: v for k, v in result.items() if k != "output_member_sha256"}), flush=True)


if __name__ == "__main__":
    main()
