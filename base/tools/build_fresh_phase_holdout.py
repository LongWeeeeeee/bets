#!/usr/bin/env python3
"""Freeze new pro maps beyond the E-260 training/evaluation archive, without scoring."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import numpy as np

from base.build_draft_phase_corpus import (
    _atomic_json, _atomic_npz, _consolidate, _parse_source, _source_paths,
    rule_fingerprint, source_fingerprint,
)
from base.tools.export_draft_phase_serving import digest
from base.train_draft_phase_models import load_rows


def fresh_rows(rows, public, previous_pro, embargo_seconds=3600):
    """Disjoint IDs and strict label-availability cutoff, shared by all four models."""
    if embargo_seconds < 0:
        raise ValueError("embargo must be non-negative")
    if not len(public["mid"]) or not len(previous_pro["mid"]):
        raise ValueError("reference corpora must be nonempty")
    public_ready = int(np.max(public["ts"] + public["duration"])) + embargo_seconds
    cutoff = max(public_ready, int(np.max(previous_pro["ts"])))
    overlap_public = np.isin(rows["mid"], public["mid"])
    overlap_pro = np.isin(rows["mid"], previous_pro["mid"])
    after = rows["ts"] > cutoff
    keep = after & ~overlap_public & ~overlap_pro
    return {k: v[keep] for k, v in rows.items()}, {
        "fresh_after_ts": cutoff, "public_labels_available_ts": public_ready,
        "previous_pro_last_ts": int(np.max(previous_pro["ts"])),
        "embargo_seconds": embargo_seconds, "parsed_rows": len(keep),
        "not_after_cutoff": int((~after).sum()),
        "public_id_overlap": int(overlap_public.sum()),
        "previous_pro_id_overlap": int(overlap_pro.sum()), "rows": int(keep.sum()),
    }


def build(source, public_corpus, previous_pro_corpus, output, embargo_seconds=3600):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    if (output / "manifest.json").exists():
        raise FileExistsError("holdout already frozen; use a new output directory")
    source = Path(source)
    previous = json.loads((Path(previous_pro_corpus) / "manifest.json").read_text())
    rules = rule_fingerprint()
    if previous["rule_fingerprint"] != rules:
        raise ValueError("canonical rules changed; incremental holdout requires a full rebuild")
    old_sources = {item["source"]: item["fingerprint"] for item in previous["sources"]}
    public = load_rows(public_corpus)
    old_pro = load_rows(previous_pro_corpus)
    paths, skipped = _source_paths(source)
    inventory, parsed, changed = [], [], []
    for path in paths:
        fingerprint = source_fingerprint(path, source, rules)
        inventory.append(fingerprint)
        old = old_sources.get(path.name)
        if old and old["source_sha256"] == fingerprint["source_sha256"]:
            continue
        print(f"parse new/changed {path.name}", flush=True)
        rows, counts = _parse_source(path)
        if source_fingerprint(path, source, rules) != fingerprint:
            raise RuntimeError(f"source changed while parsing: {path}")
        parsed.append((path.name, rows))
        changed.append({"source": path.name, "fingerprint": fingerprint, "counts": counts})
    missing = sorted(set(old_sources) - {path.name for path in paths})
    if missing:
        raise ValueError(f"previous source files missing: {missing}")
    rows, dedup = _consolidate(parsed)
    selected, selection = fresh_rows(rows, public, old_pro, embargo_seconds)
    if not len(selected["mid"]):
        raise ValueError("no new eligible pro maps")
    _atomic_npz(output / "rows.npz", **selected)
    manifest = {
        "complete": True, "purpose": "unscored_frozen_forward_holdout",
        "frozen_at": time.time(), "source": str(source.resolve()),
        "rule_fingerprint": rules, "selection": selection,
        "rows": len(selected["mid"]), "first_ts": int(selected["ts"][0]),
        "last_ts": int(selected["ts"][-1]), "dedup": dict(dedup),
        "changed_sources": changed, "source_inventory": inventory,
        "skipped_metadata": skipped, "rows_sha256": digest(output / "rows.npz"),
        "public_rows_sha256": digest(Path(public_corpus) / "rows.npz"),
        "previous_pro_rows_sha256": digest(Path(previous_pro_corpus) / "rows.npz"),
        "builder_sha256": digest(Path(__file__)),
    }
    _atomic_json(output / "manifest.json", manifest)
    print(json.dumps({"status": "DONE", **selection}, sort_keys=True), flush=True)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--public-corpus", type=Path, required=True)
    parser.add_argument("--previous-pro-corpus", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--embargo-seconds", type=int, default=3600)
    args = parser.parse_args()
    build(args.source, args.public_corpus, args.previous_pro_corpus,
          args.output, args.embargo_seconds)


if __name__ == "__main__":
    main()
