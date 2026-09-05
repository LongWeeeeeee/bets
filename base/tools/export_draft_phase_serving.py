#!/usr/bin/env python3
"""Export phase bundles for existing production readers without changing targets."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import joblib
import numpy as np

FILES = {
    "all": ("win_feature_encoder.joblib", "radiant_win_model.joblib"),
    "late": ("late_win_feature_encoder.joblib", "late_win_model.joblib"),
    "early_nw": ("early_nw_feature_encoder.joblib", "early_nw_model.joblib"),
    "early_win": ("early_win_feature_encoder.joblib", "early_win_model.joblib"),
}


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_dump(obj, path):
    tmp = path.with_name(path.name + ".tmp")
    joblib.dump(obj, tmp, compress=3)
    os.replace(tmp, path)


def export(source, output, heroes):
    if (output / "manifest.json").exists():
        raise FileExistsError(f"finished export already exists: {output}")
    output.mkdir(parents=True, exist_ok=True)
    manifest = {"schema_version": 1, "complete": False, "models": {},
                "production_changed": False}
    probabilities = {"heroes": heroes}
    for phase, (encoder_file, model_file) in FILES.items():
        source_dir = source / phase / "hero_role_position_pair"
        bundle_path = source_dir / "model.joblib"
        bundle = joblib.load(bundle_path)
        report = json.loads((source_dir / "results.json").read_text())
        expected_classes = ["dire", "radiant", "no_marker"] if phase == "early_nw" else ["dire", "radiant"]
        if bundle.phase != phase or bundle.classes_.tolist() != expected_classes:
            raise ValueError(f"wrong phase/classes: {bundle_path}")
        if report["final_artifact"]["held_out_test"] is not False:
            raise ValueError(f"expected full-fit artifact: {source_dir}")
        if bundle.encoder.n_columns != bundle.classifier.n_features_in_:
            raise ValueError(f"encoder/model width mismatch: {phase}")
        directory = output / phase
        directory.mkdir(exist_ok=True)
        # Keep the complete bundle, including occurrence, for provenance/future readers.
        atomic_dump(bundle, directory / "model.joblib")
        atomic_dump(bundle.encoder, directory / encoder_file)
        atomic_dump(bundle.classifier, directory / model_file)
        encoder = joblib.load(directory / encoder_file)
        model = joblib.load(directory / model_file)
        actual = model.predict_proba(encoder.transform(heroes))[:, 1]
        joint = bundle.predict_proba(heroes)
        expected = joint[:, 1] / joint[:, :2].sum(axis=1) if phase == "early_nw" else joint[:, 1]
        if not np.isfinite(actual).all() or not np.allclose(actual, expected, atol=1e-12, rtol=0):
            raise ValueError(f"export changed probabilities: {phase}")
        probabilities[phase] = actual
        manifest["models"][phase] = {
            "source": str(bundle_path), "source_sha256": digest(bundle_path),
            "target": "radiant_direction_given_marker" if phase == "early_nw" else "radiant_win",
            "population": report["population"], "fit_rows": report["final_artifact"]["fit_rows"],
            "columns": int(encoder.n_columns), "probability_max_delta": float(np.max(abs(actual-expected))),
            "files": {p.name: digest(p) for p in directory.iterdir() if p.is_file()},
        }
    tmp = output / "verification_probe.npz.tmp"
    with tmp.open("wb") as f:
        np.savez_compressed(f, **probabilities)
    os.replace(tmp, output / "verification_probe.npz")
    manifest["probe_sha256"] = digest(output / "verification_probe.npz")
    manifest["complete"] = True
    tmp = output / "manifest.json.tmp"
    tmp.write_text(json.dumps(manifest, indent=2) + "\n")
    os.replace(tmp, output / "manifest.json")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with np.load(args.corpus) as z:
        h = z["heroes"]
        heroes = h[np.linspace(0, len(h)-1, min(512, len(h)), dtype=int)]
    result = export(args.source, args.output, heroes)
    print(json.dumps({p: {k: r[k] for k in ("columns", "fit_rows", "probability_max_delta")}
                      for p, r in result["models"].items()}, indent=2))


if __name__ == "__main__":
    main()
