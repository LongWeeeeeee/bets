"""Freeze an existing candidate and completed history; does not train models."""
import argparse
import hashlib
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path

import joblib
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / 'base')]
from duration43_shadow import FrozenCandidate, candidate_vector, sha256
from prematch_panel_scorer import load_bundle, DEFAULT_DIR

E299_AUGUST = {
    'causal_history.cbm': 'b21442378c3c96d3a80a4b43630b08f906c4ecb556186b466009bbd3540f692c',
    'encoder.joblib': '1fcc4175c58863514adfbac235ab466f309fcc0f3c27b458f94cb51e152a0053',
    'calibrator.joblib': '73a2e1657ace87883eca426ee012c8184e0e321bcf5fbc5160fee12f3dc9ba22',
}
INCUMBENT_SHA = '80bb42c8f141a54b0804d85c26a07c0618c790143bb5747aa5e23ca2499747d8'


def verify_candidate(directory):
    for name, expected in E299_AUGUST.items():
        if sha256(directory / name) != expected:
            raise ValueError('Not the frozen E299/August input: ' + name)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--candidate-dir', type=Path, required=True)
    p.add_argument('--corpus', type=Path, required=True)
    p.add_argument('--output-dir', type=Path, required=True)
    args = p.parse_args()
    dest = args.output_dir
    if dest.exists():
        raise ValueError('New immutable directory required')
    verify_candidate(args.candidate_dir)
    built = time.time()
    z = np.load(args.corpus)
    start, duration = z['ts'], z['durations']
    mask = ((start >= 1672531200) & (duration > 0) & (start + duration < built)
            & (z['heroes'] > 0).all(1)
            & (np.diff(np.sort(z['heroes'], axis=1), axis=1) != 0).all(1))
    mids = z['mids'][mask]
    assert len(mids) == len(np.unique(mids))
    dur = duration[mask].astype(float)
    values = np.repeat(np.column_stack([dur / 60, dur >= 2580, dur >= 2160, np.ones(len(dur))]), 10, axis=0)
    history = {}
    for name in ['accounts', 'heroes']:
        keys = z[name][mask].ravel()
        positive = keys > 0
        unique, index = np.unique(keys[positive], return_inverse=True)
        sums = np.column_stack([np.bincount(index, weights=v[positive]) for v in values.T])
        history[name] = {str(int(k)): row.tolist() for k, row in zip(unique, sums)}
    enc = joblib.load(args.candidate_dir / 'encoder.joblib')
    assert enc.kind == 'hero_role' and not enc.signed
    cal = joblib.load(args.candidate_dir / 'calibrator.joblib')
    bundle = load_bundle(DEFAULT_DIR)
    if bundle.artifact_hashes.get('dur43') != INCUMBENT_SHA:
        raise ValueError('Not the frozen September19 incumbent')
    spec = next(s for s in bundle.specs if s.key == 'dur43')
    manifest = {'schema': 'duration43-shadow-v1', 'built_at': built,
                'max_history_end': float(np.max(start[mask] + duration[mask])),
                'history_rows': len(dur), 'history_from': '2023-01-01',
                'history_policy': 'Frozen before prospective enrollment; no online outcome updates',
                'candidate_origin': 'E299 August fold: train before June, early stopping June, Platt July',
                'candidate_source_sha256': E299_AUGUST,
                'candidate_sha256': sha256(args.candidate_dir / 'causal_history.cbm'),
                'incumbent_sha256': bundle.artifact_hashes['dur43'],
                'incumbent_spec': asdict(spec), 'incumbent_columns': list(bundle.columns),
                'hero_ids': enc.hero_ids.tolist(), 'platt': [float(cal.coef_[0, 0]), float(cal.intercept_[0])],
                'history': history, 'corpus_sha256': sha256(args.corpus),
                'evaluation': {'min_maps': 300, 'min_days': 30, 'target_seconds': 2580,
                               'primary': 'paired calibrated logloss',
                               'bootstrap_unit': 'UTC day', 'bootstrap_repeats': 5000,
                               'look_policy': 'First capture prefix with at least 300 enrolled maps spanning 30 days; wait for every outcome. If fewer than 300 eligible pairs remain, stop as INSUFFICIENT_ELIGIBLE without metrics or extending this endpoint.',
                               'outcome_producer': 'prematch_prediction_journal.record_outcome schema_version=1, STRATZ result sources only; local observed_ts is not upstream fetch time',
                               'selection': 'first valid paired record per map; verify recorded_at<actual_start and outcome_observed_at>recorded_at after enrollment closes; report exclusions and missing outcomes'}}
    # Verify the portable draft encoder against its exact saved training encoder.
    for heroes in z['heroes'][mask][-32:]:
        x = candidate_vector(heroes, [0] * 10, manifest)
        assert np.array_equal(x[:6 * len(enc.hero_ids)], enc.transform(heroes.reshape(1, -1)).toarray()[0])
    dest.mkdir(parents=True)
    for source, name in [(args.candidate_dir / 'causal_history.cbm', 'candidate.cbm'),
                         (DEFAULT_DIR / 'dur43.cbm', 'incumbent.cbm')]:
        tmp = dest / (name + '.tmp'); tmp.write_bytes(source.read_bytes()); tmp.replace(dest / name)
    assert sha256(dest / 'incumbent.cbm') == manifest['incumbent_sha256']
    verify_candidate(args.candidate_dir)
    tmp = dest / 'manifest.json.tmp'; tmp.write_text(json.dumps(manifest, allow_nan=False, indent=2) + '\n'); tmp.replace(dest / 'manifest.json')
    c = FrozenCandidate(dest)
    x, raw, calibrated = c.predict(z['heroes'][mask][-1], z['accounts'][mask][-1])
    assert np.isfinite(x).all() and 0 < raw < 1 and 0 < calibrated < 1
    print(json.dumps({'directory': str(dest), 'manifest_sha256': c.manifest_sha,
                      'history_rows': len(dur), 'encoder_parity_cases': 32,
                      'inference_smoke': 'PASS'}))


if __name__ == '__main__':
    main()
