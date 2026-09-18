"""Offline fixed-C sensitivity on the frozen E287 cohort, never model admission."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
from threadpoolctl import threadpool_limits

from base.player_metadata import PlayerHistory
from base.tools.player_metadata import load_snapshot, write_new
from base.tools.refit_prematch_draft_component import atomic_npz
from base.tools.retrain_prematch_general import retrain


def metrics(y, p):
    y, p = np.asarray(y), np.asarray(p)
    if (y.shape != p.shape or y.ndim != 1 or not len(y)
            or not set(np.unique(y)) <= {0, 1}
            or not np.isfinite(p).all() or np.any((p <= 0) | (p >= 1))):
        raise ValueError('invalid evaluation predictions')
    return {'n': len(y), 'correct': int(np.sum((p >= .5) == y)),
            'accuracy': float(np.mean((p >= .5) == y)),
            'log_loss': float(np.mean(-y*np.log(p)-(1-y)*np.log1p(-p))),
            'brier': float(np.mean((p-y)**2))}


def paired_interval(y, baseline, candidate, series, seed=295):
    """Map-weighted loss delta, bootstrap whole series with replacement."""
    ids, inverse = np.unique(series, return_inverse=True)
    delta = (-y*np.log(candidate)-(1-y)*np.log1p(-candidate)
             + y*np.log(baseline)+(1-y)*np.log1p(-baseline))
    totals = np.bincount(inverse, weights=delta)
    counts = np.bincount(inverse)
    draws = np.random.default_rng(seed).integers(len(ids), size=(5000, len(ids)))
    boot = totals[draws].sum(axis=1) / counts[draws].sum(axis=1)
    return {'candidate_minus_baseline': float(delta.mean()),
            'series_bootstrap_95pct': np.quantile(boot, [.025, .975]).tolist(),
            'series': len(ids), 'draws': 5000, 'seed': seed}


def historical_metadata_coverage(snapshots, timestamps):
    """Certify zero coverage by time only; refuse to invent a lineup/role join.

    If history overlaps any map, a separate account/position-aligned export is
    necessary. This narrow audit cannot claim that such a block is all missing.
    """
    PlayerHistory(snapshots)
    if not snapshots or not len(timestamps):
        raise ValueError('need source observations and map timestamps')
    first = min(float(s['observed_at']) for s in snapshots)
    last = int(np.max(timestamps))
    if first < last:
        raise ValueError('overlapping metadata requires account/position alignment')
    return {'snapshots': len(snapshots), 'earliest_observation': first,
            'latest_map_start': last, 'covered_maps': 0,
            'maps': len(timestamps), 'metadata_fit': 'SKIPPED_NO_HISTORICAL_COVERAGE'}


def check_alignment(matrix, split, reference):
    mids = matrix['mids']
    if len(np.unique(mids)) != len(mids) or not np.array_equal(mids, split['mids']):
        raise ValueError('matrix/split map identity mismatch')
    for key in ('train', 'test'):
        if split[key].dtype != bool or split[key].shape != mids.shape:
            raise ValueError('invalid split mask')
    if np.any(split['train'] & split['test']):
        raise ValueError('overlapping train/test')
    for key in ('mids', 'y', 'ts', 'sids'):
        if not np.array_equal(matrix[key][split['test']], reference[key]):
            raise ValueError('reference map identity mismatch: ' + key)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--threads', type=int, default=1)
    args = parser.parse_args()
    plan = json.loads(args.plan.read_text())
    for path, expected in plan['sha256'].items():
        if hashlib.sha256(Path(path).read_bytes()).hexdigest() != expected:
            raise ValueError('input hash mismatch: ' + path)
    out = args.output_dir
    if out.exists() and any(out.iterdir()):
        raise ValueError('output must be new or empty')
    out.mkdir(parents=True, exist_ok=True)
    def read(key):
        with np.load(plan[key], allow_pickle=False) as source:
            return dict(source)
    matrix, split, reference, weights = map(read, ('matrix', 'split', 'reference', 'weights'))
    check_alignment(matrix, split, reference)
    matrix['routed_branch'] = split['routed_branch']
    coverage = historical_metadata_coverage(
        [load_snapshot(Path(p)) for p in plan['snapshots']], matrix['ts'])
    test = split['test']
    y, series = matrix['y'][test], matrix['sids'][test]
    baseline = reference['candidate_routed']
    results, predictions = {}, {'mids': matrix['mids'][test], 'y': y,
                                'sids': series, 'baseline': baseline}
    with threadpool_limits(limits=args.threads):
        for c in plan['C_values']:
            arrays, p, train, actual_test, optimization = retrain(
                matrix, weights, cutoff=plan['test_cutoff'], c=c,
                embargo=plan['embargo_seconds'])
            if not np.array_equal(train, split['train']) or not np.array_equal(actual_test, test):
                raise ValueError('refit changed frozen split')
            routed = matrix['routed_branch'][test]
            prior = np.where(routed == 'full', p['parent_full'], p['parent_no_org'])
            if np.max(np.abs(prior-baseline)) >= 1e-6:
                raise ValueError('frozen baseline replay failed')
            candidate = np.where(routed == 'full', p['full'], p['no_org'])
            error = float(np.max(np.abs(candidate-baseline)))
            if c == plan['baseline_C'] and error >= 1e-6:
                raise ValueError('baseline refit reproduction failed')
            key = str(c)
            predictions['C_' + key] = candidate
            results[key] = {'metrics': metrics(y, candidate),
                            'paired_log_loss': paired_interval(y, baseline, candidate, series),
                            'max_probability_change': error, 'optimization': optimization}
            atomic_npz(out / ('weights_C_' + key + '.npz'), arrays)
            print(json.dumps({'C': c, **results[key]}), flush=True)
    atomic_npz(out / 'predictions.npz', predictions)
    report = {'status': 'DONE', 'train_maps': int(split['train'].sum()),
              'test_maps': int(test.sum()), 'test_series': len(np.unique(series)),
              'baseline': metrics(y, baseline), 'refits': results, 'metadata': coverage,
              'protocol': plan, 'production_admission': False,
              'limitations': [
                  'Historical 38-map test already examined; not an independent holdout.',
                  'Frozen draft backbone reaches earlier fit rows: no clean inner validation.',
                  'Fixed C sensitivity only; no test-based winner selection for serving.',
                  'Earnings/rank uplift unmeasured: zero causal historical coverage.',
                  'Baseline is frozen E287 weights; current remote runtime not inspected.',
                  'Probability calibration after weight changes remains unvalidated.']}
    write_new(out / 'summary.json', json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
