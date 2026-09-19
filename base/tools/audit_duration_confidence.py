"""Describe held-out duration probabilities; never fit on test outcomes."""
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np


def audit(path, key='baseline_platt'):
    z = np.load(path)
    p, y = z[key], z['y']
    days, ix = np.unique((z['ts'] // 86400).astype(int), return_inverse=True)
    rng = np.random.default_rng(305)
    draws = rng.integers(0, len(days), size=(5000, len(days)))

    def group(mask):
        n, wins = int(mask.sum()), int(y[mask].sum())
        dc = np.bincount(ix, weights=mask, minlength=len(days))
        dw = np.bincount(ix, weights=mask * y, minlength=len(days))
        counts = dc[draws].sum(1)
        rates = dw[draws].sum(1)[counts > 0] / counts[counts > 0]
        return {'n': n, 'wins': wins, 'wr': wins / n if n else None,
                'mean_p': float(p[mask].mean()) if n else None,
                'day_bootstrap_ci95': np.quantile(rates, [.025, .975]).tolist() if n else None}

    edges = [0, .3, .4, .5, .55, .6, .65, .7, .75, 1.000001]
    thresholds = {str(t): group(p >= t) for t in [.5, .55, .6, .65, .7]}
    months = np.array([datetime.fromtimestamp(t, timezone.utc).strftime('%Y-%m') for t in z['ts']])
    return {'source_sha256': hashlib.sha256(Path(path).read_bytes()).hexdigest(),
            'key': key, 'n': len(y), 'days': len(days), 'target': 'duration >= 2580 seconds',
            'scope': 'Four historical refits; independent calibration month per fold; no odds or ROI',
            'thresholds': thresholds,
            'months_at_60': {m: group((months == m) & (p >= .6)) for m in np.unique(months)},
            'bins': [dict(lo=a, hi=b, **group((p >= a) & (p < b))) for a, b in zip(edges[:-1], edges[1:])]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--predictions', type=Path, required=True)
    ap.add_argument('--output', type=Path, required=True)
    args = ap.parse_args()
    result = audit(args.predictions)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    tmp = args.output.with_suffix('.tmp')
    tmp.write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    tmp.replace(args.output)
    print(json.dumps({k: result[k] for k in ['n', 'days', 'thresholds', 'months_at_60']}))


if __name__ == '__main__':
    main()
