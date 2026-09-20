#!/usr/bin/env python3
"""Monthly causal public-draft pretraining; never read pro outcomes.

Public examples are admitted by completion time and consumed once, in end-time
order. All pro IDs are excluded even when the public dump contains duplicates.
The encoder vocabulary is frozen on the first admitted prefix. Outputs before
the first snapshot stay missing instead of receiving future-trained forecasts.
"""
import argparse
import datetime as dt
import hashlib
import json
import sys
import time
from pathlib import Path

import joblib
import numpy as np
from sklearn.linear_model import SGDClassifier
from threadpoolctl import threadpool_limits

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from base.draft_features import DraftFeatureEncoder


def epoch(value):
    return int(dt.datetime.fromisoformat(value).replace(tzinfo=dt.timezone.utc).timestamp())


def project_snapshots(args):
    """Apply existing public models to heroes ordered using past player roles."""
    with np.load(args.targets, allow_pickle=False) as z:
        assert str(z['role_assignment'].item()) == 'past_completed_player_roles'
        mid, starts, heroes = (z[k] for k in ('mid', 'ts', 'heroes'))
    p = np.full(len(mid), np.nan, dtype=np.float32)
    snapshot = np.zeros(len(mid), dtype=np.int64)
    files = sorted(args.project_snapshots.glob('draft_*.joblib'))
    assert files, 'no frozen draft snapshots'
    bundles = sorted([(path, joblib.load(path)) for path in files], key=lambda item: item[1]['cutoff'])
    receipts = []
    with threadpool_limits(limits=args.threads):
        for number, (path, bundle) in enumerate(bundles):
            cutoff = bundle['cutoff']
            next_cut = bundles[number+1][1]['cutoff'] if number+1 < len(bundles) else 2**62
            assert next_cut > cutoff, 'duplicate snapshot cutoff'
            assert bundle['max_label_end'] < cutoff and bundle['kind'] == args.kind
            rows = np.flatnonzero((starts >= cutoff) & (starts < next_cut))
            for offset in range(0, len(rows), 40000):
                selected = rows[offset:offset+40000]
                p[selected] = bundle['model'].predict_proba(bundle['encoder'].transform(heroes[selected]))[:, 1]
                snapshot[selected] = cutoff
            receipts.append({'source': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                             'cutoff': cutoff, 'max_label_end': bundle['max_label_end'], 'target_rows': len(rows)})
    np.savez_compressed(args.output_dir/'draft_features.npz', mid=mid, p=p, snapshot=snapshot)
    metadata = {'mode': 'project_frozen_public_models', 'kind': args.kind, 'snapshots': receipts,
                'target_role_assignment': 'past_completed_player_roles', 'target_outcomes_read': False,
                'targets_sha256': hashlib.sha256(args.targets.read_bytes()).hexdigest()}
    (args.output_dir/'metadata.json').write_text(json.dumps(metadata, indent=2)+'\n')


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--public', type=Path)
    ap.add_argument('--rich', type=Path)
    ap.add_argument('--project-snapshots', type=Path)
    ap.add_argument('--targets', type=Path)
    ap.add_argument('--output-dir', type=Path, required=True)
    ap.add_argument('--threads', type=int, default=1)
    ap.add_argument('--kind', choices=['hero_role', 'hero_role_position_pair'], required=True)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.project_snapshots:
        if args.targets is None:
            ap.error('--project-snapshots requires --targets')
        project_snapshots(args)
        return
    if args.public is None or args.rich is None:
        ap.error('pretraining requires --public and --rich')
    started = time.time()
    with np.load(args.public) as z:
        heroes, mid, starts, duration, y = (z[k] for k in ('heroes', 'mid', 'ts', 'duration', 'wins'))
    with np.load(args.rich) as z:
        target_mid, target_ts, target_heroes = (z[k] for k in ('mids', 'ts', 'heroes'))
    ends = starts + duration
    valid = ((duration > 0) & np.isin(y, [0, 1]) & ~np.isin(mid, target_mid)
             & (heroes > 0).all(axis=1))
    order = np.flatnonzero(valid)
    order = order[np.lexsort((mid[order], ends[order]))]
    assert len(np.unique(mid[order])) == len(order), 'duplicate public match IDs'
    cuts = [epoch(f'2026-{month:02d}-01') for month in range(4, 10)] + [epoch('2026-09-21')]
    first = order[ends[order] < cuts[0]]
    assert len(first) > 1000, 'insufficient first causal vocabulary prefix'
    encoder = DraftFeatureEncoder.fit(heroes[first], kind=args.kind, signed=True, pair_min_support=30)
    model = SGDClassifier(loss='log_loss', penalty='l2', alpha=1e-5,
                          learning_rate='invscaling', eta0=.04, power_t=.15,
                          average=True, random_state=20260921)
    p = np.full(len(target_mid), np.nan, dtype=np.float32)
    snapshot = np.zeros(len(target_mid), dtype=np.int64)
    consumed, receipts = 0, []
    rng = np.random.default_rng(20260921)
    with threadpool_limits(limits=args.threads):
        for number, cutoff in enumerate(cuts):
            stop = int(np.searchsorted(ends[order], cutoff, side='left'))
            for offset in range(consumed, stop, 40000):
                rows = order[offset:min(offset+40000, stop)].copy()
                rng.shuffle(rows)
                design = encoder.transform(heroes[rows])
                model.partial_fit(design, y[rows], classes=np.array([0, 1]))
            consumed = stop
            next_cut = cuts[number+1] if number+1 < len(cuts) else 2**62
            targets = np.flatnonzero((target_ts >= cutoff) & (target_ts < next_cut))
            for offset in range(0, len(targets), 40000):
                rows = targets[offset:offset+40000]
                p[rows] = model.predict_proba(encoder.transform(target_heroes[rows]))[:, 1]
                snapshot[rows] = cutoff
            max_end = int(ends[order[stop-1]])
            assert max_end < cutoff
            name = dt.datetime.fromtimestamp(cutoff, dt.timezone.utc).strftime('%Y%m%d')
            joblib.dump({'encoder': encoder, 'model': model, 'cutoff': cutoff,
                         'max_label_end': max_end, 'kind': args.kind}, args.output_dir / f'draft_{name}.joblib')
            receipt = {'cutoff': cutoff, 'max_label_end': max_end, 'public_train_rows': stop,
                       'target_rows': len(targets), 'elapsed_seconds': time.time()-started}
            receipts.append(receipt)
            print(json.dumps(receipt), flush=True)
    np.savez_compressed(args.output_dir / 'draft_features.npz', mid=target_mid,
                        p=p, snapshot=snapshot)
    metadata = {'kind': args.kind, 'source_sha256': {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in (args.public, args.rich)},
        'public_rows': len(mid), 'eligible_public_rows': len(order),
        'excluded_pro_overlap': int(np.isin(mid, target_mid).sum()),
        'encoder_initial_rows': len(first), 'encoder_columns': encoder.n_columns,
        'snapshots': receipts,
        'method': 'One SGD pass over every admitted public map; monthly frozen as-of predictions.',
        'pro_outcomes_used': False, 'earlier_targets': 'NaN before first causal public snapshot'}
    (args.output_dir / 'metadata.json').write_text(json.dumps(metadata, indent=2)+'\n')


if __name__ == '__main__':
    main()
