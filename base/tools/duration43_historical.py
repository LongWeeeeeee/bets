"""E304: past-trained auxiliary draft models and historical duration ablations."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import warnings
from pathlib import Path

import joblib
import numpy as np
from scipy import sparse
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import LogisticRegression

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / 'runtime/experiments/misc')]
from base.draft_features import DraftFeatureEncoder, KIND_PAIR

SOURCE = 'runtime/artifacts/misc/pro_corpus_rich.npz'
NW_SOURCE = 'data/draft_phase_corpus/2026-09-05_position_pairs/pro/rows.npz'
BASE_RUN = '.orchestra/jobs/run-2dcf60a5ff61532186ec2781'
THRESHOLDS = (32, 36, 40, 43, 47)


def utc(day):
    return int(np.datetime64(day, 's').astype(np.int64))


def save(path, payload):
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, indent=2, allow_nan=False) + '\n')
    tmp.replace(path)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def past_split(ts, duration, year):
    cut = utc(f'{year}-01-01')
    train = (ts >= utc('2023-01-01')) & (ts + duration < cut)
    predict = (ts >= cut) & (ts < utc(f'{year+1}-01-01'))
    return train, predict


def join_nw(rows, source):
    """Require exact identity/target/draft; padded rich NW is never a label."""
    ids, left, right = np.intersect1d(rows['mids'], source['mid'], return_indices=True)
    if len(np.unique(source['mid'])) != len(source['mid']):
        raise ValueError('Duplicate canonical NW IDs')
    agree = ((rows['ts'][left] == source['ts'][right])
             & (rows['durations'][left] == source['duration'][right])
             & (rows['wins'][left] == source['wins'][right])
             & (rows['heroes'][left] == source['heroes'][right]).all(1))
    labels = np.full(len(rows['mids']), -1, dtype=np.int8)
    labels[left[agree]] = source['early_nw'][right[agree]]
    if not np.isin(labels, [-1, 0, 1, 2]).all():
        raise ValueError('Unknown NW class')
    return labels, {'matched_ids': len(ids), 'consistent': int(agree.sum()),
                    'conflicting': int((~agree).sum()), 'usable_nw': int((labels >= 0).sum())}


def winner_masks(duration, nw):
    return [np.isin(nw, [0, 1]), (duration >= 1200) & (duration <= 2040),
            duration >= 1200, duration >= 2160,
            (duration > 2040) & (duration < 2580), duration >= 2580]


def invariant_features(probabilities):
    margins = 2 * probabilities - 1
    columns = [np.abs(margins)]
    columns += [(margins[:, i] * margins[:, j])[:, None]
                for i in range(margins.shape[1]) for j in range(i+1, margins.shape[1])]
    return np.column_stack(columns).astype(np.float32)


def fit_logistic(x, y, *, c=.03):
    if len(np.unique(y)) != 2:
        raise ValueError('Auxiliary split lacks both classes')
    with warnings.catch_warnings():
        warnings.simplefilter('error', ConvergenceWarning)
        return LogisticRegression(C=c, solver='liblinear', max_iter=1000,
                                  tol=1e-5, random_state=304).fit(x, y)


def prepare(out):
    from duration43_rolling import build_history
    z = np.load(ROOT / SOURCE)
    mask = ((z['ts'] >= utc('2023-01-01')) & (z['ts'] < utc('2026-09-01'))
            & (z['durations'] > 0) & (z['heroes'] > 0).all(1)
            & (np.diff(np.sort(z['heroes'], axis=1), axis=1) != 0).all(1))
    rows = {k: z[k][mask] for k in ['mids', 'ts', 'durations', 'heroes', 'accounts', 'wins']}
    order = np.lexsort((rows['mids'], rows['ts']))
    rows = {k: v[order] for k, v in rows.items()}
    if len(np.unique(rows['mids'])) != len(rows['mids']):
        raise ValueError('Duplicate duration IDs')
    nw, join = join_nw(rows, np.load(ROOT / NW_SOURCE))
    ts, duration, heroes = (rows[k] for k in ('ts', 'durations', 'heroes'))
    history = build_history(ts, duration, heroes, rows['accounts'])
    phases = np.full((len(ts), 6), np.nan, dtype=np.float32)
    occurrence = np.full(len(ts), np.nan, dtype=np.float32)
    direct = np.full((len(ts), len(THRESHOLDS)), np.nan, dtype=np.float32)
    report = {'join': join, 'source_sha256': sha(ROOT / SOURCE),
              'nw_source_sha256': sha(ROOT / NW_SOURCE), 'years': {}}
    for year in (2024, 2025, 2026):
        train, target = past_split(ts, duration, year)
        enc = DraftFeatureEncoder.fit(heroes[train], KIND_PAIR, signed=True)
        unsigned = DraftFeatureEncoder.fit(heroes[train], KIND_PAIR, signed=False)
        signed_x = enc.transform(heroes[train])
        signed_t = enc.transform(heroes[target])
        unsigned_x = unsigned.transform(heroes[train])
        unsigned_t = unsigned.transform(heroes[target])
        swapped = np.column_stack([heroes[target][:8, 5:], heroes[target][:8, :5]])
        if (enc.transform(swapped) != -signed_t[:8]).nnz:
            raise ValueError('Signed encoder swap mismatch')
        models, counts = {}, {}
        names = ['early_nw', 'early_win', 'all', 'late', 'winner_34_43', 'winner_ge43']
        for j, (name, eligible) in enumerate(zip(names, winner_masks(duration, nw))):
            take = eligible[train]
            labels = nw[train][take] if name == 'early_nw' else rows['wins'][train][take]
            model = fit_logistic(signed_x[take], labels)
            # Remove Radiant intercept bias for duration's side-invariant features.
            p = model.predict_proba(signed_t)[:, 1]
            swapped_p = model.predict_proba(-signed_t)[:, 1]
            phases[target, j] = .5 * (p + 1 - swapped_p)
            models[name] = model; counts[name] = int(take.sum())
        known = nw[train] >= 0
        model = fit_logistic(unsigned_x[known], nw[train][known] != 2)
        occurrence[target] = model.predict_proba(unsigned_t)[:, 1]
        models['nw_occurrence'] = model; counts['nw_occurrence'] = int(known.sum())
        for j, threshold in enumerate(THRESHOLDS):
            model = fit_logistic(unsigned_x, duration[train] >= threshold*60)
            direct[target, j] = model.predict_proba(unsigned_t)[:, 1]
            models[f'duration_ge{threshold}'] = model
        joblib.dump({'signed_encoder': enc, 'unsigned_encoder': unsigned, 'models': models},
                    out / f'auxiliary_{year}.joblib')
        report['years'][str(year)] = {'training': int(train.sum()), 'predicted': int(target.sum()),
                                     'latest_training_end': int((ts+duration)[train].max()),
                                     'earliest_prediction_start': int(ts[target].min()), 'label_counts': counts}
        print('AUXILIARY', year, report['years'][str(year)], flush=True)
    eligible = ts >= utc('2024-01-01')
    if not all(np.isfinite(a[eligible]).all() for a in [history, phases, occurrence, direct]):
        raise ValueError('Non-finite prepared features')
    np.savez_compressed(out/'features.npz', **rows, history=history, phases=phases,
                        occurrence=occurrence, direct=direct)
    save(out/'preparation.json', report)


def train_fold(out, features, fold, threads, baseline_root):
    from catboost import CatBoostClassifier
    from duration43_rolling import FOLDS, metrics, paired
    z = np.load(features)
    ts, duration, heroes = (z[k] for k in ('ts', 'durations', 'heroes'))
    cuts = list(map(utc, FOLDS[fold])); ends = ts + duration
    train = (ts >= utc('2024-01-01')) & (ends < cuts[0])
    early, cal, test = [(ts >= a) & (ts < b) & (ends < b) for a, b in zip(cuts[:-1], cuts[1:])]
    y = (duration >= 2580).astype(int)
    baseline = baseline_root / fold / 'output'
    enc = joblib.load(baseline/'encoder.joblib')
    base_x = sparse.hstack([enc.transform(heroes), sparse.csr_matrix(z['history'])], format='csr').astype(np.float32)
    previous = np.load(baseline/'predictions.npz')
    if not np.array_equal(previous['mids'], z['mids'][test]) or not np.array_equal(previous['y'], y[test]):
        raise ValueError('Different historical baseline cohort')
    model = CatBoostClassifier(); model.load_model(str(baseline/'causal_history.cbm'))
    reproduced = model.predict_proba(base_x[test], thread_count=threads)[:, 1]
    if not np.allclose(reproduced, previous['causal_history'], rtol=0, atol=1e-12):
        raise ValueError('Historical baseline feature replay mismatch')
    protocol = json.loads((baseline/'protocol.json').read_text())
    predictions = {'baseline_raw': previous['causal_history'], 'baseline_platt': previous['causal_platt']}
    config = dict(protocol['model']); rounds = config.pop('early_stopping_rounds')
    specifications = {'plus_phases': np.column_stack([invariant_features(z['phases'][:, :4]), z['occurrence']]),
                      'plus_phases_bands': np.column_stack([invariant_features(z['phases']), z['occurrence'], z['direct']])}
    selected = {}
    logit = lambda p: np.log(np.clip(p, 1e-6, 1-1e-6)/(1-np.clip(p, 1e-6, 1-1e-6)))[:, None]
    for name, extras in specifications.items():
        # Dense float32 is bounded (<1 GiB here). CatBoost's CSR ingestion
        # for these continuous interactions allocated >79 GiB on macOS.
        x = sparse.hstack([base_x, sparse.csr_matrix(extras)], format='csr').toarray()
        m = CatBoostClassifier(**config, thread_count=threads, verbose=False, allow_writing_files=False)
        print('FIT', fold, name, x.shape, 'bytes', x.nbytes, flush=True)
        m.fit(x[train], y[train], eval_set=(x[early], y[early]), early_stopping_rounds=rounds)
        raw = m.predict_proba(x[test], thread_count=threads)[:, 1]
        pc = m.predict_proba(x[cal], thread_count=threads)[:, 1]
        calibrator = LogisticRegression(C=1., solver='lbfgs', max_iter=1000).fit(logit(pc), y[cal])
        predictions[name+'_raw'] = raw
        predictions[name+'_platt'] = calibrator.predict_proba(logit(raw))[:, 1]
        if not np.isfinite(predictions[name+'_platt']).all():
            raise ValueError('Non-finite calibration')
        m.save_model(str(out/(name+'.cbm')))
        joblib.dump(calibrator, out/(name+'_calibrator.joblib'))
        selected[name] = int(m.tree_count_)
        np.savez_compressed(out/(name+'_calibration.npz'), p=pc, y=y[cal], mids=z['mids'][cal])
    np.savez_compressed(out/'predictions.npz', mids=z['mids'][test], ts=ts[test], y=y[test], **predictions)
    report = {'fold': fold, 'baseline_replay_max_error': float(np.max(np.abs(reproduced-previous['causal_history']))),
              'trees': selected, 'counts': {k:int(m.sum()) for k,m in zip(['train','early','cal','test'],[train,early,cal,test])},
              'models': {k:metrics(y[test],p) for k,p in predictions.items()},
              'paired': {k:paired(y[test],predictions['baseline_platt'],p,ts[test])
                         for k,p in predictions.items() if k.endswith('_platt') and k != 'baseline_platt'}}
    save(out/'metrics.json', report)
    print('DONE', fold, {k:v['logloss'] for k,v in report['models'].items()}, flush=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--stage', choices=['prepare', 'fold'], required=True)
    p.add_argument('--fold', choices=['may','jun','jul','aug'])
    p.add_argument('--features', type=Path)
    p.add_argument('--baseline-root', type=Path, default=ROOT / BASE_RUN)
    p.add_argument('--output-dir', type=Path, required=True)
    p.add_argument('--threads', type=int, default=1)
    args = p.parse_args()
    from threadpoolctl import threadpool_limits
    threadpool_limits(args.threads)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if any(args.output_dir.iterdir()):
        raise ValueError('Output directory must be empty')
    if args.stage == 'prepare':
        prepare(args.output_dir)
    else:
        train_fold(args.output_dir, args.features, args.fold, args.threads, args.baseline_root)


if __name__ == '__main__':
    main()
