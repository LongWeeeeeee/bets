#!/usr/bin/env python3
"""Frozen temporal selection and terminal evaluation of causal prematch models.

The input is a previously built end-time-safe feature matrix, not current
production snapshots. Search touches June/early July only. Terminal evaluation
uses July 15-31 calibration and August-September test, once after recipe freeze.
"""
import argparse
import datetime as dt
import hashlib
import json
import time
from pathlib import Path

import joblib
import numpy as np
from lightgbm import LGBMClassifier
from scipy import sparse
from scipy.special import expit, logit
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from threadpoolctl import threadpool_limits


def epoch(date):
    return int(dt.datetime.fromisoformat(date).replace(tzinfo=dt.timezone.utc).timestamp())


RECIPES = {
    'linear_all': dict(learner='linear', days=0, half_life=365, draft=True),
    'linear_recent': dict(learner='linear', days=730, half_life=180, draft=True),
    'lgb15_all': dict(learner='lgb', days=0, half_life=365, draft=True, leaves=15, trees=350),
    'lgb31_all': dict(learner='lgb', days=0, half_life=365, draft=True, leaves=31, trees=450),
    'lgb15_recent': dict(learner='lgb', days=730, half_life=180, draft=True, leaves=15, trees=450),
    'lgb31_recent': dict(learner='lgb', days=730, half_life=180, draft=True, leaves=31, trees=550),
    'lgb31_short': dict(learner='lgb', days=365, half_life=90, draft=True, leaves=31, trees=450),
    'lgb63_recent': dict(learner='lgb', days=730, half_life=180, draft=True, leaves=63, trees=450),
    'lgb31_history': dict(learner='lgb', days=730, half_life=180, draft=False, leaves=31, trees=550),
    'linear_short': dict(learner='linear', days=180, half_life=60, draft=True),
    'lgb31_fresh': dict(learner='lgb', days=90, half_life=45, draft=True, leaves=31, trees=350, min_child=30),
    'lgb63_fresh': dict(learner='lgb', days=180, half_life=60, draft=True, leaves=63, trees=450, min_child=30),
    'lgb127_recent': dict(learner='lgb', days=730, half_life=180, draft=True, leaves=127, trees=550, min_child=30),
    'roster_all': dict(learner='roster', C=.1, days=0, half_life=365, draft=True),
    'roster_recent': dict(learner='roster', C=.1, days=730, half_life=180, draft=True),
}

ROSTER_NUMERIC = {'k24_rating_diff': 1 / 400, 'team_rating_diff': 1 / 400,
                  'recent_winrate_diff': 1., 'player_hero_winrate_diff': 1.}


def load_data(args):
    with np.load(args.dataset, allow_pickle=False) as z:
        data = {k: z[k] for k in z.files}
    for key, expected in {'label_side': 'radiant', 'hero_slots': 'radiant_1_5,dire_1_5',
                          'k24_direction': 'radiant_minus_dire',
                          'role_assignment': 'past_completed_player_roles'}.items():
        assert str(data[key].item()) == expected, f'incompatible side semantics: {key}'
    names = data['feature_names'].astype(str).tolist()
    x = data['X'].astype(np.float32)
    x[~np.isfinite(x)] = np.nan
    blocks = [x]
    for path in args.draft:
        with np.load(path) as z:
            order = np.argsort(z['mid'])
            ids = z['mid'][order]
            pos = np.searchsorted(ids, data['mid'])
            assert (pos < len(ids)).all() and np.array_equal(ids[pos], data['mid'])
            p = z['p'][order][pos]
            snapshot = z['snapshot'][order][pos]
            assert (snapshot[np.isfinite(p)] <= data['ts'][np.isfinite(p)]).all()
            values = logit(np.clip(p, .002, .998)).astype(np.float32)
            blocks.extend([values[:, None], np.isfinite(p).astype(np.float32)[:, None]])
            names.extend([f'public_{len(names)}_logit', f'public_{len(names)}_available'])
    data['numeric_count'] = len(names)
    blocks.extend([data['heroes'].astype(np.float32), data['league'].astype(np.float32)[:, None]])
    names += [f'hero_pos_{i}' for i in range(10)] + ['league_category']
    data['X'] = np.column_stack(blocks).astype(np.float32)
    data['names'] = names
    data['public_columns'] = [i for i, name in enumerate(names) if name.startswith('public_')]
    assert len(np.unique(data['mid'])) == len(data['mid'])
    assert np.isin(data['y'], [0, 1]).all()
    assert (data['end'] > data['ts']).all()
    return data


def indices(data, begin, end):
    return np.flatnonzero((data['ts'] >= begin) & (data['ts'] < end)
                          & (data['league'] > 0) & data['elo_eligible'].astype(bool))


def completed_indices(data, begin, end):
    rows = indices(data, begin, end)
    return rows[data['end'][rows] < end-3600]


def train_indices(data, cutoff, future_rows, days):
    mask = data['end'] < cutoff-3600
    if days:
        mask &= data['ts'] >= cutoff-days*86400
    # Maps crossing a split may not train and validate the same series.
    series = np.unique(data['series'][future_rows])
    series = series[series > 0]
    mask &= ~np.isin(data['series'], series)
    return np.flatnonzero(mask)


def _roster_numeric_columns(data):
    names = {name: i for i, name in enumerate(data['names'])}
    missing = set(ROSTER_NUMERIC) - set(names)
    if missing:
        raise ValueError(f'roster learner missing numeric features: {sorted(missing)}')
    chosen = [(names[name], scale) for name, scale in ROSTER_NUMERIC.items()]
    chosen += [(i, 1.) for i, name in enumerate(data['names'])
               if name.startswith('public_') and name.endswith('_logit')]
    return chosen


def _roster_vocabulary(data, rows):
    accounts = sorted({int(a) for a in data['accounts'][rows].ravel() if a > 0})
    heroes = sorted({(int(hero), slot % 5) for slot in range(10)
                     for hero in data['heroes'][rows, slot] if hero > 0})
    return {value: i for i, value in enumerate(accounts)}, {value: i for i, value in enumerate(heroes)}


def _roster_matrix(data, rows, account_vocab, hero_vocab, numeric):
    rows = np.asarray(rows, dtype=int)
    rr, cc, values = [], [], []
    hero_offset = len(account_vocab)
    for local_row, row in enumerate(rows):
        for side in range(2):
            sign = 1. if side == 0 else -1.
            for slot in range(side * 5, side * 5 + 5):
                account = account_vocab.get(int(data['accounts'][row, slot]))
                if account is not None:
                    rr.append(local_row); cc.append(account); values.append(sign)
                hero = hero_vocab.get((int(data['heroes'][row, slot]), slot % 5))
                if hero is not None:
                    rr.append(local_row); cc.append(hero_offset + hero); values.append(sign)
    numeric_offset = hero_offset + len(hero_vocab)
    if numeric:
        numeric_values = np.nan_to_num(data['X'][np.ix_(rows, [i for i, _ in numeric])], nan=0.)
        numeric_values *= np.asarray([scale for _, scale in numeric], dtype=np.float32)
        nr, nc = np.nonzero(numeric_values)
        rr.extend(nr.tolist()); cc.extend((numeric_offset + nc).tolist()); values.extend(numeric_values[nr, nc].tolist())
    return sparse.csr_matrix((values, (rr, cc)), shape=(len(rows), numeric_offset + len(numeric)), dtype=np.float32)


def fit_model(data, recipe, cutoff, future_rows, threads):
    rows = train_indices(data, cutoff, future_rows, recipe['days'])
    columns = list(range(data['X'].shape[1]))
    if not recipe['draft']:
        columns = [c for c in columns if c not in data['public_columns']]
    if recipe['learner'] == 'linear':
        columns = [c for c in columns if c < data['numeric_count']]
    weights = np.maximum(.01, 2.**(-(cutoff-data['ts'][rows])/86400/recipe['half_life']))
    weights *= np.where(data['league'][rows] > 0, 1., .25)
    weights /= weights.mean()
    if recipe['learner'] == 'roster':
        account_vocab, hero_vocab = _roster_vocabulary(data, rows)
        numeric = _roster_numeric_columns(data)
        x = _roster_matrix(data, rows, account_vocab, hero_vocab, numeric)
        estimator = LogisticRegression(C=recipe['C'], max_iter=200, solver='liblinear', random_state=20260921)
        estimator.fit(x, data['y'][rows], sample_weight=weights)
        model = {'kind': 'roster', 'model': estimator, 'account_vocab': account_vocab,
                 'hero_vocab': hero_vocab, 'numeric': numeric}
        columns = None
    else:
        x = data['X'][np.ix_(rows, columns)]
    if recipe['learner'] == 'linear':
        model = make_pipeline(SimpleImputer(strategy='median', add_indicator=True), StandardScaler(),
                              LogisticRegression(C=.1, max_iter=300, solver='lbfgs'))
        model.fit(x, data['y'][rows], logisticregression__sample_weight=weights)
    elif recipe['learner'] == 'lgb':
        model = LGBMClassifier(n_estimators=recipe['trees'], learning_rate=.04,
                              num_leaves=recipe['leaves'], max_bin=63,
                              min_child_samples=recipe.get('min_child', 100),
                              reg_lambda=20., colsample_bytree=.85, verbosity=-1,
                              n_jobs=threads, random_state=20260921, deterministic=True,
                              force_col_wise=True)
        cat = [i for i, c in enumerate(columns) if c >= data['numeric_count']]
        model.fit(x, data['y'][rows], sample_weight=weights, categorical_feature=cat)
    audit = {'train_n': len(rows), 'train_max_end': int(data['end'][rows].max()),
             'test_min_start': int(data['ts'][future_rows].min()) if len(future_rows) else None,
             'train_series_overlap': int(np.isin(data['series'][rows],
                                                  data['series'][future_rows][data['series'][future_rows] > 0]).sum())}
    if recipe['learner'] == 'roster':
        audit.update(feature_count=x.shape[1], n_iter_=estimator.n_iter_.astype(int).tolist())
        model['feature_names'] = list(data['names'])
    else:
        model._prematch_feature_names = list(data['names'])
    return model, columns, audit


def predict(model, columns, data, rows):
    expected = (model['feature_names'] if isinstance(model, dict)
                else model._prematch_feature_names)
    if list(data['names']) != expected:
        raise ValueError('prediction feature schema differs from fitted model')
    if isinstance(model, dict) and model.get('kind') == 'roster':
        x = _roster_matrix(data, rows, model['account_vocab'], model['hero_vocab'], model['numeric'])
        return model['model'].predict_proba(x)[:, 1]
    return model.predict_proba(data['X'][np.ix_(rows, columns)])[:, 1]


def metrics(y, p):
    return {'n': len(y), 'wins': int(((p >= .5) == y).sum()), 'accuracy': float(accuracy_score(y, p >= .5)),
            'logloss': float(log_loss(y, p, labels=[0, 1])), 'brier': float(brier_score_loss(y, p)),
            'auc': float(roc_auc_score(y, p)) if len(np.unique(y)) == 2 else None}


def calibrator(y, p):
    model = LogisticRegression(C=1., solver='lbfgs')
    model.fit(logit(np.clip(p, 1e-5, 1-1e-5))[:, None], y)
    return model


def paired_report(y, p, baseline, series, mid):
    """Paired accuracy/LL differences, resampling whole series, never maps."""
    a, b = (p >= .5) == y, (baseline >= .5) == y
    disagreement = (p >= .5) != (baseline >= .5)
    keys = np.where(series > 0, series, -mid)
    _, group = np.unique(keys, return_inverse=True)
    counts = np.bincount(group)
    gain = np.bincount(group, weights=a.astype(float)-b)
    clipped_p, clipped_b = np.clip(p, 1e-8, 1-1e-8), np.clip(baseline, 1e-8, 1-1e-8)
    loss_p = -y*np.log(clipped_p)-(1-y)*np.log1p(-clipped_p)
    loss_b = -y*np.log(clipped_b)-(1-y)*np.log1p(-clipped_b)
    loss_gain = np.bincount(group, weights=loss_b-loss_p)
    rng = np.random.default_rng(20260921)
    boot_acc, boot_loss = [], []
    for offset in range(0, 4000, 200):
        sample = rng.integers(0, len(counts), (min(200, 4000-offset), len(counts)))
        denominator = counts[sample].sum(axis=1)
        boot_acc.extend(gain[sample].sum(axis=1)/denominator)
        boot_loss.extend(loss_gain[sample].sum(axis=1)/denominator)
    ci = np.quantile(boot_acc, [.025, .975])
    return {'n': len(y), 'clusters': len(counts), 'ml_only_correct': int((a & ~b).sum()),
            'elo_only_correct': int((~a & b).sum()), 'both_correct': int((a & b).sum()),
            'both_wrong': int((~a & ~b).sum()), 'disagreements': int(disagreement.sum()),
            'ml_disagreement_wins': int(a[disagreement].sum()),
            'elo_disagreement_wins': int(b[disagreement].sum()),
            'accuracy_gain_pp': float((a.mean()-b.mean())*100),
            'accuracy_gain_cluster95_pp': (ci*100).tolist(),
            'logloss_gain': float((loss_b-loss_p).mean()),
            'logloss_gain_cluster95': np.quantile(boot_loss, [.025, .975]).tolist(),
            'strong_criterion_pass': bool(a.mean()-b.mean() >= .05 and ci[0] > 0
                                          and loss_p.mean() < loss_b.mean())}


def search(data, args):
    recipe = RECIPES[args.recipe]
    outputs, reports = [], []
    for begin, end in [('2026-06-01', '2026-07-01'), ('2026-07-01', '2026-07-15')]:
        rows = completed_indices(data, epoch(begin), epoch(end))
        model, columns, audit = fit_model(data, recipe, epoch(begin), rows, args.threads)
        p = predict(model, columns, data, rows)
        baseline = expit(data['k24_diff'][rows]*np.log(10.)/400.)
        reports.append({'begin': begin, 'end': end, 'audit': audit,
                        'model': metrics(data['y'][rows], p), 'k24': metrics(data['y'][rows], baseline)})
        outputs.append((rows, p))
        print(json.dumps(reports[-1]), flush=True)
    rows, p = np.concatenate([r for r, p in outputs]), np.concatenate([p for r, p in outputs])
    np.savez_compressed(args.output_dir/'validation.npz', mid=data['mid'][rows], y=data['y'][rows], p=p,
                        k24=expit(data['k24_diff'][rows]*np.log(10.)/400.), series=data['series'][rows])
    return {'recipe_id': args.recipe, 'recipe': recipe, 'folds': reports, 'pooled': metrics(data['y'][rows], p)}


def terminal(data, args):
    recipes = args.recipe.split(',')
    verify_selection(args, recipes)
    cal = indices(data, epoch('2026-07-15'), epoch('2026-08-01'))
    test = completed_indices(data, epoch('2026-08-01'), epoch('2026-09-21'))
    test_series = data['series'][test]
    cal = cal[(data['end'][cal] < epoch('2026-08-01')-3600)
              & ~np.isin(data['series'][cal], test_series[test_series > 0])]
    models, cp, tp, audits = [], [], [], []
    for name in recipes:
        model, columns, audit = fit_model(data, RECIPES[name], epoch('2026-07-15'), np.r_[cal, test], args.threads)
        models.append((name, model, columns))
        cp.append(predict(model, columns, data, cal))
        tp.append(predict(model, columns, data, test))
        audits.append(audit)
    cp, raw = np.mean(cp, axis=0), np.mean(tp, axis=0)
    calibration = calibrator(data['y'][cal], cp)
    p = calibration.predict_proba(logit(np.clip(raw, 1e-5, 1-1e-5))[:, None])[:, 1]
    elo_raw = expit(data['k24_diff'][test]*np.log(10.)/400.)
    elo_cal = calibrator(data['y'][cal], expit(data['k24_diff'][cal]*np.log(10.)/400.))
    elo = elo_cal.predict_proba((data['k24_diff'][test]*np.log(10.)/400.)[:, None])[:, 1]
    bundle = {'models': models, 'calibrator': calibration, 'feature_names': data['names'],
              'fit_cutoff': epoch('2026-07-15'), 'calibration_end': epoch('2026-08-01')}
    joblib.dump(bundle, args.output_dir/'evaluation_model.joblib')
    np.savez_compressed(args.output_dir/'terminal.npz', mid=data['mid'][test], y=data['y'][test], p=p,
                        raw=raw, k24=elo_raw, k24_calibrated=elo, series=data['series'][test],
                        ts=data['ts'][test], league=data['league'][test])
    replay = np.mean([predict(m, c, data, test) for _, m, c in joblib.load(args.output_dir/'evaluation_model.joblib')['models']], axis=0)
    assert np.max(np.abs(raw-replay)) < 1e-12
    leagues, counts = np.unique(data['league'][test], return_counts=True)
    largest = leagues[counts.argmax()]
    rest = data['league'][test] != largest
    return {'recipe_ids': recipes, 'audit': audits, 'calibration_n': len(cal),
            'model': metrics(data['y'][test], p), 'raw_model': metrics(data['y'][test], raw),
            'k24': metrics(data['y'][test], elo_raw), 'calibrated_k24': metrics(data['y'][test], elo),
            'paired_raw_k24': paired_report(data['y'][test], p, elo_raw, data['series'][test], data['mid'][test]),
            'paired_calibrated_k24': paired_report(data['y'][test], p, elo, data['series'][test], data['mid'][test]),
            'largest_league': int(largest),
            'without_largest_league': (paired_report(data['y'][test][rest], p[rest], elo[rest],
                                                    data['series'][test][rest], data['mid'][test][rest])
                                       if rest.any() else None),
            'k24_exact_ties': int((elo_raw == .5).sum()),
            'by_league': {str(league): {'model': metrics(data['y'][test][data['league'][test] == league], p[data['league'][test] == league]),
                                      'k24': metrics(data['y'][test][data['league'][test] == league], elo_raw[data['league'][test] == league])}
                          for league in np.unique(data['league'][test]) if (data['league'][test] == league).sum() >= 20},
            'artifact_replay_max_error': float(np.max(np.abs(raw-replay)))}


def refit(data, args):
    """Only run after frozen terminal result; retrain selected recipe on all known labels."""
    verify_selection(args, args.recipe.split(','))
    if args.terminal_report is None:
        raise ValueError('refit requires the completed frozen terminal report')
    terminal_report = json.loads(args.terminal_report.read_text())
    assert terminal_report['mode'] == 'terminal'
    assert terminal_report['recipe_ids'] == args.recipe.split(',')
    assert terminal_report['dataset_sha256'] == hashlib.sha256(args.dataset.read_bytes()).hexdigest()
    assert terminal_report['selection_sha256'] == hashlib.sha256(args.selection.read_bytes()).hexdigest()
    cutoff = int(data['end'].max())+3601
    models, audits = [], []
    for name in args.recipe.split(','):
        model, columns, audit = fit_model(data, RECIPES[name], cutoff, np.array([], dtype=int), args.threads)
        models.append((name, model, columns)); audits.append(audit)
    bundle = {'models': models, 'calibrator': None, 'feature_names': data['names'],
              'fit_cutoff': cutoff,
              'scope': 'OFFLINE_CANDIDATE; raw scores; post-refit calibration and prospective quality unmeasured'}
    joblib.dump(bundle, args.output_dir/'candidate.joblib')
    rows = np.arange(max(0, len(data['y'])-32), len(data['y']))
    p = np.mean([predict(m, c, data, rows) for _, m, c in models], axis=0)
    saved = joblib.load(args.output_dir/'candidate.joblib')
    check = np.mean([predict(m, c, data, rows) for _, m, c in saved['models']], axis=0)
    assert np.max(abs(p-check)) < 1e-12
    np.savez_compressed(args.output_dir/'smoke_rows.npz', X=data['X'][rows],
                        mid=data['mid'][rows], accounts=data['accounts'][rows],
                        heroes=data['heroes'][rows], names=np.asarray(data['names']), expected_raw=p)
    return {'recipe_ids': args.recipe.split(','), 'audit': audits, 'control_replay_max_error': float(np.max(abs(p-check))),
            'last_known_end': int(data['end'].max()), 'scope': bundle['scope']}


def verify_selection(args, recipes):
    if args.selection is None:
        raise ValueError('terminal requires a frozen selection manifest')
    frozen = json.loads(args.selection.read_text())
    assert frozen['recipe_ids'] == recipes, 'terminal recipe differs from frozen selection'
    assert frozen['recipes'] == {r: RECIPES[r] for r in recipes}, 'recipe parameters changed after freeze'
    assert frozen['dataset_sha256'] == hashlib.sha256(args.dataset.read_bytes()).hexdigest()
    assert frozen['draft_sha256'] == [hashlib.sha256(p.read_bytes()).hexdigest() for p in args.draft]
    assert frozen['selection_data_end'] == '2026-07-15'


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--dataset', type=Path, required=True)
    ap.add_argument('--draft', type=Path, action='append', default=[])
    ap.add_argument('--output-dir', type=Path, required=True)
    ap.add_argument('--recipe', required=True)
    ap.add_argument('--mode', choices=['search', 'terminal', 'refit'], required=True)
    ap.add_argument('--selection', type=Path)
    ap.add_argument('--terminal-report', type=Path)
    ap.add_argument('--threads', type=int, default=1)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if (args.output_dir/'metrics.json').exists():
        raise FileExistsError('refusing to overwrite a completed experiment')
    started = time.time()
    data = load_data(args)
    with threadpool_limits(limits=args.threads):
        result = {'search': search, 'terminal': terminal, 'refit': refit}[args.mode](data, args)
    result.update(elapsed_seconds=time.time()-started, features=data['names'], mode=args.mode,
                  dataset_sha256=hashlib.sha256(args.dataset.read_bytes()).hexdigest(),
                  draft_sha256=[hashlib.sha256(p.read_bytes()).hexdigest() for p in args.draft],
                  selection_sha256=hashlib.sha256(args.selection.read_bytes()).hexdigest() if args.selection else None)
    (args.output_dir/'metrics.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps({k: v for k, v in result.items() if k != 'features'}), flush=True)


if __name__ == '__main__':
    main()
