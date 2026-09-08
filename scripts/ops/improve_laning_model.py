#!/usr/bin/env python3
"""Fixed v2 candidates; validation selection before a new-ID historical confirmation."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import numpy as np
from catboost import CatBoostClassifier
from scipy.optimize import minimize_scalar

from base.laning_model import LANE_NAMES, LaningModel, build_history, lane_features
from scripts.ops.train_laning_model import atomic_json, atomic_npz, metrics, sample_rows, splits


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def temperature_scale(probabilities, temperature):
    logits = np.log(np.maximum(probabilities, 1e-15)) / temperature
    logits -= logits.max(axis=-1, keepdims=True)
    exp = np.exp(logits)
    return exp / exp.sum(axis=-1, keepdims=True)


def fit_temperature(y, probabilities):
    labels = y.reshape(-1)
    p = probabilities.reshape(-1, 5)
    def objective(t):
        scaled = temperature_scale(p, t)
        return float(-np.log(scaled[np.arange(len(labels)), labels]).mean())
    fit = minimize_scalar(objective, bounds=(0.6, 1.6), method='bounded')
    return float(fit.x) if fit.success and objective(fit.x) < objective(1) else 1.0


def calibration(y, probabilities):
    y, p = y.reshape(-1), probabilities.reshape(-1, 5)
    observed = np.eye(5)[y]
    def ece(confidence, target):
        bucket = np.minimum((confidence * 10).astype(int), 9)
        return float(sum(np.mean(bucket == b) * abs(confidence[bucket == b].mean() -
            target[bucket == b].mean()) for b in range(10) if np.any(bucket == b)))
    return {'brier_multiclass_sum': float(np.mean(np.sum((p - observed) ** 2, axis=1))),
            'top_label_ece10': ece(p.max(axis=1), p.argmax(axis=1) == y),
            'per_class_ece10': [ece(p[:, c], observed[:, c]) for c in range(5)]}


def paired_comparison(ts, y, baseline, candidate):
    labels = y.reshape(-1)
    row = np.arange(len(labels))
    delta = (np.log(np.maximum(baseline.reshape(-1, 5)[row, labels], 1e-15)) -
             np.log(np.maximum(candidate.reshape(-1, 5)[row, labels], 1e-15))).reshape(-1, 3)
    days, inv = np.unique(ts // 86400, return_inverse=True)
    counts = np.bincount(inv)
    draws = np.random.default_rng(265).integers(0, len(days), (2000, len(days)))
    def interval(values):
        sums = np.bincount(inv, weights=values)
        boot = sums[draws].sum(axis=1) / counts[draws].sum(axis=1)
        return {'candidate_minus_baseline_loss': float(values.mean()),
                'day_block_95ci': np.quantile(boot, [.025, .975]).tolist()}
    return {'days': len(days), 'overall': interval(delta.mean(axis=1)),
            'lanes': {name: interval(delta[:, lane]) for lane, name in enumerate(LANE_NAMES)}}


def partition_v2(corpus, previous, large, small, evaluation):
    rows, info = splits(corpus, large, evaluation)
    train, validation, _ = rows
    old_test_ids = previous['mid'][previous['test_offset']:]
    future = np.flatnonzero((corpus['ts'] >= info['cuts_unix'][1]) &
                            np.all(corpus['lane_labels'] >= 0, axis=1))
    fresh = future[~np.isin(corpus['mid'][future], old_test_ids)]
    test = sample_rows(fresh, corpus['mid'], evaluation)
    if len(test) < evaluation:
        raise ValueError('Insufficient unused future IDs for the predeclared confirmation')
    small_rows = sample_rows(train, corpus['mid'], small)
    positions = np.searchsorted(train, small_rows)
    assert np.array_equal(train[positions], small_rows)
    assert not np.intersect1d(corpus['mid'][test], old_test_ids).size
    selected = np.concatenate((train, validation, test))
    info.update(sampled_maps=[len(train), len(validation), len(test)], small_train_maps=len(small_rows),
                previous_test_ids_excluded=len(old_test_ids), available_unused_future_maps=len(fresh),
                confirmation='Unseen map IDs in the same historical period; not a new prospective period')
    return selected, positions, info


def run(args):
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    if (out / 'summary.json').exists():
        raise ValueError('Run already completed; preserve its sealed confirmation')
    def stage(name):
        print(f'stage={name} time={time.time():.0f}', flush=True)
        atomic_json(out / 'status.json', dict(state='RUNNING', stage=name, pid=os.getpid()))
    stage('prepare')
    source_hash = sha(ROOT / 'base/laning_model.py')
    old_summary = json.loads((args.baseline / 'summary.json').read_text())
    with np.load(args.corpus / 'rows.npz') as file:
        corpus = {key: file[key] for key in file.files}
    manifest = json.loads((args.corpus / 'manifest.json').read_text())
    if not manifest.get('complete') or manifest['rows'] != len(corpus['mid']):
        raise ValueError('Incomplete corpus')
    with np.load(args.baseline / 'selected_rows.npz') as file:
        previous = {key: file[key] for key in file.files}
    old_counts = old_summary['split']['sampled_maps']
    previous['test_offset'] = sum(old_counts[:2])
    selected, small_positions, split = partition_v2(corpus, previous, args.train_maps,
                                                   args.small_train_maps, args.eval_maps)
    ntrain, nval, ntest = split['sampled_maps']
    val = slice(ntrain, ntrain + nval)
    test = slice(ntrain + nval, None)
    candidate_specs = {'longer': dict(context=False, maps=len(small_positions), delay=0),
                       'context_recent': dict(context=True, maps=ntrain, delay=3600)}
    plan = dict(split=split, iterations=args.iterations, depth=6, candidates=candidate_specs,
        recent_window_seconds=30 * 86400, feature_source_sha256=source_hash,
        trainer_source_sha256=sha(__file__),
        shared_trainer_source_sha256=sha(ROOT / 'scripts/ops/train_laning_model.py'),
        corpus_manifest_sha256=sha(args.corpus / 'manifest.json'), baseline_summary_sha256=sha(args.baseline / 'summary.json'),
        selected_ids_sha256=hashlib.sha256(corpus['mid'][selected].tobytes()).hexdigest(),
        selection='Minimum raw validation loss among v1 and two fixed candidates; temperature on validation only',
        confirmation_gate='Selected calibrated candidate day-bootstrap loss CI upper bound <0 vs v1',
        limitations=['Post-match role annotations', 'Ingestion delay modeled, not observed',
                     'New IDs in an already observed historical period, not prospective evidence'])
    if (out / 'plan.json').exists() and json.loads((out / 'plan.json').read_text()) != plan:
        raise ValueError('Plan or feature code changed: choose a fresh run directory')
    atomic_json(out / 'plan.json', plan)
    atomic_npz(out / 'reserved_rows.npz', indices=selected, mid=corpus['mid'][selected],
               ts=corpus['ts'][selected], small_positions=small_positions)
    inputs_path = out / 'inputs.npz'
    if inputs_path.exists():
        with np.load(inputs_path) as file:
            data = {key: file[key] for key in file.files}
        np.testing.assert_array_equal(data['mid'], corpus['mid'][selected])
        np.testing.assert_array_equal(data['labels'], corpus['lane_labels'][selected])
    else:
        stage('history_alltime')
        h0 = build_history(corpus, selected)
        common, current, old = np.intersect1d(selected, previous['indices'], return_indices=True)
        np.testing.assert_array_equal(h0[current], previous['history'][old])
        stage('history_delayed_recent')
        hc = build_history(corpus, selected, availability_delay_seconds=3600,
                           recent_window_seconds=30 * 86400)
        stage('history_delay15_validation')
        h15 = build_history(corpus, selected[val], availability_delay_seconds=900)
        data = dict(heroes=corpus['heroes'][selected], labels=corpus['lane_labels'][selected],
                    ts=corpus['ts'][selected], mid=corpus['mid'][selected], h0=h0, hc=hc, h15=h15)
        atomic_npz(inputs_path, **data)
    del corpus, previous
    yval = data['labels'][val]
    baseline_model = LaningModel.load(args.baseline, True)
    pval = {'v1': baseline_model.predict_proba(data['heroes'][val], data['h0'][val])}
    if abs(metrics(yval, pval['v1'])['logloss5'] - old_summary['validation']['history']['logloss5']) > 1e-10:
        raise ValueError('V1 validation reproduction failed')
    stage('validation_controls')
    control = {'original': metrics(yval, pval['v1'])}
    for name, history in [('delay15', data['h15']), ('delay60', data['hc'][val, :, :6])]:
        control[name] = metrics(yval, baseline_model.predict_proba(data['heroes'][val], history))
    shuffled = data['h0'][val].copy()
    days = data['ts'][val] // 86400
    rng = np.random.default_rng(265)
    for day in np.unique(days):
        take = np.flatnonzero(days == day)
        shuffled[take] = shuffled[rng.permutation(take)]
    control['history_permuted_within_day'] = metrics(yval,
        baseline_model.predict_proba(data['heroes'][val], shuffled))
    atomic_json(out / 'validation_controls.json', control)
    del shuffled
    validation = {'v1': metrics(yval, pval['v1'])}
    paths = {'v1': args.baseline / 'history.cbm'}
    for name, spec in candidate_specs.items():
        stage('fit_' + name)
        directory = out / name
        directory.mkdir(exist_ok=True)
        paths[name] = directory / 'history.cbm'
        cached = directory / 'validation.npz'
        if paths[name].exists() and cached.exists():
            with np.load(cached) as file:
                pval[name] = file['probabilities']
        else:
            train = np.arange(ntrain) if spec['context'] else small_positions
            rows = np.concatenate((train, np.arange(ntrain, ntrain + nval)))
            hist = data['hc'] if spec['context'] else data['h0']
            x = lane_features(data['heroes'][rows], hist[rows], context=spec['context'])
            categories = list(x.select_dtypes(include=['object', 'string', 'category']).columns)
            metadata = {'laning_feature_set': 'context_recent_v1' if spec['context'] else 'legacy',
                        'laning_temperature': '1', 'laning_history_delay_seconds': str(spec['delay'])}
            if spec['context']:
                metadata['laning_recent_window_seconds'] = str(30 * 86400)
            model = CatBoostClassifier(iterations=args.iterations, depth=6, learning_rate=.08,
                loss_function='MultiClass', thread_count=args.threads, random_seed=42,
                l2_leaf_reg=5, allow_writing_files=False, one_hot_max_size=255, max_ctr_complexity=1,
                metadata=metadata)
            boundary = len(train) * 3
            model.fit(x.iloc[:boundary], data['labels'][train].reshape(-1), cat_features=categories,
                eval_set=(x.iloc[boundary:], yval.reshape(-1)), early_stopping_rounds=60, verbose=100)
            pval[name] = model.predict_proba(x.iloc[boundary:]).reshape(-1, 3, 5)
            model.save_model(str(paths[name]) + '.tmp')
            os.replace(str(paths[name]) + '.tmp', paths[name])
            atomic_npz(cached, probabilities=pval[name])
            del x, model
        validation[name] = metrics(yval, pval[name])
        model = LaningModel.load(directory, True)
        history = data['hc'] if spec['context'] else data['h0']
        np.testing.assert_allclose(model.predict_proba(data['heroes'][val][:32], history[val][:32]),
                                   pval[name][:32], atol=1e-12, rtol=0)
        validation[name]['trees'] = model.model.tree_count_
        atomic_json(directory / 'validation.json', validation[name])
    winner = min(validation, key=lambda name: validation[name]['logloss5'])
    temperature = fit_temperature(yval, pval[winner])
    selection = dict(selected=winner, temperature=temperature, validation=validation,
        calibration_before=calibration(yval, pval[winner]),
        calibration_after=calibration(yval, temperature_scale(pval[winner], temperature)),
        selected_calibrated_validation_loss=metrics(yval, temperature_scale(pval[winner], temperature))['logloss5'])
    atomic_json(out / 'selection.json', selection)
    selected_model = CatBoostClassifier()
    selected_model.load_model(str(paths[winner]))
    selected_model.get_metadata()['laning_temperature'] = str(temperature)
    destination = out / 'selected'
    destination.mkdir(exist_ok=True)
    selected_model.save_model(str(destination / 'history.cbm.tmp'))
    os.replace(destination / 'history.cbm.tmp', destination / 'history.cbm')
    # Selection and calibration are now frozen, before any confirmation metric.
    stage('sealed_confirmation')
    predictions = {}
    for name, path in paths.items():
        history = data['hc'] if name == 'context_recent' else data['h0']
        predictions[name] = LaningModel.load(path.parent, True).predict_proba(data['heroes'][test], history[test])
    predictions['selected_calibrated'] = temperature_scale(predictions[winner], temperature)
    history = data['hc'] if winner == 'context_recent' else data['h0']
    loaded = LaningModel.load(destination, True).predict_proba(data['heroes'][test][:64], history[test][:64])
    delta = float(np.max(np.abs(loaded - predictions['selected_calibrated'][:64])))
    if delta > 1e-12:
        raise ValueError('Selected serialized inference mismatch')
    y = data['labels'][test]
    results = {name: {'overall': metrics(y, p), 'calibration': calibration(y, p),
        'lanes': {lane_name: metrics(y[:, lane], p[:, lane]) for lane, lane_name in enumerate(LANE_NAMES)}}
        for name, p in predictions.items()}
    paired = paired_comparison(data['ts'][test], y, predictions['v1'], predictions['selected_calibrated'])
    atomic_npz(out / 'confirmation_predictions.npz', mid=data['mid'][test], ts=data['ts'][test],
               labels=y, **predictions)
    summary = dict(complete=True, selected=winner, temperature=temperature, full_refit=False,
        improvement_confirmed=paired['overall']['day_block_95ci'][1] < 0, split=split,
        validation=selection, controls=control, confirmation=results, paired_loss=paired,
        reload_max_delta=delta, plan_sha256=sha(out / 'plan.json'),
        source_sha256={str(p.relative_to(ROOT)): sha(p) for p in (Path(__file__), ROOT / 'base/laning_model.py')},
        limitations=plan['limitations'])
    atomic_json(out / 'summary.json', summary)
    atomic_json(out / 'status.json', dict(state='DONE', stage='complete', pid=os.getpid()))
    print(json.dumps(dict(state='DONE', selected=winner, paired=paired['overall'])), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--corpus', type=Path, required=True)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--train-maps', type=int, default=800000)
    parser.add_argument('--small-train-maps', type=int, default=400000)
    parser.add_argument('--eval-maps', type=int, default=100000)
    parser.add_argument('--iterations', type=int, default=900)
    parser.add_argument('--threads', type=int, default=4)
    args = parser.parse_args()
    try:
        run(args)
    except Exception as exc:
        atomic_json(args.output_dir / 'status.json', dict(state='FAIL', error=str(exc), pid=os.getpid()))
        raise
