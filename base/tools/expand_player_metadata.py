"""Bounded DLTV collection and frozen-split metadata experiments; no serving writes."""
import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import re
import time

import numpy as np

from base.player_metadata import PlayerHistory, calendar_keys
from base.tools.player_metadata import collect, load_snapshot, write_new


def match_urls(html):
    return sorted(set(re.findall(r'https://dltv\.org/matches/\d+/[a-zA-Z0-9_-]+', html)))


def dump(path, value):
    write_new(path, json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def collect_dates(dates, output, limit=150, delay=1., exclude_collection=None):
    import requests
    if not 1 <= limit <= 300 or delay < 1:
        raise ValueError('limit must be 1..300 and delay >= 1 second')
    dates = sorted({date.fromisoformat(d).isoformat() for d in dates})
    if not 1 <= len(dates) <= 10:
        raise ValueError('need 1..10 discovery dates')
    excluded = set()
    if exclude_collection is not None:
        prior = json.loads(Path(exclude_collection).read_text())
        excluded.update(row['url'] for row in prior['failures'])
        excluded.update(load_snapshot(Path(row['snapshot']))['source_url'] for row in prior['results'])
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    urls = set()
    for day in dates:
        url = 'https://dltv.org/results?date=' + day
        try:
            response = requests.get(url, timeout=30, allow_redirects=False)
            response.raise_for_status()
            if response.status_code != 200:
                raise ValueError('unexpected discovery redirect')
        except (requests.RequestException, ValueError) as error:
            dump(output / 'collection.json', {'phase': 'discovery', 'completed': 0, 'results': [],
                 'failures': [{'url': url, 'error': str(error),
                              'status': getattr(getattr(error, 'response', None), 'status_code', None)}],
                 'stopped': 'discovery failed'})
            raise
        write_new(output / 'discovery' / (day + '.html'), response.text)
        urls.update(match_urls(response.text))
        time.sleep(delay)
    discovered = len(urls)
    urls = sorted(urls - excluded)
    dump(output / 'urls.json', {'dates': dates, 'discovered': discovered,
                              'excluded': len(excluded), 'selected': urls[:limit]})
    results, failures, stopped = [], [], None
    network_failures = 0
    for url in urls[:limit]:
        try:
            result = collect(url, output / 'snapshots')
            results.append(result)
            print(json.dumps({'done': len(results), 'url': url, **result}), flush=True)
        except (requests.RequestException, ValueError) as error:
            status = getattr(getattr(error, 'response', None), 'status_code', None)
            failures.append({'url': url, 'error': str(error), 'status': status})
            print(json.dumps({'failure': failures[-1]}), flush=True)
            network_failures += isinstance(error, requests.RequestException)
            if status in (403, 429) or network_failures >= 5:
                stopped = 'source blocked or error budget reached'
                break
        time.sleep(delay)
    report = {'selected': len(urls[:limit]), 'completed': len(results), 'failures': failures,
              'stopped': stopped, 'results': results}
    dump(output / 'collection.json', report)
    if stopped:
        raise RuntimeError(stopped)
    return report


def build_metadata(rows, history):
    """Reuse known account/position ordering, with no outcome-dependent selection."""
    accounts, ts = rows['accounts'], rows['ts']
    if accounts.shape != (len(ts), 10) or len(np.unique(rows['mids'])) != len(ts):
        raise ValueError('invalid account matrix or duplicate map IDs')
    names, matrix, diagnostics = None, [], []
    months = {month for _, month in history.monthly}
    weeks = {week for _, week in history.weekly}
    template = history.features(range(1, 6), range(6, 11), asof=1,
                                time_policy='calendar_period')
    names = sorted(template['features'])
    for mid, when, lineup in zip(rows['mids'], ts, accounts):
        month, week = calendar_keys(float(when))
        if month not in months and week not in weeks:
            matrix.append([0.] * len(names))
            continue
        result = history.features(lineup[:5], lineup[5:], asof=float(when),
                                  time_policy='calendar_period')
        matrix.append([result['features'][name] for name in names])
        details = [p for side in result['diagnostics'] for p in side]
        diagnostics.append({'mid': int(mid), 'earnings_players': sum(p['earnings_known'] for p in details),
                            'rank_players': sum(p['rank_status'] == 'usable' for p in details),
                            'earnings_backfilled': sum(p['earnings_backfilled'] for p in details),
                            'rank_backfilled': sum(p['rank_backfilled'] for p in details)})
    return np.asarray(matrix, dtype=float), names, diagnostics


def verify_plan(plan, required):
    for key in required:
        if plan[key] not in plan['sha256']:
            raise ValueError('missing required input hash: ' + key)
    for path, sha in plan['sha256'].items():
        if hashlib.sha256(Path(path).read_bytes()).hexdigest() != sha:
            raise ValueError('input hash mismatch: ' + path)


def prepare(plan_path, snapshot_dirs, output):
    from base.tools.refit_prematch_draft_component import atomic_npz
    plan = json.loads(Path(plan_path).read_text())
    verify_plan(plan, ('rows', 'matrix', 'split', 'reference', 'weights'))
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    paths = sorted({p for root in snapshot_dirs for p in Path(root).glob('*/snapshot.json')})
    if not paths:
        raise ValueError('no snapshots')
    snapshots = [load_snapshot(p) for p in paths]
    with np.load(plan['rows'], allow_pickle=False) as source:
        rows = dict(source)
    with np.load(plan['split'], allow_pickle=False) as source:
        split = dict(source)
    with np.load(plan['matrix'], allow_pickle=False) as source:
        matrix = dict(source)
    for key in ('mids', 'ts', 'y', 'sids'):
        if not np.array_equal(rows[key], matrix[key]):
            raise ValueError('row identity mismatch: ' + key)
    if not np.array_equal(rows['mids'], split['mids']):
        raise ValueError('split identity mismatch')
    features, names, diagnostics = build_metadata(rows, PlayerHistory(snapshots))
    atomic_npz(output / 'metadata.npz', {'mids': rows['mids'], 'X': features,
                                        'feature_names': np.asarray(names)})
    by_mid = {row['mid']: row for row in diagnostics}
    coverage = {}
    for key in ('train', 'test'):
        selected = [by_mid.get(int(mid), {}) for mid in rows['mids'][split[key]]]
        coverage[key] = {'maps': len(selected),
                         'earnings_any': sum(d.get('earnings_players', 0) > 0 for d in selected),
                         'earnings_full': sum(d.get('earnings_players', 0) == 10 for d in selected),
                         'earnings_slots': sum(d.get('earnings_players', 0) for d in selected),
                         'ranks_any': sum(d.get('rank_players', 0) > 0 for d in selected)}
    files = paths + [p.with_name('source.html') for p in paths]
    files += [Path(plan[k]) for k in ('rows', 'matrix', 'split', 'reference', 'weights')]
    report = {'policy': 'calendar_period', 'retrospective_assumption': True,
              'snapshots': len(paths), 'accounts': len(PlayerHistory(snapshots).records),
              'coverage': coverage, 'maps': diagnostics,
              'sha256': {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}}
    dump(output / 'coverage.json', report)
    return report


def augmented_fit(matrix, split, metadata, weights, c=.1):
    from base.tools.refit_prematch_draft_component import unpack_branches
    from base.tools.retrain_prematch_general import fit, probability
    if (not np.array_equal(matrix['mids'], metadata['mids'])
            or not np.array_equal(matrix['mids'], split['mids'])
            or metadata['X'].shape != (len(matrix['mids']), len(metadata['feature_names']))
            or not np.isfinite(metadata['X']).all()):
        raise ValueError('metadata identity/shape/value mismatch')
    train, test, routed = split['train'], split['test'], split['routed_branch']
    names = list(map(str, matrix['feature_names']))
    branches = unpack_branches(weights)
    prediction = np.zeros(int(test.sum()))
    output, details = {}, {}
    for name in ('full', 'no_org'):
        mask = train if name == 'full' else train & (routed == name)
        indices = [names.index(col) for col in branches[name].cols]
        # Constants are selected only from each branch's training rows.
        keep = np.std(metadata['X'][mask], axis=0) > 1e-9
        X = np.column_stack([matrix['X'][:, indices], metadata['X'][:, keep]])
        fitted, info = fit(X[mask], matrix['y'][mask], c)
        p = probability(X[test], fitted)
        selected = routed[test] == name
        prediction[selected] = p[selected]
        details[name] = {**info, 'train_maps': int(mask.sum()), 'added_features': int(keep.sum())}
        for key in ('mu', 'sd', 'coef', 'intercept'):
            output[name + '_' + key] = np.asarray(getattr(fitted, key))
        output[name + '_feature_names'] = np.asarray(branches[name].cols + list(metadata['feature_names'][keep]))
    if np.any(prediction <= 0):
        raise ValueError('unsupported routed branch')
    return prediction, output, details


def evaluate(plan_path, output, threads=1):
    from threadpoolctl import threadpool_limits
    from base.tools.compare_prematch_refits import check_alignment, metrics, paired_interval
    from base.tools.retrain_prematch_general import retrain
    from base.tools.refit_prematch_draft_component import atomic_npz
    plan = json.loads(Path(plan_path).read_text())
    verify_plan(plan, ('matrix', 'split', 'reference', 'weights', 'metadata'))
    def read(key):
        with np.load(plan[key], allow_pickle=False) as source:
            return dict(source)
    matrix, split, reference, weights, metadata = map(read, ('matrix', 'split', 'reference', 'weights', 'metadata'))
    check_alignment(matrix, split, reference)
    matrix['routed_branch'] = split['routed_branch']
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise ValueError('output directory not empty')
    test = split['test']
    with threadpool_limits(limits=threads):
        _, p, train, actual_test, _ = retrain(matrix, weights, cutoff=plan['test_cutoff'],
                                            embargo=plan['embargo_seconds'], c=plan['baseline_C'])
        baseline = np.where(split['routed_branch'][test] == 'full', p['full'], p['no_org'])
        if (not np.array_equal(train, split['train']) or not np.array_equal(test, actual_test)
                or np.max(np.abs(baseline-reference['candidate_routed'])) >= 1e-6):
            raise ValueError('baseline replay / split mismatch')
        candidate, arrays, optimization = augmented_fit(matrix, split, metadata, weights, plan['baseline_C'])
    y, series = matrix['y'][test], matrix['sids'][test]
    report = {'baseline': metrics(y, baseline), 'candidate': metrics(y, candidate),
              'paired_log_loss': paired_interval(y, baseline, candidate, series),
              'optimization': optimization, 'protocol': plan, 'production_admission': False,
              'limitations': ['Calendar-period approximation permits within-period future information.',
                              'Previously examined 38-map test; exploratory comparison only.',
                              'Current snapshots cover September only; older months remain missing.',
                              'Frozen serving baseline; production runtime not rechecked.']}
    joint = list(metadata['feature_names']).index('team_earnings_coverage_joint')
    covered = metadata['X'][test, joint] == 1
    report['fully_covered_test_maps'] = int(covered.sum())
    if covered.any():
        report['fully_covered_test'] = {'purpose': 'descriptive only; no selection',
                                        'baseline': metrics(y[covered], baseline[covered]),
                                        'candidate': metrics(y[covered], candidate[covered])}
    atomic_npz(output / 'research_weights.npz', arrays)
    atomic_npz(output / 'predictions.npz', {'mids': matrix['mids'][test], 'y': y,
                                          'sids': series, 'baseline': baseline, 'candidate': candidate})
    dump(output / 'summary.json', report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    collector = sub.add_parser('collect')
    collector.add_argument('--dates', nargs='+', required=True)
    collector.add_argument('--limit', type=int, default=150)
    collector.add_argument('--output', type=Path, required=True)
    collector.add_argument('--exclude-collection', type=Path)
    preparer = sub.add_parser('prepare')
    preparer.add_argument('--plan', type=Path, required=True)
    preparer.add_argument('--snapshots', type=Path, nargs='+', required=True)
    preparer.add_argument('--output', type=Path, required=True)
    evaluator = sub.add_parser('evaluate')
    evaluator.add_argument('--plan', type=Path, required=True)
    evaluator.add_argument('--output', type=Path, required=True)
    evaluator.add_argument('--threads', type=int, default=1)
    args = parser.parse_args()
    if args.command == 'collect':
        result = collect_dates(args.dates, args.output, args.limit, exclude_collection=args.exclude_collection)
    elif args.command == 'prepare':
        result = prepare(args.plan, args.snapshots, args.output)
    else:
        result = evaluate(args.plan, args.output, args.threads)
    print(json.dumps({k: v for k, v in result.items() if k not in ('maps', 'sha256', 'results')}, indent=2))


if __name__ == '__main__':
    main()
