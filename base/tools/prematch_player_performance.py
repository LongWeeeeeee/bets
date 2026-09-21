#!/usr/bin/env python3
"""Offline, strict end-before-start player-performance and lane histories.

Raw missing statistics stay NaN. Current-map statistics never become predictors.
Source maps use the audited E309 canonical record choice, including incomplete
rosters. Query slots retain E308's causal role assignment. No serving integration.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import joblib
import numpy as np
from threadpoolctl import threadpool_limits

from base.tools import prematch_pro_player_history as history
from base.tools import prematch_winner_research as winner

PERFORMANCE = ('experiencePerMinute', 'networth', 'numLastHits', 'numDenies',
               'heroDamage', 'towerDamage', 'heroHealing', 'level')
LANE = tuple(f'{name}_t_{minute}' for name in ('gold', 'xp', 'lh', 'dn') for minute in (300, 600))
SOURCE = np.dtype([('account', '<i8'), ('hero', '<i4'), ('end', '<i8'), ('mid', '<i8'),
                   ('values', '<f4', (8,))])
WINDOW = 20
SHRINKAGE = 10.0


def number(value):
    if value is None or isinstance(value, bool):
        return np.nan
    try:
        value = float(value)
    except (ValueError, TypeError, OverflowError):
        return np.nan
    return value if np.isfinite(value) and value >= 0 else np.nan


def performance_values(player, duration):
    values = np.asarray([number(player.get(k)) for k in PERFORMANCE], dtype=np.float32)
    # XPM is already a rate. Final level is an observed past-map value.
    values[1:7] /= duration / 60.0
    return values


def raw_source(raw_dir, candidates, data, output):
    counters = {}
    canonical = history._canonicalize(candidates, counters)
    history._validate_overlaps(canonical, data, history._query_index(data), counters)
    files = sorted(Path(raw_dir).glob('*.json'))
    target_accounts = set(map(int, data['accounts'].ravel()))
    batch, written, visited = [], 0, 0
    temporary = output.with_name(output.name + '.tmp')
    if output.exists() or temporary.exists():
        raise FileExistsError(output)
    with temporary.open('wb') as handle:
        for file_order, path in enumerate(files):
            chosen = canonical[canonical['file_order'] == file_order]
            wanted = {int(r['record_order']): r for r in chosen}
            for record_order, (key, match) in enumerate(history._iter_raw_items(path)):
                old = wanted.get(record_order)
                if old is None:
                    continue
                actual = history._record_from_raw(key, match, file_order, record_order)
                if actual is None or history._records_differ(old, actual):
                    raise ValueError(f'raw source no longer matches canonical record: {path}:{key}')
                visited += 1
                valid = set(int(a) for a in old['accounts'] if a > 0)
                for player in match.get('players') or []:
                    if not isinstance(player, dict) or type(player.get('isRadiant')) is not bool:
                        continue
                    account = history._positive_int((player.get('steamAccount') or {}).get('id'))
                    if account not in valid or account not in target_accounts:
                        continue
                    values = performance_values(player, int(old['end']) - int(old['start']))
                    batch.append((account, int(player['heroId']), int(old['end']), int(old['mid']), values))
                if len(batch) >= 8192:
                    np.asarray(batch, dtype=SOURCE).tofile(handle)
                    written += len(batch)
                    batch.clear()
            if file_order % 40 == 0:
                print(json.dumps({'raw_file': file_order, 'maps': visited, 'player_rows': written}), flush=True)
        if batch:
            np.asarray(batch, dtype=SOURCE).tofile(handle)
            written += len(batch)
    if visited != len(canonical):
        raise ValueError(f'canonical records missing from raw source: {visited}/{len(canonical)}')
    temporary.replace(output)
    counters.update(canonical_maps=visited, player_rows=written)
    return counters


def lane_source(database, data):
    """Account/hero/side/outcome/time checked against matching query maps."""
    index = {int(mid): i for i, mid in enumerate(data['mid'])}
    rows, seen, maps = [], set(), set()
    with sqlite3.connect(f'file:{Path(database).resolve()}?mode=ro&immutable=1', uri=True) as db:
        query = ('SELECT p.match_id,p.account_id,p.hero_id,p.side,m.response_start_time,'
                 'm.duration,m.radiant_win,' + ','.join('p.' + k for k in LANE) +
                 ' FROM player_timelines p JOIN matches m ON p.match_id=m.match_id'
                 " WHERE m.status IN ('complete','partial') ORDER BY p.match_id,p.row_index")
        for mid, account, hero, side, start, duration, won, *values in db.execute(query):
            if mid not in index:
                raise ValueError(f'OpenDota map lacks reference identity: {mid}')
            r = index[mid]
            if (start != data['ts'][r] or not duration or start + duration != data['end'][r]
                    or won not in (0, 1) or won != data['y'][r]):
                raise ValueError(f'OpenDota time/outcome mismatch: {mid}')
            if side not in ('radiant', 'dire'):
                raise ValueError(f'OpenDota unknown side: {mid}')
            if (mid, account) in seen:
                raise ValueError(f'OpenDota duplicate player: {mid}/{account}')
            seen.add((mid, account))
            slots = np.flatnonzero(data['accounts'][r] == account)
            if (len(slots) != 1 or data['heroes'][r, slots[0]] != hero
                    or (slots[0] < 5) != (side == 'radiant')):
                raise ValueError(f'OpenDota account/hero/side mismatch: {mid}/{account}')
            # An uncompleted 10-minute window cannot supply a valid value.
            values = [number(v) if duration >= (300 if j % 2 == 0 else 600) else np.nan
                      for j, v in enumerate(values)]
            rows.append((account, hero, start + duration, mid, values))
            maps.add(mid)
    return np.asarray(rows, dtype=SOURCE), {'maps': len(maps), 'player_rows': len(rows)}


def rolling(source, q_accounts, q_heroes, q_times, *, hero=False):
    """Mean of finite observations among last 20 completed maps, strict <.

    Grouped prefix sums and predecessor lookup avoid mutable cross-query state.
    Equal end/start is excluded; equal source ends are ordered by MID. Counts
    are metric-specific, so a missing value is never interpreted as zero.
    """
    shape = (len(q_accounts), 8)
    means = np.full(shape, np.nan, dtype=np.float32)
    counts = np.zeros(shape, dtype=np.float32)
    games = np.zeros(len(q_accounts), dtype=np.float32)
    if not len(source):
        return means, counts, games
    if hero and (np.any(source['hero'] >= 1024) or np.any(q_heroes >= 1024)
                 or np.any(source['account'] > np.iinfo(np.int64).max // 1024)):
        raise ValueError('player-hero key out of range')
    keys = source['account'] * 1024 + source['hero'] if hero else source['account']
    qkeys = q_accounts * 1024 + q_heroes if hero else q_accounts
    order = np.lexsort((source['mid'], source['end'], keys))
    keys, ends = keys[order], source['end'][order]
    dtype = np.dtype([('key', '<i8'), ('time', '<i8')])
    lookup = np.empty(len(order), dtype=dtype)
    lookup['key'], lookup['time'] = keys, ends
    query = np.empty(len(qkeys), dtype=dtype)
    query['key'], query['time'] = qkeys, q_times
    pos = np.searchsorted(lookup, query, side='left') - 1
    valid = (pos >= 0) & (keys[np.maximum(pos, 0)] == qkeys) & (q_accounts > 0)
    qr, pos = np.flatnonzero(valid), pos[valid]
    group_start = np.maximum.accumulate(np.where(np.r_[True, keys[1:] != keys[:-1]],
                                                 np.arange(len(keys)), 0))
    begin = np.maximum(group_start[pos], pos - WINDOW + 1)
    games[qr] = pos - begin + 1
    for metric in range(8):
        values = source['values'][order, metric]
        finite = np.isfinite(values)
        total = np.r_[0., np.cumsum(np.where(finite, values, 0), dtype=np.float64)]
        count = np.r_[0, np.cumsum(finite, dtype=np.int64)]
        n = count[pos + 1] - count[begin]
        sums = total[pos + 1] - total[begin]
        counts[qr, metric] = n
        means[qr, metric] = np.divide(sums, n, out=np.full(len(n), np.nan), where=n > 0)
    return means, counts, games


def summarize(values, stem, *, roles=True):
    values = np.asarray(values, dtype=np.float32).reshape(-1, 10)
    finite = np.isfinite(values)
    def mean(part):
        count = finite[:, part].sum(axis=1)
        return np.divide(np.where(finite[:, part], values[:, part], 0).sum(axis=1), count,
                         out=np.full(len(values), np.nan), where=count > 0)
    r, d = mean(slice(0, 5)), mean(slice(5, 10))
    columns, names = [r, r - d], [stem + '_Rmean', stem + '_diff']
    if roles:
        for i in range(5):
            columns.extend((values[:, i], values[:, i] - values[:, i + 5]))
            names.extend((f'{stem}_R{i+1}', f'{stem}_diff{i+1}'))
    return np.column_stack(columns).astype(np.float32), names


def project(source, data, family):
    # Partition accounts to cap temporary arrays on the full historical corpus.
    n = len(data['mid'])
    names = PERFORMANCE if family == 'performance' else LANE
    feature_blocks, feature_names = [], []
    qaccounts, qheroes = data['accounts'].ravel(), data['heroes'].ravel()
    qtimes = np.repeat(data['ts'], 10)
    player = np.full((n * 10, 8), np.nan, dtype=np.float32)
    residual = np.full_like(player, np.nan)
    counts = np.zeros((n * 10, 4), dtype=np.float32)
    for partition in range(32):
        q = np.flatnonzero(qaccounts % 32 == partition)
        src = source[source['account'] % 32 == partition]
        a, ac, ag = rolling(src, qaccounts[q], qheroes[q], qtimes[q])
        h, hc, hg = rolling(src, qaccounts[q], qheroes[q], qtimes[q], hero=True)
        player[q] = a
        # Residual of hero mean shrunk toward the player's general mean.
        residual[q] = np.where(np.isfinite(a), np.nan_to_num(h - a, nan=0.) * hc / (hc + SHRINKAGE), np.nan)
        counts[q] = np.column_stack((np.log1p(ag), np.log1p(hg), ac.mean(axis=1), hc.mean(axis=1)))
    for j, name in enumerate(names):
        for values, suffix, roles in ((player[:, j], 'player20', True), (residual[:, j], 'hero20_delta', False)):
            block, labels = summarize(values, f'{family}_{name}_{suffix}', roles=roles)
            feature_blocks.append(block); feature_names.extend(labels)
    for j, name in enumerate(('games20_log1p', 'hero_games20_log1p', 'valid20_mean', 'hero_valid20_mean')):
        block, labels = summarize(counts[:, j], f'{family}_{name}')
        feature_blocks.append(block); feature_names.extend(labels)
    return np.column_stack(feature_blocks), feature_names


def build(args):
    with np.load(args.dataset) as z:
        data = {k: z[k] for k in z.files}
    winner.load_data(SimpleNamespace(dataset=args.dataset, draft=[]))  # schema validation
    source_path = args.output_dir / 'performance_source.bin'
    if source_path.exists():
        raise FileExistsError(source_path)
    # File ordinals are snapshot-specific: never reuse them after new raw files arrive.
    candidates = args.output_dir / 'canonical_candidates.bin'
    parsed = history._append_records(args.raw_dir, candidates)
    audit = {'parsed': parsed, 'performance': raw_source(args.raw_dir, candidates, data, source_path)}
    source = np.memmap(source_path, dtype=SOURCE, mode='r')
    performance, pnames = project(source, data, 'performance')
    history._atomic_npz(args.output_dir / 'performance.npz', X=performance, feature_names=np.asarray(pnames),
                        mid=data['mid'], ts=data['ts'])
    del performance, source
    lanes, audit['lane'] = lane_source(args.database, data)
    history._atomic_npz(args.output_dir / 'lane_source.npz', rows=lanes)
    lane, lnames = project(lanes, data, 'lane')
    history._atomic_npz(args.output_dir / 'lane.npz', X=lane, feature_names=np.asarray(lnames),
                        mid=data['mid'], ts=data['ts'])
    audit.update(rows=len(data['mid']), features_per_family=len(lnames), window=WINDOW, shrinkage=SHRINKAGE,
                 temporal_contract='source end < query start; last 20 completed maps, metric-wise missing counts',
                 missing_contract='raw null/missing/negative/nonfinite -> NaN; zero remains zero',
                 publication_time_verified=False)
    return audit


def experiment(args):
    data = winner.load_data(args)
    # Insert numeric columns before categorical hero/league fields.
    for family in args.family:
        with np.load(args.features / (family + '.npz')) as z:
            if not np.array_equal(z['mid'], data['mid']) or not np.array_equal(z['ts'], data['ts']):
                raise ValueError('feature projection row mismatch')
            count = data['numeric_count']
            data['X'] = np.column_stack((data['X'][:, :count], z['X'], data['X'][:, count:]))
            data['names'][count:count] = z['feature_names'].astype(str).tolist()
            data['numeric_count'] += z['X'].shape[1]
    reports, outputs, models = [], [], []
    recipe = winner.RECIPES['lgb31_all']
    for begin, end in [('2026-06-01', '2026-07-01'), ('2026-07-01', '2026-07-15')]:
        rows = winner.completed_indices(data, winner.epoch(begin), winner.epoch(end))
        model, columns, audit = winner.fit_model(data, recipe, winner.epoch(begin), rows, args.threads)
        p = winner.predict(model, columns, data, rows)
        reports.append({'begin': begin, 'end': end, 'audit': audit, 'metrics': winner.metrics(data['y'][rows], p)})
        models.append({'model': model, 'columns': columns, 'cutoff': begin})
        outputs.append((rows, p))
        print(json.dumps(reports[-1]), flush=True)
    rows = np.concatenate([r for r, p in outputs]); p = np.concatenate([p for r, p in outputs])
    history._atomic_npz(args.output_dir / 'validation.npz', mid=data['mid'][rows], y=data['y'][rows], p=p,
                        k24=winner.expit(data['k24_diff'][rows] * np.log(10.) / 400.), series=data['series'][rows])
    joblib.dump({'models': models, 'names': data['names'], 'families': args.family,
                 'recipe': recipe, 'offline_only': True}, args.output_dir / 'evaluation_models.joblib')
    loaded = joblib.load(args.output_dir / 'evaluation_models.joblib')
    errors = []
    for saved, (fold_rows, probabilities) in zip(loaded['models'], outputs):
        replay = winner.predict(saved['model'], saved['columns'], data, fold_rows)
        errors.append(float(np.max(np.abs(replay - probabilities))))
    if any(error != 0. for error in errors):
        raise AssertionError(f'saved model replay changed: {errors}')
    return {'reload_max_abs_errors': errors, 'folds': reports, 'pooled': winner.metrics(data['y'][rows], p), 'families': args.family,
            'features': data['names'], 'recipe': recipe, 'evaluation': 'previously used selection, exploratory'}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', choices=['build', 'experiment'], required=True)
    for name in ('dataset', 'output-dir', 'raw-dir', 'database', 'features'):
        p.add_argument('--' + name, type=Path, required=name in ('dataset', 'output-dir'))
    p.add_argument('--draft', action='append', type=Path, default=[])
    p.add_argument('--family', choices=['performance', 'lane'], action='append', default=[])
    p.add_argument('--threads', type=int, default=1)
    args = p.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if (args.output_dir / 'metrics.json').exists():
        raise FileExistsError(args.output_dir)
    with threadpool_limits(limits=args.threads):
        result = build(args) if args.mode == 'build' else experiment(args)
    (args.output_dir / 'metrics.json').write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main()
