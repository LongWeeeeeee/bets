"""Offline E313 individual hero and contextual performance ablations.

All history and context normalization use completed maps strictly before the
query start. Patch IDs follow the existing announcement calendar, not observed
client versions. CLI outputs are research artifacts, never serving models.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import numpy as np
from threadpoolctl import threadpool_limits

from base.dota_patch_calendar import PATCH_RELEASES
from base.tools import prematch_player_performance as performance
from base.tools import prematch_pro_player_history as history
from base.tools import prematch_winner_research as winner


METRICS = ('xpm', 'last_hits_per_minute', 'hero_damage_per_minute', 'tower_damage_per_minute')
METRIC_INDICES = (0, 2, 4, 5)


def patch_ids(times):
    boundaries = np.sort([p.release_ts for p in PATCH_RELEASES])
    return np.searchsorted(boundaries, times, side='right').astype(np.int64)


def rolling(keys, ends, mids, values, qkeys, qtimes, window):
    """Metric-specific means/counts over last N completed rows in each group.

    Equal completion times sort by MID; a map ending exactly at query start is
    excluded. Callers must provide at most one row per (group, map).
    """
    values = np.asarray(values)
    means = np.full((len(qkeys), values.shape[1]), np.nan, dtype=np.float32)
    counts = np.zeros(means.shape, dtype=np.float32)
    games = np.zeros(len(qkeys), dtype=np.float32)
    last = np.full(len(qkeys), np.nan, dtype=np.float64)
    if not len(keys):
        return means, counts, games, last
    if window <= 0:
        raise ValueError('window must be positive')
    order = np.lexsort((mids, ends, keys))
    keys, ends = keys[order], ends[order]
    pairs = np.empty(len(keys), dtype=[('key', '<i8'), ('time', '<i8')])
    pairs['key'], pairs['time'] = keys, ends
    query = np.empty(len(qkeys), dtype=pairs.dtype)
    query['key'], query['time'] = qkeys, qtimes
    right = np.searchsorted(pairs, query, side='left')
    group_start = np.searchsorted(keys, qkeys, side='left')
    left = np.maximum(group_start, right - window)
    n = right - left
    valid = n > 0
    games[valid] = n[valid]
    last[valid] = ends[right[valid] - 1]
    for metric in range(values.shape[1]):
        v = values[order, metric].astype(np.float64)
        finite = np.isfinite(v)
        total = np.r_[0., np.cumsum(np.where(finite, v, 0.))]
        count = np.r_[0, np.cumsum(finite)]
        numer = total[right] - total[left]
        denom = count[right] - count[left]
        means[:, metric] = np.divide(numer, denom, out=np.full(len(qkeys), np.nan), where=denom > 0)
        counts[:, metric] = denom
    return means, counts, games, last


def slots(values, stem):
    """Keep each role's Radiant value and Radiant-minus-Dire difference."""
    v = np.asarray(values).reshape(-1, 10)
    out, names = [], []
    for role in range(5):
        out.extend((v[:, role], v[:, role] - v[:, role + 5]))
        names.extend((f'{stem}_R{role+1}', f'{stem}_diff{role+1}'))
    return np.column_stack(out).astype(np.float32), names


def individual(source, data):
    n = len(data['mid'])
    accounts, heroes = data['accounts'].ravel(), data['heroes'].ravel()
    times = np.repeat(data['ts'], 10)
    residual = np.full((n * 10, 8), np.nan, dtype=np.float32)
    for part in range(32):
        q = np.flatnonzero(accounts % 32 == part)
        src = source[source['account'] % 32 == part]
        a, _, _ = performance.rolling(src, accounts[q], heroes[q], times[q])
        h, counts, _ = performance.rolling(src, accounts[q], heroes[q], times[q], hero=True)
        residual[q] = np.where(np.isfinite(a), np.nan_to_num(h - a, nan=0.) * counts / (counts + 10.), np.nan)
    blocks, names = [], []
    for j, metric in enumerate(performance.PERFORMANCE):
        block, labels = slots(residual[:, j], 'individual_' + metric)
        blocks.append(block); names.extend(labels)
    return np.column_stack(blocks), names


def join_performance(source, data):
    """Join E311 source by MID/account/hero/end, independent of player order."""
    order = np.argsort(data['mid'])
    ids = data['mid'][order]
    values = np.full((len(ids), 10, 8), np.nan, dtype=np.float32)
    seen = np.zeros((len(ids), 10), dtype=bool)
    matched = 0
    for offset in range(0, len(source), 250000):
        src = source[offset:offset + 250000]
        pos = np.searchsorted(ids, src['mid'])
        valid = pos < len(ids)
        valid[valid] &= ids[pos[valid]] == src['mid'][valid]
        src, rows = src[valid], order[pos[valid]]
        if not len(src):
            continue
        match = data['accounts'][rows] == src['account'][:, None]
        if not np.all(match.sum(axis=1) == 1):
            raise ValueError('source account missing/duplicate in reference')
        slot = match.argmax(axis=1)
        if not (np.array_equal(data['heroes'][rows, slot], src['hero'])
                and np.array_equal(data['end'][rows], src['end'])):
            raise ValueError('source hero/end differs from reference')
        flat = rows * 10 + slot
        if seen[rows, slot].any() or len(np.unique(flat)) != len(flat):
            raise ValueError('duplicate source map/account')
        values[rows, slot] = src['values']
        seen[rows, slot] = True
        matched += len(src)
    return values, {'joined_player_rows': matched, 'missing_player_rows': int((~seen).sum())}


def contextual_values(data, values):
    """Past performance vs the opposing same estimated role, hero/patch adjusted.

    The role is the existing pregame causal role estimate. It is not an observed
    lane matchup. Each map's own normalization sees only earlier completed maps.
    """
    n = len(data['mid'])
    heroes = data['heroes'].ravel().astype(np.int64)
    roles = np.tile(np.arange(10) % 5, n)
    starts, ends, mids = (np.repeat(data[k], 10) for k in ('ts', 'end', 'mid'))
    patches = np.repeat(patch_ids(data['ts']), 10)
    if np.any((heroes < 1) | (heroes >= 1024)):
        raise ValueError('invalid hero id')
    raw = np.log1p(values[:, :, METRIC_INDICES].astype(np.float64))
    opposing = raw[:, np.r_[5:10, 0:5], :]
    relative = (raw - opposing).reshape(-1, len(METRICS))
    keys = heroes * 8 + roles
    # At most one player on a given hero per role per side. Same hero may occur
    # on both sides in malformed/nonstandard maps; exclude these for baselines.
    duplicate = (data['heroes'][:, :5] == data['heroes'][:, 5:]).ravel()
    eligible = ~np.column_stack((duplicate.reshape(n, 5), duplicate.reshape(n, 5))).ravel()
    global_mean, _, _, _ = rolling(keys[eligible], ends[eligible], mids[eligible], relative[eligible], keys, starts, 500)
    pkeys = keys * 64 + patches
    if patches.max(initial=0) >= 64:
        raise ValueError('patch calendar exceeds key capacity')
    valid_patch = eligible & (patches > 0)
    patch_mean, patch_count, _, _ = rolling(pkeys[valid_patch], ends[valid_patch], mids[valid_patch], relative[valid_patch], pkeys, starts, 500)
    # Fallback is global hero-role; a first patch observation cannot invent a
    # zero reference. If only the patch baseline exists, use it directly.
    base = np.where(np.isfinite(global_mean), global_mean, patch_mean)
    delta = np.where(np.isfinite(patch_mean - base), patch_mean - base, 0.)
    baseline = base + delta * patch_count / (patch_count + 20.)
    normalized = (relative - baseline).astype(np.float32)
    p = winner.expit(data['k24_diff'].astype(float) * np.log(10.) / 400.)
    wr = data['y'] - p
    surprise = np.column_stack((np.repeat(wr[:, None], 5, axis=1), np.repeat(-wr[:, None], 5, axis=1))).ravel()
    surprise[~np.repeat(data['elo_eligible'], 10)] = np.nan
    return np.column_stack((normalized, surprise)).astype(np.float32)


def context(data, observations):
    n = len(data['mid'])
    accounts = data['accounts'].ravel()
    heroes = data['heroes'].ravel().astype(np.int64)
    roles = np.tile(np.arange(10) % 5, n)
    times, ends, mids = (np.repeat(data[k], 10) for k in ('ts', 'end', 'mid'))
    patches = np.repeat(patch_ids(data['ts']), 10)
    if np.any(accounts <= 0) or accounts.max(initial=0) >= np.iinfo(np.int64).max // (8 * 1024 * 64):
        raise ValueError('invalid account key')
    account_keys = accounts * 8 + roles
    hero_keys = account_keys * 1024 + heroes
    patch_keys = hero_keys * 64 + patches
    out = np.full((n * 10, 19), np.nan, dtype=np.float32)
    for part in range(32):
        q = np.flatnonzero(accounts % 32 == part)
        args = (ends[q], mids[q], observations[q])
        general, gc, _, _ = rolling(account_keys[q], *args, account_keys[q], times[q], 20)
        hero, hc, hg, last = rolling(hero_keys[q], *args, hero_keys[q], times[q], 20)
        recent, _, _, _ = rolling(account_keys[q], *args, account_keys[q], times[q], 5)
        valid_patch = q[patches[q] > 0]
        _, _, pg, _ = rolling(patch_keys[valid_patch], ends[valid_patch], mids[valid_patch], observations[valid_patch], patch_keys[q], times[q], 20)
        delta = np.where(np.isfinite(general), np.nan_to_num(hero - general, nan=0.) * hc / (hc + 10.), np.nan)
        counts = np.column_stack((gc.mean(axis=1), hc.mean(axis=1), np.log1p(pg), np.log1p((times[q] - last) / 86400.)))
        out[q] = np.column_stack((general, delta, recent - general, counts))
    names, blocks = [], []
    metrics = METRICS + ('win_minus_k24',)
    labels = [kind + '_' + metric for kind in ('player20', 'hero20_delta', 'recent5_delta') for metric in metrics]
    labels += ['player_valid20', 'hero_valid20', 'patch_hero_games20_log1p', 'days_since_hero_log1p']
    for j, name in enumerate(labels):
        block, labels = slots(out[:, j], 'context_' + name)
        blocks.append(block); names.extend(labels)
    return np.column_stack(blocks), names


def build(args):
    with np.load(args.dataset) as z:
        data = {k: z[k] for k in z.files}
    source = np.memmap(args.source, dtype=performance.SOURCE, mode='r')
    x, names = individual(source, data)
    history._atomic_npz(args.output_dir / 'individual.npz', X=x, feature_names=np.asarray(names), mid=data['mid'], ts=data['ts'])
    del x
    raw, audit = join_performance(source, data)
    observations = contextual_values(data, raw)
    history._atomic_npz(args.output_dir / 'context_observations.npz', values=observations)
    del raw
    x, names = context(data, observations)
    history._atomic_npz(args.output_dir / 'context.npz', X=x, feature_names=np.asarray(names), mid=data['mid'], ts=data['ts'])
    audit.update(rows=len(data['mid']), individual_features=80, context_features=len(names),
                 temporal_contract='source end < query start, including per-source context normalization',
                 role_contract='past-completed estimated role, not actual lane opponent',
                 patch_contract='repository announcement/legacy calendar; bucket0 unknown',
                 normalization_source='reference cohort only; individual family uses full E311 source',
                 finite_context_fraction=float(np.isfinite(x).mean()))
    return audit


def load_data(args):
    data = winner.load_data(args)
    paths = [args.performance, args.features / 'individual.npz']
    if args.arm == 'context':
        paths.append(args.features / 'context.npz')
    for path in paths:
        with np.load(path) as z:
            if not (np.array_equal(z['mid'], data['mid']) and np.array_equal(z['ts'], data['ts'])):
                raise ValueError('projection identity mismatch')
            n = data['numeric_count']
            data['X'] = np.column_stack((data['X'][:, :n], z['X'], data['X'][:, n:]))
            data['names'][n:n] = z['feature_names'].astype(str).tolist()
            data['numeric_count'] += z['X'].shape[1]
    return data


def experiment(args):
    data = load_data(args)
    recipe = winner.RECIPES['lgb31_all']
    models, outputs, reports = [], [], []
    for begin, end in [('2026-06-01', '2026-07-01'), ('2026-07-01', '2026-07-15')]:
        rows = winner.completed_indices(data, winner.epoch(begin), winner.epoch(end))
        model, columns, audit = winner.fit_model(data, recipe, winner.epoch(begin), rows, args.threads)
        p = winner.predict(model, columns, data, rows)
        models.append({'model': model, 'columns': columns, 'cutoff': begin})
        outputs.append((rows, p))
        reports.append({'begin': begin, 'end': end, 'audit': audit, 'metrics': winner.metrics(data['y'][rows], p)})
        print(json.dumps(reports[-1]), flush=True)
    rows = np.concatenate([r for r, _ in outputs]); p = np.concatenate([p for _, p in outputs])
    history._atomic_npz(args.output_dir / 'validation.npz', mid=data['mid'][rows], y=data['y'][rows], p=p,
                        k24=winner.expit(data['k24_diff'][rows] * np.log(10.) / 400.), series=data['series'][rows])
    joblib.dump({'models': models, 'names': data['names'], 'arm': args.arm, 'recipe': recipe, 'offline_only': True}, args.output_dir / 'evaluation_models.joblib')
    errors = []
    for saved, (rows, before) in zip(joblib.load(args.output_dir / 'evaluation_models.joblib')['models'], outputs):
        after = winner.predict(saved['model'], saved['columns'], data, rows)
        errors.append(float(np.max(np.abs(before - after))))
    if any(errors):
        raise AssertionError('saved model replay differs')
    return {'arm': args.arm, 'recipe': recipe, 'features': data['names'], 'folds': reports,
            'pooled': winner.metrics(np.concatenate([data['y'][r] for r, _ in outputs]), p),
            'reload_max_abs_errors': errors, 'evaluation': 'reused selection, not fresh terminal'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['build', 'experiment'], required=True)
    for name in ('dataset', 'source', 'features', 'performance', 'output-dir'):
        parser.add_argument('--' + name, type=Path, required=name in ('dataset', 'output-dir'))
    parser.add_argument('--draft', action='append', type=Path, default=[])
    parser.add_argument('--arm', choices=['individual', 'context'])
    parser.add_argument('--threads', type=int, default=1)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if any(args.output_dir.glob('*.npz')) or (args.output_dir / 'metrics.json').exists():
        raise FileExistsError(args.output_dir)
    with threadpool_limits(limits=args.threads):
        result = build(args) if args.mode == 'build' else experiment(args)
    (args.output_dir / 'metrics.json').write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main()
