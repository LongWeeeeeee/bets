#!/usr/bin/env python3
"""Полный бэктест на всех данных с отступом от начала."""

import sys
import os
import json
import contextlib
import io
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from core.transitive_analyzer import TransitiveAnalyzer, get_transitiv


def run_full_backtest(
    skip_days: int = 14,
    max_days: int = 30,
    min_strength: float = 0.0,
    use_transitive: bool = True,
    by_series: bool = True,
    progress_every: int = 500,
):
    """Бэктест на всех данных.
    
    skip_days: отступ от начала данных (чтобы были исторические данные)
    by_series: если True, считаем accuracy по сериям, иначе по картам
    """
    analyzer = TransitiveAnalyzer()
    
    # Собираем все матчи с timestamp
    all_matches = []
    for m in analyzer.matches_data.values():
        ts = m.get('startDateTime', 0)
        if ts > 0:
            all_matches.append(m)
    
    all_matches.sort(key=lambda m: m.get('startDateTime', 0))
    
    if not all_matches:
        print("No matches found")
        return
    
    min_ts = all_matches[0].get('startDateTime', 0)
    start_ts = min_ts + skip_days * 86400
    
    print(f"Total matches: {len(all_matches)}")
    print(f"Skip first {skip_days} days, start from: {datetime.fromtimestamp(start_ts)}")
    
    if by_series:
        return backtest_by_series(analyzer, all_matches, start_ts, max_days, min_strength, use_transitive, progress_every)
    else:
        return backtest_by_maps(analyzer, all_matches, start_ts, max_days, min_strength, use_transitive, progress_every)


def backtest_by_series(analyzer, all_matches, start_ts, max_days, min_strength, use_transitive, progress_every):
    """Бэктест по сериям."""
    # Группируем матчи по сериям
    series_dict = defaultdict(list)
    for m in all_matches:
        ts = m.get('startDateTime', 0)
        if ts < start_ts:
            continue
        series_id = m.get('seriesId', m.get('id'))
        series_dict[series_id].append(m)
    
    # Сортируем серии по времени первого матча
    series_list = []
    for series_id, matches in series_dict.items():
        matches.sort(key=lambda m: m.get('startDateTime', 0))
        first_ts = matches[0].get('startDateTime', 0)
        series_list.append({
            'series_id': series_id,
            'matches': matches,
            'start_ts': first_ts,
        })
    
    series_list.sort(key=lambda s: s['start_ts'])
    
    print(f"Total series to test: {len(series_list)}")
    
    used = 0
    hits = 0
    skipped_no_data = 0
    skipped_weak = 0
    ties = 0
    
    # Статистика по strength bins
    strength_bins = defaultdict(lambda: {'used': 0, 'hits': 0})
    
    for idx, series in enumerate(series_list, 1):
        matches = series['matches']
        series_ts = series['start_ts']
        
        # Определяем команды и победителя серии
        rad = matches[0].get('radiantTeam') or {}
        dire = matches[0].get('direTeam') or {}
        rid = rad.get('id')
        did = dire.get('id')
        rname = rad.get('name', f'Team_{rid}')
        dname = dire.get('name', f'Team_{did}')
        
        if not rid or not did:
            skipped_no_data += 1
            continue
        
        # Считаем победителя серии по картам
        radiant_maps = 0
        dire_maps = 0
        for m in matches:
            rw = m.get('didRadiantWin')
            if rw is True:
                radiant_maps += 1
            elif rw is False:
                dire_maps += 1
        
        if radiant_maps == dire_maps:
            ties += 1
            continue
        
        actual_winner_id = rid if radiant_maps > dire_maps else did
        
        # Получаем предсказание
        with contextlib.redirect_stdout(io.StringIO()):
            res = get_transitiv(
                radiant_team_id=rid,
                dire_team_id=did,
                radiant_team_name_original=rname,
                dire_team_name_original=dname,
                as_of_timestamp=series_ts,
                analyzer=analyzer,
                max_days=max_days,
                use_transitive=use_transitive,
                verbose=False,
            )
        
        if not res.get('has_data'):
            skipped_no_data += 1
            continue
        
        strength = float(res.get('strength', 0.0) or 0.0)
        if strength < min_strength:
            skipped_weak += 1
            continue
        
        pred_name = res.get('prediction') or res.get('winner')
        if pred_name == 'Ничья' or pred_name == 'Unknown' or pred_name is None:
            ties += 1
            continue
        
        if pred_name == rname:
            pred_id = rid
        elif pred_name == dname:
            pred_id = did
        else:
            skipped_no_data += 1
            continue
        
        used += 1
        hit = (pred_id == actual_winner_id)
        if hit:
            hits += 1
        
        # Статистика по strength bins
        if strength >= 0.7:
            bin_name = '0.7+'
        elif strength >= 0.5:
            bin_name = '0.5-0.7'
        elif strength >= 0.3:
            bin_name = '0.3-0.5'
        else:
            bin_name = '<0.3'
        
        strength_bins[bin_name]['used'] += 1
        if hit:
            strength_bins[bin_name]['hits'] += 1
        
        if progress_every > 0 and idx % progress_every == 0:
            acc_so_far = hits / used if used > 0 else 0
            print(f"Progress: {idx}/{len(series_list)} series, used={used}, acc={acc_so_far:.3f}")
    
    total = len(series_list)
    acc = hits / used if used > 0 else 0
    cov = used / total if total > 0 else 0
    
    print("\n" + "=" * 60)
    print("BACKTEST RESULTS (by series)")
    print("=" * 60)
    print(f"Total series: {total}")
    print(f"Used: {used}")
    print(f"Skipped (no data): {skipped_no_data}")
    print(f"Skipped (weak): {skipped_weak}")
    print(f"Ties: {ties}")
    print(f"Accuracy: {acc:.4f} ({hits}/{used})")
    print(f"Coverage: {cov:.4f}")
    
    print("\nAccuracy by strength:")
    for bin_name in ['0.7+', '0.5-0.7', '0.3-0.5', '<0.3']:
        b = strength_bins[bin_name]
        if b['used'] > 0:
            bin_acc = b['hits'] / b['used']
            print(f"  {bin_name}: {bin_acc:.4f} ({b['hits']}/{b['used']})")
    
    return {
        'total': total,
        'used': used,
        'hits': hits,
        'accuracy': acc,
        'coverage': cov,
        'strength_bins': dict(strength_bins),
    }


def backtest_by_maps(analyzer, all_matches, start_ts, max_days, min_strength, use_transitive, progress_every):
    """Бэктест по картам."""
    valid_matches = [m for m in all_matches if m.get('startDateTime', 0) >= start_ts]
    
    print(f"Total maps to test: {len(valid_matches)}")
    
    used = 0
    hits = 0
    skipped_no_data = 0
    skipped_weak = 0
    
    strength_bins = defaultdict(lambda: {'used': 0, 'hits': 0})
    
    for idx, m in enumerate(valid_matches, 1):
        rad = m.get('radiantTeam') or {}
        dire = m.get('direTeam') or {}
        rid = rad.get('id')
        did = dire.get('id')
        rname = rad.get('name', f'Team_{rid}')
        dname = dire.get('name', f'Team_{did}')
        ts = m.get('startDateTime', 0)
        rw = m.get('didRadiantWin')
        
        if not rid or not did or not isinstance(rw, bool):
            skipped_no_data += 1
            continue
        
        with contextlib.redirect_stdout(io.StringIO()):
            res = get_transitiv(
                radiant_team_id=rid,
                dire_team_id=did,
                radiant_team_name_original=rname,
                dire_team_name_original=dname,
                as_of_timestamp=ts,
                analyzer=analyzer,
                max_days=max_days,
                use_transitive=use_transitive,
                verbose=False,
            )
        
        if not res.get('has_data'):
            skipped_no_data += 1
            continue
        
        strength = float(res.get('strength', 0.0) or 0.0)
        if strength < min_strength:
            skipped_weak += 1
            continue
        
        pred_name = res.get('prediction') or res.get('winner')
        if pred_name == 'Ничья' or pred_name == 'Unknown' or pred_name is None:
            skipped_no_data += 1
            continue
        
        if pred_name == rname:
            pred_id = rid
        elif pred_name == dname:
            pred_id = did
        else:
            skipped_no_data += 1
            continue
        
        actual_id = rid if rw else did
        
        used += 1
        hit = (pred_id == actual_id)
        if hit:
            hits += 1
        
        if strength >= 0.7:
            bin_name = '0.7+'
        elif strength >= 0.5:
            bin_name = '0.5-0.7'
        elif strength >= 0.3:
            bin_name = '0.3-0.5'
        else:
            bin_name = '<0.3'
        
        strength_bins[bin_name]['used'] += 1
        if hit:
            strength_bins[bin_name]['hits'] += 1
        
        if progress_every > 0 and idx % progress_every == 0:
            acc_so_far = hits / used if used > 0 else 0
            print(f"Progress: {idx}/{len(valid_matches)} maps, used={used}, acc={acc_so_far:.3f}")
    
    total = len(valid_matches)
    acc = hits / used if used > 0 else 0
    cov = used / total if total > 0 else 0
    
    print("\n" + "=" * 60)
    print("BACKTEST RESULTS (by maps)")
    print("=" * 60)
    print(f"Total maps: {total}")
    print(f"Used: {used}")
    print(f"Skipped (no data + weak): {skipped_no_data + skipped_weak}")
    print(f"Accuracy: {acc:.4f} ({hits}/{used})")
    print(f"Coverage: {cov:.4f}")
    
    print("\nAccuracy by strength:")
    for bin_name in ['0.7+', '0.5-0.7', '0.3-0.5', '<0.3']:
        b = strength_bins[bin_name]
        if b['used'] > 0:
            bin_acc = b['hits'] / b['used']
            print(f"  {bin_name}: {bin_acc:.4f} ({b['hits']}/{b['used']})")
    
    return {
        'total': total,
        'used': used,
        'hits': hits,
        'accuracy': acc,
        'coverage': cov,
        'strength_bins': dict(strength_bins),
    }


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--skip-days', type=int, default=14)
    p.add_argument('--max-days', type=int, default=30)
    p.add_argument('--min-strength', type=float, default=0.0)
    p.add_argument('--no-transitive', action='store_true')
    p.add_argument('--by-maps', action='store_true')
    p.add_argument('--progress', type=int, default=500)
    args = p.parse_args()
    
    run_full_backtest(
        skip_days=args.skip_days,
        max_days=args.max_days,
        min_strength=args.min_strength,
        use_transitive=not args.no_transitive,
        by_series=not args.by_maps,
        progress_every=args.progress,
    )
