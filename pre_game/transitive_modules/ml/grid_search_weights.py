#!/usr/bin/env python3
"""Grid search по весам для transitive_analyzer."""

import sys
import os
import json
import contextlib
import io
from datetime import datetime
from collections import defaultdict
from itertools import product
import csv

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.transitive_analyzer import TransitiveAnalyzer, get_transitiv

# Глобальный кэш
_ANALYZER = None
_SERIES_CACHE = None


def get_analyzer():
    global _ANALYZER
    if _ANALYZER is None:
        _ANALYZER = TransitiveAnalyzer()
    return _ANALYZER


def get_series_list(skip_days: int = 14):
    global _SERIES_CACHE
    if _SERIES_CACHE is not None:
        return _SERIES_CACHE
    
    analyzer = get_analyzer()
    all_matches = [m for m in analyzer.matches_data.values() if m.get('startDateTime', 0) > 0]
    all_matches.sort(key=lambda m: m.get('startDateTime', 0))
    
    if not all_matches:
        return []
    
    min_ts = all_matches[0].get('startDateTime', 0)
    start_ts = min_ts + skip_days * 86400
    
    series_dict = defaultdict(list)
    for m in all_matches:
        ts = m.get('startDateTime', 0)
        if ts < start_ts:
            continue
        series_id = m.get('seriesId', m.get('id'))
        series_dict[series_id].append(m)
    
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
    _SERIES_CACHE = series_list
    return series_list


def run_backtest_with_weights(
    weights: dict,
    feature_weights: dict,
    skip_days: int = 14,
    max_days: int = 30,
    min_strength: float = 0.0,
    use_transitive: bool = True,
    sample_size: int = 0,  # 0 = все данные
):
    """Бэктест с заданными весами."""
    analyzer = get_analyzer()
    series_list = get_series_list(skip_days)
    
    # Собираем серии
    all_matches = [m for m in analyzer.matches_data.values() if m.get('startDateTime', 0) > 0]
    all_matches.sort(key=lambda m: m.get('startDateTime', 0))
    
    if not all_matches:
        return {'accuracy': 0, 'used': 0}
    
    min_ts = all_matches[0].get('startDateTime', 0)
    start_ts = min_ts + skip_days * 86400
    
    # Группируем по сериям
    series_dict = defaultdict(list)
    for m in all_matches:
        ts = m.get('startDateTime', 0)
        if ts < start_ts:
            continue
        series_id = m.get('seriesId', m.get('id'))
        series_dict[series_id].append(m)
    
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
    
    if sample_size > 0:
        series_list = series_list[:sample_size]
    
    used = 0
    hits = 0
    high_conf_used = 0
    high_conf_hits = 0
    
    for series in series_list:
        matches = series['matches']
        series_ts = series['start_ts']
        
        rad = matches[0].get('radiantTeam') or {}
        dire = matches[0].get('direTeam') or {}
        rid = rad.get('id')
        did = dire.get('id')
        rname = rad.get('name', f'Team_{rid}')
        dname = dire.get('name', f'Team_{did}')
        
        if not rid or not did:
            continue
        
        radiant_maps = sum(1 for m in matches if m.get('didRadiantWin') is True)
        dire_maps = sum(1 for m in matches if m.get('didRadiantWin') is False)
        
        if radiant_maps == dire_maps:
            continue
        
        actual_winner_id = rid if radiant_maps > dire_maps else did
        
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
                weights=weights,
                verbose=False,
            )
        
        if not res.get('has_data'):
            continue
        
        strength = float(res.get('strength', 0.0) or 0.0)
        if strength < min_strength:
            continue
        
        # Применяем feature_weights к total_score
        base_score = float(res.get('total_score', 0.0) or 0.0)
        
        # Добавляем влияние новых фич
        form_diff = float(res.get('form_diff', 0.0) or 0.0)
        momentum_diff = float(res.get('momentum_diff', 0.0) or 0.0)
        streak_diff = float(res.get('streak_diff', 0.0) or 0.0)
        consistency_diff = float(res.get('consistency_diff', 0.0) or 0.0)
        activity_score = float(res.get('activity_score', 0.0) or 0.0)
        
        # Нормализуем и добавляем
        adjusted_score = base_score
        adjusted_score += form_diff * feature_weights.get('form', 0.0)
        adjusted_score += (momentum_diff / 100.0) * feature_weights.get('momentum', 0.0)  # momentum в Elo points
        adjusted_score += (streak_diff / 5.0) * feature_weights.get('streak', 0.0)  # streak обычно -10..+10
        adjusted_score += consistency_diff * feature_weights.get('consistency', 0.0)
        adjusted_score += activity_score * feature_weights.get('activity', 0.0)
        
        if adjusted_score > 0:
            pred_id = rid
        elif adjusted_score < 0:
            pred_id = did
        else:
            continue
        
        used += 1
        hit = (pred_id == actual_winner_id)
        if hit:
            hits += 1
        
        if strength >= 0.5:
            high_conf_used += 1
            if hit:
                high_conf_hits += 1
    
    acc = hits / used if used > 0 else 0
    high_conf_acc = high_conf_hits / high_conf_used if high_conf_used > 0 else 0
    
    return {
        'accuracy': acc,
        'used': used,
        'hits': hits,
        'high_conf_accuracy': high_conf_acc,
        'high_conf_used': high_conf_used,
    }


def grid_search():
    """Grid search по весам."""
    
    # Базовые веса (source weights) - уменьшенная сетка
    h2h_values = [1.5, 2.0, 3.0]
    common_values = [1.5, 2.5, 3.5]
    trans_values = [0.0, 0.2, 0.4]
    elo_values = [0.5, 1.0, 2.0]
    
    # Feature weights (новые фичи) - уменьшенная сетка
    form_values = [0.0, 0.5, 1.0]
    momentum_values = [0.0, 0.3]
    streak_values = [0.0, 0.3]
    consistency_values = [0.0, 0.3]
    activity_values = [0.0, 0.3]
    
    # Сначала ищем лучшие базовые веса на sample
    print("=" * 60)
    print("PHASE 1: Grid search базовых весов (sample=1000)")
    print("=" * 60)
    
    best_acc = 0
    best_weights = None
    results = []
    
    total_combos = len(h2h_values) * len(common_values) * len(trans_values) * len(elo_values)
    combo_idx = 0
    
    for h2h, common, trans, elo in product(h2h_values, common_values, trans_values, elo_values):
        combo_idx += 1
        weights = {
            'head_to_head': h2h,
            'common_opponents': common,
            'transitive': trans,
            'elo': elo,
        }
        feature_weights = {'form': 0, 'momentum': 0, 'streak': 0, 'consistency': 0, 'activity': 0}
        
        res = run_backtest_with_weights(
            weights=weights,
            feature_weights=feature_weights,
            sample_size=1000,
        )
        
        results.append({
            'h2h': h2h, 'common': common, 'trans': trans, 'elo': elo,
            **res
        })
        
        if res['accuracy'] > best_acc:
            best_acc = res['accuracy']
            best_weights = weights.copy()
            print(f"[{combo_idx}/{total_combos}] NEW BEST: {best_acc:.4f} | h2h={h2h}, common={common}, trans={trans}, elo={elo}")
        
        if combo_idx % 20 == 0:
            print(f"[{combo_idx}/{total_combos}] Current best: {best_acc:.4f}")
    
    print(f"\nBest base weights: {best_weights}")
    print(f"Best accuracy: {best_acc:.4f}")
    
    # Phase 2: Grid search feature weights с лучшими базовыми весами
    print("\n" + "=" * 60)
    print("PHASE 2: Grid search feature weights (sample=1000)")
    print("=" * 60)
    
    best_feature_acc = best_acc
    best_feature_weights = {'form': 0, 'momentum': 0, 'streak': 0, 'consistency': 0, 'activity': 0}
    
    total_feature_combos = len(form_values) * len(momentum_values) * len(streak_values) * len(consistency_values) * len(activity_values)
    combo_idx = 0
    
    for form, momentum, streak, consistency, activity in product(
        form_values, momentum_values, streak_values, consistency_values, activity_values
    ):
        combo_idx += 1
        feature_weights = {
            'form': form,
            'momentum': momentum,
            'streak': streak,
            'consistency': consistency,
            'activity': activity,
        }
        
        res = run_backtest_with_weights(
            weights=best_weights,
            feature_weights=feature_weights,
            sample_size=1000,
        )
        
        if res['accuracy'] > best_feature_acc:
            best_feature_acc = res['accuracy']
            best_feature_weights = feature_weights.copy()
            print(f"[{combo_idx}/{total_feature_combos}] NEW BEST: {best_feature_acc:.4f} | {feature_weights}")
        
        if combo_idx % 50 == 0:
            print(f"[{combo_idx}/{total_feature_combos}] Current best: {best_feature_acc:.4f}")
    
    print(f"\nBest feature weights: {best_feature_weights}")
    print(f"Best accuracy with features: {best_feature_acc:.4f}")
    
    # Phase 3: Валидация на полных данных
    print("\n" + "=" * 60)
    print("PHASE 3: Validation on full data")
    print("=" * 60)
    
    # Baseline (текущие веса)
    baseline_weights = {
        'head_to_head': 2.0,
        'common_opponents': 2.5,
        'transitive': 0.25,
        'elo': 1.0,
    }
    baseline_features = {'form': 0, 'momentum': 0, 'streak': 0, 'consistency': 0, 'activity': 0}
    
    print("Testing baseline...")
    baseline_res = run_backtest_with_weights(
        weights=baseline_weights,
        feature_weights=baseline_features,
        sample_size=0,
    )
    print(f"Baseline: acc={baseline_res['accuracy']:.4f}, used={baseline_res['used']}, high_conf_acc={baseline_res['high_conf_accuracy']:.4f}")
    
    print("\nTesting optimized weights...")
    optimized_res = run_backtest_with_weights(
        weights=best_weights,
        feature_weights=best_feature_weights,
        sample_size=0,
    )
    print(f"Optimized: acc={optimized_res['accuracy']:.4f}, used={optimized_res['used']}, high_conf_acc={optimized_res['high_conf_accuracy']:.4f}")
    
    print("\n" + "=" * 60)
    print("FINAL RESULTS")
    print("=" * 60)
    print(f"Baseline accuracy: {baseline_res['accuracy']:.4f}")
    print(f"Optimized accuracy: {optimized_res['accuracy']:.4f}")
    print(f"Improvement: {(optimized_res['accuracy'] - baseline_res['accuracy']) * 100:.2f}%")
    print(f"\nBest base weights: {best_weights}")
    print(f"Best feature weights: {best_feature_weights}")
    
    # Сохраняем результаты
    final_config = {
        'base_weights': best_weights,
        'feature_weights': best_feature_weights,
        'baseline_accuracy': baseline_res['accuracy'],
        'optimized_accuracy': optimized_res['accuracy'],
        'high_conf_accuracy': optimized_res['high_conf_accuracy'],
    }
    
    with open('grid_search_results.json', 'w') as f:
        json.dump(final_config, f, indent=2)
    
    print(f"\nResults saved to grid_search_results.json")
    
    return final_config


if __name__ == "__main__":
    grid_search()
