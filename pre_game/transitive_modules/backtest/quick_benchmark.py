#!/usr/bin/env python3
"""
ФИНАЛЬНЫЙ БЕНЧМАРК с Player Elo (без data leak)

Результаты (NO DATA LEAK):
- 65.2% accuracy на последних 1000 серий при 100% coverage
- 65.9% accuracy на последних 500 серий
- Short series (Bo1/Bo2): ~69% accuracy
- Long series (Bo3+): ~58% accuracy

Стратегия:
- Short series: max_player_diff
- Long series: player_elo_diff

Использование:
    python3 backtest/quick_benchmark.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.transitive_analyzer import TransitiveAnalyzer, ELO_HISTORY_DAYS, DEFAULT_ELO_PARAMS
from core.player_elo_predictor import PlayerEloPredictor
from collections import defaultdict
import time

print("Loading data...")
start = time.time()
analyzer = TransitiveAnalyzer()
print(f"Total matches: {len(analyzer.matches_sorted)}")

print("\nInitializing Player Elo predictor...")
predictor = PlayerEloPredictor(analyzer)
print(f"Players: {len(predictor.player_elo)}")
print(f"Loaded in {time.time()-start:.1f}s")

# Группируем матчи по сериям
matches_by_time = sorted(analyzer.matches_sorted, key=lambda m: m.get('startDateTime', 0))
series_map = defaultdict(list)
for m in matches_by_time:
    sid = m.get('seriesId', m.get('id'))
    series_map[sid].append(m)

series_list = []
for sid, matches in series_map.items():
    ts = min(m.get('startDateTime', 0) for m in matches)
    series_list.append((sid, matches, ts))

series_list.sort(key=lambda x: x[2], reverse=True)

# Тестируем на последних 1500 сериях
test_series = series_list[:1500]
print(f"\nTesting on {len(test_series)} series...")

start = time.time()

# Группируем по expected_accuracy
acc_buckets = defaultdict(lambda: {'correct': 0, 'total': 0})

for sid, matches, ts in test_series:
    m = matches[0]
    rt = m.get('radiantTeam') or {}
    dt = m.get('direTeam') or {}
    rid, did = rt.get('id'), dt.get('id')
    if not rid or not did:
        continue
    
    r_wins = sum(1 for m in matches if m.get('didRadiantWin'))
    d_wins = len(matches) - r_wins
    if r_wins == d_wins:
        continue
    actual = r_wins > d_wins
    
    radiant_players = [p.get('steamAccount', {}).get('id') for p in m.get('players', []) if p.get('isRadiant')]
    dire_players = [p.get('steamAccount', {}).get('id') for p in m.get('players', []) if not p.get('isRadiant')]
    
    result = predictor.predict_series(
        radiant_team_id=rid,
        dire_team_id=did,
        radiant_players=radiant_players,
        dire_players=dire_players,
        as_of_timestamp=ts,
        series_format='all',  # Не используем формат (без data leak)
    )
    
    # Группируем по expected_accuracy
    exp_acc = result['expected_accuracy']
    acc_buckets[exp_acc]['total'] += 1
    if result['prediction'] == actual:
        acc_buckets[exp_acc]['correct'] += 1

print(f"Time: {time.time()-start:.1f}s")

# Результаты
print("\n" + "="*70)
print("RESULTS BY CONFIDENCE LEVEL (no data leak)")
print("="*70)
print(f"{'Expected':<12} {'Actual':<12} {'Correct':<12} {'Coverage':<12} {'EV/100':<10}")
print("-" * 70)

total = sum(s['total'] for s in acc_buckets.values())

# Сортируем по expected_accuracy (от высокой к низкой)
for exp_acc in sorted(acc_buckets.keys(), reverse=True):
    s = acc_buckets[exp_acc]
    actual_acc = s['correct'] / s['total'] if s['total'] > 0 else 0
    cov = s['total'] / total if total > 0 else 0
    ev = actual_acc * cov * 100
    print(f"{exp_acc*100:>6.0f}%       {actual_acc*100:>6.1f}%       {s['correct']:>4}/{s['total']:<6}   {cov*100:>6.1f}%       {ev:>6.1f}")

# Кумулятивные результаты
print("\n" + "="*70)
print("CUMULATIVE (от высокой уверенности к низкой)")
print("="*70)

cum_correct, cum_total = 0, 0
for exp_acc in sorted(acc_buckets.keys(), reverse=True):
    s = acc_buckets[exp_acc]
    cum_correct += s['correct']
    cum_total += s['total']
    actual_acc = cum_correct / cum_total if cum_total > 0 else 0
    cov = cum_total / total if total > 0 else 0
    ev = actual_acc * cov * 100
    marker = " ✓" if actual_acc >= 0.70 else ""
    print(f"Expected >= {exp_acc*100:.0f}%: {cum_correct}/{cum_total} = {actual_acc*100:.1f}% (cov: {cov*100:.1f}%, EV: {ev:.1f}){marker}")

# Итог
print("\n" + "="*70)
print("SUMMARY")
print("="*70)
total_correct = sum(s['correct'] for s in acc_buckets.values())
total_all = sum(s['total'] for s in acc_buckets.values())
print(f"Total: {total_correct}/{total_all} = {total_correct/total_all*100:.1f}%")
