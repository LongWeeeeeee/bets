#!/usr/bin/env python3
import sys
sys.path.insert(0, '.')
print("Starting...", flush=True)

from core.transitive_analyzer import TransitiveAnalyzer
from collections import defaultdict

print("Loading analyzer...", flush=True)
analyzer = TransitiveAnalyzer()
print(f"Loaded {len(analyzer.matches_sorted)} matches", flush=True)

# Quick Elo test on 100 series
series_map = defaultdict(list)
for m in analyzer.matches_sorted:
    sid = m.get('seriesId', m.get('id'))
    series_map[sid].append(m)

series_list = sorted(
    [(sid, matches, min(m.get('startDateTime', 0) for m in matches)) 
     for sid, matches in series_map.items()],
    key=lambda x: x[2], reverse=True
)[:100]

print(f"Testing on {len(series_list)} series...", flush=True)

correct, total = 0, 0
for sid, matches, ts in series_list:
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
    
    total += 1
    actual = r_wins > d_wins
    
    # Simple Elo prediction
    ratings = analyzer.compute_elo_ratings_up_to(ts, max_days=365)
    elo_diff = ratings.get(rid, 1500) - ratings.get(did, 1500)
    pred = elo_diff > 0
    
    if pred == actual:
        correct += 1

print(f"Result: {correct}/{total} = {correct/total*100:.1f}%", flush=True)
