#!/usr/bin/env python3
import sys
sys.path.insert(0, '.')

with open('data/test_output.txt', 'w') as f:
    f.write("Starting...\n")
    f.flush()
    
    from core.transitive_analyzer import TransitiveAnalyzer
    from collections import defaultdict
    
    f.write("Loading analyzer...\n")
    f.flush()
    
    analyzer = TransitiveAnalyzer()
    f.write(f"Loaded {len(analyzer.matches_sorted)} matches\n")
    f.flush()
    
    # Group by series
    series_map = defaultdict(list)
    for m in analyzer.matches_sorted:
        sid = m.get('seriesId', m.get('id'))
        series_map[sid].append(m)
    
    series_list = []
    for sid, matches in series_map.items():
        ts = min(m.get('startDateTime', 0) for m in matches)
        series_list.append((sid, matches, ts))
    
    series_list.sort(key=lambda x: x[2], reverse=True)
    test = series_list[:100]
    
    f.write(f"Testing {len(test)} series\n")
    f.flush()
    
    correct, total = 0, 0
    for i, (sid, matches, ts) in enumerate(test):
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
        
        ratings = analyzer.compute_elo_ratings_up_to(ts, max_days=365)
        elo_diff = ratings.get(rid, 1500) - ratings.get(did, 1500)
        pred = elo_diff > 0
        
        if pred == actual:
            correct += 1
        
        if (i + 1) % 10 == 0:
            f.write(f"Progress: {i+1}/100\n")
            f.flush()
    
    f.write(f"\nResult: {correct}/{total} = {correct/total*100:.1f}%\n")
    f.write("Done!\n")
