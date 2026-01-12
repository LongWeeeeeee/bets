#!/usr/bin/env python3
"""Тест новых методов compute_common_opponents_v2 и compute_transitive_chains_v2."""

import sys
import os
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from core.transitive_analyzer import TransitiveAnalyzer, DEFAULT_ELO_PARAMS


def main():
    print("Loading data...")
    analyzer = TransitiveAnalyzer()
    
    all_matches = [m for m in analyzer.matches_data.values() if m.get('startDateTime', 0) > 0]
    all_matches.sort(key=lambda m: m.get('startDateTime', 0))
    
    min_ts = all_matches[0].get('startDateTime', 0)
    start_ts = min_ts + 14 * 86400
    
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
        series_list.append({
            'series_id': series_id,
            'matches': matches,
            'start_ts': matches[0].get('startDateTime', 0),
        })
    
    series_list.sort(key=lambda s: s['start_ts'])
    print(f"Total series: {len(series_list)}")
    
    elo_cache = {}
    
    # Тест на всех сериях
    elo_hits = 0
    combined_hits = 0
    used = 0
    
    for series in series_list:
        matches = series['matches']
        ts = series['start_ts']
        
        rad = matches[0].get('radiantTeam') or {}
        dire = matches[0].get('direTeam') or {}
        rid, did = rad.get('id'), dire.get('id')
        
        if not rid or not did:
            continue
        
        r_maps = sum(1 for m in matches if m.get('didRadiantWin') is True)
        d_maps = sum(1 for m in matches if m.get('didRadiantWin') is False)
        if r_maps == d_maps:
            continue
        
        actual = rid if r_maps > d_maps else did
        
        day_ts = (ts // 86400) * 86400
        if day_ts not in elo_cache:
            elo_cache[day_ts] = analyzer.compute_elo_ratings_up_to(ts, max_days=365, **DEFAULT_ELO_PARAMS)
        
        elo = elo_cache[day_ts]
        elo_r, elo_d = elo.get(rid, 1500), elo.get(did, 1500)
        
        if elo_r == 1500 and elo_d == 1500:
            continue
        
        # Elo prediction
        elo_diff = (elo_r - elo_d) / 150
        elo_pred = rid if elo_diff > 0 else did
        
        # New methods
        common_score, common_n = analyzer.compute_common_opponents_v2(rid, did, ts, 21, elo)
        trans_score, trans_n = analyzer.compute_transitive_chains_v2(rid, did, ts, 21, elo)
        
        # Combined
        total = elo_diff + common_score * 0.3 + trans_score * 0.1
        combined_pred = rid if total > 0 else did
        
        used += 1
        if elo_pred == actual:
            elo_hits += 1
        if combined_pred == actual:
            combined_hits += 1
    
    print(f"\nResults on {used} series:")
    print(f"Elo only: {elo_hits/used:.4f} ({elo_hits}/{used})")
    print(f"Elo + Common_v2 + Trans_v2: {combined_hits/used:.4f} ({combined_hits}/{used})")
    print(f"Improvement: {(combined_hits - elo_hits)/used*100:+.2f}%")


if __name__ == "__main__":
    main()
