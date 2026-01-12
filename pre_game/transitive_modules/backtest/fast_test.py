#!/usr/bin/env python3
"""Fast test with pre-computed ratings."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.transitive_analyzer import TransitiveAnalyzer, ELO_HISTORY_DAYS, DEFAULT_ELO_PARAMS
from core.player_elo_predictor import PlayerEloPredictor
from collections import defaultdict
import time

def main():
    print("=== FAST TEST (NO DATA LEAK) ===")
    print()
    
    start = time.time()
    
    analyzer = TransitiveAnalyzer()
    predictor = PlayerEloPredictor(analyzer)
    
    print(f"Loaded in {time.time()-start:.1f}s")
    
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
    
    # Pre-compute team ratings at key timestamps to speed up
    # For now, just test on smaller sample
    
    for n_series in [500, 700, 1000, 1500]:
        test = series_list[:n_series]
        
        correct = 0
        total = 0
        short_correct = 0
        short_total = 0
        long_correct = 0
        long_total = 0
        
        start_test = time.time()
        
        for sid, matches, ts in test:
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
            
            # Team Elo - NO DATA LEAK
            ratings = analyzer.compute_elo_ratings_up_to(ts, max_days=ELO_HISTORY_DAYS, **DEFAULT_ELO_PARAMS)
            team_elo_diff = ratings.get(rid, 1500) - ratings.get(did, 1500)
            
            # Player Elo - NO DATA LEAK
            r_elos = [predictor.get_player_elo_before(p, ts) for p in radiant_players if p]
            d_elos = [predictor.get_player_elo_before(p, ts) for p in dire_players if p]
            
            if len(r_elos) >= 3 and len(d_elos) >= 3:
                max_player_diff = max(r_elos) - max(d_elos)
                player_elo_diff = sum(r_elos)/len(r_elos) - sum(d_elos)/len(d_elos)
            else:
                max_player_diff = None
                player_elo_diff = None
            
            is_short = len(matches) <= 2
            
            if is_short:
                pred = max_player_diff > 0 if max_player_diff else team_elo_diff > 0
                short_total += 1
                if pred == actual:
                    short_correct += 1
            else:
                pred = player_elo_diff > 0 if player_elo_diff else team_elo_diff > 0
                long_total += 1
                if pred == actual:
                    long_correct += 1
            
            total += 1
            if pred == actual:
                correct += 1
        
        elapsed = time.time() - start_test
        acc = correct/total*100 if total else 0
        marker = " <-- 65%+" if acc >= 65 else ""
        
        print(f"Last {n_series}: {correct}/{total} = {acc:.1f}%{marker} ({elapsed:.1f}s)")
        if short_total:
            print(f"  Short: {short_correct}/{short_total} = {short_correct/short_total*100:.1f}%")
        if long_total:
            print(f"  Long: {long_correct}/{long_total} = {long_correct/long_total*100:.1f}%")
        print()

if __name__ == '__main__':
    main()
