#!/usr/bin/env python3
"""Optimize prediction strategy."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.transitive_analyzer import TransitiveAnalyzer, ELO_HISTORY_DAYS, DEFAULT_ELO_PARAMS
from core.player_elo_predictor import PlayerEloPredictor
from collections import defaultdict

def main():
    analyzer = TransitiveAnalyzer()
    predictor = PlayerEloPredictor(analyzer)

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
    test = series_list[:1500]

    results = []

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
        actual = r_wins > d_wins
        
        radiant_players = [p.get('steamAccount', {}).get('id') for p in m.get('players', []) if p.get('isRadiant')]
        dire_players = [p.get('steamAccount', {}).get('id') for p in m.get('players', []) if not p.get('isRadiant')]
        
        ratings = analyzer.compute_elo_ratings_up_to(ts, max_days=ELO_HISTORY_DAYS, **DEFAULT_ELO_PARAMS)
        team_elo_diff = ratings.get(rid, 1500) - ratings.get(did, 1500)
        
        r_elos = [predictor.get_player_elo_before(p, ts) for p in radiant_players if p]
        d_elos = [predictor.get_player_elo_before(p, ts) for p in dire_players if p]
        
        player_elo_diff = None
        max_player_diff = None
        
        if len(r_elos) >= 3 and len(d_elos) >= 3:
            player_elo_diff = sum(r_elos) / len(r_elos) - sum(d_elos) / len(d_elos)
            max_player_diff = max(r_elos) - max(d_elos)
        
        all_agree = (team_elo_diff > 0) == (player_elo_diff > 0) == (max_player_diff > 0) if max_player_diff else True
        max_team_agree = (max_player_diff > 0) == (team_elo_diff > 0) if max_player_diff else True
        
        results.append({
            'actual': actual,
            'team_elo_diff': team_elo_diff,
            'player_elo_diff': player_elo_diff,
            'max_player_diff': max_player_diff,
            'max_strength': abs(max_player_diff) if max_player_diff else 0,
            'all_agree': all_agree,
            'max_team_agree': max_team_agree,
            'n_maps': len(matches),
        })

    print(f'Total: {len(results)} series')
    print()

    # Baseline
    def predict_baseline(r):
        if r['n_maps'] <= 2:
            return r['max_player_diff'] > 0 if r['max_player_diff'] else r['team_elo_diff'] > 0
        else:
            return r['player_elo_diff'] > 0 if r['player_elo_diff'] else r['team_elo_diff'] > 0

    correct_baseline = sum(1 for r in results if predict_baseline(r) == r['actual'])
    print(f'Baseline: {correct_baseline}/{len(results)} = {correct_baseline/len(results)*100:.1f}%')

    # V5: Combined score
    def predict_v5(r):
        if r['max_player_diff'] is None:
            return r['team_elo_diff'] > 0
        score = (r['team_elo_diff'] / 100 + 
                 r['player_elo_diff'] / 50 + 
                 r['max_player_diff'] / 50)
        return score > 0

    correct_v5 = sum(1 for r in results if predict_v5(r) == r['actual'])
    print(f'V5 (combined score): {correct_v5}/{len(results)} = {correct_v5/len(results)*100:.1f}%')

    # V6: Majority vote
    def predict_v6(r):
        votes = []
        votes.append(r['team_elo_diff'] > 0)
        if r['player_elo_diff'] is not None:
            votes.append(r['player_elo_diff'] > 0)
        if r['max_player_diff'] is not None:
            votes.append(r['max_player_diff'] > 0)
        return sum(votes) > len(votes) / 2

    correct_v6 = sum(1 for r in results if predict_v6(r) == r['actual'])
    print(f'V6 (majority): {correct_v6}/{len(results)} = {correct_v6/len(results)*100:.1f}%')

    # V7: Weighted majority
    def predict_v7(r):
        if r['max_player_diff'] is None:
            return r['team_elo_diff'] > 0
        score = 0
        score += 2 if r['max_player_diff'] > 0 else -2
        score += 1 if r['player_elo_diff'] > 0 else -1
        score += 1 if r['team_elo_diff'] > 0 else -1
        return score > 0

    correct_v7 = sum(1 for r in results if predict_v7(r) == r['actual'])
    print(f'V7 (weighted max=2): {correct_v7}/{len(results)} = {correct_v7/len(results)*100:.1f}%')

    # Max only
    def predict_max(r):
        return r['max_player_diff'] > 0 if r['max_player_diff'] else r['team_elo_diff'] > 0

    correct_max = sum(1 for r in results if predict_max(r) == r['actual'])
    print(f'Max only: {correct_max}/{len(results)} = {correct_max/len(results)*100:.1f}%')

if __name__ == '__main__':
    main()
