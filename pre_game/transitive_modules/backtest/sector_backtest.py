"""
Backtest for sector-based prediction strategy.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.transitive_analyzer import TransitiveAnalyzer, ELO_HISTORY_DAYS, DEFAULT_ELO_PARAMS
from core.player_elo_predictor import PlayerEloPredictor
from core.sector_predictor import predict_sector, is_high_confidence, get_sector_info
from collections import defaultdict


def run_backtest(n_series: int = 1000, offset: int = 0):
    """
    Run backtest on n_series starting from offset.
    
    Args:
        n_series: Number of series to test
        offset: Skip first N series (0 = most recent)
    """
    print(f'=== SECTOR BACKTEST (n={n_series}, offset={offset}) ===')
    print()
    
    analyzer = TransitiveAnalyzer()
    predictor = PlayerEloPredictor(analyzer)
    
    # Build series list
    series_map = defaultdict(list)
    for m in analyzer.matches_sorted:
        sid = m.get('seriesId', m.get('id'))
        series_map[sid].append(m)
    
    series_list = []
    for sid, matches in series_map.items():
        ts = min(m.get('startDateTime', 0) for m in matches)
        series_list.append((sid, matches, ts))
    
    series_list.sort(key=lambda x: x[2], reverse=True)
    test = series_list[offset:offset + n_series]
    
    results = []
    baseline_results = []
    
    for sid, matches, ts in test:
        m = matches[0]
        rt = m.get('radiantTeam') or {}
        dt = m.get('direTeam') or {}
        rid, did = rt.get('id'), dt.get('id')
        if not rid or not did:
            continue
        
        r_wins = sum(1 for mm in matches if mm.get('didRadiantWin'))
        d_wins = len(matches) - r_wins
        if r_wins == d_wins:
            continue
        actual = r_wins > d_wins
        
        radiant_players = [p.get('steamAccount', {}).get('id') for p in m.get('players', []) if p.get('isRadiant')]
        dire_players = [p.get('steamAccount', {}).get('id') for p in m.get('players', []) if not p.get('isRadiant')]
        
        ratings = analyzer.compute_elo_ratings_up_to(ts, max_days=ELO_HISTORY_DAYS, **DEFAULT_ELO_PARAMS)
        r_elo = ratings.get(rid, 1500)
        d_elo = ratings.get(did, 1500)
        team_elo_diff = r_elo - d_elo
        
        r_elos = [predictor.get_player_elo_before(p, ts) for p in radiant_players if p]
        d_elos = [predictor.get_player_elo_before(p, ts) for p in dire_players if p]
        
        if len(r_elos) < 3 or len(d_elos) < 3:
            continue
        
        player_elo_diff = sum(r_elos)/len(r_elos) - sum(d_elos)/len(d_elos)
        max_player_diff = max(r_elos) - max(d_elos)
        min_player_diff = min(r_elos) - min(d_elos)
        player_gap = abs(player_elo_diff)
        n_maps = len(matches)
        is_short = n_maps <= 2
        
        # Sector prediction
        pred, confidence, expected_acc = predict_sector(
            team_elo_diff, player_elo_diff, max_player_diff, min_player_diff,
            r_elo, d_elo, n_maps
        )
        
        results.append({
            'actual': actual,
            'pred': pred,
            'confidence': confidence,
            'is_high_conf': is_high_confidence(r_elo, d_elo, player_gap, n_maps),
            'is_short': is_short,
        })
        
        # Baseline prediction
        if is_short:
            baseline_pred = max_player_diff > 0
        else:
            score = 0.2*team_elo_diff/200 + 0.6*player_elo_diff/100 + 0.1*min_player_diff/100
            baseline_pred = score > 0
        
        baseline_results.append({
            'actual': actual,
            'pred': baseline_pred,
            'is_short': is_short,
        })
    
    # Results
    sector_correct = sum(1 for r in results if r['pred'] == r['actual'])
    baseline_correct = sum(1 for r in baseline_results if r['pred'] == r['actual'])
    
    print(f'Sector strategy: {sector_correct}/{len(results)} = {sector_correct/len(results)*100:.1f}%')
    print(f'Baseline:        {baseline_correct}/{len(baseline_results)} = {baseline_correct/len(baseline_results)*100:.1f}%')
    print(f'Improvement:     +{(sector_correct-baseline_correct)/len(results)*100:.1f}%')
    print()
    
    # By confidence
    print('By confidence:')
    for conf in ['high', 'medium', 'low']:
        data = [r for r in results if r['confidence'] == conf]
        if data:
            correct = sum(1 for r in data if r['pred'] == r['actual'])
            print(f'  {conf}: {correct}/{len(data)} = {correct/len(data)*100:.1f}% ({len(data)/len(results)*100:.1f}% cov)')
    
    print()
    
    # High confidence only
    high_conf = [r for r in results if r['is_high_conf']]
    if high_conf:
        hc_correct = sum(1 for r in high_conf if r['pred'] == r['actual'])
        print(f'High confidence: {hc_correct}/{len(high_conf)} = {hc_correct/len(high_conf)*100:.1f}% ({len(high_conf)/len(results)*100:.1f}% cov)')
    
    # By format
    print()
    print('By format:')
    for fmt, cond in [('Short', lambda r: r['is_short']), ('Long', lambda r: not r['is_short'])]:
        data = [r for r in results if cond(r)]
        baseline_data = [r for r in baseline_results if cond(r)]
        if data:
            correct = sum(1 for r in data if r['pred'] == r['actual'])
            bl_correct = sum(1 for r in baseline_data if r['pred'] == r['actual'])
            print(f'  {fmt}: sector={correct/len(data)*100:.1f}%, baseline={bl_correct/len(baseline_data)*100:.1f}%')
    
    return {
        'sector_acc': sector_correct / len(results),
        'baseline_acc': baseline_correct / len(baseline_results),
        'high_conf_acc': hc_correct / len(high_conf) if high_conf else 0,
        'high_conf_cov': len(high_conf) / len(results) if results else 0,
    }


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--n', type=int, default=1000, help='Number of series')
    parser.add_argument('--offset', type=int, default=0, help='Offset from most recent')
    args = parser.parse_args()
    
    run_backtest(args.n, args.offset)
