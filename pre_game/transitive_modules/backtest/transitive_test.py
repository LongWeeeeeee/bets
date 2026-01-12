import sys
sys.path.insert(0, '.')
from core.transitive_analyzer import TransitiveAnalyzer, ELO_HISTORY_DAYS, DEFAULT_ELO_PARAMS, ELO_BASE_RATING
from collections import defaultdict

analyzer = TransitiveAnalyzer()

series_map = defaultdict(list)
for m in analyzer.matches_sorted:
    sid = m.get('seriesId', m.get('id'))
    series_map[sid].append(m)

series_list = []
for sid, matches in series_map.items():
    ts = min(m.get('startDateTime', 0) for m in matches)
    series_list.append((sid, matches, ts))

series_list.sort(key=lambda x: x[2], reverse=True)
test_series = series_list[:300]

print('=== TRANSITIVE V3: SERIES-LEVEL ===')

def get_team_winrate(team_id, ts, current_sid, days=90, decay_hl=14):
    start = ts - days * 86400
    wins, total = 0.0, 0.0
    
    for m in analyzer.team_matches.get(team_id, []):
        mts = m.get('startDateTime', 0)
        if not (start <= mts < ts):
            continue
        msid = m.get('seriesId', m.get('id'))
        if msid == current_sid:
            continue
        
        rad = m.get('radiantTeam', {}).get('id')
        rw = m.get('didRadiantWin')
        won = (rad == team_id and rw) or (rad != team_id and not rw)
        
        days_ago = (ts - mts) / 86400
        decay = 0.5 ** (days_ago / decay_hl)
        
        total += decay
        if won:
            wins += decay
    
    return wins / total if total > 0 else 0.5

def compute_transitive_v3(t1, t2, ts, current_sid, days=90):
    start = ts - days * 86400
    
    # Собираем результаты по сериям
    t1_series = defaultdict(lambda: {'wins': 0, 'losses': 0})
    t2_series = defaultdict(lambda: {'wins': 0, 'losses': 0})
    
    # t1 matches by series
    t1_by_series = defaultdict(list)
    for m in analyzer.team_matches.get(t1, []):
        mts = m.get('startDateTime', 0)
        if not (start <= mts < ts):
            continue
        msid = m.get('seriesId', m.get('id'))
        if msid == current_sid:
            continue
        t1_by_series[msid].append(m)
    
    for msid, matches in t1_by_series.items():
        if not matches:
            continue
        m = matches[0]
        rad = m.get('radiantTeam', {}).get('id')
        dire = m.get('direTeam', {}).get('id')
        opp = dire if rad == t1 else rad
        if not opp or opp == t2:
            continue
        
        t1_maps = 0
        for mm in matches:
            rad_id = mm.get('radiantTeam', {}).get('id')
            rw = mm.get('didRadiantWin')
            if (rad_id == t1 and rw) or (rad_id != t1 and not rw):
                t1_maps += 1
        opp_maps = len(matches) - t1_maps
        
        if t1_maps > opp_maps:
            t1_series[opp]['wins'] += 1
        elif opp_maps > t1_maps:
            t1_series[opp]['losses'] += 1
    
    # t2 matches by series
    t2_by_series = defaultdict(list)
    for m in analyzer.team_matches.get(t2, []):
        mts = m.get('startDateTime', 0)
        if not (start <= mts < ts):
            continue
        msid = m.get('seriesId', m.get('id'))
        if msid == current_sid:
            continue
        t2_by_series[msid].append(m)
    
    for msid, matches in t2_by_series.items():
        if not matches:
            continue
        m = matches[0]
        rad = m.get('radiantTeam', {}).get('id')
        dire = m.get('direTeam', {}).get('id')
        opp = dire if rad == t2 else rad
        if not opp or opp == t1:
            continue
        
        t2_maps = 0
        for mm in matches:
            rad_id = mm.get('radiantTeam', {}).get('id')
            rw = mm.get('didRadiantWin')
            if (rad_id == t2 and rw) or (rad_id != t2 and not rw):
                t2_maps += 1
        opp_maps = len(matches) - t2_maps
        
        if t2_maps > opp_maps:
            t2_series[opp]['wins'] += 1
        elif opp_maps > t2_maps:
            t2_series[opp]['losses'] += 1
    
    # Common opponents
    common = set(t1_series.keys()) & set(t2_series.keys())
    
    if not common:
        return None, 0
    
    t1_better, t2_better = 0, 0
    for opp in common:
        t1_w = t1_series[opp]['wins']
        t1_l = t1_series[opp]['losses']
        t2_w = t2_series[opp]['wins']
        t2_l = t2_series[opp]['losses']
        
        t1_wr = t1_w / (t1_w + t1_l) if (t1_w + t1_l) > 0 else 0.5
        t2_wr = t2_w / (t2_w + t2_l) if (t2_w + t2_l) > 0 else 0.5
        
        if t1_wr > t2_wr:
            t1_better += 1
        elif t2_wr > t1_wr:
            t2_better += 1
    
    score = (t1_better - t2_better) / len(common)
    return score, len(common)

# Test
results = []
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
    
    ratings = analyzer.compute_elo_ratings_up_to(ts, max_days=ELO_HISTORY_DAYS, **DEFAULT_ELO_PARAMS)
    elo_diff = ratings.get(rid, ELO_BASE_RATING) - ratings.get(did, ELO_BASE_RATING)
    
    trans_score, n_common = compute_transitive_v3(rid, did, ts, sid)
    wr_r = get_team_winrate(rid, ts, sid)
    wr_d = get_team_winrate(did, ts, sid)
    
    results.append({
        'actual': actual,
        'elo_diff': elo_diff,
        'trans_score': trans_score,
        'n_common': n_common,
        'wr_diff': wr_r - wr_d,
    })

print(f'Total: {len(results)}')

with_trans = [r for r in results if r['trans_score'] is not None]
print(f'With transitive: {len(with_trans)}')

# Elo only
correct = sum(1 for r in results if (r['elo_diff'] > 0) == r['actual'])
print(f'\nElo only: {correct}/{len(results)} = {correct/len(results)*100:.1f}%')

# Winrate diff
correct = sum(1 for r in results if (r['wr_diff'] > 0) == r['actual'])
print(f'Winrate diff: {correct}/{len(results)} = {correct/len(results)*100:.1f}%')

if with_trans:
    correct = sum(1 for r in with_trans if (r['trans_score'] > 0) == r['actual'])
    print(f'Transitive only: {correct}/{len(with_trans)} = {correct/len(with_trans)*100:.1f}%')
