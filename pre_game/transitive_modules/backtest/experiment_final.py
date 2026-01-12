#!/usr/bin/env python3
"""Финальная модель с адаптивными весами."""

import sys
import os
from collections import defaultdict
import math

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from core.transitive_analyzer import TransitiveAnalyzer, DEFAULT_ELO_PARAMS, DECAY_HALF_LIFE_DAYS


def compute_h2h(analyzer, rid, did, ts, max_days=30):
    start_time = ts - max_days * 86400
    key = tuple(sorted((rid, did)))
    pair_series = analyzer.pair_series.get(key, {})
    
    r_val, d_val, n = 0.0, 0.0, 0
    for sid, matches in pair_series.items():
        valid = [m for m in matches if start_time <= m.get('startDateTime', 0) < ts]
        if not valid:
            continue
        n += 1
        r_w, d_w, decay_sum = 0, 0, 0.0
        for m in valid:
            m_rid = (m.get('radiantTeam') or {}).get('id')
            rw = m.get('didRadiantWin')
            mt = m.get('startDateTime', 0)
            decay = 0.5 ** ((ts - mt) / 86400 / DECAY_HALF_LIFE_DAYS)
            decay_sum += decay
            if m_rid == rid:
                r_w += 1 if rw else 0
                d_w += 0 if rw else 1
            else:
                r_w += 0 if rw else 1
                d_w += 1 if rw else 0
        avg_decay = decay_sum / len(valid) if valid else 1.0
        total = r_w + d_w
        if r_w > d_w:
            r_val += (1.0 + (r_w - d_w) / total) * avg_decay
        elif d_w > r_w:
            d_val += (1.0 + (d_w - r_w) / total) * avg_decay
    return r_val - d_val, n


def compute_common(analyzer, rid, did, ts, elo, max_days=21):
    start_time = ts - max_days * 86400
    avg_elo = sum(elo.values()) / len(elo) if elo else 1500
    
    r_data, d_data = defaultdict(list), defaultdict(list)
    for team_id, data in [(rid, r_data), (did, d_data)]:
        for m in analyzer.team_matches.get(team_id, []):
            mt = m.get('startDateTime', 0)
            if not (start_time <= mt < ts):
                continue
            rad = m.get('radiantTeam') or {}
            dire = m.get('direTeam') or {}
            r_id, d_id = rad.get('id'), dire.get('id')
            opp_id = d_id if r_id == team_id else r_id
            if not opp_id or opp_id in (rid, did):
                continue
            rw = m.get('didRadiantWin')
            margin = (1 if rw else -1) if r_id == team_id else (-1 if rw else 1)
            decay = 0.5 ** ((ts - mt) / 86400 / 7)
            data[opp_id].append((margin, decay))
    
    common = set(r_data.keys()) & set(d_data.keys())
    if not common:
        return 0.0, 0
    
    score, total_w, n = 0.0, 0.0, 0
    for opp in common:
        quality = elo.get(opp, 1500) / avg_elo
        r_perf = sum(m * d for m, d in r_data[opp])
        r_w = sum(d for m, d in r_data[opp])
        d_perf = sum(m * d for m, d in d_data[opp])
        d_w = sum(d for m, d in d_data[opp])
        if r_w > 0 and d_w > 0:
            diff = (r_perf / r_w - d_perf / d_w) * quality
            w = min(r_w, d_w) * quality
            score += diff * w
            total_w += w
            n += 1
    return (score / total_w if total_w > 0 else 0), n


def compute_transitive(analyzer, rid, did, ts, elo, max_days=21):
    start_time = ts - max_days * 86400
    avg_elo = sum(elo.values()) / len(elo) if elo else 1500
    
    edges = defaultdict(lambda: {'r_w': 0, 'd_w': 0})
    neighbors = defaultdict(set)
    for m in analyzer.matches_sorted:
        mt = m.get('startDateTime', 0)
        if mt >= ts or mt < start_time:
            continue
        rad = m.get('radiantTeam') or {}
        dire = m.get('direTeam') or {}
        r_id, d_id = rad.get('id'), dire.get('id')
        if not r_id or not d_id:
            continue
        rw = m.get('didRadiantWin')
        key = (r_id, d_id)
        if rw:
            edges[key]['r_w'] += 1
        else:
            edges[key]['d_w'] += 1
        neighbors[r_id].add(d_id)
        neighbors[d_id].add(r_id)
    
    paths = []
    queue = [(rid, [rid])]
    while queue and len(paths) < 50:
        cur, path = queue.pop(0)
        if len(path) > 4:
            continue
        for nb in neighbors[cur]:
            if nb in path:
                continue
            new_path = path + [nb]
            if nb == did and len(new_path) >= 3:
                paths.append(new_path)
            elif len(new_path) <= 3:
                queue.append((nb, new_path))
    
    if not paths:
        return 0.0, 0
    
    score, n = 0.0, 0
    for path in paths:
        chain_wr = 1.0
        valid = True
        for i in range(len(path) - 1):
            a, b = path[i], path[i + 1]
            k1, k2 = (a, b), (b, a)
            wins = edges[k1]['r_w'] + edges[k2]['d_w']
            losses = edges[k1]['d_w'] + edges[k2]['r_w']
            total = wins + losses
            if total < 2:
                valid = False
                break
            wr = wins / total
            quality = elo.get(b, 1500) / avg_elo
            chain_wr *= (0.5 + (wr - 0.5) * quality)
        if valid:
            length_decay = 1.0 / (len(path) - 1)
            score += (chain_wr - 0.5) * 2 * length_decay
            n += 1
    return (score / n if n > 0 else 0), n


def adaptive_predict(elo_diff, h2h_diff, h2h_n, common_diff, common_n, trans_diff, trans_n):
    """Адаптивное предсказание с разными весами в зависимости от ситуации."""
    
    elo_abs = abs(elo_diff)
    h2h_abs = abs(h2h_diff)
    
    # Базовый score от Elo
    score = elo_diff
    
    # Адаптивные веса для H2H
    if h2h_n > 0:
        # H2H согласен с Elo?
        agrees = (elo_diff > 0 and h2h_diff > 0) or (elo_diff < 0 and h2h_diff < 0)
        
        if agrees:
            # Усиливаем сигнал
            h2h_weight = 0.5
        elif h2h_abs > 1.5:
            # Сильный H2H противоречит - небольшой вес
            h2h_weight = 0.2
        else:
            # Слабый H2H противоречит - игнорируем
            h2h_weight = 0
        
        score += h2h_diff * h2h_weight
    
    # Common важнее когда Elo близко
    if common_n > 0:
        if elo_abs < 50:
            common_weight = 0.4
        elif elo_abs < 100:
            common_weight = 0.3
        else:
            common_weight = 0.2
        score += common_diff * common_weight
    
    # Transitive как дополнительный сигнал
    if trans_n > 0:
        if elo_abs < 50:
            trans_weight = 0.2
        else:
            trans_weight = 0.1
        score += trans_diff * trans_weight
    
    return score


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
    
    # Собираем все данные
    all_data = []
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
        
        day_ts = (ts // 86400) * 86400
        if day_ts not in elo_cache:
            elo_cache[day_ts] = analyzer.compute_elo_ratings_up_to(ts, max_days=365, **DEFAULT_ELO_PARAMS)
        
        elo = elo_cache[day_ts]
        elo_r, elo_d = elo.get(rid, 1500), elo.get(did, 1500)
        
        if elo_r == 1500 and elo_d == 1500:
            continue
        
        h2h_diff, h2h_n = compute_h2h(analyzer, rid, did, ts)
        common_diff, common_n = compute_common(analyzer, rid, did, ts, elo)
        trans_diff, trans_n = compute_transitive(analyzer, rid, did, ts, elo)
        
        all_data.append({
            'rid': rid, 'did': did,
            'actual': rid if r_maps > d_maps else did,
            'elo_diff': (elo_r - elo_d) / 150,
            'elo_abs': abs(elo_r - elo_d),
            'h2h_diff': h2h_diff, 'h2h_n': h2h_n,
            'common_diff': common_diff, 'common_n': common_n,
            'trans_diff': trans_diff, 'trans_n': trans_n,
        })
    
    print(f"Valid series: {len(all_data)}")
    
    # Тест разных стратегий
    def test_strategy(name, predict_func):
        hits = 0
        for d in all_data:
            score = predict_func(d)
            pred = d['rid'] if score > 0 else d['did']
            if pred == d['actual']:
                hits += 1
        acc = hits / len(all_data)
        print(f"{name}: {acc:.4f} ({hits}/{len(all_data)})")
        return acc
    
    print("\n=== СТРАТЕГИИ НА ВСЕХ ДАННЫХ ===")
    
    test_strategy("Elo only", lambda d: d['elo_diff'])
    
    test_strategy("Elo + H2H(0.5)", 
                  lambda d: d['elo_diff'] + d['h2h_diff'] * 0.5)
    
    test_strategy("Elo + Common(0.3)", 
                  lambda d: d['elo_diff'] + d['common_diff'] * 0.3)
    
    test_strategy("Elo + H2H + Common + Trans (fixed)", 
                  lambda d: d['elo_diff'] + d['h2h_diff'] * 0.3 + d['common_diff'] * 0.3 + d['trans_diff'] * 0.1)
    
    test_strategy("ADAPTIVE", 
                  lambda d: adaptive_predict(
                      d['elo_diff'], d['h2h_diff'], d['h2h_n'],
                      d['common_diff'], d['common_n'],
                      d['trans_diff'], d['trans_n']))
    
    # По группам Elo
    print("\n=== ПО ГРУППАМ ELO ===")
    
    for name, filter_func in [
        ("High Elo (>100)", lambda d: d['elo_abs'] > 100),
        ("Medium Elo (50-100)", lambda d: 50 < d['elo_abs'] <= 100),
        ("Low Elo (<50)", lambda d: d['elo_abs'] <= 50),
    ]:
        filtered = [d for d in all_data if filter_func(d)]
        if not filtered:
            continue
        
        elo_hits = sum(1 for d in filtered if (d['rid'] if d['elo_diff'] > 0 else d['did']) == d['actual'])
        adaptive_hits = sum(1 for d in filtered if (d['rid'] if adaptive_predict(
            d['elo_diff'], d['h2h_diff'], d['h2h_n'],
            d['common_diff'], d['common_n'],
            d['trans_diff'], d['trans_n']) > 0 else d['did']) == d['actual'])
        
        print(f"{name} (n={len(filtered)}): Elo={elo_hits/len(filtered):.4f}, Adaptive={adaptive_hits/len(filtered):.4f}")


if __name__ == "__main__":
    main()
