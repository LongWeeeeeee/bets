#!/usr/bin/env python3
"""Эксперименты с улучшением Common Opponents и Transitive chains."""

import sys
import os
import contextlib
import io
from datetime import datetime
from collections import defaultdict
import math

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from core.transitive_analyzer import TransitiveAnalyzer, DEFAULT_ELO_PARAMS


def get_series_winner(matches: list, team_id: int) -> tuple:
    """Возвращает (wins, losses, won_series) для команды в серии."""
    wins = 0
    losses = 0
    for m in matches:
        rad = m.get('radiantTeam') or {}
        dire = m.get('direTeam') or {}
        rid = rad.get('id')
        rw = m.get('didRadiantWin')
        
        if rid == team_id:
            if rw:
                wins += 1
            else:
                losses += 1
        else:
            if rw:
                losses += 1
            else:
                wins += 1
    
    return wins, losses, wins > losses


def compute_h2h_score(analyzer, team1_id, team2_id, as_of_ts, max_days=30):
    """Вычисляет H2H score между командами."""
    start_time = as_of_ts - max_days * 86400
    
    h2h_series = analyzer.find_head_to_head(team1_id, team2_id, start_time, as_of_ts)
    
    if not h2h_series:
        return 0.0, 0
    
    score = 0.0
    total_series = 0
    
    for series_data in h2h_series:
        matches = series_data['matches']
        series_ts = series_data['start_time']
        
        # Decay по времени
        days_ago = (as_of_ts - series_ts) / 86400
        decay = math.exp(-days_ago / 14)  # half-life 14 days
        
        w1, l1, won = get_series_winner(matches, team1_id)
        margin = w1 - l1
        
        # Score: положительный = team1 лучше
        score += margin * decay
        total_series += 1
    
    return score, total_series


def compute_common_score_v1(analyzer, team1_id, team2_id, as_of_ts, max_days=30, elo_ratings=None):
    """Базовый Common Opponents score (текущая логика)."""
    start_time = as_of_ts - max_days * 86400
    
    common = analyzer.find_common_opponents(team1_id, team2_id, start_time, as_of_ts)
    
    if not common:
        return 0.0, 0
    
    score = 0.0
    total_opponents = 0
    
    for opp_data in common:
        opp_id = opp_data['opponent']
        t1_series = opp_data['team1_series']
        t2_series = opp_data['team2_series']
        
        # Winrate team1 vs opponent
        t1_wins = 0
        t1_total = 0
        for sid, matches in t1_series.items():
            w, l, won = get_series_winner(matches, team1_id)
            t1_wins += w
            t1_total += w + l
        
        # Winrate team2 vs opponent
        t2_wins = 0
        t2_total = 0
        for sid, matches in t2_series.items():
            w, l, won = get_series_winner(matches, team2_id)
            t2_wins += w
            t2_total += w + l
        
        if t1_total == 0 or t2_total == 0:
            continue
        
        t1_wr = t1_wins / t1_total
        t2_wr = t2_wins / t2_total
        
        # Differential: положительный = team1 лучше
        diff = t1_wr - t2_wr
        score += diff
        total_opponents += 1
    
    return score, total_opponents


def compute_common_score_v2_quality_weighted(analyzer, team1_id, team2_id, as_of_ts, max_days=30, elo_ratings=None):
    """Quality-Weighted Common: вес противника по его Elo."""
    start_time = as_of_ts - max_days * 86400
    
    common = analyzer.find_common_opponents(team1_id, team2_id, start_time, as_of_ts)
    
    if not common:
        return 0.0, 0
    
    if elo_ratings is None:
        elo_ratings = analyzer.compute_elo_ratings_up_to(as_of_ts, max_days=365, **DEFAULT_ELO_PARAMS)
    
    # Средний Elo для нормализации
    avg_elo = sum(elo_ratings.values()) / len(elo_ratings) if elo_ratings else 1500
    
    score = 0.0
    total_weight = 0.0
    total_opponents = 0
    
    for opp_data in common:
        opp_id = opp_data['opponent']
        t1_series = opp_data['team1_series']
        t2_series = opp_data['team2_series']
        
        # Качество противника
        opp_elo = elo_ratings.get(opp_id, 1500)
        quality_weight = opp_elo / avg_elo  # >1 для сильных, <1 для слабых
        
        # Winrate team1 vs opponent
        t1_wins = 0
        t1_total = 0
        for sid, matches in t1_series.items():
            w, l, won = get_series_winner(matches, team1_id)
            t1_wins += w
            t1_total += w + l
        
        # Winrate team2 vs opponent
        t2_wins = 0
        t2_total = 0
        for sid, matches in t2_series.items():
            w, l, won = get_series_winner(matches, team2_id)
            t2_wins += w
            t2_total += w + l
        
        if t1_total == 0 or t2_total == 0:
            continue
        
        t1_wr = t1_wins / t1_total
        t2_wr = t2_wins / t2_total
        
        diff = t1_wr - t2_wr
        score += diff * quality_weight
        total_weight += quality_weight
        total_opponents += 1
    
    # Нормализуем по весу
    if total_weight > 0:
        score = score / total_weight * total_opponents
    
    return score, total_opponents


def compute_common_score_v3_fresh_margin(analyzer, team1_id, team2_id, as_of_ts, max_days=21, elo_ratings=None):
    """Fresh + Margin: свежие данные + учёт margin побед."""
    start_time = as_of_ts - max_days * 86400
    
    common = analyzer.find_common_opponents(team1_id, team2_id, start_time, as_of_ts)
    
    if not common:
        return 0.0, 0
    
    score = 0.0
    total_opponents = 0
    
    for opp_data in common:
        opp_id = opp_data['opponent']
        t1_series = opp_data['team1_series']
        t2_series = opp_data['team2_series']
        
        # Margin team1 vs opponent (с decay)
        t1_margin = 0.0
        t1_weight = 0.0
        for sid, matches in t1_series.items():
            if not matches:
                continue
            series_ts = matches[0].get('startDateTime', 0)
            days_ago = (as_of_ts - series_ts) / 86400
            decay = math.exp(-days_ago / 7)  # half-life 7 days
            
            w, l, won = get_series_winner(matches, team1_id)
            margin = w - l
            t1_margin += margin * decay
            t1_weight += decay
        
        # Margin team2 vs opponent (с decay)
        t2_margin = 0.0
        t2_weight = 0.0
        for sid, matches in t2_series.items():
            if not matches:
                continue
            series_ts = matches[0].get('startDateTime', 0)
            days_ago = (as_of_ts - series_ts) / 86400
            decay = math.exp(-days_ago / 7)
            
            w, l, won = get_series_winner(matches, team2_id)
            margin = w - l
            t2_margin += margin * decay
            t2_weight += decay
        
        if t1_weight == 0 or t2_weight == 0:
            continue
        
        # Нормализованный margin
        t1_norm = t1_margin / t1_weight
        t2_norm = t2_margin / t2_weight
        
        diff = t1_norm - t2_norm
        score += diff
        total_opponents += 1
    
    return score, total_opponents


def compute_transitive_score_v1(analyzer, team1_id, team2_id, as_of_ts, max_days=30):
    """Базовый Transitive score."""
    start_time = as_of_ts - max_days * 86400
    
    chains = analyzer.find_transitive_connections(team1_id, team2_id, start_time, as_of_ts, max_depth=4)
    
    if not chains:
        return 0.0, 0
    
    score = 0.0
    total_chains = 0
    
    for chain in chains:
        path = chain['path']
        edge_series = chain['edge_series']
        
        # Длина цепи
        chain_len = len(path) - 1
        length_decay = 1.0 / (chain_len ** 1.5)  # Короткие цепи важнее
        
        # Проверяем направление каждого ребра
        chain_direction = 1  # 1 = team1 лучше, -1 = team2 лучше
        min_margin = float('inf')
        
        for i, edge in enumerate(edge_series):
            teams = edge['teams']
            series_dict = edge['series']
            
            # Определяем кто победил в этом ребре
            from_team = path[i]
            to_team = path[i + 1]
            
            edge_wins = 0
            edge_losses = 0
            for sid, matches in series_dict.items():
                w, l, won = get_series_winner(matches, from_team)
                edge_wins += w
                edge_losses += l
            
            margin = edge_wins - edge_losses
            if margin < 0:
                chain_direction *= -1
                margin = -margin
            
            min_margin = min(min_margin, margin)
        
        if min_margin == float('inf'):
            min_margin = 0
        
        # Score: направление × минимальный margin × decay по длине
        score += chain_direction * min_margin * length_decay
        total_chains += 1
    
    return score, total_chains


def compute_transitive_score_v2_propagation(analyzer, team1_id, team2_id, as_of_ts, max_days=30):
    """Transitive с propagation: перемножаем winrate по цепи."""
    start_time = as_of_ts - max_days * 86400
    
    chains = analyzer.find_transitive_connections(team1_id, team2_id, start_time, as_of_ts, max_depth=4)
    
    if not chains:
        return 0.0, 0
    
    score = 0.0
    total_chains = 0
    
    for chain in chains:
        path = chain['path']
        edge_series = chain['edge_series']
        
        chain_len = len(path) - 1
        
        # Propagation: перемножаем winrate по цепи
        propagated_wr = 1.0
        valid_chain = True
        
        for i, edge in enumerate(edge_series):
            series_dict = edge['series']
            from_team = path[i]
            
            edge_wins = 0
            edge_total = 0
            for sid, matches in series_dict.items():
                w, l, won = get_series_winner(matches, from_team)
                edge_wins += w
                edge_total += w + l
            
            if edge_total == 0:
                valid_chain = False
                break
            
            edge_wr = edge_wins / edge_total
            # Центрируем вокруг 0.5: >0.5 = положительный, <0.5 = отрицательный
            propagated_wr *= (edge_wr - 0.5) * 2 + 0.5
        
        if not valid_chain:
            continue
        
        # Финальный score: отклонение от 0.5
        chain_score = (propagated_wr - 0.5) * 2
        
        # Decay по длине
        length_decay = 1.0 / chain_len
        
        score += chain_score * length_decay
        total_chains += 1
    
    return score, total_chains


def run_experiment(
    analyzer,
    series_list,
    elo_ratings_cache,
    h2h_weight=0.7,
    common_weight=0.2,
    trans_weight=0.1,
    common_version='v1',
    trans_version='v1',
    max_days=30,
    use_elo=True,
    elo_weight=1.0,
):
    """Запускает эксперимент с заданными параметрами."""
    
    common_funcs = {
        'v1': compute_common_score_v1,
        'v2_quality': compute_common_score_v2_quality_weighted,
        'v3_fresh_margin': compute_common_score_v3_fresh_margin,
    }
    
    trans_funcs = {
        'v1': compute_transitive_score_v1,
        'v2_propagation': compute_transitive_score_v2_propagation,
    }
    
    common_func = common_funcs.get(common_version, compute_common_score_v1)
    trans_func = trans_funcs.get(trans_version, compute_transitive_score_v1)
    
    used = 0
    hits = 0
    
    for series in series_list:
        matches = series['matches']
        series_ts = series['start_ts']
        
        rad = matches[0].get('radiantTeam') or {}
        dire = matches[0].get('direTeam') or {}
        rid = rad.get('id')
        did = dire.get('id')
        
        if not rid or not did:
            continue
        
        # Actual winner
        radiant_maps = sum(1 for m in matches if m.get('didRadiantWin') is True)
        dire_maps = sum(1 for m in matches if m.get('didRadiantWin') is False)
        
        if radiant_maps == dire_maps:
            continue
        
        actual_winner_id = rid if radiant_maps > dire_maps else did
        
        # Elo ratings
        elo_ratings = elo_ratings_cache.get(series_ts)
        if elo_ratings is None:
            elo_ratings = analyzer.compute_elo_ratings_up_to(series_ts, max_days=365, **DEFAULT_ELO_PARAMS)
            elo_ratings_cache[series_ts] = elo_ratings
        
        # Compute scores
        h2h_score, h2h_n = compute_h2h_score(analyzer, rid, did, series_ts, max_days)
        common_score, common_n = common_func(analyzer, rid, did, series_ts, max_days, elo_ratings)
        trans_score, trans_n = trans_func(analyzer, rid, did, series_ts, max_days)
        
        # Elo score
        elo_r = elo_ratings.get(rid, 1500)
        elo_d = elo_ratings.get(did, 1500)
        elo_diff = (elo_r - elo_d) / 150  # Normalize
        
        # Combined score
        total_score = 0.0
        if use_elo:
            total_score += elo_diff * elo_weight
        if h2h_n > 0:
            total_score += h2h_score * h2h_weight
        if common_n > 0:
            total_score += common_score * common_weight
        if trans_n > 0:
            total_score += trans_score * trans_weight
        
        # Prediction
        if total_score > 0:
            pred_id = rid
        elif total_score < 0:
            pred_id = did
        else:
            continue  # Skip ties
        
        used += 1
        if pred_id == actual_winner_id:
            hits += 1
    
    acc = hits / used if used > 0 else 0
    return acc, used, hits


def main():
    print("Loading data...")
    analyzer = TransitiveAnalyzer()
    
    # Собираем серии
    all_matches = [m for m in analyzer.matches_data.values() if m.get('startDateTime', 0) > 0]
    all_matches.sort(key=lambda m: m.get('startDateTime', 0))
    
    min_ts = all_matches[0].get('startDateTime', 0)
    start_ts = min_ts + 14 * 86400  # Skip first 14 days
    
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
    print(f"Total series: {len(series_list)}")
    
    # Cache for Elo ratings
    elo_cache = {}
    
    # Baseline: Elo only
    print("\n=== BASELINE: Elo only ===")
    acc, used, hits = run_experiment(
        analyzer, series_list, elo_cache,
        h2h_weight=0, common_weight=0, trans_weight=0,
        use_elo=True, elo_weight=1.0,
    )
    print(f"Accuracy: {acc:.4f} ({hits}/{used})")
    
    # H2H only (no Elo)
    print("\n=== H2H only (no Elo) ===")
    acc, used, hits = run_experiment(
        analyzer, series_list, elo_cache,
        h2h_weight=1.0, common_weight=0, trans_weight=0,
        use_elo=False, elo_weight=0,
    )
    print(f"Accuracy: {acc:.4f} ({hits}/{used})")
    
    # Elo + H2H
    print("\n=== Elo + H2H ===")
    acc, used, hits = run_experiment(
        analyzer, series_list, elo_cache,
        h2h_weight=0.7, common_weight=0, trans_weight=0,
        use_elo=True, elo_weight=1.0,
    )
    print(f"Accuracy: {acc:.4f} ({hits}/{used})")
    
    # Тест: Common only (без H2H и Elo)
    print("\n=== Common v1 only (no Elo, no H2H) ===")
    acc, used, hits = run_experiment(
        analyzer, series_list, elo_cache,
        h2h_weight=0, common_weight=1.0, trans_weight=0,
        common_version='v1',
        use_elo=False, elo_weight=0,
    )
    print(f"Accuracy: {acc:.4f} ({hits}/{used})")
    
    # Elo + Common v1 (без H2H)
    print("\n=== Elo + Common v1 (no H2H) ===")
    acc, used, hits = run_experiment(
        analyzer, series_list, elo_cache,
        h2h_weight=0, common_weight=0.3, trans_weight=0,
        common_version='v1',
        use_elo=True, elo_weight=1.0,
    )
    print(f"Accuracy: {acc:.4f} ({hits}/{used})")
    
    # Elo + Common v2 quality (без H2H)
    print("\n=== Elo + Common v2 quality (no H2H) ===")
    acc, used, hits = run_experiment(
        analyzer, series_list, elo_cache,
        h2h_weight=0, common_weight=0.3, trans_weight=0,
        common_version='v2_quality',
        use_elo=True, elo_weight=1.0,
    )
    print(f"Accuracy: {acc:.4f} ({hits}/{used})")
    
    # Elo + H2H + Common v1
    print("\n=== Elo + H2H + Common v1 ===")
    acc, used, hits = run_experiment(
        analyzer, series_list, elo_cache,
        h2h_weight=0.7, common_weight=0.2, trans_weight=0,
        common_version='v1',
        use_elo=True, elo_weight=1.0,
    )
    print(f"Accuracy: {acc:.4f} ({hits}/{used})")
    
    # Elo + H2H + Common v2 quality
    print("\n=== Elo + H2H + Common v2 quality ===")
    acc, used, hits = run_experiment(
        analyzer, series_list, elo_cache,
        h2h_weight=0.7, common_weight=0.2, trans_weight=0,
        common_version='v2_quality',
        use_elo=True, elo_weight=1.0,
    )
    print(f"Accuracy: {acc:.4f} ({hits}/{used})")
    
    # Transitive only
    print("\n=== Transitive v1 only ===")
    acc, used, hits = run_experiment(
        analyzer, series_list, elo_cache,
        h2h_weight=0, common_weight=0, trans_weight=1.0,
        trans_version='v1',
        use_elo=False, elo_weight=0,
    )
    print(f"Accuracy: {acc:.4f} ({hits}/{used})")
    
    # Elo + Transitive v1
    print("\n=== Elo + Transitive v1 ===")
    acc, used, hits = run_experiment(
        analyzer, series_list, elo_cache,
        h2h_weight=0, common_weight=0, trans_weight=0.3,
        trans_version='v1',
        use_elo=True, elo_weight=1.0,
    )
    print(f"Accuracy: {acc:.4f} ({hits}/{used})")
    
    # Elo + H2H + Transitive v1
    print("\n=== Elo + H2H + Transitive v1 ===")
    acc, used, hits = run_experiment(
        analyzer, series_list, elo_cache,
        h2h_weight=0.7, common_weight=0, trans_weight=0.2,
        trans_version='v1',
        use_elo=True, elo_weight=1.0,
    )
    print(f"Accuracy: {acc:.4f} ({hits}/{used})")


if __name__ == "__main__":
    main()
