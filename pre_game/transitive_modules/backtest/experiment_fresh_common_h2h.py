#!/usr/bin/env python3
"""Эксперимент: Fresh Common Opponents + H2H комбинации."""

import sys
import os
import math
from datetime import datetime
from collections import defaultdict

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


def compute_h2h_score(analyzer, team1_id, team2_id, as_of_ts, max_days=90):
    """H2H score с 90-дневным окном."""
    start_time = as_of_ts - max_days * 86400
    
    h2h_series = analyzer.find_head_to_head(team1_id, team2_id, start_time, as_of_ts)
    
    if not h2h_series:
        return 0.0, 0
    
    score = 0.0
    total_series = 0
    
    for series_data in h2h_series:
        matches = series_data['matches']
        series_ts = series_data['start_time']
        
        # Decay по времени (half-life 14 days)
        days_ago = (as_of_ts - series_ts) / 86400
        decay = math.exp(-days_ago / 14)
        
        w1, l1, won = get_series_winner(matches, team1_id)
        margin = w1 - l1
        
        score += margin * decay
        total_series += 1
    
    return score, total_series


def compute_fresh_common_score(analyzer, team1_id, team2_id, as_of_ts, max_days=21, decay_half_life=7):
    """Fresh Common Opponents с коротким окном и агрессивным decay."""
    start_time = as_of_ts - max_days * 86400
    
    common = analyzer.find_common_opponents(team1_id, team2_id, start_time, as_of_ts)
    
    if not common:
        return 0.0, 0
    
    score = 0.0
    total_opponents = 0
    
    for opp_data in common:
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
            decay = math.exp(-days_ago / decay_half_life)
            
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
            decay = math.exp(-days_ago / decay_half_life)
            
            w, l, won = get_series_winner(matches, team2_id)
            margin = w - l
            t2_margin += margin * decay
            t2_weight += decay
        
        if t1_weight == 0 or t2_weight == 0:
            continue
        
        t1_norm = t1_margin / t1_weight
        t2_norm = t2_margin / t2_weight
        
        diff = t1_norm - t2_norm
        score += diff
        total_opponents += 1
    
    return score, total_opponents


def run_experiment(
    analyzer,
    series_list,
    elo_by_day,
    h2h_weight=0.7,
    common_weight=0.0,
    h2h_max_days=90,
    common_max_days=21,
    common_decay=7,
):
    """Запускает эксперимент с заданными параметрами."""
    
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
        
        # Elo ratings (используем предвычисленные по дням)
        day_ts = (series_ts // 86400) * 86400
        elo_ratings = elo_by_day.get(day_ts, {})
        
        # Compute scores
        h2h_score, h2h_n = compute_h2h_score(analyzer, rid, did, series_ts, h2h_max_days)
        common_score, common_n = compute_fresh_common_score(
            analyzer, rid, did, series_ts, common_max_days, common_decay
        )
        
        # Elo score
        elo_r = elo_ratings.get(rid, 1500)
        elo_d = elo_ratings.get(did, 1500)
        elo_diff = (elo_r - elo_d) / 150  # Normalize
        
        # Combined score
        total_score = elo_diff  # Elo always included
        if h2h_n > 0:
            total_score += h2h_score * h2h_weight
        if common_n > 0:
            total_score += common_score * common_weight
        
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


def test_fresh_common_only(analyzer, series_list, elo_ratings_cache, max_days=21, decay=7):
    """Тестирует только Fresh Common (без Elo и H2H) для оценки accuracy метода."""
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
        
        radiant_maps = sum(1 for m in matches if m.get('didRadiantWin') is True)
        dire_maps = sum(1 for m in matches if m.get('didRadiantWin') is False)
        
        if radiant_maps == dire_maps:
            continue
        
        actual_winner_id = rid if radiant_maps > dire_maps else did
        
        common_score, common_n = compute_fresh_common_score(
            analyzer, rid, did, series_ts, max_days, decay
        )
        
        if common_n == 0:
            continue
        
        if common_score > 0:
            pred_id = rid
        elif common_score < 0:
            pred_id = did
        else:
            continue
        
        used += 1
        if pred_id == actual_winner_id:
            hits += 1
    
    return hits, used


def main():
    print("Loading data...")
    analyzer = TransitiveAnalyzer()
    
    # Собираем серии
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
        first_ts = matches[0].get('startDateTime', 0)
        series_list.append({
            'series_id': series_id,
            'matches': matches,
            'start_ts': first_ts,
        })
    
    series_list.sort(key=lambda s: s['start_ts'])
    print(f"Total series: {len(series_list)}")
    
    # Предвычисляем Elo для уникальных timestamps (округляем до дня)
    print("Pre-computing Elo ratings...")
    unique_days = set()
    for s in series_list:
        day_ts = (s['start_ts'] // 86400) * 86400
        unique_days.add(day_ts)
    
    elo_by_day = {}
    for i, day_ts in enumerate(sorted(unique_days)):
        if i % 50 == 0:
            print(f"  Computing Elo for day {i+1}/{len(unique_days)}...")
        elo_by_day[day_ts] = analyzer.compute_elo_ratings_up_to(day_ts, max_days=365, **DEFAULT_ELO_PARAMS)
    print(f"Pre-computed Elo for {len(unique_days)} unique days")
    
    # === Fresh Common only ===
    print("\n=== Fresh Common (max_days=21, decay=7) ===")
    hits, used = test_fresh_common_only(analyzer, series_list, elo_by_day, max_days=21, decay=7)
    print(f"Fresh Common only: {hits}/{used} = {hits/used*100:.1f}%")
    
    # === Combined H2H + fresh Common ===
    print("\n=== Combined H2H + fresh Common ===")
    
    # Baseline: Elo only
    acc, used, hits = run_experiment(
        analyzer, series_list, elo_by_day,
        h2h_weight=0, common_weight=0,
    )
    print(f"Elo only: {acc*100:.2f}%")
    
    # Elo + H2H(0.7)
    acc, used, hits = run_experiment(
        analyzer, series_list, elo_by_day,
        h2h_weight=0.7, common_weight=0,
    )
    print(f"Elo + H2H(0.7): {acc*100:.2f}%")
    
    # Elo + H2H(0.7) + Common(0.1)
    acc, used, hits = run_experiment(
        analyzer, series_list, elo_by_day,
        h2h_weight=0.7, common_weight=0.1,
    )
    print(f"Elo + H2H(0.7) + Common(0.1): {acc*100:.2f}%")
    
    # Elo + H2H(0.7) + Common(0.2)
    acc, used, hits = run_experiment(
        analyzer, series_list, elo_by_day,
        h2h_weight=0.7, common_weight=0.2,
    )
    print(f"Elo + H2H(0.7) + Common(0.2): {acc*100:.2f}%")
    
    # Elo + H2H(0.7) + Common(0.3)
    acc, used, hits = run_experiment(
        analyzer, series_list, elo_by_day,
        h2h_weight=0.7, common_weight=0.3,
    )
    print(f"Elo + H2H(0.7) + Common(0.3): {acc*100:.2f}%")


if __name__ == "__main__":
    main()
