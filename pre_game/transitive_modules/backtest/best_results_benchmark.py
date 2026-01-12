#!/usr/bin/env python3
"""
Бенчмарк лучших результатов из всех экспериментов.
Воспроизводит все лучшие винрейты для каждой метрики.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.transitive_analyzer import TransitiveAnalyzer, ELO_HISTORY_DAYS, DEFAULT_ELO_PARAMS

analyzer = TransitiveAnalyzer()

print("Loading data...")
print(f"Total matches: {len(analyzer.matches_sorted)}")
print("=" * 70)


def get_h2h_score(team1_id, team2_id, as_of_ts, days=90):
    """Получить H2H score между командами."""
    start_time = as_of_ts - days * 86400
    h2h_series = analyzer.find_head_to_head(team1_id, team2_id, start_time, as_of_ts)
    
    if not h2h_series:
        return None, 0
    
    wins1 = 0
    total = 0
    for series in h2h_series:
        matches = series['matches']
        t1_wins = sum(1 for m in matches if 
                     (m.get('radiantTeam', {}).get('id') == team1_id and m.get('didRadiantWin')) or 
                     (m.get('direTeam', {}).get('id') == team1_id and not m.get('didRadiantWin')))
        t2_wins = len(matches) - t1_wins
        if t1_wins > t2_wins:
            wins1 += 1
        total += 1
    
    return wins1 / total if total > 0 else None, total


def get_common_score(team1_id, team2_id, as_of_ts, days=90):
    """Получить Common Opponents score."""
    start_time = as_of_ts - days * 86400
    common = analyzer.find_common_opponents(team1_id, team2_id, start_time, as_of_ts)
    
    if not common:
        return None, 0
    
    t1_wins, t1_games = 0, 0
    t2_wins, t2_games = 0, 0
    
    for opp_data in common:
        for sid, matches in opp_data['team1_series'].items():
            for m in matches:
                rad = m.get('radiantTeam', {}).get('id')
                mrw = m.get('didRadiantWin')
                t1_games += 1
                if (rad == team1_id and mrw) or (rad != team1_id and not mrw):
                    t1_wins += 1
        
        for sid, matches in opp_data['team2_series'].items():
            for m in matches:
                rad = m.get('radiantTeam', {}).get('id')
                mrw = m.get('didRadiantWin')
                t2_games += 1
                if (rad == team2_id and mrw) or (rad != team2_id and not mrw):
                    t2_wins += 1
    
    if t1_games == 0 or t2_games == 0:
        return None, 0
    
    t1_wr = t1_wins / t1_games
    t2_wr = t2_wins / t2_games
    score = 0.5 + (t1_wr - t2_wr) / 2
    
    return score, len(common)


def get_trans_score(team1_id, team2_id, as_of_ts, days=90, max_depth=3):
    """Получить Transitive score."""
    start_time = as_of_ts - days * 86400
    chains = analyzer.find_transitive_connections(team1_id, team2_id, start_time, as_of_ts, max_depth=max_depth)
    
    if not chains:
        return None, 0
    
    t1_favored = 0
    t2_favored = 0
    
    for chain in chains:
        path = chain.get('path', [])
        edges = chain.get('edge_series', [])
        if len(path) < 3:
            continue
        
        all_t1 = True
        all_t2 = True
        for i, edge in enumerate(edges):
            series_dict = edge.get('series', {})
            a = path[i]
            b = path[i+1]
            a_wins = 0
            b_wins = 0
            for sid, matches in series_dict.items():
                for m in matches:
                    rad = m.get('radiantTeam', {}).get('id')
                    mrw = m.get('didRadiantWin')
                    if (rad == a and mrw) or (rad != a and not mrw):
                        a_wins += 1
                    else:
                        b_wins += 1
            if a_wins <= b_wins:
                all_t1 = False
            if b_wins <= a_wins:
                all_t2 = False
        
        if all_t1:
            t1_favored += 1
        elif all_t2:
            t2_favored += 1
    
    total = t1_favored + t2_favored
    if total == 0:
        return None, 0
    
    return t1_favored / total, total


def test_elo_only(matches, sample_name=""):
    """Тест только Elo."""
    correct = 0
    total = 0
    
    for match in matches:
        mt = match.get('startDateTime', 0)
        radiant_team = match.get('radiantTeam') or {}
        dire_team = match.get('direTeam') or {}
        rid = radiant_team.get('id')
        did = dire_team.get('id')
        rw = match.get('didRadiantWin')
        
        if not rid or not did or rw is None:
            continue
        
        ratings = analyzer.compute_elo_ratings_up_to(mt, max_days=ELO_HISTORY_DAYS, **DEFAULT_ELO_PARAMS)
        ra = ratings.get(rid, 1500)
        rb = ratings.get(did, 1500)
        
        if ra == rb:
            continue
        
        if (ra > rb) == rw:
            correct += 1
        total += 1
    
    acc = correct / total * 100 if total > 0 else 0
    print(f"  Elo only {sample_name}: {correct}/{total} = {acc:.2f}%")
    return acc


def test_elo_h2h(matches, h2h_weight=0.7, h2h_days=90, sample_name=""):
    """Тест Elo + H2H."""
    correct = 0
    total = 0
    h2h_used = 0
    
    for match in matches:
        mt = match.get('startDateTime', 0)
        radiant_team = match.get('radiantTeam') or {}
        dire_team = match.get('direTeam') or {}
        rid = radiant_team.get('id')
        did = dire_team.get('id')
        rw = match.get('didRadiantWin')
        
        if not rid or not did or rw is None:
            continue
        
        ratings = analyzer.compute_elo_ratings_up_to(mt, max_days=ELO_HISTORY_DAYS, **DEFAULT_ELO_PARAMS)
        ra = ratings.get(rid, 1500)
        rb = ratings.get(did, 1500)
        
        h2h_score, h2h_n = get_h2h_score(rid, did, mt, days=h2h_days)
        if h2h_score is not None and h2h_n >= 1:
            ra += h2h_weight * (h2h_score - 0.5) * 100
            rb += h2h_weight * (0.5 - h2h_score) * 100
            h2h_used += 1
        
        if ra == rb:
            continue
        
        if (ra > rb) == rw:
            correct += 1
        total += 1
    
    acc = correct / total * 100 if total > 0 else 0
    print(f"  Elo + H2H({h2h_weight}) {sample_name}: {correct}/{total} = {acc:.2f}% (H2H used: {h2h_used})")
    return acc


def test_h2h_only(matches, sample_name="", h2h_days=90):
    """Тест только H2H (без Elo)."""
    correct = 0
    total = 0
    
    for match in matches:
        mt = match.get('startDateTime', 0)
        radiant_team = match.get('radiantTeam') or {}
        dire_team = match.get('direTeam') or {}
        rid = radiant_team.get('id')
        did = dire_team.get('id')
        rw = match.get('didRadiantWin')
        
        if not rid or not did or rw is None:
            continue
        
        h2h_score, h2h_n = get_h2h_score(rid, did, mt, days=h2h_days)
        if h2h_score is None or h2h_n < 1:
            continue
        
        predicted = h2h_score > 0.5
        if predicted == rw:
            correct += 1
        total += 1
    
    acc = correct / total * 100 if total > 0 else 0
    print(f"  H2H only {sample_name}: {correct}/{total} = {acc:.2f}%")
    return acc


def test_h2h_few_series(matches, h2h_days=90, min_series=1, max_series=2):
    """Тест H2H когда мало серий (1-2)."""
    correct = 0
    total = 0
    
    for match in matches:
        mt = match.get('startDateTime', 0)
        radiant_team = match.get('radiantTeam') or {}
        dire_team = match.get('direTeam') or {}
        rid = radiant_team.get('id')
        did = dire_team.get('id')
        rw = match.get('didRadiantWin')
        
        if not rid or not did or rw is None:
            continue
        
        h2h_score, h2h_n = get_h2h_score(rid, did, mt, days=h2h_days)
        if h2h_score is None or h2h_n < min_series or h2h_n > max_series:
            continue
        
        predicted = h2h_score > 0.5
        if predicted == rw:
            correct += 1
        total += 1
    
    acc = correct / total * 100 if total > 0 else 0
    print(f"  H2H only ({min_series}-{max_series} series): {correct}/{total} = {acc:.2f}%")
    return acc


def test_common_only(matches, sample_name="", common_days=90):
    """Тест только Common Opponents."""
    correct = 0
    total = 0
    
    for match in matches:
        mt = match.get('startDateTime', 0)
        radiant_team = match.get('radiantTeam') or {}
        dire_team = match.get('direTeam') or {}
        rid = radiant_team.get('id')
        did = dire_team.get('id')
        rw = match.get('didRadiantWin')
        
        if not rid or not did or rw is None:
            continue
        
        common_score, common_n = get_common_score(rid, did, mt, days=common_days)
        if common_score is None or common_n < 1:
            continue
        
        predicted = common_score > 0.5
        if predicted == rw:
            correct += 1
        total += 1
    
    acc = correct / total * 100 if total > 0 else 0
    print(f"  Common only {sample_name}: {correct}/{total} = {acc:.2f}%")
    return acc


def test_common_when_h2h_tie(matches, h2h_days=90, common_days=90):
    """Тест Common когда H2H = 0.5 (ничья)."""
    correct = 0
    total = 0
    
    for match in matches:
        mt = match.get('startDateTime', 0)
        radiant_team = match.get('radiantTeam') or {}
        dire_team = match.get('direTeam') or {}
        rid = radiant_team.get('id')
        did = dire_team.get('id')
        rw = match.get('didRadiantWin')
        
        if not rid or not did or rw is None:
            continue
        
        h2h_score, h2h_n = get_h2h_score(rid, did, mt, days=h2h_days)
        if h2h_score is None or h2h_score != 0.5:
            continue
        
        common_score, common_n = get_common_score(rid, did, mt, days=common_days)
        if common_score is None or common_n < 1:
            continue
        
        predicted = common_score > 0.5
        if predicted == rw:
            correct += 1
        total += 1
    
    acc = correct / total * 100 if total > 0 else 0
    print(f"  Common when H2H=0.5: {correct}/{total} = {acc:.2f}%")
    return acc


def test_trans_only(matches, sample_name="", trans_days=90):
    """Тест только Transitive."""
    correct = 0
    total = 0
    
    for match in matches:
        mt = match.get('startDateTime', 0)
        radiant_team = match.get('radiantTeam') or {}
        dire_team = match.get('direTeam') or {}
        rid = radiant_team.get('id')
        did = dire_team.get('id')
        rw = match.get('didRadiantWin')
        
        if not rid or not did or rw is None:
            continue
        
        trans_score, trans_n = get_trans_score(rid, did, mt, days=trans_days)
        if trans_score is None or trans_n < 1:
            continue
        
        predicted = trans_score > 0.5
        if predicted == rw:
            correct += 1
        total += 1
    
    acc = correct / total * 100 if total > 0 else 0
    print(f"  Transitive only {sample_name}: {correct}/{total} = {acc:.2f}%")
    return acc


def test_combined(matches, h2h_w=0.7, common_w=0.2, h2h_days=90, common_days=90, sample_name=""):
    """Тест Elo + H2H + Common."""
    correct = 0
    total = 0
    
    for match in matches:
        mt = match.get('startDateTime', 0)
        radiant_team = match.get('radiantTeam') or {}
        dire_team = match.get('direTeam') or {}
        rid = radiant_team.get('id')
        did = dire_team.get('id')
        rw = match.get('didRadiantWin')
        
        if not rid or not did or rw is None:
            continue
        
        ratings = analyzer.compute_elo_ratings_up_to(mt, max_days=ELO_HISTORY_DAYS, **DEFAULT_ELO_PARAMS)
        ra = ratings.get(rid, 1500)
        rb = ratings.get(did, 1500)
        
        # H2H
        h2h_score, h2h_n = get_h2h_score(rid, did, mt, days=h2h_days)
        if h2h_score is not None and h2h_n >= 1:
            ra += h2h_w * (h2h_score - 0.5) * 100
            rb += h2h_w * (0.5 - h2h_score) * 100
        
        # Common
        common_score, common_n = get_common_score(rid, did, mt, days=common_days)
        if common_score is not None and common_n >= 1:
            ra += common_w * (common_score - 0.5) * 100
            rb += common_w * (0.5 - common_score) * 100
        
        if ra == rb:
            continue
        
        if (ra > rb) == rw:
            correct += 1
        total += 1
    
    acc = correct / total * 100 if total > 0 else 0
    print(f"  Elo + H2H({h2h_w}) + Common({common_w}) {sample_name}: {correct}/{total} = {acc:.2f}%")
    return acc


# ============================================================
# MAIN BENCHMARK
# ============================================================

print("\n" + "=" * 70)
print("BENCHMARK: ЛУЧШИЕ РЕЗУЛЬТАТЫ ИЗ ВСЕХ ЭКСПЕРИМЕНТОВ")
print("=" * 70)

# Разные выборки
last_200 = analyzer.matches_sorted[-200:]
last_500 = analyzer.matches_sorted[-500:]

print("\n### 1. ELO ONLY ###")
print("Ожидаемый лучший результат: ~68-69%")
test_elo_only(last_200, "(last 200)")

print("\n### 2. H2H ONLY ###")
print("Ожидаемый лучший результат: 82-92%")
test_h2h_only(last_200, "(last 200)")
test_h2h_few_series(last_200, min_series=1, max_series=2)

print("\n### 3. ELO + H2H ###")
print("Ожидаемый лучший результат: 79-87%")
test_elo_h2h(last_200, h2h_weight=0.7, sample_name="(last 200)")

print("\n### 4. COMMON OPPONENTS ###")
print("Ожидаемый лучший результат: 53-68%")
test_common_only(last_200, common_days=90, sample_name="(last 200)")
test_common_when_h2h_tie(last_200)

print("\n### 5. TRANSITIVE ###")
print("Ожидаемый лучший результат: 62-63%")
test_trans_only(last_200, trans_days=90, sample_name="(last 200)")

print("\n### 6. COMBINED: ELO + H2H + COMMON ###")
print("Ожидаемый лучший результат: 79-85%")
test_combined(last_200, h2h_w=0.7, common_w=0.2, sample_name="(last 200)")

print("\n" + "=" * 70)
print("VALIDATION ON DIFFERENT SAMPLE SIZES")
print("=" * 70)

for n in [100, 200, 300, 500]:
    sample = analyzer.matches_sorted[-n:]
    print(f"\n--- Last {n} matches ---")
    test_elo_only(sample, "")
    test_elo_h2h(sample, h2h_weight=0.7, sample_name="")
    test_h2h_only(sample, "")

print("\n" + "=" * 70)
print("BENCHMARK COMPLETE")
print("=" * 70)
