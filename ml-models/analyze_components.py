#!/usr/bin/env python3
"""
Analyze each draft component separately to find what actually works.
"""

import json
import os
from collections import defaultdict
from typing import Any, Dict, List, Tuple

import numpy as np


def load_matches(data_dir: str, limit: int = 200000) -> List[Dict[str, Any]]:
    """Load matches."""
    matches = []
    files = sorted([f for f in os.listdir(data_dir) if f.endswith('.json') and f.startswith('combined')])
    
    for fname in files:
        if len(matches) >= limit:
            break
        fpath = os.path.join(data_dir, fname)
        print(f"Loading {fname}...")
        
        with open(fpath) as f:
            data = json.load(f)
        
        for match_id, m in data.items():
            if len(matches) >= limit:
                break
            
            players = m.get("players", [])
            if len(players) != 10:
                continue
            
            r_by_pos: Dict[int, int] = {}
            d_by_pos: Dict[int, int] = {}
            
            for p in players:
                hero_id = p.get("heroId")
                pos_str = p.get("position", "")
                is_rad = p.get("isRadiant", False)
                
                if not hero_id or "POSITION_" not in pos_str:
                    continue
                
                pos = int(pos_str.replace("POSITION_", ""))
                if is_rad:
                    r_by_pos[pos] = hero_id
                else:
                    d_by_pos[pos] = hero_id
            
            if len(r_by_pos) != 5 or len(d_by_pos) != 5:
                continue
            
            matches.append({
                "match_id": int(match_id),
                "start_time": m.get("startDateTime", 0),
                "radiant_win": 1 if m.get("didRadiantWin") else 0,
                "r_pos": r_by_pos,
                "d_pos": d_by_pos,
            })
    
    matches.sort(key=lambda x: x["start_time"])
    return matches


def build_stats(matches: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build all statistics."""
    hero_stats: Dict[int, List[int]] = defaultdict(list)
    hero_pos_stats: Dict[Tuple[int, int], List[int]] = defaultdict(list)
    synergy_stats: Dict[Tuple[int, int], List[int]] = defaultdict(list)
    counter_stats: Dict[Tuple[int, int], List[int]] = defaultdict(list)
    matchup_stats: Dict[Tuple[int, int, int], List[int]] = defaultdict(list)
    
    for m in matches:
        rw = m["radiant_win"]
        r_heroes = [m["r_pos"][p] for p in range(1, 6)]
        d_heroes = [m["d_pos"][p] for p in range(1, 6)]
        
        for h in r_heroes:
            hero_stats[h].append(rw)
        for h in d_heroes:
            hero_stats[h].append(1 - rw)
        
        for pos in range(1, 6):
            hero_pos_stats[(m["r_pos"][pos], pos)].append(rw)
            hero_pos_stats[(m["d_pos"][pos], pos)].append(1 - rw)
        
        for i in range(5):
            for j in range(i + 1, 5):
                rh1, rh2 = r_heroes[i], r_heroes[j]
                dh1, dh2 = d_heroes[i], d_heroes[j]
                key_r = (min(rh1, rh2), max(rh1, rh2))
                key_d = (min(dh1, dh2), max(dh1, dh2))
                synergy_stats[key_r].append(rw)
                synergy_stats[key_d].append(1 - rw)
        
        for rh in r_heroes:
            for dh in d_heroes:
                counter_stats[(rh, dh)].append(rw)
        
        for pos in range(1, 6):
            rh = m["r_pos"][pos]
            dh = m["d_pos"][pos]
            matchup_stats[(rh, dh, pos)].append(rw)
    
    # Compute final stats
    hero_wr = {}
    for h, wins in hero_stats.items():
        if len(wins) >= 100:
            hero_wr[h] = (np.mean(wins), len(wins))
    
    hero_pos_wr = {}
    for key, wins in hero_pos_stats.items():
        if len(wins) >= 50:
            hero_pos_wr[key] = (np.mean(wins), len(wins))
    
    synergy = {}
    for key, wins in synergy_stats.items():
        if len(wins) >= 30:
            synergy[key] = (np.mean(wins) - 0.5, len(wins))
    
    counter = {}
    for key, wins in counter_stats.items():
        if len(wins) >= 30:
            counter[key] = (np.mean(wins) - 0.5, len(wins))
    
    matchup = {}
    for key, wins in matchup_stats.items():
        if len(wins) >= 20:
            matchup[key] = (np.mean(wins), len(wins))
    
    return {
        "hero_wr": hero_wr,
        "hero_pos_wr": hero_pos_wr,
        "synergy": synergy,
        "counter": counter,
        "matchup": matchup,
    }


def analyze_component(
    test_matches: List[Dict[str, Any]],
    stats: Dict[str, Any],
    component: str,
) -> None:
    """Analyze single component."""
    print(f"\n=== {component.upper()} ===")
    
    scores = []
    actuals = []
    
    for m in test_matches:
        r_heroes = [m["r_pos"][p] for p in range(1, 6)]
        d_heroes = [m["d_pos"][p] for p in range(1, 6)]
        
        score = 0.0
        
        if component == "hero_wr":
            r_wr = sum(stats["hero_wr"].get(h, (0.5, 0))[0] for h in r_heroes)
            d_wr = sum(stats["hero_wr"].get(h, (0.5, 0))[0] for h in d_heroes)
            score = r_wr - d_wr
        
        elif component == "pos_wr":
            for pos in range(1, 6):
                rh, dh = m["r_pos"][pos], m["d_pos"][pos]
                r_pwr = stats["hero_pos_wr"].get((rh, pos), (0.5, 0))[0]
                d_pwr = stats["hero_pos_wr"].get((dh, pos), (0.5, 0))[0]
                score += r_pwr - d_pwr
        
        elif component == "synergy":
            for i in range(5):
                for j in range(i + 1, 5):
                    key_r = (min(r_heroes[i], r_heroes[j]), max(r_heroes[i], r_heroes[j]))
                    key_d = (min(d_heroes[i], d_heroes[j]), max(d_heroes[i], d_heroes[j]))
                    score += stats["synergy"].get(key_r, (0, 0))[0]
                    score -= stats["synergy"].get(key_d, (0, 0))[0]
        
        elif component == "counter":
            for rh in r_heroes:
                for dh in d_heroes:
                    score += stats["counter"].get((rh, dh), (0, 0))[0]
                    score -= stats["counter"].get((dh, rh), (0, 0))[0]
        
        elif component == "matchup":
            for pos in range(1, 6):
                rh, dh = m["r_pos"][pos], m["d_pos"][pos]
                if (rh, dh, pos) in stats["matchup"]:
                    wr, cnt = stats["matchup"][(rh, dh, pos)]
                    score += (wr - 0.5) * min(1.0, cnt / 50)
        
        scores.append(score)
        actuals.append(m["radiant_win"])
    
    scores = np.array(scores)
    actuals = np.array(actuals)
    
    print(f"Score range: [{scores.min():.4f}, {scores.max():.4f}], std={scores.std():.4f}")
    
    # Analyze by threshold
    for th in [0.02, 0.05, 0.08, 0.10, 0.15, 0.20, 0.25, 0.30]:
        mask = np.abs(scores) >= th
        if mask.sum() < 50:
            print(f"  |score|>={th:.2f}: Too few ({mask.sum()})")
            continue
        
        preds = (scores >= 0).astype(int)
        wr = (preds[mask] == actuals[mask]).mean()
        cov = mask.mean()
        print(f"  |score|>={th:.2f}: WR={wr:.2%}, Cov={cov:.2%} ({mask.sum()})")


def analyze_combined(
    test_matches: List[Dict[str, Any]],
    stats: Dict[str, Any],
) -> None:
    """Analyze combined score with different weights."""
    print("\n=== COMBINED ANALYSIS ===")
    
    # Calculate all component scores
    all_scores = {
        "hero_wr": [],
        "pos_wr": [],
        "synergy": [],
        "counter": [],
        "matchup": [],
    }
    actuals = []
    
    for m in test_matches:
        r_heroes = [m["r_pos"][p] for p in range(1, 6)]
        d_heroes = [m["d_pos"][p] for p in range(1, 6)]
        
        # Hero WR
        r_wr = sum(stats["hero_wr"].get(h, (0.5, 0))[0] for h in r_heroes)
        d_wr = sum(stats["hero_wr"].get(h, (0.5, 0))[0] for h in d_heroes)
        all_scores["hero_wr"].append(r_wr - d_wr)
        
        # Pos WR
        pos_score = 0.0
        for pos in range(1, 6):
            rh, dh = m["r_pos"][pos], m["d_pos"][pos]
            r_pwr = stats["hero_pos_wr"].get((rh, pos), (0.5, 0))[0]
            d_pwr = stats["hero_pos_wr"].get((dh, pos), (0.5, 0))[0]
            pos_score += r_pwr - d_pwr
        all_scores["pos_wr"].append(pos_score)
        
        # Synergy
        syn_score = 0.0
        for i in range(5):
            for j in range(i + 1, 5):
                key_r = (min(r_heroes[i], r_heroes[j]), max(r_heroes[i], r_heroes[j]))
                key_d = (min(d_heroes[i], d_heroes[j]), max(d_heroes[i], d_heroes[j]))
                syn_score += stats["synergy"].get(key_r, (0, 0))[0]
                syn_score -= stats["synergy"].get(key_d, (0, 0))[0]
        all_scores["synergy"].append(syn_score)
        
        # Counter
        cnt_score = 0.0
        for rh in r_heroes:
            for dh in d_heroes:
                cnt_score += stats["counter"].get((rh, dh), (0, 0))[0]
                cnt_score -= stats["counter"].get((dh, rh), (0, 0))[0]
        all_scores["counter"].append(cnt_score)
        
        # Matchup
        match_score = 0.0
        for pos in range(1, 6):
            rh, dh = m["r_pos"][pos], m["d_pos"][pos]
            if (rh, dh, pos) in stats["matchup"]:
                wr, cnt = stats["matchup"][(rh, dh, pos)]
                match_score += (wr - 0.5) * min(1.0, cnt / 50)
        all_scores["matchup"].append(match_score)
        
        actuals.append(m["radiant_win"])
    
    for k in all_scores:
        all_scores[k] = np.array(all_scores[k])
    actuals = np.array(actuals)
    
    # Try different weight combinations
    print("\nWeight combinations (hero_wr, pos_wr, synergy, counter, matchup):")
    
    best_result = (0, 0, None)
    
    weight_sets = [
        (1, 0, 0, 0, 0),  # Only hero_wr
        (0, 1, 0, 0, 0),  # Only pos_wr
        (0, 0, 1, 0, 0),  # Only synergy
        (0, 0, 0, 1, 0),  # Only counter
        (0, 0, 0, 0, 1),  # Only matchup
        (1, 1, 0, 0, 0),  # hero + pos
        (1, 1, 1, 0, 0),  # hero + pos + syn
        (1, 1, 0, 1, 0),  # hero + pos + counter
        (1, 1, 0, 0, 1),  # hero + pos + matchup
        (1, 1, 1, 1, 0),  # all except matchup
        (1, 1, 1, 1, 1),  # all
        (1, 2, 0, 0, 0),  # pos weighted more
        (0, 1, 0, 0, 2),  # pos + matchup
        (1, 1, 0, 0, 2),  # hero + pos + matchup*2
    ]
    
    for weights in weight_sets:
        combined = (
            all_scores["hero_wr"] * weights[0] +
            all_scores["pos_wr"] * weights[1] +
            all_scores["synergy"] * weights[2] +
            all_scores["counter"] * weights[3] +
            all_scores["matchup"] * weights[4]
        )
        
        # Find best threshold for 30%+ coverage
        for th in np.arange(0.02, 0.50, 0.02):
            mask = np.abs(combined) >= th
            if mask.sum() < 100:
                continue
            
            cov = mask.mean()
            if cov < 0.30:
                continue
            
            preds = (combined >= 0).astype(int)
            wr = (preds[mask] == actuals[mask]).mean()
            
            if wr > best_result[0]:
                best_result = (wr, cov, weights, th)
        
        # Show best for this weight set at ~30% coverage
        for th in np.arange(0.02, 0.50, 0.02):
            mask = np.abs(combined) >= th
            if mask.sum() < 100:
                continue
            cov = mask.mean()
            if 0.28 <= cov <= 0.35:
                preds = (combined >= 0).astype(int)
                wr = (preds[mask] == actuals[mask]).mean()
                print(f"  {weights}: th={th:.2f}, WR={wr:.2%}, Cov={cov:.2%}")
                break
    
    print(f"\nBest result: WR={best_result[0]:.2%}, Cov={best_result[1]:.2%}, "
          f"weights={best_result[2]}, th={best_result[3]:.2f}")


def main() -> None:
    data_dir = "bets_data/analise_pub_matches/json_parts_split_from_object"
    
    print("Loading matches...")
    matches = load_matches(data_dir, limit=300000)
    print(f"Loaded {len(matches)} matches")
    
    # Time-based split
    test_size = int(len(matches) * 0.1)
    train_m = matches[:-test_size]
    test_m = matches[-test_size:]
    print(f"Train: {len(train_m)}, Test: {len(test_m)}")
    
    print("\nBuilding statistics...")
    stats = build_stats(train_m)
    print(f"Stats: hero_wr={len(stats['hero_wr'])}, pos_wr={len(stats['hero_pos_wr'])}, "
          f"synergy={len(stats['synergy'])}, counter={len(stats['counter'])}, matchup={len(stats['matchup'])}")
    
    # Analyze each component
    for comp in ["hero_wr", "pos_wr", "synergy", "counter", "matchup"]:
        analyze_component(test_m, stats, comp)
    
    # Combined analysis
    analyze_combined(test_m, stats)


if __name__ == "__main__":
    main()
