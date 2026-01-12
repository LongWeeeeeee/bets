#!/usr/bin/env python3
"""
Analyze specific hero matchups and find high-confidence patterns.
"""

import json
import os
from collections import defaultdict
from typing import Any, Dict, List, Tuple

import numpy as np


def load_matches(data_dir: str, limit: int = 1000000) -> List[Dict[str, Any]]:
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


def build_all_stats(matches: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build comprehensive statistics."""
    hero_stats: Dict[int, List[int]] = defaultdict(list)
    hero_pos_stats: Dict[Tuple[int, int], List[int]] = defaultdict(list)
    lane_matchup_stats: Dict[Tuple[int, int, int], List[int]] = defaultdict(list)  # (hero, enemy, pos)
    
    for m in matches:
        rw = m["radiant_win"]
        
        for pos in range(1, 6):
            rh = m["r_pos"][pos]
            dh = m["d_pos"][pos]
            
            hero_stats[rh].append(rw)
            hero_stats[dh].append(1 - rw)
            
            hero_pos_stats[(rh, pos)].append(rw)
            hero_pos_stats[(dh, pos)].append(1 - rw)
            
            # Lane matchup (same position)
            lane_matchup_stats[(rh, dh, pos)].append(rw)
            lane_matchup_stats[(dh, rh, pos)].append(1 - rw)
    
    # Compute final stats
    hero_wr = {}
    for h, wins in hero_stats.items():
        if len(wins) >= 200:
            hero_wr[h] = (np.mean(wins), len(wins))
    
    hero_pos_wr = {}
    for key, wins in hero_pos_stats.items():
        if len(wins) >= 100:
            hero_pos_wr[key] = (np.mean(wins), len(wins))
    
    lane_matchup = {}
    for key, wins in lane_matchup_stats.items():
        if len(wins) >= 30:
            lane_matchup[key] = (np.mean(wins), len(wins))
    
    return {
        "hero_wr": hero_wr,
        "hero_pos_wr": hero_pos_wr,
        "lane_matchup": lane_matchup,
    }


def find_strong_matchups(stats: Dict[str, Any]) -> None:
    """Find matchups with strong winrate differences."""
    print("\n=== STRONG LANE MATCHUPS ===")
    
    # Find matchups where one hero dominates
    strong_matchups = []
    for (h1, h2, pos), (wr, cnt) in stats["lane_matchup"].items():
        if cnt >= 50 and (wr >= 0.60 or wr <= 0.40):
            strong_matchups.append((h1, h2, pos, wr, cnt))
    
    strong_matchups.sort(key=lambda x: -abs(x[3] - 0.5))
    
    print(f"Found {len(strong_matchups)} strong matchups (WR >= 60% or <= 40%)")
    print("\nTop 20 strongest:")
    for h1, h2, pos, wr, cnt in strong_matchups[:20]:
        print(f"  Hero {h1} vs {h2} (pos {pos}): WR={wr:.2%} ({cnt} games)")


def analyze_with_matchups(
    test_matches: List[Dict[str, Any]],
    stats: Dict[str, Any],
) -> None:
    """Analyze using lane matchups."""
    print("\n=== LANE MATCHUP ANALYSIS ===")
    
    scores = []
    actuals = []
    matchup_counts = []
    
    for m in test_matches:
        score = 0.0
        count = 0
        
        for pos in range(1, 6):
            rh = m["r_pos"][pos]
            dh = m["d_pos"][pos]
            
            # Lane matchup
            if (rh, dh, pos) in stats["lane_matchup"]:
                wr, cnt = stats["lane_matchup"][(rh, dh, pos)]
                # Weight by sample size and deviation from 50%
                weight = min(1.0, cnt / 100)
                score += (wr - 0.5) * weight
                count += 1
        
        scores.append(score)
        actuals.append(m["radiant_win"])
        matchup_counts.append(count)
    
    scores = np.array(scores)
    actuals = np.array(actuals)
    matchup_counts = np.array(matchup_counts)
    
    print(f"Score range: [{scores.min():.4f}, {scores.max():.4f}]")
    print(f"Avg matchups per game: {matchup_counts.mean():.2f}")
    
    # Analyze by threshold
    print("\nBy score threshold:")
    for th in [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]:
        mask = np.abs(scores) >= th
        if mask.sum() < 50:
            print(f"  |score|>={th:.2f}: Too few ({mask.sum()})")
            continue
        
        preds = (scores >= 0).astype(int)
        wr = (preds[mask] == actuals[mask]).mean()
        cov = mask.mean()
        print(f"  |score|>={th:.2f}: WR={wr:.2%}, Cov={cov:.2%} ({mask.sum()})")
    
    # Analyze by number of known matchups
    print("\nBy number of known matchups:")
    for min_matchups in [3, 4, 5]:
        mask = matchup_counts >= min_matchups
        if mask.sum() < 50:
            continue
        
        preds = (scores >= 0).astype(int)
        wr = (preds[mask] == actuals[mask]).mean()
        cov = mask.mean()
        print(f"  matchups>={min_matchups}: WR={wr:.2%}, Cov={cov:.2%} ({mask.sum()})")


def analyze_combined_approach(
    test_matches: List[Dict[str, Any]],
    stats: Dict[str, Any],
) -> None:
    """Combine hero_wr, pos_wr, and lane_matchup."""
    print("\n=== COMBINED APPROACH ===")
    
    hero_wr_scores = []
    pos_wr_scores = []
    matchup_scores = []
    actuals = []
    
    for m in test_matches:
        # Hero WR
        r_wr = sum(stats["hero_wr"].get(m["r_pos"][p], (0.5, 0))[0] for p in range(1, 6))
        d_wr = sum(stats["hero_wr"].get(m["d_pos"][p], (0.5, 0))[0] for p in range(1, 6))
        hero_wr_scores.append(r_wr - d_wr)
        
        # Pos WR
        pos_score = 0.0
        for pos in range(1, 6):
            rh, dh = m["r_pos"][pos], m["d_pos"][pos]
            r_pwr = stats["hero_pos_wr"].get((rh, pos), (0.5, 0))[0]
            d_pwr = stats["hero_pos_wr"].get((dh, pos), (0.5, 0))[0]
            pos_score += r_pwr - d_pwr
        pos_wr_scores.append(pos_score)
        
        # Lane matchup
        match_score = 0.0
        for pos in range(1, 6):
            rh, dh = m["r_pos"][pos], m["d_pos"][pos]
            if (rh, dh, pos) in stats["lane_matchup"]:
                wr, cnt = stats["lane_matchup"][(rh, dh, pos)]
                weight = min(1.0, cnt / 100)
                match_score += (wr - 0.5) * weight
        matchup_scores.append(match_score)
        
        actuals.append(m["radiant_win"])
    
    hero_wr_scores = np.array(hero_wr_scores)
    pos_wr_scores = np.array(pos_wr_scores)
    matchup_scores = np.array(matchup_scores)
    actuals = np.array(actuals)
    
    # Try different combinations
    print("\nWeight combinations (hero_wr, pos_wr, matchup):")
    
    best = (0, 0, None, 0)
    
    for w1 in [0, 1, 2]:
        for w2 in [0, 1, 2]:
            for w3 in [0, 1, 2, 3]:
                if w1 == 0 and w2 == 0 and w3 == 0:
                    continue
                
                combined = hero_wr_scores * w1 + pos_wr_scores * w2 + matchup_scores * w3
                
                # Find threshold for ~30% coverage
                for th in np.arange(0.02, 0.60, 0.02):
                    mask = np.abs(combined) >= th
                    if mask.sum() < 100:
                        continue
                    
                    cov = mask.mean()
                    if cov < 0.25 or cov > 0.35:
                        continue
                    
                    preds = (combined >= 0).astype(int)
                    wr = (preds[mask] == actuals[mask]).mean()
                    
                    if wr > best[0]:
                        best = (wr, cov, (w1, w2, w3), th)
    
    print(f"Best at ~30% cov: WR={best[0]:.2%}, Cov={best[1]:.2%}, weights={best[2]}, th={best[3]:.2f}")
    
    # Show results for best weights
    w1, w2, w3 = best[2]
    combined = hero_wr_scores * w1 + pos_wr_scores * w2 + matchup_scores * w3
    
    print(f"\nDetailed results for weights {best[2]}:")
    for th in [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40]:
        mask = np.abs(combined) >= th
        if mask.sum() < 50:
            continue
        
        preds = (combined >= 0).astype(int)
        wr = (preds[mask] == actuals[mask]).mean()
        cov = mask.mean()
        print(f"  |score|>={th:.2f}: WR={wr:.2%}, Cov={cov:.2%} ({mask.sum()})")


def main() -> None:
    data_dir = "bets_data/analise_pub_matches/json_parts_split_from_object"
    
    print("Loading matches...")
    matches = load_matches(data_dir, limit=1000000)
    print(f"Loaded {len(matches)} matches")
    
    # Time-based split
    test_size = int(len(matches) * 0.1)
    train_m = matches[:-test_size]
    test_m = matches[-test_size:]
    print(f"Train: {len(train_m)}, Test: {len(test_m)}")
    
    print("\nBuilding statistics...")
    stats = build_all_stats(train_m)
    print(f"Stats: hero_wr={len(stats['hero_wr'])}, pos_wr={len(stats['hero_pos_wr'])}, "
          f"lane_matchup={len(stats['lane_matchup'])}")
    
    find_strong_matchups(stats)
    analyze_with_matchups(test_m, stats)
    analyze_combined_approach(test_m, stats)


if __name__ == "__main__":
    main()
