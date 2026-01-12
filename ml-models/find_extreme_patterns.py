#!/usr/bin/env python3
"""
Find extreme patterns where multiple strong signals align.
Goal: Find cases where we can predict with 70%+ accuracy.
"""

import json
import os
from collections import defaultdict
from typing import Any, Dict, List, Tuple

import numpy as np


def load_matches(data_dir: str, limit: int = 1400000) -> List[Dict[str, Any]]:
    """Load all matches."""
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
    lane_matchup_stats: Dict[Tuple[int, int, int], List[int]] = defaultdict(list)
    
    for m in matches:
        rw = m["radiant_win"]
        
        for pos in range(1, 6):
            rh = m["r_pos"][pos]
            dh = m["d_pos"][pos]
            
            hero_stats[rh].append(rw)
            hero_stats[dh].append(1 - rw)
            
            hero_pos_stats[(rh, pos)].append(rw)
            hero_pos_stats[(dh, pos)].append(1 - rw)
            
            lane_matchup_stats[(rh, dh, pos)].append(rw)
    
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


def calculate_signals(
    m: Dict[str, Any],
    stats: Dict[str, Any],
) -> Dict[str, float]:
    """Calculate all signals for a match."""
    signals = {}
    
    # Hero WR diff
    r_wr = sum(stats["hero_wr"].get(m["r_pos"][p], (0.5, 0))[0] for p in range(1, 6))
    d_wr = sum(stats["hero_wr"].get(m["d_pos"][p], (0.5, 0))[0] for p in range(1, 6))
    signals["hero_wr"] = r_wr - d_wr
    
    # Pos WR diff
    pos_score = 0.0
    for pos in range(1, 6):
        rh, dh = m["r_pos"][pos], m["d_pos"][pos]
        r_pwr = stats["hero_pos_wr"].get((rh, pos), (0.5, 0))[0]
        d_pwr = stats["hero_pos_wr"].get((dh, pos), (0.5, 0))[0]
        pos_score += r_pwr - d_pwr
    signals["pos_wr"] = pos_score
    
    # Lane matchup score
    match_score = 0.0
    strong_matchups = 0
    for pos in range(1, 6):
        rh, dh = m["r_pos"][pos], m["d_pos"][pos]
        if (rh, dh, pos) in stats["lane_matchup"]:
            wr, cnt = stats["lane_matchup"][(rh, dh, pos)]
            weight = min(1.0, cnt / 100)
            delta = (wr - 0.5) * weight
            match_score += delta
            if abs(delta) >= 0.10:
                strong_matchups += 1 if delta > 0 else -1
    signals["matchup"] = match_score
    signals["strong_matchups"] = strong_matchups
    
    # Count extreme heroes
    extreme_high = 0
    extreme_low = 0
    for pos in range(1, 6):
        rh, dh = m["r_pos"][pos], m["d_pos"][pos]
        r_pwr = stats["hero_pos_wr"].get((rh, pos), (0.5, 0))[0]
        d_pwr = stats["hero_pos_wr"].get((dh, pos), (0.5, 0))[0]
        
        if r_pwr >= 0.54:
            extreme_high += 1
        if r_pwr <= 0.46:
            extreme_low += 1
        if d_pwr >= 0.54:
            extreme_high -= 1
        if d_pwr <= 0.46:
            extreme_low -= 1
    
    signals["extreme_heroes"] = extreme_high - extreme_low
    
    return signals


def analyze_extreme_patterns(
    test_matches: List[Dict[str, Any]],
    stats: Dict[str, Any],
) -> None:
    """Find patterns with high winrate."""
    print("\n=== EXTREME PATTERN ANALYSIS ===")
    
    # Calculate all signals
    all_signals = []
    actuals = []
    
    for m in test_matches:
        signals = calculate_signals(m, stats)
        all_signals.append(signals)
        actuals.append(m["radiant_win"])
    
    actuals = np.array(actuals)
    
    # Convert to arrays
    hero_wr = np.array([s["hero_wr"] for s in all_signals])
    pos_wr = np.array([s["pos_wr"] for s in all_signals])
    matchup = np.array([s["matchup"] for s in all_signals])
    strong_matchups = np.array([s["strong_matchups"] for s in all_signals])
    extreme_heroes = np.array([s["extreme_heroes"] for s in all_signals])
    
    # Pattern 1: All signals agree and are strong
    print("\n1. All signals agree (hero_wr, pos_wr, matchup all same sign):")
    for th in [0.02, 0.04, 0.06, 0.08, 0.10]:
        mask = (
            (np.sign(hero_wr) == np.sign(pos_wr)) &
            (np.sign(hero_wr) == np.sign(matchup)) &
            (np.abs(hero_wr) >= th) &
            (np.abs(pos_wr) >= th) &
            (np.abs(matchup) >= th)
        )
        if mask.sum() < 50:
            continue
        
        preds = (hero_wr >= 0).astype(int)
        wr = (preds[mask] == actuals[mask]).mean()
        cov = mask.mean()
        print(f"  all |signal|>={th:.2f}: WR={wr:.2%}, Cov={cov:.2%} ({mask.sum()})")
    
    # Pattern 2: Strong matchup advantage
    print("\n2. Strong matchup advantage (>=2 strong matchups):")
    for min_strong in [1, 2, 3]:
        mask_r = strong_matchups >= min_strong
        mask_d = strong_matchups <= -min_strong
        
        if mask_r.sum() >= 50:
            wr_r = actuals[mask_r].mean()
            print(f"  Radiant has {min_strong}+ strong matchups: WR={wr_r:.2%}, Cov={mask_r.mean():.2%} ({mask_r.sum()})")
        
        if mask_d.sum() >= 50:
            wr_d = 1 - actuals[mask_d].mean()
            print(f"  Dire has {min_strong}+ strong matchups: WR={wr_d:.2%}, Cov={mask_d.mean():.2%} ({mask_d.sum()})")
    
    # Pattern 3: Extreme hero advantage
    print("\n3. Extreme hero advantage:")
    for adv in [1, 2, 3]:
        mask_r = extreme_heroes >= adv
        mask_d = extreme_heroes <= -adv
        
        if mask_r.sum() >= 50:
            wr_r = actuals[mask_r].mean()
            print(f"  Radiant +{adv} extreme heroes: WR={wr_r:.2%}, Cov={mask_r.mean():.2%} ({mask_r.sum()})")
        
        if mask_d.sum() >= 50:
            wr_d = 1 - actuals[mask_d].mean()
            print(f"  Dire +{adv} extreme heroes: WR={wr_d:.2%}, Cov={mask_d.mean():.2%} ({mask_d.sum()})")
    
    # Pattern 4: Combined score with percentile threshold
    print("\n4. Combined score (percentile-based):")
    combined = hero_wr + pos_wr + matchup * 2
    
    for pct in [90, 95, 97, 99]:
        th_high = np.percentile(combined, pct)
        th_low = np.percentile(combined, 100 - pct)
        
        mask_r = combined >= th_high
        mask_d = combined <= th_low
        
        if mask_r.sum() >= 50:
            wr_r = actuals[mask_r].mean()
            print(f"  Top {100-pct}% (score>={th_high:.3f}): WR={wr_r:.2%} ({mask_r.sum()})")
        
        if mask_d.sum() >= 50:
            wr_d = 1 - actuals[mask_d].mean()
            print(f"  Bottom {100-pct}% (score<={th_low:.3f}): WR={wr_d:.2%} ({mask_d.sum()})")
    
    # Pattern 5: Multiple conditions combined
    print("\n5. Multiple conditions combined:")
    
    # Strong hero_wr + strong pos_wr + positive matchup
    for th in [0.06, 0.08, 0.10]:
        mask = (
            (np.abs(hero_wr) >= th) &
            (np.abs(pos_wr) >= th) &
            (np.sign(hero_wr) == np.sign(pos_wr)) &
            (np.sign(hero_wr) == np.sign(matchup))
        )
        if mask.sum() < 50:
            continue
        
        preds = (hero_wr >= 0).astype(int)
        wr = (preds[mask] == actuals[mask]).mean()
        cov = mask.mean()
        print(f"  hero+pos>={th:.2f}, matchup agrees: WR={wr:.2%}, Cov={cov:.2%} ({mask.sum()})")
    
    # Pattern 6: Weighted score with optimal weights
    print("\n6. Optimized weighted score:")
    
    best = (0, 0, None, 0)
    
    for w1 in np.arange(0.5, 2.5, 0.5):
        for w2 in np.arange(0.5, 2.5, 0.5):
            for w3 in np.arange(1.0, 4.0, 0.5):
                combined = hero_wr * w1 + pos_wr * w2 + matchup * w3
                
                # Find best threshold for 10-15% coverage
                for th in np.arange(0.10, 0.60, 0.02):
                    mask = np.abs(combined) >= th
                    if mask.sum() < 100:
                        continue
                    
                    cov = mask.mean()
                    if cov < 0.10 or cov > 0.20:
                        continue
                    
                    preds = (combined >= 0).astype(int)
                    wr = (preds[mask] == actuals[mask]).mean()
                    
                    if wr > best[0]:
                        best = (wr, cov, (w1, w2, w3), th)
    
    print(f"  Best at 10-20% cov: WR={best[0]:.2%}, Cov={best[1]:.2%}, weights={best[2]}, th={best[3]:.2f}")
    
    # Show detailed results for best weights
    if best[2]:
        w1, w2, w3 = best[2]
        combined = hero_wr * w1 + pos_wr * w2 + matchup * w3
        
        print(f"\n  Detailed for weights {best[2]}:")
        for th in np.arange(0.10, 0.50, 0.05):
            mask = np.abs(combined) >= th
            if mask.sum() < 50:
                continue
            
            preds = (combined >= 0).astype(int)
            wr = (preds[mask] == actuals[mask]).mean()
            cov = mask.mean()
            print(f"    |score|>={th:.2f}: WR={wr:.2%}, Cov={cov:.2%} ({mask.sum()})")


def main() -> None:
    data_dir = "bets_data/analise_pub_matches/json_parts_split_from_object"
    
    print("Loading matches...")
    matches = load_matches(data_dir, limit=1400000)
    print(f"Loaded {len(matches)} matches")
    
    # Time-based split
    test_size = int(len(matches) * 0.1)
    train_m = matches[:-test_size]
    test_m = matches[-test_size:]
    print(f"Train: {len(train_m)}, Test: {len(test_m)}")
    
    print("\nBuilding statistics...")
    stats = build_stats(train_m)
    print(f"Stats: hero_wr={len(stats['hero_wr'])}, pos_wr={len(stats['hero_pos_wr'])}, "
          f"lane_matchup={len(stats['lane_matchup'])}")
    
    analyze_extreme_patterns(test_m, stats)


if __name__ == "__main__":
    main()
