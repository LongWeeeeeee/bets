#!/usr/bin/env python3
"""
Focus on strong signals only: hero_wr and pos_wr.
Try to maximize winrate at different coverage levels.
"""

import json
import os
from collections import defaultdict
from typing import Any, Dict, List, Tuple

import numpy as np


def load_matches(data_dir: str, limit: int = 500000) -> List[Dict[str, Any]]:
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


def build_stats(matches: List[Dict[str, Any]], min_samples: int = 100) -> Dict[str, Any]:
    """Build statistics."""
    hero_stats: Dict[int, List[int]] = defaultdict(list)
    hero_pos_stats: Dict[Tuple[int, int], List[int]] = defaultdict(list)
    
    for m in matches:
        rw = m["radiant_win"]
        
        for pos in range(1, 6):
            rh = m["r_pos"][pos]
            dh = m["d_pos"][pos]
            hero_stats[rh].append(rw)
            hero_stats[dh].append(1 - rw)
            hero_pos_stats[(rh, pos)].append(rw)
            hero_pos_stats[(dh, pos)].append(1 - rw)
    
    hero_wr = {}
    for h, wins in hero_stats.items():
        if len(wins) >= min_samples:
            hero_wr[h] = (np.mean(wins), len(wins))
    
    hero_pos_wr = {}
    for key, wins in hero_pos_stats.items():
        if len(wins) >= min_samples // 2:
            hero_pos_wr[key] = (np.mean(wins), len(wins))
    
    return {"hero_wr": hero_wr, "hero_pos_wr": hero_pos_wr}


def calculate_scores(
    matches: List[Dict[str, Any]],
    stats: Dict[str, Any],
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Calculate hero_wr and pos_wr scores."""
    hero_wr_scores = []
    pos_wr_scores = []
    actuals = []
    
    for m in matches:
        # Hero WR score
        r_wr = sum(stats["hero_wr"].get(m["r_pos"][p], (0.5, 0))[0] for p in range(1, 6))
        d_wr = sum(stats["hero_wr"].get(m["d_pos"][p], (0.5, 0))[0] for p in range(1, 6))
        hero_wr_scores.append(r_wr - d_wr)
        
        # Pos WR score
        pos_score = 0.0
        for pos in range(1, 6):
            rh, dh = m["r_pos"][pos], m["d_pos"][pos]
            r_pwr = stats["hero_pos_wr"].get((rh, pos), (0.5, 0))[0]
            d_pwr = stats["hero_pos_wr"].get((dh, pos), (0.5, 0))[0]
            pos_score += r_pwr - d_pwr
        pos_wr_scores.append(pos_score)
        
        actuals.append(m["radiant_win"])
    
    return np.array(hero_wr_scores), np.array(pos_wr_scores), np.array(actuals)


def analyze_agreement(
    hero_wr: np.ndarray,
    pos_wr: np.ndarray,
    actuals: np.ndarray,
) -> None:
    """Analyze when both signals agree."""
    print("\n=== AGREEMENT ANALYSIS ===")
    
    # Both signals agree on direction
    both_radiant = (hero_wr > 0) & (pos_wr > 0)
    both_dire = (hero_wr < 0) & (pos_wr < 0)
    agree = both_radiant | both_dire
    
    print(f"Both agree: {agree.mean():.2%} of matches")
    
    # When both agree
    preds = (hero_wr >= 0).astype(int)
    wr_agree = (preds[agree] == actuals[agree]).mean()
    print(f"WR when both agree: {wr_agree:.2%}")
    
    # When both agree AND both are strong
    for th in [0.02, 0.04, 0.06, 0.08, 0.10]:
        strong_agree = agree & (np.abs(hero_wr) >= th) & (np.abs(pos_wr) >= th)
        if strong_agree.sum() < 50:
            continue
        
        wr = (preds[strong_agree] == actuals[strong_agree]).mean()
        cov = strong_agree.mean()
        print(f"  Both |score|>={th:.2f}: WR={wr:.2%}, Cov={cov:.2%} ({strong_agree.sum()})")


def analyze_combined_threshold(
    hero_wr: np.ndarray,
    pos_wr: np.ndarray,
    actuals: np.ndarray,
) -> None:
    """Find optimal combined threshold."""
    print("\n=== COMBINED THRESHOLD ===")
    
    # Combined score
    combined = hero_wr + pos_wr
    
    print("Combined (hero_wr + pos_wr):")
    for th in [0.04, 0.06, 0.08, 0.10, 0.12, 0.14, 0.16, 0.18, 0.20]:
        mask = np.abs(combined) >= th
        if mask.sum() < 50:
            continue
        
        preds = (combined >= 0).astype(int)
        wr = (preds[mask] == actuals[mask]).mean()
        cov = mask.mean()
        print(f"  |score|>={th:.2f}: WR={wr:.2%}, Cov={cov:.2%} ({mask.sum()})")


def analyze_extreme_cases(
    hero_wr: np.ndarray,
    pos_wr: np.ndarray,
    actuals: np.ndarray,
) -> None:
    """Look at extreme cases."""
    print("\n=== EXTREME CASES ===")
    
    combined = hero_wr + pos_wr
    
    # Top 10% most confident
    threshold_90 = np.percentile(np.abs(combined), 90)
    mask_90 = np.abs(combined) >= threshold_90
    preds = (combined >= 0).astype(int)
    wr_90 = (preds[mask_90] == actuals[mask_90]).mean()
    print(f"Top 10% confident (|score|>={threshold_90:.3f}): WR={wr_90:.2%}")
    
    # Top 5%
    threshold_95 = np.percentile(np.abs(combined), 95)
    mask_95 = np.abs(combined) >= threshold_95
    wr_95 = (preds[mask_95] == actuals[mask_95]).mean()
    print(f"Top 5% confident (|score|>={threshold_95:.3f}): WR={wr_95:.2%}")
    
    # Top 1%
    threshold_99 = np.percentile(np.abs(combined), 99)
    mask_99 = np.abs(combined) >= threshold_99
    wr_99 = (preds[mask_99] == actuals[mask_99]).mean()
    print(f"Top 1% confident (|score|>={threshold_99:.3f}): WR={wr_99:.2%}")


def analyze_hero_extremes(
    matches: List[Dict[str, Any]],
    stats: Dict[str, Any],
) -> None:
    """Look at matches with extreme hero winrates."""
    print("\n=== HERO EXTREMES ===")
    
    # Find heroes with extreme winrates
    extreme_high = {h for h, (wr, cnt) in stats["hero_wr"].items() if wr >= 0.54 and cnt >= 500}
    extreme_low = {h for h, (wr, cnt) in stats["hero_wr"].items() if wr <= 0.46 and cnt >= 500}
    
    print(f"High WR heroes (>=54%): {len(extreme_high)}")
    print(f"Low WR heroes (<=46%): {len(extreme_low)}")
    
    # Matches where radiant has more high WR heroes
    results = []
    for m in matches:
        r_heroes = set(m["r_pos"][p] for p in range(1, 6))
        d_heroes = set(m["d_pos"][p] for p in range(1, 6))
        
        r_high = len(r_heroes & extreme_high)
        r_low = len(r_heroes & extreme_low)
        d_high = len(d_heroes & extreme_high)
        d_low = len(d_heroes & extreme_low)
        
        # Net advantage
        r_adv = (r_high - r_low) - (d_high - d_low)
        results.append((r_adv, m["radiant_win"]))
    
    results = np.array(results)
    
    for adv in [-3, -2, -1, 0, 1, 2, 3]:
        mask = results[:, 0] == adv
        if mask.sum() < 50:
            continue
        wr = results[mask, 1].mean()
        print(f"  Radiant advantage={adv}: WR={wr:.2%} ({mask.sum()} matches)")


def main() -> None:
    data_dir = "bets_data/analise_pub_matches/json_parts_split_from_object"
    
    print("Loading matches...")
    matches = load_matches(data_dir, limit=500000)
    print(f"Loaded {len(matches)} matches")
    
    # Time-based split
    test_size = int(len(matches) * 0.1)
    train_m = matches[:-test_size]
    test_m = matches[-test_size:]
    print(f"Train: {len(train_m)}, Test: {len(test_m)}")
    
    print("\nBuilding statistics...")
    stats = build_stats(train_m)
    print(f"Stats: hero_wr={len(stats['hero_wr'])}, pos_wr={len(stats['hero_pos_wr'])}")
    
    # Calculate scores
    hero_wr, pos_wr, actuals = calculate_scores(test_m, stats)
    
    print(f"\nHero WR score: mean={hero_wr.mean():.4f}, std={hero_wr.std():.4f}")
    print(f"Pos WR score: mean={pos_wr.mean():.4f}, std={pos_wr.std():.4f}")
    
    # Correlation
    corr = np.corrcoef(hero_wr, pos_wr)[0, 1]
    print(f"Correlation between hero_wr and pos_wr: {corr:.3f}")
    
    # Analyses
    analyze_agreement(hero_wr, pos_wr, actuals)
    analyze_combined_threshold(hero_wr, pos_wr, actuals)
    analyze_extreme_cases(hero_wr, pos_wr, actuals)
    analyze_hero_extremes(test_m, stats)


if __name__ == "__main__":
    main()
