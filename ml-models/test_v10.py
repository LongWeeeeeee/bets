#!/usr/bin/env python3
"""Test draft predictor v10 on held-out data."""

import json
import os
from typing import Any, Dict, List

import numpy as np

from draft_predictor_v10 import DraftPredictorV10


def load_test_matches(data_dir: str, skip_first: int = 1250000) -> List[Dict[str, Any]]:
    """Load test matches (last ~140k)."""
    matches = []
    files = sorted([f for f in os.listdir(data_dir) if f.endswith('.json') and f.startswith('combined')])
    
    total_loaded = 0
    
    for fname in files:
        fpath = os.path.join(data_dir, fname)
        
        with open(fpath) as f:
            data = json.load(f)
        
        for match_id, m in data.items():
            total_loaded += 1
            
            if total_loaded <= skip_first:
                continue
            
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
                "radiant_win": 1 if m.get("didRadiantWin") else 0,
                "r_pos": r_by_pos,
                "d_pos": d_by_pos,
            })
    
    return matches


def main() -> None:
    print("Loading predictor...")
    predictor = DraftPredictorV10("ml-models/draft_v10_stats.json")
    
    print("Loading test matches...")
    data_dir = "bets_data/analise_pub_matches/json_parts_split_from_object"
    test_matches = load_test_matches(data_dir, skip_first=1250000)
    print(f"Loaded {len(test_matches)} test matches")
    
    # Calculate scores for all matches
    scores = []
    actuals = []
    
    for m in test_matches:
        score, _ = predictor.calculate_score(m["r_pos"], m["d_pos"])
        scores.append(score)
        actuals.append(m["radiant_win"])
    
    scores = np.array(scores)
    actuals = np.array(actuals)
    
    print(f"\nScore distribution:")
    print(f"  Mean: {scores.mean():.4f}")
    print(f"  Std: {scores.std():.4f}")
    print(f"  Min: {scores.min():.4f}")
    print(f"  Max: {scores.max():.4f}")
    
    # Test at different confidence levels
    print("\n=== CONFIDENCE LEVEL ANALYSIS ===")
    
    thresholds = {
        "low": 0.30,
        "medium": 0.52,
        "high": 0.70,
        "very_high": 1.0,
    }
    
    for level, th in thresholds.items():
        mask = np.abs(scores) >= th
        if mask.sum() < 50:
            print(f"{level}: Too few samples ({mask.sum()})")
            continue
        
        preds = (scores >= 0).astype(int)
        wr = (preds[mask] == actuals[mask]).mean()
        cov = mask.mean()
        
        expected_wr = predictor.get_expected_winrate(level)
        
        print(f"{level:12s}: WR={wr:.2%} (expected {expected_wr:.0%}), Cov={cov:.2%} ({mask.sum()} matches)")
    
    # Detailed threshold analysis
    print("\n=== DETAILED THRESHOLD ANALYSIS ===")
    for th in [0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00]:
        mask = np.abs(scores) >= th
        if mask.sum() < 50:
            print(f"|score|>={th:.2f}: Too few ({mask.sum()})")
            continue
        
        preds = (scores >= 0).astype(int)
        wr = (preds[mask] == actuals[mask]).mean()
        cov = mask.mean()
        
        print(f"|score|>={th:.2f}: WR={wr:.2%}, Cov={cov:.2%} ({mask.sum()} matches)")
    
    # Percentile analysis
    print("\n=== PERCENTILE ANALYSIS ===")
    for pct in [70, 80, 90, 95, 97, 99]:
        th = np.percentile(np.abs(scores), pct)
        mask = np.abs(scores) >= th
        
        preds = (scores >= 0).astype(int)
        wr = (preds[mask] == actuals[mask]).mean()
        cov = mask.mean()
        
        print(f"Top {100-pct}% (|score|>={th:.3f}): WR={wr:.2%}, Cov={cov:.2%} ({mask.sum()} matches)")


if __name__ == "__main__":
    main()
