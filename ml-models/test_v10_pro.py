#!/usr/bin/env python3
"""Test draft predictor v10 on pro matches."""

import pandas as pd
import numpy as np
from typing import Dict

from draft_predictor_v10 import DraftPredictorV10


def main() -> None:
    print("Loading predictor (trained on pub matches)...")
    predictor = DraftPredictorV10("ml-models/draft_v10_stats.json")
    
    print("Loading pro matches...")
    df = pd.read_csv("data/pro_matches_enriched.csv")
    print(f"Total pro matches: {len(df)}")
    
    # Sort by start_time for time-based analysis
    df = df.sort_values("start_time").reset_index(drop=True)
    
    # Calculate scores for all matches
    scores = []
    actuals = []
    skipped = 0
    
    for _, row in df.iterrows():
        # Get hero IDs (positions 1-5 map to hero columns)
        # In pro matches, hero_1 is typically pos 1 (carry), hero_2 is pos 2 (mid), etc.
        r_pos: Dict[int, int] = {}
        d_pos: Dict[int, int] = {}
        
        valid = True
        for pos in range(1, 6):
            rh = row.get(f"radiant_hero_{pos}")
            dh = row.get(f"dire_hero_{pos}")
            
            if pd.isna(rh) or pd.isna(dh):
                valid = False
                break
            
            r_pos[pos] = int(rh)
            d_pos[pos] = int(dh)
        
        if not valid:
            skipped += 1
            continue
        
        score, _ = predictor.calculate_score(r_pos, d_pos)
        scores.append(score)
        actuals.append(1 if row["radiant_win"] else 0)
    
    print(f"Valid matches: {len(scores)}, Skipped: {skipped}")
    
    scores = np.array(scores)
    actuals = np.array(actuals)
    
    print(f"\nScore distribution:")
    print(f"  Mean: {scores.mean():.4f}")
    print(f"  Std: {scores.std():.4f}")
    print(f"  Min: {scores.min():.4f}")
    print(f"  Max: {scores.max():.4f}")
    
    # Overall accuracy (predict based on score sign)
    preds = (scores >= 0).astype(int)
    overall_acc = (preds == actuals).mean()
    print(f"\nOverall accuracy (all matches): {overall_acc:.2%}")
    
    # Test at different confidence levels
    print("\n=== CONFIDENCE LEVEL ANALYSIS ===")
    
    thresholds = {
        "low": 0.35,
        "medium": 0.55,
        "high": 0.70,
        "very_high": 0.80,
    }
    
    for level, th in thresholds.items():
        mask = np.abs(scores) >= th
        if mask.sum() < 20:
            print(f"{level}: Too few samples ({mask.sum()})")
            continue
        
        wr = (preds[mask] == actuals[mask]).mean()
        cov = mask.mean()
        
        expected_wr = predictor.get_expected_winrate(level)
        
        print(f"{level:12s}: WR={wr:.2%} (expected {expected_wr:.0%}), Cov={cov:.2%} ({mask.sum()} matches)")
    
    # Detailed threshold analysis
    print("\n=== DETAILED THRESHOLD ANALYSIS ===")
    for th in [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80]:
        mask = np.abs(scores) >= th
        if mask.sum() < 20:
            print(f"|score|>={th:.2f}: Too few ({mask.sum()})")
            continue
        
        wr = (preds[mask] == actuals[mask]).mean()
        cov = mask.mean()
        
        print(f"|score|>={th:.2f}: WR={wr:.2%}, Cov={cov:.2%} ({mask.sum()} matches)")
    
    # Percentile analysis
    print("\n=== PERCENTILE ANALYSIS ===")
    for pct in [70, 80, 90, 95]:
        th = np.percentile(np.abs(scores), pct)
        mask = np.abs(scores) >= th
        
        if mask.sum() < 20:
            continue
        
        wr = (preds[mask] == actuals[mask]).mean()
        cov = mask.mean()
        
        print(f"Top {100-pct}% (|score|>={th:.3f}): WR={wr:.2%}, Cov={cov:.2%} ({mask.sum()} matches)")
    
    # Time-based analysis (last 20% as test)
    print("\n=== TIME-BASED SPLIT (last 20% as test) ===")
    test_size = int(len(scores) * 0.2)
    test_scores = scores[-test_size:]
    test_actuals = actuals[-test_size:]
    test_preds = (test_scores >= 0).astype(int)
    
    print(f"Test set size: {len(test_scores)}")
    print(f"Overall accuracy on test: {(test_preds == test_actuals).mean():.2%}")
    
    for th in [0.20, 0.30, 0.40, 0.50]:
        mask = np.abs(test_scores) >= th
        if mask.sum() < 20:
            continue
        
        wr = (test_preds[mask] == test_actuals[mask]).mean()
        cov = mask.mean()
        
        print(f"  |score|>={th:.2f}: WR={wr:.2%}, Cov={cov:.2%} ({mask.sum()} matches)")


if __name__ == "__main__":
    main()
