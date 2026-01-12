#!/usr/bin/env python3
"""
Final Draft Predictor - High confidence predictions.

Strategy:
1. Use counter/synergy stats from 1.4M matches
2. Calculate draft advantage score
3. Only predict when score exceeds threshold
4. Higher threshold = higher winrate, lower coverage
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np


class DraftPredictor:
    """Draft-based match predictor."""
    
    def __init__(self, stats_path: str = "ml-models/draft_v7_stats.json"):
        self.hero_wr: Dict[int, float] = {}
        self.hero_pos_wr: Dict[Tuple[int, int], float] = {}
        self.synergy: Dict[str, float] = {}
        self.counter: Dict[str, float] = {}
        
        self._load_stats(stats_path)
    
    def _load_stats(self, path: str) -> None:
        """Load pre-computed statistics."""
        with open(path) as f:
            stats = json.load(f)
        
        self.hero_wr = {int(k): v for k, v in stats["hero_wr"].items()}
        
        for k, v in stats["hero_pos_wr"].items():
            h, p = k.rsplit("_", 1)
            self.hero_pos_wr[(int(h), int(p))] = v
        
        self.synergy = stats["synergy"]
        self.counter = stats["counter"]
    
    def calculate_draft_score(
        self,
        radiant_heroes: List[int],
        dire_heroes: List[int],
        radiant_positions: Optional[Dict[int, int]] = None,
        dire_positions: Optional[Dict[int, int]] = None,
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Calculate draft advantage score for radiant.
        
        Returns:
            (score, details)
            score > 0 means radiant advantage
            score < 0 means dire advantage
        """
        details: Dict[str, Any] = {}
        
        # 1. Hero winrate component
        r_wr = sum(self.hero_wr.get(h, 0.5) for h in radiant_heroes)
        d_wr = sum(self.hero_wr.get(h, 0.5) for h in dire_heroes)
        wr_diff = r_wr - d_wr
        details["wr_diff"] = wr_diff
        
        # 2. Position winrate component (if positions known)
        pos_diff = 0.0
        if radiant_positions and dire_positions:
            for pos in range(1, 6):
                rh = radiant_positions.get(pos, 0)
                dh = dire_positions.get(pos, 0)
                r_pwr = self.hero_pos_wr.get((rh, pos), 0.5)
                d_pwr = self.hero_pos_wr.get((dh, pos), 0.5)
                pos_diff += r_pwr - d_pwr
        details["pos_diff"] = pos_diff
        
        # 3. Synergy component
        rh_sorted = sorted(radiant_heroes)
        dh_sorted = sorted(dire_heroes)
        
        r_syn = sum(
            self.synergy.get(f"{rh_sorted[i]}_{rh_sorted[j]}", 0.0)
            for i in range(5) for j in range(i + 1, 5)
        )
        d_syn = sum(
            self.synergy.get(f"{dh_sorted[i]}_{dh_sorted[j]}", 0.0)
            for i in range(5) for j in range(i + 1, 5)
        )
        syn_diff = r_syn - d_syn
        details["synergy_diff"] = syn_diff
        
        # 4. Counter component (most important)
        r_cnt = 0.0
        d_cnt = 0.0
        strong_counters: List[Tuple[int, int, float]] = []
        
        for rh in radiant_heroes:
            for dh in dire_heroes:
                rc = self.counter.get(f"{rh}_vs_{dh}", 0.0)
                dc = self.counter.get(f"{dh}_vs_{rh}", 0.0)
                r_cnt += rc
                d_cnt += dc
                
                # Track strong counters
                if rc >= 0.08:
                    strong_counters.append((rh, dh, rc))
                if dc >= 0.08:
                    strong_counters.append((dh, rh, dc))
        
        cnt_diff = r_cnt - d_cnt
        details["counter_diff"] = cnt_diff
        details["strong_counters"] = strong_counters
        
        # Combined score with weights
        # Counter is most predictive based on feature importance
        score = (
            wr_diff * 1.0 +
            pos_diff * 1.0 +
            syn_diff * 1.5 +
            cnt_diff * 2.0
        )
        details["score"] = score
        
        return score, details
    
    def predict(
        self,
        radiant_heroes: List[int],
        dire_heroes: List[int],
        radiant_positions: Optional[Dict[int, int]] = None,
        dire_positions: Optional[Dict[int, int]] = None,
        min_score: float = 0.05,
    ) -> Tuple[Optional[int], float, Dict[str, Any]]:
        """
        Predict match winner.
        
        Args:
            radiant_heroes: List of 5 hero IDs
            dire_heroes: List of 5 hero IDs
            radiant_positions: Optional {pos: hero_id}
            dire_positions: Optional {pos: hero_id}
            min_score: Minimum absolute score to make prediction
        
        Returns:
            (prediction, abs_score, details)
            prediction: 1 = radiant, 0 = dire, None = no prediction
        """
        score, details = self.calculate_draft_score(
            radiant_heroes, dire_heroes, radiant_positions, dire_positions
        )
        
        abs_score = abs(score)
        
        if abs_score < min_score:
            return None, abs_score, details
        
        prediction = 1 if score > 0 else 0
        return prediction, abs_score, details


def evaluate_predictor(
    predictor: DraftPredictor,
    matches: List[Dict[str, Any]],
) -> None:
    """Evaluate predictor at different thresholds."""
    scores = []
    actuals = []
    
    for m in matches:
        r_heroes = [m["r_pos"][p] for p in range(1, 6)]
        d_heroes = [m["d_pos"][p] for p in range(1, 6)]
        
        score, _ = predictor.calculate_draft_score(
            r_heroes, d_heroes, m["r_pos"], m["d_pos"]
        )
        scores.append(score)
        actuals.append(m["radiant_win"])
    
    scores = np.array(scores)
    actuals = np.array(actuals)
    
    print("Score distribution:")
    print(f"  Mean: {np.mean(scores):.4f}")
    print(f"  Std: {np.std(scores):.4f}")
    print(f"  Min: {np.min(scores):.4f}")
    print(f"  Max: {np.max(scores):.4f}")
    
    print("\nThreshold analysis:")
    for th in [0.02, 0.04, 0.06, 0.08, 0.10, 0.12, 0.15, 0.20]:
        # Confident predictions
        mask = np.abs(scores) >= th
        if mask.sum() == 0:
            continue
        
        preds = (scores >= 0).astype(int)
        wr = (preds[mask] == actuals[mask]).mean()
        cov = mask.mean()
        
        print(f"  |score|>={th:.2f}: WR={wr:.2%}, Cov={cov:.2%} ({mask.sum()} matches)")


if __name__ == "__main__":
    predictor = DraftPredictor()
    
    # Example
    r_heroes = [82, 8, 2, 86, 26]  # Meepo draft
    d_heroes = [1, 39, 7, 14, 75]
    
    pred, score, details = predictor.predict(r_heroes, d_heroes)
    print(f"Prediction: {'Radiant' if pred == 1 else 'Dire' if pred == 0 else 'Skip'}")
    print(f"Score: {score:.4f}")
    print(f"Counter diff: {details['counter_diff']:.4f}")
