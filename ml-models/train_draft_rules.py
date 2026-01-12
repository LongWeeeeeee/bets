#!/usr/bin/env python3
"""
Draft predictor v9 - Rule-based with confidence filtering.

Strategy:
1. Build strong statistics from 1.4M matches
2. Use simple rules with confidence thresholds
3. Only predict when we have high confidence
4. Target: 75%+ winrate, 30%+ coverage
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("train_draft_rules")


def load_all_matches(data_dir: str) -> List[Dict[str, Any]]:
    """Load all matches."""
    matches = []
    
    files = sorted([f for f in os.listdir(data_dir) if f.endswith('.json') and f.startswith('combined')])
    logger.info(f"Found {len(files)} data files")
    
    for fname in files:
        fpath = os.path.join(data_dir, fname)
        logger.info(f"Loading {fname}...")
        
        with open(fpath) as f:
            data = json.load(f)
        
        for match_id, m in data.items():
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
    logger.info(f"Total: {len(matches)} matches")
    return matches


class DraftRules:
    """Rule-based draft predictor."""
    
    def __init__(self, min_samples: int = 50):
        self.min_samples = min_samples
        
        # Statistics
        self.hero_wr: Dict[int, Tuple[float, int]] = {}  # hero -> (winrate, count)
        self.hero_pos_wr: Dict[Tuple[int, int], Tuple[float, int]] = {}  # (hero, pos) -> (wr, count)
        self.synergy: Dict[Tuple[int, int], Tuple[float, int]] = {}  # (h1, h2) -> (wr_delta, count)
        self.counter: Dict[Tuple[int, int], Tuple[float, int]] = {}  # (hero, enemy) -> (wr_delta, count)
        self.matchup: Dict[Tuple[int, int, int], Tuple[float, int]] = {}  # (h1, h2, pos) -> (wr, count)
    
    def fit(self, matches: List[Dict[str, Any]]) -> None:
        """Build statistics from matches."""
        logger.info("Building statistics...")
        
        # Accumulators
        hero_stats: Dict[int, List[int]] = defaultdict(list)
        hero_pos_stats: Dict[Tuple[int, int], List[int]] = defaultdict(list)
        synergy_stats: Dict[Tuple[int, int], List[int]] = defaultdict(list)
        counter_stats: Dict[Tuple[int, int], List[int]] = defaultdict(list)
        matchup_stats: Dict[Tuple[int, int, int], List[int]] = defaultdict(list)
        
        for m in matches:
            rw = m["radiant_win"]
            r_heroes = [m["r_pos"][p] for p in range(1, 6)]
            d_heroes = [m["d_pos"][p] for p in range(1, 6)]
            
            # Hero winrates
            for h in r_heroes:
                hero_stats[h].append(rw)
            for h in d_heroes:
                hero_stats[h].append(1 - rw)
            
            # Position winrates
            for pos in range(1, 6):
                hero_pos_stats[(m["r_pos"][pos], pos)].append(rw)
                hero_pos_stats[(m["d_pos"][pos], pos)].append(1 - rw)
            
            # Synergy (same team)
            for i in range(5):
                for j in range(i + 1, 5):
                    rh1, rh2 = r_heroes[i], r_heroes[j]
                    dh1, dh2 = d_heroes[i], d_heroes[j]
                    key_r = (min(rh1, rh2), max(rh1, rh2))
                    key_d = (min(dh1, dh2), max(dh1, dh2))
                    synergy_stats[key_r].append(rw)
                    synergy_stats[key_d].append(1 - rw)
            
            # Counter (cross-team)
            for rh in r_heroes:
                for dh in d_heroes:
                    counter_stats[(rh, dh)].append(rw)
            
            # Position matchups
            for pos in range(1, 6):
                rh = m["r_pos"][pos]
                dh = m["d_pos"][pos]
                matchup_stats[(rh, dh, pos)].append(rw)
        
        # Compute final stats
        for h, wins in hero_stats.items():
            if len(wins) >= self.min_samples:
                self.hero_wr[h] = (np.mean(wins), len(wins))
        
        for key, wins in hero_pos_stats.items():
            if len(wins) >= self.min_samples:
                self.hero_pos_wr[key] = (np.mean(wins), len(wins))
        
        for key, wins in synergy_stats.items():
            if len(wins) >= 30:
                self.synergy[key] = (np.mean(wins) - 0.5, len(wins))
        
        for key, wins in counter_stats.items():
            if len(wins) >= 30:
                self.counter[key] = (np.mean(wins) - 0.5, len(wins))
        
        for key, wins in matchup_stats.items():
            if len(wins) >= 20:
                self.matchup[key] = (np.mean(wins), len(wins))
        
        logger.info(f"Stats: heroes={len(self.hero_wr)}, pos_wr={len(self.hero_pos_wr)}, "
                   f"synergy={len(self.synergy)}, counter={len(self.counter)}, matchup={len(self.matchup)}")
    
    def predict(
        self,
        r_pos: Dict[int, int],
        d_pos: Dict[int, int],
    ) -> Tuple[Optional[int], float, Dict[str, float]]:
        """
        Predict winner with confidence.
        
        Returns:
            (prediction, confidence, details)
            prediction: 1 = radiant, 0 = dire, None = no prediction
            confidence: 0-1 score
            details: breakdown of signals
        """
        signals: Dict[str, float] = {}
        weights: Dict[str, float] = {}
        
        r_heroes = [r_pos[p] for p in range(1, 6)]
        d_heroes = [d_pos[p] for p in range(1, 6)]
        
        # 1. Hero winrates
        r_wr = []
        d_wr = []
        for h in r_heroes:
            if h in self.hero_wr:
                r_wr.append(self.hero_wr[h][0])
        for h in d_heroes:
            if h in self.hero_wr:
                d_wr.append(self.hero_wr[h][0])
        
        if r_wr and d_wr:
            signals["hero_wr"] = np.mean(r_wr) - np.mean(d_wr)
            weights["hero_wr"] = 1.0
        
        # 2. Position winrates
        r_pos_wr = []
        d_pos_wr = []
        for pos in range(1, 6):
            rh, dh = r_pos[pos], d_pos[pos]
            if (rh, pos) in self.hero_pos_wr:
                r_pos_wr.append(self.hero_pos_wr[(rh, pos)][0])
            if (dh, pos) in self.hero_pos_wr:
                d_pos_wr.append(self.hero_pos_wr[(dh, pos)][0])
        
        if r_pos_wr and d_pos_wr:
            signals["pos_wr"] = np.mean(r_pos_wr) - np.mean(d_pos_wr)
            weights["pos_wr"] = 1.5
        
        # 3. Synergy
        r_syn = []
        d_syn = []
        for i in range(5):
            for j in range(i + 1, 5):
                key_r = (min(r_heroes[i], r_heroes[j]), max(r_heroes[i], r_heroes[j]))
                key_d = (min(d_heroes[i], d_heroes[j]), max(d_heroes[i], d_heroes[j]))
                if key_r in self.synergy:
                    r_syn.append(self.synergy[key_r][0])
                if key_d in self.synergy:
                    d_syn.append(self.synergy[key_d][0])
        
        if r_syn or d_syn:
            signals["synergy"] = sum(r_syn) - sum(d_syn)
            weights["synergy"] = 2.0
        
        # 4. Counter
        r_cnt = []
        d_cnt = []
        for rh in r_heroes:
            for dh in d_heroes:
                if (rh, dh) in self.counter:
                    r_cnt.append(self.counter[(rh, dh)][0])
                if (dh, rh) in self.counter:
                    d_cnt.append(self.counter[(dh, rh)][0])
        
        if r_cnt or d_cnt:
            signals["counter"] = sum(r_cnt) - sum(d_cnt)
            weights["counter"] = 2.5
        
        # 5. Position matchups (most reliable)
        matchup_signals = []
        for pos in range(1, 6):
            rh, dh = r_pos[pos], d_pos[pos]
            if (rh, dh, pos) in self.matchup:
                wr, cnt = self.matchup[(rh, dh, pos)]
                # Weight by sample size
                weight = min(1.0, cnt / 100)
                matchup_signals.append((wr - 0.5) * weight)
        
        if matchup_signals:
            signals["matchup"] = sum(matchup_signals)
            weights["matchup"] = 3.0
        
        # Combine signals
        if not signals:
            return None, 0.0, {}
        
        total_weight = sum(weights.values())
        score = sum(signals[k] * weights[k] for k in signals) / total_weight
        
        # Confidence = absolute score (higher score = more confident)
        # Score typically ranges from -0.1 to +0.1, so we scale it
        confidence = abs(score)
        
        # Prediction based on score sign
        prediction = 1 if score > 0 else 0
        
        return prediction, confidence, signals
    
    def evaluate(
        self,
        matches: List[Dict[str, Any]],
        confidence_threshold: float = 0.5,
    ) -> Dict[str, float]:
        """Evaluate on test set."""
        predictions = []
        actuals = []
        confidences = []
        
        for m in matches:
            pred, conf, _ = self.predict(m["r_pos"], m["d_pos"])
            if pred is not None:
                predictions.append(pred)
                actuals.append(m["radiant_win"])
                confidences.append(conf)
        
        predictions = np.array(predictions)
        actuals = np.array(actuals)
        confidences = np.array(confidences)
        
        # Overall
        overall_acc = (predictions == actuals).mean()
        
        # With threshold
        mask = confidences >= confidence_threshold
        if mask.sum() > 0:
            filtered_acc = (predictions[mask] == actuals[mask]).mean()
            coverage = mask.mean()
        else:
            filtered_acc = 0.0
            coverage = 0.0
        
        return {
            "overall_accuracy": overall_acc,
            "filtered_accuracy": filtered_acc,
            "coverage": coverage,
            "total_predictions": len(predictions),
            "filtered_predictions": int(mask.sum()),
        }
    
    def find_best_threshold(
        self,
        matches: List[Dict[str, Any]],
        target_wr: float = 0.75,
        min_cov: float = 0.30,
    ) -> Tuple[float, float, float]:
        """Find best confidence threshold."""
        predictions = []
        actuals = []
        confidences = []
        
        for m in matches:
            pred, conf, _ = self.predict(m["r_pos"], m["d_pos"])
            if pred is not None:
                predictions.append(pred)
                actuals.append(m["radiant_win"])
                confidences.append(conf)
        
        predictions = np.array(predictions)
        actuals = np.array(actuals)
        confidences = np.array(confidences)
        
        best = (0.0, 0.5, 1.0)  # (threshold, winrate, coverage)
        
        # Score-based threshold (score typically 0-0.2)
        for th in np.arange(0.01, 0.20, 0.005):
            mask = confidences >= th
            if mask.sum() < 100:
                continue
            
            cov = mask.sum() / len(matches)
            wr = (predictions[mask] == actuals[mask]).mean()
            
            if wr >= target_wr and cov >= min_cov and cov > best[2]:
                best = (th, wr, cov)
            elif cov >= min_cov and wr > best[1]:
                best = (th, wr, cov)
        
        return best
    
    def save(self, path: str) -> None:
        """Save statistics."""
        data = {
            "hero_wr": {str(k): v for k, v in self.hero_wr.items()},
            "hero_pos_wr": {f"{h}_{p}": v for (h, p), v in self.hero_pos_wr.items()},
            "synergy": {f"{h1}_{h2}": v for (h1, h2), v in self.synergy.items()},
            "counter": {f"{h1}_vs_{h2}": v for (h1, h2), v in self.counter.items()},
            "matchup": {f"{h1}_{h2}_{p}": v for (h1, h2, p), v in self.matchup.items()},
        }
        with open(path, "w") as f:
            json.dump(data, f)
        logger.info(f"Saved to {path}")




def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="bets_data/analise_pub_matches/json_parts_split_from_object")
    ap.add_argument("--output", default="ml-models")
    ap.add_argument("--test-ratio", type=float, default=0.05)
    ap.add_argument("--min-samples", type=int, default=50)
    args = ap.parse_args()
    
    matches = load_all_matches(args.data_dir)
    
    # Time-based split
    test_size = int(len(matches) * args.test_ratio)
    train_m = matches[:-test_size]
    test_m = matches[-test_size:]
    logger.info(f"Train: {len(train_m)}, Test: {len(test_m)}")
    
    # Build rules
    rules = DraftRules(min_samples=args.min_samples)
    rules.fit(train_m)
    
    # Evaluate
    logger.info("\nEvaluating...")
    
    # Find best threshold
    th, wr, cov = rules.find_best_threshold(test_m, target_wr=0.75, min_cov=0.30)
    logger.info(f"Best threshold: {th:.3f}, WR={wr:.2%}, Coverage={cov:.2%}")
    
    # Detailed evaluation at different thresholds (score-based)
    logger.info("\nThreshold analysis (score-based):")
    for threshold in [0.02, 0.04, 0.06, 0.08, 0.10, 0.12, 0.15]:
        results = rules.evaluate(test_m, confidence_threshold=threshold)
        logger.info(f"  score>={threshold:.2f}: WR={results['filtered_accuracy']:.2%}, "
                   f"Cov={results['coverage']:.2%} ({results['filtered_predictions']}/{results['total_predictions']})")
    
    # Save
    out_dir = Path(args.output)
    rules.save(str(out_dir / "draft_rules_stats.json"))
    
    meta = {
        "threshold": th,
        "metrics": {
            "winrate_at_threshold": wr,
            "coverage_at_threshold": cov,
        },
        "train_size": len(train_m),
        "test_size": len(test_m),
    }
    with open(out_dir / "draft_rules_meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
