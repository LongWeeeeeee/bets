#!/usr/bin/env python3
"""
Draft classifier v8 - Pure categorical approach.

Remove aggregated features, let CatBoost learn hero interactions directly.
Use more iterations and deeper trees.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import accuracy_score, roc_auc_score

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("train_draft_v8")


def load_all_matches(data_dir: str) -> List[Dict[str, Any]]:
    """Load all matches from split JSON files."""
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
    logger.info(f"Total loaded: {len(matches)} matches")
    return matches


def build_features(m: Dict[str, Any]) -> Dict[str, Any]:
    """Build features - ONLY hero IDs, no aggregations."""
    f: Dict[str, Any] = {}
    
    # Hero IDs by position (categorical)
    for pos in range(1, 6):
        f[f"r_pos{pos}"] = m["r_pos"].get(pos, -1)
        f[f"d_pos{pos}"] = m["d_pos"].get(pos, -1)
    
    # Lane matchups (categorical) - pos1 vs pos1, etc.
    for pos in range(1, 6):
        rh = m["r_pos"].get(pos, -1)
        dh = m["d_pos"].get(pos, -1)
        # Encode as combined ID
        f[f"lane{pos}"] = rh * 1000 + dh if rh > 0 and dh > 0 else -1
    
    # Key cross-lane matchups
    # Carry vs Offlane
    f["r1_vs_d3"] = m["r_pos"].get(1, -1) * 1000 + m["d_pos"].get(3, -1)
    f["d1_vs_r3"] = m["d_pos"].get(1, -1) * 1000 + m["r_pos"].get(3, -1)
    
    # Mid matchup (most important)
    f["mid_matchup"] = m["r_pos"].get(2, -1) * 1000 + m["d_pos"].get(2, -1)
    
    return f


def swap_match(m: Dict[str, Any]) -> Dict[str, Any]:
    """Swap sides."""
    return {
        "match_id": m["match_id"],
        "start_time": m["start_time"],
        "radiant_win": 1 - m["radiant_win"],
        "r_pos": m["d_pos"],
        "d_pos": m["r_pos"],
    }


def generate_dataset(
    matches: List[Dict[str, Any]],
    symmetric: bool = False,
) -> Tuple[pd.DataFrame, np.ndarray]:
    """Generate dataset."""
    all_m = matches.copy()
    if symmetric:
        all_m.extend([swap_match(m) for m in matches])
    
    features = [build_features(m) for m in all_m]
    labels = np.array([m["radiant_win"] for m in all_m])
    
    return pd.DataFrame(features), labels


def find_threshold(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    target_wr: float = 0.70,
    min_cov: float = 0.30,
) -> Tuple[float, float, float]:
    """Find confidence threshold."""
    best = (0.5, 0.5, 1.0)
    
    for th in np.arange(0.50, 0.90, 0.005):
        mask = (y_proba >= th) | (y_proba <= 1 - th)
        if mask.sum() < 100:
            continue
        
        cov = mask.mean()
        preds = (y_proba >= 0.5).astype(int)
        wr = (preds[mask] == y_true[mask]).mean()
        
        if wr >= target_wr and cov >= min_cov and cov > best[2]:
            best = (th, wr, cov)
        elif cov >= min_cov and wr > best[1]:
            best = (th, wr, cov)
    
    return best


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="bets_data/analise_pub_matches/json_parts_split_from_object")
    ap.add_argument("--output", default="ml-models")
    ap.add_argument("--test-ratio", type=float, default=0.05)
    ap.add_argument("--iterations", type=int, default=10000)
    ap.add_argument("--depth", type=int, default=10)
    ap.add_argument("--lr", type=float, default=0.03)
    ap.add_argument("--l2", type=float, default=1.0)
    ap.add_argument("--symmetric", action="store_true", default=True)
    args = ap.parse_args()
    
    matches = load_all_matches(args.data_dir)
    
    # Time-based split
    test_size = int(len(matches) * args.test_ratio)
    train_m = matches[:-test_size]
    test_m = matches[-test_size:]
    logger.info(f"Train: {len(train_m)}, Test: {len(test_m)}")
    
    # Generate features
    logger.info("Generating features...")
    X_train, y_train = generate_dataset(train_m, symmetric=args.symmetric)
    X_test, y_test = generate_dataset(test_m, symmetric=False)
    
    logger.info(f"Train: {len(X_train)}, Test: {len(X_test)}, Features: {len(X_train.columns)}")
    
    # ALL features are categorical
    feature_cols = list(X_train.columns)
    cat_cols = feature_cols
    cat_idx = list(range(len(feature_cols)))
    
    # Prepare - convert to string for categorical
    for col in feature_cols:
        X_train[col] = X_train[col].fillna(-1).astype(int).astype(str)
        X_test[col] = X_test[col].fillna(-1).astype(int).astype(str)
    
    # Train
    logger.info("Training...")
    model = CatBoostClassifier(
        iterations=args.iterations,
        depth=args.depth,
        learning_rate=args.lr,
        l2_leaf_reg=args.l2,
        loss_function="Logloss",
        eval_metric="AUC",
        random_seed=42,
        verbose=500,
        early_stopping_rounds=500,
        use_best_model=True,
        one_hot_max_size=0,  # Force embeddings
    )
    
    train_pool = Pool(X_train, y_train, cat_features=cat_idx)
    test_pool = Pool(X_test, y_test, cat_features=cat_idx)
    model.fit(train_pool, eval_set=test_pool)
    
    # Evaluate
    y_proba = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_proba)
    acc = accuracy_score(y_test, (y_proba >= 0.5).astype(int))
    
    logger.info(f"Base: AUC={auc:.4f}, ACC={acc:.4f}")
    
    # Threshold analysis
    logger.info("\nThreshold analysis:")
    for target in [0.55, 0.60, 0.65, 0.70, 0.75]:
        th, wr, cov = find_threshold(y_test, y_proba, target_wr=target, min_cov=0.10)
        logger.info(f"  Target {target:.0%}: th={th:.3f}, WR={wr:.2%}, Cov={cov:.2%}")
    
    # Symmetric eval
    X_test_sym, y_test_sym = generate_dataset(test_m, symmetric=True)
    for col in feature_cols:
        X_test_sym[col] = X_test_sym[col].fillna(-1).astype(int).astype(str)
    
    y_proba_sym = model.predict_proba(X_test_sym)[:, 1]
    auc_sym = roc_auc_score(y_test_sym, y_proba_sym)
    acc_sym = accuracy_score(y_test_sym, (y_proba_sym >= 0.5).astype(int))
    
    logger.info(f"\nSymmetric: AUC={auc_sym:.4f}, ACC={acc_sym:.4f}")
    
    th, wr, cov = find_threshold(y_test_sym, y_proba_sym, target_wr=0.70, min_cov=0.30)
    logger.info(f"Best threshold: {th:.3f}, WR={wr:.2%}, Cov={cov:.2%}")
    
    # Save
    out_dir = Path(args.output)
    model.save_model(str(out_dir / "draft_v8.cbm"))
    
    meta = {
        "feature_cols": feature_cols,
        "cat_features": cat_cols,
        "cat_indices": cat_idx,
        "threshold": th,
        "metrics": {
            "auc": auc,
            "accuracy": acc,
            "auc_sym": auc_sym,
            "accuracy_sym": acc_sym,
            "winrate_at_threshold": wr,
            "coverage_at_threshold": cov,
        },
        "train_size": len(train_m),
        "test_size": len(test_m),
    }
    with open(out_dir / "draft_v8_meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    
    logger.info(f"Saved to {out_dir}")
    
    # Feature importance
    imp = model.get_feature_importance()
    logger.info("\nFeature importance:")
    for name, val in sorted(zip(feature_cols, imp), key=lambda x: -x[1]):
        logger.info(f"  {name}: {val:.2f}")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
