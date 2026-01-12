#!/usr/bin/env python3
"""
Fast draft classifier - use subset of data for quick iteration.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import accuracy_score, roc_auc_score

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("train_draft_fast")


def load_matches(data_dir: str, max_files: int = 10) -> List[Dict[str, Any]]:
    """Load matches from first N files."""
    matches = []
    
    files = sorted([f for f in os.listdir(data_dir) if f.endswith('.json') and f.startswith('combined')])[:max_files]
    logger.info(f"Loading {len(files)} files...")
    
    for fname in files:
        with open(os.path.join(data_dir, fname)) as f:
            data = json.load(f)
        
        for match_id, m in data.items():
            players = m.get("players", [])
            if len(players) != 10:
                continue
            
            r_by_pos, d_by_pos = {}, {}
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
                "start_time": m.get("startDateTime", 0),
                "radiant_win": 1 if m.get("didRadiantWin") else 0,
                "r_pos": r_by_pos,
                "d_pos": d_by_pos,
            })
    
    matches.sort(key=lambda x: x["start_time"])
    logger.info(f"Loaded {len(matches)} matches")
    return matches


class DraftStats:
    """Build statistics from training data."""
    
    def __init__(self, matches: List[Dict[str, Any]]):
        self.hero_wr: Dict[int, float] = {}
        self.hero_pos_wr: Dict[Tuple[int, int], float] = {}
        self.synergy: Dict[str, float] = {}
        self.counter: Dict[str, float] = {}
        self._compute(matches)
    
    def _compute(self, matches: List[Dict[str, Any]]) -> None:
        hw: Dict[int, List[int]] = defaultdict(list)
        hpw: Dict[Tuple[int, int], List[int]] = defaultdict(list)
        syn: Dict[str, List[int]] = defaultdict(list)
        cnt: Dict[str, List[int]] = defaultdict(list)
        
        for m in matches:
            rw = m["radiant_win"]
            r_heroes = [m["r_pos"][p] for p in range(1, 6)]
            d_heroes = [m["d_pos"][p] for p in range(1, 6)]
            
            for h in r_heroes:
                hw[h].append(rw)
            for h in d_heroes:
                hw[h].append(1 - rw)
            
            for pos in range(1, 6):
                hpw[(m["r_pos"][pos], pos)].append(rw)
                hpw[(m["d_pos"][pos], pos)].append(1 - rw)
            
            rh = sorted(r_heroes)
            dh = sorted(d_heroes)
            for i in range(5):
                for j in range(i + 1, 5):
                    syn[f"{rh[i]}_{rh[j]}"].append(rw)
                    syn[f"{dh[i]}_{dh[j]}"].append(1 - rw)
            
            for rh in r_heroes:
                for dh in d_heroes:
                    cnt[f"{rh}_vs_{dh}"].append(rw)
        
        for h, wins in hw.items():
            if len(wins) >= 50:
                self.hero_wr[h] = np.mean(wins)
        
        for key, wins in hpw.items():
            if len(wins) >= 30:
                self.hero_pos_wr[key] = np.mean(wins)
        
        for key, wins in syn.items():
            if len(wins) >= 20:
                self.synergy[key] = np.mean(wins) - 0.5
        
        for key, wins in cnt.items():
            if len(wins) >= 15:
                self.counter[key] = np.mean(wins) - 0.5


def build_features(m: Dict[str, Any], stats: DraftStats) -> Dict[str, Any]:
    """Build features."""
    f: Dict[str, Any] = {}
    
    # Hero IDs by position
    for pos in range(1, 6):
        f[f"r_pos{pos}"] = m["r_pos"].get(pos, -1)
        f[f"d_pos{pos}"] = m["d_pos"].get(pos, -1)
    
    r_heroes = [m["r_pos"][p] for p in range(1, 6)]
    d_heroes = [m["d_pos"][p] for p in range(1, 6)]
    
    # Hero winrates
    r_wr = sum(stats.hero_wr.get(h, 0.5) for h in r_heroes)
    d_wr = sum(stats.hero_wr.get(h, 0.5) for h in d_heroes)
    f["wr_diff"] = r_wr - d_wr
    
    # Position winrates
    r_pos_wr = sum(stats.hero_pos_wr.get((m["r_pos"][p], p), 0.5) for p in range(1, 6))
    d_pos_wr = sum(stats.hero_pos_wr.get((m["d_pos"][p], p), 0.5) for p in range(1, 6))
    f["pos_wr_diff"] = r_pos_wr - d_pos_wr
    
    # Synergy
    rh = sorted(r_heroes)
    dh = sorted(d_heroes)
    r_syn = sum(stats.synergy.get(f"{rh[i]}_{rh[j]}", 0.0) for i in range(5) for j in range(i+1, 5))
    d_syn = sum(stats.synergy.get(f"{dh[i]}_{dh[j]}", 0.0) for i in range(5) for j in range(i+1, 5))
    f["synergy_diff"] = r_syn - d_syn
    
    # Counter
    r_cnt = sum(stats.counter.get(f"{rh}_vs_{dh}", 0.0) for rh in r_heroes for dh in d_heroes)
    d_cnt = sum(stats.counter.get(f"{dh}_vs_{rh}", 0.0) for rh in r_heroes for dh in d_heroes)
    f["counter_diff"] = r_cnt - d_cnt
    
    # Combined
    f["draft_diff"] = f["wr_diff"] + f["pos_wr_diff"] + f["synergy_diff"] * 2 + f["counter_diff"] * 2
    
    return f


def swap_match(m: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "start_time": m["start_time"],
        "radiant_win": 1 - m["radiant_win"],
        "r_pos": m["d_pos"],
        "d_pos": m["r_pos"],
    }


def find_threshold(y_true: np.ndarray, y_proba: np.ndarray) -> Tuple[float, float, float]:
    best = (0.5, 0.5, 1.0)
    for th in np.arange(0.50, 0.90, 0.005):
        mask = (y_proba >= th) | (y_proba <= 1 - th)
        if mask.sum() < 100:
            continue
        cov = mask.mean()
        wr = ((y_proba[mask] >= 0.5).astype(int) == y_true[mask]).mean()
        if cov >= 0.30 and wr > best[1]:
            best = (th, wr, cov)
    return best


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="bets_data/analise_pub_matches/json_parts_split_from_object")
    ap.add_argument("--max-files", type=int, default=15)
    ap.add_argument("--test-ratio", type=float, default=0.1)
    ap.add_argument("--iterations", type=int, default=2000)
    ap.add_argument("--depth", type=int, default=6)
    ap.add_argument("--lr", type=float, default=0.03)
    args = ap.parse_args()
    
    matches = load_matches(args.data_dir, args.max_files)
    
    test_size = int(len(matches) * args.test_ratio)
    train_m = matches[:-test_size]
    test_m = matches[-test_size:]
    logger.info(f"Train: {len(train_m)}, Test: {len(test_m)}")
    
    stats = DraftStats(train_m)
    logger.info(f"Stats: heroes={len(stats.hero_wr)}, pos_wr={len(stats.hero_pos_wr)}, "
               f"synergy={len(stats.synergy)}, counter={len(stats.counter)}")
    
    # Generate features
    train_sym = train_m + [swap_match(m) for m in train_m]
    X_train = pd.DataFrame([build_features(m, stats) for m in train_sym])
    y_train = np.array([m["radiant_win"] for m in train_sym])
    X_test = pd.DataFrame([build_features(m, stats) for m in test_m])
    y_test = np.array([m["radiant_win"] for m in test_m])
    
    logger.info(f"Train: {len(X_train)}, Test: {len(X_test)}, Features: {len(X_train.columns)}")
    
    # Categorical features
    cat_cols = [f"r_pos{i}" for i in range(1, 6)] + [f"d_pos{i}" for i in range(1, 6)]
    feature_cols = list(X_train.columns)
    cat_idx = [feature_cols.index(c) for c in cat_cols]
    
    for col in feature_cols:
        if col in cat_cols:
            X_train[col] = X_train[col].fillna(-1).astype(int).astype(str)
            X_test[col] = X_test[col].fillna(-1).astype(int).astype(str)
        else:
            X_train[col] = pd.to_numeric(X_train[col], errors="coerce").fillna(0.0)
            X_test[col] = pd.to_numeric(X_test[col], errors="coerce").fillna(0.0)
    
    # Train
    logger.info("Training...")
    model = CatBoostClassifier(
        iterations=args.iterations,
        depth=args.depth,
        learning_rate=args.lr,
        l2_leaf_reg=3.0,
        loss_function="Logloss",
        eval_metric="AUC",
        random_seed=42,
        verbose=500,
        early_stopping_rounds=200,
    )
    
    train_pool = Pool(X_train, y_train, cat_features=cat_idx)
    test_pool = Pool(X_test, y_test, cat_features=cat_idx)
    model.fit(train_pool, eval_set=test_pool, use_best_model=True)
    
    y_proba = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_proba)
    acc = accuracy_score(y_test, (y_proba >= 0.5).astype(int))
    
    logger.info(f"Base: AUC={auc:.4f}, ACC={acc:.4f}")
    
    th, wr, cov = find_threshold(y_test, y_proba)
    logger.info(f"Best: th={th:.3f}, WR={wr:.2%}, Cov={cov:.2%}")
    
    # Threshold analysis
    logger.info("\nThreshold analysis:")
    for th_test in np.arange(0.55, 0.75, 0.05):
        mask = (y_proba >= th_test) | (y_proba <= 1 - th_test)
        if mask.sum() > 0:
            wr_test = ((y_proba[mask] >= 0.5).astype(int) == y_test[mask]).mean()
            cov_test = mask.mean()
            logger.info(f"  th={th_test:.2f}: WR={wr_test:.2%}, Cov={cov_test:.2%}")
    
    # Feature importance
    imp = model.get_feature_importance()
    logger.info("\nFeature importance:")
    for name, val in sorted(zip(feature_cols, imp), key=lambda x: -x[1])[:10]:
        logger.info(f"  {name}: {val:.2f}")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
