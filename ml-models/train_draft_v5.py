#!/usr/bin/env python3
"""
Draft-only classifier v5 - High confidence predictions.

Strategy:
1. Use hero IDs as main categorical features (let CatBoost learn embeddings)
2. Add strong draft signals: synergy, counters, position winrates
3. Use symmetric training (swap sides) for better generalization
4. Find confidence threshold for 75%+ winrate, 30%+ coverage
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("train_draft_v5")

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_pub_matches(path: str) -> List[Dict[str, Any]]:
    """Load pub matches."""
    logger.info(f"Loading {path}...")
    with open(path) as f:
        data = json.load(f)
    
    matches = []
    for match_id, m in data.items():
        players = m.get("players", [])
        if len(players) != 10:
            continue
        
        r_heroes, d_heroes = [], []
        r_pos, d_pos = {}, {}
        
        for p in players:
            hero_id = p.get("heroId")
            pos_str = p.get("position", "")
            is_rad = p.get("isRadiant", False)
            
            if not hero_id:
                continue
            
            pos = int(pos_str.replace("POSITION_", "")) if "POSITION_" in pos_str else 0
            
            if is_rad:
                r_heroes.append(hero_id)
                if pos:
                    r_pos[pos] = hero_id
            else:
                d_heroes.append(hero_id)
                if pos:
                    d_pos[pos] = hero_id
        
        if len(r_heroes) != 5 or len(d_heroes) != 5:
            continue
        
        matches.append({
            "match_id": int(match_id),
            "start_time": m.get("startDateTime", 0),
            "radiant_win": 1 if m.get("didRadiantWin") else 0,
            "r_heroes": r_heroes,
            "d_heroes": d_heroes,
            "r_pos": r_pos,
            "d_pos": d_pos,
            "bracket": m.get("bracket", 0),
        })
    
    matches.sort(key=lambda x: x["start_time"])
    logger.info(f"Loaded {len(matches)} matches")
    return matches


class DraftStats:
    """Compute draft statistics from training data."""
    
    def __init__(self, matches: List[Dict[str, Any]]):
        self.hero_wr: Dict[int, float] = {}
        self.hero_pos_wr: Dict[Tuple[int, int], float] = {}
        self.synergy: Dict[str, float] = {}
        self.counter: Dict[str, float] = {}
        self._compute(matches)
    
    def _compute(self, matches: List[Dict[str, Any]]) -> None:
        # Hero winrates
        hw: Dict[int, List[int]] = defaultdict(list)
        # Hero-position winrates
        hpw: Dict[Tuple[int, int], List[int]] = defaultdict(list)
        # Synergy
        syn: Dict[str, List[int]] = defaultdict(list)
        # Counter
        cnt: Dict[str, List[int]] = defaultdict(list)
        
        for m in matches:
            rw = m["radiant_win"]
            
            for h in m["r_heroes"]:
                hw[h].append(rw)
            for h in m["d_heroes"]:
                hw[h].append(1 - rw)
            
            for pos, h in m["r_pos"].items():
                hpw[(h, pos)].append(rw)
            for pos, h in m["d_pos"].items():
                hpw[(h, pos)].append(1 - rw)
            
            # Synergy (same team pairs)
            rh = sorted(m["r_heroes"])
            dh = sorted(m["d_heroes"])
            for i in range(5):
                for j in range(i + 1, 5):
                    syn[f"{rh[i]}_{rh[j]}"].append(rw)
                    syn[f"{dh[i]}_{dh[j]}"].append(1 - rw)
            
            # Counter (cross-team)
            for rh in m["r_heroes"]:
                for dh in m["d_heroes"]:
                    cnt[f"{rh}_vs_{dh}"].append(rw)
        
        # Compute winrates with min samples
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
        
        logger.info(f"Stats: heroes={len(self.hero_wr)}, pos_wr={len(self.hero_pos_wr)}, "
                   f"synergy={len(self.synergy)}, counter={len(self.counter)}")


def build_features(
    m: Dict[str, Any],
    stats: DraftStats,
) -> Dict[str, Any]:
    """Build features for a single match."""
    f: Dict[str, Any] = {}
    
    r_heroes = m["r_heroes"]
    d_heroes = m["d_heroes"]
    r_pos = m["r_pos"]
    d_pos = m["d_pos"]
    
    # === Hero IDs (categorical) ===
    for i in range(5):
        f[f"r_hero_{i+1}"] = r_heroes[i] if i < len(r_heroes) else -1
        f[f"d_hero_{i+1}"] = d_heroes[i] if i < len(d_heroes) else -1
    
    # === Hero winrates ===
    r_wr = [stats.hero_wr.get(h, 0.5) for h in r_heroes]
    d_wr = [stats.hero_wr.get(h, 0.5) for h in d_heroes]
    
    f["r_wr_sum"] = sum(r_wr)
    f["d_wr_sum"] = sum(d_wr)
    f["wr_diff"] = f["r_wr_sum"] - f["d_wr_sum"]
    
    # === Position winrates ===
    for pos in range(1, 6):
        rh = r_pos.get(pos, 0)
        dh = d_pos.get(pos, 0)
        r_pwr = stats.hero_pos_wr.get((rh, pos), 0.5)
        d_pwr = stats.hero_pos_wr.get((dh, pos), 0.5)
        f[f"r_pos{pos}_wr"] = r_pwr
        f[f"d_pos{pos}_wr"] = d_pwr
        f[f"pos{pos}_diff"] = r_pwr - d_pwr
    
    # Aggregate position winrates
    r_pos_wrs = [f[f"r_pos{p}_wr"] for p in range(1, 6)]
    d_pos_wrs = [f[f"d_pos{p}_wr"] for p in range(1, 6)]
    f["r_pos_wr_sum"] = sum(r_pos_wrs)
    f["d_pos_wr_sum"] = sum(d_pos_wrs)
    f["pos_wr_diff"] = f["r_pos_wr_sum"] - f["d_pos_wr_sum"]
    
    # Core vs support
    f["r_core_wr"] = sum(r_pos_wrs[:3])
    f["d_core_wr"] = sum(d_pos_wrs[:3])
    f["core_wr_diff"] = f["r_core_wr"] - f["d_core_wr"]
    
    # === Synergy ===
    r_syn = 0.0
    d_syn = 0.0
    rh_sorted = sorted(r_heroes)
    dh_sorted = sorted(d_heroes)
    
    for i in range(5):
        for j in range(i + 1, 5):
            r_syn += stats.synergy.get(f"{rh_sorted[i]}_{rh_sorted[j]}", 0.0)
            d_syn += stats.synergy.get(f"{dh_sorted[i]}_{dh_sorted[j]}", 0.0)
    
    f["r_synergy"] = r_syn
    f["d_synergy"] = d_syn
    f["synergy_diff"] = r_syn - d_syn
    
    # === Counter ===
    r_cnt = 0.0
    d_cnt = 0.0
    r_max_cnt = -1.0
    d_max_cnt = -1.0
    
    for rh in r_heroes:
        for dh in d_heroes:
            rc = stats.counter.get(f"{rh}_vs_{dh}", 0.0)
            dc = stats.counter.get(f"{dh}_vs_{rh}", 0.0)
            r_cnt += rc
            d_cnt += dc
            r_max_cnt = max(r_max_cnt, rc)
            d_max_cnt = max(d_max_cnt, dc)
    
    f["r_counter"] = r_cnt
    f["d_counter"] = d_cnt
    f["counter_diff"] = r_cnt - d_cnt
    f["r_max_counter"] = r_max_cnt
    f["d_max_counter"] = d_max_cnt
    
    # === Combined draft score ===
    f["r_draft"] = f["r_wr_sum"] + f["r_pos_wr_sum"] + f["r_synergy"] * 2 + f["r_counter"] * 2
    f["d_draft"] = f["d_wr_sum"] + f["d_pos_wr_sum"] + f["d_synergy"] * 2 + f["d_counter"] * 2
    f["draft_diff"] = f["r_draft"] - f["d_draft"]
    
    return f


def swap_match(m: Dict[str, Any]) -> Dict[str, Any]:
    """Swap radiant/dire for symmetric training."""
    return {
        "match_id": m["match_id"],
        "start_time": m["start_time"],
        "radiant_win": 1 - m["radiant_win"],
        "r_heroes": m["d_heroes"],
        "d_heroes": m["r_heroes"],
        "r_pos": m["d_pos"],
        "d_pos": m["r_pos"],
        "bracket": m["bracket"],
    }



def generate_dataset(
    matches: List[Dict[str, Any]],
    stats: DraftStats,
    symmetric: bool = False,
) -> Tuple[pd.DataFrame, np.ndarray]:
    """Generate features for all matches."""
    all_matches = matches.copy()
    if symmetric:
        all_matches.extend([swap_match(m) for m in matches])
    
    features = [build_features(m, stats) for m in all_matches]
    labels = np.array([m["radiant_win"] for m in all_matches])
    
    return pd.DataFrame(features), labels


def find_threshold(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    target_wr: float = 0.75,
    min_cov: float = 0.30,
) -> Tuple[float, float, float]:
    """Find confidence threshold."""
    best = (0.5, 0.5, 1.0)  # (threshold, winrate, coverage)
    
    for th in np.arange(0.50, 0.85, 0.005):
        mask = (y_proba >= th) | (y_proba <= 1 - th)
        if mask.sum() == 0:
            continue
        
        cov = mask.mean()
        preds = (y_proba >= 0.5).astype(int)
        wr = (preds[mask] == y_true[mask]).mean()
        
        # Prefer higher coverage at target winrate
        if wr >= target_wr and cov >= min_cov:
            if cov > best[2] or (cov == best[2] and wr > best[1]):
                best = (th, wr, cov)
        # Or best winrate at min coverage
        elif cov >= min_cov and wr > best[1]:
            best = (th, wr, cov)
    
    return best


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="bets_data/analise_pub_matches/extracted_100k_matches.json")
    ap.add_argument("--output", default="ml-models")
    ap.add_argument("--test-size", type=int, default=10000)
    ap.add_argument("--iterations", type=int, default=5000)
    ap.add_argument("--depth", type=int, default=8)
    ap.add_argument("--lr", type=float, default=0.01)
    ap.add_argument("--l2", type=float, default=3.0)
    ap.add_argument("--symmetric", action="store_true", default=True)
    ap.add_argument("--target-wr", type=float, default=0.75)
    ap.add_argument("--min-cov", type=float, default=0.30)
    args = ap.parse_args()
    
    matches = load_pub_matches(args.data)
    
    # Time-based split
    train_m = matches[:-args.test_size]
    test_m = matches[-args.test_size:]
    logger.info(f"Train: {len(train_m)}, Test: {len(test_m)}")
    
    # Build stats from train only
    stats = DraftStats(train_m)
    
    # Generate features
    logger.info("Generating features...")
    X_train, y_train = generate_dataset(train_m, stats, symmetric=args.symmetric)
    X_test, y_test = generate_dataset(test_m, stats, symmetric=False)
    
    logger.info(f"Train samples: {len(X_train)} (symmetric={args.symmetric})")
    logger.info(f"Test samples: {len(X_test)}")
    logger.info(f"Features: {len(X_train.columns)}")
    
    # Categorical features
    cat_cols = [f"r_hero_{i}" for i in range(1, 6)] + [f"d_hero_{i}" for i in range(1, 6)]
    feature_cols = list(X_train.columns)
    cat_idx = [feature_cols.index(c) for c in cat_cols]
    
    # Prepare data
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
        l2_leaf_reg=args.l2,
        loss_function="Logloss",
        eval_metric="AUC",
        random_seed=42,
        verbose=500,
        early_stopping_rounds=300,
        use_best_model=True,
    )
    
    train_pool = Pool(X_train, y_train, cat_features=cat_idx)
    test_pool = Pool(X_test, y_test, cat_features=cat_idx)
    model.fit(train_pool, eval_set=test_pool)
    
    # Evaluate
    y_proba = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_proba)
    acc = accuracy_score(y_test, (y_proba >= 0.5).astype(int))
    
    logger.info(f"Base: AUC={auc:.4f}, ACC={acc:.4f}")
    
    # Find threshold
    th, wr, cov = find_threshold(y_test, y_proba, args.target_wr, args.min_cov)
    logger.info(f"Threshold={th:.3f}: WR={wr:.2%}, Coverage={cov:.2%}")
    
    # Symmetric evaluation
    X_test_sym, y_test_sym = generate_dataset(test_m, stats, symmetric=True)
    for col in feature_cols:
        if col in cat_cols:
            X_test_sym[col] = X_test_sym[col].fillna(-1).astype(int).astype(str)
        else:
            X_test_sym[col] = pd.to_numeric(X_test_sym[col], errors="coerce").fillna(0.0)
    
    y_proba_sym = model.predict_proba(X_test_sym)[:, 1]
    auc_sym = roc_auc_score(y_test_sym, y_proba_sym)
    acc_sym = accuracy_score(y_test_sym, (y_proba_sym >= 0.5).astype(int))
    th_sym, wr_sym, cov_sym = find_threshold(y_test_sym, y_proba_sym, args.target_wr, args.min_cov)
    
    logger.info(f"Symmetric: AUC={auc_sym:.4f}, ACC={acc_sym:.4f}")
    logger.info(f"Symmetric threshold={th_sym:.3f}: WR={wr_sym:.2%}, Coverage={cov_sym:.2%}")
    
    # Save
    out_dir = Path(args.output)
    out_dir.mkdir(exist_ok=True)
    
    model.save_model(str(out_dir / "draft_v5.cbm"))
    
    meta = {
        "feature_cols": feature_cols,
        "cat_features": cat_cols,
        "cat_indices": cat_idx,
        "threshold": th_sym,
        "metrics": {
            "auc": auc,
            "accuracy": acc,
            "auc_sym": auc_sym,
            "accuracy_sym": acc_sym,
            "winrate_at_threshold": wr_sym,
            "coverage_at_threshold": cov_sym,
        },
        "train_size": len(train_m),
        "test_size": len(test_m),
    }
    
    with open(out_dir / "draft_v5_meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    
    # Save stats
    stats_dict = {
        "hero_wr": {str(k): v for k, v in stats.hero_wr.items()},
        "hero_pos_wr": {f"{h}_{p}": v for (h, p), v in stats.hero_pos_wr.items()},
        "synergy": stats.synergy,
        "counter": stats.counter,
    }
    with open(out_dir / "draft_v5_stats.json", "w") as f:
        json.dump(stats_dict, f)
    
    logger.info(f"Saved to {out_dir}")
    
    # Feature importance
    imp = model.get_feature_importance()
    top = sorted(zip(feature_cols, imp), key=lambda x: -x[1])[:15]
    logger.info("Top features:")
    for name, val in top:
        logger.info(f"  {name}: {val:.2f}")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
