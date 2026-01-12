#!/usr/bin/env python3
"""
Train high-confidence draft-only classifier.
Target: 75%+ winrate with 30%+ coverage on pub matches.

Key ideas:
1. Use ONLY draft features (no team/player data)
2. Build hero-position winrates from pub data
3. Focus on synergy/counter signals
4. Train confidence threshold for high precision
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("train_draft_high_confidence")

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_pub_matches(path: str) -> List[Dict[str, Any]]:
    """Load pub matches from JSON."""
    logger.info(f"Loading pub matches from {path}...")
    with open(path, "r") as f:
        data = json.load(f)
    
    matches = []
    for match_id, match in data.items():
        players = match.get("players", [])
        if len(players) != 10:
            continue
        
        radiant_heroes = []
        dire_heroes = []
        radiant_positions = {}
        dire_positions = {}
        
        for p in players:
            hero_id = p.get("heroId")
            pos = p.get("position", "")
            is_radiant = p.get("isRadiant", False)
            
            if not hero_id or not pos:
                continue
            
            pos_num = int(pos.replace("POSITION_", "")) if "POSITION_" in pos else 0
            
            if is_radiant:
                radiant_heroes.append(hero_id)
                radiant_positions[pos_num] = hero_id
            else:
                dire_heroes.append(hero_id)
                dire_positions[pos_num] = hero_id
        
        if len(radiant_heroes) != 5 or len(dire_heroes) != 5:
            continue
        
        matches.append({
            "match_id": int(match_id),
            "start_time": match.get("startDateTime", 0),
            "radiant_win": 1 if match.get("didRadiantWin", False) else 0,
            "radiant_heroes": radiant_heroes,
            "dire_heroes": dire_heroes,
            "radiant_positions": radiant_positions,
            "dire_positions": dire_positions,
            "bracket": match.get("bracket", 0),
        })
    
    # Sort by time
    matches.sort(key=lambda x: x["start_time"])
    logger.info(f"Loaded {len(matches)} valid matches")
    return matches


def build_hero_position_winrates(matches: List[Dict[str, Any]]) -> Dict[Tuple[int, int], float]:
    """Build hero winrates by position from training data."""
    stats: Dict[Tuple[int, int], Dict[str, int]] = defaultdict(lambda: {"wins": 0, "total": 0})
    
    for m in matches:
        radiant_win = m["radiant_win"]
        
        for pos, hero_id in m["radiant_positions"].items():
            key = (hero_id, pos)
            stats[key]["total"] += 1
            if radiant_win:
                stats[key]["wins"] += 1
        
        for pos, hero_id in m["dire_positions"].items():
            key = (hero_id, pos)
            stats[key]["total"] += 1
            if not radiant_win:
                stats[key]["wins"] += 1
    
    winrates = {}
    for key, s in stats.items():
        if s["total"] >= 30:  # Min sample size
            winrates[key] = s["wins"] / s["total"]
    
    return winrates


def build_hero_winrates(matches: List[Dict[str, Any]]) -> Dict[int, float]:
    """Build overall hero winrates from training data."""
    stats: Dict[int, Dict[str, int]] = defaultdict(lambda: {"wins": 0, "total": 0})
    
    for m in matches:
        radiant_win = m["radiant_win"]
        
        for hero_id in m["radiant_heroes"]:
            stats[hero_id]["total"] += 1
            if radiant_win:
                stats[hero_id]["wins"] += 1
        
        for hero_id in m["dire_heroes"]:
            stats[hero_id]["total"] += 1
            if not radiant_win:
                stats[hero_id]["wins"] += 1
    
    return {h: s["wins"] / s["total"] for h, s in stats.items() if s["total"] >= 50}


def build_synergy_matrix(matches: List[Dict[str, Any]]) -> Dict[str, float]:
    """Build hero synergy from training data."""
    pair_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"wins": 0, "total": 0})
    
    for m in matches:
        radiant_win = m["radiant_win"]
        
        # Radiant pairs
        r_heroes = sorted(m["radiant_heroes"])
        for i in range(len(r_heroes)):
            for j in range(i + 1, len(r_heroes)):
                key = f"{r_heroes[i]}_{r_heroes[j]}"
                pair_stats[key]["total"] += 1
                if radiant_win:
                    pair_stats[key]["wins"] += 1
        
        # Dire pairs
        d_heroes = sorted(m["dire_heroes"])
        for i in range(len(d_heroes)):
            for j in range(i + 1, len(d_heroes)):
                key = f"{d_heroes[i]}_{d_heroes[j]}"
                pair_stats[key]["total"] += 1
                if not radiant_win:
                    pair_stats[key]["wins"] += 1
    
    synergy = {}
    for key, s in pair_stats.items():
        if s["total"] >= 20:
            synergy[key] = s["wins"] / s["total"] - 0.5  # Deviation from 50%
    
    return synergy


def build_counter_matrix(matches: List[Dict[str, Any]]) -> Dict[str, float]:
    """Build hero counter matrix from training data."""
    counter_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"wins": 0, "total": 0})
    
    for m in matches:
        radiant_win = m["radiant_win"]
        
        for r_hero in m["radiant_heroes"]:
            for d_hero in m["dire_heroes"]:
                key = f"{r_hero}_vs_{d_hero}"
                counter_stats[key]["total"] += 1
                if radiant_win:
                    counter_stats[key]["wins"] += 1
    
    counters = {}
    for key, s in counter_stats.items():
        if s["total"] >= 15:
            counters[key] = s["wins"] / s["total"] - 0.5
    
    return counters




class DraftFeatureBuilder:
    """Build draft features for prediction."""
    
    def __init__(
        self,
        hero_winrates: Dict[int, float],
        hero_pos_winrates: Dict[Tuple[int, int], float],
        synergy: Dict[str, float],
        counters: Dict[str, float],
    ):
        self.hero_winrates = hero_winrates
        self.hero_pos_winrates = hero_pos_winrates
        self.synergy = synergy
        self.counters = counters
        
        # Load static hero data
        self.hero_roles = self._load_json("data/hero_roles.json").get("heroes", {})
        self.hero_public_stats = self._load_hero_public_stats()
    
    def _load_json(self, path: str) -> Dict:
        full_path = PROJECT_ROOT / path
        if full_path.exists():
            with open(full_path) as f:
                return json.load(f)
        return {}
    
    def _load_hero_public_stats(self) -> Dict[int, Dict[str, float]]:
        path = PROJECT_ROOT / "data/hero_public_stats.csv"
        if not path.exists():
            return {}
        
        stats = {}
        df = pd.read_csv(path)
        for _, row in df.iterrows():
            hero_id = int(row["hero_id"])
            stats[hero_id] = {
                "aggression": float(row.get("aggression", 0.5)),
                "feed": float(row.get("feed", 0.15)),
                "pace": float(row.get("pace", 2400)),
                "gpm": float(row.get("gpm", 500)),
            }
        return stats
    
    def _get_hero_role(self, hero_id: int, role: str) -> int:
        return self.hero_roles.get(str(hero_id), {}).get(role, 0)
    
    def _get_synergy(self, h1: int, h2: int) -> float:
        key = f"{min(h1, h2)}_{max(h1, h2)}"
        return self.synergy.get(key, 0.0)
    
    def _get_counter(self, hero: int, enemy: int) -> float:
        return self.counters.get(f"{hero}_vs_{enemy}", 0.0)
    
    def build_features(
        self,
        radiant_heroes: List[int],
        dire_heroes: List[int],
        radiant_positions: Dict[int, int],
        dire_positions: Dict[int, int],
    ) -> Dict[str, Any]:
        """Build all draft features."""
        f: Dict[str, Any] = {}
        
        # === Hero IDs (categorical) ===
        for i, h in enumerate(radiant_heroes[:5]):
            f[f"radiant_hero_{i+1}"] = h
        for i, h in enumerate(dire_heroes[:5]):
            f[f"dire_hero_{i+1}"] = h
        
        # === Overall hero winrates ===
        r_wr = [self.hero_winrates.get(h, 0.5) for h in radiant_heroes]
        d_wr = [self.hero_winrates.get(h, 0.5) for h in dire_heroes]
        
        f["radiant_avg_winrate"] = np.mean(r_wr)
        f["dire_avg_winrate"] = np.mean(d_wr)
        f["winrate_diff"] = f["radiant_avg_winrate"] - f["dire_avg_winrate"]
        f["radiant_min_winrate"] = min(r_wr)
        f["dire_min_winrate"] = min(d_wr)
        f["radiant_max_winrate"] = max(r_wr)
        f["dire_max_winrate"] = max(d_wr)
        
        # === Position-based winrates (LATE GAME SOLO) ===
        r_pos_wr = []
        d_pos_wr = []
        
        for pos in range(1, 6):
            r_hero = radiant_positions.get(pos, 0)
            d_hero = dire_positions.get(pos, 0)
            
            r_wr_pos = self.hero_pos_winrates.get((r_hero, pos), 0.5)
            d_wr_pos = self.hero_pos_winrates.get((d_hero, pos), 0.5)
            
            f[f"radiant_pos{pos}_winrate"] = r_wr_pos
            f[f"dire_pos{pos}_winrate"] = d_wr_pos
            f[f"pos{pos}_winrate_diff"] = r_wr_pos - d_wr_pos
            
            r_pos_wr.append(r_wr_pos)
            d_pos_wr.append(d_wr_pos)
        
        f["radiant_avg_pos_winrate"] = np.mean(r_pos_wr)
        f["dire_avg_pos_winrate"] = np.mean(d_pos_wr)
        f["pos_winrate_diff"] = f["radiant_avg_pos_winrate"] - f["dire_avg_pos_winrate"]
        
        # Core positions (1, 2, 3) vs support (4, 5)
        f["radiant_core_pos_wr"] = np.mean(r_pos_wr[:3])
        f["dire_core_pos_wr"] = np.mean(d_pos_wr[:3])
        f["radiant_support_pos_wr"] = np.mean(r_pos_wr[3:])
        f["dire_support_pos_wr"] = np.mean(d_pos_wr[3:])
        f["core_pos_wr_diff"] = f["radiant_core_pos_wr"] - f["dire_core_pos_wr"]
        f["support_pos_wr_diff"] = f["radiant_support_pos_wr"] - f["dire_support_pos_wr"]
        
        # Carry (pos1) advantage
        f["carry_wr_diff"] = r_pos_wr[0] - d_pos_wr[0] if r_pos_wr and d_pos_wr else 0
        
        # === Synergy scores ===
        r_synergy = 0.0
        d_synergy = 0.0
        
        for i in range(len(radiant_heroes)):
            for j in range(i + 1, len(radiant_heroes)):
                r_synergy += self._get_synergy(radiant_heroes[i], radiant_heroes[j])
        
        for i in range(len(dire_heroes)):
            for j in range(i + 1, len(dire_heroes)):
                d_synergy += self._get_synergy(dire_heroes[i], dire_heroes[j])
        
        f["radiant_synergy"] = r_synergy
        f["dire_synergy"] = d_synergy
        f["synergy_diff"] = r_synergy - d_synergy
        
        # === Counter scores ===
        r_counter = 0.0
        d_counter = 0.0
        
        for r_hero in radiant_heroes:
            for d_hero in dire_heroes:
                r_counter += self._get_counter(r_hero, d_hero)
                d_counter += self._get_counter(d_hero, r_hero)
        
        f["radiant_counter_score"] = r_counter
        f["dire_counter_score"] = d_counter
        f["counter_diff"] = r_counter - d_counter
        
        # Max counter advantage
        max_r_counter = 0.0
        max_d_counter = 0.0
        for r_hero in radiant_heroes:
            for d_hero in dire_heroes:
                c = self._get_counter(r_hero, d_hero)
                max_r_counter = max(max_r_counter, c)
                c = self._get_counter(d_hero, r_hero)
                max_d_counter = max(max_d_counter, c)
        
        f["radiant_max_counter"] = max_r_counter
        f["dire_max_counter"] = max_d_counter
        
        # === Role composition ===
        for role in ["Carry", "Support", "Nuker", "Disabler", "Durable", "Escape", "Pusher", "Initiator"]:
            r_count = sum(self._get_hero_role(h, role) for h in radiant_heroes)
            d_count = sum(self._get_hero_role(h, role) for h in dire_heroes)
            f[f"radiant_{role.lower()}_count"] = r_count
            f[f"dire_{role.lower()}_count"] = d_count
            f[f"{role.lower()}_diff"] = r_count - d_count
        
        # === Aggression/Pace from public stats ===
        r_agg = [self.hero_public_stats.get(h, {}).get("aggression", 0.5) for h in radiant_heroes]
        d_agg = [self.hero_public_stats.get(h, {}).get("aggression", 0.5) for h in dire_heroes]
        
        f["radiant_avg_aggression"] = np.mean(r_agg)
        f["dire_avg_aggression"] = np.mean(d_agg)
        f["aggression_diff"] = f["radiant_avg_aggression"] - f["dire_avg_aggression"]
        
        r_pace = [self.hero_public_stats.get(h, {}).get("pace", 2400) for h in radiant_heroes]
        d_pace = [self.hero_public_stats.get(h, {}).get("pace", 2400) for h in dire_heroes]
        
        f["radiant_avg_pace"] = np.mean(r_pace)
        f["dire_avg_pace"] = np.mean(d_pace)
        f["pace_diff"] = f["radiant_avg_pace"] - f["dire_avg_pace"]
        
        # === Combined draft score ===
        f["radiant_draft_score"] = (
            f["radiant_avg_winrate"] * 2 +
            f["radiant_avg_pos_winrate"] * 3 +
            f["radiant_synergy"] * 5 +
            f["radiant_counter_score"] * 5
        )
        f["dire_draft_score"] = (
            f["dire_avg_winrate"] * 2 +
            f["dire_avg_pos_winrate"] * 3 +
            f["dire_synergy"] * 5 +
            f["dire_counter_score"] * 5
        )
        f["draft_score_diff"] = f["radiant_draft_score"] - f["dire_draft_score"]
        
        return f



def generate_features(
    matches: List[Dict[str, Any]],
    builder: DraftFeatureBuilder,
) -> Tuple[pd.DataFrame, np.ndarray]:
    """Generate features for all matches."""
    features_list = []
    labels = []
    
    for m in matches:
        f = builder.build_features(
            radiant_heroes=m["radiant_heroes"],
            dire_heroes=m["dire_heroes"],
            radiant_positions=m["radiant_positions"],
            dire_positions=m["dire_positions"],
        )
        features_list.append(f)
        labels.append(m["radiant_win"])
    
    return pd.DataFrame(features_list), np.array(labels)


def find_optimal_threshold(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    target_winrate: float = 0.75,
    min_coverage: float = 0.30,
) -> Tuple[float, float, float, float]:
    """Find threshold that achieves target winrate with max coverage."""
    best_threshold = 0.5
    best_coverage = 0.0
    best_winrate = 0.5
    
    for threshold in np.arange(0.50, 0.80, 0.01):
        # Predictions where we're confident
        confident_mask = (y_proba >= threshold) | (y_proba <= (1 - threshold))
        
        if confident_mask.sum() == 0:
            continue
        
        coverage = confident_mask.mean()
        
        # Predictions
        preds = (y_proba >= 0.5).astype(int)
        confident_preds = preds[confident_mask]
        confident_true = y_true[confident_mask]
        
        winrate = (confident_preds == confident_true).mean()
        
        if winrate >= target_winrate and coverage >= min_coverage:
            if coverage > best_coverage:
                best_threshold = threshold
                best_coverage = coverage
                best_winrate = winrate
        elif coverage >= min_coverage and winrate > best_winrate:
            best_threshold = threshold
            best_coverage = coverage
            best_winrate = winrate
    
    return best_threshold, best_winrate, best_coverage, 1 - best_threshold


def evaluate_with_threshold(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    threshold: float,
) -> Dict[str, float]:
    """Evaluate model with confidence threshold."""
    # Confident predictions
    confident_high = y_proba >= threshold
    confident_low = y_proba <= (1 - threshold)
    confident_mask = confident_high | confident_low
    
    coverage = confident_mask.mean()
    
    if confident_mask.sum() == 0:
        return {"coverage": 0, "winrate": 0, "auc": 0}
    
    preds = (y_proba >= 0.5).astype(int)
    confident_preds = preds[confident_mask]
    confident_true = y_true[confident_mask]
    
    winrate = (confident_preds == confident_true).mean()
    
    # Overall AUC
    auc = roc_auc_score(y_true, y_proba)
    
    return {
        "coverage": coverage,
        "winrate": winrate,
        "auc": auc,
        "confident_samples": int(confident_mask.sum()),
        "total_samples": len(y_true),
    }


def train_model(
    X_train: pd.DataFrame,
    y_train: np.ndarray,
    X_test: pd.DataFrame,
    y_test: np.ndarray,
    cat_features: List[str],
    iterations: int = 2000,
    depth: int = 6,
    learning_rate: float = 0.03,
    l2_leaf_reg: float = 5.0,
    seed: int = 42,
) -> Tuple[CatBoostClassifier, Dict[str, Any]]:
    """Train CatBoost model."""
    feature_cols = list(X_train.columns)
    cat_indices = [feature_cols.index(c) for c in cat_features if c in feature_cols]
    
    # Prepare data
    X_train_prep = X_train.copy()
    X_test_prep = X_test.copy()
    
    for col in feature_cols:
        if col in cat_features:
            X_train_prep[col] = X_train_prep[col].fillna(-1).astype(int).astype(str)
            X_test_prep[col] = X_test_prep[col].fillna(-1).astype(int).astype(str)
        else:
            X_train_prep[col] = pd.to_numeric(X_train_prep[col], errors="coerce").fillna(0.0)
            X_test_prep[col] = pd.to_numeric(X_test_prep[col], errors="coerce").fillna(0.0)
    
    model = CatBoostClassifier(
        iterations=iterations,
        learning_rate=learning_rate,
        depth=depth,
        l2_leaf_reg=l2_leaf_reg,
        loss_function="Logloss",
        eval_metric="AUC",
        random_seed=seed,
        verbose=200,
        early_stopping_rounds=200,
    )
    
    train_pool = Pool(X_train_prep, y_train, cat_features=cat_indices)
    test_pool = Pool(X_test_prep, y_test, cat_features=cat_indices)
    
    model.fit(train_pool, eval_set=test_pool, use_best_model=True)
    
    # Evaluate
    y_proba = model.predict_proba(X_test_prep)[:, 1]
    
    metrics = {
        "auc": float(roc_auc_score(y_test, y_proba)),
        "accuracy": float(accuracy_score(y_test, (y_proba >= 0.5).astype(int))),
        "logloss": float(log_loss(y_test, y_proba)),
    }
    
    return model, metrics, y_proba


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="bets_data/analise_pub_matches/extracted_100k_matches.json")
    ap.add_argument("--output-dir", default="ml-models")
    ap.add_argument("--test-size", type=int, default=10000)
    ap.add_argument("--iterations", type=int, default=2000)
    ap.add_argument("--depth", type=int, default=6)
    ap.add_argument("--learning-rate", type=float, default=0.03)
    ap.add_argument("--l2", type=float, default=5.0)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--target-winrate", type=float, default=0.75)
    ap.add_argument("--min-coverage", type=float, default=0.30)
    args = ap.parse_args()
    
    # Load data
    matches = load_pub_matches(args.data)
    
    if len(matches) < args.test_size + 1000:
        logger.error("Not enough matches")
        return 1
    
    # Time-based split
    train_matches = matches[:-args.test_size]
    test_matches = matches[-args.test_size:]
    
    logger.info(f"Train: {len(train_matches)}, Test: {len(test_matches)}")
    
    # Build statistics from training data only (no leakage!)
    logger.info("Building hero statistics from training data...")
    hero_winrates = build_hero_winrates(train_matches)
    hero_pos_winrates = build_hero_position_winrates(train_matches)
    synergy = build_synergy_matrix(train_matches)
    counters = build_counter_matrix(train_matches)
    
    logger.info(f"Hero winrates: {len(hero_winrates)}")
    logger.info(f"Hero-position winrates: {len(hero_pos_winrates)}")
    logger.info(f"Synergy pairs: {len(synergy)}")
    logger.info(f"Counter pairs: {len(counters)}")
    
    # Build feature builder
    builder = DraftFeatureBuilder(
        hero_winrates=hero_winrates,
        hero_pos_winrates=hero_pos_winrates,
        synergy=synergy,
        counters=counters,
    )
    
    # Generate features
    logger.info("Generating features...")
    X_train, y_train = generate_features(train_matches, builder)
    X_test, y_test = generate_features(test_matches, builder)
    
    logger.info(f"Features: {len(X_train.columns)}")
    
    # Categorical features
    cat_features = [f"radiant_hero_{i}" for i in range(1, 6)] + [f"dire_hero_{i}" for i in range(1, 6)]
    
    # Train model
    logger.info("Training model...")
    model, metrics, y_proba = train_model(
        X_train=X_train,
        y_train=y_train,
        X_test=X_test,
        y_test=y_test,
        cat_features=cat_features,
        iterations=args.iterations,
        depth=args.depth,
        learning_rate=args.learning_rate,
        l2_leaf_reg=args.l2,
        seed=args.seed,
    )
    
    logger.info(f"Base metrics: AUC={metrics['auc']:.4f}, ACC={metrics['accuracy']:.4f}")
    
    # Find optimal threshold
    logger.info("Finding optimal confidence threshold...")
    threshold, winrate, coverage, low_threshold = find_optimal_threshold(
        y_test, y_proba,
        target_winrate=args.target_winrate,
        min_coverage=args.min_coverage,
    )
    
    logger.info(f"Optimal threshold: {threshold:.3f}")
    logger.info(f"Winrate: {winrate:.2%}, Coverage: {coverage:.2%}")
    
    # Detailed evaluation
    eval_results = evaluate_with_threshold(y_test, y_proba, threshold)
    logger.info(f"Evaluation: {eval_results}")
    
    # Save model and meta
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    model_path = output_dir / "draft_high_confidence.cbm"
    meta_path = output_dir / "draft_high_confidence_meta.json"
    stats_path = output_dir / "draft_high_confidence_stats.json"
    
    model.save_model(str(model_path))
    
    feature_cols = list(X_train.columns)
    cat_indices = [feature_cols.index(c) for c in cat_features if c in feature_cols]
    
    meta = {
        "feature_cols": feature_cols,
        "cat_features": cat_features,
        "cat_indices": cat_indices,
        "confidence_threshold": threshold,
        "low_threshold": low_threshold,
        "train_size": len(train_matches),
        "test_size": len(test_matches),
        "metrics": {
            "auc": metrics["auc"],
            "accuracy": metrics["accuracy"],
            "logloss": metrics["logloss"],
            "winrate_at_threshold": winrate,
            "coverage_at_threshold": coverage,
        },
        "training_params": {
            "iterations": args.iterations,
            "depth": args.depth,
            "learning_rate": args.learning_rate,
            "l2_leaf_reg": args.l2,
        },
    }
    
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    
    # Save statistics for inference
    stats = {
        "hero_winrates": {str(k): v for k, v in hero_winrates.items()},
        "hero_pos_winrates": {f"{h}_{p}": v for (h, p), v in hero_pos_winrates.items()},
        "synergy": synergy,
        "counters": counters,
    }
    
    with open(stats_path, "w") as f:
        json.dump(stats, f)
    
    logger.info(f"Saved model: {model_path}")
    logger.info(f"Saved meta: {meta_path}")
    logger.info(f"Saved stats: {stats_path}")
    
    # Print feature importances
    importances = model.get_feature_importance()
    feature_imp = sorted(zip(feature_cols, importances), key=lambda x: -x[1])
    
    logger.info("\nTop 20 features:")
    for feat, imp in feature_imp[:20]:
        logger.info(f"  {feat}: {imp:.2f}")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
