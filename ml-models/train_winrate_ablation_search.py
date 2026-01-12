#!/usr/bin/env python3
"""
Winrate feature-set ablation search (train-time).

Purpose
-------
Search for the best *stable* pre-game feature blocks to maximize:
- Winrate model quality (AUC / LogLoss / Accuracy)
- Coverage at a fixed confidence threshold (not "tuning threshold", just measuring tradeoff)

Key principles
--------------
1) Training features MUST match production features to avoid offline/online skew.
2) Feature blocks must be *stable* (high availability pre-game).
3) Time-based split (by start_time when available; else match_id as proxy).

This script:
- Loads data/pro_matches_enriched.csv (for hero ids, team ids, labels, timestamps)
- Uses src.live_predictor.LivePredictor to compute candidate winrate features:
  - A base stable core (heroes + deterministic draft stats)
  - Optional stable blocks (ratings, tiers, lane matrix, stratz, counters, etc.)
- Trains CatBoostClassifier for each feature-set variant and evaluates:
  - AUC, LogLoss, Accuracy on test
  - Bet coverage / bet accuracy at threshold t and symmetric (t and 1-t)
- Writes:
  - ml-models/winrate_ablation_results.json (ranked results)
  - optionally best model artifacts if --save-best is set

Usage
-----
./venv_catboost/bin/python ml-models/train_winrate_ablation_search.py \
  --data data/pro_matches_enriched.csv \
  --test-size 500 \
  --threshold 0.65 \
  --max-combos 80 \
  --save-best

Notes
-----
- Player/DNA/account_id features are intentionally excluded (coverage + temporal leakage risk).
- Team IDs can be included as categorical in some variants; this may increase memorization risk.
  We include both variants (with/without team_id cats) to measure actual generalization on time split.
"""

from __future__ import annotations

import argparse
import itertools
import json
import logging
import math
import os
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score

# This file lives in: ingame/ml-models/train_winrate_ablation_search.py
# The directory that contains `src/` is: ingame/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.live_predictor import LivePredictor  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("winrate_ablation_search")


# -----------------------------
# Data helpers
# -----------------------------


def _coerce_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, np.integer)):
            return int(v)
        if isinstance(v, (float, np.floating)) and (math.isnan(v) or math.isinf(v)):
            return None
        s = str(v).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def _coerce_bool01(v: Any) -> int:
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, np.integer)):
        return 1 if int(v) != 0 else 0
    if isinstance(v, (float, np.floating)):
        return 1 if float(v) >= 0.5 else 0
    s = str(v).strip().lower()
    if s in ("true", "1", "yes", "y", "t"):
        return 1
    return 0


def load_dataset(path: str) -> Tuple[pd.DataFrame, str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"dataset not found: {path}")

    df = pd.read_csv(path)

    required = {"match_id", "radiant_win"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"dataset missing required columns: {sorted(missing)}")

    # Ensure hero columns exist
    for side in ("radiant", "dire"):
        for i in range(1, 6):
            col = f"{side}_hero_{i}"
            if col not in df.columns:
                raise ValueError(f"dataset missing hero column: {col}")

    # Time key
    if "start_time" in df.columns:
        df["__sort_key"] = pd.to_numeric(df["start_time"], errors="coerce")
        time_key = "start_time"
    else:
        df["__sort_key"] = pd.to_numeric(df["match_id"], errors="coerce")
        time_key = "match_id"

    df = df.dropna(subset=["__sort_key"]).copy()
    df = df.sort_values("__sort_key").reset_index(drop=True)

    df["match_id"] = df["match_id"].apply(_coerce_int)
    df = df[df["match_id"].notna()].copy()
    df["match_id"] = df["match_id"].astype("int64")

    df["radiant_win"] = df["radiant_win"].apply(_coerce_bool01).astype(int)

    logger.info(f"Loaded {len(df)} rows from {path} (time key={time_key})")
    return df, time_key


def split_time_based(df: pd.DataFrame, test_size: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if len(df) <= test_size:
        raise ValueError("test_size is >= dataset size")
    train = df.iloc[: -test_size].copy()
    test = df.iloc[-test_size :].copy()
    logger.info(f"Split: train={len(train)} test={len(test)} (test_size={test_size})")
    return train, test


# -----------------------------
# Feature blocks
# -----------------------------


@dataclass(frozen=True)
class FeatureBlock:
    name: str
    include_team_ids: bool = True
    include_ratings: bool = True
    include_tiers: bool = True
    include_lane_matrix: bool = True
    include_stratz: bool = True
    include_pub_counters: bool = True
    include_per_hero_stats: bool = True
    include_synergy: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _base_block() -> FeatureBlock:
    # "Stable core": draft + deterministic stats computed from static JSONs.
    # Team ids/ratings/tiers can be toggled by variants.
    return FeatureBlock(
        name="base",
        include_team_ids=False,
        include_ratings=False,
        include_tiers=False,
        include_lane_matrix=False,
        include_stratz=False,
        include_pub_counters=False,
        include_per_hero_stats=True,
        include_synergy=True,
    )


def build_features_for_row(
    predictor: LivePredictor,
    row: pd.Series,
    block: FeatureBlock,
) -> Dict[str, Any]:
    radiant_ids = [int(row[f"radiant_hero_{i}"]) for i in range(1, 6)]
    dire_ids = [int(row[f"dire_hero_{i}"]) for i in range(1, 6)]

    r_tid: Optional[int] = None
    d_tid: Optional[int] = None
    if block.include_team_ids and "radiant_team_id" in row.index and "dire_team_id" in row.index:
        r_tid = _coerce_int(row.get("radiant_team_id"))
        d_tid = _coerce_int(row.get("dire_team_id"))

    # Start from stable winrate builder (which itself calls build_features)
    f = predictor.build_winrate_features(
        radiant_ids=radiant_ids,
        dire_ids=dire_ids,
        radiant_team_id=r_tid,
        dire_team_id=d_tid,
    )

    # Now ablate by removing groups, not by building different codepaths.
    # This keeps a single source of truth for computations.
    #
    # We implement block toggles as *feature key filtering* based on prefixes/known keys.
    # This is intentionally coarse but stable; refinement can come later.
    drop_keys: List[str] = []

    if not block.include_ratings:
        drop_keys += [
            "radiant_glicko_rating",
            "dire_glicko_rating",
            "glicko_rating_diff",
            "radiant_glicko_rd",
            "dire_glicko_rd",
            "glicko_rating_win_prob",
            "both_teams_reliable",
            "glicko_rating_diff_abs",
            "glicko_rd_sum",
            "glicko_reliability_score",
        ]

    if not block.include_tiers:
        drop_keys += [
            "radiant_tier",
            "dire_tier",
            "avg_tier",
            "tier_diff",
            "both_tier1",
            "tier1_vs_other",
            "both_tier2_plus",
            "tier_advantage_radiant",
            "tier_advantage_dire",
            "tier_equal",
        ]

    if not block.include_lane_matrix:
        # Built from hero_lane_matchups.json
        for k in [
            "radiant_lane_advantage",
            "dire_lane_advantage",
            "lane_advantage_diff",
            "total_lane_volatility",
            "lane_stomp_potential",
            "lane_adv_x_blood",
            "lane_stomp_x_aggression",
        ]:
            drop_keys.append(k)

    if not block.include_stratz:
        for k in [
            "radiant_stratz_matchup",
            "dire_stratz_matchup",
            "stratz_matchup_diff",
            "radiant_stratz_synergy",
            "dire_stratz_synergy",
            "stratz_synergy_diff",
            "radiant_stratz_draft",
            "dire_stratz_draft",
            "stratz_draft_diff",
+        ]:
+            drop_keys.append(k)
+
+    if not block.include_pub_counters:
+        for k in [
+            "radiant_early_counter_pub",
+            "dire_early_counter_pub",
+            "early_counter_diff_pub",
+            "radiant_late_counter_pub",
+            "dire_late_counter_pub",
+            "late_counter_diff_pub",
+            "radiant_early_synergy_pub",
+            "dire_early_synergy_pub",
+            "combined_early_synergy_pub",
+            "early_synergy_diff_pub",
+            "radiant_late_synergy_pub",
+            "dire_late_synergy_pub",
+            "combined_late_synergy_pub",
+            "late_synergy_diff_pub",
+            "radiant_trio_synergy_early",
+            "dire_trio_synergy_early",
+            "combined_trio_synergy_early",
+            "trio_synergy_diff_early",
+            "radiant_trio_synergy_late",
+            "dire_trio_synergy_late",
+            "combined_trio_synergy_late",
+            "trio_synergy_diff_late",
+            "radiant_2v1_early",
+            "dire_2v1_early",
+            "counter_2v1_diff_early",
+            "radiant_2v1_late",
+            "dire_2v1_late",
+            "counter_2v1_diff_late",
+            "radiant_1v2_early",
+            "dire_1v2_early",
+            "counter_1v2_diff_early",
+            "radiant_1v2_late",
+            "dire_1v2_late",
+            "counter_1v2_diff_late",
+            "radiant_pair_counter_early",
+            "dire_pair_counter_early",
+            "pair_counter_diff_early",
+            "radiant_pair_counter_late",
+            "dire_pair_counter_late",
+            "pair_counter_diff_late",
+        ]:
+            drop_keys.append(k)
+
+    if not block.include_synergy:
+        for k in [
+            "radiant_draft_synergy",
+            "dire_draft_synergy",
+            "total_draft_synergy",
+            "draft_synergy_diff",
+            "radiant_early_synergy",
+            "dire_early_synergy",
+            "combined_early_synergy",
+            "early_synergy_diff",
+            "radiant_late_synergy",
+            "dire_late_synergy",
+            "combined_late_synergy",
+            "late_synergy_diff",
+        ]:
+            drop_keys.append(k)
+
+    if not block.include_per_hero_stats:
+        # Drop per-hero slot expansions (hundreds of keys)
+        for k in list(f.keys()):
+            if k.startswith("radiant_hero_") and k not in {
+                "radiant_hero_1",
+                "radiant_hero_2",
+                "radiant_hero_3",
+                "radiant_hero_4",
+                "radiant_hero_5",
+            }:
+                drop_keys.append(k)
+            if k.startswith("dire_hero_") and k not in {
+                "dire_hero_1",
+                "dire_hero_2",
+                "dire_hero_3",
+                "dire_hero_4",
+                "dire_hero_5",
+            }:
+                drop_keys.append(k)
+
+    # Apply drops
+    for k in drop_keys:
+        if k in f:
+            del f[k]
+
+    return f
+
+
+def generate_feature_frame(
+    predictor: LivePredictor,
+    df: pd.DataFrame,
+    block: FeatureBlock,
+) -> pd.DataFrame:
+    feats: List[Dict[str, Any]] = []
+    for _, row in df.iterrows():
+        feats.append(build_features_for_row(predictor, row, block))
+    X = pd.DataFrame(feats)
+    return X
+
+
+# -----------------------------
+# Training & evaluation
+# -----------------------------
+
+
+def detect_cat_features(feature_cols: List[str], include_team_ids: bool) -> List[str]:
+    cat: List[str] = []
+    for side in ("radiant", "dire"):
+        for pos in range(1, 6):
+            cat.append(f"{side}_hero_{pos}")
+    if include_team_ids:
+        cat += ["radiant_team_id", "dire_team_id"]
+    return [c for c in cat if c in feature_cols]
+
+
+def prepare_matrix(
+    X_df: pd.DataFrame,
+    feature_cols: List[str],
+    cat_features: List[str],
+) -> pd.DataFrame:
+    X = X_df[feature_cols].copy()
+    cat_set = set(cat_features)
+    for col in feature_cols:
+        if col in cat_set:
+            X[col] = (
+                pd.to_numeric(X[col], errors="coerce")
+                .fillna(-1)
+                .astype(int)
+                .astype(str)
+            )
+        else:
+            X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0.0)
+    return X
+
+
+@dataclass
+class EvalResult:
+    block_name: str
+    config: Dict[str, Any]
+    features: int
+    cat_features: int
+    train_size: int
+    test_size: int
+    auc: float
+    logloss: float
+    acc: float
+    threshold: float
+    bet_coverage: float
+    bet_acc: Optional[float]
+    bet_count: int
+    best_iteration: Optional[int]
+
+
+def evaluate_threshold(
+    proba: np.ndarray,
+    y_true: np.ndarray,
+    threshold: float,
+) -> Tuple[float, Optional[float], int]:
+    mask = (proba >= threshold) | (proba <= (1.0 - threshold))
+    bet_count = int(mask.sum())
+    coverage = float(mask.mean()) if len(proba) else 0.0
+    if bet_count == 0:
+        return coverage, None, 0
+    pred = (proba[mask] >= 0.5).astype(int)
+    bet_acc = float(accuracy_score(y_true[mask], pred))
+    return coverage, bet_acc, bet_count
+
+
+def train_and_eval(
+    X_train_df: pd.DataFrame,
+    y_train: np.ndarray,
+    X_test_df: pd.DataFrame,
+    y_test: np.ndarray,
+    include_team_ids_as_cat: bool,
+    threshold: float,
+    seed: int,
+    iterations: int,
+    depth: int,
+    learning_rate: float,
+    l2_leaf_reg: float,
+) -> Tuple[CatBoostClassifier, EvalResult]:
+    # align columns by intersection for stability
+    feature_cols = sorted(list(set(X_train_df.columns) & set(X_test_df.columns)))
+    if not feature_cols:
+        raise RuntimeError("No overlapping feature columns between train and test")
+
+    cat_features = detect_cat_features(feature_cols, include_team_ids_as_cat)
+    cat_indices = [feature_cols.index(c) for c in cat_features if c in feature_cols]
+
+    X_train = prepare_matrix(X_train_df, feature_cols, cat_features)
+    X_test = prepare_matrix(X_test_df, feature_cols, cat_features)
+
+    model = CatBoostClassifier(
+        iterations=iterations,
+        learning_rate=learning_rate,
+        depth=depth,
+        l2_leaf_reg=l2_leaf_reg,
+        loss_function="Logloss",
+        eval_metric="AUC",
+        random_seed=seed,
+        verbose=False,
+        early_stopping_rounds=300,
+    )
+
+    train_pool = Pool(X_train, y_train, cat_features=cat_indices)
+    test_pool = Pool(X_test, y_test, cat_features=cat_indices)
+    model.fit(train_pool, eval_set=test_pool, use_best_model=True)
+
+    proba = model.predict_proba(X_test)[:, 1]
+    auc = float(roc_auc_score(y_test, proba))
+    ll = float(log_loss(y_test, proba, labels=[0, 1]))
+    acc = float(accuracy_score(y_test, (proba >= 0.5).astype(int)))
+    cov, bet_acc, bet_cnt = evaluate_threshold(proba, y_test, threshold=threshold)
+
+    best_iter = None
+    try:
+        best_iter = int(getattr(model, "best_iteration_", None))
+    except Exception:
+        best_iter = None
+
+    result = EvalResult(
+        block_name="",
+        config={},
+        features=len(feature_cols),
+        cat_features=len(cat_features),
+        train_size=int(len(X_train_df)),
+        test_size=int(len(X_test_df)),
+        auc=auc,
+        logloss=ll,
+        acc=acc,
+        threshold=float(threshold),
+        bet_coverage=float(cov),
+        bet_acc=bet_acc,
+        bet_count=bet_cnt,
+        best_iteration=best_iter,
+    )
+
+    return model, result
+
+
+# -----------------------------
+# Search space
+# -----------------------------
+
+
+def build_search_space() -> List[FeatureBlock]:
+    """
+    Construct a reasonable search space over stable blocks.
+    We avoid exhaustive 2^N over too many booleans by enumerating curated variants.
+    """
+    base = _base_block()
+
+    variants: List[FeatureBlock] = []
+
+    # Core + ratings/tiers (with and without team_id)
+    variants.append(
+        FeatureBlock(
+            name="core_only",
+            include_team_ids=False,
+            include_ratings=False,
+            include_tiers=False,
+            include_lane_matrix=False,
+            include_stratz=False,
+            include_pub_counters=False,
+            include_per_hero_stats=True,
+            include_synergy=True,
+        )
+    )
+    variants.append(
+        FeatureBlock(
+            name="core+ratings+tiers_no_teamid",
+            include_team_ids=False,
+            include_ratings=True,
+            include_tiers=True,
+            include_lane_matrix=False,
+            include_stratz=False,
+            include_pub_counters=False,
+            include_per_hero_stats=True,
+            include_synergy=True,
+        )
+    )
+    variants.append(
+        FeatureBlock(
+            name="core+ratings+tiers_teamid",
+            include_team_ids=True,
+            include_ratings=True,
+            include_tiers=True,
+            include_lane_matrix=False,
+            include_stratz=False,
+            include_pub_counters=False,
+            include_per_hero_stats=True,
+            include_synergy=True,
+        )
+    )
+
+    # Add lane matrix
+    variants.append(
+        FeatureBlock(
+            name="core+ratings+tiers+lane",
+            include_team_ids=True,
+            include_ratings=True,
+            include_tiers=True,
+            include_lane_matrix=True,
+            include_stratz=False,
+            include_pub_counters=False,
+            include_per_hero_stats=True,
+            include_synergy=True,
+        )
+    )
+
+    # Add Stratz
+    variants.append(
+        FeatureBlock(
+            name="core+ratings+tiers+stratz",
+            include_team_ids=True,
+            include_ratings=True,
+            include_tiers=True,
+            include_lane_matrix=False,
+            include_stratz=True,
+            include_pub_counters=False,
+            include_per_hero_stats=True,
+            include_synergy=True,
+        )
+    )
+
+    # Add public counters
+    variants.append(
+        FeatureBlock(
+            name="core+ratings+tiers+pubcounters",
+            include_team_ids=True,
+            include_ratings=True,
+            include_tiers=True,
+            include_lane_matrix=False,
+            include_stratz=False,
+            include_pub_counters=True,
+            include_per_hero_stats=True,
+            include_synergy=True,
+        )
+    )
+
+    # Combine lane + stratz + pub counters
+    variants.append(
+        FeatureBlock(
+            name="core+ratings+tiers+lane+stratz+pubcounters",
+            include_team_ids=True,
+            include_ratings=True,
+            include_tiers=True,
+            include_lane_matrix=True,
+            include_stratz=True,
+            include_pub_counters=True,
+            include_per_hero_stats=True,
+            include_synergy=True,
+        )
+    )
+
+    # Reduce per-hero stats (to test generalization/overfit)
+    variants.append(
+        FeatureBlock(
+            name="core+ratings+tiers_no_per_hero",
+            include_team_ids=True,
+            include_ratings=True,
+            include_tiers=True,
+            include_lane_matrix=False,
+            include_stratz=False,
+            include_pub_counters=False,
+            include_per_hero_stats=False,
+            include_synergy=True,
+        )
+    )
+
+    # Minimal: heroes only (cat) without any engineered stats (hard baseline)
+    variants.append(
+        FeatureBlock(
+            name="heroes_only",
+            include_team_ids=True,
+            include_ratings=False,
+            include_tiers=False,
+            include_lane_matrix=False,
+            include_stratz=False,
+            include_pub_counters=False,
+            include_per_hero_stats=False,
+            include_synergy=False,
+        )
+    )
+
+    # De-dup by name just in case
+    uniq: Dict[str, FeatureBlock] = {v.name: v for v in variants}
+    return list(uniq.values())
+
+
+# -----------------------------
+# Main
+# -----------------------------
+
+
+def save_json(path: Path, obj: Any) -> None:
+    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
+
+
+def main() -> int:
+    ap = argparse.ArgumentParser()
+    ap.add_argument("--data", default="data/pro_matches_enriched.csv")
+    ap.add_argument("--test-size", type=int, default=500)
+    ap.add_argument("--threshold", type=float, default=0.65)
+    ap.add_argument("--seed", type=int, default=42)
+    ap.add_argument("--iterations", type=int, default=4000)
+    ap.add_argument("--depth", type=int, default=6)
+    ap.add_argument("--learning-rate", type=float, default=0.02)
+    ap.add_argument("--l2", type=float, default=6.0)
+    ap.add_argument("--max-combos", type=int, default=9999, help="Max variants to run")
+    ap.add_argument("--save-best", action="store_true", help="Save best model artifacts")
+    ap.add_argument(
+        "--out-json",
+        default="ml-models/winrate_ablation_results.json",
+        help="Where to write ranked results (relative to project root)",
+    )
+    args = ap.parse_args()
+
+    df, time_key = load_dataset(args.data)
+    df_train, df_test = split_time_based(df, test_size=args.test_size)
+
+    y_train = df_train["radiant_win"].astype(int).values
+    y_test = df_test["radiant_win"].astype(int).values
+
+    predictor = LivePredictor()
+
+    variants = build_search_space()
+    if args.max_combos and len(variants) > args.max_combos:
+        variants = variants[: args.max_combos]
+
+    logger.info(f"Running {len(variants)} ablation variants...")
+
+    results: List[Dict[str, Any]] = []
+    best = None
+    best_model: Optional[CatBoostClassifier] = None
+    best_meta: Optional[Dict[str, Any]] = None
+
+    for idx, block in enumerate(variants, start=1):
+        logger.info(f"[{idx}/{len(variants)}] Variant: {block.name}")
+
+        # Generate feature frames
+        X_train_df = generate_feature_frame(predictor, df_train, block)
+        X_test_df = generate_feature_frame(predictor, df_test, block)
+
+        # Determine whether to treat team_id columns as categorical
+        include_team_ids_as_cat = bool(block.include_team_ids)
+
+        try:
+            model, er = train_and_eval(
+                X_train_df=X_train_df,
+                y_train=y_train,
+                X_test_df=X_test_df,
+                y_test=y_test,
+                include_team_ids_as_cat=include_team_ids_as_cat,
+                threshold=float(args.threshold),
+                seed=int(args.seed),
+                iterations=int(args.iterations),
+                depth=int(args.depth),
+                learning_rate=float(args.learning_rate),
+                l2_leaf_reg=float(args.l2),
+            )
+        except Exception as e:
+            logger.warning(f"Variant failed: {block.name}: {e}")
+            results.append(
+                {
+                    "block": block.name,
+                    "config": block.to_dict(),
+                    "status": "failed",
+                    "error": str(e),
+                }
+            )
+            continue
+
+        er.block_name = block.name
+        er.config = block.to_dict()
+        row = asdict(er)
+        row["status"] = "ok"
+        results.append(row)
+
+        # Primary objective: AUC, secondary: logloss, tertiary: coverage
+        # (Coverage helps; but we prioritize predictive quality first.)
+        score_tuple = (er.auc, -er.logloss, er.bet_coverage)
+        if best is None or score_tuple > best["score_tuple"]:
+            best = {"score_tuple": score_tuple, "row": row}
+            best_model = model
+            best_meta = {
+                "feature_block": block.to_dict(),
+                "time_key": time_key,
+                "threshold_eval": float(args.threshold),
+            }
+
+        logger.info(
+            f"  AUC={er.auc:.4f} LogLoss={er.logloss:.4f} Acc={er.acc:.4f} "
+            f"Coverage@{args.threshold:.2f}={er.bet_coverage:.3f} BetAcc={er.bet_acc}"
+        )
+
+    # Rank results
+    ok_results = [r for r in results if r.get("status") == "ok"]
+    ok_results_sorted = sorted(
+        ok_results,
+        key=lambda r: (r.get("auc", 0.0), -r.get("logloss", 1e9), r.get("bet_coverage", 0.0)),
+        reverse=True,
+    )
+
+    out = {
+        "data": args.data,
+        "time_key": time_key,
+        "train_size": int(len(df_train)),
+        "test_size": int(len(df_test)),
+        "threshold_eval": float(args.threshold),
+        "params": {
+            "iterations": int(args.iterations),
+            "depth": int(args.depth),
+            "learning_rate": float(args.learning_rate),
+            "l2_leaf_reg": float(args.l2),
+            "seed": int(args.seed),
+        },
+        "best": best["row"] if best else None,
+        "results": ok_results_sorted,
+        "failed": [r for r in results if r.get("status") != "ok"],
+    }
+
+    out_path = PROJECT_ROOT / args.out_json
+    out_path.parent.mkdir(parents=True, exist_ok=True)
+    save_json(out_path, out)
+    logger.info(f"Saved results to {out_path}")
+
+    # Save best model artifacts optionally
+    if args.save_best and best_model is not None and best is not None:
+        models_dir = PROJECT_ROOT / "ml-models"
+        models_dir.mkdir(parents=True, exist_ok=True)
+        model_path = models_dir / "winrate_classifier_best_ablation.cbm"
+        meta_path = models_dir / "winrate_classifier_best_ablation_meta.json"
+        best_model.save_model(str(model_path))
+
+        # meta: include selected feature cols and cat features if possible
+        # We cannot easily retrieve feature list from CatBoost model reliably across versions,
+        # so we store the result record and the feature-block definition.
+        meta = {
+            "best_result": best["row"],
+            "best_meta": best_meta,
+        }
+        save_json(meta_path, meta)
+        logger.info(f"Saved best model to {model_path}")
+        logger.info(f"Saved best meta  to {meta_path}")
+
+    # Print top-5 summary
+    topn = ok_results_sorted[:5]
+    if topn:
+        logger.info("Top variants:")
+        for i, r in enumerate(topn, start=1):
+            logger.info(
+                f"{i}. {r['block_name']} AUC={r['auc']:.4f} LogLoss={r['logloss']:.4f} "
+                f"Acc={r['acc']:.4f} Coverage={r['bet_coverage']:.3f} BetAcc={r['bet_acc']}"
+            )
+    else:
+        logger.warning("No successful variants.")
+
+    return 0
+
+
+if __name__ == "__main__":
+    raise SystemExit(main())
