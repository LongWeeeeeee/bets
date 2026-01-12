#!/usr/bin/env python3
"""
Train Winrate (radiant_win) classifier v3.

Goals:
- Use ONLY features that are realistically available pre-game (anti-leakage).
- Generate features through src.live_predictor.LivePredictor.build_features() so training matches production.
- Use a time-based split (by start_time when present, else match_id as a proxy) to reduce temporal leakage.
- Export:
  - ml-models/winrate_classifier_v3.cbm
  - ml-models/winrate_classifier_v3_meta.json

Notes:
- This script intentionally avoids using raw columns from pro_matches_enriched.csv as features.
  It uses the CSV only as a source for hero IDs, team IDs, and the target label (radiant_win).
- Team IDs are treated as categorical by default to improve quality/coverage. This can increase
  memorization risk; to disable, pass --no-team-ids.
- Player DNA / player IDs are NOT used here (coverage issues and potential temporal leakage).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score

# Ensure project root is importable so we can import src.live_predictor.
# This file lives at: ingame/ml-models/train_winrate_classifier_v3.py
# The directory that contains `src/` is: ingame/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.live_predictor import LivePredictor  # noqa: E402

# Keep a reference named REPO_ROOT for downstream path usage in this script.
REPO_ROOT = PROJECT_ROOT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("train_winrate_classifier_v3")


DATA_PATH_DEFAULT = "data/pro_matches_enriched.csv"
MODELS_DIR_DEFAULT = "ml-models"


LEAKY_SUBSTRINGS = (
    "_hero_damage",
    "_tower_damage",
    "_dota_plus",
    "radiant_score",
    "dire_score",
    "total_hero_damage",
    "total_tower_damage",
    "total_healing",
)
LEAKY_SUFFIXES = (
    "_kills",
    "_deaths",
    "_assists",
    "_gpm",
    "_xpm",
    "_lh",
    "_dn",
)


@dataclass
class SplitConfig:
    test_size: int
    sort_key: str  # 'start_time' or 'match_id'
    sort_ascending: bool = True


def _coerce_bool(v: Any) -> int:
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


def load_dataset(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"dataset not found: {path}")
    df = pd.read_csv(path)

    required = {"match_id", "radiant_win"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"dataset missing columns {sorted(missing)}")

    # Pick sort key for time-based split
    if "start_time" in df.columns:
        df["__sort_key"] = pd.to_numeric(df["start_time"], errors="coerce")
        sort_key = "start_time"
    else:
        df["__sort_key"] = pd.to_numeric(df["match_id"], errors="coerce")
        sort_key = "match_id"

    df = df.dropna(subset=["__sort_key"]).copy()
    df = df.sort_values("__sort_key").reset_index(drop=True)
    df["radiant_win"] = df["radiant_win"].apply(_coerce_bool).astype(int)

    # Ensure hero columns exist
    for side in ("radiant", "dire"):
        for i in range(1, 6):
            col = f"{side}_hero_{i}"
            if col not in df.columns:
                raise ValueError(f"dataset missing hero column: {col}")

    logger.info(f"Loaded {len(df)} rows from {path} (time key={sort_key})")
    return df


def split_time_based(
    df: pd.DataFrame, cfg: SplitConfig
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if len(df) <= cfg.test_size:
        raise ValueError("test_size is >= dataset size")

    df_sorted = df.sort_values("__sort_key", ascending=cfg.sort_ascending).reset_index(
        drop=True
    )
    train = df_sorted.iloc[: -cfg.test_size].copy()
    test = df_sorted.iloc[-cfg.test_size :].copy()
    logger.info(
        f"Split: train={len(train)} test={len(test)} (test_size={cfg.test_size})"
    )
    return train, test


def generate_features(
    predictor: LivePredictor,
    df: pd.DataFrame,
    include_team_ids: bool,
) -> pd.DataFrame:
    """
    Generate feature dict per row using LivePredictor.build_features().
    Only uses hero IDs + (optional) team IDs.
    """
    feats: List[Dict[str, Any]] = []

    # We do not pass account_ids to avoid DNA and player-history coverage issues.
    for _, row in df.iterrows():
        radiant_ids = [int(row[f"radiant_hero_{i}"]) for i in range(1, 6)]
        dire_ids = [int(row[f"dire_hero_{i}"]) for i in range(1, 6)]

        radiant_team_id: Optional[int] = None
        dire_team_id: Optional[int] = None
        if include_team_ids:
            if "radiant_team_id" in df.columns and pd.notna(row.get("radiant_team_id")):
                try:
                    radiant_team_id = int(row["radiant_team_id"])
                except Exception:
                    radiant_team_id = None
            if "dire_team_id" in df.columns and pd.notna(row.get("dire_team_id")):
                try:
                    dire_team_id = int(row["dire_team_id"])
                except Exception:
                    dire_team_id = None

        f = predictor.build_features(
            radiant_ids=radiant_ids,
            dire_ids=dire_ids,
            radiant_account_ids=None,
            dire_account_ids=None,
            radiant_team_id=radiant_team_id,
            dire_team_id=dire_team_id,
        )
        feats.append(f)

    X = pd.DataFrame(feats)
    return X


def detect_cat_features(feature_cols: List[str], include_team_ids: bool) -> List[str]:
    # Hero slots are categorical.
    cat = []
    for side in ("radiant", "dire"):
        for pos in range(1, 6):
            cat.append(f"{side}_hero_{pos}")
    if include_team_ids:
        cat += ["radiant_team_id", "dire_team_id"]
    # Keep only those present in feature_cols
    return [c for c in cat if c in feature_cols]


def anti_leakage_filter(feature_cols: List[str]) -> List[str]:
    """
    Drop obviously post-match / in-game / target-adjacent features based on name heuristics.

    This is conservative: it removes many fields that could be computed from history but are
    high-risk for offline/online skew unless you guarantee temporal stats construction.
    """
    keep: List[str] = []
    dropped: List[str] = []

    for col in feature_cols:
        # Always exclude explicit targets/results if present
        if col in ("radiant_win", "total_kills", "duration", "duration_min"):
            dropped.append(col)
            continue

        # Exclude leaky patterns
        if any(s in col for s in LEAKY_SUBSTRINGS) or any(
            col.endswith(s) for s in LEAKY_SUFFIXES
        ):
            dropped.append(col)
            continue

        # Exclude obvious in-game fields if they ever appear
        if col.startswith("ingame_"):
            dropped.append(col)
            continue

        keep.append(col)

    logger.info(f"Anti-leakage: kept={len(keep)} dropped={len(dropped)}")
    if dropped:
        logger.info(f"Anti-leakage dropped sample: {dropped[:25]}")
    return keep


def prepare_catboost_matrix(
    X_df: pd.DataFrame,
    feature_cols: List[str],
    cat_features: List[str],
) -> pd.DataFrame:
    X = X_df[feature_cols].copy()

    cat_set = set(cat_features)
    for col in feature_cols:
        if col in cat_set:
            X[col] = (
                pd.to_numeric(X[col], errors="coerce")
                .fillna(-1)
                .astype(int)
                .astype(str)
            )
        else:
            X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0.0)

    return X


def train_model(
    X_train: pd.DataFrame,
    y_train: np.ndarray,
    X_test: pd.DataFrame,
    y_test: np.ndarray,
    cat_features: List[str],
    seed: int,
) -> Tuple[CatBoostClassifier, Dict[str, float]]:
    feature_cols = list(X_train.columns)
    cat_indices = [feature_cols.index(c) for c in cat_features if c in feature_cols]

    model = CatBoostClassifier(
        iterations=4000,
        learning_rate=0.02,
        depth=6,
        l2_leaf_reg=6,
        loss_function="Logloss",
        eval_metric="AUC",
        random_seed=seed,
        verbose=200,
        early_stopping_rounds=300,
    )

    train_pool = Pool(X_train, y_train, cat_features=cat_indices)
    test_pool = Pool(X_test, y_test, cat_features=cat_indices)
    model.fit(train_pool, eval_set=test_pool, use_best_model=True)

    proba = model.predict_proba(X_test)[:, 1]
    metrics = {
        "auc": float(roc_auc_score(y_test, proba)),
        "accuracy": float(accuracy_score(y_test, (proba >= 0.5).astype(int))),
        "logloss": float(log_loss(y_test, proba, labels=[0, 1])),
    }
    return model, metrics


def save_artifacts(
    model: CatBoostClassifier,
    models_dir: Path,
    feature_cols: List[str],
    cat_features: List[str],
    cat_indices: List[int],
    metrics: Dict[str, float],
    train_size: int,
    test_size: int,
) -> None:
    models_dir.mkdir(parents=True, exist_ok=True)
    model_path = models_dir / "winrate_classifier_v3.cbm"
    meta_path = models_dir / "winrate_classifier_v3_meta.json"

    model.save_model(str(model_path))

    meta = {
        "feature_cols": feature_cols,
        "cat_features": cat_features,
        "cat_indices": cat_indices,
        "train_size": train_size,
        "test_size": test_size,
        "auc_estimate": metrics.get("auc"),
        "accuracy_estimate": metrics.get("accuracy"),
        "logloss_estimate": metrics.get("logloss"),
        "notes": {
            "split": "time-based (sorted by start_time if present else match_id)",
            "features_source": "LivePredictor.build_features (heroes + optional team_ids)",
            "anti_leakage": {
                "dropped_suffixes": list(LEAKY_SUFFIXES),
                "dropped_substrings": list(LEAKY_SUBSTRINGS),
            },
        },
    }

    meta_path.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    logger.info(f"Saved model: {model_path}")
    logger.info(f"Saved meta:  {meta_path}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--data", default=DATA_PATH_DEFAULT, help="Path to pro_matches_enriched.csv"
    )
    ap.add_argument(
        "--models-dir",
        default=MODELS_DIR_DEFAULT,
        help="Output directory (relative to repo root)",
    )
    ap.add_argument(
        "--test-size",
        type=int,
        default=500,
        help="Number of most recent matches to use as test set",
    )
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--no-team-ids",
        action="store_true",
        help="Exclude team IDs from categorical features to reduce memorization risk",
    )
    ap.add_argument(
        "--no-anti-leakage-filter",
        action="store_true",
        help="Disable name-based anti-leakage feature dropping (NOT RECOMMENDED)",
    )
    args = ap.parse_args()

    df = load_dataset(args.data)
    train_df, test_df = split_time_based(
        df, SplitConfig(test_size=args.test_size, sort_key="__sort_key")
    )

    predictor = LivePredictor()

    include_team_ids = not args.no_team_ids

    logger.info("Generating features (train)...")
    X_train_df = generate_features(
        predictor, train_df, include_team_ids=include_team_ids
    )
    logger.info("Generating features (test)...")
    X_test_df = generate_features(predictor, test_df, include_team_ids=include_team_ids)

    # Align columns
    feature_cols = sorted(list(set(X_train_df.columns) & set(X_test_df.columns)))
    if not feature_cols:
        raise RuntimeError(
            "No overlapping feature columns between train and test feature frames"
        )

    # Anti-leakage drop list (name-based)
    if not args.no_anti_leakage_filter:
        feature_cols = anti_leakage_filter(feature_cols)

    cat_features = detect_cat_features(feature_cols, include_team_ids=include_team_ids)

    # Prepare matrices
    X_train = prepare_catboost_matrix(X_train_df, feature_cols, cat_features)
    X_test = prepare_catboost_matrix(X_test_df, feature_cols, cat_features)

    y_train = train_df["radiant_win"].astype(int).values
    y_test = test_df["radiant_win"].astype(int).values

    logger.info(
        f"Training winrate v3: features={len(feature_cols)} cat={len(cat_features)}"
    )
    model, metrics = train_model(
        X_train, y_train, X_test, y_test, cat_features, seed=args.seed
    )
    logger.info(
        f"Test metrics: AUC={metrics['auc']:.4f} ACC={metrics['accuracy']:.4f} LogLoss={metrics['logloss']:.4f}"
    )

    cat_indices = [feature_cols.index(c) for c in cat_features if c in feature_cols]
    save_artifacts(
        model=model,
        models_dir=(REPO_ROOT / args.models_dir),
        feature_cols=feature_cols,
        cat_features=cat_features,
        cat_indices=cat_indices,
        metrics=metrics,
        train_size=len(train_df),
        test_size=len(test_df),
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
