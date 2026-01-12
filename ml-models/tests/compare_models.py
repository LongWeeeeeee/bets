#!/usr/bin/env python3
"""
Compare all training scripts on the same data.

Tests:
1. train_live_models_catboost.py - Main CatBoost model (Kills, Winner, Duration, KPM)
2. train_winrate_classifier.py - Winrate classifier

All use same data: data/pro_matches_enriched.csv
Same split: last 500 matches for test
"""

import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, mean_absolute_error, roc_auc_score
from tqdm import tqdm

# Ensure repository root is importable so we can import src.live_predictor reliably.
# This repo layout is: ingame/src/live_predictor.py (package: src)
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Suppress verbose logging
logging.getLogger("src.live_predictor").setLevel(logging.WARNING)

DATA_PATH = "data/pro_matches_enriched.csv"
TEST_SIZE = 500


def load_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load and split data."""
    df = pd.read_csv(DATA_PATH)
    df = df.sort_values("match_id").reset_index(drop=True)

    train_idx = len(df) - TEST_SIZE
    df_train = df.iloc[:train_idx].copy()
    df_test = df.iloc[train_idx:].copy()

    logger.info(f"Loaded {len(df)} matches: Train={len(df_train)}, Test={len(df_test)}")
    return df_train, df_test


def compute_backtest_roi(
    proba: np.ndarray, y_true: np.ndarray, bk_line: float, threshold: float = 0.60
) -> Tuple[int, float, float]:
    """Compute backtest ROI at given threshold."""
    bets = 0
    wins = 0

    for i in range(len(y_true)):
        prob = proba[i]
        actual = y_true[i]

        if prob > threshold:
            bets += 1
            if actual > bk_line:
                wins += 1
        elif prob < (1 - threshold):
            bets += 1
            if actual < bk_line:
                wins += 1

    if bets > 0:
        wr = wins / bets * 100
        roi = (wins * 1.9 - bets) / bets * 100
        return bets, wr, roi
    return 0, 0.0, 0.0


def test_catboost_model(
    df_train: pd.DataFrame, df_test: pd.DataFrame
) -> Dict[str, Any]:
    """Test CatBoost model from train_live_models_catboost.py."""
    logger.info("\n" + "=" * 60)
    logger.info("TESTING: train_live_models_catboost.py")
    logger.info("=" * 60)

    from catboost import CatBoostClassifier, CatBoostRegressor, Pool

    from src.live_predictor import LivePredictor

    # Initialize predictor for feature generation
    predictor = LivePredictor()

    # BK Line from train
    bk_line = df_train["total_kills"].median()
    logger.info(f"BK Line: {bk_line}")

    # Generate features
    logger.info("Generating features...")

    def generate_features(df: pd.DataFrame) -> pd.DataFrame:
        features_list = []
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Features"):
            radiant_ids = [
                int(row[f"radiant_hero_{i}"])
                for i in range(1, 6)
                if pd.notna(row.get(f"radiant_hero_{i}"))
            ]
            dire_ids = [
                int(row[f"dire_hero_{i}"])
                for i in range(1, 6)
                if pd.notna(row.get(f"dire_hero_{i}"))
            ]

            radiant_team_id = (
                int(row["radiant_team_id"])
                if pd.notna(row.get("radiant_team_id"))
                else None
            )
            dire_team_id = (
                int(row["dire_team_id"]) if pd.notna(row.get("dire_team_id")) else None
            )

            radiant_players = []
            dire_players = []
            for i in range(1, 6):
                if f"radiant_player_{i}_id" in row.index and pd.notna(
                    row[f"radiant_player_{i}_id"]
                ):
                    radiant_players.append(int(row[f"radiant_player_{i}_id"]))
                if f"dire_player_{i}_id" in row.index and pd.notna(
                    row[f"dire_player_{i}_id"]
                ):
                    dire_players.append(int(row[f"dire_player_{i}_id"]))

            features = predictor.build_features(
                radiant_ids,
                dire_ids,
                radiant_players,
                dire_players,
                radiant_team_id,
                dire_team_id,
            )
            features_list.append(features)
        return pd.DataFrame(features_list)

    X_train_df = generate_features(df_train)
    X_test_df = generate_features(df_test)

    feature_cols = list(X_train_df.columns)
    cat_features = [
        c
        for c in feature_cols
        if "hero_" in c and c.endswith(("_1", "_2", "_3", "_4", "_5"))
    ]
    cat_features += [
        c for c in ["radiant_team_id", "dire_team_id"] if c in feature_cols
    ]
    cat_indices = [feature_cols.index(c) for c in cat_features if c in feature_cols]

    # Prepare data
    def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
        X = df[feature_cols].copy()
        for col in X.columns:
            if col not in cat_features:
                X[col] = X[col].fillna(X[col].median())
        for col in cat_features:
            if col in X.columns:
                X[col] = X[col].fillna(-1).astype(int).astype(str)
        return X

    X_train = prepare_data(X_train_df)
    X_test = prepare_data(X_test_df)

    # Targets
    y_train_kills = (df_train["total_kills"] > bk_line).astype(int)
    y_test_kills = (df_test["total_kills"] > bk_line).astype(int)
    y_train_winner = df_train["radiant_win"].astype(int)
    y_test_winner = df_test["radiant_win"].astype(int)

    logger.info(f"Features: {len(feature_cols)}, Cat: {len(cat_features)}")

    # Train Kills Classifier
    logger.info("Training Kills Classifier...")
    kills_model = CatBoostClassifier(
        iterations=1000,
        learning_rate=0.03,
        depth=6,
        loss_function="Logloss",
        eval_metric="AUC",
        cat_features=cat_indices,
        random_seed=42,
        verbose=0,
        early_stopping_rounds=100,
        l2_leaf_reg=10,
    )

    train_pool = Pool(X_train, y_train_kills, cat_features=cat_indices)
    test_pool = Pool(X_test, y_test_kills, cat_features=cat_indices)
    kills_model.fit(train_pool, eval_set=test_pool, use_best_model=True)

    kills_proba = kills_model.predict_proba(X_test)[:, 1]
    kills_auc = roc_auc_score(y_test_kills, kills_proba)
    kills_acc = accuracy_score(y_test_kills, (kills_proba > 0.5).astype(int))

    # Train Winner Classifier
    logger.info("Training Winner Classifier...")
    winner_model = CatBoostClassifier(
        iterations=1000,
        learning_rate=0.03,
        depth=6,
        loss_function="Logloss",
        eval_metric="AUC",
        cat_features=cat_indices,
        random_seed=42,
        verbose=0,
        early_stopping_rounds=100,
        l2_leaf_reg=10,
    )

    train_pool_w = Pool(X_train, y_train_winner, cat_features=cat_indices)
    test_pool_w = Pool(X_test, y_test_winner, cat_features=cat_indices)
    winner_model.fit(train_pool_w, eval_set=test_pool_w, use_best_model=True)

    winner_proba = winner_model.predict_proba(X_test)[:, 1]
    winner_auc = roc_auc_score(y_test_winner, winner_proba)
    winner_acc = accuracy_score(y_test_winner, (winner_proba > 0.5).astype(int))

    # Backtest
    bets_60, wr_60, roi_60 = compute_backtest_roi(
        kills_proba, df_test["total_kills"].values, bk_line, 0.60
    )
    bets_58, wr_58, roi_58 = compute_backtest_roi(
        kills_proba, df_test["total_kills"].values, bk_line, 0.58
    )

    results = {
        "model": "CatBoost (train_live_models_catboost.py)",
        "kills_auc": kills_auc,
        "kills_acc": kills_acc,
        "winner_auc": winner_auc,
        "winner_acc": winner_acc,
        "backtest_60": {"bets": bets_60, "wr": wr_60, "roi": roi_60},
        "backtest_58": {"bets": bets_58, "wr": wr_58, "roi": roi_58},
        "features": len(feature_cols),
        "bk_line": bk_line,
    }

    logger.info(f"Kills AUC: {kills_auc:.4f}, Acc: {kills_acc * 100:.2f}%")
    logger.info(f"Winner AUC: {winner_auc:.4f}, Acc: {winner_acc * 100:.2f}%")
    logger.info(f"Backtest @60%: {bets_60} bets, {wr_60:.1f}% WR, {roi_60:+.1f}% ROI")

    return results


def test_winrate_classifier(
    df_train: pd.DataFrame, df_test: pd.DataFrame
) -> Dict[str, Any]:
    """Test Winrate classifier from train_winrate_classifier.py."""
    logger.info("\n" + "=" * 60)
    logger.info("TESTING: train_winrate_classifier.py")
    logger.info("=" * 60)

    from catboost import CatBoostClassifier, Pool

    from src.live_predictor import LivePredictor

    predictor = LivePredictor()

    # Generate features (same as above)
    logger.info("Generating features...")

    def generate_features(df: pd.DataFrame) -> pd.DataFrame:
        features_list = []
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Features"):
            radiant_ids = [
                int(row[f"radiant_hero_{i}"])
                for i in range(1, 6)
                if pd.notna(row.get(f"radiant_hero_{i}"))
            ]
            dire_ids = [
                int(row[f"dire_hero_{i}"])
                for i in range(1, 6)
                if pd.notna(row.get(f"dire_hero_{i}"))
            ]

            radiant_team_id = (
                int(row["radiant_team_id"])
                if pd.notna(row.get("radiant_team_id"))
                else None
            )
            dire_team_id = (
                int(row["dire_team_id"]) if pd.notna(row.get("dire_team_id")) else None
            )

            features = predictor.build_features(
                radiant_ids,
                dire_ids,
                radiant_team_id=radiant_team_id,
                dire_team_id=dire_team_id,
            )
            features_list.append(features)
        return pd.DataFrame(features_list)

    X_train_df = generate_features(df_train)
    X_test_df = generate_features(df_test)

    feature_cols = list(X_train_df.columns)

    # Winrate classifier excludes team IDs to prevent memorization
    cat_features = [
        c
        for c in feature_cols
        if "hero_" in c and c.endswith(("_1", "_2", "_3", "_4", "_5"))
    ]
    # NOTE: team IDs excluded for winrate classifier
    cat_indices = [feature_cols.index(c) for c in cat_features if c in feature_cols]

    def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
        X = df[feature_cols].copy()
        for col in X.columns:
            if col not in cat_features:
                X[col] = X[col].fillna(X[col].median())
        for col in cat_features:
            if col in X.columns:
                X[col] = X[col].fillna(-1).astype(int).astype(str)
        return X

    X_train = prepare_data(X_train_df)
    X_test = prepare_data(X_test_df)

    y_train = df_train["radiant_win"].astype(int)
    y_test = df_test["radiant_win"].astype(int)

    logger.info(f"Features: {len(feature_cols)}, Cat: {len(cat_features)}")

    # Train
    logger.info("Training Winrate Classifier...")
    model = CatBoostClassifier(
        iterations=2000,
        learning_rate=0.02,
        depth=6,
        l2_leaf_reg=5,
        loss_function="Logloss",
        eval_metric="AUC",
        cat_features=cat_indices,
        random_seed=42,
        verbose=0,
        early_stopping_rounds=200,
    )

    train_pool = Pool(X_train, y_train, cat_features=cat_indices)
    test_pool = Pool(X_test, y_test, cat_features=cat_indices)
    model.fit(train_pool, eval_set=test_pool, use_best_model=True)

    proba = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, proba)
    acc = accuracy_score(y_test, (proba > 0.5).astype(int))

    results = {
        "model": "Winrate Classifier (train_winrate_classifier.py)",
        "winner_auc": auc,
        "winner_acc": acc,
        "features": len(feature_cols),
    }

    logger.info(f"Winner AUC: {auc:.4f}, Acc: {acc * 100:.2f}%")

    return results


def main() -> None:
    """Run comparison of all models."""
    print("\n" + "=" * 70)
    print("MODEL COMPARISON TEST")
    print("=" * 70)
    print(f"Data: {DATA_PATH}")
    print(f"Test size: {TEST_SIZE} matches")
    print("=" * 70)

    # Load data
    df_train, df_test = load_data()

    results = []

    # Test CatBoost
    try:
        r1 = test_catboost_model(df_train, df_test)
        results.append(r1)
    except Exception as e:
        logger.error(f"CatBoost test failed: {e}")

    # Test Winrate Classifier
    try:
        r2 = test_winrate_classifier(df_train, df_test)
        results.append(r2)
    except Exception as e:
        logger.error(f"Winrate test failed: {e}")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    print(f"\n{'Model':<50} {'Kills AUC':<12} {'Winner AUC':<12} {'ROI@60%':<12}")
    print("-" * 86)

    for r in results:
        model = r["model"][:48]
        kills_auc = f"{r.get('kills_auc', 0):.4f}" if r.get("kills_auc") else "N/A"
        winner_auc = f"{r.get('winner_auc', 0):.4f}"
        roi = (
            f"{r.get('backtest_60', {}).get('roi', 0):+.1f}%"
            if r.get("backtest_60")
            else "N/A"
        )
        print(f"{model:<50} {kills_auc:<12} {winner_auc:<12} {roi:<12}")

    # Best model
    print("\n" + "=" * 70)
    print("RECOMMENDATION")
    print("=" * 70)

    if results:
        best_kills = max(results, key=lambda x: x.get("kills_auc", 0))
        best_winner = max(results, key=lambda x: x.get("winner_auc", 0))

        print(f"\nBest for Kills prediction: {best_kills['model']}")
        print(f"  AUC: {best_kills.get('kills_auc', 0):.4f}")

        print(f"\nBest for Winner prediction: {best_winner['model']}")
        print(f"  AUC: {best_winner.get('winner_auc', 0):.4f}")

        if best_kills.get("backtest_60"):
            bt = best_kills["backtest_60"]
            print(f"\nBacktest Results (Kills @60% threshold):")
            print(f"  Bets: {bt['bets']}")
            print(f"  Win Rate: {bt['wr']:.1f}%")
            print(f"  ROI: {bt['roi']:+.1f}%")


if __name__ == "__main__":
    main()
