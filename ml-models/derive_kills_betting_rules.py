#!/usr/bin/env python3
"""
Derive betting rules for low (<40) and high (>50) total kills using validation split.

Uses existing models:
- live_cb_kills_reg.cbm
- live_cb_kills_reg_low.cbm
- live_cb_kills_reg_high.cbm
- live_cb_kills_low_cls.cbm
- live_cb_kills_high_cls.cbm
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
from catboost import CatBoostClassifier, CatBoostRegressor

import train_kills_regression_pro as tkr

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("kills_rules")


def prepare_x(df, feature_cols: List[str], cat_cols: List[str]):
    X = df[feature_cols].copy()
    for c in cat_cols:
        X[c] = X[c].fillna("UNKNOWN").astype(str)
    return X


def eval_rule(y_true: np.ndarray, bet_mask: np.ndarray, is_win: np.ndarray, odds: float) -> Dict[str, float]:
    if bet_mask.sum() == 0:
        return {"bets": 0, "precision": 0.0, "recall": 0.0, "ev": 0.0}
    wins = (is_win & bet_mask).sum()
    precision = wins / bet_mask.sum()
    recall = wins / is_win.sum() if is_win.sum() else 0.0
    ev = precision * odds - 1.0
    return {
        "bets": int(bet_mask.sum()),
        "precision": float(precision),
        "recall": float(recall),
        "ev": float(ev),
    }


def pick_best_rule(
    candidates: List[Tuple[float, Dict[str, Any]]],
    min_bets: int,
) -> Dict[str, Any]:
    best = None
    for score, rule in candidates:
        if rule["val"]["bets"] < min_bets:
            continue
        if rule["val"]["ev"] <= 0:
            continue
        if best is None or score > best[0]:
            best = (score, rule)
    return best[1] if best else {}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--clean-path", type=str, default=str(tkr.DEFAULT_CLEAN_PATH))
    parser.add_argument("--test-size", type=int, default=100)
    parser.add_argument("--val-size", type=int, default=300)
    parser.add_argument("--no-pub-priors", action="store_true")
    parser.add_argument("--min-bets-low", type=int, default=50)
    parser.add_argument("--min-bets-high", type=int, default=30)
    parser.add_argument("--odds", type=float, default=1.8)
    parser.add_argument("--save-path", type=str, default=str(tkr.MODELS_DIR / "kills_betting_rules.json"))
    args = parser.parse_args()

    matches = tkr.load_clean_data(Path(args.clean_path))
    pub_priors = {}
    if not args.no_pub_priors:
        pub_priors = tkr.build_pub_hero_priors(tkr.PUB_PLAYERS_DIR, tkr.PUB_PRIORS_PATH)

    df = tkr.build_dataset(matches, pub_priors)
    train_df, val_df, test_df = tkr.split_time(
        df, tkr.SplitConfig(test_size=args.test_size, val_size=args.val_size)
    )

    feature_cols = [c for c in df.columns if c not in ("total_kills", "start_time")]
    cat_cols = [
        c
        for c in feature_cols
        if c.startswith("radiant_hero_")
        or c.startswith("dire_hero_")
        or c.startswith("radiant_player_")
        or c.startswith("dire_player_")
        or c.endswith("_team_id")
        or c
        in (
            "league_id",
            "game_version_id",
            "series_type",
            "tournament_round",
            "lobby_type",
            "region_id",
            "rank",
            "bracket",
        )
    ]

    X_val = prepare_x(val_df, feature_cols, cat_cols)
    X_test = prepare_x(test_df, feature_cols, cat_cols)

    y_val = val_df["total_kills"].values
    y_test = test_df["total_kills"].values

    # Load models
    reg_all = CatBoostRegressor()
    reg_all.load_model(str(tkr.MODELS_DIR / "live_cb_kills_reg.cbm"))
    reg_low = CatBoostRegressor()
    reg_low.load_model(str(tkr.MODELS_DIR / "live_cb_kills_reg_low.cbm"))
    reg_high = CatBoostRegressor()
    reg_high.load_model(str(tkr.MODELS_DIR / "live_cb_kills_reg_high.cbm"))
    low_cls = CatBoostClassifier()
    low_cls.load_model(str(tkr.MODELS_DIR / "live_cb_kills_low_cls.cbm"))
    high_cls = CatBoostClassifier()
    high_cls.load_model(str(tkr.MODELS_DIR / "live_cb_kills_high_cls.cbm"))

    pred_val_all = reg_all.predict(X_val)
    pred_test_all = reg_all.predict(X_test)
    pred_val_low = reg_low.predict(X_val)
    pred_test_low = reg_low.predict(X_test)
    pred_val_high = reg_high.predict(X_val)
    pred_test_high = reg_high.predict(X_test)
    prob_val_low = low_cls.predict_proba(X_val)[:, 1]
    prob_test_low = low_cls.predict_proba(X_test)[:, 1]
    prob_val_high = high_cls.predict_proba(X_val)[:, 1]
    prob_test_high = high_cls.predict_proba(X_test)[:, 1]

    low_val_target = y_val < 40
    low_test_target = y_test < 40
    high_val_target = y_val > 50
    high_test_target = y_test > 50

    low_rules = []
    low_probs = [0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9]
    low_pred_low = [40, 39, 38, 37, 36, 35, 34]
    low_pred_all = [46, 44, 42, 40, 38, 36, 34]
    for p in low_probs:
        bet_val = prob_val_low >= p
        bet_test = prob_test_low >= p
        val_metrics = eval_rule(y_val, bet_val, low_val_target, args.odds)
        test_metrics = eval_rule(y_test, bet_test, low_test_target, args.odds)
        rule = {
            "type": "low_prob",
            "prob_threshold": float(p),
            "val": val_metrics,
            "test": test_metrics,
        }
        low_rules.append((val_metrics["ev"], rule))

    for p in low_probs:
        for t in low_pred_low:
            bet_val = (prob_val_low >= p) & (pred_val_low <= t)
            bet_test = (prob_test_low >= p) & (pred_test_low <= t)
            val_metrics = eval_rule(y_val, bet_val, low_val_target, args.odds)
            test_metrics = eval_rule(y_test, bet_test, low_test_target, args.odds)
            rule = {
                "type": "low_prob_and_reg_low",
                "prob_threshold": float(p),
                "pred_threshold": float(t),
                "val": val_metrics,
                "test": test_metrics,
            }
            low_rules.append((val_metrics["ev"], rule))

    for p in low_probs:
        for t in low_pred_all:
            bet_val = (prob_val_low >= p) & (pred_val_all <= t)
            bet_test = (prob_test_low >= p) & (pred_test_all <= t)
            val_metrics = eval_rule(y_val, bet_val, low_val_target, args.odds)
            test_metrics = eval_rule(y_test, bet_test, low_test_target, args.odds)
            rule = {
                "type": "low_prob_and_reg_all",
                "prob_threshold": float(p),
                "pred_threshold": float(t),
                "val": val_metrics,
                "test": test_metrics,
            }
            low_rules.append((val_metrics["ev"], rule))

    for t in low_pred_all:
        bet_val = pred_val_all <= t
        bet_test = pred_test_all <= t
        val_metrics = eval_rule(y_val, bet_val, low_val_target, args.odds)
        test_metrics = eval_rule(y_test, bet_test, low_test_target, args.odds)
        rule = {
            "type": "reg_all_low",
            "pred_threshold": float(t),
            "val": val_metrics,
            "test": test_metrics,
        }
        low_rules.append((val_metrics["ev"], rule))

    for t in low_pred_low:
        bet_val = pred_val_low <= t
        bet_test = pred_test_low <= t
        val_metrics = eval_rule(y_val, bet_val, low_val_target, args.odds)
        test_metrics = eval_rule(y_test, bet_test, low_test_target, args.odds)
        rule = {
            "type": "reg_low",
            "pred_threshold": float(t),
            "val": val_metrics,
            "test": test_metrics,
        }
        low_rules.append((val_metrics["ev"], rule))

    low_best = pick_best_rule(low_rules, args.min_bets_low)

    high_rules = []
    high_probs = [0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9]
    high_pred_all = [54, 56, 58, 60, 62, 64, 66, 68]
    high_pred_high = [50, 52, 54, 56, 58, 60, 62, 64]
    for t in high_pred_all:
        bet_val = pred_val_all >= t
        bet_test = pred_test_all >= t
        val_metrics = eval_rule(y_val, bet_val, high_val_target, args.odds)
        test_metrics = eval_rule(y_test, bet_test, high_test_target, args.odds)
        rule = {
            "type": "reg_all",
            "pred_threshold": float(t),
            "val": val_metrics,
            "test": test_metrics,
        }
        high_rules.append((val_metrics["ev"], rule))

    for p in high_probs:
        for t in high_pred_high:
            bet_val = (prob_val_high >= p) & (pred_val_high >= t)
            bet_test = (prob_test_high >= p) & (pred_test_high >= t)
            val_metrics = eval_rule(y_val, bet_val, high_val_target, args.odds)
            test_metrics = eval_rule(y_test, bet_test, high_test_target, args.odds)
            rule = {
                "type": "high_prob_and_reg_high",
                "prob_threshold": float(p),
                "pred_threshold": float(t),
                "val": val_metrics,
                "test": test_metrics,
            }
            high_rules.append((val_metrics["ev"], rule))

    for p in high_probs:
        bet_val = prob_val_high >= p
        bet_test = prob_test_high >= p
        val_metrics = eval_rule(y_val, bet_val, high_val_target, args.odds)
        test_metrics = eval_rule(y_test, bet_test, high_test_target, args.odds)
        rule = {
            "type": "high_prob",
            "prob_threshold": float(p),
            "val": val_metrics,
            "test": test_metrics,
        }
        high_rules.append((val_metrics["ev"], rule))

    for p in high_probs:
        for t in high_pred_all:
            bet_val = (prob_val_high >= p) & (pred_val_all >= t)
            bet_test = (prob_test_high >= p) & (pred_test_all >= t)
            val_metrics = eval_rule(y_val, bet_val, high_val_target, args.odds)
            test_metrics = eval_rule(y_test, bet_test, high_test_target, args.odds)
            rule = {
                "type": "high_prob_and_reg_all",
                "prob_threshold": float(p),
                "pred_threshold": float(t),
                "val": val_metrics,
                "test": test_metrics,
            }
            high_rules.append((val_metrics["ev"], rule))

    for t in high_pred_high:
        bet_val = pred_val_high >= t
        bet_test = pred_test_high >= t
        val_metrics = eval_rule(y_val, bet_val, high_val_target, args.odds)
        test_metrics = eval_rule(y_test, bet_test, high_test_target, args.odds)
        rule = {
            "type": "reg_high",
            "pred_threshold": float(t),
            "val": val_metrics,
            "test": test_metrics,
        }
        high_rules.append((val_metrics["ev"], rule))

    high_best = pick_best_rule(high_rules, args.min_bets_high)

    output = {
        "odds": args.odds,
        "low_rule": low_best,
        "high_rule": high_best,
        "min_bets_low": args.min_bets_low,
        "min_bets_high": args.min_bets_high,
        "test_window": {
            "size": int(len(test_df)),
            "start_time_min": int(test_df["start_time"].min()),
            "start_time_max": int(test_df["start_time"].max()),
        },
    }

    save_path = Path(args.save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    with save_path.open("w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    logger.info("Saved rules: %s", save_path)
    logger.info("Low rule: %s", low_best)
    logger.info("High rule: %s", high_best)


if __name__ == "__main__":
    main()
