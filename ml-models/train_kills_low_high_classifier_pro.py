#!/usr/bin/env python3
"""
Train low/high total-kills classifiers on pro matches (time-based split).

Constraints:
- NO winRates
- NO networth-based in-game stats
- Use last 100 pro matches (by startDateTime) as test
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score

import train_kills_regression_pro as tkr

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("kills_cls")

LOW_THRESHOLD = 40
HIGH_THRESHOLD = 50


def prepare_x(
    df,
    feature_cols: List[str],
    cat_cols: List[str],
) -> Tuple[Any, List[int]]:
    X = df[feature_cols].copy()
    for c in cat_cols:
        if c in X.columns:
            X[c] = X[c].fillna("UNKNOWN").astype(str)
    cat_indices = [X.columns.get_loc(c) for c in cat_cols if c in X.columns]
    return X, cat_indices


def pick_threshold(y_true: np.ndarray, probas: np.ndarray) -> float:
    best_f1 = -1.0
    best_thr = 0.5
    for thr in np.linspace(0.2, 0.8, 13):
        preds = (probas >= thr).astype(int)
        f1 = f1_score(y_true, preds, zero_division=0)
        if f1 > best_f1:
            best_f1 = f1
            best_thr = float(thr)
    return best_thr


def train_classifier(
    X_train,
    y_train,
    X_val,
    y_val,
    cat_indices: List[int],
    cfg: Dict[str, Any],
    sample_weight: Optional[np.ndarray] = None,
) -> CatBoostClassifier:
    model = CatBoostClassifier(**cfg)
    train_pool = Pool(X_train, y_train, cat_features=cat_indices, weight=sample_weight)
    val_pool = Pool(X_val, y_val, cat_features=cat_indices)
    model.fit(train_pool, eval_set=val_pool, use_best_model=True)
    return model


def eval_binary(
    y_true: np.ndarray,
    probas: np.ndarray,
    thr: float,
) -> Dict[str, float]:
    preds = (probas >= thr).astype(int)
    return {
        "auc": roc_auc_score(y_true, probas) if len(np.unique(y_true)) > 1 else 0.0,
        "accuracy": accuracy_score(y_true, preds),
        "precision": precision_score(y_true, preds, zero_division=0),
        "recall": recall_score(y_true, preds, zero_division=0),
        "f1": f1_score(y_true, preds, zero_division=0),
    }


def _group_split_sizes(
    n_rows: int,
    base_val: int,
    base_test: int,
    min_train: int = 80,
    min_val: int = 20,
    min_test: int = 20,
) -> Optional[Tuple[int, int]]:
    if n_rows <= min_train + min_val + min_test:
        return None
    val_size = min(base_val, max(min_val, int(n_rows * 0.20)))
    test_size = min(base_test, max(min_test, int(n_rows * 0.15)))
    if n_rows - (val_size + test_size) < min_train:
        max_holdout = n_rows - min_train
        if max_holdout <= 0:
            return None
        val_size = max(min_val, int(max_holdout * 0.6))
        test_size = max(min_test, max_holdout - val_size)
    if n_rows <= val_size + test_size:
        return None
    return val_size, test_size


def _is_invalid_group_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and np.isnan(value):
        return True
    if isinstance(value, (int, np.integer)) and int(value) <= 0:
        return True
    if isinstance(value, str):
        v = value.strip()
        if not v or v.upper() == "UNKNOWN":
            return True
    return False


def train_group_classifiers(
    df,
    group_col: str,
    group_label: str,
    feature_cols: List[str],
    cat_cols: List[str],
    cfg_builder,
    test_size: int,
    val_size: int,
    save_models: bool,
) -> None:
    if group_col not in df.columns:
        logger.warning("Group column '%s' missing; skipping %s models", group_col, group_label)
        return

    values = sorted(df[group_col].dropna().unique().tolist())
    for value in values:
        if _is_invalid_group_value(value):
            continue
        group_df = df[df[group_col] == value].copy()
        split_sizes = _group_split_sizes(len(group_df), val_size, test_size)
        if split_sizes is None:
            logger.info("Skip %s=%s: insufficient rows=%d", group_label, value, len(group_df))
            continue
        group_val_size, group_test_size = split_sizes
        group_df = group_df.sort_values("start_time").reset_index(drop=True)
        try:
            train_df, val_df, test_df = tkr.split_time(
                group_df, tkr.SplitConfig(test_size=group_test_size, val_size=group_val_size)
            )
        except ValueError:
            logger.info("Skip %s=%s: insufficient rows for split", group_label, value)
            continue

        X_train, cat_indices = prepare_x(train_df, feature_cols, cat_cols)
        X_val, _ = prepare_x(val_df, feature_cols, cat_cols)
        X_test, _ = prepare_x(test_df, feature_cols, cat_cols)

        y_train_low = (train_df["total_kills"] < LOW_THRESHOLD).astype(int).values
        y_val_low = (val_df["total_kills"] < LOW_THRESHOLD).astype(int).values
        y_test_low = (test_df["total_kills"] < LOW_THRESHOLD).astype(int).values

        y_train_high = (train_df["total_kills"] > HIGH_THRESHOLD).astype(int).values
        y_val_high = (val_df["total_kills"] > HIGH_THRESHOLD).astype(int).values
        y_test_high = (test_df["total_kills"] > HIGH_THRESHOLD).astype(int).values

        if y_train_low.sum() < 10 or y_val_low.sum() < 5:
            logger.info("Skip %s=%s: low class too small", group_label, value)
        else:
            low_cfg = cfg_builder(y_train_low)
            low_model = train_classifier(X_train, y_train_low, X_val, y_val_low, cat_indices, low_cfg)
            low_val_proba = low_model.predict_proba(X_val)[:, 1]
            low_thr = pick_threshold(y_val_low, low_val_proba)
            low_test_proba = low_model.predict_proba(X_test)[:, 1]
            low_metrics = eval_binary(y_test_low, low_test_proba, low_thr)
            logger.info("Group %s=%s LOW metrics=%s", group_label, value, low_metrics)

            if save_models:
                if group_label == "patch":
                    suffix = f"patch_{tkr.patch_label_to_slug(str(value))}"
                else:
                    suffix = f"tier_{int(value)}"
                low_path = tkr.MODELS_DIR / f"live_cb_kills_low_cls_{suffix}.cbm"
                low_model.save_model(str(low_path))
                logger.info("Saved group low cls: %s", low_path)

        if y_train_high.sum() < 10 or y_val_high.sum() < 5:
            logger.info("Skip %s=%s: high class too small", group_label, value)
        else:
            high_cfg = cfg_builder(y_train_high)
            high_model = train_classifier(X_train, y_train_high, X_val, y_val_high, cat_indices, high_cfg)
            high_val_proba = high_model.predict_proba(X_val)[:, 1]
            high_thr = pick_threshold(y_val_high, high_val_proba)
            high_test_proba = high_model.predict_proba(X_test)[:, 1]
            high_metrics = eval_binary(y_test_high, high_test_proba, high_thr)
            logger.info("Group %s=%s HIGH metrics=%s", group_label, value, high_metrics)

            if save_models:
                if group_label == "patch":
                    suffix = f"patch_{tkr.patch_label_to_slug(str(value))}"
                else:
                    suffix = f"tier_{int(value)}"
                high_path = tkr.MODELS_DIR / f"live_cb_kills_high_cls_{suffix}.cbm"
                high_model.save_model(str(high_path))
                logger.info("Saved group high cls: %s", high_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--clean-path", type=str, default=str(tkr.DEFAULT_CLEAN_PATH))
    parser.add_argument("--test-size", type=int, default=100)
    parser.add_argument("--val-size", type=int, default=300)
    parser.add_argument("--no-pub-priors", action="store_true")
    parser.add_argument("--save-model", action="store_true")
    parser.add_argument("--use-selected", action="store_true")
    parser.add_argument("--selected-features-path", type=str, default=str(tkr.SELECTED_FEATURES_PATH))
    parser.add_argument("--drop-networth", action="store_true")
    parser.add_argument("--iterations", type=int, default=2500)
    parser.add_argument("--depth", type=int, default=7)
    parser.add_argument("--learning-rate", type=float, default=0.04)
    parser.add_argument("--by-patch", action="store_true", help="Train per-patch major models")
    parser.add_argument("--by-tier", action="store_true", help="Train per-tier models")
    parser.add_argument("--focus-patch", type=str, default=None)
    parser.add_argument("--patch-weight-decay", type=float, default=0.6)
    parser.add_argument("--patch-weight-min", type=float, default=0.25)
    parser.add_argument("--patch-weight-unknown", type=float, default=0.2)
    parser.add_argument("--roster-lock-weight", type=float, default=0.7)
    parser.add_argument("--save-focus-as-patch", action="store_true")
    args = parser.parse_args()

    clean_path = Path(args.clean_path)
    logger.info("Loading matches: %s", clean_path)
    matches = tkr.load_clean_data(clean_path)

    pub_priors = {}
    if not args.no_pub_priors:
        pub_priors = tkr.build_pub_hero_priors(tkr.PUB_PLAYERS_DIR, tkr.PUB_PRIORS_PATH)

    logger.info("Building dataset (time-aware features)...")
    df = tkr.build_dataset(matches, pub_priors)
    logger.info("Dataset rows: %d", len(df))

    if args.focus_patch:
        try:
            train_df, val_df, test_df = tkr.split_focus_patch(
                df, args.focus_patch, args.val_size, args.test_size
            )
            logger.info(
                "Focus patch=%s split sizes: train=%d val=%d test=%d",
                args.focus_patch,
                len(train_df),
                len(val_df),
                len(test_df),
            )
        except Exception as e:
            logger.warning("Focus patch split failed (%s); falling back to standard split", e)
            train_df, val_df, test_df = tkr.split_time(
                df, tkr.SplitConfig(test_size=args.test_size, val_size=args.val_size)
            )
            logger.info(
                "Split sizes: train=%d val=%d test=%d", len(train_df), len(val_df), len(test_df)
            )
    else:
        train_df, val_df, test_df = tkr.split_time(
            df, tkr.SplitConfig(test_size=args.test_size, val_size=args.val_size)
        )
        logger.info("Split sizes: train=%d val=%d test=%d", len(train_df), len(val_df), len(test_df))

    feature_cols = [c for c in df.columns if c not in ("total_kills", "start_time")]
    feature_cols = tkr.select_feature_cols(feature_cols, args.use_selected, Path(args.selected_features_path))
    if args.drop_networth:
        feature_cols = tkr.drop_networth_features(feature_cols)
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
            "patch_id",
            "patch_major_label",
            "game_version_id",
            "series_type",
            "tournament_round",
            "lobby_type",
            "region_id",
            "rank",
            "bracket",
            "bottom_lane_outcome",
            "mid_lane_outcome",
            "top_lane_outcome",
        )
    ]

    X_train, cat_indices = prepare_x(train_df, feature_cols, cat_cols)
    X_val, _ = prepare_x(val_df, feature_cols, cat_cols)
    X_test, _ = prepare_x(test_df, feature_cols, cat_cols)

    y_train_low = (train_df["total_kills"] < LOW_THRESHOLD).astype(int).values
    y_val_low = (val_df["total_kills"] < LOW_THRESHOLD).astype(int).values
    y_test_low = (test_df["total_kills"] < LOW_THRESHOLD).astype(int).values

    y_train_high = (train_df["total_kills"] > HIGH_THRESHOLD).astype(int).values
    y_val_high = (val_df["total_kills"] > HIGH_THRESHOLD).astype(int).values
    y_test_high = (test_df["total_kills"] > HIGH_THRESHOLD).astype(int).values

    def cfg_from_pos_weight(y: np.ndarray) -> Dict[str, Any]:
        pos = float(y.sum())
        neg = float(len(y) - pos)
        pos_weight = neg / max(pos, 1.0)
        return dict(
            iterations=args.iterations,
            depth=args.depth,
            learning_rate=args.learning_rate,
            loss_function="Logloss",
            eval_metric="AUC",
            random_seed=42,
            early_stopping_rounds=200,
            verbose=False,
            scale_pos_weight=pos_weight,
        )

    sample_weight = None
    if args.focus_patch:
        base_weights = tkr.compute_patch_weights(
            train_df,
            args.focus_patch,
            args.patch_weight_decay,
            args.patch_weight_min,
            args.patch_weight_unknown,
        )
        base_weights *= tkr.compute_roster_lock_weights(train_df, args.roster_lock_weight)
        sample_weight = base_weights

    # Low classifier
    low_cfg = cfg_from_pos_weight(y_train_low)
    low_model = train_classifier(
        X_train,
        y_train_low,
        X_val,
        y_val_low,
        cat_indices,
        low_cfg,
        sample_weight=sample_weight,
    )
    low_val_proba = low_model.predict_proba(X_val)[:, 1]
    low_thr = pick_threshold(y_val_low, low_val_proba)

    low_test_proba = low_model.predict_proba(X_test)[:, 1]
    low_metrics = eval_binary(y_test_low, low_test_proba, low_thr)
    logger.info("LOW (<%d) val_thr=%.2f metrics=%s", LOW_THRESHOLD, low_thr, low_metrics)

    # High classifier
    high_cfg = cfg_from_pos_weight(y_train_high)
    high_model = train_classifier(
        X_train,
        y_train_high,
        X_val,
        y_val_high,
        cat_indices,
        high_cfg,
        sample_weight=sample_weight,
    )
    high_val_proba = high_model.predict_proba(X_val)[:, 1]
    high_thr = pick_threshold(y_val_high, high_val_proba)

    high_test_proba = high_model.predict_proba(X_test)[:, 1]
    high_metrics = eval_binary(y_test_high, high_test_proba, high_thr)
    logger.info("HIGH (>%d) val_thr=%.2f metrics=%s", HIGH_THRESHOLD, high_thr, high_metrics)

    if args.save_model:
        tkr.MODELS_DIR.mkdir(parents=True, exist_ok=True)
        low_path = tkr.MODELS_DIR / "live_cb_kills_low_cls.cbm"
        high_path = tkr.MODELS_DIR / "live_cb_kills_high_cls.cbm"
        low_model.save_model(str(low_path))
        high_model.save_model(str(high_path))

        meta = {
            "feature_cols": feature_cols,
            "cat_features": cat_cols,
            "cat_indices": cat_indices,
            "thresholds": {"low": low_thr, "high": high_thr},
            "low_threshold": LOW_THRESHOLD,
            "high_threshold": HIGH_THRESHOLD,
            "train_size": len(train_df),
            "val_size": len(val_df),
            "test_size": len(test_df),
            "test_start_time_min": int(test_df["start_time"].min()),
            "test_start_time_max": int(test_df["start_time"].max()),
            "metrics": {"low": low_metrics, "high": high_metrics},
        }
        meta_path = tkr.MODELS_DIR / "live_cb_kills_low_high_cls_meta.json"
        with meta_path.open("w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)
        logger.info("Saved models: %s | %s", low_path, high_path)
        logger.info("Saved meta: %s", meta_path)

        if args.focus_patch and args.save_focus_as_patch:
            suffix = tkr.patch_label_to_slug(args.focus_patch)
            low_focus = tkr.MODELS_DIR / f"live_cb_kills_low_cls_patch_{suffix}.cbm"
            high_focus = tkr.MODELS_DIR / f"live_cb_kills_high_cls_patch_{suffix}.cbm"
            low_model.save_model(str(low_focus))
            high_model.save_model(str(high_focus))
            logger.info("Saved focus patch cls models: %s | %s", low_focus, high_focus)

    if args.by_patch or args.by_tier:
        if args.by_patch:
            train_group_classifiers(
                df,
                "patch_major_label",
                "patch",
                feature_cols,
                cat_cols,
                cfg_from_pos_weight,
                args.test_size,
                args.val_size,
                args.save_model,
            )
        if args.by_tier:
            tier_df = df
            if "match_tier_known" in df.columns:
                tier_df = df[df["match_tier_known"] == 1].copy()
            train_group_classifiers(
                tier_df,
                "match_tier",
                "tier",
                feature_cols,
                cat_cols,
                cfg_from_pos_weight,
                args.test_size,
                args.val_size,
                args.save_model,
            )


if __name__ == "__main__":
    main()
