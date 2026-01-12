#!/usr/bin/env python3
"""
Train Winrate (radiant_win) classifier v4.

This version is specifically aligned with production winrate inference:
- Uses LivePredictor.build_winrate_features() (stable, pre-game, high coverage).
- Generates features through the same code used in production to avoid offline/online skew.
- Uses a time-based split:
  - Prefer `start_time` when present; else fall back to `match_id` as a proxy.
- Saves artifacts as preferred production files:
  - ml-models/winrate_classifier_v4.cbm
  - ml-models/winrate_classifier_v4_meta.json

Usage:
  ./venv_catboost/bin/python ml-models/train_winrate_classifier_v4.py \
      --data data/pro_matches_enriched.csv \
      --models-dir ml-models \
      --test-size 500

Optional:
  --no-team-ids          Disable team_id usage in build_winrate_features (not recommended for coverage/quality)
  --iterations 4000      CatBoost iterations cap (early stopping still applies)
  --depth 6              Tree depth
  --learning-rate 0.02   Learning rate
  --l2 6                 L2 leaf regularization
  --seed 42              Random seed

Notes:
- This script does NOT consume post-match columns as features.
- It uses only:
  - hero ids (always available)
  - team ids (optional)
  - ratings/tier (derived from team_id + static rating DB in LivePredictor)
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score

# This file lives in: ingame/ml-models/train_winrate_classifier_v4.py
# The directory that contains `src/` is: ingame/
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.live_predictor import LivePredictor  # noqa: E402
from src.utils.team_ratings import Glicko2Rating, update_glicko2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("train_winrate_classifier_v4")


DATA_PATH_DEFAULT = "data/pro_matches_enriched.csv"
MODELS_DIR_DEFAULT = "ml-models"


ROLLING_DNA_COLS = [
    "radiant_dna_avg_kills",
    "radiant_dna_avg_deaths",
    "radiant_dna_aggression",
    "radiant_dna_pace",
    "radiant_dna_feed",
    "radiant_dna_avg_duration",
    "radiant_dna_versatility",
    "radiant_dna_kda",
    "radiant_dna_coverage",
    "dire_dna_avg_kills",
    "dire_dna_avg_deaths",
    "dire_dna_aggression",
    "dire_dna_pace",
    "dire_dna_feed",
    "dire_dna_avg_duration",
    "dire_dna_versatility",
    "dire_dna_kda",
    "dire_dna_coverage",
    "combined_dna_kills",
    "combined_dna_deaths",
    "combined_dna_aggression",
    "combined_dna_pace",
    "dna_kills_diff",
    "dna_aggression_diff",
    "dna_pace_diff",
    "dna_duration_diff",
    "dna_pace_clash",
    "high_dna_aggression",
    "low_dna_aggression",
    "combined_dna_coverage",
    "high_dna_coverage",
    "roster_confidence",
    "radiant_roster_coverage",
    "dire_roster_coverage",
    "synthetic_kills_diff",
    "synthetic_pace_diff",
]


@dataclass
class SplitConfig:
    test_size: int


def _coerce_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, np.integer)):
            return int(v)
        s = str(v).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


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


_Q_GLICKO = math.log(10) / 400


def _calc_glicko_win_prob(
    rating1: float,
    rd1: float,
    rating2: float,
    rd2: float,
) -> float:
    combined_rd = math.sqrt((rd1 ** 2) + (rd2 ** 2))
    g_rd = 1.0 / math.sqrt(
        1.0 + 3.0 * (_Q_GLICKO**2) * (combined_rd**2) / (math.pi**2)
    )
    return 1.0 / (1.0 + 10 ** (-g_rd * (rating1 - rating2) / 400.0))


def _apply_time_aware_glicko(
    features: Dict[str, Any],
    r_rating: float,
    r_rd: float,
    d_rating: float,
    d_rd: float,
) -> None:
    r_rating_f = float(r_rating)
    d_rating_f = float(d_rating)
    r_rd_f = float(r_rd)
    d_rd_f = float(d_rd)

    diff = r_rating_f - d_rating_f
    avg = (r_rating_f + d_rating_f) / 2.0

    features["radiant_glicko_rating"] = r_rating_f
    features["dire_glicko_rating"] = d_rating_f
    features["glicko_rating_diff"] = diff
    features["radiant_glicko_rd"] = r_rd_f
    features["dire_glicko_rd"] = d_rd_f
    features["glicko_rating_win_prob"] = _calc_glicko_win_prob(
        r_rating_f, r_rd_f, d_rating_f, d_rd_f
    )
    features["both_teams_reliable"] = 1.0 if r_rd_f < 150 and d_rd_f < 150 else 0.0

    features["avg_glicko_rating"] = avg
    if avg >= 1700:
        features["glicko_tier"] = 2
    elif avg >= 1550:
        features["glicko_tier"] = 1
    else:
        features["glicko_tier"] = 0

    features["glicko_rating_diff_abs"] = abs(diff)
    features["glicko_rd_sum"] = r_rd_f + d_rd_f
    features["glicko_reliability_score"] = 1.0 / max(
        1.0, (features["glicko_rd_sum"] / 100.0)
    )


def add_time_aware_glicko_columns(
    df: pd.DataFrame,
    include_team_ids: bool,
    rating_half_life_days: float = 0.0,
    rd_per_day: float = 0.0,
    roster_reset_factor: float = 0.0,
    min_core_players: int = 4,
) -> pd.DataFrame:
    if not include_team_ids:
        return df
    if "radiant_team_id" not in df.columns or "dire_team_id" not in df.columns:
        return df

    has_start_time = "start_time" in df.columns

    ratings: Dict[int, Glicko2Rating] = {}
    last_match_ts: Dict[int, int] = {}
    last_lineup: Dict[int, set] = {}

    r_rating: List[float] = []
    d_rating: List[float] = []
    r_rd: List[float] = []
    d_rd: List[float] = []

    for _, row in df.iterrows():
        ts: Optional[int] = None
        if has_start_time:
            ts = _coerce_int(row.get("start_time"))

        rid = _coerce_int(row.get("radiant_team_id"))
        did = _coerce_int(row.get("dire_team_id"))
        if rid is None or did is None or rid <= 0 or did <= 0:
            r_rating.append(1500.0)
            d_rating.append(1500.0)
            r_rd.append(350.0)
            d_rd.append(350.0)
            continue

        if rid not in ratings:
            ratings[rid] = Glicko2Rating()
        if did not in ratings:
            ratings[did] = Glicko2Rating()

        def apply_time_adjustments(team_id: int) -> None:
            if ts is None:
                return
            prev = last_match_ts.get(team_id)
            if prev is None:
                return
            if ts <= prev:
                return
            dt_days = (ts - prev) / 86400.0
            if dt_days <= 0:
                return

            t = ratings[team_id]
            if rating_half_life_days and rating_half_life_days > 0:
                decay = 0.5 ** (dt_days / float(rating_half_life_days))
                t.rating = 1500.0 + (t.rating - 1500.0) * decay

            if rd_per_day and rd_per_day > 0:
                t.rd = min(350.0, math.sqrt((t.rd**2) + (float(rd_per_day) ** 2) * dt_days))

            ratings[team_id] = t

        apply_time_adjustments(rid)
        apply_time_adjustments(did)

        def get_lineup(prefix: str) -> set:
            ids = set()
            for pos in range(1, 6):
                pid = _coerce_int(row.get(f"{prefix}_player_{pos}_id"))
                if pid is not None and pid > 0:
                    ids.add(pid)
            return ids

        if roster_reset_factor and roster_reset_factor > 0:
            r_players = get_lineup("radiant")
            d_players = get_lineup("dire")

            def maybe_reset(team_id: int, new_players: set) -> None:
                prev = last_lineup.get(team_id)
                if not prev or not new_players:
                    return
                overlap = len(prev & new_players)
                if overlap < int(min_core_players):
                    t = ratings[team_id]
                    t.rating = t.rating * (1.0 - float(roster_reset_factor)) + 1500.0 * float(
                        roster_reset_factor
                    )
                    ratings[team_id] = t

            maybe_reset(rid, r_players)
            maybe_reset(did, d_players)

            last_lineup[rid] = r_players
            last_lineup[did] = d_players

        tr = ratings[rid]
        td = ratings[did]

        r_rating.append(float(tr.rating))
        d_rating.append(float(td.rating))
        r_rd.append(float(tr.rd))
        d_rd.append(float(td.rd))

        rw = int(row.get("radiant_win") or 0)
        score_r = 1.0 if rw == 1 else 0.0
        score_d = 1.0 - score_r

        new_tr = update_glicko2(tr, td, score_r)
        new_td = update_glicko2(td, tr, score_d)
        ratings[rid] = new_tr
        ratings[did] = new_td

        if ts is not None:
            last_match_ts[rid] = ts
            last_match_ts[did] = ts

    out = df.copy()
    out["ta_radiant_glicko_rating"] = r_rating
    out["ta_dire_glicko_rating"] = d_rating
    out["ta_radiant_glicko_rd"] = r_rd
    out["ta_dire_glicko_rd"] = d_rd
    return out


def swap_sides_df(df: pd.DataFrame, include_team_ids: bool) -> pd.DataFrame:
    out = df.copy()

    for i in range(1, 6):
        r_col = f"radiant_hero_{i}"
        d_col = f"dire_hero_{i}"
        if r_col in out.columns and d_col in out.columns:
            tmp = out[r_col].copy()
            out[r_col] = out[d_col]
            out[d_col] = tmp

    if include_team_ids and ("radiant_team_id" in out.columns) and ("dire_team_id" in out.columns):
        tmp_tid = out["radiant_team_id"].copy()
        out["radiant_team_id"] = out["dire_team_id"]
        out["dire_team_id"] = tmp_tid

    if "ta_radiant_glicko_rating" in out.columns and "ta_dire_glicko_rating" in out.columns:
        tmp_r = out["ta_radiant_glicko_rating"].copy()
        out["ta_radiant_glicko_rating"] = out["ta_dire_glicko_rating"]
        out["ta_dire_glicko_rating"] = tmp_r

    if "ta_radiant_glicko_rd" in out.columns and "ta_dire_glicko_rd" in out.columns:
        tmp_r = out["ta_radiant_glicko_rd"].copy()
        out["ta_radiant_glicko_rd"] = out["ta_dire_glicko_rd"]
        out["ta_dire_glicko_rd"] = tmp_r

    for suffix in (
        "avg_kills",
        "avg_deaths",
        "aggression",
        "pace",
        "feed",
        "avg_duration",
        "versatility",
        "kda",
        "coverage",
    ):
        r_col = f"radiant_dna_{suffix}"
        d_col = f"dire_dna_{suffix}"
        if r_col in out.columns and d_col in out.columns:
            tmp = out[r_col].copy()
            out[r_col] = out[d_col]
            out[d_col] = tmp

    if "radiant_roster_coverage" in out.columns and "dire_roster_coverage" in out.columns:
        tmp = out["radiant_roster_coverage"].copy()
        out["radiant_roster_coverage"] = out["dire_roster_coverage"]
        out["dire_roster_coverage"] = tmp

    for diff_col in (
        "dna_kills_diff",
        "dna_aggression_diff",
        "dna_pace_diff",
        "dna_duration_diff",
        "synthetic_kills_diff",
        "synthetic_pace_diff",
    ):
        if diff_col in out.columns:
            out[diff_col] = -pd.to_numeric(out[diff_col], errors="coerce")

    if "radiant_win" in out.columns:
        out["radiant_win"] = 1 - out["radiant_win"].astype(int)

    return out


def load_dataset(path: str) -> Tuple[pd.DataFrame, str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"dataset not found: {path}")

    df = pd.read_csv(path)

    required = {"match_id", "radiant_win"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"dataset missing columns: {sorted(missing)}")

    # Determine time key
    if "start_time" in df.columns:
        df["__sort_key"] = pd.to_numeric(df["start_time"], errors="coerce")
        time_key = "start_time"
    else:
        df["__sort_key"] = pd.to_numeric(df["match_id"], errors="coerce")
        time_key = "match_id"

    df = df.dropna(subset=["__sort_key"]).copy()
    df = df.sort_values("__sort_key").reset_index(drop=True)

    # Ensure hero columns exist
    for side in ("radiant", "dire"):
        for i in range(1, 6):
            col = f"{side}_hero_{i}"
            if col not in df.columns:
                raise ValueError(f"dataset missing hero column: {col}")

    # Normalize ids/targets
    df["match_id"] = df["match_id"].apply(_coerce_int)
    df = df[df["match_id"].notna()].copy()
    df["match_id"] = df["match_id"].astype("int64")

    df["radiant_win"] = df["radiant_win"].apply(_coerce_bool).astype(int)

    logger.info(f"Loaded {len(df)} rows from {path} (time key={time_key})")
    return df, time_key


def split_time_based(
    df: pd.DataFrame, cfg: SplitConfig
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if len(df) <= cfg.test_size:
        raise ValueError("test_size is >= dataset size")
    train = df.iloc[: -cfg.test_size].copy()
    test = df.iloc[-cfg.test_size :].copy()
    logger.info(
        f"Split: train={len(train)} test={len(test)} (test_size={cfg.test_size})"
    )
    return train, test


def generate_winrate_features(
    predictor: LivePredictor,
    df: pd.DataFrame,
    include_team_ids: bool,
    include_rolling_dna: bool = False,
) -> pd.DataFrame:
    feats: List[Dict[str, Any]] = []

    has_team_ids = "radiant_team_id" in df.columns and "dire_team_id" in df.columns
    has_time_aware = (
        "ta_radiant_glicko_rating" in df.columns
        and "ta_dire_glicko_rating" in df.columns
        and "ta_radiant_glicko_rd" in df.columns
        and "ta_dire_glicko_rd" in df.columns
    )

    for _, row in df.iterrows():
        radiant_ids = [int(row[f"radiant_hero_{i}"]) for i in range(1, 6)]
        dire_ids = [int(row[f"dire_hero_{i}"]) for i in range(1, 6)]

        r_tid: Optional[int] = None
        d_tid: Optional[int] = None
        if include_team_ids and has_team_ids:
            r_tid = _coerce_int(row.get("radiant_team_id"))
            d_tid = _coerce_int(row.get("dire_team_id"))

        f = predictor.build_winrate_features(
            radiant_ids=radiant_ids,
            dire_ids=dire_ids,
            radiant_team_id=r_tid,
            dire_team_id=d_tid,
        )

        if include_team_ids and has_time_aware:
            _apply_time_aware_glicko(
                f,
                r_rating=float(row.get("ta_radiant_glicko_rating", 1500.0)),
                r_rd=float(row.get("ta_radiant_glicko_rd", 350.0)),
                d_rating=float(row.get("ta_dire_glicko_rating", 1500.0)),
                d_rd=float(row.get("ta_dire_glicko_rd", 350.0)),
            )

        if include_rolling_dna:
            for col in ROLLING_DNA_COLS:
                if col in df.columns:
                    f[col] = row.get(col)
        feats.append(f)

    X = pd.DataFrame(feats)
    return X


def detect_cat_features(feature_cols: List[str], include_team_ids: bool) -> List[str]:
    cat: List[str] = []
    for side in ("radiant", "dire"):
        for pos in range(1, 6):
            cat.append(f"{side}_hero_{pos}")
    if include_team_ids:
        cat += ["radiant_team_id", "dire_team_id"]
    return [c for c in cat if c in feature_cols]


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
    iterations: int,
    depth: int,
    learning_rate: float,
    l2_leaf_reg: float,
) -> Tuple[CatBoostClassifier, Dict[str, float]]:
    feature_cols = list(X_train.columns)
    cat_indices = [feature_cols.index(c) for c in cat_features if c in feature_cols]

    model = CatBoostClassifier(
        iterations=iterations,
        learning_rate=learning_rate,
        depth=depth,
        l2_leaf_reg=l2_leaf_reg,
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
    metrics: Dict[str, float],
    train_size: int,
    test_size: int,
    time_key: str,
    include_team_ids: bool,
    params: Dict[str, Any],
) -> None:
    models_dir.mkdir(parents=True, exist_ok=True)

    model_path = models_dir / "winrate_classifier_v4.cbm"
    meta_path = models_dir / "winrate_classifier_v4_meta.json"

    model.save_model(str(model_path))

    cat_indices = [feature_cols.index(c) for c in cat_features if c in feature_cols]

    meta = {
        "feature_cols": feature_cols,
        "cat_features": cat_features,
        "cat_indices": cat_indices,
        "train_size": train_size,
        "test_size": test_size,
        "auc_estimate": metrics.get("auc"),
        "accuracy_estimate": metrics.get("accuracy"),
        "logloss_estimate": metrics.get("logloss"),
        "auc_sym_estimate": metrics.get("auc_sym"),
        "accuracy_sym_estimate": metrics.get("accuracy_sym"),
        "logloss_sym_estimate": metrics.get("logloss_sym"),
        "test_size_sym": metrics.get("test_size_sym"),
        "training": {
            "time_key": time_key,
            "split": "time-based: oldest->train, newest->test",
            "features_source": "LivePredictor.build_winrate_features",
            "include_team_ids": include_team_ids,
            "params": params,
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
        help="Output directory relative to project root (ingame/)",
    )
    ap.add_argument(
        "--test-size", type=int, default=500, help="Newest N matches used as test set"
    )
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--iterations", type=int, default=4000)
    ap.add_argument("--depth", type=int, default=6)
    ap.add_argument("--learning-rate", type=float, default=0.02)
    ap.add_argument("--l2", type=float, default=6.0)
    ap.add_argument(
        "--no-team-ids",
        action="store_true",
        help="Disable team_id usage (reduces coverage/quality in pro matches)",
    )
    ap.add_argument("--no-time-aware-glicko", action="store_true")
    ap.add_argument("--ta-glicko-half-life-days", type=float, default=0.0)
    ap.add_argument("--ta-glicko-rd-per-day", type=float, default=0.0)
    ap.add_argument("--ta-glicko-roster-reset", type=float, default=0.0)
    ap.add_argument("--ta-glicko-min-core", type=int, default=4)
    ap.add_argument("--symmetric-train", action="store_true")
    ap.add_argument("--symmetric-eval", action="store_true")
    ap.add_argument("--include-rolling-dna", action="store_true")
    args = ap.parse_args()

    df, time_key = load_dataset(args.data)
    include_team_ids = not args.no_team_ids

    if include_team_ids and (not args.no_time_aware_glicko):
        logger.info("Computing time-aware team ratings (Glicko-2)...")
        df = add_time_aware_glicko_columns(
            df,
            include_team_ids=include_team_ids,
            rating_half_life_days=float(args.ta_glicko_half_life_days),
            rd_per_day=float(args.ta_glicko_rd_per_day),
            roster_reset_factor=float(args.ta_glicko_roster_reset),
            min_core_players=int(args.ta_glicko_min_core),
        )

    train_df, test_df = split_time_based(df, SplitConfig(test_size=args.test_size))

    if args.symmetric_train:
        swapped_train = swap_sides_df(train_df, include_team_ids=include_team_ids)
        train_df = pd.concat([train_df, swapped_train], ignore_index=True)
        logger.info(f"Symmetric train enabled: train_size={len(train_df)}")

    # Initialize predictor (loads static stats needed to compute features)
    predictor = LivePredictor()

    logger.info("Generating winrate features (train)...")
    X_train_df = generate_winrate_features(
        predictor,
        train_df,
        include_team_ids=include_team_ids,
        include_rolling_dna=bool(args.include_rolling_dna),
    )
    logger.info("Generating winrate features (test)...")
    X_test_df = generate_winrate_features(
        predictor,
        test_df,
        include_team_ids=include_team_ids,
        include_rolling_dna=bool(args.include_rolling_dna),
    )

    X_test_sym_df: Optional[pd.DataFrame] = None
    y_test_sym: Optional[np.ndarray] = None
    if args.symmetric_eval:
        swapped_test = swap_sides_df(test_df, include_team_ids=include_team_ids)
        test_sym_df = pd.concat([test_df, swapped_test], ignore_index=True)
        logger.info(
            f"Symmetric eval enabled: test_size={len(test_sym_df)} (orig={len(test_df)})"
        )
        X_test_sym_df = generate_winrate_features(
            predictor,
            test_sym_df,
            include_team_ids=include_team_ids,
            include_rolling_dna=bool(args.include_rolling_dna),
        )
        y_test_sym = test_sym_df["radiant_win"].astype(int).values

    # Align columns (intersection) to avoid train/test drift if something is missing
    feature_cols = sorted(list(set(X_train_df.columns) & set(X_test_df.columns)))
    if not feature_cols:
        raise RuntimeError(
            "No overlapping feature columns between train and test feature frames"
        )

    cat_features = detect_cat_features(feature_cols, include_team_ids=include_team_ids)

    X_train = prepare_catboost_matrix(X_train_df, feature_cols, cat_features)
    X_test = prepare_catboost_matrix(X_test_df, feature_cols, cat_features)

    y_train = train_df["radiant_win"].astype(int).values
    y_test = test_df["radiant_win"].astype(int).values

    params = {
        "iterations": args.iterations,
        "depth": args.depth,
        "learning_rate": args.learning_rate,
        "l2_leaf_reg": args.l2,
        "seed": args.seed,
        "time_aware_glicko": bool(include_team_ids and (not args.no_time_aware_glicko)),
        "ta_glicko_half_life_days": float(args.ta_glicko_half_life_days),
        "ta_glicko_rd_per_day": float(args.ta_glicko_rd_per_day),
        "ta_glicko_roster_reset": float(args.ta_glicko_roster_reset),
        "ta_glicko_min_core": int(args.ta_glicko_min_core),
        "symmetric_train": bool(args.symmetric_train),
        "symmetric_eval": bool(args.symmetric_eval),
        "include_rolling_dna": bool(args.include_rolling_dna),
    }

    logger.info(
        f"Training winrate v4: features={len(feature_cols)} cat={len(cat_features)}"
    )
    model, metrics = train_model(
        X_train=X_train,
        y_train=y_train,
        X_test=X_test,
        y_test=y_test,
        cat_features=cat_features,
        seed=args.seed,
        iterations=args.iterations,
        depth=args.depth,
        learning_rate=args.learning_rate,
        l2_leaf_reg=args.l2,
    )

    logger.info(
        f"Test metrics: AUC={metrics['auc']:.4f} ACC={metrics['accuracy']:.4f} LogLoss={metrics['logloss']:.4f}"
    )

    if args.symmetric_eval and X_test_sym_df is not None and y_test_sym is not None:
        X_test_sym = prepare_catboost_matrix(X_test_sym_df, feature_cols, cat_features)
        proba_sym = model.predict_proba(X_test_sym)[:, 1]
        metrics["auc_sym"] = float(roc_auc_score(y_test_sym, proba_sym))
        metrics["accuracy_sym"] = float(
            accuracy_score(y_test_sym, (proba_sym >= 0.5).astype(int))
        )
        metrics["logloss_sym"] = float(log_loss(y_test_sym, proba_sym, labels=[0, 1]))
        metrics["test_size_sym"] = int(len(y_test_sym))
        logger.info(
            f"Sym test metrics: AUC={metrics['auc_sym']:.4f} ACC={metrics['accuracy_sym']:.4f} LogLoss={metrics['logloss_sym']:.4f}"
        )

    save_artifacts(
        model=model,
        models_dir=(PROJECT_ROOT / args.models_dir),
        feature_cols=feature_cols,
        cat_features=cat_features,
        metrics=metrics,
        train_size=len(train_df),
        test_size=len(test_df),
        time_key=time_key,
        include_team_ids=include_team_ids,
        params=params,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
