#!/usr/bin/env python3
"""
Walk-forward evaluation for low/high kill betting using time-based splits.

Trains LOW/HIGH classifiers per window (no leakage) and searches probability
thresholds on the validation window to maximize EV.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from catboost import CatBoostClassifier, CatBoostRegressor, Pool

import train_kills_regression_pro as tkr


def _drop_networth_features(cols: List[str]) -> List[str]:
    dropped = []
    kept = []
    for c in cols:
        lc = c.lower()
        if "networth" in lc or "net_worth" in lc or "_nw" in lc or lc.startswith("nw"):
            dropped.append(c)
            continue
        kept.append(c)
    return kept


def prepare_x(df, feature_cols: List[str], cat_cols: List[str]) -> Tuple[Any, List[int]]:
    X = df[feature_cols].copy()
    for c in cat_cols:
        if c in X.columns:
            X[c] = X[c].fillna("UNKNOWN").astype(str)
    cat_indices = [X.columns.get_loc(c) for c in cat_cols if c in X.columns]
    return X, cat_indices


def train_classifier(
    X_train,
    y_train,
    X_val,
    y_val,
    cat_indices: List[int],
    cfg: Dict[str, Any],
) -> CatBoostClassifier:
    model = CatBoostClassifier(**cfg)
    train_pool = Pool(X_train, y_train, cat_features=cat_indices)
    val_pool = Pool(X_val, y_val, cat_features=cat_indices)
    model.fit(train_pool, eval_set=val_pool, use_best_model=True)
    return model


def eval_combo(
    prob_low: np.ndarray,
    prob_high: np.ndarray,
    y_true: np.ndarray,
    low_thr: float,
    high_thr: float,
    odds: float,
    *,
    strategy: str = "prob_only",
    pred_all: Optional[np.ndarray] = None,
    low_pred_thr: Optional[float] = None,
    high_pred_thr: Optional[float] = None,
    total10: Optional[np.ndarray] = None,
    low_total_thr: Optional[float] = None,
    high_total_thr: Optional[float] = None,
    low_margin: Optional[float] = None,
    high_margin: Optional[float] = None,
) -> Dict[str, float]:
    low_sig = prob_low >= low_thr
    high_sig = prob_high >= high_thr
    if strategy == "prob_margin":
        if low_margin is None or high_margin is None:
            raise ValueError("prob_margin requires low_margin and high_margin")
        low_sig = low_sig & ((prob_low - prob_high) >= low_margin)
        high_sig = high_sig & ((prob_high - prob_low) >= high_margin)
    elif strategy == "prob_reg_all":
        if pred_all is None or low_pred_thr is None or high_pred_thr is None:
            raise ValueError("prob_reg_all requires pred_all and pred thresholds")
        low_sig = low_sig & (pred_all <= low_pred_thr)
        high_sig = high_sig & (pred_all >= high_pred_thr)
    elif strategy == "prob_total10":
        if total10 is None or low_total_thr is None or high_total_thr is None:
            raise ValueError("prob_total10 requires total10 and total thresholds")
        low_sig = low_sig & (total10 <= low_total_thr)
        high_sig = high_sig & (total10 >= high_total_thr)
    bet_low = low_sig & (~high_sig)
    bet_high = high_sig & (~low_sig)
    bets = int(bet_low.sum() + bet_high.sum())
    if bets == 0:
        return {"bets": 0, "wins": 0, "losses": 0, "profit": 0.0, "ev": 0.0, "low_bets": 0, "high_bets": 0}
    wins = int(((bet_low & (y_true < 40)) | (bet_high & (y_true > 50))).sum())
    losses = bets - wins
    profit = wins * (odds - 1.0) - losses * 1.0
    ev = profit / bets
    return {
        "bets": bets,
        "wins": wins,
        "losses": losses,
        "profit": float(profit),
        "ev": float(ev),
        "low_bets": int(bet_low.sum()),
        "high_bets": int(bet_high.sum()),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--clean-path", type=str, default=str(tkr.DEFAULT_CLEAN_PATH))
    parser.add_argument("--test-size", type=int, default=100)
    parser.add_argument("--val-size", type=int, default=300)
    parser.add_argument("--last-n", type=int, default=400, help="Last N matches for walk-forward")
    parser.add_argument("--iterations", type=int, default=800)
    parser.add_argument("--depth", type=int, default=7)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--reg-iterations", type=int, default=800)
    parser.add_argument("--reg-depth", type=int, default=6)
    parser.add_argument("--reg-learning-rate", type=float, default=0.08)
    parser.add_argument("--odds", type=float, default=1.8)
    parser.add_argument("--min-val-bets", type=int, default=20)
    parser.add_argument("--min-low-bets", type=int, default=0, help="Minimum LOW bets in selection to accept a threshold")
    parser.add_argument("--min-high-bets", type=int, default=0, help="Minimum HIGH bets in selection to accept a threshold")
    parser.add_argument("--min-window-bets", type=int, default=0, help="Minimum bets per selection window")
    parser.add_argument("--min-window-low-bets", type=int, default=0, help="Minimum LOW bets per selection window")
    parser.add_argument("--min-window-high-bets", type=int, default=0, help="Minimum HIGH bets per selection window")
    parser.add_argument("--drop-networth", action="store_true", help="Exclude networth-related features")
    parser.add_argument(
        "--mode",
        choices=("per-window", "fixed"),
        default="per-window",
        help="per-window: tune thresholds per window; fixed: pick global thresholds on past windows and test on last",
    )
    parser.add_argument(
        "--objective",
        choices=("ev", "profit"),
        default="ev",
        help="Objective for threshold search",
    )
    parser.add_argument(
        "--strategy",
        choices=("prob_only", "prob_reg_all", "prob_total10", "prob_margin"),
        default="prob_only",
        help="Decision rule to evaluate",
    )
    parser.add_argument("--low-pred-grid", type=str, default="")
    parser.add_argument("--high-pred-grid", type=str, default="")
    parser.add_argument("--low-total-grid", type=str, default="")
    parser.add_argument("--high-total-grid", type=str, default="")
    parser.add_argument("--low-margin-grid", type=str, default="")
    parser.add_argument("--high-margin-grid", type=str, default="")
    parser.add_argument("--leak-check", action="store_true", help="Shuffle labels for sanity check")
    parser.add_argument("--leak-seed", type=int, default=42)
    args = parser.parse_args()

    matches = tkr.load_clean_data(Path(args.clean_path))
    pub_priors = tkr.build_pub_hero_priors(tkr.PUB_PLAYERS_DIR, tkr.PUB_PRIORS_PATH)
    df = tkr.build_dataset(matches, pub_priors)
    df = df.sort_values("start_time").reset_index(drop=True)

    feature_cols = [c for c in df.columns if c not in ("total_kills", "start_time")]
    feature_cols = tkr.select_feature_cols(feature_cols, False, Path(tkr.SELECTED_FEATURES_PATH))
    if args.drop_networth:
        feature_cols = _drop_networth_features(feature_cols)

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
            "bottom_lane_outcome",
            "mid_lane_outcome",
            "top_lane_outcome",
        )
    ]

    test_size = args.test_size
    val_size = args.val_size
    n = len(df)
    start = max(0, n - args.last_n)
    start_indices = list(range(start, n, test_size))

    low_probs = np.round(np.arange(0.35, 0.91, 0.05), 2).tolist()
    high_probs = low_probs.copy()

    def parse_grid(text: str) -> List[float]:
        if not text:
            return []
        out: List[float] = []
        for chunk in text.split(","):
            chunk = chunk.strip()
            if not chunk:
                continue
            try:
                out.append(float(chunk))
            except ValueError:
                continue
        return out

    low_pred_grid = parse_grid(args.low_pred_grid) or [38.0, 40.0, 42.0, 44.0]
    high_pred_grid = parse_grid(args.high_pred_grid) or [52.0, 54.0, 56.0, 58.0]
    low_total_grid = parse_grid(args.low_total_grid) or [8.0, 10.0, 12.0, 14.0, 16.0]
    high_total_grid = parse_grid(args.high_total_grid) or [14.0, 16.0, 18.0, 20.0, 22.0]
    low_margin_grid = parse_grid(args.low_margin_grid) or [0.0, 0.05, 0.1, 0.15, 0.2]
    high_margin_grid = parse_grid(args.high_margin_grid) or [0.0, 0.05, 0.1, 0.15, 0.2]

    agg = {"bets": 0, "wins": 0, "losses": 0, "profit": 0.0, "low_bets": 0, "high_bets": 0}
    windows = 0

    # Build windows
    windows_list: List[Tuple[slice, slice]] = []
    for test_start in start_indices:
        test_end = min(test_start + test_size, n)
        if test_end - test_start < test_size:
            continue
        val_start = test_start - val_size
        val_end = test_start
        if val_start < 0:
            continue
        windows_list.append((slice(val_start, val_end), slice(test_start, test_end)))

    if args.mode == "fixed":
        if len(windows_list) < 2:
            raise SystemExit("Need at least 2 windows for fixed mode")
        selection_windows = windows_list[:-1]
        final_window = windows_list[-1]

        selection_preds: List[Dict[str, np.ndarray]] = []

        for val_idx, test_idx in selection_windows:
            train_df = df.iloc[: val_idx.start].copy()
            val_df = df.iloc[val_idx].copy()
            test_df = df.iloc[test_idx].copy()

            if len(train_df) < 500:
                continue

            X_train, cat_indices = prepare_x(train_df, feature_cols, cat_cols)
            X_val, _ = prepare_x(val_df, feature_cols, cat_cols)
            X_test, _ = prepare_x(test_df, feature_cols, cat_cols)

            y_train_low = (train_df["total_kills"] < 40).astype(int).values
            y_train_high = (train_df["total_kills"] > 50).astype(int).values

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
                    early_stopping_rounds=100,
                    verbose=False,
                    scale_pos_weight=pos_weight,
                )

            low_model = train_classifier(
                X_train, y_train_low, X_val, (val_df["total_kills"] < 40).astype(int).values, cat_indices,
                cfg_from_pos_weight(y_train_low),
            )
            high_model = train_classifier(
                X_train, y_train_high, X_val, (val_df["total_kills"] > 50).astype(int).values, cat_indices,
                cfg_from_pos_weight(y_train_high),
            )

            prob_low_test = low_model.predict_proba(X_test)[:, 1]
            prob_high_test = high_model.predict_proba(X_test)[:, 1]
            entry: Dict[str, np.ndarray] = {
                "prob_low": prob_low_test,
                "prob_high": prob_high_test,
                "y_true": test_df["total_kills"].values,
            }
            if args.strategy == "prob_reg_all":
                reg_cfg = dict(
                    iterations=args.reg_iterations,
                    depth=args.reg_depth,
                    learning_rate=args.reg_learning_rate,
                    loss_function="MAE",
                    eval_metric="MAE",
                    random_seed=42,
                    early_stopping_rounds=100,
                    verbose=False,
                )
                reg = CatBoostRegressor(**reg_cfg)
                reg.fit(
                    Pool(X_train, train_df["total_kills"].values, cat_features=cat_indices),
                    eval_set=Pool(X_val, val_df["total_kills"].values, cat_features=cat_indices),
                    use_best_model=True,
                )
                entry["pred_all"] = reg.predict(X_test)
            if args.strategy == "prob_total10":
                entry["total10"] = test_df["total10"].values
            selection_preds.append(entry)

        best = None
        best_pair = None
        for lp in low_probs:
            for hp in high_probs:
                if args.strategy == "prob_reg_all":
                    for lpred in low_pred_grid:
                        for hpred in high_pred_grid:
                            total = {
                                "bets": 0,
                                "wins": 0,
                                "losses": 0,
                                "profit": 0.0,
                                "low_bets": 0,
                                "high_bets": 0,
                            }
                            window_ok = True
                            for entry in selection_preds:
                                metrics = eval_combo(
                                    entry["prob_low"],
                                    entry["prob_high"],
                                    entry["y_true"],
                                    lp,
                                    hp,
                                    args.odds,
                                    strategy=args.strategy,
                                    pred_all=entry.get("pred_all"),
                                    low_pred_thr=lpred,
                                    high_pred_thr=hpred,
                                )
                                if (
                                    metrics["bets"] < args.min_window_bets
                                    or metrics["low_bets"] < args.min_window_low_bets
                                    or metrics["high_bets"] < args.min_window_high_bets
                                ):
                                    window_ok = False
                                    break
                                if metrics["bets"] == 0:
                                    continue
                                for k in total:
                                    total[k] += metrics[k]
                            if not window_ok:
                                continue
                            if total["bets"] < args.min_val_bets:
                                continue
                            if total["low_bets"] < args.min_low_bets or total["high_bets"] < args.min_high_bets:
                                continue
                            ev = total["profit"] / total["bets"]
                            score = ev if args.objective == "ev" else total["profit"]
                            if best is None or score > best["score"]:
                                best = {**total, "ev": ev, "score": score}
                                best_pair = (lp, hp, lpred, hpred)
                elif args.strategy == "prob_total10":
                    for ltot in low_total_grid:
                        for htot in high_total_grid:
                            total = {
                                "bets": 0,
                                "wins": 0,
                                "losses": 0,
                                "profit": 0.0,
                                "low_bets": 0,
                                "high_bets": 0,
                            }
                            window_ok = True
                            for entry in selection_preds:
                                metrics = eval_combo(
                                    entry["prob_low"],
                                    entry["prob_high"],
                                    entry["y_true"],
                                    lp,
                                    hp,
                                    args.odds,
                                    strategy=args.strategy,
                                    total10=entry.get("total10"),
                                    low_total_thr=ltot,
                                    high_total_thr=htot,
                                )
                                if (
                                    metrics["bets"] < args.min_window_bets
                                    or metrics["low_bets"] < args.min_window_low_bets
                                    or metrics["high_bets"] < args.min_window_high_bets
                                ):
                                    window_ok = False
                                    break
                                if metrics["bets"] == 0:
                                    continue
                                for k in total:
                                    total[k] += metrics[k]
                            if not window_ok:
                                continue
                            if total["bets"] < args.min_val_bets:
                                continue
                            if total["low_bets"] < args.min_low_bets or total["high_bets"] < args.min_high_bets:
                                continue
                            ev = total["profit"] / total["bets"]
                            score = ev if args.objective == "ev" else total["profit"]
                            if best is None or score > best["score"]:
                                best = {**total, "ev": ev, "score": score}
                                best_pair = (lp, hp, ltot, htot)
                elif args.strategy == "prob_margin":
                    for lmargin in low_margin_grid:
                        for hmargin in high_margin_grid:
                            total = {
                                "bets": 0,
                                "wins": 0,
                                "losses": 0,
                                "profit": 0.0,
                                "low_bets": 0,
                                "high_bets": 0,
                            }
                            window_ok = True
                            for entry in selection_preds:
                                metrics = eval_combo(
                                    entry["prob_low"],
                                    entry["prob_high"],
                                    entry["y_true"],
                                    lp,
                                    hp,
                                    args.odds,
                                    strategy=args.strategy,
                                    low_margin=lmargin,
                                    high_margin=hmargin,
                                )
                                if (
                                    metrics["bets"] < args.min_window_bets
                                    or metrics["low_bets"] < args.min_window_low_bets
                                    or metrics["high_bets"] < args.min_window_high_bets
                                ):
                                    window_ok = False
                                    break
                                if metrics["bets"] == 0:
                                    continue
                                for k in total:
                                    total[k] += metrics[k]
                            if not window_ok:
                                continue
                            if total["bets"] < args.min_val_bets:
                                continue
                            if total["low_bets"] < args.min_low_bets or total["high_bets"] < args.min_high_bets:
                                continue
                            ev = total["profit"] / total["bets"]
                            score = ev if args.objective == "ev" else total["profit"]
                            if best is None or score > best["score"]:
                                best = {**total, "ev": ev, "score": score}
                                best_pair = (lp, hp, lmargin, hmargin)
                else:
                    total = {"bets": 0, "wins": 0, "losses": 0, "profit": 0.0, "low_bets": 0, "high_bets": 0}
                    window_ok = True
                    for entry in selection_preds:
                        metrics = eval_combo(
                            entry["prob_low"],
                            entry["prob_high"],
                            entry["y_true"],
                            lp,
                            hp,
                            args.odds,
                        )
                        if (
                            metrics["bets"] < args.min_window_bets
                            or metrics["low_bets"] < args.min_window_low_bets
                            or metrics["high_bets"] < args.min_window_high_bets
                        ):
                            window_ok = False
                            break
                        if metrics["bets"] == 0:
                            continue
                        for k in total:
                            total[k] += metrics[k]
                    if not window_ok:
                        continue
                    if total["bets"] < args.min_val_bets:
                        continue
                    if total["low_bets"] < args.min_low_bets or total["high_bets"] < args.min_high_bets:
                        continue
                    ev = total["profit"] / total["bets"]
                    score = ev if args.objective == "ev" else total["profit"]
                    if best is None or score > best["score"]:
                        best = {**total, "ev": ev, "score": score}
                        best_pair = (lp, hp)

        if best_pair is None:
            raise SystemExit("No threshold pair meets constraints for fixed mode")

        # Final window evaluation
        val_idx, test_idx = final_window
        train_df = df.iloc[: val_idx.start].copy()
        val_df = df.iloc[val_idx].copy()
        test_df = df.iloc[test_idx].copy()

        X_train, cat_indices = prepare_x(train_df, feature_cols, cat_cols)
        X_val, _ = prepare_x(val_df, feature_cols, cat_cols)
        X_test, _ = prepare_x(test_df, feature_cols, cat_cols)

        y_train_low = (train_df["total_kills"] < 40).astype(int).values
        y_train_high = (train_df["total_kills"] > 50).astype(int).values

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
                early_stopping_rounds=100,
                verbose=False,
                scale_pos_weight=pos_weight,
            )

        low_model = train_classifier(
            X_train, y_train_low, X_val, (val_df["total_kills"] < 40).astype(int).values, cat_indices,
            cfg_from_pos_weight(y_train_low),
        )
        high_model = train_classifier(
            X_train, y_train_high, X_val, (val_df["total_kills"] > 50).astype(int).values, cat_indices,
            cfg_from_pos_weight(y_train_high),
        )

        prob_low_test = low_model.predict_proba(X_test)[:, 1]
        prob_high_test = high_model.predict_proba(X_test)[:, 1]
        final_y = test_df["total_kills"].values
        pred_all_test = None
        total10_test = None
        if args.strategy == "prob_reg_all":
            reg_cfg = dict(
                iterations=args.reg_iterations,
                depth=args.reg_depth,
                learning_rate=args.reg_learning_rate,
                loss_function="MAE",
                eval_metric="MAE",
                random_seed=42,
                early_stopping_rounds=100,
                verbose=False,
            )
            reg = CatBoostRegressor(**reg_cfg)
            reg.fit(
                Pool(X_train, train_df["total_kills"].values, cat_features=cat_indices),
                eval_set=Pool(X_val, val_df["total_kills"].values, cat_features=cat_indices),
                use_best_model=True,
            )
            pred_all_test = reg.predict(X_test)
        if args.strategy == "prob_total10":
            total10_test = test_df["total10"].values
        final_metrics = eval_combo(
            prob_low_test,
            prob_high_test,
            final_y,
            best_pair[0],
            best_pair[1],
            args.odds,
            strategy=args.strategy,
            pred_all=pred_all_test,
            low_pred_thr=best_pair[2] if args.strategy == "prob_reg_all" else None,
            high_pred_thr=best_pair[3] if args.strategy == "prob_reg_all" else None,
            total10=total10_test,
            low_total_thr=best_pair[2] if args.strategy == "prob_total10" else None,
            high_total_thr=best_pair[3] if args.strategy == "prob_total10" else None,
            low_margin=best_pair[2] if args.strategy == "prob_margin" else None,
            high_margin=best_pair[3] if args.strategy == "prob_margin" else None,
        )
        leak_metrics = None
        if args.leak_check:
            rng = np.random.default_rng(args.leak_seed)
            shuffled = np.array(final_y, copy=True)
            rng.shuffle(shuffled)
            leak_metrics = eval_combo(
                prob_low_test,
                prob_high_test,
                shuffled,
                best_pair[0],
                best_pair[1],
                args.odds,
                strategy=args.strategy,
                pred_all=pred_all_test,
                low_pred_thr=best_pair[2] if args.strategy == "prob_reg_all" else None,
                high_pred_thr=best_pair[3] if args.strategy == "prob_reg_all" else None,
                total10=total10_test,
                low_total_thr=best_pair[2] if args.strategy == "prob_total10" else None,
                high_total_thr=best_pair[3] if args.strategy == "prob_total10" else None,
                low_margin=best_pair[2] if args.strategy == "prob_margin" else None,
                high_margin=best_pair[3] if args.strategy == "prob_margin" else None,
            )

        print(
            json.dumps(
                {
                    "mode": "fixed",
                    "selection_windows": len(selection_preds),
                    "best_low_thr": best_pair[0],
                    "best_high_thr": best_pair[1],
                    "best_low_gate": best_pair[2] if len(best_pair) > 2 else None,
                    "best_high_gate": best_pair[3] if len(best_pair) > 3 else None,
                    "strategy": args.strategy,
                    "objective": args.objective,
                    "selection_agg": best,
                    "final_window": {
                        "start_time_min": int(test_df["start_time"].min()),
                        "start_time_max": int(test_df["start_time"].max()),
                        "metrics": final_metrics,
                        "leak_check": leak_metrics,
                    },
                },
                ensure_ascii=True,
            )
        )
        return

    for val_idx, test_idx in windows_list:
        train_df = df.iloc[: val_idx.start].copy()
        val_df = df.iloc[val_idx].copy()
        test_df = df.iloc[test_idx].copy()

        if len(train_df) < 500:
            continue

        X_train, cat_indices = prepare_x(train_df, feature_cols, cat_cols)
        X_val, _ = prepare_x(val_df, feature_cols, cat_cols)
        X_test, _ = prepare_x(test_df, feature_cols, cat_cols)

        y_train_low = (train_df["total_kills"] < 40).astype(int).values
        y_val_low = (val_df["total_kills"] < 40).astype(int).values
        y_test = test_df["total_kills"].values

        y_train_high = (train_df["total_kills"] > 50).astype(int).values
        y_val_high = (val_df["total_kills"] > 50).astype(int).values

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
                early_stopping_rounds=100,
                verbose=False,
                scale_pos_weight=pos_weight,
            )

        low_model = train_classifier(X_train, y_train_low, X_val, y_val_low, cat_indices, cfg_from_pos_weight(y_train_low))
        high_model = train_classifier(
            X_train, y_train_high, X_val, y_val_high, cat_indices, cfg_from_pos_weight(y_train_high)
        )

        prob_low_val = low_model.predict_proba(X_val)[:, 1]
        prob_high_val = high_model.predict_proba(X_val)[:, 1]
        prob_low_test = low_model.predict_proba(X_test)[:, 1]
        prob_high_test = high_model.predict_proba(X_test)[:, 1]
        pred_all_val = None
        pred_all_test = None
        if args.strategy == "prob_reg_all":
            reg_cfg = dict(
                iterations=args.reg_iterations,
                depth=args.reg_depth,
                learning_rate=args.reg_learning_rate,
                loss_function="MAE",
                eval_metric="MAE",
                random_seed=42,
                early_stopping_rounds=100,
                verbose=False,
            )
            reg = CatBoostRegressor(**reg_cfg)
            reg.fit(
                Pool(X_train, train_df["total_kills"].values, cat_features=cat_indices),
                eval_set=Pool(X_val, val_df["total_kills"].values, cat_features=cat_indices),
                use_best_model=True,
            )
            pred_all_val = reg.predict(X_val)
            pred_all_test = reg.predict(X_test)
        total10_val = val_df["total10"].values if args.strategy == "prob_total10" else None
        total10_test = test_df["total10"].values if args.strategy == "prob_total10" else None

        best = None
        best_pair = None
        for lp in low_probs:
            for hp in high_probs:
                if args.strategy == "prob_reg_all":
                    for lpred in low_pred_grid:
                        for hpred in high_pred_grid:
                            metrics_val = eval_combo(
                                prob_low_val,
                                prob_high_val,
                                val_df["total_kills"].values,
                                lp,
                                hp,
                                args.odds,
                                strategy=args.strategy,
                                pred_all=pred_all_val,
                                low_pred_thr=lpred,
                                high_pred_thr=hpred,
                            )
                            if metrics_val["bets"] < args.min_val_bets:
                                continue
                            if metrics_val["low_bets"] < args.min_low_bets or metrics_val["high_bets"] < args.min_high_bets:
                                continue
                            if best is None or metrics_val["ev"] > best["ev"]:
                                best = metrics_val
                                best_pair = (lp, hp, lpred, hpred)
                elif args.strategy == "prob_total10":
                    for ltot in low_total_grid:
                        for htot in high_total_grid:
                            metrics_val = eval_combo(
                                prob_low_val,
                                prob_high_val,
                                val_df["total_kills"].values,
                                lp,
                                hp,
                                args.odds,
                                strategy=args.strategy,
                                total10=total10_val,
                                low_total_thr=ltot,
                                high_total_thr=htot,
                            )
                            if metrics_val["bets"] < args.min_val_bets:
                                continue
                            if metrics_val["low_bets"] < args.min_low_bets or metrics_val["high_bets"] < args.min_high_bets:
                                continue
                            if best is None or metrics_val["ev"] > best["ev"]:
                                best = metrics_val
                                best_pair = (lp, hp, ltot, htot)
                elif args.strategy == "prob_margin":
                    for lmargin in low_margin_grid:
                        for hmargin in high_margin_grid:
                            metrics_val = eval_combo(
                                prob_low_val,
                                prob_high_val,
                                val_df["total_kills"].values,
                                lp,
                                hp,
                                args.odds,
                                strategy=args.strategy,
                                low_margin=lmargin,
                                high_margin=hmargin,
                            )
                            if metrics_val["bets"] < args.min_val_bets:
                                continue
                            if metrics_val["low_bets"] < args.min_low_bets or metrics_val["high_bets"] < args.min_high_bets:
                                continue
                            if best is None or metrics_val["ev"] > best["ev"]:
                                best = metrics_val
                                best_pair = (lp, hp, lmargin, hmargin)
                else:
                    metrics_val = eval_combo(
                        prob_low_val,
                        prob_high_val,
                        val_df["total_kills"].values,
                        lp,
                        hp,
                        args.odds,
                    )
                    if metrics_val["bets"] < args.min_val_bets:
                        continue
                    if metrics_val["low_bets"] < args.min_low_bets or metrics_val["high_bets"] < args.min_high_bets:
                        continue
                    if best is None or metrics_val["ev"] > best["ev"]:
                        best = metrics_val
                        best_pair = (lp, hp)

        if best_pair is None:
            continue

        metrics_test = eval_combo(
            prob_low_test,
            prob_high_test,
            y_test,
            best_pair[0],
            best_pair[1],
            args.odds,
            strategy=args.strategy,
            pred_all=pred_all_test,
            low_pred_thr=best_pair[2] if args.strategy == "prob_reg_all" else None,
            high_pred_thr=best_pair[3] if args.strategy == "prob_reg_all" else None,
            total10=total10_test,
            low_total_thr=best_pair[2] if args.strategy == "prob_total10" else None,
            high_total_thr=best_pair[3] if args.strategy == "prob_total10" else None,
            low_margin=best_pair[2] if args.strategy == "prob_margin" else None,
            high_margin=best_pair[3] if args.strategy == "prob_margin" else None,
        )

        windows += 1
        agg["bets"] += metrics_test["bets"]
        agg["wins"] += metrics_test["wins"]
        agg["losses"] += metrics_test["losses"]
        agg["profit"] += metrics_test["profit"]
        agg["low_bets"] += metrics_test["low_bets"]
        agg["high_bets"] += metrics_test["high_bets"]

        print(
            json.dumps(
                {
                    "test_start": int(test_df["start_time"].min()),
                    "test_end": int(test_df["start_time"].max()),
                    "low_thr": best_pair[0],
                    "high_thr": best_pair[1],
                    "low_gate": best_pair[2] if len(best_pair) > 2 else None,
                    "high_gate": best_pair[3] if len(best_pair) > 3 else None,
                    "strategy": args.strategy,
                    "val_metrics": best,
                    "test_metrics": metrics_test,
                },
                ensure_ascii=True,
            )
        )

    agg_ev = agg["profit"] / agg["bets"] if agg["bets"] else 0.0
    print(
        json.dumps(
            {
                "windows": windows,
                "agg": {**agg, "ev": agg_ev},
            },
            ensure_ascii=True,
        )
    )


if __name__ == "__main__":
    main()
