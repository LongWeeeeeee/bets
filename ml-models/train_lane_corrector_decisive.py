#!/usr/bin/env python3
"""
Train decisive (win/lose-only) lane corrector models.

These models use the same runtime feature row as lane_corrector_{lane}.cbm,
but optimize only decisive lane outcomes and are meant to switch baseline
win/lose predictions when confidence delta is high enough.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "base"))

from base.functions import (  # noqa: E402
    calculate_lanes,
    check_bad_map,
    structure_lane_dict,
    _lc_build_lane_row,
    _lc_build_player_maps,
    _lc_coerce_int,
    _lc_parse_lane_prediction,
    _lc_team_id,
    _lc_update_stats_after_match,
)

LOGGER = logging.getLogger("lane_corrector_decisive")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

LANES = ("top", "mid", "bot")
CAT_COLS = (
    "lane",
    "patch_bucket",
    "pred_outcome",
    "base_out",
    "raw2v1_status",
    "raw2v2_outcome",
    "raw2v1_outcome",
    "rawcp_outcome",
    "rawsy_outcome",
)


def _lane_actual(match: Dict[str, Any], lane: str) -> Optional[str]:
    field = {
        "top": "topLaneOutcome",
        "mid": "midLaneOutcome",
        "bot": "bottomLaneOutcome",
    }[lane]
    raw = match.get(field)
    if not raw:
        return None
    s = str(raw).upper()
    if "RADIANT" in s:
        return "win"
    if "DIRE" in s:
        return "lose"
    if "TIE" in s or "DRAW" in s:
        return "draw"
    return None


def build_rows(matches: List[Dict[str, Any]], lane_dict: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    rows_by_lane: Dict[str, List[Dict[str, Any]]] = {lane: [] for lane in LANES}

    player_stats = {}
    pair_stats = {}
    pair_hero_stats = {}
    team_lane_history = {}

    items = sorted(
        matches,
        key=lambda m: (_lc_coerce_int(m.get("startDateTime")), _lc_coerce_int(m.get("id"))),
    )
    for idx, match in enumerate(items, 1):
        if idx % 1000 == 0:
            LOGGER.info("feature rows: %s / %s", idx, len(items))

        bad_map = check_bad_map(match)
        if not bad_map:
            continue
        radiant_heroes_and_pos, dire_heroes_and_pos = bad_map
        top_msg, bot_msg, mid_msg = calculate_lanes(radiant_heroes_and_pos, dire_heroes_and_pos, lane_dict)
        baseline_preds = {
            "top": _lc_parse_lane_prediction(top_msg),
            "mid": _lc_parse_lane_prediction(mid_msg),
            "bot": _lc_parse_lane_prediction(bot_msg),
        }

        rad_pos, dire_pos = _lc_build_player_maps(match.get("players"))
        if rad_pos is None or dire_pos is None:
            _lc_update_stats_after_match(
                match,
                baseline_preds,
                player_stats,
                pair_stats,
                pair_hero_stats,
                team_lane_history,
            )
            continue

        rad_team_id = _lc_team_id(match.get("radiantTeam"))
        dire_team_id = _lc_team_id(match.get("direTeam"))
        rad_roster = frozenset(
            _lc_coerce_int(v.get("account_id")) for v in rad_pos.values() if _lc_coerce_int(v.get("account_id")) > 0
        )
        dire_roster = frozenset(
            _lc_coerce_int(v.get("account_id")) for v in dire_pos.values() if _lc_coerce_int(v.get("account_id")) > 0
        )
        start_ts = _lc_coerce_int(match.get("startDateTime"))

        for lane in LANES:
            actual = _lane_actual(match, lane)
            if actual not in ("win", "lose"):
                continue
            base_out, base_conf = baseline_preds.get(lane, (None, None))
            row = _lc_build_lane_row(
                lane=lane,
                radiant_heroes_and_pos=radiant_heroes_and_pos,
                dire_heroes_and_pos=dire_heroes_and_pos,
                heroes_data=lane_dict,
                match_start_time=start_ts,
                baseline_outcome=base_out,
                baseline_conf=base_conf,
                rad_pos=rad_pos,
                dire_pos=dire_pos,
                player_stats=player_stats,
                pair_stats=pair_stats,
                pair_hero_stats=pair_hero_stats,
                team_lane_history=team_lane_history,
                rad_team_id=rad_team_id,
                dire_team_id=dire_team_id,
                rad_roster=rad_roster,
                dire_roster=dire_roster,
            )
            if row is None:
                continue
            row = dict(row)
            row["target"] = 1 if actual == "win" else 0
            rows_by_lane[lane].append(row)

        _lc_update_stats_after_match(
            match,
            baseline_preds,
            player_stats,
            pair_stats,
            pair_hero_stats,
            team_lane_history,
        )

    return rows_by_lane


def train_lane_model(
    lane: str,
    rows: List[Dict[str, Any]],
    out_dir: Path,
    iterations: int,
    depth: int,
    learning_rate: float,
    seed: int,
    eval_size: int,
) -> None:
    if not rows:
        LOGGER.warning("lane=%s has no rows, skipping", lane)
        return

    df = pd.DataFrame(rows)
    if "target" not in df.columns:
        LOGGER.warning("lane=%s missing target, skipping", lane)
        return
    feature_cols = [c for c in df.columns if c != "target"]
    cat_idx = [feature_cols.index(c) for c in CAT_COLS if c in feature_cols]

    if eval_size > 0 and len(df) > eval_size + 200:
        train_df = df.iloc[:-eval_size].copy()
        eval_df = df.iloc[-eval_size:].copy()
    else:
        train_df = df.copy()
        eval_df = None

    X_train = train_df[feature_cols]
    y_train = train_df["target"].astype(int)

    class_counts = y_train.value_counts().to_dict()
    neg = class_counts.get(0, 1)
    pos = class_counts.get(1, 1)
    scale_pos_weight = max(1.0, float(neg) / float(max(1, pos)))

    from catboost import CatBoostClassifier, Pool  # type: ignore

    model = CatBoostClassifier(
        loss_function="Logloss",
        iterations=iterations,
        depth=depth,
        learning_rate=learning_rate,
        random_seed=seed,
        verbose=False,
        scale_pos_weight=scale_pos_weight,
    )
    train_pool = Pool(X_train, y_train, cat_features=cat_idx)
    if eval_df is not None:
        X_eval = eval_df[feature_cols]
        y_eval = eval_df["target"].astype(int)
        eval_pool = Pool(X_eval, y_eval, cat_features=cat_idx)
        model.fit(train_pool, eval_set=eval_pool, verbose=False)
        preds = (model.predict_proba(eval_pool)[:, 1] >= 0.5).astype(int)
        eval_acc = float((preds == y_eval.values).mean())
    else:
        model.fit(train_pool, verbose=False)
        eval_acc = float("nan")

    out_dir.mkdir(parents=True, exist_ok=True)
    model_path = out_dir / f"lane_corrector_decisive_{lane}.cbm"
    meta_path = out_dir / f"lane_corrector_decisive_{lane}_meta.json"
    model.save_model(str(model_path))
    meta = {
        "lane": lane,
        "head": "decisive_winlose",
        "rows": int(len(df)),
        "train_rows": int(len(train_df)),
        "eval_rows": int(0 if eval_df is None else len(eval_df)),
        "eval_accuracy": eval_acc,
        "feature_cols": feature_cols,
        "cat_idx": cat_idx,
        "label_map": {0: "lose", 1: "win"},
        "params": {
            "iterations": int(iterations),
            "depth": int(depth),
            "learning_rate": float(learning_rate),
            "seed": int(seed),
            "scale_pos_weight": float(scale_pos_weight),
        },
    }
    with meta_path.open("w") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    LOGGER.info(
        "lane=%s rows=%s eval_acc=%s model=%s",
        lane,
        len(df),
        "nan" if eval_df is None else f"{eval_acc:.4f}",
        model_path,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Train decisive lane corrector models")
    parser.add_argument("--matches-json", default="tmp/combined1_train_recent4000_excl_test.json")
    parser.add_argument("--lane-dict", default="bets_data/analise_pub_matches/lane_dict_raw.json")
    parser.add_argument("--out-dir", default="ml-models")
    parser.add_argument("--iterations", type=int, default=500)
    parser.add_argument("--depth", type=int, default=8)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--eval-size", type=int, default=500)
    args = parser.parse_args()

    matches_path = PROJECT_ROOT / args.matches_json
    lane_dict_path = PROJECT_ROOT / args.lane_dict
    out_dir = PROJECT_ROOT / args.out_dir

    if not matches_path.exists():
        raise FileNotFoundError(f"matches file not found: {matches_path}")
    if not lane_dict_path.exists():
        raise FileNotFoundError(f"lane dict not found: {lane_dict_path}")

    with matches_path.open("r") as f:
        raw = json.load(f)
    if isinstance(raw, dict):
        matches = list(raw.values())
    elif isinstance(raw, list):
        matches = raw
    else:
        raise TypeError("matches json must be list or dict")

    with lane_dict_path.open("r") as f:
        lane_dict = json.load(f)
    if isinstance(lane_dict, dict) and "2v2_lanes" not in lane_dict:
        lane_dict = structure_lane_dict(lane_dict)

    rows_by_lane = build_rows(matches, lane_dict)
    for lane in LANES:
        train_lane_model(
            lane=lane,
            rows=rows_by_lane.get(lane, []),
            out_dir=out_dir,
            iterations=args.iterations,
            depth=args.depth,
            learning_rate=args.learning_rate,
            seed=args.seed,
            eval_size=args.eval_size,
        )


if __name__ == "__main__":
    main()
