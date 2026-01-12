#!/usr/bin/env python3
"""
Backtest kills betting rules on pro dataset and build team predictability report.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from heapq import heappop, heappush
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, CatBoostRegressor

import train_kills_regression_pro as tkr


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("kills_backtest")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODELS_DIR = PROJECT_ROOT / "ml-models"
REPORTS_DIR = PROJECT_ROOT / "reports"

sys.path.insert(0, str(PROJECT_ROOT / "base"))


def _load_rules(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_rules(rules: Dict[str, Any], patch_label: Optional[str]) -> Dict[str, Any]:
    merged = dict(rules or {})
    patch_overrides = merged.get("patch_overrides") or {}
    if isinstance(patch_label, str) and patch_label in patch_overrides:
        override = patch_overrides.get(patch_label) or {}
        if isinstance(override, dict):
            for key, val in override.items():
                merged[key] = val
    merged.setdefault("low_rule", {"type": "low_prob", "prob_threshold": 0.7})
    merged.setdefault("high_rule", {"type": "high_prob", "prob_threshold": 0.6})
    return merged


def _apply_networth_mode(df: pd.DataFrame, mode: str) -> None:
    if str(mode).strip().lower() == "on":
        return
    for col in df.columns:
        lc = col.lower()
        if (
            lc.startswith("nw")
            or "networth" in lc
            or "net_worth" in lc
            or "_nw" in lc
            or "nw_" in lc
        ):
            df[col] = np.nan


def _load_meta() -> Tuple[List[str], List[str]]:
    meta_path = MODELS_DIR / "live_cb_kills_reg_meta.json"
    if not meta_path.exists():
        raise FileNotFoundError(f"Missing meta: {meta_path}")
    with meta_path.open("r", encoding="utf-8") as f:
        meta = json.load(f)
    feature_cols = meta.get("feature_cols") or []
    cat_cols = meta.get("cat_features") or []
    return feature_cols, cat_cols


def _parse_list(text: str, cast=float) -> List[Any]:
    if not text:
        return []
    out: List[Any] = []
    for chunk in text.split(","):
        val = chunk.strip()
        if not val:
            continue
        try:
            out.append(cast(val))
        except Exception:
            continue
    return out


def _parse_bool_list(text: str) -> List[bool]:
    if not text:
        return []
    out: List[bool] = []
    for chunk in text.split(","):
        val = chunk.strip().lower()
        if not val:
            continue
        if val in ("1", "true", "yes", "y", "t"):
            out.append(True)
        elif val in ("0", "false", "no", "n", "f"):
            out.append(False)
    return out


def _load_model_set(
    reg_all_path: Path,
    reg_low_path: Path,
    reg_high_path: Path,
    cls_low_path: Path,
    cls_high_path: Path,
) -> Optional[Dict[str, Any]]:
    if not all(p.exists() for p in (reg_all_path, reg_low_path, reg_high_path, cls_low_path, cls_high_path)):
        return None
    reg_all = CatBoostRegressor()
    reg_all.load_model(str(reg_all_path))
    reg_low = CatBoostRegressor()
    reg_low.load_model(str(reg_low_path))
    reg_high = CatBoostRegressor()
    reg_high.load_model(str(reg_high_path))
    cls_low = CatBoostClassifier()
    cls_low.load_model(str(cls_low_path))
    cls_high = CatBoostClassifier()
    cls_high.load_model(str(cls_high_path))
    return {
        "reg_all": reg_all,
        "reg_low": reg_low,
        "reg_high": reg_high,
        "cls_low": cls_low,
        "cls_high": cls_high,
    }


def _predict(models: Dict[str, Any], X: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    pred_all = models["reg_all"].predict(X)
    pred_low = models["reg_low"].predict(X)
    pred_high = models["reg_high"].predict(X)
    low_prob = models["cls_low"].predict_proba(X)[:, 1]
    high_prob = models["cls_high"].predict_proba(X)[:, 1]
    return pred_all, pred_low, pred_high, low_prob, high_prob


def _build_team_name_map() -> Dict[int, str]:
    try:
        import id_to_names
    except Exception:
        return {}

    mapping: Dict[int, str] = {}

    def add_source(src: Dict[str, Any]) -> None:
        for name, val in src.items():
            ids: List[int] = []
            if isinstance(val, set):
                ids = [int(v) for v in val if isinstance(v, int)]
            elif isinstance(val, int):
                ids = [val]
            for tid in ids:
                if tid not in mapping and tid > 0:
                    mapping[tid] = str(name)

    add_source(getattr(id_to_names, "tier_one_teams", {}))
    add_source(getattr(id_to_names, "tier_two_teams", {}))
    add_source(getattr(id_to_names, "rest_teams", {}))
    return mapping


def _build_filter_mask(
    r_team_ids: np.ndarray,
    d_team_ids: np.ndarray,
    r_new: np.ndarray,
    d_new: np.ndarray,
    metrics: Dict[int, Dict[str, Any]],
    *,
    min_matches: int,
    max_mae: float,
    min_stable_rate: float,
    block_new_team: bool,
    block_if_unknown: bool,
) -> np.ndarray:
    team_ok: Dict[int, bool] = {}
    for team_id, data in metrics.items():
        try:
            matches = int(data.get("matches", 0))
        except Exception:
            matches = 0
        try:
            mae = float(data.get("mae", 0.0))
        except Exception:
            mae = 0.0
        try:
            stable_rate = float(data.get("stable_rate", 0.0))
        except Exception:
            stable_rate = 0.0
        ok = matches >= min_matches and mae <= max_mae and stable_rate >= min_stable_rate
        team_ok[int(team_id)] = ok

    def ok_for(team_id: int) -> bool:
        if team_id <= 0:
            return not block_if_unknown
        if team_id not in team_ok:
            return not block_if_unknown
        return bool(team_ok.get(team_id))

    r_ok = np.array([ok_for(tid) for tid in r_team_ids], dtype=bool)
    d_ok = np.array([ok_for(tid) for tid in d_team_ids], dtype=bool)

    if block_new_team:
        r_new_mask = np.nan_to_num(r_new, nan=0.0) >= 1.0
        d_new_mask = np.nan_to_num(d_new, nan=0.0) >= 1.0
        new_mask = r_new_mask | d_new_mask
    else:
        new_mask = np.zeros(len(r_team_ids), dtype=bool)

    return r_ok & d_ok & (~new_mask)


def _make_low_candidates(
    low_prob: np.ndarray,
    high_prob: np.ndarray,
    pred_low: np.ndarray,
    pred_all: np.ndarray,
    prob_grid: List[float],
    pred_grid: List[float],
    margin_grid: List[float],
    rule_set: str,
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []

    def add(rule_type: str, params: Dict[str, Any], mask: np.ndarray) -> None:
        candidates.append({"type": rule_type, "params": params, "mask": mask})

    for p in prob_grid:
        add("low_prob", {"type": "low_prob", "prob_threshold": p}, low_prob >= p)

    for t in pred_grid:
        add("reg_low", {"type": "reg_low", "pred_threshold": t}, pred_low <= t)
        add("reg_all_low", {"type": "reg_all_low", "pred_threshold": t}, pred_all <= t)

    for p in prob_grid:
        for t in pred_grid:
            add(
                "low_prob_and_reg_low",
                {"type": "low_prob_and_reg_low", "prob_threshold": p, "pred_threshold": t},
                (low_prob >= p) & (pred_low <= t),
            )
            add(
                "low_prob_and_reg_all",
                {"type": "low_prob_and_reg_all", "prob_threshold": p, "pred_threshold": t},
                (low_prob >= p) & (pred_all <= t),
            )

    if rule_set == "full":
        for p in prob_grid:
            for m in margin_grid:
                add(
                    "low_prob_margin",
                    {"type": "low_prob_margin", "prob_threshold": p, "margin": m},
                    (low_prob >= p) & ((low_prob - high_prob) >= m),
                )

    return candidates


def _make_high_candidates(
    low_prob: np.ndarray,
    high_prob: np.ndarray,
    pred_high: np.ndarray,
    pred_all: np.ndarray,
    prob_grid: List[float],
    pred_grid: List[float],
    margin_grid: List[float],
    rule_set: str,
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []

    def add(rule_type: str, params: Dict[str, Any], mask: np.ndarray) -> None:
        candidates.append({"type": rule_type, "params": params, "mask": mask})

    for p in prob_grid:
        add("high_prob", {"type": "high_prob", "prob_threshold": p}, high_prob >= p)

    for t in pred_grid:
        add("reg_high", {"type": "reg_high", "pred_threshold": t}, pred_high >= t)
        add("reg_all", {"type": "reg_all", "pred_threshold": t}, pred_all >= t)

    for p in prob_grid:
        for t in pred_grid:
            add(
                "high_prob_and_reg_high",
                {"type": "high_prob_and_reg_high", "prob_threshold": p, "pred_threshold": t},
                (high_prob >= p) & (pred_high >= t),
            )
            add(
                "high_prob_and_reg_all",
                {"type": "high_prob_and_reg_all", "prob_threshold": p, "pred_threshold": t},
                (high_prob >= p) & (pred_all >= t),
            )

    if rule_set == "full":
        for p in prob_grid:
            for m in margin_grid:
                add(
                    "high_prob_margin",
                    {"type": "high_prob_margin", "prob_threshold": p, "margin": m},
                    (high_prob >= p) & ((high_prob - low_prob) >= m),
                )

    return candidates


def _search_best_rules(
    df: pd.DataFrame,
    *,
    focus_patch: str,
    rules: Dict[str, Any],
    metrics: Dict[int, Dict[str, Any]],
    min_bets: int,
    min_low_bets: int,
    min_high_bets: int,
    max_results: int,
    rule_set: str,
    low_prob_grid: List[float],
    high_prob_grid: List[float],
    low_pred_grid: List[float],
    high_pred_grid: List[float],
    margin_grid: List[float],
    min_matches_grid: List[int],
    max_mae_grid: List[float],
    min_stable_grid: List[float],
    block_new_team_grid: List[bool],
    block_unknown_grid: List[bool],
) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    focus_mask = df["patch_major_label"].astype(str) == focus_patch
    if not focus_mask.any():
        raise RuntimeError(f"No matches for patch {focus_patch}")

    sub = df.loc[focus_mask].copy()
    odds = float(rules.get("odds", 1.8))

    low_prob = sub["low_prob"].to_numpy()
    high_prob = sub["high_prob"].to_numpy()
    pred_all = sub["pred_all"].to_numpy()
    pred_low = sub["pred_low"].to_numpy()
    pred_high = sub["pred_high"].to_numpy()
    total_kills = sub["total_kills"].to_numpy()

    r_team_ids = sub["radiant_team_id"].fillna(0).astype(int).to_numpy()
    d_team_ids = sub["dire_team_id"].fillna(0).astype(int).to_numpy()
    r_new = sub["radiant_roster_new_team"].to_numpy()
    d_new = sub["dire_roster_new_team"].to_numpy()

    low_candidates = _make_low_candidates(
        low_prob, high_prob, pred_low, pred_all, low_prob_grid, low_pred_grid, margin_grid, rule_set
    )
    high_candidates = _make_high_candidates(
        low_prob, high_prob, pred_high, pred_all, high_prob_grid, high_pred_grid, margin_grid, rule_set
    )

    best: Optional[Dict[str, Any]] = None
    heap: List[Tuple[Tuple[float, int, float], int, Dict[str, Any]]] = []
    seq = 0

    for min_matches in min_matches_grid:
        for max_mae in max_mae_grid:
            for min_stable in min_stable_grid:
                for block_new in block_new_team_grid:
                    for block_unknown in block_unknown_grid:
                        filter_mask = _build_filter_mask(
                            r_team_ids,
                            d_team_ids,
                            r_new,
                            d_new,
                            metrics,
                            min_matches=min_matches,
                            max_mae=max_mae,
                            min_stable_rate=min_stable,
                            block_new_team=block_new,
                            block_if_unknown=block_unknown,
                        )
                        for low_rule in low_candidates:
                            low_sig = low_rule["mask"]
                            for high_rule in high_candidates:
                                high_sig = high_rule["mask"]
                                bet_low = low_sig & (~high_sig) & filter_mask
                                bet_high = high_sig & (~low_sig) & filter_mask
                                bets = int(bet_low.sum() + bet_high.sum())
                                if bets < min_bets:
                                    continue
                                low_bets = int(bet_low.sum())
                                high_bets = int(bet_high.sum())
                                if low_bets < min_low_bets or high_bets < min_high_bets:
                                    continue
                                wins = int(((bet_low & (total_kills < 40)) | (bet_high & (total_kills > 50))).sum())
                                losses = bets - wins
                                profit = wins * (odds - 1.0) - losses
                                ev = profit / bets if bets else 0.0
                                result = {
                                    "profit": float(profit),
                                    "ev": float(ev),
                                    "bets": bets,
                                    "wins": wins,
                                    "losses": losses,
                                    "low_bets": low_bets,
                                    "high_bets": high_bets,
                                    "low_rule": low_rule["params"],
                                    "high_rule": high_rule["params"],
                                    "team_filter": {
                                        "enabled": True,
                                        "min_matches": int(min_matches),
                                        "max_mae": float(max_mae),
                                        "min_stable_rate": float(min_stable),
                                        "block_new_team": bool(block_new),
                                        "block_if_unknown": bool(block_unknown),
                                    },
                                }

                                key = (profit, bets, ev)
                                if best is None or key > (best["profit"], best["bets"], best["ev"]):
                                    best = result

                                heappush(heap, (key, seq, result))
                                seq += 1
                                if len(heap) > max_results:
                                    heappop(heap)

    top_results = [item[2] for item in sorted(heap, key=lambda x: x[0], reverse=True)]
    return best, top_results


def _apply_best_rules(rules_path: Path, focus_patch: str, best: Dict[str, Any]) -> None:
    rules = _load_rules(rules_path)
    patch_overrides = rules.get("patch_overrides")
    if not isinstance(patch_overrides, dict):
        patch_overrides = {}
        rules["patch_overrides"] = patch_overrides
    patch_overrides[str(focus_patch)] = {
        "low_rule": best["low_rule"],
        "high_rule": best["high_rule"],
        "team_predictability_filter": best["team_filter"],
    }
    with rules_path.open("w", encoding="utf-8") as f:
        json.dump(rules, f, indent=2)


def _apply_min_matches(rules_path: Path, focus_patch: str, min_matches: int) -> None:
    rules = _load_rules(rules_path)
    patch_overrides = rules.get("patch_overrides")
    if not isinstance(patch_overrides, dict):
        patch_overrides = {}
        rules["patch_overrides"] = patch_overrides
    patch = patch_overrides.get(str(focus_patch))
    if not isinstance(patch, dict):
        patch = {}
        patch_overrides[str(focus_patch)] = patch
    team_filter = patch.get("team_predictability_filter")
    if not isinstance(team_filter, dict):
        team_filter = {}
    team_filter.setdefault("enabled", True)
    team_filter["min_matches"] = int(min_matches)
    patch["team_predictability_filter"] = team_filter
    patch_overrides[str(focus_patch)] = patch
    with rules_path.open("w", encoding="utf-8") as f:
        json.dump(rules, f, indent=2)


def _rule_mask_low(
    low_rule: Dict[str, Any],
    low_prob: np.ndarray,
    high_prob: np.ndarray,
    pred_all: np.ndarray,
    pred_low: np.ndarray,
) -> np.ndarray:
    rule_type = low_rule.get("type")
    if rule_type == "low_prob":
        thr = float(low_rule.get("prob_threshold", 0.6))
        return low_prob >= thr
    if rule_type == "low_prob_margin":
        thr = float(low_rule.get("prob_threshold", 0.6))
        margin = float(low_rule.get("margin", 0.0))
        return (low_prob >= thr) & ((low_prob - high_prob) >= margin)
    if rule_type == "low_prob_and_reg_low":
        thr = float(low_rule.get("prob_threshold", 0.6))
        pred_thr = float(low_rule.get("pred_threshold", 40.0))
        return (low_prob >= thr) & (pred_low <= pred_thr)
    if rule_type == "low_prob_and_reg_all":
        thr = float(low_rule.get("prob_threshold", 0.6))
        pred_thr = float(low_rule.get("pred_threshold", 40.0))
        return (low_prob >= thr) & (pred_all <= pred_thr)
    if rule_type == "reg_all_low":
        pred_thr = float(low_rule.get("pred_threshold", 40.0))
        return pred_all <= pred_thr
    if rule_type == "reg_low":
        pred_thr = float(low_rule.get("pred_threshold", 40.0))
        return pred_low <= pred_thr
    return np.zeros_like(low_prob, dtype=bool)


def _rule_mask_high(
    high_rule: Dict[str, Any],
    low_prob: np.ndarray,
    high_prob: np.ndarray,
    pred_all: np.ndarray,
    pred_high: np.ndarray,
) -> np.ndarray:
    rule_type = high_rule.get("type")
    if rule_type == "reg_all":
        pred_thr = float(high_rule.get("pred_threshold", 56.0))
        return pred_all >= pred_thr
    if rule_type == "high_prob_and_reg_high":
        thr = float(high_rule.get("prob_threshold", 0.65))
        pred_thr = float(high_rule.get("pred_threshold", 56.0))
        return (high_prob >= thr) & (pred_high >= pred_thr)
    if rule_type == "high_prob":
        thr = float(high_rule.get("prob_threshold", 0.65))
        return high_prob >= thr
    if rule_type == "high_prob_margin":
        thr = float(high_rule.get("prob_threshold", 0.65))
        margin = float(high_rule.get("margin", 0.0))
        return (high_prob >= thr) & ((high_prob - low_prob) >= margin)
    if rule_type == "high_prob_and_reg_all":
        thr = float(high_rule.get("prob_threshold", 0.65))
        pred_thr = float(high_rule.get("pred_threshold", 56.0))
        return (high_prob >= thr) & (pred_all >= pred_thr)
    if rule_type == "reg_high":
        pred_thr = float(high_rule.get("pred_threshold", 56.0))
        return pred_high >= pred_thr
    return np.zeros_like(high_prob, dtype=bool)


def _team_predictability_filter(
    radiant_team_id: Optional[int],
    dire_team_id: Optional[int],
    row: Dict[str, Any],
    rules: Dict[str, Any],
    metrics: Dict[int, Dict[str, Any]],
) -> Tuple[bool, Optional[str]]:
    cfg = dict(rules.get("team_predictability_filter") or {})
    enabled = bool(cfg.get("enabled", True))
    if not enabled:
        return True, None

    min_matches = int(cfg.get("min_matches", 20))
    max_mae = float(cfg.get("max_mae", 14.0))
    min_stable_rate = float(cfg.get("min_stable_rate", 0.9))
    block_new_team = bool(cfg.get("block_new_team", True))
    block_if_unknown = bool(cfg.get("block_if_unknown", False))

    def _is_new(val: Any) -> bool:
        try:
            v = float(val)
            if math.isnan(v):
                return False
            return v >= 1.0
        except Exception:
            return False

    if block_new_team and (
        _is_new(row.get("radiant_roster_new_team"))
        or _is_new(row.get("dire_roster_new_team"))
    ):
        return False, "new_team"

    def _check(team_id: Optional[int]) -> Optional[str]:
        if not team_id or team_id <= 0:
            return "unknown" if block_if_unknown else None
        data = metrics.get(int(team_id))
        if not data:
            return "unknown" if block_if_unknown else None
        matches = data.get("matches")
        mae = data.get("mae")
        stable_rate = data.get("stable_rate")
        try:
            matches = int(matches)
        except Exception:
            matches = 0
        if matches < min_matches:
            return "unknown" if block_if_unknown else None
        try:
            if mae is not None and float(mae) > max_mae:
                return "mae"
        except Exception:
            pass
        try:
            if stable_rate is not None and float(stable_rate) < min_stable_rate:
                return "stable_rate"
        except Exception:
            pass
        return None

    for reason in (_check(radiant_team_id), _check(dire_team_id)):
        if reason in {"mae", "stable_rate", "unknown"}:
            return False, reason

    return True, None


def _low_ok(low_rule: Dict[str, Any], low_prob: float, high_prob: float, pred_all: float, pred_low: float) -> bool:
    rule_type = low_rule.get("type")
    if rule_type == "low_prob":
        return low_prob >= float(low_rule.get("prob_threshold", 0.6))
    if rule_type == "low_prob_margin":
        margin = float(low_rule.get("margin", 0.0))
        return low_prob >= float(low_rule.get("prob_threshold", 0.6)) and (low_prob - high_prob) >= margin
    if rule_type == "low_prob_and_reg_low":
        return low_prob >= float(low_rule.get("prob_threshold", 0.6)) and pred_low <= float(
            low_rule.get("pred_threshold", 40.0)
        )
    if rule_type == "low_prob_and_reg_all":
        return low_prob >= float(low_rule.get("prob_threshold", 0.6)) and pred_all <= float(
            low_rule.get("pred_threshold", 40.0)
        )
    if rule_type == "reg_all_low":
        return pred_all <= float(low_rule.get("pred_threshold", 40.0))
    if rule_type == "reg_low":
        return pred_low <= float(low_rule.get("pred_threshold", 40.0))
    return False


def _high_ok(
    high_rule: Dict[str, Any],
    low_prob: float,
    high_prob: float,
    pred_all: float,
    pred_high: float,
) -> bool:
    rule_type = high_rule.get("type")
    if rule_type == "reg_all":
        return pred_all >= float(high_rule.get("pred_threshold", 56.0))
    if rule_type == "high_prob_and_reg_high":
        return high_prob >= float(high_rule.get("prob_threshold", 0.65)) and pred_high >= float(
            high_rule.get("pred_threshold", 56.0)
        )
    if rule_type == "high_prob":
        return high_prob >= float(high_rule.get("prob_threshold", 0.65))
    if rule_type == "high_prob_margin":
        margin = float(high_rule.get("margin", 0.0))
        return high_prob >= float(high_rule.get("prob_threshold", 0.65)) and (high_prob - low_prob) >= margin
    if rule_type == "high_prob_and_reg_all":
        return high_prob >= float(high_rule.get("prob_threshold", 0.65)) and pred_all >= float(
            high_rule.get("pred_threshold", 56.0)
        )
    if rule_type == "reg_high":
        return pred_high >= float(high_rule.get("pred_threshold", 56.0))
    return False


def _write_team_report(report: List[Dict[str, Any]]) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    json_path = REPORTS_DIR / "team_kills_predictability.json"
    csv_path = REPORTS_DIR / "team_kills_predictability.csv"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    pd.DataFrame(report).to_csv(csv_path, index=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--clean-path", default=str(tkr.DEFAULT_CLEAN_PATH))
    parser.add_argument("--rules-path", default=str(MODELS_DIR / "kills_betting_rules.json"))
    parser.add_argument("--focus-patch", default="7.40")
    parser.add_argument("--min-team-matches", type=int, default=5)
    parser.add_argument("--team-top-n", type=int, default=10)
    parser.add_argument("--skip-team-report", action="store_true")
    parser.add_argument("--search", action="store_true", help="Search rule/filter grid for max profit")
    parser.add_argument(
        "--search-min-matches",
        action="store_true",
        help="Search min_matches only using current patch rules",
    )
    parser.add_argument("--apply-best", action="store_true", help="Apply best search result to rules file")
    parser.add_argument("--min-bets", type=int, default=4)
    parser.add_argument("--min-low-bets", type=int, default=0)
    parser.add_argument("--min-high-bets", type=int, default=0)
    parser.add_argument("--max-results", type=int, default=10)
    parser.add_argument("--rule-set", choices=("basic", "full"), default="full")
    parser.add_argument("--low-prob-grid", type=str, default="")
    parser.add_argument("--high-prob-grid", type=str, default="")
    parser.add_argument("--low-pred-grid", type=str, default="")
    parser.add_argument("--high-pred-grid", type=str, default="")
    parser.add_argument("--margin-grid", type=str, default="")
    parser.add_argument("--min-matches-grid", type=str, default="")
    parser.add_argument("--max-mae-grid", type=str, default="")
    parser.add_argument("--min-stable-grid", type=str, default="")
    parser.add_argument("--block-new-team-grid", type=str, default="")
    parser.add_argument("--block-unknown-grid", type=str, default="")
    args = parser.parse_args()

    rules = _load_rules(Path(args.rules_path))
    networth_mode = str(rules.get("networth_mode", "off")).strip().lower()

    logger.info("Loading matches: %s", args.clean_path)
    matches = tkr.load_clean_data(Path(args.clean_path))
    pub_priors = tkr.build_pub_hero_priors(tkr.PUB_PLAYERS_DIR, tkr.PUB_PRIORS_PATH)
    df = tkr.build_dataset(matches, pub_priors)
    df = df.sort_values("start_time").reset_index(drop=True)

    feature_cols, cat_cols = _load_meta()
    X = df.reindex(columns=feature_cols).copy()
    for c in cat_cols:
        if c in X.columns:
            X[c] = X[c].fillna("UNKNOWN").astype(str)
    _apply_networth_mode(X, networth_mode)

    global_models = _load_model_set(
        MODELS_DIR / "live_cb_kills_reg.cbm",
        MODELS_DIR / "live_cb_kills_reg_low.cbm",
        MODELS_DIR / "live_cb_kills_reg_high.cbm",
        MODELS_DIR / "live_cb_kills_low_cls.cbm",
        MODELS_DIR / "live_cb_kills_high_cls.cbm",
    )
    if not global_models:
        raise RuntimeError("Missing global kills models")

    pred_all, pred_low, pred_high, low_prob, high_prob = _predict(global_models, X)
    model_variant = np.array(["global"] * len(df), dtype=object)

    patch_models: Dict[str, Dict[str, Any]] = {}
    for label in sorted({str(v) for v in df["patch_major_label"].dropna().unique()}):
        slug = tkr.patch_label_to_slug(label)
        models = _load_model_set(
            MODELS_DIR / f"live_cb_kills_reg_patch_{slug}.cbm",
            MODELS_DIR / f"live_cb_kills_reg_patch_{slug}_low.cbm",
            MODELS_DIR / f"live_cb_kills_reg_patch_{slug}_high.cbm",
            MODELS_DIR / f"live_cb_kills_low_cls_patch_{slug}.cbm",
            MODELS_DIR / f"live_cb_kills_high_cls_patch_{slug}.cbm",
        )
        if models:
            patch_models[label] = models

    if patch_models:
        for label, models in patch_models.items():
            mask = df["patch_major_label"].astype(str) == label
            if not mask.any():
                continue
            p_all, p_low, p_high, p_lp, p_hp = _predict(models, X.loc[mask])
            pred_all[mask] = p_all
            pred_low[mask] = p_low
            pred_high[mask] = p_high
            low_prob[mask] = p_lp
            high_prob[mask] = p_hp
            model_variant[mask] = f"patch:{label}"

    tier_models: Dict[int, Dict[str, Any]] = {}
    for tier in (1, 2):
        models = _load_model_set(
            MODELS_DIR / f"live_cb_kills_reg_tier_{tier}.cbm",
            MODELS_DIR / f"live_cb_kills_reg_tier_{tier}_low.cbm",
            MODELS_DIR / f"live_cb_kills_reg_tier_{tier}_high.cbm",
            MODELS_DIR / f"live_cb_kills_low_cls_tier_{tier}.cbm",
            MODELS_DIR / f"live_cb_kills_high_cls_tier_{tier}.cbm",
        )
        if models:
            tier_models[tier] = models

    if tier_models:
        for tier, models in tier_models.items():
            mask = (
                (model_variant == "global")
                & (df.get("match_tier_known", 0).astype(int) == 1)
                & (df.get("match_tier", 0).astype(int) == tier)
            )
            if not mask.any():
                continue
            p_all, p_low, p_high, p_lp, p_hp = _predict(models, X.loc[mask])
            pred_all[mask] = p_all
            pred_low[mask] = p_low
            pred_high[mask] = p_high
            low_prob[mask] = p_lp
            high_prob[mask] = p_hp
            model_variant[mask] = f"tier:{tier}"

    df["pred_all"] = pred_all
    df["pred_low"] = pred_low
    df["pred_high"] = pred_high
    df["low_prob"] = low_prob
    df["high_prob"] = high_prob
    df["model_variant"] = model_variant

    team_name_map = _build_team_name_map()
    team_rows = []
    for side in ("radiant", "dire"):
        team_id_col = f"{side}_team_id"
        stable_col = f"{side}_roster_stable_prev"
        new_team_col = f"{side}_roster_new_team"
        shared_col = f"{side}_roster_shared_prev"
        cols = [
            team_id_col,
            stable_col,
            new_team_col,
            shared_col,
            "total_kills",
            "pred_all",
        ]
        sub = df[cols].copy()
        sub = sub.rename(
            columns={
                team_id_col: "team_id",
                stable_col: "roster_stable_prev",
                new_team_col: "roster_new_team",
                shared_col: "roster_shared_prev",
            }
        )
        team_rows.append(sub)

    team_df = pd.concat(team_rows, ignore_index=True)
    team_df = team_df[team_df["team_id"].notna()].copy()
    team_df["team_id"] = team_df["team_id"].astype(int)
    team_df = team_df[team_df["team_id"] > 0]
    team_df["abs_error"] = (team_df["pred_all"] - team_df["total_kills"]).abs()

    report: List[Dict[str, Any]] = []
    metrics_cache: Dict[int, Dict[str, Any]] = {}
    for team_id, group in team_df.groupby("team_id"):
        matches_cnt = int(len(group))
        mae = float(group["abs_error"].mean()) if matches_cnt else 0.0
        median_abs = float(group["abs_error"].median()) if matches_cnt else 0.0
        stable_rate = float(group["roster_stable_prev"].mean()) if matches_cnt else 0.0
        new_team_rate = float(group["roster_new_team"].mean()) if matches_cnt else 0.0
        shared_vals = group["roster_shared_prev"].dropna()
        avg_shared_recent = float(shared_vals.mean()) if not shared_vals.empty else None
        team_name = team_name_map.get(team_id) or str(team_id)
        row = {
            "team_id": team_id,
            "team_name": team_name,
            "matches": matches_cnt,
            "mae": round(mae, 10),
            "median_abs_error": round(median_abs, 10),
            "stable_rate": round(stable_rate, 10),
            "avg_shared_recent": round(avg_shared_recent, 10) if avg_shared_recent is not None else None,
            "new_team_rate": round(new_team_rate, 10),
        }
        metrics_cache[team_id] = row
        report.append(row)

    report.sort(key=lambda r: (-r["matches"], r["team_id"]))
    if not args.skip_team_report:
        _write_team_report(report)
        logger.info("Saved team predictability report: %s (%d teams)", REPORTS_DIR, len(report))

    if args.search_min_matches:
        focus_patch = str(args.focus_patch).strip()
        rules_row = _resolve_rules(rules, focus_patch)
        low_rule = dict(rules_row.get("low_rule") or {})
        high_rule = dict(rules_row.get("high_rule") or {})
        filter_cfg = dict(rules_row.get("team_predictability_filter") or {})
        max_mae = float(filter_cfg.get("max_mae", 14.0))
        min_stable = float(filter_cfg.get("min_stable_rate", 0.9))
        block_new = bool(filter_cfg.get("block_new_team", True))
        block_unknown = bool(filter_cfg.get("block_if_unknown", False))

        min_matches_grid = [int(v) for v in (_parse_list(args.min_matches_grid, int) or [0, 5, 10, 15, 20, 25, 30])]
        min_matches_grid = sorted(set(min_matches_grid))

        focus_mask = df["patch_major_label"].astype(str) == focus_patch
        if not focus_mask.any():
            raise RuntimeError(f"No matches for patch {focus_patch}")
        sub = df.loc[focus_mask].copy()
        odds = float(rules_row.get("odds", rules.get("odds", 1.8)))

        low_prob = sub["low_prob"].to_numpy()
        high_prob = sub["high_prob"].to_numpy()
        pred_all = sub["pred_all"].to_numpy()
        pred_low = sub["pred_low"].to_numpy()
        pred_high = sub["pred_high"].to_numpy()
        total_kills = sub["total_kills"].to_numpy()
        r_team_ids = sub["radiant_team_id"].fillna(0).astype(int).to_numpy()
        d_team_ids = sub["dire_team_id"].fillna(0).astype(int).to_numpy()
        r_new = sub["radiant_roster_new_team"].to_numpy()
        d_new = sub["dire_roster_new_team"].to_numpy()

        low_sig = _rule_mask_low(low_rule, low_prob, high_prob, pred_all, pred_low)
        high_sig = _rule_mask_high(high_rule, low_prob, high_prob, pred_all, pred_high)

        best_row: Optional[Dict[str, Any]] = None
        for min_matches in min_matches_grid:
            filter_mask = _build_filter_mask(
                r_team_ids,
                d_team_ids,
                r_new,
                d_new,
                metrics_cache,
                min_matches=int(min_matches),
                max_mae=max_mae,
                min_stable_rate=min_stable,
                block_new_team=block_new,
                block_if_unknown=block_unknown,
            )
            bet_low = low_sig & (~high_sig) & filter_mask
            bet_high = high_sig & (~low_sig) & filter_mask
            bets = int(bet_low.sum() + bet_high.sum())
            if bets < int(args.min_bets):
                logger.info("min_matches=%d -> bets=%d (skip, min_bets=%d)", min_matches, bets, int(args.min_bets))
                continue
            wins = int(((bet_low & (total_kills < 40)) | (bet_high & (total_kills > 50))).sum())
            losses = bets - wins
            profit = wins * (odds - 1.0) - losses
            ev = profit / bets if bets else 0.0
            row = {
                "min_matches": int(min_matches),
                "bets": bets,
                "wins": wins,
                "losses": losses,
                "profit": float(profit),
                "ev": float(ev),
                "low_bets": int(bet_low.sum()),
                "high_bets": int(bet_high.sum()),
            }
            logger.info(
                "min_matches=%d bets=%d (low=%d high=%d) profit=%.3f ev=%.3f",
                row["min_matches"],
                row["bets"],
                row["low_bets"],
                row["high_bets"],
                row["profit"],
                row["ev"],
            )
            if best_row is None or (row["profit"], row["bets"], row["ev"]) > (
                best_row["profit"],
                best_row["bets"],
                best_row["ev"],
            ):
                best_row = row

        if best_row:
            logger.info(
                "Best min_matches=%d profit=%.3f ev=%.3f bets=%d (low=%d high=%d)",
                best_row["min_matches"],
                best_row["profit"],
                best_row["ev"],
                best_row["bets"],
                best_row["low_bets"],
                best_row["high_bets"],
            )
            if args.apply_best:
                _apply_min_matches(Path(args.rules_path), focus_patch, best_row["min_matches"])
                logger.info(
                    "Applied min_matches=%d to %s (patch %s)",
                    best_row["min_matches"],
                    args.rules_path,
                    focus_patch,
                )
        else:
            logger.info("No min_matches met the min bet constraints.")

    if args.search:
        prob_grid = _parse_list(args.low_prob_grid) or [0.6, 0.65, 0.68, 0.7, 0.72, 0.75, 0.78, 0.8, 0.82, 0.85]
        high_prob_grid = _parse_list(args.high_prob_grid) or prob_grid
        pred_grid = _parse_list(args.low_pred_grid) or [36, 37, 38, 39, 40, 41, 42]
        high_pred_grid = _parse_list(args.high_pred_grid) or [54, 55, 56, 57, 58, 59, 60]
        margin_grid = _parse_list(args.margin_grid) or [0.0, 0.05, 0.1, 0.15, 0.2]
        min_matches_grid = [int(v) for v in (_parse_list(args.min_matches_grid, int) or [0, 10, 20, 30])]
        max_mae_grid = _parse_list(args.max_mae_grid) or [10.5, 12.0, 13.5]
        min_stable_grid = _parse_list(args.min_stable_grid) or [0.88, 0.92, 0.95]
        block_new_grid = _parse_bool_list(args.block_new_team_grid) or [True]
        block_unknown_grid = _parse_bool_list(args.block_unknown_grid) or [True]

        best, top_results = _search_best_rules(
            df,
            focus_patch=str(args.focus_patch),
            rules=rules,
            metrics=metrics_cache,
            min_bets=int(args.min_bets),
            min_low_bets=int(args.min_low_bets),
            min_high_bets=int(args.min_high_bets),
            max_results=int(args.max_results),
            rule_set=str(args.rule_set),
            low_prob_grid=prob_grid,
            high_prob_grid=high_prob_grid,
            low_pred_grid=pred_grid,
            high_pred_grid=high_pred_grid,
            margin_grid=margin_grid,
            min_matches_grid=min_matches_grid,
            max_mae_grid=max_mae_grid,
            min_stable_grid=min_stable_grid,
            block_new_team_grid=block_new_grid,
            block_unknown_grid=block_unknown_grid,
        )

        if best:
            logger.info(
                "Best result: profit=%.3f ev=%.3f bets=%d (low=%d high=%d) low_rule=%s high_rule=%s filter=%s",
                best["profit"],
                best["ev"],
                best["bets"],
                best["low_bets"],
                best["high_bets"],
                best["low_rule"],
                best["high_rule"],
                best["team_filter"],
            )
            if args.apply_best:
                _apply_best_rules(Path(args.rules_path), str(args.focus_patch), best)
                logger.info("Applied best rules to %s (patch %s)", args.rules_path, args.focus_patch)
        else:
            logger.info("No results met the min bet constraints.")

        if top_results:
            logger.info("Top %d results:", len(top_results))
            for row in top_results:
                logger.info(
                    "profit=%.3f ev=%.3f bets=%d (low=%d high=%d) low=%s high=%s filter=%s",
                    row["profit"],
                    row["ev"],
                    row["bets"],
                    row["low_bets"],
                    row["high_bets"],
                    row["low_rule"],
                    row["high_rule"],
                    row["team_filter"],
                )

    focus_patch = str(args.focus_patch).strip()
    focus_mask = df["patch_major_label"].astype(str) == focus_patch
    if not focus_mask.any():
        raise RuntimeError(f"No matches for patch {focus_patch}")

    odds = float(rules.get("odds", 1.8))
    totals = {
        "bets": 0,
        "wins": 0,
        "losses": 0,
        "profit": 0.0,
        "low_bets": 0,
        "high_bets": 0,
    }
    reasons: Dict[str, int] = {}
    signal_counts = {"low_only": 0, "high_only": 0, "both": 0, "none": 0}

    for idx in df[focus_mask].index.tolist():
        row = df.loc[idx]
        row_dict = row.to_dict()
        patch_label = row_dict.get("patch_major_label")
        rules_row = _resolve_rules(rules, patch_label)
        low_rule = rules_row.get("low_rule") or {}
        high_rule = rules_row.get("high_rule") or {}

        low_ok = _low_ok(low_rule, row["low_prob"], row["high_prob"], row["pred_all"], row["pred_low"])
        high_ok = _high_ok(high_rule, row["low_prob"], row["high_prob"], row["pred_all"], row["pred_high"])

        if low_ok and high_ok:
            signal_counts["both"] += 1
            continue
        if low_ok:
            signal_counts["low_only"] += 1
            bet_type = "low"
        elif high_ok:
            signal_counts["high_only"] += 1
            bet_type = "high"
        else:
            signal_counts["none"] += 1
            continue

        filter_ok, reason = _team_predictability_filter(
            int(row_dict.get("radiant_team_id") or 0),
            int(row_dict.get("dire_team_id") or 0),
            row_dict,
            rules_row,
            metrics_cache,
        )
        if not filter_ok:
            reasons[reason or "filtered"] = reasons.get(reason or "filtered", 0) + 1
            continue

        totals["bets"] += 1
        win = False
        if bet_type == "low":
            totals["low_bets"] += 1
            win = row["total_kills"] < 40
        else:
            totals["high_bets"] += 1
            win = row["total_kills"] > 50
        if win:
            totals["wins"] += 1
        else:
            totals["losses"] += 1

    totals["profit"] = totals["wins"] * (odds - 1.0) - totals["losses"] * 1.0
    ev = totals["profit"] / totals["bets"] if totals["bets"] else 0.0
    roi = ev * 100.0

    logger.info("Backtest patch=%s matches=%d", focus_patch, int(focus_mask.sum()))
    logger.info("Signals: low=%d high=%d both=%d none=%d", signal_counts["low_only"], signal_counts["high_only"], signal_counts["both"], signal_counts["none"])
    if reasons:
        logger.info("Filtered reasons: %s", reasons)
    logger.info(
        "Bets=%d (low=%d high=%d) wins=%d losses=%d EV=%.3f ROI=%.1f%% profit=%.2f",
        totals["bets"],
        totals["low_bets"],
        totals["high_bets"],
        totals["wins"],
        totals["losses"],
        ev,
        roi,
        totals["profit"],
    )

    min_matches = max(1, int(args.min_team_matches))
    eligible = [r for r in report if r["matches"] >= min_matches]
    if eligible:
        top_n = max(1, int(args.team_top_n))
        best = sorted(eligible, key=lambda r: r.get("mae", 0.0))[:top_n]
        worst = sorted(eligible, key=lambda r: r.get("mae", 0.0), reverse=True)[:top_n]
        logger.info("Best MAE teams (min_matches=%d): %s", min_matches, [(r["team_name"], r["mae"]) for r in best])
        logger.info("Worst MAE teams (min_matches=%d): %s", min_matches, [(r["team_name"], r["mae"]) for r in worst])


if __name__ == "__main__":
    main()
