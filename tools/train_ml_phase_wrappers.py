#!/usr/bin/env python3
"""Train separate early/late ML wrappers for draft signals and compare vs baseline."""

from __future__ import annotations

import argparse
import json
import pickle
import time
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

import sys

ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = ROOT / "base"
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import metrics_winrate as mw  # type: ignore  # noqa: E402


TARGET_METRICS: Tuple[str, ...] = (
    "counterpick_1vs1",
    "counterpick_1vs2",
    "solo",
    "synergy_duo",
    "synergy_trio",
)
POS_SLOTS: Tuple[int, ...] = (1, 2, 3, 4, 5)
HERO_ID_VOCAB: Tuple[int, ...] = tuple(sorted(int(hid) for hid in mw._load_hero_features().keys()))
HERO_ID_TO_INDEX: Dict[int, int] = {hid: idx for idx, hid in enumerate(HERO_ID_VOCAB)}

# Фазовый набор "не шумных" фич от пользователя:
# - early_* только для early модели
# - late_* только для late модели
EARLY_EDGE_FEATURE_KEYS: Tuple[str, ...] = (
    "strong_dispel_count",
    "is_melee",
    "interruptible_channel_count",
    "interruptible_channel_ult_count",
    "escape_count",
    "channeling_spell_count",
    "channeling_ult_count",
    "has_pusher",
    "has_pusher_late",
)

LATE_EDGE_FEATURE_KEYS: Tuple[str, ...] = (
    "save_count",
    "root_dispellable_count",
    "role_coverage_4",
    "hex_count",
    "hg_defence",
    "has_leash",
    "has_root",
    "has_hard_disable",
    "has_hex",
    "has_initiator",
    "has_control",
    "has_disarm",
    "has_escape",
    "hard_carry",
    "disarm_count",
    "escape_count",
    "complexity",
    "big_ult_100s_lvl3",
    "has_pusher",
    "has_pusher_late",
)


@dataclass
class PhaseSample:
    match_idx: int
    phase: str
    metric: str
    value: float
    abs_idx: int
    sign: int
    label: int
    features: List[float]
    prob: Optional[float] = None


def _interaction_specs_for_phase(phase: str) -> List[Tuple[str, str, str]]:
    # (feature_name, key_a, key_b), uses signed edge values.
    if phase == "early":
        return [
            ("inter_escape_x_channeling", "escape_count", "channeling_spell_count"),
            ("inter_escape_x_interruptible", "escape_count", "interruptible_channel_count"),
            ("inter_pusher_x_melee", "has_pusher", "is_melee"),
            ("inter_latepusher_x_escape", "has_pusher_late", "escape_count"),
        ]
    if phase == "late":
        return [
            ("inter_save_x_hard_disable", "save_count", "has_hard_disable"),
            ("inter_escape_x_control", "has_escape", "has_control"),
            ("inter_hex_x_hex_count", "has_hex", "hex_count"),
            ("inter_root_x_leash", "has_root", "has_leash"),
            ("inter_pusherlate_x_hg", "has_pusher_late", "hg_defence"),
            ("inter_hardcarry_x_save", "hard_carry", "save_count"),
        ]
    return []


def _extract_lineups_by_pos(match: Dict[str, Any]) -> Tuple[Dict[int, int], Dict[int, int]]:
    radiant: Dict[int, int] = {}
    dire: Dict[int, int] = {}
    players = match.get("players")
    if not isinstance(players, list):
        return radiant, dire
    for player in players:
        if not isinstance(player, dict):
            continue
        hero_id = player.get("heroId") or (player.get("hero") or {}).get("id")
        hero_id = mw._to_int_id(hero_id)  # type: ignore[attr-defined]
        pos_num = mw._extract_position(player.get("position"))  # type: ignore[attr-defined]
        if hero_id is None or pos_num not in POS_SLOTS:
            continue
        if bool(player.get("isRadiant")):
            radiant[int(pos_num)] = int(hero_id)
        else:
            dire[int(pos_num)] = int(hero_id)
    return radiant, dire


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("/Users/alex/Documents/ingame/pro_heroes_data/pro_new_holdout_200kfiles.txt"),
        help="Precomputed matches JSON file",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("reports/metric_experiments/ml_wrapper_vs_baseline_phase_models.json"),
        help="Path to save experiment report",
    )
    parser.add_argument(
        "--max-matches",
        type=int,
        default=50000,
        help="Maximum matches to use (<=0 means full file)",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Match offset before taking max-matches",
    )
    parser.add_argument(
        "--train-ratio",
        type=float,
        default=0.70,
        help="Train split ratio by chronological matches",
    )
    parser.add_argument(
        "--valid-ratio",
        type=float,
        default=0.15,
        help="Validation split ratio by chronological matches",
    )
    parser.add_argument(
        "--high-index-tail",
        type=int,
        default=8,
        help="Rollup only top-N non-empty indices per metric (<=0 means all)",
    )
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=0.30,
        help="Minimum coverage for threshold selection",
    )
    parser.add_argument(
        "--min-train-bets",
        type=int,
        default=200,
        help="Minimum kept bets for threshold selection",
    )
    parser.add_argument(
        "--min-train-bets-per-metric",
        type=int,
        default=80,
        help="Minimum kept bets for per-metric threshold selection",
    )
    parser.add_argument(
        "--min-coverage-per-metric",
        type=float,
        default=0.15,
        help="Minimum coverage for per-metric threshold selection",
    )
    parser.add_argument(
        "--threshold-mode",
        type=str,
        default="phase_metric",
        choices=("phase", "phase_metric"),
        help="Use one threshold per phase or separate threshold per phase+metric",
    )
    parser.add_argument(
        "--min-valid-coverage-ratio",
        type=float,
        default=0.35,
        help="Minimum wrapper/baseline phase coverage ratio on validation when choosing boosts",
    )
    parser.add_argument(
        "--save-model-dir",
        type=Path,
        default=Path("ml-models"),
        help="Directory to store trained early/late model artifacts",
    )
    parser.add_argument(
        "--drop-noisy-metrics",
        action="store_true",
        help="Auto-disable low-support metrics for wrapper application per phase",
    )
    parser.add_argument(
        "--noisy-min-tail-matches",
        type=int,
        default=60,
        help="Minimum baseline tail matches per metric/phase to keep it in wrapper",
    )
    parser.add_argument(
        "--prior-strength",
        type=float,
        default=20.0,
        help="Smoothing strength for metric+index baseline prior probability feature",
    )
    parser.add_argument(
        "--threshold-objective",
        type=str,
        default="delta_times_cov",
        choices=("delta_plus_cov", "delta_times_cov"),
        help="Threshold objective: WR delta + small coverage term, or WR delta multiplied by coverage",
    )
    parser.add_argument(
        "--threshold-cov-weight",
        type=float,
        default=0.01,
        help="Coverage weight for delta_plus_cov threshold objective",
    )
    parser.add_argument(
        "--logreg-c",
        type=float,
        default=0.8,
        help="Inverse regularization strength for logistic regression wrapper",
    )
    parser.add_argument(
        "--include-hero-id-features",
        action="store_true",
        help="Include raw per-position hero-id signed features (+1 own, -1 opponent)",
    )
    return parser.parse_args()


def _to_int_ts(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return 0
        try:
            return int(text)
        except ValueError:
            try:
                return int(float(text))
            except ValueError:
                return 0
    return 0


def _edge_keys_for_phase(phase: str) -> Tuple[str, ...]:
    if phase == "early":
        return EARLY_EDGE_FEATURE_KEYS
    if phase == "late":
        return LATE_EDGE_FEATURE_KEYS
    return tuple()


def _feature_names(phase: str, include_hero_id_features: bool) -> List[str]:
    names: List[str] = []
    names.extend([f"metric_{metric}" for metric in TARGET_METRICS])
    names.extend(["abs_idx", "abs_idx_sq", "weak_idx", "strong_idx"])
    names.extend([f"context_aligned_{metric}" for metric in TARGET_METRICS])
    names.extend(
        [
            "context_agree_count",
            "context_disagree_count",
            "context_zero_count",
            "context_aligned_abs_sum",
            "context_aligned_abs_max",
        ]
    )
    if include_hero_id_features:
        for pos in POS_SLOTS:
            names.extend([f"signed_pos{pos}_hero_{hid}" for hid in HERO_ID_VOCAB])
    keys = list(_edge_keys_for_phase(phase))
    names.extend([f"edge_{key}" for key in keys])
    names.extend([f"edge_abs_{key}" for key in keys])
    names.extend([spec[0] for spec in _interaction_specs_for_phase(phase)])
    names.extend(["prior_wr", "prior_logit", "prior_conf"])
    return names


def _build_feature_vector(
    phase: str,
    metric: str,
    value: float,
    hero_metrics: Dict[str, Any],
    edge_feature_keys: Sequence[str],
    phase_output: Dict[str, Any],
    radiant_by_pos: Dict[int, int],
    dire_by_pos: Dict[int, int],
    include_hero_id_features: bool,
) -> Tuple[List[float], int, int]:
    sign = 1 if value > 0 else -1
    abs_idx = int(round(abs(value)))
    abs_idx = max(1, min(abs_idx, 99))

    vec: List[float] = []
    for metric_name in TARGET_METRICS:
        vec.append(1.0 if metric_name == metric else 0.0)
    vec.append(float(abs_idx))
    vec.append(float(abs_idx * abs_idx))
    vec.append(1.0 if abs_idx <= 3 else 0.0)
    vec.append(1.0 if abs_idx >= 8 else 0.0)

    aligned_vals: List[float] = []
    agree = 0
    disagree = 0
    zero = 0
    for metric_name in TARGET_METRICS:
        raw_ctx = phase_output.get(metric_name, 0.0)
        try:
            ctx_val = float(raw_ctx)
        except (TypeError, ValueError):
            ctx_val = 0.0
        aligned = float(sign) * ctx_val
        aligned_vals.append(aligned)
        vec.append(aligned)
        if aligned > 0:
            agree += 1
        elif aligned < 0:
            disagree += 1
        else:
            zero += 1
    vec.append(float(agree))
    vec.append(float(disagree))
    vec.append(float(zero))
    vec.append(float(sum(abs(v) for v in aligned_vals)))
    vec.append(float(max((abs(v) for v in aligned_vals), default=0.0)))

    if include_hero_id_features:
        # Raw hero-id context by position, aligned to predicted side.
        # +1 for hero on predicted side at this pos, -1 for opponent hero at this pos.
        hero_block = [0.0] * (len(POS_SLOTS) * len(HERO_ID_VOCAB))
        for pos_idx, pos in enumerate(POS_SLOTS):
            own_id = radiant_by_pos.get(pos) if sign > 0 else dire_by_pos.get(pos)
            opp_id = dire_by_pos.get(pos) if sign > 0 else radiant_by_pos.get(pos)
            base = pos_idx * len(HERO_ID_VOCAB)
            own_off = HERO_ID_TO_INDEX.get(int(own_id)) if own_id is not None else None
            opp_off = HERO_ID_TO_INDEX.get(int(opp_id)) if opp_id is not None else None
            if own_off is not None:
                hero_block[base + own_off] += 1.0
            if opp_off is not None:
                hero_block[base + opp_off] -= 1.0
        vec.extend(hero_block)

    signed_edges: Dict[str, float] = {}
    for key in edge_feature_keys:
        raw = hero_metrics.get(key, 0.0)
        try:
            edge = float(raw)
        except (TypeError, ValueError):
            edge = 0.0
        signed = float(sign) * edge
        signed_edges[key] = signed
        vec.append(signed)
    for key in edge_feature_keys:
        vec.append(abs(signed_edges.get(key, 0.0)))
    for _, key_a, key_b in _interaction_specs_for_phase(phase):
        vec.append(float(signed_edges.get(key_a, 0.0) * signed_edges.get(key_b, 0.0)))

    return vec, abs_idx, sign


def _collect_phase_samples(
    matches: Sequence[Dict[str, Any]],
    include_hero_id_features: bool,
) -> Dict[str, List[PhaseSample]]:
    out: Dict[str, List[PhaseSample]] = {"early": [], "late": []}
    early_edge_keys = _edge_keys_for_phase("early")
    late_edge_keys = _edge_keys_for_phase("late")
    for idx, match in enumerate(matches):
        global_idx = int(match.get("_ml_row_idx", idx))
        match_id = match.get("id") or match.get("_map_id") or idx
        hero_metrics = mw._compute_hero_feature_diff(match_id, match) or {}
        radiant_by_pos, dire_by_pos = _extract_lineups_by_pos(match)

        is_early, early_actual = mw.is_early_match(match)
        if is_early and early_actual in {"radiant", "dire"}:
            early_output = match.get("early_output") or {}
            if isinstance(early_output, dict):
                for metric in TARGET_METRICS:
                    value = early_output.get(metric)
                    if not isinstance(value, (int, float)) or float(value) == 0.0:
                        continue
                    fv, abs_idx, sign = _build_feature_vector(
                        "early",
                        metric,
                        float(value),
                        hero_metrics,
                        early_edge_keys,
                        early_output,
                        radiant_by_pos,
                        dire_by_pos,
                        include_hero_id_features=include_hero_id_features,
                    )
                    predicted = "radiant" if sign > 0 else "dire"
                    label = 1 if predicted == early_actual else 0
                    out["early"].append(
                        PhaseSample(
                            match_idx=global_idx,
                            phase="early",
                            metric=metric,
                            value=float(value),
                            abs_idx=abs_idx,
                            sign=sign,
                            label=label,
                            features=fv,
                        )
                    )

        is_late, late_actual = mw.is_late_match(match)
        if is_late and late_actual in {"radiant", "dire"}:
            late_output = match.get("late_output") or match.get("mid_output") or {}
            if isinstance(late_output, dict):
                for metric in TARGET_METRICS:
                    value = late_output.get(metric)
                    if not isinstance(value, (int, float)) or float(value) == 0.0:
                        continue
                    fv, abs_idx, sign = _build_feature_vector(
                        "late",
                        metric,
                        float(value),
                        hero_metrics,
                        late_edge_keys,
                        late_output,
                        radiant_by_pos,
                        dire_by_pos,
                        include_hero_id_features=include_hero_id_features,
                    )
                    predicted = "radiant" if sign > 0 else "dire"
                    label = 1 if predicted == late_actual else 0
                    out["late"].append(
                        PhaseSample(
                            match_idx=global_idx,
                            phase="late",
                            metric=metric,
                            value=float(value),
                            abs_idx=abs_idx,
                            sign=sign,
                            label=label,
                            features=fv,
                        )
                    )
    return out


def _split_bounds(total_matches: int, train_ratio: float, valid_ratio: float) -> Tuple[int, int]:
    train_ratio = min(max(train_ratio, 0.10), 0.90)
    valid_ratio = min(max(valid_ratio, 0.05), 0.40)
    if train_ratio + valid_ratio >= 0.95:
        valid_ratio = 0.95 - train_ratio
    train_end = int(total_matches * train_ratio)
    valid_end = int(total_matches * (train_ratio + valid_ratio))
    train_end = max(1, min(train_end, total_matches - 2))
    valid_end = max(train_end + 1, min(valid_end, total_matches - 1))
    return train_end, valid_end


def _phase_sample_split(
    samples: Sequence[PhaseSample],
    train_end: int,
    valid_end: int,
) -> Tuple[List[PhaseSample], List[PhaseSample], List[PhaseSample]]:
    train: List[PhaseSample] = []
    valid: List[PhaseSample] = []
    test: List[PhaseSample] = []
    for sample in samples:
        if sample.match_idx < train_end:
            train.append(sample)
        elif sample.match_idx < valid_end:
            valid.append(sample)
        else:
            test.append(sample)
    return train, valid, test


def _samples_to_matrix(samples: Sequence[PhaseSample]) -> Tuple[np.ndarray, np.ndarray]:
    x = np.asarray([sample.features for sample in samples], dtype=np.float32)
    y = np.asarray([sample.label for sample in samples], dtype=np.int8)
    return x, y


def _build_metric_index_priors(
    train_samples: Sequence[PhaseSample],
    smoothing: float,
) -> Dict[str, Any]:
    smoothing = max(1.0, float(smoothing))
    per_metric: Dict[str, Dict[str, Any]] = {}
    for metric in TARGET_METRICS:
        subset = [s for s in train_samples if s.metric == metric]
        if subset:
            wins = float(sum(s.label for s in subset))
            total = float(len(subset))
            metric_base = (wins + 1.0) / (total + 2.0)
        else:
            metric_base = 0.5
        idx_stat: Dict[int, Tuple[float, float]] = {}
        for idx in range(1, 100):
            rows = [s for s in subset if s.abs_idx == idx]
            if rows:
                w = float(sum(s.label for s in rows))
                n = float(len(rows))
                idx_stat[idx] = (w, n)
        per_metric[metric] = {"base": metric_base, "idx_stat": idx_stat}
    return {"smoothing": smoothing, "per_metric": per_metric}


def _prior_prob(priors: Dict[str, Any], metric: str, abs_idx: int) -> float:
    m = priors.get("per_metric", {}).get(metric, {})
    base = float(m.get("base", 0.5))
    wins, total = m.get("idx_stat", {}).get(abs_idx, (0.0, 0.0))
    smoothing = float(priors.get("smoothing", 20.0))
    prob = (wins + smoothing * base) / (total + smoothing) if (total + smoothing) > 0 else base
    return max(1e-4, min(prob, 1.0 - 1e-4))


def _append_prior_features(samples: Sequence[PhaseSample], priors: Dict[str, Any]) -> None:
    for s in samples:
        p0 = _prior_prob(priors, s.metric, s.abs_idx)
        logit = float(np.log(p0 / (1.0 - p0)))
        s.features.extend([p0, logit, abs(logit)])


def _fit_model(samples: Sequence[PhaseSample], logreg_c: float) -> Optional[Pipeline]:
    if len(samples) < 200:
        return None
    x, y = _samples_to_matrix(samples)
    if x.size == 0 or len(np.unique(y)) < 2:
        return None
    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "clf",
                LogisticRegression(
                    max_iter=1500,
                    class_weight="balanced",
                    C=max(1e-4, float(logreg_c)),
                    solver="lbfgs",
                    random_state=42,
                ),
            ),
        ]
    )
    model.fit(x, y)
    return model


def _predict_probs(model: Optional[Pipeline], samples: Sequence[PhaseSample]) -> np.ndarray:
    if model is None or not samples:
        return np.zeros((len(samples),), dtype=np.float32)
    x, _ = _samples_to_matrix(samples)
    probs = model.predict_proba(x)[:, 1]
    return probs.astype(np.float32)


def _pick_threshold(
    labels: np.ndarray,
    probs: np.ndarray,
    min_coverage: float,
    min_bets: int,
    objective_mode: str,
    cov_weight: float,
) -> Dict[str, Any]:
    if labels.size == 0:
        return {"threshold": 0.5, "coverage": 0.0, "winrate": None, "baseline_winrate": None}
    base_wr = float(labels.mean())
    thresholds = [round(x, 2) for x in np.arange(0.45, 0.91, 0.02)]

    best: Optional[Dict[str, Any]] = None
    for thr in thresholds:
        mask = probs >= thr
        kept = int(mask.sum())
        if kept < max(1, min_bets):
            continue
        cov = kept / float(labels.size)
        if cov < min_coverage:
            continue
        wr = float(labels[mask].mean())
        delta = wr - base_wr
        if objective_mode == "delta_times_cov":
            objective = delta * cov
        else:
            objective = delta + float(cov_weight) * cov
        row = {
            "threshold": thr,
            "kept": kept,
            "coverage": cov,
            "winrate": wr,
            "winrate_delta_vs_baseline": delta,
            "objective": objective,
            "baseline_winrate": base_wr,
        }
        if best is None or row["objective"] > best["objective"]:
            best = row

    if best is None:
        thr = 0.50
        mask = probs >= thr
        kept = int(mask.sum())
        cov = kept / float(labels.size)
        wr = float(labels[mask].mean()) if kept > 0 else None
        return {
            "threshold": thr,
            "kept": kept,
            "coverage": cov,
            "winrate": wr,
            "winrate_delta_vs_baseline": (wr - base_wr) if wr is not None else None,
            "objective": None,
            "baseline_winrate": base_wr,
        }
    return best


def _pick_thresholds_per_metric(
    samples: Sequence[PhaseSample],
    min_coverage: float,
    min_bets: int,
    objective_mode: str,
    cov_weight: float,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for metric in TARGET_METRICS:
        subset = [s for s in samples if s.metric == metric]
        if not subset:
            out[metric] = {"threshold": 0.5, "coverage": 0.0, "winrate": None, "baseline_winrate": None}
            continue
        labels = np.asarray([s.label for s in subset], dtype=np.int8)
        probs = np.asarray([float(s.prob or 0.0) for s in subset], dtype=np.float32)
        out[metric] = _pick_threshold(
            labels=labels,
            probs=probs,
            min_coverage=min_coverage,
            min_bets=min_bets,
            objective_mode=objective_mode,
            cov_weight=cov_weight,
        )
    return out


def _boost_abs_index(abs_idx: int, prob: float, threshold: float, boost_strength: float) -> int:
    abs_idx = max(1, min(int(abs_idx), 99))
    if prob <= threshold or boost_strength <= 0.0:
        return abs_idx
    denom = max(1e-6, 1.0 - threshold)
    confidence = max(0.0, min((prob - threshold) / denom, 1.0))
    mult = 1.0 + (boost_strength * confidence)
    # Additional lift for weak signals if model confidence is high.
    if abs_idx <= 3:
        mult += 0.5 * boost_strength * confidence
    boosted = int(round(abs_idx * mult))
    return max(1, min(boosted, 99))


def _build_prob_map(samples: Sequence[PhaseSample]) -> Dict[Tuple[int, str, str], float]:
    out: Dict[Tuple[int, str, str], float] = {}
    for sample in samples:
        if sample.prob is None:
            continue
        out[(sample.match_idx, sample.phase, sample.metric)] = float(sample.prob)
    return out


def _resolve_threshold(
    phase: str,
    metric: str,
    thresholds: Dict[str, float],
    thresholds_by_phase_metric: Optional[Dict[str, Dict[str, float]]],
) -> float:
    if thresholds_by_phase_metric:
        phase_map = thresholds_by_phase_metric.get(phase, {})
        if metric in phase_map:
            return float(phase_map[metric])
    return float(thresholds.get(phase, 0.5))


def _default_allowed_metrics_by_phase() -> Dict[str, List[str]]:
    return {"early": list(TARGET_METRICS), "late": list(TARGET_METRICS)}


def _select_allowed_metrics_by_phase(
    baseline_summary: Dict[str, Any],
    min_tail_matches: int,
) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    min_tail_matches = max(1, int(min_tail_matches))
    for phase in ("early", "late"):
        phase_rows = baseline_summary.get(phase, {}).get("metrics", [])
        metric_to_matches: Dict[str, int] = {}
        for row in phase_rows:
            if not isinstance(row, dict):
                continue
            full_metric = str(row.get("metric", ""))
            prefix = f"{phase}_"
            if not full_metric.startswith(prefix):
                continue
            metric = full_metric[len(prefix) :]
            if metric in TARGET_METRICS:
                metric_to_matches[metric] = int(row.get("matches") or 0)
        keep = [m for m in TARGET_METRICS if metric_to_matches.get(m, 0) >= min_tail_matches]
        if not keep:
            keep = list(TARGET_METRICS)
        out[phase] = keep
    return out


def _apply_wrapper_to_matches(
    matches: List[Dict[str, Any]],
    prob_map: Dict[Tuple[int, str, str], float],
    thresholds: Dict[str, float],
    boost_early: float,
    boost_late: float,
    thresholds_by_phase_metric: Optional[Dict[str, Dict[str, float]]] = None,
    allowed_metrics_by_phase: Optional[Dict[str, List[str]]] = None,
) -> Dict[str, int]:
    allowed = allowed_metrics_by_phase or _default_allowed_metrics_by_phase()
    stats = {"updated": 0, "zeroed": 0, "boosted": 0}
    for match_idx, match in enumerate(matches):
        global_idx = int(match.get("_ml_row_idx", match_idx))
        for phase, output_key in (("early", "early_output"), ("late", "late_output")):
            phase_output = match.get(output_key)
            if not isinstance(phase_output, dict):
                continue
            phase_allowed = set(allowed.get(phase, list(TARGET_METRICS)))
            boost_strength = boost_early if phase == "early" else boost_late
            for metric in TARGET_METRICS:
                if metric not in phase_allowed:
                    continue
                value = phase_output.get(metric)
                if not isinstance(value, (int, float)):
                    continue
                value_f = float(value)
                if value_f == 0.0:
                    continue
                key = (global_idx, phase, metric)
                prob = prob_map.get(key)
                if prob is None:
                    continue
                threshold = _resolve_threshold(
                    phase=phase,
                    metric=metric,
                    thresholds=thresholds,
                    thresholds_by_phase_metric=thresholds_by_phase_metric,
                )
                sign = 1 if value_f > 0 else -1
                abs_idx = max(1, min(int(round(abs(value_f))), 99))
                if prob < threshold:
                    phase_output[metric] = 0
                    stats["zeroed"] += 1
                    stats["updated"] += 1
                    continue
                boosted_abs = _boost_abs_index(abs_idx, prob, threshold, boost_strength)
                if boosted_abs != abs_idx:
                    stats["boosted"] += 1
                phase_output[metric] = int(sign * boosted_abs)
                stats["updated"] += 1
    return stats


def _bucket_wins_matches(bucket: Dict[str, Any]) -> Tuple[int, int]:
    pos = bucket.get("positive", {})
    neg = bucket.get("negative", {})
    wins = int(pos.get("wins", 0)) + int(neg.get("wins", 0))
    losses = int(pos.get("loses", 0)) + int(neg.get("loses", 0))
    return wins, wins + losses


def _metric_bucket(metric_data: Dict[str, Any], index: int) -> Dict[str, Any] | None:
    bucket = metric_data.get(index)
    if isinstance(bucket, dict):
        return bucket
    bucket = metric_data.get(str(index))
    if isinstance(bucket, dict):
        return bucket
    return None


def _non_empty_indices(metric_data: Dict[str, Any]) -> List[int]:
    indices: List[int] = []
    for raw_index in metric_data.keys():
        try:
            index = int(raw_index)
        except (TypeError, ValueError):
            continue
        bucket = _metric_bucket(metric_data, index)
        if not isinstance(bucket, dict):
            continue
        _, matches = _bucket_wins_matches(bucket)
        if matches > 0:
            indices.append(index)
    return sorted(set(indices))


def _metric_rollup(
    results: Dict[str, Any],
    metric_name: str,
    selected_indices: Optional[List[int]] = None,
    high_index_tail: Optional[int] = None,
) -> Dict[str, Any]:
    metric_data = results.get(metric_name, {})
    if not isinstance(metric_data, dict):
        return {"metric": metric_name, "wins": 0, "matches": 0, "winrate": None, "selected_indices": []}

    if selected_indices is None:
        if high_index_tail is not None and high_index_tail > 0:
            selected_indices = _non_empty_indices(metric_data)[-high_index_tail:]
        else:
            selected_indices = sorted(
                {int(raw) for raw in metric_data.keys() if str(raw).lstrip("-").isdigit()}
            )

    total_wins = 0
    total_matches = 0
    for index in selected_indices:
        bucket = _metric_bucket(metric_data, index)
        if not isinstance(bucket, dict):
            continue
        wins, matches = _bucket_wins_matches(bucket)
        total_wins += wins
        total_matches += matches

    return {
        "metric": metric_name,
        "wins": total_wins,
        "matches": total_matches,
        "winrate": (total_wins / total_matches) if total_matches > 0 else None,
        "selected_indices": selected_indices,
    }


def _phase_summary(
    results: Dict[str, Any],
    phase: str,
    high_index_tail: Optional[int] = None,
    selected_indices_map: Optional[Dict[str, List[int]]] = None,
) -> Dict[str, Any]:
    rows = []
    selected_by_metric: Dict[str, List[int]] = {}
    for metric in TARGET_METRICS:
        full_name = f"{phase}_{metric}"
        fixed = None if selected_indices_map is None else selected_indices_map.get(full_name)
        row = _metric_rollup(
            results,
            full_name,
            selected_indices=fixed,
            high_index_tail=high_index_tail,
        )
        rows.append(row)
        selected_by_metric[full_name] = list(row.get("selected_indices") or [])
    valid = [row for row in rows if row.get("winrate") is not None]
    weighted_wins = sum(int(row["wins"]) for row in valid)
    weighted_matches = sum(int(row["matches"]) for row in valid)
    return {
        "phase": phase,
        "metrics": rows,
        "weighted_winrate": (weighted_wins / weighted_matches) if weighted_matches > 0 else None,
        "mean_metric_winrate": (sum(float(row["winrate"]) for row in valid) / len(valid)) if valid else None,
        "total_matches_across_metrics": weighted_matches,
        "selected_indices_by_metric": selected_by_metric,
    }


def _summarize_results(
    results: Dict[str, Any],
    high_index_tail: Optional[int],
    selected_indices_map: Optional[Dict[str, List[int]]] = None,
) -> Tuple[Dict[str, Any], Dict[str, List[int]]]:
    early = _phase_summary(
        results,
        "early",
        high_index_tail=high_index_tail,
        selected_indices_map=selected_indices_map,
    )
    late = _phase_summary(
        results,
        "late",
        high_index_tail=high_index_tail,
        selected_indices_map=selected_indices_map,
    )
    used_indices: Dict[str, List[int]] = {}
    used_indices.update(early.get("selected_indices_by_metric", {}))
    used_indices.update(late.get("selected_indices_by_metric", {}))
    return {"early": early, "late": late}, used_indices


def _run_metrics(matches: List[Dict[str, Any]]) -> Dict[str, Any]:
    mw.EARLY_MIN_INDEX = 1
    mw.LATE_MIN_INDEX = 1
    results, _, _ = mw.process_metrics_winrate(
        matches,
        use_train_dicts=False,
        early_filter_fn=mw.is_early_match,
        late_filter_fn=mw.is_late_match,
    )
    return results


def _phase_delta(after: Dict[str, Any], before: Dict[str, Any], phase: str) -> Optional[float]:
    a = after[phase].get("weighted_winrate")
    b = before[phase].get("weighted_winrate")
    if a is None or b is None:
        return None
    return float(a) - float(b)


def _coverage_ratio(after: Dict[str, Any], before: Dict[str, Any], phase: str) -> Optional[float]:
    a = float(after[phase].get("total_matches_across_metrics") or 0.0)
    b = float(before[phase].get("total_matches_across_metrics") or 0.0)
    if b <= 0:
        return None
    return a / b


def _select_boosts_on_validation(
    valid_matches: List[Dict[str, Any]],
    valid_samples: Dict[str, List[PhaseSample]],
    thresholds: Dict[str, float],
    thresholds_by_phase_metric: Optional[Dict[str, Dict[str, float]]],
    high_index_tail: Optional[int],
    min_valid_coverage_ratio: float,
    drop_noisy_metrics: bool,
    noisy_min_tail_matches: int,
) -> Dict[str, Any]:
    baseline_results = _run_metrics(deepcopy(valid_matches))
    baseline_summary, selected_indices = _summarize_results(
        baseline_results,
        high_index_tail=high_index_tail,
    )
    if drop_noisy_metrics:
        allowed_metrics_by_phase = _select_allowed_metrics_by_phase(
            baseline_summary=baseline_summary,
            min_tail_matches=noisy_min_tail_matches,
        )
    else:
        allowed_metrics_by_phase = _default_allowed_metrics_by_phase()

    prob_map = {}
    prob_map.update(_build_prob_map(valid_samples["early"]))
    prob_map.update(_build_prob_map(valid_samples["late"]))

    boost_grid = [0.0, 0.5, 1.0, 1.5]
    best: Optional[Dict[str, Any]] = None
    for early_boost in boost_grid:
        for late_boost in boost_grid:
            wrapped = deepcopy(valid_matches)
            _apply_wrapper_to_matches(
                wrapped,
                prob_map=prob_map,
                thresholds=thresholds,
                boost_early=early_boost,
                boost_late=late_boost,
                thresholds_by_phase_metric=thresholds_by_phase_metric,
                allowed_metrics_by_phase=allowed_metrics_by_phase,
            )
            wrapped_results = _run_metrics(wrapped)
            wrapped_summary, _ = _summarize_results(
                wrapped_results,
                high_index_tail=high_index_tail,
                selected_indices_map=selected_indices,
            )
            early_cov = _coverage_ratio(wrapped_summary, baseline_summary, "early")
            late_cov = _coverage_ratio(wrapped_summary, baseline_summary, "late")
            if early_cov is None or late_cov is None:
                continue
            if early_cov < min_valid_coverage_ratio or late_cov < min_valid_coverage_ratio:
                continue
            d_early = _phase_delta(wrapped_summary, baseline_summary, "early")
            d_late = _phase_delta(wrapped_summary, baseline_summary, "late")
            if d_early is None or d_late is None:
                continue
            objective = d_early + d_late
            row = {
                "early_boost": early_boost,
                "late_boost": late_boost,
                "objective_delta_sum": objective,
                "early_delta": d_early,
                "late_delta": d_late,
                "early_coverage_ratio": early_cov,
                "late_coverage_ratio": late_cov,
            }
            if best is None or row["objective_delta_sum"] > best["objective_delta_sum"]:
                best = row

    if best is None:
        best = {
            "early_boost": 1.0,
            "late_boost": 1.0,
            "objective_delta_sum": None,
            "early_delta": None,
            "late_delta": None,
            "early_coverage_ratio": None,
            "late_coverage_ratio": None,
        }
    best["baseline_summary_valid"] = baseline_summary
    best["allowed_metrics_by_phase"] = allowed_metrics_by_phase
    best["dropped_metrics_by_phase"] = {
        phase: [m for m in TARGET_METRICS if m not in set(allowed_metrics_by_phase.get(phase, []))]
        for phase in ("early", "late")
    }
    return best


def _sample_stats(samples: Sequence[PhaseSample], threshold: float) -> Dict[str, Any]:
    if not samples:
        return {"bets": 0, "wr": None, "coverage": 0.0}
    labels = np.asarray([sample.label for sample in samples], dtype=np.float32)
    probs = np.asarray([float(sample.prob or 0.0) for sample in samples], dtype=np.float32)
    mask = probs >= threshold
    kept = int(mask.sum())
    base_wr = float(labels.mean())
    if kept == 0:
        return {
            "bets": 0,
            "wr": None,
            "coverage": 0.0,
            "baseline_wr": base_wr,
            "wr_delta_vs_baseline": None,
        }
    wr = float(labels[mask].mean())
    return {
        "bets": kept,
        "wr": wr,
        "coverage": kept / float(len(samples)),
        "baseline_wr": base_wr,
        "wr_delta_vs_baseline": wr - base_wr,
    }


def _sample_stats_with_threshold_map(
    samples: Sequence[PhaseSample],
    phase: str,
    thresholds: Dict[str, float],
    thresholds_by_phase_metric: Optional[Dict[str, Dict[str, float]]],
) -> Dict[str, Any]:
    if not samples:
        return {"bets": 0, "wr": None, "coverage": 0.0}
    labels = np.asarray([sample.label for sample in samples], dtype=np.float32)
    probs = np.asarray([float(sample.prob or 0.0) for sample in samples], dtype=np.float32)
    keep_mask = np.zeros((len(samples),), dtype=bool)
    for i, sample in enumerate(samples):
        thr = _resolve_threshold(
            phase=phase,
            metric=sample.metric,
            thresholds=thresholds,
            thresholds_by_phase_metric=thresholds_by_phase_metric,
        )
        keep_mask[i] = probs[i] >= thr
    kept = int(keep_mask.sum())
    base_wr = float(labels.mean())
    if kept == 0:
        return {
            "bets": 0,
            "wr": None,
            "coverage": 0.0,
            "baseline_wr": base_wr,
            "wr_delta_vs_baseline": None,
        }
    wr = float(labels[keep_mask].mean())
    return {
        "bets": kept,
        "wr": wr,
        "coverage": kept / float(len(samples)),
        "baseline_wr": base_wr,
        "wr_delta_vs_baseline": wr - base_wr,
    }


def main() -> None:
    args = _parse_args()
    if not args.input.exists():
        raise FileNotFoundError(f"Input file not found: {args.input}")

    start_ts = time.time()
    matches = mw.load_matches(str(args.input))
    if args.offset > 0:
        matches = matches[int(args.offset) :]
    if args.max_matches and args.max_matches > 0:
        matches = matches[: int(args.max_matches)]
    matches = sorted(matches, key=lambda m: _to_int_ts(m.get("startDateTime")))
    for idx, match in enumerate(matches):
        match["_ml_row_idx"] = idx
    print(f"Loaded matches: {len(matches)}")

    train_end, valid_end = _split_bounds(len(matches), args.train_ratio, args.valid_ratio)
    print(f"Split bounds: train=[0,{train_end}), valid=[{train_end},{valid_end}), test=[{valid_end},{len(matches)})")

    include_hero_id_features = bool(args.include_hero_id_features)
    samples_by_phase = _collect_phase_samples(
        matches,
        include_hero_id_features=include_hero_id_features,
    )
    print(
        "Samples: "
        f"early={len(samples_by_phase['early'])}, "
        f"late={len(samples_by_phase['late'])}"
    )

    phase_models: Dict[str, Optional[Pipeline]] = {"early": None, "late": None}
    phase_thresholds: Dict[str, float] = {"early": 0.5, "late": 0.5}
    thresholds_by_phase_metric: Dict[str, Dict[str, float]] = {"early": {}, "late": {}}
    phase_tune_meta: Dict[str, Dict[str, Any]] = {}
    split_samples: Dict[str, Dict[str, List[PhaseSample]]] = {"early": {}, "late": {}}
    phase_priors: Dict[str, Dict[str, Any]] = {}

    for phase in ("early", "late"):
        train_samples, valid_samples, test_samples = _phase_sample_split(
            samples_by_phase[phase],
            train_end=train_end,
            valid_end=valid_end,
        )
        priors = _build_metric_index_priors(
            train_samples=train_samples,
            smoothing=float(args.prior_strength),
        )
        phase_priors[phase] = priors
        _append_prior_features(train_samples, priors)
        _append_prior_features(valid_samples, priors)
        _append_prior_features(test_samples, priors)

        split_samples[phase]["train"] = train_samples
        split_samples[phase]["valid"] = valid_samples
        split_samples[phase]["test"] = test_samples

        model = _fit_model(train_samples, logreg_c=float(args.logreg_c))
        phase_models[phase] = model
        if model is None:
            print(f"[{phase}] model training skipped (insufficient data)")
            continue
        valid_probs = _predict_probs(model, valid_samples)
        for sample, prob in zip(valid_samples, valid_probs):
            sample.prob = float(prob)
        labels = np.asarray([sample.label for sample in valid_samples], dtype=np.int8)
        tune_global = _pick_threshold(
            labels=labels,
            probs=valid_probs,
            min_coverage=float(args.min_coverage),
            min_bets=int(args.min_train_bets),
            objective_mode=str(args.threshold_objective),
            cov_weight=float(args.threshold_cov_weight),
        )
        phase_thresholds[phase] = float(tune_global["threshold"])
        tune_metric = _pick_thresholds_per_metric(
            samples=valid_samples,
            min_coverage=float(args.min_coverage_per_metric),
            min_bets=int(args.min_train_bets_per_metric),
            objective_mode=str(args.threshold_objective),
            cov_weight=float(args.threshold_cov_weight),
        )
        thresholds_by_phase_metric[phase] = {
            metric: float(meta.get("threshold", 0.5))
            for metric, meta in tune_metric.items()
        }
        phase_tune_meta[phase] = {
            "global": tune_global,
            "per_metric": tune_metric,
        }
        print(
            f"[{phase}] threshold={phase_thresholds[phase]:.2f} "
            f"valid_wr={tune_global.get('winrate')} cov={tune_global.get('coverage')}"
        )

        # Fill probabilities for train/test for later stats and wrapper map.
        train_probs = _predict_probs(model, train_samples)
        test_probs = _predict_probs(model, test_samples)
        for sample, prob in zip(train_samples, train_probs):
            sample.prob = float(prob)
        for sample, prob in zip(test_samples, test_probs):
            sample.prob = float(prob)

    valid_matches = matches[train_end:valid_end]
    valid_phase_samples = {
        "early": split_samples["early"].get("valid", []),
        "late": split_samples["late"].get("valid", []),
    }
    boost_pick = _select_boosts_on_validation(
        valid_matches=valid_matches,
        valid_samples=valid_phase_samples,
        thresholds=phase_thresholds,
        thresholds_by_phase_metric=(
            thresholds_by_phase_metric if args.threshold_mode == "phase_metric" else None
        ),
        high_index_tail=(args.high_index_tail if args.high_index_tail > 0 else None),
        min_valid_coverage_ratio=float(args.min_valid_coverage_ratio),
        drop_noisy_metrics=bool(args.drop_noisy_metrics),
        noisy_min_tail_matches=int(args.noisy_min_tail_matches),
    )
    boost_early = float(boost_pick["early_boost"])
    boost_late = float(boost_pick["late_boost"])
    allowed_metrics_by_phase = boost_pick.get("allowed_metrics_by_phase") or _default_allowed_metrics_by_phase()
    print(f"Boosts selected on valid: early={boost_early:.2f}, late={boost_late:.2f}")

    test_matches = matches[valid_end:]
    baseline_test_results = _run_metrics(deepcopy(test_matches))
    high_tail = args.high_index_tail if args.high_index_tail > 0 else None
    baseline_test_summary, test_selected_indices = _summarize_results(
        baseline_test_results,
        high_index_tail=high_tail,
    )

    test_prob_map = {}
    test_prob_map.update(_build_prob_map(split_samples["early"].get("test", [])))
    test_prob_map.update(_build_prob_map(split_samples["late"].get("test", [])))
    wrapped_test_matches = deepcopy(test_matches)
    wrapper_transform_stats = _apply_wrapper_to_matches(
        wrapped_test_matches,
        prob_map=test_prob_map,
        thresholds=phase_thresholds,
        boost_early=boost_early,
        boost_late=boost_late,
        thresholds_by_phase_metric=(
            thresholds_by_phase_metric if args.threshold_mode == "phase_metric" else None
        ),
        allowed_metrics_by_phase=allowed_metrics_by_phase,
    )
    wrapped_test_results = _run_metrics(wrapped_test_matches)
    wrapped_test_summary, _ = _summarize_results(
        wrapped_test_results,
        high_index_tail=high_tail,
        selected_indices_map=test_selected_indices,
    )

    test_deltas = {
        "early_weighted_wr_delta": _phase_delta(wrapped_test_summary, baseline_test_summary, "early"),
        "late_weighted_wr_delta": _phase_delta(wrapped_test_summary, baseline_test_summary, "late"),
        "early_coverage_ratio": _coverage_ratio(wrapped_test_summary, baseline_test_summary, "early"),
        "late_coverage_ratio": _coverage_ratio(wrapped_test_summary, baseline_test_summary, "late"),
    }

    sample_eval = {
        "early": {
            "baseline_test": _sample_stats(split_samples["early"].get("test", []), threshold=-1.0),
            "wrapper_test": _sample_stats_with_threshold_map(
                split_samples["early"].get("test", []),
                phase="early",
                thresholds=phase_thresholds,
                thresholds_by_phase_metric=(
                    thresholds_by_phase_metric if args.threshold_mode == "phase_metric" else None
                ),
            ),
        },
        "late": {
            "baseline_test": _sample_stats(split_samples["late"].get("test", []), threshold=-1.0),
            "wrapper_test": _sample_stats_with_threshold_map(
                split_samples["late"].get("test", []),
                phase="late",
                thresholds=phase_thresholds,
                thresholds_by_phase_metric=(
                    thresholds_by_phase_metric if args.threshold_mode == "phase_metric" else None
                ),
            ),
        },
    }

    args.save_model_dir.mkdir(parents=True, exist_ok=True)
    model_paths: Dict[str, Optional[str]] = {"early": None, "late": None}
    feature_names_by_phase = {
        "early": _feature_names("early", include_hero_id_features=include_hero_id_features),
        "late": _feature_names("late", include_hero_id_features=include_hero_id_features),
    }
    edge_feature_keys_by_phase = {
        "early": list(_edge_keys_for_phase("early")),
        "late": list(_edge_keys_for_phase("late")),
    }
    for phase in ("early", "late"):
        model = phase_models.get(phase)
        if model is None:
            continue
        path = args.save_model_dir / f"phase_signal_wrapper_{phase}.pkl"
        payload = {
            "phase": phase,
            "model": model,
            "feature_names": feature_names_by_phase[phase],
            "target_metrics": list(TARGET_METRICS),
            "edge_feature_keys": edge_feature_keys_by_phase[phase],
            "threshold": float(phase_thresholds[phase]),
            "threshold_mode": str(args.threshold_mode),
            "thresholds_by_metric": thresholds_by_phase_metric.get(phase, {}),
            "boost_strength": float(boost_early if phase == "early" else boost_late),
            "prior_strength": float(args.prior_strength),
            "logreg_c": float(args.logreg_c),
            "include_hero_id_features": include_hero_id_features,
        }
        with path.open("wb") as f:
            pickle.dump(payload, f)
        model_paths[phase] = str(path)

    report = {
        "input_file": str(args.input),
        "matches_count": len(matches),
        "splits": {
            "train_matches": train_end,
            "valid_matches": max(valid_end - train_end, 0),
            "test_matches": max(len(matches) - valid_end, 0),
            "train_ratio": args.train_ratio,
            "valid_ratio": args.valid_ratio,
        },
        "target_metrics": list(TARGET_METRICS),
        "feature_names_by_phase": feature_names_by_phase,
        "edge_feature_keys_by_phase": edge_feature_keys_by_phase,
        "threshold_mode": str(args.threshold_mode),
        "threshold_objective": str(args.threshold_objective),
        "threshold_cov_weight": float(args.threshold_cov_weight),
        "logreg_c": float(args.logreg_c),
        "include_hero_id_features": include_hero_id_features,
        "thresholds": phase_thresholds,
        "thresholds_by_phase_metric": thresholds_by_phase_metric,
        "threshold_tuning": phase_tune_meta,
        "prior_strength": float(args.prior_strength),
        "boost_selection_on_valid": boost_pick,
        "selected_boosts": {"early": boost_early, "late": boost_late},
        "allowed_metrics_by_phase": allowed_metrics_by_phase,
        "dropped_metrics_by_phase": {
            phase: [m for m in TARGET_METRICS if m not in set(allowed_metrics_by_phase.get(phase, []))]
            for phase in ("early", "late")
        },
        "sample_level_eval": sample_eval,
        "metrics_eval_test": {
            "baseline": baseline_test_summary,
            "wrapper": wrapped_test_summary,
            "delta_wrapper_minus_baseline": test_deltas,
            "wrapper_transform_stats": wrapper_transform_stats,
            "high_index_tail": high_tail,
            "selected_indices_by_metric": test_selected_indices,
        },
        "model_artifacts": model_paths,
        "runtime_seconds": round(time.time() - start_ts, 3),
    }

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    def _pct(v: Any) -> str:
        if v is None:
            return "n/a"
        return f"{float(v):.2%}"

    print(f"Saved report: {args.output_json}")
    print(
        "Test weighted WR (tail): "
        f"early { _pct(baseline_test_summary['early']['weighted_winrate']) } -> { _pct(wrapped_test_summary['early']['weighted_winrate']) }, "
        f"late { _pct(baseline_test_summary['late']['weighted_winrate']) } -> { _pct(wrapped_test_summary['late']['weighted_winrate']) }"
    )
    print(
        "Test coverage ratio: "
        f"early={_pct(test_deltas['early_coverage_ratio'])}, "
        f"late={_pct(test_deltas['late_coverage_ratio'])}"
    )


if __name__ == "__main__":
    main()
