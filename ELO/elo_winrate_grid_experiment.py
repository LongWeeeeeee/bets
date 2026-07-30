from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from bisect import bisect_right
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ELO.config import EvaluationConfig, HybridEloConfig
from ELO.data_loader import load_matches
from ELO.domain import LeagueTier, SeriesBundle
from ELO.evaluation import _clip_probability
from ELO.models import HybridPlayerRosterEloModel
from ELO.series_data import build_series_bundles
from ELO.series_evaluation import probability_to_win_series
from ELO.tiering import attach_league_tiers, classify_leagues, get_known_team_tier


SEGMENT_OVERALL = "overall"
SEGMENT_TIER1_ONLY = "tier1_only"
SEGMENT_TIER2_ONLY = "tier2_only"
SEGMENT_TIER1_VS_TIER2 = "tier1_vs_tier2"
SEGMENT_OTHER = "other"

LEAGUE_SEGMENT_TIER1 = "league_tier1"
LEAGUE_SEGMENT_TIER2 = "league_tier2"
LEAGUE_SEGMENT_TIER3 = "league_tier3"

DEFAULT_GRID_BINS = 8
DEFAULT_GRID_PRIOR_STRENGTH = 24.0


def _to_json_ready(value: Any) -> Any:
    if isinstance(value, LeagueTier):
        return value.value
    if is_dataclass(value):
        return _to_json_ready(asdict(value))
    if isinstance(value, dict):
        return {str(key): _to_json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_ready(item) for item in value]
    return value


def _timestamp_to_iso(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _default_data_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "pro_heroes_data" / "json_parts_split_from_object"


def _default_output_dir() -> Path:
    return Path(__file__).resolve().parent / "output" / "elo_winrate_grid_experiment"


def _known_team_segment(
    team_a_id: int | None,
    team_a_name: str,
    team_b_id: int | None,
    team_b_name: str,
) -> str:
    team_a_tier = get_known_team_tier(team_a_id, team_a_name)
    team_b_tier = get_known_team_tier(team_b_id, team_b_name)
    if team_a_tier == LeagueTier.TIER1 and team_b_tier == LeagueTier.TIER1:
        return SEGMENT_TIER1_ONLY
    if team_a_tier == LeagueTier.TIER2 and team_b_tier == LeagueTier.TIER2:
        return SEGMENT_TIER2_ONLY
    if {team_a_tier, team_b_tier} == {LeagueTier.TIER1, LeagueTier.TIER2}:
        return SEGMENT_TIER1_VS_TIER2
    return SEGMENT_OTHER


def _league_segment(tier: LeagueTier) -> str:
    if tier == LeagueTier.TIER1:
        return LEAGUE_SEGMENT_TIER1
    if tier == LeagueTier.TIER2:
        return LEAGUE_SEGMENT_TIER2
    return LEAGUE_SEGMENT_TIER3


def _variant_definitions() -> dict[str, Callable[[dict[str, Any]], float]]:
    variants: dict[str, Callable[[dict[str, Any]], float]] = {
        "abs_diff": lambda row: float(row["abs_diff"]),
        "pct_gap_avg_pp": lambda row: float(row["pct_gap_avg_pp"]),
        "pct_gap_fav_pp": lambda row: float(row["pct_gap_fav_pp"]),
        "ratio_gap_pp": lambda row: float(row["ratio_gap_pp"]),
        "log_ratio_points": lambda row: float(row["log_ratio_points"]),
    }
    for coef in (5, 10, 15, 20, 25, 30, 35, 40, 50):
        variants[f"blend_avg_k{coef}"] = (
            lambda row, coef=coef: float(row["abs_diff"] + float(coef) * row["pct_gap_avg_pp"])
        )
    for coef in (10, 20, 30, 40):
        variants[f"blend_fav_k{coef}"] = (
            lambda row, coef=coef: float(row["abs_diff"] + float(coef) * row["pct_gap_fav_pp"])
        )
    return variants


def _collect_series_rows(
    *,
    series_bundles: list[SeriesBundle],
    model: HybridPlayerRosterEloModel,
    evaluation_config: EvaluationConfig,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    eligible_bundles = [bundle for bundle in series_bundles if bundle.series.eligible_for_winner_target]
    evaluation_series_count = max(1, int(len(eligible_bundles) * evaluation_config.evaluation_fraction))
    evaluation_start_idx = max(evaluation_config.min_train_series, len(eligible_bundles) - evaluation_series_count)

    train_rows: list[dict[str, Any]] = []
    eval_rows: list[dict[str, Any]] = []
    eligible_seen = 0

    for bundle in series_bundles:
        series = bundle.series
        first_map = bundle.deciding_maps[0] if bundle.deciding_maps else None
        if series.eligible_for_winner_target and first_map is not None:
            step = model.predict_match(first_map)
            favorite_is_team_a = step.radiant_strength >= step.dire_strength
            favorite_strength = max(step.radiant_strength, step.dire_strength)
            underdog_strength = min(step.radiant_strength, step.dire_strength)
            abs_diff = favorite_strength - underdog_strength
            avg_strength = max(1.0, (favorite_strength + underdog_strength) / 2.0)
            pct_gap_avg_pp = 100.0 * abs_diff / avg_strength
            pct_gap_fav_pp = 100.0 * abs_diff / max(1.0, favorite_strength)
            ratio_gap_pp = 100.0 * (favorite_strength / max(1.0, underdog_strength) - 1.0)
            log_ratio_points = 400.0 * math.log10(max(1.0, favorite_strength) / max(1.0, underdog_strength))
            p_series_team_a = probability_to_win_series(step.p_radiant, series.best_of)
            favorite_series_prob = p_series_team_a if favorite_is_team_a else (1.0 - p_series_team_a)
            favorite_won = (series.team_a_won is True and favorite_is_team_a) or (
                series.team_a_won is False and not favorite_is_team_a
            )
            favorite_team_name = series.team_a_name if favorite_is_team_a else series.team_b_name
            underdog_team_name = series.team_b_name if favorite_is_team_a else series.team_a_name
            favorite_team_id = series.team_a_id if favorite_is_team_a else series.team_b_id
            underdog_team_id = series.team_b_id if favorite_is_team_a else series.team_a_id
            team_segment = _known_team_segment(
                series.team_a_id,
                series.team_a_name,
                series.team_b_id,
                series.team_b_name,
            )
            row = {
                "series_id": series.series_id,
                "timestamp": series.start_timestamp,
                "best_of": series.best_of,
                "league_id": series.league_id,
                "league_name": series.league_name,
                "league_tier": series.derived_league_tier.value,
                "league_segment": _league_segment(series.derived_league_tier),
                "team_segment": team_segment,
                "favorite_is_team_a": favorite_is_team_a,
                "favorite_team_id": favorite_team_id,
                "favorite_team_name": favorite_team_name,
                "underdog_team_id": underdog_team_id,
                "underdog_team_name": underdog_team_name,
                "favorite_strength": favorite_strength,
                "underdog_strength": underdog_strength,
                "abs_diff": abs_diff,
                "pct_gap_avg_pp": pct_gap_avg_pp,
                "pct_gap_fav_pp": pct_gap_fav_pp,
                "ratio_gap_pp": ratio_gap_pp,
                "log_ratio_points": log_ratio_points,
                "favorite_series_prob_direct": favorite_series_prob,
                "favorite_won": 1.0 if favorite_won else 0.0,
            }
            if eligible_seen >= evaluation_start_idx:
                eval_rows.append(row)
            else:
                train_rows.append(row)
            eligible_seen += 1

        for match in bundle.all_maps:
            model.process_match(match)

    return train_rows, eval_rows, {
        "eligible_series_count": len(eligible_bundles),
        "evaluation_start_idx": evaluation_start_idx,
        "train_rows": len(train_rows),
        "eval_rows": len(eval_rows),
    }


def _quantile_edges(values: list[float], bins: int) -> list[float]:
    if not values:
        return []
    sorted_values = sorted(float(value) for value in values)
    unique_edges: list[float] = []
    for idx in range(1, bins):
        pos = idx * (len(sorted_values) - 1) / bins
        lower = math.floor(pos)
        upper = math.ceil(pos)
        if lower == upper:
            edge = sorted_values[lower]
        else:
            weight = pos - lower
            edge = sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight
        if not unique_edges or edge > unique_edges[-1]:
            unique_edges.append(edge)
    return unique_edges


def _fit_grid(
    rows: list[dict[str, Any]],
    *,
    variant_name: str,
    score_fn: Callable[[dict[str, Any]], float],
    bins: int,
    prior_strength: float,
) -> dict[str, Any] | None:
    if not rows:
        return None

    scores = [float(score_fn(row)) for row in rows]
    edges = _quantile_edges(scores, bins)
    global_wr = sum(float(row["favorite_won"]) for row in rows) / len(rows)
    bin_counts = [0 for _ in range(len(edges) + 1)]
    bin_wins = [0.0 for _ in range(len(edges) + 1)]

    for row, score in zip(rows, scores):
        bucket_idx = bisect_right(edges, score)
        bin_counts[bucket_idx] += 1
        bin_wins[bucket_idx] += float(row["favorite_won"])

    bucket_rows = []
    smoothed_probs: list[float] = []
    for idx in range(len(edges) + 1):
        count = bin_counts[idx]
        wins = bin_wins[idx]
        smoothed_wr = (
            (wins + global_wr * prior_strength) / (count + prior_strength)
            if count or prior_strength > 0
            else global_wr
        )
        smoothed_probs.append(smoothed_wr)
        bucket_rows.append(
            {
                "bucket": idx,
                "start": edges[idx - 1] if idx > 0 else None,
                "end": edges[idx] if idx < len(edges) else None,
                "matches": count,
                "favorite_wins": wins,
                "empirical_favorite_wr": (wins / count) if count else None,
                "smoothed_favorite_wr": smoothed_wr,
            }
        )

    return {
        "variant": variant_name,
        "train_matches": len(rows),
        "train_avg_score": statistics.fmean(scores),
        "train_avg_favorite_wr": global_wr,
        "edges": edges,
        "buckets": bucket_rows,
        "smoothed_probs": smoothed_probs,
    }


def _summarize_prob_rows(rows: list[dict[str, float]]) -> dict[str, Any]:
    if not rows:
        return {
            "matches": 0,
            "favorite_accuracy": None,
            "favorite_log_loss": None,
            "favorite_brier": None,
            "avg_predicted_favorite_win": None,
            "avg_actual_favorite_win": None,
        }
    accuracy = sum(int((row["p_favorite"] >= 0.5) == bool(row["actual_favorite"])) for row in rows) / len(rows)
    log_loss = -sum(
        row["actual_favorite"] * math.log(_clip_probability(row["p_favorite"]))
        + (1.0 - row["actual_favorite"]) * math.log(_clip_probability(1.0 - row["p_favorite"]))
        for row in rows
    ) / len(rows)
    brier = sum((row["p_favorite"] - row["actual_favorite"]) ** 2 for row in rows) / len(rows)
    return {
        "matches": len(rows),
        "favorite_accuracy": accuracy,
        "favorite_log_loss": log_loss,
        "favorite_brier": brier,
        "avg_predicted_favorite_win": sum(row["p_favorite"] for row in rows) / len(rows),
        "avg_actual_favorite_win": sum(row["actual_favorite"] for row in rows) / len(rows),
    }


def _evaluate_grid(
    rows: list[dict[str, Any]],
    *,
    score_fn: Callable[[dict[str, Any]], float],
    grid: dict[str, Any] | None,
) -> dict[str, Any]:
    if not rows:
        return _summarize_prob_rows([])
    if grid is None:
        return {
            **_summarize_prob_rows([]),
            "matches": len(rows),
            "grid_missing": True,
        }
    edges = [float(value) for value in grid.get("edges", [])]
    smoothed_probs = [float(value) for value in grid.get("smoothed_probs", [])]
    prob_rows = []
    for row in rows:
        score = float(score_fn(row))
        bucket_idx = bisect_right(edges, score)
        bucket_idx = min(bucket_idx, len(smoothed_probs) - 1)
        prob_rows.append(
            {
                "p_favorite": smoothed_probs[bucket_idx],
                "actual_favorite": float(row["favorite_won"]),
            }
        )
    return _summarize_prob_rows(prob_rows)


def _segment_rows(rows: list[dict[str, Any]], segment_name: str) -> list[dict[str, Any]]:
    if segment_name == SEGMENT_OVERALL:
        return rows
    if segment_name in {SEGMENT_TIER1_ONLY, SEGMENT_TIER2_ONLY, SEGMENT_TIER1_VS_TIER2, SEGMENT_OTHER}:
        return [row for row in rows if row["team_segment"] == segment_name]
    return [row for row in rows if row["league_segment"] == segment_name]


def _direct_baseline(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return _summarize_prob_rows(
        [
            {
                "p_favorite": float(row["favorite_series_prob_direct"]),
                "actual_favorite": float(row["favorite_won"]),
            }
            for row in rows
        ]
    )


def _best_variant_name(results: dict[str, dict[str, Any]]) -> str | None:
    ranked = [
        (name, metrics)
        for name, metrics in results.items()
        if metrics.get("favorite_log_loss") is not None
    ]
    if not ranked:
        return None
    ranked.sort(
        key=lambda item: (
            float(item[1]["favorite_log_loss"]),
            float(item[1]["favorite_brier"]),
            -float(item[1]["favorite_accuracy"]),
        )
    )
    return ranked[0][0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate Elo diff -> favorite WR grids on series data.")
    parser.add_argument("--data-dir", type=Path, default=_default_data_dir())
    parser.add_argument("--output-dir", type=Path, default=_default_output_dir())
    parser.add_argument("--bins", type=int, default=DEFAULT_GRID_BINS)
    parser.add_argument("--prior-strength", type=float, default=DEFAULT_GRID_PRIOR_STRENGTH)
    args = parser.parse_args()

    matches, load_summary = load_matches(args.data_dir)
    if not matches:
        raise SystemExit("No valid matches were loaded.")

    league_info, league_summary = classify_leagues(matches)
    attach_league_tiers(matches, league_info)
    series_bundles, series_summary = build_series_bundles(matches)

    model = HybridPlayerRosterEloModel(HybridEloConfig())
    evaluation_config = EvaluationConfig()
    train_rows, eval_rows, split_summary = _collect_series_rows(
        series_bundles=series_bundles,
        model=model,
        evaluation_config=evaluation_config,
    )

    variant_defs = _variant_definitions()
    segments = [
        SEGMENT_OVERALL,
        SEGMENT_TIER1_ONLY,
        SEGMENT_TIER2_ONLY,
        SEGMENT_TIER1_VS_TIER2,
        SEGMENT_OTHER,
        LEAGUE_SEGMENT_TIER1,
        LEAGUE_SEGMENT_TIER2,
        LEAGUE_SEGMENT_TIER3,
    ]

    segment_report: dict[str, Any] = {}
    for segment_name in segments:
        segment_train = _segment_rows(train_rows, segment_name)
        segment_eval = _segment_rows(eval_rows, segment_name)
        grids = {
            variant_name: _fit_grid(
                segment_train,
                variant_name=variant_name,
                score_fn=score_fn,
                bins=args.bins,
                prior_strength=args.prior_strength,
            )
            for variant_name, score_fn in variant_defs.items()
        }
        variant_results = {
            variant_name: _evaluate_grid(segment_eval, score_fn=score_fn, grid=grids[variant_name])
            for variant_name, score_fn in variant_defs.items()
        }
        direct_metrics = _direct_baseline(segment_eval)
        best_variant = _best_variant_name(variant_results)
        segment_report[segment_name] = {
            "train_matches": len(segment_train),
            "eval_matches": len(segment_eval),
            "direct_series_prob_baseline": direct_metrics,
            "best_variant_by_log_loss": best_variant,
            "variant_results": variant_results,
            "best_variant_grid": grids.get(best_variant),
        }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "data_dir": str(args.data_dir),
        "output_dir": str(args.output_dir),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_summary": {
            **load_summary,
            "first_match_ts": matches[0].timestamp,
            "first_match_utc": _timestamp_to_iso(matches[0].timestamp),
            "last_match_ts": matches[-1].timestamp,
            "last_match_utc": _timestamp_to_iso(matches[-1].timestamp),
        },
        "league_summary": league_summary,
        "series_summary": series_summary,
        "split_summary": split_summary,
        "config": {
            "evaluation": _to_json_ready(evaluation_config),
            "hybrid": _to_json_ready(HybridEloConfig()),
            "grid_bins": args.bins,
            "grid_prior_strength": args.prior_strength,
            "variants": list(variant_defs.keys()),
        },
        "segments": segment_report,
    }

    report_path = args.output_dir / "elo_winrate_grid_report.json"
    with report_path.open("w", encoding="utf-8") as fh:
        json.dump(_to_json_ready(report), fh, ensure_ascii=False, indent=2)

    print(f"Loaded matches: {len(matches)}")
    print(f"Eligible series: {split_summary['eligible_series_count']}")
    print(f"Train/Eval: {split_summary['train_rows']} / {split_summary['eval_rows']}")
    print("")
    for segment_name in [SEGMENT_OVERALL, SEGMENT_TIER1_ONLY, SEGMENT_TIER2_ONLY, SEGMENT_TIER1_VS_TIER2]:
        segment = segment_report[segment_name]
        direct = segment["direct_series_prob_baseline"]
        best_name = segment["best_variant_by_log_loss"]
        best_metrics = segment["variant_results"].get(best_name) if best_name else None
        print(
            f"{segment_name:>14}  "
            f"eval={segment['eval_matches']:>4}  "
            f"direct_ll={direct['favorite_log_loss'] if direct['favorite_log_loss'] is not None else 'n/a'}  "
            f"best={best_name or 'n/a'}  "
            f"best_ll={best_metrics['favorite_log_loss'] if isinstance(best_metrics, dict) else 'n/a'}"
        )
    print("")
    print(f"Saved report to {report_path}")


if __name__ == "__main__":
    main()
