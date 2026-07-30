from __future__ import annotations

import math
from collections import defaultdict

from ELO.config import EvaluationConfig
from ELO.domain import LeagueTier, MatchRecord


def _clip_probability(probability: float) -> float:
    return min(max(probability, 1e-12), 1.0 - 1e-12)


def _summarize_rows(rows: list[dict], calibration_buckets: int) -> dict:
    if not rows:
        return {
            "matches": 0,
            "accuracy": None,
            "log_loss": None,
            "brier": None,
            "avg_predicted_radiant_win": None,
            "avg_actual_radiant_win": None,
            "calibration": [],
        }

    accuracy = sum(int((row["p_radiant"] >= 0.5) == bool(row["actual"])) for row in rows) / len(rows)
    log_loss = -sum(
        row["actual"] * math.log(_clip_probability(row["p_radiant"]))
        + (1.0 - row["actual"]) * math.log(_clip_probability(1.0 - row["p_radiant"]))
        for row in rows
    ) / len(rows)
    brier = sum((row["p_radiant"] - row["actual"]) ** 2 for row in rows) / len(rows)
    avg_predicted = sum(row["p_radiant"] for row in rows) / len(rows)
    avg_actual = sum(row["actual"] for row in rows) / len(rows)

    buckets: dict[int, list[dict]] = defaultdict(list)
    for row in rows:
        bucket_idx = min(calibration_buckets - 1, int(row["p_radiant"] * calibration_buckets))
        buckets[bucket_idx].append(row)

    calibration = []
    for bucket_idx in range(calibration_buckets):
        bucket_rows = buckets.get(bucket_idx, [])
        if not bucket_rows:
            continue
        calibration.append(
            {
                "bucket": bucket_idx,
                "range_start": bucket_idx / calibration_buckets,
                "range_end": (bucket_idx + 1) / calibration_buckets,
                "matches": len(bucket_rows),
                "avg_predicted": sum(row["p_radiant"] for row in bucket_rows) / len(bucket_rows),
                "avg_actual": sum(row["actual"] for row in bucket_rows) / len(bucket_rows),
            }
        )

    return {
        "matches": len(rows),
        "accuracy": accuracy,
        "log_loss": log_loss,
        "brier": brier,
        "avg_predicted_radiant_win": avg_predicted,
        "avg_actual_radiant_win": avg_actual,
        "calibration": calibration,
    }


def run_online_evaluation(model, matches: list[MatchRecord], config: EvaluationConfig) -> dict:
    evaluation_match_count = max(1, int(len(matches) * config.evaluation_fraction))
    evaluation_start_idx = max(config.min_train_matches, len(matches) - evaluation_match_count)

    prediction_rows: list[dict] = []
    for idx, match in enumerate(matches):
        step = model.process_match(match)
        if idx < evaluation_start_idx:
            continue
        prediction_rows.append(
            {
                "match_id": match.match_id,
                "timestamp": match.timestamp,
                "league_id": match.league_id,
                "league_tier": match.derived_league_tier.value,
                "radiant_team_name": match.radiant_team_name,
                "dire_team_name": match.dire_team_name,
                "p_radiant": step.p_radiant,
                "actual": 1.0 if match.radiant_win else 0.0,
                "metadata": step.metadata,
            }
        )

    summary = _summarize_rows(prediction_rows, calibration_buckets=config.calibration_buckets)
    summary["evaluation_start_idx"] = evaluation_start_idx
    summary["warmup_matches"] = evaluation_start_idx
    summary["sample_predictions"] = prediction_rows[:20]

    by_tier = {}
    for tier in LeagueTier:
        tier_rows = [row for row in prediction_rows if row["league_tier"] == tier.value]
        by_tier[tier.value] = _summarize_rows(tier_rows, calibration_buckets=config.calibration_buckets)
    summary["by_tier"] = by_tier
    return summary
