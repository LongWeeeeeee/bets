from __future__ import annotations

import math
from collections import Counter, defaultdict

from ELO.config import EvaluationConfig
from ELO.domain import LeagueTier, SeriesBundle
from ELO.evaluation import _summarize_rows


def _clip_probability(probability: float) -> float:
    return min(max(probability, 1e-12), 1.0 - 1e-12)


def probability_to_win_series(p_map: float, best_of: int) -> float:
    p_map = _clip_probability(p_map)
    if best_of == 1:
        return p_map
    required_wins = best_of // 2 + 1
    dp = [[0.0 for _ in range(required_wins + 1)] for _ in range(required_wins + 1)]
    for team_a_wins in range(required_wins, -1, -1):
        for team_b_wins in range(required_wins, -1, -1):
            if team_a_wins >= required_wins:
                dp[team_a_wins][team_b_wins] = 1.0
            elif team_b_wins >= required_wins:
                dp[team_a_wins][team_b_wins] = 0.0
            else:
                dp[team_a_wins][team_b_wins] = (
                    p_map * dp[team_a_wins + 1][team_b_wins]
                    + (1.0 - p_map) * dp[team_a_wins][team_b_wins + 1]
                )
    return dp[0][0]


def run_series_online_evaluation(model, series_bundles: list[SeriesBundle], config: EvaluationConfig) -> dict:
    eligible_bundles = [bundle for bundle in series_bundles if bundle.series.eligible_for_winner_target]
    evaluation_series_count = max(1, int(len(eligible_bundles) * config.evaluation_fraction))
    evaluation_start_idx = max(config.min_train_series, len(eligible_bundles) - evaluation_series_count)

    prediction_rows: list[dict] = []
    eligible_seen = 0
    best_of_counter: Counter[int] = Counter()
    applied_bo3_sweep_bonus_count = 0

    for bundle in series_bundles:
        series = bundle.series
        first_map = bundle.deciding_maps[0] if bundle.deciding_maps else None
        pre_map_prob: float | None = None
        pre_series_prob: float | None = None
        if series.eligible_for_winner_target:
            if first_map is None:
                continue
            map_step = model.predict_match(first_map)
            pre_map_prob = map_step.p_radiant
            pre_series_prob = probability_to_win_series(pre_map_prob, series.best_of)
            best_of_counter[series.best_of] += 1
            if eligible_seen >= evaluation_start_idx:
                prediction_rows.append(
                    {
                        "series_id": series.series_id,
                        "timestamp": series.start_timestamp,
                        "league_id": series.league_id,
                        "league_tier": series.derived_league_tier.value,
                        "series_type": series.series_type,
                        "best_of": series.best_of,
                        "team_a_name": series.team_a_name,
                        "team_b_name": series.team_b_name,
                        "p_radiant": pre_series_prob,
                        "actual": 1.0 if series.team_a_won else 0.0,
                        "metadata": {
                            "p_first_map_team_a": pre_map_prob,
                            "series_best_of": series.best_of,
                            "team_a_map_wins": series.team_a_map_wins,
                            "team_b_map_wins": series.team_b_map_wins,
                        },
                    }
                )
            eligible_seen += 1

        for match in bundle.all_maps:
            model.process_match(match)

        apply_bo3_sweep_bonus = getattr(model, "apply_bo3_sweep_bonus", None)
        if (
            callable(apply_bo3_sweep_bonus)
            and series.eligible_for_winner_target
            and first_map is not None
            and pre_map_prob is not None
            and pre_series_prob is not None
            and series.best_of == 3
            and (
                (series.team_a_map_wins == 2 and series.team_b_map_wins == 0)
                or (series.team_a_map_wins == 0 and series.team_b_map_wins == 2)
            )
        ):
            bonus_applied = apply_bo3_sweep_bonus(
                first_map=first_map,
                actual=1.0 if series.team_a_won else 0.0,
                pre_map_prob=pre_map_prob,
                pre_series_prob=pre_series_prob,
            )
            if bonus_applied:
                applied_bo3_sweep_bonus_count += 1

    summary = _summarize_rows(prediction_rows, calibration_buckets=config.calibration_buckets)
    summary["evaluation_start_idx"] = evaluation_start_idx
    summary["warmup_series"] = evaluation_start_idx
    summary["sample_predictions"] = prediction_rows[:20]
    summary["evaluated_best_of_counts"] = dict(best_of_counter)
    summary["applied_bo3_sweep_bonus_count"] = applied_bo3_sweep_bonus_count

    by_tier = {}
    for tier in LeagueTier:
        tier_rows = [row for row in prediction_rows if row["league_tier"] == tier.value]
        by_tier[tier.value] = _summarize_rows(tier_rows, calibration_buckets=config.calibration_buckets)
    summary["by_tier"] = by_tier

    by_best_of = {}
    for best_of in sorted({row["best_of"] for row in prediction_rows}):
        rows = [row for row in prediction_rows if row["best_of"] == best_of]
        by_best_of[str(best_of)] = _summarize_rows(rows, calibration_buckets=config.calibration_buckets)
    summary["by_best_of"] = by_best_of
    return summary
