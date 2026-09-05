from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import replace

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
    ordered_bundles = sorted(
        series_bundles,
        key=lambda bundle: (bundle.series.start_timestamp, bundle.series.series_id),
    )
    eligible_bundles = [bundle for bundle in ordered_bundles if bundle.series.eligible_for_winner_target]
    evaluation_series_count = max(1, int(len(eligible_bundles) * config.evaluation_fraction))
    evaluation_start_idx = max(config.min_train_series, len(eligible_bundles) - evaluation_series_count)

    prediction_rows: list[dict] = []
    eligible_seen = 0
    best_of_counter: Counter[int] = Counter()
    applied_bo3_sweep_bonus_count = 0

    states: list[dict] = []
    maps: list = []
    final_state_by_map_object: dict[int, dict] = {}
    for bundle in ordered_bundles:
        series = bundle.series
        first_map = bundle.deciding_maps[0] if bundle.deciding_maps else None
        state = {
            "bundle": bundle,
            "first_map": first_map,
            "pre_map_prob": None,
            "pre_series_prob": None,
        }
        states.append(state)
        maps.extend(bundle.all_maps)
        if bundle.all_maps:
            final_state_by_map_object[id(bundle.all_maps[-1])] = state

    states.sort(key=lambda state: (
        state["bundle"].series.start_timestamp,
        state["bundle"].series.series_id,
    ))
    maps.sort(key=lambda match: (match.timestamp, match.match_id))

    state_index = 0
    map_index = 0
    maybe_apply_patch_reset = getattr(model, "_maybe_apply_patch_local_reset", None)
    while state_index < len(states) or map_index < len(maps):
        next_state_ts = (
            states[state_index]["bundle"].series.start_timestamp
            if state_index < len(states) else None
        )
        next_map_ts = maps[map_index].timestamp if map_index < len(maps) else None
        if next_state_ts is None:
            timestamp = next_map_ts
        elif next_map_ts is None:
            timestamp = next_state_ts
        else:
            timestamp = min(next_state_ts, next_map_ts)
        if callable(maybe_apply_patch_reset):
            maybe_apply_patch_reset(timestamp)

        # Every prediction made at t sees only results strictly before t. This
        # also keeps simultaneous series starts independent of map-id ordering.
        while (
            state_index < len(states)
            and states[state_index]["bundle"].series.start_timestamp == timestamp
        ):
            state = states[state_index]
            bundle = state["bundle"]
            series = bundle.series
            first_map = state["first_map"]
            if series.eligible_for_winner_target and first_map is not None:
                map_step = model.predict_match(first_map)
                pre_map_prob = map_step.p_radiant
                pre_series_prob = probability_to_win_series(pre_map_prob, series.best_of)
                state["pre_map_prob"] = pre_map_prob
                state["pre_series_prob"] = pre_series_prob
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
            state_index += 1

        while map_index < len(maps) and maps[map_index].timestamp == timestamp:
            match = maps[map_index]
            model.process_match(match)
            state = final_state_by_map_object.get(id(match))
            if state is not None:
                bundle = state["bundle"]
                series = bundle.series
                first_map = state["first_map"]
                pre_map_prob = state["pre_map_prob"]
                pre_series_prob = state["pre_series_prob"]
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
                        # Keep the first-map lineup, but evaluate it when the
                        # sweep outcome became available to avoid a stale-time
                        # context if model decay is enabled.
                        first_map=replace(first_map, timestamp=match.timestamp),
                        actual=1.0 if series.team_a_won else 0.0,
                        pre_map_prob=pre_map_prob,
                        pre_series_prob=pre_series_prob,
                    )
                    if bonus_applied:
                        applied_bo3_sweep_bonus_count += 1
            map_index += 1

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
