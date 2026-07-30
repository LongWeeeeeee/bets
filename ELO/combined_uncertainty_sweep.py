from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass, is_dataclass, replace
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ELO.config import EvaluationConfig, HybridEloConfig
from ELO.data_loader import load_matches
from ELO.domain import LeagueTier, SeriesBundle
from ELO.evaluation import _summarize_rows
from ELO.models import HybridPlayerRosterEloModel, _team_key
from ELO.series_data import build_series_bundles
from ELO.series_evaluation import probability_to_win_series
from ELO.tiering import attach_league_tiers, classify_leagues


def _to_json_ready(value: Any) -> Any:
    if isinstance(value, LeagueTier):
        return value.value
    if is_dataclass(value):
        return _to_json_ready(asdict(value))
    if isinstance(value, dict):
        return {str(key.value if isinstance(key, LeagueTier) else key): _to_json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_ready(item) for item in value]
    return value


def _default_data_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "pro_heroes_data" / "json_parts_split_from_object"


def _default_output_path() -> Path:
    return Path(__file__).resolve().parent / "output" / "combined_uncertainty_sweep.json"


@dataclass(frozen=True)
class TrialCfg:
    name: str
    lineup_boost_max: float
    lineup_boost_matches: int
    lineup_tier1_enabled: bool
    player_boost_max: float
    player_boost_matches: int
    player_use_global: bool
    player_use_local: bool
    player_tier1_enabled: bool


class CombinedUncertaintyModel(HybridPlayerRosterEloModel):
    def __init__(
        self,
        config: HybridEloConfig,
        *,
        lineup_tier1_enabled: bool,
        player_boost_max: float,
        player_boost_matches: int,
        player_use_global: bool,
        player_use_local: bool,
        player_tier1_enabled: bool,
    ) -> None:
        super().__init__(config)
        self._lineup_tier1_enabled = bool(lineup_tier1_enabled)
        self._player_boost_max = float(player_boost_max)
        self._player_boost_matches = int(player_boost_matches)
        self._player_use_global = bool(player_use_global)
        self._player_use_local = bool(player_use_local)
        self._player_tier1_enabled = bool(player_tier1_enabled)
        self._player_current_org: dict[int, str] = {}
        self._player_current_org_matches: defaultdict[tuple[int, str], int] = defaultdict(int)

    def _build_team_context(self, *args, **kwargs):
        tier = kwargs.get("tier")
        if tier is None and len(args) >= 4:
            tier = args[3]
        context = super()._build_team_context(*args, **kwargs)
        if tier == LeagueTier.TIER1 and not self._lineup_tier1_enabled:
            return replace(context, lineup_k_multiplier=1.0)
        return context

    def _player_multiplier(self, player_id: int, org_key: str, tier: LeagueTier) -> float:
        if self._player_boost_max <= 0.0 or self._player_boost_matches <= 0:
            return 1.0
        if tier == LeagueTier.TIER1 and not self._player_tier1_enabled:
            return 1.0
        current_org = self._player_current_org.get(player_id)
        if current_org != org_key:
            stint_matches = 0
        else:
            stint_matches = int(self._player_current_org_matches[(player_id, org_key)])
        freshness = max(0.0, 1.0 - (float(stint_matches) / float(max(1, self._player_boost_matches))))
        return 1.0 + self._player_boost_max * freshness

    def _commit_player_org(self, player_id: int, org_key: str) -> None:
        if self._player_current_org.get(player_id) != org_key:
            self._player_current_org[player_id] = org_key
            self._player_current_org_matches[(player_id, org_key)] = 1
            return
        self._player_current_org_matches[(player_id, org_key)] += 1

    def process_match(self, match):
        step, radiant_context, dire_context, tier, side_bias = self._preview_match(match, mutate=True)
        actual = 1.0 if match.radiant_win else 0.0
        error = actual - step.p_radiant

        k_global = self.config.k_global_by_tier[tier]
        k_local = self.config.k_local_by_tier[tier]
        k_roster = self.config.k_roster_by_tier[tier]
        rad_lineup_mult = radiant_context.lineup_k_multiplier
        dire_lineup_mult = dire_context.lineup_k_multiplier
        rad_org = _team_key(match.radiant_team_id, match.radiant_team_name)
        dire_org = _team_key(match.dire_team_id, match.dire_team_name)

        for player_id in match.radiant_player_ids:
            player_mult = self._player_multiplier(player_id, rad_org, tier)
            global_mult = rad_lineup_mult * (player_mult if self._player_use_global else 1.0)
            local_mult = rad_lineup_mult * (player_mult if self._player_use_local else 1.0)
            self.player_global[player_id] += k_global * global_mult * error
            self.player_local[tier][player_id] += k_local * local_mult * error
            self._commit_player_org(player_id, rad_org)
        for player_id in match.dire_player_ids:
            player_mult = self._player_multiplier(player_id, dire_org, tier)
            global_mult = dire_lineup_mult * (player_mult if self._player_use_global else 1.0)
            local_mult = dire_lineup_mult * (player_mult if self._player_use_local else 1.0)
            self.player_global[player_id] -= k_global * global_mult * error
            self.player_local[tier][player_id] -= k_local * local_mult * error
            self._commit_player_org(player_id, dire_org)

        self.roster_ratings[tier][radiant_context.roster_key] = radiant_context.roster_rating + k_roster * rad_lineup_mult * error
        self.roster_ratings[tier][dire_context.roster_key] = dire_context.roster_rating - k_roster * dire_lineup_mult * error
        self.roster_match_counts[tier][radiant_context.roster_key] = radiant_context.roster_matches + 1
        self.roster_match_counts[tier][dire_context.roster_key] = dire_context.roster_matches + 1
        self.lineup_match_counts[radiant_context.lineup_key] = radiant_context.lineup_matches + 1
        self.lineup_match_counts[dire_context.lineup_key] = dire_context.lineup_matches + 1
        self.side_bias[tier] = side_bias + self.config.side_bias_k * error
        return step


def _collect_series_rows(model: Any, series_bundles: list[SeriesBundle], config: EvaluationConfig) -> list[dict[str, Any]]:
    eligible_bundles = [bundle for bundle in series_bundles if bundle.series.eligible_for_winner_target]
    evaluation_series_count = max(1, int(len(eligible_bundles) * config.evaluation_fraction))
    evaluation_start_idx = max(config.min_train_series, len(eligible_bundles) - evaluation_series_count)

    rows: list[dict[str, Any]] = []
    eligible_seen = 0
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
            if eligible_seen >= evaluation_start_idx:
                rows.append(
                    {
                        "series_id": series.series_id,
                        "league_tier": series.derived_league_tier.value,
                        "best_of": series.best_of,
                        "p_radiant": pre_series_prob,
                        "actual": 1.0 if series.team_a_won else 0.0,
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
            apply_bo3_sweep_bonus(
                first_map=first_map,
                actual=1.0 if series.team_a_won else 0.0,
                pre_map_prob=pre_map_prob,
                pre_series_prob=pre_series_prob,
            )
    return rows


def _row_summary(rows: list[dict[str, Any]], config: EvaluationConfig) -> dict[str, Any]:
    summary = _summarize_rows(rows, calibration_buckets=config.calibration_buckets)
    by_tier = {}
    for tier in LeagueTier:
        tier_rows = [row for row in rows if row["league_tier"] == tier.value]
        by_tier[tier.value] = _summarize_rows(tier_rows, calibration_buckets=config.calibration_buckets)
    summary["by_tier"] = by_tier
    return summary


def _compare_predictions(baseline_rows: list[dict[str, Any]], trial_rows: list[dict[str, Any]]) -> dict[str, Any]:
    baseline_by_id = {int(row["series_id"]): row for row in baseline_rows}
    trial_by_id = {int(row["series_id"]): row for row in trial_rows}
    shared_ids = sorted(set(baseline_by_id) & set(trial_by_id))
    if not shared_ids:
        return {"shared_series": 0, "prediction_flips": 0}

    flips = 0
    same_side_better_logloss = 0
    same_side_worse_logloss = 0
    avg_abs_prob_shift = 0.0
    max_abs_prob_shift = 0.0
    max_abs_prob_shift_series_id = None
    for series_id in shared_ids:
        base = baseline_by_id[series_id]
        trial = trial_by_id[series_id]
        base_label = base["p_radiant"] >= 0.5
        trial_label = trial["p_radiant"] >= 0.5
        if base_label != trial_label:
            flips += 1
        else:
            actual = float(base["actual"])
            base_ll = -(
                actual * math.log(min(max(base["p_radiant"], 1e-12), 1.0 - 1e-12))
                + (1.0 - actual) * math.log(min(max(1.0 - base["p_radiant"], 1e-12), 1.0 - 1e-12))
            )
            trial_ll = -(
                actual * math.log(min(max(trial["p_radiant"], 1e-12), 1.0 - 1e-12))
                + (1.0 - actual) * math.log(min(max(1.0 - trial["p_radiant"], 1e-12), 1.0 - 1e-12))
            )
            if trial_ll < base_ll:
                same_side_better_logloss += 1
            elif trial_ll > base_ll:
                same_side_worse_logloss += 1
        shift = abs(float(trial["p_radiant"]) - float(base["p_radiant"]))
        avg_abs_prob_shift += shift
        if shift > max_abs_prob_shift:
            max_abs_prob_shift = shift
            max_abs_prob_shift_series_id = series_id

    return {
        "shared_series": len(shared_ids),
        "prediction_flips": flips,
        "same_side_better_logloss": same_side_better_logloss,
        "same_side_worse_logloss": same_side_worse_logloss,
        "avg_abs_prob_shift": avg_abs_prob_shift / len(shared_ids),
        "max_abs_prob_shift": max_abs_prob_shift,
        "max_abs_prob_shift_series_id": max_abs_prob_shift_series_id,
    }


def _trial_configs() -> list[TrialCfg]:
    return [
        TrialCfg("baseline", 0.0, 0, True, 0.0, 0, False, False, True),
        TrialCfg("lineup_all_1.0_decay4", 1.0, 4, True, 0.0, 0, False, False, True),
        TrialCfg("lineup_all_1.0_decay4_no_t1", 1.0, 4, False, 0.0, 0, False, False, False),
        TrialCfg("combined_lineup_all_player_local_1.0_decay10", 1.0, 4, True, 1.0, 10, False, True, True),
        TrialCfg("combined_lineup_all_player_local_1.0_decay12", 1.0, 4, True, 1.0, 12, False, True, True),
        TrialCfg("combined_lineup_all_player_local_1.0_decay15", 1.0, 4, True, 1.0, 15, False, True, True),
        TrialCfg("combined_lineup_all_player_all_1.0_decay10", 1.0, 4, True, 1.0, 10, True, True, True),
        TrialCfg("combined_lineup_all_player_all_1.0_decay12", 1.0, 4, True, 1.0, 12, True, True, True),
        TrialCfg("combined_lineup_all_player_all_1.0_decay15", 1.0, 4, True, 1.0, 15, True, True, True),
        TrialCfg("combined_lineup_not1_player_local_1.0_decay15", 1.0, 4, False, 1.0, 15, False, True, False),
        TrialCfg("combined_lineup_not1_player_all_1.0_decay10", 1.0, 4, False, 1.0, 10, True, True, False),
        TrialCfg("combined_lineup_not1_player_all_1.0_decay15", 1.0, 4, False, 1.0, 15, True, True, False),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Series-level sweep for combined lineup + player volatility.")
    parser.add_argument("--data-dir", type=Path, default=_default_data_dir())
    parser.add_argument("--output-path", type=Path, default=_default_output_path())
    args = parser.parse_args()

    matches, load_summary = load_matches(args.data_dir)
    league_info, league_summary = classify_leagues(matches)
    attach_league_tiers(matches, league_info)
    series_bundles, series_summary = build_series_bundles(matches)
    eval_config = EvaluationConfig()

    results: list[dict[str, Any]] = []
    baseline_rows: list[dict[str, Any]] | None = None
    for trial in _trial_configs():
        cfg = HybridEloConfig(
            lineup_uncertainty_boost_max=trial.lineup_boost_max,
            lineup_uncertainty_boost_matches=trial.lineup_boost_matches,
            lineup_uncertainty_boost_global=trial.lineup_boost_max > 0.0,
            lineup_uncertainty_boost_local=trial.lineup_boost_max > 0.0,
            lineup_uncertainty_boost_roster=trial.lineup_boost_max > 0.0,
        )
        model = CombinedUncertaintyModel(
            config=cfg,
            lineup_tier1_enabled=trial.lineup_tier1_enabled,
            player_boost_max=trial.player_boost_max,
            player_boost_matches=trial.player_boost_matches,
            player_use_global=trial.player_use_global,
            player_use_local=trial.player_use_local,
            player_tier1_enabled=trial.player_tier1_enabled,
        )
        rows = _collect_series_rows(model, series_bundles, eval_config)
        summary = _row_summary(rows, eval_config)
        if trial.name == "baseline":
            baseline_rows = rows
        compare = _compare_predictions(baseline_rows, rows) if baseline_rows is not None else None
        results.append(
            {
                "trial": trial.name,
                "config": _to_json_ready(trial),
                "accuracy": summary["accuracy"],
                "log_loss": summary["log_loss"],
                "brier": summary["brier"],
                "by_tier": summary["by_tier"],
                "compare_vs_baseline": compare,
            }
        )
        print(
            f"{trial.name}: accuracy={summary['accuracy']:.6f} "
            f"log_loss={summary['log_loss']:.6f} brier={summary['brier']:.6f}"
        )

    baseline_result = next(item for item in results if item["trial"] == "baseline")
    for item in results:
        item["delta_vs_baseline"] = {
            "accuracy": float(item["accuracy"] or 0.0) - float(baseline_result["accuracy"] or 0.0),
            "log_loss": float(item["log_loss"] or 0.0) - float(baseline_result["log_loss"] or 0.0),
            "brier": float(item["brier"] or 0.0) - float(baseline_result["brier"] or 0.0),
        }

    results_by_log_loss = sorted(results, key=lambda item: (item["log_loss"], -float(item["accuracy"] or 0.0)))
    results_by_accuracy = sorted(results, key=lambda item: (-float(item["accuracy"] or 0.0), item["log_loss"]))
    payload = {
        "data_dir": str(args.data_dir),
        "dataset_summary": load_summary,
        "league_summary": league_summary,
        "series_summary": series_summary,
        "evaluation": _to_json_ready(eval_config),
        "baseline": baseline_result,
        "best_by_log_loss": results_by_log_loss[0],
        "best_by_accuracy": results_by_accuracy[0],
        "results_by_log_loss": results_by_log_loss,
    }
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    with args.output_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    print(f"Saved sweep to {args.output_path}")


if __name__ == "__main__":
    main()
