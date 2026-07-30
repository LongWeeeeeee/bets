from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ELO.config import EvaluationConfig, HybridEloConfig
from ELO.data_loader import load_matches
from ELO.domain import LeagueTier
from ELO.models import HybridPlayerRosterEloModel
from ELO.series_data import build_series_bundles
from ELO.series_evaluation import run_series_online_evaluation
from ELO.tiering import attach_league_tiers, classify_leagues

SECONDS_PER_DAY = 24 * 60 * 60


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
    return Path(__file__).resolve().parent / "output" / "inactivity_penalty_sweep.json"


@dataclass(frozen=True)
class TrialCfg:
    name: str
    gap_days: int
    keep: float
    local: bool
    roster: bool
    global_rating: bool = False
    tier1_only: bool = True


class InactivityPenaltyModel(HybridPlayerRosterEloModel):
    def __init__(
        self,
        config: HybridEloConfig,
        *,
        gap_days: int,
        keep: float,
        apply_local: bool,
        apply_roster: bool,
        apply_global: bool,
        tier1_only: bool,
    ) -> None:
        super().__init__(config)
        self.gap_seconds = max(0, int(gap_days) * SECONDS_PER_DAY)
        self.keep = max(0.0, min(float(keep), 1.0))
        self.apply_local = bool(apply_local)
        self.apply_roster = bool(apply_roster)
        self.apply_global = bool(apply_global)
        self.tier1_only = bool(tier1_only)

    def _enabled_for_tier(self, tier: LeagueTier) -> bool:
        return not self.tier1_only or tier == LeagueTier.TIER1

    def _maybe_shrink(self, rating: float, target: float, last_seen_ts: int | None, timestamp: int) -> float:
        if last_seen_ts is None or self.gap_seconds <= 0:
            return rating
        if timestamp - last_seen_ts < self.gap_seconds:
            return rating
        return target + (rating - target) * self.keep

    def _get_player_global_rating(self, player_id: int, timestamp: int, *, mutate: bool) -> float:
        rating = super()._get_player_global_rating(player_id, timestamp, mutate=False)
        if self.apply_global:
            rating = self._maybe_shrink(
                rating,
                self.config.initial_rating,
                self.player_global_last_seen_ts.get(player_id),
                timestamp,
            )
        if mutate:
            self.player_global[player_id] = rating
            self.player_global_last_seen_ts[player_id] = timestamp
        return rating

    def _get_player_local_rating(
        self,
        player_id: int,
        tier: LeagueTier,
        timestamp: int,
        *,
        mutate: bool,
    ) -> float:
        rating = super()._get_player_local_rating(player_id, tier, timestamp, mutate=False)
        if self.apply_local and self._enabled_for_tier(tier):
            rating = self._maybe_shrink(
                rating,
                self.config.initial_rating,
                self.player_local_last_seen_ts[tier].get(player_id),
                timestamp,
            )
        if mutate:
            self.player_local[tier][player_id] = rating
            self.player_local_last_seen_ts[tier][player_id] = timestamp
        return rating

    def _get_roster_rating(
        self,
        roster_key: str,
        tier: LeagueTier,
        timestamp: int,
        target_strength: float,
        *,
        mutate: bool,
    ) -> float:
        rating = super()._get_roster_rating(
            roster_key,
            tier,
            timestamp,
            target_strength,
            mutate=False,
        )
        if self.apply_roster and self._enabled_for_tier(tier):
            rating = self._maybe_shrink(
                rating,
                target_strength,
                self.roster_last_seen_ts[tier].get(roster_key),
                timestamp,
            )
        if mutate:
            self.roster_ratings[tier][roster_key] = rating
            self.roster_last_seen_ts[tier][roster_key] = timestamp
        return rating


def _summarize_trial(trial: TrialCfg, report: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    return {
        "trial": trial.name,
        "trial_cfg": _to_json_ready(trial),
        "accuracy": float(report["accuracy"]),
        "log_loss": float(report["log_loss"]),
        "brier": float(report["brier"]),
        "by_tier": _to_json_ready(report.get("by_tier", {})),
        "delta_vs_baseline": {
            "accuracy": float(report["accuracy"]) - float(baseline["accuracy"]),
            "log_loss": float(report["log_loss"]) - float(baseline["log_loss"]),
            "brier": float(report["brier"]) - float(baseline["brier"]),
            "tier1_accuracy": float(report["by_tier"]["TIER1"]["accuracy"])
            - float(baseline["by_tier"]["TIER1"]["accuracy"]),
            "tier1_log_loss": float(report["by_tier"]["TIER1"]["log_loss"])
            - float(baseline["by_tier"]["TIER1"]["log_loss"]),
            "tier1_brier": float(report["by_tier"]["TIER1"]["brier"])
            - float(baseline["by_tier"]["TIER1"]["brier"]),
        },
    }


def main() -> None:
    data_dir = _default_data_dir()
    output_path = _default_output_path()

    matches, load_summary = load_matches(data_dir)
    if not matches:
        raise SystemExit("No valid matches were loaded.")

    league_info, league_summary = classify_leagues(matches)
    attach_league_tiers(matches, league_info)
    series_bundles, series_summary = build_series_bundles(matches)

    eval_cfg = EvaluationConfig()
    base_cfg = HybridEloConfig()
    baseline_report = run_series_online_evaluation(
        model=HybridPlayerRosterEloModel(config=base_cfg),
        series_bundles=series_bundles,
        config=eval_cfg,
    )

    trials = [
        TrialCfg("baseline", 0, 1.0, False, False, False, True),
        TrialCfg("gap45_keep0.50_local_roster_t1", 45, 0.50, True, True, False, True),
        TrialCfg("gap60_keep0.50_local_roster_t1", 60, 0.50, True, True, False, True),
        TrialCfg("gap90_keep0.50_local_roster_t1", 90, 0.50, True, True, False, True),
        TrialCfg("gap60_keep0.75_local_roster_t1", 60, 0.75, True, True, False, True),
        TrialCfg("gap90_keep0.75_local_roster_t1", 90, 0.75, True, True, False, True),
        TrialCfg("gap60_keep0.50_local_only_t1", 60, 0.50, True, False, False, True),
        TrialCfg("gap90_keep0.50_local_only_t1", 90, 0.50, True, False, False, True),
        TrialCfg("gap60_keep0.50_local_roster_all", 60, 0.50, True, True, False, False),
    ]

    results: list[dict[str, Any]] = []
    for trial in trials:
        if trial.name == "baseline":
            report = baseline_report
        else:
            report = run_series_online_evaluation(
                model=InactivityPenaltyModel(
                    config=base_cfg,
                    gap_days=trial.gap_days,
                    keep=trial.keep,
                    apply_local=trial.local,
                    apply_roster=trial.roster,
                    apply_global=trial.global_rating,
                    tier1_only=trial.tier1_only,
                ),
                series_bundles=series_bundles,
                config=eval_cfg,
            )
        results.append(_summarize_trial(trial, report, baseline_report))

    best_by_tier1_accuracy = max(
        results,
        key=lambda row: (
            row["by_tier"]["TIER1"]["accuracy"],
            -row["by_tier"]["TIER1"]["log_loss"],
        ),
    )
    best_by_tier1_log_loss = min(
        results,
        key=lambda row: (
            row["by_tier"]["TIER1"]["log_loss"],
            -row["by_tier"]["TIER1"]["accuracy"],
        ),
    )
    best_by_overall_log_loss = min(results, key=lambda row: (row["log_loss"], row["brier"], -row["accuracy"]))

    output = {
        "data_dir": str(data_dir),
        "dataset_summary": load_summary,
        "league_summary": league_summary,
        "series_summary": series_summary,
        "evaluation": _to_json_ready(eval_cfg),
        "baseline": results[0],
        "best_by_tier1_accuracy": best_by_tier1_accuracy,
        "best_by_tier1_log_loss": best_by_tier1_log_loss,
        "best_by_overall_log_loss": best_by_overall_log_loss,
        "results": results,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(_to_json_ready(output), fh, ensure_ascii=False, indent=2)

    print(f"Saved inactivity-penalty sweep to {output_path}")
    print(
        "Best TIER1 accuracy: "
        f"{best_by_tier1_accuracy['trial']} "
        f"acc={best_by_tier1_accuracy['by_tier']['TIER1']['accuracy']:.4f} "
        f"ll={best_by_tier1_accuracy['by_tier']['TIER1']['log_loss']:.4f}"
    )
    print(
        "Best TIER1 log_loss: "
        f"{best_by_tier1_log_loss['trial']} "
        f"acc={best_by_tier1_log_loss['by_tier']['TIER1']['accuracy']:.4f} "
        f"ll={best_by_tier1_log_loss['by_tier']['TIER1']['log_loss']:.4f}"
    )
    print(
        "Best overall log_loss: "
        f"{best_by_overall_log_loss['trial']} "
        f"acc={best_by_overall_log_loss['accuracy']:.4f} "
        f"ll={best_by_overall_log_loss['log_loss']:.4f}"
    )


if __name__ == "__main__":
    main()
