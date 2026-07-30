from __future__ import annotations

import json
import re
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
from base.sort_pub_matches_by_patch import PATCH_RELEASES, OLDER_BUCKET


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
    return Path(__file__).resolve().parent / "output" / "patch_local_sweep.json"


def _patch_label_for_ts(start_ts: int) -> str:
    for patch in PATCH_RELEASES:
        if start_ts >= patch.release_ts:
            return patch.version
    return OLDER_BUCKET


def _major_patch_label(version: str) -> str:
    match = re.match(r"^(\d+\.\d+)", str(version))
    return match.group(1) if match else str(version)


@dataclass(frozen=True)
class TrialCfg:
    name: str
    patch_mode: str
    local_keep: float
    roster_keep: float
    reset_tier1_only: bool = False


class PatchAwareLocalModel(HybridPlayerRosterEloModel):
    def __init__(
        self,
        config: HybridEloConfig,
        *,
        patch_mode: str,
        local_keep: float,
        roster_keep: float,
        reset_tier1_only: bool,
    ) -> None:
        super().__init__(config)
        self._patch_mode = str(patch_mode)
        self._local_keep = float(local_keep)
        self._roster_keep = float(roster_keep)
        self._reset_tier1_only = bool(reset_tier1_only)
        self._current_patch_key: str | None = None

    def _patch_key(self, timestamp: int) -> str:
        label = _patch_label_for_ts(timestamp)
        if self._patch_mode == "major":
            return _major_patch_label(label)
        return label

    def _apply_patch_transition(self) -> None:
        local_keep = min(max(self._local_keep, 0.0), 1.0)
        roster_keep = min(max(self._roster_keep, 0.0), 1.0)
        tiers = [LeagueTier.TIER1] if self._reset_tier1_only else list(LeagueTier)
        for tier in tiers:
            player_store = self.player_local[tier]
            for player_id, rating in list(player_store.items()):
                player_store[player_id] = self.config.initial_rating + (float(rating) - self.config.initial_rating) * local_keep
            roster_store = self.roster_ratings[tier]
            for roster_key, rating in list(roster_store.items()):
                roster_store[roster_key] = self.config.initial_rating + (float(rating) - self.config.initial_rating) * roster_keep

    def process_match(self, match):
        patch_key = self._patch_key(match.timestamp)
        if self._current_patch_key is None:
            self._current_patch_key = patch_key
        elif patch_key != self._current_patch_key:
            self._apply_patch_transition()
            self._current_patch_key = patch_key
        return super().process_match(match)


def _summarize_trial(trial: str, cfg: HybridEloConfig, report: dict[str, Any], baseline: dict[str, Any], trial_cfg: TrialCfg) -> dict[str, Any]:
    return {
        "trial": trial,
        "trial_cfg": _to_json_ready(trial_cfg),
        "config": _to_json_ready(cfg),
        "accuracy": float(report["accuracy"]),
        "log_loss": float(report["log_loss"]),
        "brier": float(report["brier"]),
        "by_tier": _to_json_ready(report.get("by_tier", {})),
        "delta_vs_baseline": {
            "accuracy": float(report["accuracy"]) - float(baseline["accuracy"]),
            "log_loss": float(report["log_loss"]) - float(baseline["log_loss"]),
            "brier": float(report["brier"]) - float(baseline["brier"]),
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
    baseline_cfg = HybridEloConfig()
    baseline_report = run_series_online_evaluation(
        model=HybridPlayerRosterEloModel(config=baseline_cfg),
        series_bundles=series_bundles,
        config=eval_cfg,
    )

    trials = [
        TrialCfg("baseline", "major", 1.0, 1.0),
        TrialCfg("major_local_reset", "major", 0.0, 1.0),
        TrialCfg("major_local_shrink50", "major", 0.5, 1.0),
        TrialCfg("major_local_reset_t1only", "major", 0.0, 1.0, True),
        TrialCfg("major_local_shrink50_t1only", "major", 0.5, 1.0, True),
        TrialCfg("major_local_roster_shrink50", "major", 0.5, 0.5),
        TrialCfg("major_local_roster_reset", "major", 0.0, 0.0),
        TrialCfg("exact_local_reset", "exact", 0.0, 1.0),
        TrialCfg("exact_local_reset_t1only", "exact", 0.0, 1.0, True),
        TrialCfg("exact_local_shrink50", "exact", 0.5, 1.0),
        TrialCfg("exact_local_shrink50_t1only", "exact", 0.5, 1.0, True),
        TrialCfg("exact_local_roster_shrink50", "exact", 0.5, 0.5),
    ]

    results: list[dict[str, Any]] = []
    for trial in trials:
        cfg = HybridEloConfig()
        if trial.name == "baseline":
            report = baseline_report
        else:
            report = run_series_online_evaluation(
                model=PatchAwareLocalModel(
                    config=cfg,
                    patch_mode=trial.patch_mode,
                    local_keep=trial.local_keep,
                    roster_keep=trial.roster_keep,
                    reset_tier1_only=trial.reset_tier1_only,
                ),
                series_bundles=series_bundles,
                config=eval_cfg,
            )
        results.append(_summarize_trial(trial.name, cfg, report, baseline_report, trial))

    best_by_tier1_accuracy = max(
        results,
        key=lambda row: (
            row["by_tier"]["TIER1"]["accuracy"] if row["by_tier"]["TIER1"]["accuracy"] is not None else float("-inf"),
            -(row["by_tier"]["TIER1"]["log_loss"] if row["by_tier"]["TIER1"]["log_loss"] is not None else float("inf")),
        ),
    )
    best_by_tier1_log_loss = min(
        results,
        key=lambda row: (
            row["by_tier"]["TIER1"]["log_loss"] if row["by_tier"]["TIER1"]["log_loss"] is not None else float("inf"),
            -(row["by_tier"]["TIER1"]["accuracy"] if row["by_tier"]["TIER1"]["accuracy"] is not None else float("-inf")),
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

    print(f"Saved patch-local sweep to {output_path}")
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
