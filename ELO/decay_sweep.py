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
    return Path(__file__).resolve().parent / "output" / "decay_sweep.json"


@dataclass(frozen=True)
class TrialCfg:
    name: str
    player_global_decay_half_life_days: float
    player_local_decay_half_life_days: float


def _summarize_trial(trial: str, cfg: HybridEloConfig, report: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    return {
        "trial": trial,
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
        TrialCfg("baseline", 0.0, 0.0),
        TrialCfg("global365_local0", 365.0, 0.0),
        TrialCfg("global240_local0", 240.0, 0.0),
        TrialCfg("global365_local180", 365.0, 180.0),
        TrialCfg("global240_local120", 240.0, 120.0),
        TrialCfg("global180_local90", 180.0, 90.0),
        TrialCfg("global540_local180", 540.0, 180.0),
    ]

    results: list[dict[str, Any]] = []
    for trial in trials:
        cfg = HybridEloConfig(
            player_global_decay_half_life_days=trial.player_global_decay_half_life_days,
            player_local_decay_half_life_days=trial.player_local_decay_half_life_days,
        )
        report = run_series_online_evaluation(
            model=HybridPlayerRosterEloModel(config=cfg),
            series_bundles=series_bundles,
            config=eval_cfg,
        )
        results.append(_summarize_trial(trial.name, cfg, report, baseline_report))

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

    print(f"Saved decay sweep to {output_path}")
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
