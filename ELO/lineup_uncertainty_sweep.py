from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, is_dataclass
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
    return Path(__file__).resolve().parent / "output" / "lineup_uncertainty_sweep.json"


def _trial_configs() -> list[tuple[str, HybridEloConfig]]:
    base = HybridEloConfig()
    return [
        ("baseline", base),
        (
            "local_boost_0.25_decay4",
            HybridEloConfig(
                lineup_uncertainty_boost_max=0.25,
                lineup_uncertainty_boost_matches=4,
                lineup_uncertainty_boost_global=False,
                lineup_uncertainty_boost_local=True,
                lineup_uncertainty_boost_roster=False,
            ),
        ),
        (
            "local_boost_0.50_decay4",
            HybridEloConfig(
                lineup_uncertainty_boost_max=0.50,
                lineup_uncertainty_boost_matches=4,
                lineup_uncertainty_boost_global=False,
                lineup_uncertainty_boost_local=True,
                lineup_uncertainty_boost_roster=False,
            ),
        ),
        (
            "local_boost_1.00_decay4",
            HybridEloConfig(
                lineup_uncertainty_boost_max=1.00,
                lineup_uncertainty_boost_matches=4,
                lineup_uncertainty_boost_global=False,
                lineup_uncertainty_boost_local=True,
                lineup_uncertainty_boost_roster=False,
            ),
        ),
        (
            "local_boost_0.50_decay8",
            HybridEloConfig(
                lineup_uncertainty_boost_max=0.50,
                lineup_uncertainty_boost_matches=8,
                lineup_uncertainty_boost_global=False,
                lineup_uncertainty_boost_local=True,
                lineup_uncertainty_boost_roster=False,
            ),
        ),
        (
            "all_boost_0.25_decay4",
            HybridEloConfig(
                lineup_uncertainty_boost_max=0.25,
                lineup_uncertainty_boost_matches=4,
                lineup_uncertainty_boost_global=True,
                lineup_uncertainty_boost_local=True,
                lineup_uncertainty_boost_roster=True,
            ),
        ),
        (
            "all_boost_0.50_decay4",
            HybridEloConfig(
                lineup_uncertainty_boost_max=0.50,
                lineup_uncertainty_boost_matches=4,
                lineup_uncertainty_boost_global=True,
                lineup_uncertainty_boost_local=True,
                lineup_uncertainty_boost_roster=True,
            ),
        ),
        (
            "all_boost_1.00_decay4",
            HybridEloConfig(
                lineup_uncertainty_boost_max=1.00,
                lineup_uncertainty_boost_matches=4,
                lineup_uncertainty_boost_global=True,
                lineup_uncertainty_boost_local=True,
                lineup_uncertainty_boost_roster=True,
            ),
        ),
        (
            "all_boost_0.50_decay8",
            HybridEloConfig(
                lineup_uncertainty_boost_max=0.50,
                lineup_uncertainty_boost_matches=8,
                lineup_uncertainty_boost_global=True,
                lineup_uncertainty_boost_local=True,
                lineup_uncertainty_boost_roster=True,
            ),
        ),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Series-level sweep for lineup uncertainty K-boost after roster changes.")
    parser.add_argument("--data-dir", type=Path, default=_default_data_dir())
    parser.add_argument("--output-path", type=Path, default=_default_output_path())
    args = parser.parse_args()

    matches, load_summary = load_matches(args.data_dir)
    league_info, league_summary = classify_leagues(matches)
    attach_league_tiers(matches, league_info)
    series_bundles, series_summary = build_series_bundles(matches)
    eval_config = EvaluationConfig()

    results: list[dict[str, Any]] = []
    for trial_name, cfg in _trial_configs():
        report = run_series_online_evaluation(
            model=HybridPlayerRosterEloModel(cfg),
            series_bundles=series_bundles,
            config=eval_config,
        )
        results.append(
            {
                "trial": trial_name,
                "config": _to_json_ready(cfg),
                "accuracy": report["accuracy"],
                "log_loss": report["log_loss"],
                "brier": report["brier"],
                "by_tier": report.get("by_tier"),
            }
        )
        print(
            f"{trial_name}: accuracy={report['accuracy']:.6f} "
            f"log_loss={report['log_loss']:.6f} brier={report['brier']:.6f}"
        )

    baseline = next(item for item in results if item["trial"] == "baseline")
    for item in results:
        item["delta_vs_baseline"] = {
            "accuracy": float(item["accuracy"] or 0.0) - float(baseline["accuracy"] or 0.0),
            "log_loss": float(item["log_loss"] or 0.0) - float(baseline["log_loss"] or 0.0),
            "brier": float(item["brier"] or 0.0) - float(baseline["brier"] or 0.0),
        }

    results_by_log_loss = sorted(results, key=lambda item: (item["log_loss"], -float(item["accuracy"] or 0.0)))
    results_by_accuracy = sorted(results, key=lambda item: (-float(item["accuracy"] or 0.0), item["log_loss"]))

    payload = {
        "data_dir": str(args.data_dir),
        "dataset_summary": load_summary,
        "league_summary": league_summary,
        "series_summary": series_summary,
        "evaluation": _to_json_ready(eval_config),
        "baseline": baseline,
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
