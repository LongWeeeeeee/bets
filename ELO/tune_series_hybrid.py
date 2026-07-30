from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, is_dataclass
from itertools import product
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
    return Path(__file__).resolve().parent / "output" / "series_hybrid_tuning.json"


def main() -> None:
    parser = argparse.ArgumentParser(description="Grid search for the hybrid model on series-winner prediction.")
    parser.add_argument("--data-dir", type=Path, default=_default_data_dir())
    parser.add_argument("--output-path", type=Path, default=_default_output_path())
    args = parser.parse_args()

    matches, _ = load_matches(args.data_dir)
    league_info, _ = classify_leagues(matches)
    attach_league_tiers(matches, league_info)
    series_bundles, _ = build_series_bundles(matches)
    eval_config = EvaluationConfig()

    base_global = {
        LeagueTier.TIER1: 28.8,
        LeagueTier.TIER2: 12.0,
        LeagueTier.TIER3: 7.2,
    }
    base_local = {
        LeagueTier.TIER1: 18.4,
        LeagueTier.TIER2: 25.3,
        LeagueTier.TIER3: 20.7,
    }
    base_roster = {
        LeagueTier.TIER1: 18.0,
        LeagueTier.TIER2: 8.0,
        LeagueTier.TIER3: 5.0,
    }

    trials: list[dict[str, Any]] = []
    for global_weight, max_roster_weight, roster_full_weight_matches, g_scale, l_scale in product(
        [0.68, 0.72, 0.76],
        [0.05, 0.10, 0.15],
        [20, 28, 36],
        [0.9, 1.0, 1.1],
        [0.9, 1.0, 1.1],
    ):
        cfg = HybridEloConfig(
            player_global_weight=global_weight,
            player_tier_weight=1.0 - global_weight,
            max_roster_weight=max_roster_weight,
            roster_full_weight_matches=roster_full_weight_matches,
            k_global_by_tier={tier: value * g_scale for tier, value in base_global.items()},
            k_local_by_tier={tier: value * l_scale for tier, value in base_local.items()},
            k_roster_by_tier=base_roster,
        )
        report = run_series_online_evaluation(
            model=HybridPlayerRosterEloModel(cfg),
            series_bundles=series_bundles,
            config=eval_config,
        )
        trials.append(
            {
                "config": _to_json_ready(cfg),
                "accuracy": report["accuracy"],
                "log_loss": report["log_loss"],
                "brier": report["brier"],
            }
        )

    trials.sort(key=lambda item: item["log_loss"])
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    with args.output_path.open("w", encoding="utf-8") as fh:
        json.dump(
            {
                "evaluation": _to_json_ready(eval_config),
                "num_trials": len(trials),
                "best": trials[0],
                "top10": trials[:10],
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )

    print(f"Trials: {len(trials)}")
    print(f"Best log_loss: {trials[0]['log_loss']:.6f}")
    print(f"Best accuracy: {trials[0]['accuracy']:.6f}")
    print(f"Saved tuning report to {args.output_path}")


if __name__ == "__main__":
    main()
