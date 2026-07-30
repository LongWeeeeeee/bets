from __future__ import annotations

import argparse
import copy
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
from ELO.tiering import attach_league_tiers, attach_league_tiers_time_aware, classify_leagues


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
    return Path(__file__).resolve().parents[1] / "pro_heroes_data" / "json_parts_split_prod"


def _default_output_path() -> Path:
    return Path(__file__).resolve().parent / "output" / "time_aware_tiering_experiment_2026_03_24.json"


def _series_tier_map(series_bundles) -> dict[int, str]:
    out: dict[int, str] = {}
    for bundle in series_bundles:
        out[int(bundle.series.series_id)] = bundle.series.derived_league_tier.value
    return out


def _tier_change_summary(before: dict[int, str], after: dict[int, str]) -> dict[str, Any]:
    changed = 0
    change_pairs: dict[str, int] = {}
    for key, prev in before.items():
        nxt = after.get(key)
        if nxt is None or nxt == prev:
            continue
        changed += 1
        change_pairs[f"{prev}->{nxt}"] = change_pairs.get(f"{prev}->{nxt}", 0) + 1
    return {
        "changed": changed,
        "change_pairs": dict(sorted(change_pairs.items())),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare static league tiering against rolling time-aware tiering.")
    parser.add_argument("--data-dir", type=Path, default=_default_data_dir())
    parser.add_argument("--output-path", type=Path, default=_default_output_path())
    args = parser.parse_args()

    matches, load_summary = load_matches(args.data_dir)
    if not matches:
        raise SystemExit("No valid matches were loaded.")

    static_matches = copy.deepcopy(matches)
    time_aware_matches = copy.deepcopy(matches)

    static_league_info, static_league_summary = classify_leagues(static_matches)
    attach_league_tiers(static_matches, static_league_info)
    static_series_bundles, static_series_summary = build_series_bundles(static_matches)

    time_aware_match_summary = attach_league_tiers_time_aware(time_aware_matches)
    time_aware_series_bundles, time_aware_series_summary = build_series_bundles(time_aware_matches)

    eval_cfg = EvaluationConfig()
    model_cfg = HybridEloConfig()

    static_report = run_series_online_evaluation(
        model=HybridPlayerRosterEloModel(config=model_cfg),
        series_bundles=static_series_bundles,
        config=eval_cfg,
    )
    time_aware_report = run_series_online_evaluation(
        model=HybridPlayerRosterEloModel(config=model_cfg),
        series_bundles=time_aware_series_bundles,
        config=eval_cfg,
    )

    static_match_tiers = {match.match_id: match.derived_league_tier.value for match in static_matches}
    time_aware_match_tiers = {match.match_id: match.derived_league_tier.value for match in time_aware_matches}
    static_series_tiers = _series_tier_map(static_series_bundles)
    time_aware_series_tiers = _series_tier_map(time_aware_series_bundles)

    output = {
        "data_dir": str(args.data_dir),
        "dataset_summary": load_summary,
        "evaluation": _to_json_ready(eval_cfg),
        "model_config": _to_json_ready(model_cfg),
        "static": {
            "league_summary": static_league_summary,
            "series_summary": static_series_summary,
            "report": static_report,
        },
        "time_aware": {
            "match_tier_summary": time_aware_match_summary,
            "series_summary": time_aware_series_summary,
            "report": time_aware_report,
        },
        "tier_changes": {
            "matches": _tier_change_summary(static_match_tiers, time_aware_match_tiers),
            "series": _tier_change_summary(static_series_tiers, time_aware_series_tiers),
        },
        "delta_time_aware_minus_static": {
            "accuracy": float(time_aware_report["accuracy"]) - float(static_report["accuracy"]),
            "log_loss": float(time_aware_report["log_loss"]) - float(static_report["log_loss"]),
            "brier": float(time_aware_report["brier"]) - float(static_report["brier"]),
            "tier1_accuracy": float(time_aware_report["by_tier"]["TIER1"]["accuracy"])
            - float(static_report["by_tier"]["TIER1"]["accuracy"]),
            "tier1_log_loss": float(time_aware_report["by_tier"]["TIER1"]["log_loss"])
            - float(static_report["by_tier"]["TIER1"]["log_loss"]),
            "tier1_brier": float(time_aware_report["by_tier"]["TIER1"]["brier"])
            - float(static_report["by_tier"]["TIER1"]["brier"]),
            "tier2_accuracy": float(time_aware_report["by_tier"]["TIER2"]["accuracy"])
            - float(static_report["by_tier"]["TIER2"]["accuracy"]),
            "tier2_log_loss": float(time_aware_report["by_tier"]["TIER2"]["log_loss"])
            - float(static_report["by_tier"]["TIER2"]["log_loss"]),
            "tier2_brier": float(time_aware_report["by_tier"]["TIER2"]["brier"])
            - float(static_report["by_tier"]["TIER2"]["brier"]),
        },
    }

    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    with args.output_path.open("w", encoding="utf-8") as fh:
        json.dump(_to_json_ready(output), fh, ensure_ascii=False, indent=2)

    print(f"Saved time-aware tiering experiment to {args.output_path}")
    print(
        "Static "
        f"acc={static_report['accuracy']:.4f} "
        f"ll={static_report['log_loss']:.4f} "
        f"brier={static_report['brier']:.4f}"
    )
    print(
        "TimeAware "
        f"acc={time_aware_report['accuracy']:.4f} "
        f"ll={time_aware_report['log_loss']:.4f} "
        f"brier={time_aware_report['brier']:.4f}"
    )
    print(
        "Delta "
        f"acc={output['delta_time_aware_minus_static']['accuracy']:.4f} "
        f"ll={output['delta_time_aware_minus_static']['log_loss']:.4f} "
        f"brier={output['delta_time_aware_minus_static']['brier']:.4f}"
    )


if __name__ == "__main__":
    main()
