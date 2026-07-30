from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ELO.config import EvaluationConfig, HybridEloConfig, SimpleTeamEloConfig
from ELO.data_loader import load_matches
from ELO.domain import LeagueTier
from ELO.models import HybridPlayerRosterEloModel, SimpleTeamEloModel
from ELO.series_data import build_series_bundles
from ELO.series_evaluation import run_series_online_evaluation
from ELO.tiering import attach_league_tiers, classify_leagues


def _to_json_ready(value: Any) -> Any:
    if isinstance(value, LeagueTier):
        return value.value
    if is_dataclass(value):
        return _to_json_ready(asdict(value))
    if isinstance(value, dict):
        converted: dict[str, Any] = {}
        for key, item in value.items():
            if isinstance(key, LeagueTier):
                converted[key.value] = _to_json_ready(item)
            else:
                converted[str(key)] = _to_json_ready(item)
        return converted
    if isinstance(value, (list, tuple)):
        return [_to_json_ready(item) for item in value]
    return value


def _timestamp_to_iso(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _default_data_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "pro_heroes_data" / "json_parts_split_prod"


def _default_output_dir() -> Path:
    return Path(__file__).resolve().parent / "output" / "series_run"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run series winner experiments for Dota 2 pre-series prediction.")
    parser.add_argument("--data-dir", type=Path, default=_default_data_dir())
    parser.add_argument("--output-dir", type=Path, default=_default_output_dir())
    args = parser.parse_args()

    matches, load_summary = load_matches(args.data_dir)
    if not matches:
        raise SystemExit("No valid matches were loaded.")

    league_info, league_summary = classify_leagues(matches)
    attach_league_tiers(matches, league_info)
    series_bundles, series_summary = build_series_bundles(matches)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    evaluation_config = EvaluationConfig()
    baseline_config = SimpleTeamEloConfig()
    hybrid_config = HybridEloConfig()

    baseline_report = run_series_online_evaluation(
        model=SimpleTeamEloModel(config=baseline_config),
        series_bundles=series_bundles,
        config=evaluation_config,
    )
    hybrid_report = run_series_online_evaluation(
        model=HybridPlayerRosterEloModel(config=hybrid_config),
        series_bundles=series_bundles,
        config=evaluation_config,
    )

    tier_series_counts: dict[str, int] = {}
    for tier in LeagueTier:
        tier_series_counts[tier.value] = sum(
            1
            for bundle in series_bundles
            if bundle.series.eligible_for_winner_target and bundle.series.derived_league_tier == tier
        )

    report = {
        "data_dir": str(args.data_dir),
        "output_dir": str(args.output_dir),
        "dataset_summary": {
            **load_summary,
            "first_match_ts": matches[0].timestamp,
            "first_match_utc": _timestamp_to_iso(matches[0].timestamp),
            "last_match_ts": matches[-1].timestamp,
            "last_match_utc": _timestamp_to_iso(matches[-1].timestamp),
        },
        "league_summary": league_summary,
        "series_summary": {
            **series_summary,
            "eligible_series_by_tier": tier_series_counts,
        },
        "configs": {
            "evaluation": _to_json_ready(evaluation_config),
            "baseline": _to_json_ready(baseline_config),
            "hybrid": _to_json_ready(hybrid_config),
        },
        "models": {
            "simple_team_elo_series": baseline_report,
            "hybrid_player_roster_series": hybrid_report,
        },
    }

    series_diagnostics = [
        _to_json_ready(bundle.series)
        for bundle in series_bundles
        if bundle.series.eligible_for_winner_target
    ]

    with (args.output_dir / "report.json").open("w", encoding="utf-8") as fh:
        json.dump(_to_json_ready(report), fh, ensure_ascii=False, indent=2)
    with (args.output_dir / "series_diagnostics.json").open("w", encoding="utf-8") as fh:
        json.dump(series_diagnostics[:2000], fh, ensure_ascii=False, indent=2)

    print(f"Loaded matches: {len(matches)}")
    print(f"Eligible series: {sum(1 for bundle in series_bundles if bundle.series.eligible_for_winner_target)}")
    print(
        "SimpleTeamEloSeries "
        f"accuracy={baseline_report['accuracy']:.4f} "
        f"log_loss={baseline_report['log_loss']:.4f} "
        f"brier={baseline_report['brier']:.4f}"
    )
    print(
        "HybridPlayerRosterSeries "
        f"accuracy={hybrid_report['accuracy']:.4f} "
        f"log_loss={hybrid_report['log_loss']:.4f} "
        f"brier={hybrid_report['brier']:.4f}"
    )
    print(f"Saved report to {args.output_dir / 'report.json'}")


if __name__ == "__main__":
    main()
