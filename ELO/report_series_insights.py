from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ELO.config import EvaluationConfig, HybridEloConfig
from ELO.data_loader import load_matches
from ELO.domain import LeagueTier
from ELO.models import HybridPlayerRosterEloModel
from ELO.series_data import build_series_bundles
from ELO.series_evaluation import probability_to_win_series
from ELO.team_identity import resolve_org_key
from ELO.tiering import attach_league_tiers, classify_leagues


EDGE_BINS = [0, 25, 50, 75, 100, 150, 200, 300, 400]
SECONDS_PER_DAY = 24 * 60 * 60
LEADERBOARD_BASELINE = 1500.0


def _to_json_ready(value: Any) -> Any:
    if isinstance(value, LeagueTier):
        return value.value
    if is_dataclass(value):
        return _to_json_ready(asdict(value))
    if isinstance(value, dict):
        return {str(key): _to_json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_ready(item) for item in value]
    return value


def _timestamp_to_iso(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _default_data_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "pro_heroes_data" / "json_parts_split_from_object"


def _default_output_dir() -> Path:
    return Path(__file__).resolve().parent / "output" / "series_insights"


def _bin_label(edge: float) -> str:
    for idx, start in enumerate(EDGE_BINS[:-1]):
        end = EDGE_BINS[idx + 1]
        if start <= edge < end:
            return f"{start}-{end}"
    return f"{EDGE_BINS[-1]}+"


def _series_evaluation_start(eligible_series_count: int, config: EvaluationConfig) -> int:
    evaluation_series_count = max(1, int(eligible_series_count * config.evaluation_fraction))
    return max(config.min_train_series, eligible_series_count - evaluation_series_count)


def _decay_strength_for_leaderboard(
    raw_strength: float,
    days_inactive: float,
    half_life_days: float,
) -> float:
    if half_life_days <= 0 or days_inactive <= 0:
        return raw_strength
    keep_factor = math.pow(0.5, days_inactive / half_life_days)
    return LEADERBOARD_BASELINE + (raw_strength - LEADERBOARD_BASELINE) * keep_factor


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect hybrid series model edge bins and leaderboard.")
    parser.add_argument("--data-dir", type=Path, default=_default_data_dir())
    parser.add_argument("--output-dir", type=Path, default=_default_output_dir())
    parser.add_argument("--active-cutoff-days", type=float, default=180.0)
    parser.add_argument("--display-decay-half-life-days", type=float, default=120.0)
    args = parser.parse_args()

    matches, load_summary = load_matches(args.data_dir)
    league_info, _ = classify_leagues(matches)
    attach_league_tiers(matches, league_info)
    series_bundles, series_summary = build_series_bundles(matches)

    model = HybridPlayerRosterEloModel(HybridEloConfig())
    eval_config = EvaluationConfig()
    eligible_series_count = sum(1 for bundle in series_bundles if bundle.series.eligible_for_winner_target)
    evaluation_start_idx = _series_evaluation_start(eligible_series_count, eval_config)
    reference_timestamp = matches[-1].timestamp

    edge_bucket_rows: dict[str, list[dict[str, float]]] = defaultdict(list)
    team_snapshots: dict[str, dict[str, Any]] = {}
    eligible_seen = 0

    for bundle in series_bundles:
        series = bundle.series
        if series.eligible_for_winner_target:
            first_map = bundle.deciding_maps[0]
            step = model.predict_match(first_map)
            if eligible_seen >= evaluation_start_idx:
                team_a_is_favorite = step.radiant_strength >= step.dire_strength
                favorite_strength = max(step.radiant_strength, step.dire_strength)
                underdog_strength = min(step.radiant_strength, step.dire_strength)
                favorite_edge = favorite_strength - underdog_strength
                p_series_team_a = probability_to_win_series(step.p_radiant, series.best_of)
                p_series_favorite = p_series_team_a if team_a_is_favorite else (1.0 - p_series_team_a)
                favorite_won = (series.team_a_won is True and team_a_is_favorite) or (
                    series.team_a_won is False and not team_a_is_favorite
                )
                edge_bucket_rows[_bin_label(favorite_edge)].append(
                    {
                        "favorite_edge": favorite_edge,
                        "favorite_won": 1.0 if favorite_won else 0.0,
                        "favorite_prob": p_series_favorite,
                    }
                )
            eligible_seen += 1

        for match in bundle.all_maps:
            model.process_match(match)
            for is_radiant, team_id, team_name, player_ids in (
                (True, match.radiant_team_id, match.radiant_team_name, match.radiant_player_ids),
                (False, match.dire_team_id, match.dire_team_name, match.dire_player_ids),
            ):
                team_key = resolve_org_key(team_id, team_name)
                if team_key not in team_snapshots or match.timestamp >= team_snapshots[team_key]["timestamp"]:
                    team_snapshots[team_key] = {
                        "org_key": team_key,
                        "team_id": team_id,
                        "team_name": team_name,
                        "player_ids": player_ids,
                        "tier": match.derived_league_tier,
                        "timestamp": match.timestamp,
                        "is_radiant_last": is_radiant,
                    }

    edge_report = []
    for label in [_bin_label(value) for value in EDGE_BINS[:-1]] + [_bin_label(EDGE_BINS[-1])]:
        rows = edge_bucket_rows.get(label, [])
        if not rows:
            continue
        edge_report.append(
            {
                "edge_bin": label,
                "series_count": len(rows),
                "avg_favorite_edge": sum(row["favorite_edge"] for row in rows) / len(rows),
                "favorite_winrate": sum(row["favorite_won"] for row in rows) / len(rows),
                "avg_favorite_series_prob": sum(row["favorite_prob"] for row in rows) / len(rows),
            }
        )

    leaderboard = []
    for snapshot in team_snapshots.values():
        preview = model.preview_team_strength(
            team_id=snapshot["team_id"],
            team_name=snapshot["team_name"],
            player_ids=snapshot["player_ids"],
            tier=snapshot["tier"],
            timestamp=snapshot["timestamp"] + 1,
        )
        leaderboard.append(
            {
                "org_key": snapshot["org_key"],
                "team_id": snapshot["team_id"],
                "team_name": snapshot["team_name"],
                "tier": snapshot["tier"].value,
                "timestamp": snapshot["timestamp"],
                "last_seen_utc": _timestamp_to_iso(snapshot["timestamp"]),
                "raw_team_strength": preview["team_strength"],
                "player_strength": preview["player_strength"],
                "roster_rating": preview["roster_rating"],
                "roster_matches": preview["roster_matches"],
                "roster_weight": preview["roster_weight"],
                "roster_key": preview["roster_key"],
            }
        )

    for row in leaderboard:
        days_inactive = max(0.0, (reference_timestamp - row["timestamp"]) / SECONDS_PER_DAY)
        row["days_inactive"] = days_inactive
        row["current_strength"] = _decay_strength_for_leaderboard(
            raw_strength=row["raw_team_strength"],
            days_inactive=days_inactive,
            half_life_days=args.display_decay_half_life_days,
        )
        row["is_active"] = days_inactive <= args.active_cutoff_days

    leaderboard.sort(key=lambda row: (-row["current_strength"], row["team_name"].lower()))
    active_leaderboard = [row for row in leaderboard if row["is_active"]]
    top20 = active_leaderboard[:20]

    args.output_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "dataset_summary": {
            **load_summary,
            "eligible_series_count": eligible_series_count,
            "series_evaluation_start_idx": evaluation_start_idx,
            "series_summary": series_summary,
            "leaderboard_reference_ts": reference_timestamp,
            "leaderboard_reference_utc": _timestamp_to_iso(reference_timestamp),
            "leaderboard_active_cutoff_days": args.active_cutoff_days,
            "leaderboard_display_decay_half_life_days": args.display_decay_half_life_days,
        },
        "favorite_edge_winrates": edge_report,
        "leaderboard_top20_active": top20,
        "leaderboard_top20_all": leaderboard[:20],
    }
    with (args.output_dir / "series_insights.json").open("w", encoding="utf-8") as fh:
        json.dump(_to_json_ready(report), fh, ensure_ascii=False, indent=2)

    print("Favorite edge bins:")
    for row in edge_report:
        print(
            f"{row['edge_bin']:>7}  "
            f"n={row['series_count']:>4}  "
            f"avg_edge={row['avg_favorite_edge']:>6.1f}  "
            f"fav_wr={row['favorite_winrate']:.4f}  "
            f"avg_p={row['avg_favorite_series_prob']:.4f}"
        )
    print("")
    print("Active leaderboard top 20:")
    for idx, row in enumerate(top20, start=1):
        print(
            f"{idx:>2}. {row['team_name']:<22} "
            f"current={row['current_strength']:.1f} "
            f"raw={row['raw_team_strength']:.1f} "
            f"idle={row['days_inactive']:.0f}d "
            f"tier={row['tier']:<5} "
            f"last_seen={row['last_seen_utc']}"
        )
    print("")
    print(f"Saved report to {args.output_dir / 'series_insights.json'}")


if __name__ == "__main__":
    main()
