#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from comeback_solo_hero_experiment import (
    HERO_FEATURES_PATH,
    _combined_files,
    _load_hero_name_map,
    _pass_one_minute_profile,
    _pass_two_hero_stats,
)


DEFAULT_CONFIG_SUMMARY = Path(
    "/Users/alex/Documents/ingame/bets_data/analise_pub_matches/comeback_experiment_hard_1_7_hero_position/comeback_experiment_summary.json"
)
DEFAULT_OUTPUT_PATH = Path(
    "/Users/alex/Documents/ingame/bets_data/analise_pub_matches/comeback_experiment_hard_1_7_hero_position/hero_position_minute_edges.json"
)


def _load_summary_config(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    config = payload.get("config")
    if not isinstance(config, dict):
        raise SystemExit(f"Invalid config payload in {path}")
    return config


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build hero-position minute comeback edge file.")
    parser.add_argument("--config-summary", type=Path, default=DEFAULT_CONFIG_SUMMARY)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--limit-files", type=int, default=None)
    parser.add_argument("--min-minute", type=int, default=None)
    parser.add_argument("--max-minute", type=int, default=None)
    parser.add_argument("--min-profile-samples", type=int, default=None)
    parser.add_argument("--min-hero-observations", type=int, default=None)
    parser.add_argument("--group-by", choices=("hero", "hero_position"), default=None)
    parser.add_argument("--deficit-multiplier", type=float, default=None)
    args = parser.parse_args()

    summary_config = _load_summary_config(args.config_summary)
    data_dir = args.data_dir or Path(str(summary_config["data_dir"]))
    min_minute = int(args.min_minute or summary_config["min_minute"])
    max_minute = int(args.max_minute or summary_config["max_minute"])
    min_profile_samples = int(args.min_profile_samples or summary_config["min_profile_samples"])
    min_hero_observations = int(args.min_hero_observations or summary_config["min_hero_observations"])
    group_by = str(args.group_by or summary_config["group_by"])
    deficit_multiplier = float(args.deficit_multiplier or summary_config["deficit_multiplier"])

    files = _combined_files(data_dir)
    if not files:
        raise SystemExit(f"No combined*.json files found in {data_dir}")

    limit_files = args.limit_files
    if limit_files is None:
        config_files = summary_config.get("files")
        if isinstance(config_files, int) and config_files > 0:
            limit_files = int(config_files)
    if limit_files:
        files = files[: int(limit_files)]

    hero_name_by_id = _load_hero_name_map(HERO_FEATURES_PATH)
    minute_profile, pass1_meta = _pass_one_minute_profile(
        files=files,
        min_minute=min_minute,
        max_minute=max_minute,
    )
    hero_total, hero_by_minute, baseline_total, pass2_meta = _pass_two_hero_stats(
        files=files,
        minute_profile=minute_profile,
        min_minute=min_minute,
        max_minute=max_minute,
        min_profile_samples=min_profile_samples,
        group_by=group_by,
        deficit_multiplier=deficit_multiplier,
    )

    rows: list[dict[str, Any]] = []
    for hero_key, stat in sorted(
        hero_total.items(),
        key=lambda item: (item[0][1] or "", hero_name_by_id.get(item[0][0], f"hero_{item[0][0]}")),
    ):
        if stat.total < min_hero_observations:
            continue
        hero_id, position = hero_key
        hero_name = hero_name_by_id.get(hero_id, f"hero_{hero_id}")
        entity_key = f"{hero_name}:{position}" if position else hero_name
        minute_rows: list[dict[str, Any]] = []
        for minute in range(min_minute, max_minute + 1):
            minute_stat = (hero_by_minute.get(hero_key) or {}).get(minute)
            baseline_stat = baseline_total.get(minute)
            baseline_wr = round(baseline_stat.wr * 100.0, 2) if baseline_stat and baseline_stat.total else None
            hero_wr = round(minute_stat.wr * 100.0, 2) if minute_stat and minute_stat.total else None
            edge_pp = round(hero_wr - baseline_wr, 2) if hero_wr is not None and baseline_wr is not None else None
            minute_rows.append(
                {
                    "minute": minute,
                    "hero_wins": int(minute_stat.wins) if minute_stat else 0,
                    "hero_total": int(minute_stat.total) if minute_stat else 0,
                    "hero_wr": hero_wr,
                    "hero_avg_deficit": round(minute_stat.avg_deficit, 2) if minute_stat and minute_stat.total else None,
                    "baseline_wins": int(baseline_stat.wins) if baseline_stat else 0,
                    "baseline_total": int(baseline_stat.total) if baseline_stat else 0,
                    "baseline_wr": baseline_wr,
                    "baseline_avg_deficit": round(baseline_stat.avg_deficit, 2) if baseline_stat and baseline_stat.total else None,
                    "edge_vs_baseline_pp": edge_pp,
                }
            )
        rows.append(
            {
                "hero_id": int(hero_id),
                "hero_name": hero_name,
                "position": position,
                "entity_key": entity_key,
                "wins": int(stat.wins),
                "total": int(stat.total),
                "wr": round(stat.wr * 100.0, 2),
                "avg_deficit": round(stat.avg_deficit, 2),
                "minute_rows": minute_rows,
            }
        )

    payload = {
        "config": {
            "data_dir": str(data_dir),
            "files": len(files),
            "min_minute": min_minute,
            "max_minute": max_minute,
            "min_profile_samples": min_profile_samples,
            "min_hero_observations": min_hero_observations,
            "group_by": group_by,
            "deficit_multiplier": deficit_multiplier,
            "source_config_summary": str(args.config_summary),
        },
        "pass1_meta": pass1_meta,
        "pass2_meta": pass2_meta,
        "rows": rows,
    }
    _write_json(args.output_path, payload)
    print(f"Wrote {len(rows)} entities to {args.output_path}")


if __name__ == "__main__":
    main()
