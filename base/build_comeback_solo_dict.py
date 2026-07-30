#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Iterator

import orjson

try:
    import ijson
except Exception:
    ijson = None


DEFAULT_DATA_DIR = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/json_parts_split_from_object")
DEFAULT_PROFILE_PATH = Path(
    "/Users/alex/Documents/ingame/bets_data/analise_pub_matches/comeback_experiment_hard_1_7_hero_position/comeback_minute_profile.json"
)
DEFAULT_OUTPUT_DIR = Path(
    "/Users/alex/Documents/ingame/bets_data/analise_pub_matches/comeback_experiment_hard_1_7_hero_position"
)


def _iter_match_items(path: Path) -> Iterator[tuple[str, dict]]:
    if ijson is not None:
        with path.open("rb") as f:
            for key, value in ijson.kvitems(f, "", use_float=True):
                if isinstance(value, dict):
                    yield str(key), value
        return

    payload = orjson.loads(path.read_bytes())
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(value, dict):
                yield str(key), value


def _combined_files(data_dir: Path) -> list[Path]:
    return sorted(
        (path for path in data_dir.glob("combined*.json") if path.is_file()),
        key=lambda path: int(path.stem.replace("combined", "")),
    )


def _append_stat(target_dict: dict, key: str, is_win: bool) -> None:
    row = target_dict.get(key)
    if row is None:
        row = {"wins": 0, "draws": 0, "games": 0}
        target_dict[key] = row
    row["games"] += 1
    if is_win:
        row["wins"] += 1


def _hero_pos_key(hero_id: int, position: str) -> str:
    return f"{hero_id}{position}"


def _extract_side_entities(match: dict) -> tuple[list[tuple[int, str]], list[tuple[int, str]]]:
    radiant: list[tuple[int, str]] = []
    dire: list[tuple[int, str]] = []
    for player in match.get("players") or []:
        try:
            hero_id = int(player.get("heroId"))
        except Exception:
            continue
        if hero_id <= 0:
            continue
        position = player.get("position")
        if not isinstance(position, str) or not position.startswith("POSITION_"):
            continue
        pos_slug = f"pos{position.split('_')[-1]}"
        entity = (hero_id, pos_slug)
        if bool(player.get("isRadiant")):
            radiant.append(entity)
        else:
            dire.append(entity)
    return radiant, dire


def _load_minute_profile(path: Path, min_profile_samples: int) -> dict[int, float]:
    rows = json.loads(path.read_text(encoding="utf-8"))
    profile: dict[int, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            minute = int(row["minute"])
            samples = int(row["samples"])
            avg_deficit = float(row["avg_deficit"])
        except Exception:
            continue
        if samples < min_profile_samples or avg_deficit <= 0:
            continue
        profile[minute] = avg_deficit
    return profile


def main() -> None:
    parser = argparse.ArgumentParser(description="Build 21+ minute comeback solo dict.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--profile-path", type=Path, default=DEFAULT_PROFILE_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output-name", default="comeback_solo_dict_21plus.json")
    parser.add_argument("--meta-name", default="comeback_solo_dict_21plus_meta.json")
    parser.add_argument("--min-minute", type=int, default=21)
    parser.add_argument("--max-minute", type=int, default=45)
    parser.add_argument("--deficit-multiplier", type=float, default=1.7)
    parser.add_argument("--min-profile-samples", type=int, default=200)
    parser.add_argument("--limit-files", type=int, default=0)
    args = parser.parse_args()

    files = _combined_files(args.data_dir)
    if not files:
        raise SystemExit(f"No combined*.json files found in {args.data_dir}")
    if args.limit_files > 0:
        files = files[:args.limit_files]

    minute_profile = _load_minute_profile(args.profile_path, args.min_profile_samples)
    if not minute_profile:
        raise SystemExit(f"No usable minute profile rows in {args.profile_path}")

    comeback_dict: dict[str, dict] = {}
    baseline_games = 0
    baseline_wins = 0
    qualifying_by_minute: DefaultDict[int, int] = defaultdict(int)
    scanned_matches = 0
    valid_matches = 0

    for file_index, path in enumerate(files, start=1):
        for _, match in _iter_match_items(path):
            scanned_matches += 1
            leads = match.get("radiantNetworthLeads") or []
            did_radiant_win = match.get("didRadiantWin")
            if not isinstance(leads, list) or not isinstance(did_radiant_win, bool):
                continue

            radiant_entities, dire_entities = _extract_side_entities(match)
            if len(radiant_entities) != 5 or len(dire_entities) != 5:
                continue

            valid_matches += 1
            for minute, avg_deficit in minute_profile.items():
                if minute < args.min_minute or minute > args.max_minute:
                    continue
                if minute >= len(leads):
                    continue
                lead_value = leads[minute]
                if not isinstance(lead_value, (int, float)):
                    continue

                threshold = float(avg_deficit) * float(args.deficit_multiplier)
                lead_float = float(lead_value)
                if lead_float > 0:
                    deficit = lead_float
                    entities = dire_entities
                    is_win = not did_radiant_win
                elif lead_float < 0:
                    deficit = -lead_float
                    entities = radiant_entities
                    is_win = did_radiant_win
                else:
                    continue

                if deficit < threshold:
                    continue

                baseline_games += 1
                qualifying_by_minute[minute] += 1
                if is_win:
                    baseline_wins += 1

                for hero_id, pos_slug in entities:
                    _append_stat(comeback_dict, _hero_pos_key(hero_id, pos_slug), is_win=is_win)

        print(
            f"[build] {file_index}/{len(files)} {path.name} "
            f"scanned={scanned_matches:,} valid={valid_matches:,} qualifying={baseline_games:,}"
        )

    baseline_wr = (baseline_wins / baseline_games) if baseline_games else 0.0
    meta = {
        "config": {
            "data_dir": str(args.data_dir),
            "profile_path": str(args.profile_path),
            "files": len(files),
            "min_minute": args.min_minute,
            "max_minute": args.max_minute,
            "deficit_multiplier": args.deficit_multiplier,
            "min_profile_samples": args.min_profile_samples,
        },
        "scanned_matches": scanned_matches,
        "valid_matches": valid_matches,
        "qualifying_observations": baseline_games,
        "baseline_wins": baseline_wins,
        "baseline_wr": baseline_wr,
        "baseline_wr_pct": round(baseline_wr * 100, 4),
        "qualifying_by_minute": {
            str(minute): count for minute, count in sorted(qualifying_by_minute.items())
        },
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / args.output_name).write_text(
        json.dumps(comeback_dict, ensure_ascii=False),
        encoding="utf-8",
    )
    (args.output_dir / args.meta_name).write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(
        f"built entries={len(comeback_dict):,} "
        f"baseline_wr={meta['baseline_wr_pct']:.2f}% "
        f"qualifying={baseline_games:,}"
    )


if __name__ == "__main__":
    main()
