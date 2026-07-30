#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import DefaultDict, Iterator, Optional

import orjson

try:
    import ijson
except Exception:
    ijson = None


DEFAULT_DATA_DIR = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/json_parts_split_from_object")
DEFAULT_OUTPUT_DIR = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/comeback_experiment")
HERO_FEATURES_PATH = Path("/Users/alex/Documents/ingame/base/hero_features_processed.json")
VALID_POSITIONS = {"POSITION_1", "POSITION_2", "POSITION_3", "POSITION_4", "POSITION_5"}


@dataclass
class Stat:
    wins: int = 0
    total: int = 0
    deficit_sum: float = 0.0

    def add(self, is_win: bool, deficit: float) -> None:
        self.total += 1
        self.deficit_sum += deficit
        if is_win:
            self.wins += 1

    @property
    def wr(self) -> float:
        return (self.wins / self.total) if self.total else 0.0

    @property
    def avg_deficit(self) -> float:
        return (self.deficit_sum / self.total) if self.total else 0.0


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


def _load_hero_name_map(path: Path) -> dict[int, str]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    hero_name_by_id: dict[int, str] = {}
    if not isinstance(raw, dict):
        return hero_name_by_id
    for hero_id, payload in raw.items():
        if not isinstance(payload, dict):
            continue
        try:
            hero_id_int = int(payload.get("hero_id", hero_id))
        except Exception:
            continue
        hero_name = payload.get("hero_name") or payload.get("hero_slug") or f"hero_{hero_id_int}"
        hero_name_by_id[hero_id_int] = str(hero_name)
    return hero_name_by_id


def _combined_files(data_dir: Path) -> list[Path]:
    return sorted(
        (
            path
            for path in data_dir.glob("combined*.json")
            if path.is_file()
        ),
        key=lambda path: int(path.stem.replace("combined", "")),
    )


def _winner_deficit(lead: float, did_radiant_win: bool) -> Optional[float]:
    if did_radiant_win:
        return -lead if lead < 0 else None
    return lead if lead > 0 else None


def _position_slug(position: Optional[str]) -> Optional[str]:
    if position not in VALID_POSITIONS:
        return None
    return f"pos{position.split('_')[-1]}"


def _extract_side_entities(match: dict, group_by: str) -> tuple[list[tuple[int, Optional[str]]], list[tuple[int, Optional[str]]]]:
    radiant: list[tuple[int, Optional[str]]] = []
    dire: list[tuple[int, Optional[str]]] = []
    for player in match.get("players") or []:
        try:
            hero_id = int(player.get("heroId"))
        except Exception:
            continue
        if hero_id <= 0:
            continue
        position = _position_slug(player.get("position"))
        if position is None:
            continue
        entity = (hero_id, position) if group_by == "hero_position" else (hero_id, None)
        if bool(player.get("isRadiant")):
            radiant.append(entity)
        else:
            dire.append(entity)
    return radiant, dire


def _minute_from_index(index: int) -> int:
    return int(index)


def _pass_one_minute_profile(
    files: list[Path],
    min_minute: int,
    max_minute: int,
) -> tuple[dict[int, dict], dict[str, int]]:
    deficit_sum_by_minute: DefaultDict[int, float] = defaultdict(float)
    deficit_count_by_minute: DefaultDict[int, int] = defaultdict(int)
    scanned_matches = 0
    valid_matches = 0

    for file_index, path in enumerate(files, start=1):
        for _, match in _iter_match_items(path):
            scanned_matches += 1
            leads = match.get("radiantNetworthLeads") or []
            did_radiant_win = match.get("didRadiantWin")
            if not isinstance(leads, list) or not isinstance(did_radiant_win, bool):
                continue
            valid_matches += 1
            for index, lead in enumerate(leads):
                minute = _minute_from_index(index)
                if minute < min_minute or minute > max_minute:
                    continue
                if not isinstance(lead, (int, float)):
                    continue
                deficit = _winner_deficit(float(lead), did_radiant_win)
                if deficit is None or deficit <= 0:
                    continue
                deficit_sum_by_minute[minute] += deficit
                deficit_count_by_minute[minute] += 1

        print(
            f"[pass1] {file_index}/{len(files)} {path.name} "
            f"scanned={scanned_matches:,} valid={valid_matches:,}"
        )

    profile: dict[int, dict] = {}
    for minute in range(min_minute, max_minute + 1):
        count = deficit_count_by_minute.get(minute, 0)
        avg_deficit = (deficit_sum_by_minute[minute] / count) if count else 0.0
        profile[minute] = {
            "minute": minute,
            "samples": count,
            "avg_deficit": round(avg_deficit, 2),
        }

    meta = {
        "scanned_matches": scanned_matches,
        "valid_matches": valid_matches,
        "minutes_with_samples": sum(1 for row in profile.values() if row["samples"] > 0),
    }
    return profile, meta


def _pass_two_hero_stats(
    files: list[Path],
    minute_profile: dict[int, dict],
    min_minute: int,
    max_minute: int,
    min_profile_samples: int,
    group_by: str,
    deficit_multiplier: float,
) -> tuple[dict[tuple[int, Optional[str]], Stat], dict[tuple[int, Optional[str]], dict[int, Stat]], dict[int, Stat], dict[str, int]]:
    hero_total: DefaultDict[tuple[int, Optional[str]], Stat] = defaultdict(Stat)
    hero_by_minute: DefaultDict[tuple[int, Optional[str]], dict[int, Stat]] = defaultdict(dict)
    baseline_total: DefaultDict[int, Stat] = defaultdict(Stat)
    scanned_matches = 0
    valid_matches = 0

    for file_index, path in enumerate(files, start=1):
        for _, match in _iter_match_items(path):
            scanned_matches += 1
            leads = match.get("radiantNetworthLeads") or []
            did_radiant_win = match.get("didRadiantWin")
            if not isinstance(leads, list) or not isinstance(did_radiant_win, bool):
                continue

            radiant_heroes, dire_heroes = _extract_side_entities(match, group_by=group_by)
            if len(radiant_heroes) != 5 or len(dire_heroes) != 5:
                continue

            valid_matches += 1
            for index, lead in enumerate(leads):
                minute = _minute_from_index(index)
                if minute < min_minute or minute > max_minute:
                    continue
                if not isinstance(lead, (int, float)):
                    continue

                minute_row = minute_profile.get(minute)
                if not minute_row or int(minute_row["samples"]) < min_profile_samples:
                    continue
                target_deficit = float(minute_row["avg_deficit"]) * deficit_multiplier
                if target_deficit <= 0:
                    continue

                lead_value = float(lead)
                if lead_value > 0:
                    deficit = lead_value
                    heroes = dire_heroes
                    is_win = not did_radiant_win
                elif lead_value < 0:
                    deficit = -lead_value
                    heroes = radiant_heroes
                    is_win = did_radiant_win
                else:
                    continue

                if deficit < target_deficit:
                    continue

                baseline_total[minute].add(is_win, deficit)
                for hero_key in heroes:
                    hero_total[hero_key].add(is_win, deficit)
                    minute_bucket = hero_by_minute[hero_key].get(minute)
                    if minute_bucket is None:
                        minute_bucket = Stat()
                        hero_by_minute[hero_key][minute] = minute_bucket
                    minute_bucket.add(is_win, deficit)

        print(
            f"[pass2] {file_index}/{len(files)} {path.name} "
            f"scanned={scanned_matches:,} valid={valid_matches:,}"
        )

    meta = {
        "scanned_matches": scanned_matches,
        "valid_matches": valid_matches,
        "entities_with_samples": len(hero_total),
    }
    return hero_total, hero_by_minute, baseline_total, meta


def _hero_summary_rows(
    hero_total: dict[tuple[int, Optional[str]], Stat],
    hero_by_minute: dict[tuple[int, Optional[str]], dict[int, Stat]],
    baseline_total: dict[int, Stat],
    hero_name_by_id: dict[int, str],
    min_hero_observations: int,
) -> list[dict]:
    baseline_wins = sum(stat.wins for stat in baseline_total.values())
    baseline_obs = sum(stat.total for stat in baseline_total.values())
    overall_baseline_wr = (baseline_wins / baseline_obs) if baseline_obs else 0.0

    rows: list[dict] = []
    for hero_key, stat in hero_total.items():
        if stat.total < min_hero_observations:
            continue
        hero_id, position = hero_key
        minute_hits = hero_by_minute.get(hero_key, {})
        strongest_minutes = sorted(
            (
                {
                    "minute": minute,
                    "wr": round(minute_stat.wr * 100, 2),
                    "wins": minute_stat.wins,
                    "total": minute_stat.total,
                    "avg_deficit": round(minute_stat.avg_deficit, 2),
                }
                for minute, minute_stat in minute_hits.items()
                if minute_stat.total >= max(10, min_hero_observations // 5)
            ),
            key=lambda row: (row["wr"], row["total"]),
            reverse=True,
        )[:5]

        rows.append(
            {
                "hero_id": hero_id,
                "hero_name": hero_name_by_id.get(hero_id, f"hero_{hero_id}"),
                "position": position,
                "entity_key": (
                    f"{hero_name_by_id.get(hero_id, f'hero_{hero_id}')}:{position}"
                    if position
                    else hero_name_by_id.get(hero_id, f"hero_{hero_id}")
                ),
                "wins": stat.wins,
                "total": stat.total,
                "wr": round(stat.wr * 100, 2),
                "delta_vs_baseline_pp": round((stat.wr - overall_baseline_wr) * 100, 2),
                "avg_deficit": round(stat.avg_deficit, 2),
                "strongest_minutes": strongest_minutes,
            }
        )

    rows.sort(key=lambda row: (row["wr"], row["total"]), reverse=True)
    return rows


def _minute_baseline_rows(profile: dict[int, dict], baseline_total: dict[int, Stat]) -> list[dict]:
    rows: list[dict] = []
    for minute in sorted(profile):
        row = dict(profile[minute])
        stat = baseline_total.get(minute)
        row["qualifying_obs"] = stat.total if stat else 0
        row["qualifying_wr"] = round(stat.wr * 100, 2) if stat and stat.total else None
        row["qualifying_avg_deficit"] = round(stat.avg_deficit, 2) if stat and stat.total else None
        rows.append(row)
    return rows


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Comeback experiment on public matches.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--min-minute", type=int, default=10)
    parser.add_argument("--max-minute", type=int, default=45)
    parser.add_argument("--min-profile-samples", type=int, default=200)
    parser.add_argument("--min-hero-observations", type=int, default=100)
    parser.add_argument("--limit-files", type=int, default=0)
    parser.add_argument("--group-by", choices=("hero", "hero_position"), default="hero")
    parser.add_argument("--deficit-multiplier", type=float, default=1.0)
    args = parser.parse_args()

    files = _combined_files(args.data_dir)
    if not files:
        raise SystemExit(f"No combined*.json files found in {args.data_dir}")
    if args.limit_files > 0:
        files = files[:args.limit_files]

    hero_name_by_id = _load_hero_name_map(HERO_FEATURES_PATH)

    minute_profile, pass1_meta = _pass_one_minute_profile(
        files=files,
        min_minute=args.min_minute,
        max_minute=args.max_minute,
    )
    hero_total, hero_by_minute, baseline_total, pass2_meta = _pass_two_hero_stats(
        files=files,
        minute_profile=minute_profile,
        min_minute=args.min_minute,
        max_minute=args.max_minute,
        min_profile_samples=args.min_profile_samples,
        group_by=args.group_by,
        deficit_multiplier=args.deficit_multiplier,
    )

    minute_rows = _minute_baseline_rows(minute_profile, baseline_total)
    hero_rows = _hero_summary_rows(
        hero_total=hero_total,
        hero_by_minute=hero_by_minute,
        baseline_total=baseline_total,
        hero_name_by_id=hero_name_by_id,
        min_hero_observations=args.min_hero_observations,
    )

    summary = {
        "config": {
            "data_dir": str(args.data_dir),
            "files": len(files),
            "min_minute": args.min_minute,
            "max_minute": args.max_minute,
            "min_profile_samples": args.min_profile_samples,
            "min_hero_observations": args.min_hero_observations,
            "group_by": args.group_by,
            "deficit_multiplier": args.deficit_multiplier,
        },
        "pass1_meta": pass1_meta,
        "pass2_meta": pass2_meta,
        "minute_profile": minute_rows,
        "top_heroes": hero_rows[:50],
    }

    _write_json(args.output_dir / "comeback_minute_profile.json", minute_rows)
    _write_json(args.output_dir / "comeback_hero_summary.json", hero_rows)
    _write_json(args.output_dir / "comeback_experiment_summary.json", summary)

    print("\nTop entities by comeback WR on target deficits:")
    for row in hero_rows[:30]:
        print(
            f"{row['entity_key']:<32} "
            f"wr={row['wr']:>5.2f}% "
            f"obs={row['total']:<6} "
            f"delta={row['delta_vs_baseline_pp']:>+6.2f} pp "
            f"avg_deficit={row['avg_deficit']:>7.1f}"
        )


if __name__ == "__main__":
    main()
