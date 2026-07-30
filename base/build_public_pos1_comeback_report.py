#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Iterator, Optional

import orjson

try:
    import ijson
except Exception:
    ijson = None


DEFAULT_DATA_DIR = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/json_parts_split_from_object")
DEFAULT_OUTPUT_DIR = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/pos1_comeback_report_50pct")
HERO_FEATURES_PATH = Path("/Users/alex/Documents/ingame/base/hero_features_processed.json")
VALID_POSITIONS = {"POSITION_1", "POSITION_2", "POSITION_3", "POSITION_4", "POSITION_5"}


def _iter_match_items(path: Path) -> Iterator[tuple[str, dict]]:
    if ijson is not None:
        with path.open("rb") as fh:
            for key, value in ijson.kvitems(fh, "", use_float=True):
                if isinstance(value, dict):
                    yield str(key), value
        return

    payload = orjson.loads(path.read_bytes())
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(value, dict):
                yield str(key), value


def _combined_files(data_dir: Path) -> list[Path]:
    def _index(path: Path) -> int:
        stem = path.stem.replace("combined", "")
        return int(stem) if stem.isdigit() else 0

    return sorted(
        (path for path in data_dir.glob("combined*.json") if path.is_file()),
        key=_index,
    )


def _load_hero_name_map(path: Path) -> dict[int, str]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    out: dict[int, str] = {}
    for hero_id, payload in raw.items():
        if not isinstance(payload, dict):
            continue
        try:
            hero_id_int = int(payload.get("hero_id", hero_id))
        except Exception:
            continue
        hero_name = payload.get("hero_name") or payload.get("hero_slug") or f"hero_{hero_id_int}"
        out[hero_id_int] = str(hero_name)
    return out


def _winner_deficit(lead: float, did_radiant_win: bool) -> Optional[float]:
    if did_radiant_win:
        return -lead if lead < 0 else None
    return lead if lead > 0 else None


def _extract_pos1_heroes(match: dict) -> tuple[Optional[int], Optional[int]]:
    radiant_pos1 = None
    dire_pos1 = None
    for player in match.get("players") or []:
        position = player.get("position")
        if position not in VALID_POSITIONS or position != "POSITION_1":
            continue
        try:
            hero_id = int(player.get("heroId"))
        except Exception:
            continue
        if hero_id <= 0:
            continue
        if bool(player.get("isRadiant")):
            radiant_pos1 = hero_id
        else:
            dire_pos1 = hero_id
    return radiant_pos1, dire_pos1


def _safe_pct(numerator: int, denominator: int) -> Optional[float]:
    if denominator <= 0:
        return None
    return round((numerator / denominator) * 100.0, 2)


def _bucket_for_minute(minute: int) -> str:
    if minute <= 25:
        return "21-25"
    if minute <= 30:
        return "26-30"
    if minute <= 35:
        return "31-35"
    if minute <= 40:
        return "36-40"
    return "41-45"


def _round_or_none(value: Optional[float], digits: int = 2) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), digits)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build public-match pos1 comeback timing report.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--min-minute", type=int, default=21)
    parser.add_argument("--max-minute", type=int, default=45)
    parser.add_argument("--threshold-factor", type=float, default=0.5)
    parser.add_argument("--min-hero-matches", type=int, default=100)
    parser.add_argument("--min-comeback-matches", type=int, default=20)
    parser.add_argument("--limit-files", type=int, default=0)
    args = parser.parse_args()

    files = _combined_files(args.data_dir)
    if not files:
        raise SystemExit(f"No combined*.json files found in {args.data_dir}")
    if args.limit_files > 0:
        files = files[: args.limit_files]

    hero_name_by_id = _load_hero_name_map(HERO_FEATURES_PATH)

    minute_deficit_sum: DefaultDict[int, float] = defaultdict(float)
    minute_deficit_count: DefaultDict[int, int] = defaultdict(int)
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
            upper_bound = min(len(leads) - 1, int(args.max_minute))
            for minute in range(int(args.min_minute), upper_bound + 1):
                lead = leads[minute]
                if not isinstance(lead, (int, float)):
                    continue
                deficit = _winner_deficit(float(lead), did_radiant_win)
                if deficit is None or deficit <= 0:
                    continue
                minute_deficit_sum[minute] += float(deficit)
                minute_deficit_count[minute] += 1

        print(
            f"[pass1] {file_index}/{len(files)} {path.name} "
            f"scanned={scanned_matches:,} valid={valid_matches:,}"
        )

    minute_profile: dict[int, dict[str, float | int]] = {}
    for minute in range(int(args.min_minute), int(args.max_minute) + 1):
        samples = int(minute_deficit_count.get(minute, 0))
        avg_deficit = (minute_deficit_sum[minute] / samples) if samples else 0.0
        threshold = avg_deficit * float(args.threshold_factor) if avg_deficit > 0 else 0.0
        minute_profile[minute] = {
            "minute": minute,
            "samples": samples,
            "avg_deficit": round(avg_deficit, 2),
            "threshold_50pct": round(threshold, 2),
        }

    matches_as_pos1: DefaultDict[int, int] = defaultdict(int)
    comeback_match_count: DefaultDict[int, int] = defaultdict(int)
    qualifying_obs_count: DefaultDict[int, int] = defaultdict(int)
    first_trigger_minutes: DefaultDict[int, list[int]] = defaultdict(list)
    peak_trigger_minutes: DefaultDict[int, list[int]] = defaultdict(list)
    peak_deficits: DefaultDict[int, list[float]] = defaultdict(list)
    first_bucket_counts: DefaultDict[int, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))
    peak_bucket_counts: DefaultDict[int, DefaultDict[str, int]] = defaultdict(lambda: defaultdict(int))

    scanned_matches_pass2 = 0
    valid_matches_pass2 = 0
    valid_pos1_matches = 0

    for file_index, path in enumerate(files, start=1):
        for match_key, match in _iter_match_items(path):
            scanned_matches_pass2 += 1
            leads = match.get("radiantNetworthLeads") or []
            did_radiant_win = match.get("didRadiantWin")
            if not isinstance(leads, list) or not isinstance(did_radiant_win, bool):
                continue

            radiant_pos1, dire_pos1 = _extract_pos1_heroes(match)
            if radiant_pos1:
                matches_as_pos1[int(radiant_pos1)] += 1
            if dire_pos1:
                matches_as_pos1[int(dire_pos1)] += 1
            if not radiant_pos1 or not dire_pos1:
                continue

            valid_matches_pass2 += 1
            valid_pos1_matches += 2
            winning_pos1 = int(radiant_pos1 if did_radiant_win else dire_pos1)

            first_trigger_minute: Optional[int] = None
            peak_trigger_minute: Optional[int] = None
            peak_deficit: Optional[float] = None

            upper_bound = min(len(leads) - 1, int(args.max_minute))
            for minute in range(int(args.min_minute), upper_bound + 1):
                lead = leads[minute]
                if not isinstance(lead, (int, float)):
                    continue
                deficit = _winner_deficit(float(lead), did_radiant_win)
                if deficit is None or deficit <= 0:
                    continue
                threshold = float((minute_profile.get(minute) or {}).get("threshold_50pct") or 0.0)
                if threshold <= 0.0 or deficit < threshold:
                    continue

                qualifying_obs_count[winning_pos1] += 1
                if first_trigger_minute is None:
                    first_trigger_minute = int(minute)
                if peak_deficit is None or deficit > peak_deficit:
                    peak_deficit = float(deficit)
                    peak_trigger_minute = int(minute)

            if first_trigger_minute is None or peak_trigger_minute is None or peak_deficit is None:
                continue

            comeback_match_count[winning_pos1] += 1
            first_trigger_minutes[winning_pos1].append(first_trigger_minute)
            peak_trigger_minutes[winning_pos1].append(peak_trigger_minute)
            peak_deficits[winning_pos1].append(float(peak_deficit))
            first_bucket_counts[winning_pos1][_bucket_for_minute(first_trigger_minute)] += 1
            peak_bucket_counts[winning_pos1][_bucket_for_minute(peak_trigger_minute)] += 1

        print(
            f"[pass2] {file_index}/{len(files)} {path.name} "
            f"scanned={scanned_matches_pass2:,} valid={valid_matches_pass2:,} "
            f"comeback_matches={sum(comeback_match_count.values()):,}"
        )

    hero_rows: list[dict[str, object]] = []
    for hero_id, total_matches in matches_as_pos1.items():
        comeback_matches = int(comeback_match_count.get(hero_id, 0))
        first_list = first_trigger_minutes.get(hero_id) or []
        peak_minute_list = peak_trigger_minutes.get(hero_id) or []
        peak_deficit_list = peak_deficits.get(hero_id) or []
        row = {
            "hero_id": int(hero_id),
            "hero_name": hero_name_by_id.get(int(hero_id), f"hero_{hero_id}"),
            "position": "pos1",
            "matches_as_pos1": int(total_matches),
            "comeback_matches": comeback_matches,
            "comeback_rate_pct": _safe_pct(comeback_matches, int(total_matches)),
            "qualifying_observations": int(qualifying_obs_count.get(hero_id, 0)),
            "avg_first_trigger_minute": _round_or_none(statistics.mean(first_list), 2) if first_list else None,
            "median_first_trigger_minute": _round_or_none(statistics.median(first_list), 2) if first_list else None,
            "avg_peak_minute": _round_or_none(statistics.mean(peak_minute_list), 2) if peak_minute_list else None,
            "median_peak_minute": _round_or_none(statistics.median(peak_minute_list), 2) if peak_minute_list else None,
            "avg_peak_deficit": _round_or_none(statistics.mean(peak_deficit_list), 2) if peak_deficit_list else None,
            "max_peak_deficit": _round_or_none(max(peak_deficit_list), 2) if peak_deficit_list else None,
            "first_bucket_counts": dict(sorted((first_bucket_counts.get(hero_id) or {}).items())),
            "peak_bucket_counts": dict(sorted((peak_bucket_counts.get(hero_id) or {}).items())),
        }
        hero_rows.append(row)

    hero_rows.sort(
        key=lambda row: (
            -int(row["comeback_matches"]),
            -int(row["matches_as_pos1"]),
            str(row["hero_name"]),
        )
    )

    filtered_rows = [
        row
        for row in hero_rows
        if int(row["matches_as_pos1"]) >= int(args.min_hero_matches)
        and int(row["comeback_matches"]) >= int(args.min_comeback_matches)
    ]
    latest_peak_rows = sorted(
        filtered_rows,
        key=lambda row: (
            -(float(row["median_peak_minute"]) if row["median_peak_minute"] is not None else -1.0),
            -int(row["comeback_matches"]),
        ),
    )

    summary = {
        "config": {
            "data_dir": str(args.data_dir),
            "files": len(files),
            "min_minute": int(args.min_minute),
            "max_minute": int(args.max_minute),
            "threshold_factor": float(args.threshold_factor),
            "threshold_definition": "winner deficit >= threshold_factor * avg winner deficit for that minute",
            "min_hero_matches": int(args.min_hero_matches),
            "min_comeback_matches": int(args.min_comeback_matches),
        },
        "pass1": {
            "scanned_matches": int(scanned_matches),
            "valid_matches": int(valid_matches),
        },
        "pass2": {
            "scanned_matches": int(scanned_matches_pass2),
            "valid_matches": int(valid_matches_pass2),
            "valid_pos1_appearances": int(valid_pos1_matches),
            "heroes_with_pos1_matches": len(matches_as_pos1),
            "heroes_with_comebacks": sum(1 for value in comeback_match_count.values() if value > 0),
            "total_comeback_matches": int(sum(comeback_match_count.values())),
            "total_qualifying_observations": int(sum(qualifying_obs_count.values())),
        },
        "minute_profile": [minute_profile[minute] for minute in sorted(minute_profile)],
        "top_by_comeback_matches": hero_rows[:30],
        "latest_peak_minute_leaders": latest_peak_rows[:30],
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (args.output_dir / "hero_rows.json").write_text(
        json.dumps(hero_rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    csv_path = args.output_dir / "hero_rows.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "hero_id",
                "hero_name",
                "matches_as_pos1",
                "comeback_matches",
                "comeback_rate_pct",
                "qualifying_observations",
                "avg_first_trigger_minute",
                "median_first_trigger_minute",
                "avg_peak_minute",
                "median_peak_minute",
                "avg_peak_deficit",
                "max_peak_deficit",
                "first_21_25",
                "first_26_30",
                "first_31_35",
                "first_36_40",
                "first_41_45",
                "peak_21_25",
                "peak_26_30",
                "peak_31_35",
                "peak_36_40",
                "peak_41_45",
            ]
        )
        for row in hero_rows:
            first_counts = row["first_bucket_counts"] or {}
            peak_counts = row["peak_bucket_counts"] or {}
            writer.writerow(
                [
                    row["hero_id"],
                    row["hero_name"],
                    row["matches_as_pos1"],
                    row["comeback_matches"],
                    row["comeback_rate_pct"],
                    row["qualifying_observations"],
                    row["avg_first_trigger_minute"],
                    row["median_first_trigger_minute"],
                    row["avg_peak_minute"],
                    row["median_peak_minute"],
                    row["avg_peak_deficit"],
                    row["max_peak_deficit"],
                    first_counts.get("21-25", 0),
                    first_counts.get("26-30", 0),
                    first_counts.get("31-35", 0),
                    first_counts.get("36-40", 0),
                    first_counts.get("41-45", 0),
                    peak_counts.get("21-25", 0),
                    peak_counts.get("26-30", 0),
                    peak_counts.get("31-35", 0),
                    peak_counts.get("36-40", 0),
                    peak_counts.get("41-45", 0),
                ]
            )

    print(
        "built public pos1 comeback report: "
        f"heroes={len(hero_rows)}, filtered={len(filtered_rows)}, "
        f"total_comeback_matches={sum(comeback_match_count.values()):,}"
    )
    print(f"saved summary to {args.output_dir / 'summary.json'}")
    print(f"saved hero rows to {args.output_dir / 'hero_rows.json'}")


if __name__ == "__main__":
    main()
