from __future__ import annotations

import argparse
import glob
import json
import math
import os
import re
import resource
import sys
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Iterable, Optional


BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
for _path in (str(BASE_DIR), str(ROOT_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

try:
    import ijson
except Exception:  # pragma: no cover
    ijson = None

try:
    import orjson
except Exception:  # pragma: no cover
    orjson = None

from analise_database import is_early_match, is_late_match, is_post_lane_match
import dota2protracker
from functions import (
    calculate_lanes,
    check_bad_map,
    structure_lane_dict,
    synergy_and_counterpick,
)


DEFAULT_MAPS_PATH = ROOT_DIR / "pro_heroes_data" / "pro.json"
DEFAULT_STATS_DIR = ROOT_DIR / "bets_data" / "analise_pub_matches"
DEFAULT_OUTPUT_PATH = ROOT_DIR / "runtime" / "pro_maps_metrics_2025-12-15.json"
DEC_15_2025_UTC = 1765756800
PATCH_START_TIMES = {
    "7.40": 1765756800,
    "7.41": 1774310400,
}


def _rss_mb() -> float:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / 1024 / 1024 if sys.platform == "darwin" else rss / 1024


def _load_json(path: Path) -> Any:
    if orjson is not None:
        return orjson.loads(path.read_bytes())
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if orjson is not None:
        path.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))
        return
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _iter_json_object_items(path: Path):
    if ijson is not None:
        with path.open("rb") as f:
            yield from ijson.kvitems(f, "", use_float=True)
        return
    data = _load_json(path)
    if isinstance(data, dict):
        yield from data.items()
    else:
        for item in data:
            if isinstance(item, dict):
                yield str(item.get("id", "")), item


def _has_glob_meta(value: str) -> bool:
    return any(ch in value for ch in "*?[]")


def _resolve_maps_paths(maps_path: str | Path, patch: Optional[str] = None) -> list[Path]:
    raw_parts = [part.strip() for part in str(maps_path).split(",") if part.strip()]
    resolved: list[Path] = []
    for raw_part in raw_parts:
        path = Path(raw_part)
        if _has_glob_meta(raw_part):
            matches = sorted(Path(item) for item in glob.glob(raw_part))
        elif path.is_dir():
            pattern = f"{patch}_part*.json" if patch else "*.json"
            matches = sorted(
                item
                for item in path.glob(pattern)
                if item.is_file() and item.name != "merge_patch_summary.json"
            )
        else:
            matches = [path]
        resolved.extend(matches)

    seen: set[Path] = set()
    unique_paths: list[Path] = []
    for path in resolved:
        normalized = path.expanduser()
        key = normalized.resolve() if normalized.exists() else normalized
        if key in seen:
            continue
        seen.add(key)
        unique_paths.append(normalized)

    if not unique_paths:
        suffix = f" for patch {patch}" if patch else ""
        raise FileNotFoundError(f"No map files found{suffix}: {maps_path}")
    missing = [str(path) for path in unique_paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Map file does not exist: {missing[0]}")
    return unique_paths


def _stats_key_leading_hero_id(key: Any) -> str:
    match = re.match(r"^(\d+)pos[1-5]", str(key))
    return match.group(1) if match else "misc"


class ShardedStatsLookup(dict):
    def __init__(self, shard_dir: Path, max_cached_shards: int = 48):
        super().__init__()
        self.shard_dir = Path(shard_dir)
        self.max_cached_shards = max(1, int(max_cached_shards))
        self._shards: OrderedDict[str, dict] = OrderedDict()

    def __bool__(self) -> bool:
        return True

    def _load_shard(self, shard_id: str) -> dict:
        shard_id = str(shard_id or "misc")
        cached = self._shards.get(shard_id)
        if cached is not None:
            self._shards.move_to_end(shard_id)
            return cached

        shard_data: dict = {}
        shard_path = self.shard_dir / f"{shard_id}.jsonl"
        if shard_path.exists():
            with shard_path.open("rb") as f:
                for raw_line in f:
                    line = raw_line.strip()
                    if not line:
                        continue
                    if orjson is not None:
                        key, value = orjson.loads(line)
                    else:
                        key, value = json.loads(line)
                    shard_data[str(key)] = value

        self._shards[shard_id] = shard_data
        self._shards.move_to_end(shard_id)
        while len(self._shards) > self.max_cached_shards:
            self._shards.popitem(last=False)
        return shard_data

    def warm_hero_ids(self, hero_ids: Iterable[Any]) -> None:
        for hero_id in hero_ids:
            try:
                self._load_shard(str(int(hero_id)))
            except (TypeError, ValueError):
                continue

    def get(self, key: Any, default=None):
        shard = self._load_shard(_stats_key_leading_hero_id(key))
        return shard.get(str(key), default)


def _load_stats_dicts(
    stats_dir: Path,
    *,
    include_dicts: bool,
    post_lane_max_cached_shards: int = 48,
) -> tuple[dict, dict, Any, Any]:
    if not include_dicts:
        return {}, {}, {}, {}

    stats_dir = Path(stats_dir)
    early_dict = _load_json(stats_dir / "early_dict_raw.json")
    print(f"  ✓ early_dict: {len(early_dict):,} keys, RSS≈{_rss_mb():.0f}MB")
    late_dict = _load_json(stats_dir / "late_dict_raw.json")
    print(f"  ✓ late_dict: {len(late_dict):,} keys, RSS≈{_rss_mb():.0f}MB")
    lane_dict = _load_json(stats_dir / "lane_dict_raw.json")
    lane_dict = structure_lane_dict(lane_dict)
    print(f"  ✓ lane_dict: structured, RSS≈{_rss_mb():.0f}MB")

    post_lane_path = stats_dir / "post_lane_dict_raw.json"
    shard_dir = stats_dir / "post_lane_dict_raw.shards"
    if shard_dir.exists() and (shard_dir / "_complete").exists():
        post_lane_dict = ShardedStatsLookup(shard_dir, max_cached_shards=post_lane_max_cached_shards)
        print(f"  ✓ post_lane_dict: sharded lookup {shard_dir}")
    elif post_lane_path.exists():
        post_lane_dict = _load_json(post_lane_path)
        print(f"  ✓ post_lane_dict: {len(post_lane_dict):,} keys, RSS≈{_rss_mb():.0f}MB")
    else:
        post_lane_dict = {}
        print(f"  ⚠ post_lane_dict missing: {post_lane_path}")

    return early_dict, late_dict, lane_dict, post_lane_dict


def _compact_bucket(bucket: Any) -> dict:
    if not isinstance(bucket, dict):
        return {}
    return {
        key: value
        for key, value in bucket.items()
        if not key.endswith("_games") and (key == "synergy_duo" or not key.startswith("synergy_duo_"))
    }


def _team_payload(team: dict) -> dict:
    out = {}
    for pos in ("pos1", "pos2", "pos3", "pos4", "pos5"):
        hero_id = int((team.get(pos) or {}).get("hero_id") or 0)
        hero_name = dota2protracker.get_hero_name(hero_id) if hero_id else ""
        out[pos] = {"hero_id": hero_id, "hero_name": hero_name}
    return out


def _match_outcomes(match: dict) -> dict:
    early_ok, early_winner = is_early_match(match)
    late_ok, late_winner = is_late_match(match, early_winner, if_check=True)
    post_lane_ok, post_lane_winner = is_post_lane_match(match, if_check=True)
    actual_winner = "radiant" if match.get("didRadiantWin") else "dire"
    return {
        "actual_winner": actual_winner,
        "is_early": bool(early_ok),
        "early_win": early_winner if early_ok else None,
        "is_late": bool(late_ok),
        "late_win": late_winner if late_ok else None,
        "is_post_lane": bool(post_lane_ok),
        "post_lane_win": post_lane_winner if post_lane_ok else None,
    }


def _cached_protracker_payload(hero_name: str, *, allow_stale: bool) -> Optional[dict]:
    if not allow_stale:
        return None
    cache_file = Path(dota2protracker.CACHE_DIR) / f"{hero_name.replace(' ', '_').lower()}.json"
    if not cache_file.exists():
        return None
    try:
        data = _load_json(cache_file)
    except Exception:
        return None
    if isinstance(data, dict) and data.get("cache_schema_version") == dota2protracker.CACHE_SCHEMA_VERSION:
        return data
    return None


def _collect_dota2protracker_data(
    records: list[dict],
    *,
    allow_stale_cache: bool = True,
    refresh: bool = False,
    sleep_seconds: float = 0.0,
) -> dict[str, dict]:
    hero_names: set[str] = set()
    for record in records:
        for side_key in ("radiant_draft", "dire_draft"):
            for payload in record.get(side_key, {}).values():
                hero_name = str((payload or {}).get("hero_name") or "").strip().lower()
                if hero_name:
                    hero_names.add(hero_name)

    hero_data: dict[str, dict] = {}
    heroes = sorted(hero_names)
    print(f"\n[Dota2ProTracker] unique heroes: {len(heroes)}")

    for idx, hero_name in enumerate(heroes, 1):
        cached = None if refresh else _cached_protracker_payload(hero_name, allow_stale=allow_stale_cache)
        if cached is not None:
            hero_data[hero_name] = cached
            source = "cache"
        else:
            hero_data[hero_name] = dota2protracker.parse_hero_matchups(hero_name, use_cache=not refresh)
            source = "site"
            if sleep_seconds > 0:
                time.sleep(float(sleep_seconds))

        if idx == 1 or idx % 10 == 0 or idx == len(heroes):
            print(f"  [{idx:>3}/{len(heroes)}] {hero_name} ({source}) RSS≈{_rss_mb():.0f}MB", flush=True)

    return hero_data


def _avg_or_zero(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _protracker_metrics_for_match(radiant_draft: dict, dire_draft: dict, hero_data: dict, min_games: int) -> dict:
    result = {
        "pro_cp1vs1_early": 0.0,
        "pro_cp1vs1_late": 0.0,
        "pro_cp1vs1_valid": False,
        "pro_cp1vs1_reason": "not_computed",
        "pro_duo_synergy_early": 0.0,
        "pro_duo_synergy_late": 0.0,
        "pro_duo_synergy_valid": False,
        "pro_duo_synergy_reason": "not_computed",
        "pro_lane_advantage": 0.0,
        "pro_lane_cp1vs1_valid": False,
        "pro_lane_duo_valid": False,
    }
    radiant_positions, radiant_cores, _ = dota2protracker._extract_team_positions_and_cores(radiant_draft)
    dire_positions, dire_cores, _ = dota2protracker._extract_team_positions_and_cores(dire_draft)
    if len(radiant_cores) < 3 or len(dire_cores) < 3:
        result["pro_cp1vs1_reason"] = "insufficient_core_heroes"
        result["pro_duo_synergy_reason"] = "insufficient_core_heroes"
        return result

    cp_valid, cp_data = dota2protracker._calculate_cp1vs1_all_positions(
        radiant_positions, dire_positions, hero_data, min_games
    )
    result["pro_cp1vs1_valid"] = bool(cp_valid)
    result["pro_cp1vs1_reason"] = "ok" if cp_valid else "insufficient_core_vs_core_coverage"
    result["pro_cp1vs1_games"] = int(cp_data.get("games") or 0)
    if cp_valid and cp_data.get("scores"):
        cp_score = _avg_or_zero(cp_data["scores"])
        result["pro_cp1vs1_early"] = cp_score
        result["pro_cp1vs1_late"] = cp_score

    r_duo_valid, r_duo_data = dota2protracker._calculate_duo_synergy_all_positions(
        radiant_positions, hero_data, min_games, dota2protracker.PRO_EARLY_POSITION_WEIGHTS
    )
    d_duo_valid, d_duo_data = dota2protracker._calculate_duo_synergy_all_positions(
        dire_positions, hero_data, min_games, dota2protracker.PRO_EARLY_POSITION_WEIGHTS
    )
    result["pro_duo_synergy_valid"] = bool(r_duo_valid and d_duo_valid)
    result["pro_duo_synergy_reason"] = "ok" if result["pro_duo_synergy_valid"] else "insufficient_duo_core_coverage"
    result["pro_duo_synergy_games"] = int((r_duo_data.get("games") or 0) + (d_duo_data.get("games") or 0))
    if result["pro_duo_synergy_valid"] and r_duo_data.get("scores") and d_duo_data.get("scores"):
        duo_score = _avg_or_zero(r_duo_data["scores"]) - _avg_or_zero(d_duo_data["scores"])
        result["pro_duo_synergy_early"] = duo_score
        result["pro_duo_synergy_late"] = duo_score

    lane_data = dota2protracker.calculate_lane_advantage(
        radiant_positions, dire_positions, hero_data, min_games
    )
    result.update(
        {
            "pro_lane_mid_cp1vs1": lane_data["mid"]["cp1vs1"],
            "pro_lane_top_cp1vs1": lane_data["top"]["cp1vs1"],
            "pro_lane_bot_cp1vs1": lane_data["bot"]["cp1vs1"],
            "pro_lane_mid_cp1vs1_valid": lane_data["mid"]["cp1vs1_valid"],
            "pro_lane_top_cp1vs1_valid": lane_data["top"]["cp1vs1_valid"],
            "pro_lane_bot_cp1vs1_valid": lane_data["bot"]["cp1vs1_valid"],
            "pro_lane_top_duo": lane_data["top"]["duo"],
            "pro_lane_bot_duo": lane_data["bot"]["duo"],
            "pro_lane_top_duo_valid": lane_data["top"]["duo_valid"],
            "pro_lane_bot_duo_valid": lane_data["bot"]["duo_valid"],
            "pro_lane_advantage": lane_data["lane_advantage"],
            "pro_lane_cp1vs1_valid": lane_data["cp1vs1_valid"],
            "pro_lane_duo_valid": lane_data["duo_valid"],
        }
    )
    return result


def collect_matches(
    maps_paths: str | Path | Iterable[Path],
    start_date_time: int,
    max_matches: Optional[int],
) -> list[tuple[str, dict, dict, dict]]:
    if isinstance(maps_paths, (str, Path)):
        paths = [Path(maps_paths)]
    else:
        paths = [Path(path) for path in maps_paths]
    rows: list[tuple[str, dict, dict, dict]] = []
    scanned = 0
    skipped = 0
    for path in paths:
        print(f"Reading maps: {path}", flush=True)
        for match_id, match in _iter_json_object_items(path):
            scanned += 1
            if not isinstance(match, dict):
                skipped += 1
                continue
            if int(match.get("startDateTime") or 0) < int(start_date_time):
                continue
            parsed = check_bad_map(match, start_date_time=start_date_time)
            if parsed is None:
                skipped += 1
                continue
            radiant_draft, dire_draft = parsed
            rows.append((str(match_id), match, _team_payload(radiant_draft), _team_payload(dire_draft)))
            if max_matches is not None and len(rows) >= int(max_matches):
                break
            if scanned % 5000 == 0:
                print(f"  scanned={scanned:,} selected={len(rows):,} skipped={skipped:,}", flush=True)
        if max_matches is not None and len(rows) >= int(max_matches):
            break
    print(f"Selected matches: {len(rows):,} (scanned={scanned:,}, skipped={skipped:,})")
    return rows


def check_old_maps(
    early_dict=None,
    late_dict=None,
    lane_data=None,
    outfile_name: str = "pro_maps_metrics_2025-12-15",
    custom_weights=None,
    write_to_file: bool = True,
    start_date_time: int = DEC_15_2025_UTC,
    maps_path: Optional[str | Path] = None,
    output_path: Optional[str | Path] = None,
    merge_side_lanes: bool = False,
    disable_lanes: bool = False,
    max_matches: Optional[int] = None,
    autoload_dicts: bool = True,
    use_lane_corrector: bool = False,
    lane_corrector_dir: Optional[str] = None,
    post_lane_dict=None,
    *,
    dicts: bool = True,
    patch: Optional[str] = None,
    dota2protracker_enabled: bool = False,
    dota2protracker_min_games: int = 10,
    dota2protracker_allow_stale_cache: bool = True,
    dota2protracker_refresh: bool = False,
    dota2protracker_sleep: float = 0.0,
    post_lane_max_cached_shards: int = 48,
    stats_dir: str | Path = DEFAULT_STATS_DIR,
) -> dict:
    del use_lane_corrector, lane_corrector_dir
    started_at = time.monotonic()
    maps_paths = _resolve_maps_paths(maps_path or DEFAULT_MAPS_PATH, patch=patch)
    output_path = Path(output_path or ROOT_DIR / "runtime" / f"{outfile_name}.json")

    print("\nCHECK_OLD_MAPS OFFLINE")
    print("=" * 80)
    print(f"maps_path: {maps_paths[0] if len(maps_paths) == 1 else f'{len(maps_paths)} files'}")
    if len(maps_paths) > 1:
        print(f"first_map_file: {maps_paths[0]}")
        print(f"last_map_file: {maps_paths[-1]}")
    if patch:
        print(f"patch: {patch}")
    print(f"start_date_time: {start_date_time}")
    print(f"flags: dicts={dicts}, dota2protracker={dota2protracker_enabled}")
    if dicts:
        print(f"post_lane_max_cached_shards: {post_lane_max_cached_shards}")

    rows = collect_matches(maps_paths, int(start_date_time), max_matches)
    records: list[dict] = []
    for match_id, match, radiant_draft, dire_draft in rows:
        records.append(
            {
                "id": int(match.get("id") or match_id or 0),
                "startDateTime": int(match.get("startDateTime") or 0),
                "radiantTeam": match.get("radiantTeam"),
                "direTeam": match.get("direTeam"),
                "didRadiantWin": match.get("didRadiantWin"),
                "radiantNetworthLeads": match.get("radiantNetworthLeads", []),
                "winRates": match.get("winRates", []),
                "topLaneOutcome": match.get("topLaneOutcome"),
                "midLaneOutcome": match.get("midLaneOutcome"),
                "bottomLaneOutcome": match.get("bottomLaneOutcome"),
                "radiant_draft": radiant_draft,
                "dire_draft": dire_draft,
                **_match_outcomes(match),
            }
        )

    if dicts:
        if autoload_dicts:
            early_dict, late_dict, lane_data, post_lane_dict = _load_stats_dicts(
                Path(stats_dir),
                include_dicts=True,
                post_lane_max_cached_shards=post_lane_max_cached_shards,
            )
        else:
            early_dict = early_dict or {}
            late_dict = late_dict or {}
            lane_data = structure_lane_dict(lane_data or {}) if lane_data and "2v2_lanes" not in lane_data else (lane_data or {})
            post_lane_dict = post_lane_dict or {}

        for idx, record in enumerate(records, 1):
            if hasattr(post_lane_dict, "warm_hero_ids"):
                hero_ids = [
                    payload.get("hero_id")
                    for side_key in ("radiant_draft", "dire_draft")
                    for payload in record.get(side_key, {}).values()
                ]
                post_lane_dict.warm_hero_ids(hero_ids)
            metrics = synergy_and_counterpick(
                radiant_heroes_and_pos=record["radiant_draft"],
                dire_heroes_and_pos=record["dire_draft"],
                early_dict=early_dict,
                mid_dict=late_dict,
                custom_weights=custom_weights,
                post_lane_dict=post_lane_dict,
            ) or {}
            record["early_output"] = _compact_bucket(metrics.get("early_output"))
            record["late_output"] = _compact_bucket(metrics.get("mid_output"))
            record["post_lane_output"] = _compact_bucket(metrics.get("post_lane_output"))
            if not disable_lanes:
                top, bot, mid = calculate_lanes(
                    record["radiant_draft"],
                    record["dire_draft"],
                    lane_data,
                    merge_side_lanes=merge_side_lanes,
                )
                record["top"] = top
                record["bot"] = bot
                record["mid"] = mid
            if idx == 1 or idx % 250 == 0 or idx == len(records):
                print(f"  dicts [{idx:>5}/{len(records)}] RSS≈{_rss_mb():.0f}MB", flush=True)

    if dota2protracker_enabled:
        hero_data = _collect_dota2protracker_data(
            records,
            allow_stale_cache=dota2protracker_allow_stale_cache,
            refresh=dota2protracker_refresh,
            sleep_seconds=dota2protracker_sleep,
        )
        for idx, record in enumerate(records, 1):
            record["dota2protracker"] = _protracker_metrics_for_match(
                record["radiant_draft"],
                record["dire_draft"],
                hero_data,
                int(dota2protracker_min_games),
            )
            if idx == 1 or idx % 250 == 0 or idx == len(records):
                print(f"  dota2protracker [{idx:>5}/{len(records)}]", flush=True)

    output = {str(record["id"]): record for record in records}
    if write_to_file:
        _dump_json(output_path, output)
        print(f"\nSaved: {output_path}")
    elapsed = time.monotonic() - started_at
    print(f"Done: {len(records):,} matches in {elapsed / 60:.1f} min, RSS≈{_rss_mb():.0f}MB")
    return output


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline draft metrics collector for historical maps.")
    parser.add_argument("--maps-path", default=str(DEFAULT_MAPS_PATH), help="JSON file, comma-separated files, glob, or directory with patch parts.")
    parser.add_argument("--patch", default=None, help="Patch prefix for split public files, for example 7.41.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--stats-dir", default=str(DEFAULT_STATS_DIR))
    parser.add_argument("--start-date-time", type=int, default=None)
    parser.add_argument("--max-matches", type=int, default=None)
    parser.add_argument("--dicts", dest="dicts", action="store_true", default=True)
    parser.add_argument("--no-dicts", dest="dicts", action="store_false")
    parser.add_argument("--dota2protracker", dest="dota2protracker", action="store_true", default=False)
    parser.add_argument("--no-dota2protracker", dest="dota2protracker", action="store_false")
    parser.add_argument("--dota2protracker-min-games", type=int, default=10)
    parser.add_argument("--dota2protracker-refresh", action="store_true", default=False)
    parser.add_argument("--no-stale-dota2protracker-cache", action="store_true", default=False)
    parser.add_argument("--dota2protracker-sleep", type=float, default=0.0)
    parser.add_argument("--post-lane-max-cached-shards", type=int, default=48)
    parser.add_argument("--merge-side-lanes", action="store_true", default=False)
    parser.add_argument("--disable-lanes", action="store_true", default=False)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    start_date_time = args.start_date_time
    if start_date_time is None:
        start_date_time = PATCH_START_TIMES.get(str(args.patch), DEC_15_2025_UTC) if args.patch else DEC_15_2025_UTC
    check_old_maps(
        maps_path=args.maps_path,
        patch=args.patch,
        output_path=args.output,
        start_date_time=start_date_time,
        max_matches=args.max_matches,
        dicts=args.dicts,
        stats_dir=args.stats_dir,
        dota2protracker_enabled=args.dota2protracker,
        dota2protracker_min_games=args.dota2protracker_min_games,
        dota2protracker_allow_stale_cache=not args.no_stale_dota2protracker_cache,
        dota2protracker_refresh=args.dota2protracker_refresh,
        dota2protracker_sleep=args.dota2protracker_sleep,
        post_lane_max_cached_shards=args.post_lane_max_cached_shards,
        merge_side_lanes=args.merge_side_lanes,
        disable_lanes=args.disable_lanes,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
