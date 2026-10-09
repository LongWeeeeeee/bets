#!/usr/bin/env python3
"""Convert OpenDota explorer listing/players JSON arrays to ELO-only maps."""
from __future__ import annotations

import argparse
import json
import os
import tempfile
from collections import Counter, defaultdict
from pathlib import Path


DEFAULT_OUTPUT = Path(__file__).resolve().parents[2] / "data/elo_supplement/opendota_maps.json"
SERIES_TYPES = {0: "BEST_OF_ONE", 1: "BEST_OF_THREE", 2: "BEST_OF_FIVE", 3: "BEST_OF_TWO"}


def _positive_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _rows(input_dir: Path, pattern: str):
    for path in sorted(input_dir.glob(pattern)):
        rows = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ValueError(f"{path}: expected a JSON array of row objects")
        yield from rows


def convert(input_dir: Path, excluded_ids: set[int]) -> tuple[dict[str, dict], dict[str, int]]:
    counts = Counter(listing_rows=0, player_rows=0, excluded=0, invalid=0, invalid_team=0, written=0,
                     duplicate_listing=0, conflicting_listing=0)
    listings: dict[int, dict] = {}
    conflicts: set[int] = set()
    players: dict[int, set[tuple[int, int]]] = defaultdict(set)
    invalid_players: set[int] = set()
    for row in _rows(input_dir, "players_*.json"):
        counts["player_rows"] += 1
        mid, account, slot = row.get("match_id"), row.get("account_id"), row.get("player_slot")
        if not _positive_int(mid):
            continue
        if (not _positive_int(account) or account >= 4294967295
                or isinstance(slot, bool) or not isinstance(slot, int) or not 0 <= slot < 256):
            invalid_players.add(mid)
            continue
        players[mid].add((account, slot))
    for row in _rows(input_dir, "listing_*.json"):
        counts["listing_rows"] += 1
        mid = row.get("match_id")
        if not _positive_int(mid):
            counts["invalid"] += 1
            continue
        if mid in listings:
            counts["duplicate_listing"] += 1
            if listings[mid] != row:
                conflicts.add(mid)
            continue
        listings[mid] = row
    maps = {}
    for mid, row in sorted(listings.items()):
        if mid in excluded_ids:
            counts["excluded"] += 1
            continue
        slots = sorted(players[mid], key=lambda item: (item[1], item[0]))
        if (mid in conflicts or mid in invalid_players
                or not _positive_int(row.get("start_time"))
                or not _positive_int(row.get("duration"))
                or not isinstance(row.get("radiant_win"), bool)
                or len(slots) != 10 or len({account for account, _ in slots}) != 10
                or sum(slot < 128 for _, slot in slots) != 5):
            counts["invalid"] += 1
            counts["conflicting_listing"] += int(mid in conflicts)
            continue
        league_id = row.get("leagueid")
        if not _positive_int(league_id):
            counts["invalid"] += 1
            continue
        teams = {}
        for side in ("radiant", "dire"):
            team_id = row.get(f"{side}_team_id")
            team_name = row.get(f"{side}_name")
            if (not _positive_int(team_id) or not isinstance(team_name, str)
                    or not team_name.strip() or team_name.strip().casefold().startswith("od-")):
                break
            teams[f"{side}Team"] = {"id": team_id, "name": team_name}
        if len(teams) != 2:
            counts["invalid_team"] += 1
            continue
        tier = row.get("tier")
        series_type = row.get("series_type")
        maps[str(mid)] = {
            "id": mid, "startDateTime": row["start_time"],
            "durationSeconds": row["duration"], "didRadiantWin": row["radiant_win"],
            "leagueId": league_id,
            "league": {"id": league_id, "tier": tier.upper() if isinstance(tier, str) else None,
                       "name": row.get("league_name") or ""},
            # OpenDota answers 0 (and null) when it has no series; 0 would group unrelated
            # maps of the same pair in ELO/series_data.py, so only a positive id is kept.
            "series": {"id": row.get("series_id") if _positive_int(row.get("series_id")) else None,
                       "type": SERIES_TYPES.get(series_type) if isinstance(series_type, int)
                       and not isinstance(series_type, bool) else None},
            **teams,
            "players": [{"isRadiant": slot < 128, "steamAccount": {"id": account},
                         "position": None} for account, slot in slots],
            "source": "opendota",
        }
    counts["written"] = len(maps)
    return maps, dict(counts)


def _load_excluded_ids(path: Path) -> set[int]:
    with path.open(encoding="utf-8") as fh:
        try:
            rows = json.load(fh)
        except json.JSONDecodeError:
            fh.seek(0)
            return {int(line.strip()) for line in fh if line.strip()}
        # A single newline-separated ID is also a valid JSON integer.
        if isinstance(rows, int) and not isinstance(rows, bool):
            return {rows}
        if not isinstance(rows, list):
            raise ValueError(f"{path}: expected a JSON list or newline-separated match IDs")
        return {int(match_id) for match_id in rows}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True, type=Path)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--exclude-corpus-ids", type=Path,
                        help="processed_ids.txt: JSON list (corpus format) or one match ID per line")
    args = parser.parse_args()
    if not args.input_dir.is_dir() or not list(args.input_dir.glob("listing_*.json")):
        parser.error("--input-dir must contain listing_*.json arrays")
    excluded_ids = set()
    if args.exclude_corpus_ids is not None:
        excluded_ids = _load_excluded_ids(args.exclude_corpus_ids)
    maps, counts = convert(args.input_dir, excluded_ids)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=args.output.parent,
                                         prefix=args.output.name + ".", suffix=".tmp", delete=False) as fh:
            temporary_path = fh.name
            json.dump(maps, fh, indent=2, sort_keys=True)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temporary_path, args.output)
    finally:
        if temporary_path is not None and os.path.exists(temporary_path):
            os.unlink(temporary_path)
    print(json.dumps(counts, sort_keys=True))


if __name__ == "__main__":
    main()
