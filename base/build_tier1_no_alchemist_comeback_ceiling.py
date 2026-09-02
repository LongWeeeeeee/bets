#!/usr/bin/env python3
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable

from id_to_names import tier_one_teams


ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = ROOT / "pro_heroes_data" / "json_parts_split_from_object"
OUTPUT_PATH = Path(__file__).resolve().with_name(
    "tier1_no_alchemist_comeback_ceiling_by_minute.json"
)
ALCHEMIST_ID = 73
MINUTE_START = 20


def _iter_tier1_ids() -> Iterable[int]:
    for value in tier_one_teams.values():
        if isinstance(value, int):
            yield value
        else:
            for item in value:
                yield int(item)


def build_payload() -> Dict[str, object]:
    tier1_ids = set(_iter_tier1_ids())
    thresholds_by_minute: Dict[str, int] = {}
    examples_by_minute: Dict[str, Dict[str, object]] = {}
    match_count = 0
    excluded_vs_alchemist = 0

    for path in sorted(SOURCE_DIR.glob("combined*.json")):
        with path.open("r", encoding="utf-8") as f:
            matches = json.load(f)
        for match_id, match in matches.items():
            radiant_team = match.get("radiantTeam") or {}
            dire_team = match.get("direTeam") or {}
            radiant_team_id = radiant_team.get("id")
            dire_team_id = dire_team.get("id")
            if radiant_team_id not in tier1_ids or dire_team_id not in tier1_ids:
                continue

            match_count += 1
            did_radiant_win = bool(match.get("didRadiantWin"))
            players = match.get("players") or []
            radiant_heroes = [p.get("heroId") for p in players if p.get("isRadiant")]
            dire_heroes = [p.get("heroId") for p in players if not p.get("isRadiant")]
            opponent_has_alchemist = (
                ALCHEMIST_ID in dire_heroes if did_radiant_win else ALCHEMIST_ID in radiant_heroes
            )
            if opponent_has_alchemist:
                excluded_vs_alchemist += 1
                continue

            leads = match.get("radiantNetworthLeads") or []
            for minute in range(MINUTE_START, len(leads)):
                deficit = -float(leads[minute]) if did_radiant_win else float(leads[minute])
                if deficit <= 0:
                    continue
                key = str(minute)
                if int(deficit) <= int(thresholds_by_minute.get(key, 0)):
                    continue
                thresholds_by_minute[key] = int(deficit)
                start_ts = match.get("startDateTime")
                examples_by_minute[key] = {
                    "match_id": str(match_id),
                    "radiant": radiant_team.get("name"),
                    "dire": dire_team.get("name"),
                    "winner": "radiant" if did_radiant_win else "dire",
                    "league": (match.get("league") or {}).get("name")
                    if isinstance(match.get("league"), dict)
                    else match.get("league"),
                    "startDateTime": datetime.fromtimestamp(
                        int(start_ts), tz=timezone.utc
                    ).isoformat().replace("+00:00", "Z")
                    if start_ts
                    else None,
                }

    minute_keys = sorted(int(k) for k in thresholds_by_minute)
    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "source_dir": str(SOURCE_DIR),
            "scope": "tier1_vs_tier1_eventual_winner_max_deficit_no_alchemist_opponent",
            "minute_start": MINUTE_START,
            "minute_end": max(minute_keys) if minute_keys else None,
            "matches_total": match_count,
            "excluded_vs_alchemist": excluded_vs_alchemist,
        },
        "thresholds_by_minute": {
            str(minute): int(thresholds_by_minute[str(minute)]) for minute in minute_keys
        },
        "examples_by_minute": {
            str(minute): examples_by_minute[str(minute)] for minute in minute_keys
        },
    }


def main() -> None:
    payload = build_payload()
    # Атомарно: прямая запись усечёт файл на месте, и обрыв оставит словарь неполным.
    tmp_path = OUTPUT_PATH.with_name(OUTPUT_PATH.name + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    tmp_path.replace(OUTPUT_PATH)
    print(f"Wrote {OUTPUT_PATH}")
    meta = payload.get("meta") or {}
    print(json.dumps(meta, ensure_ascii=False))


if __name__ == "__main__":
    main()
