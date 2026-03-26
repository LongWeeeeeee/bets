from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from ELO.domain import MatchRecord


def _extract_player_slots(players: list[dict], is_radiant: bool) -> tuple[tuple[int, ...], tuple[str | None, ...]]:
    player_slots: dict[int, str | None] = {}
    for player in players:
        if bool(player.get("isRadiant")) != is_radiant:
            continue
        steam_account = player.get("steamAccount") or {}
        account_id = steam_account.get("id")
        if isinstance(account_id, int):
            position = player.get("position")
            player_slots[account_id] = str(position) if position is not None else None
    ordered_slots = sorted(player_slots.items())
    return (
        tuple(player_id for player_id, _ in ordered_slots),
        tuple(position for _, position in ordered_slots),
    )


def _parse_match(raw_match: dict) -> MatchRecord | None:
    match_id = raw_match.get("id")
    timestamp = raw_match.get("startDateTime")
    if not isinstance(match_id, int) or not isinstance(timestamp, int):
        return None

    players = raw_match.get("players") or []
    if len(players) != 10:
        return None

    radiant_player_ids, radiant_player_positions = _extract_player_slots(players, is_radiant=True)
    dire_player_ids, dire_player_positions = _extract_player_slots(players, is_radiant=False)
    if len(radiant_player_ids) != 5 or len(dire_player_ids) != 5:
        return None

    radiant_team = raw_match.get("radiantTeam") or {}
    dire_team = raw_match.get("direTeam") or {}
    radiant_team_id = radiant_team.get("id")
    dire_team_id = dire_team.get("id")
    radiant_team_name = str(radiant_team.get("name") or "")
    dire_team_name = str(dire_team.get("name") or "")
    if radiant_team_id is None and not radiant_team_name:
        return None
    if dire_team_id is None and not dire_team_name:
        return None

    league = raw_match.get("league") or {}
    series = raw_match.get("series") or {}
    radiant_win = raw_match.get("didRadiantWin")
    if not isinstance(radiant_win, bool):
        return None

    return MatchRecord(
        match_id=match_id,
        timestamp=timestamp,
        radiant_win=radiant_win,
        radiant_team_id=radiant_team_id if isinstance(radiant_team_id, int) else None,
        radiant_team_name=radiant_team_name,
        dire_team_id=dire_team_id if isinstance(dire_team_id, int) else None,
        dire_team_name=dire_team_name,
        radiant_player_ids=radiant_player_ids,
        dire_player_ids=dire_player_ids,
        league_id=raw_match.get("leagueId") if isinstance(raw_match.get("leagueId"), int) else None,
        league_name=str(league.get("name") or ""),
        source_league_tier=str(league.get("tier")) if league.get("tier") is not None else None,
        series_id=series.get("id") if isinstance(series.get("id"), int) else None,
        series_type=str(series.get("type")) if series.get("type") is not None else None,
        radiant_player_positions=radiant_player_positions,
        dire_player_positions=dire_player_positions,
    )


def load_matches(data_dir: Path) -> tuple[list[MatchRecord], dict[str, int]]:
    summary: Counter[str] = Counter()
    matches: list[MatchRecord] = []
    for json_path in sorted(data_dir.glob("*.json")):
        summary["files"] += 1
        with json_path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if not isinstance(payload, dict):
            continue
        summary["raw_matches"] += len(payload)
        for raw_match in payload.values():
            summary["seen_matches"] += 1
            if not isinstance(raw_match, dict):
                summary["skipped_non_dict"] += 1
                continue
            match = _parse_match(raw_match)
            if match is None:
                summary["skipped_invalid"] += 1
                continue
            matches.append(match)
            summary["loaded_matches"] += 1
    matches.sort(key=lambda match: (match.timestamp, match.match_id))
    return matches, dict(summary)
