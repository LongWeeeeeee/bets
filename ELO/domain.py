from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class LeagueTier(str, Enum):
    TIER1 = "TIER1"
    TIER2 = "TIER2"
    TIER3 = "TIER3"


@dataclass
class MatchRecord:
    match_id: int
    timestamp: int
    radiant_win: bool
    radiant_team_id: int | None
    radiant_team_name: str
    dire_team_id: int | None
    dire_team_name: str
    radiant_player_ids: tuple[int, ...]
    dire_player_ids: tuple[int, ...]
    league_id: int | None
    league_name: str
    source_league_tier: str | None
    series_id: int | None
    series_type: str | None
    radiant_player_positions: tuple[str | None, ...] = field(default_factory=tuple)
    dire_player_positions: tuple[str | None, ...] = field(default_factory=tuple)
    derived_league_tier: LeagueTier = LeagueTier.TIER3


@dataclass
class SeriesRecord:
    series_id: int
    start_timestamp: int
    series_type: str
    best_of: int
    team_a_id: int | None
    team_a_name: str
    team_b_id: int | None
    team_b_name: str
    team_a_player_ids: tuple[int, ...]
    team_b_player_ids: tuple[int, ...]
    team_a_won: bool | None
    team_a_map_wins: int
    team_b_map_wins: int
    league_id: int | None
    league_name: str
    source_league_tier: str | None
    derived_league_tier: LeagueTier
    eligible_for_winner_target: bool
    skip_reason: str | None = None


@dataclass
class SeriesBundle:
    series: SeriesRecord
    deciding_maps: tuple[MatchRecord, ...]
    all_maps: tuple[MatchRecord, ...]


@dataclass
class LeagueTierInfo:
    league_id: int
    league_name: str
    source_tier: str | None
    team_count: int
    tier1_team_count: int
    tier12_team_count: int
    tier1_share: float
    tier12_share: float
    derived_tier: LeagueTier
    teams: list[int] = field(default_factory=list)


@dataclass
class StepResult:
    p_radiant: float
    radiant_strength: float
    dire_strength: float
    metadata: dict[str, Any] = field(default_factory=dict)
