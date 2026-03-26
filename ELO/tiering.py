from __future__ import annotations

from collections import Counter, defaultdict

from base.id_to_names import tier_one_teams, tier_two_teams

from ELO.domain import LeagueTier, LeagueTierInfo, MatchRecord
from ELO.team_identity import resolve_org_key


def _extract_ids(mapping: dict[str, int | set[int]]) -> set[int]:
    team_ids: set[int] = set()
    for value in mapping.values():
        if isinstance(value, int):
            team_ids.add(value)
        elif isinstance(value, set):
            team_ids.update(int(item) for item in value if isinstance(item, int))
    return team_ids


KNOWN_TIER1_IDS = _extract_ids(tier_one_teams)
KNOWN_TIER2_IDS = _extract_ids(tier_two_teams)


def _sample_team_id(raw_team_ids: int | set[int]) -> int | None:
    if isinstance(raw_team_ids, int):
        return raw_team_ids
    if isinstance(raw_team_ids, set):
        normalized = sorted(int(item) for item in raw_team_ids if isinstance(item, int))
        if normalized:
            return normalized[0]
    return None


def _build_known_org_keys(mapping: dict[str, int | set[int]]) -> set[str]:
    org_keys: set[str] = set()
    for alias, raw_team_ids in mapping.items():
        sample_team_id = _sample_team_id(raw_team_ids)
        org_keys.add(resolve_org_key(sample_team_id, alias))
    return org_keys


KNOWN_TIER1_ORG_KEYS = _build_known_org_keys(tier_one_teams)
KNOWN_TIER2_ORG_KEYS = _build_known_org_keys(tier_two_teams)


def get_known_team_tier(team_id: int | None, team_name: str | None = None) -> LeagueTier | None:
    if team_id in KNOWN_TIER1_IDS:
        return LeagueTier.TIER1
    if team_id in KNOWN_TIER2_IDS:
        return LeagueTier.TIER2
    org_key = resolve_org_key(team_id, str(team_name or ""))
    if org_key in KNOWN_TIER1_ORG_KEYS:
        return LeagueTier.TIER1
    if org_key in KNOWN_TIER2_ORG_KEYS:
        return LeagueTier.TIER2
    return None


def _source_tier_fallback(source_tier: str | None) -> LeagueTier:
    if source_tier == "PREMIUM":
        return LeagueTier.TIER1
    if source_tier == "PROFESSIONAL":
        return LeagueTier.TIER2
    return LeagueTier.TIER3


def _derive_league_tier_from_teams(teams: set[int], source_tier: str | None) -> LeagueTier:
    team_count = len(teams)
    tier1_team_count = sum(1 for team_id in teams if team_id in KNOWN_TIER1_IDS)
    tier12_team_count = sum(
        1 for team_id in teams if team_id in KNOWN_TIER1_IDS or team_id in KNOWN_TIER2_IDS
    )
    tier1_share = tier1_team_count / team_count if team_count else 0.0
    tier12_share = tier12_team_count / team_count if team_count else 0.0
    if team_count and tier1_share >= 0.60:
        return LeagueTier.TIER1
    if team_count and tier12_share >= 0.60:
        return LeagueTier.TIER2
    return _source_tier_fallback(source_tier)


def classify_leagues(matches: list[MatchRecord]) -> tuple[dict[int, LeagueTierInfo], dict[str, int]]:
    league_teams: dict[int, set[int]] = defaultdict(set)
    league_names: dict[int, str] = {}
    source_tiers: dict[int, str | None] = {}
    for match in matches:
        if match.league_id is None:
            continue
        if match.radiant_team_id is not None:
            league_teams[match.league_id].add(match.radiant_team_id)
        if match.dire_team_id is not None:
            league_teams[match.league_id].add(match.dire_team_id)
        league_names[match.league_id] = match.league_name
        source_tiers[match.league_id] = match.source_league_tier

    league_info: dict[int, LeagueTierInfo] = {}
    summary: Counter[str] = Counter()
    for league_id, teams in league_teams.items():
        team_count = len(teams)
        tier1_team_count = sum(1 for team_id in teams if team_id in KNOWN_TIER1_IDS)
        tier12_team_count = sum(
            1 for team_id in teams if team_id in KNOWN_TIER1_IDS or team_id in KNOWN_TIER2_IDS
        )
        tier1_share = tier1_team_count / team_count if team_count else 0.0
        tier12_share = tier12_team_count / team_count if team_count else 0.0
        derived_tier = _derive_league_tier_from_teams(teams, source_tiers.get(league_id))
        summary[derived_tier.value] += 1
        league_info[league_id] = LeagueTierInfo(
            league_id=league_id,
            league_name=league_names.get(league_id, ""),
            source_tier=source_tiers.get(league_id),
            team_count=team_count,
            tier1_team_count=tier1_team_count,
            tier12_team_count=tier12_team_count,
            tier1_share=tier1_share,
            tier12_share=tier12_share,
            derived_tier=derived_tier,
            teams=sorted(teams),
        )
    return league_info, dict(summary)


def attach_league_tiers(matches: list[MatchRecord], league_info: dict[int, LeagueTierInfo]) -> None:
    for match in matches:
        if match.league_id is None:
            match.derived_league_tier = _source_tier_fallback(match.source_league_tier)
            continue
        info = league_info.get(match.league_id)
        if info is None:
            match.derived_league_tier = _source_tier_fallback(match.source_league_tier)
            continue
        match.derived_league_tier = info.derived_tier


def attach_league_tiers_time_aware(
    matches: list[MatchRecord],
    *,
    include_current_match_teams: bool = True,
) -> dict[str, int]:
    seen_teams_by_league: dict[int, set[int]] = defaultdict(set)
    summary: Counter[str] = Counter()
    for match in sorted(matches, key=lambda item: (item.timestamp, item.match_id)):
        if match.league_id is None:
            match.derived_league_tier = _source_tier_fallback(match.source_league_tier)
            summary[match.derived_league_tier.value] += 1
            continue

        league_teams = set(seen_teams_by_league[match.league_id])
        if include_current_match_teams:
            if match.radiant_team_id is not None:
                league_teams.add(match.radiant_team_id)
            if match.dire_team_id is not None:
                league_teams.add(match.dire_team_id)
        match.derived_league_tier = _derive_league_tier_from_teams(league_teams, match.source_league_tier)
        summary[match.derived_league_tier.value] += 1

        if match.radiant_team_id is not None:
            seen_teams_by_league[match.league_id].add(match.radiant_team_id)
        if match.dire_team_id is not None:
            seen_teams_by_league[match.league_id].add(match.dire_team_id)

    return dict(summary)
