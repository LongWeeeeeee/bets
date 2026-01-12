"""
Team Tier Classification based on curated lists.

Tier 1: Top pro teams (Spirit, Liquid, Falcons, etc.)
Tier 2: Strong teams (Secret, Alliance, etc.)
Tier 3: Everyone else
"""

from typing import Dict, Set, Union
import sys
from pathlib import Path

# Add base directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from base.id_to_names import tier_one_teams, tier_two_teams


def _build_tier_lookup() -> Dict[int, int]:
    """
    Builds a lookup dict: team_id -> tier (1, 2, or 3).
    
    Handles both single IDs and sets of IDs (for rebranded teams).
    """
    lookup: Dict[int, int] = {}
    
    # Process Tier 1 teams
    for team_name, team_ids in tier_one_teams.items():
        if isinstance(team_ids, set):
            for tid in team_ids:
                lookup[tid] = 1
        else:
            lookup[team_ids] = 1
    
    # Process Tier 2 teams
    for team_name, team_ids in tier_two_teams.items():
        if isinstance(team_ids, set):
            for tid in team_ids:
                if tid not in lookup:  # Don't override Tier 1
                    lookup[tid] = 2
        else:
            if team_ids not in lookup:
                lookup[team_ids] = 2
    
    return lookup


# Pre-build lookup for fast access
_TIER_LOOKUP = _build_tier_lookup()


def get_team_tier(team_id: int) -> int:
    """
    Returns team tier (1, 2, or 3) based on curated lists.
    
    Args:
        team_id: Stratz team ID
        
    Returns:
        1 for Tier 1 (top pro teams)
        2 for Tier 2 (strong teams)
        3 for Tier 3 (everyone else)
    """
    return _TIER_LOOKUP.get(team_id, 3)


def get_match_tier_score(radiant_team_id: int, dire_team_id: int) -> float:
    """
    Calculates match quality score based on team tiers.
    
    Returns:
        1.0 = Elite match (both Tier 1)
        1.5 = David vs Goliath (Tier 1 vs Tier 2)
        2.0 = Solid Tier 2 match
        2.5 = Mixed quality
        3.0 = Lower tier match
    """
    radiant_tier = get_team_tier(radiant_team_id)
    dire_tier = get_team_tier(dire_team_id)
    return (radiant_tier + dire_tier) / 2


# Stats for debugging
if __name__ == '__main__':
    print(f"Tier 1 teams: {sum(1 for v in _TIER_LOOKUP.values() if v == 1)}")
    print(f"Tier 2 teams: {sum(1 for v in _TIER_LOOKUP.values() if v == 2)}")
    print(f"Total tracked: {len(_TIER_LOOKUP)}")
    
    # Test some known teams
    test_ids = [
        (7119388, "Spirit"),
        (2163, "Liquid"),
        (1838315, "Secret"),
        (111474, "Alliance"),
        (12345, "Unknown"),
    ]
    for tid, name in test_ids:
        print(f"  {name} ({tid}): Tier {get_team_tier(tid)}")
