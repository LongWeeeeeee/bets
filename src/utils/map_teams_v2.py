"""
Team name to ID mapping utility.

Uses the authoritative team data from base/id_to_names.py.
Provides normalized lookup for team names -> IDs and tier classification.
"""

import sys
from pathlib import Path
from typing import Dict, Optional, Set, Tuple, Union

# Add base to path for import
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'base'))

from id_to_names import tier_one_teams, tier_two_teams


def _normalize_name(name: str) -> str:
    """Normalize team name for lookup."""
    return name.lower().replace(' ', '').replace('.', '').replace('-', '').replace('_', '')


def _flatten_id(value: Union[int, Set[int]]) -> int:
    """Extract single ID from value (handles sets)."""
    if isinstance(value, set):
        return max(value)  # Return highest ID (usually most recent)
    return value


# Build unified lookup dictionaries
TEAM_NAME_TO_ID: Dict[str, int] = {}
TIER_ONE_IDS: Set[int] = set()
TIER_TWO_IDS: Set[int] = set()

# Process tier one teams
for name, team_id in tier_one_teams.items():
    normalized = _normalize_name(name)
    flat_id = _flatten_id(team_id)
    TEAM_NAME_TO_ID[normalized] = flat_id
    TIER_ONE_IDS.add(flat_id)

# Process tier two teams
for name, team_id in tier_two_teams.items():
    normalized = _normalize_name(name)
    flat_id = _flatten_id(team_id)
    TEAM_NAME_TO_ID[normalized] = flat_id
    TIER_TWO_IDS.add(flat_id)

# Add common aliases
ALIASES: Dict[str, str] = {
    'teamspirit': 'spirit',
    'ts': 'spirit',
    'gg': 'gaimingladiators',
    'gaimin': 'gaimingladiators',
    'gladiators': 'gaimingladiators',
    'navi': 'natusvincere',
    'vp': 'virtuspro',
    'virtus': 'virtuspro',
    'tl': 'liquid',
    'teamliquid': 'liquid',
    'falcons': 'falcons',
    'teamfalcons': 'falcons',
    'betboomteam': 'betboom',
    'bbt': 'betboom',
    'tundraesports': 'tundra',
    'heroicesports': 'heroic',
    'auroragaming': 'aurora',
    'xtremegaming': 'xtreme',
    'xg': 'xtreme',
    'ar': 'azureray',
    'lgd': 'lgdgaming',
    'psg': 'psgquest',
    'quest': 'psgquest',
    'nigma': 'nigmagalaxy',
    'ng': 'nigmagalaxy',
    'sr': 'shopifyrebellion',
    'shopify': 'shopifyrebellion',
    'rebellion': 'shopifyrebellion',
    'parivision': 'parivision',
    'pv': 'parivision',
    '1w': '1win',
    'onewin': '1win',
    'pandas': '9pandas',
    '9p': '9pandas',
    'bc': 'beastcoast',
    'beastcoast': 'beastcoast',
    'entity': 'entity',
    'talon': 'talon',
    'boom': 'boom',
    'boomesports': 'boom',
    'vici': 'vici',
    'vicigaming': 'vici',
    'vg': 'vici',
    'ig': 'invictus',
    'invictusgaming': 'invictus',
    'secretteam': 'secret',
    'teamsecret': 'secret',
}

# Add aliases to lookup
for alias, canonical in ALIASES.items():
    if canonical in TEAM_NAME_TO_ID:
        TEAM_NAME_TO_ID[alias] = TEAM_NAME_TO_ID[canonical]


def get_team_id(team_name: str) -> Optional[int]:
    """
    Get team ID from name.
    
    Args:
        team_name: Team name (case-insensitive, spaces/dots ignored)
        
    Returns:
        Team ID or None if not found
    """
    if not team_name:
        return None
    normalized = _normalize_name(team_name)
    return TEAM_NAME_TO_ID.get(normalized)


def get_team_tier(team_id: int) -> int:
    """
    Get team tier (1, 2, or 3 for unknown).
    
    Args:
        team_id: Team ID
        
    Returns:
        1 for tier one, 2 for tier two, 3 for unknown/rest
    """
    if team_id in TIER_ONE_IDS:
        return 1
    elif team_id in TIER_TWO_IDS:
        return 2
    return 3


def is_tier_one_team(team_id: int) -> bool:
    """Check if team is tier one."""
    return team_id in TIER_ONE_IDS


def is_tier_two_team(team_id: int) -> bool:
    """Check if team is tier two."""
    return team_id in TIER_TWO_IDS


def get_match_tier_info(
    radiant_team_id: Optional[int],
    dire_team_id: Optional[int],
) -> Tuple[bool, bool, bool]:
    """
    Get tier classification for a match.
    
    Returns:
        Tuple of (is_tier_one_match, is_tier_two_match, is_mismatch)
    """
    r_tier = get_team_tier(radiant_team_id) if radiant_team_id else 3
    d_tier = get_team_tier(dire_team_id) if dire_team_id else 3
    
    is_tier_one = r_tier == 1 and d_tier == 1
    is_tier_two = r_tier == 2 and d_tier == 2
    is_mismatch = abs(r_tier - d_tier) >= 1 and r_tier <= 2 and d_tier <= 2
    
    return is_tier_one, is_tier_two, is_mismatch


if __name__ == '__main__':
    # Test lookups
    test_names = ['MOUZ', 'Team Spirit', 'GG', 'Gaimin Gladiators', 'Liquid', 
                  'Falcons', 'BetBoom', 'Secret', 'Navi', 'VP']
    
    print("Team Name Lookup Test:")
    print("=" * 50)
    for name in test_names:
        team_id = get_team_id(name)
        tier = get_team_tier(team_id) if team_id else 'N/A'
        print(f"  {name:20} -> ID: {team_id}, Tier: {tier}")
    
    print(f"\nTotal teams mapped: {len(TEAM_NAME_TO_ID)}")
    print(f"Tier 1 teams: {len(TIER_ONE_IDS)}")
    print(f"Tier 2 teams: {len(TIER_TWO_IDS)}")
