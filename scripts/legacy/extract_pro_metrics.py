#!/usr/bin/env python3
"""
Extract pro matches from April 9, 2026 onwards and enrich with dota2protracker metrics.
"""

import json
import os
import time
import glob

# April 9, 2026 00:00:00 UTC
CUTOFF_TIMESTAMP = 1775682000

# Import dota2protracker functions
import sys
sys.path.insert(0, 'base')
from dota2protracker import enrich_with_pro_tracker, get_hero_name, parse_hero_matchups

def load_pro_matches():
    """Load all pro matches from combined*.json files."""
    matches = {}
    files = sorted(glob.glob('pro_heroes_data/json_parts_split_from_object/combined*.json'))

    for filepath in files:
        print(f"Loading {filepath}...")
        with open(filepath, 'r') as f:
            data = json.load(f)
        for match_id, match_data in data.items():
            # Filter by date
            start_time = match_data.get('startDateTime', 0)
            if start_time < CUTOFF_TIMESTAMP:
                continue

            # Skip if no players or teams
            players = match_data.get('players', [])
            if not players:
                continue

            radiant_team = match_data.get('radiantTeam', {})
            dire_team = match_data.get('direTeam', {})
            if not radiant_team or not dire_team:
                continue

            matches[match_id] = match_data

    print(f"Loaded {len(matches)} matches from April 9, 2026 onwards")
    return matches


def extract_draft(match_data):
    """Extract draft (heroes by position) from match data."""
    radiant_heroes_and_pos = {}
    dire_heroes_and_pos = {}

    POSITION_MAP = {
        'POSITION_1': 'pos1',
        'POSITION_2': 'pos2',
        'POSITION_3': 'pos3',
        'POSITION_4': 'pos4',
        'POSITION_5': 'pos5',
    }

    players = match_data.get('players', [])
    for player in players:
        hero_id = player.get('heroId', 0)
        if not hero_id:
            continue

        position = player.get('position', '')
        pos_key = POSITION_MAP.get(position)
        if not pos_key:
            continue

        hero_name = get_hero_name(hero_id)
        player_data = {
            'hero_id': hero_id,
            'hero_name': hero_name,
        }

        if player.get('isRadiant', False):
            radiant_heroes_and_pos[pos_key] = player_data
        else:
            dire_heroes_and_pos[pos_key] = player_data

    return radiant_heroes_and_pos, dire_heroes_and_pos


def main():
    print("Loading pro matches...")
    matches = load_pro_matches()

    results = []
    match_ids = list(matches.keys())

    for i, match_id in enumerate(match_ids):
        match_data = matches[match_id]
        print(f"\n[{i+1}/{len(match_ids)}] Processing match {match_id}...")

        radiant_team = match_data.get('radiantTeam', {})
        dire_team = match_data.get('direTeam', {})
        radiant_name = radiant_team.get('name', 'Unknown')
        dire_name = dire_team.get('name', 'Unknown')
        winner = 'radiant' if match_data.get('didRadiantWin', False) else 'dire'
        start_time = match_data.get('startDateTime', 0)

        # Extract draft
        radiant_heroes, dire_heroes = extract_draft(match_data)

        if len(radiant_heroes) < 3 or len(dire_heroes) < 3:
            print(f"  ⚠️ Skipping: insufficient heroes ({len(radiant_heroes)}/5 radiant, {len(dire_heroes)}/5 dire)")
            continue

        # Enrich with dota2protracker
        synergy_dict = {}
        try:
            enriched = enrich_with_pro_tracker(
                radiant_heroes, dire_heroes, synergy_dict, min_games=10
            )
        except Exception as e:
            print(f"  ⚠️ Error: {e}")
            continue

        result = {
            'map_id': match_id,
            'start_time': start_time,
            'radiant_team': radiant_name,
            'dire_team': dire_name,
            'winner': winner,
            'draft': {
                'radiant': {pos: data['hero_name'] for pos, data in radiant_heroes.items()},
                'dire': {pos: data['hero_name'] for pos, data in dire_heroes.items()},
            },
            'cp1vs1': enriched.get('pro_cp1vs1_late', 0),
            'cp1vs1_valid': enriched.get('pro_cp1vs1_valid', False),
            'synergy_duo': enriched.get('pro_duo_synergy_late', 0),
            'synergy_duo_valid': enriched.get('pro_duo_synergy_valid', False),
        }

        results.append(result)

        print(f"  ✅ {radiant_name} vs {dire_name} | winner={winner}")
        print(f"     cp1vs1: {result['cp1vs1']:+.2f} (valid={result['cp1vs1_valid']})")
        print(f"     synergy_duo: {result['synergy_duo']:+.2f} (valid={result['synergy_duo_valid']})")

        # Rate limit to avoid overwhelming dota2protracker
        time.sleep(2)

    # Save results
    output_file = 'pro_matches_with_metrics.json'
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\n\n✅ Saved {len(results)} matches to {output_file}")


if __name__ == '__main__':
    main()
