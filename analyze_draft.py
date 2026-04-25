#!/usr/bin/env python3
"""
Parse a draft matchup using dota2protracker.com.
Shows detailed counterpick (cp1vs1) and synergy analysis.

Radiant: pos1 Lone Druid, pos2 Puck, pos3 Underlord, pos4 Muerta, pos5 Treant Protector
Dire: pos1 Ursa, pos2 Night Stalker, pos3 Sand King, pos4 Shadow Demon, pos5 Warlock
"""

import json
import time
import sys
sys.path.insert(0, '.')

from base.dota2protracker import (
    parse_hero_matchups, _calculate_cp1vs1, _calculate_duo_synergy,
    CORE_POSITIONS, PRO_CP1VS1_PAIR_WEIGHTS, PRO_EARLY_POSITION_WEIGHTS,
    MIN_GAMES_THRESHOLD, get_hero_id, get_hero_name
)

# Draft composition
radiant_heroes_and_pos = {
    'pos1': {'hero_name': 'Lone Druid'},
    'pos2': {'hero_name': 'Puck'},
    'pos3': {'hero_name': 'Underlord'},
    'pos4': {'hero_name': 'Muerta'},
    'pos5': {'hero_name': 'Treant Protector'},
}

dire_heroes_and_pos = {
    'pos1': {'hero_name': 'Ursa'},
    'pos2': {'hero_name': 'Night Stalker'},
    'pos3': {'hero_name': 'Sand King'},
    'pos4': {'hero_name': 'Shadow Demon'},
    'pos5': {'hero_name': 'Warlock'},
}


def normalize_cache_key(name: str) -> str:
    """Convert hero name to cache key format."""
    return name.lower().replace(' ', '_')


def find_in_dict(d: dict, key: str):
    """Find key in dict, trying various formats."""
    # Direct
    if key in d:
        return d[key]
    # With underscore
    underscore = key.lower().replace(' ', '_')
    if underscore in d:
        return d[underscore]
    # Title case
    title = key.title()
    if title in d:
        return d[title]
    # Title with underscore
    title_us = key.title().replace(' ', '_')
    if title_us in d:
        return d[title_us]
    return None


def get_matchup_data(hero_data: dict, hero: str, opponent: str, hero_pos: str, opp_pos: str) -> dict:
    """
    Get matchup data between two heroes at specific positions.
    hero: our hero (e.g., 'lone_druid')
    opponent: opponent hero (e.g., 'sand king')
    hero_pos: our hero's position ('1', '2', '3')
    opp_pos: opponent's position ('1', '2', '3')
    Returns: winrate when OUR hero plays at hero_pos vs OPPONENT at opp_pos.
    """
    # Try new format first (_matchups_by_hero_pos)
    # Structure: {opponent: {opp_pos: {hero_pos: {wr, games, wins}}}}
    h_data = hero_data.get(hero, {})
    new_format = h_data.get('_matchups_by_hero_pos', {})

    opp_key = opponent.lower().replace(' ', '_')
    if opp_key in new_format:
        opp_data = new_format[opp_key]
        if opp_pos in opp_data:
            hero_pos_data = opp_data[opp_pos]
            if hero_pos in hero_pos_data:
                pos_data = hero_pos_data[hero_pos]
                if pos_data.get('games', 0) >= MIN_GAMES_THRESHOLD:
                    return {
                        'wr': pos_data['wr'],
                        'games': pos_data['games'],
                        'wins': pos_data.get('wins', 0),
                        'diff': pos_data['wr'] - 50
                    }

    return None


def parse_draft(radiant: dict, dire: dict):
    """Parse all heroes in draft and return hero_data dict."""
    print("="*80)
    print("PARSING DRAFT HEROES FROM DOTA2PROTRACKER")
    print("="*80)

    all_heroes = set()
    for pos in CORE_POSITIONS:
        if radiant.get(pos, {}).get('hero_name'):
            all_heroes.add(normalize_cache_key(radiant[pos]['hero_name']))
        if dire.get(pos, {}).get('hero_name'):
            all_heroes.add(normalize_cache_key(dire[pos]['hero_name']))

    print(f"\nHeroes to parse: {len(all_heroes)}")
    for h in sorted(all_heroes):
        print(f"  - {h}")
    print()

    hero_data = {}
    for hero_name in sorted(all_heroes):
        print(f"Fetching {hero_name}...")
        # Try to parse using the lowercase name
        display_name = hero_name.replace('_', ' ').title()

        data = parse_hero_matchups(display_name, use_cache=True)
        if 'error' not in data or 'Unknown hero' not in data.get('error', ''):
            hero_data[hero_name] = data
        else:
            # Try all lowercase
            data = parse_hero_matchups(display_name.lower(), use_cache=True)
            hero_data[hero_name] = data
        time.sleep(1)

    return hero_data


def analyze_cp1vs1_detailed(radiant: list, dire: list, hero_data: dict):
    """Detailed counterpick analysis."""
    print("\n" + "="*80)
    print("COUNTERPICK ANALYSIS (cp1vs1)")
    print("="*80)
    print("\nRadiant cores vs Dire cores matchups:")
    print("-" * 80)

    total_scores = []
    valid_count = 0

    for r_idx, r_hero in enumerate(radiant):
        r_pos = CORE_POSITIONS[r_idx]
        r_cache_key = normalize_cache_key(r_hero)

        for d_idx, d_hero in enumerate(dire):
            d_pos = CORE_POSITIONS[d_idx]
            d_cache_key = normalize_cache_key(d_hero)
            pair_weight = PRO_CP1VS1_PAIR_WEIGHTS.get((r_pos, d_pos), 1.0)

            # The position number of the opponent (1, 2, 3)
            d_pos_num = str(d_idx + 1)

            # R vs D: how does Radiant hero do against Dire hero at position d_pos_num?
            r_pos_num = str(r_idx + 1)
            d_pos_num = str(d_idx + 1)
            data = get_matchup_data(hero_data, r_cache_key, d_hero, r_pos_num, d_pos_num)

            if data:
                print(f"\n{r_hero} (pos{r_idx+1}) vs {d_hero} (pos{d_idx+1})")
                print(f"  Win Rate: {data['wr']:.1f}%  |  Games: {data['games']}  |  Diff: {data['diff']:+.1f}%")
                print(f"  Weight: {pair_weight}x  |  Score: {data['diff'] * pair_weight:+.2f}")
                total_scores.append(data['diff'] * pair_weight)
                valid_count += 1
            else:
                # Try reverse: how does Dire hero do against Radiant hero at r_pos?
                d_pos = str(d_idx + 1)
                rev_data = get_matchup_data(hero_data, d_cache_key, r_hero, d_pos, r_pos)
                if rev_data:
                    # Invert the WR
                    inv_wr = 100 - rev_data['wr']
                    inv_diff = inv_wr - 50
                    print(f"\n{r_hero} (pos{r_idx+1}) vs {d_hero} (pos{d_idx+1}) [INVERTED from {d_hero} perspective]")
                    print(f"  Win Rate: {inv_wr:.1f}%  |  Games: {rev_data['games']}  |  Diff: {inv_diff:+.1f}%")
                    print(f"  Weight: {pair_weight}x  |  Score: {inv_diff * pair_weight:+.2f}")
                    total_scores.append(inv_diff * pair_weight)
                    valid_count += 1
                else:
                    print(f"\n{r_hero} (pos{r_idx+1}) vs {d_hero} (pos{d_idx+1})")
                    print(f"  ⚠️ No data (need 10+ games)")

    if total_scores:
        avg_score = sum(total_scores) / len(total_scores)
        print(f"\n{'='*80}")
        print(f"📊 OVERALL CP1VS1 SCORE: {avg_score:+.2f}% (based on {valid_count} matchups)")
        if avg_score > 2:
            print("🏆 RADIANT FAVORED in counterpicks")
        elif avg_score < -2:
            print("🏆 DIRE FAVORED in counterpicks")
        else:
            print("⚖️ EVEN counterpick matchup")

    return total_scores


def analyze_duo_synergy_detailed(heroes: list, team_name: str, hero_data: dict):
    """Detailed duo synergy analysis."""
    print("\n" + "="*80)
    print(f"DUO SYNERGY ANALYSIS ({team_name})")
    print("="*80)

    from itertools import combinations

    synergy_scores = []
    valid_count = 0

    for h1, h2 in combinations(heroes, 2):
        h1_key = normalize_cache_key(h1)
        h2_key = normalize_cache_key(h2)

        print(f"\n{h1} + {h2}:")

        found = False
        # How does h1 synergize with h2?
        h1_matchups = hero_data.get(h1_key, {}).get('matchups', {})
        h2_data = find_in_dict(h1_matchups, h2)

        if h2_data:
            for pos in ['1', '2', '3', '4', '5']:
                pos_data = h2_data.get(pos)
                if pos_data and pos_data.get('games', 0) >= MIN_GAMES_THRESHOLD:
                    wr = pos_data['wr']
                    diff = wr - 50
                    print(f"  {h1} + {h2} at {pos}: {wr:.1f}% WR, {pos_data['games']} games, {diff:+.1f}% diff")
                    synergy_scores.append(diff)
                    valid_count += 1
                    found = True

        # How does h2 synergize with h1?
        h2_matchups = hero_data.get(h2_key, {}).get('matchups', {})
        h1_data = find_in_dict(h2_matchups, h1)

        if h1_data:
            for pos in ['1', '2', '3', '4', '5']:
                pos_data = h1_data.get(pos)
                if pos_data and pos_data.get('games', 0) >= MIN_GAMES_THRESHOLD:
                    wr = pos_data['wr']
                    diff = wr - 50
                    print(f"  {h2} + {h1} at {pos}: {wr:.1f}% WR, {pos_data['games']} games, {diff:+.1f}% diff")
                    if not found:  # Don't double count
                        synergy_scores.append(diff)
                        valid_count += 1
                    found = True

        if not found:
            print(f"  ⚠️ No synergy data (need 10+ games)")

    if synergy_scores:
        avg_synergy = sum(synergy_scores) / len(synergy_scores)
        print(f"\n{'='*80}")
        print(f"📊 {team_name} DUO SYNERGY: {avg_synergy:+.2f}% (based on {valid_count} pairs)")

    return synergy_scores


def main():
    print("DOTA2PROTRACKER DRAFT ANALYSIS")
    print("="*80)
    print("\nRadiant:")
    for pos in CORE_POSITIONS:
        hero = radiant_heroes_and_pos[pos]['hero_name']
        print(f"  {pos}: {hero}")

    print("\nDire:")
    for pos in CORE_POSITIONS:
        hero = dire_heroes_and_pos[pos]['hero_name']
        print(f"  {pos}: {hero}")

    # Extract core heroes
    radiant_cores = [radiant_heroes_and_pos[p]['hero_name'] for p in CORE_POSITIONS]
    dire_cores = [dire_heroes_and_pos[p]['hero_name'] for p in CORE_POSITIONS]

    # Parse all heroes
    hero_data = parse_draft(radiant_heroes_and_pos, dire_heroes_and_pos)

    # Analyze cp1vs1
    cp1vs1_scores = analyze_cp1vs1_detailed(radiant_cores, dire_cores, hero_data)

    # Analyze duo synergy for each team
    radiant_synergy = analyze_duo_synergy_detailed(radiant_cores, "Radiant", hero_data)
    dire_synergy = analyze_duo_synergy_detailed(dire_cores, "Dire", hero_data)

    # Final summary
    print("\n" + "="*80)
    print("FINAL SUMMARY")
    print("="*80)

    if cp1vs1_scores:
        cp_score = sum(cp1vs1_scores) / len(cp1vs1_scores)
        print(f"\nCounterpick Score: {cp_score:+.2f}%")

    if radiant_synergy and dire_synergy:
        r_avg = sum(radiant_synergy) / len(radiant_synergy)
        d_avg = sum(dire_synergy) / len(dire_synergy)
        synergy_diff = r_avg - d_avg
        print(f"Radiant Synergy: {r_avg:+.2f}%")
        print(f"Dire Synergy: {d_avg:+.2f}%")
        print(f"Synergy Difference: {synergy_diff:+.2f}%")

        if synergy_diff > 2:
            print("\n🏆 RADIANT has better duo synergy")
        elif synergy_diff < -2:
            print("\n🏆 DIRE has better duo synergy")
        else:
            print("\n⚖️ Synergy is roughly equal")

    print("\n✅ Analysis complete!")


if __name__ == '__main__':
    main()
