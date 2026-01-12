#!/usr/bin/env python3
"""
Build draft execution stats for team rosters.

Tracks how teams perform when they have draft advantage vs disadvantage.
Requires at least 3 matching players to consider it the "same roster".
"""

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MIN_ROSTER_OVERLAP = 3  # Minimum players to match for "same roster"
MIN_GAMES_FOR_STATS = 5  # Minimum games to compute stats


def get_roster_key(player_ids: List[int]) -> Tuple[int, ...]:
    """Create a sorted tuple of player IDs as roster key."""
    valid_ids = [int(p) for p in player_ids if pd.notna(p) and p > 0]
    return tuple(sorted(valid_ids))


def rosters_match(roster1: Tuple[int, ...], roster2: Tuple[int, ...], min_overlap: int = 3) -> bool:
    """Check if two rosters have at least min_overlap players in common."""
    if not roster1 or not roster2:
        return False
    overlap = len(set(roster1) & set(roster2))
    return overlap >= min_overlap


def compute_draft_execution_stats(df: pd.DataFrame) -> Dict[str, Dict]:
    """
    Compute draft execution stats for each roster.
    
    Returns dict with roster_key -> {
        'games_with_adv': int,
        'wins_with_adv': int,
        'winrate_with_adv': float,
        'games_with_disadv': int,
        'wins_with_disadv': int,
        'winrate_with_disadv': float,
        'execution_score': float,  # How well they convert draft advantage
        'resilience_score': float,  # How well they play from behind
    }
    """
    # Sort by match_id for time-based processing
    df = df.sort_values('match_id').reset_index(drop=True)
    
    # Track stats per roster
    roster_stats: Dict[Tuple[int, ...], Dict] = defaultdict(lambda: {
        'games_with_adv': 0,
        'wins_with_adv': 0,
        'games_with_disadv': 0,
        'wins_with_disadv': 0,
    })
    
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Computing draft execution"):
        # Get rosters
        radiant_roster = get_roster_key([
            row.get('radiant_player_1_id'),
            row.get('radiant_player_2_id'),
            row.get('radiant_player_3_id'),
            row.get('radiant_player_4_id'),
            row.get('radiant_player_5_id'),
        ])
        dire_roster = get_roster_key([
            row.get('dire_player_1_id'),
            row.get('dire_player_2_id'),
            row.get('dire_player_3_id'),
            row.get('dire_player_4_id'),
            row.get('dire_player_5_id'),
        ])
        
        if len(radiant_roster) < MIN_ROSTER_OVERLAP or len(dire_roster) < MIN_ROSTER_OVERLAP:
            continue
        
        # Get draft advantage (use early + late average)
        r_draft_adv = (row.get('radiant_draft_adv_early', 0) + row.get('radiant_draft_adv_late', 0)) / 2
        d_draft_adv = (row.get('dire_draft_adv_early', 0) + row.get('dire_draft_adv_late', 0)) / 2
        
        radiant_win = row.get('radiant_win', 0)
        
        # Update radiant roster stats
        if r_draft_adv > 0:  # Radiant has draft advantage
            roster_stats[radiant_roster]['games_with_adv'] += 1
            if radiant_win:
                roster_stats[radiant_roster]['wins_with_adv'] += 1
        elif r_draft_adv < 0:  # Radiant has draft disadvantage
            roster_stats[radiant_roster]['games_with_disadv'] += 1
            if radiant_win:
                roster_stats[radiant_roster]['wins_with_disadv'] += 1
        
        # Update dire roster stats
        if d_draft_adv > 0:  # Dire has draft advantage
            roster_stats[dire_roster]['games_with_adv'] += 1
            if not radiant_win:
                roster_stats[dire_roster]['wins_with_adv'] += 1
        elif d_draft_adv < 0:  # Dire has draft disadvantage
            roster_stats[dire_roster]['games_with_disadv'] += 1
            if not radiant_win:
                roster_stats[dire_roster]['wins_with_disadv'] += 1
    
    # Compute derived stats
    result = {}
    for roster, stats in roster_stats.items():
        total_games = stats['games_with_adv'] + stats['games_with_disadv']
        if total_games < MIN_GAMES_FOR_STATS:
            continue
        
        # Winrate with advantage
        if stats['games_with_adv'] >= 2:
            wr_adv = stats['wins_with_adv'] / stats['games_with_adv']
        else:
            wr_adv = 0.5  # Default
        
        # Winrate with disadvantage
        if stats['games_with_disadv'] >= 2:
            wr_disadv = stats['wins_with_disadv'] / stats['games_with_disadv']
        else:
            wr_disadv = 0.5  # Default
        
        # Execution score: how well they convert draft advantage (expected ~60-70%)
        execution = (wr_adv - 0.5) * 2  # Normalize to -1 to 1
        
        # Resilience score: how well they play from behind (expected ~30-40%)
        resilience = (wr_disadv - 0.3) * 2  # Normalize, 0.3 is baseline
        
        roster_key = '_'.join(str(p) for p in roster)
        result[roster_key] = {
            'players': list(roster),
            'games_with_adv': stats['games_with_adv'],
            'wins_with_adv': stats['wins_with_adv'],
            'winrate_with_adv': round(wr_adv, 3),
            'games_with_disadv': stats['games_with_disadv'],
            'wins_with_disadv': stats['wins_with_disadv'],
            'winrate_with_disadv': round(wr_disadv, 3),
            'execution_score': round(execution, 3),
            'resilience_score': round(resilience, 3),
        }
    
    return result


def compute_rolling_draft_execution(
    df: pd.DataFrame,
    min_games: int = 5
) -> pd.DataFrame:
    """
    Compute rolling draft execution stats for each match.
    Uses only PAST matches to avoid data leakage.
    """
    df = df.sort_values('match_id').reset_index(drop=True)
    
    # Track historical stats per roster
    roster_history: Dict[Tuple[int, ...], Dict] = defaultdict(lambda: {
        'games_adv': [], 'wins_adv': [],
        'games_disadv': [], 'wins_disadv': [],
    })
    
    results = []
    
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Computing rolling draft execution"):
        # Get rosters
        radiant_roster = get_roster_key([
            row.get('radiant_player_1_id'),
            row.get('radiant_player_2_id'),
            row.get('radiant_player_3_id'),
            row.get('radiant_player_4_id'),
            row.get('radiant_player_5_id'),
        ])
        dire_roster = get_roster_key([
            row.get('dire_player_1_id'),
            row.get('dire_player_2_id'),
            row.get('dire_player_3_id'),
            row.get('dire_player_4_id'),
            row.get('dire_player_5_id'),
        ])
        
        # Find matching rosters from history (3+ player overlap)
        r_exec, r_resil = 0.0, 0.0
        d_exec, d_resil = 0.0, 0.0
        r_coverage, d_coverage = 0, 0
        
        for hist_roster, hist_stats in roster_history.items():
            # Check radiant match
            if rosters_match(radiant_roster, hist_roster, MIN_ROSTER_OVERLAP):
                total = len(hist_stats['games_adv']) + len(hist_stats['games_disadv'])
                if total >= min_games:
                    if hist_stats['games_adv']:
                        wr_adv = sum(hist_stats['wins_adv']) / len(hist_stats['games_adv'])
                        r_exec = max(r_exec, (wr_adv - 0.5) * 2)
                    if hist_stats['games_disadv']:
                        wr_disadv = sum(hist_stats['wins_disadv']) / len(hist_stats['games_disadv'])
                        r_resil = max(r_resil, (wr_disadv - 0.3) * 2)
                    r_coverage = max(r_coverage, total)
            
            # Check dire match
            if rosters_match(dire_roster, hist_roster, MIN_ROSTER_OVERLAP):
                total = len(hist_stats['games_adv']) + len(hist_stats['games_disadv'])
                if total >= min_games:
                    if hist_stats['games_adv']:
                        wr_adv = sum(hist_stats['wins_adv']) / len(hist_stats['games_adv'])
                        d_exec = max(d_exec, (wr_adv - 0.5) * 2)
                    if hist_stats['games_disadv']:
                        wr_disadv = sum(hist_stats['wins_disadv']) / len(hist_stats['games_disadv'])
                        d_resil = max(d_resil, (wr_disadv - 0.3) * 2)
                    d_coverage = max(d_coverage, total)
        
        results.append({
            'radiant_draft_execution': r_exec,
            'dire_draft_execution': d_exec,
            'draft_execution_diff': r_exec - d_exec,
            'radiant_draft_resilience': r_resil,
            'dire_draft_resilience': d_resil,
            'draft_resilience_diff': r_resil - d_resil,
            'radiant_exec_coverage': r_coverage,
            'dire_exec_coverage': d_coverage,
        })
        
        # Update history with current match result
        r_draft_adv = (row.get('radiant_draft_adv_early', 0) + row.get('radiant_draft_adv_late', 0)) / 2
        d_draft_adv = (row.get('dire_draft_adv_early', 0) + row.get('dire_draft_adv_late', 0)) / 2
        radiant_win = row.get('radiant_win', 0)
        
        if len(radiant_roster) >= MIN_ROSTER_OVERLAP:
            if r_draft_adv > 0:
                roster_history[radiant_roster]['games_adv'].append(1)
                roster_history[radiant_roster]['wins_adv'].append(1 if radiant_win else 0)
            elif r_draft_adv < 0:
                roster_history[radiant_roster]['games_disadv'].append(1)
                roster_history[radiant_roster]['wins_disadv'].append(1 if radiant_win else 0)
        
        if len(dire_roster) >= MIN_ROSTER_OVERLAP:
            if d_draft_adv > 0:
                roster_history[dire_roster]['games_adv'].append(1)
                roster_history[dire_roster]['wins_adv'].append(1 if not radiant_win else 0)
            elif d_draft_adv < 0:
                roster_history[dire_roster]['games_disadv'].append(1)
                roster_history[dire_roster]['wins_disadv'].append(1 if not radiant_win else 0)
    
    return pd.DataFrame(results)


def main() -> None:
    """Build draft execution stats and save."""
    df = pd.read_csv('data/pro_matches_enriched.csv')
    df = df.sort_values('match_id').reset_index(drop=True)
    logger.info(f"Loaded {len(df)} matches")
    
    # Compute static stats (for analysis)
    stats = compute_draft_execution_stats(df)
    
    # Save stats
    with open('data/draft_execution_stats.json', 'w') as f:
        json.dump(stats, f, indent=2)
    logger.info(f"Saved draft execution stats for {len(stats)} rosters")
    
    # Show top rosters by execution
    sorted_by_exec = sorted(stats.items(), key=lambda x: x[1]['execution_score'], reverse=True)
    print("\nTop 10 rosters by draft execution:")
    for roster_key, s in sorted_by_exec[:10]:
        print(f"  {s['execution_score']:+.2f} exec, {s['resilience_score']:+.2f} resil, "
              f"{s['games_with_adv']}+{s['games_with_disadv']} games")
    
    # Compute rolling stats for CSV
    rolling_df = compute_rolling_draft_execution(df)
    
    # Add to main dataframe
    for col in rolling_df.columns:
        df[col] = rolling_df[col].values
    
    # Save updated CSV
    df.to_csv('data/pro_matches_enriched.csv', index=False)
    logger.info(f"Updated CSV with {len(rolling_df.columns)} draft execution features")
    
    # Show coverage
    coverage = (rolling_df['radiant_exec_coverage'] > 0).sum()
    print(f"\nMatches with radiant execution data: {coverage} ({coverage/len(df)*100:.1f}%)")


if __name__ == '__main__':
    main()
