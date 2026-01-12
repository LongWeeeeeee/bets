"""
Build Hero Push/Defense Stats from public matches.

Uses streaming to handle large datasets without loading everything into memory.
"""

import json
import logging
from collections import defaultdict
from glob import glob
from pathlib import Path
from typing import Any, Dict, Set

import numpy as np
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFENSE_HERO_IDS: Set[int] = {
    35, 34, 80, 22, 43, 31, 58, 25, 27, 74, 101, 87, 86, 39, 10, 47, 68
}


class PushStatsAccumulator:
    """Accumulates push stats without storing raw data."""
    
    def __init__(self) -> None:
        # hero_id -> {'sum_damage': float, 'sum_duration': float, 'sum_rate': float, 'count': int}
        self.hero_stats: Dict[int, Dict[str, float]] = defaultdict(
            lambda: {'sum_damage': 0.0, 'sum_duration': 0.0, 'sum_rate': 0.0, 'count': 0}
        )
        self.valid_matches = 0
    
    def process_match(self, match: Dict[str, Any]) -> bool:
        """Process a single match. Returns True if valid."""
        players = match.get('players', [])
        if len(players) != 10:
            return False
        
        duration = len(match.get('direKills', []))
        if duration < 10:
            return False
        
        self.valid_matches += 1
        
        for p in players:
            hero_id = p.get('heroId', 0)
            tower_dmg = p.get('towerDamage', 0) or 0
            
            if hero_id:
                push_rate = tower_dmg / max(duration, 1)
                self.hero_stats[hero_id]['sum_damage'] += tower_dmg
                self.hero_stats[hero_id]['sum_duration'] += duration
                self.hero_stats[hero_id]['sum_rate'] += push_rate
                self.hero_stats[hero_id]['count'] += 1
        
        return True
    
    def compute_results(self, min_games: int = 100) -> Dict[int, Dict[str, Any]]:
        """Compute final push stats."""
        # First pass: compute global average
        all_rates = []
        for hero_id, stats in self.hero_stats.items():
            if stats['count'] >= min_games:
                avg_rate = stats['sum_rate'] / stats['count']
                all_rates.append(avg_rate)
        
        global_avg_rate = np.mean(all_rates) if all_rates else 50.0
        
        result: Dict[int, Dict[str, Any]] = {}
        for hero_id, stats in self.hero_stats.items():
            if stats['count'] < min_games:
                continue
            
            avg_damage = stats['sum_damage'] / stats['count']
            avg_duration = stats['sum_duration'] / stats['count']
            avg_push_rate = stats['sum_rate'] / stats['count']
            
            result[hero_id] = {
                'avg_tower_damage': round(avg_damage, 1),
                'avg_duration': round(avg_duration, 1),
                'push_rate': round(avg_push_rate, 2),
                'push_score': round(avg_push_rate / global_avg_rate, 3),
                'is_defense_hero': hero_id in DEFENSE_HERO_IDS,
                'games': stats['count'],
            }
        
        return result


def build_hero_push_stats(
    pub_matches_path: str = 'bets_data/analise_pub_matches/json_parts_split_from_object',
    output_path: str = 'data/hero_push_stats.json',
    min_games: int = 100
) -> Dict[int, Dict[str, Any]]:
    """Build hero push stats."""
    all_files = sorted(glob(f"{pub_matches_path}/combined*.json"))
    logger.info(f"Found {len(all_files)} pub match files")
    
    if not all_files:
        logger.error(f"No files in {pub_matches_path}")
        return {}
    
    accumulator = PushStatsAccumulator()
    
    for file_path in tqdm(all_files, desc="Processing files"):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for match_data in data.values():
                accumulator.process_match(match_data)
            del data
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
    
    logger.info(f"Valid matches: {accumulator.valid_matches}")
    
    result = accumulator.compute_results(min_games)
    logger.info(f"Heroes with push stats: {len(result)}")
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)
    
    logger.info(f"Saved to {output_path}")
    return result


if __name__ == '__main__':
    build_hero_push_stats()
