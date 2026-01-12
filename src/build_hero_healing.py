"""
Build Hero Healing Stats from public matches.

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

DEFAULT_PUB_DIR = 'bets_data/analise_pub_matches/json_parts_split_from_object'

SAVE_HERO_IDS: Set[int] = {
    50, 111, 91, 102, 79, 76, 57, 66, 112, 110, 37, 83, 90
}


class HealingAccumulator:
    """Accumulates healing stats without storing raw data."""
    
    def __init__(self) -> None:
        # hero_id -> {'sum_heal_pm': float, 'count': int}
        self.hero_stats: Dict[int, Dict[str, float]] = defaultdict(
            lambda: {'sum_heal_pm': 0.0, 'count': 0}
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
            healing = p.get('heroHealing', 0) or 0
            
            if hero_id:
                heal_per_min = healing / max(duration, 1)
                self.hero_stats[hero_id]['sum_heal_pm'] += heal_per_min
                self.hero_stats[hero_id]['count'] += 1
        
        return True
    
    def compute_results(self, min_games: int = 100) -> Dict[int, Dict[str, Any]]:
        """Compute final healing stats."""
        # First pass: compute averages
        hero_avgs: Dict[int, float] = {}
        for hero_id, stats in self.hero_stats.items():
            if stats['count'] >= min_games:
                hero_avgs[hero_id] = stats['sum_heal_pm'] / stats['count']
        
        if not hero_avgs:
            return {}
        
        global_avg = np.mean(list(hero_avgs.values()))
        
        result: Dict[int, Dict[str, Any]] = {}
        for hero_id, avg_heal in hero_avgs.items():
            result[hero_id] = {
                'avg_healing_per_min': round(avg_heal, 2),
                'healing_score': round(avg_heal / global_avg, 3),
                'is_save_hero': hero_id in SAVE_HERO_IDS,
                'games': self.hero_stats[hero_id]['count'],
            }
        
        return result


def build_hero_healing_stats(
    input_dir: str = DEFAULT_PUB_DIR,
    output_path: str = 'data/hero_healing_stats.json',
    min_games: int = 100
) -> Dict[int, Dict[str, Any]]:
    """Build hero healing stats."""
    all_files = sorted(glob(f"{input_dir}/combined*.json"))
    logger.info(f"Found {len(all_files)} pub match files")
    
    if not all_files:
        logger.error(f"No files in {input_dir}")
        return {}
    
    accumulator = HealingAccumulator()
    
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
    logger.info(f"Heroes with healing stats: {len(result)}")
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)
    
    logger.info(f"Saved to {output_path}")
    return result


if __name__ == '__main__':
    build_hero_healing_stats()
