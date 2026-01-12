"""
Wave Clear Stats: Способность героев быстро убивать волны крипов.

Uses streaming to handle large datasets without loading everything into memory.
"""

import json
import logging
from collections import defaultdict
from glob import glob
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MIN_GAMES = 50


class WaveClearAccumulator:
    """Accumulates wave clear stats without storing raw data."""
    
    def __init__(self) -> None:
        # hero_id -> {'sum_lh_pm': float, 'count': int}
        self.hero_stats: Dict[int, Dict[str, float]] = defaultdict(
            lambda: {'sum_lh_pm': 0.0, 'count': 0}
        )
        self.valid_matches = 0
    
    def process_match(self, match: Dict[str, Any]) -> bool:
        """Process a single match. Returns True if valid."""
        players = match.get('players', [])
        dire_kills = match.get('direKills', [])
        duration_min = len(dire_kills) if dire_kills else 35
        
        if duration_min < 15:
            return False
        
        self.valid_matches += 1
        
        for player in players:
            hero_id = player.get('heroId', 0)
            if not hero_id:
                continue
            
            last_hits = player.get('numLastHits', 0)
            lh_per_min = last_hits / max(duration_min, 1)
            
            self.hero_stats[hero_id]['sum_lh_pm'] += lh_per_min
            self.hero_stats[hero_id]['count'] += 1
        
        return True
    
    def compute_results(self, min_games: int = MIN_GAMES) -> Dict[int, Dict[str, float]]:
        """Compute final wave clear stats."""
        # First pass: compute averages
        hero_avgs: Dict[int, float] = {}
        for hero_id, stats in self.hero_stats.items():
            if stats['count'] >= min_games:
                hero_avgs[hero_id] = stats['sum_lh_pm'] / stats['count']
        
        if not hero_avgs:
            return {}
        
        # Normalize
        min_lh = min(hero_avgs.values())
        max_lh = max(hero_avgs.values())
        range_lh = max_lh - min_lh if max_lh > min_lh else 1
        
        result: Dict[int, Dict[str, float]] = {}
        for hero_id, avg_lh in hero_avgs.items():
            wave_clear_score = (avg_lh - min_lh) / range_lh
            result[hero_id] = {
                'avg_lh_per_min': round(avg_lh, 2),
                'wave_clear_score': round(wave_clear_score, 4),
                'games': self.hero_stats[hero_id]['count'],
            }
        
        return result


def build_wave_clear_stats(
    pub_path: str = 'bets_data/analise_pub_matches/json_parts_split_from_object',
    output_path: str = 'data/hero_wave_clear.json'
) -> Dict[int, Dict[str, float]]:
    """Build wave clear stats."""
    all_files = sorted(glob(f"{pub_path}/combined*.json"))
    logger.info(f"Found {len(all_files)} pub match files")
    
    if not all_files:
        logger.error(f"No files in {pub_path}")
        return {}
    
    accumulator = WaveClearAccumulator()
    
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
    
    result = accumulator.compute_results()
    logger.info(f"Heroes with wave clear stats: {len(result)}")
    
    # Convert int keys to str for JSON
    result_str = {str(k): v for k, v in result.items()}
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(result_str, f, indent=2)
    
    logger.info(f"Saved to {output_path}")
    return result


if __name__ == '__main__':
    build_wave_clear_stats()
