"""
Build Hero Power Spikes: Early/Late game power based on winrate by duration.

Uses streaming to handle large datasets without loading everything into memory.
"""

import json
import logging
from collections import defaultdict
from glob import glob
from pathlib import Path
from typing import Any, Dict, List

from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PowerSpikeAccumulator:
    """Accumulates power spike stats without storing raw data."""
    
    def __init__(self) -> None:
        # hero_id -> bucket -> [wins, total]
        self.hero_stats: Dict[int, Dict[str, List[int]]] = defaultdict(
            lambda: {'early': [0, 0], 'mid': [0, 0], 'late': [0, 0]}
        )
        self.valid_matches = 0
    
    def process_match(self, match: Dict[str, Any]) -> bool:
        """Process a single match. Returns True if valid."""
        players = match.get('players', [])
        if len(players) != 10:
            return False
        
        dire_kills = match.get('direKills', [])
        duration_min = len(dire_kills)
        
        if duration_min < 10:
            return False
        
        self.valid_matches += 1
        radiant_win = match.get('didRadiantWin', False)
        
        # Determine bucket
        if duration_min < 25:
            bucket = 'early'
        elif duration_min <= 40:
            bucket = 'mid'
        else:
            bucket = 'late'
        
        for player in players:
            hero_id = player.get('heroId', 0)
            if not hero_id:
                continue
            
            is_radiant = player.get('isRadiant', False)
            won = (is_radiant and radiant_win) or (not is_radiant and not radiant_win)
            
            self.hero_stats[hero_id][bucket][1] += 1
            if won:
                self.hero_stats[hero_id][bucket][0] += 1
        
        return True
    
    def compute_results(self, min_games: int = 100) -> Dict[int, Dict[str, Any]]:
        """Compute final power spike stats."""
        result: Dict[int, Dict[str, Any]] = {}
        
        def safe_winrate(wins: int, total: int, prior: float = 0.5, prior_weight: int = 10) -> float:
            return (wins + prior * prior_weight) / (total + prior_weight)
        
        for hero_id, buckets in self.hero_stats.items():
            early_wins, early_total = buckets['early']
            mid_wins, mid_total = buckets['mid']
            late_wins, late_total = buckets['late']
            
            total_games = early_total + mid_total + late_total
            if total_games < min_games:
                continue
            
            early_wr = safe_winrate(early_wins, early_total)
            mid_wr = safe_winrate(mid_wins, mid_total)
            late_wr = safe_winrate(late_wins, late_total)
            
            power_curve = late_wr - early_wr
            winrates = {'early': early_wr, 'mid': mid_wr, 'late': late_wr}
            spike_timing = max(winrates, key=winrates.get)
            
            result[hero_id] = {
                'early_power': round(early_wr, 4),
                'mid_power': round(mid_wr, 4),
                'late_power': round(late_wr, 4),
                'power_curve': round(power_curve, 4),
                'spike_timing': spike_timing,
                'early_games': early_total,
                'mid_games': mid_total,
                'late_games': late_total,
                'total_games': total_games,
            }
        
        return result


def build_hero_power_spikes(
    pub_matches_path: str = 'bets_data/analise_pub_matches/json_parts_split_from_object',
    output_path: str = 'data/hero_power_spikes.json',
    min_games: int = 100
) -> Dict[int, Dict[str, Any]]:
    """Build hero power spikes."""
    all_files = sorted(glob(f"{pub_matches_path}/combined*.json"))
    logger.info(f"Found {len(all_files)} pub match files")
    
    if not all_files:
        logger.error(f"No files in {pub_matches_path}")
        return {}
    
    accumulator = PowerSpikeAccumulator()
    
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
    logger.info(f"Heroes with power spikes: {len(result)}")
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)
    
    logger.info(f"Saved to {output_path}")
    return result


if __name__ == '__main__':
    build_hero_power_spikes()
