"""
Генерация профилей героев из паблик-матчей.

Uses streaming to handle large datasets without loading everything into memory.
"""

import json
import logging
from collections import defaultdict
from glob import glob
from pathlib import Path
from typing import Any, Dict

from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HeroStatsAccumulator:
    """Accumulates hero stats without storing raw data."""
    
    def __init__(self) -> None:
        # hero_id -> {'sum_kills', 'sum_deaths', 'sum_assists', 'sum_gpm', 'sum_duration', 'count'}
        self.hero_stats: Dict[int, Dict[str, float]] = defaultdict(
            lambda: {'sum_kills': 0.0, 'sum_deaths': 0.0, 'sum_assists': 0.0, 
                     'sum_gpm': 0.0, 'sum_duration': 0.0, 'count': 0}
        )
        self.valid_matches = 0
    
    def process_match(self, match: Dict[str, Any]) -> bool:
        """Process a single match. Returns True if valid."""
        players = match.get('players', [])
        if len(players) != 10:
            return False
        
        dire_kills_arr = match.get('direKills', [])
        duration_min = len(dire_kills_arr)
        
        if duration_min < 10:
            return False
        
        self.valid_matches += 1
        
        for player in players:
            hero_id = player.get('heroId')
            if not hero_id:
                continue
            
            kills = player.get('kills', 0) or 0
            deaths = player.get('deaths', 0) or 0
            assists = player.get('assists', 0) or 0
            gpm = player.get('goldPerMinute', 0) or 0
            
            self.hero_stats[hero_id]['sum_kills'] += kills
            self.hero_stats[hero_id]['sum_deaths'] += deaths
            self.hero_stats[hero_id]['sum_assists'] += assists
            self.hero_stats[hero_id]['sum_gpm'] += gpm
            self.hero_stats[hero_id]['sum_duration'] += duration_min
            self.hero_stats[hero_id]['count'] += 1
        
        return True
    
    def compute_results(self, min_games: int = 100) -> Dict[int, Dict[str, float]]:
        """Compute final hero stats."""
        result: Dict[int, Dict[str, float]] = {}
        
        for hero_id, stats in self.hero_stats.items():
            if stats['count'] < min_games:
                continue
            
            avg_kills = stats['sum_kills'] / stats['count']
            avg_deaths = stats['sum_deaths'] / stats['count']
            avg_assists = stats['sum_assists'] / stats['count']
            avg_gpm = stats['sum_gpm'] / stats['count']
            avg_duration = stats['sum_duration'] / stats['count']
            
            aggression = (avg_kills + avg_assists) / max(avg_duration, 1)
            feed = avg_deaths / max(avg_duration, 1)
            
            result[hero_id] = {
                'aggression': round(aggression, 4),
                'feed': round(feed, 4),
                'pace': round(avg_duration * 60, 1),  # seconds
                'gpm': round(avg_gpm, 1),
                'total_matches': stats['count'],
            }
        
        return result


def main(
    input_dir: str = 'bets_data/analise_pub_matches/json_parts_split_from_object',
    output_path: str = 'data/hero_public_stats.csv'
) -> None:
    """Build hero stats."""
    all_files = sorted(glob(f"{input_dir}/combined*.json"))
    logger.info(f"Found {len(all_files)} pub match files")
    
    if not all_files:
        logger.error(f"No files in {input_dir}")
        return
    
    accumulator = HeroStatsAccumulator()
    
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
    logger.info(f"Heroes with stats: {len(result)}")
    
    # Save as CSV
    import csv
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['hero_id', 'aggression', 'feed', 'pace', 'gpm', 'total_matches'])
        for hero_id, stats in sorted(result.items()):
            writer.writerow([
                hero_id, stats['aggression'], stats['feed'], 
                stats['pace'], stats['gpm'], stats['total_matches']
            ])
    
    logger.info(f"Saved to {output_path}")


if __name__ == '__main__':
    main()
