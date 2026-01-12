"""
Построение матриц синергий и контр-пиков на основе паблик-матчей.

Uses streaming to handle large datasets without loading everything into memory.
"""

import json
import logging
from collections import defaultdict
from glob import glob
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_PUB_DIR = 'bets_data/analise_pub_matches/json_parts_split_from_object'


def extract_match_heroes(match: Dict[str, Any]) -> Optional[Tuple[List[int], List[int], bool]]:
    """Извлекает героев и результат из матча."""
    players = match.get('players', [])
    if len(players) != 10:
        return None
    
    radiant_heroes: List[int] = []
    dire_heroes: List[int] = []
    
    for p in players:
        hero_id = p.get('heroId', 0)
        if not hero_id:
            return None
        if p.get('isRadiant'):
            radiant_heroes.append(hero_id)
        else:
            dire_heroes.append(hero_id)
    
    if len(radiant_heroes) != 5 or len(dire_heroes) != 5:
        return None
    
    radiant_win = match.get('didRadiantWin', False)
    return radiant_heroes, dire_heroes, radiant_win


class SynergyAccumulator:
    """Accumulates synergy/counter stats without storing raw data."""
    
    def __init__(self) -> None:
        self.hero_stats: Dict[int, Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.pair_stats: Dict[Tuple[int, int], Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.matchup_stats: Dict[Tuple[int, int], Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.valid_matches = 0
    
    def process_match(self, match: Dict[str, Any]) -> bool:
        """Process a single match. Returns True if valid."""
        result = extract_match_heroes(match)
        if result is None:
            return False
        
        radiant_heroes, dire_heroes, radiant_win = result
        self.valid_matches += 1
        
        # Hero winrates
        for hero_id in radiant_heroes:
            self.hero_stats[hero_id]['games'] += 1
            if radiant_win:
                self.hero_stats[hero_id]['wins'] += 1
        
        for hero_id in dire_heroes:
            self.hero_stats[hero_id]['games'] += 1
            if not radiant_win:
                self.hero_stats[hero_id]['wins'] += 1
        
        # Synergy pairs (same team)
        for h1, h2 in combinations(sorted(radiant_heroes), 2):
            self.pair_stats[(h1, h2)]['games'] += 1
            if radiant_win:
                self.pair_stats[(h1, h2)]['wins'] += 1
        
        for h1, h2 in combinations(sorted(dire_heroes), 2):
            self.pair_stats[(h1, h2)]['games'] += 1
            if not radiant_win:
                self.pair_stats[(h1, h2)]['wins'] += 1
        
        # Counter matchups (cross team)
        for r_hero in radiant_heroes:
            for d_hero in dire_heroes:
                self.matchup_stats[(r_hero, d_hero)]['games'] += 1
                if radiant_win:
                    self.matchup_stats[(r_hero, d_hero)]['wins'] += 1
                
                self.matchup_stats[(d_hero, r_hero)]['games'] += 1
                if not radiant_win:
                    self.matchup_stats[(d_hero, r_hero)]['wins'] += 1
        
        return True
    
    def compute_results(self, min_hero_games: int = 100, min_pair_games: int = 50) -> Dict[str, Any]:
        """Compute final synergy/counter matrices."""
        if self.valid_matches == 0:
            logger.error("No valid matches!")
            return {}
        
        logger.info(f"Valid matches: {self.valid_matches}")
        
        # Hero winrates
        hero_winrates: Dict[int, float] = {}
        for hero_id, stats in self.hero_stats.items():
            if stats['games'] >= min_hero_games:
                hero_winrates[hero_id] = stats['wins'] / stats['games']
        
        logger.info(f"Heroes with winrates: {len(hero_winrates)}")
        
        # Synergy matrix
        synergy_matrix: Dict[str, float] = {}
        for (h1, h2), stats in self.pair_stats.items():
            if stats['games'] < min_pair_games:
                continue
            if h1 not in hero_winrates or h2 not in hero_winrates:
                continue
            
            pair_wr = stats['wins'] / stats['games']
            expected_wr = (hero_winrates[h1] + hero_winrates[h2]) / 2
            synergy_matrix[f"{h1}_{h2}"] = round(pair_wr - expected_wr, 4)
        
        logger.info(f"Synergy pairs: {len(synergy_matrix)}")
        
        # Counter matrix
        counter_matrix: Dict[str, float] = {}
        for (hero_a, hero_b), stats in self.matchup_stats.items():
            if stats['games'] < min_hero_games:
                continue
            if hero_a not in hero_winrates:
                continue
            
            matchup_wr = stats['wins'] / stats['games']
            counter_matrix[f"{hero_a}_vs_{hero_b}"] = round(matchup_wr - hero_winrates[hero_a], 4)
        
        logger.info(f"Counter matchups: {len(counter_matrix)}")
        
        return {
            'hero_winrates': {str(k): round(v, 4) for k, v in hero_winrates.items()},
            'synergy': synergy_matrix,
            'counter': counter_matrix,
            'total_matches': self.valid_matches
        }


def build_synergy_matrix(
    input_dir: str = DEFAULT_PUB_DIR,
    output_path: str = 'data/hero_synergy.json'
) -> Dict[str, Any]:
    """Build synergy/counter matrices."""
    all_files = sorted(glob(f"{input_dir}/combined*.json"))
    logger.info(f"Found {len(all_files)} pub match files")
    
    if not all_files:
        logger.error(f"No files in {input_dir}")
        return {}
    
    accumulator = SynergyAccumulator()
    
    for file_path in tqdm(all_files, desc="Processing files"):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for match_data in data.values():
                accumulator.process_match(match_data)
            del data
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
    
    result = accumulator.compute_results()
    if not result:
        return {}
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)
    
    logger.info(f"Saved to {output_path}")
    return result


def main() -> None:
    build_synergy_matrix()


if __name__ == '__main__':
    main()
