"""
Build Blood Stats: вычисляет "кровавость" героев и их связок.

Обрабатывает файлы по одному, не загружая всё в память сразу.
"""

import json
import logging
from collections import defaultdict
from glob import glob
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_PUB_DIR = 'bets_data/analise_pub_matches/json_parts_split_from_object'


def extract_match_data(match: Dict[str, Any]) -> Optional[Tuple[List[int], List[int], int, int]]:
    """Извлекает данные матча: radiant heroes, dire heroes, total kills, duration."""
    players = match.get('players', [])
    if len(players) != 10:
        return None
    
    dire_kills = match.get('direKills', [])
    duration_min = len(dire_kills)
    if duration_min < 15:
        return None
    
    radiant_heroes: List[int] = []
    dire_heroes: List[int] = []
    total_kills = 0
    
    for p in players:
        hero_id = p.get('heroId', 0)
        if not hero_id:
            return None
        kills = p.get('kills', 0) or 0
        total_kills += kills
        if p.get('isRadiant'):
            radiant_heroes.append(hero_id)
        else:
            dire_heroes.append(hero_id)
    
    if len(radiant_heroes) != 5 or len(dire_heroes) != 5:
        return None
    
    return radiant_heroes, dire_heroes, total_kills, duration_min


def make_pair_key(h1: int, h2: int) -> str:
    return f"{min(h1, h2)}_{max(h1, h2)}"


class BloodStatsAccumulator:
    """Accumulates blood stats without storing raw data."""
    
    def __init__(self) -> None:
        self.hero_stats: Dict[int, Dict[str, float]] = defaultdict(
            lambda: {'sum_kills': 0.0, 'sum_kpm': 0.0, 'sum_duration': 0.0, 'count': 0}
        )
        self.ally_pair_stats: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {'sum_kills': 0.0, 'sum_kpm': 0.0, 'count': 0}
        )
        self.enemy_pair_stats: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {'sum_kills': 0.0, 'sum_kpm': 0.0, 'count': 0}
        )
        self.total_kills_sum = 0.0
        self.total_kpm_sum = 0.0
        self.total_duration_sum = 0.0
        self.valid_matches = 0
    
    def process_match(self, match: Dict[str, Any]) -> bool:
        result = extract_match_data(match)
        if result is None:
            return False
        
        radiant_heroes, dire_heroes, total_kills, duration_min = result
        kpm = total_kills / max(duration_min, 1)
        
        self.valid_matches += 1
        self.total_kills_sum += total_kills
        self.total_kpm_sum += kpm
        self.total_duration_sum += duration_min
        
        for hero in radiant_heroes + dire_heroes:
            self.hero_stats[hero]['sum_kills'] += total_kills
            self.hero_stats[hero]['sum_kpm'] += kpm
            self.hero_stats[hero]['sum_duration'] += duration_min
            self.hero_stats[hero]['count'] += 1
        
        for i, h1 in enumerate(radiant_heroes):
            for h2 in radiant_heroes[i+1:]:
                key = make_pair_key(h1, h2)
                self.ally_pair_stats[key]['sum_kills'] += total_kills
                self.ally_pair_stats[key]['sum_kpm'] += kpm
                self.ally_pair_stats[key]['count'] += 1
        
        for i, h1 in enumerate(dire_heroes):
            for h2 in dire_heroes[i+1:]:
                key = make_pair_key(h1, h2)
                self.ally_pair_stats[key]['sum_kills'] += total_kills
                self.ally_pair_stats[key]['sum_kpm'] += kpm
                self.ally_pair_stats[key]['count'] += 1
        
        for h1 in radiant_heroes:
            for h2 in dire_heroes:
                key = make_pair_key(h1, h2)
                self.enemy_pair_stats[key]['sum_kills'] += total_kills
                self.enemy_pair_stats[key]['sum_kpm'] += kpm
                self.enemy_pair_stats[key]['count'] += 1
        
        return True
    
    def compute_results(self, min_hero_games: int = 100, min_pair_games: int = 30) -> Dict[str, Any]:
        if self.valid_matches == 0:
            return {}
        
        global_avg_kills = self.total_kills_sum / self.valid_matches
        global_avg_kpm = self.total_kpm_sum / self.valid_matches
        global_avg_duration = self.total_duration_sum / self.valid_matches
        
        logger.info(f"Valid matches: {self.valid_matches}")
        logger.info(f"Global avg kills: {global_avg_kills:.2f}, KPM: {global_avg_kpm:.4f}")
        
        hero_blood: Dict[str, Dict[str, Any]] = {}
        for hero_id, stats in self.hero_stats.items():
            if stats['count'] >= min_hero_games:
                avg_kills = stats['sum_kills'] / stats['count']
                avg_kpm = stats['sum_kpm'] / stats['count']
                hero_blood[str(hero_id)] = {
                    'avg_kills': round(avg_kills, 2),
                    'avg_kpm': round(avg_kpm, 4),
                    'games': stats['count'],
                    'blood_score': round(avg_kills - global_avg_kills, 2),
                    'blood_score_pm': round(avg_kpm - global_avg_kpm, 4),
                }
        
        duo_blood: Dict[str, Dict[str, Any]] = {}
        for pair_key, stats in self.ally_pair_stats.items():
            if stats['count'] >= min_pair_games:
                h1, h2 = map(int, pair_key.split('_'))
                avg_kills = stats['sum_kills'] / stats['count']
                avg_kpm = stats['sum_kpm'] / stats['count']
                
                h1_blood = hero_blood.get(str(h1), {}).get('blood_score', 0)
                h2_blood = hero_blood.get(str(h2), {}).get('blood_score', 0)
                h1_blood_pm = hero_blood.get(str(h1), {}).get('blood_score_pm', 0)
                h2_blood_pm = hero_blood.get(str(h2), {}).get('blood_score_pm', 0)
                
                actual = avg_kills - global_avg_kills
                actual_pm = avg_kpm - global_avg_kpm
                
                duo_blood[pair_key] = {
                    'avg_kills': round(avg_kills, 2),
                    'games': stats['count'],
                    'synergy': round(actual - (h1_blood + h2_blood), 2),
                    'synergy_pm': round(actual_pm - (h1_blood_pm + h2_blood_pm), 4),
                }
        
        vs_blood: Dict[str, Dict[str, Any]] = {}
        for pair_key, stats in self.enemy_pair_stats.items():
            if stats['count'] >= min_pair_games:
                h1, h2 = map(int, pair_key.split('_'))
                avg_kills = stats['sum_kills'] / stats['count']
                avg_kpm = stats['sum_kpm'] / stats['count']
                
                h1_blood = hero_blood.get(str(h1), {}).get('blood_score', 0)
                h2_blood = hero_blood.get(str(h2), {}).get('blood_score', 0)
                h1_blood_pm = hero_blood.get(str(h1), {}).get('blood_score_pm', 0)
                h2_blood_pm = hero_blood.get(str(h2), {}).get('blood_score_pm', 0)
                
                actual = avg_kills - global_avg_kills
                actual_pm = avg_kpm - global_avg_kpm
                
                vs_blood[pair_key] = {
                    'avg_kills': round(avg_kills, 2),
                    'games': stats['count'],
                    'clash': round(actual - (h1_blood + h2_blood), 2),
                    'clash_pm': round(actual_pm - (h1_blood_pm + h2_blood_pm), 4),
                }
        
        logger.info(f"Heroes: {len(hero_blood)}, Duo pairs: {len(duo_blood)}, VS pairs: {len(vs_blood)}")
        
        return {
            'global_avg_kills': round(global_avg_kills, 2),
            'global_avg_kpm': round(global_avg_kpm, 4),
            'total_matches': self.valid_matches,
            'hero_blood': hero_blood,
            'duo_blood': duo_blood,
            'vs_blood': vs_blood
        }


def build_blood_stats(
    input_dir: str = DEFAULT_PUB_DIR,
    output_path: str = 'data/blood_stats.json',
    min_hero_games: int = 100,
    min_pair_games: int = 30
) -> Dict[str, Any]:
    """Build blood stats - processes files one by one."""
    all_files = sorted(glob(f"{input_dir}/combined*.json"))
    logger.info(f"Found {len(all_files)} pub match files")
    
    if not all_files:
        return {}
    
    accumulator = BloodStatsAccumulator()
    
    for file_path in tqdm(all_files, desc="Processing files"):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for match in data.values():
                accumulator.process_match(match)
            del data  # Free memory
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
    
    result = accumulator.compute_results(min_hero_games, min_pair_games)
    if not result:
        return {}
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)
    
    logger.info(f"Saved to {output_path}")
    return result


if __name__ == '__main__':
    build_blood_stats()
