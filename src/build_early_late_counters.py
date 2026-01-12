#!/usr/bin/env python3
"""
Build Early/Late Counter Matrices from public matches.

Uses streaming to handle large datasets without loading everything into memory.
"""

import json
import logging
from collections import defaultdict
from glob import glob
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Optional

from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EARLY_LEAD_THRESHOLD = 7000
EARLY_LEAD_START_MINUTE = 20
EARLY_LEAD_END_MINUTE = 28
EARLY_END_MINUTE = 32
LATE_GAME_MINUTE = 33
LATE_LEAD_THRESHOLD = 10000
MIN_MATCHES = 30


def classify_game(match: Dict[str, Any]) -> Optional[str]:
    """Classify game as 'early' or 'late'."""
    nw_leads = match.get('radiantNetworthLeads', [])
    duration_min = len(nw_leads)
    
    if duration_min < 20:
        return None
    
    if duration_min < EARLY_END_MINUTE:
        return 'early'

    early_lead = False
    start_m = max(0, EARLY_LEAD_START_MINUTE)
    end_m = min(EARLY_LEAD_END_MINUTE, len(nw_leads))
    for minute in range(start_m, end_m):
        if abs(nw_leads[minute]) >= EARLY_LEAD_THRESHOLD:
            early_lead = True
            break

    if early_lead:
        return 'early'

    if duration_min >= LATE_GAME_MINUTE:
        ref_minute = 32
        if len(nw_leads) > ref_minute and abs(nw_leads[ref_minute]) < LATE_LEAD_THRESHOLD:
            return 'late'
    return None


def get_hero_positions(players: List[Dict]) -> Dict[str, Dict[str, int]]:
    """Extract heroes by position for each team."""
    result: Dict[str, Dict[str, int]] = {'radiant': {}, 'dire': {}}
    pos_map = {'POSITION_1': 'pos1', 'POSITION_2': 'pos2', 'POSITION_3': 'pos3', 
               'POSITION_4': 'pos4', 'POSITION_5': 'pos5'}
    
    for player in players:
        team = 'radiant' if player.get('isRadiant') else 'dire'
        pos = pos_map.get(player.get('position', ''), '')
        hero_id = player.get('heroId', 0)
        if pos and hero_id:
            result[team][pos] = hero_id
    return result


class EarlyLateAccumulator:
    """Accumulates early/late counter stats."""
    
    def __init__(self) -> None:
        # Stats containers: key -> {'wins': int, 'games': int}
        self.early_1v1: Dict[str, Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.late_1v1: Dict[str, Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.early_2v1: Dict[str, Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.late_2v1: Dict[str, Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.early_syn_2: Dict[str, Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.late_syn_2: Dict[str, Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.early_syn_3: Dict[str, Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.late_syn_3: Dict[str, Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.early_mid_1v1: Dict[str, Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.late_mid_1v1: Dict[str, Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.early_safe_2v2: Dict[str, Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.late_safe_2v2: Dict[str, Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.early_off_2v2: Dict[str, Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.late_off_2v2: Dict[str, Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'games': 0})
        self.early_count = 0
        self.late_count = 0
    
    def process_match(self, match: Dict[str, Any]) -> bool:
        """Process a single match. Returns True if valid."""
        game_type = classify_game(match)
        if game_type is None:
            return False
        
        players = match.get('players', [])
        if len(players) != 10:
            return False
        
        radiant_win = match.get('didRadiantWin', False)
        positions = get_hero_positions(players)
        
        radiant_heroes = list(positions['radiant'].values())
        dire_heroes = list(positions['dire'].values())
        
        if len(radiant_heroes) != 5 or len(dire_heroes) != 5:
            return False
        
        if game_type == 'early':
            self.early_count += 1
            c1v1, c2v1, s2, s3 = self.early_1v1, self.early_2v1, self.early_syn_2, self.early_syn_3
            mid1v1, safe2v2, off2v2 = self.early_mid_1v1, self.early_safe_2v2, self.early_off_2v2
        else:
            self.late_count += 1
            c1v1, c2v1, s2, s3 = self.late_1v1, self.late_2v1, self.late_syn_2, self.late_syn_3
            mid1v1, safe2v2, off2v2 = self.late_mid_1v1, self.late_safe_2v2, self.late_off_2v2
        
        # 1v1 Counters
        for r_hero in radiant_heroes:
            for d_hero in dire_heroes:
                key = f"{r_hero}_vs_{d_hero}"
                c1v1[key]['games'] += 1
                if radiant_win:
                    c1v1[key]['wins'] += 1
                key_rev = f"{d_hero}_vs_{r_hero}"
                c1v1[key_rev]['games'] += 1
                if not radiant_win:
                    c1v1[key_rev]['wins'] += 1
        
        # 2v1 Counters
        for h1, h2 in combinations(sorted(radiant_heroes), 2):
            for d_hero in dire_heroes:
                key = f"{h1}_{h2}_vs_{d_hero}"
                c2v1[key]['games'] += 1
                if radiant_win:
                    c2v1[key]['wins'] += 1
        for h1, h2 in combinations(sorted(dire_heroes), 2):
            for r_hero in radiant_heroes:
                key = f"{h1}_{h2}_vs_{r_hero}"
                c2v1[key]['games'] += 1
                if not radiant_win:
                    c2v1[key]['wins'] += 1
        
        # Synergy 1+1
        for h1, h2 in combinations(sorted(radiant_heroes), 2):
            key = f"{h1}_{h2}"
            s2[key]['games'] += 1
            if radiant_win:
                s2[key]['wins'] += 1
        for h1, h2 in combinations(sorted(dire_heroes), 2):
            key = f"{h1}_{h2}"
            s2[key]['games'] += 1
            if not radiant_win:
                s2[key]['wins'] += 1
        
        # Synergy 1+1+1
        for h1, h2, h3 in combinations(sorted(radiant_heroes), 3):
            key = f"{h1}_{h2}_{h3}"
            s3[key]['games'] += 1
            if radiant_win:
                s3[key]['wins'] += 1
        for h1, h2, h3 in combinations(sorted(dire_heroes), 3):
            key = f"{h1}_{h2}_{h3}"
            s3[key]['games'] += 1
            if not radiant_win:
                s3[key]['wins'] += 1
        
        # Lane Matchups
        r_pos, d_pos = positions['radiant'], positions['dire']
        
        if 'pos2' in r_pos and 'pos2' in d_pos:
            key = f"{r_pos['pos2']}_vs_{d_pos['pos2']}"
            mid1v1[key]['games'] += 1
            if radiant_win:
                mid1v1[key]['wins'] += 1
            key_rev = f"{d_pos['pos2']}_vs_{r_pos['pos2']}"
            mid1v1[key_rev]['games'] += 1
            if not radiant_win:
                mid1v1[key_rev]['wins'] += 1
        
        if all(p in r_pos for p in ['pos1', 'pos5']) and all(p in d_pos for p in ['pos3', 'pos4']):
            r_pair = tuple(sorted([r_pos['pos1'], r_pos['pos5']]))
            d_pair = tuple(sorted([d_pos['pos3'], d_pos['pos4']]))
            key = f"{r_pair[0]}_{r_pair[1]}_vs_{d_pair[0]}_{d_pair[1]}"
            safe2v2[key]['games'] += 1
            if radiant_win:
                safe2v2[key]['wins'] += 1
        
        if all(p in r_pos for p in ['pos3', 'pos4']) and all(p in d_pos for p in ['pos1', 'pos5']):
            r_pair = tuple(sorted([r_pos['pos3'], r_pos['pos4']]))
            d_pair = tuple(sorted([d_pos['pos1'], d_pos['pos5']]))
            key = f"{r_pair[0]}_{r_pair[1]}_vs_{d_pair[0]}_{d_pair[1]}"
            off2v2[key]['games'] += 1
            if radiant_win:
                off2v2[key]['wins'] += 1
        
        return True
    
    def compute_results(self) -> Dict[str, Any]:
        """Compute final early/late counter matrices."""
        def to_winrate(stats: Dict[str, Dict[str, int]], min_games: int) -> Dict[str, float]:
            return {k: round(v['wins'] / v['games'], 4) 
                    for k, v in stats.items() if v['games'] >= min_games}
        
        return {
            'early': {
                'counter_1v1': to_winrate(self.early_1v1, MIN_MATCHES),
                'counter_2v1': to_winrate(self.early_2v1, 20),
                'synergy_2': to_winrate(self.early_syn_2, MIN_MATCHES),
                'synergy_3': to_winrate(self.early_syn_3, 15),
                'mid_1v1': to_winrate(self.early_mid_1v1, 20),
                'safe_2v2': to_winrate(self.early_safe_2v2, 15),
                'off_2v2': to_winrate(self.early_off_2v2, 15),
            },
            'late': {
                'counter_1v1': to_winrate(self.late_1v1, MIN_MATCHES),
                'counter_2v1': to_winrate(self.late_2v1, 20),
                'synergy_2': to_winrate(self.late_syn_2, MIN_MATCHES),
                'synergy_3': to_winrate(self.late_syn_3, 15),
                'mid_1v1': to_winrate(self.late_mid_1v1, 20),
                'safe_2v2': to_winrate(self.late_safe_2v2, 15),
                'off_2v2': to_winrate(self.late_off_2v2, 15),
            },
            'meta': {
                'early_games': self.early_count,
                'late_games': self.late_count,
            }
        }


def main() -> None:
    """Build early/late counters."""
    input_dir = 'bets_data/analise_pub_matches/json_parts_split_from_object'
    output_path = 'data/early_late_counters.json'
    
    all_files = sorted(glob(f"{input_dir}/combined*.json"))
    logger.info(f"Found {len(all_files)} pub match files")
    
    if not all_files:
        logger.error(f"No files in {input_dir}")
        return
    
    accumulator = EarlyLateAccumulator()
    
    for file_path in tqdm(all_files, desc="Processing files"):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for match_data in data.values():
                accumulator.process_match(match_data)
            del data
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
    
    logger.info(f"Early games: {accumulator.early_count}, Late games: {accumulator.late_count}")
    
    result = accumulator.compute_results()
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)
    
    logger.info(f"Saved to {output_path}")


if __name__ == '__main__':
    main()
