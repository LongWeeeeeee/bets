"""
Complex Hero Stats: Laning Matrix, Phase Synergy, Core Trio.

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

MIN_LANE_GAMES = 30
MIN_PHASE_GAMES = 20
MIN_TRIO_GAMES = 15
MIN_PAIR_GAMES = 30


def parse_lane_outcome(outcome: str) -> int:
    if outcome in ['RADIANT_VICTORY', 'RADIANT_STOMP']:
        return 1
    elif outcome in ['DIRE_VICTORY', 'DIRE_STOMP']:
        return -1
    return 0


def get_lane_positions(players: List[Dict], is_radiant: bool) -> Dict[str, List[int]]:
    team_players = [p for p in players if p.get('isRadiant') == is_radiant]
    positions: Dict[str, List[int]] = {'safe': [], 'mid': [], 'off': []}
    
    for p in team_players:
        hero_id = p.get('heroId', 0)
        pos = p.get('position', 'POSITION_5')
        pos_num = int(pos.replace('POSITION_', '')) if pos else 5
        
        if pos_num == 1 or pos_num == 5:
            positions['safe'].append(hero_id)
        elif pos_num == 2:
            positions['mid'].append(hero_id)
        elif pos_num == 3 or pos_num == 4:
            positions['off'].append(hero_id)
    
    return positions


def make_pair_key(h1: int, h2: int) -> str:
    return f"{min(h1, h2)}_{max(h1, h2)}"


def make_trio_key(h1: int, h2: int, h3: int) -> str:
    heroes = sorted([h1, h2, h3])
    return f"{heroes[0]}_{heroes[1]}_{heroes[2]}"


class ComplexStatsAccumulator:
    """Accumulates complex stats without storing raw data."""
    
    def __init__(self) -> None:
        # Laning: lane -> pair_key -> {'wins': int, 'total': int}
        self.laning_stats: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: {'wins': 0, 'total': 0})
        )
        # Phase synergy: pair_key -> {'early_wins', 'early_total', 'late_wins', 'late_total'}
        self.phase_synergy: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {'early_wins': 0, 'early_total': 0, 'late_wins': 0, 'late_total': 0}
        )
        # Core trio: trio_key -> {'wins', 'total'}
        self.core_trio: Dict[str, Dict[str, int]] = defaultdict(lambda: {'wins': 0, 'total': 0})
        self.valid_matches = 0
    
    def process_match(self, match: Dict[str, Any]) -> bool:
        """Process a single match. Returns True if valid."""
        players = match.get('players', [])
        if len(players) != 10:
            return False
        
        self.valid_matches += 1
        did_radiant_win = match.get('didRadiantWin', False)
        
        dire_kills = match.get('direKills', [])
        duration_min = len(dire_kills) if dire_kills else 35
        
        nw_leads = match.get('radiantNetworthLeads', [])
        lead_at_20 = nw_leads[20] if len(nw_leads) > 20 else 0
        
        bot_outcome = parse_lane_outcome(match.get('bottomLaneOutcome', ''))
        top_outcome = parse_lane_outcome(match.get('topLaneOutcome', ''))
        mid_outcome = parse_lane_outcome(match.get('midLaneOutcome', ''))
        
        radiant_pos = get_lane_positions(players, is_radiant=True)
        dire_pos = get_lane_positions(players, is_radiant=False)
        
        # Laning Matrix
        if radiant_pos['safe'] and dire_pos['off']:
            for rh in radiant_pos['safe']:
                for dh in dire_pos['off']:
                    key = make_pair_key(rh, dh)
                    self.laning_stats['radiant_safe'][key]['total'] += 1
                    if bot_outcome > 0:
                        self.laning_stats['radiant_safe'][key]['wins'] += 1
        
        if radiant_pos['off'] and dire_pos['safe']:
            for rh in radiant_pos['off']:
                for dh in dire_pos['safe']:
                    key = make_pair_key(rh, dh)
                    self.laning_stats['radiant_off'][key]['total'] += 1
                    if top_outcome > 0:
                        self.laning_stats['radiant_off'][key]['wins'] += 1
        
        if radiant_pos['mid'] and dire_pos['mid']:
            key = make_pair_key(radiant_pos['mid'][0], dire_pos['mid'][0])
            self.laning_stats['mid'][key]['total'] += 1
            if mid_outcome > 0:
                self.laning_stats['mid'][key]['wins'] += 1
        
        # Phase Synergy
        radiant_heroes = [p.get('heroId', 0) for p in players if p.get('isRadiant')]
        dire_heroes = [p.get('heroId', 0) for p in players if not p.get('isRadiant')]
        
        is_early = duration_min < 32 or abs(lead_at_20) > 7000
        is_late = duration_min > 40
        
        winning_heroes = radiant_heroes if did_radiant_win else dire_heroes
        losing_heroes = dire_heroes if did_radiant_win else radiant_heroes
        
        for i, h1 in enumerate(winning_heroes):
            for h2 in winning_heroes[i+1:]:
                key = make_pair_key(h1, h2)
                if is_early:
                    self.phase_synergy[key]['early_wins'] += 1
                    self.phase_synergy[key]['early_total'] += 1
                if is_late:
                    self.phase_synergy[key]['late_wins'] += 1
                    self.phase_synergy[key]['late_total'] += 1
        
        for i, h1 in enumerate(losing_heroes):
            for h2 in losing_heroes[i+1:]:
                key = make_pair_key(h1, h2)
                if is_early:
                    self.phase_synergy[key]['early_total'] += 1
                if is_late:
                    self.phase_synergy[key]['late_total'] += 1
        
        # Core Trio
        radiant_cores = []
        dire_cores = []
        for p in players:
            pos = p.get('position', 'POSITION_5')
            pos_num = int(pos.replace('POSITION_', '')) if pos else 5
            hero_id = p.get('heroId', 0)
            if pos_num <= 3:
                if p.get('isRadiant'):
                    radiant_cores.append(hero_id)
                else:
                    dire_cores.append(hero_id)
        
        if len(radiant_cores) == 3:
            key = make_trio_key(*radiant_cores)
            self.core_trio[key]['total'] += 1
            if did_radiant_win:
                self.core_trio[key]['wins'] += 1
        
        if len(dire_cores) == 3:
            key = make_trio_key(*dire_cores)
            self.core_trio[key]['total'] += 1
            if not did_radiant_win:
                self.core_trio[key]['wins'] += 1
        
        return True
    
    def compute_results(self) -> Dict[str, Any]:
        """Compute final complex stats."""
        # Laning Matrix
        laning_matrix: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for lane, pairs in self.laning_stats.items():
            laning_matrix[lane] = {}
            for key, stats in pairs.items():
                if stats['total'] >= MIN_LANE_GAMES:
                    laning_matrix[lane][key] = {
                        'win_rate': round(stats['wins'] / stats['total'], 4),
                        'games': stats['total'],
                    }
        
        # Phase Synergy
        phase_matrix: Dict[str, Dict[str, Any]] = {}
        for key, stats in self.phase_synergy.items():
            early_wr = None
            late_wr = None
            if stats['early_total'] >= MIN_PHASE_GAMES:
                early_wr = round(stats['early_wins'] / stats['early_total'], 4)
            if stats['late_total'] >= MIN_PHASE_GAMES:
                late_wr = round(stats['late_wins'] / stats['late_total'], 4)
            if early_wr is not None or late_wr is not None:
                phase_matrix[key] = {
                    'early_winrate': early_wr,
                    'late_winrate': late_wr,
                    'early_games': stats['early_total'],
                    'late_games': stats['late_total'],
                }
        
        # Core Trio
        trio_matrix: Dict[str, Dict[str, Any]] = {}
        for key, stats in self.core_trio.items():
            if stats['total'] >= MIN_TRIO_GAMES:
                trio_matrix[key] = {
                    'winrate': round(stats['wins'] / stats['total'], 4),
                    'games': stats['total'],
                }
        
        # Pair winrates fallback
        pair_winrates: Dict[str, float] = {}
        for key, stats in self.phase_synergy.items():
            total = stats['early_total'] + stats['late_total']
            wins = stats['early_wins'] + stats['late_wins']
            if total >= MIN_PAIR_GAMES:
                pair_winrates[key] = round(wins / total, 4) if total > 0 else 0.5
        
        return {
            'laning_matrix': laning_matrix,
            'phase_synergy': phase_matrix,
            'core_trio': trio_matrix,
            'pair_winrates': pair_winrates,
        }


def build_complex_stats(
    pub_path: str = 'bets_data/analise_pub_matches/json_parts_split_from_object',
    output_path: str = 'data/complex_hero_stats.json'
) -> Dict[str, Any]:
    """Build complex stats."""
    all_files = sorted(glob(f"{pub_path}/combined*.json"))
    logger.info(f"Found {len(all_files)} pub match files")
    
    if not all_files:
        logger.error(f"No files in {pub_path}")
        return {}
    
    accumulator = ComplexStatsAccumulator()
    
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
    
    logger.info(f"Laning pairs: {sum(len(v) for v in result['laning_matrix'].values())}")
    logger.info(f"Phase synergy pairs: {len(result['phase_synergy'])}")
    logger.info(f"Core trios: {len(result['core_trio'])}")
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)
    
    logger.info(f"Saved to {output_path}")
    return result


if __name__ == '__main__':
    build_complex_stats()
