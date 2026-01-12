"""
Build Hero Lane Matchup Matrix from public matches.

Uses streaming to handle large datasets without loading everything into memory.
"""

import json
import logging
from collections import defaultdict
from glob import glob
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

LANE_OUTCOME_MAP: Dict[str, int] = {
    'RADIANT_STOMP': 2,
    'RADIANT_VICTORY': 1,
    'TIE': 0,
    'DIRE_VICTORY': -1,
    'DIRE_STOMP': -2,
}


def parse_lane_outcome(outcome: Optional[str]) -> int:
    return LANE_OUTCOME_MAP.get(outcome, 0) if outcome else 0


def get_lane_heroes(players: List[Dict]) -> Dict[str, Tuple[int, int]]:
    """Extract heroes by lane."""
    radiant = {p.get('position', ''): p.get('heroId', 0) for p in players if p.get('isRadiant')}
    dire = {p.get('position', ''): p.get('heroId', 0) for p in players if not p.get('isRadiant')}
    
    return {
        'mid': (radiant.get('POSITION_2', 0), dire.get('POSITION_2', 0)),
        'safe': (radiant.get('POSITION_1', 0), dire.get('POSITION_3', 0)),
        'off': (radiant.get('POSITION_3', 0), dire.get('POSITION_1', 0)),
    }


class LaneMatchupAccumulator:
    """Accumulates lane matchup stats without storing raw data."""
    
    def __init__(self) -> None:
        # matchup_key -> {'sum_outcome': float, 'sum_gold': float, 'wins': int, 'stomps': int, 'count': int}
        self.matchups: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {'sum_outcome': 0.0, 'sum_gold': 0.0, 'wins': 0, 'stomps': 0, 'count': 0}
        )
        self.valid_matches = 0
    
    def process_match(self, match: Dict[str, Any]) -> bool:
        """Process a single match. Returns True if valid."""
        players = match.get('players', [])
        if len(players) != 10:
            return False
        
        lane_heroes = get_lane_heroes(players)
        nw_leads = match.get('radiantNetworthLeads', [])
        gold_10 = nw_leads[10] if len(nw_leads) > 10 else (nw_leads[-1] if nw_leads else 0)
        
        self.valid_matches += 1
        
        # Mid lane
        mid_outcome = parse_lane_outcome(match.get('midLaneOutcome'))
        r_mid, d_mid = lane_heroes['mid']
        if r_mid and d_mid:
            self._add_matchup(f"{r_mid}_{d_mid}", mid_outcome, gold_10 / 5)
            self._add_matchup(f"{d_mid}_{r_mid}", -mid_outcome, -gold_10 / 5)
        
        # Safe lane
        safe_outcome = parse_lane_outcome(match.get('bottomLaneOutcome'))
        r_carry, d_off = lane_heroes['safe']
        if r_carry and d_off:
            self._add_matchup(f"{r_carry}_{d_off}", safe_outcome, gold_10 / 5)
            self._add_matchup(f"{d_off}_{r_carry}", -safe_outcome, -gold_10 / 5)
        
        # Off lane
        off_outcome = parse_lane_outcome(match.get('topLaneOutcome'))
        r_off, d_carry = lane_heroes['off']
        if r_off and d_carry:
            self._add_matchup(f"{r_off}_{d_carry}", off_outcome, gold_10 / 5)
            self._add_matchup(f"{d_carry}_{r_off}", -off_outcome, -gold_10 / 5)
        
        return True
    
    def _add_matchup(self, key: str, outcome: int, gold_diff: float) -> None:
        stats = self.matchups[key]
        stats['sum_outcome'] += outcome
        stats['sum_gold'] += gold_diff
        stats['count'] += 1
        if outcome > 0:
            stats['wins'] += 1
        if outcome >= 2:
            stats['stomps'] += 1
    
    def compute_results(self, min_games: int = 10) -> Dict[str, Dict[str, float]]:
        """Compute final lane matchup stats."""
        result: Dict[str, Dict[str, float]] = {}
        
        for key, stats in self.matchups.items():
            if stats['count'] < min_games:
                continue
            
            result[key] = {
                'winrate': round(stats['wins'] / stats['count'], 4),
                'gold_diff': round(stats['sum_gold'] / stats['count'], 1),
                'stomp_rate': round(stats['stomps'] / stats['count'], 4),
                'matches': stats['count'],
            }
        
        return result


def main(
    input_path: str = 'bets_data/analise_pub_matches/json_parts_split_from_object',
    output_path: str = 'data/hero_lane_matchups.json'
) -> None:
    """Build lane matchups."""
    all_files = sorted(glob(f"{input_path}/combined*.json"))
    logger.info(f"Found {len(all_files)} pub match files")
    
    if not all_files:
        logger.error(f"No files in {input_path}")
        return
    
    accumulator = LaneMatchupAccumulator()
    
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
    logger.info(f"Matchups with >= 10 games: {len(result)}")
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(result, f, indent=2)
    
    logger.info(f"Saved to {output_path}")


if __name__ == '__main__':
    main()
