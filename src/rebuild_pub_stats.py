#!/usr/bin/env python3
"""
Rebuild ALL pub-based statistics from full dataset.

This script runs all build_* scripts that depend on public match data,
using ALL available pub match files (not just extracted_100k_matches.json).

WARNING: This will take a LONG time (hours) due to ~21GB of data.

Run from project root: python src/rebuild_pub_stats.py
"""

import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Get project root and add src to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Run all pub-based stat builders."""
    # Ensure we're in project root for relative paths in build scripts
    os.chdir(PROJECT_ROOT)
    logger.info(f"Working directory: {os.getcwd()}")
    
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("REBUILDING ALL PUB-BASED STATISTICS")
    logger.info(f"Started at: {datetime.now()}")
    logger.info("=" * 60)
    
    # 1. Blood Stats (hero blood scores, synergies, clashes)
    logger.info("\n[1/11] Building Blood Stats...")
    try:
        from build_blood_stats import build_blood_stats
        build_blood_stats()
        logger.info("Blood Stats: DONE")
    except Exception as e:
        logger.error(f"Blood Stats FAILED: {e}", exc_info=True)
    
    # 2. Early/Late Counters (1v1, 2v1, synergies by game phase)
    logger.info("\n[2/11] Building Early/Late Counters...")
    try:
        from build_early_late_counters import main as build_counters
        build_counters()
        logger.info("Early/Late Counters: DONE")
    except Exception as e:
        logger.error(f"Early/Late Counters FAILED: {e}", exc_info=True)
    
    # 3. Lane Matchups (lane winrates, gold diffs)
    logger.info("\n[3/11] Building Lane Matchups...")
    try:
        from build_lane_matrix import main as build_lanes
        build_lanes()
        logger.info("Lane Matchups: DONE")
    except Exception as e:
        logger.error(f"Lane Matchups FAILED: {e}", exc_info=True)
    
    # 4. Hero Power Spikes (early/mid/late winrates)
    logger.info("\n[4/11] Building Hero Power Spikes...")
    try:
        from build_hero_power_spikes import build_hero_power_spikes
        build_hero_power_spikes()
        logger.info("Hero Power Spikes: DONE")
    except Exception as e:
        logger.error(f"Hero Power Spikes FAILED: {e}", exc_info=True)
    
    # 5. Wave Clear Stats
    logger.info("\n[5/11] Building Wave Clear Stats...")
    try:
        from build_wave_clear import build_wave_clear_stats
        build_wave_clear_stats()
        logger.info("Wave Clear Stats: DONE")
    except Exception as e:
        logger.error(f"Wave Clear Stats FAILED: {e}", exc_info=True)
    
    # 6. Hero Push Stats
    logger.info("\n[6/11] Building Hero Push Stats...")
    try:
        from build_hero_push_stats import build_hero_push_stats
        build_hero_push_stats()
        logger.info("Hero Push Stats: DONE")
    except Exception as e:
        logger.error(f"Hero Push Stats FAILED: {e}", exc_info=True)
    
    # 7. Complex Stats (laning matrix, phase synergy, core trio)
    logger.info("\n[7/11] Building Complex Stats...")
    try:
        from build_complex_stats import build_complex_stats
        build_complex_stats()
        logger.info("Complex Stats: DONE")
    except Exception as e:
        logger.error(f"Complex Stats FAILED: {e}", exc_info=True)
    
    # 8. Synergy Matrix (hero synergies and counter picks)
    logger.info("\n[8/11] Building Synergy Matrix...")
    try:
        from build_synergy_matrix import build_synergy_matrix
        build_synergy_matrix()
        logger.info("Synergy Matrix: DONE")
    except Exception as e:
        logger.error(f"Synergy Matrix FAILED: {e}", exc_info=True)
    
    # 9. Hero Stats (aggression, feed, pace, gpm)
    logger.info("\n[9/11] Building Hero Stats...")
    try:
        from build_hero_stats import main as build_hero_stats
        build_hero_stats(
            input_dir='bets_data/analise_pub_matches/json_parts_split_from_object',
            output_path='data/hero_public_stats.csv'
        )
        logger.info("Hero Stats: DONE")
    except Exception as e:
        logger.error(f"Hero Stats FAILED: {e}", exc_info=True)
    
    # 10. Hero Healing Stats (healing per min, save heroes)
    logger.info("\n[10/11] Building Hero Healing Stats...")
    try:
        from build_hero_healing import build_hero_healing_stats
        build_hero_healing_stats()
        logger.info("Hero Healing Stats: DONE")
    except Exception as e:
        logger.error(f"Hero Healing Stats FAILED: {e}", exc_info=True)
    
    # 11. Greed Index (depends on hero_public_stats.csv)
    logger.info("\n[11/11] Building Greed Index...")
    try:
        from build_greed_index import main as build_greed
        build_greed()
        logger.info("Greed Index: DONE")
    except Exception as e:
        logger.error(f"Greed Index FAILED: {e}", exc_info=True)
    
    elapsed = time.time() - start_time
    hours = int(elapsed // 3600)
    minutes = int((elapsed % 3600) // 60)
    
    logger.info("\n" + "=" * 60)
    logger.info("REBUILD COMPLETE")
    logger.info(f"Total time: {hours}h {minutes}m")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
