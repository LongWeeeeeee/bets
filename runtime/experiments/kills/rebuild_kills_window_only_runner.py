#!/usr/bin/env python3
"""Kills-window-only explore rebuild (5-15 / 10-20 / 15-25 / 20-30).

Does not touch lane/early/late/post_lane production dictionaries.
"""
import os
import sys
import runpy

sys.path.insert(0, "/root/main")
sys.path.insert(0, "/root/main/base")

import keys

# Include all patches present under json_parts_split_from_object.
keys.start_date_time_739 = 0

os.environ.setdefault("EXPLORE_METRICS", "kills_window")
os.environ.setdefault(
    "EXPLORE_JSON_DIR",
    "/root/main/bets_data/analise_pub_matches/json_parts_split_from_object",
)
os.environ.setdefault(
    "EXPLORE_STATS_DIR",
    "/root/main/bets_data/analise_pub_matches",
)
os.environ.setdefault(
    "EXPLORE_TEST_SET_PATH",
    "/root/main/bets_data/analise_pub_matches/extracted_100k_matches.json",
)
os.environ.setdefault("EXPLORE_WRITE_JSON", "0")
os.environ.setdefault("EXPLORE_KEEP_SHARDS", "0")

runpy.run_path("/root/main/base/explore_database.py", run_name="__main__")
