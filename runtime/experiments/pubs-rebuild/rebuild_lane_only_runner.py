#!/usr/bin/env python3
"""Lane-only explore rebuild with correct import order + full patch window."""
import os
import sys
import runpy

# base MUST be first — root /root/main/maps_research.py is stale and empties
# the hero position catalog, which rejects every match.
sys.path.insert(0, "/root/main")
sys.path.insert(0, "/root/main/base")

import keys

# Include all patches present under json_parts_split_from_object (same as prior rebuilds).
keys.start_date_time_739 = 0

os.environ.setdefault("EXPLORE_METRICS", "lane")
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
