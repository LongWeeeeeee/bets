#!/bin/zsh
set -euo pipefail
cd /Users/alex/Documents/ingame
source venv_catboost/bin/activate
export MAP_ID_CHECK_PATH=/Users/alex/Documents/ingame/map_id_check_test.txt
export TEST_DISABLE_ADD_URL=1
export BOOKMAKER_PREFETCH_ENABLED=1
export BOOKMAKER_PREFETCH_USE_SUBPROCESS=1
export BOOKMAKER_PREFETCH_MODE=live
export STATS_EARLY_PATH=/Users/alex/Documents/ingame/bets_data/analise_pub_matches/test_dicts/early_dict_raw.json
export STATS_LATE_PATH=/Users/alex/Documents/ingame/bets_data/analise_pub_matches/test_dicts/late_dict_raw.json
export STATS_LANE_PATH=/Users/alex/Documents/ingame/bets_data/analise_pub_matches/lane_dict_raw.json
echo "=== START $(date '+%Y-%m-%d %H:%M:%S') ==="
/usr/bin/time -l python -u base/cyberscore_try.py --odds
rc=$?
echo "=== EXIT_CODE ${rc} at $(date '+%Y-%m-%d %H:%M:%S') ==="
exit ${rc}
