#!/bin/bash
# Build-tree-only cache and ELO maps; partial API nights still convert the cache.
set -u
source "$(dirname "${BASH_SOURCE[0]}")/../run/lib_pro_chain.sh"
pro_chain_guard || exit 2
cd "$ROOT" || exit 2

CORPUS_IDS="$ROOT/pro_heroes_data/json_parts_split_from_object/processed_ids.txt"
CACHE_DIR="$ROOT/runtime/artifacts/elo/opendota_supplement"
OUTPUT="$ROOT/data/elo_supplement/opendota_maps.json"

fetch_rc=0
"$PY" "$ROOT/scripts/pro_chain/od_explorer_fetch.py" \
  --cache-dir "$CACHE_DIR" --corpus-ids "$CORPUS_IDS" || fetch_rc=$?
case "$fetch_rc" in
  0|3) ;;
  *) exit "$fetch_rc" ;;
esac

# The converter fsyncs a temporary sibling and replaces OUTPUT only on success.
# Neither a fetch outage nor a conversion failure truncates yesterday's maps.
"$PY" "$ROOT/scripts/pro_chain/build_elo_supplement.py" \
  --input-dir "$CACHE_DIR" --exclude-corpus-ids "$CORPUS_IDS" --output "$OUTPUT" || exit $?
exit "$fetch_rc"
