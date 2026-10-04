#!/bin/bash
# Nightly kills-v3 serving state for prematch panel B.
#
# What: replays the kills-v3 history (base/kills_v3_serving.py build-state) up to
# "now" with the SAME history_start and visibility_delay the deployed model was
# trained with (read from ml-models/prematch_panel_kv3/manifest.json), checks the
# file loads, and optionally delivers it to the production checkout atomically.
#
# Two hosts, one script (scripts/run/lib_pro_chain.sh): on the Mac the production
# checkout is serv1:/root/main (ssh/scp); on serv1 the build runs in a SEPARATE
# checkout (/root/pro_chain) and delivers into /root/main on the same host by cp.
# PRO_CHAIN_SHADOW=1 builds the state and delivers nothing (only a line in
# $PRO_CHAIN_SUMMARY).
#
# Why a separate build tree: build-state peaks at 3.33 GB RSS (measured 2026-09-24,
# E-329); serv1 has ~2.7 GB free next to the 3.65 GB prod process, so the build
# must be scheduled when the prod process is not squeezed, and it must never write
# into the production checkout. The state file is ~220 MB.
#
# Sources: runtime/artifacts/misc/pro_corpus_rich.npz (rebuilt by the nightly
# prematch chain) + the OpenDota timeline DBs. Run AFTER the rich rebuild.
#
# Delivery is opt-in: only with --deliver, and the nightly hook in
# scripts/run/rebuild_prematch_snapshot.sh passes it only while the marker file
# runtime/kv3_state_deliver.on exists. Panel B reloads the state when its
# mtime/size changes (base/kv3_panel_serving.py); no restart is needed.
#
# Usage: scripts/ops/build_kv3_state.sh [--deliver]
# Output: data/kills_v3_state/state.npz (build tree), $PROD_ROOT/data/kills_v3_state/state.npz
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../run/lib_pro_chain.sh"
cd "$ROOT"
pro_chain_guard || exit 2
export PRO_CHAIN_MODE PROD_ROOT PRO_CHAIN_SHADOW PRO_CHAIN_SUMMARY
MODEL_DIR=${KV3_SHADOW_DIR:-ml-models/prematch_panel_kv3}
OUT_DIR=data/kills_v3_state
DELIVER=0
[ "${1:-}" = "--deliver" ] && DELIVER=1

[ -f "$MODEL_DIR/manifest.json" ] || { echo "kv3 state: no $MODEL_DIR/manifest.json, nothing to build"; exit 0; }
read -r HISTORY_START DELAY HL PG LR EK < <($PY - "$MODEL_DIR/manifest.json" <<'PY'
import json, sys
m = json.load(open(sys.argv[1]))
p = m["od3_parameters"]
print(int(p["history_start"]), int(p["visibility_delay"]), p["half_life_days"], p["pseudo_games"], p["poisson_lr"], p["elo_k"])
PY
)
CUTOFF=$(date +%s)
mkdir -p "$OUT_DIR"
TMP="$OUT_DIR/state.npz.tmp"
rm -f "$TMP"
echo "kv3 state: build cutoff=$CUTOFF history_start=$HISTORY_START delay=$DELAY params=$HL/$PG/$LR/$EK $(date '+%F %T')"
nice -n 10 $PY -m base.kills_v3_serving build-state --cutoff "$CUTOFF" --history-start "$HISTORY_START" \
  --visibility-delay "$DELAY" --half-life-days "$HL" --pseudo-games "$PG" --poisson-lr "$LR" --elo-k "$EK" \
  --output "$TMP"
# Freshness: the newest replayed or pending event must be recent, otherwise the
# source chain (corpus top-up / rich rebuild) has stalled and the state is stale.
$PY - "$TMP" "$CUTOFF" <<'PY'
import os, sys
sys.path.insert(0, os.getcwd())  # build tree: the script did cd "$ROOT"
from base import kills_v3_serving as sv
import numpy as np
st = sv.load_state(sys.argv[1])
cutoff = int(sys.argv[2])
m = st.serving_meta
with np.load(m["rich_path"], allow_pickle=False) as z:
    ends = z["ts"].astype(np.int64) + z["durations"].astype(np.int64)
newest = int(ends[ends < cutoff].max()) if (ends < cutoff).any() else 0
age_h = (cutoff - newest) / 3600 if newest else float("nan")
print(f"kv3 state: loaded events_replayed={m['events_replayed']} pending={m.get('events_pending')} newest_rich_map_end_age_h={age_h:.1f}")
if not newest or age_h > 36:
    print("ВНИМАНИЕ: свежесть kv3 state: newest event older than 36 h - source chain stalled?")
PY
mv "$TMP" "$OUT_DIR/state.npz"
L=$(sha1_of "$OUT_DIR/state.npz")
echo "kv3 state: local ready sha1 $L $(du -h "$OUT_DIR/state.npz" | cut -f1)"

[ "$DELIVER" = 1 ] || exit 0
prod_run "mkdir -p $PROD_ROOT/$OUT_DIR"
if ! prod_stage "$OUT_DIR/state.npz" "$OUT_DIR/state.npz"; then
  echo "ВНИМАНИЕ: kv3 state доехал битым (против $L) — на проде остался прежний"
  exit 1
fi
prod_commit "$OUT_DIR/state.npz"
if shadow_on; then
  echo "kv3 state: [тень] не доставлен, sha1 $L"
else
  echo "kv3 state доставлен: sha1 $L"
fi
