#!/bin/bash
# Submit a guarded five-day sweep; --since EPOCH is required only once.
# The runner owns the lock, cursor rollover, PID and retry window.
set -eu
umask 077
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
if [ "$ROOT" = /root/main ]; then
  PY="$ROOT/venv/bin/python3"
else
  PY="$ROOT/venv_catboost/bin/python3"
fi
export PYTHONUNBUFFERED=1
mkdir -p runtime/artifacts/pubs-rebuild
STAMP=$(date +%Y%m%d_%H%M%S)_$$
LOG="$ROOT/runtime/artifacts/pubs-rebuild/get_pubs_${STAMP}.log"
if command -v caffeinate >/dev/null 2>&1; then
  nohup caffeinate -i "$PY" base/pub_recrawl.py "$@" > "$LOG" 2>&1 < /dev/null &
else
  nohup "$PY" base/pub_recrawl.py "$@" > "$LOG" 2>&1 < /dev/null &
fi
echo "SUBMITTED_PID=$!"
echo "LOG=$LOG"
echo "State: $ROOT/runtime/pub_recrawl.json (PID is written only after acquiring the lock)"
