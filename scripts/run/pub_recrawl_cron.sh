#!/bin/bash
# Hourly cron entry for the five-day public-match sweep, independent of any agent app.
#   23 * * * * /bin/bash /root/main/scripts/run/pub_recrawl_cron.sh >> /root/main/runtime/artifacts/pubs-rebuild/cron.log 2>&1
# Cheap skips (no heavy imports, no per-hour log files):
#   - a sweep holds runtime/pub_recrawl.lock  -> "skip busy"
#   - the last sweep completed and 5 days have not passed since it started -> "skip not_due"
# Otherwise hands over to get_pubs_full_recrawl.sh: it resumes a failed/stale sweep with the
# same cutoff and cursor, or starts the next one. pub_recrawl.py re-checks lock and interval.
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT" || exit 1
PY="$ROOT/venv/bin/python3"
[ -x "$PY" ] || PY="$ROOT/venv_catboost/bin/python3"
STAMP="$(date '+%Y-%m-%dT%H:%M:%S%z')"

if ! flock -n runtime/pub_recrawl.lock true; then
  echo "$STAMP skip busy"
  exit 0
fi

if [ -f runtime/pub_recrawl.json ]; then
  "$PY" - <<'EOF'
import json, sys, time
sys.path.insert(0, "base")
from pub_recrawl import INTERVAL_SECONDS  # light import: maps_research loads only in main()
state = json.load(open("runtime/pub_recrawl.json"))
if state.get("status") == "complete" and time.time() < state["started_at"] + INTERVAL_SECONDS:
    sys.exit(3)
EOF
  rc=$?
  if [ "$rc" -eq 3 ]; then
    echo "$STAMP skip not_due"
    exit 0
  fi
  # Any other non-zero code (unreadable state) falls through: the runner reports it in its log.
fi

echo "$STAMP launch"
exec bash scripts/run/get_pubs_full_recrawl.sh
