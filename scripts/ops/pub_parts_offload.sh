#!/bin/bash
# Mac-side: move finished pub-corpus part files from serv1 to the Mac, then free them on serv1.
# Logic and guards live in scripts/ops/pub_parts_offload.py (fail closed; see its docstring).
# Usage: bash scripts/ops/pub_parts_offload.sh [--dry-run] [--allow-busy-sweep] ...
# Scheduled daily 06:40 by scripts/ops/launchd/com.ingame.pub-parts-offload.plist (not auto-installed).
set -u
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT" || exit 1
PY=/Users/alex/Documents/ingame/venv_catboost/bin/python3
[ -x "$PY" ] || { echo "pub_parts_offload: venv python missing: $PY" >&2; exit 1; }
exec "$PY" "$ROOT/scripts/ops/pub_parts_offload.py" "$@"
