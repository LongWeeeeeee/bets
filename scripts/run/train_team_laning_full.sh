#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_NAME="${1:?run name required}"
case "$RUN_NAME" in *[!a-zA-Z0-9_-]*) exit 2;; esac
case "$RUN_NAME" in 2026090*) echo "protected model run name" >&2; exit 2;; esac
shift
cd "$ROOT_DIR"
PY="$ROOT_DIR/venv_catboost/bin/python3"
RUN_DIR="$ROOT_DIR/runtime/artifacts/laning/$RUN_NAME"
mkdir -p "$RUN_DIR"
export PYTHONUNBUFFERED=1 OPENBLAS_NUM_THREADS=2 OMP_NUM_THREADS=4 VECLIB_MAXIMUM_THREADS=2
status() {
  "$PY" - "$RUN_DIR/status.json" "$1" "$$" <<'PY'
import json, os, sys, time
path, state, pid = sys.argv[1:]
with open(path+'.tmp','w') as f:
    json.dump(dict(state=state,pid=int(pid),updated=time.time()),f)
os.replace(path+'.tmp',path)
PY
}
finish() { rc=$?; if [ "$rc" -eq 0 ]; then status DONE; else status FAIL; fi; }
trap finish EXIT
status RUNNING
"$PY" -u scripts/ops/train_team_laning_full.py \
  --corpus data/laning_corpus/20260908_stratz_v1 \
  --output-dir "data/laning_models/$RUN_NAME" \
  "$@" > "$RUN_DIR/run.log" 2>&1
