#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_NAME="${1:?run name required}"
case "$RUN_NAME" in *[!a-zA-Z0-9_-]*) exit 2;; esac
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
"$PY" -u scripts/ops/train_team_laning_model.py \
  --corpus data/laning_corpus/20260908_stratz_v1 \
  --v1-model data/laning_models/20260908_stratz_v1 \
  --v2-model data/laning_models/20260908_stratz_v2 \
  --output-dir "data/laning_models/$RUN_NAME" \
  --train-maps 800000 --eval-maps 100000 --iterations 600 --threads 4
