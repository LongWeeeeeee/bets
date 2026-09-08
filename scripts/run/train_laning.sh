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
STAGE=corpus
status() {
  "$PY" - "$RUN_DIR/status.json" "$1" "$STAGE" "$$" <<'PY'
import json, os, sys, time
path, state, stage, pid = sys.argv[1:]
with open(path+'.tmp','w') as f:
    json.dump(dict(state=state,stage=stage,pid=int(pid),updated=time.time()),f)
os.replace(path+'.tmp',path)
PY
}
finish() { rc=$?; if [ "$rc" -eq 0 ]; then status DONE; else status FAIL; fi; }
trap finish EXIT
status RUNNING
"$PY" -m base.build_laning_corpus --source bets_data/analise_pub_matches/json_parts_split_from_object \
  --output-dir "data/laning_corpus/$RUN_NAME" --workers 2
STAGE=training
status RUNNING
"$PY" -u scripts/ops/train_laning_model.py --corpus "data/laning_corpus/$RUN_NAME" \
  --output-dir "data/laning_models/$RUN_NAME" --train-maps 400000 --eval-maps 100000 --iterations 300 --threads 4
STAGE=complete
