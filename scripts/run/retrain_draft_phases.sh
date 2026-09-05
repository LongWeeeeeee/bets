#!/usr/bin/env bash
# One offline run: fresh public/pro corpora, matched controls, four final models.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PY="$ROOT_DIR/venv_catboost/bin/python3"
RUN_NAME="${1:?usage: bash scripts/run/retrain_draft_phases.sh RUN_NAME}"
case "$RUN_NAME" in *[!a-zA-Z0-9_-]*) echo 'invalid run name' >&2; exit 2;; esac
ART_DIR="$ROOT_DIR/runtime/artifacts/draft-cp/$RUN_NAME"
DATA_DIR="$ROOT_DIR/data/draft_phase_corpus/$RUN_NAME"
MODEL_DIR="$ROOT_DIR/data/draft_phase_models/$RUN_NAME"
mkdir -p "$ART_DIR"
cd "$ROOT_DIR"
export PYTHONUNBUFFERED=1 OPENBLAS_NUM_THREADS=2 OMP_NUM_THREADS=2 VECLIB_MAXIMUM_THREADS=2
printf '%s\n' "$$" > "$ART_DIR/run.pid.tmp"
mv "$ART_DIR/run.pid.tmp" "$ART_DIR/run.pid"
STAGE=initializing
write_status() {
  "$PY" - "$ART_DIR/status.json" "$1" "$STAGE" "$$" "$MODEL_DIR" <<'PY'
import json, os, sys, time
path, state, stage, pid, output = sys.argv[1:]
with open(path+'.tmp', 'w') as f:
    json.dump(dict(state=state, stage=stage, pid=int(pid), updated_at=time.time(), models=output), f)
os.replace(path+'.tmp', path)
PY
}
finish() {
  rc=$?
  if [ "$rc" -eq 0 ]; then write_status DONE; else write_status FAIL; fi
}
trap finish EXIT
STAGE=public_corpus
write_status RUNNING
"$PY" -u base/build_draft_phase_corpus.py \
  --source bets_data/analise_pub_matches/json_parts_split_from_object \
  --output-dir "$DATA_DIR/public" --workers 2
STAGE=pro_corpus
write_status RUNNING
"$PY" -u base/build_draft_phase_corpus.py \
  --source pro_heroes_data/json_parts_split_from_object \
  --output-dir "$DATA_DIR/pro" --workers 2
STAGE=training
write_status RUNNING
"$PY" -u base/train_draft_phase_models.py \
  --corpus "$DATA_DIR/public" --pro-corpus "$DATA_DIR/pro" \
  --output-dir "$MODEL_DIR" --scratch "$ART_DIR/sparse_scratch" \
  --models early_nw late all early_win --threads 2 --resume
STAGE=complete
