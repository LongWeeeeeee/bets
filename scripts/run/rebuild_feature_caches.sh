#!/bin/bash
# Пересборка позиционных кэшей признаков на ТЕКУЩЕМ корпусе (1.26M карт).
#
# ЗАЧЕМ. `build_prematch_artifact.py:208` падает с «РАССИНХРОН КОРПУСА И КЭШЕЙ»:
# кэши собраны 12.08 на корпусе 482 486 карт, корпус с тех пор вырос до
# 1 260 420. Кэши адресуются ПОЗИЦИЕЙ в массиве, а не match_id, поэтому
# переобучение весов на таком входе невозможно, и ночная цепочка всегда идёт
# PREMATCH_SNAPSHOT_ONLY=1 — прод скорит дрейфующие признаки летними весами.
#
# ПОРЯДОК ЗАВИСИМОСТЕЙ (каждый следующий читает предыдущие):
#   EXT (strong_cohesion_followup) -> LOGIT (rebuild_draft_logit_cache) ->
#   B1 (ideas_batch1) -> B2 (ideas_batch2) -> B5 (ideas_batch5) ->
#   B5b (ideas_batch5b) -> FULLLOGIT (ideas_batch7c)
#
# Все билдеры ПЕРЕИСПОЛЬЗУЮТ кэш, если файл существует, поэтому старые кэши
# сначала ОТКЛАДЫВАЮТСЯ в _stale_482k_<stamp>/ (перемещение, не удаление —
# откат возможен). Отчёты .md копируются туда же: билдеры перезапишут их
# свежими числами, а старые цифры процитированы в журнале экспериментов.
#
#combined_model_eval.py НАМЕРЕННО не запускается целиком: он читает
# pro_series_map.npz (12.08) и на текущем корпусе упал бы на формах; вместо
# него драфт-логит строит rebuild_draft_logit_cache.py (та же функция
# draft_logit(), без лестницы оценок).
#
# Запуск (фоном, с ожиданием других тяжёлых процессов — 16 ГБ RAM на двоих не
# хватает, ELO-пересборка снимка ест ~7 ГБ):
#   WAIT_PID=50833 nohup bash scripts/run/rebuild_feature_caches.sh \
#     > runtime/rebuild_feature_caches.log 2>&1 &
set -euo pipefail
cd /Users/alex/Documents/ingame
PY=venv_catboost/bin/python3
ART=runtime/artifacts/misc
MISC=runtime/experiments/misc
STAMP=$(date +%Y%m%d_%H%M)
BK="$ART/_stale_482k_$STAMP"
LOGDIR="runtime/rebuild_caches_$STAMP"

say() { echo "[$(date '+%F %T')] $*"; }

# 0. Не стартовать поверх другого тяжёлого прогона (ELO-снимок, ночная цепочка).
WAIT_PID="${WAIT_PID:-}"
if [ -n "$WAIT_PID" ] && kill -0 "$WAIT_PID" 2>/dev/null; then
  say "жду завершения PID $WAIT_PID перед стартом (RAM)..."
  while kill -0 "$WAIT_PID" 2>/dev/null; do sleep 60; done
  say "PID $WAIT_PID завершился, продолжаю"
fi

mkdir -p "$BK" "$LOGDIR"
CACHES="pro_features_ext.npz pro_draft_logit.npz ideas_batch1.npz ideas_batch2.npz
ideas_batch5.npz ideas_batch5b.npz pro_draft_logit_full.npz"
for f in $CACHES; do
  if [ -f "$ART/$f" ]; then
    mv "$ART/$f" "$BK/$f"
    say "отложен старый кэш: $f -> $BK/"
  fi
done
for r in ideas_batch1.md ideas_batch2.md ideas_batch5.md ideas_batch5b.md \
         ideas_batch7c.md combined_model_eval.md strong_cohesion_followup.md; do
  [ -f "$ART/$r" ] && cp -p "$ART/$r" "$BK/$r"
done
say "бэкап: $BK"

step() {
  name="$1"; shift
  say "=== ШАГ: $name"
  if "$@" > "$LOGDIR/$name.log" 2>&1; then
    say "    готово ($name), последние строки:"
    tail -3 "$LOGDIR/$name.log" | sed 's/^/    | /'
  else
    rc=$?
    say "ОШИБКА на шаге $name (rc=$rc), лог: $LOGDIR/$name.log"
    tail -15 "$LOGDIR/$name.log" | sed 's/^/    | /'
    exit "$rc"
  fi
}

step ext          $PY $MISC/strong_cohesion_followup.py
step draft_logit  $PY $MISC/rebuild_draft_logit_cache.py
step batch1       $PY $MISC/ideas_batch1.py
step batch2       $PY $MISC/ideas_batch2.py
step batch5       $PY $MISC/ideas_batch5.py
step batch5b      $PY $MISC/ideas_batch5b.py
step fulllogit    $PY $MISC/ideas_batch7c.py

say "=== ПРОВЕРКА ДЛИН (кэши обязаны совпасть с корпусом)"
$PY - <<'CHECK'
import sys
from pathlib import Path
import numpy as np

ART = Path("runtime/artifacts/misc")
zc = np.load(ART / "pro_corpus_compact.npz")
zr = np.load(ART / "pro_corpus_rich.npz")
n_compact = int(zc["mids"].shape[0])
pos = {int(m) for m in zr["mids"].tolist()}
in_rich = sum(1 for m in zc["mids"].tolist() if int(m) in pos)
print(f"compact строк: {n_compact:,}; из них в rich: {in_rich:,}")

rows = {}
for name in ("pro_features_ext", "ideas_batch1", "ideas_batch2",
             "ideas_batch5", "ideas_batch5b"):
    rows[name] = int(np.load(ART / f"{name}.npz")["F"].shape[0])
for name in ("pro_draft_logit", "pro_draft_logit_full"):
    rows[name] = int(np.load(ART / f"{name}.npz")["logit"].shape[0])

bad = []
for k, v in rows.items():
    flag = "" if v == n_compact else "  <-- РАСХОЖДЕНИЕ"
    if v != n_compact:
        bad.append(k)
    print(f"  {k}: {v:,}{flag}")
if bad:
    print(f"ПРОВАЛ: не совпали с корпусом: {bad}")
    sys.exit(1)
print("ВСЕ КЭШИ СИНХРОННЫ С КОРПУСОМ — build_prematch_artifact без SNAPSHOT_ONLY разблокирован")
CHECK

say "=== ЦЕПОЧКА ЗАВЕРШЕНА: $LOGDIR, бэкап старых кэшей: $BK"
say "СЛЕДУЮЩИЙ ШАГ (отдельное решение): сверить пересобранный draft_logit с живым"
say "кодировщиком (E-201), затем build_prematch_artifact.py БЕЗ SNAPSHOT_ONLY и"
say "forward-валидация весов."
