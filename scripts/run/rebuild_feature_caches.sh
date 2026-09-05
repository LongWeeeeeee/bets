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
# ВНИМАНИЕ: ЭТА ЦЕПОЧКА ПОКРЫВАЕТ СЕМЬ КЭШЕЙ ИЗ ДВЕНАДАТИ позиционных (E-258).
# За её пределами остаются и блокируют рефит:
#   undercount_retry_cache.npz (G)  — ПЕРВЫЕ 23 КОЛОНКИ ИЗ 35, включая
#                                     draft_logit и elo; не пересобирался с 13.08
#   ideas_batch8.npz                — cp_lane, syn_pos_mean
#   h2h_as_served.npz               — h2h_resid, выровнен по строкам матрицы
#   map_winner_hybrid_quality_forward/hybrid_features.npz — задаёт ПОРЯДОК СТРОК
#                                     всей матрицы, поэтому без него остальные
#                                     не к чему привязать
#   hybrid_strength_tier3.npz       — keyed по mid, длина свободна, но своей эпохи
# Итоговая проверка ниже теперь сверяет ВСЕ двенадцать и на текущем состоянии
# честно падает с кодом 1, перечисляя, что именно не пересобрано. Раньше она
# сверяла только свой список CACHES и печатала «разблокировано», после чего
# win_model_matrix_cache.py падал на np.column_stack (482 486 против 1 260 895)
# с сообщением про формы массивов, не называющим ни одного кэша.
#
# ОКНО. Ночной добор растит корпус КАЖДОЕ УТРО (pro_corpus_compact.npz
# пересобирается в ~05:32), поэтому пересборка устаревает к следующему утру, а
# доопознанные карты ВСТАВЛЯЮТСЯ в середину хвоста — последние ~150 строк кэша
# начинают тихо указывать на соседние матчи. Значит пересборка ВСЕХ двенадцати и
# рефит обязаны уложиться в одно окно между двумя доборами, иначе промежуточное
# состояние хуже исходного.
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

say "=== ПРОВЕРКА ДЛИН: ВСЕ позиционные кэши, а не только пересобранные здесь"
$PY - <<'CHECK'
import sys
from pathlib import Path
import numpy as np

ART = Path("runtime/artifacts/misc")
HYB = ART / "map_winner_hybrid_quality_forward" / "hybrid_features.npz"

zc = np.load(ART / "pro_corpus_compact.npz")
zr = np.load(ART / "pro_corpus_rich.npz")
n_compact = int(zc["mids"].shape[0])
n_rich = int(zr["mids"].shape[0])
pos = {int(m) for m in zr["mids"].tolist()}
in_rich = sum(1 for m in zc["mids"].tolist() if int(m) in pos)
print(f"корпус: compact {n_compact:,} строк, rich {n_rich:,}, compact-в-rich {in_rich:,}")

REBUILT_HERE = {"pro_features_ext", "pro_draft_logit", "pro_draft_logit_full",
                "ideas_batch1", "ideas_batch2", "ideas_batch5", "ideas_batch5b"}

def nrows(path, key):
    if not path.exists():
        return None
    return int(np.load(path, allow_pickle=True)[key].shape[0])

def tag(name):
    return "пересобран здесь" if name in REBUILT_HERE else "НЕ пересобирается этим скриптом"

# ГРУППА 1 — выровнены ПОЗИЦИЕЙ по compact-корпусу после фильтра keep
# (`keep = [mid in rich]` в undercount_next.py:110 и ideas_batch8.py:125).
CORPUS = [("pro_features_ext", "F"), ("pro_draft_logit", "logit"),
          ("pro_draft_logit_full", "logit"), ("ideas_batch1", "F"),
          ("ideas_batch2", "F"), ("ideas_batch5", "F"), ("ideas_batch5b", "F"),
          ("ideas_batch8", "F"), ("undercount_retry_cache", "G")]
# ГРУППА 2 — выровнены по порядку строк hybrid_features; он же задаёт строки
# матрицы (win_model_matrix_cache.py сверяет len(h2h) с len(X)).
MATRIX = [("h2h_as_served", "value")]
# ГРУППА 3 — keyed ПО mid, несут собственный mids, длина вправе отличаться.
MIDKEYED = [("hybrid_strength_tier3", "value")]

bad = []
print("\n--- выровнены по корпусу (обязаны равняться compact) ---")
for name, key in CORPUS:
    n = nrows(ART / f"{name}.npz", key)
    if n is None:
        print(f"  {name:26s} ОТСУТСТВУЕТ  [{tag(name)}]")
        bad.append(name)
        continue
    flag = "" if n == n_compact else "  <-- РАСХОЖДЕНИЕ"
    if n != n_compact:
        bad.append(name)
    print(f"  {name:26s} {n:>10,}{flag}  [{tag(name)}]")

n_hyb = nrows(HYB, "mids")
print("\n--- задаёт порядок строк матрицы ---")
if n_hyb is None:
    print(f"  {'hybrid_features':26s} ОТСУТСТВУЕТ  [{tag('hybrid_features')}]")
    bad.append("hybrid_features")
else:
    flag = "" if n_hyb == n_compact else "  <-- РАСХОЖДЕНИЕ"
    if n_hyb != n_compact:
        bad.append("hybrid_features")
    print(f"  {'hybrid_features':26s} {n_hyb:>10,}{flag}  [{tag('hybrid_features')}]")

print("\n--- выровнены по порядку строк матрицы ---")
ref = n_hyb if n_hyb is not None else n_compact
for name, key in MATRIX:
    n = nrows(ART / f"{name}.npz", key)
    if n is None:
        print(f"  {name:26s} ОТСУТСТВУЕТ  [{tag(name)}]")
        bad.append(name)
        continue
    flag = "" if n == ref else f"  <-- РАСХОЖДЕНИЕ с hybrid_features ({ref:,})"
    if n != ref:
        bad.append(name)
    print(f"  {name:26s} {n:>10,}{flag}  [{tag(name)}]")

print("\n--- keyed по mid, длина свободна ---")
for name, key in MIDKEYED:
    p = ART / f"{name}.npz"
    if not p.exists():
        print(f"  {name:26s} ОТСУТСТВУЕТ")
        continue
    z = np.load(p, allow_pickle=True)
    if "mids" not in z.files:
        print(f"  {name:26s} НЕТ массива mids — считать его mid-keyed нельзя")
        bad.append(name)
        continue
    nv, nm = int(z[key].shape[0]), int(z["mids"].shape[0])
    flag = "" if nv == nm else "  <-- value и mids разной длины"
    if nv != nm:
        bad.append(name)
    print(f"  {name:26s} {nv:>10,} (mids {nm:,}){flag}")

print()
if bad:
    foreign = [b for b in bad if b not in REBUILT_HERE]
    print(f"ПРОВАЛ: не совпали {bad}")
    if foreign:
        print(f"Из них НЕ пересобираются этим скриптом: {foreign}")
        print("Эта цепочка покрывает только семь кэшей из двенадцати позиционных.")
        print("Рефит весов на таком входе НЕВОЗМОЖЕН: audit_live_path.train_columns")
        print("берёт из undercount_retry_cache (G) ПЕРВЫЕ 23 КОЛОНКИ ИЗ 35, включая")
        print("draft_logit и elo, и складывает их np.column_stack с пересобранными —")
        print("поэтому падение будет на формах массивов, а не на осмысленной проверке.")
    print("«Разблокировано» намеренно НЕ печатается: см. E-258.")
    sys.exit(1)

print("ВСЕ ДВЕНАДЦАТЬ ПОЗИЦИОННЫХ КЭШЕЙ СИНХРОННЫ С КОРПУСОМ —")
print("build_prematch_artifact.py БЕЗ PREMATCH_SNAPSHOT_ONLY действительно разблокирован.")
CHECK

say "=== ЦЕПОЧКА ЗАВЕРШЕНА: $LOGDIR, бэкап старых кэшей: $BK"
say "СЛЕДУЮЩИЙ ШАГ (отдельное решение): сверить пересобранный draft_logit с живым"
say "кодировщиком (E-201), затем build_prematch_artifact.py БЕЗ SNAPSHOT_ONLY и"
say "forward-валидация весов."
