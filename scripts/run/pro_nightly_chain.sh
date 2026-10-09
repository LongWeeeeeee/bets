#!/bin/bash
# Ночная цепочка про-корпуса на serv1: добор (topup) -> пересборка снимка -> доставка на прод.
#
# ЗАПУСКАТЬ ИЗ БОЕВОГО checkout'а (/root/main/scripts/run/pro_nightly_chain.sh, cron/systemd),
# а не из дерева сборки: синхронизация кода ниже переключает дерево сборки на другой
# коммит, и bash, читающий СВОЙ скрипт построчно, получил бы посреди прогона обрывок
# нового файла.
#
# Дерево сборки — ОТДЕЛЬНЫЙ checkout ($PRO_CHAIN_BUILD_ROOT, по умолчанию /root/pro_chain,
# git worktree от $PROD_ROOT). Каждый шаг пишет выходы относительно своего checkout'а, а в
# /root/main это те самые файлы, которые читает прод. Доставка из дерева сборки в $PROD_ROOT
# идёт через scripts/run/lib_pro_chain.sh (cp -> sha1 -> mv). Подготовка дерева —
# scripts/ops/setup_pro_chain_serv1.sh.
#
# Порядок: добор -> пересборка. Пересборка идёт ВСЕГДА, даже если добор упал: она обязана
# отработать на том корпусе, что есть (см. шапку topup_pro_corpus.sh). Код выхода — код
# пересборки.
#
# Переменные: PROD_ROOT (/root/main), PRO_CHAIN_BUILD_ROOT (/root/pro_chain),
#   PRO_CHAIN_SHADOW=1 — собрать, ничего не доставлять (проходит насквозь).
#   PRO_CHAIN_ELO_SUPPLEMENT=0 — пропустить добор ELO-добавки OpenDota и собрать снимок без неё (ELO_SUPPLEMENT_DIR=/nonexistent).
# Коды выхода: 0 ок или уже идёт другой прогон; 2 неверная конфигурация;
#   1 дерево сборки грязное / сорвалась синхронизация; иначе код пересборки.
# Совместимо с bash 3.2 (тесты на macOS): без ассоциативных массивов и mapfile.
set -u

PROD_ROOT="${PROD_ROOT:-/root/main}"
BUILD_ROOT="${PRO_CHAIN_BUILD_ROOT:-/root/pro_chain}"
TOPUP_TIMEOUT="${PRO_CHAIN_TOPUP_TIMEOUT:-3h}"
# Пересборку НЕ ограничиваем по времени: GNU timeout бьёт всю группу процессов,
# и если срок выйдет внутри перебазировки ELO (после `systemctl stop cyberscore`),
# прод останется остановленным. На Маке таймаута тоже не было; зависание ловит
# freshness-watchdog («снимок старше»), а flock не даст запустить вторую цепочку.
REBUILD_TIMEOUT="${PRO_CHAIN_REBUILD_TIMEOUT:-}"

die() { echo "ОШИБКА: $*" >&2; exit 2; }

# A rebuild timeout could leave prod stopped, or kill only the notifier while
# the isolated rebuild group keeps running and delivering after flock unlocks.
[ -z "$REBUILD_TIMEOUT" ] || die "PRO_CHAIN_REBUILD_TIMEOUT недопустим: таймаут пересборки опасен для перебазировки и блокировки"

[ -e "$PROD_ROOT/.git" ] || die "боевой checkout $PROD_ROOT не git-репозиторий"
[ -d "$BUILD_ROOT" ] || die "дерево сборки $BUILD_ROOT не существует (scripts/ops/setup_pro_chain_serv1.sh)"
PROD_REAL="$(cd "$PROD_ROOT" && pwd -P)"
BUILD_REAL="$(cd "$BUILD_ROOT" && pwd -P)"
[ "$PROD_REAL" != "$BUILD_REAL" ] || die "дерево сборки $BUILD_ROOT совпадает с боевым $PROD_ROOT"
[ -e "$BUILD_REAL/.git" ] || die "$BUILD_ROOT не git worktree"
git -C "$BUILD_REAL" rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "$BUILD_ROOT не git worktree"
# Только worktree ЭТОГО репозитория: иначе checkout нужного коммита не найдёт объекты.
prod_common="$(cd "$PROD_REAL" && cd "$(git rev-parse --git-common-dir)" && pwd -P)"
build_common="$(cd "$BUILD_REAL" && cd "$(git rev-parse --git-common-dir)" && pwd -P)"
[ "$prod_common" = "$build_common" ] || die "$BUILD_ROOT не worktree репозитория $PROD_ROOT ($build_common против $prod_common)"

BUILD_ROOT="$BUILD_REAL"
PROD_ROOT="$PROD_REAL"
mkdir -p "$BUILD_ROOT/runtime" || die "не создать $BUILD_ROOT/runtime"
LOG="$BUILD_ROOT/runtime/pro_nightly_chain_$(date +%Y%m%d_%H%M).log"
LOCK="$BUILD_ROOT/runtime/pro_nightly_chain.lock"

command -v flock >/dev/null 2>&1 || die "нет flock: без него нельзя отличить занятый замок от его отсутствия"
# Блокировка держится на fd 9 до конца скрипта. Детям fd закрывается (9>&-): иначе
# демон, запущенный пересборкой, держал бы замок после её конца.
exec 9>>"$LOCK" || die "не открыть $LOCK"
if ! flock -n 9; then
  msg="$(date '+%F %T') другой прогон держит $LOCK — выхожу"
  echo "$msg"; echo "$msg" >> "$LOG"
  exit 0
fi

exec >>"$LOG" 2>&1
echo "=== $(date '+%F %T') ночная цепочка про-корпуса: прод=$PROD_ROOT сборка=$BUILD_ROOT тень=${PRO_CHAIN_SHADOW:-0} ==="

# Режим local и общая обвязка: ROOT = дерево сборки, PY — по правилам библиотеки.
# GZIP-флаг не наследуем: он нужен только процессу добора (его ставит topup_pro_corpus.sh).
unset PRO_CORPUS_GZIP
export PRO_CHAIN_MODE=local PROD_ROOT
export PRO_CHAIN_SHADOW="${PRO_CHAIN_SHADOW:-0}"
export PRO_CHAIN_ROOT="$BUILD_ROOT"
# shellcheck source=lib_pro_chain.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib_pro_chain.sh"

notify() {
  # Одна строка в админ-чат; сбой уведомления цепочку не ломает.
  local script="$BUILD_ROOT/scripts/ops/notify_admin.py"
  if [ -f "$script" ]; then
    echo "$1" | "$PY" "$script" 9>&- || echo "ВНИМАНИЕ: notify_admin не отработал"
  else
    echo "ВНИМАНИЕ: нет $script, уведомление не отправлено: $1"
  fi
  return 0
}

# --- синхронизация кода: дерево сборки = HEAD боевого checkout'а ---
# Отслеживаемые git'ом ВЫХОДЫ цепочки: пересборка пишет их в дерево сборки каждую ночь.
# Копия в дереве сборки — выход, уже доставленный на прод или пересобираемый, поэтому
# перед проверкой её возвращаем к коммиту. Любая другая правка отслеживаемых файлов по-прежнему
# останавливает прогон. Список расширять только по `git ls-files` выходов цепочки.
CHAIN_OUTPUT_ALLOWLIST="data/team_org_aliases.json"
for out_path in $CHAIN_OUTPUT_ALLOWLIST; do
  if [ -n "$(git -C "$BUILD_ROOT" status --porcelain --untracked-files=no -- "$out_path")" ]; then
    if git -C "$BUILD_ROOT" checkout -- "$out_path"; then
      echo "восстановлен выход цепочки из коммита: $out_path"
    else
      echo "ОШИБКА: не удалось восстановить $out_path"
    fi
  fi
done
dirty="$(git -C "$BUILD_ROOT" status --porcelain --untracked-files=no)" || {
  notify "⚠️ serv1: ночная цепочка не запущена: git status в $BUILD_ROOT упал ($(date '+%F %T'))"; exit 1; }
if [ -n "$dirty" ]; then
  echo "ОШИБКА: в дереве сборки есть правки отслеживаемых файлов:"; echo "$dirty"
  notify "⚠️ serv1: ночная цепочка не запущена: дерево сборки $BUILD_ROOT грязное ($(printf '%s\n' "$dirty" | head -3 | tr '\n' ';'))"
  exit 1
fi
old_sha="$(git -C "$BUILD_ROOT" rev-parse HEAD 2>/dev/null || echo none)"
new_sha="$(git -C "$PROD_ROOT" rev-parse HEAD)" || { notify "⚠️ serv1: ночная цепочка: нет HEAD в $PROD_ROOT"; exit 1; }
if ! git -C "$BUILD_ROOT" checkout -q --detach "$new_sha"; then
  echo "ОШИБКА: checkout $new_sha в $BUILD_ROOT не удался"
  notify "⚠️ serv1: ночная цепочка не запущена: checkout $new_sha в $BUILD_ROOT не удался ($(date '+%F %T'))"
  exit 1
fi
echo "код дерева сборки: $old_sha -> $new_sha"

# --- прогон ---
run_step() {  # run_step ИМЯ ТАЙМАУТ|"" СКРИПТ  -> rc, сообщает о таймауте
  local name="$1" tmo="$2" script="$3" rc=0
  echo "=== $(date '+%F %T') $name ==="
  if [ -n "$tmo" ]; then
    nice -n 10 ionice -c 3 timeout -k 120 "$tmo" bash "$script" 9>&- || rc=$?
  else
    nice -n 10 ionice -c 3 bash "$script" 9>&- || rc=$?
  fi
  echo "=== $(date '+%F %T') $name: rc=$rc ==="
  if [ "$rc" -eq 124 ]; then
    notify "⚠️ serv1: $name — таймаут $tmo ($(date '+%F %T'))"
  elif [ "$rc" -ne 0 ]; then
    notify "⚠️ serv1: $name — rc=$rc ($(date '+%F %T'), лог $LOG)"
  fi
  return "$rc"
}

topup_rc=0
wait_no_live_map "${PRO_CHAIN_HEAVY_WAIT_SECONDS:-2700}" "добор про-корпуса"
run_step "добор про-корпуса" "$TOPUP_TIMEOUT" "$BUILD_ROOT/scripts/run/topup_pro_corpus.sh" || topup_rc=$?
[ "$topup_rc" -eq 0 ] || echo "добор упал (rc=$topup_rc), пересобираю на текущем корпусе"

supplement_rc=skipped
if [ "${PRO_CHAIN_ELO_SUPPLEMENT:-1}" != 0 ]; then
  supplement_rc=0
  run_step "ELO-добавка OpenDota" 1800 "$BUILD_ROOT/scripts/pro_chain/update_elo_supplement.sh" || supplement_rc=$?
else
  # Полный откат одним флагом: снимок строится без добавки, даже если файл прошлой ночи лежит в data/elo_supplement.
  export ELO_SUPPLEMENT_DIR="${ELO_SUPPLEMENT_DIR:-/nonexistent}"
fi

rebuild_rc=0
wait_no_live_map "${PRO_CHAIN_HEAVY_WAIT_SECONDS:-2700}" "пересборка снимка"
run_step "пересборка снимка" "$REBUILD_TIMEOUT" "$BUILD_ROOT/scripts/run/rebuild_prematch_snapshot.sh" || rebuild_rc=$?

echo "=== $(date '+%F %T') цепочка завершена: добор rc=$topup_rc, пересборка rc=$rebuild_rc, ELO-добавка rc=$supplement_rc ==="
exit "$rebuild_rc"
