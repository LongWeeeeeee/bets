# shellcheck shell=bash
# Общая обвязка ночной цепочки про-корпуса. Подключается через `source`
# (не исполняется сама) из scripts/run/rebuild_prematch_snapshot.sh,
# scripts/run/topup_pro_corpus.sh, scripts/ops/build_kv3_state.sh и
# scripts/run/pro_nightly_chain.sh.
#
# Одна цепочка, две машины.
#   remote (Мак): дерево сборки = этот checkout; прод = $SERV1:$PROD_ROOT,
#     доступ по ssh/scp. Так цепочка работала до 10.2026.
#   local (serv1): дерево сборки = ОТДЕЛЬНЫЙ checkout (/root/pro_chain,
#     git worktree от /root/main); прод = $PROD_ROOT на той же машине,
#     доступ через cp и bash.
#
# Дерево сборки НИКОГДА не совпадает с боевым checkout'ом. Каждый шаг пишет
# выходы относительно своего checkout'а (ELO/output/live_team_elo_snapshot.json,
# data/rating_snapshot.npz, data/prior_snapshot.npz, ...), а в /root/main это
# и есть файлы, которые читает прод. Сборка внутри /root/main подменила бы их
# недособранными файлами без проверки sha1 и без рестарта.
#
# PRO_CHAIN_SHADOW=1 — собрать всё, не доставить ничего: ни одной записи под
# $PROD_ROOT, ни перебазировки, ни systemctl. Что уехало бы на прод, пишется
# строкой «путь<TAB>sha1<TAB>байт» в $PRO_CHAIN_SUMMARY. Чтение с прода
# (справочник имён, overlay tier2) в тени разрешено.
#
# API (менять сигнатуры нельзя, добавлять функции можно):
#   ROOT PROD_ROOT SERV1 PY PRO_CHAIN_MODE PRO_CHAIN_SHADOW PRO_CHAIN_SUMMARY
#   pro_chain_guard          — выход 2, если сборка смотрит в боевой checkout
#   sha1_of FILE             — sha1 локального файла (sha1sum или shasum)
#   prod_fetch REL DST       — скопировать $PROD_ROOT/REL в локальный DST (чтение)
#   prod_read 'CMD'          — выполнить команду на проде без изменений (чтение)
#   prod_sha1 REL            — sha1 файла $PROD_ROOT/REL (пусто, если нет)
#   prod_run 'CMD'           — изменяющая команда на проде; в тени пропуск, rc 0
#   prod_script ARGS... <S   — stdin-скрипт bash на проде; в тени stdin
#                              выбрасывается, rc 0
#   prod_stage SRC REL       — положить SRC в $PROD_ROOT/REL.tmp и сверить sha1;
#                              rc 1 при расхождении (.tmp удалён); в тени только
#                              строка в $PRO_CHAIN_SUMMARY, rc 0
#   prod_commit REL          — mv $PROD_ROOT/REL.tmp -> $PROD_ROOT/REL (prod_run)
#   shadow_on                — rc 0, если PRO_CHAIN_SHADOW=1

PRO_CHAIN_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Корень всегда канонический (pwd -P): `/root/main/.` или симлинк на прод не
# должны пройти мимо pro_chain_guard.
ROOT="$(cd "${PRO_CHAIN_ROOT:-$PRO_CHAIN_LIB_DIR/../..}" && pwd -P)" || {
  echo "ОШИБКА: дерево сборки ${PRO_CHAIN_ROOT:-$PRO_CHAIN_LIB_DIR/../..} недоступно" >&2; exit 2; }
# Python-шаги и base/tools/build_team_org_aliases.py берут корень данных из
# DRAFT_ROOT (у последнего умолчание — путь Мака). На Маке ROOT совпадает с
# прежним умолчанием, на serv1 — дерево сборки.
export DRAFT_ROOT="$ROOT"
PROD_ROOT="${PROD_ROOT:-/root/main}"
SERV1="${SERV1:-serv1}"  # ssh-алиас из ~/.ssh/config; IP не писать (переезд 22.09.2026)
PRO_CHAIN_SHADOW="${PRO_CHAIN_SHADOW:-0}"

if [ -z "${PRO_CHAIN_MODE:-}" ]; then
  if [ -e "$PROD_ROOT/.git" ]; then PRO_CHAIN_MODE=local; else PRO_CHAIN_MODE=remote; fi
fi

if [ -z "${PY:-}" ]; then
  if [ -n "${PRO_CHAIN_PY:-}" ]; then PY="$PRO_CHAIN_PY"
  elif [ -x "$ROOT/venv_catboost/bin/python3" ]; then PY="$ROOT/venv_catboost/bin/python3"
  elif [ -x "$PROD_ROOT/venv/bin/python3" ]; then PY="$PROD_ROOT/venv/bin/python3"
  else PY=python3
  fi
fi

PRO_CHAIN_SUMMARY="${PRO_CHAIN_SUMMARY:-$ROOT/runtime/pro_chain_shadow_$(date +%Y%m%d_%H%M).tsv}"

shadow_on() { [ "$PRO_CHAIN_SHADOW" = 1 ]; }

pro_chain_guard() {
  case "$PRO_CHAIN_MODE" in
    local|remote) ;;
    *) echo "ОШИБКА: PRO_CHAIN_MODE=$PRO_CHAIN_MODE (ждём local или remote)" >&2; return 2 ;;
  esac
  # Перебазировка ELO (heredoc в rebuild_prematch_snapshot.sh) жёстко работает
  # с /root/main: другой PROD_ROOT дал бы доставку в одно дерево и
  # перебазировку другого. Подмена разрешена только тестам.
  if [ "$PROD_ROOT" != /root/main ] && [ "${PRO_CHAIN_ALLOW_FAKE_PROD:-0}" != 1 ]; then
    echo "ОШИБКА: PROD_ROOT=$PROD_ROOT, а перебазировка работает с /root/main" >&2
    return 2
  fi
  # В обоих режимах: на машине, где лежит боевой checkout, сборка в нём
  # затёрла бы файлы прода (ручной PRO_CHAIN_MODE=remote из /root/main тоже).
  if [ -e "$PROD_ROOT" ]; then
    local prod_real
    prod_real="$(cd "$PROD_ROOT" 2>/dev/null && pwd -P)" || {
      echo "ОШИБКА: боевой checkout $PROD_ROOT недоступен" >&2; return 2; }
    if [ "$prod_real" = "$ROOT" ]; then
      echo "ОШИБКА: дерево сборки $ROOT совпадает с боевым $PROD_ROOT — сборка затёрла бы файлы прода" >&2
      return 2
    fi
  elif [ "$PRO_CHAIN_MODE" = local ]; then
    echo "ОШИБКА: боевой checkout $PROD_ROOT недоступен" >&2; return 2
  fi
  return 0
}

sha1_of() {
  if command -v sha1sum >/dev/null 2>&1; then sha1sum "$1" | cut -d' ' -f1
  else shasum -a 1 "$1" | cut -d' ' -f1
  fi
}

prod_fetch() {
  if [ "$PRO_CHAIN_MODE" = local ]; then cp "$PROD_ROOT/$1" "$2"
  else scp -q -o BatchMode=yes -o ConnectTimeout=15 "$SERV1:$PROD_ROOT/$1" "$2"
  fi
}

prod_read() {
  if [ "$PRO_CHAIN_MODE" = local ]; then bash -c "$1"
  else ssh -o BatchMode=yes "$SERV1" "$1"
  fi
}

prod_sha1() {
  prod_read "sha1sum $PROD_ROOT/$1 2>/dev/null | cut -d' ' -f1"
}

prod_run() {
  if shadow_on; then echo "[тень] пропуск на проде: $1"; return 0; fi
  if [ "$PRO_CHAIN_MODE" = local ]; then bash -c "$1"
  else ssh -o BatchMode=yes "$SERV1" "$1"
  fi
}

prod_script() {
  if shadow_on; then cat >/dev/null; echo "[тень] пропуск скрипта на проде: $*"; return 0; fi
  if [ "$PRO_CHAIN_MODE" = local ]; then bash -s -- "$@"
  else ssh -o BatchMode=yes "$SERV1" bash -s -- "$@"
  fi
}

prod_stage() {
  local src="$1" rel="$2" l r
  if [ ! -f "$src" ] || [ ! -r "$src" ]; then
    echo "ВНИМАНИЕ: $rel — нет собранного файла $src, на проде остался прежний"
    return 1
  fi
  l="$(sha1_of "$src")"
  if shadow_on; then
    # set -e здесь выключен (вызов из if): каждую ошибку проверяем явно, иначе
    # тень «прошла» бы с пустым sha1 или без строки в сводке.
    local bytes
    bytes="$(wc -c < "$src" | tr -d ' ')"
    if [ -z "$l" ] || [ -z "$bytes" ]; then
      echo "ВНИМАНИЕ: [тень] $rel — sha1/размер не посчитаны"; return 1
    fi
    mkdir -p "$(dirname "$PRO_CHAIN_SUMMARY")" \
      && printf '%s\t%s\t%s\n' "$rel" "$l" "$bytes" >> "$PRO_CHAIN_SUMMARY" || {
      echo "ВНИМАНИЕ: [тень] $rel — не записан в сводку $PRO_CHAIN_SUMMARY"; return 1; }
    echo "[тень] не доставляю $rel: sha1 $l"
    return 0
  fi
  # Код копирования проверяем явно: prod_stage зовут из `if`, где set -e
  # выключен, а пустой sha1 с обеих сторон совпал бы.
  if [ "$PRO_CHAIN_MODE" = local ]; then cp "$src" "$PROD_ROOT/$rel.tmp"
  else scp -q -o BatchMode=yes "$src" "$SERV1:$PROD_ROOT/$rel.tmp"
  fi || {
    local crc=$?
    prod_run "rm -f $PROD_ROOT/$rel.tmp"
    echo "ВНИМАНИЕ: $rel не скопирован на прод (rc=$crc) — на проде остался прежний"
    return 1
  }
  r="$(prod_sha1 "$rel.tmp")"
  if [ -z "$l" ] || [ -z "$r" ]; then
    prod_run "rm -f $PROD_ROOT/$rel.tmp"
    echo "ВНИМАНИЕ: $rel — пустой sha1 (локально '$l', на проде '$r') — на проде остался прежний"
    return 1
  fi
  if [ "$l" != "$r" ]; then
    prod_run "rm -f $PROD_ROOT/$rel.tmp"
    echo "ВНИМАНИЕ: $rel доехал битым ($r против $l) — на проде остался прежний"
    return 1
  fi
  return 0
}

prod_commit() {
  prod_run "mv $PROD_ROOT/$1.tmp $PROD_ROOT/$1"
}
