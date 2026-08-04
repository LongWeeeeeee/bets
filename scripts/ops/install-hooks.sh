#!/bin/bash
# Ставит гард раскладки в .git/hooks/pre-commit на текущей машине.
# Прежний хук сохраняется рядом (.git/hooks/pre-commit.before-layout-guard);
# если в нём был запрет на коммит AGENTS.md — он остаётся включённым через маркер.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
SRC="$ROOT/scripts/ops/hooks/pre-commit"
DST="$ROOT/.git/hooks/pre-commit"

[ -f "$SRC" ] || { echo "нет $SRC" >&2; exit 1; }

if [ -f "$DST" ] && ! cmp -s "$SRC" "$DST"; then
  if grep -q "AGENTS.md нельзя менять коммитом" "$DST"; then
    touch "$ROOT/.git/agents-md-locked"
    echo "прежний хук запрещал коммит AGENTS.md — запрет сохранён (.git/agents-md-locked)"
  fi
  cp "$DST" "$DST.before-layout-guard"
  echo "прежний хук сохранён: $DST.before-layout-guard"
fi

install -m 755 "$SRC" "$DST"
echo "гард раскладки установлен: $DST"
echo "снять запрет на AGENTS.md: rm $ROOT/.git/agents-md-locked"
