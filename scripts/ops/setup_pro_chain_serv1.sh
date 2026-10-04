#!/bin/bash
# Разовая идемпотентная подготовка дерева сборки ночной цепочки про-корпуса на serv1.
#
# Что делает (и печатает, что именно):
#   1. создаёт git worktree $BUILD_ROOT (по умолчанию /root/pro_chain) от $PROD_ROOT (/root/main)
#      на текущем HEAD боевого checkout'а, отсоединённо;
#   2. кладёт симлинк base/keys.py -> $PROD_ROOT/base/keys.py (секрет, в .gitignore);
#   3. создаёт пустые каталоги выходов;
#   4. печатает чек-лист gitignored входов (есть / ОТСУТСТВУЕТ, размеры). Недостающее
#      подкладывается вручную (rsync с Мака) — скрипт ничего не копирует и не генерирует.
#
# Чего НЕ делает: ничего не удаляет, не переносит и не перезаписывает под $PROD_ROOT
# (единственная запись туда — служебная запись `git worktree add` в $PROD_ROOT/.git/worktrees),
# не трогает $PROD_ROOT/pro_heroes_data. Повторный запуск ничего не меняет.
# Коды: 0 — даже если в чек-листе есть пропуски; 1 — сорвалось действие.
# Переопределения для тестов: PROD_ROOT, PRO_CHAIN_BUILD_ROOT.
set -u

PROD_ROOT="${PROD_ROOT:-/root/main}"
BUILD_ROOT="${PRO_CHAIN_BUILD_ROOT:-/root/pro_chain}"

fail() { echo "ОШИБКА: $*" >&2; exit 1; }

[ -e "$PROD_ROOT/.git" ] || fail "боевой checkout $PROD_ROOT не git-репозиторий"
prod_real="$(cd "$PROD_ROOT" && pwd -P)"

echo "== 1. дерево сборки $BUILD_ROOT"
if [ -e "$BUILD_ROOT/.git" ]; then
  build_real="$(cd "$BUILD_ROOT" && pwd -P)"
  [ "$build_real" != "$prod_real" ] || fail "$BUILD_ROOT совпадает с боевым $PROD_ROOT"
  echo "уже есть, HEAD $(git -C "$BUILD_ROOT" rev-parse --short HEAD 2>/dev/null || echo '?')"
elif [ -e "$BUILD_ROOT" ] && [ -n "$(ls -A "$BUILD_ROOT" 2>/dev/null)" ]; then
  fail "$BUILD_ROOT существует, не пуст и не git worktree — не трогаю"
else
  echo "создаю: git -C $PROD_ROOT worktree add --detach $BUILD_ROOT HEAD"
  git -C "$PROD_ROOT" worktree add --detach "$BUILD_ROOT" HEAD || fail "git worktree add не удался"
fi

echo "== 2. base/keys.py"
KEYS_LINK="$BUILD_ROOT/base/keys.py"
if [ -L "$KEYS_LINK" ] || [ -e "$KEYS_LINK" ]; then
  if [ -L "$KEYS_LINK" ] && [ ! -e "$KEYS_LINK" ]; then
    echo "ОТСУТСТВУЕТ цель симлинка: $KEYS_LINK -> $(readlink "$KEYS_LINK")"
  else
    echo "уже есть: $KEYS_LINK"
  fi
elif [ -e "$PROD_ROOT/base/keys.py" ]; then
  ln -s "$PROD_ROOT/base/keys.py" "$KEYS_LINK" || fail "симлинк keys.py не создан"
  echo "создан симлинк $KEYS_LINK -> $PROD_ROOT/base/keys.py"
else
  echo "ОТСУТСТВУЕТ $PROD_ROOT/base/keys.py — симлинк не создан (секрет кладёт владелец)"
fi

echo "== 3. каталоги выходов"
for d in runtime/artifacts/misc data ELO/output pro_heroes_data/json_parts_split_from_object; do
  mkdir -p "$BUILD_ROOT/$d" || fail "не создать $BUILD_ROOT/$d"
  echo "ok $d"
done

echo "== 4. чек-лист входов (gitignored; путь относительно $BUILD_ROOT)"
n_missing=0
while read -r kind rel; do
  [ -n "$kind" ] || continue
  case "$kind" in
    glob)
      n=0; bytes=0
      for f in "$BUILD_ROOT"/$rel; do
        [ -f "$f" ] || continue
        n=$((n + 1)); bytes=$((bytes + $(wc -c < "$f")))
      done
      if [ "$n" -gt 0 ]; then printf 'есть       %-70s %s файлов, %s байт\n' "$rel" "$n" "$bytes"
      else printf 'ОТСУТСТВУЕТ %-70s\n' "$rel"; n_missing=$((n_missing + 1)); fi ;;
    file)
      if [ -f "$BUILD_ROOT/$rel" ]; then printf 'есть       %-70s %s байт\n' "$rel" "$(wc -c < "$BUILD_ROOT/$rel")"
      else printf 'ОТСУТСТВУЕТ %-70s\n' "$rel"; n_missing=$((n_missing + 1)); fi ;;
  esac
done <<'EOF'
glob pro_heroes_data/json_parts_split_from_object/*.json.gz
file pro_heroes_data/json_parts_split_from_object/processed_ids.txt
file pro_heroes_data/json_parts_split_from_object/scan_manifest.json
file pro_heroes_data/json_parts_split_from_object/part_counters.json
file pro_heroes_data/json_parts_split_from_object/merge_patch_summary.json
file runtime/artifacts/misc/pro_corpus_compact.npz
file runtime/artifacts/misc/pro_corpus_rich.npz
file runtime/artifacts/misc/pro_features_ext.npz
file runtime/artifacts/misc/pro_draft_logit.npz
file runtime/artifacts/misc/pro_draft_logit_full.npz
file runtime/artifacts/misc/ideas_batch1.npz
file runtime/artifacts/misc/ideas_batch2.npz
file runtime/artifacts/misc/ideas_batch5.npz
file runtime/artifacts/misc/ideas_batch5b.npz
file runtime/artifacts/misc/undercount_glicko_features.npz
file runtime/artifacts/misc/prematch_weights_win120.npz
file runtime/artifacts/misc/ideas_batch8.npz
file runtime/artifacts/misc/branch_weights.npz
file runtime/artifacts/misc/branch_calibration.npz
file runtime/artifacts/misc/org_rating_snapshot.npz
file runtime/artifacts/misc/prematch_model_artifact_v2.npz
file runtime/artifacts/misc/prematch_model_artifact_v2_nohybrid.npz
file runtime/artifacts/misc/h2h_as_served.npz
file runtime/artifacts/misc/map_winner_hybrid_quality_forward/hybrid_features.npz
file pro_heroes_data/visited_teams.json
file pro_heroes_data/trash_maps.txt
file runtime/kv3_state_deliver.on
file ml-models/prematch_panel_kv3/manifest.json
EOF

echo "== итог: отсутствует входов — $n_missing (чек-лист, код выхода 0)"
# Print only: installation and cutover need separate owner approval.
# Mac launchd jobs stay on until one serv1 shadow night matches.
cat <<EOF
== systemd: команды ниже только напечатаны, НЕ выполнены
# Сначала сверить shadow-ночь serv1; задания Mac launchd остаются включёнными.
install -m 0644 $PROD_ROOT/scripts/ops/systemd/pro-chain-nightly.service /etc/systemd/system/pro-chain-nightly.service
install -m 0644 $PROD_ROOT/scripts/ops/systemd/pro-chain-nightly.timer /etc/systemd/system/pro-chain-nightly.timer
systemctl daemon-reload
# ТОЛЬКО после совпадения shadow-ночи и отдельного одобрения владельца на cutover:
systemctl enable --now pro-chain-nightly.timer
EOF
exit 0
