#!/bin/bash
# Свести дерево с origin одной командой: закоммитить остатки -> подтянуть -> отправить.
#
#   scripts/ops/sync.sh                       # авто-сообщение по областям правки
#   scripts/ops/sync.sh -m "текст коммита"
#   scripts/ops/sync.sh --no-push             # только коммит и pull --rebase
#   scripts/ops/sync.sh --check               # ничего не менять, показать расхождение
#
# Из индекса всегда исключаются файлы с ключами и бэкапы: base/keys.py, *.bak_*.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

MSG=""; PUSH=1; CHECK=0
while [ $# -gt 0 ]; do
  case "$1" in
    -m) MSG="${2:-}"; shift 2 ;;
    --no-push) PUSH=0; shift ;;
    --check) CHECK=1; shift ;;
    *) echo "неизвестный аргумент: $1" >&2; exit 2 ;;
  esac
done

if [ -e .git/MERGE_HEAD ] || [ -d .git/rebase-merge ] || [ -d .git/rebase-apply ]; then
  echo "в дереве незавершённый merge/rebase — доведи его до конца, потом sync" >&2; exit 1
fi

branch="$(git rev-parse --abbrev-ref HEAD)"
git fetch origin --quiet
read -r behind ahead <<<"$(git rev-list --left-right --count "origin/$branch...HEAD" 2>/dev/null || echo '0 0')"
dirty="$(git status --porcelain | wc -l | tr -d ' ')"
echo "ветка $branch: своих коммитов $ahead, в origin новых $behind, незакоммиченного $dirty"

if command -v ssh >/dev/null && ssh -o BatchMode=yes -o ConnectTimeout=4 serv1 true 2>/dev/null; then
  s1="$(ssh serv1 'cd /root/main && git rev-parse --short HEAD; git status --porcelain | wc -l' 2>/dev/null | tr '\n' ' ')"
  echo "serv1: HEAD $s1(незакоммиченного)"
fi
[ "$CHECK" -eq 1 ] && exit 0

if [ "$dirty" -gt 0 ]; then
  git add -A
  git reset -q -- base/keys.py 2>/dev/null
  git diff --cached --name-only | grep -E '\.bak_|\.orig$' | while read -r f; do git reset -q -- "$f"; done
  staged="$(git diff --cached --name-only | wc -l | tr -d ' ')"
  if [ "$staged" -gt 0 ]; then
    if [ -z "$MSG" ]; then
      areas="$(git diff --cached --name-only | awk -F/ '{print ($1=="base"&&$2=="tests")?"tests":$1}' | sort -u | paste -sd, -)"
      MSG="sync: $staged файлов ($areas)"
    fi
    git commit -q -m "$MSG" || { echo "коммит отклонён (см. вывод хука)" >&2; exit 1; }
    echo "закоммичено: $MSG"
  else
    echo "коммитить нечего (всё в .gitignore или исключено)"
  fi
fi

git pull --rebase origin "$branch" || { echo "pull --rebase не прошёл — разреши конфликты и повтори" >&2; exit 1; }
if [ "$PUSH" -eq 1 ]; then
  git push origin "$branch" && echo "отправлено в origin/$branch"
else
  echo "push пропущен (--no-push)"
fi
git rev-list --left-right --count "origin/$branch...HEAD" | awk '{print "итог: origin впереди на "$1", локально впереди на "$2}'
