#!/usr/bin/env bash
# Настройка канонических симлинков AGENTS.md без блокировки его легитимных правок.
# ПЕРЕД запуском положи в корень проекта:
#   - AGENTS.md            (новая версия)
#   - .claude/agents/reviewer.md
# Затем: cd /Users/alex/Documents/ingame && bash setup-protection.sh
set -uo pipefail

echo '==> 1. Структура симлинков (canonical = AGENTS.md)'
chflags nouchg AGENTS.md 2>/dev/null || true
mkdir -p .github
rm -f CLAUDE.md GEMINI.md .cursorrules .github/copilot-instructions.md README.md
ln -s AGENTS.md CLAUDE.md
ln -s AGENTS.md GEMINI.md
ln -s AGENTS.md .cursorrules
ln -s ../AGENTS.md .github/copilot-instructions.md
ls -la CLAUDE.md GEMINI.md .cursorrules .github/copilot-instructions.md

echo '==> 2. Защита правок отключена'
mkdir -p .claude/hooks
rm -f .claude/hooks/protect-canonical.sh

echo '==> 3. Снятие только canonical-защиты в settings.json'
if [ -f .claude/settings.json ]; then
  python3 - <<'PY'
import json
from pathlib import Path

path = Path('.claude/settings.json')
data = json.loads(path.read_text())
deny = data.setdefault('permissions', {}).setdefault('deny', [])
data['permissions']['deny'] = [
    rule for rule in deny
    if rule not in {'Edit(AGENTS.md)', 'Edit(CLAUDE.md)', 'Edit(GEMINI.md)'}
]
pre = data.get('hooks', {}).get('PreToolUse', [])
pre = [
    item for item in pre
    if not any(
        hook.get('command') == '.claude/hooks/protect-canonical.sh'
        for hook in item.get('hooks', [])
    )
]
if 'hooks' in data:
    if pre:
        data['hooks']['PreToolUse'] = pre
    else:
        data['hooks'].pop('PreToolUse', None)
path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + '\n')
PY
else
  mkdir -p .claude
  printf '{\n  "permissions": {"deny": []},\n  "hooks": {}\n}\n' > .claude/settings.json
fi

echo '==> 4. Удаление старого pre-commit guard'
if [ -f .git/hooks/pre-commit ] && grep -Fq 'AGENTS.md нельзя менять коммитом' .git/hooks/pre-commit; then
  rm -f .git/hooks/pre-commit
fi

echo 'ГОТОВО. AGENTS.md остаётся каноническим, но доступен для правок.'
