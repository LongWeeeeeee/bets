#!/usr/bin/env bash
# Настройка защиты канонического AGENTS.md.
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

echo '==> 2. PreToolUse-хук'
mkdir -p .claude/hooks
cat > .claude/hooks/protect-canonical.sh <<'HOOK_EOF'
#!/usr/bin/env bash
input=$(cat)
P='AGENTS\.md|CLAUDE\.md|GEMINI\.md|\.cursorrules|copilot-instructions\.md'
py='import sys,json;d=json.load(sys.stdin)'
tool=$(printf '%s' "$input" | python3 -c "$py;print(d.get('tool_name',''))")
if [ "$tool" = "Bash" ]; then
  field=$(printf '%s' "$input" | python3 -c "$py;print(d.get('tool_input',{}).get('command',''))")
else
  field=$(printf '%s' "$input" | python3 -c "$py;print(d.get('tool_input',{}).get('file_path',''))")
fi
if printf '%s' "$field" | grep -Eq "($P)"; then
  echo "BLOCKED: AGENTS.md и его симлинки меняет только человек вручную." >&2
  exit 2
fi
exit 0
HOOK_EOF
chmod +x .claude/hooks/protect-canonical.sh

echo '==> 3. settings.json'
if [ -f .claude/settings.json ]; then
  echo '   .claude/settings.json уже существует — НЕ трогаю. Впиши permissions.deny и hooks.PreToolUse вручную.'
else
  cat > .claude/settings.json <<'SETTINGS_EOF'
{
  "permissions": {
    "deny": ["Edit(AGENTS.md)", "Edit(CLAUDE.md)", "Edit(GEMINI.md)"]
  },
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Edit|Write|MultiEdit|Bash",
        "hooks": [
          { "type": "command", "command": ".claude/hooks/protect-canonical.sh" }
        ]
      }
    ]
  }
}
SETTINGS_EOF
fi

echo '==> 4. git pre-commit guard'
if [ -d .git ]; then
  cat > .git/hooks/pre-commit <<'PRECOMMIT_EOF'
#!/bin/sh
if git diff --cached --name-only | grep -Fxq AGENTS.md; then
  echo "AGENTS.md нельзя менять коммитом (обход: git commit --no-verify)." >&2
  exit 1
fi
PRECOMMIT_EOF
  chmod +x .git/hooks/pre-commit
else
  echo '   .git не найден — пропускаю pre-commit guard.'
fi

echo '==> 5. Коммит (легитимная фиксация восстановленного AGENTS.md)'
git add -A
git commit -m 'restore AGENTS.md (canonical) + review-after-run workflow + protection' --no-verify || true

echo '==> 6. Блокировка флагом ОС'
chflags uchg AGENTS.md
ls -lO AGENTS.md

echo 'ГОТОВО. Проверка: echo test >> AGENTS.md должно вернуть Operation not permitted.'
echo 'Легитимная правка позже: chflags nouchg AGENTS.md ... chflags uchg AGENTS.md'
