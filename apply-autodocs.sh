#!/usr/bin/env bash
# apply-autodocs.sh — ставит doc-sync: правило в AGENTS.md + проверку в reviewer.md.
# Запускать из КОРНЯ репозитория ingame (там, где лежит AGENTS.md).
# Идемпотентно: повторный запуск ничего не дублирует.
set -euo pipefail

ROOT="$(pwd)"
AGENTS="$ROOT/AGENTS.md"
REVIEWER="$ROOT/.claude/agents/reviewer.md"

if [ ! -f "$AGENTS" ]; then
  echo "ОШИБКА: AGENTS.md не найден в $ROOT — запусти скрипт из корня репозитория ingame." >&2
  exit 1
fi

# ---- 1) Правило doc-sync в AGENTS.md (proactive) ----
if grep -q 'autodocs-rule-v1' "$AGENTS"; then
  echo "= AGENTS.md: правило doc-sync уже стоит, пропускаю."
else
  cat >> "$AGENTS" <<'EOF'

<!-- autodocs-rule-v1 -->
## Doc-sync (правило 8: держи docs/ синхронными с кодом)
Если правка меняет ПУБЛИЧНЫЙ контракт — сигнатуру функции, env-переменную, CLI-флаг,
формат входа/выхода или сквозной поток сигнала — в ТОМ ЖЕ ходу обнови соответствующий
док:
- `docs/CODE_MAP.md` — файлы, публичные функции/сигнатуры, env, CLI-флаги, побочные эффекты;
- `docs/ARCHITECTURE.md` — сквозной поток данных сигнала и концепции.
Чисто внутренние правки (рефактор тела, логи, тесты, мелочи) док НЕ трогают.
Источник правды — код: при расхождении правь док, а не наоборот.
EOF
  echo "+ AGENTS.md: правило doc-sync дописано."
fi

# ---- 2) Проверка docs-sync в reviewer.md (enforcement) ----
if [ ! -f "$REVIEWER" ]; then
  echo "ВНИМАНИЕ: $REVIEWER не найден — пропускаю расширение reviewer (поставь review-механизм сначала)." >&2
elif grep -q 'docs-sync-check-v1' "$REVIEWER"; then
  echo "= reviewer.md: проверка docs-sync уже стоит, пропускаю."
else
  cat >> "$REVIEWER" <<'EOF'

<!-- docs-sync-check-v1 -->
## Docs-sync (обязательная часть каждого ревью)
После проверки кода оцени diff на изменение ПУБЛИЧНОГО контракта: сигнатуры функций,
env-переменные, CLI-флаги, формат входа/выхода, сквозной поток сигнала.
- Если такие изменения ЕСТЬ, проверь, обновлены ли соответствующие docs/
  (CODE_MAP.md — файлы/функции/env/флаги; ARCHITECTURE.md — поток данных).
- Если код изменил контракт, а docs/ в этом diff НЕ тронуты → вердикт REQUEST CHANGES
  с конкретным списком: какие doc-секции дописать.
- Чисто внутренние правки (рефактор тела, логи, тесты) док НЕ требуют — не блокируй из-за них.
Никогда не печатай секреты из keys.py при цитировании diff.
EOF
  echo "+ reviewer.md: проверка docs-sync дописана."
fi

echo
echo "Готово. Проверь: tail -20 AGENTS.md ; tail -20 .claude/agents/reviewer.md"
