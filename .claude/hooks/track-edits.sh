#!/usr/bin/env bash
# PostToolUse hook (matcher: Write|Edit|MultiEdit)
# Records EVERY edited file path so the Stop hook can require a review.
# Works for ANY file, including git-ignored ones like runtime/.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PENDING="$ROOT/.claude/.pending-review"

INPUT="$(cat)"
FILE="$(printf '%s' "$INPUT" | jq -r '.tool_input.file_path // .tool_input.path // empty' 2>/dev/null || true)"

[ -z "${FILE:-}" ] && exit 0

mkdir -p "$ROOT/.claude"
touch "$PENDING"
grep -qxF "$FILE" "$PENDING" 2>/dev/null || printf '%s\n' "$FILE" >> "$PENDING"
exit 0
