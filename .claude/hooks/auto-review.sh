#!/usr/bin/env bash
# Stop hook. Blocks finishing until the `reviewer` subagent reviewed every
# edited file. The marker .claude/.pending-review is cleared ONLY by the
# reviewer (on APPROVE). A safety counter prevents an infinite loop.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PENDING="$ROOT/.claude/.pending-review"
NAG="$ROOT/.claude/.review-nag-count"

if [ ! -s "$PENDING" ]; then
	echo 0 > "$NAG" 2>/dev/null || true
	exit 0
fi

count="$(cat "$NAG" 2>/dev/null || echo 0)"
count=$((count + 1))
echo "$count" > "$NAG" 2>/dev/null || true
if [ "$count" -gt 6 ]; then
	echo 0 > "$NAG" 2>/dev/null || true
	exit 0
fi

FILES="$(tr '\n' ' ' < "$PENDING")"
cat <<EOF
{"decision":"block","reason":"Files were modified and NOT yet reviewed: ${FILES}. Your IMMEDIATE and ONLY next action is to invoke the \`reviewer\` subagent now. Do NOT write code, run commands, or answer the user before that. The reviewer reads .claude/.pending-review, reviews those files, and clears the marker on APPROVE. If it returns REQUEST CHANGES or BLOCK, fix the Critical issues and the cycle repeats until APPROVE."}
EOF
exit 0
