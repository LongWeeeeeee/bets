#!/bin/bash
# cccc-rotate-brain.sh — automatic ChatGPT chat rotation with Muse compact.
# No browser clicks: arms "new chat on next delivery" via CCCC web API,
# compacts mission state from the ledger via `muse exec`, delivers a
# bootstrap+mission message, and verifies the new chat binds and replies.
#
# Usage: scripts/run/cccc-rotate-brain.sh [--check] [--reason TEXT]
#   --check   only verify preconditions, change nothing
# Trigger phrase for the user: "ротируй".
set -u
GROUP="g_6aa70178816c"
ACTOR="brain"
WEB="http://127.0.0.1:8848"
CCCC="/Users/alex/.local/bin/cccc"
MUSE="/Users/alex/.local/bin/muse"
PY="/Users/alex/Documents/ingame/venv_catboost/bin/python3"
HOME_CCCC="$HOME/.cccc"
LEDGER="$HOME_CCCC/groups/$GROUP/ledger.jsonl"
TASKS_DIR="$HOME_CCCC/groups/$GROUP/context/tasks"
ART_DIR="/Users/alex/Documents/ingame/runtime/artifacts/orchestration"

CHECK_ONLY=0
REASON="context rotation"
while [ $# -gt 0 ]; do
  case "$1" in
    --check) CHECK_ONLY=1; shift;;
    --reason) REASON="$2"; shift 2;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

fail() { echo "ROTATE FAIL: $1" >&2; exit 1; }
api() { curl -s --max-time 15 "$@"; }

# 1. Preconditions: daemon + web reachable.
"$CCCC" status >/dev/null 2>&1 || fail "daemon unreachable (cccc status)"
api "$WEB/api/v1/ping" | grep -q '"ok":true' || fail "web unreachable ($WEB)"

# 2. Brain must be idle: no active turn, runtime waiting.
STATE_JSON="$(api "$WEB/api/v1/web-model/browser-session?group_id=$GROUP&actor_id=$ACTOR")"
echo "$STATE_JSON" | grep -q '"ok":true' || fail "browser-session query failed"
OLD_URL="$(echo "$STATE_JSON" | "$PY" -c "import json,sys; print(json.load(sys.stdin)['result']['browser_session'].get('conversation_url',''))")"
TURN="$("$PY" -c "
import yaml
g = yaml.safe_load(open('$HOME_CCCC/groups/$GROUP/group.yaml'))
print(repr(g.get('runtime_states', {}).get('$ACTOR', {})))" 2>/dev/null || echo "yaml-missing")"
echo "old chat: $OLD_URL"
echo "runtime: $TURN"
case "$TURN" in
  *\'status\':\ \'waiting\'*) case "$TURN" in *\'active_turn_id\':\ \'\'*) ;; *) fail "brain not idle, retry when turn closes: $TURN";; esac ;;
  *) fail "brain not idle, retry when turn closes: $TURN";;
esac
[ "$CHECK_ONLY" = "1" ] && { echo "CHECK OK: rotation possible"; exit 0; }

mkdir -p "$ART_DIR"
TS="$(date +%Y%m%d-%H%M%S)"
BRIEF="$ART_DIR/handoff-$TS.md"
PROMPT_TMP="$(mktemp /tmp/rotate-prompt.XXXXXX)"

# 3. Compact via Muse: ledger + tasks -> handoff brief.
{
  echo "Ledger (last 120 events):"
  tail -n 120 "$LEDGER"
  echo; echo "=== Tracked tasks ==="
  for f in "$TASKS_DIR"/*.yaml; do [ -f "$f" ] && { echo "--- $f"; cat "$f"; }; done
} > "$PROMPT_TMP.data"
cat > "$PROMPT_TMP" <<'EOF'
You are compacting CCCC group state into a handoff brief for a fresh ChatGPT
session that will continue as group foreman (actor: brain).
Input: ledger events + tracked task files appended after this instruction.
Write a markdown brief with exactly these sections:
## Mission (one paragraph: goal + current phase)
## Done (bullets with evidence: what + where proven)
## Open tasks (T-id, title, status, next step each)
## Key facts (paths, commands, decisions the next session must not re-derive)
## Next action (single concrete step)
Rules: no chat retelling, only state. If input is thin, say so in one line and
still emit all sections. Output markdown only.
==================== INPUT ====================
EOF
cat "$PROMPT_TMP.data" >> "$PROMPT_TMP"
rm -f "$PROMPT_TMP.data"
"$MUSE" exec --workspace /Users/alex/Documents/ingame --prompt-file "$PROMPT_TMP" > "$BRIEF" 2>"$BRIEF.err" \
  || fail "muse compact failed (see $BRIEF.err)"
rm -f "$PROMPT_TMP"
[ -s "$BRIEF" ] || fail "compact produced empty brief"
echo "brief: $BRIEF ($(wc -c < "$BRIEF") bytes)"

# 4. Arm new chat on next delivery (== UI "Start new chat on next delivery" + Save).
ARM="$(api -X POST "$WEB/api/v1/web-model/browser-session/bind-current" \
  -H 'Content-Type: application/json' \
  -d "{\"group_id\":\"$GROUP\",\"actor_id\":\"$ACTOR\",\"conversation_url\":\"\",\"new_chat\":true,\"clear\":false}")"
echo "$ARM" | grep -q '"ok":true' || fail "arm new chat failed: $(echo "$ARM" | head -c 300)"
sleep 3
api "$WEB/api/v1/web-model/browser-session?group_id=$GROUP&actor_id=$ACTOR" \
  | grep -q 'new_chat_armed' || fail "target not armed (expected new_chat_armed)"
echo "armed: next delivery starts a fresh chat"

# 5. Deliver bootstrap + mission message; CCCC creates and binds the new chat.
SEND_TS="$(date -u +%Y-%m-%dT%H:%M:%S)"
"$CCCC" send --to "$ACTOR" --mode request-reply \
  "ROTATION ($REASON). Call cccc_bootstrap. Then read mission brief: $BRIEF. Continue as foreman from 'Next action'. Reply with: rotated ok + mission restated in one sentence." \
  >/dev/null || fail "send failed"

# 6. Verify: new bound URL differs + brain reply newer than send.
for i in $(seq 1 30); do
  sleep 20
  NEW_STATE="$(api "$WEB/api/v1/web-model/browser-session?group_id=$GROUP&actor_id=$ACTOR")"
  NEW_URL="$(echo "$NEW_STATE" | "$PY" -c "import json,sys; print(json.load(sys.stdin)['result']['browser_session'].get('conversation_url',''))" 2>/dev/null)"
  REPLY="$($PY -c "
import json
n = 0
for line in open('$LEDGER'):
    try: e = json.loads(line)
    except ValueError: continue
    if e.get('kind') == 'chat.message' and e.get('by') == '$ACTOR' and e.get('ts','') >= '$SEND_TS': n += 1
print(n)")"
  echo "wait $((i*20))s url=${NEW_URL:-?} replies=$REPLY"
  if [ -n "$NEW_URL" ] && [ "$NEW_URL" != "$OLD_URL" ] && [ "$REPLY" -ge 1 ]; then
    echo "ROTATE OK: $OLD_URL -> $NEW_URL (brief: $BRIEF)"
    exit 0
  fi
done
fail "new chat did not bind/reply in 10 min. Target stays armed; next delivery still opens a fresh chat. Brief kept: $BRIEF"
