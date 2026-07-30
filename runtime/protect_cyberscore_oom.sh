#!/bin/bash
# Keep cyberscore preferred under memory pressure; raise adj on heavy guests.
# Priority (lower adj = more protected):
#   cyberscore_try  -900
#   4 Hermes TG bots  50   (asfjash/default, worker, orch1, orch2) — next after cyberscore
#   heavy guests     300   (omniroute, camoufox, browsers, pyright)
# Safe to run periodically (cron / systemd timer). Idempotent.
set -uo pipefail

CYBERSCORE_PAT='base/cyberscore_try.py'
PROTECT_ADJ=-900
HERMES_BOT_ADJ=50
GUEST_ADJ=300

set_adj() {
  local pid="$1" want="$2"
  [ -d "/proc/$pid" ] || return 0
  [ -r "/proc/$pid/oom_score_adj" ] || return 0
  [ -w "/proc/$pid/oom_score_adj" ] || return 0
  local cur
  cur="$(cat "/proc/$pid/oom_score_adj" 2>/dev/null || echo 0)"
  if [ "$cur" != "$want" ]; then
    echo "$want" > "/proc/$pid/oom_score_adj" 2>/dev/null || true
  fi
}

read_cmd() {
  local pid="$1"
  [ -r "/proc/$pid/cmdline" ] || { echo ""; return 0; }
  tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true
}

# 1) Protect live pipeline (all matching PIDs)
while read -r pid; do
  [ -n "${pid:-}" ] || continue
  cmd="$(read_cmd "$pid")"
  case "$cmd" in
    *"$CYBERSCORE_PAT"*) set_adj "$pid" "$PROTECT_ADJ" ;;
  esac
done < <(ps -eo pid=)

# 2) Hermes TG gateways (4 bots) — next after cyberscore
# 3) Guests that balloon: scrapers / routers / langservers
while read -r pid; do
  [ -n "${pid:-}" ] || continue
  cmd="$(read_cmd "$pid")"
  case "$cmd" in
    *"$CYBERSCORE_PAT"*) continue ;;
    *hermes_cli.main*gateway*|*hermes_cli.main*--profile*gateway*)
      set_adj "$pid" "$HERMES_BOT_ADJ"
      ;;
    *omniroute*|*pyright-langserver*|*camoufox*|*firefox*|*chromium*)
      set_adj "$pid" "$GUEST_ADJ"
      ;;
  esac
done < <(ps -eo pid=)

exit 0
