#!/bin/bash
# Переключатель профилей OpenCode на сервере (Telegram gate).
# Не ломает соседние профили: активные файлы — копии из profiles/.
#
#   ./opencode-switch.sh glm      # GLM 5.2 (OpenCode Zen/Go) — прежний
#   ./opencode-switch.sh grok     # Grok 4.5 high через xAI / Grok Build tokens
#   ./opencode-switch.sh cursor   # Grok 4.5 high-fast через Cursor subscription
#   ./opencode-switch.sh zen      # Claude Opus 4.8 brains + GLM worker (legacy)
#   ./opencode-switch.sh status
#   ./opencode-switch.sh apply    # применить + restart services

set -euo pipefail
ROOT="/root/main"
PROFILES="$ROOT/opencode-profiles"
ACTIVE_PROJECT="$ROOT/opencode.json"
ACTIVE_AGENTS="$ROOT/.opencode/agent"
ACTIVE_GLOBAL="/root/.config/opencode/opencode.json"
TARGET="${1:-}"

apply_profile() {
  local name="$1"
  local pdir="$PROFILES/$name"
  if [[ ! -d "$pdir" ]]; then
    echo "profile not found: $pdir" >&2
    exit 1
  fi
  # project config
  cp -f "$pdir/opencode.json" "$ACTIVE_PROJECT"
  # agents
  mkdir -p "$ACTIVE_AGENTS"
  cp -f "$pdir/agent/"*.md "$ACTIVE_AGENTS/"
  # global
  if [[ -f "$pdir/global-opencode.json" ]]; then
    cp -f "$pdir/global-opencode.json" "$ACTIVE_GLOBAL"
  fi
  # active marker
  echo "$name" > "$PROFILES/ACTIVE"
  echo "✅ Active profile: $name"
  python3 - <<'PY'
import json
from pathlib import Path
p=json.loads(Path("/root/main/opencode.json").read_text())
g=json.loads(Path("/root/.config/opencode/opencode.json").read_text())
print("  project model:", p.get("model"))
print("  global  model:", g.get("model"))
for name in ("planner","worker","reviewer"):
    t=Path(f"/root/main/.opencode/agent/{name}.md").read_text().splitlines()[:8]
    model=next((l for l in t if l.startswith("model:")), "?")
    print(f"  agent {name}: {model}")
PY
}

restart_services() {
  # refresh xai token if grok/xai profile
  if [[ "$(cat "$PROFILES/ACTIVE" 2>/dev/null || true)" == "xai-grok-4.5-high" ]]; then
    /usr/local/bin/xai-token-refresh.py || true
  fi
  systemctl restart opencode.service --no-block
  sleep 2
  systemctl restart opencode-telegram-bridge.service --no-block
  sleep 1
  systemctl is-active opencode.service opencode-telegram-bridge.service
  echo "Services restarted."
}

case "$TARGET" in
  glm|GLM|glm-5.2|5.2)
    apply_profile "glm-5.2"
    ;;
  grok|GROK|xai|grok-4.5|high)
    apply_profile "xai-grok-4.5-high"
    ;;
  cursor|CURSOR|cursor-grok|grok-cursor|high-fast)
    apply_profile "cursor-grok-4.5-high-fast"
    ;;
  zen|ZEN|opus|claude)
    apply_profile "zen-opus4.8"
    ;;
  status|show|current)
    echo "ACTIVE: $(cat "$PROFILES/ACTIVE" 2>/dev/null || echo unknown)"
    apply_profile(){ :; }
    python3 - <<'PY'
import json
from pathlib import Path
for label, path in [("project","/root/main/opencode.json"),("global","/root/.config/opencode/opencode.json")]:
  d=json.loads(Path(path).read_text())
  print(f"{label} model:", d.get("model"))
for name in ("planner","worker","reviewer"):
  p=Path(f"/root/main/.opencode/agent/{name}.md")
  if p.exists():
    model=next((l for l in p.read_text().splitlines() if l.startswith("model:")), "?")
    print(f"agent {name}:", model)
auth=json.loads(Path("/root/.local/share/opencode/auth.json").read_text())
print("auth providers:", list(auth.keys()))
if "xai" in auth:
  x=auth["xai"]
  print("  xai source:", x.get("source"), "email:", x.get("email"), "expires:", x.get("expires_at"))
PY
    systemctl is-active opencode.service opencode-telegram-bridge.service || true
    exit 0
    ;;
  apply|restart)
    restart_services
    exit 0
    ;;
  *)
    echo "Usage: $0 {glm|grok|cursor|zen|status|apply}"
    echo "  glm   — GLM 5.2 (OpenCode Zen tokens) [previous]"
    echo "  grok   — Grok 4.5 high (xAI / Grok Build subscription tokens)"
    echo "  cursor — Grok 4.5 high-fast (Cursor subscription tokens)"
    echo "  zen    — Claude Opus 4.8 + GLM worker (legacy)"
    echo "  status"
    echo "  apply — restart opencode + telegram bridge (refresh xai if grok)"
    exit 1
    ;;
esac

echo
echo "Чтобы применить к Telegram gate: $0 apply"
