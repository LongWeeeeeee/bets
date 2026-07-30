#!/bin/zsh
set -euo pipefail

ROOT="/Users/alex/Documents/ingame"
LOG_PATH="${ROOT}/noodds.log"
SESSION_PREFIX="cyberscore_noodds"
TS="$(date '+%Y%m%d_%H%M%S')"
SESSION_NAME="${SESSION_PREFIX}_${TS}"

find_noodds_python_pid() {
  local pid=""
  local cmd=""
  while read -r pid; do
    [[ -z "${pid}" ]] && continue
    cmd="$(ps -p "${pid}" -o command= || true)"
    if [[ -n "${cmd}" && "${cmd}" == *"cyberscore_try.py --no-odds"* && "${cmd}" != *"nohup "* && "${cmd}" != *"bash -lc"* && "${cmd}" != *"login -pflq"* && "${cmd}" != *"SCREEN -dmS"* ]]; then
      echo "${pid}"
      return 0
    fi
  done < <(pgrep -f "cyberscore_try.py --no-odds" || true)
  return 1
}

cd "${ROOT}"

if existing_pid="$(find_noodds_python_pid)"; then
  echo "NO_ODDS_ALREADY_RUNNING=1"
  echo "PID=${existing_pid}"
  echo "LOG_PATH=${LOG_PATH}"
  exit 0
fi

if ! command -v screen >/dev/null 2>&1; then
  echo "ERROR: 'screen' is required for detached launch in this environment." >&2
  exit 1
fi

# Detached shell avoids child-process reap on parent shell exit in task-runner sessions.
screen -dmS "${SESSION_NAME}" bash -lc \
  "cd \"${ROOT}\" && PATH=\"${ROOT}/venv_catboost/bin:\$PATH\" nohup python3 base/cyberscore_try.py --no-odds >> \"${LOG_PATH}\" 2>&1"

sleep 2
if ! new_pid="$(find_noodds_python_pid)"; then
  echo "ERROR: launch failed, no live --no-odds process found." >&2
  exit 1
fi

echo "NO_ODDS_ALREADY_RUNNING=0"
echo "PID=${new_pid}"
echo "LOG_PATH=${LOG_PATH}"
echo "SCREEN_SESSION=${SESSION_NAME}"
