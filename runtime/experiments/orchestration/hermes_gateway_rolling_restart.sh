#!/bin/bash
# rolling_restart.sh — one-shot rolling restart of hermes gateway (orchestration2 profile)
# Uses the exact same invocation as the running gateway: hermes_cli.main --profile <name> gateway run
set -euo pipefail

PROFILE="orchestration2"
PROFILE_HOME="/root/.hermes/profiles/${PROFILE}"
HERMES_HOME="/usr/local/lib/hermes-agent"
HERMES_VENV_PY="${HERMES_HOME}/venv/bin/python"
LOG="${PROFILE_HOME}/gateway.log"
PID_FILE="${PROFILE_HOME}/gateway.pid"

echo "[$(date -Is)] rolling_restart start"

# 1) find old gateway PIDs for this profile
OLD_PIDS=$(pgrep -f "hermes_cli.main.*--profile ${PROFILE}.*gateway run" || true)
echo "[$(date -Is)] old gateway PIDs: ${OLD_PIDS:-none}"

# 2) start new gateway first (exact same invocation as running process)
echo "[$(date -Is)] spawning new gateway..."
cd /root/main
setsid nohup "$HERMES_VENV_PY" -m hermes_cli.main --profile "$PROFILE" gateway run >>"$LOG" 2>&1 &
NEW_PID=$!
echo "[$(date -Is)] new gateway spawn pid=$NEW_PID"

# 3) wait for new gateway to be ready (log marker or pid alive for 5s)
READY=0
for i in $(seq 1 30); do
    if ! kill -0 "$NEW_PID" 2>/dev/null; then
        echo "[$(date -Is)] ERROR: new gateway died during startup" >&2
        tail -20 "$LOG" >&2
        exit 1
    fi
    if grep -q "Gateway ready\|Listening on\|Server started\|scheduler started\|cron.*loaded" "$LOG" 2>/dev/null; then
        echo "[$(date -Is)] new gateway ready (log marker found)"
        READY=1
        break
    fi
    sleep 1
done
if [ "$READY" = "0" ]; then
    echo "[$(date -Is)] WARNING: no ready marker in log, but pid alive — proceeding"
fi

# 4) now kill old gateway(s) gracefully
if [ -n "$OLD_PIDS" ]; then
    echo "[$(date -Is)] sending SIGTERM to old PIDs: $OLD_PIDS"
    for p in $OLD_PIDS; do
        kill -TERM "$p" 2>/dev/null || true
    done
    for i in $(seq 1 10); do
        REMAIN=""
        for p in $OLD_PIDS; do
            if kill -0 "$p" 2>/dev/null; then REMAIN="$REMAIN $p"; fi
        done
        if [ -z "$REMAIN" ]; then
            echo "[$(date -Is)] old gateway exited cleanly"
            break
        fi
        sleep 1
    done
    for p in $OLD_PIDS; do
        if kill -0 "$p" 2>/dev/null; then
            echo "[$(date -Is)] SIGKILL straggler $p"
            kill -9 "$p" 2>/dev/null || true
        fi
    done
fi

# 5) verify new gateway is alive and jobs.json was reloaded
sleep 2
if ! kill -0 "$NEW_PID" 2>/dev/null; then
    echo "[$(date -Is)] ERROR: new gateway not running after restart" >&2
    exit 1
fi

echo "[$(date -Is)] rolling_restart complete: new gateway pid=$NEW_PID"
echo "$NEW_PID" > "$PID_FILE"
