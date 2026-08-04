# Swarm Telegram bot

Standalone private control plane for the Codex-planner / Cursor-worker swarm. It is a
separate bot and service; it must not reuse or modify the existing Cursor bridge.

## Security contract

- Long polling only; commands are accepted only from one numeric user ID and only in
  a private chat. Other messages receive no reply.
- Bot token and OmniRoute management key exist only in a root-readable environment
  file. They must never be command-line arguments, source code, logs, or git.
- OmniRoute management is accepted only over loopback HTTP. The adapter pins
  OmniRoute `3.8.46` and schema `1`; a mismatch disables account management.
- Telegram shows deterministic hashed account aliases, never OmniRoute account IDs,
  OAuth credentials, tokens, or email addresses.
- Every pool mutation requires a short-lived second step: `/confirm <code>`.
- Tasks are passed to the orchestrator on stdin (never through a shell). Only one task
  runs at a time; `/cancel` sends SIGTERM to its whole process group.

## Configuration

Place these values in a mode `0600`, root-owned environment file:

```dotenv
SWARM_TELEGRAM_BOT_TOKEN=<secret>
SWARM_TELEGRAM_ALLOWED_USER_ID=<numeric-id>
SWARM_ORCHESTRATOR_COMMAND=["/root/main/runtime/swarm/run"]
OMNIROUTE_MANAGEMENT_URL=http://127.0.0.1:20128
OMNIROUTE_MANAGEMENT_KEY=<manage-scope-secret>
OMNIROUTE_EXPECTED_VERSION=3.8.46
```

Optional account enrollment is enabled only when an exact, audited device-login
command is configured:

```dotenv
SWARM_CODEX_DEVICE_LOGIN_COMMAND=["/root/.local/bin/codex","login","--device-auth"]
```

The command runs with a fresh temporary `CODEX_HOME`. The bot relays only the public
verification URL and one-time device code; repeat `/account_add` after authorization.
The bot then reads `auth.json` locally and sends it directly to OmniRoute
`POST /api/oauth/codex/import`, and destroys the temporary directory. Credential bytes
are never sent to Telegram or written to the service log. Enrollment expires after
five minutes. Production deployment should first verify the exact Codex CLI output and
OmniRoute import response against the pinned build.

## Commands

- `/run <task>`, `/status`, `/cancel`
- `/accounts`
- `/account_add`
- `/account_enable <alias>`, `/account_disable <alias>`
- `/account_remove <alias>`, `/account_check <alias>`
- `/confirm <code>` for any state-changing pool command

## Verification

```bash
/Users/alex/Documents/ingame/venv_catboost/bin/python3 -m pytest \
  base/tests/test_swarm_telegram_bot.py -v
```

Do not deploy the bot until the pinned OmniRoute `/api/version`, `/api/providers`,
`/api/providers/{id}/test`, and `/api/oauth/codex/import` responses have been checked
on the actual server build. The adapter intentionally fails closed rather than guessing
when a response changes.
