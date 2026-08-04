#!/usr/bin/env python3
"""Small, dependency-free Telegram control plane for the swarm orchestrator.

Secrets are accepted only through environment variables.  OmniRoute is deliberately
restricted to a loopback management endpoint and to one pinned API/schema version.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import signal
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


PINNED_OMNIROUTE_VERSION = "3.8.46"
PINNED_SCHEMA = 1
REDACT_PATTERNS = (
    (re.compile(r"(?i)\b(?:bearer|token|secret|password)\s*[:=]?\s*[^\s,]+"), "[secret]"),
    (re.compile(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}"), "[email]"),
    (re.compile(r"\b\d{7,}:[A-Za-z0-9_-]{20,}\b"), "[telegram-token]"),
)


class ConfigurationError(RuntimeError):
    pass


class OmniRouteError(RuntimeError):
    pass


def redact(value: object) -> str:
    text = str(value)
    for pattern, replacement in REDACT_PATTERNS:
        text = pattern.sub(replacement, text)
    return text[:3500]


def account_alias(account_id: str) -> str:
    return "acct-" + hashlib.sha256(account_id.encode()).hexdigest()[:10]


@dataclass(frozen=True)
class Settings:
    telegram_token: str
    allowed_user_id: int
    orchestrator_command: tuple[str, ...]
    omniroute_url: str
    omniroute_key: str
    omniroute_version: str = PINNED_OMNIROUTE_VERSION
    oauth_command: tuple[str, ...] = ()

    @classmethod
    def from_env(cls) -> "Settings":
        token = os.environ.get("SWARM_TELEGRAM_BOT_TOKEN", "").strip()
        uid = os.environ.get("SWARM_TELEGRAM_ALLOWED_USER_ID", "").strip()
        command = _json_command("SWARM_ORCHESTRATOR_COMMAND", required=True)
        oauth_command = _json_command("SWARM_CODEX_DEVICE_LOGIN_COMMAND", required=False)
        url = os.environ.get("OMNIROUTE_MANAGEMENT_URL", "http://127.0.0.1:20128").rstrip("/")
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ConfigurationError("OMNIROUTE_MANAGEMENT_URL must be loopback HTTP")
        if not token or not uid.isdigit():
            raise ConfigurationError("Telegram token and numeric allowlisted user id are required")
        key = os.environ.get("OMNIROUTE_MANAGEMENT_KEY", "").strip()
        if not key:
            raise ConfigurationError("OMNIROUTE_MANAGEMENT_KEY is required")
        return cls(token, int(uid), command, url, key,
                   os.environ.get("OMNIROUTE_EXPECTED_VERSION", PINNED_OMNIROUTE_VERSION), oauth_command)


def _json_command(name: str, *, required: bool) -> tuple[str, ...]:
    raw = os.environ.get(name, "")
    if not raw and not required:
        return ()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigurationError(f"{name} must be a JSON array") from exc
    if not isinstance(value, list) or not value or not all(isinstance(x, str) and x for x in value):
        raise ConfigurationError(f"{name} must be a non-empty JSON string array")
    return tuple(value)


class OmniRouteClient:
    """Strict adapter for the pinned OmniRoute 3.8.46 management API."""

    def __init__(self, base_url: str, key: str, expected_version: str = PINNED_OMNIROUTE_VERSION,
                 opener: Callable[..., Any] = urllib.request.urlopen):
        self.base_url, self.key, self.expected_version, self.opener = base_url.rstrip("/"), key, expected_version, opener
        self._mutations = threading.Lock()
        self._aliases: dict[str, str] = {}

    def _request(self, method: str, path: str, body: object | None = None) -> Any:
        data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        req = urllib.request.Request(self.base_url + path, data=data, method=method,
                                     headers={"Authorization": "Bearer " + self.key,
                                              "Content-Type": "application/json"})
        try:
            with self.opener(req, timeout=10) as response:
                payload = json.loads(response.read().decode())
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            raise OmniRouteError("OmniRoute management request failed") from exc
        if not isinstance(payload, dict):
            raise OmniRouteError("Unexpected OmniRoute response schema")
        return payload

    def verify(self) -> None:
        payload = self._request("GET", "/api/version")
        version = payload.get("version")
        schema = payload.get("schemaVersion", payload.get("schema_version", PINNED_SCHEMA))
        if version != self.expected_version or schema != PINNED_SCHEMA:
            raise OmniRouteError("OmniRoute version/schema mismatch; management disabled")

    def accounts(self) -> list[dict[str, Any]]:
        self.verify()
        payload = self._request("GET", "/api/providers")
        connections = payload.get("connections")
        if not isinstance(connections, list):
            raise OmniRouteError("Unexpected providers schema")
        result, aliases = [], {}
        for item in connections:
            if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not isinstance(item.get("isActive"), bool):
                raise OmniRouteError("Unexpected provider entry schema")
            raw_id = item["id"]
            alias = account_alias(raw_id)
            aliases[alias] = raw_id
            result.append({"alias": alias, "active": item["isActive"],
                           "provider": str(item.get("provider", item.get("type", "unknown")))[:32],
                           "status": str(item.get("status", "unknown"))[:32]})
        self._aliases = aliases
        return result

    def _resolve(self, alias: str) -> str:
        self.accounts()
        try:
            return self._aliases[alias]
        except KeyError as exc:
            raise OmniRouteError("Unknown account alias; run /accounts again") from exc

    def set_active(self, alias: str, active: bool) -> None:
        with self._mutations:
            account_id = self._resolve(alias)
            payload = self._request("PATCH", "/api/providers", {"ids": [account_id], "isActive": active})
            if payload.get("success") is not True:
                raise OmniRouteError("OmniRoute rejected account update")

    def remove(self, alias: str) -> None:
        with self._mutations:
            account_id = self._resolve(alias)
            payload = self._request("DELETE", "/api/providers", {"ids": [account_id]})
            if payload.get("success") is not True:
                raise OmniRouteError("OmniRoute rejected account removal")

    def check(self, alias: str) -> str:
        account_id = self._resolve(alias)
        payload = self._request("POST", f"/api/providers/{urllib.parse.quote(account_id, safe='')}/test")
        if not isinstance(payload.get("success"), bool):
            raise OmniRouteError("Unexpected account test schema")
        return "ok" if payload["success"] else "failed"

    def import_codex(self, auth: object) -> None:
        with self._mutations:
            self.verify()
            payload = self._request("POST", "/api/oauth/codex/import", {"accounts": auth})
            if payload.get("success") is not True:
                raise OmniRouteError("OmniRoute rejected Codex account import")


@dataclass
class PendingAction:
    action: str
    argument: str
    code: str
    expires: float


class OrchestratorProcess:
    def __init__(self, command: tuple[str, ...], popen: Callable[..., Any] | None = None):
        import subprocess
        self.command = command
        self.popen = popen or subprocess.Popen
        self.process: Any = None
        self.output: Any = None
        self.started = 0.0
        self._lock = threading.Lock()

    def run(self, task: str) -> None:
        if not task.strip():
            raise ValueError("task is empty")
        with self._lock:
            if self.process is not None and self.process.poll() is None:
                raise RuntimeError("orchestrator is busy")
            self.output = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
            self.process = self.popen(self.command, stdin=-1, stdout=self.output, stderr=-2,
                                      text=True, start_new_session=True)
            self.process.stdin.write(task)
            self.process.stdin.close()
            self.started = time.time()

    def status(self) -> str:
        with self._lock:
            if self.process is None:
                return "idle"
            code = self.process.poll()
            return f"running ({int(time.time() - self.started)}s)" if code is None else f"finished (exit {code})"

    def cancel(self) -> bool:
        with self._lock:
            if self.process is None or self.process.poll() is not None:
                return False
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
            except ProcessLookupError:
                return False
            process = self.process
            def force_kill() -> None:
                if process.poll() is None:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            timer = threading.Timer(5, force_kill)
            timer.daemon = True
            timer.start()
            return True


class SwarmBot:
    def __init__(self, allowed_user_id: int, omni: OmniRouteClient, orchestrator: OrchestratorProcess,
                 oauth_command: tuple[str, ...] = ()):
        self.allowed_user_id, self.omni, self.orchestrator = allowed_user_id, omni, orchestrator
        self.oauth_command = oauth_command
        self.pending: PendingAction | None = None
        self.oauth: dict[str, Any] | None = None

    def handle(self, user_id: int, chat_type: str, text: str) -> str | None:
        if user_id != self.allowed_user_id or chat_type != "private":
            return None
        command, _, argument = text.strip().partition(" ")
        try:
            if command in {"/help", "/start"}:
                return HELP
            if command == "/run":
                self.orchestrator.run(argument)
                return "Задача запущена. /status — состояние, /cancel — остановка."
            if command == "/status":
                return self.orchestrator.status()
            if command == "/cancel":
                return "Остановка запрошена." if self.orchestrator.cancel() else "Активной задачи нет."
            if command == "/accounts":
                rows = self.omni.accounts()
                return "Пул пуст." if not rows else "\n".join(
                    f"{x['alias']} | {x['provider']} | {'on' if x['active'] else 'off'} | {x['status']}" for x in rows)
            if command == "/account_check":
                return f"{argument}: {self.omni.check(argument)}"
            if command in {"/account_enable", "/account_disable", "/account_remove", "/account_add"}:
                if command != "/account_add" and not argument:
                    return "Укажите alias из /accounts."
                if command == "/account_add" and not self.oauth_command:
                    return "Добавление отключено: device-login helper не настроен."
                if command == "/account_add" and self.oauth is not None:
                    return self._poll_device_login()
                return self._prepare(command[1:], argument)
            if command == "/confirm":
                return self._confirm(argument)
            return HELP
        except (RuntimeError, ValueError, OmniRouteError) as exc:
            return "Ошибка: " + redact(exc)

    def _prepare(self, action: str, argument: str) -> str:
        code = secrets.token_hex(3)
        self.pending = PendingAction(action, argument, code, time.time() + 120)
        target = argument or "новый Codex OAuth account"
        return f"Подтвердите {action} для {target}: /confirm {code} (120 секунд)"

    def _confirm(self, code: str) -> str:
        pending, self.pending = self.pending, None
        if pending is None or pending.expires < time.time() or not secrets.compare_digest(pending.code, code):
            return "Подтверждение недействительно или истекло."
        if pending.action == "account_enable":
            self.omni.set_active(pending.argument, True)
        elif pending.action == "account_disable":
            self.omni.set_active(pending.argument, False)
        elif pending.action == "account_remove":
            self.omni.remove(pending.argument)
        elif pending.action == "account_add":
            return self._device_login_and_import()
        return "Готово."

    def _device_login_and_import(self) -> str:
        """Start isolated device auth and relay only its public URL/user code."""
        import subprocess
        home = tempfile.mkdtemp(prefix="swarm-codex-auth-")
        output = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
        env = {**os.environ, "CODEX_HOME": home}
        try:
            process = subprocess.Popen(self.oauth_command, env=env, stdin=-3, stdout=output,
                                       stderr=-2, text=True, start_new_session=True)
        except OSError as exc:
            Path(home).rmdir()
            output.close()
            raise RuntimeError("device login failed") from exc
        self.oauth = {"home": home, "output": output, "process": process, "started": time.time()}
        deadline, public = time.time() + 5, ""
        while time.time() < deadline and not public:
            time.sleep(0.1)
            public = self._oauth_public_text()
        if process.poll() not in (None, 0):
            self._cleanup_oauth()
            raise RuntimeError("device login did not start")
        return ((public + "\n") if public else "Device login запущен.\n") + \
               "После авторизации повторите /account_add для безопасного импорта."

    def _oauth_public_text(self) -> str:
        if self.oauth is None:
            return ""
        output = self.oauth["output"]
        output.flush(); output.seek(0)
        text = output.read(12000)
        output.seek(0, 2)  # subprocess and reader share a file offset; preserve append semantics
        urls = re.findall(r"https://auth\.openai\.com/[A-Za-z0-9_./-]+", text)
        codes = re.findall(r"(?i)(?:code[^A-Z0-9]{0,8})([A-Z0-9][A-Z0-9-]{4,})", text)
        parts = (["URL: " + urls[0]] if urls else []) + (["Code: " + codes[0].upper()] if codes else [])
        return "\n".join(parts)

    def _poll_device_login(self) -> str:
        assert self.oauth is not None
        process = self.oauth["process"]
        if process.poll() is None:
            if time.time() - self.oauth["started"] > 300:
                try: os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError: pass
                self._cleanup_oauth()
                return "Device login истёк; запустите /account_add заново."
            return (self._oauth_public_text() or "Device login ещё ожидает авторизацию.")
        if process.returncode != 0:
            self._cleanup_oauth()
            raise RuntimeError("device login did not complete")
        auth_path = Path(self.oauth["home"]) / "auth.json"
        try:
            auth = json.loads(auth_path.read_text(encoding="utf-8"))
            self.omni.import_codex(auth)
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError("device login produced no valid auth file") from exc
        finally:
            self._cleanup_oauth()
        return "Codex account импортирован; временные credentials удалены."

    def _cleanup_oauth(self) -> None:
        if self.oauth is None:
            return
        import shutil
        self.oauth["output"].close()
        shutil.rmtree(self.oauth["home"], ignore_errors=True)
        self.oauth = None


HELP = """Команды:
/run <задача> — запустить оркестрацию
/status, /cancel
/accounts
/account_add
/account_enable <alias>, /account_disable <alias>
/account_remove <alias>, /account_check <alias>
Все изменения пула требуют /confirm."""


class TelegramAPI:
    def __init__(self, token: str):
        self.url = f"https://api.telegram.org/bot{token}/"

    def call(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        req = urllib.request.Request(self.url + method, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=45) as response:
            result = json.loads(response.read().decode())
        if result.get("ok") is not True:
            raise RuntimeError("Telegram API failure")
        return result


def main() -> int:
    settings = Settings.from_env()
    telegram = TelegramAPI(settings.telegram_token)
    bot = SwarmBot(settings.allowed_user_id,
                   OmniRouteClient(settings.omniroute_url, settings.omniroute_key, settings.omniroute_version),
                   OrchestratorProcess(settings.orchestrator_command), settings.oauth_command)
    offset = 0
    while True:
        try:
            updates = telegram.call("getUpdates", {"offset": offset, "timeout": 30, "allowed_updates": ["message"]})["result"]
            for update in updates:
                offset = max(offset, int(update["update_id"]) + 1)
                message = update.get("message", {})
                sender, chat = message.get("from", {}), message.get("chat", {})
                reply = bot.handle(sender.get("id", -1), chat.get("type", ""), message.get("text", ""))
                if reply is not None:
                    telegram.call("sendMessage", {"chat_id": chat["id"], "text": redact(reply)})
        except (OSError, KeyError, ValueError, RuntimeError):
            time.sleep(2)


if __name__ == "__main__":
    raise SystemExit(main())
