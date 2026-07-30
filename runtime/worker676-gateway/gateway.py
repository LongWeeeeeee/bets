#!/usr/bin/env python3
"""Telegram gateway for a single Codex worker routed through OmniRoute."""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


LOG = logging.getLogger("worker676-gateway")
MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024
IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp", ".gif"}


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def split_allowed(raw: str) -> set[str]:
    return {
        item.strip().lower().lstrip("@")
        for item in re.split(r"[,;\s]+", raw or "")
        if item.strip()
    }


def safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", name or "attachment")[:120]


def chunks(text: str, size: int = 3900) -> list[str]:
    remaining = text.strip() or "Готово."
    result: list[str] = []
    while len(remaining) > size:
        split_at = remaining.rfind("\n", 0, size)
        if split_at < size // 2:
            split_at = size
        result.append(remaining[:split_at])
        remaining = remaining[split_at:].lstrip()
    result.append(remaining)
    return result


def context_label(value: int) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:g}M"
    if value >= 1_000:
        return f"{value // 1_000}K"
    return str(value)


def safe_progress_text(value: str) -> str:
    value = re.sub(
        r"(?i)\b(api[_-]?key|authorization|secret|token)(\s*[:=]\s*)(\S+)",
        r"\1\2<redacted>",
        value,
    )
    return re.sub(r"\b[A-Za-z0-9_-]{48,}\b", "<redacted>", value)


class TelegramAPI:
    def __init__(self, token: str):
        self.token = token
        self.base = f"https://api.telegram.org/bot{token}/"

    def call(self, method: str, payload: dict[str, Any] | None = None, timeout: int = 45) -> Any:
        request = urllib.request.Request(
            self.base + method,
            data=json.dumps(payload or {}).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                data = json.loads(response.read())
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Telegram {method} failed: {exc}") from exc
        if not data.get("ok"):
            raise RuntimeError(f"Telegram {method} rejected request: {data.get('description')}")
        return data.get("result")

    def get_updates(self, offset: int | None) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {"timeout": 30, "allowed_updates": ["message"]}
        if offset is not None:
            payload["offset"] = offset
        return list(self.call("getUpdates", payload, timeout=40) or [])

    def send(
        self,
        chat_id: int,
        text: str,
        reply_to: int | None = None,
        *,
        silent: bool = False,
    ) -> int | None:
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
            "disable_notification": silent,
        }
        if reply_to:
            payload["reply_parameters"] = {"message_id": reply_to}
        result = self.call("sendMessage", payload)
        return int(result["message_id"]) if isinstance(result, dict) else None

    def edit(self, chat_id: int, message_id: int, text: str) -> None:
        self.call("editMessageText", {"chat_id": chat_id, "message_id": message_id, "text": text})

    def typing(self, chat_id: int) -> None:
        self.call("sendChatAction", {"chat_id": chat_id, "action": "typing"}, timeout=15)

    def download(self, file_id: str, target: Path) -> None:
        info = self.call("getFile", {"file_id": file_id})
        size = int(info.get("file_size") or 0)
        if size > MAX_ATTACHMENT_BYTES:
            raise RuntimeError("Файл больше 20 МБ.")
        remote = str(info.get("file_path") or "")
        if not remote:
            raise RuntimeError("Telegram не вернул путь к файлу.")
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        request = urllib.request.Request(f"https://api.telegram.org/file/bot{self.token}/{remote}")
        with urllib.request.urlopen(request, timeout=120) as response, target.open("wb") as output:
            while block := response.read(1024 * 1024):
                output.write(block)
        target.chmod(0o600)


@dataclass(frozen=True)
class Model:
    id: str
    owner: str
    context: int


class ModelCatalog:
    def __init__(self, api_key: str, storage: Path, base_url: str):
        self.api_key = api_key
        self.storage = storage
        self.base_url = base_url.rstrip("/")
        self.lock = threading.Lock()
        self.cached_at = 0.0
        self.cached: dict[str, Model] = {}

    def _active_providers(self) -> tuple[set[str], dict[str, str]]:
        with sqlite3.connect(self.storage) as db:
            active = {
                str(row[0])
                for row in db.execute(
                    "SELECT DISTINCT provider FROM provider_connections WHERE is_active=1"
                )
            }
            prefixes = {
                str(row[0]): str(row[1])
                for row in db.execute("SELECT id,prefix FROM provider_nodes")
                if row[1]
            }
        return active, prefixes

    def _canonical_prefixes(self, active: set[str], custom: dict[str, str]) -> dict[str, str]:
        result = {provider: provider for provider in active if not provider.startswith(("openai-compatible-", "anthropic-compatible-"))}
        result.update({provider: prefix for provider, prefix in custom.items() if provider in active})
        return result

    def refresh(self, force: bool = False) -> dict[str, Model]:
        with self.lock:
            if not force and self.cached and time.monotonic() - self.cached_at < 60:
                return dict(self.cached)
            request = urllib.request.Request(
                self.base_url + "/v1/models",
                headers={"Authorization": "Bearer " + self.api_key},
            )
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = json.loads(response.read())
            active, custom = self._active_providers()
            prefixes = self._canonical_prefixes(active, custom)
            models: dict[str, Model] = {}
            canonical_by_target: dict[str, Model] = {}
            for raw in payload.get("data", []):
                model_id = str(raw.get("id") or "")
                owner = str(raw.get("owned_by") or "")
                capabilities = raw.get("capabilities") or {}
                if owner not in active or not capabilities.get("tool_calling"):
                    continue
                if model_id.startswith("no-think/"):
                    continue
                canonical_prefix = prefixes.get(owner, owner)
                if not model_id.startswith(canonical_prefix + "/"):
                    continue
                context = int(raw.get("context_length") or 200_000)
                model = Model(model_id, owner, context)
                models[model_id] = model
                root = str(raw.get("root") or model_id.rsplit("/", 1)[-1])
                canonical_by_target[f"{owner}/{root}"] = model
            with sqlite3.connect(self.storage) as db:
                for alias, encoded in db.execute(
                    "SELECT key,value FROM key_value WHERE namespace='modelAliases'"
                ):
                    try:
                        target = str(json.loads(encoded))
                    except (TypeError, ValueError, json.JSONDecodeError):
                        continue
                    owner, _, root = target.partition("/")
                    if owner not in active or not root:
                        continue
                    canonical = canonical_by_target.get(target)
                    if canonical:
                        models[str(alias)] = Model(str(alias), owner, canonical.context)
            self.cached = models
            self.cached_at = time.monotonic()
            return dict(models)


class StateStore:
    def __init__(self, path: Path, default_model: str):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = path
        self.default_model = default_model
        self.lock = threading.Lock()
        with self._connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS chat_state (chat_id INTEGER PRIMARY KEY, thread_id TEXT, model TEXT NOT NULL, updated_at INTEGER NOT NULL)"
            )
            db.execute("CREATE TABLE IF NOT EXISTS bot_state (key TEXT PRIMARY KEY, value TEXT)")

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=30)

    def get_chat(self, chat_id: int) -> tuple[str | None, str]:
        with self.lock, self._connect() as db:
            row = db.execute("SELECT thread_id,model FROM chat_state WHERE chat_id=?", (chat_id,)).fetchone()
        return (str(row[0]) if row and row[0] else None, str(row[1])) if row else (None, self.default_model)

    def save_chat(self, chat_id: int, thread_id: str | None, model: str) -> None:
        with self.lock, self._connect() as db:
            db.execute(
                "INSERT INTO chat_state(chat_id,thread_id,model,updated_at) VALUES(?,?,?,?) ON CONFLICT(chat_id) DO UPDATE SET thread_id=excluded.thread_id,model=excluded.model,updated_at=excluded.updated_at",
                (chat_id, thread_id, model, int(time.time())),
            )

    def get_offset(self) -> int | None:
        with self.lock, self._connect() as db:
            row = db.execute("SELECT value FROM bot_state WHERE key='telegram_offset'").fetchone()
        return int(row[0]) if row else None

    def save_offset(self, offset: int) -> None:
        with self.lock, self._connect() as db:
            db.execute(
                "INSERT INTO bot_state(key,value) VALUES('telegram_offset',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(offset),),
            )


class CodexRunner:
    def __init__(
        self,
        binary: str,
        workspace: Path,
        api_key: str,
        model_efforts: dict[str, str] | None = None,
    ):
        self.binary = binary
        self.workspace = workspace
        self.api_key = api_key
        self.model_efforts = dict(model_efforts or {})
        self.lock = threading.Lock()
        self.active: dict[str, subprocess.Popen[str]] = {}

    def stop(self, run_key: str) -> bool:
        with self.lock:
            process = self.active.get(run_key)
        if not process or process.poll() is not None:
            return False
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return False
        return True

    def run(
        self,
        run_key: str,
        model: Model,
        prompt: str,
        thread_id: str | None,
        attachments: list[Path],
        on_thread: Callable[[str], None] | None = None,
        on_event: Callable[[dict[str, Any]], None] | None = None,
    ) -> tuple[str, str]:
        common = [
            "--json",
            "--skip-git-repo-check",
            "--dangerously-bypass-approvals-and-sandbox",
            "-m",
            model.id,
            "-c",
            f"model_context_window={max(32768, model.context)}",
        ]
        effort = self.model_efforts.get(model.id)
        if effort:
            common.extend(["-c", f'model_reasoning_effort="{effort}"'])
        images = [path for path in attachments if path.suffix.lower() in IMAGE_SUFFIXES]
        other = [path for path in attachments if path not in images]
        if other:
            prompt += "\n\nПрикреплённые файлы находятся здесь:\n" + "\n".join(str(path) for path in other)
        if thread_id:
            command = [self.binary, "exec", "resume", *common]
            for image in images:
                command.extend(["-i", str(image)])
            command.extend([thread_id, prompt])
        else:
            command = [self.binary, "exec", *common, "-C", str(self.workspace)]
            for image in images:
                command.extend(["-i", str(image)])
            command.append(prompt)
        env = os.environ.copy()
        env["OMNIROUTE_API_KEY"] = self.api_key
        process = subprocess.Popen(
            command,
            cwd=self.workspace,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        with self.lock:
            self.active[run_key] = process
        stdout_lines: list[str] = []
        stderr_lines: list[str] = []

        def drain_stderr() -> None:
            assert process.stderr is not None
            stderr_lines.extend(process.stderr.readlines())

        stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        stderr_thread.start()
        try:
            # There is deliberately no wall-clock timeout here. Agentic tasks can
            # spend a long time inside model calls or tools; the user controls
            # cancellation explicitly with /stop. Stream stdout so the session id
            # is checkpointed as soon as Codex creates it, not only on completion.
            assert process.stdout is not None
            for line in process.stdout:
                stdout_lines.append(line)
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if event.get("type") == "thread.started" and event.get("thread_id"):
                    started_thread = str(event["thread_id"])
                    if on_thread:
                        on_thread(started_thread)
                if on_event:
                    try:
                        on_event(event)
                    except Exception:
                        LOG.debug("Cannot publish progress event", exc_info=True)
            process.wait()
            stderr_thread.join(timeout=5)
        finally:
            with self.lock:
                self.active.pop(run_key, None)
        stdout = "".join(stdout_lines)
        stderr = "".join(stderr_lines)
        new_thread = thread_id
        messages: list[str] = []
        errors: list[str] = []
        for line in stdout.splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("type") == "thread.started":
                new_thread = str(event.get("thread_id") or new_thread or "")
            item = event.get("item") if event.get("type") == "item.completed" else None
            if isinstance(item, dict) and item.get("type") == "agent_message" and item.get("text"):
                messages.append(str(item["text"]))
            if isinstance(item, dict) and item.get("type") == "error" and item.get("message"):
                message = str(item["message"])
                if not message.startswith("Model metadata for"):
                    errors.append(message)
            if event.get("type") == "turn.failed":
                errors.append(str((event.get("error") or {}).get("message") or "turn failed"))
        if process.returncode != 0 or not messages or not new_thread:
            detail = " | ".join(errors) or (stderr or stdout)[-1500:] or f"exit {process.returncode}"
            raise RuntimeError(detail)
        return messages[-1], new_thread


@dataclass
class RunTask:
    chat_id: int
    message_id: int
    prompt: str
    attachments: list[Path]
    task_id: str = ""
    background: bool = False
    clarification: bool = False


class Gateway:
    def __init__(self):
        token_path = Path(os.environ.get("BOT_TOKEN_FILE", Path(os.environ["CREDENTIALS_DIRECTORY"]) / "telegram-token"))
        token = token_path.read_text().strip()
        shared = read_env(Path(os.environ["OMNIROUTE_ENV_FILE"]))
        api_key = shared.get("OMNIROUTE_API_KEY") or shared.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OMNIROUTE_API_KEY is missing")
        self.allowed = split_allowed(os.environ.get("TELEGRAM_ALLOWED_USERS", "") or shared.get("TELEGRAM_ALLOWED_USERS", ""))
        if not self.allowed:
            raise RuntimeError("TELEGRAM_ALLOWED_USERS must not be empty for an unrestricted tool bot")
        self.api = TelegramAPI(token)
        identity = self.api.call("getMe")
        self.username = str(identity.get("username") or "worker676_bot")
        self.default_model = os.environ.get("DEFAULT_MODEL", "claude-fable-5")
        self.state_root = Path(os.environ.get("STATE_ROOT", "/root/.local/state/worker676-gateway"))
        self.state_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.state = StateStore(self.state_root / "state.sqlite3", self.default_model)
        self.catalog = ModelCatalog(
            api_key,
            Path(os.environ.get("OMNIROUTE_STORAGE", "/root/.omniroute/storage.sqlite")),
            os.environ.get("OMNIROUTE_BASE_URL", "http://127.0.0.1:20128"),
        )
        initial = self.catalog.refresh(force=True)
        if self.default_model not in initial:
            raise RuntimeError(f"Default model is unavailable: {self.default_model}")
        self.runner = CodexRunner(
            os.environ.get("CODEX_BIN", "/usr/local/bin/codex"),
            Path(os.environ.get("WORKSPACE", "/root/main")),
            api_key,
            {
                alias.strip(): effort.strip()
                for item in os.environ.get(
                    "MODEL_REASONING_EFFORTS", "claude-fable-5=high"
                ).split(",")
                if "=" in item
                for alias, effort in [item.split("=", 1)]
                if alias.strip() and effort.strip()
            },
        )
        self.pool = ThreadPoolExecutor(max_workers=int(os.environ.get("MAX_CONCURRENT_RUNS", "4")))
        self.queues: dict[int, deque[RunTask]] = {}
        self.foreground_active: set[int] = set()
        self.background_active: dict[int, set[str]] = {}
        self.task_lock = threading.Lock()
        self.stop_event = threading.Event()

    def authorized(self, message: dict[str, Any]) -> bool:
        sender = message.get("from") or {}
        return str(sender.get("id")) in self.allowed or str(sender.get("username") or "").lower().lstrip("@") in self.allowed

    def commands(self) -> None:
        self.api.call(
            "setMyCommands",
            {
                "commands": [
                    {"command": "models", "description": "Доступные модели OmniRoute"},
                    {"command": "model", "description": "Выбрать модель"},
                    {"command": "new", "description": "Новая сессия"},
                    {"command": "status", "description": "Текущая модель и сессия"},
                    {"command": "queue", "description": "Очередь последовательных задач"},
                    {"command": "background", "description": "Параллельная задача"},
                    {"command": "stop", "description": "Остановить выполнение"},
                    {"command": "tools", "description": "Доступные инструменты"},
                    {"command": "help", "description": "Помощь"},
                ]
            },
        )

    def help(self, chat_id: int, reply_to: int | None = None) -> None:
        self.api.send(
            chat_id,
            "Worker подключён к OmniRoute и выполняет задачи с terminal/file/web-инструментами.\n\n"
            "/models — провайдеры и поиск моделей\n"
            "/models fable — модели по фильтру\n"
            "/model claude-fable-5 — выбрать модель\n"
            "/new — новая независимая сессия\n"
            "/queue <задача> — добавить последовательную задачу\n"
            "/background <задача> — запустить параллельно\n"
            "/status /stop /tools\n\n"
            "Сообщение во время работы становится приоритетным уточнением в текущей сессии. "
            "Фоновые задачи независимы, но используют тот же workspace: не задавайте им конфликтующие правки одних файлов.",
            reply_to,
        )

    def models(self, chat_id: int, query: str, reply_to: int | None) -> None:
        models = self.catalog.refresh(force=True)
        if not query:
            grouped: dict[str, int] = {}
            for model in models.values():
                grouped[model.owner] = grouped.get(model.owner, 0) + 1
            lines = ["Авторизованные tool-capable модели OmniRoute:"]
            lines.extend(f"• {owner}: {count}" for owner, count in sorted(grouped.items()))
            lines.extend([
                "",
                f"Всего: {len(models)}. Для поиска: /models fable, /models codex, /models kimi.",
                "Выбор: /model <точный ID>",
            ])
            self.api.send(chat_id, "\n".join(lines), reply_to)
            return
        needle = query.lower()
        found = [model for model in models.values() if needle in model.id.lower()]
        found.sort(key=lambda item: (item.id.count("/"), len(item.id), item.id))
        if not found:
            self.api.send(chat_id, "Совпадений нет.", reply_to)
            return
        lines = [f"Найдено {len(found)}:"] + [f"• {item.id} ({context_label(item.context)})" for item in found]
        for index, part in enumerate(chunks("\n".join(lines))):
            self.api.send(chat_id, part, reply_to if index == 0 else None)

    def command(self, chat_id: int, message_id: int, text: str) -> None:
        first, _, arg = text.strip().partition(" ")
        name = first.split("@", 1)[0].lower()
        arg = arg.strip()
        if name in {"/start", "/help"}:
            self.help(chat_id, message_id)
        elif name == "/models":
            self.models(chat_id, arg, message_id)
        elif name == "/model":
            thread_id, current = self.state.get_chat(chat_id)
            if not arg:
                self.api.send(chat_id, f"Текущая модель: {current}\nВыбор: /model <ID>", message_id)
                return
            available = self.catalog.refresh(force=True)
            if arg not in available:
                self.api.send(chat_id, "Модель не найдена среди активных tool-capable маршрутов. Используйте /models <поиск>.", message_id)
                return
            self.state.save_chat(chat_id, None, arg)
            self.api.send(chat_id, f"Модель установлена: {arg}. Начата новая сессия.", message_id)
        elif name == "/new":
            _, model = self.state.get_chat(chat_id)
            self.state.save_chat(chat_id, None, model)
            self.api.send(chat_id, "Новая независимая сессия создана.", message_id)
        elif name == "/status":
            thread_id, model = self.state.get_chat(chat_id)
            with self.task_lock:
                running = chat_id in self.foreground_active
                queued = len(self.queues.get(chat_id, ()))
                backgrounds = len(self.background_active.get(chat_id, ()))
            self.api.send(chat_id, f"Модель: {model}\nСессия: {thread_id or 'новая'}\nWorkspace: {self.runner.workspace}\nTools: terminal, files, web, attachments\nВыполняется: {'да' if running else 'нет'}\nВ очереди: {queued}\nФоновых задач: {backgrounds}", message_id)
        elif name == "/tools":
            self.api.send(chat_id, "Активны встроенные инструменты Codex: terminal-команды, чтение/запись файлов, web-поиск и анализ Telegram-вложений. Доступ ограничен авторизованными Telegram-пользователями.", message_id)
        elif name == "/stop":
            stopped = self.runner.stop(f"fg:{chat_id}")
            self.api.send(chat_id, "Останавливаю текущую задачу." if stopped else "Активной задачи нет.", message_id)
        elif name == "/queue":
            if arg:
                self.enqueue_foreground(RunTask(chat_id, message_id, arg, []), priority=False)
                return
            with self.task_lock:
                queued_tasks = list(self.queues.get(chat_id, ()))
            if not queued_tasks:
                self.api.send(chat_id, "Очередь пуста.", message_id)
                return
            lines = [f"Очередь ({len(queued_tasks)}):"]
            lines.extend(f"{index}. {task.prompt[:180]}" for index, task in enumerate(queued_tasks, 1))
            self.api.send(chat_id, "\n".join(lines), message_id)
        elif name == "/background":
            if not arg:
                with self.task_lock:
                    active = sorted(self.background_active.get(chat_id, ()))
                text = "Активных фоновых задач нет." if not active else "Фоновые задачи: " + ", ".join(active)
                self.api.send(chat_id, text, message_id)
                return
            self.start_background(RunTask(chat_id, message_id, arg, [], task_id=uuid.uuid4().hex[:8], background=True))
        else:
            self.api.send(chat_id, "Неизвестная команда. Используйте /help.", message_id)

    def attachment(self, message: dict[str, Any], chat_id: int) -> list[Path]:
        file_id = ""
        filename = "attachment"
        if isinstance(message.get("document"), dict):
            document = message["document"]
            file_id = str(document.get("file_id") or "")
            filename = safe_name(str(document.get("file_name") or filename))
        elif isinstance(message.get("photo"), list) and message["photo"]:
            photo = message["photo"][-1]
            file_id = str(photo.get("file_id") or "")
            filename = f"photo-{message.get('message_id', 'unknown')}.jpg"
        if not file_id:
            return []
        target = self.state_root / "attachments" / str(chat_id) / f"{uuid.uuid4().hex}-{filename}"
        self.api.download(file_id, target)
        return [target]

    def _progress_event(self, task: RunTask, event: dict[str, Any], status_id: int | None) -> None:
        item = event.get("item")
        if not isinstance(item, dict):
            return
        event_type = str(event.get("type") or "")
        item_type = str(item.get("type") or "")
        label = f"[BG {task.task_id}] " if task.background else ""
        if event_type == "item.completed" and item_type == "reasoning":
            summary = str(item.get("text") or "").strip()
            if summary:
                # This is Codex's user-facing summary, never raw hidden chain-of-thought.
                self.api.send(task.chat_id, f"{label}🧭 {safe_progress_text(summary[:1200])}", silent=True)
        elif item_type == "command_execution" and event_type in {"item.started", "item.completed"}:
            status = "выполняет terminal-команду"
            if event_type == "item.completed":
                code = item.get("exit_code")
                status = f"terminal завершён{f' · exit {code}' if code is not None else ''}"
            if status_id:
                self.api.edit(task.chat_id, status_id, f"{label}⏳ {status}")
        elif event_type == "item.completed" and item_type in {"web_search", "mcp_tool_call"}:
            self.api.send(task.chat_id, f"{label}🔧 Завершён этап: {item_type}", silent=True)

    def execute(self, task: RunTask) -> None:
        chat_id, message_id = task.chat_id, task.message_id
        label = f"[BG {task.task_id}] " if task.background else ""
        status_id: int | None = None
        typing_stop = threading.Event()

        started_at = time.monotonic()
        last_summary_at = 0.0

        def publish_event(event: dict[str, Any]) -> None:
            nonlocal last_summary_at
            item = event.get("item")
            is_summary = (
                event.get("type") == "item.completed"
                and isinstance(item, dict)
                and item.get("type") == "reasoning"
            )
            now = time.monotonic()
            if is_summary and now - last_summary_at < 30:
                return
            if is_summary:
                last_summary_at = now
            self._progress_event(task, event, status_id)

        def typing_loop() -> None:
            next_progress_at = 300.0
            while not typing_stop.wait(4):
                try:
                    self.api.typing(chat_id)
                    elapsed = time.monotonic() - started_at
                    if status_id and elapsed >= next_progress_at:
                        minutes = max(1, int(elapsed // 60))
                        self.api.edit(
                            chat_id,
                            status_id,
                            f"{label}Выполняется · {model_id} · {minutes} мин. Для остановки: /stop",
                        )
                        next_progress_at += 300.0
                except Exception:
                    LOG.debug("typing failed", exc_info=True)

        try:
            saved_thread, model_id = self.state.get_chat(chat_id)
            thread_id = None if task.background else saved_thread
            models = self.catalog.refresh()
            model = models.get(model_id)
            if not model:
                if not task.background:
                    self.state.save_chat(chat_id, None, self.default_model)
                thread_id, model_id, model = None, self.default_model, models[self.default_model]
            status_id = self.api.send(chat_id, f"{label}Запускаю {model_id} с инструментами…", message_id, silent=True)
            threading.Thread(target=typing_loop, daemon=True).start()
            run_key = f"bg:{chat_id}:{task.task_id}" if task.background else f"fg:{chat_id}"
            result, new_thread = self.runner.run(
                run_key,
                model,
                task.prompt,
                thread_id,
                task.attachments,
                on_thread=None if task.background else lambda value: self.state.save_chat(chat_id, value, model_id),
                on_event=publish_event,
            )
            if not task.background:
                self.state.save_chat(chat_id, new_thread, model_id)
            if status_id:
                self.api.edit(chat_id, status_id, f"{label}✅ Готово · {model_id}")
            for part in chunks(result):
                self.api.send(chat_id, f"{label}{part}" if task.background else part)
        except Exception as exc:
            LOG.exception("Worker run failed")
            text = f"{label}Ошибка выполнения: {str(exc)[:1200]}"
            if status_id:
                try:
                    self.api.edit(chat_id, status_id, f"{label}❌ Выполнение завершилось ошибкой")
                except Exception:
                    pass
            self.api.send(chat_id, text, message_id if not status_id else None)
        finally:
            typing_stop.set()
            if task.background:
                with self.task_lock:
                    self.background_active.get(chat_id, set()).discard(task.task_id)

    def _run_foreground(self, first: RunTask) -> None:
        task: RunTask | None = first
        while task:
            self.execute(task)
            with self.task_lock:
                queue = self.queues.setdefault(task.chat_id, deque())
                task = queue.popleft() if queue else None
                if task is None:
                    self.foreground_active.discard(first.chat_id)

    def enqueue_foreground(self, task: RunTask, *, priority: bool) -> None:
        with self.task_lock:
            if task.chat_id not in self.foreground_active:
                self.foreground_active.add(task.chat_id)
                start_now = True
                position = 0
            else:
                queue = self.queues.setdefault(task.chat_id, deque())
                if priority:
                    task.clarification = True
                    position = next(
                        (index for index, queued in enumerate(queue) if not queued.clarification),
                        len(queue),
                    )
                    queue.insert(position, task)
                    position += 1
                else:
                    queue.append(task)
                    position = len(queue)
                start_now = False
        if start_now:
            self.pool.submit(self._run_foreground, task)
        else:
            wording = "Уточнение принято: передам модели следующим сообщением в текущей сессии." if priority else f"Добавлено в очередь: позиция {position}."
            self.api.send(task.chat_id, wording, task.message_id, silent=True)

    def start_background(self, task: RunTask) -> None:
        with self.task_lock:
            active = self.background_active.setdefault(task.chat_id, set())
            if len(active) >= 2:
                self.api.send(task.chat_id, "Уже выполняются две фоновые задачи. Дождитесь завершения одной.", task.message_id)
                return
            active.add(task.task_id)
        self.pool.submit(self.execute, task)

    def message(self, message: dict[str, Any]) -> None:
        chat_id = int((message.get("chat") or {}).get("id"))
        message_id = int(message.get("message_id") or 0)
        if not self.authorized(message):
            self.api.send(chat_id, "Доступ запрещён.", message_id)
            return
        text = str(message.get("text") or message.get("caption") or "").strip()
        if text.startswith("/"):
            self.command(chat_id, message_id, text)
            return
        attachments = self.attachment(message, chat_id)
        if not text and not attachments:
            self.api.send(chat_id, "Пришлите задачу текстом или файлом.", message_id)
            return
        if not text:
            text = "Проанализируй прикреплённый файл и выполни уместную задачу по его содержимому."
        with self.task_lock:
            priority = chat_id in self.foreground_active
        self.enqueue_foreground(RunTask(chat_id, message_id, text, attachments), priority=priority)

    def run(self) -> None:
        self.commands()
        LOG.info("Connected as @%s; default=%s", self.username, self.default_model)
        offset = self.state.get_offset()
        while not self.stop_event.is_set():
            try:
                for update in self.api.get_updates(offset):
                    offset = int(update["update_id"]) + 1
                    self.state.save_offset(offset)
                    message = update.get("message")
                    if isinstance(message, dict):
                        self.message(message)
            except Exception:
                LOG.exception("Polling failed")
                self.stop_event.wait(3)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    gateway = Gateway()
    signal.signal(signal.SIGTERM, lambda *_: gateway.stop_event.set())
    signal.signal(signal.SIGINT, lambda *_: gateway.stop_event.set())
    gateway.run()


if __name__ == "__main__":
    main()
