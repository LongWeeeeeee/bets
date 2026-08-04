"""Deterministic Codex-planner/Cursor-worker/Codex-reviewer commander.

The commander intentionally contains no model-driven routing decisions.  Planner and
reviewer are Codex ``exec`` calls using strict JSON schemas; workers are direct Cursor
CLI child processes in isolated git worktrees.  State is atomically persisted so a
process restart can resume between phases.

This module is independent from the live Dota/Telegram runtime.  It may be used as a
library or as a small CLI: ``run``, ``resume``, ``status`` and ``cancel``.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


SCHEMA_VERSION = 1
TERMINAL_STATES = {"APPROVED", "FAILED", "CANCELLED"}
_SECRET_KEY = re.compile(r"(?i)(token|secret|password|passwd|api[_-]?key|authorization|cookie|proxy)")
_BEARER = re.compile(r"(?i)\b(bearer\s+)[A-Za-z0-9._~+/=-]+")
_URL_CREDENTIALS = re.compile(r"(https?://)([^\s/@:]+):([^\s/@]+)@")


def _object(properties: Dict[str, Any], required: Sequence[str]) -> Dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": list(required),
        "additionalProperties": False,
    }


STRING_ARRAY = {"type": "array", "items": {"type": "string"}}
PLANNER_SCHEMA: Dict[str, Any] = _object(
    {
        "schema": {"const": 1},
        "status": {"const": "PLAN"},
        "scope": {"enum": ["initial", "replan"]},
        "base_sha": {"type": "string", "minLength": 7},
        "summary": {"type": "string"},
        "open_issues": STRING_ARRAY,
        "acceptance_criteria": STRING_ARRAY,
        "subtasks": {
            "type": "array",
            "minItems": 1,
            "items": _object(
                {
                    "id": {"type": "string", "pattern": "^[a-zA-Z0-9_-]{1,64}$"},
                    "title": {"type": "string"},
                    "depends_on": STRING_ARRAY,
                    "allowed_paths": STRING_ARRAY,
                    "instructions": {"type": "string"},
                    "tests": STRING_ARRAY,
                    "acceptance_criteria": STRING_ARRAY,
                },
                ["id", "title", "depends_on", "allowed_paths", "instructions", "tests", "acceptance_criteria"],
            ),
        },
    },
    ["schema", "status", "scope", "base_sha", "summary", "open_issues", "acceptance_criteria", "subtasks"],
)

WORKER_SCHEMA: Dict[str, Any] = _object(
    {
        "schema": {"const": 1},
        "status": {"enum": ["SUCCESS", "FAILED"]},
        "subtask_id": {"type": "string"},
        "session_id": {"type": "string"},
        "base_sha": {"type": "string"},
        "head_sha": {"type": "string"},
        "summary": {"type": "string"},
        "files_changed": STRING_ARRAY,
        "tests": STRING_ARRAY,
        "error": {"type": "string"},
    },
    ["schema", "status", "subtask_id", "session_id", "base_sha", "head_sha", "summary", "files_changed", "tests", "error"],
)

REVIEWER_SCHEMA: Dict[str, Any] = _object(
    {
        "schema": {"const": 1},
        "verdict": {"enum": ["APPROVE", "ISSUES"]},
        "base_sha": {"type": "string"},
        "head_sha": {"type": "string"},
        "findings": {
            "type": "array",
            "items": _object(
                {
                    "sig": {"type": "string"},
                    "severity": {"enum": ["Critical", "Minor"]},
                    "category": {"type": "string"},
                    "file": {"type": "string"},
                    "line": {"type": "integer", "minimum": 0},
                    "message": {"type": "string"},
                    "required_change": {"type": "string"},
                    "affected_paths": STRING_ARRAY,
                },
                ["sig", "severity", "category", "file", "line", "message", "required_change", "affected_paths"],
            ),
        },
        "test_gaps": STRING_ARRAY,
    },
    ["schema", "verdict", "base_sha", "head_sha", "findings", "test_gaps"],
)


class OrchestratorError(RuntimeError):
    pass


def redact(value: Any) -> Any:
    """Return a log-safe copy without mutating the original value."""
    if isinstance(value, Mapping):
        return {str(k): ("<redacted>" if _SECRET_KEY.search(str(k)) else redact(v)) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v) for v in value]
    if isinstance(value, tuple):
        return tuple(redact(v) for v in value)
    if isinstance(value, str):
        value = _BEARER.sub(r"\1<redacted>", value)
        return _URL_CREDENTIALS.sub(r"\1<redacted>:<redacted>@", value)
    return value


def _atomic_json(path: Path, value: Mapping[str, Any], mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp-{os.getpid()}")
    data = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


@dataclasses.dataclass(frozen=True)
class Config:
    repo_root: Path
    state_dir: Path
    worktree_dir: Path
    codex_binary: str = "codex"
    cursor_binary: str = "cursor-agent"
    planner_model: str = "gpt-5.6"
    reviewer_model: str = "gpt-5.6"
    reasoning_effort: str = "high"
    cursor_model: str = "grok-4.5-fast-high"
    max_parallel_workers: int = 3
    max_fix_iterations: int = 3
    max_prompt_chars: int = 120_000
    command_timeout_seconds: int = 3600
    cursor_proxy_env_file: Optional[Path] = None
    codex_env: Mapping[str, str] = dataclasses.field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> "Config":
        raw = json.loads(path.read_text(encoding="utf-8"))
        base = path.resolve().parent

        def p(name: str, default: Optional[str] = None) -> Optional[Path]:
            value = raw.get(name, default)
            if value is None:
                return None
            out = Path(value).expanduser()
            return out if out.is_absolute() else (base / out).resolve()

        cfg = cls(
            repo_root=p("repo_root", ".") or base,
            state_dir=p("state_dir", "../runtime/agent-orchestrator") or base,
            worktree_dir=p("worktree_dir", "../runtime/agent-worktrees") or base,
            codex_binary=str(raw.get("codex_binary", "codex")),
            cursor_binary=str(raw.get("cursor_binary", "cursor-agent")),
            planner_model=str(raw.get("planner_model", "gpt-5.6")),
            reviewer_model=str(raw.get("reviewer_model", "gpt-5.6")),
            reasoning_effort=str(raw.get("reasoning_effort", "high")),
            cursor_model=str(raw.get("cursor_model", "grok-4.5-fast-high")),
            max_parallel_workers=int(raw.get("max_parallel_workers", 3)),
            max_fix_iterations=int(raw.get("max_fix_iterations", 3)),
            max_prompt_chars=int(raw.get("max_prompt_chars", 120_000)),
            command_timeout_seconds=int(raw.get("command_timeout_seconds", 3600)),
            cursor_proxy_env_file=p("cursor_proxy_env_file"),
            codex_env={str(k): str(v) for k, v in raw.get("codex_env", {}).items()},
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if self.planner_model != self.reviewer_model or self.planner_model != "gpt-5.6":
            raise OrchestratorError("planner and reviewer must use exact model gpt-5.6")
        if self.reasoning_effort != "high":
            raise OrchestratorError("planner/reviewer reasoning_effort must be high")
        if not 1 <= self.max_parallel_workers <= 3:
            raise OrchestratorError("max_parallel_workers must be in [1, 3]")
        if self.max_fix_iterations < 0:
            raise OrchestratorError("max_fix_iterations must be non-negative")
        if self.max_prompt_chars < 1000:
            raise OrchestratorError("max_prompt_chars is unreasonably small")


class ProcessRunner:
    """Run argv directly (never through a shell), tracking process groups for cancel."""

    def __init__(self, timeout: int):
        self.timeout = timeout
        self._lock = threading.Lock()
        self._active: set[int] = set()

    def run(self, argv: Sequence[str], *, cwd: Path, env: Mapping[str, str], stdin: str = "") -> subprocess.CompletedProcess[str]:
        proc = subprocess.Popen(
            list(argv), cwd=cwd, env=dict(env), stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            start_new_session=True,
        )
        with self._lock:
            self._active.add(proc.pid)
        try:
            stdout, stderr = proc.communicate(stdin, timeout=self.timeout)
        except subprocess.TimeoutExpired as exc:
            self._kill_group(proc.pid)
            stdout, stderr = proc.communicate()
            raise OrchestratorError(f"command timed out: {argv[0]}: {redact(stderr[-1000:])}") from exc
        finally:
            with self._lock:
                self._active.discard(proc.pid)
        if proc.returncode:
            raise OrchestratorError(f"command failed ({proc.returncode}): {argv[0]}: {redact(stderr[-2000:])}")
        return subprocess.CompletedProcess(argv, proc.returncode, stdout, stderr)

    @staticmethod
    def _kill_group(pid: int) -> None:
        try:
            os.killpg(pid, signal.SIGTERM)
            time.sleep(0.05)
            os.killpg(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def cancel(self) -> None:
        with self._lock:
            pids = list(self._active)
        for pid in pids:
            self._kill_group(pid)


def _validate_json(data: Any, schema: Mapping[str, Any], where: str = "$") -> None:
    """Small dependency-free validator for the strict schemas above."""
    if "const" in schema and data != schema["const"]:
        raise OrchestratorError(f"{where}: expected {schema['const']!r}")
    if "enum" in schema and data not in schema["enum"]:
        raise OrchestratorError(f"{where}: invalid value {data!r}")
    typ = schema.get("type")
    checks = {"object": dict, "array": list, "string": str, "integer": int}
    if typ in checks and (not isinstance(data, checks[typ]) or (typ == "integer" and isinstance(data, bool))):
        raise OrchestratorError(f"{where}: expected {typ}")
    if typ == "string":
        if len(data) < schema.get("minLength", 0) or ("pattern" in schema and not re.fullmatch(schema["pattern"], data)):
            raise OrchestratorError(f"{where}: invalid string")
    elif typ == "integer" and data < schema.get("minimum", data):
        raise OrchestratorError(f"{where}: integer below minimum")
    elif typ == "array":
        if len(data) < schema.get("minItems", 0):
            raise OrchestratorError(f"{where}: too few items")
        for index, item in enumerate(data):
            _validate_json(item, schema["items"], f"{where}[{index}]")
    elif typ == "object":
        required = set(schema.get("required", []))
        missing = required - data.keys()
        if missing:
            raise OrchestratorError(f"{where}: missing {sorted(missing)}")
        props = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            extra = data.keys() - props.keys()
            if extra:
                raise OrchestratorError(f"{where}: unexpected {sorted(extra)}")
        for key, item in data.items():
            if key in props:
                _validate_json(item, props[key], f"{where}.{key}")


def _safe_paths(paths: Iterable[str]) -> List[str]:
    out: List[str] = []
    for raw in paths:
        if not raw or "\x00" in raw or "\\" in raw:
            raise OrchestratorError(f"unsafe allowed path: {raw!r}")
        p = PurePosixPath(raw)
        if p.is_absolute() or ".." in p.parts or p.parts[0] == ".git":
            raise OrchestratorError(f"unsafe allowed path: {raw!r}")
        normalized = p.as_posix().rstrip("/")
        if normalized in ("", "."):
            raise OrchestratorError("repository-wide allowed path is forbidden")
        out.append(normalized)
    if not out:
        raise OrchestratorError("subtask allowed_paths must not be empty")
    return sorted(set(out))


def validate_plan(plan: Mapping[str, Any], expected_base: str) -> List[List[Dict[str, Any]]]:
    _validate_json(plan, PLANNER_SCHEMA)
    if plan["base_sha"] != expected_base:
        raise OrchestratorError("planner base_sha does not match integration HEAD")
    tasks = {item["id"]: dict(item) for item in plan["subtasks"]}
    if len(tasks) != len(plan["subtasks"]):
        raise OrchestratorError("duplicate subtask id")
    for task in tasks.values():
        task["allowed_paths"] = _safe_paths(task["allowed_paths"])
        unknown = set(task["depends_on"]) - tasks.keys()
        if unknown or task["id"] in task["depends_on"]:
            raise OrchestratorError(f"invalid dependencies for {task['id']}: {sorted(unknown)}")
    # Kahn waves; tasks within one wave must not have overlapping scopes.
    pending = set(tasks)
    done: set[str] = set()
    waves: List[List[Dict[str, Any]]] = []
    while pending:
        ids = sorted(i for i in pending if set(tasks[i]["depends_on"]) <= done)
        if not ids:
            raise OrchestratorError("subtask dependency cycle")
        wave = [tasks[i] for i in ids]
        for i, left in enumerate(wave):
            for right in wave[i + 1:]:
                for a in left["allowed_paths"]:
                    for b in right["allowed_paths"]:
                        if a == b or a.startswith(b + "/") or b.startswith(a + "/"):
                            raise OrchestratorError(f"parallel path overlap: {left['id']} and {right['id']}")
        waves.append(wave)
        pending -= set(ids)
        done |= set(ids)
    return waves


class Commander:
    def __init__(self, config: Config, runner: Optional[ProcessRunner] = None):
        self.config = config
        self.runner = runner or ProcessRunner(config.command_timeout_seconds)
        self._cancel = threading.Event()

    def _git(self, args: Sequence[str], cwd: Optional[Path] = None) -> str:
        return self.runner.run(["git", *args], cwd=cwd or self.config.repo_root, env=os.environ).stdout.strip()

    def _state_path(self, task_id: str) -> Path:
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", task_id):
            raise OrchestratorError("invalid task id")
        return self.config.state_dir / task_id / "state.json"

    def _save(self, state: Dict[str, Any]) -> None:
        state["updated_at"] = time.time()
        _atomic_json(self._state_path(state["task_id"]), redact(state))

    def _load(self, task_id: str) -> Dict[str, Any]:
        path = self._state_path(task_id)
        if not path.exists():
            raise OrchestratorError(f"task not found: {task_id}")
        return json.loads(path.read_text(encoding="utf-8"))

    def _prompt(self, name: str, payload: Mapping[str, Any]) -> str:
        template = (Path(__file__).with_name("prompts") / name).read_text(encoding="utf-8")
        text = template + "\n\nINPUT_JSON\n" + json.dumps(payload, ensure_ascii=False, sort_keys=True)
        if len(text) > self.config.max_prompt_chars:
            raise OrchestratorError(f"prompt exceeds max_prompt_chars ({len(text)})")
        return text

    def _codex(self, role: str, prompt: str, schema: Mapping[str, Any], cwd: Path) -> Dict[str, Any]:
        task_dir = self.config.state_dir / ".calls"
        task_dir.mkdir(parents=True, exist_ok=True)
        nonce = uuid.uuid4().hex
        schema_path, output_path = task_dir / f"{nonce}.schema.json", task_dir / f"{nonce}.out.json"
        _atomic_json(schema_path, schema)
        env = os.environ.copy()
        env.update(self.config.codex_env)  # OmniRoute Responses endpoint/auth are injected here.
        argv = [
            self.config.codex_binary, "exec", "--model", self.config.planner_model,
            "--config", f'model_reasoning_effort="{self.config.reasoning_effort}"',
            "--sandbox", "read-only", "--output-schema", str(schema_path),
            "--output-last-message", str(output_path), "-",
        ]
        self.runner.run(argv, cwd=cwd, env=env, stdin=prompt)
        try:
            data = json.loads(output_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise OrchestratorError(f"{role} returned no valid JSON") from exc
        _validate_json(data, schema)
        return data

    def _cursor_env(self) -> Dict[str, str]:
        env = os.environ.copy()
        if self.config.cursor_proxy_env_file:
            for line in self.config.cursor_proxy_env_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                key, sep, value = line.partition("=")
                if not sep or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
                    raise OrchestratorError("invalid cursor proxy env file")
                env[key] = value
        return env

    @staticmethod
    def _path_allowed(path: str, allowed: Sequence[str]) -> bool:
        return any(path == root or path.startswith(root + "/") for root in allowed)

    def _worker(self, task_id: str, task: Mapping[str, Any], integration: Path, round_no: int) -> Dict[str, Any]:
        if self._cancel.is_set():
            raise OrchestratorError("cancelled")
        base = self._git(["rev-parse", "HEAD"], integration)
        root = self.config.worktree_dir / task_id
        worktree = root / f"r{round_no}-{task['id']}"
        branch = f"codex/swarm-{task_id}-r{round_no}-{task['id']}"
        worktree.parent.mkdir(parents=True, exist_ok=True)
        self._git(["worktree", "add", "-b", branch, str(worktree), base])
        payload = dict(task)
        payload.update({"schema": 1, "task_id": task_id, "base_sha": base, "worker_output_schema": WORKER_SCHEMA})
        prompt = self._prompt("swarm_worker.md", payload)
        result = self.runner.run(
            [self.config.cursor_binary, "-p", "--model", self.config.cursor_model,
             "--output-format", "json", "--trust", "--force", "--approve-mcps", prompt],
            cwd=worktree, env=self._cursor_env(),
        )
        try:
            message = json.loads(result.stdout.strip().splitlines()[-1])
            # Cursor JSON envelopes commonly put the assistant content in `result`.
            if isinstance(message, dict) and isinstance(message.get("result"), str):
                message = json.loads(message["result"])
        except (IndexError, json.JSONDecodeError, TypeError) as exc:
            raise OrchestratorError(f"worker {task['id']} returned invalid JSON") from exc
        _validate_json(message, WORKER_SCHEMA)
        if message["subtask_id"] != task["id"] or message["status"] != "SUCCESS":
            raise OrchestratorError(f"worker {task['id']} failed: {redact(message.get('error', ''))}")
        changed = self._git(["status", "--porcelain", "--untracked-files=all"], worktree)
        paths = [line[3:] for line in changed.splitlines() if len(line) >= 4]
        # Rename status uses "old -> new"; both sides must be scoped.
        expanded = [piece for p in paths for piece in p.split(" -> ")]
        bad = [p for p in expanded if not self._path_allowed(p, task["allowed_paths"])]
        if bad:
            raise OrchestratorError(f"worker {task['id']} changed paths outside scope: {bad}")
        if paths:
            self._git(["add", "--", *task["allowed_paths"]], worktree)
            self._git(["commit", "-m", f"swarm({task_id}): {task['id']}", "--no-verify"], worktree)
        head = self._git(["rev-parse", "HEAD"], worktree)
        return {**message, "base_sha": base, "head_sha": head, "files_changed": sorted(expanded)}

    def _prepare(self, state: Dict[str, Any]) -> Path:
        integration = Path(state["integration_worktree"])
        if integration.exists():
            return integration
        integration.parent.mkdir(parents=True, exist_ok=True)
        self._git(["worktree", "add", "-b", state["result_branch"], str(integration), state["base_sha"]])
        return integration

    def run(self, task: str, task_id: Optional[str] = None) -> Dict[str, Any]:
        task_id = task_id or time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8]
        if self._state_path(task_id).exists():
            raise OrchestratorError(f"task already exists: {task_id}")
        base = self._git(["rev-parse", "HEAD"])
        integration = self.config.worktree_dir / task_id / "integration"
        state: Dict[str, Any] = {
            "schema": 1, "task_id": task_id, "task": task, "status": "PLANNING",
            "base_sha": base, "result_branch": f"codex/swarm-{task_id}",
            "integration_worktree": str(integration), "round": 0, "plans": [],
            "worker_results": [], "reviews": [], "open_issue_history": [],
        }
        self._save(state)
        self._prepare(state)
        return self._drive(state)

    def resume(self, task_id: str) -> Dict[str, Any]:
        state = self._load(task_id)
        if state["status"] in TERMINAL_STATES:
            return state
        self._prepare(state)
        return self._drive(state)

    def _drive(self, state: Dict[str, Any]) -> Dict[str, Any]:
        integration = Path(state["integration_worktree"])
        try:
            while state["status"] not in TERMINAL_STATES:
                if self._cancel.is_set() or self._state_path(state["task_id"]).with_name("cancel").exists():
                    state["status"] = "CANCELLED"
                    break
                head = self._git(["rev-parse", "HEAD"], integration)
                if state["status"] in ("PLANNING", "REPLANNING"):
                    payload = {"task": state["task"], "scope": "initial" if not state["plans"] else "replan",
                               "base_sha": head, "open_issues": state["reviews"][-1]["findings"] if state["reviews"] else []}
                    plan = self._codex("planner", self._prompt("swarm_planner.md", payload), PLANNER_SCHEMA, integration)
                    expected_scope = payload["scope"]
                    if plan["scope"] != expected_scope:
                        raise OrchestratorError("planner returned wrong scope")
                    waves = validate_plan(plan, head)
                    state["plans"].append(plan)
                    state["pending_waves"] = waves
                    state["status"] = "WORKING"
                    self._save(state)
                if state["status"] == "WORKING":
                    for wave in state.pop("pending_waves", []):
                        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.max_parallel_workers) as pool:
                            futures = {task["id"]: pool.submit(self._worker, state["task_id"], task, integration, state["round"]) for task in wave}
                            results = [futures[key].result() for key in sorted(futures)]
                        # Deterministic merge order independent of completion order.
                        for result in results:
                            if result["head_sha"] != result["base_sha"]:
                                self._git(["cherry-pick", result["head_sha"]], integration)
                        state["worker_results"].extend(results)
                        self._save(state)
                    state["status"] = "REVIEWING"
                    self._save(state)
                if state["status"] == "REVIEWING":
                    head = self._git(["rev-parse", "HEAD"], integration)
                    diff = self._git(["diff", "--stat", state["base_sha"] + ".." + head], integration)
                    payload = {"task": state["task"], "base_sha": state["base_sha"], "head_sha": head,
                               "plans": state["plans"], "workers": state["worker_results"], "diff_stat": diff}
                    review = self._codex("reviewer", self._prompt("swarm_reviewer.md", payload), REVIEWER_SCHEMA, integration)
                    if review["base_sha"] != state["base_sha"] or review["head_sha"] != head:
                        raise OrchestratorError("reviewer SHA mismatch")
                    state["reviews"].append(review)
                    if review["verdict"] == "APPROVE":
                        state["status"] = "APPROVED"
                        state["head_sha"] = head
                        break
                    critical = sorted(f["sig"] for f in review["findings"] if f["severity"] == "Critical")
                    if not critical:
                        state["status"] = "APPROVED"
                        state["head_sha"] = head
                        break
                    history = state["open_issue_history"]
                    state["round"] += 1
                    if state["round"] > self.config.max_fix_iterations:
                        raise OrchestratorError("safeguard: fix iteration limit")
                    current = set(critical)
                    if history and current & set(history[-1]):
                        raise OrchestratorError("safeguard: stuck finding")
                    if any(current == set(old) for old in history):
                        raise OrchestratorError("safeguard: finding cycle")
                    history.append(critical)
                    state["status"] = "REPLANNING"
                    self._save(state)
        except Exception as exc:
            state["status"] = "CANCELLED" if self._cancel.is_set() else "FAILED"
            state["error"] = str(redact(str(exc)))
        self._save(state)
        return state

    def status(self, task_id: str) -> Dict[str, Any]:
        return self._load(task_id)

    def cancel(self, task_id: str) -> Dict[str, Any]:
        state = self._load(task_id)
        self._cancel.set()
        cancel_path = self._state_path(task_id).with_name("cancel")
        cancel_path.touch(mode=0o600, exist_ok=True)
        self.runner.cancel()
        if state["status"] not in TERMINAL_STATES:
            state["status"] = "CANCELLED"
            self._save(state)
        return state


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("task")
    run.add_argument("--task-id")
    for name in ("resume", "status", "cancel"):
        item = sub.add_parser(name)
        item.add_argument("task_id")
    args = parser.parse_args(argv)
    commander = Commander(Config.load(args.config))
    try:
        if args.command == "run":
            result = commander.run(args.task, args.task_id)
        else:
            result = getattr(commander, args.command)(args.task_id)
        print(json.dumps(redact(result), ensure_ascii=False, sort_keys=True))
        return 0 if result["status"] not in {"FAILED"} else 1
    except Exception as exc:
        print(json.dumps({"status": "FAILED", "error": redact(str(exc))}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
