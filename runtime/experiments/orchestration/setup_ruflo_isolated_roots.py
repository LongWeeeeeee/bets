#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import time
from pathlib import Path


BASE = Path("/root/main/runtime/ruflo-orchestrators")
SLOTS = {
    "orchestration1": 3301,
    "orchestration2": 3302,
    "orchestration3": 3303,
}
REQUIRED_AGENTDB_TABLES = {"metadata", "memory_entries"}
REQUIRED_AGENTDB_TABLES.update(
    {"causal_edges", "episodes", "reasoning_patterns", "skills"}
)


def atomic_write(path: Path, text: str, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(text)
    os.chmod(temporary, mode)
    os.replace(temporary, path)


def validate_agentdb(path: Path) -> None:
    if not path.is_file():
        raise RuntimeError(f"AgentDB is missing: {path}")
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    finally:
        connection.close()
    missing = REQUIRED_AGENTDB_TABLES - tables
    if missing:
        raise RuntimeError(
            f"AgentDB {path} is missing tables: {', '.join(sorted(missing))}"
        )


def materialize_agentdb(source: Path, destination: Path) -> None:
    source_connection = sqlite3.connect(source)
    destination_connection = sqlite3.connect(destination)
    try:
        source_connection.backup(destination_connection)
        destination_connection.execute("PRAGMA journal_mode = DELETE")
        destination_connection.commit()
    finally:
        destination_connection.close()
        source_connection.close()
    os.chmod(destination, 0o600)
    validate_agentdb(destination)


def snapshot_sqlite(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    source_connection = sqlite3.connect(source)
    destination_connection = sqlite3.connect(destination)
    try:
        source_connection.backup(destination_connection)
        destination_connection.execute("PRAGMA journal_mode = DELETE")
        destination_connection.commit()
    finally:
        destination_connection.close()
        source_connection.close()
    os.chmod(destination, 0o600)


def build_agentdb(root: Path) -> Path:
    bootstrap = root / ".swarm" / "backups" / (
        f"bootstrap-{time.strftime('%Y%m%dT%H%M%S')}-{os.getpid()}-{time.time_ns()}"
    )
    bootstrap.mkdir(parents=True, exist_ok=False, mode=0o700)
    initialized = bootstrap / "memory.db"
    ruflo_bin = os.environ.get("RUFLO_BIN") or shutil.which("ruflo")
    if not ruflo_bin:
        raise RuntimeError("ruflo binary is required to initialize isolated AgentDB")
    subprocess.run(
        [
            ruflo_bin,
            "memory",
            "init",
            "--backend",
            "agentdb",
            "--path",
            str(initialized),
            "--verify",
        ],
        cwd=bootstrap,
        check=True,
    )
    validate_agentdb(initialized)
    return initialized


def ensure_agentdb(root: Path, repair_invalid: bool = False) -> Path:
    path = root / ".swarm" / "memory.db"
    if path.exists():
        try:
            validate_agentdb(path)
        except RuntimeError:
            if not repair_invalid:
                raise
            backup = root / ".swarm" / "backups" / (
                f"invalid-memory-{time.strftime('%Y%m%dT%H%M%S')}-"
                f"{os.getpid()}-{time.time_ns()}.db"
            )
            snapshot_sqlite(path, backup)
        else:
            os.chmod(path, 0o600)
            return path

    initialized = build_agentdb(root)
    ready = path.with_name(f"{path.name}.tmp-ready-{os.getpid()}-{time.time_ns()}")
    materialize_agentdb(initialized, ready)
    os.replace(ready, path)
    return path


def config(slot: str, port: int) -> dict[str, object]:
    root = BASE / slot
    return {
        "version": "3.5",
        "agents": {
            "defaultType": "coordinator",
            "autoSpawn": False,
            "maxConcurrent": 12,
            "timeout": 900000,
            "providers": ["openrouter"],
        },
        "swarm": {
            "topology": "hierarchical-mesh",
            "maxAgents": 12,
            "autoScale": True,
            "coordinationStrategy": "consensus",
            "healthCheckInterval": 30000,
            "strategy": "specialized",
            "communicationProtocol": "message-bus",
        },
        "memory": {
            "backend": "agentdb",
            "persistPath": str(root / ".swarm"),
            "cacheSize": 2000,
            "enableHNSW": True,
            "vectorDimension": 384,
            "learningBridge": {
                "enabled": True,
                "sonaMode": "balanced",
                "confidenceDecayRate": 0.005,
                "accessBoostAmount": 0.03,
                "consolidationThreshold": 10,
            },
            "memoryGraph": {
                "enabled": True,
                "pageRankDamping": 0.85,
                "maxNodes": 10000,
                "similarityThreshold": 0.8,
            },
            "agentScopes": {"enabled": True, "defaultScope": "project"},
        },
        "mcp": {
            "serverHost": "127.0.0.1",
            "serverPort": port,
            "autoStart": False,
            "transportType": "stdio",
            "tools": [],
        },
        "cli": {
            "colorOutput": False,
            "interactive": False,
            "verbosity": "normal",
            "outputFormat": "json",
            "progressStyle": "none",
        },
        "hooks": {"enabled": True, "autoExecute": True, "hooks": []},
        "neural": {
            "enabled": True,
            "modelPath": str(root / ".claude-flow" / "neural"),
        },
        "autopilot": {
            "maxIterations": 3,
            "timeoutMinutes": 30,
            "taskSources": ["swarm-tasks"],
            "requireTerminalOutcome": True,
        },
        "resilience": {
            "maxAttempts": 3,
            "sameSignatureNoDeltaRetries": 0,
            "protocolViolationMaxAttempts": 2,
            "preservePartialArtifacts": True,
            "replanOpenOutcomesOnly": True,
            "consolidateOnTerminal": True,
            "circuitBreaker": {"enabled": True},
        },
        "projectRoot": str(root),
    }


def yaml_config(slot: str, port: int) -> str:
    root = BASE / slot
    return f"""version: '3.5'
agents:
  defaultType: coordinator
  autoSpawn: false
  maxConcurrent: 12
  timeout: 900000
  providers:
  - openrouter
swarm:
  topology: hierarchical-mesh
  maxAgents: 12
  autoScale: true
  coordinationStrategy: consensus
  healthCheckInterval: 30000
  strategy: specialized
  communicationProtocol: message-bus
memory:
  backend: agentdb
  persistPath: {root}/.swarm
  cacheSize: 2000
  enableHNSW: true
  vectorDimension: 384
  learningBridge:
    enabled: true
    sonaMode: balanced
    confidenceDecayRate: 0.005
    accessBoostAmount: 0.03
    consolidationThreshold: 10
  memoryGraph:
    enabled: true
    pageRankDamping: 0.85
    maxNodes: 10000
    similarityThreshold: 0.8
  agentScopes:
    enabled: true
    defaultScope: project
mcp:
  serverHost: 127.0.0.1
  serverPort: {port}
  autoStart: false
  transportType: stdio
  tools: []
cli:
  colorOutput: false
  interactive: false
  verbosity: normal
  outputFormat: json
  progressStyle: none
hooks:
  enabled: true
  autoExecute: true
  hooks: []
neural:
  enabled: true
  modelPath: {root}/.claude-flow/neural
autopilot:
  maxIterations: 3
  timeoutMinutes: 30
  taskSources:
  - swarm-tasks
  requireTerminalOutcome: true
resilience:
  maxAttempts: 3
  sameSignatureNoDeltaRetries: 0
  protocolViolationMaxAttempts: 2
  preservePartialArtifacts: true
  replanOpenOutcomesOnly: true
  consolidateOnTerminal: true
  circuitBreaker:
    enabled: true
projectRoot: {root}
"""


def setup_roots(repair_invalid: bool = False) -> None:
    for slot, port in SLOTS.items():
        root = BASE / slot
        for directory in (
            root / ".claude-flow" / "agents",
            root / ".claude-flow" / "logs",
            root / ".claude-flow" / "metrics",
            root / ".claude-flow" / "neural",
            root / ".swarm" / "backups",
            root / "staging",
            root / "final",
        ):
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        atomic_write(
            root / "claude-flow.config.json",
            json.dumps(config(slot, port), ensure_ascii=False, indent=2) + "\n",
        )
        atomic_write(root / ".claude-flow" / "config.yaml", yaml_config(slot, port))
        atomic_write(root / "LANE", slot + "\n")
        board = root / ".claude-flow" / "swarm-tasks.json"
        if not board.exists():
            atomic_write(board, "[]\n")
        else:
            os.chmod(board, 0o600)
        task_store = root / ".claude-flow" / "tasks" / "store.json"
        if not task_store.exists():
            atomic_write(
                task_store,
                json.dumps({"tasks": {}, "version": "3.0.0"}, indent=2) + "\n",
            )
        else:
            os.chmod(task_store, 0o600)
        ensure_agentdb(root, repair_invalid=repair_invalid)


def verify_roots() -> list[dict[str, object]]:
    report = []
    memory_inodes = set()
    task_store_inodes = set()
    root_paths = set()
    for slot, port in SLOTS.items():
        root = BASE / slot
        root_paths.add(root.resolve())
        json_path = root / "claude-flow.config.json"
        yaml_path = root / ".claude-flow" / "config.yaml"
        lane_path = root / "LANE"
        board_path = root / ".claude-flow" / "swarm-tasks.json"
        task_store_path = root / ".claude-flow" / "tasks" / "store.json"
        memory_path = root / ".swarm" / "memory.db"
        if json.loads(json_path.read_text()) != config(slot, port):
            raise RuntimeError(f"JSON config drift detected: {json_path}")
        if yaml_path.read_text() != yaml_config(slot, port):
            raise RuntimeError(f"YAML config drift detected: {yaml_path}")
        if lane_path.read_text() != slot + "\n":
            raise RuntimeError(f"Lane marker mismatch: {lane_path}")
        if not board_path.is_file():
            raise RuntimeError(f"Board is missing: {board_path}")
        if board_path.stat().st_mode & 0o777 != 0o600:
            raise RuntimeError(f"Board mode must be 0600: {board_path}")
        if not task_store_path.is_file():
            raise RuntimeError(f"Task store is missing: {task_store_path}")
        if task_store_path.stat().st_mode & 0o777 != 0o600:
            raise RuntimeError(f"Task store mode must be 0600: {task_store_path}")
        validate_agentdb(memory_path)
        if memory_path.stat().st_mode & 0o777 != 0o600:
            raise RuntimeError(f"AgentDB mode must be 0600: {memory_path}")
        memory_inodes.add(memory_path.stat().st_ino)
        task_store_inodes.add(task_store_path.stat().st_ino)
        report.append(
            {
                "slot": slot,
                "root": str(root),
                "mcpPort": port,
                "board": str(board_path),
                "taskStore": str(task_store_path),
                "agentdb": str(memory_path),
            }
        )
    if len(root_paths) != len(SLOTS):
        raise RuntimeError("RuFlo roots are not unique")
    if len(memory_inodes) != len(SLOTS):
        raise RuntimeError("AgentDB files are not physically isolated")
    if len(task_store_inodes) != len(SLOTS):
        raise RuntimeError("Task stores are not physically isolated")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check-only",
        "--check",
        action="store_true",
        help="validate isolated roots without changing them",
    )
    parser.add_argument(
        "--repair-invalid",
        action="store_true",
        help="preserve an invalid AgentDB backup, then atomically replace it",
    )
    args = parser.parse_args()
    if not args.check_only:
        setup_roots(repair_invalid=args.repair_invalid)
    print(json.dumps({"roots": verify_roots()}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
