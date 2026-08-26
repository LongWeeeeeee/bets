from __future__ import annotations

import importlib.util
import json
import sqlite3
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ruflo_migrate_anti_stall.py"

spec = importlib.util.spec_from_file_location("ruflo_migrate_anti_stall", SCRIPT)
assert spec and spec.loader
migration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration)


def test_migration_manifest_matches_source_and_agentdb() -> None:
    manifest = json.loads((ROOT / ".claude-flow/migration/anti-stall-v1.json").read_text())
    payloads, evidence = migration.load_sources()
    expected = migration.build_entries(payloads, evidence)

    assert manifest["schema"] == migration.SCHEMA
    assert manifest["namespace"] == migration.NAMESPACE
    assert manifest["source_evidence"] == evidence
    assert manifest["verified_count"] == len(expected) == 6

    verified = migration.verify_db(ROOT / ".swarm/memory.db", expected)
    assert [row["key"] for row in verified] == [entry["key"] for entry in expected]


def test_transient_execution_state_is_not_migrated() -> None:
    manifest = json.loads((ROOT / ".claude-flow/migration/anti-stall-v1.json").read_text())
    excluded = set(manifest["excluded_transient_state"])
    assert {"tick_count", "cooldowns", "pids", "in_flight_tasks", "audit_log"} <= excluded

    con = sqlite3.connect(f"file:{ROOT / '.swarm/memory.db'}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT key FROM memory_entries WHERE namespace=? ORDER BY key",
            (migration.NAMESPACE,),
        ).fetchall()
    finally:
        con.close()
    keys = {row[0] for row in rows}
    assert keys == {
        "anti-stall-policy-v1",
        "anti-stall-snapshot-contract-v1",
        "anti-stall-decision-contract-v1",
        "anti-stall-executor-contract-v1",
        "anti-stall-report-contract-v1",
        "anti-stall-ruflo-handoff-v1",
    }


def test_ruflo_policy_is_bounded_and_preserves_orchestration_freeze() -> None:
    yaml_text = (ROOT / ".claude-flow/config.yaml").read_text()
    json_config = json.loads((ROOT / "claude-flow.config.json").read_text())
    agents = (ROOT / "AGENTS.md").read_text()

    assert "maxIterations: 3" in yaml_text
    assert "timeoutMinutes: 30" in yaml_text
    assert "maxAttempts: 3" in yaml_text
    assert json_config["autopilot"]["maxIterations"] == 3
    assert json_config["autopilot"]["timeoutMinutes"] == 30
    assert json_config["resilience"]["maxAttempts"] == 3
    assert json_config["memory"]["persistPath"] == ".swarm"
    assert json_config["swarm"]["strategy"] == "specialized"
    assert json_config["swarm"]["communicationProtocol"] == "message-bus"
    assert "не меняет роли/модели/assignee" in agents
    assert "не обходит Reviewer" in agents
    assert "не включай его systemd timer" in agents


def test_json_and_yaml_config_surfaces_are_value_equivalent() -> None:
    json_config = json.loads((ROOT / "claude-flow.config.json").read_text())
    yaml_config = yaml.safe_load((ROOT / ".claude-flow/config.yaml").read_text())
    for section in (
        "agents",
        "swarm",
        "memory",
        "mcp",
        "cli",
        "hooks",
        "neural",
        "autopilot",
        "resilience",
    ):
        assert json_config[section] == yaml_config[section], section


def test_legacy_supervisor_timer_is_retired_but_evidence_is_preserved() -> None:
    enabled = subprocess.run(
        ["systemctl", "is-enabled", "hermes-anti-stall-supervisor.timer"],
        text=True,
        capture_output=True,
        check=False,
    )
    active = subprocess.run(
        ["systemctl", "is-active", "hermes-anti-stall-supervisor.timer"],
        text=True,
        capture_output=True,
        check=False,
    )
    assert enabled.stdout.strip() == "disabled"
    assert active.stdout.strip() == "inactive"
    for rel in (
        "services/anti_stall_supervisor/policy.json",
        "services/anti_stall_supervisor/CONTRACT.json",
        "runtime/anti_stall_supervisor_var/state.json",
        "runtime/anti_stall_supervisor_var/report.json",
        "runtime/anti_stall_supervisor_var/audit.jsonl",
    ):
        assert (ROOT / rel).is_file(), rel
