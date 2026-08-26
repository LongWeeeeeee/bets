#!/usr/bin/env python3
"""Migrate the retired Hermes anti-stall supervisor's durable contract into RuFlo.

The original runtime files remain as an immutable archival source. This script imports
only durable policy/contract facts into AgentDB-backed `memory_entries`; transient tick,
PID, cooldown, and in-flight task state is deliberately excluded.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = ROOT / "services" / "anti_stall_supervisor"
DEFAULT_DB = ROOT / ".swarm" / "memory.db"
DEFAULT_MANIFEST = ROOT / ".claude-flow" / "migration" / "anti-stall-v1.json"
NAMESPACE = "ingame-orchestration"
SCHEMA = "anti_stall_to_ruflo_v1"

SOURCES = {
    "policy": SOURCE_ROOT / "policy.json",
    "snapshot_contract": SOURCE_ROOT / "CONTRACT.json",
    "decision_contract": SOURCE_ROOT / "DECISION_SCHEMA.json",
    "executor_contract": SOURCE_ROOT / "EXECUTOR_CONTRACT.json",
    "report_contract": SOURCE_ROOT / "REPORT_SCHEMA.json",
}

SUMMARY = (
    "The legacy hermes-anti-stall-supervisor is retired. RuFlo owns persistent "
    "orchestration via hierarchical-mesh swarm, goals, intelligence, bounded autopilot, "
    "and AgentDB. Preserve these invariants: no action for needs_input/human/secret/"
    "credential/auth/production-safety/ownership/reviewer-issues/checksum/evidence/"
    "stale-approval blockers; dependency children start only after all parents succeed; "
    "protocol violations get at most two attempts; default action cooldown is 300s; "
    "stalls require repeated no-progress evidence; artifact identity is content-hash "
    "based; fail closed on malformed evidence. Task ownership, kanban terminal protocol, "
    "retry ceilings, salvage, and staging-to-single-INT graph rules remain canonical in "
    "AGENTS.md. Do not restore the custom systemd supervisor."
)


def canonical(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def load_sources() -> tuple[dict[str, object], dict[str, dict[str, object]]]:
    payloads: dict[str, object] = {}
    evidence: dict[str, dict[str, object]] = {}
    for name, path in SOURCES.items():
        raw = path.read_bytes()
        payloads[name] = json.loads(raw)
        evidence[name] = {
            "path": str(path.relative_to(ROOT)),
            "sha256": sha256_bytes(raw),
            "size": len(raw),
        }
    return payloads, evidence


def build_entries(payloads: dict[str, object], evidence: dict[str, dict[str, object]]) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    entries.append(
        {
            "key": "anti-stall-policy-v1",
            "value": canonical(payloads["policy"]),
            "tags": ["migration", "anti-stall", "policy", "ruflo"],
            "metadata": {"schema": SCHEMA, "source": evidence["policy"]},
        }
    )
    for name in ("snapshot_contract", "decision_contract", "executor_contract", "report_contract"):
        entries.append(
            {
                "key": f"anti-stall-{name.replace('_', '-')}-v1",
                "value": canonical(payloads[name]),
                "tags": ["migration", "anti-stall", "contract", "ruflo"],
                "metadata": {"schema": SCHEMA, "source": evidence[name]},
            }
        )
    entries.append(
        {
            "key": "anti-stall-ruflo-handoff-v1",
            "value": SUMMARY,
            "tags": ["migration", "anti-stall", "handoff", "ruflo", "canonical"],
            "metadata": {
                "schema": SCHEMA,
                "source_hashes": {name: row["sha256"] for name, row in evidence.items()},
                "excluded_transient_state": [
                    "tick_count",
                    "cooldowns",
                    "action_keys",
                    "consecutive_stall_counters",
                    "pids",
                    "in_flight_tasks",
                    "audit_log",
                ],
            },
        }
    )
    return entries


def ruflo_store(entry: dict[str, object]) -> dict[str, object]:
    params = {
        "key": entry["key"],
        "value": entry["value"],
        "namespace": NAMESPACE,
        "tags": entry["tags"],
        "metadata": entry["metadata"],
        "upsert": True,
    }
    proc = subprocess.run(
        ["ruflo", "mcp", "exec", "-t", "memory_store", "-p", canonical(params)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"memory_store failed for {entry['key']}: {proc.stderr or proc.stdout}")
    marker = "Result:\n"
    if marker not in proc.stdout:
        raise RuntimeError(f"unexpected memory_store output for {entry['key']}: {proc.stdout}")
    result = json.loads(proc.stdout.split(marker, 1)[1])
    if not result.get("success") or not result.get("stored"):
        raise RuntimeError(f"memory_store rejected {entry['key']}: {result}")
    return result


def verify_db(db_path: Path, entries: list[dict[str, object]]) -> list[dict[str, object]]:
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    rows: list[dict[str, object]] = []
    try:
        for expected in entries:
            row = con.execute(
                "SELECT key, namespace, content, type, tags, metadata, status "
                "FROM memory_entries WHERE namespace=? AND key=?",
                (NAMESPACE, expected["key"]),
            ).fetchone()
            if row is None:
                raise RuntimeError(f"missing migrated entry: {expected['key']}")
            key, namespace, content, kind, tags, metadata, status = row
            if content != expected["value"] or status != "active":
                raise RuntimeError(f"mismatched migrated entry: {key}")
            rows.append(
                {
                    "key": key,
                    "namespace": namespace,
                    "type": kind,
                    "status": status,
                    "content_sha256": sha256_bytes(content.encode()),
                    "tags": json.loads(tags or "[]"),
                    "metadata_sha256": sha256_bytes((metadata or "").encode()),
                }
            )
    finally:
        con.close()
    return rows


def atomic_write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile("w", dir=path.parent, prefix=path.name + ".", suffix=".tmp", delete=False) as tmp:
        tmp.write(data)
        tmp.flush()
        Path(tmp.name).replace(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    args = parser.parse_args()

    payloads, evidence = load_sources()
    entries = build_entries(payloads, evidence)
    store_results: list[dict[str, object]] = []
    if not args.verify_only:
        store_results = [ruflo_store(entry) for entry in entries]
    verified = verify_db(args.db, entries)
    manifest = {
        "schema": SCHEMA,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "namespace": NAMESPACE,
        "source_root": str(SOURCE_ROOT.relative_to(ROOT)),
        "source_evidence": evidence,
        "excluded_transient_state": entries[-1]["metadata"]["excluded_transient_state"],
        "entries": verified,
        "stored_count": len(store_results) if not args.verify_only else None,
        "verified_count": len(verified),
    }
    if not args.verify_only:
        atomic_write_json(args.manifest, manifest)
    print(canonical({"ok": True, "verified": len(verified), "manifest": str(args.manifest)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
