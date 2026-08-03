#!/usr/bin/env python3
"""Build isolated full Early/Post-lane dictionaries without minute-10 gates.

The existing ``explore_database.py`` pipeline remains the sole implementation
of dictionary families and targets.  This orchestrator gives it a symlink-only
view of exact 7.41..7.41d shards, runs Early and Post-lane sequentially in
separate runtime staging directories, validates both SQLite files, and only
then publishes distinct ``*_no10gate.sqlite3`` artifacts.

The current production ``early_dict_raw.sqlite3`` and
``post_lane_dict_raw.sqlite3`` are never targeted or modified.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXPLORE_SCRIPT = PROJECT_ROOT / "base" / "explore_database.py"
DEFAULT_SOURCE_DIR = Path("/root/main/bets_data/analise_pub_matches/json_parts_split_from_object")
DEFAULT_OUTPUT_DIR = Path("/root/main/bets_data/analise_pub_matches/no10gate_full_dicts")
DEFAULT_RUNTIME_ROOT = Path("/root/main/runtime/no10_full_dicts")
SHARD_NAME_RE = re.compile(r"^7\.41(?:[a-d])?_part\d+\.json$")

PHASES: tuple[dict[str, str], ...] = (
    {
        "metric": "early",
        "staged_name": "early_dict_raw.sqlite3",
        "artifact_name": "early_dict_raw_no10gate.sqlite3",
        "disabled_gate": "ANALISE_EARLY_MINUTE10_GATE_ENABLED",
    },
    {
        "metric": "post_lane",
        "staged_name": "post_lane_dict_raw.sqlite3",
        "artifact_name": "post_lane_dict_raw_no10gate.sqlite3",
        "disabled_gate": "ANALISE_POST_LANE_MINUTE10_GATE_ENABLED",
    },
)


def select_741_shards(source_dir: Path) -> list[Path]:
    """Select only exact 7.41, 7.41a, ..., 7.41d part files."""
    source_dir = Path(source_dir)
    shards = sorted(
        path for path in source_dir.iterdir()
        if path.is_file() and SHARD_NAME_RE.fullmatch(path.name)
    )
    if not shards:
        raise FileNotFoundError(f"No exact 7.41..7.41d shards found in {source_dir}")
    return shards


def create_source_view(shards: Sequence[Path], view_dir: Path) -> Path:
    """Create a fresh symlink-only source view; never delete or replace links."""
    view_dir = Path(view_dir)
    view_dir.mkdir(parents=True, exist_ok=False)
    for shard in shards:
        link = view_dir / shard.name
        if link.exists() or link.is_symlink():
            raise FileExistsError(f"Refusing to replace source-view entry: {link}")
        link.symlink_to(Path(shard).resolve())
    return view_dir


def phase_environment(
    base_environment: Mapping[str, str],
    *,
    metric: str,
    source_view: Path,
    stats_dir: Path,
    shard_dir: Path,
    no_test_set_path: Path,
    run_id: str,
) -> dict[str, str]:
    env = dict(base_environment)
    env.update({
        "EXPLORE_METRICS": metric,
        "EXPLORE_JSON_DIR": str(source_view),
        "EXPLORE_STATS_DIR": str(stats_dir),
        "EXPLORE_TEST_SET_PATH": str(no_test_set_path),
        "EXPLORE_SHARD_DIR": str(shard_dir),
        "EXPLORE_RUN_ID": run_id,
        "EXPLORE_KEEP_SHARDS": "1",
        "EXPLORE_WRITE_JSON": "0",
        "EXPLORE_MAX_FILES": "0",
        "EXPLORE_MAX_MATCHES": "0",
        "PYTHONUNBUFFERED": "1",
        # Explicit isolation: exactly one phase gate is disabled per process.
        "ANALISE_EARLY_MINUTE10_GATE_ENABLED": "0" if metric == "early" else "1",
        "ANALISE_POST_LANE_MINUTE10_GATE_ENABLED": "0" if metric == "post_lane" else "1",
    })
    return env


def validate_sqlite(path: Path) -> dict[str, Any]:
    path = Path(path)
    if not path.is_file() or path.stat().st_size <= 0:
        raise RuntimeError(f"Missing or empty staged SQLite: {path}")
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        quick_check = connection.execute("PRAGMA quick_check").fetchone()
        if not quick_check or quick_check[0] != "ok":
            raise RuntimeError(f"SQLite quick_check failed for {path}: {quick_check}")
        entries = int(connection.execute("SELECT COUNT(*) FROM kv").fetchone()[0])
        if entries <= 0:
            raise RuntimeError(f"Refusing to publish empty dictionary: {path}")
    finally:
        connection.close()
    return {"quick_check": "ok", "entries": entries, "size_bytes": path.stat().st_size}


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def build_full_no10_dicts(
    *,
    source_dir: Path,
    output_dir: Path,
    runtime_root: Path,
    python_executable: Path,
    explore_script: Path = EXPLORE_SCRIPT,
    run_id: str | None = None,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> dict[str, Any]:
    source_dir = Path(source_dir).resolve()
    output_dir = Path(output_dir).resolve()
    runtime_root = Path(runtime_root).resolve()
    explore_script = Path(explore_script).resolve()
    python_executable = Path(python_executable).resolve()
    if not explore_script.is_file():
        raise FileNotFoundError(f"explore_database.py not found: {explore_script}")

    shards = select_741_shards(source_dir)
    run_id = run_id or f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{os.getpid()}"
    run_dir = runtime_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    source_view = create_source_view(shards, run_dir / "source_741_view")
    no_test_set_path = run_dir / "no_test_set_exclusion.json"
    if no_test_set_path.exists():
        raise FileExistsError(f"No-test-set sentinel must not exist: {no_test_set_path}")

    staged: dict[str, Path] = {}
    phase_records: dict[str, Any] = {}
    for phase in PHASES:  # Deliberately sequential for bounded disk/RAM usage.
        metric = phase["metric"]
        phase_root = run_dir / metric
        stats_dir = phase_root / "stats"
        shard_dir = phase_root / "explore_staging"
        stats_dir.mkdir(parents=True, exist_ok=False)
        env = phase_environment(
            os.environ,
            metric=metric,
            source_view=source_view,
            stats_dir=stats_dir,
            shard_dir=shard_dir,
            no_test_set_path=no_test_set_path,
            run_id=f"{run_id}_{metric}",
        )
        command = [str(python_executable), str(explore_script)]
        runner(command, cwd=str(PROJECT_ROOT), env=env, check=True)
        staged_path = stats_dir / phase["staged_name"]
        validation = validate_sqlite(staged_path)
        staged[metric] = staged_path
        phase_records[metric] = {
            "command": command,
            "gate_disabled": phase["disabled_gate"],
            "staged_path": str(staged_path),
            "validation": validation,
        }

    # Neither final artifact is touched until both complete builds validate.
    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts: dict[str, Any] = {}
    for phase in PHASES:
        metric = phase["metric"]
        target = output_dir / phase["artifact_name"]
        os.replace(staged[metric], target)
        artifacts[metric] = {
            "path": str(target),
            **validate_sqlite(target),
        }

    manifest = {
        "schema": "no10_full_dicts.v1",
        "run_id": run_id,
        "source_dir": str(source_dir),
        "source_rule": SHARD_NAME_RE.pattern,
        "source_shards": [str(path) for path in shards],
        "source_shard_count": len(shards),
        "test_set_exclusion": False,
        "metrics_sequential": [phase["metric"] for phase in PHASES],
        "current_artifacts_touched": False,
        "post_lane_solo_scope": "existing analise_database semantics: latest patch only",
        "run_dir": str(run_dir),
        "phases": phase_records,
        "artifacts": artifacts,
    }
    manifest_path = output_dir / "no10gate_full_dicts_manifest.json"
    _atomic_json(manifest_path, manifest)
    manifest["manifest_path"] = str(manifest_path)
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--runtime-root", type=Path, default=DEFAULT_RUNTIME_ROOT)
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    parser.add_argument("--explore-script", type=Path, default=EXPLORE_SCRIPT)
    parser.add_argument("--run-id", default=None)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest = build_full_no10_dicts(
        source_dir=args.source_dir,
        output_dir=args.output_dir,
        runtime_root=args.runtime_root,
        python_executable=args.python,
        explore_script=args.explore_script,
        run_id=args.run_id,
    )
    print(json.dumps({
        "status": "ok",
        "manifest": manifest["manifest_path"],
        "artifacts": manifest["artifacts"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
