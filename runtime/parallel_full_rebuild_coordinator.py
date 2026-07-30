#!/usr/bin/env python3
"""Run three isolated full-source dictionary rebuild lanes, then publish all six."""
from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path("/root/main")
SOURCE = ROOT / "bets_data/analise_pub_matches/json_parts_split_from_object"
PRODUCTION = ROOT / "bets_data/analise_pub_matches"
RUNTIME = ROOT / "runtime/full_dicts_rebuild"
RUN_ID = os.environ.get("PARALLEL_REBUILD_RUN_ID", datetime.now().strftime("parallel_all_%Y%m%d_%H%M%S"))
RUN_ROOT = RUNTIME / RUN_ID
PYTHON = ROOT / "venv/bin/python"
SCRIPT = ROOT / "base/explore_database.py"
TEST_SET = PRODUCTION / "extracted_100k_matches.json"
LANES = {
    "early": ("early", "early_end"),
    "phase": ("late", "post_lane"),
    "lane": ("lane",),
    "kills": ("kills_window",),
}
OUTPUTS = {
    "lane": "lane_dict_raw.sqlite3",
    "early": "early_dict_raw.sqlite3",
    "early_end": "early_end_dict_raw.sqlite3",
    "late": "late_dict_raw.sqlite3",
    "post_lane": "post_lane_dict_raw.sqlite3",
    "kills_window": "kills_window_dict_raw.sqlite3",
}
children: dict[str, subprocess.Popen] = {}
logs = {}


def stop_children(*_args) -> None:
    for proc in children.values():
        if proc.poll() is None:
            proc.terminate()
    deadline = time.time() + 15
    for proc in children.values():
        if proc.poll() is None:
            try:
                proc.wait(timeout=max(0.1, deadline - time.time()))
            except subprocess.TimeoutExpired:
                proc.kill()
    for handle in logs.values():
        handle.close()


def source_files() -> list[Path]:
    return sorted(p for p in SOURCE.glob("*.json") if p.name != "merge_patch_summary.json")


def validate_db(path: Path) -> tuple[int, int]:
    if not path.exists() or path.stat().st_size <= 0:
        raise RuntimeError(f"missing/empty artifact: {path}")
    uri = f"{path.resolve().as_uri()}?mode=ro&immutable=1"
    with sqlite3.connect(uri, uri=True) as conn:
        check = conn.execute("PRAGMA quick_check").fetchone()
        if not check or check[0] != "ok":
            raise RuntimeError(f"quick_check failed for {path}: {check}")
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        table = "kv" if "kv" in tables else "stats" if "stats" in tables else None
        if table is None:
            raise RuntimeError(f"no stats table in {path}: {tables}")
        rows = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        if rows <= 0:
            raise RuntimeError(f"empty stats table in {path}")
    return rows, path.stat().st_size


def main() -> int:
    files = source_files()
    if not files:
        raise RuntimeError("no source parts found")
    if RUN_ROOT.exists():
        raise FileExistsError(f"run root already exists: {RUN_ROOT}")
    RUN_ROOT.mkdir(parents=True)
    manifest = {
        "run_id": RUN_ID,
        "started_at": datetime.now().astimezone().isoformat(),
        "source_count": len(files),
        "source_first": files[0].name,
        "source_last": files[-1].name,
        "source_bytes": sum(p.stat().st_size for p in files),
        "time_filter": "disabled",
        "test_set_exists": TEST_SET.exists(),
        "flush_matches": 20000,
        "flush_keys": 2000000,
        "lanes": {name: list(metrics) for name, metrics in LANES.items()},
    }
    (RUN_ROOT / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (RUNTIME / "current_parallel_run.txt").write_text(str(RUN_ROOT) + "\n", encoding="utf-8")
    print(f"RUN_ID={RUN_ID}", flush=True)
    print(f"RUN_ROOT={RUN_ROOT}", flush=True)
    print(f"SOURCE_FILES={len(files)} {files[0].name}..{files[-1].name}", flush=True)
    print("TIME_FILTER=disabled TEST_SET=" + ("present" if TEST_SET.exists() else "absent"), flush=True)

    base_env = os.environ.copy()
    base_env.update({
        "EXPLORE_WRITE_JSON": "0",
        "EXPLORE_MAX_FILES": "0",
        "EXPLORE_MAX_MATCHES": "0",
        "EXPLORE_JSON_DIR": str(SOURCE),
        "EXPLORE_TEST_SET_PATH": str(TEST_SET),
        "EXPLORE_FLUSH_MATCHES": "20000",
        "EXPLORE_FLUSH_KEYS": "2000000",
        "PYTHONUNBUFFERED": "1",
    })
    for lane, metrics in LANES.items():
        lane_root = RUN_ROOT / lane
        stats = lane_root / "stats"
        staging = lane_root / "staging"
        stats.mkdir(parents=True)
        staging.mkdir(parents=True)
        env = base_env.copy()
        env.update({
            "EXPLORE_METRICS": ",".join(metrics),
            "EXPLORE_STATS_DIR": str(stats),
            "EXPLORE_SHARD_DIR": str(staging),
            "EXPLORE_RUN_ID": f"{RUN_ID}_{lane}",
        })
        handle = (lane_root / "rebuild.log").open("wb")
        logs[lane] = handle
        children[lane] = subprocess.Popen(
            [str(PYTHON), "-u", str(SCRIPT)],
            cwd=ROOT,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=handle,
            stderr=subprocess.STDOUT,
        )
        print(f"LANE_START {lane} pid={children[lane].pid} metrics={','.join(metrics)} log={lane_root / 'rebuild.log'}", flush=True)

    while True:
        states = {lane: proc.poll() for lane, proc in children.items()}
        failed = {lane: rc for lane, rc in states.items() if rc not in (None, 0)}
        if failed:
            print(f"LANE_FAILED {failed}; terminating siblings", flush=True)
            stop_children()
            return 1
        if all(rc == 0 for rc in states.values()):
            break
        sizes = {}
        rss = {}
        for lane, proc in children.items():
            lane_root = RUN_ROOT / lane
            sizes[lane] = sum(p.stat().st_size for p in lane_root.rglob("*.sqlite3") if p.is_file())
            if proc.poll() is None:
                try:
                    fields = Path(f"/proc/{proc.pid}/status").read_text().splitlines()
                    rss[lane] = next(line.split(":", 1)[1].strip() for line in fields if line.startswith("VmRSS:"))
                except Exception:
                    rss[lane] = "?"
        print(f"PROGRESS states={states} sqlite_bytes={sizes} rss={rss}", flush=True)
        time.sleep(60)

    for handle in logs.values():
        handle.close()
    logs.clear()
    print("ALL_LANES_SUCCESS; validating all artifacts before production publication", flush=True)
    validated = {}
    publish_plan = []
    for lane, metrics in LANES.items():
        for metric in metrics:
            source = RUN_ROOT / lane / "stats" / OUTPUTS[metric]
            rows, size = validate_db(source)
            validated[metric] = {"rows": rows, "bytes": size, "path": str(source)}
            publish_plan.append((metric, source, PRODUCTION / OUTPUTS[metric]))
            print(f"VALID {metric} rows={rows} bytes={size}", flush=True)

    # Recheck every source immediately before the first production-side effect.
    for _metric, source, _target in publish_plan:
        validate_db(source)
    for metric, source, target in publish_plan:
        os.replace(source, target)
        print(f"PUBLISHED {metric} -> {target}", flush=True)
    manifest["completed_at"] = datetime.now().astimezone().isoformat()
    manifest["status"] = "SUCCESS"
    manifest["validated"] = validated
    (RUN_ROOT / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print("EXIT:0", flush=True)
    return 0


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop_children)
    signal.signal(signal.SIGINT, stop_children)
    try:
        raise SystemExit(main())
    except BaseException as exc:
        if not isinstance(exc, SystemExit):
            print(f"COORDINATOR_ERROR: {type(exc).__name__}: {exc}", flush=True)
        stop_children()
        raise
