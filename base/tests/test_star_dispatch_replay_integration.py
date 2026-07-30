#!/usr/bin/env python3
"""Integration TDD for resumable STAR dispatch replay CLI.

Covers:
- deterministic small fixture extraction
- global cross-shard map_id dedup
- unique event_id
- absent E/L/A null (never loss)
- checkpoint observability observed/map_ended/missing
- interrupt/resume without duplicate rows
- validate-only failure on incomplete/corrupt staging
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path
from typing import Any

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
ROOT_DIR = BASE_DIR.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import run_star_dispatch_replay as replay  # noqa: E402


def _leads(values_by_minute: dict[int, float], length: int = 45) -> list:
    arr: list[Any] = [None] * length
    for minute, val in values_by_minute.items():
        if 1 <= minute <= length:
            arr[minute - 1] = val
    return arr


def _write_fixture_corpus(root: Path) -> Path:
    """Two shards with intentional duplicate map_id + invalid id."""
    shards = root / "json_parts"
    shards.mkdir(parents=True)
    part1 = {
        "1001": {
            "id": 1001,
            "startDateTime": 1_800_000_100,
            "durationSeconds": 2500,
            "didRadiantWin": True,
            "radiantNetworthLeads": _leads(
                {
                    6: 100,
                    10: 500,
                    12: 900,
                    15: 1200,
                    20: 2000,
                    27: 2500,
                    34: 3000,
                    35: 3100,
                    40: 4000,
                }
            ),
            "players": [
                {"heroId": 1, "isRadiant": True, "position": "POSITION_1"},
                {"heroId": 2, "isRadiant": True, "position": "POSITION_2"},
                {"heroId": 3, "isRadiant": True, "position": "POSITION_3"},
                {"heroId": 4, "isRadiant": True, "position": "POSITION_4"},
                {"heroId": 5, "isRadiant": True, "position": "POSITION_5"},
                {"heroId": 6, "isRadiant": False, "position": "POSITION_1"},
                {"heroId": 7, "isRadiant": False, "position": "POSITION_2"},
                {"heroId": 8, "isRadiant": False, "position": "POSITION_3"},
                {"heroId": 9, "isRadiant": False, "position": "POSITION_4"},
                {"heroId": 10, "isRadiant": False, "position": "POSITION_5"},
            ],
        },
        "1002": {
            "id": 1002,
            "startDateTime": 1_800_000_200,
            "durationSeconds": 900,  # dies before 34
            "didRadiantWin": False,
            "radiantNetworthLeads": _leads({6: -500, 10: -900, 12: -1000}, length=15),
            "players": [
                {"heroId": 11, "isRadiant": True, "position": "POSITION_1"},
                {"heroId": 12, "isRadiant": True, "position": "POSITION_2"},
                {"heroId": 13, "isRadiant": True, "position": "POSITION_3"},
                {"heroId": 14, "isRadiant": True, "position": "POSITION_4"},
                {"heroId": 15, "isRadiant": True, "position": "POSITION_5"},
                {"heroId": 16, "isRadiant": False, "position": "POSITION_1"},
                {"heroId": 17, "isRadiant": False, "position": "POSITION_2"},
                {"heroId": 18, "isRadiant": False, "position": "POSITION_3"},
                {"heroId": 19, "isRadiant": False, "position": "POSITION_4"},
                {"heroId": 20, "isRadiant": False, "position": "POSITION_5"},
            ],
        },
        "bad": {
            "id": "not-valid",
            "startDateTime": 1_800_000_050,
            "durationSeconds": 100,
            "didRadiantWin": True,
            "radiantNetworthLeads": [],
            "players": [],
        },
    }
    part2 = {
        # cross-shard duplicate of 1001 (must be skipped)
        "1001": {
            "id": 1001,
            "startDateTime": 1_800_000_100,
            "durationSeconds": 1111,
            "didRadiantWin": False,
            "radiantNetworthLeads": _leads({6: 1}),
            "players": part1["1001"]["players"],
        },
        "1003": {
            "id": 1003,
            "startDateTime": 1_800_000_300,
            "durationSeconds": 2100,
            "didRadiantWin": True,
            "radiantNetworthLeads": _leads(
                {6: 50, 10: 80, 12: 100, 15: 200, 20: 300, 27: 400, 34: 500, 35: 550}
            ),
            "players": part1["1001"]["players"],
        },
    }
    (shards / "7.41a_part001.json").write_text(
        json.dumps(part1, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    (shards / "7.41a_part002.json").write_text(
        json.dumps(part2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return shards


def _blocks_by_map() -> dict[int, dict[str, Any]]:
    """Precomputed E/L/A for fixture maps (no dictionary load)."""
    return {
        1001: {
            "E": {"present": True, "sign": 1, "tier": 70, "hit_count": 2},
            "L": {"present": True, "sign": 1, "tier": 75, "hit_count": 3},
            "A": {"present": False},
        },
        1002: {
            "E": {"present": True, "sign": 1, "tier": 65, "hit_count": 1},
            "L": {"present": True, "sign": -1, "tier": 80, "hit_count": 2},
            "A": {"present": True, "sign": 1, "tier": 70, "hit_count": 1},
        },
        1003: {
            "E": {"present": False},
            "L": {"present": True, "sign": -1, "tier": 70, "hit_count": 1},
            "A": {"present": True, "sign": -1, "tier": 65, "hit_count": 1},
        },
    }


def _cfg(tmp_path: Path, maps_path: Path, staging: Path, **overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "population": "pub_maps_fixture",
        "maps_path": str(maps_path),
        "stats_dir": str(tmp_path / "stats_unused"),
        "start_date_time": 1_700_000_000,
        "max_unique": 40000,
        "checkpoints": [6, 10, 12, 15, 20, 27, 34, 35, 40],
        "buckets": {"train": 0.6, "valid": 0.2, "test": 0.2},
        "split_rule": "chronology_60_20_20",
        "seed": 20260716,
        "cutoffs": {"start_date_time": 1_700_000_000},
        "policy_constants": {"stake_unit": 1.0, "max_unique": 40000},
        "code_version": "test-integration-v1",
        "checkpoint_every_unique_maps": 2,
        "staging_dir": str(staging),
        "dispatch_minute": 12,
        "block_source": "precomputed",
        "precomputed_blocks": {
            str(k): v for k, v in _blocks_by_map().items()
        },
        "skip_dictionary_load": True,
        "run_downstream_analysis": True,
        "expected_unique": 3,
        "legacy_anchors": {
            "generic_opposite_n": 13266,
            "exact_la_n": 874,
            "late_wins_on_exact_la": 257,
            "e_eq_a_ne_l_n": 860,
        },
    }
    cfg.update(overrides)
    return cfg


def _write_config(path: Path, cfg: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cfg, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


@pytest.fixture
def fixture_env(tmp_path: Path):
    maps = _write_fixture_corpus(tmp_path)
    staging = tmp_path / "staging" / "replay"
    cfg = _cfg(tmp_path, maps, staging)
    cfg_path = _write_config(tmp_path / "config.json", cfg)
    return {"cfg": cfg, "cfg_path": cfg_path, "staging": staging, "maps": maps}


def test_deterministic_small_fixture_extraction(fixture_env):
    rc = replay.main(["--config", str(fixture_env["cfg_path"])])
    assert rc == 0
    staging = fixture_env["staging"]
    assert (staging / "checkpoint_state.json").exists()
    state = json.loads((staging / "checkpoint_state.json").read_text(encoding="utf-8"))
    assert state["lifecycle_status"] == "complete"
    assert state["counts"]["unique_maps"] == 3

    map_rows = replay.load_map_rows(staging)
    disp_rows = replay.load_dispatch_rows(staging)
    assert len(map_rows) == 3
    assert len(disp_rows) == 3

    # Global cross-shard dedup: 1001 appears once (first shard wins)
    map_ids = [int(r["map_id"]) for r in map_rows]
    assert map_ids.count(1001) == 1
    assert sorted(map_ids) == [1001, 1002, 1003]

    # Unique event_ids
    eids = [r["event_id"] for r in disp_rows]
    assert len(eids) == len(set(eids))
    map_eids = [r["event_id"] for r in map_rows]
    assert len(map_eids) == len(set(map_eids))
    assert set(eids) == set(map_eids)

    # Counts file
    counts = json.loads((staging / "counts.json").read_text(encoding="utf-8"))
    assert counts["unique_accepted"] == 3
    assert counts["duplicate_skipped"] >= 1
    assert counts["invalid_id"] >= 1
    assert counts["raw_seen"] >= 5


def test_absent_blocks_null_and_checkpoint_states(fixture_env):
    assert replay.main(["--config", str(fixture_env["cfg_path"])]) == 0
    staging = fixture_env["staging"]
    disp_rows = {int(r["map_id"]): r for r in replay.load_dispatch_rows(staging)}
    map_rows = {int(r["map_id"]): r for r in replay.load_map_rows(staging)}

    # Map 1001: A absent → all value fields null
    a = disp_rows[1001]["blocks"]["A"]
    assert a["present"] is False
    for k in ("side", "sign", "tier", "hit_count", "won"):
        assert a[k] is None

    # Independent present-block outcomes still set
    assert disp_rows[1001]["blocks"]["E"]["present"] is True
    assert disp_rows[1001]["blocks"]["E"]["won"] is True  # radiant won, E sign=1
    assert disp_rows[1001]["actual_send_time"] is None
    assert disp_rows[1001]["time_kind"] == "checkpoint"

    # Checkpoints: 1002 short map → map_ended for late minutes
    cps = map_rows[1002]["checkpoints"]
    # keys may be int or str after JSON roundtrip
    def cp(m):
        return cps.get(m) or cps.get(str(m))

    assert cp(6)["state"] == "observed"
    assert cp(34)["state"] == "map_ended"
    # 1001 has full leads → observed at 12/34
    cps1 = map_rows[1001]["checkpoints"]
    def cp1(m):
        return cps1.get(m) or cps1.get(str(m))
    assert cp1(12)["state"] == "observed"
    assert cp1(34)["state"] == "observed"


def test_interrupt_resume_no_duplicate_rows(fixture_env, tmp_path: Path):
    cfg = dict(fixture_env["cfg"])
    # Force small checkpoint batches and artificial stop after 2 unique maps
    cfg["checkpoint_every_unique_maps"] = 1
    cfg["max_unique"] = 2
    cfg["expected_unique"] = 2
    cfg_path = _write_config(tmp_path / "cfg_partial.json", cfg)
    assert replay.main(["--config", str(cfg_path)]) == 0
    staging = Path(cfg["staging_dir"])
    state1 = json.loads((staging / "checkpoint_state.json").read_text(encoding="utf-8"))
    assert state1["counts"]["unique_maps"] == 2

    # Resume with higher max_unique — must append only the missing map
    cfg2 = dict(cfg)
    cfg2["max_unique"] = 3
    cfg2["expected_unique"] = 3
    # Keep same identity-bearing config fields for resume
    cfg_path2 = _write_config(tmp_path / "cfg_resume.json", cfg2)
    assert replay.main(["--config", str(cfg_path2), "--resume"]) == 0

    map_rows = replay.load_map_rows(staging)
    map_ids = [int(r["map_id"]) for r in map_rows]
    assert len(map_ids) == 3
    assert len(set(map_ids)) == 3
    assert sorted(map_ids) == [1001, 1002, 1003]

    # Re-running resume again must not duplicate
    assert replay.main(["--config", str(cfg_path2), "--resume"]) == 0
    map_rows2 = replay.load_map_rows(staging)
    assert len(map_rows2) == 3


def test_validate_staging_ok_and_fail_on_corrupt(fixture_env):
    assert replay.main(["--config", str(fixture_env["cfg_path"])]) == 0
    staging = fixture_env["staging"]
    assert replay.main(["--validate-staging", str(staging)]) == 0

    # Corrupt a shard hash by mutating content after state was written
    shard = next(staging.glob("map_rows_*.jsonl"))
    original = shard.read_text(encoding="utf-8")
    shard.write_text(original + '{"map_id":"999","event_id":"bogus"}\n', encoding="utf-8")
    rc = replay.main(["--validate-staging", str(staging)])
    assert rc != 0


def test_validate_incomplete_staging_fails(tmp_path: Path):
    staging = tmp_path / "empty_staging"
    staging.mkdir(parents=True)
    (staging / "checkpoint_state.json").write_text(
        json.dumps(
            {
                "version": 1,
                "lifecycle_status": "partial",
                "counts": {"unique_maps": 1, "map_rows": 1, "dispatch_rows": 0},
                "shard_hashes": {},
                "committed_map_ids": ["1"],
                "committed_event_ids": ["e1"],
            }
        ),
        encoding="utf-8",
    )
    rc = replay.main(["--validate-staging", str(staging)])
    assert rc != 0


def test_downstream_analysis_artifacts_written(fixture_env):
    assert replay.main(["--config", str(fixture_env["cfg_path"])]) == 0
    staging = fixture_env["staging"]
    for name in (
        "metrics_result.json",
        "policy_result.json",
        "leakage_labels.json",
        "legacy_anchor_delta.json",
        "counts.json",
        "config.json",
    ):
        assert (staging / name).exists(), name
    leakage = json.loads((staging / "leakage_labels.json").read_text(encoding="utf-8"))
    assert leakage.get("label") == "diagnostic_in_sample"
    delta = json.loads((staging / "legacy_anchor_delta.json").read_text(encoding="utf-8"))
    assert "exact_la" in delta
    assert "generic_opposite" in delta
