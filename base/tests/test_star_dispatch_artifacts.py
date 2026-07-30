"""TDD tests for crash-safe, duplicate-safe star-dispatch artifact layer.

Covers:
- crash-before-replace leaves previous checkpoint resumable
- resumed no-duplicate append
- config/hash identity mismatch fail-closed
- corrupted shard detection
- deterministic source corpus digest
- gzip compaction + validation
- manifest completeness
- preservation of old shards (no deletion)
"""

from __future__ import annotations

import gzip
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import star_dispatch_artifacts as sda  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
STAGING_ARTIFACTS = REPO_ROOT / "runtime" / "star_dispatch_replay" / "staging" / "artifacts"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    d = tmp_path / "ckpt"
    d.mkdir()
    return d


@pytest.fixture
def source_tree(tmp_path: Path) -> Path:
    """Tiny source corpus with two shards + one dictionary fingerprint target."""
    root = tmp_path / "corpus"
    (root / "shards").mkdir(parents=True)
    (root / "dicts").mkdir()
    (root / "shards" / "part_a.json").write_text(
        json.dumps({"maps": [1, 2]}, sort_keys=True) + "\n", encoding="utf-8"
    )
    (root / "shards" / "part_b.json").write_text(
        json.dumps({"maps": [3]}, sort_keys=True) + "\n", encoding="utf-8"
    )
    (root / "dicts" / "hero.json").write_text(
        json.dumps({"1": "antimage"}, sort_keys=True) + "\n", encoding="utf-8"
    )
    return root


def _base_config(**overrides: Any) -> Dict[str, Any]:
    cfg = {
        "population": "pub_maps",
        "checkpoints": [6, 10, 12, 15, 20, 27, 34, 35, 40],
        "buckets": {"train": 0.6, "valid": 0.2, "test": 0.2},
        "split_rule": "chronology_60_20_20",
        "cutoffs": {"start_date_time": "2025-12-15T00:00:00Z"},
        "seed": 20260716,
        "policy_constants": {"stake_unit": 1.0, "max_unique": 40000},
        "code_version": "test-v1",
        "checkpoint_every_unique_maps": 3,
    }
    cfg.update(overrides)
    return cfg


def _map_row(map_id: str, event_id: str | None = None, **extra: Any) -> Dict[str, Any]:
    row = {
        "map_id": map_id,
        "event_id": event_id or f"evt:{map_id}",
        "series_id": map_id.split(":")[0],
        "map_number": int(map_id.split(":")[1]) if ":" in map_id else 1,
    }
    row.update(extra)
    return row


def _dispatch_row(map_id: str, event_id: str | None = None, **extra: Any) -> Dict[str, Any]:
    row = {
        "map_id": map_id,
        "event_id": event_id or f"evt:{map_id}",
        "block": "late",
        "side": "radiant",
    }
    row.update(extra)
    return row


# ---------------------------------------------------------------------------
# Hash / digest determinism
# ---------------------------------------------------------------------------


def test_file_sha256_stable(tmp_path: Path):
    p = tmp_path / "x.bin"
    p.write_bytes(b"abc")
    h1 = sda.sha256_file(p)
    h2 = sda.sha256_file(p)
    assert h1 == h2
    assert len(h1) == 64


def test_source_corpus_digest_deterministic(source_tree: Path):
    paths = [
        source_tree / "shards" / "part_b.json",
        source_tree / "shards" / "part_a.json",
    ]
    d1 = sda.source_corpus_digest(paths, root=source_tree)
    d2 = sda.source_corpus_digest(list(reversed(paths)), root=source_tree)
    assert d1 == d2
    assert len(d1) == 64

    # Content change must change digest
    (source_tree / "shards" / "part_a.json").write_text('{"maps":[9]}\n', encoding="utf-8")
    d3 = sda.source_corpus_digest(paths, root=source_tree)
    assert d3 != d1


def test_config_hash_covers_required_fields():
    c1 = _base_config()
    c2 = _base_config(seed=999)
    c3 = _base_config(code_version="test-v2")
    h1 = sda.config_hash(c1)
    h2 = sda.config_hash(c2)
    h3 = sda.config_hash(c3)
    assert h1 != h2
    assert h1 != h3
    assert len(h1) == 64
    # same config → same hash regardless of key insertion order
    c1b = dict(reversed(list(c1.items())))
    assert sda.config_hash(c1b) == h1


def test_dictionary_fingerprints(source_tree: Path):
    fps = sda.dictionary_fingerprints(
        {
            "hero": source_tree / "dicts" / "hero.json",
        }
    )
    assert "hero" in fps
    assert len(fps["hero"]) == 64


# ---------------------------------------------------------------------------
# Checkpoint store: append, resume, no-duplicate
# ---------------------------------------------------------------------------


def test_append_and_checkpoint_creates_shards(work_dir: Path, source_tree: Path):
    store = sda.ArtifactCheckpointStore(
        work_dir,
        config=_base_config(checkpoint_every_unique_maps=2),
        source_paths=[
            source_tree / "shards" / "part_a.json",
            source_tree / "shards" / "part_b.json",
        ],
        dictionaries={"hero": source_tree / "dicts" / "hero.json"},
        source_root=source_tree,
    )
    store.open_or_create()

    for i in range(3):
        mid = f"100:{i + 1}"
        store.append_map_row(_map_row(mid), input_cursor=f"cursor:{i}")
        store.append_dispatch_row(_dispatch_row(mid))

    store.maybe_checkpoint(force=True)
    state = store.load_state()
    assert state["counts"]["unique_maps"] == 3
    assert state["counts"]["map_rows"] == 3
    assert state["counts"]["dispatch_rows"] == 3
    assert state["last_committed_input_cursor"] == "cursor:2"
    assert len(state["committed_map_ids"]) == 3
    assert len(state["shard_hashes"]) >= 1
    shards = list(work_dir.glob("map_rows_*.jsonl"))
    assert shards


def test_resume_skips_already_committed_ids(work_dir: Path, source_tree: Path):
    cfg = _base_config(checkpoint_every_unique_maps=2)
    paths = [
        source_tree / "shards" / "part_a.json",
        source_tree / "shards" / "part_b.json",
    ]
    dicts = {"hero": source_tree / "dicts" / "hero.json"}

    s1 = sda.ArtifactCheckpointStore(
        work_dir, config=cfg, source_paths=paths, dictionaries=dicts, source_root=source_tree
    )
    s1.open_or_create()
    s1.append_map_row(_map_row("200:1"), input_cursor="c0")
    s1.append_dispatch_row(_dispatch_row("200:1"))
    s1.append_map_row(_map_row("200:2"), input_cursor="c1")
    s1.append_dispatch_row(_dispatch_row("200:2"))
    s1.maybe_checkpoint(force=True)

    s2 = sda.ArtifactCheckpointStore(
        work_dir, config=cfg, source_paths=paths, dictionaries=dicts, source_root=source_tree
    )
    s2.open_or_create()  # resume
    # re-feed same IDs — must not duplicate
    r1 = s2.append_map_row(_map_row("200:1"), input_cursor="c0")
    r2 = s2.append_map_row(_map_row("200:2"), input_cursor="c1")
    assert r1["status"] == "skipped_duplicate"
    assert r2["status"] == "skipped_duplicate"
    r3 = s2.append_map_row(_map_row("200:3"), input_cursor="c2")
    assert r3["status"] == "accepted"
    s2.append_dispatch_row(_dispatch_row("200:3"))
    s2.maybe_checkpoint(force=True)

    state = s2.load_state()
    assert state["counts"]["unique_maps"] == 3
    assert state["counts"]["map_rows"] == 3
    # raw file lines across shards must equal unique count (no dups)
    all_ids: List[str] = []
    for p in sorted(work_dir.glob("map_rows_*.jsonl")):
        for line in p.read_text(encoding="utf-8").splitlines():
            if line.strip():
                all_ids.append(json.loads(line)["map_id"])
    assert all_ids == ["200:1", "200:2", "200:3"]


def test_config_mismatch_fail_closed(work_dir: Path, source_tree: Path):
    paths = [source_tree / "shards" / "part_a.json"]
    dicts = {"hero": source_tree / "dicts" / "hero.json"}
    s1 = sda.ArtifactCheckpointStore(
        work_dir,
        config=_base_config(seed=1),
        source_paths=paths,
        dictionaries=dicts,
        source_root=source_tree,
    )
    s1.open_or_create()
    s1.append_map_row(_map_row("1:1"), input_cursor="c0")
    s1.maybe_checkpoint(force=True)
    state_before = s1.load_state()

    s2 = sda.ArtifactCheckpointStore(
        work_dir,
        config=_base_config(seed=999),  # different identity
        source_paths=paths,
        dictionaries=dicts,
        source_root=source_tree,
    )
    with pytest.raises(sda.IdentityMismatchError) as ei:
        s2.open_or_create()
    msg = str(ei.value)
    assert "config" in msg.lower() or "identity" in msg.lower()
    # old checkpoint preserved
    assert (work_dir / "checkpoint_state.json").exists()
    state_after = json.loads((work_dir / "checkpoint_state.json").read_text(encoding="utf-8"))
    assert state_after["config_hash"] == state_before["config_hash"]
    assert state_after["counts"]["unique_maps"] == 1


def test_source_fingerprint_mismatch_fail_closed(work_dir: Path, source_tree: Path):
    paths = [source_tree / "shards" / "part_a.json"]
    dicts = {"hero": source_tree / "dicts" / "hero.json"}
    s1 = sda.ArtifactCheckpointStore(
        work_dir,
        config=_base_config(),
        source_paths=paths,
        dictionaries=dicts,
        source_root=source_tree,
    )
    s1.open_or_create()
    s1.append_map_row(_map_row("9:1"), input_cursor="c0")
    s1.maybe_checkpoint(force=True)

    # mutate source content
    paths[0].write_text('{"maps":[99]}\n', encoding="utf-8")
    s2 = sda.ArtifactCheckpointStore(
        work_dir,
        config=_base_config(),
        source_paths=paths,
        dictionaries=dicts,
        source_root=source_tree,
    )
    with pytest.raises(sda.IdentityMismatchError):
        s2.open_or_create()
    assert (work_dir / "checkpoint_state.json").exists()


# ---------------------------------------------------------------------------
# Crash safety
# ---------------------------------------------------------------------------


def test_crash_before_replace_preserves_previous_checkpoint(
    work_dir: Path, source_tree: Path, monkeypatch: pytest.MonkeyPatch
):
    paths = [source_tree / "shards" / "part_a.json"]
    dicts = {"hero": source_tree / "dicts" / "hero.json"}
    store = sda.ArtifactCheckpointStore(
        work_dir,
        config=_base_config(checkpoint_every_unique_maps=1),
        source_paths=paths,
        dictionaries=dicts,
        source_root=source_tree,
    )
    store.open_or_create()
    store.append_map_row(_map_row("50:1"), input_cursor="c0")
    store.append_dispatch_row(_dispatch_row("50:1"))
    store.maybe_checkpoint(force=True)
    state_v1 = store.load_state()
    assert state_v1["counts"]["unique_maps"] == 1

    # Next append + checkpoint, but crash on os.replace of state
    store.append_map_row(_map_row("50:2"), input_cursor="c1")
    store.append_dispatch_row(_dispatch_row("50:2"))

    real_replace = os.replace
    replace_calls = {"n": 0}

    def flaky_replace(src, dst):
        replace_calls["n"] += 1
        # Fail when replacing the main state file (last critical step)
        if Path(dst).name == "checkpoint_state.json":
            raise OSError("injected crash before state replace")
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", flaky_replace)
    with pytest.raises(OSError, match="injected crash"):
        store.maybe_checkpoint(force=True)

    # Previous valid state still on disk and loadable
    monkeypatch.setattr(os, "replace", real_replace)
    state = json.loads((work_dir / "checkpoint_state.json").read_text(encoding="utf-8"))
    assert state["counts"]["unique_maps"] == 1
    assert state["last_committed_input_cursor"] == "c0"

    # Resume from previous checkpoint works
    store2 = sda.ArtifactCheckpointStore(
        work_dir,
        config=_base_config(checkpoint_every_unique_maps=1),
        source_paths=paths,
        dictionaries=dicts,
        source_root=source_tree,
    )
    store2.open_or_create()
    st = store2.load_state()
    assert st["counts"]["unique_maps"] == 1
    # re-accept map 50:2
    r = store2.append_map_row(_map_row("50:2"), input_cursor="c1")
    assert r["status"] == "accepted"


def test_atomic_write_uses_tmp_then_replace(work_dir: Path, monkeypatch: pytest.MonkeyPatch):
    target = work_dir / "probe.json"
    seen: List[str] = []
    real_replace = os.replace

    def tracking_replace(src, dst):
        seen.append(f"{Path(src).name}->{Path(dst).name}")
        assert Path(src).name.endswith(".tmp") or str(src).endswith(".tmp")
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", tracking_replace)
    sda.atomic_write_json(target, {"ok": True})
    assert target.exists()
    assert any("probe.json" in s for s in seen)
    assert json.loads(target.read_text(encoding="utf-8"))["ok"] is True


# ---------------------------------------------------------------------------
# Corrupted shard
# ---------------------------------------------------------------------------


def test_corrupted_shard_detected_on_resume(work_dir: Path, source_tree: Path):
    paths = [source_tree / "shards" / "part_a.json"]
    dicts = {"hero": source_tree / "dicts" / "hero.json"}
    store = sda.ArtifactCheckpointStore(
        work_dir,
        config=_base_config(checkpoint_every_unique_maps=1),
        source_paths=paths,
        dictionaries=dicts,
        source_root=source_tree,
    )
    store.open_or_create()
    store.append_map_row(_map_row("70:1"), input_cursor="c0")
    store.append_dispatch_row(_dispatch_row("70:1"))
    store.maybe_checkpoint(force=True)

    # Corrupt a map shard
    shards = sorted(work_dir.glob("map_rows_*.jsonl"))
    assert shards
    shards[0].write_bytes(b"NOT-JSON{{{\n")

    store2 = sda.ArtifactCheckpointStore(
        work_dir,
        config=_base_config(checkpoint_every_unique_maps=1),
        source_paths=paths,
        dictionaries=dicts,
        source_root=source_tree,
    )
    with pytest.raises(sda.CorruptedShardError):
        store2.open_or_create()


# ---------------------------------------------------------------------------
# Compaction / validation / manifest
# ---------------------------------------------------------------------------


def test_compact_and_validate_gzip(work_dir: Path, source_tree: Path):
    paths = [
        source_tree / "shards" / "part_a.json",
        source_tree / "shards" / "part_b.json",
    ]
    dicts = {"hero": source_tree / "dicts" / "hero.json"}
    store = sda.ArtifactCheckpointStore(
        work_dir,
        config=_base_config(checkpoint_every_unique_maps=2),
        source_paths=paths,
        dictionaries=dicts,
        source_root=source_tree,
    )
    store.open_or_create()
    for i in range(4):
        mid = f"300:{i + 1}"
        store.append_map_row(_map_row(mid), input_cursor=f"c{i}")
        store.append_dispatch_row(_dispatch_row(mid))
    store.maybe_checkpoint(force=True)

    dest = work_dir / "compacted"
    dest.mkdir()
    result = sda.compact_shards(
        work_dir,
        destination=dest,
        lifecycle_status="complete",
    )
    map_gz = dest / "map_rows.jsonl.gz"
    disp_gz = dest / "dispatch_rows.jsonl.gz"
    assert map_gz.exists()
    assert disp_gz.exists()
    assert result["counts"]["unique_maps"] == 4
    assert result["counts"]["map_rows"] == 4
    assert result["counts"]["dispatch_rows"] == 4

    # gzip integrity
    with gzip.open(map_gz, "rt", encoding="utf-8") as fh:
        lines = [json.loads(l) for l in fh if l.strip()]
    assert len(lines) == 4
    assert {r["map_id"] for r in lines} == {f"300:{i}" for i in range(1, 5)}

    report = sda.validate_compacted(dest)
    assert report["ok"] is True
    assert report["map_rows"] == 4
    assert report["dispatch_rows"] == 4


def test_validate_rejects_duplicate_map_ids(work_dir: Path):
    dest = work_dir / "bad_compact"
    dest.mkdir()
    rows = [_map_row("1:1"), _map_row("1:1")]  # duplicate
    payload = "\n".join(json.dumps(r, sort_keys=True) for r in rows) + "\n"
    with gzip.open(dest / "map_rows.jsonl.gz", "wt", encoding="utf-8") as fh:
        fh.write(payload)
    with gzip.open(dest / "dispatch_rows.jsonl.gz", "wt", encoding="utf-8") as fh:
        fh.write(json.dumps(_dispatch_row("1:1"), sort_keys=True) + "\n")
    report = sda.validate_compacted(dest)
    assert report["ok"] is False
    assert "duplicate" in report["errors"][0].lower() or "unique" in report["errors"][0].lower()


def test_manifest_completeness(work_dir: Path, source_tree: Path):
    paths = [source_tree / "shards" / "part_a.json"]
    dicts = {"hero": source_tree / "dicts" / "hero.json"}
    store = sda.ArtifactCheckpointStore(
        work_dir,
        config=_base_config(checkpoint_every_unique_maps=2),
        source_paths=paths,
        dictionaries=dicts,
        source_root=source_tree,
    )
    store.open_or_create()
    store.append_map_row(_map_row("400:1"), input_cursor="c0")
    store.append_dispatch_row(_dispatch_row("400:1"))
    store.record_counts(raw_seen=5, duplicate_skipped=2, invalid=1)
    store.maybe_checkpoint(force=True)

    dest = work_dir / "out"
    dest.mkdir()
    sda.compact_shards(work_dir, destination=dest, lifecycle_status="partial")
    manifest = sda.build_manifest(
        work_dir,
        compacted_dir=dest,
        lifecycle_status="partial",
    )
    required_keys = {
        "raw_seen",
        "unique_accepted",
        "duplicate_skipped",
        "invalid",
        "start_ts",
        "end_ts",
        "lifecycle_status",
        "artifact_paths",
        "artifact_sizes",
        "artifact_hashes",
        "config_hash",
        "source_corpus_digest",
        "dictionary_fingerprints",
        "environment",
        "interpreter",
    }
    missing = required_keys - set(manifest.keys())
    assert not missing, f"missing keys: {missing}"
    assert manifest["lifecycle_status"] == "partial"
    assert manifest["unique_accepted"] == 1
    assert manifest["raw_seen"] == 5
    assert manifest["duplicate_skipped"] == 2
    assert manifest["invalid"] == 1
    assert "python" in manifest["interpreter"].lower() or "Python" in manifest["environment"].get(
        "python_version", ""
    )
    # no secrets
    blob = json.dumps(manifest)
    for banned in ("api_key", "password", "token", "secret"):
        assert banned not in blob.lower()

    # atomic write of manifest
    man_path = dest / "manifest.json"
    sda.atomic_write_json(man_path, manifest)
    assert man_path.exists()


def test_old_shards_preserved_not_deleted(work_dir: Path, source_tree: Path):
    paths = [source_tree / "shards" / "part_a.json"]
    dicts = {"hero": source_tree / "dicts" / "hero.json"}
    store = sda.ArtifactCheckpointStore(
        work_dir,
        config=_base_config(checkpoint_every_unique_maps=1),
        source_paths=paths,
        dictionaries=dicts,
        source_root=source_tree,
    )
    store.open_or_create()
    store.append_map_row(_map_row("500:1"), input_cursor="c0")
    store.append_dispatch_row(_dispatch_row("500:1"))
    store.maybe_checkpoint(force=True)
    first_shards = set(p.name for p in work_dir.glob("map_rows_*.jsonl"))
    assert first_shards

    store.append_map_row(_map_row("500:2"), input_cursor="c1")
    store.append_dispatch_row(_dispatch_row("500:2"))
    store.maybe_checkpoint(force=True)
    later = set(p.name for p in work_dir.glob("map_rows_*.jsonl"))
    # all previous shard files still present
    assert first_shards.issubset(later)
    assert len(later) >= len(first_shards)


# ---------------------------------------------------------------------------
# Staging evidence (owned path only)
# ---------------------------------------------------------------------------


def test_write_staging_evidence_pack(source_tree: Path):
    """Produce a small real evidence pack under owned staging/artifacts."""
    STAGING_ARTIFACTS.mkdir(parents=True, exist_ok=True)
    # clean only our subdir contents if re-run; never touch final/
    pack = STAGING_ARTIFACTS / "unit_evidence"
    if pack.exists():
        shutil.rmtree(pack)
    pack.mkdir(parents=True)

    paths = [
        source_tree / "shards" / "part_a.json",
        source_tree / "shards" / "part_b.json",
    ]
    dicts = {"hero": source_tree / "dicts" / "hero.json"}
    store = sda.ArtifactCheckpointStore(
        pack,
        config=_base_config(checkpoint_every_unique_maps=2),
        source_paths=paths,
        dictionaries=dicts,
        source_root=source_tree,
    )
    store.open_or_create()
    for i in range(5):
        mid = f"900:{i + 1}"
        store.append_map_row(_map_row(mid), input_cursor=f"src:{i}")
        store.append_dispatch_row(_dispatch_row(mid))
    store.record_counts(raw_seen=7, duplicate_skipped=1, invalid=1)
    store.maybe_checkpoint(force=True)

    compact_dir = pack / "compacted"
    compact_dir.mkdir()
    sda.compact_shards(pack, destination=compact_dir, lifecycle_status="complete")
    manifest = sda.build_manifest(
        pack, compacted_dir=compact_dir, lifecycle_status="complete"
    )
    sda.atomic_write_json(compact_dir / "manifest.json", manifest)

    assert (compact_dir / "map_rows.jsonl.gz").exists()
    assert (compact_dir / "dispatch_rows.jsonl.gz").exists()
    assert (compact_dir / "manifest.json").exists()
    assert (pack / "checkpoint_state.json").exists()
    report = sda.validate_compacted(compact_dir)
    assert report["ok"] is True
    assert report["map_rows"] == 5
