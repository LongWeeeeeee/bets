"""Salvage merge for already-collected temp files (prod 16-22.09 gap)."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import maps_research
import orjson

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "pub_salvage" / "record_sample.json"

ID_A = 8746279701
ID_B = 8746279702
ID_C = 8746279703  # duplicate: already in processed_ids.txt
ID_D = 8746279704  # duplicate: already in an existing part file
TS_741 = 1774629620  # inside 7.41 [1774310400, 1774656000)


def _record(template, match_id):
    rec = dict(template)
    rec["id"] = match_id
    rec["startDateTime"] = TS_741
    return rec


@pytest.fixture
def salvage_dir(tmp_path, monkeypatch):
    template = json.loads(FIXTURE.read_text())["record"]
    mkdir = tmp_path / "analise_pub_matches"
    out_dir = mkdir / "json_parts_split_from_object"
    out_dir.mkdir(parents=True)
    # Pre-existing corpus state: C in the id cache, D in a part file.
    (out_dir / "processed_ids.txt").write_bytes(orjson.dumps([ID_C]))
    (out_dir / "7.41_part001.json").write_bytes(
        orjson.dumps({str(ID_D): _record(template, ID_D)}))
    temp = {
        "t1": {ID_A: _record(template, ID_A), ID_B: _record(template, ID_B)},
        "t2": {ID_B: _record(template, ID_B), ID_C: _record(template, ID_C)},
        "t3": {ID_D: _record(template, ID_D)},
    }
    for name, payload in temp.items():
        maps_research.save_temp_file(
            {str(k): v for k, v in payload.items()}, str(mkdir), name)
        time.sleep(0.003)  # save_temp_file keys names off wall-clock ms
    assert len(list((mkdir / "temp_files").glob("*.txt"))) == 3
    monkeypatch.setattr(maps_research, "ANALYSE_PUB_DIR", mkdir)
    return mkdir


def _part_records(mkdir):
    found = {}
    for part in sorted((mkdir / "json_parts_split_from_object").glob("7.41_part*.json")):
        if part.name == "7.41_part001.json":
            continue
        found.update(orjson.loads(part.read_bytes()))
    return found


def test_dry_run_writes_nothing(salvage_dir, capsys):
    out = maps_research.salvage_pub_temp_files(dry_run=True)
    assert out == []
    report = capsys.readouterr().out
    assert "3" in report  # files scanned
    out_dir = salvage_dir / "json_parts_split_from_object"
    assert sorted(p.name for p in out_dir.iterdir()) == ["7.41_part001.json", "processed_ids.txt"]
    assert orjson.loads((out_dir / "processed_ids.txt").read_bytes()) == [ID_C]
    assert len(list((salvage_dir / "temp_files").glob("*.txt"))) == 3


def test_real_run_merges_only_unique_new(salvage_dir):
    out = maps_research.salvage_pub_temp_files(dry_run=False)
    assert len(out) == 1 and out[0].endswith("7.41_part002.json")
    new_records = _part_records(salvage_dir)
    assert sorted(int(k) for k in new_records) == [ID_A, ID_B]
    assert new_records[str(ID_A)]["startDateTime"] == TS_741
    processed = set(orjson.loads(
        (salvage_dir / "json_parts_split_from_object" / "processed_ids.txt").read_bytes()))
    assert {ID_A, ID_B, ID_C, ID_D} <= processed
    # The end-of-sweep merge keeps temp files (cleanup=False); salvage matches it.
    assert len(list((salvage_dir / "temp_files").glob("*.txt"))) == 3


TS_OUTSIDE = 1700000000  # Nov 2023: before every patch in DOTA_PATCH_SPECS
ID_OUTSIDE = 8746279710


def _snapshot_tree(root):
    root = Path(root)
    return {p.relative_to(root) for p in root.rglob("*")} if root.exists() else set()


def _write_temp(mkdir, template, payload, name):
    maps_research.save_temp_file(
        {str(k): v for k, v in payload.items()}, str(mkdir), name)
    time.sleep(0.003)  # save_temp_file keys names off wall-clock ms


def test_dry_run_creates_nothing_on_missing_dirs(tmp_path, capsys):
    mkdir = tmp_path / "fresh_analise"
    before = _snapshot_tree(tmp_path)
    out = maps_research.salvage_pub_temp_files(dry_run=True, mkdir=str(mkdir))
    assert out == []
    assert _snapshot_tree(tmp_path) == before
    assert not (mkdir / "temp_files").exists()
    assert not (mkdir / "json_parts_split_from_object").exists()


def test_dry_run_with_clear_output_dir_moves_nothing(salvage_dir):
    out_dir = salvage_dir / "json_parts_split_from_object"
    before = _snapshot_tree(salvage_dir)
    out = maps_research.merge_temp_files_by_patch_streaming(
        mkdir=str(salvage_dir), clear_output_dir=True, dry_run=True)
    assert out == []
    assert _snapshot_tree(salvage_dir) == before
    assert sorted(p.name for p in out_dir.iterdir()) == ["7.41_part001.json", "processed_ids.txt"]


def _dry_run_unique_new(mkdir, capsys):
    out = maps_research.salvage_pub_temp_files(dry_run=True, mkdir=str(mkdir))
    assert out == []
    report = capsys.readouterr().out
    for line in report.splitlines():
        if "уникальных новых:" in line:
            return int(line.split(":")[-1].strip())
    raise AssertionError("dry-run report has no unique-new line:\n" + report)


def test_dry_run_counts_match_real_with_drop_outside_patch(tmp_path, monkeypatch, capsys):
    template = json.loads(FIXTURE.read_text())["record"]
    mkdir = tmp_path / "analise_pub_matches"
    (mkdir / "temp_files").mkdir(parents=True)
    outside_rec = _record(template, ID_OUTSIDE)
    outside_rec["startDateTime"] = TS_OUTSIDE
    _write_temp(mkdir, template,
                {ID_A: _record(template, ID_A), ID_OUTSIDE: outside_rec}, "t1")
    monkeypatch.setattr(maps_research, "ANALYSE_PUB_DIR", mkdir)
    monkeypatch.setattr(maps_research, "MERGE_DROP_OUTSIDE_PATCH", True)
    assert _dry_run_unique_new(mkdir, capsys) == 1
    out = maps_research.salvage_pub_temp_files(dry_run=False, mkdir=str(mkdir))
    assert len(out) == 1
    summary = orjson.loads(
        (mkdir / "json_parts_split_from_object" / "merge_patch_summary.json").read_bytes())
    assert summary["unique_matches_added"] == 1
    assert summary["outside_patch_skipped"] == 1
    assert summary["broken_files_skipped"] == 0


def test_corrupt_temp_file_is_incomplete_but_merges_good(salvage_dir):
    corrupt = salvage_dir / "temp_files" / "zzz_corrupt.txt"
    corrupt.write_bytes(b"{this is not json, recovery impossible!!!")
    with pytest.raises(maps_research.MergeIncompleteError) as exc:
        maps_research.salvage_pub_temp_files(dry_run=False, mkdir=str(salvage_dir))
    assert "zzz_corrupt.txt" in exc.value.broken_files
    new_records = _part_records(salvage_dir)
    assert sorted(int(k) for k in new_records) == [ID_A, ID_B]
    summary = orjson.loads(
        (salvage_dir / "json_parts_split_from_object" / "merge_patch_summary.json").read_bytes())
    assert summary["broken_files_skipped"] == 1
    assert "zzz_corrupt.txt" in summary["broken_files"]


def test_dry_run_with_corrupt_file_also_raises(salvage_dir):
    (salvage_dir / "temp_files" / "zzz_corrupt.txt").write_bytes(b"\x00\x01binary\xff")
    with pytest.raises(maps_research.MergeIncompleteError):
        maps_research.salvage_pub_temp_files(dry_run=True, mkdir=str(salvage_dir))


def test_interrupted_publish_keeps_originals(salvage_dir, monkeypatch):
    import os as _os
    out_dir = salvage_dir / "json_parts_split_from_object"
    part_before = (out_dir / "7.41_part001.json").read_bytes()
    ids_before = (out_dir / "processed_ids.txt").read_bytes()
    real_replace = _os.replace

    def _boom(src, dst):
        if Path(dst).name == "processed_ids.txt":
            raise OSError("injected mid-publish failure")
        return real_replace(src, dst)

    monkeypatch.setattr(_os, "replace", _boom)
    with pytest.raises(OSError, match="injected mid-publish failure"):
        maps_research.salvage_pub_temp_files(dry_run=False, mkdir=str(salvage_dir))
    assert (out_dir / "7.41_part001.json").read_bytes() == part_before
    assert (out_dir / "processed_ids.txt").read_bytes() == ids_before


def test_cleanup_keeps_temp_folder_when_a_file_is_broken(salvage_dir):
    temp_dir = salvage_dir / "temp_files"
    (temp_dir / "zzz_corrupt.txt").write_bytes(b"{truncated")
    with pytest.raises(maps_research.MergeIncompleteError):
        maps_research.merge_temp_files_by_patch_streaming(
            str(salvage_dir), cleanup=True)
    # The broken file is the only copy of its matches: it must survive cleanup.
    assert (temp_dir / "zzz_corrupt.txt").exists()
    assert sorted(int(k) for k in _part_records(salvage_dir)) == [ID_A, ID_B]


def test_temp_write_failure_leaves_no_truncated_temp_file(salvage_dir):
    template = json.loads(FIXTURE.read_text())["record"]
    # json encoding fails on the second record, after the first one was serialised.
    bad = {str(ID_A): _record(template, ID_A), "8746279799": object()}
    with pytest.raises(TypeError):
        maps_research.save_temp_file(bad, str(salvage_dir), "crash")
    names = sorted(p.name for p in (salvage_dir / "temp_files").iterdir())
    assert not [n for n in names if n.startswith("crash_")]
    # The next merge sees only the three intact temp files and completes.
    maps_research.salvage_pub_temp_files(dry_run=False, mkdir=str(salvage_dir))
    assert sorted(int(k) for k in _part_records(salvage_dir)) == [ID_A, ID_B]
