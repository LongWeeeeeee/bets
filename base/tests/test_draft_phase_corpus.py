import json
import os
from pathlib import Path

import numpy as np
import pytest

from base import build_draft_phase_corpus as corpus


def _players(*, duplicate_position=False, duplicate_hero=False):
    players = []
    for radiant in (True, False):
        for position in range(1, 6):
            hero = position if radiant else position + 5
            if duplicate_hero and not radiant and position == 5:
                hero = 1
            players.append({"isRadiant": radiant, "position": f"POSITION_{1 if duplicate_position and position == 2 else position}", "heroId": hero})
    return players


def _match(mid=1, *, duration=1200, ts=1, leads=None, **overrides):
    raw = {
        "id": mid,
        "startDateTime": ts,
        "durationSeconds": duration,
        "didRadiantWin": True,
        "players": _players(),
        "radiantNetworthLeads": [0] * min(28, duration // 60) if leads is None else leads,
    }
    raw.update(overrides)
    return raw


def _write(path: Path, payload):
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_phase_labels_duration_boundaries_and_unusable_timeline():
    positive = _match(1, leads=[0] * 19 + [6000])
    row, reason = corpus.canonicalize_match(positive, "1")
    assert reason is None
    assert row is not None and (row.early_nw, row.marker_minute) == (1, 20)

    for duration in (20 * 60, 34 * 60, 36 * 60):
        row, reason = corpus.canonicalize_match(_match(duration, duration=duration), str(duration))
        assert reason is None
        assert row is not None and (row.early_nw, row.marker_minute) == (2, -1)

    row, reason = corpus.canonicalize_match(_match(99, duration=1199), "99")
    assert row is None and reason == "invalid_duration_seconds"
    row, _ = corpus.canonicalize_match(_match(100, leads=None, radiantNetworthLeads=None), "100")
    assert row is not None and row.early_nw == -1
    row, _ = corpus.canonicalize_match(_match(101, leads=[0] * 19), "101")
    assert row is not None and row.early_nw == -1
    row, _ = corpus.canonicalize_match(_match(102, leads=[0] * 19 + [float("inf")]), "102")
    assert row is not None and row.early_nw == -1
    delayed = [0] * 24
    delayed[20], delayed[23] = None, -7000
    row, _ = corpus.canonicalize_match(_match(103, duration=1500, leads=delayed), "103")
    assert row is not None and row.early_nw == -1
    row, _ = corpus.canonicalize_match(_match(104, duration=1200, leads=[0] * 24 + [10000]), "104")
    assert row is not None and (row.early_nw, row.marker_minute) == (2, -1)


def test_strict_draft_contract_rejects_duplicate_slots_heroes_and_outcome():
    row, reason = corpus.canonicalize_match(_match(players=_players(duplicate_position=True)), "1")
    assert row is None and reason == "duplicate_position"
    row, reason = corpus.canonicalize_match(_match(players=_players(duplicate_hero=True)), "1")
    assert row is None and reason == "duplicate_hero"
    row, reason = corpus.canonicalize_match(_match(didRadiantWin=1), "1")
    assert row is None and reason == "invalid_outcome"


def test_build_deduplicates_conflicts_skips_metadata_and_uses_int32_heroes(tmp_path: Path):
    source, output = tmp_path / "raw", tmp_path / "out"
    source.mkdir()
    _write(source / "a.json", {"1": _match(1, ts=10), "2": _match(2, ts=20)})
    # An identical copy is harmless; incompatible copies reject the id entirely.
    _write(source / "b.json", {"1": _match(1, ts=10), "2": _match(2, ts=21), "3": _match(3, ts=5)})
    _write(source / "merge_patch_summary.json", {"not": "a match archive"})
    _write(source / "scan_manifest.json", {"not": "a match archive"})

    manifest = corpus.build_corpus(source, output, workers=2)
    with np.load(output / "rows.npz", allow_pickle=False) as rows:
        assert rows["mid"].tolist() == [3, 1]
        assert rows["heroes"].dtype == np.int32
        assert rows["early_nw"].tolist() == [2, 2]
    assert manifest["global_counts"]["duplicate_identical"] == 1
    assert manifest["global_counts"]["conflicting_map_ids_rejected"] == 1
    assert manifest["skipped_metadata"] == ["merge_patch_summary.json", "scan_manifest.json"]


def test_dedup_rejects_a_b_a_conflict_across_three_shards(tmp_path: Path):
    source, output = tmp_path / "raw", tmp_path / "out"
    source.mkdir()
    _write(source / "a.json", {"7": _match(7, ts=7)})
    _write(source / "b.json", {"7": _match(7, ts=8)})
    _write(source / "c.json", {"7": _match(7, ts=7), "8": _match(8, ts=1)})
    manifest = corpus.build_corpus(source, output, workers=2)
    with np.load(output / "rows.npz", allow_pickle=False) as rows:
        assert rows["mid"].tolist() == [8]
    assert manifest["global_counts"]["conflicting_map_ids_rejected"] == 1


def test_stale_cache_is_rebuilt_and_source_error_does_not_replace_rows(tmp_path: Path):
    source, output = tmp_path / "raw", tmp_path / "out"
    source.mkdir()
    input_file = source / "part.json"
    _write(input_file, {"1": _match(1, ts=1)})
    first = corpus.build_corpus(source, output, workers=1)
    assert first["sources"][0]["cache"] == "rebuilt"
    second = corpus.build_corpus(source, output, workers=1)
    assert second["sources"][0]["cache"] == "hit"

    original_stat = input_file.stat()
    _write(input_file, {"2": _match(2, ts=2)})
    # A changed payload can preserve both size and mtime; SHA must still bust cache.
    os.utime(input_file, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
    third = corpus.build_corpus(source, output, workers=1)
    assert third["sources"][0]["cache"] == "rebuilt"
    with np.load(output / "rows.npz", allow_pickle=False) as rows:
        assert rows["mid"].tolist() == [2]
    before_failure = (output / "rows.npz").read_bytes()
    input_file.write_text("{", encoding="utf-8")
    with pytest.raises(corpus.SourceBuildError):
        corpus.build_corpus(source, output, workers=1)
    assert (output / "rows.npz").read_bytes() == before_failure
    assert json.loads((output / "manifest.json").read_text())["complete"] is False


def test_source_changed_while_streaming_is_not_cached(tmp_path: Path, monkeypatch):
    source, output = tmp_path / "raw", tmp_path / "out"
    source.mkdir()
    input_file = source / "part.json"
    _write(input_file, {"1": _match(1)})
    original = corpus.iter_json_objects

    def mutate_after_read(path):
        yield from original(path)
        stat = path.stat()
        os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))

    monkeypatch.setattr(corpus, "iter_json_objects", mutate_after_read)
    with pytest.raises(corpus.SourceBuildError, match="changed during parse"):
        corpus.build_corpus(source, output, workers=1)
    assert not list((output / "cache").glob("*.npz"))
    assert json.loads((output / "manifest.json").read_text())["complete"] is False


def test_chronological_split_does_not_split_timestamp_ties():
    assert corpus.chronological_split_indices(np.array([1, 2, 2, 2, 3], dtype=np.int64)) == (4, 4)
