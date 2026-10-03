"""Pro corpus gzip at merge/read boundaries; fixture capture is in ELO's test."""
import gzip
import json
import sys
from pathlib import Path
from types import ModuleType

import orjson
import pytest

from ELO.tests.test_data_loader_gzip import CAPTURED_CORPUS_JSON

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
# Never read the credential-bearing main-checkout keys module.
if "keys" not in sys.modules:
    keys = ModuleType("keys")
    keys.STRATZ_PROXY_MAP = {}
    keys.start_date_time = 0
    keys.start_date_time_739 = 0
    keys.start_date_time_736 = 0
    # DOTA_PATCH_SPECS uses the module's existing ImportError fallback.
    sys.modules["keys"] = keys

import maps_research as mr


PATCH_SPECS = [("7.39", 1747785600, 1748476800)]


def prepare_merge(root, payload=CAPTURED_CORPUS_JSON):
    temp_dir = root / "temp_files"
    temp_dir.mkdir(parents=True)
    (temp_dir / "captured.txt").write_bytes(payload)
    return root / "json_parts_split_from_object"


def run_merge(root):
    # Public wrapper delegates to the active streaming writer, not legacy flush.
    return mr.merge_temp_files_by_patch(root, patch_specs=PATCH_SPECS)


def test_gzip_merge_has_identical_json_bytes_and_atomic_publication(tmp_path, monkeypatch):
    monkeypatch.delenv("PRO_CORPUS_GZIP", raising=False)
    plain_root = tmp_path / "plain"
    prepare_merge(plain_root)
    plain_path = Path(run_merge(plain_root)[0])
    assert plain_path.name == "7.39_part001.json"
    expected = plain_path.read_bytes()
    assert expected == CAPTURED_CORPUS_JSON

    monkeypatch.setenv("PRO_CORPUS_GZIP", "1")
    gzip_root = tmp_path / "gzip"
    output = prepare_merge(gzip_root)
    published = []
    replace = mr.os.replace

    def observe_replace(src, dst):
        if str(dst).endswith("_part001.json.gz"):
            assert Path(src).name == "7.39_part001.json.gz.tmp"
            assert not Path(dst).exists()
            # Publication must happen after gzip's CRC/footer has been written.
            with gzip.open(src, "rb") as fh:
                assert fh.read() == expected
            published.append(Path(dst))
        replace(src, dst)

    monkeypatch.setattr(mr.os, "replace", observe_replace)
    paths = run_merge(gzip_root)
    assert paths == [str(output / "7.39_part001.json.gz")]
    assert published == [Path(paths[0])]
    with gzip.open(paths[0], "rb") as fh:
        assert fh.read() == expected
    assert not list(output.glob("*.tmp"))
    assert not (output / "7.39_part001.json").exists()


@pytest.mark.parametrize("flag", [None, "0", "true"])
def test_pub_writer_stays_plain_unless_process_explicitly_sets_one(tmp_path, monkeypatch, flag):
    if flag is None:
        monkeypatch.delenv("PRO_CORPUS_GZIP", raising=False)
    else:
        monkeypatch.setenv("PRO_CORPUS_GZIP", flag)
    output = prepare_merge(tmp_path / "bets_data" / "analise_pub_matches")
    paths = run_merge(output.parent)
    assert paths == [str(output / "7.39_part001.json")]
    assert Path(paths[0]).read_bytes() == CAPTURED_CORPUS_JSON
    assert not list(output.glob("*.gz"))


@pytest.mark.parametrize("counter, expected", [(0, 8), (12, 13)])
def test_numbering_counts_both_suffixes_and_persisted_counter(tmp_path, counter, expected):
    (tmp_path / "7.39_part003.json").write_bytes(b"{}")
    with gzip.open(tmp_path / "7.39_part007.json.gz", "wb") as fh:
        fh.write(CAPTURED_CORPUS_JSON)
    # Interrupted publication and unrelated patches are not completed parts.
    (tmp_path / "7.39_part999.json.gz.tmp").write_bytes(b"partial")
    (tmp_path / "7.39b_part999.json.gz").write_bytes(b"unrelated")
    assert mr._next_part_numbers(tmp_path, ["7.39"], {"7.39": counter}) == {"7.39": expected}


@pytest.mark.parametrize("flag, new_suffix", [(None, ".json"), ("1", ".json.gz")])
def test_merge_dedups_gzip_part_without_rewriting_or_colliding(tmp_path, monkeypatch, flag, new_suffix):
    if flag is None:
        monkeypatch.delenv("PRO_CORPUS_GZIP", raising=False)
    else:
        monkeypatch.setenv("PRO_CORPUS_GZIP", flag)
    captured = list(json.loads(CAPTURED_CORPUS_JSON).items())
    output = prepare_merge(tmp_path)
    output.mkdir()
    existing = output / "7.39_part007.json.gz"
    with gzip.open(existing, "wb") as fh:
        fh.write(orjson.dumps(dict(captured[:1])))
    original = existing.read_bytes()
    # No processed_ids.txt: compressed parts must supply the dedup source.
    expected_path = output / ("7.39_part008" + new_suffix)
    assert run_merge(tmp_path) == [str(expected_path)]
    opener = gzip.open if new_suffix.endswith(".gz") else open
    with opener(expected_path, "rb") as fh:
        assert orjson.loads(fh.read()) == dict(captured[1:])
    assert existing.read_bytes() == original
    assert not (output / "7.39_part007.json").exists()
    summary = orjson.loads((output / "merge_patch_summary.json").read_bytes())
    assert summary["duplicates_filtered"] == 1
    assert summary["unique_matches_added"] == 1
    assert run_merge(tmp_path) == []  # Cached scan_manifest retains gzip IDs.
    assert existing.read_bytes() == original


@pytest.mark.parametrize("streaming", [True, False])
def test_dedup_scan_reads_gzip_parts_with_and_without_ijson(tmp_path, monkeypatch, streaming):
    with gzip.open(tmp_path / "7.39_part001.json.gz", "wb") as fh:
        fh.write(CAPTURED_CORPUS_JSON)
    (tmp_path / "7.39_part002.json.gz.tmp").write_bytes(b"not a part")
    if not streaming:
        monkeypatch.setattr(mr, "ijson", None)
    assert mr._scan_output_dir_match_ids(tmp_path) == {8311776153, 8310044973}


def test_teams_match_plain_gzip_and_mixed_corpora(tmp_path):
    plain, compressed = tmp_path / "plain", tmp_path / "compressed"
    plain.mkdir()
    compressed.mkdir()
    (plain / "7.39_part001.json").write_bytes(CAPTURED_CORPUS_JSON)
    with gzip.open(compressed / "7.39_part001.json.gz", "wb") as fh:
        fh.write(CAPTURED_CORPUS_JSON)
    captured = json.loads(CAPTURED_CORPUS_JSON)
    expected = {match[side]["id"] for match in captured.values()
                for side in ("radiantTeam", "direTeam")}
    assert expected
    assert mr._teams_from_corpus(plain) == expected
    assert mr._teams_from_corpus(compressed) == expected
    (compressed / "7.39_part002.json").write_bytes(CAPTURED_CORPUS_JSON)
    assert mr._teams_from_corpus(compressed) == expected


def test_collect_all_maps_reads_compressed_pro_part(tmp_path):
    with gzip.open(tmp_path / "7.39_part001.json.gz", "wb") as fh:
        fh.write(CAPTURED_CORPUS_JSON)
    assert mr.collect_all_maps(tmp_path, output=True) == json.loads(CAPTURED_CORPUS_JSON)


def test_pro_playback_reads_compressed_corpus(tmp_path, monkeypatch):
    corpus = tmp_path / "json_parts_split_from_object"
    corpus.mkdir()
    with gzip.open(corpus / "7.39_part001.json.gz", "wb") as fh:
        fh.write(CAPTURED_CORPUS_JSON)
    monkeypatch.setattr(mr, "PRO_HEROES_DIR", tmp_path)
    monkeypatch.setattr(mr.time, "time", lambda: 1748500000)

    async def capture_ids(*, ids, out_dir):
        return ids

    monkeypatch.setattr(mr, "get_playback_new", capture_ids)
    assert mr.get_pros_playback() == [8311776153, 8310044973]
