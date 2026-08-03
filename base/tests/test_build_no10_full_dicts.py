from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

from base import analise_database as stats
from base.build_no10_full_dicts import (
    build_full_no10_dicts,
    phase_environment,
    select_741_shards,
    validate_sqlite,
)


def _player(hero_id: int, position: int, radiant: bool) -> dict:
    return {
        "heroId": hero_id,
        "position": f"POSITION_{position}",
        "isRadiant": radiant,
        "intentionalFeeding": False,
    }


def _match(*, duration=35, gate_lead=5000, start_time=None, radiant_win=True) -> dict:
    leads = [0] * duration
    if duration > 9:
        leads[9] = gate_lead
    if duration > 19:
        leads[19] = 20_000
    return {
        "id": 1,
        "startDateTime": stats.LATEST_PATCH_START_TS if start_time is None else start_time,
        "didRadiantWin": radiant_win,
        "radiantNetworthLeads": leads,
        "winRates": [0.6 if radiant_win else 0.4],
        "topLaneOutcome": "RADIANT_WIN",
        "midLaneOutcome": "TIE",
        "bottomLaneOutcome": "DIRE_WIN",
        "players": [
            *[_player(position, position, True) for position in range(1, 6)],
            *[_player(position + 5, position, False) for position in range(1, 6)],
        ],
    }


def _sqlite(path: Path, entries: int = 1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE kv (key TEXT PRIMARY KEY, value BLOB)")
        connection.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB)")
        for index in range(entries):
            connection.execute("INSERT INTO kv VALUES (?, ?)", (f"key{index}", b"value"))
        connection.commit()
    finally:
        connection.close()


def test_early_gate_default_on_and_post_lane_gate_default_off(monkeypatch):
    assert stats.ANALISE_EARLY_MINUTE10_GATE_ENABLED is True
    assert stats.ANALISE_POST_LANE_MINUTE10_GATE_ENABLED is False

    early_match = _match(duration=35, gate_lead=5000)
    assert stats.is_early_match(early_match) == (False, None)
    monkeypatch.setattr(stats, "ANALISE_EARLY_MINUTE10_GATE_ENABLED", False)
    assert stats.is_early_match(early_match) == (True, "radiant")

    # Fast-map winner semantics are identical with the switch off.
    fast = _match(duration=34, gate_lead=9000, radiant_win=False)
    assert stats.is_early_match(fast) == (True, "dire")

    post_match = _match(duration=25, gate_lead=5000)
    assert stats.is_post_lane_match(post_match) is True
    monkeypatch.setattr(stats, "ANALISE_POST_LANE_MINUTE10_GATE_ENABLED", True)
    assert stats.is_post_lane_match(post_match) is False
    # Non-gate duration semantics remain enforced.
    assert stats.is_post_lane_match(_match(duration=19, gate_lead=0)) is False


def test_no10_post_lane_still_scopes_solo_to_latest_patch(monkeypatch):
    monkeypatch.setattr(stats, "ANALISE_POST_LANE_MINUTE10_GATE_ENABLED", False)
    old_dict = {}
    stats.analise_database(
        _match(duration=35, gate_lead=5000, start_time=stats.LATEST_PATCH_START_TS - 1),
        {}, {}, {}, post_lane_dict=old_dict,
    )
    assert "1pos1" not in old_dict
    assert old_dict["1pos1_vs_6pos1"]["games"] == 1

    latest_dict = {}
    stats.analise_database(
        _match(duration=35, gate_lead=5000, start_time=stats.LATEST_PATCH_START_TS),
        {}, {}, {}, post_lane_dict=latest_dict,
    )
    assert latest_dict["1pos1"]["games"] == 1


def test_select_shards_includes_only_exact_741_parts(tmp_path):
    included = (
        "7.41_part001.json",
        "7.41a_part002.json",
        "7.41b_part003.json",
        "7.41c_part004.json",
        "7.41d_part005.json",
    )
    excluded = (
        "7.40_part001.json",
        "7.41e_part001.json",
        "7.41d.json",
        "merge_patch_summary.json",
        "7.41d_partABC.json",
    )
    for name in (*included, *excluded):
        (tmp_path / name).write_text("{}", encoding="utf-8")
    assert [path.name for path in select_741_shards(tmp_path)] == sorted(included)


def test_phase_environment_disables_exactly_one_gate(tmp_path):
    common = dict(
        source_view=tmp_path / "view",
        stats_dir=tmp_path / "stats",
        shard_dir=tmp_path / "staging",
        no_test_set_path=tmp_path / "missing.json",
        run_id="run",
    )
    early = phase_environment({}, metric="early", **common)
    assert early["EXPLORE_METRICS"] == "early"
    assert early["ANALISE_EARLY_MINUTE10_GATE_ENABLED"] == "0"
    assert early["ANALISE_POST_LANE_MINUTE10_GATE_ENABLED"] == "1"
    assert early["EXPLORE_KEEP_SHARDS"] == "1"
    assert early["PYTHONUNBUFFERED"] == "1"
    post = phase_environment({}, metric="post_lane", **common)
    assert post["ANALISE_EARLY_MINUTE10_GATE_ENABLED"] == "1"
    assert post["ANALISE_POST_LANE_MINUTE10_GATE_ENABLED"] == "0"


def test_validate_sqlite_requires_quickcheck_and_nonempty(tmp_path):
    valid = tmp_path / "valid.sqlite3"
    _sqlite(valid, entries=2)
    assert validate_sqlite(valid)["entries"] == 2
    empty = tmp_path / "empty.sqlite3"
    _sqlite(empty, entries=0)
    with pytest.raises(RuntimeError, match="empty dictionary"):
        validate_sqlite(empty)


def test_builder_preserves_symlinked_python_and_runs_sequentially(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    for name in ("7.41_part001.json", "7.41a_part001.json", "7.41d_part001.json"):
        (source / name).write_text("{}", encoding="utf-8")
    (source / "unrelated.json").write_text("{}", encoding="utf-8")
    production_dir = tmp_path / "stats"
    production_dir.mkdir()
    current_early = production_dir / "early_dict_raw.sqlite3"
    current_post = production_dir / "post_lane_dict_raw.sqlite3"
    current_early.write_bytes(b"current-early")
    current_post.write_bytes(b"current-post")
    output = production_dir / "no10gate_full_dicts"
    runtime_root = tmp_path / "runtime"
    fake_explore = tmp_path / "explore_database.py"
    fake_explore.write_text("# fake", encoding="utf-8")
    venv_python_link = tmp_path / "venv_python"
    venv_python_link.symlink_to(Path(sys.executable))
    calls = []

    def fake_runner(command, *, cwd, env, check):
        assert check is True
        assert command[0] == str(venv_python_link.absolute())
        metric = env["EXPLORE_METRICS"]
        calls.append(metric)
        view_names = sorted(path.name for path in Path(env["EXPLORE_JSON_DIR"]).iterdir())
        assert view_names == ["7.41_part001.json", "7.41a_part001.json", "7.41d_part001.json"]
        name = "early_dict_raw.sqlite3" if metric == "early" else "post_lane_dict_raw.sqlite3"
        _sqlite(Path(env["EXPLORE_STATS_DIR"]) / name, entries=3)
        return None

    manifest = build_full_no10_dicts(
        source_dir=source,
        output_dir=output,
        runtime_root=runtime_root,
        python_executable=venv_python_link,
        explore_script=fake_explore,
        run_id="test_run",
        runner=fake_runner,
    )

    assert calls == ["early", "post_lane"]
    assert current_early.read_bytes() == b"current-early"
    assert current_post.read_bytes() == b"current-post"
    assert validate_sqlite(output / "early_dict_raw_no10gate.sqlite3")["entries"] == 3
    assert validate_sqlite(output / "post_lane_dict_raw_no10gate.sqlite3")["entries"] == 3
    manifest_path = output / "no10gate_full_dicts_manifest.json"
    assert manifest_path.exists()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["source_shard_count"] == 3
    assert payload["post_lane_solo_scope"] == "existing analise_database semantics: latest patch only"
    assert manifest["metrics_sequential"] == ["early", "post_lane"]
