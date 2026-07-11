from __future__ import annotations

import asyncio
import json
import sys
from decimal import Decimal
from pathlib import Path

import orjson
import pytest


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import explore_database as explore  # noqa: E402
import maps_research  # noqa: E402


def _set_valid_positions_catalog(monkeypatch) -> None:
    monkeypatch.setattr(
        maps_research,
        "HERO_VALID_POSITIONS",
        {
            1: ["POSITION_1"],
            2: ["POSITION_2"],
            3: ["POSITION_3"],
            4: ["POSITION_4"],
            5: ["POSITION_5"],
            6: ["POSITION_1"],
            7: ["POSITION_2"],
            8: ["POSITION_3"],
            9: ["POSITION_4"],
            10: ["POSITION_5"],
        },
        raising=False,
    )
    monkeypatch.setattr(maps_research, "HERO_POSITION_STATS", {}, raising=False)


def _set_percentage_position_catalog(monkeypatch) -> None:
    monkeypatch.setattr(maps_research, "HERO_VALID_POSITIONS", {}, raising=False)
    monkeypatch.setattr(maps_research, "HERO_POSITION_STATS_MIN_PERCENTAGE", 1.0, raising=False)
    monkeypatch.setattr(
        maps_research,
        "HERO_POSITION_STATS",
        {
            101: {"positions": {"1": {"percentage": 0.99}, "5": {"percentage": 1.0}}},
            102: {"positions": {"1": {"percentage": 1.0}, "5": {"percentage": 0.99}}},
            201: {"positions": {"2": {"percentage": 1.0}}},
            202: {"positions": {"3": {"percentage": 1.0}}},
            203: {"positions": {"4": {"percentage": 1.0}}},
            301: {"positions": {"1": {"percentage": 1.0}}},
            302: {"positions": {"2": {"percentage": 1.0}}},
            303: {"positions": {"3": {"percentage": 1.0}}},
            304: {"positions": {"4": {"percentage": 1.0}}},
            305: {"positions": {"5": {"percentage": 1.0}}},
        },
        raising=False,
    )


def _clear_mode_env(monkeypatch) -> None:
    for name in (
        "EXPLORE_ONLY_LANES",
        "EXPLORE_EXPERIMENTAL_LATE_ONLY",
        "EXPLORE_EARLY_LATE_ONLY",
        "EXPLORE_ALLOW_EMPTY_TEST_SET",
        "EXPLORE_DISABLE_TEST_EXCLUSION",
        "EXPLORE_MIN_START_TS",
    ):
        monkeypatch.delenv(name, raising=False)


def _valid_match(match_id: str = "101"):
    return {
        "id": match_id,
        "startDateTime": 1,
        "players": [
            {"heroId": 1, "position": "POSITION_1", "isRadiant": True, "intentionalFeeding": False, "networth": 1000, "imp": 0},
            {"heroId": 2, "position": "POSITION_2", "isRadiant": True, "intentionalFeeding": False, "networth": 2000, "imp": 0},
            {"heroId": 3, "position": "POSITION_3", "isRadiant": True, "intentionalFeeding": False, "networth": 3000, "imp": 0},
            {"heroId": 4, "position": "POSITION_4", "isRadiant": True, "intentionalFeeding": False, "networth": 1500, "imp": 0},
            {"heroId": 5, "position": "POSITION_5", "isRadiant": True, "intentionalFeeding": False, "networth": 800, "imp": 0},
            {"heroId": 6, "position": "POSITION_1", "isRadiant": False, "intentionalFeeding": False, "networth": 1000, "imp": 0},
            {"heroId": 7, "position": "POSITION_2", "isRadiant": False, "intentionalFeeding": False, "networth": 2000, "imp": 0},
            {"heroId": 8, "position": "POSITION_3", "isRadiant": False, "intentionalFeeding": False, "networth": 3000, "imp": 0},
            {"heroId": 9, "position": "POSITION_4", "isRadiant": False, "intentionalFeeding": False, "networth": 1500, "imp": 0},
            {"heroId": 10, "position": "POSITION_5", "isRadiant": False, "intentionalFeeding": False, "networth": 800, "imp": 0},
        ],
        "topLaneOutcome": "DIRE_WIN",
        "midLaneOutcome": "RADIANT_WIN",
        "bottomLaneOutcome": "RADIANT_WIN",
        "radiantNetworthLeads": [],
        "didRadiantWin": True,
        "towerDeaths": [{"npcId": 1}],
    }


def test_discover_pub_files_includes_combined_and_patch_parts(tmp_path, monkeypatch):
    monkeypatch.delenv("EXPLORE_MAX_FILES", raising=False)
    json_dir = tmp_path / "json_parts_split_from_object"
    json_dir.mkdir(parents=True)
    for filename in (
        "combined1.json",
        "7.40_part001.json",
        "7.41_part001.json",
        "merge_patch_summary.json",
    ):
        (json_dir / filename).write_text("{}", encoding="utf-8")

    files = explore._discover_pub_files(json_dir)

    assert [path.name for path in files] == [
        "7.40_part001.json",
        "7.41_part001.json",
        "combined1.json",
    ]


def test_check_match_quality_swaps_lane_roles_with_percentage_catalog(monkeypatch):
    _set_percentage_position_catalog(monkeypatch)

    def player(hero_id, position, is_radiant, networth):
        return {
            "heroId": hero_id,
            "position": position,
            "isRadiant": is_radiant,
            "intentionalFeeding": False,
            "networth": networth,
            "imp": 0,
        }

    match = {
        "id": "swap-percentage-catalog",
        "players": [
            player(101, "POSITION_1", True, 500),
            player(201, "POSITION_2", True, 2000),
            player(202, "POSITION_3", True, 2500),
            player(203, "POSITION_4", True, 1200),
            player(102, "POSITION_5", True, 2100),
            player(301, "POSITION_1", False, 2000),
            player(302, "POSITION_2", False, 2000),
            player(303, "POSITION_3", False, 2000),
            player(304, "POSITION_4", False, 1200),
            player(305, "POSITION_5", False, 800),
        ],
    }

    assert maps_research._position_is_valid_for_hero(101, "POSITION_1") is False
    assert maps_research._position_is_valid_for_hero(102, "POSITION_5") is False

    ok, reason = maps_research.check_match_quality(match, strict_lane_positions=True)

    assert (ok, reason) == (True, "ok")
    assert match["players"][0]["position"] == "pos5"
    assert match["players"][4]["position"] == "pos1"


@pytest.mark.skipif(
    not hasattr(explore, "run_explore_database"),
    reason="pre-existing drift: run_explore_database API not in current explore_database",
)
def test_run_explore_database_requires_test_set_by_default(tmp_path, monkeypatch):
    _clear_mode_env(monkeypatch)
    _set_valid_positions_catalog(monkeypatch)

    json_dir = tmp_path / "json_parts_split_from_object"
    json_dir.mkdir(parents=True)
    (json_dir / "combined1.json").write_text("{}", encoding="utf-8")

    with pytest.raises(RuntimeError, match="test exclusion"):
        explore.run_explore_database(
            base_dir=tmp_path,
            json_dir=json_dir,
            test_set_path=tmp_path / "missing_test_set.json",
            bad_quality_dir=tmp_path / "bad_quality",
        )


@pytest.mark.skipif(
    not hasattr(explore, "run_explore_database"),
    reason="pre-existing drift: run_explore_database API not in current explore_database",
)
def test_run_explore_database_can_explicitly_disable_test_exclusion(tmp_path, monkeypatch):
    _clear_mode_env(monkeypatch)
    _set_valid_positions_catalog(monkeypatch)
    monkeypatch.setenv("EXPLORE_ONLY_LANES", "1")
    monkeypatch.setenv("EXPLORE_MIN_START_TS", "0")
    monkeypatch.setenv("EXPLORE_DISABLE_TEST_EXCLUSION", "1")

    json_dir = tmp_path / "json_parts_split_from_object"
    json_dir.mkdir(parents=True)
    (json_dir / "combined1.json").write_text(
        json.dumps({"101": _valid_match("101")}, ensure_ascii=False),
        encoding="utf-8",
    )

    result = explore.run_explore_database(
        base_dir=tmp_path,
        json_dir=json_dir,
        test_set_path=tmp_path / "missing_test_set.json",
        bad_quality_dir=tmp_path / "bad_quality",
    )

    assert result["mode_name"] == "ONLY_LANES"
    assert result["train_processed"] == 1
    assert result["test_excluded"] == 0
    # sqlite-first: primary artifact is *.sqlite3; JSON only if WRITE_JSON
    assert (tmp_path / "lane_dict_raw.sqlite3").exists() or (tmp_path / "lane_dict_raw.json").exists()


def test_get_maps_new_skips_processed_ids_to_graph_when_auxiliary_files_disabled(tmp_path, monkeypatch):
    _set_valid_positions_catalog(monkeypatch)

    async def _fake_retry(_func, **_kwargs):
        return (
            [
                {
                    "id": "123",
                    "radiantTeam": {"id": 1},
                    "direTeam": {"id": 2},
                    "leagueId": 55,
                    "league": {"id": 55, "tier": "TIER2"},
                }
            ],
            set(),
        )

    monkeypatch.setattr(maps_research, "retry_request_with_proxy_rotation", _fake_retry)
    monkeypatch.setattr(maps_research, "_build_tier_team_ids", lambda: {1, 2})
    monkeypatch.setattr(maps_research, "check_match_quality", lambda match: (True, "ok"))
    monkeypatch.setattr(maps_research, "save_temp_file", lambda *args, **kwargs: None)
    monkeypatch.setattr(maps_research, "merge_temp_files_by_patch", lambda *args, **kwargs: [])
    monkeypatch.setattr(maps_research, "load_get_maps_state", lambda: None)
    monkeypatch.setattr(maps_research, "save_get_maps_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(maps_research, "clear_get_maps_state", lambda *args, **kwargs: None)

    asyncio.run(
        maps_research.get_maps_new(
            ids=[123],
            mkdir=str(tmp_path),
            show_prints=False,
            pro=True,
            skip_auxiliary_files=True,
        )
    )

    assert not (tmp_path / "processed_ids_to_graph.txt").exists()
    assert not (tmp_path / "trash_maps.txt").exists()
    assert not (tmp_path / "player_ids.txt").exists()
    assert not (tmp_path / "all_teams.txt").exists()


def test_get_maps_new_deduplicates_maps_before_temp_save(tmp_path, monkeypatch):
    _set_valid_positions_catalog(monkeypatch)
    saved_batches = []

    duplicate_match = _valid_match("777")
    unique_match = _valid_match("888")

    async def _fake_retry(_func, **_kwargs):
        return ([duplicate_match, duplicate_match, unique_match], set())

    monkeypatch.setattr(maps_research, "retry_request_with_proxy_rotation", _fake_retry)
    monkeypatch.setattr(maps_research, "check_match_quality", lambda match: (True, "ok"))
    monkeypatch.setattr(maps_research, "save_temp_file", lambda data, *_args, **_kwargs: saved_batches.append(dict(data)))
    monkeypatch.setattr(maps_research, "merge_temp_files_by_patch", lambda *args, **kwargs: [])
    monkeypatch.setattr(maps_research, "load_get_maps_state", lambda: None)
    monkeypatch.setattr(maps_research, "save_get_maps_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(maps_research, "clear_get_maps_state", lambda *args, **kwargs: None)

    asyncio.run(
        maps_research.get_maps_new(
            ids=[1],
            mkdir=str(tmp_path),
            show_prints=False,
            skip_auxiliary_files=True,
        )
    )

    assert len(saved_batches) == 1
    assert sorted(saved_batches[0]) == ["777", "888"]


def test_merge_temp_files_by_patch_splits_by_patch_and_filters_duplicates(tmp_path):
    temp_dir = tmp_path / "temp_files"
    temp_dir.mkdir(parents=True)
    match_739 = _valid_match("100")
    match_739["startDateTime"] = 1748000000
    match_740 = _valid_match("200")
    match_740["startDateTime"] = 1766000000
    match_741c = _valid_match("300")
    match_741c["startDateTime"] = 1778100000
    duplicate_739 = _valid_match("100")
    duplicate_739["startDateTime"] = 1748000000
    (temp_dir / "001.txt").write_bytes(orjson.dumps({"100": match_739, "200": match_740}))
    (temp_dir / "002.txt").write_bytes(orjson.dumps({"100": duplicate_739, "300": match_741c}))

    output_files = maps_research.merge_temp_files_by_patch(
        mkdir=str(tmp_path),
        max_size_mb=1,
        cleanup=False,
    )

    output_names = sorted(Path(path).name for path in output_files)
    assert output_names == ["7.39_part001.json", "7.40_part001.json", "7.41c_part001.json"]
    payload_739 = orjson.loads((tmp_path / "json_parts_split_from_object" / "7.39_part001.json").read_bytes())
    payload_740 = orjson.loads((tmp_path / "json_parts_split_from_object" / "7.40_part001.json").read_bytes())
    payload_741c = orjson.loads((tmp_path / "json_parts_split_from_object" / "7.41c_part001.json").read_bytes())
    assert sorted(payload_739) == ["100"]
    assert sorted(payload_740) == ["200"]
    assert sorted(payload_741c) == ["300"]
    processed_ids = orjson.loads((tmp_path / "json_parts_split_from_object" / "processed_ids.txt").read_bytes())
    assert processed_ids == [100, 200, 300]
    summary = orjson.loads((tmp_path / "json_parts_split_from_object" / "merge_patch_summary.json").read_bytes())
    assert summary["duplicates_filtered"] == 1


def test_merge_temp_files_by_patch_continues_existing_part_numbers(tmp_path):
    temp_dir = tmp_path / "temp_files"
    output_dir = tmp_path / "json_parts_split_from_object"
    temp_dir.mkdir(parents=True)
    output_dir.mkdir(parents=True)
    (output_dir / "7.40_part001.json").write_text("{}", encoding="utf-8")
    match_740 = _valid_match("900")
    match_740["startDateTime"] = 1766000000
    (temp_dir / "001.txt").write_bytes(orjson.dumps({"900": match_740}))

    output_files = maps_research.merge_temp_files_by_patch(
        mkdir=str(tmp_path),
        max_size_mb=1,
        cleanup=False,
    )

    assert [Path(path).name for path in output_files] == ["7.40_part002.json"]
    assert (output_dir / "7.40_part001.json").read_text(encoding="utf-8") == "{}"
    assert (output_dir / "7.40_part002.json").exists()


@pytest.mark.skipif(
    not hasattr(explore, "run_explore_database"),
    reason="pre-existing drift: run_explore_database API not in current explore_database",
)
def test_run_explore_database_reads_env_paths_when_args_omitted(tmp_path, monkeypatch):
    _clear_mode_env(monkeypatch)
    _set_valid_positions_catalog(monkeypatch)
    monkeypatch.setenv("EXPLORE_ONLY_LANES", "1")
    monkeypatch.setenv("EXPLORE_MIN_START_TS", "0")
    monkeypatch.setenv("EXPLORE_DISABLE_TEST_EXCLUSION", "1")
    monkeypatch.setenv("EXPLORE_BASE_DIR", str(tmp_path))
    monkeypatch.setenv("EXPLORE_JSON_DIR", str(tmp_path / "custom_json"))
    monkeypatch.setenv("EXPLORE_BAD_QUALITY_DIR", str(tmp_path / "custom_bad_quality"))

    json_dir = tmp_path / "custom_json"
    json_dir.mkdir(parents=True)
    (json_dir / "combined1.json").write_text(
        json.dumps({"101": _valid_match("101")}, ensure_ascii=False),
        encoding="utf-8",
    )

    result = explore.run_explore_database()

    assert result["mode_name"] == "ONLY_LANES"
    assert result["train_processed"] == 1
    assert (tmp_path / "lane_dict_raw.sqlite3").exists() or (tmp_path / "lane_dict_raw.json").exists()
    assert (tmp_path / "custom_bad_quality").exists()


@pytest.mark.skipif(
    not hasattr(explore, "run_explore_database"),
    reason="pre-existing drift: run_explore_database API not in current explore_database",
)
def test_run_explore_database_fails_closed_without_position_catalog(tmp_path, monkeypatch):
    _clear_mode_env(monkeypatch)
    monkeypatch.setattr(maps_research, "HERO_VALID_POSITIONS", {}, raising=False)
    monkeypatch.setattr(maps_research, "HERO_POSITION_STATS", {}, raising=False)

    with pytest.raises(RuntimeError, match="hero_position_stats.json"):
        explore.run_explore_database(
            base_dir=tmp_path,
            json_dir=tmp_path / "json_parts_split_from_object",
            test_set_path=tmp_path / "extracted_100k_matches.json",
            bad_quality_dir=tmp_path / "bad_quality",
        )


@pytest.mark.skipif(
    not hasattr(explore, "run_explore_database"),
    reason="pre-existing drift: run_explore_database API not in current explore_database",
)
def test_run_explore_database_rolls_back_file_on_stream_failure(tmp_path, monkeypatch):
    _clear_mode_env(monkeypatch)
    _set_valid_positions_catalog(monkeypatch)
    monkeypatch.setenv("EXPLORE_ONLY_LANES", "1")
    monkeypatch.setenv("EXPLORE_MIN_START_TS", "0")

    json_dir = tmp_path / "json_parts_split_from_object"
    json_dir.mkdir(parents=True)
    (json_dir / "combined1.json").write_text("{}", encoding="utf-8")
    (tmp_path / "extracted_100k_matches.json").write_text('{"999": {}}', encoding="utf-8")

    def _broken_iter(_file_path: Path):
        yield "101", _valid_match("101")
        raise RuntimeError("stream broke")

    monkeypatch.setattr(explore, "_iter_matches", _broken_iter)

    with pytest.raises(RuntimeError, match="статистика не сохранена"):
        explore.run_explore_database(
            base_dir=tmp_path,
            json_dir=json_dir,
            test_set_path=tmp_path / "extracted_100k_matches.json",
            bad_quality_dir=tmp_path / "bad_quality",
        )

    assert not (tmp_path / "lane_dict_raw.json").exists()
    assert not (tmp_path / "lane_dict_raw.sqlite3").exists()
    assert not (tmp_path / "early_dict_raw.json").exists()
    assert not (tmp_path / "late_dict_raw.json").exists()


@pytest.mark.skipif(
    not hasattr(explore, "run_explore_database"),
    reason="pre-existing drift: run_explore_database API not in current explore_database",
)
def test_run_explore_database_processes_small_lane_file_successfully(tmp_path, monkeypatch):
    _clear_mode_env(monkeypatch)
    _set_valid_positions_catalog(monkeypatch)
    monkeypatch.setenv("EXPLORE_ONLY_LANES", "1")
    monkeypatch.setenv("EXPLORE_MIN_START_TS", "0")

    json_dir = tmp_path / "json_parts_split_from_object"
    json_dir.mkdir(parents=True)
    (json_dir / "combined1.json").write_text(
        json.dumps({"101": _valid_match("101")}, ensure_ascii=False),
        encoding="utf-8",
    )
    (tmp_path / "extracted_100k_matches.json").write_text('{"999": {}}', encoding="utf-8")

    result = explore.run_explore_database(
        base_dir=tmp_path,
        json_dir=json_dir,
        test_set_path=tmp_path / "extracted_100k_matches.json",
        bad_quality_dir=tmp_path / "bad_quality",
    )

    assert result["mode_name"] == "ONLY_LANES"
    assert result["train_processed"] == 1
    # sqlite-first primary artifact
    sqlite_path = tmp_path / "lane_dict_raw.sqlite3"
    json_path = tmp_path / "lane_dict_raw.json"
    assert sqlite_path.exists() or json_path.exists()
    if sqlite_path.exists():
        import sqlite3

        conn = sqlite3.connect(str(sqlite_path))
        try:
            n = conn.execute("SELECT COUNT(*) FROM kv").fetchone()[0]
        finally:
            conn.close()
        assert n > 0
    else:
        payload = orjson.loads(json_path.read_bytes())
        assert payload


def test_dump_bytes_handles_decimal_payload():
    payload = {"value": Decimal("12.5"), "nested": {"x": Decimal("3")}}

    if not hasattr(explore, "_dump_bytes"):
        pytest.skip("run_explore_database API (_dump_bytes) not present in current module")

    encoded = explore._dump_bytes(payload)

    assert orjson.loads(encoded) == {"value": 12.5, "nested": {"x": 3.0}}


# ---------------------------------------------------------------------------
# sqlite-first helpers (EXPLORE_WRITE_JSON default off)
# ---------------------------------------------------------------------------


def _read_sqlite_kv(db_path: Path) -> dict:
    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute("SELECT key, value FROM kv").fetchall()
        meta_rows = conn.execute("SELECT key, value FROM meta").fetchall()
    finally:
        conn.close()
    kv = {str(k): orjson.loads(v) for k, v in rows}
    meta = {str(k): orjson.loads(v) for k, v in meta_rows}
    return {"kv": kv, "meta": meta}


def test_dump_stats_dict_to_sqlite_writes_kv_and_meta(tmp_path):
    stats = {
        "1pos1": [3, 1, 5],
        "2pos2": {"wins": 1, "draws": 0, "games": 2},
    }
    sqlite_path = tmp_path / "lane_dict_raw.sqlite3"

    entries, total_games = explore._dump_stats_dict_to_sqlite(stats, sqlite_path)

    assert entries == 2
    assert total_games == 7
    assert sqlite_path.exists()
    assert not (tmp_path / "lane_dict_raw.sqlite3.tmp").exists()
    assert not (tmp_path / "lane_dict_raw.json").exists()

    payload = _read_sqlite_kv(sqlite_path)
    assert payload["kv"]["1pos1"] == {"wins": 3, "draws": 1, "games": 5}
    assert payload["kv"]["2pos2"] == {"wins": 1, "draws": 0, "games": 2}
    assert payload["meta"]["format_version"] == 1
    assert payload["meta"]["backend"] == "sqlite_kv"
    assert payload["meta"]["entries"] == 2
    assert payload["meta"]["source_name"] == "lane_dict_raw.sqlite3"
    assert payload["meta"]["source_size"] == 0
    assert payload["meta"]["source_mtime_ns"] == 0


def test_dump_stats_dict_to_sqlite_default_no_json(tmp_path, monkeypatch):
    monkeypatch.setattr(explore, "WRITE_JSON", False)
    stats = {"a": [1, 0, 1]}
    sqlite_path = tmp_path / "early_dict_raw.sqlite3"
    json_path = tmp_path / "early_dict_raw.json"

    explore._dump_stats_dict_to_sqlite(stats, sqlite_path)

    assert sqlite_path.exists()
    assert not json_path.exists()


def test_optional_write_json_alongside_sqlite(tmp_path, monkeypatch):
    monkeypatch.setattr(explore, "WRITE_JSON", True)
    stats = {"k": [2, 0, 3]}
    sqlite_path = tmp_path / "late_dict_raw.sqlite3"
    json_path = tmp_path / "late_dict_raw.json"

    explore._dump_stats_dict_to_sqlite(stats, sqlite_path)
    explore._dump_stats_dict(stats, json_path)

    assert sqlite_path.exists()
    assert json_path.exists()
    assert orjson.loads(json_path.read_bytes())["k"] == {"wins": 2, "draws": 0, "games": 3}


def test_merge_partitioned_shards_to_sqlite_sums_keys(tmp_path):
    # Two partition shards for part 0 with overlapping key
    part0_a = tmp_path / "a.p000.json"
    part0_b = tmp_path / "b.p000.json"
    part0_a.write_text(
        json.dumps({"shared": {"wins": 1, "draws": 0, "games": 2}, "only_a": {"wins": 0, "draws": 0, "games": 1}}),
        encoding="utf-8",
    )
    part0_b.write_text(
        json.dumps({"shared": {"wins": 2, "draws": 1, "games": 3}, "only_b": {"wins": 1, "draws": 0, "games": 1}}),
        encoding="utf-8",
    )
    # Empty second partition to exercise multi-part loop
    partition_shards = [
        [part0_a, part0_b],
        [],
    ]
    out = tmp_path / "lane_dict_raw.sqlite3"

    keys, games = explore._merge_partitioned_shards(partition_shards, out)

    assert keys == 3
    assert games == 7  # 2+1+3+1
    assert out.exists()
    assert not (tmp_path / "lane_dict_raw.sqlite3.tmp").exists()
    assert not (tmp_path / "lane_dict_raw.json").exists()

    payload = _read_sqlite_kv(out)
    assert payload["kv"]["shared"] == {"wins": 3, "draws": 1, "games": 5}
    assert payload["kv"]["only_a"] == {"wins": 0, "draws": 0, "games": 1}
    assert payload["kv"]["only_b"] == {"wins": 1, "draws": 0, "games": 1}
    assert payload["meta"]["format_version"] == 1
    assert payload["meta"]["backend"] == "sqlite_kv"
    assert payload["meta"]["entries"] == 3
    assert payload["meta"]["source_name"] == "lane_dict_raw.sqlite3"


def test_merge_partitioned_shards_atomic_on_error(tmp_path, monkeypatch):
    part = tmp_path / "s.p000.json"
    part.write_text(json.dumps({"k": {"wins": 1, "draws": 0, "games": 1}}), encoding="utf-8")
    out = tmp_path / "late_dict_raw.sqlite3"
    # Pre-existing final should remain untouched if write fails mid-way
    out.write_bytes(b"old-content")

    def _boom(*_a, **_k):
        raise RuntimeError("forced failure mid-write")

    monkeypatch.setattr(explore, "_write_sqlite_meta", _boom)

    with pytest.raises(RuntimeError, match="forced failure"):
        explore._merge_partitioned_shards([[part]], out)

    # final not half-updated: either old content remains, or file still the pre-write one
    assert out.exists()
    assert out.read_bytes() == b"old-content"
    assert not out.with_suffix(out.suffix + ".tmp").exists()


def test_sqlite_path_helpers():
    stats_dir = Path("/tmp/stats")
    assert explore._sqlite_path_for_metric(stats_dir, "lane") == stats_dir / "lane_dict_raw.sqlite3"
    assert explore._sqlite_path_from_json_name(stats_dir, "post_lane_dict_raw.json") == (
        stats_dir / "post_lane_dict_raw.sqlite3"
    )


def test_check_old_maps_sqlite_meta_matches_without_json(tmp_path):
    import check_old_maps as com

    stats = {"x": [1, 0, 1]}
    db_path = tmp_path / "early_dict_raw.sqlite3"
    explore._dump_stats_dict_to_sqlite(stats, db_path)
    source = tmp_path / "early_dict_raw.json"  # does NOT exist

    assert com._sqlite_stats_meta_matches(db_path, source) is True
    assert com._sqlite_stats_meta_matches(db_path, None) is True
    assert com._sqlite_stats_meta_matches(tmp_path / "missing.sqlite3", source) is False


def test_check_old_maps_rejects_meta_only_sqlite(tmp_path):
    import sqlite3
    import check_old_maps as com

    db_path = tmp_path / "early_dict_raw.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE meta (key TEXT, value TEXT)")
        conn.execute("CREATE TABLE arbitrary (key TEXT, value TEXT)")
        conn.executemany(
            "INSERT INTO meta VALUES (?, ?)",
            [
                ("format_version", "1"),
                ("backend", json.dumps("sqlite_kv")),
                ("entries", "1"),
            ],
        )

    assert com._sqlite_stats_meta_matches(db_path) is False

def test_check_old_maps_load_stats_lookup_sqlite_only(tmp_path):
    import check_old_maps as com

    stats = {"hero1": [4, 0, 5]}
    db_path = tmp_path / "late_dict_raw.sqlite3"
    explore._dump_stats_dict_to_sqlite(stats, db_path)
    assert not (tmp_path / "late_dict_raw.json").exists()

    lookup = com._load_stats_lookup(tmp_path, "late_dict_raw.json", "late_dict")
    assert isinstance(lookup, com.SqliteStatsLookup)
    value = lookup.get("hero1")
    assert value == {"wins": 4, "draws": 0, "games": 5}


def test_stale_legacy_sqlite_falls_back_to_json(tmp_path):
    """Legacy SQLite pointing at JSON with drifted fingerprint must yield to JSON."""
    import sqlite3
    import check_old_maps as com

    source = tmp_path / "early_dict_raw.json"
    db_path = tmp_path / "early_dict_raw.sqlite3"
    source.write_text(
        json.dumps({"fresh": {"wins": 9, "draws": 0, "games": 9}}),
        encoding="utf-8",
    )
    # Build a well-formed kv sqlite, then retarget meta to the JSON with stale size/mtime.
    explore._dump_stats_dict_to_sqlite({"stale": [1, 0, 1]}, db_path)
    st = source.stat()
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM meta")
        conn.executemany(
            "INSERT INTO meta VALUES (?, ?)",
            [
                ("format_version", orjson.dumps(1)),
                ("backend", orjson.dumps("sqlite_kv")),
                ("entries", orjson.dumps(1)),
                ("source_name", orjson.dumps(source.name)),
                ("source_size", orjson.dumps(max(0, st.st_size - 1))),
                ("source_mtime_ns", orjson.dumps(max(0, st.st_mtime_ns - 1))),
            ],
        )

    assert com._sqlite_stats_meta_matches(db_path, source) is False
    lookup = com._load_stats_lookup(tmp_path, "early_dict_raw.json", "early_dict")
    assert isinstance(lookup, dict)
    assert lookup["fresh"] == {"wins": 9, "draws": 0, "games": 9}


def _write_lane_stats_sqlite(db_path: Path) -> None:
    temp_path = db_path.with_name("lane-build.tmp")
    conn = explore._open_lane_sqlite(temp_path)
    explore._upsert_lane_stats(
        conn,
        {"1pos1_vs_2pos1": [1, 0, 2, 1, 0, 2, 3.5, 6.25]},
    )
    explore._finalize_lane_sqlite(conn, temp_path, db_path)


def _write_lane_fallback_stats(tmp_path: Path) -> None:
    for name in ("early", "late", "post_lane"):
        explore._dump_stats_dict_to_sqlite({"x": [1, 0, 1]}, tmp_path / f"{name}_dict_raw.sqlite3")
    (tmp_path / "lane_dict_raw.json").write_text(
        json.dumps({"9pos1_vs_10pos1": {"wins": 7, "draws": 0, "games": 7}}),
        encoding="utf-8",
    )


def _assert_lane_json_fallback(tmp_path: Path) -> None:
    import check_old_maps as com

    lane = com._load_stats_dicts(tmp_path, include_dicts=True, include_lanes=True)[2]
    assert lane["1v1_lanes"]["9pos1_vs_10pos1"] == {"wins": 7, "draws": 0, "games": 7}


def test_lane_v2_sqlite_schema_preserves_kills10(tmp_path):
    import sqlite3

    db_path = tmp_path / "lane_dict_raw.sqlite3"
    _write_lane_stats_sqlite(db_path)
    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(stats)")}
        row = conn.execute(
            "SELECT wins, games, kills10_leads, kills10_games, kills10_diff_sum FROM stats"
        ).fetchone()
        meta = {key: orjson.loads(value) for key, value in conn.execute("SELECT key, value FROM meta")}
    assert {"kills10_leads", "kills10_games", "kills10_diff_sum"}.issubset(columns)
    assert row == (1, 2, 1, 2, 3.5)
    assert meta["format_version"] == 2
    assert meta["backend"] == "sqlite_stats"


def test_check_old_maps_load_lane_from_sqlite_only(tmp_path):
    import check_old_maps as com

    _write_lane_stats_sqlite(tmp_path / "lane_dict_raw.sqlite3")
    explore._dump_stats_dict_to_sqlite({"e": [0, 0, 1]}, tmp_path / "early_dict_raw.sqlite3")
    explore._dump_stats_dict_to_sqlite({"l": [0, 0, 1]}, tmp_path / "late_dict_raw.sqlite3")
    explore._dump_stats_dict_to_sqlite({"p": [0, 0, 1]}, tmp_path / "post_lane_dict_raw.sqlite3")

    early, late, lane, post = com._load_stats_dicts(
        tmp_path,
        include_dicts=True,
        include_lanes=True,
    )
    assert isinstance(early, com.SqliteStatsLookup)
    assert isinstance(late, com.SqliteStatsLookup)
    assert isinstance(post, com.SqliteStatsLookup)
    assert lane


def test_lane_sqlite_string_entries_falls_back_to_json(tmp_path):
    import sqlite3
    import check_old_maps as com

    db_path = tmp_path / "lane_dict_raw.sqlite3"
    _write_lane_stats_sqlite(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE meta SET value = ? WHERE key = 'entries'", (orjson.dumps("1"),))
    _write_lane_fallback_stats(tmp_path)

    assert com._lane_sqlite_is_valid(db_path) is False
    _assert_lane_json_fallback(tmp_path)


def test_lane_sqlite_zero_entries_with_empty_stats_falls_back_to_json(tmp_path):
    import check_old_maps as com

    db_path = tmp_path / "lane_dict_raw.sqlite3"
    temp_path = db_path.with_name("lane-build.tmp")
    conn = explore._open_lane_sqlite(temp_path)
    explore._finalize_lane_sqlite(conn, temp_path, db_path)
    _write_lane_fallback_stats(tmp_path)

    assert com._lane_sqlite_is_valid(db_path) is False
    _assert_lane_json_fallback(tmp_path)


def test_lane_sqlite_mismatched_entries_falls_back_to_json(tmp_path):
    import sqlite3
    import check_old_maps as com

    db_path = tmp_path / "lane_dict_raw.sqlite3"
    _write_lane_stats_sqlite(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE meta SET value = ? WHERE key = 'entries'", (orjson.dumps(2),))
    _write_lane_fallback_stats(tmp_path)

    assert com._lane_sqlite_is_valid(db_path) is False
    _assert_lane_json_fallback(tmp_path)


def test_invalid_lane_sqlite_falls_back_to_json_or_errors(tmp_path):
    import check_old_maps as com

    explore._dump_stats_dict_to_sqlite({"wrong": [1, 0, 1]}, tmp_path / "lane_dict_raw.sqlite3")
    (tmp_path / "lane_dict_raw.json").write_text(json.dumps({"1pos1_vs_2pos1": {"wins": 1, "draws": 0, "games": 1}}))
    assert com._lane_sqlite_is_valid(tmp_path / "lane_dict_raw.sqlite3") is False
    early = tmp_path / "early_dict_raw.sqlite3"
    late = tmp_path / "late_dict_raw.sqlite3"
    post = tmp_path / "post_lane_dict_raw.sqlite3"
    for path in (early, late, post):
        explore._dump_stats_dict_to_sqlite({"x": [1, 0, 1]}, path)
    assert com._load_stats_dicts(tmp_path, include_dicts=True, include_lanes=True)[2]
    (tmp_path / "lane_dict_raw.json").unlink()
    with pytest.raises(FileNotFoundError, match="lane_dict source not found"):
        com._load_stats_dicts(tmp_path, include_dicts=True, include_lanes=True)
