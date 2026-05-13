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
    assert (tmp_path / "lane_dict_raw.json").exists()


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
    assert (tmp_path / "lane_dict_raw.json").exists()
    assert (tmp_path / "custom_bad_quality").exists()


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
    assert not (tmp_path / "early_dict_raw.json").exists()
    assert not (tmp_path / "late_dict_raw.json").exists()


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
    lane_path = tmp_path / "lane_dict_raw.json"
    assert lane_path.exists()
    payload = orjson.loads(lane_path.read_bytes())
    assert payload


def test_dump_bytes_handles_decimal_payload():
    payload = {"value": Decimal("12.5"), "nested": {"x": Decimal("3")}}

    encoded = explore._dump_bytes(payload)

    assert orjson.loads(encoded) == {"value": 12.5, "nested": {"x": 3.0}}
