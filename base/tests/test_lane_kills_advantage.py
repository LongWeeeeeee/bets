from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import analise_database
import explore_database
import functions
import cyberscore_try as runtime


def _match():
    players = []
    for radiant, offset in ((True, 0), (False, 5)):
        for pos in range(1, 6):
            players.append({
                "heroId": offset + pos,
                "position": f"POSITION_{pos}",
                "isRadiant": radiant,
            })
    return {
        "players": players,
        "topLaneOutcome": "RADIANT_WIN",
        "midLaneOutcome": "RADIANT_WIN",
        "bottomLaneOutcome": "DIRE_WIN",
        "radiantKills": [1] * 10 + [100],
        "direKills": [0] * 10 + [200],
    }


def _draft(start):
    return {
        f"pos{pos}": {"hero_id": start + pos - 1}
        for pos in range(1, 6)
    }


def test_lanes_collect_kills10_and_exclude_bucket_ten():
    lane_dict = {}

    analise_database.lanes(_match(), lane_dict)

    radiant = lane_dict["3pos3,4pos4_vs_6pos1,10pos5"]
    dire = lane_dict["6pos1_with_10pos5"]
    assert radiant["kills10_games"] == 1
    assert radiant["kills10_leads"] == 1
    assert radiant["kills10_diff_sum"] == 10
    assert radiant["kills10_diff_sq_sum"] == 100
    assert dire["kills10_diff_sum"] == -10


@pytest.mark.parametrize(
    "radiant,dire",
    [([1] * 9, [0] * 10), ([1] * 10, [0] * 9), ([1] * 9 + [-1], [0] * 10)],
)
def test_lanes_skip_invalid_kills10_without_losing_lane_outcome(radiant, dire):
    match = _match()
    match["radiantKills"] = radiant
    match["direKills"] = dire
    lane_dict = {}

    analise_database.lanes(match, lane_dict)

    entry = lane_dict["2pos2_vs_7pos2"]
    assert entry["games"] == 1
    assert "kills10_games" not in entry


def test_calculate_lane_kills_advantage_uses_reverse_lookup_and_weighted_mean():
    lane_data = {
        # Reverse top key: stored from Dire orientation; inversion gives +2.
        "6pos1,10pos5_vs_3pos3,4pos4": {
            "wins": 1, "draws": 0, "games": 20,
            "kills10_leads": 5, "kills10_draws": 0, "kills10_games": 20,
            "kills10_diff_sum": -40, "kills10_diff_sq_sum": 120,
        },
        "2pos2_vs_7pos2": {
            "wins": 1, "draws": 0, "games": 100,
            "kills10_leads": 70, "kills10_draws": 10, "kills10_games": 100,
            "kills10_diff_sum": 100, "kills10_diff_sq_sum": 500,
        },
        "1pos1,5pos5_vs_8pos3,9pos4": {
            "wins": 1, "draws": 0, "games": 300,
            "kills10_leads": 180, "kills10_draws": 30, "kills10_games": 300,
            "kills10_diff_sum": 0, "kills10_diff_sq_sum": 900,
        },
    }

    result = functions.calculate_lane_kills_advantage(_draft(1), _draft(6), lane_data)

    assert result is not None
    # weights: 20/120, 100/200, 300/400
    assert result["expected_diff"] == pytest.approx((2 * (20 / 120) + 1 * 0.5) / ((20 / 120) + 0.5 + 0.75))
    assert result["coverage"] == 3
    assert result["total_lanes"] == 3
    assert 0 < result["lead_probability"] < 1


def test_exact_lane_kills_layer_ignores_overlapping_fallback_samples():
    exact = {
        "wins": 1, "draws": 0, "games": 20,
        "kills10_leads": 15, "kills10_draws": 1, "kills10_games": 20,
        "kills10_diff_sum": 40, "kills10_diff_sq_sum": 120,
    }
    fallback = {
        "wins": 0, "draws": 0, "games": 500,
        "kills10_leads": 10, "kills10_draws": 0, "kills10_games": 500,
        "kills10_diff_sum": -5000, "kills10_diff_sq_sum": 60000,
    }
    base = {"3pos3,4pos4_vs_6pos1,10pos5": exact}
    noisy = dict(base)
    noisy.update({
        "3pos3,4pos4_vs_6pos1": fallback,
        "3pos3_vs_6pos1": fallback,
        "3pos3_with_4pos4": fallback,
        "3pos3": fallback,
    })

    base_result = functions.calculate_lane_kills_advantage(_draft(1), _draft(6), base)
    noisy_result = functions.calculate_lane_kills_advantage(_draft(1), _draft(6), noisy)

    assert base_result == noisy_result
    assert noisy_result["expected_diff"] == pytest.approx(2.0)
    assert noisy_result["games"] == 20


def test_two_v_one_effective_games_use_min_for_full_and_max_for_partial():
    def stats(games):
        return {
            "wins": 1, "draws": 0, "games": games,
            "kills10_leads": games, "kills10_draws": 0, "kills10_games": games,
            "kills10_diff_sum": games, "kills10_diff_sq_sum": games,
        }

    full = {
        "3pos3,4pos4_vs_6pos1": stats(20),
        "3pos3,4pos4_vs_10pos5": stats(30),
        "3pos3_vs_6pos1,10pos5": stats(40),
        "4pos4_vs_6pos1,10pos5": stats(50),
    }
    partial = {"4pos4_vs_6pos1,10pos5": stats(50)}

    full_result = functions.calculate_lane_kills_advantage(_draft(1), _draft(6), full)
    partial_result = functions.calculate_lane_kills_advantage(_draft(1), _draft(6), partial)

    assert full_result["games"] == 20
    assert partial_result["games"] == 50


def test_direct_lane_sqlite_schema_and_accumulation(tmp_path):
    temp_path = tmp_path / "lane.sqlite3.tmp"
    output_path = tmp_path / "lane.sqlite3"
    conn = explore_database._open_lane_sqlite(temp_path)
    # Полная строка: три счётчика линии + блок kills10 + блок nw10.
    stats = [1, 0, 1, 1, 0, 1, 2.0, 4.0, 1, 0, 1, 900.0, 810000.0, 900.0]
    explore_database._upsert_lane_stats(conn, {"key": stats})
    explore_database._upsert_lane_stats(conn, {"key": stats})

    entries, games = explore_database._finalize_lane_sqlite(conn, temp_path, output_path)

    assert (entries, games) == (1, 2)
    with sqlite3.connect(output_path) as check:
        row = check.execute("SELECT * FROM stats WHERE key='key'").fetchone()
    assert row == ("key", 2, 0, 2, 2, 0, 2, 4.0, 8.0, 2, 0, 2, 1800.0, 1620000.0, 1800.0)

    loaded = runtime._load_lane_dict_from_source(str(tmp_path / "lane.json"))
    assert loaded["key"]["kills10_diff_sum"] == 4.0
    # nw10 материализуется только когда каскад их читает (иначе +2 ГБ RSS зря)
    assert "nw10_clip_sum" not in loaded["key"]
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv("LANE_CELL_VALUE", "nw_mean")
    try:
        loaded_nw = runtime._load_lane_dict_from_source(str(tmp_path / "lane.json"))
    finally:
        monkeypatch.undo()
    assert loaded_nw["key"]["nw10_clip_sum"] == 1800.0


def test_lane_entry_writes_both_early_targets():
    """Одна запись ключа копит и kills@10, и NW@10 с обрезкой выбросов."""
    target = {}
    analise_database._append_lane_entry(target, "1pos1", 1, 2.0, 5000.0)
    analise_database._append_lane_entry(target, "1pos1", 0, -1.0, -800.0)
    stats = target["1pos1"]
    assert stats["games"] == 2 and stats["wins"] == 1
    assert stats["kills10_games"] == 2 and stats["kills10_diff_sum"] == 1.0
    assert stats["nw10_games"] == 2 and stats["nw10_diff_sum"] == 4200.0
    # 5000 обрезано до 3000, -800 осталось как есть
    assert stats["nw10_clip_sum"] == 2200.0
    assert stats["nw10_leads"] == 1


def test_lane_nw_advantage_reads_nw_block():
    """Новый читатель берёт nw10-колонки и не смешивает их с kills10."""
    def cell(games, nw_per_game):
        return {
            "wins": games, "draws": 0, "games": games,
            "kills10_leads": games, "kills10_draws": 0, "kills10_games": games,
            "kills10_diff_sum": games * 3.0, "kills10_diff_sq_sum": games * 9.0,
            "nw10_leads": games, "nw10_draws": 0, "nw10_games": games,
            "nw10_diff_sum": games * nw_per_game, "nw10_diff_sq_sum": games * nw_per_game ** 2,
            "nw10_clip_sum": games * nw_per_game,
        }

    data = {"2pos2_vs_7pos2": cell(50, 400.0)}
    result = functions.calculate_lane_nw_advantage(_draft(1), _draft(6), data)
    assert result is not None
    assert result["expected_diff"] == pytest.approx(400.0)
    kills = functions.calculate_lane_kills_advantage(_draft(1), _draft(6), data)
    assert kills["expected_diff"] == pytest.approx(3.0)


def test_lane_kills_telegram_line_is_signed_radiant_diff_with_lead():
    line = runtime._build_lane_kills_adv_line({
        "expected_diff": -1.24,
        "lead_probability": 0.30,
        "draw_probability": 0.10,
        "coverage": 3,
        "total_lanes": 3,
    })

    assert line == "lane_kills_adv_dict: -1.24 kills @10 (lead 60%)\n"
    block = runtime._build_lane_block("Top: win 60%", "", "", lane_kills_adv={
        "expected_diff": 1.0,
        "lead_probability": 0.58,
        "draw_probability": 0.1,
        "coverage": 1,
        "total_lanes": 3,
    })
    assert "lane_kills_adv_dict: +1.00 kills @10 (lead 58%)" in block


def test_lane_loader_keeps_legacy_kv_sqlite_compatibility(tmp_path):
    import orjson

    sqlite_path = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("CREATE TABLE kv (key TEXT PRIMARY KEY, value BLOB)")
        conn.execute(
            "INSERT INTO kv VALUES (?, ?)",
            ("1pos1", sqlite3.Binary(orjson.dumps({"wins": 2, "draws": 1, "games": 4}))),
        )

    loaded = runtime._load_lane_dict_from_source(str(tmp_path / "legacy.json"))

    assert loaded == {"1pos1": {"wins": 2, "draws": 1, "games": 4}}


def test_owned_lane_sqlite_success_replaces_only_at_finalize(tmp_path):
    output = tmp_path / "lane.sqlite3"
    output.write_bytes(b"production")
    temp = tmp_path / "owned.tmp"
    build = explore_database._OwnedLaneSqliteBuild()
    conn = build.open(temp)
    explore_database._upsert_lane_stats(conn, {"key": [1, 0, 1, 1, 0, 1, 2.0, 4.0]})

    assert output.read_bytes() == b"production"
    build.prepare(output)
    assert output.read_bytes() == b"production"
    build.publish()
    build.rollback()

    with sqlite3.connect(output) as conn:
        assert conn.execute("SELECT games FROM stats WHERE key='key'").fetchone() == (1,)
    assert not temp.exists()


@pytest.mark.parametrize("error", [RuntimeError("boom"), KeyboardInterrupt()])
def test_main_rolls_back_owned_lane_sqlite_on_error_and_interrupt(tmp_path, monkeypatch, error):
    output = tmp_path / "lane.sqlite3"
    output.write_bytes(b"production")
    temp = tmp_path / "owned.tmp"

    def broken(build, kills_window_build=None, kv_builds=None):
        build.open(temp)
        raise error

    monkeypatch.setattr(explore_database, "_main_impl", broken)

    with pytest.raises(type(error)):
        explore_database.main()

    assert output.read_bytes() == b"production"
    assert not temp.exists()


def _write_lane_sqlite(path: Path, stats: dict) -> None:
    temp = path.with_suffix(".tmp")
    conn = explore_database._open_lane_sqlite(temp)
    explore_database._upsert_lane_stats(conn, stats)
    explore_database._finalize_lane_sqlite(conn, temp, path)


def test_structure_lane_dict_wraps_point_lookup_without_scanning():
    class _ExplodingBackend(dict):
        def get_many(self, keys):
            return {}

        def get(self, key, default=None):
            if key == "3pos3,4pos4_vs_6pos1,10pos5":
                return {"wins": 8, "draws": 1, "games": 10}
            return default

        def items(self):
            raise AssertionError("lane sqlite backend must not be scanned")

        def keys(self):
            raise AssertionError("lane sqlite backend must not be scanned")

        def __iter__(self):
            raise AssertionError("lane sqlite backend must not be scanned")

    structured = functions.structure_lane_dict(_ExplodingBackend())
    assert isinstance(structured, functions._StructuredLaneLookup)
    assert "2v2_lanes" in structured
    assert structured["2v2_lanes"].get("3pos3,4pos4_vs_6pos1,10pos5")["games"] == 10
    assert structured["2v2_lanes"].get("missing") is None


def test_sqlite_lane_lookup_matches_in_memory_kills_advantage(tmp_path):
    cell = {
        "wins": 30, "draws": 2, "games": 40,
        "kills10_leads": 30, "kills10_draws": 2, "kills10_games": 40,
        "kills10_diff_sum": 80.0, "kills10_diff_sq_sum": 240.0,
        "nw10_leads": 24, "nw10_draws": 2, "nw10_games": 40,
        "nw10_diff_sum": 16000.0, "nw10_diff_sq_sum": 8_000_000.0,
        "nw10_clip_sum": 12000.0,
    }
    stats = {"3pos3,4pos4_vs_6pos1,10pos5": cell}
    sqlite_path = tmp_path / "lane_dict_raw.sqlite3"
    _write_lane_sqlite(sqlite_path, stats)

    memory = functions.calculate_lane_kills_advantage(_draft(1), _draft(6), stats)
    lookup = runtime._prepare_indexed_stats_lookup(str(tmp_path / "lane_dict_raw.json"), "lane")
    try:
        wrapped = functions.structure_lane_dict(lookup)
        sqlite = functions.calculate_lane_kills_advantage(_draft(1), _draft(6), wrapped)
        top, _bot, _mid = functions.calculate_lanes(_draft(1), _draft(6), wrapped)
    finally:
        lookup.close()

    assert memory is not None
    assert sqlite == memory
    assert sqlite["expected_diff"] == pytest.approx(2.0)
    assert top.startswith("Top: win ")


def test_load_stats_dicts_opens_lane_sqlite_without_materializing(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "LIVE_LANE_ANALYSIS_ENABLED", True, raising=False)
    monkeypatch.setattr(runtime, "STATS_SEQUENTIAL_WARMUP_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "STATS_SQLITE_AUTOBUILD", False, raising=False)
    monkeypatch.setattr(runtime, "STATS_LOOKUP_BACKEND", "auto", raising=False)
    monkeypatch.setattr(runtime, "stats_warmup_last_heavy_load_ts", 0.0, raising=False)
    monkeypatch.setattr(runtime, "lane_data", None, raising=False)
    monkeypatch.setattr(runtime, "early_dict", {"ok": 1}, raising=False)
    monkeypatch.setattr(runtime, "early_end_dict", {"ok": 1}, raising=False)
    monkeypatch.setattr(runtime, "late_dict", {"ok": 1}, raising=False)
    monkeypatch.setattr(runtime, "post_lane_dict", {"ok": 1}, raising=False)
    monkeypatch.setattr(runtime, "late_pub_comeback_table_data", None, raising=False)
    monkeypatch.setattr(runtime, "late_pre27_watcher_data", None, raising=False)
    monkeypatch.setattr(runtime, "all_only_watcher_data", None, raising=False)

    stats = {
        "3pos3,4pos4_vs_6pos1,10pos5": {
            "wins": 15, "draws": 1, "games": 20,
            "kills10_leads": 15, "kills10_draws": 1, "kills10_games": 20,
            "kills10_diff_sum": 40.0, "kills10_diff_sq_sum": 120.0,
        }
    }
    json_path = tmp_path / "lane_dict_raw.json"
    _write_lane_sqlite(tmp_path / "lane_dict_raw.sqlite3", stats)
    table_path = tmp_path / "pub_late_star_comeback_table_piecewise.json"
    table_path.write_text('{"table_rows": []}', encoding="utf-8")
    pre27_path = tmp_path / "pub_late_pre27_watcher_thresholds.json"
    pre27_path.write_text('{"table_rows": []}', encoding="utf-8")
    all_only_path = tmp_path / "pub_all_only_watcher_thresholds.json"
    all_only_path.write_text('{"table_rows": []}', encoding="utf-8")

    monkeypatch.setenv("STATS_DIR", str(tmp_path))
    monkeypatch.setenv("STATS_LANE_PATH", str(json_path))
    monkeypatch.setenv("STATS_LATE_PUB_COMEBACK_TABLE_PATH", str(table_path))
    monkeypatch.setenv("STATS_LATE_PRE27_WATCHER_PATH", str(pre27_path))
    monkeypatch.setenv("STATS_ALL_ONLY_WATCHER_PATH", str(all_only_path))

    assert runtime._load_stats_dicts() is True
    assert isinstance(runtime.lane_data, functions._StructuredLaneLookup)
    backend = runtime.lane_data._backend
    assert isinstance(backend, runtime._SqliteStatsLookup)
    assert len(backend) == 0
    cell = runtime.lane_data["2v2_lanes"].get("3pos3,4pos4_vs_6pos1,10pos5")
    assert cell["kills10_diff_sum"] == 40.0
    assert len(backend) == 0
    runtime.lane_data.close()
