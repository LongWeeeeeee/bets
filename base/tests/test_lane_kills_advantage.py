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
    stats = [1, 0, 1, 1, 0, 1, 2.0, 4.0]
    explore_database._upsert_lane_stats(conn, {"key": stats})
    explore_database._upsert_lane_stats(conn, {"key": stats})

    entries, games = explore_database._finalize_lane_sqlite(conn, temp_path, output_path)

    assert (entries, games) == (1, 2)
    with sqlite3.connect(output_path) as check:
        row = check.execute("SELECT * FROM stats WHERE key='key'").fetchone()
    assert row == ("key", 2, 0, 2, 2, 0, 2, 4.0, 8.0)

    loaded = runtime._load_lane_dict_from_source(str(tmp_path / "lane.json"))
    assert loaded["key"]["kills10_diff_sum"] == 4.0


def test_lane_kills_telegram_line_is_diagnostic_and_directional():
    line = runtime._build_lane_kills_adv_line({
        "expected_diff": -1.24,
        "lead_probability": 0.30,
        "draw_probability": 0.10,
        "coverage": 3,
        "total_lanes": 3,
    })

    assert line == "lane_kills_adv_dict: Dire +1.24 kills @10 (lead 60%, 3/3)\n"
    block = runtime._build_lane_block("Top: win 60%", "", "", lane_kills_adv={
        "expected_diff": 1.0,
        "lead_probability": 0.58,
        "draw_probability": 0.1,
        "coverage": 1,
        "total_lanes": 3,
    })
    assert "lane_kills_adv_dict: Radiant +1.00 kills @10 (lead 58%, 1/3)" in block


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
    build.finalize(output)
    build.rollback()

    with sqlite3.connect(output) as conn:
        assert conn.execute("SELECT games FROM stats WHERE key='key'").fetchone() == (1,)
    assert not temp.exists()


@pytest.mark.parametrize("error", [RuntimeError("boom"), KeyboardInterrupt()])
def test_main_rolls_back_owned_lane_sqlite_on_error_and_interrupt(tmp_path, monkeypatch, error):
    output = tmp_path / "lane.sqlite3"
    output.write_bytes(b"production")
    temp = tmp_path / "owned.tmp"

    def broken(build):
        build.open(temp)
        raise error

    monkeypatch.setattr(explore_database, "_main_impl", broken)

    with pytest.raises(type(error)):
        explore_database.main()

    assert output.read_bytes() == b"production"
    assert not temp.exists()
