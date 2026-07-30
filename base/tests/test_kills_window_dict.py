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


def _match(radiant_kills=None, dire_kills=None):
    players = []
    for radiant, offset in ((True, 0), (False, 5)):
        for pos in range(1, 6):
            players.append(
                {
                    "heroId": offset + pos,
                    "position": f"POSITION_{pos}",
                    "isRadiant": radiant,
                }
            )
    return {
        "players": players,
        "radiantKills": radiant_kills if radiant_kills is not None else [1] * 30,
        "direKills": dire_kills if dire_kills is not None else [0] * 30,
        "didRadiantWin": True,
        "startDateTime": 1_800_000_000,
        "durationSeconds": 2400,
    }


def test_kills_window_diff_half_open_slices():
    match = _match(
        # indices: 5→2 (in 5-15), 10→3 (in 10-20 and also in 5-15)
        radiant_kills=[0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 3, 0, 0, 0, 0] + [0] * 15,
        dire_kills=[0] * 30,
    )
    assert analise_database._kills_window_diff(match, 5, 15) == 5.0  # 2 + 3
    assert analise_database._kills_window_diff(match, 10, 20) == 3.0
    assert analise_database._kills_window_diff(match, 0, 10) == 2.0  # only index 5
    assert analise_database._kills10_diff(match) == 2.0


def test_kills_window_diff_rejects_short_or_invalid_series():
    assert analise_database._kills_window_diff({"radiantKills": [1] * 10, "direKills": [0] * 30}, 5, 15) is None
    bad = _match(radiant_kills=[1] * 14 + [-1] + [0] * 15, dire_kills=[0] * 30)
    assert analise_database._kills_window_diff(bad, 5, 15) is None


def test_kills_windows_collects_all_four_windows_and_inverts_dire_keys():
    d = {}
    # Radiant leads every window by 1 kill / minute over 10 minutes → diff 10
    assert analise_database.kills_windows(_match(), d) is True

    # solo radiant
    r = d["1pos1"]
    assert r[0] == 1 and r[2] == 1 and r[3] == 10.0  # 5_15 lead
    assert r[5] == 1 and r[7] == 1 and r[8] == 10.0  # 10_20
    assert r[10] == 1 and r[12] == 1 and r[13] == 10.0  # 15_25
    assert r[15] == 1 and r[17] == 1 and r[18] == 10.0  # 20_30

    # solo dire inverted
    dire = d["6pos1"]
    assert dire[0] == 0 and dire[2] == 1 and dire[3] == -10.0

    # matchup key present
    assert "1pos1_vs_6pos1" in d
    assert "1pos1_with_2pos2" in d
    # no trios
    assert not any("_with_" in k and k.count("pos") == 3 for k in d)


def test_kills_windows_partial_series_fills_only_valid_windows():
    d = {}
    match = _match(radiant_kills=[1] * 20, dire_kills=[0] * 20)
    assert analise_database.kills_windows(match, d)
    row = d["1pos1"]
    assert row[2] == 1  # 5_15 games
    assert row[7] == 1  # 10_20 games
    assert row[12] == 0  # 15_25 missing
    assert row[17] == 0  # 20_30 missing


def test_analise_database_wires_kills_window_without_lane_gates():
    d = {}
    match = _match()
    # No lane outcomes → lane dict stays empty, kills_window still fills.
    updated = analise_database.analise_database(
        match,
        lane_dict={},
        early_dict=None,
        late_dict=None,
        kills_window_dict=d,
        exclude_pro_matches=False,
    )
    assert updated is True
    assert d


def test_kills_window_sqlite_roundtrip(tmp_path):
    build = explore_database._OwnedKillsWindowSqliteBuild()
    temp = tmp_path / "kw.tmp"
    out = tmp_path / "kills_window_dict_raw.sqlite3"
    conn = build.open(temp)
    payload = {
        "1pos1": [1, 0, 1, 2.0, 4.0] + [0, 0, 0, 0.0, 0.0] * 3,
    }
    explore_database._upsert_kills_window_stats(conn, payload)
    assert out.exists() is False
    entries, games = build.finalize(out)
    assert entries == 1
    assert games == 1
    with sqlite3.connect(out) as c:
        row = c.execute(
            "SELECT kills_5_15_leads, kills_5_15_games, kills_5_15_diff_sum FROM stats WHERE key='1pos1'"
        ).fetchone()
    assert row == (1, 1, 2.0)
    assert not temp.exists()


def test_calculate_kills_window_advantage_prefers_1v1_and_inverts_dire():
    data = {
        "1pos1_vs_6pos1": {
            "kills_10_20_leads": 8,
            "kills_10_20_draws": 0,
            "kills_10_20_games": 10,
            "kills_10_20_diff_sum": 20.0,
        },
        "1pos1": {
            "kills_10_20_leads": 5,
            "kills_10_20_draws": 0,
            "kills_10_20_games": 10,
            "kills_10_20_diff_sum": 5.0,
        },
    }
    radiant = ["1pos1", "2pos2", "3pos3", "4pos4", "5pos5"]
    dire = ["6pos1", "7pos2", "8pos3", "9pos4", "10pos5"]
    result = functions.calculate_kills_window_advantage(
        radiant, dire, data, window=(10, 20)
    )
    assert result is not None
    # Default policy core_1v1_with: only 1v1 present among core layers → 1v1.
    assert result["layer"] == "1v1"
    assert result["expected_diff"] == pytest.approx(2.0)
    assert result["window"] == "10_20"
    assert result["lead_probability"] == pytest.approx(0.8)


def test_calculate_kills_window_core_same_sign_blend_1v1_with(monkeypatch):
    monkeypatch.setenv("KILLS_WINDOW_LAYER_POLICY", "core_1v1_with")
    monkeypatch.setenv("KILLS_WINDOW_RELIABILITY_PRIOR", "100")
    # Both layers same sign; reliability = games/(games+100) → equal games → mean.
    data = {
        "1pos1_vs_6pos1": {
            "kills_10_20_leads": 8,
            "kills_10_20_draws": 0,
            "kills_10_20_games": 20,
            "kills_10_20_diff_sum": 40.0,  # mean +2
        },
        "1pos1_with_2pos2": {
            "kills_10_20_leads": 12,
            "kills_10_20_draws": 0,
            "kills_10_20_games": 20,
            "kills_10_20_diff_sum": 20.0,  # mean +1
        },
    }
    radiant = ["1pos1", "2pos2"]
    dire = ["6pos1"]
    result = functions.calculate_kills_window_advantage(
        radiant, dire, data, window=(10, 20)
    )
    assert result is not None
    assert result["layer"] == "1v1+with_same_sign"
    assert result["expected_diff"] == pytest.approx(1.5)


def test_calculate_kills_window_first_hit_legacy_policy(monkeypatch):
    monkeypatch.setenv("KILLS_WINDOW_LAYER_POLICY", "first_hit")
    data = {
        "1pos1_vs_6pos1,7pos2": {
            "kills_10_20_leads": 9,
            "kills_10_20_draws": 0,
            "kills_10_20_games": 10,
            "kills_10_20_diff_sum": 30.0,
        },
        "1pos1_vs_6pos1": {
            "kills_10_20_leads": 8,
            "kills_10_20_draws": 0,
            "kills_10_20_games": 10,
            "kills_10_20_diff_sum": 20.0,
        },
    }
    radiant = ["1pos1"]
    dire = ["6pos1", "7pos2"]
    result = functions.calculate_kills_window_advantage(
        radiant, dire, data, window=(10, 20)
    )
    assert result is not None
    assert result["layer"] == "1v2"
    assert result["expected_diff"] == pytest.approx(3.0)


def test_calculate_kills_window_all_windows_map():
    data = {
        "1pos1": {
            "kills_5_15_leads": 6,
            "kills_5_15_draws": 0,
            "kills_5_15_games": 10,
            "kills_5_15_diff_sum": 10.0,
            "kills_10_20_leads": 7,
            "kills_10_20_draws": 0,
            "kills_10_20_games": 10,
            "kills_10_20_diff_sum": 20.0,
            "kills_15_25_leads": 5,
            "kills_15_25_draws": 0,
            "kills_15_25_games": 10,
            "kills_15_25_diff_sum": 5.0,
            "kills_20_30_leads": 4,
            "kills_20_30_draws": 0,
            "kills_20_30_games": 10,
            "kills_20_30_diff_sum": -5.0,
        },
    }
    radiant = ["1pos1"]
    dire = ["6pos1"]
    # dire solo missing → still uses radiant solo
    result = functions.calculate_kills_window_advantage(radiant, dire, data, window=None)
    assert set(result) == {"5_15", "10_20", "15_25", "20_30"}
    assert result["10_20"]["expected_diff"] == pytest.approx(2.0)
    assert result["20_30"]["expected_diff"] == pytest.approx(-0.5)
