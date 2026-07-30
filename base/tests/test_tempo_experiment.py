from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
BASE_DIR = ROOT / "base"
for path in (str(ROOT), str(BASE_DIR)):
    if path not in sys.path:
        sys.path.insert(0, path)

from tempo_analise_database_experiment import (
    build_tempo_draft_metrics,
    process_tempo_pub_match,
)


def _make_player(hero_id, pos, is_radiant, kills, deaths, assists, hero_damage):
    return {
        "heroId": hero_id,
        "position": pos,
        "isRadiant": is_radiant,
        "kills": kills,
        "deaths": deaths,
        "assists": assists,
        "heroDamage": hero_damage,
        "intentionalFeeding": False,
        "networth": 1000,
    }


def test_process_tempo_pub_match_builds_all_three_dicts():
    match = {
        "id": 1,
        "startDateTime": 1750000000,
        "durationSeconds": 1800,
        "players": [
            _make_player(1, "pos1", True, 10, 2, 5, 10000),
            _make_player(2, "pos2", True, 8, 3, 7, 9000),
            _make_player(3, "pos3", True, 5, 4, 8, 7000),
            _make_player(4, "pos4", True, 3, 5, 10, 5000),
            _make_player(5, "pos5", True, 1, 6, 12, 3000),
            _make_player(6, "pos1", False, 9, 3, 6, 9500),
            _make_player(7, "pos2", False, 7, 4, 7, 8500),
            _make_player(8, "pos3", False, 4, 5, 9, 6500),
            _make_player(9, "pos4", False, 2, 6, 11, 4500),
            _make_player(10, "pos5", False, 1, 7, 13, 2500),
        ],
    }
    solo_dict = {}
    duo_dict = {}
    cp1v1_dict = {}

    updated = process_tempo_pub_match(
        match,
        solo_dict,
        duo_dict,
        cp1v1_dict,
        min_start_ts=1747785600,
        strict_positions=False,
    )

    assert updated is True
    assert solo_dict["1pos1"]["games"] == 1
    assert solo_dict["1pos1"]["kills_pm_sum"] == 10 / 30
    assert duo_dict["1pos1_with_2pos2"]["games"] == 1
    assert duo_dict["1pos1_with_2pos2"]["kills_pm_sum"] == (10 + 8) / 30
    assert cp1v1_dict["1pos1_vs_6pos1"]["games"] == 1
    assert cp1v1_dict["1pos1_vs_6pos1"]["kills_pm_sum"] == (10 + 9) / 30


def test_build_tempo_draft_metrics_uses_match_level_factors():
    radiant = {
        "pos1": {"hero_id": 1},
        "pos2": {"hero_id": 2},
        "pos3": {"hero_id": 3},
        "pos4": {"hero_id": 4},
        "pos5": {"hero_id": 5},
    }
    dire = {
        "pos1": {"hero_id": 6},
        "pos2": {"hero_id": 7},
        "pos3": {"hero_id": 8},
        "pos4": {"hero_id": 9},
        "pos5": {"hero_id": 10},
    }

    def rec(kills, deaths, assists, dmg):
        return {
            "games": 1,
            "kills_pm_sum": kills,
            "deaths_pm_sum": deaths,
            "assists_pm_sum": assists,
            "hero_damage_pm_sum": dmg,
        }

    solo_dict = {f"{hero_id}pos{pos}": rec(0.2, 0.2, 0.5, 300.0) for hero_id, pos in [(1,1),(2,2),(3,3),(4,4),(5,5),(6,1),(7,2),(8,3),(9,4),(10,5)]}
    duo_dict = {}
    cp_dict = {}
    radiant_keys = ["1pos1", "2pos2", "3pos3", "4pos4", "5pos5"]
    dire_keys = ["6pos1", "7pos2", "8pos3", "9pos4", "10pos5"]
    for team_keys in (radiant_keys, dire_keys):
        for i, left in enumerate(team_keys):
            for right in team_keys[i + 1 :]:
                duo_dict[f"{left}_with_{right}" if left <= right else f"{right}_with_{left}"] = rec(0.4, 0.4, 1.0, 600.0)
    for left in radiant_keys:
        for right in dire_keys:
            cp_dict[f"{left}_vs_{right}" if left <= right else f"{right}_vs_{left}"] = rec(0.4, 0.4, 1.0, 600.0)

    out = build_tempo_draft_metrics(radiant, dire, solo_dict, duo_dict, cp_dict)

    assert out["solo"]["complete"] is True
    assert out["solo"]["kills_pm"]["predicted_total_pm"] == pytest.approx(2.0)
    assert out["solo"]["kills_pm"]["index"] == 20
    assert out["synergy_duo"]["kills_pm"]["predicted_total_pm"] == pytest.approx(2.0)
    assert out["synergy_duo"]["kills_pm"]["index"] == 20
    assert out["counterpick_1vs1"]["kills_pm"]["predicted_total_pm"] == pytest.approx(2.0)
    assert out["counterpick_1vs1"]["kills_pm"]["index"] == 20
