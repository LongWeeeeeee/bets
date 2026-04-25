from __future__ import annotations

import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import dota2protracker as protracker  # noqa: E402


def _add_precise_matchup(
    hero_data: dict,
    radiant_hero: str,
    dire_hero: str,
    radiant_pos: str,
    dire_pos: str,
    wr: float = 60.0,
    games: int = 20,
) -> None:
    radiant_key = protracker._hero_norm_key(radiant_hero)
    dire_key = protracker._hero_norm_key(dire_hero)
    radiant_pos_num = protracker.POSITION_MAP[radiant_pos]
    dire_pos_num = protracker.POSITION_MAP[dire_pos]
    entry = hero_data.setdefault(radiant_key, {"_matchups_by_hero_pos": {}})
    entry["_matchups_by_hero_pos"].setdefault(dire_key, {}).setdefault(dire_pos_num, {})[radiant_pos_num] = {
        "wr": wr,
        "games": games,
    }


def _add_precise_synergy(
    hero_data: dict,
    hero1: str,
    hero2: str,
    pos1: str,
    pos2: str,
    wr: float = 60.0,
    games: int = 20,
) -> None:
    hero1_key = protracker._hero_norm_key(hero1)
    hero2_key = protracker._hero_norm_key(hero2)
    pos1_num = protracker.POSITION_MAP[pos1]
    pos2_num = protracker.POSITION_MAP[pos2]
    entry = hero_data.setdefault(hero1_key, {"_synergies_by_hero_pos": {}})
    entry["_synergies_by_hero_pos"].setdefault(hero2_key, {}).setdefault(pos2_num, {})[pos1_num] = {
        "wr": wr,
        "games": games,
    }


def _pro_core_sides() -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    radiant = [("pos1", "RadiantOne"), ("pos2", "RadiantTwo"), ("pos3", "RadiantThree")]
    dire = [("pos1", "DireOne"), ("pos2", "DireTwo"), ("pos3", "DireThree")]
    return radiant, dire


def test_global_pro_cp1vs1_accepts_two_of_three_core_matchups() -> None:
    radiant, dire = _pro_core_sides()
    hero_data: dict = {}

    for radiant_pos, dire_pos in (
        ("pos1", "pos1"),
        ("pos1", "pos2"),
        ("pos2", "pos1"),
        ("pos2", "pos3"),
        ("pos3", "pos2"),
        ("pos3", "pos3"),
    ):
        radiant_hero = dict(radiant)[radiant_pos]
        dire_hero = dict(dire)[dire_pos]
        _add_precise_matchup(hero_data, radiant_hero, dire_hero, radiant_pos, dire_pos)

    valid, data = protracker._calculate_cp1vs1_all_positions(
        radiant_positions=radiant,
        dire_positions=dire,
        hero_data=hero_data,
        min_games=10,
    )

    assert valid is True
    assert data["required_core_vs_core"] == 2
    assert data["radiant_core_vs_core_coverage"] == {"pos1": 2, "pos2": 2, "pos3": 2}
    assert data["dire_core_vs_core_coverage"] == {"pos1": 2, "pos2": 2, "pos3": 2}


def test_global_pro_cp1vs1_rejects_one_of_three_core_matchups() -> None:
    radiant, dire = _pro_core_sides()
    hero_data: dict = {}

    for radiant_pos, dire_pos in (
        ("pos1", "pos1"),
        ("pos1", "pos2"),
        ("pos2", "pos1"),
        ("pos2", "pos2"),
        ("pos3", "pos3"),
    ):
        radiant_hero = dict(radiant)[radiant_pos]
        dire_hero = dict(dire)[dire_pos]
        _add_precise_matchup(hero_data, radiant_hero, dire_hero, radiant_pos, dire_pos)

    valid, data = protracker._calculate_cp1vs1_all_positions(
        radiant_positions=radiant,
        dire_positions=dire,
        hero_data=hero_data,
        min_games=10,
    )

    assert valid is False
    assert data["required_core_vs_core"] == 2
    assert data["radiant_core_vs_core_coverage"]["pos3"] == 1
    assert data["dire_core_vs_core_coverage"]["pos3"] == 1


def test_global_pro_duo_synergy_accepts_reverse_hero_page_direction() -> None:
    radiant, _dire = _pro_core_sides()
    hero_data: dict = {}
    heroes_by_pos = dict(radiant)

    for pos1, pos2 in (("pos1", "pos2"), ("pos1", "pos3"), ("pos2", "pos3")):
        _add_precise_synergy(
            hero_data,
            heroes_by_pos[pos2],
            heroes_by_pos[pos1],
            pos2,
            pos1,
            wr=63.0,
            games=22,
        )

    valid, data = protracker._calculate_duo_synergy_all_positions(
        team_positions=radiant,
        hero_data=hero_data,
        min_games=10,
        position_weights=protracker.PRO_EARLY_POSITION_WEIGHTS,
    )

    assert valid is True
    assert data["count"] == 3
    assert data["games"] == 66
    assert data["core_coverage"] == {"pos1": 2, "pos2": 2, "pos3": 2}


def test_calculate_lane_advantage_uses_team_specific_lane_pairs(monkeypatch) -> None:
    radiant_positions = [
        ("pos1", "RadiantCarry"),
        ("pos2", "RadiantMid"),
        ("pos3", "RadiantOfflane"),
        ("pos4", "RadiantSoft"),
        ("pos5", "RadiantHard"),
    ]
    dire_positions = [
        ("pos1", "DireCarry"),
        ("pos2", "DireMid"),
        ("pos3", "DireOfflane"),
        ("pos4", "DireSoft"),
        ("pos5", "DireHard"),
    ]

    cp_values = {
        ("RadiantMid", "DireMid", "pos2", "pos2"): (22.0, 120),
        ("RadiantOfflane", "DireCarry", "pos3", "pos1"): (31.0, 120),
        ("RadiantOfflane", "DireHard", "pos3", "pos5"): (35.0, 120),
        ("RadiantSoft", "DireCarry", "pos4", "pos1"): (41.0, 120),
        ("RadiantSoft", "DireHard", "pos4", "pos5"): (45.0, 120),
        ("RadiantCarry", "DireOfflane", "pos1", "pos3"): (13.0, 120),
        ("RadiantCarry", "DireSoft", "pos1", "pos4"): (14.0, 120),
        ("RadiantHard", "DireOfflane", "pos5", "pos3"): (53.0, 120),
        ("RadiantHard", "DireSoft", "pos5", "pos4"): (54.0, 120),
    }
    duo_values = {
        ("RadiantOfflane", "RadiantSoft", "pos3", "pos4", "DireCarry", "DireHard", "pos1", "pos5"): (34.15, 240),
        ("RadiantCarry", "RadiantHard", "pos1", "pos5", "DireOfflane", "DireSoft", "pos3", "pos4"): (15.34, 240),
    }

    def fake_get_matchup_1v1(_hero_data, r_hero, d_hero, r_pos, d_pos, _min_games):
        return cp_values.get((r_hero, d_hero, r_pos, d_pos), (None, 0))

    def fake_get_duo_synergy_pair(
        _hero_data,
        r_hero1,
        r_hero2,
        r_pos1,
        r_pos2,
        d_hero1,
        d_hero2,
        d_pos1,
        d_pos2,
        _min_games,
    ):
        return duo_values.get(
            (r_hero1, r_hero2, r_pos1, r_pos2, d_hero1, d_hero2, d_pos1, d_pos2),
            (None, 0),
        )

    monkeypatch.setattr(protracker, "_get_matchup_1v1", fake_get_matchup_1v1)
    monkeypatch.setattr(protracker, "_get_duo_synergy_pair", fake_get_duo_synergy_pair)

    result = protracker.calculate_lane_advantage(
        radiant_positions=radiant_positions,
        dire_positions=dire_positions,
        hero_data={},
        min_games=10,
    )

    assert result["mid"]["cp1vs1"] == 22.0
    assert result["mid"]["cp1vs1_valid"] is True

    assert result["top"]["cp1vs1"] == 38.0
    assert result["top"]["cp1vs1_valid"] is True
    assert result["top"]["duo"] == 34.15
    assert result["top"]["duo_valid"] is True

    assert result["bot"]["cp1vs1"] == 33.5
    assert result["bot"]["cp1vs1_valid"] is True
    assert result["bot"]["duo"] == 15.34
    assert result["bot"]["duo_valid"] is True

    assert result["top"]["cp1vs1"] != -result["bot"]["cp1vs1"]
    assert result["top"]["duo"] != -result["bot"]["duo"]
