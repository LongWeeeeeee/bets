from __future__ import annotations

import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import dota2protracker as protracker  # noqa: E402


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
