"""ProTracker TG: only Lane_adv_protracker + Protracker_1vs1 + Protracker_duo.

- Lane_adv_protracker: lane win @10 (cp1v1 lane + duo_lane folded in)
- Protracker_1vs1: match WR counterpick 1v1
- Protracker_duo: match WR duo synergy
No other ProTracker lines in TG.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
ROOT = BASE_DIR.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_lane_advantage_uses_lane_adv_not_match_wr_for_cp_and_duo_lane(monkeypatch):
    import dota2protracker as protracker

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

    def fake_matchup(_hd, r, d, rp, dp, _mg, **kwargs):
        metric = kwargs.get("metric", "match_wr")
        if metric == "lane_adv":
            return (10.0, 50)  # lane NW proxy
        return (99.0, 50)  # match WR — must not leak into lane_advantage

    def fake_duo(
        _hd, rh1, rh2, rp1, rp2, dh1, dh2, dp1, dp2, _mg, **kwargs
    ):
        metric = kwargs.get("metric", "match_wr")
        if metric == "lane_adv":
            return (4.0, 40)
        return (88.0, 40)

    monkeypatch.setattr(protracker, "_get_matchup_1v1", fake_matchup)
    monkeypatch.setattr(protracker, "_get_duo_synergy_pair", fake_duo)

    result = protracker.calculate_lane_advantage(
        radiant_positions, dire_positions, hero_data={}, min_games=10
    )
    # mid cp + top/bot cp + top/bot duo_lane all  lane_adv=10 or 4
    assert result["mid"]["cp1vs1"] == 10.0
    assert result["top"]["duo_lane"] == 4.0
    assert result["bot"]["duo_lane"] == 4.0
    assert result["top"]["duo"] == 88.0  # match WR separate
    # lane_advantage averages only lane components, never 99/88
    assert abs(result["lane_advantage"] - (10.0 * 3 + 4.0 * 2) / 5) < 1e-6
    assert result["lane_metric"] == "lane_adv"
    assert result["duo_metric"] == "match_wr"


def test_enrich_sets_aggregated_pro_duo_lane(monkeypatch):
    import dota2protracker as protracker

    def fake_lane(*_a, **_k):
        return {
            "mid": {"cp1vs1": 1.0, "cp1vs1_valid": True, "cp1vs1_games": 10,
                    "duo": 0.0, "duo_valid": False, "duo_games": 0,
                    "duo_lane": 0.0, "duo_lane_valid": False, "duo_lane_games": 0},
            "top": {"cp1vs1": 2.0, "cp1vs1_valid": True, "cp1vs1_games": 10,
                    "duo": 9.0, "duo_valid": True, "duo_games": 20,
                    "duo_lane": 3.0, "duo_lane_valid": True, "duo_lane_games": 12},
            "bot": {"cp1vs1": 3.0, "cp1vs1_valid": True, "cp1vs1_games": 10,
                    "duo": 8.0, "duo_valid": True, "duo_games": 20,
                    "duo_lane": 5.0, "duo_lane_valid": True, "duo_lane_games": 14},
            "lane_advantage": 2.8,
            "cp1vs1_valid": True,
            "duo_valid": True,
            "duo_lane_valid": True,
            "duo_metric": "match_wr",
            "lane_metric": "lane_adv",
        }

    monkeypatch.setattr(protracker, "calculate_lane_advantage", fake_lane)
    monkeypatch.setattr(
        protracker,
        "_extract_team_positions_and_cores",
        lambda team: (
            [("pos1", "A"), ("pos2", "B"), ("pos3", "C"), ("pos4", "D"), ("pos5", "E")],
            ["A", "B", "C"],
            {},
        ),
    )
    monkeypatch.setattr(
        protracker,
        "_calculate_cp1vs1_all_positions",
        lambda *a, **k: (True, {"scores": [1.0], "games": 10, "count": 1,
                                 "radiant_core_coverage": {}, "dire_core_coverage": {},
                                 "radiant_core_vs_core_coverage": {},
                                 "dire_core_vs_core_coverage": {},
                                 "required_core_vs_core": 2}),
    )
    monkeypatch.setattr(
        protracker,
        "_calculate_duo_synergy_all_positions",
        lambda *a, **k: (True, {"scores": [2.0], "games": 5, "count": 2,
                                 "core_coverage": {}, "required_per_core": 1}),
    )
    monkeypatch.setattr(protracker, "parse_hero_matchups", lambda *_a, **_k: {})

    heroes = {
        "pos1": {"hero_id": 1, "name": "A"},
        "pos2": {"hero_id": 2, "name": "B"},
        "pos3": {"hero_id": 3, "name": "C"},
        "pos4": {"hero_id": 4, "name": "D"},
        "pos5": {"hero_id": 5, "name": "E"},
    }
    out = protracker.enrich_with_pro_tracker(heroes, heroes, {}, min_games=10)

    assert out.get("pro_lane_advantage") == 2.8
    assert out.get("pro_duo_lane_valid") is True
    assert abs(float(out.get("pro_duo_lane")) - 4.0) < 1e-6  # (3+5)/2
    assert out.get("pro_duo_lane_metric") == "lane_adv"
    assert out.get("pro_duo_synergy_metric") == "match_wr"


def test_tg_labels_lane_duo_1vs1():
    import cyberscore_try as runtime

    payload = {
        "pro_cp1vs1_late": 6.5,
        "pro_cp1vs1_valid": True,
        "pro_duo_synergy_late": 12.0,
        "pro_duo_synergy_valid": True,
        "pro_duo_lane": 3.5,  # folded into Lane_adv_protracker, not a TG line
        "pro_duo_lane_valid": True,
        "pro_lane_advantage": 4.2,
    }
    block = runtime._build_dota2protracker_block(payload)
    assert "Protracker_1vs1:" in block
    assert "6.5" in block or "+6.50" in block
    assert "Protracker_duo:" in block
    # match-WR duo (12.0), not lane duo_lane (3.5)
    for line in block.splitlines():
        if line.startswith("Protracker_duo:"):
            assert "12" in line
            assert "3.5" not in line

    lane_line = runtime._build_dota2protracker_lane_adv_line(payload)
    assert lane_line.startswith("Lane_adv_protracker:")
    assert "4.20" in lane_line or "+4.20" in lane_line

    only = runtime._build_dota2protracker_only_message(
        radiant_team_name="R",
        dire_team_name="D",
        live_league={},
        protracker_payload=payload,
    )
    assert "Lane_adv_protracker:" in only
    assert "Protracker_1vs1:" in only
    assert "Protracker_duo:" in only



def test_all_star_output_includes_match_wr_protracker_duo_under_1vs1():
    import cyberscore_try as runtime

    payload = {
        "pro_cp1vs1_late": 2.5,
        "pro_cp1vs1_valid": True,
        "pro_duo_synergy_late": 4.0,  # match WR duo for All
        "pro_duo_synergy_valid": True,
        "pro_duo_lane": 1.25,  # lane 1+1 — must NOT become Protracker_duo
        "pro_duo_lane_valid": True,
        "pro_lane_advantage": 3.0,
    }
    out = runtime._build_dota2protracker_star_output(payload)
    assert out["dota2protracker_cp1vs1"] == 2.5
    assert out["dota2protracker_duo"] == 4.0

    all_out = runtime._build_all_star_output(
        post_lane_output={"counterpick_1vs1": 1.0, "solo": 0.5},
        protracker_payload=payload,
    )
    assert all_out["dota2protracker_cp1vs1"] == 2.5
    assert all_out["dota2protracker_duo"] == 4.0
