from __future__ import annotations

import sys
from pathlib import Path

import pytest


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import functions as lane_functions  # noqa: E402
import maps_research  # noqa: E402


def _heroes(seed: int):
    return {
        "pos1": {"hero_id": seed + 1},
        "pos2": {"hero_id": seed + 2},
        "pos3": {"hero_id": seed + 3},
        "pos4": {"hero_id": seed + 4},
        "pos5": {"hero_id": seed + 5},
    }


def _set_empty_position_counts(monkeypatch):
    monkeypatch.setattr(maps_research, "HERO_VALID_POSITIONS_COUNTS", {}, raising=False)
    monkeypatch.setattr(maps_research, "HERO_VALID_POSITIONS_COUNTS_MIN_GAMES", 100, raising=False)


def test_check_match_quality_strict_lane_positions_rejects_single_invalid_slot(monkeypatch):
    _set_empty_position_counts(monkeypatch)
    monkeypatch.setattr(
        maps_research,
        "HERO_VALID_POSITIONS",
        {
            1: ["pos5"],
            2: ["pos2"],
            3: ["pos3"],
            4: ["pos4"],
            5: ["pos5"],
            6: ["pos1"],
            7: ["pos2"],
            8: ["pos3"],
            9: ["pos4"],
            10: ["pos5"],
        },
        raising=False,
    )
    match = {
        "players": [
            {"heroId": 1, "position": "pos1", "intentionalFeeding": False, "isRadiant": True, "networth": 1000},
            {"heroId": 2, "position": "pos2", "intentionalFeeding": False, "isRadiant": True, "networth": 2000},
            {"heroId": 3, "position": "pos3", "intentionalFeeding": False, "isRadiant": True, "networth": 3000},
            {"heroId": 4, "position": "pos4", "intentionalFeeding": False, "isRadiant": True, "networth": 1500},
            {"heroId": 5, "position": "pos5", "intentionalFeeding": False, "isRadiant": True, "networth": 800},
            {"heroId": 6, "position": "pos1", "intentionalFeeding": False, "isRadiant": False, "networth": 1000},
            {"heroId": 7, "position": "pos2", "intentionalFeeding": False, "isRadiant": False, "networth": 2000},
            {"heroId": 8, "position": "pos3", "intentionalFeeding": False, "isRadiant": False, "networth": 3000},
            {"heroId": 9, "position": "pos4", "intentionalFeeding": False, "isRadiant": False, "networth": 1500},
            {"heroId": 10, "position": "pos5", "intentionalFeeding": False, "isRadiant": False, "networth": 800},
        ]
    }

    assert maps_research.check_match_quality(match, strict_lane_positions=False) == (True, "ok")
    assert maps_research.check_match_quality(match, strict_lane_positions=True) == (
        False,
        "invalid positions strict",
    )


def test_check_match_quality_strict_lane_positions_requires_catalog(monkeypatch):
    _set_empty_position_counts(monkeypatch)
    monkeypatch.setattr(maps_research, "HERO_VALID_POSITIONS", {}, raising=False)

    match = {
        "players": [
            {"heroId": 1, "position": "pos1", "intentionalFeeding": False, "isRadiant": True, "networth": 1000},
            {"heroId": 2, "position": "pos2", "intentionalFeeding": False, "isRadiant": True, "networth": 2000},
            {"heroId": 3, "position": "pos3", "intentionalFeeding": False, "isRadiant": True, "networth": 3000},
            {"heroId": 4, "position": "pos4", "intentionalFeeding": False, "isRadiant": True, "networth": 1500},
            {"heroId": 5, "position": "pos5", "intentionalFeeding": False, "isRadiant": True, "networth": 800},
            {"heroId": 6, "position": "pos1", "intentionalFeeding": False, "isRadiant": False, "networth": 1000},
            {"heroId": 7, "position": "pos2", "intentionalFeeding": False, "isRadiant": False, "networth": 2000},
            {"heroId": 8, "position": "pos3", "intentionalFeeding": False, "isRadiant": False, "networth": 3000},
            {"heroId": 9, "position": "pos4", "intentionalFeeding": False, "isRadiant": False, "networth": 1500},
            {"heroId": 10, "position": "pos5", "intentionalFeeding": False, "isRadiant": False, "networth": 800},
        ]
    }

    assert maps_research.check_match_quality(match, strict_lane_positions=True) == (
        False,
        "hero valid positions unavailable",
    )


def test_check_match_quality_accepts_secondary_role_from_counts_catalog(monkeypatch):
    monkeypatch.setattr(maps_research, "HERO_VALID_POSITIONS", {41: ["POSITION_1", "POSITION_3"]}, raising=False)
    monkeypatch.setattr(
        maps_research,
        "HERO_VALID_POSITIONS_COUNTS",
        {41: {"POSITION_4": 813}},
        raising=False,
    )
    monkeypatch.setattr(maps_research, "HERO_VALID_POSITIONS_COUNTS_MIN_GAMES", 100, raising=False)

    match = {
        "players": [
            {"heroId": 1, "position": "POSITION_1", "intentionalFeeding": False, "isRadiant": True, "networth": 1000},
            {"heroId": 2, "position": "POSITION_2", "intentionalFeeding": False, "isRadiant": True, "networth": 2000},
            {"heroId": 3, "position": "POSITION_3", "intentionalFeeding": False, "isRadiant": True, "networth": 3000},
            {"heroId": 41, "position": "POSITION_4", "intentionalFeeding": False, "isRadiant": True, "networth": 1500},
            {"heroId": 5, "position": "POSITION_5", "intentionalFeeding": False, "isRadiant": True, "networth": 800},
            {"heroId": 6, "position": "POSITION_1", "intentionalFeeding": False, "isRadiant": False, "networth": 1000},
            {"heroId": 7, "position": "POSITION_2", "intentionalFeeding": False, "isRadiant": False, "networth": 2000},
            {"heroId": 8, "position": "POSITION_3", "intentionalFeeding": False, "isRadiant": False, "networth": 3000},
            {"heroId": 9, "position": "POSITION_4", "intentionalFeeding": False, "isRadiant": False, "networth": 1500},
            {"heroId": 10, "position": "POSITION_5", "intentionalFeeding": False, "isRadiant": False, "networth": 800},
        ]
    }

    assert maps_research.check_match_quality(match, strict_lane_positions=True) == (True, "ok")


@pytest.mark.parametrize(
    ("stats", "should_use"),
    [
        ({"wins": 12, "draws": 0, "games": 15}, False),
        ({"value": [1] * 15}, False),
        ({"wins": 15, "draws": 0, "games": 20}, True),
        ({"value": [1] * 20}, True),
    ],
)
def test_get_values_uses_same_threshold_for_legacy_and_aggregated_stats(stats, should_use):
    output = {}
    lane_functions.get_values("bot_radiant", "1pos1,2pos5_vs_3pos3", {"1pos1,2pos5_vs_3pos3": stats}, output)

    if should_use:
        assert output["bot_radiant"]["win"]
    else:
        assert "bot_radiant" not in output or "win" not in output["bot_radiant"]


def test_both_found_weights_two_boxes_by_games():
    lane_data = {
        "top_radiant": {
            "win": [(0.80, 100)],
            "draw": [(0.10, 100)],
            "lose": [(0.10, 100)],
        },
        "top_dire": {
            "win": [(0.20, 10)],
            "draw": [(0.20, 10)],
            "lose": [(0.60, 10)],
        },
    }

    outcome, conf = lane_functions.both_found("top", lane_data, output={})

    assert outcome == "win"
    assert conf >= 70


def test_lane_2vs2_does_not_double_count_canonical_and_reverse_entries():
    radiant = _heroes(0)
    dire = _heroes(10)
    output = {}
    canonical_key = "3pos3,4pos4_vs_11pos1,15pos5"
    reverse_key = "11pos1,15pos5_vs_3pos3,4pos4"
    heroes_data = {
        "2v2_lanes": {
            canonical_key: {"wins": 10, "draws": 0, "games": 10},
            reverse_key: {"wins": 0, "draws": 0, "games": 10},
        }
    }

    lane_functions.lane_2vs2(radiant, dire, heroes_data, output)

    assert output["top"]["win"] > 80
    assert output["top"]["lose"] < 10


def test_merge_lane_predictions_blends_probability_buckets():
    counterpick_probs = {"win": 70.0, "draw": 20.0, "lose": 10.0, "games": 100}
    synergy_probs = {"win": 55.0, "draw": 25.0, "lose": 20.0, "games": 50}

    merged_probs = lane_functions._merge_lane_predictions(
        counterpick_probs,
        synergy_probs,
        return_probs=True,
    )
    outcome, conf = lane_functions._merge_lane_predictions(counterpick_probs, synergy_probs)

    assert merged_probs is not None
    assert merged_probs["win"] > merged_probs["draw"] > merged_probs["lose"]
    assert outcome == "win"
    assert conf >= 60


def test_synergy_lanes_uses_side_strength_proxy():
    radiant = _heroes(0)
    dire = _heroes(10)
    heroes_data = {
        "1_with_1_lanes": {
            "1pos1_with_5pos5": {"wins": 45, "draws": 5, "games": 60},
            "13pos3_with_14pos4": {"wins": 10, "draws": 5, "games": 60},
        }
    }

    outcome, conf = lane_functions.synergy_lanes(radiant, dire, heroes_data, "bot")

    assert outcome == "win"
    assert conf >= 55
