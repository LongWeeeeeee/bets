from __future__ import annotations

import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import analise_database as ad  # noqa: E402


def _players():
    return [
        {"heroId": 1, "position": "POSITION_1", "isRadiant": True, "imp": 0},
        {"heroId": 2, "position": "POSITION_2", "isRadiant": True, "imp": 0},
        {"heroId": 3, "position": "POSITION_3", "isRadiant": True, "imp": 0},
        {"heroId": 4, "position": "POSITION_4", "isRadiant": True, "imp": 0},
        {"heroId": 5, "position": "POSITION_5", "isRadiant": True, "imp": 0},
        {"heroId": 6, "position": "POSITION_1", "isRadiant": False, "imp": 0},
        {"heroId": 7, "position": "POSITION_2", "isRadiant": False, "imp": 0},
        {"heroId": 8, "position": "POSITION_3", "isRadiant": False, "imp": 0},
        {"heroId": 9, "position": "POSITION_4", "isRadiant": False, "imp": 0},
        {"heroId": 10, "position": "POSITION_5", "isRadiant": False, "imp": 0},
    ]


def _base_match():
    return {
        "id": "123",
        "players": _players(),
        "topLaneOutcome": "DIRE_WIN",
        "midLaneOutcome": "RADIANT_WIN",
        "bottomLaneOutcome": "RADIANT_WIN",
        "radiantNetworthLeads": [],
        "didRadiantWin": True,
    }


def test_normalize_comparable_id_str_int_equivalence():
    assert ad._normalize_comparable_id("123") == 123
    assert ad._normalize_comparable_id(123) == 123
    assert ad._normalize_comparable_id(" 456 ") == 456
    assert ad._normalize_comparable_id(None) is None
    assert ad._normalize_comparable_id("") is None
    assert ad._normalize_comparable_id("abc") == "abc"


def test_normalize_comparable_id_rejects_bool_fractional_nonfinite():
    assert ad._normalize_comparable_id(True) is None
    assert ad._normalize_comparable_id(False) is None
    assert ad._normalize_comparable_id(1.5) is None
    assert ad._normalize_comparable_id(-2.7) is None
    assert ad._normalize_comparable_id(float("nan")) is None
    assert ad._normalize_comparable_id(float("inf")) is None
    assert ad._normalize_comparable_id(float("-inf")) is None
    assert ad._normalize_comparable_id("   ") is None
    # Integer-valued floats still normalize; negatives as strings too.
    assert ad._normalize_comparable_id(-2.0) == -2
    assert ad._normalize_comparable_id("-99") == -99


def test_lanes_returns_true_when_lane_stats_added():
    lane_dict = {}

    updated = ad.lanes(_base_match(), lane_dict)

    assert updated is True
    assert lane_dict


def test_lanes_returns_false_when_positions_incomplete():
    lane_dict = {}
    match = _base_match()
    # Drop Radiant pos5 so bot lane can't form; clear outcomes so nothing writes.
    match["players"] = [p for p in match["players"] if not (p["isRadiant"] and p["position"] == "POSITION_5")]
    match["topLaneOutcome"] = ""
    match["midLaneOutcome"] = ""
    match["bottomLaneOutcome"] = ""

    updated = ad.lanes(match, lane_dict)

    assert updated is False
    assert lane_dict == {}


def test_lanes_returns_false_when_lane_outcomes_invalid():
    lane_dict = {}
    match = _base_match()
    match["topLaneOutcome"] = "UNKNOWN"
    match["midLaneOutcome"] = ""
    match["bottomLaneOutcome"] = None

    updated = ad.lanes(match, lane_dict)

    assert updated is False
    assert lane_dict == {}


def test_analise_database_normalizes_exclude_match_ids_str_vs_int():
    lane_dict = {}

    # match id is str "123", exclude set holds int 123
    updated = ad.analise_database(
        _base_match(),
        lane_dict=lane_dict,
        early_dict=None,
        late_dict=None,
        exclude_match_ids={123},
    )

    assert updated is False
    assert lane_dict == {}


def test_analise_database_normalizes_exclude_match_ids_int_vs_str():
    lane_dict = {}
    match = _base_match()
    match["id"] = 123

    updated = ad.analise_database(
        match,
        lane_dict=lane_dict,
        early_dict=None,
        late_dict=None,
        exclude_match_ids={"123"},
    )

    assert updated is False
    assert lane_dict == {}


def test_analise_database_uses_match_id_hint_for_exclusion():
    lane_dict = {}
    match = _base_match()
    match.pop("id", None)

    updated = ad.analise_database(
        match,
        lane_dict=lane_dict,
        early_dict=None,
        late_dict=None,
        exclude_match_ids={"123"},
        match_id_hint=123,
    )

    assert updated is False
    assert lane_dict == {}


def test_analise_database_excludes_via_map_id_field():
    lane_dict = {}
    match = _base_match()
    match.pop("id", None)
    match["_map_id"] = 123

    updated = ad.analise_database(
        match,
        lane_dict=lane_dict,
        early_dict=None,
        late_dict=None,
        exclude_match_ids={"123"},
    )

    assert updated is False
    assert lane_dict == {}


def test_match_in_exclude_set_does_not_use_stale_normalization_cache():
    exclude_match_ids = {"123"}
    match = _base_match()
    match.pop("id", None)
    match.pop("match_id", None)

    assert ad._match_in_exclude_set(match, exclude_match_ids, match_id_hint=123) is True

    exclude_match_ids.add("456")

    assert ad._match_in_exclude_set(match, exclude_match_ids, match_id_hint=456) is True
    # Original hint still works after mutation of the same set object
    assert ad._match_in_exclude_set(match, exclude_match_ids, match_id_hint=123) is True


def test_analise_database_returns_false_when_no_dict_was_updated():
    # All target dicts None: nothing can be written even with a full match.
    updated = ad.analise_database(
        _base_match(),
        lane_dict=None,
        early_dict=None,
        late_dict=None,
        post_lane_dict=None,
    )
    assert updated is False

    # Incomplete positions: extract_heroes_by_position fails, no writes.
    early_dict = {}
    late_dict = {}
    match = _base_match()
    match["players"] = match["players"][:5]
    updated = ad.analise_database(
        match,
        lane_dict=None,
        early_dict=early_dict,
        late_dict=late_dict,
    )
    assert updated is False
    assert early_dict == {}
    assert late_dict == {}


def test_analise_database_returns_true_when_lane_dict_updated():
    lane_dict = {}

    updated = ad.analise_database(
        _base_match(),
        lane_dict=lane_dict,
        early_dict=None,
        late_dict=None,
    )

    assert updated is True
    assert lane_dict
