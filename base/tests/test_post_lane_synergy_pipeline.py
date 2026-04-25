from __future__ import annotations

import itertools
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import functions  # noqa: E402


POSITIONS = ("pos1", "pos2", "pos3", "pos4", "pos5")


def _side(start_hero_id: int) -> dict:
    return {
        pos: {"hero_id": start_hero_id + idx, "hero_name": f"hero_{start_hero_id + idx}"}
        for idx, pos in enumerate(POSITIONS)
    }


def _hero_key(item: tuple[str, dict]) -> str:
    pos, payload = item
    return f"{int(payload['hero_id'])}{pos}"


def _put_stats(data: dict, key: str, wr: float, games: int) -> None:
    data[key] = {"wins": int(round(wr * games)), "games": games}


def _put_vs(data: dict, left: str, right: str, left_wr: float, games: int) -> None:
    if left <= right:
        key = f"{left}_vs_{right}"
        wr = left_wr
    else:
        key = f"{right}_vs_{left}"
        wr = 1.0 - left_wr
    _put_stats(data, key, wr, games)


def _put_duo(data: dict, side: dict, left_pos: str, right_pos: str, wr: float, games: int) -> None:
    pair = sorted([
        f"{int(side[left_pos]['hero_id'])}{left_pos}",
        f"{int(side[right_pos]['hero_id'])}{right_pos}",
    ])
    _put_stats(data, f"{pair[0]}_with_{pair[1]}", wr, games)


def _build_post_lane_stats(radiant: dict, dire: dict) -> dict:
    games = max(
        functions.SOLO_MIN_MATCHES,
        functions.SYNERGY_DUO_MIN_MATCHES,
        functions.SYNERGY_TRIO_MIN_MATCHES,
        functions.COUNTERPICK_1VS1_MIN_MATCHES,
        functions.COUNTERPICK_1VS2_MIN_MATCHES,
        functions.GET_DIFF_MIN_MATCHES,
    ) + 100
    data: dict = {}
    radiant_items = list(radiant.items())
    dire_items = list(dire.items())

    for item in radiant_items:
        _put_stats(data, _hero_key(item), 0.8, games)
    for item in dire_items:
        _put_stats(data, _hero_key(item), 0.2, games)

    for team_items, wr in ((radiant_items, 0.8), (dire_items, 0.2)):
        for a, b in itertools.combinations(team_items, 2):
            pair = sorted([_hero_key(a), _hero_key(b)])
            _put_stats(data, f"{pair[0]}_with_{pair[1]}", wr, games)
        for trio in itertools.combinations(team_items, 3):
            trio_key = ",".join(sorted(_hero_key(item) for item in trio))
            _put_stats(data, trio_key, wr, games)

    for r_item in radiant_items:
        r_key = _hero_key(r_item)
        for d_item in dire_items:
            _put_vs(data, r_key, _hero_key(d_item), 0.8, games)
        for d_duo in itertools.combinations(dire_items, 2):
            d_duo_key = ",".join(sorted(_hero_key(item) for item in d_duo))
            _put_vs(data, r_key, d_duo_key, 0.8, games)

    for d_item in dire_items:
        d_key = _hero_key(d_item)
        for r_duo in itertools.combinations(radiant_items, 2):
            r_duo_key = ",".join(sorted(_hero_key(item) for item in r_duo))
            _put_vs(data, d_key, r_duo_key, 0.2, games)

    return data


def test_synergy_and_counterpick_emits_post_lane_output() -> None:
    radiant = _side(1)
    dire = _side(6)
    post_lane_dict = _build_post_lane_stats(radiant, dire)

    result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict={},
        mid_dict={},
        post_lane_dict=post_lane_dict,
    )

    post_lane_output = result.get("post_lane_output")
    assert isinstance(post_lane_output, dict)
    assert post_lane_output["solo"] > 0
    assert post_lane_output["counterpick_1vs1"] > 0
    assert post_lane_output["counterpick_1vs2"] > 0
    assert post_lane_output["synergy_duo"] > 0
    assert post_lane_output["synergy_trio"] > 0


def test_pos1_vs_pos1_uses_separate_sample_gate() -> None:
    radiant = _side(1)
    dire = _side(6)
    radiant_pos1 = f"{radiant['pos1']['hero_id']}pos1"
    dire_pos1 = f"{dire['pos1']['hero_id']}pos1"

    low_sample: dict = {}
    _put_vs(
        low_sample,
        radiant_pos1,
        dire_pos1,
        0.8,
        functions.POS1_VS_POS1_MIN_MATCHES - 1,
    )
    low_result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict=low_sample,
        mid_dict={},
    )
    assert "pos1_vs_pos1" not in low_result["early_output"]

    enough_sample: dict = {}
    _put_vs(
        enough_sample,
        radiant_pos1,
        dire_pos1,
        0.8,
        functions.POS1_VS_POS1_MIN_MATCHES,
    )
    enough_result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict=enough_sample,
        mid_dict={},
    )
    assert enough_result["early_output"]["pos1_vs_pos1"] > 0
    assert enough_result["early_output"]["pos1_vs_pos1_games"] == functions.POS1_VS_POS1_MIN_MATCHES


def test_synergy_duo_requires_core_pair_coverage_for_primary_metric() -> None:
    radiant = _side(1)
    dire = _side(6)
    data: dict = {}

    _put_duo(data, radiant, "pos1", "pos2", 0.8, functions.SYNERGY_DUO_MIN_MATCHES)
    _put_duo(data, dire, "pos1", "pos2", 0.2, functions.SYNERGY_DUO_MIN_MATCHES)

    result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict=data,
        mid_dict={},
    )

    early_output = result["early_output"]
    assert "synergy_duo" not in early_output
    assert early_output["synergy_duo_partial"] > 0
    assert early_output["synergy_duo_partial_reason"] == "partial_core_duo_coverage"


def test_synergy_duo_drops_primary_metric_on_core_support_conflict() -> None:
    radiant = _side(1)
    dire = _side(6)
    data: dict = {}
    games = functions.SYNERGY_DUO_MIN_MATCHES

    for left_pos, right_pos in (("pos1", "pos2"), ("pos1", "pos3")):
        _put_duo(data, radiant, left_pos, right_pos, 0.8, games)
        _put_duo(data, dire, left_pos, right_pos, 0.2, games)
    _put_duo(data, radiant, "pos4", "pos5", 0.2, games)
    _put_duo(data, dire, "pos4", "pos5", 0.8, games)

    result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict=data,
        mid_dict={},
    )

    early_output = result["early_output"]
    assert "synergy_duo" not in early_output
    assert early_output["synergy_duo_conflict"] is True
    assert early_output["synergy_duo_core"] > 0
    assert early_output["synergy_duo_support"] < 0
    assert early_output["synergy_duo_partial_reason"] == "core_support_conflict"
