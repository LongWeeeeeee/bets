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


def _put_exact_two_of_three_core_cp(data: dict, radiant: dict, dire: dict, games: int) -> None:
    for radiant_pos, dire_pos in (
        ("pos1", "pos1"),
        ("pos1", "pos2"),
        ("pos2", "pos1"),
        ("pos2", "pos3"),
        ("pos3", "pos2"),
        ("pos3", "pos3"),
    ):
        _put_vs(
            data,
            f"{int(radiant[radiant_pos]['hero_id'])}{radiant_pos}",
            f"{int(dire[dire_pos]['hero_id'])}{dire_pos}",
            0.8,
            games,
        )


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


def _reverse_trio_keys(data: dict) -> dict:
    rewritten = {}
    for key, value in data.items():
        if "," in key and "_vs_" not in key and "_with_" not in key:
            key = ",".join(reversed(key.split(",")))
        rewritten[key] = value
    return rewritten


def _reverse_with_keys(data: dict) -> dict:
    rewritten = {}
    for key, value in data.items():
        if "_with_" in key:
            left, right = key.split("_with_", 1)
            key = f"{right}_with_{left}"
        rewritten[key] = value
    return rewritten


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
    assert "solo" not in post_lane_output
    assert post_lane_output["counterpick_1vs1"] > 0
    assert post_lane_output["counterpick_1vs2"] > 0
    assert post_lane_output["synergy_duo"] > 0
    assert post_lane_output["synergy_trio"] > 0


def test_synergy_trio_accepts_any_key_order_in_pipeline() -> None:
    radiant = _side(1)
    dire = _side(6)
    post_lane_dict = _reverse_trio_keys(_build_post_lane_stats(radiant, dire))

    result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict={},
        mid_dict={},
        post_lane_dict=post_lane_dict,
    )

    assert result["post_lane_output"]["synergy_trio"] > 0
    assert result["post_lane_output"]["synergy_trio_games"] > 0


def test_synergy_duo_accepts_either_with_key_order() -> None:
    radiant = _side(1)
    dire = _side(6)
    post_lane_dict = _reverse_with_keys(_build_post_lane_stats(radiant, dire))

    result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict={},
        mid_dict={},
        post_lane_dict=post_lane_dict,
    )

    assert result["post_lane_output"]["synergy_duo"] > 0
    assert result["post_lane_output"]["synergy_duo_games"] > 0


def test_synergy_trio_dedupes_identical_order_aliases() -> None:
    radiant = _side(1)
    data = {
        "1pos1,2pos2,3pos3": {"wins": 12, "draws": 0, "games": 15},
        "3pos3,2pos2,1pos1": {"wins": 12, "draws": 0, "games": 15},
    }
    output: dict = {}

    functions.synergy_team(
        radiant,
        output,
        "radiant_synergy",
        data,
        min_matches_trio=15,
    )

    assert output["radiant_synergy_trio"] == [(0.8, 15)]


def test_counterpick_1vs2_accepts_duo_key_order_and_reverse_side() -> None:
    radiant = _side(1)
    dire = _side(6)
    data = {
        "7pos2,6pos1_vs_1pos1": {"wins": 3, "draws": 0, "games": 20},
    }
    output: dict = {}

    functions.counterpick_team(
        radiant,
        dire,
        output,
        "radiant_counterpick",
        data,
        min_matches_1vs2=15,
    )

    item = output["radiant_counterpick_1vs2"]["pos1"][0]
    assert item[0] == 0.85
    assert item[1] == 20


def test_lane_vs_lookup_accepts_reversed_side_and_group_order() -> None:
    data = {
        "7pos4,6pos3_vs_5pos5,1pos1": {"wins": 12, "draws": 0, "games": 60},
    }

    stats, invert, _left, _right = functions._get_lane_stats_for_key(
        "1pos1,5pos5_vs_6pos3,7pos4",
        data,
    )
    counts = functions._lane_stats_to_counts(stats, invert=invert)

    assert counts == (48, 0, 12, 60)


def test_lane_with_lookup_accepts_either_pair_order() -> None:
    data = {
        "5pos5_with_1pos1": {"wins": 36, "draws": 0, "games": 60},
    }

    stats = functions._aggregate_lane_with_stats(data, "1pos1", "5pos5")
    counts = functions._lane_stats_to_counts(stats)

    assert counts == (36, 0, 24, 60)


def test_pos1_vs_pos1_is_not_emitted_even_with_enough_sample() -> None:
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
    assert "pos1_vs_pos1" not in enough_result["early_output"]
    assert "pos1_vs_pos1_games" not in enough_result["early_output"]


def test_pos1_vs_pos1_aggregates_directional_samples() -> None:
    radiant = {
        "pos1": {"hero_id": 114, "hero_name": "Monkey King"},
        "pos2": {"hero_id": 38, "hero_name": "Beastmaster"},
        "pos3": {"hero_id": 65, "hero_name": "Batrider"},
        "pos4": {"hero_id": 51, "hero_name": "Clockwerk"},
        "pos5": {"hero_id": 31, "hero_name": "Lich"},
    }
    dire = {
        "pos1": {"hero_id": 41, "hero_name": "Faceless Void"},
        "pos2": {"hero_id": 43, "hero_name": "Death Prophet"},
        "pos3": {"hero_id": 99, "hero_name": "Bristleback"},
        "pos4": {"hero_id": 86, "hero_name": "Rubick"},
        "pos5": {"hero_id": 58, "hero_name": "Enchantress"},
    }
    data = {
        "114pos1_vs_41pos1": {"wins": 3, "draws": 0, "games": 37},
        "41pos1_vs_114pos1": {"wins": 25, "draws": 0, "games": 31},
    }

    result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict={},
        mid_dict=data,
    )

    late_output = result["mid_output"]
    assert "pos1_vs_pos1" not in late_output
    assert "pos1_vs_pos1_games" not in late_output


def test_pos1_vs_pos1_reads_reverse_only_direction() -> None:
    radiant = _side(114)
    dire = _side(41)
    data = {
        "41pos1_vs_114pos1": {
            "wins": 25,
            "draws": 0,
            "games": functions.POS1_VS_POS1_MIN_MATCHES + 1,
        },
    }

    result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict=data,
        mid_dict={},
    )

    early_output = result["early_output"]
    assert "pos1_vs_pos1" not in early_output
    assert "pos1_vs_pos1_games" not in early_output


def test_counterpick_1vs1_requires_two_of_three_core_matchups_per_core() -> None:
    radiant = _side(1)
    dire = _side(6)
    data: dict = {}
    _put_exact_two_of_three_core_cp(data, radiant, dire, functions.COUNTERPICK_1VS1_MIN_MATCHES)

    result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict=data,
        mid_dict={},
    )

    assert result["early_output"]["counterpick_1vs1"] > 0
    assert result["early_output"]["counterpick_1vs1_games"] > 0


def test_counterpick_1vs1_rejects_one_of_three_core_matchups() -> None:
    radiant = _side(1)
    dire = _side(6)
    data: dict = {}

    for radiant_pos, dire_pos in (
        ("pos1", "pos1"),
        ("pos1", "pos2"),
        ("pos2", "pos1"),
        ("pos2", "pos2"),
        ("pos3", "pos3"),
    ):
        _put_vs(
            data,
            f"{int(radiant[radiant_pos]['hero_id'])}{radiant_pos}",
            f"{int(dire[dire_pos]['hero_id'])}{dire_pos}",
            0.8,
            functions.COUNTERPICK_1VS1_MIN_MATCHES,
        )

    result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict=data,
        mid_dict={},
    )

    assert result["early_output"]["counterpick_1vs1"] is None
    assert result["early_output"]["counterpick_1vs1_games"] == 0


def test_post_lane_counterpick_1vs1_uses_two_of_three_core_gate() -> None:
    radiant = _side(1)
    dire = _side(6)
    data: dict = {}
    _put_exact_two_of_three_core_cp(data, radiant, dire, functions.POST_LANE_COUNTERPICK_1VS1_MIN_MATCHES)

    result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict={},
        mid_dict={},
        post_lane_dict=data,
    )

    assert result["post_lane_output"]["counterpick_1vs1"] > 0
    assert result["post_lane_output"]["counterpick_1vs1_games"] > 0


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
    assert not any(key.startswith("synergy_duo_") for key in early_output)


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
    assert not any(key.startswith("synergy_duo_") for key in early_output)
