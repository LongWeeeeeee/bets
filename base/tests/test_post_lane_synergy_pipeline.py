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


def _base_stats_games() -> int:
    """Заполнение ячейки для базовой фикстуры: выше любого действующего порога."""
    return max(
        functions.SOLO_MIN_MATCHES,
        functions.SYNERGY_DUO_MIN_MATCHES,
        functions.SYNERGY_TRIO_MIN_MATCHES,
        functions.COUNTERPICK_1VS1_MIN_MATCHES,
        functions.COUNTERPICK_1VS2_MIN_MATCHES,
        functions.GET_DIFF_MIN_MATCHES,
    ) + 100


def _build_post_lane_stats(radiant: dict, dire: dict) -> dict:
    games = _base_stats_games()
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
    # Option C: post_lane-solo теперь эмитится (собран на последнем патче 7.41d),
    # раньше был выключен (name != 'post_lane_output').
    assert "solo" in post_lane_output
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


def test_synergy_trio_role_pool_combines_thin_exact_position_cells() -> None:
    data = {
        "1pos1,2pos2,3pos4": {"wins": 10, "draws": 0, "games": 14},
        "1pos3,2pos1,3pos5": {"wins": 6, "draws": 0, "games": 14},
    }

    value, games = functions._lookup_synergy_trio_role_pool_winrate(
        data,
        ["1pos1", "2pos2", "3pos4"],
    )

    assert games == 28
    assert value == 16 / 28


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


def test_pos1_vs_pos1_emitted_only_with_enough_sample() -> None:
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
    # Key is always present (init loop), but below the sample floor it stays None.
    assert low_result["early_output"].get("pos1_vs_pos1") is None

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
    # ONE-SIDED index from the Radiant pos1 perspective: (WR_radiant_pos1 - 50).
    # Radiant carry winrate 0.8 -> (0.8 - 0.5) * 100 = 30.
    assert enough_result["early_output"]["pos1_vs_pos1"] == 30
    assert (
        enough_result["early_output"]["pos1_vs_pos1_games"]
        == functions.POS1_VS_POS1_MIN_MATCHES
    )


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
    # Both directions aggregate to the Radiant carry winrate 9/68 ≈ 0.1324;
    # one-sided index = round((0.1324 - 0.5) * 100) = -37, over 37 + 31 = 68 games.
    assert late_output["pos1_vs_pos1"] == -37
    assert late_output["pos1_vs_pos1_games"] == 68


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
    # Reverse-only sample -> Radiant carry winrate = (31 - 25) / 31 ≈ 0.1935;
    # one-sided index = round((0.1935 - 0.5) * 100) = -31.
    assert early_output["pos1_vs_pos1"] == -31
    assert early_output["pos1_vs_pos1_games"] == functions.POS1_VS_POS1_MIN_MATCHES + 1


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


def test_counterpick_1vs1_uses_core_position_fallback_for_missing_matchup() -> None:
    radiant = _side(1)
    dire = _side(6)
    games = functions.COUNTERPICK_1VS1_MIN_MATCHES
    data: dict = {}

    for radiant_pos, dire_pos in (
        ("pos1", "pos1"),
        ("pos1", "pos2"),
        ("pos2", "pos1"),
        ("pos2", "pos2"),
        ("pos2", "pos3"),
        ("pos3", "pos1"),
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

    _put_vs(
        data,
        f"{int(radiant['pos1']['hero_id'])}pos1",
        f"{int(dire['pos3']['hero_id'])}pos2",
        0.8,
        games,
    )

    result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict=data,
        mid_dict={},
    )

    assert result["early_output"]["counterpick_1vs1"] > 0
    assert result["early_output"]["counterpick_1vs1_games"] > 0


def test_counterpick_1vs1_uses_support_position_fallback() -> None:
    radiant = _side(1)
    dire = _side(6)
    output: dict = {}
    games = functions.COUNTERPICK_1VS1_MIN_MATCHES
    data: dict = {}

    _put_vs(
        data,
        f"{int(radiant['pos1']['hero_id'])}pos1",
        f"{int(dire['pos4']['hero_id'])}pos5",
        0.8,
        games,
    )

    functions.counterpick_team(
        radiant,
        dire,
        output,
        "radiant_counterpick",
        data,
    )

    # Ячейка: (значение, СЫРЫЕ игры, позиция врага, вес). Игры и вес — разные
    # поля: игры читают диагностики `*_games`, вес идёт в агрегатор. Без режима
    # надёжности вес равен числу игр.
    assert output["radiant_counterpick_1vs1"]["pos1"] == [(0.8, games, "pos4", games)]


def test_counterpick_1vs1_role_topup_keeps_exact_anchor_and_caps_at_35() -> None:
    data: dict = {}
    left = "1pos1"
    right = "6pos1"
    _put_vs(data, left, right, 0.6, 10)
    _put_vs(data, left, "6pos2", 0.8, 50)

    value, games = functions._lookup_counterpick_1vs1_role_topup_winrate(
        data,
        left,
        right,
    )

    assert games == functions.COUNTERPICK_1VS1_ROLE_TOPUP_MIN_MATCHES == 35
    assert value == (0.6 * 10 + 0.8 * 25) / 35


def test_counterpick_1vs1_role_topup_does_not_dilute_covered_exact() -> None:
    data: dict = {}
    left = "1pos1"
    right = "6pos1"
    _put_vs(data, left, right, 0.6, 35)
    _put_vs(data, left, "6pos2", 0.9, 100)

    assert functions._lookup_counterpick_1vs1_role_topup_winrate(data, left, right) == (0.6, 35)


def test_counterpick_1vs1_role_topup_is_authority_only_near_high_boundary() -> None:
    select = functions._select_counterpick_1vs1_role_topup_score

    assert select(8, 7, "early_output") == 8
    assert select(9, 8, "early_output") == 8
    assert select(8, 9, "early_output") == 9
    assert select(10, 9, "mid_output") == 9
    assert select(7, 8, "post_lane_output") == 8


def test_counterpick_1vs2_does_not_use_position_fallback() -> None:
    radiant = _side(1)
    dire = _side(6)
    output: dict = {}
    games = functions.COUNTERPICK_1VS2_MIN_MATCHES
    data: dict = {}

    fallback_duo = ",".join(sorted([
        f"{int(dire['pos1']['hero_id'])}pos2",
        f"{int(dire['pos4']['hero_id'])}pos4",
    ]))
    _put_vs(
        data,
        f"{int(radiant['pos1']['hero_id'])}pos1",
        fallback_duo,
        0.8,
        games,
    )

    functions.counterpick_team(
        radiant,
        dire,
        output,
        "radiant_counterpick",
        data,
    )

    assert "radiant_counterpick_1vs2" not in output


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

def test_counterpick_1vs2_games_is_conservative_min_support() -> None:
    """counterpick_1vs2_games is min support across used core matchups/sides.

    Score/coverage must stay independent of the diagnostic games field.
    Unequal correlated matchup counts must not be summed.
    """
    radiant = _side(1)
    dire = _side(6)
    min_games = functions.COUNTERPICK_1VS2_MIN_MATCHES

    # Build full coverage with deliberately unequal games per matchup:
    # Radiant pos1 matchups: 20, 40, 60  -> pos min = 20
    # Radiant pos2 matchups: 30, 50, 70  -> pos min = 30
    # Radiant pos3 matchups: 25, 45, 65  -> pos min = 25
    # Radiant overall min = 20
    # Dire side: all matchups 15 (still >= min threshold) -> min = 15
    # Final counterpick_1vs2_games = min(20, 15) = 15  (NOT the sum)
    data: dict = {}

    def _put_cp1vs2(solo_side, duo_side, solo_pos, duo_a, duo_b, wr, games):
        solo = f"{int(solo_side[solo_pos]['hero_id'])}{solo_pos}"
        duo = ",".join(sorted([
            f"{int(duo_side[duo_a]['hero_id'])}{duo_a}",
            f"{int(duo_side[duo_b]['hero_id'])}{duo_b}",
        ]))
        _put_vs(data, solo, duo, wr, games)

    # Radiant core solos vs every Dire core duo, unequal games
    r_games = {
        ("pos1", "pos1", "pos2"): 20,
        ("pos1", "pos1", "pos3"): 40,
        ("pos1", "pos2", "pos3"): 60,
        ("pos2", "pos1", "pos2"): 30,
        ("pos2", "pos1", "pos3"): 50,
        ("pos2", "pos2", "pos3"): 70,
        ("pos3", "pos1", "pos2"): 25,
        ("pos3", "pos1", "pos3"): 45,
        ("pos3", "pos2", "pos3"): 65,
    }
    for (solo_pos, a, b), games in r_games.items():
        _put_cp1vs2(radiant, dire, solo_pos, a, b, 0.8, games)

    # Dire core solos vs every Radiant core duo, flat 15 games (conservative side)
    for solo_pos in ("pos1", "pos2", "pos3"):
        for a, b in (("pos1", "pos2"), ("pos1", "pos3"), ("pos2", "pos3")):
            _put_cp1vs2(dire, radiant, solo_pos, a, b, 0.2, 15)

    # Also seed 1vs1/synergy enough so other metrics don't block the path if needed.
    # Full post-lane stats for remaining keys:
    base = _build_post_lane_stats(radiant, dire)
    # Overlay our unequal cp1vs2 keys (base uses uniform games)
    base.update(data)

    result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict={},
        mid_dict={},
        post_lane_dict=base,
    )
    post = result["post_lane_output"]

    assert "counterpick_1vs2" in post
    score = post["counterpick_1vs2"]
    assert score is not None

    # Conservative min: min(radiant_min=20, dire_min=15) = 15
    assert post["counterpick_1vs2_games"] == 15

    # Sanity: helper alone on the side dicts used by counterpick_team shape.
    r_side = {
        "pos1": [(0.8, 20), (0.8, 40), (0.8, 60)],
        "pos2": [(0.8, 30), (0.8, 50), (0.8, 70)],
        "pos3": [(0.8, 25), (0.8, 45), (0.8, 65)],
    }
    d_side = {
        "pos1": [(0.2, 15), (0.2, 15), (0.2, 15)],
        "pos2": [(0.2, 15), (0.2, 15), (0.2, 15)],
        "pos3": [(0.2, 15), (0.2, 15), (0.2, 15)],
    }
    assert functions._min_support_games_from_pos_dict(r_side) == 20
    assert functions._min_support_games_from_pos_dict(d_side) == 15
    assert min(20, 15) == 15

    # Sum would be huge and must not be the reported diagnostic.
    r_sum = sum(g for vals in r_side.values() for _, g in vals)
    d_sum = sum(g for vals in d_side.values() for _, g in vals)
    assert post["counterpick_1vs2_games"] != min(r_sum, d_sum)
    assert post["counterpick_1vs2_games"] < min(r_sum, d_sum)

    # Score is independent: re-run with equal games still yields a score (gate unchanged).
    assert isinstance(score, (int, float))


def test_diagnostic_support_helpers_reject_invalid_and_use_min() -> None:
    """Shape-aware diagnostic effective-N: min of used entries, never sum.

    Invalid observation values (entry[0]) must never grant positive support,
    even when games (entry[1]) is a positive integer.
    """
    # Valid finite nonzero observations keep positive games support.
    assert functions._diagnostic_support_from_entry((0.6, 12)) == 12
    assert functions._diagnostic_support_from_entry((0.6, 12, "pos2")) == 12
    assert functions._diagnostic_support_from_entry((0.6, 20)) == 20
    assert functions._diagnostic_support_from_entry((-0.6, 20)) == 20

    # Invalid observation values must return 0 even with games=20.
    for bad_value in (
        None,
        0,
        -0.0,
        False,
        True,
        float("nan"),
        float("inf"),
        float("-inf"),
        "bad",
        "0.6",
    ):
        assert functions._diagnostic_support_from_entry((bad_value, 20)) == 0, bad_value

    # Invalid / nonpositive games still reject even with a valid observation.
    assert functions._diagnostic_support_from_entry((0.6, True)) == 0
    assert functions._diagnostic_support_from_entry((0.6, 0)) == 0
    assert functions._diagnostic_support_from_entry((0.6, -3)) == 0
    assert functions._diagnostic_support_from_entry((0.6, float("nan"))) == 0
    assert functions._diagnostic_support_from_entry("bad") == 0

    # Mixed list: invalid observations ignored; min over valid-only entries.
    assert functions._diagnostic_support_from_list(
        [(0.5, 20), (0.6, 10), (0.7, True), (0.8, 0)]
    ) == 10
    assert functions._diagnostic_support_from_list([]) == 0
    assert functions._diagnostic_support_from_list(
        [(None, 20), (0, 15), (False, 12), (True, 30), (float("nan"), 40), ("bad", 50)]
    ) == 0
    assert functions._diagnostic_support_from_list(
        [(None, 50), (0.6, 20), (float("inf"), 40), (-0.6, 15), ("0.6", 99)]
    ) == 15

    pos = {
        "pos1": [(0.8, 20), (0.8, 40), (0.8, float("inf"))],
        "pos2": [(0.8, 30), (0.8, False)],
        "pos3": [(0.8, 25)],
    }
    # min among all valid used = 20
    assert functions._diagnostic_support_from_pos_dict(pos) == 20
    # Backward-compatible alias used by cp1vs2 path
    assert functions._min_support_games_from_pos_dict(pos) == 20

    # Invalid-only position dict grants no support.
    invalid_only_pos = {
        "pos1": [(None, 20), (0, 30)],
        "pos2": [(False, 40), (float("nan"), 50)],
        "pos3": [("bad", 60), (True, 70)],
    }
    assert functions._diagnostic_support_from_pos_dict(invalid_only_pos) == 0
    assert functions._min_support_games_from_pos_dict(invalid_only_pos) == 0

    # Two-sided reducer
    assert functions._diagnostic_support_two_sides(20, 15) == 15
    assert functions._diagnostic_support_two_sides(20, 0) == 0
    assert functions._diagnostic_support_two_sides(None, 15) == 0
    # Either invalid-only side reduces to 0 after side reduction.
    assert functions._diagnostic_support_two_sides(
        functions._diagnostic_support_from_list([(None, 20), (0, 15)]),
        functions._diagnostic_support_from_list([(0.6, 18)]),
    ) == 0
    assert functions._diagnostic_support_two_sides(
        functions._diagnostic_support_from_list([(0.6, 18)]),
        functions._diagnostic_support_from_list([(True, 20), ("bad", 30)]),
    ) == 0


def test_all_draft_metrics_games_are_conservative_min_across_phases() -> None:
    """All six *_games diagnostics use effective-N min, in early/mid/post_lane.

    Scores must remain present and independent of the diagnostic field.
    """
    radiant = _side(1)
    dire = _side(6)

    # Full-coverage base first (high equal games), then overwrite selected
    # families with unequal support so min << sum while gates still pass.
    base = _build_post_lane_stats(radiant, dire)

    # Пороги берём из констант: тест проверяет семантику min, а не конкретные
    # числа, и не должен краснеть при каждом пересмотре порогов.
    solo_min = functions.SOLO_MIN_MATCHES
    cp1_min = max(functions.COUNTERPICK_1VS1_MIN_MATCHES,
                  functions.POST_LANE_COUNTERPICK_1VS1_MIN_MATCHES)
    pos1_min = max(functions.POS1_VS_POS1_MIN_MATCHES,
                   functions.POST_LANE_POS1_VS_POS1_MIN_MATCHES)
    duo_min = max(functions.SYNERGY_DUO_MIN_MATCHES,
                  functions.POST_LANE_SYNERGY_DUO_MIN_MATCHES)

    # Solo: keep above SOLO_MIN_MATCHES but unequal mins across heroes.
    solo_lowest = solo_min + 5
    for pos in POSITIONS:
        hid_r = int(radiant[pos]["hero_id"])
        hid_d = int(dire[pos]["hero_id"])
        g_r = solo_lowest if pos == "pos1" else solo_min + 70
        g_d = solo_min + 10
        base[f"{hid_r}{pos}"] = {"wins": int(0.8 * g_r), "games": g_r}
        base[f"{hid_d}{pos}"] = {"wins": int(0.2 * g_d), "games": g_d}

    # 1vs1 unequal. pos1-vs-pos1 живёт по своему порогу (он развязан от cp1vs1),
    # поэтому кладём его между pos1_min и cp1_min: для pos1_vs_pos1 ячейка
    # используется, в cp1vs1 не попадает — остальные 24 пары держат метрику.
    pos1_cell = pos1_min + 2
    cp1_cell = cp1_min + 5
    for r_pos in ("pos1", "pos2", "pos3", "pos4", "pos5"):
        for d_pos in ("pos1", "pos2", "pos3", "pos4", "pos5"):
            r_key = f"{int(radiant[r_pos]['hero_id'])}{r_pos}"
            d_key = f"{int(dire[d_pos]['hero_id'])}{d_pos}"
            g = pos1_cell if (r_pos == "pos1" and d_pos == "pos1") else cp1_cell
            _put_vs(base, r_key, d_key, 0.8, g)

    # Trio: unequal used entries (min on dire side)
    import itertools as _it
    r_items = list(radiant.items())
    d_items = list(dire.items())
    for team_items, wr, base_g in ((r_items, 0.8, 70), (d_items, 0.2, 40)):
        for i, trio in enumerate(_it.combinations(team_items, 3)):
            tk = ",".join(sorted(_hero_key(item) for item in trio))
            g = base_g
            if wr < 0.5 and i == 0:
                g = 22  # dire min trio entry
            if wr >= 0.5 and i == 0:
                g = 28  # radiant min trio entry
            base[tk] = {"wins": int(round(wr * g)), "games": g}

    # Duo pairs unequal (все выше SYNERGY_DUO_MIN_MATCHES, но с разным запасом)
    duo_lowest = duo_min + 2
    for team, wr in ((radiant, 0.8), (dire, 0.2)):
        for a, b in (("pos1", "pos2"), ("pos1", "pos3"), ("pos2", "pos3"), ("pos4", "pos5")):
            g = duo_min + 40
            if a == "pos1" and b == "pos2":
                g = duo_lowest if wr < 0.5 else duo_min + 6
            _put_duo(base, team, a, b, wr, g)

    # Snapshot scores with equal high-support base first to prove diagnostics
    # do not change score formulas (re-run after unequal overwrites still scores).
    equal_base = _build_post_lane_stats(radiant, dire)
    equal_result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict=equal_base,
        mid_dict=equal_base,
        post_lane_dict=equal_base,
    )

    result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict=base,
        mid_dict=base,
        post_lane_dict=base,
    )

    for phase in ("early_output", "mid_output", "post_lane_output"):
        bucket = result[phase]
        equal_bucket = equal_result[phase]
        for metric in (
            "counterpick_1vs1",
            "counterpick_1vs2",
            "solo",
            "synergy_duo",
            "synergy_trio",
            "pos1_vs_pos1",
        ):
            score = bucket.get(metric)
            games = bucket.get(f"{metric}_games", 0)
            # Score may be None only if coverage fails; with full base it should emit.
            assert score is not None, f"{phase}.{metric} missing score: {bucket}"
            assert isinstance(score, (int, float))
            assert isinstance(games, int)
            assert games > 0, f"{phase}.{metric}_games should be positive, got {games}"
            # Never a huge correlated sum: with our unequal mins, support is modest.
            assert games < 500, f"{phase}.{metric}_games={games} looks like a sum"
            # Score still present on equal-support baseline (formula independent of diagnostic).
            assert equal_bucket.get(metric) is not None, f"{phase}.{metric} missing on equal base"

        # Explicit non-sum bounds for each family under unequal support:
        # каждая граница — «минимальная использованная ячейка семейства», а не
        # сумма по всем. 1vs2 мы не переписывали, там ячейки базовой фикстуры.
        assert bucket["counterpick_1vs2_games"] <= _base_stats_games()
        assert bucket["solo_games"] <= solo_lowest
        assert bucket["pos1_vs_pos1_games"] <= pos1_cell
        assert bucket["synergy_trio_games"] <= 40
        assert bucket["synergy_duo_games"] <= duo_lowest
        assert bucket["counterpick_1vs1_games"] <= cp1_cell


def test_live_all_output_is_post_lane_score_alias_not_offline_phase() -> None:
    """Live all_output copies selected post_lane scores + ProTracker; not a 4th offline phase.

    Does not edit cyberscore_try.py — only calls the existing builder.
    """
    import cyberscore_try as cs

    post_lane = {
        "counterpick_1vs1": 3,
        "counterpick_1vs2": -2,
        "solo": 1,
        "synergy_duo": 4,
        "synergy_trio": -5,
        "pos1_vs_pos1": 7,
        "counterpick_1vs1_games": 20,
        "counterpick_1vs2_games": 15,
        "solo_games": 12,
        "synergy_duo_games": 18,
        "synergy_trio_games": 11,
        "pos1_vs_pos1_games": 9,
    }
    # Production star-output builder expects late/early keys + valid flag;
    # it maps into dota2protracker_cp1vs1 (not raw pro_* keys).
    protracker = {
        "pro_cp1vs1_late": 2.0,
        "pro_cp1vs1_early": 2.0,
        "pro_cp1vs1_valid": True,
        "pro_duo_synergy_late": 1.0,
        "pro_duo_synergy_early": 1.0,
        "pro_duo_synergy_valid": True,
    }
    all_out = cs._build_all_star_output(post_lane, protracker)

    for key in (
        "counterpick_1vs1",
        "counterpick_1vs2",
        "solo",
        "synergy_duo",
        "synergy_trio",
        "pos1_vs_pos1",
    ):
        assert all_out.get(key) == post_lane[key]

    # Diagnostics are intentionally NOT copied into live All alias.
    for key in (
        "counterpick_1vs1_games",
        "counterpick_1vs2_games",
        "solo_games",
        "synergy_duo_games",
        "synergy_trio_games",
        "pos1_vs_pos1_games",
    ):
        assert key not in all_out

    # ProTracker remains additive on top of post_lane scores (live key name).
    assert all_out.get("dota2protracker_cp1vs1") == 2.0


# --- Alchemist directional favored-side 0.7 factor on public early_output ---

_ALCHEMIST_EARLY_OUTPUT_SCORE_KEYS = (
    "counterpick_1vs1",
    "counterpick_1vs2",
    "solo",
    "synergy_duo",
    "synergy_trio",
    "pos1_vs_pos1",
)

_ALCHEMIST_EARLY_OUTPUT_GAMES_KEYS = tuple(
    f"{key}_games" for key in _ALCHEMIST_EARLY_OUTPUT_SCORE_KEYS
)

# 0.8 WR baseline: five main metrics hit 60; pos1_vs_pos1 is half-scale 30.
# 60 * 0.7 = 42 exact; 30 * 0.7 = 21 exact. Separate non-integral proof uses 11/15.
_ALCHEMIST_EARLY_OUTPUT_POSITIVE_RAW = {
    "counterpick_1vs1": 60,
    "counterpick_1vs2": 60,
    "solo": 60,
    "synergy_duo": 60,
    "synergy_trio": 60,
    "pos1_vs_pos1": 30,
}

_ALCHEMIST_EARLY_OUTPUT_NEGATIVE_RAW = {
    "counterpick_1vs1": -60,
    "counterpick_1vs2": -60,
    "solo": -60,
    "synergy_duo": -60,
    "synergy_trio": -60,
    "pos1_vs_pos1": -30,
}

# One-pass proof values: 10 -> 7 (not 5=two-pass), -10 -> -7 (not -5).
# Non-integral proof: 15 * 0.7 = 10.5 -> 10; 11 * 0.7 = 7.7 -> 8.
_ALCHEMIST_ONE_PASS_POSITIVE_RAW = {
    "counterpick_1vs1": 10,
    "counterpick_1vs2": 10,
    "solo": 10,
    "synergy_duo": 10,
    "synergy_trio": 10,
    "pos1_vs_pos1": 10,
}

_ALCHEMIST_ONE_PASS_NEGATIVE_RAW = {
    "counterpick_1vs1": -10,
    "counterpick_1vs2": -10,
    "solo": -10,
    "synergy_duo": -10,
    "synergy_trio": -10,
    "pos1_vs_pos1": -10,
}

# radiant_wr=0.575 yields main metrics 15 and pos1_vs_pos1 7 (half-scale of get_diff).
# 15 * 0.7 = 10.5 -> 10; 7 * 0.7 = 4.9 -> 5 (both non-integral before round).
_ALCHEMIST_NON_INTEGRAL_RAW = {
    "counterpick_1vs1": 15,
    "counterpick_1vs2": 15,
    "solo": 15,
    "synergy_duo": 15,
    "synergy_trio": 15,
    "pos1_vs_pos1": 7,
}

_ALCHEMIST_ZERO_RAW = {key: 0 for key in _ALCHEMIST_EARLY_OUTPUT_SCORE_KEYS}


def _alchemist_early_output_flip_wr(data: dict) -> dict:
    flipped: dict = {}
    for key, value in data.items():
        games = int(value["games"])
        wins = int(value["wins"])
        flipped[key] = {"wins": games - wins, "games": games}
    return flipped


def _alchemist_early_output_build_stats(
    radiant: dict,
    dire: dict,
    *,
    radiant_wr: float = 0.8,
) -> dict:
    """Uniform WR fixture so all six allowlisted early_output scores share one edge."""
    dire_wr = 1.0 - radiant_wr
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
        _put_stats(data, _hero_key(item), radiant_wr, games)
    for item in dire_items:
        _put_stats(data, _hero_key(item), dire_wr, games)

    for team_items, wr in ((radiant_items, radiant_wr), (dire_items, dire_wr)):
        for a, b in itertools.combinations(team_items, 2):
            pair = sorted([_hero_key(a), _hero_key(b)])
            _put_stats(data, f"{pair[0]}_with_{pair[1]}", wr, games)
        for trio in itertools.combinations(team_items, 3):
            trio_key = ",".join(sorted(_hero_key(item) for item in trio))
            _put_stats(data, trio_key, wr, games)

    for r_item in radiant_items:
        r_key = _hero_key(r_item)
        for d_item in dire_items:
            _put_vs(data, r_key, _hero_key(d_item), radiant_wr, games)
        for d_duo in itertools.combinations(dire_items, 2):
            d_duo_key = ",".join(sorted(_hero_key(item) for item in d_duo))
            _put_vs(data, r_key, d_duo_key, radiant_wr, games)

    for d_item in dire_items:
        d_key = _hero_key(d_item)
        for r_duo in itertools.combinations(radiant_items, 2):
            r_duo_key = ",".join(sorted(_hero_key(item) for item in r_duo))
            _put_vs(data, d_key, r_duo_key, dire_wr, games)

    return data


def _alchemist_scale_once(value: int) -> int:
    return int(round(value * 0.7))


def _alchemist_early_output_scaled(raw: dict[str, int]) -> dict[str, int]:
    return {key: _alchemist_scale_once(value) for key, value in raw.items()}


def _alchemist_expected_directional(
    raw: dict[str, int],
    *,
    radiant_has: bool,
    dire_has: bool,
) -> dict[str, int]:
    """Discount only scores whose sign favors a side that drafted Alchemist."""
    expected: dict[str, int] = {}
    for key, value in raw.items():
        if value > 0 and radiant_has:
            expected[key] = _alchemist_scale_once(value)
        elif value < 0 and dire_has:
            expected[key] = _alchemist_scale_once(value)
        else:
            expected[key] = value
    return expected


def _alchemist_sides(
    *,
    radiant_has: bool = False,
    dire_has: bool = False,
    radiant_id: int | str = 73,
    dire_id: int | str = 73,
) -> tuple[dict, dict]:
    radiant = _side(1)
    dire = _side(6)
    if radiant_has:
        radiant["pos1"]["hero_id"] = radiant_id
    if dire_has:
        dire["pos3"]["hero_id"] = dire_id
    return radiant, dire


def _alchemist_early_output_call(radiant: dict, dire: dict, early_dict: dict) -> dict:
    result = functions.synergy_and_counterpick(
        radiant_heroes_and_pos=radiant,
        dire_heroes_and_pos=dire,
        early_dict=early_dict,
        mid_dict={},
    )
    early_output = result["early_output"]
    assert isinstance(early_output, dict)
    # Non-early public buckets must remain untouched by the Alchemist transform.
    for other_name in ("mid_output", "post_lane_output", "early_end_output"):
        other = result.get(other_name)
        if isinstance(other, dict):
            assert other is not early_output
    return early_output


def _assert_alchemist_early_output_scores(
    early_output: dict,
    expected_scores: dict[str, int],
    *,
    games: int | None = None,
) -> None:
    # По умолчанию ждём заполнение базовой фикстуры — оно едет вслед за порогами.
    if games is None:
        games = _base_stats_games()
    for key, expected in expected_scores.items():
        assert key in early_output
        value = early_output[key]
        assert type(value) is int
        assert value == expected
    for key in _ALCHEMIST_EARLY_OUTPUT_GAMES_KEYS:
        assert key in early_output
        observed_games = early_output[key]
        assert type(observed_games) is int
        assert observed_games == games


def test_alchemist_early_output_radiant_only_scales_positive_favored_scores() -> None:
    """Radiant-only Alchemist scales v>0; leaves v<0 and exact 0 unchanged."""
    radiant, dire = _alchemist_sides(radiant_has=True)

    positive = _alchemist_early_output_call(
        radiant, dire, _alchemist_early_output_build_stats(radiant, dire, radiant_wr=0.8)
    )
    _assert_alchemist_early_output_scores(
        positive,
        _alchemist_expected_directional(
            _ALCHEMIST_EARLY_OUTPUT_POSITIVE_RAW, radiant_has=True, dire_has=False
        ),
    )

    negative = _alchemist_early_output_call(
        radiant,
        dire,
        _alchemist_early_output_flip_wr(
            _alchemist_early_output_build_stats(radiant, dire, radiant_wr=0.8)
        ),
    )
    _assert_alchemist_early_output_scores(
        negative,
        _alchemist_expected_directional(
            _ALCHEMIST_EARLY_OUTPUT_NEGATIVE_RAW, radiant_has=True, dire_has=False
        ),
    )

    zero = _alchemist_early_output_call(
        radiant, dire, _alchemist_early_output_build_stats(radiant, dire, radiant_wr=0.5)
    )
    _assert_alchemist_early_output_scores(zero, _ALCHEMIST_ZERO_RAW)

    # Non-integral *0.7 path on favored positive scores only.
    non_integral = _alchemist_early_output_call(
        radiant,
        dire,
        _alchemist_early_output_build_stats(radiant, dire, radiant_wr=0.575),
    )
    expected_non_integral = _alchemist_expected_directional(
        _ALCHEMIST_NON_INTEGRAL_RAW, radiant_has=True, dire_has=False
    )
    assert expected_non_integral["counterpick_1vs1"] == 10  # int(round(15 * 0.7))
    assert expected_non_integral["pos1_vs_pos1"] == 5  # int(round(7 * 0.7))
    _assert_alchemist_early_output_scores(non_integral, expected_non_integral)


def test_alchemist_early_output_dire_only_scales_negative_favored_scores() -> None:
    """Dire-only Alchemist scales v<0; leaves v>0 and exact 0 unchanged."""
    radiant, dire = _alchemist_sides(dire_has=True)

    positive = _alchemist_early_output_call(
        radiant, dire, _alchemist_early_output_build_stats(radiant, dire, radiant_wr=0.8)
    )
    _assert_alchemist_early_output_scores(
        positive,
        _alchemist_expected_directional(
            _ALCHEMIST_EARLY_OUTPUT_POSITIVE_RAW, radiant_has=False, dire_has=True
        ),
    )

    negative = _alchemist_early_output_call(
        radiant,
        dire,
        _alchemist_early_output_flip_wr(
            _alchemist_early_output_build_stats(radiant, dire, radiant_wr=0.8)
        ),
    )
    _assert_alchemist_early_output_scores(
        negative,
        _alchemist_expected_directional(
            _ALCHEMIST_EARLY_OUTPUT_NEGATIVE_RAW, radiant_has=False, dire_has=True
        ),
    )

    zero = _alchemist_early_output_call(
        radiant, dire, _alchemist_early_output_build_stats(radiant, dire, radiant_wr=0.5)
    )
    _assert_alchemist_early_output_scores(zero, _ALCHEMIST_ZERO_RAW)


def test_alchemist_early_output_both_sides_scales_each_nonzero_sign_once() -> None:
    """Both-side Alchemist scales either nonzero sign once; never compounds 0.7 twice."""
    radiant, dire = _alchemist_sides(radiant_has=True, dire_has=True)

    # 0.55 WR -> main metrics 9 / pos1 5 under broad scale would differ; use one-pass 10s.
    # 0.6 WR -> 20/10. We assert one-pass 10->7 via dedicated WR that yields 10 everywhere:
    # pos1_vs_pos1 is half of main metrics under get_diff, so force pos1 separately by
    # using the known 0.6 baseline (20/10) and checking main metrics + pos1 scale once.
    baseline_positive = _alchemist_early_output_call(
        radiant, dire, _alchemist_early_output_build_stats(radiant, dire, radiant_wr=0.6)
    )
    # Raw without alchemist would be 20 / 10; both-side favored transform => 14 / 7 once.
    expected_positive = {
        "counterpick_1vs1": 14,
        "counterpick_1vs2": 14,
        "solo": 14,
        "synergy_duo": 14,
        "synergy_trio": 14,
        "pos1_vs_pos1": 7,  # 10 -> 7 once, never 5
    }
    _assert_alchemist_early_output_scores(baseline_positive, expected_positive)

    baseline_negative = _alchemist_early_output_call(
        radiant,
        dire,
        _alchemist_early_output_flip_wr(
            _alchemist_early_output_build_stats(radiant, dire, radiant_wr=0.6)
        ),
    )
    expected_negative = {
        "counterpick_1vs1": -14,
        "counterpick_1vs2": -14,
        "solo": -14,
        "synergy_duo": -14,
        "synergy_trio": -14,
        "pos1_vs_pos1": -7,  # -10 -> -7 once, never -5
    }
    _assert_alchemist_early_output_scores(baseline_negative, expected_negative)

    zero = _alchemist_early_output_call(
        radiant, dire, _alchemist_early_output_build_stats(radiant, dire, radiant_wr=0.5)
    )
    _assert_alchemist_early_output_scores(zero, _ALCHEMIST_ZERO_RAW)


def test_alchemist_early_output_absent_leaves_all_signs_unscaled() -> None:
    """No Alchemist: positive, negative, and exact zero scores stay unscaled."""
    radiant, dire = _alchemist_sides()

    positive = _alchemist_early_output_call(
        radiant, dire, _alchemist_early_output_build_stats(radiant, dire, radiant_wr=0.8)
    )
    _assert_alchemist_early_output_scores(positive, _ALCHEMIST_EARLY_OUTPUT_POSITIVE_RAW)

    negative = _alchemist_early_output_call(
        radiant,
        dire,
        _alchemist_early_output_flip_wr(
            _alchemist_early_output_build_stats(radiant, dire, radiant_wr=0.8)
        ),
    )
    _assert_alchemist_early_output_scores(negative, _ALCHEMIST_EARLY_OUTPUT_NEGATIVE_RAW)

    zero = _alchemist_early_output_call(
        radiant, dire, _alchemist_early_output_build_stats(radiant, dire, radiant_wr=0.5)
    )
    _assert_alchemist_early_output_scores(zero, _ALCHEMIST_ZERO_RAW)


def test_alchemist_early_output_accepts_string_hero_id_73_on_either_side() -> None:
    """Accepted Alchemist detection still treats hero_id '73' like integer 73."""
    radiant_str, dire_plain = _alchemist_sides(radiant_has=True, radiant_id="73")
    radiant_out = _alchemist_early_output_call(
        radiant_str,
        dire_plain,
        _alchemist_early_output_build_stats(radiant_str, dire_plain, radiant_wr=0.8),
    )
    _assert_alchemist_early_output_scores(
        radiant_out,
        _alchemist_expected_directional(
            _ALCHEMIST_EARLY_OUTPUT_POSITIVE_RAW, radiant_has=True, dire_has=False
        ),
    )

    radiant_plain, dire_str = _alchemist_sides(dire_has=True, dire_id="73")
    dire_out = _alchemist_early_output_call(
        radiant_plain,
        dire_str,
        _alchemist_early_output_flip_wr(
            _alchemist_early_output_build_stats(radiant_plain, dire_str, radiant_wr=0.8)
        ),
    )
    _assert_alchemist_early_output_scores(
        dire_out,
        _alchemist_expected_directional(
            _ALCHEMIST_EARLY_OUTPUT_NEGATIVE_RAW, radiant_has=False, dire_has=True
        ),
    )


def test_alchemist_early_output_preserves_games_none_absent_and_non_scores() -> None:
    """Allowlisted transform only: games/None/absent stay intact; no key invention."""
    radiant, dire = _alchemist_sides(radiant_has=True, dire_has=True)

    full = _alchemist_early_output_call(
        radiant, dire, _alchemist_early_output_build_stats(radiant, dire, radiant_wr=0.8)
    )
    for key in _ALCHEMIST_EARLY_OUTPUT_GAMES_KEYS:
        assert full[key] == _base_stats_games()
        assert type(full[key]) is int
    for key in _ALCHEMIST_EARLY_OUTPUT_SCORE_KEYS:
        assert type(full[key]) is int

    sparse = _alchemist_early_output_call(radiant, dire, {})
    for key in (
        "counterpick_1vs1",
        "counterpick_1vs2",
        "pos1_vs_pos1",
        "synergy_trio",
    ):
        assert sparse[key] is None
        assert sparse[f"{key}_games"] == 0
    # optional keys remain absent when uncovered — transform must not invent them
    assert "solo" not in sparse
    assert "synergy_duo" not in sparse
