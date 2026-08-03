from base.cp1vs2_role_pool import (
    aggregate_role_samples,
    make_role_key,
    raw_lookup_keys_for_role_key,
    raw_row_to_sample,
    role_entry_winrate,
)


def test_core_example_collapses_all_three_positions_to_role_key():
    expected = "60:core_vs_13:core,110:core"
    assert make_role_key("60pos3", ["13pos2", "110pos3"]) == expected
    assert make_role_key("60pos1", ["110pos2", "13pos3"]) == expected


def test_support_positions_share_one_role_but_do_not_mix_with_core():
    support = make_role_key("71pos4", ["13pos2", "110pos3"])
    assert support == "71:support_vs_13:core,110:core"
    assert make_role_key("71pos3", ["13pos2", "110pos3"]) != support


def test_reverse_row_is_inverted_to_single_hero_perspective():
    sample = raw_row_to_sample(
        "13pos2,110pos3_vs_60pos3",
        {"wins": 6, "draws": 0, "games": 14},
    )
    assert sample is not None
    assert sample.direction == "reverse"
    assert sample.score == 8
    assert sample.games == 14


def test_duo_permutation_is_deduplicated_but_reverse_direction_is_retained():
    rows = [
        ("60pos3_vs_13pos2,110pos3", {"wins": 12, "games": 14}),
        ("60pos3_vs_110pos3,13pos2", {"wins": 12, "games": 14}),
        ("13pos2,110pos3_vs_60pos3", {"wins": 6, "games": 14}),
        ("110pos3,13pos2_vs_60pos3", {"wins": 6, "games": 14}),
    ]
    result = aggregate_role_samples(rows)["60:core_vs_13:core,110:core"]
    assert result == {"score": 20.0, "games": 28}


def test_distinct_exact_position_cells_are_added_inside_same_role_pool():
    rows = [
        ("60pos3_vs_13pos2,110pos3", {"wins": 12, "games": 14}),
        ("60pos2_vs_13pos2,110pos3", {"wins": 1, "games": 3}),
    ]
    result = aggregate_role_samples(rows)["60:core_vs_13:core,110:core"]
    assert result == {"score": 13.0, "games": 17}


def test_minimum_n_25_is_inclusive():
    assert role_entry_winrate({"score": 18, "games": 24}, min_matches=25) == (None, 24)
    assert role_entry_winrate({"score": 15, "games": 25}, min_matches=25) == (0.6, 25)


def test_role_key_expansion_contains_example_direct_and_reverse_keys():
    keys = raw_lookup_keys_for_role_key("60:core_vs_13:core,110:core")
    assert "60pos3_vs_13pos2,110pos3" in keys
    assert "13pos2,110pos3_vs_60pos3" in keys
    assert len(keys) == 108
