import subprocess
import sys
from pathlib import Path

from base.synergy_trio_role_pool import (
    aggregate_trio_role_samples,
    make_trio_role_key,
    raw_lookup_keys_for_trio_role_key,
    raw_lookup_keys_for_trio_tokens,
    raw_row_to_trio_sample,
    trio_role_entry_winrate,
)


BASE_DIR = Path(__file__).resolve().parents[1]


def test_trio_positions_collapse_to_core_and_support_roles():
    assert make_trio_role_key(["60pos3", "13pos2", "110pos4"]) == (
        "13:core,60:core,110:support"
    )
    assert make_trio_role_key(["110pos5", "60pos1", "13pos3"]) == (
        "13:core,60:core,110:support"
    )


def test_raw_non_trio_rows_are_rejected():
    assert raw_row_to_trio_sample("60pos3_vs_13pos2,110pos4", {"wins": 5, "games": 8}) is None
    assert raw_row_to_trio_sample("60pos3_with_13pos2", {"wins": 5, "games": 8}) is None


def test_permutations_of_one_exact_trio_are_deduplicated():
    rows = [
        ("60pos3,13pos2,110pos4", {"wins": 12, "games": 20}),
        ("110pos4,60pos3,13pos2", {"wins": 12, "games": 20}),
        ("13pos2,110pos4,60pos3", {"wins": 12, "games": 20}),
    ]
    result = aggregate_trio_role_samples(rows)["13:core,60:core,110:support"]
    assert result == {"score": 12.0, "games": 20}


def test_distinct_exact_position_cells_are_added_to_role_pool():
    rows = [
        ("60pos3,13pos2,110pos4", {"wins": 12, "games": 20}),
        ("60pos1,13pos3,110pos5", {"wins": 4, "games": 8}),
    ]
    result = aggregate_trio_role_samples(rows)["13:core,60:core,110:support"]
    assert result == {"score": 16.0, "games": 28}


def test_higher_min_n_gates_are_inclusive():
    entry = {"score": 55, "games": 100}
    for threshold in (25, 50, 75, 100):
        assert trio_role_entry_winrate(entry, threshold) == (0.55, 100)
    assert trio_role_entry_winrate(entry, 101) == (None, 100)


def test_role_key_expansion_covers_all_exact_permutations():
    keys = raw_lookup_keys_for_trio_role_key("13:core,60:core,110:support")
    assert "13pos2,60pos3,110pos4" in keys
    assert "110pos5,60pos1,13pos3" in keys
    assert len(keys) == 3 * 3 * 2 * 6


def test_exact_tokens_expand_to_the_same_role_family():
    assert raw_lookup_keys_for_trio_tokens(["60pos3", "13pos2", "110pos4"]) == (
        raw_lookup_keys_for_trio_role_key("13:core,60:core,110:support")
    )


def test_functions_imports_from_runtime_base_working_directory():
    result = subprocess.run(
        [sys.executable, "-c", "import functions"],
        cwd=BASE_DIR,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
