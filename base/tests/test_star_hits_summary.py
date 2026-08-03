"""Regression tests for the ⭐ Star hits (WR60+) summary block."""

import importlib
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

runtime = importlib.import_module("cyberscore_try")


def test_max_star_wr_level_for_metric_returns_highest_level_reached() -> None:
    # Late counterpick_1vs2: WR60=4, WR65=6, WR70=8 in the default runtime table.
    level = runtime._max_star_wr_level_for_metric(
        metric="counterpick_1vs2",
        value=-6.0,
        section="mid_output",
    )
    assert level == 65

    level = runtime._max_star_wr_level_for_metric(
        metric="counterpick_1vs2",
        value=-4.0,
        section="mid_output",
    )
    assert level == 60

    level = runtime._max_star_wr_level_for_metric(
        metric="counterpick_1vs2",
        value=-3.0,
        section="mid_output",
    )
    assert level is None


def test_max_star_wr_level_ignores_disabled_support_metrics() -> None:
    # synergy_duo / synergy_trio are not STAR-signal metrics anymore.
    level = runtime._max_star_wr_level_for_metric(
        metric="synergy_duo",
        value=9.0,
        section="all_output",
    )
    assert level is None


def test_build_star_hits_summary_block_carstensz_case_highlights_late_cp1vs2() -> None:
    block = runtime._build_star_hits_summary_block(
        early_output={},
        mid_output={
            "counterpick_1vs1": -1,
            "counterpick_1vs2": -6,
            "solo": 0,
            "synergy_duo": 0,
        },
        all_output={},
    )
    assert block
    assert "⭐ Star hits (WR60+):" in block
    assert "Late: Counterpick_1vs2 -6 (WR65)" in block
    # Early and All had no hits, so their rows must not appear.
    assert "Early:" not in block
    assert "All:" not in block


def test_build_star_hits_summary_block_returns_empty_without_hits() -> None:
    block = runtime._build_star_hits_summary_block(
        early_output={"counterpick_1vs1": 0, "solo": 0},
        mid_output={"counterpick_1vs2": 0},
        all_output={"synergy_duo": 9},  # disabled STAR metric
    )
    assert block == ""


def test_build_star_hits_summary_block_combines_all_three_blocks() -> None:
    block = runtime._build_star_hits_summary_block(
        early_output={
            "counterpick_1vs1": 4,
            "solo": 3,
        },
        mid_output={
            "counterpick_1vs1": -1,
            "counterpick_1vs2": -6,
        },
        all_output={
            "counterpick_1vs1": 4,
        },
    )
    assert block.startswith("⭐ Star hits (WR60+):\n")
    assert "Early: Counterpick_1vs1 +4" in block
    assert "Solo +3" in block
    assert "Late: Counterpick_1vs2 -6 (WR65)" in block
    assert "All: Counterpick_1vs1 +4" in block
    # Pure separators: block ends with a trailing newline for message composition.
    assert block.endswith("\n")


def test_build_star_hits_summary_block_preserves_metric_order() -> None:
    block = runtime._build_star_hits_summary_block(
        early_output={
            "solo": 5,
            "counterpick_1vs1": 4,
            "counterpick_1vs2": 5,
        },
        mid_output={},
        all_output={},
    )
    early_line = next(line for line in block.splitlines() if "Early:" in line)
    cp1_idx = early_line.index("Counterpick_1vs1")
    cp2_idx = early_line.index("Counterpick_1vs2")
    solo_idx = early_line.index("Solo")
    assert cp1_idx < cp2_idx < solo_idx


def test_compose_star_metric_blocks_still_concatenates_in_fixed_order() -> None:
    # Keep backwards-compat guarantee that the existing concat helper is unchanged.
    message = runtime._compose_star_metric_blocks_for_message(
        "Early 20-28:\nE\n",
        "Late: (28-60 min):\nL\n",
        "All:\nA\n",
    )
    assert message == "Early 20-28:\nE\nLate: (28-60 min):\nL\nAll:\nA\n"
