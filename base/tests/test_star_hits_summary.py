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


def test_max_star_wr_level_enables_synergy_metrics() -> None:
    level = runtime._max_star_wr_level_for_metric(
        metric="synergy_duo",
        value=9.0,
        section="all_output",
    )
    assert level == 60


def test_build_star_hits_summary_requires_all_three_families() -> None:
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
    assert block == ""


def test_build_star_hits_summary_block_returns_empty_without_hits() -> None:
    block = runtime._build_star_hits_summary_block(
        early_output={"counterpick_1vs1": 0, "solo": 0},
        mid_output={"counterpick_1vs2": 0},
        all_output={"synergy_duo": 9},
    )
    assert block == ""


def test_build_star_hits_summary_block_combines_all_three_blocks() -> None:
    block = runtime._build_star_hits_summary_block(
        early_output={
            "counterpick_1vs1": 11,
            "solo": 7,
            "synergy_duo": 12,
        },
        mid_output={
            "counterpick_1vs2": -9,
            "solo": -8,
            "synergy_trio": -11,
        },
        all_output={
            "counterpick_1vs2": 6,
            "solo": 2,
            "synergy_trio": 8,
        },
    )
    assert block.startswith("⭐ Star hits (WR60+):\n")
    assert "Early: Solo +7 (WR75), Counterpick_1vs1 +11 (WR75), Synergy_duo +12 (WR75)" in block
    assert "Late: Solo -8 (WR75), Counterpick_1vs2 -9 (WR75), Synergy_trio -11 (WR75)" in block
    assert "All: Solo +2 (WR70), Counterpick_1vs2 +6 (WR75), Synergy_trio +8 (WR75)" in block
    # Pure separators: block ends with a trailing newline for message composition.
    assert block.endswith("\n")


def test_build_star_hits_summary_uses_primary_and_suppresses_fallback() -> None:
    block = runtime._build_star_hits_summary_block(
        early_output={
            "solo": 8,
            "counterpick_1vs1": 19,
            "counterpick_1vs2": 12,
            "synergy_duo": 19,
            "synergy_trio": 14,
        },
        mid_output={},
        all_output={},
    )
    early_line = next(line for line in block.splitlines() if "Early:" in line)
    assert "Counterpick_1vs2" in early_line
    assert "Synergy_trio" in early_line
    assert "Counterpick_1vs1" not in early_line
    assert "Synergy_duo" not in early_line


def test_compose_star_metric_blocks_still_concatenates_in_fixed_order() -> None:
    # Keep backwards-compat guarantee that the existing concat helper is unchanged.
    message = runtime._compose_star_metric_blocks_for_message(
        "Early 20-28:\nE\n",
        "Late: (28-60 min):\nL\n",
        "All:\nA\n",
    )
    assert message == "Early 20-28:\nE\nLate: (28-60 min):\nL\nAll:\nA\n"
