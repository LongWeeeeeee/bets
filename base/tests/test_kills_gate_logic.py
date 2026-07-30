"""Unit tests for Late→same-sign-all promote (pt2) and kills gate (pt3).

These tests exercise the pure helpers (``_evaluate_late_all_same_sign_promote``
and ``_evaluate_kills_gate``) without running the full dispatch pipeline, so
the STAR diagnostics stay in their real (non-mocked) form and exercise the
WR60 thresholds directly.
"""

import importlib
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

runtime = importlib.import_module("cyberscore_try")


# -----------------------------
# pt2: Late promote via same-sign All
# -----------------------------


def test_late_promote_fires_when_all_same_sign_and_late_has_same_sign_hit() -> None:
    result = runtime._evaluate_late_all_same_sign_promote(
        has_early_star=True,
        has_late_star=False,
        has_all_star=True,
        early_sign=1,
        all_sign=1,
        raw_mid_output={
            "counterpick_1vs1": 4,  # WR60 threshold for late cp1v1 == 4
            "counterpick_1vs2": 0,
            "solo": 0,
        },
    )
    assert result["active"] is True
    assert result["valid"] is True
    assert "counterpick_1vs1" in result["same_sign_hits"]
    assert result["opposite_sign_hits"] == []
    assert result["opposite_sign_nonzero"] == []


def test_late_promote_rejects_carstensz_case_opposite_sign_hit() -> None:
    # Carstensz: early=+, all=+, late has cp1vs2=-6 (opposite sign hit).
    result = runtime._evaluate_late_all_same_sign_promote(
        has_early_star=True,
        has_late_star=False,
        has_all_star=True,
        early_sign=1,
        all_sign=1,
        raw_mid_output={
            "counterpick_1vs1": -1,  # opposite sign, below threshold
            "counterpick_1vs2": -6,
            "solo": 0,
        },
    )
    assert result["active"] is True
    assert result["valid"] is False
    assert "counterpick_1vs2" in result["opposite_sign_hits"]


def test_late_promote_inactive_when_all_opposite_sign() -> None:
    result = runtime._evaluate_late_all_same_sign_promote(
        has_early_star=True,
        has_late_star=False,
        has_all_star=True,
        early_sign=1,
        all_sign=-1,
        raw_mid_output={"counterpick_1vs1": 4},
    )
    assert result["active"] is False
    assert result["valid"] is False


def test_late_promote_inactive_when_late_already_valid() -> None:
    result = runtime._evaluate_late_all_same_sign_promote(
        has_early_star=True,
        has_late_star=True,
        has_all_star=True,
        early_sign=1,
        all_sign=1,
        raw_mid_output={"counterpick_1vs1": 4},
    )
    assert result["active"] is False
    assert result["valid"] is False


def test_late_promote_inactive_when_no_all_star() -> None:
    # No All star => kills path may apply, but promote must not fire.
    result = runtime._evaluate_late_all_same_sign_promote(
        has_early_star=True,
        has_late_star=False,
        has_all_star=False,
        early_sign=1,
        all_sign=None,
        raw_mid_output={"counterpick_1vs1": 4},
    )
    assert result["active"] is False
    assert result["valid"] is False


def test_late_promote_rejects_conflicting_nonzero_opposite_metric() -> None:
    # No opposite-sign hit, but a nonzero opposite-sign metric below threshold
    # must still block the promote.
    result = runtime._evaluate_late_all_same_sign_promote(
        has_early_star=True,
        has_late_star=False,
        has_all_star=True,
        early_sign=1,
        all_sign=1,
        raw_mid_output={
            "counterpick_1vs1": 4,
            "counterpick_1vs2": -1,
            "solo": 0,
        },
    )
    assert result["active"] is True
    assert result["valid"] is False
    assert "counterpick_1vs2" in result["opposite_sign_nonzero"]


# -----------------------------
# pt3: kills gate activation + WR gate
# -----------------------------


def test_kills_gate_activates_when_early_only_and_late_has_any_hit() -> None:
    result = runtime._evaluate_kills_gate(
        has_early_star=True,
        has_late_star=False,
        has_all_star=False,
        early_sign=1,
        all_sign=None,
        early_wr_pct=70.0,
        early_hit_count=2,
        raw_mid_output={"counterpick_1vs2": -6},
        late_all_same_sign_promote_valid=False,
    )
    assert result["active"] is True
    assert result["valid"] is True
    assert result["wr_gate"]["passes_wr70"] is True


def test_kills_gate_activates_on_carstensz_case_with_all_same_sign() -> None:
    # Carstensz: early=+, all=+, late has opposite-sign hit cp1vs2=-6, pt2
    # promote failed. Kills gate must fire only with Early WR>=70.
    result = runtime._evaluate_kills_gate(
        has_early_star=True,
        has_late_star=False,
        has_all_star=True,
        early_sign=1,
        all_sign=1,
        early_wr_pct=70.0,
        early_hit_count=2,
        raw_mid_output={
            "counterpick_1vs1": -1,
            "counterpick_1vs2": -6,
            "solo": 0,
        },
        late_all_same_sign_promote_valid=False,
    )
    assert result["active"] is True
    assert result["valid"] is True


def test_kills_gate_blocks_when_early_wr_below_70() -> None:
    result = runtime._evaluate_kills_gate(
        has_early_star=True,
        has_late_star=False,
        has_all_star=False,
        early_sign=1,
        all_sign=None,
        early_wr_pct=65.0,
        early_hit_count=3,
        raw_mid_output={"counterpick_1vs2": -6},
        late_all_same_sign_promote_valid=False,
    )
    assert result["active"] is True
    assert result["valid"] is False
    assert result["active_but_blocked"] is True


def test_kills_gate_blocks_when_early_wr_65_even_with_two_hits() -> None:
    # WR65 + >=2 hits is not enough; need WR>=70 AND hits>=2.
    result = runtime._evaluate_kills_gate(
        has_early_star=True,
        has_late_star=False,
        has_all_star=False,
        early_sign=1,
        all_sign=None,
        early_wr_pct=65.0,
        early_hit_count=2,
        raw_mid_output={"counterpick_1vs2": -6},
        late_all_same_sign_promote_valid=False,
    )
    assert result["active"] is True
    assert result["valid"] is False
    assert result["active_but_blocked"] is True
    assert result["wr_gate"]["passes_wr65_two_hits"] is False


def test_kills_gate_blocks_when_early_wr_70_but_only_one_hit() -> None:
    # WR>=70 alone is not enough; need >=2 early hits.
    result = runtime._evaluate_kills_gate(
        has_early_star=True,
        has_late_star=False,
        has_all_star=False,
        early_sign=1,
        all_sign=None,
        early_wr_pct=70.0,
        early_hit_count=1,
        raw_mid_output={"counterpick_1vs2": -6},
        late_all_same_sign_promote_valid=False,
    )
    assert result["active"] is True
    assert result["valid"] is False
    assert result["active_but_blocked"] is True
    assert result["wr_gate"]["passes_wr70"] is True
    assert result["wr_gate"]["passes_min_hits"] is False


def test_kills_gate_inactive_when_late_has_no_star_hits() -> None:
    result = runtime._evaluate_kills_gate(
        has_early_star=True,
        has_late_star=False,
        has_all_star=True,
        early_sign=1,
        all_sign=1,
        early_wr_pct=70.0,
        early_hit_count=3,
        raw_mid_output={"counterpick_1vs2": 0, "solo": 0},
        late_all_same_sign_promote_valid=False,
    )
    assert result["active"] is False
    assert result["valid"] is False


def test_kills_gate_inactive_when_late_already_valid() -> None:
    result = runtime._evaluate_kills_gate(
        has_early_star=True,
        has_late_star=True,
        has_all_star=True,
        early_sign=1,
        all_sign=1,
        early_wr_pct=70.0,
        early_hit_count=3,
        raw_mid_output={"counterpick_1vs2": -6},
        late_all_same_sign_promote_valid=False,
    )
    assert result["active"] is False


def test_kills_gate_inactive_when_promote_succeeds() -> None:
    result = runtime._evaluate_kills_gate(
        has_early_star=True,
        has_late_star=False,
        has_all_star=True,
        early_sign=1,
        all_sign=1,
        early_wr_pct=70.0,
        early_hit_count=3,
        raw_mid_output={"counterpick_1vs1": 4},
        late_all_same_sign_promote_valid=True,
    )
    assert result["active"] is False


def test_early_star_meets_kills_wr_gate_edge_cases() -> None:
    # WR>=70 AND hits>=2 required
    assert runtime._early_star_meets_kills_wr_gate(
        early_wr_pct=70.0, early_hit_count=2
    )["valid"] is True
    assert runtime._early_star_meets_kills_wr_gate(
        early_wr_pct=70.0, early_hit_count=1
    )["valid"] is False
    # WR65 + hits never enough
    assert runtime._early_star_meets_kills_wr_gate(
        early_wr_pct=65.0, early_hit_count=2
    )["valid"] is False
    assert runtime._early_star_meets_kills_wr_gate(
        early_wr_pct=65.0, early_hit_count=1
    )["valid"] is False
    assert runtime._early_star_meets_kills_wr_gate(
        early_wr_pct=69.9, early_hit_count=5
    )["valid"] is False
    # None WR → blocked
    assert runtime._early_star_meets_kills_wr_gate(
        early_wr_pct=None, early_hit_count=5
    )["valid"] is False
    gate = runtime._early_star_meets_kills_wr_gate(
        early_wr_pct=70.0, early_hit_count=2
    )
    assert gate["min_wr"] == 70.0
    assert gate["min_hits"] == 2
    assert gate["passes_wr70"] is True
    assert gate["passes_min_hits"] is True
    assert gate["passes_wr65_two_hits"] is True
    gate_low_hits = runtime._early_star_meets_kills_wr_gate(
        early_wr_pct=70.0, early_hit_count=0
    )
    assert gate_low_hits["passes_wr70"] is True
    assert gate_low_hits["passes_min_hits"] is False
    assert gate_low_hits["valid"] is False
    assert gate_low_hits["passes_wr65_two_hits"] is False


# -----------------------------
# Late WR60 + opposite companion must not send
# -----------------------------


def test_single_block_gate_rejects_late_only_wr60() -> None:
    result = runtime._single_block_star_min_wr_gate(
        has_selected_early_star=False,
        has_selected_late_star=True,
        has_selected_all_star=False,
        early_wr_pct=None,
        late_wr_pct=60.0,
        all_wr_pct=None,
    )
    assert result["active"] is True
    assert result["block"] == "late"
    assert result["valid"] is False
    assert result["min_wr_ok"] is False


def test_single_block_gate_inactive_when_late_and_opposite_early() -> None:
    # Hole before the fix: multi-block makes single-block gate inactive.
    result = runtime._single_block_star_min_wr_gate(
        has_selected_early_star=True,
        has_selected_late_star=True,
        has_selected_all_star=False,
        early_wr_pct=80.0,
        late_wr_pct=60.0,
        all_wr_pct=None,
    )
    assert result["active"] is False
    assert result["valid"] is True


def test_late_wr60_with_opposite_block_is_rejected() -> None:
    result = runtime._late_wr_below_min_with_opposite_block_gate(
        has_selected_late_star=True,
        late_wr_pct=60.0,
        opposite_signs_selected=True,
    )
    assert result["active"] is True
    assert result["valid"] is False
    assert result["min_wr_ok"] is False
    assert result["block"] == "late"


def test_late_wr65_with_opposite_block_is_allowed() -> None:
    result = runtime._late_wr_below_min_with_opposite_block_gate(
        has_selected_late_star=True,
        late_wr_pct=65.0,
        opposite_signs_selected=True,
    )
    assert result["active"] is False
    assert result["valid"] is True
    assert result["min_wr_ok"] is True


def test_late_wr60_same_sign_multi_block_not_caught_by_opposite_gate() -> None:
    # Same-sign multi-block is not opposite_signs_selected; other gates apply.
    result = runtime._late_wr_below_min_with_opposite_block_gate(
        has_selected_late_star=True,
        late_wr_pct=60.0,
        opposite_signs_selected=False,
    )
    assert result["active"] is False
    assert result["valid"] is True
