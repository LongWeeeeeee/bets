"""Гейт допустимых комбинаций STAR-блоков (Early Winner / Late / All).

Правила: блок валиден при WR >= 65 и >= 2 star-хитах одного знака без
встречных хитов внутри блока. Допустимые конфигурации — early_end+all,
late+all, early_end+late (без встречных хитов в All), а также одиночные
early_end (без встречных в Late/All), late (без встречных в All) и all
(встречные в Early Winner / Late допустимы).

Референс-кейс (прод, 00:00): Early Winner cp1vs2 -4 (WR60), Late solo -3
(WR60), All protracker -4 (WR60) — все блоки по одному хиту на WR60, ставка
уходить не должна.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


def _hits(*pairs: tuple) -> List[Dict[str, Any]]:
    return [
        {"metric": metric, "value": float(value), "wr_level": 60}
        for metric, value in pairs
    ]


def _evaluate(
    *,
    target_sign: Optional[int] = -1,
    early_end_hits: Optional[List[Dict[str, Any]]] = None,
    early_end_wr_pct: Optional[float] = None,
    late_hits: Optional[List[Dict[str, Any]]] = None,
    late_wr_pct: Optional[float] = None,
    all_hits: Optional[List[Dict[str, Any]]] = None,
    all_wr_pct: Optional[float] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    return runtime._evaluate_star_block_combination_gate(
        target_sign=target_sign,
        early_end_hits=early_end_hits or [],
        early_end_wr_pct=early_end_wr_pct,
        late_hits=late_hits or [],
        late_wr_pct=late_wr_pct,
        all_hits=all_hits or [],
        all_wr_pct=all_wr_pct,
        **kwargs,
    )


def test_gate_thresholds_are_pinned() -> None:
    assert runtime.STAR_COMBINATION_MIN_WR == 65.0
    assert runtime.STAR_COMBINATION_MIN_HITS == 2
    assert runtime.STAR_COMBINATION_GATE_REJECT_REASON == "star_signal_rejected_block_combination"


def test_production_case_all_blocks_wr60_single_hit_is_blocked() -> None:
    gate = _evaluate(
        target_sign=-1,
        early_end_hits=_hits(("counterpick_1vs2", -4)),
        early_end_wr_pct=60.0,
        late_hits=_hits(("solo", -3)),
        late_wr_pct=60.0,
        all_hits=_hits(("dota2protracker_cp1vs1", -4.0)),
        all_wr_pct=60.0,
    )

    assert gate["active"] is True
    assert gate["blocked"] is True
    assert gate["accepted_combinations"] == []
    assert gate["early_end_valid"] is False
    assert gate["late_valid"] is False
    assert gate["all_valid"] is False


def test_single_hit_at_wr65_is_not_enough() -> None:
    gate = _evaluate(
        target_sign=1,
        early_end_hits=_hits(("solo", 4)),
        early_end_wr_pct=65.0,
        all_hits=_hits(("solo", 4)),
        all_wr_pct=65.0,
    )

    assert gate["blocked"] is True


def test_two_hits_below_wr65_is_not_enough() -> None:
    gate = _evaluate(
        target_sign=1,
        late_hits=_hits(("solo", 4), ("counterpick_1vs1", 7)),
        late_wr_pct=60.0,
        all_hits=_hits(("solo", 4), ("counterpick_1vs1", 7)),
        all_wr_pct=60.0,
    )

    assert gate["blocked"] is True


def test_early_end_plus_all_passes() -> None:
    gate = _evaluate(
        target_sign=1,
        early_end_hits=_hits(("solo", 4), ("counterpick_1vs1", 7)),
        early_end_wr_pct=65.0,
        all_hits=_hits(("solo", 4), ("counterpick_1vs1", 7)),
        all_wr_pct=65.0,
    )

    assert gate["blocked"] is False
    assert "early_end+all" in gate["accepted_combinations"]


def test_late_plus_all_passes() -> None:
    gate = _evaluate(
        target_sign=-1,
        late_hits=_hits(("solo", -4), ("counterpick_1vs1", -7)),
        late_wr_pct=65.0,
        all_hits=_hits(("solo", -4), ("counterpick_1vs1", -7)),
        all_wr_pct=65.0,
    )

    assert gate["blocked"] is False
    assert "late+all" in gate["accepted_combinations"]


def test_early_end_plus_late_requires_no_opposite_all_hits() -> None:
    clean = _evaluate(
        target_sign=1,
        early_end_hits=_hits(("solo", 4), ("counterpick_1vs1", 7)),
        early_end_wr_pct=65.0,
        late_hits=_hits(("solo", 4), ("counterpick_1vs1", 7)),
        late_wr_pct=65.0,
    )
    assert clean["blocked"] is False
    assert "early_end+late" in clean["accepted_combinations"]

    with_same_sign_all_hit = _evaluate(
        target_sign=1,
        early_end_hits=_hits(("solo", 4), ("counterpick_1vs1", 7)),
        early_end_wr_pct=65.0,
        late_hits=_hits(("solo", 4), ("counterpick_1vs1", 7)),
        late_wr_pct=65.0,
        all_hits=_hits(("dota2protracker_cp1vs1", 5)),
        all_wr_pct=60.0,
    )
    assert with_same_sign_all_hit["blocked"] is False

    with_opposite_all_hit = _evaluate(
        target_sign=1,
        early_end_hits=_hits(("solo", 4), ("counterpick_1vs1", 7)),
        early_end_wr_pct=65.0,
        late_hits=_hits(("solo", 4), ("counterpick_1vs1", 7)),
        late_wr_pct=65.0,
        all_hits=_hits(("dota2protracker_cp1vs1", -5)),
        all_wr_pct=60.0,
    )
    assert with_opposite_all_hit["blocked"] is True
    assert with_opposite_all_hit["all_opposite_hit_metrics"] == ["dota2protracker_cp1vs1"]


def test_late_below_70_is_blocked_by_opposite_all_hit() -> None:
    """Ключевой пример: late WR65 + 2 хита, в All один встречный WR60 хит."""

    gate = _evaluate(
        target_sign=-1,
        late_hits=_hits(("solo", -4), ("counterpick_1vs1", -6)),
        late_wr_pct=65.0,
        all_hits=_hits(("counterpick_1vs1", 5)),
        all_wr_pct=60.0,
    )

    assert gate["blocked"] is True
    assert gate["late_valid"] is True
    assert gate["late_allows_opposite_all"] is False


def test_jenz_late70_is_blocked_by_opposite_all80() -> None:
    gate = _evaluate(
        target_sign=-1,
        late_hits=_hits(("solo", -6), ("counterpick_1vs1", -9)),
        late_wr_pct=70.0,
        all_hits=_hits(("counterpick_1vs1", 8), ("counterpick_1vs2", 10)),
        all_wr_pct=80.0,
    )

    assert gate["blocked"] is True
    assert gate["accepted_combinations"] == []
    assert gate["late_valid"] is True
    assert gate["all_opposite_hit_metrics"] == [
        "counterpick_1vs1",
        "counterpick_1vs2",
    ]
    assert gate["late_allows_opposite_all"] is False


def test_early_end_only_is_blocked_by_opposite_hits_elsewhere() -> None:
    clean = _evaluate(
        target_sign=1,
        early_end_hits=_hits(("solo", 4), ("counterpick_1vs1", 7)),
        early_end_wr_pct=65.0,
    )
    assert clean["blocked"] is False
    assert "early_end" in clean["accepted_combinations"]

    opposite_late = _evaluate(
        target_sign=1,
        early_end_hits=_hits(("solo", 4), ("counterpick_1vs1", 7)),
        early_end_wr_pct=65.0,
        late_hits=_hits(("solo", -4)),
        late_wr_pct=60.0,
    )
    assert opposite_late["blocked"] is True

    opposite_all = _evaluate(
        target_sign=1,
        early_end_hits=_hits(("solo", 4), ("counterpick_1vs1", 7)),
        early_end_wr_pct=65.0,
        all_hits=_hits(("solo", -4)),
        all_wr_pct=60.0,
    )
    assert opposite_all["blocked"] is True


def test_all_only_tolerates_opposite_hits_in_early_and_late() -> None:
    gate = _evaluate(
        target_sign=1,
        early_end_hits=_hits(("solo", -4)),
        early_end_wr_pct=60.0,
        late_hits=_hits(("solo", -4)),
        late_wr_pct=60.0,
        all_hits=_hits(("solo", 4), ("counterpick_1vs1", 7)),
        all_wr_pct=65.0,
    )

    assert gate["blocked"] is False
    assert gate["accepted_combinations"] == ["all"]


def test_block_with_internal_opposite_hit_is_not_valid() -> None:
    gate = _evaluate(
        target_sign=1,
        all_hits=_hits(("solo", 4), ("counterpick_1vs1", 7), ("counterpick_1vs2", -5)),
        all_wr_pct=65.0,
    )

    assert gate["all_valid"] is False
    assert gate["blocked"] is True


def test_gate_is_inactive_without_target_sign_or_when_disabled() -> None:
    no_sign = _evaluate(target_sign=None)
    assert no_sign["active"] is False
    assert no_sign["blocked"] is False

    disabled = _evaluate(
        target_sign=1,
        early_end_hits=_hits(("solo", 4)),
        early_end_wr_pct=60.0,
        enabled=False,
    )
    assert disabled["active"] is False
    assert disabled["blocked"] is False

    force_test = _evaluate(
        target_sign=1,
        early_end_hits=_hits(("solo", 4)),
        early_end_wr_pct=60.0,
        force_odds_signal_test_active=True,
    )
    assert force_test["active"] is False
    assert force_test["blocked"] is False


def test_reject_details_carry_block_summary() -> None:
    gate = _evaluate(
        target_sign=-1,
        early_end_hits=_hits(("counterpick_1vs2", -4)),
        early_end_wr_pct=60.0,
        late_hits=_hits(("solo", -3)),
        late_wr_pct=60.0,
        all_hits=_hits(("dota2protracker_cp1vs1", -4.0)),
        all_wr_pct=60.0,
    )
    details = runtime._star_combination_gate_reject_details(gate)

    assert details["dispatch_status_label"] == runtime.STAR_COMBINATION_GATE_STATUS_LABEL
    assert details["star_combination_min_wr"] == 65.0
    assert details["star_combination_min_hits"] == 2
    assert details["star_combination_blocks"]["late"]["hits_neg"] == ["solo"]
    assert "sign=-1" in runtime._format_star_combination_gate_log(gate)


# ── Сценарные проверки в реальном потоке диспатча ───────────────────────────

TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from test_networth_dispatch_gates import BranchScenario, _run_branch_scenario  # noqa: E402
from test_same_sign_lane_adv_dispatch import _patch_early_late_wr  # noqa: E402


def test_all_blocks_wr60_single_hit_signal_is_rejected_end_to_end(monkeypatch) -> None:
    """Zero Tenacity: early/late/all по одному WR60-хиту → ставка не уходит."""

    _patch_early_late_wr(monkeypatch, early_level=60, late_level=60, all_level=60)
    case = BranchScenario(
        name="star_combination_gate_reject",
        game_time_seconds=8 * 60,
        target_side="dire",
        target_networth_diff=1500,
        has_early_star=True,
        early_sign=-1,
        has_late_star=True,
        late_sign=-1,
        has_all_star=True,
        all_sign=-1,
        expected_send_calls=0,
        raw_early_output={"counterpick_1vs2": -5},
        raw_early_end_output={"counterpick_1vs2": -4},
        raw_mid_output={"solo": -3},
        raw_post_lane_output={"dota2protracker_cp1vs1": -4.0},
    )

    result = _run_branch_scenario(monkeypatch, case, star_combination_gate_enabled=True)

    assert result.sent_messages == []
    assert result.queued_payload is None
    assert result.add_url_calls[-1]["reason"] == runtime.STAR_COMBINATION_GATE_REJECT_REASON
    details = result.add_url_calls[-1]["details"]
    assert details["dispatch_status_label"] == runtime.STAR_COMBINATION_GATE_STATUS_LABEL
    assert details["star_combination_late_valid"] is False
    assert details["star_combination_all_valid"] is False


def test_jenz_late70_opposite_all80_is_rejected_end_to_end(monkeypatch) -> None:
    """Jenz 24:27: Late за Jenz, но валидные Early/All за FTS → отказ."""

    _patch_early_late_wr(monkeypatch, early_level=80, late_level=70, all_level=80)
    case = BranchScenario(
        name="jenz_late70_opposite_all80",
        game_time_seconds=(24 * 60) + 27,
        target_side="radiant",
        target_networth_diff=2270,
        has_early_star=True,
        early_sign=-1,
        has_late_star=True,
        late_sign=1,
        has_all_star=True,
        all_sign=-1,
        expected_send_calls=0,
        raw_early_output={
            "counterpick_1vs1": -18,
            "counterpick_1vs2": -30,
            "solo": -8,
        },
        raw_early_end_output={
            "counterpick_1vs1": -15,
            "counterpick_1vs2": -24,
            "solo": -6,
        },
        raw_mid_output={"counterpick_1vs2": 9, "solo": 3},
        raw_post_lane_output={
            "counterpick_1vs1": -8,
            "counterpick_1vs2": -10,
        },
    )

    result = _run_branch_scenario(
        monkeypatch,
        case,
        star_combination_gate_enabled=True,
    )

    assert result.sent_messages == []
    assert result.queued_payload is None
    assert (
        result.add_url_calls[-1]["reason"]
        == runtime.STAR_COMBINATION_GATE_REJECT_REASON
    )
    details = result.add_url_calls[-1]["details"]
    assert details["star_combination_late_valid"] is True
    assert details["star_combination_all_valid"] is False
    assert details["star_combination_all_opposite_hits"] == [
        "counterpick_1vs1",
        "counterpick_1vs2",
    ]


def test_late_plus_all_wr65_two_hits_passes_the_gate(monkeypatch) -> None:
    """late+all по WR65 с двумя хитами гейт не режет (дальше решают ветки)."""

    _patch_early_late_wr(monkeypatch, early_level=60, late_level=65, all_level=65)
    case = BranchScenario(
        name="star_combination_gate_pass",
        game_time_seconds=8 * 60,
        target_side="dire",
        target_networth_diff=1500,
        has_early_star=False,
        early_sign=-1,
        has_late_star=True,
        late_sign=-1,
        has_all_star=True,
        all_sign=-1,
        expected_send_calls=0,
        raw_early_output={"solo": 0},
        raw_early_end_output={"solo": 0},
        raw_mid_output={"solo": -4, "counterpick_1vs1": -7},
        raw_post_lane_output={"solo": -4, "counterpick_1vs1": -7},
    )

    result = _run_branch_scenario(monkeypatch, case, star_combination_gate_enabled=True)

    reasons = [call["reason"] for call in result.add_url_calls]
    assert runtime.STAR_COMBINATION_GATE_REJECT_REASON not in reasons
