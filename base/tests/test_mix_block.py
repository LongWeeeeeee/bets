"""Блок Mix: метрики внешних источников вынесены из All.

ProTracker и голосование DLTV считаются не по нашим словарям, поэтому в
сообщении показываются отдельным блоком Mix. Значения при этом по-прежнему
лежат в ``all_output`` — переезд касается ПОДАЧИ, а не места хранения и не
оценки STAR: `dota2protracker_cp1vs1` и `dltv_rating` остаются STAR-метриками
секции ``all_output``, поэтому решения диспатча не меняются.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import cyberscore_try as cs  # noqa: E402

MIX_LABELS = (
    "Protracker_1vs1",
    "Protracker_duo",
    "Protracker_solo",
    "Protracker_solo_overall",
    "DLTV_rating",
)


def _block_payload() -> dict:
    return {
        "counterpick_1vs1": 8,
        "pos1_vs_pos1": None,
        "counterpick_1vs2": 5,
        "solo": 3,
        "synergy_duo": None,
        "synergy_trio": None,
        "dota2protracker_cp1vs1": None,
        "dota2protracker_duo": None,
        "dota2protracker_solo": 1.10,
        "dota2protracker_solo_overall": 0.50,
        "dltv_rating": None,
    }


def test_all_block_no_longer_carries_external_metrics() -> None:
    text = cs._format_star_metrics_block(
        "All:", _block_payload(), cs._STAR_METRICS_BLOCK_ALL_LIST
    )

    for label in MIX_LABELS:
        assert label not in text, f"{label} не должен оставаться в All"
    assert "Counterpick_1vs1: 8" in text
    assert "Solo: 3" in text


def test_mix_block_carries_exactly_the_external_metrics() -> None:
    text = cs._format_star_metrics_block(
        "Mix:", _block_payload(), cs._STAR_METRICS_BLOCK_MIX_LIST
    )

    assert text.startswith("Mix:\n")
    for label in MIX_LABELS:
        assert f"{label}:" in text
    # Формат значений ProTracker сохраняется прежним.
    assert "Protracker_solo: +1.10" in text
    assert "Protracker_solo_overall: +0.50" in text
    # Метрики словарей в Mix не просачиваются.
    for label in ("Counterpick_1vs1", "Solo", "Synergy_duo"):
        assert f"{label}:" not in text


def test_mix_block_goes_last_in_the_message() -> None:
    composed = cs._compose_star_metric_blocks_for_message(
        "Early:\n", "Late:\n", "All:\n", "Mix:\n"
    )

    assert composed == "Early:\nLate:\nAll:\nMix:\n"


def test_message_composer_stays_compatible_without_mix() -> None:
    """Старые вызовы на трёх блоках обязаны собирать прежний текст."""
    assert cs._compose_star_metric_blocks_for_message("E\n", "L\n", "A\n") == "E\nL\nA\n"


def test_star_membership_is_untouched_by_the_move() -> None:
    """Оценка STAR не поехала: обе метрики остаются star-метриками all_output."""
    import functions

    assert "dltv_rating" in functions.STAR_SIGNAL_METRICS
    assert "dota2protracker_cp1vs1" in functions.STAR_SIGNAL_METRICS
    thresholds = functions._star_thresholds_for_wr(60, "all_output") \
        if hasattr(functions, "_star_thresholds_for_wr") \
        else cs._star_thresholds_for_wr(60, "all_output")
    assert "dltv_rating" in thresholds
    assert "dota2protracker_cp1vs1" in thresholds


def test_star_hits_summary_splits_mix_out_of_all() -> None:
    """В сводке хиты внешних источников идут строкой Mix, а не внутри All."""
    all_output = {
        "counterpick_1vs1": 9,
        "counterpick_1vs2": 6,
        "dltv_rating": 35,
        "dota2protracker_cp1vs1": 9,
    }

    text = cs._build_star_hits_summary_block(
        early_output={}, mid_output={}, all_output=all_output
    )

    assert "All:" in text and "Mix:" in text
    all_line = next(ln for ln in text.splitlines() if ln.strip().startswith("All:"))
    mix_line = next(ln for ln in text.splitlines() if ln.strip().startswith("Mix:"))
    assert "Counterpick_1vs1" in all_line
    assert "DLTV_rating" not in all_line
    assert "DLTV_rating" in mix_line
    assert "Protracker_1vs1" in mix_line
