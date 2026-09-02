"""Пороги TIER-гейта, которые ДЕЙСТВИТЕЛЬНО стоят в проде.

Зачем отдельный файл рядом с `test_tier_threshold_switch.py`. Тот модуль описывает
поведение гейта сценариями; этот фиксирует сами боевые значения, чтобы их смена
была осознанным действием с красным тестом, а не побочным эффектом правки.

История расхождения 60/65. До 02.09.2026 в рантайме стояло 60, а
`test_tier_threshold_switch.py` требовал 65 и поэтому вечно пропускался целиком:
TIER-гейт не был покрыт ни одним выполняющимся тестом, и расхождение не было
видно. 02.09.2026 alex принял решение «tier2 = 65wr», значение landed, и модуль
со сценариями разблокировался.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


def test_tier1_wr_threshold_is_60() -> None:
    assert int(runtime.STAR_THRESHOLD_WR_TIER1) == 60


def test_tier2_wr_threshold_is_65() -> None:
    """Решение alex от 02.09.2026: tier2 судится строже tier1."""
    assert int(runtime.STAR_THRESHOLD_WR_TIER2) == 65


def test_tier_signal_min_thresholds_follow_the_wr_thresholds() -> None:
    assert int(runtime.TIER_SIGNAL_MIN_THRESHOLD_TIER1_BASE) == 60
    assert int(runtime.TIER_SIGNAL_MIN_THRESHOLD_TIER2_BASE) == 65
    # Фактический порог = max(BASE, STAR_THRESHOLD_WR_*), поэтому оба равны своим BASE.
    assert int(runtime.TIER_SIGNAL_MIN_THRESHOLD_TIER1) == 60
    assert int(runtime.TIER_SIGNAL_MIN_THRESHOLD_TIER2) == 65


def test_block_labels_name_the_threshold_they_enforce() -> None:
    """Метка обязана совпадать с порогом: она уходит в журнал вердиктов и add_url."""
    assert runtime.TIER_THRESHOLD_STATUS_TIER1_MIN60_BLOCK == "tier1_min60_block"
    assert runtime.TIER_THRESHOLD_REASON_TIER1_MIN60_BLOCK == "below_tier1_min60"
    assert runtime.TIER_THRESHOLD_STATUS_TIER2_MIN65_BLOCK == "tier2_min65_block"
    assert runtime.TIER_THRESHOLD_REASON_TIER2_MIN65_BLOCK == "below_tier2_min65"


def test_old_tier2_min60_labels_are_gone() -> None:
    """Прежних меток tier2_min60_block больше нет — иначе журнал путал бы пороги."""
    assert not hasattr(runtime, "TIER_THRESHOLD_STATUS_TIER2_MIN60_BLOCK")
    assert not hasattr(runtime, "TIER_THRESHOLD_REASON_TIER2_MIN60_BLOCK")
