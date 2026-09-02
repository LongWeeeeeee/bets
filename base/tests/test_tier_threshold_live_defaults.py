"""Пороги TIER-гейта, которые ДЕЙСТВИТЕЛЬНО стоят в проде.

Зачем отдельный файл рядом с `test_tier_threshold_switch.py`. Тот модуль описывает
невыкаченный вариант «tier2 требует WR65» и целиком уходит в skip, потому что
`STAR_THRESHOLD_WR_TIER2` в рантайме равен 60, а констант
`TIER_THRESHOLD_STATUS_TIER2_MIN65_BLOCK` / `..._REASON_TIER2_MIN65_BLOCK` нет
вовсе. В результате TIER-гейт не покрыт НИ ОДНИМ выполняющимся тестом, и
расхождение 60/65 не видно: skip выглядит как «фича ещё не приехала», а не как
«боевое значение другое».

Этот файл фиксирует текущее боевое значение, чтобы его смена была осознанным
действием с красным тестом, а не побочным эффектом правки.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


def test_tier_wr_thresholds_are_60_in_prod() -> None:
    assert int(runtime.STAR_THRESHOLD_WR_TIER1) == 60
    assert int(runtime.STAR_THRESHOLD_WR_TIER2) == 60


def test_tier_signal_min_thresholds_are_60_in_prod() -> None:
    assert int(runtime.TIER_SIGNAL_MIN_THRESHOLD_TIER1) == 60
    assert int(runtime.TIER_SIGNAL_MIN_THRESHOLD_TIER2) == 60
    assert int(runtime.TIER_SIGNAL_MIN_THRESHOLD_TIER2_BASE) == 60


def test_tier1_block_labels_are_pinned() -> None:
    assert runtime.TIER_THRESHOLD_STATUS_TIER1_MIN60_BLOCK == "tier1_min60_block"
    assert runtime.TIER_THRESHOLD_REASON_TIER1_MIN60_BLOCK == "below_tier1_min60"


def test_tier2_min65_labels_are_not_landed() -> None:
    """Метка ветки tier2-мин65 отсутствует — ветка не выкачена.

    Если этот тест упадёт, значит вариант из `test_tier_threshold_switch.py`
    приехал: тогда его skip снимется сам, а боевое значение tier2 надо
    перечитать там, а не здесь.
    """
    assert not hasattr(runtime, "TIER_THRESHOLD_STATUS_TIER2_MIN65_BLOCK")
    assert not hasattr(runtime, "TIER_THRESHOLD_REASON_TIER2_MIN65_BLOCK")
