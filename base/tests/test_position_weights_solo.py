"""solo считается без позиционного перекоса, фолбэк весов — равномерный.

Замер на ПРО 2026-08-05 (каждый блок на СВОЕЙ популяции; Late-гейт проходят
лишь 40% карт, поэтому на полной выборке метрика ложно выглядела инвертированной):
прод-веса у solo худшие в обоих блоках, равномерные лучше и там, и там,
а прежний фолбэк get_diff (3/2/1.5/0.9/0.7) — худшая схема из проверенных.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import functions


POSITIONS = ("pos1", "pos2", "pos3", "pos4", "pos5")


def test_solo_weights_are_flat():
    assert functions.SOLO_POSITION_WEIGHTS == {p: 1.0 for p in POSITIONS}


def test_get_diff_fallback_weights_are_flat(monkeypatch):
    """Без явных весов get_diff не должен вносить кор-перекос."""
    monkeypatch.setattr(functions, "_ENV_POS_WEIGHTS", None)
    # сильный герой на pos5, слабый на pos1 — при кор-перекосе знак был бы иным
    radiant = {"pos1": [(0.40, 500)], "pos5": [(0.60, 500)]}
    dire = {"pos1": [(0.60, 500)], "pos5": [(0.40, 500)]}

    flat = functions.get_diff(radiant, dire, _1vs2=True,
                              custom_position_weights={p: 1.0 for p in POSITIONS})
    fallback = functions.get_diff(radiant, dire, _1vs2=True)

    assert fallback == flat, "фолбэк перестал быть равномерным"
    assert flat == 0, "симметричный расклад должен давать ноль при равных весах"


def test_core_skew_would_change_the_sign():
    """Контроль: прежний перекос на том же раскладе даёт ненулевой знак."""
    radiant = {"pos1": [(0.40, 500)], "pos5": [(0.60, 500)]}
    dire = {"pos1": [(0.60, 500)], "pos5": [(0.40, 500)]}

    skewed = functions.get_diff(
        radiant, dire, _1vs2=True,
        custom_position_weights={"pos1": 3.0, "pos2": 2.0, "pos3": 1.5,
                                 "pos4": 0.9, "pos5": 0.7},
    )
    assert skewed < 0, "кор-перекос обязан утянуть знак к pos1"


def test_solo_call_site_uses_flat_weights():
    """В synergy_and_counterpick solo берёт именно SOLO_POSITION_WEIGHTS."""
    src = (BASE_DIR / "functions.py").read_text(encoding="utf-8")
    idx = src.index("phase_bucket['solo'] = get_diff(")
    call = src[idx:idx + 400]
    assert "custom_position_weights=SOLO_POSITION_WEIGHTS" in call
    assert "phase_weights" not in call.split(")")[0]
