"""STAR-калибровка «уровень → фактический WR» обязана быть включена по
умолчанию и покрывать все три фазы, а не только early/late.

02.09.2026: `data/star_confidence_calibration.json` уже нёс фазу `all`
(таблица `build_star_confidence_calibration.py`), но `STAR_ODDS_USE_CALIBRATION`
был выключен по умолчанию (`_safe_bool_env(..., False)`), и
`_load_star_confidence_calibration` перебирал только `("early", "late")` —
фаза `all` молча терялась при загрузке, даже если бы флаг был включён.
Итог: панель показывала номинальный уровень STAR-лестницы (65, 70, ...)
вместо измеренного фактического винрейта, и кэф «от» считался неправильным.

Тест бьёт в ту же точку, что и прод-сообщение: `_recommendation_from_star_levels`
(применяет калибровку) + `_format_wr_estimate_line` (форматирует строку
Telegram-сообщения, «WR≈NN.N% от кэфа X.XX»).
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402


def test_late_block_nominal_65_shows_calibrated_wr_by_default():
    """Поздняя фаза, номинальный уровень 65 -> калиброванный WR 56.79%, не 65."""
    assert cs.STAR_ODDS_USE_CALIBRATION is True, (
        "STAR_ODDS_USE_CALIBRATION обязан быть включён по умолчанию")
    rec = cs._recommendation_from_star_levels(
        [65], candidate_levels=[65], calibration_phase_key="late")
    assert rec is not None
    assert rec["wr_pct"] == 56.79, f"ожидали калиброванный WR 56.79, получили {rec['wr_pct']}"
    assert rec["min_odds"] == round(100.0 / 56.79, 2)

    line = cs._format_wr_estimate_line("Поздняя игра", "Team A", rec["wr_pct"], rec)
    assert line == "Поздняя игра: Team A WR≈56.8% от кэфа 1.76", line


def test_all_output_block_nominal_65_shows_calibrated_wr_by_default():
    """all_output фаза, номинальный уровень 65 -> калиброванный WR 49.88%, не 65.

    До фикса фаза `all` отсутствовала в загруженной таблице калибровки
    (`_load_star_confidence_calibration` перебирал только early/late), поэтому
    даже при включённом флаге этот блок остался бы на номинальном уровне.
    """
    rec = cs._recommendation_from_star_levels(
        [65], candidate_levels=[65], calibration_phase_key="all")
    assert rec is not None
    assert rec["wr_pct"] == 49.88, f"ожидали калиброванный WR 49.88, получили {rec['wr_pct']}"
    assert rec["min_odds"] == round(100.0 / 49.88, 2)

    line = cs._format_wr_estimate_line("Всё время", "Team B", rec["wr_pct"], rec)
    assert line == "Всё время: Team B WR≈49.9% от кэфа 2.00", line


def test_calibration_table_loads_all_three_phases():
    """`_load_star_confidence_calibration` не теряет фазу `all` при парсинге."""
    table = cs._load_star_confidence_calibration()
    assert set(table.keys()) >= {"early", "late", "all"}, (
        f"фаза 'all' пропала при загрузке: {sorted(table.keys())}")
    assert table["all"][65] == 49.88
    assert table["late"][65] == 56.79
