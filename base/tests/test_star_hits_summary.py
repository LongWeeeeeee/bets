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
    # Пороги counterpick_1vs2 в mid_output: 2/3/4/5/6/8/12/14 -> WR60..WR95.
    # Ожидание 4/6/8 (WR60/65/70) устарело: коммит cf6260e от 09.08.2026 опустил
    # пороги mid_output под шкалу пересобранного late-словаря — ровно сценарий
    # E-69, где пересборка словаря сжимает шкалу метрик. Тест тогда не обновили,
    # и он падал до 19.08.2026. Значение -6.0 законно дотягивает до WR80.
    level = runtime._max_star_wr_level_for_metric(
        metric="counterpick_1vs2",
        value=-6.0,
        section="mid_output",
    )
    assert level == 80

    level = runtime._max_star_wr_level_for_metric(
        metric="counterpick_1vs2",
        value=-4.0,
        section="mid_output",
    )
    # -4.0 дотягивает до WR70 (порог 4), не до WR60 — та же причина, что выше.
    assert level == 70

    level = runtime._max_star_wr_level_for_metric(
        metric="counterpick_1vs2",
        value=-3.0,
        section="mid_output",
    )
    # -3.0 теперь дотягивает до WR65 (порог 3). Прежде порог WR60 был 4, и это
    # значение не давало ни одного уровня — та же переоценка cf6260e.
    assert level == 65


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
    assert "Late: Counterpick_1vs2 -6 (WR80)" in block
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


def test_star_summary_shows_win_model_line_even_without_hits(monkeypatch) -> None:
    """Оценка ML-модели должна быть видна в КАЖДОМ сообщении, а не только при хитах."""
    import win_model_veto as V

    monkeypatch.setenv("PREMATCH_ML_ENABLED", "1")
    # Without details the summary reads the module-global last panel
    # (cyberscore_try.py `_format_win_model_line`); a real scoring test run
    # earlier in the same session leaves its panel there.
    monkeypatch.setitem(V._LAST_PANEL, "text", "")
    block = runtime._build_star_hits_summary_block(
        early_output={V.INDEX_KEY: 6.3},
        mid_output={V.INDEX_KEY: 6.3},
        all_output={V.INDEX_KEY: 6.3},
    )
    # С 20.09.2026 у строки может быть хвост «| данные до DD.MM (N дн.)» —
    # приписка свежести данных драфт-ансамбля (`win_model_veto.MODEL_DIR`);
    # без каталога с датой хвоста нет. Проверяем начало строки и что кроме
    # неё в блоке ничего нет.
    lines = [ln for ln in block.splitlines() if ln.strip()]
    assert len(lines) == 1
    assert lines[0].startswith("\U0001F916 ML-модель: Radiant 56.3%")
    assert lines[0] == "\U0001F916 ML-модель: Radiant 56.3%" or " | данные до " in lines[0]

    # знак читается как сторона
    block = runtime._build_star_hits_summary_block(
        early_output={V.INDEX_KEY: -8.0}, mid_output={}, all_output={})
    assert "Dire 58.0%" in block

    # модель недоступна -> строки нет, шум вместо данных не печатаем
    assert runtime._build_star_hits_summary_block(
        early_output={}, mid_output={}, all_output={}) == ""


def test_star_summary_keeps_model_line_together_with_hits(monkeypatch) -> None:
    import win_model_veto as V

    monkeypatch.setenv("PREMATCH_ML_ENABLED", "1")
    monkeypatch.setitem(V._LAST_PANEL, "text", "")
    block = runtime._build_star_hits_summary_block(
        early_output={"counterpick_1vs1": 9, V.INDEX_KEY: 4.0},
        mid_output={}, all_output={})
    first_line = block.splitlines()[0]
    assert first_line.startswith("\U0001F916 ML-модель: Radiant 54.0%")
    # хвост свежести данных (20.09.2026) — на той же строке, а не отдельной
    assert first_line == "\U0001F916 ML-модель: Radiant 54.0%" or " | данные до " in first_line
    assert "\u2b50 Star hits (WR60+):" in block


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
    assert "Late: Counterpick_1vs2 -6 (WR80)" in block
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
