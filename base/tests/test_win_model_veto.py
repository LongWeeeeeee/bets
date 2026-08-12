"""Вето драфтовой ML-модели: блок со знаком против модели не идёт.

Проверяются четыре вещи, каждая из которых способна тихо сломать прод:
  * вето срабатывает только на НЕСОГЛАСИИ и только выше порога СВОЕЙ секции;
  * пороги секций разные (early 10, early_end 5, mid 9, all 0) — это результат
    тюнинга «понижать, пока отсекается мусор», а не круглые числа;
  * отсутствие индекса, сломанная модель и выключенный флаг = вето НЕ работает
    (fail-open): молча обнулять отбор из-за модели недопустимо;
  * `format_output_dict` действительно перестаёт считать блок валидным.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import win_model_veto as V  # noqa: E402


def _block(index, **extra):
    data = {V.INDEX_KEY: index}
    data.update(extra)
    return data


def test_veto_only_on_disagreement():
    # модель за Radiant (+12), блок тоже за Radiant -> вето нет
    assert V.blocks_veto(1, _block(12.0), "early_output") is False
    # модель за Dire (-12), блок за Radiant -> вето
    assert V.blocks_veto(1, _block(-12.0), "early_output") is True
    assert V.blocks_veto(-1, _block(12.0), "early_output") is True


def test_section_thresholds_differ():
    # early_output: порог 10 — несогласие на 9 не режет, на 11 режет
    assert V.blocks_veto(1, _block(-9.0), "early_output") is False
    assert V.blocks_veto(1, _block(-11.0), "early_output") is True
    # early_end_output: порог 5
    assert V.blocks_veto(1, _block(-4.0), "early_end_output") is False
    assert V.blocks_veto(1, _block(-6.0), "early_end_output") is True
    # mid_output: порог 9
    assert V.blocks_veto(1, _block(-8.0), "mid_output") is False
    assert V.blocks_veto(1, _block(-9.5), "mid_output") is True
    # all_output: режется любое несогласие
    assert V.blocks_veto(1, _block(-0.5), "all_output") is True


def test_fail_open_without_index_or_flag(monkeypatch):
    assert V.blocks_veto(1, {}, "all_output") is False              # индекса нет
    assert V.blocks_veto(1, _block(None), "all_output") is False    # индекс None
    assert V.blocks_veto(1, _block("нет"), "all_output") is False   # мусор в поле
    assert V.blocks_veto(0, _block(-12.0), "all_output") is False   # знака блока нет
    monkeypatch.setattr(V, "VETO_ENABLED", False)
    assert V.blocks_veto(1, _block(-12.0), "all_output") is False   # флаг выключен


def test_win_index_fails_open_on_broken_model(monkeypatch, tmp_path):
    """Нет файлов модели -> None, а не исключение и не блокировка."""
    monkeypatch.setattr(V, "MODEL_DIR", tmp_path / "нет-такой-папки")
    monkeypatch.setitem(V._state, "loaded", False)
    monkeypatch.setattr(V, "_cache", {})
    draft = {f"pos{i}": {"hero_id": i} for i in range(1, 6)}
    assert V.win_index(draft, {f"pos{i}": {"hero_id": i + 10} for i in range(1, 6)}) is None
    assert V.load_error() is not None


def test_format_output_dict_respects_veto(monkeypatch):
    import functions

    monkeypatch.setattr(
        functions, "STAR_THRESHOLDS_BY_WR",
        {60: {"all_output": [("counterpick_1vs1", 4)]}}, raising=False)

    # согласие: блок валиден
    assert functions.format_output_dict(
        {"all_output": {"counterpick_1vs1": 9, V.INDEX_KEY: 6.0}},
        target_wr=60, late_signal_gate_enabled=False) is True
    # несогласие: блок отменён
    assert functions.format_output_dict(
        {"all_output": {"counterpick_1vs1": 9, V.INDEX_KEY: -6.0}},
        target_wr=60, late_signal_gate_enabled=False) is False
    # индекса нет — поведение как до правки
    assert functions.format_output_dict(
        {"all_output": {"counterpick_1vs1": 9}},
        target_wr=60, late_signal_gate_enabled=False) is True


def test_ml_win_index_is_star_only_in_all_output(monkeypatch):
    """Модель как ЗВЁЗДНАЯ метрика: порог только в all_output, 10 на WR60.

    В early и late порогов нет намеренно — там замер показал шум либо минус,
    а в all_output метрика лишь добавляет карты: вето стоит на нуле и уже сняло
    все несогласия, поэтому убирать ей нечего (E-73, E-76).
    """
    import functions

    monkeypatch.setattr(
        functions, "STAR_THRESHOLDS_BY_WR",
        {60: {"all_output": [("ml_win_index", 10)], "early_output": [], "mid_output": []}},
        raising=False)

    # модель одна делает блок валидным
    assert functions.format_output_dict(
        {"all_output": {V.INDEX_KEY: 12.0}}, target_wr=60,
        late_signal_gate_enabled=False) is True
    # ниже порога — нет
    assert functions.format_output_dict(
        {"all_output": {V.INDEX_KEY: 8.0}}, target_wr=60,
        late_signal_gate_enabled=False) is False
    # в блоке без порога метрика звезды не создаёт
    assert functions.format_output_dict(
        {"early_output": {V.INDEX_KEY: 12.0}}, target_wr=60,
        late_signal_gate_enabled=False) is False


def test_star_and_veto_do_not_conflict(monkeypatch):
    """Метрика и вето — один индекс: согласие даёт звезду, несогласие рушит блок."""
    import functions

    monkeypatch.setattr(
        functions, "STAR_THRESHOLDS_BY_WR",
        {60: {"all_output": [("counterpick_1vs1", 4), ("ml_win_index", 10)]}},
        raising=False)

    assert functions.format_output_dict(
        {"all_output": {"counterpick_1vs1": 9, V.INDEX_KEY: 12.0}}, target_wr=60,
        late_signal_gate_enabled=False) is True
    # знак модели против знака блока: конфликт хитов, блок не идёт
    assert functions.format_output_dict(
        {"all_output": {"counterpick_1vs1": 9, V.INDEX_KEY: -12.0}}, target_wr=60,
        late_signal_gate_enabled=False) is False
