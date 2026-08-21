"""Запрет «драфт против названной стороны» на оконных моделях панели.

Проверяется ровно то, чем запрет отличается от обычного порога: он смотрит на
ЗНАК доли драфта, а не на её величину; ноль и отсутствие доли возражением не
считаются; причина не теряется ни в строке, ни в журнале; выключатель
возвращает прежнее поведение целиком.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import ml_panel as MP  # noqa: E402

KNOTS_X = (-2.0, -1.0, 0.0, 1.0, 2.0)
KNOTS_Y = (0.20, 0.35, 0.50, 0.68, 0.85)


def window(threshold=0.60):
    """Оконная модель: `positive`/`negative` — сами стороны."""
    return MP.ModelSpec(key="w_10_20", title="окно 10-20", positive="Radiant",
                        negative="Dire", threshold=threshold, groups=("dict",),
                        knots_x=KNOTS_X, knots_y=KNOTS_Y)


def verdict(draft_share, raw=2.0, threshold=0.60, module=None):
    mod = module or MP
    return mod.evaluate(window(threshold), raw, {"dict": True},
                        draft_share=draft_share, fill=1.0, missing=())


class TestGate:
    def test_draft_against_kills_a_passing_verdict(self):
        """Тот же вердикт без доли проходит — значит гасит именно знак."""
        assert verdict(None).ok is True
        v = verdict(-0.19)
        assert v.ok is False
        assert v.blocked == MP.BLOCK_DRAFT_AGAINST

    def test_draft_for_the_side_passes(self):
        v = verdict(+0.19)
        assert v.ok is True and v.blocked is None

    def test_zero_is_silence_not_objection(self):
        """E-217 §1: ставки, где драфту нечего сказать, — лучшие, а не худшие."""
        v = verdict(0.0)
        assert v.ok is True and v.blocked is None

    def test_missing_share_does_not_block(self):
        """«Против» надо доказать: без доли (не в DRAFT_KEYS) запрета нет."""
        v = verdict(None)
        assert v.ok is True and v.blocked is None

    def test_verdict_below_threshold_is_not_blamed_on_the_draft(self):
        """Причина не подменяется: порог не пройден — это не запрет драфта."""
        v = verdict(-0.19, raw=0.0)               # p 0.50, порога не берёт
        assert v.ok is False and v.blocked is None

    def test_blocked_verdict_is_not_the_best_of_family(self):
        good = verdict(+0.05, raw=1.0)            # p 0.68
        killed = verdict(-0.05, raw=2.0)          # p 0.85, но драфт против
        assert MP.best_of([good, killed]) is good


class TestVisibility:
    def test_reason_is_printed_in_the_line(self):
        """🔴 по драфту обязан отличаться от 🔴 по порогу прямо в сообщении."""
        text = MP.render([verdict(-0.19)])
        assert MP.NO_MARK in text
        assert MP.BLOCK_DRAFT_AGAINST in text

    def test_passing_verdict_has_no_reason_in_the_line(self):
        assert MP.BLOCK_DRAFT_AGAINST not in MP.render([verdict(+0.19)])

    def test_reason_goes_to_the_journal(self):
        """Без записи запрет нельзя ни проверить задним числом, ни померить."""
        row = MP.journal_row("8955934230", [verdict(-0.19)])
        assert row["schema"] >= 4
        assert row["models"][0]["blocked"] == MP.BLOCK_DRAFT_AGAINST
        assert row["models"][0]["ok"] is False

    def test_journal_keeps_none_when_nothing_was_blocked(self):
        row = MP.journal_row("8955934230", [verdict(+0.19)])
        assert row["models"][0]["blocked"] is None


@pytest.fixture()
def gate_off(monkeypatch):
    """Выключатель читается при импорте, поэтому модуль перезагружается."""
    monkeypatch.setenv("ML_PANEL_DRAFT_AGAINST_BLOCK", "0")
    yield importlib.reload(MP)
    monkeypatch.delenv("ML_PANEL_DRAFT_AGAINST_BLOCK", raising=False)
    importlib.reload(MP)


class TestSwitch:
    def test_switch_off_restores_previous_behaviour(self, gate_off):
        v = verdict(-0.19, module=gate_off)
        assert v.ok is True and v.blocked is None

    def test_switch_is_on_by_default(self):
        assert MP.DRAFT_AGAINST_BLOCK is True
