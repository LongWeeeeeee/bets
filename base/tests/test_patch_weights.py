"""Веса патчей: чем старше патч, тем меньше вклад его матчей в счётчики.

Идея alex (08.08). Мотив прямой: корпус получил патч 7.41e, и словарь, обученный
на смеси патчей, тянет старый баланс героев в предсказание новых матчей.

ВАЖНАЯ ОГОВОРКА ПРО ЗАМЕР, а не про код. Наш holdout собран на СТАРЫХ патчах,
поэтому понижение веса старых патчей будет выглядеть на нём хуже — мы убираем
ровно те данные, которые больше всего похожи на выборку оценки. Честная проверка
требует holdout из самого свежего патча. Тест ниже проверяет только механику.

Дефолт `ANALISE_PATCH_WEIGHT_HALFLIFE=0` выключает веса и сохраняет целые
счётчики — это первый и главный тест.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture()
def ad():
    import analise_database as m
    importlib.reload(m)
    yield m
    m.set_match_weight(1.0)


def test_default_weight_keeps_integer_counts(ad):
    d = {}
    ad._append_to_dict(d, "k", 1)
    ad._append_to_dict(d, "k", 0)
    assert d["k"] == {"wins": 1, "draws": 0, "games": 2}


def test_weight_scales_contribution(ad):
    ad.set_match_weight(0.25)
    d = {}
    ad._append_to_dict(d, "k", 1)
    assert d["k"]["games"] == pytest.approx(0.25)
    assert d["k"]["wins"] == pytest.approx(0.25)


def test_draw_also_scaled(ad):
    ad.set_match_weight(0.5)
    d = {}
    ad._append_to_dict(d, "k", 0.5)
    assert d["k"]["draws"] == pytest.approx(0.5)
    assert d["k"]["wins"] == 0


def test_negative_weight_clamped_to_zero(ad):
    ad.set_match_weight(-3)
    d = {}
    ad._append_to_dict(d, "k", 1)
    assert d["k"]["games"] == 0


def test_winrate_unchanged_by_uniform_weight(ad):
    """Равный вес у всех матчей не должен двигать долю побед — только масштаб."""
    ad.set_match_weight(0.3)
    d = {}
    for v in (1, 1, 0):
        ad._append_to_dict(d, "k", v)
    assert d["k"]["wins"] / d["k"]["games"] == pytest.approx(2 / 3)


def test_halflife_formula_matches_expectation():
    """Полураспад в шагах патчей: возраст 0 -> 1.0, возраст = полураспаду -> 0.5."""
    halflife = 2.0
    assert 0.5 ** (0 / halflife) == pytest.approx(1.0)
    assert 0.5 ** (2 / halflife) == pytest.approx(0.5)
    assert 0.5 ** (4 / halflife) == pytest.approx(0.25)
