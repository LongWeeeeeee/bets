"""Параметры популяции early вынесены в env; дефолты обязаны совпадать с прежними.

Зачем ручки. `EARLY_GATE_MAX_ABS_LEAD` — прямой аналог условия равенства у late:
он требует, чтобы к 10-й минуте игра ещё не разъехалась. Снятие аналогичного
условия у late дало +4.5…+11 п.п. (E-53), а у early этот порог ни разу не
перебирался, потому что был зашит числом.

Главный тест здесь — первый: правка не должна ничего изменить, пока переменные
не заданы. Именно так ломаются словари: дефолт уехал, а никто не заметил, потому
что сборка идёт раз в несколько дней.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _reload(monkeypatch, **env):
    for k, v in env.items():
        monkeypatch.setenv(k, str(v))
    import analise_database as ad
    return importlib.reload(ad)


def test_defaults_match_previous_hardcoded_values(monkeypatch):
    ad = _reload(monkeypatch)
    assert ad.EARLY_GATE_INDEX == 9
    # 2000 -> 500 по итогам E-57/E-58: ужесточение не стоит покрытия (100% карт
    # при любом гейте), а точность растёт до плато 250-1000.
    assert ad.EARLY_GATE_MAX_ABS_LEAD == 500
    assert ad.EARLY_LEAD_WINDOW == (20, 28)
    # 34 -> 40 и новая ручка минимальной длины по итогам E-59.
    assert ad.EARLY_FAST_FINISH_MAX_MINUTES == 40
    assert ad.EARLY_MIN_DURATION == 24


def test_short_match_is_not_early(monkeypatch):
    """Карта короче минимума не идёт в early, даже если победитель известен."""
    ad = _reload(monkeypatch)
    match = {"radiantNetworthLeads": [100] * 20, "didRadiantWin": True, "players": []}
    assert ad.is_early_match(match) == (False, None)
    match["radiantNetworthLeads"] = [100] * 30
    ok, dominator = ad.is_early_match(match)
    assert ok is True and dominator == "radiant"


def test_min_duration_knob_applies(monkeypatch):
    ad = _reload(monkeypatch, ANALISE_EARLY_MIN_DURATION=18)
    assert ad.EARLY_MIN_DURATION == 18
    match = {"radiantNetworthLeads": [100] * 20, "didRadiantWin": True, "players": []}
    assert ad.is_early_match(match)[0] is True


def test_gate_minute_is_one_based_in_env(monkeypatch):
    """В env минута человеческая (10 = десятая), внутри — индекс списка."""
    ad = _reload(monkeypatch, ANALISE_EARLY_GATE_MINUTE=12)
    assert ad.EARLY_GATE_INDEX == 11


def test_gate_lead_knob_applies(monkeypatch):
    ad = _reload(monkeypatch, ANALISE_EARLY_GATE_MAX_ABS_LEAD=6000)
    assert ad.EARLY_GATE_MAX_ABS_LEAD == 6000


def test_lead_window_knobs_apply(monkeypatch):
    ad = _reload(monkeypatch, ANALISE_EARLY_LEAD_WINDOW_FROM=18,
                 ANALISE_EARLY_LEAD_WINDOW_TO=30)
    assert ad.EARLY_LEAD_WINDOW == (18, 30)


def test_wider_gate_admits_more_matches(monkeypatch):
    """Смысловая проверка: ослабление гейта расширяет популяцию, а не сужает.

    `is_early_match` возвращает КОРТЕЖ (подходит, доминатор) — сравнение с голым
    False молча проходит мимо, потому что непустой кортеж всегда истинный.
    """
    # К 10-й минуте разрыв 4500 (гейт), в окне 20-28 крупный лид — иначе
    # доминатор не определится и матч отсеется уже ПОСЛЕ гейта, а тест будет
    # проверять не то, что заявлено.
    # Длина 45: гейт применяется только к картам длиннее EARLY_FAST_FINISH_MAX_MINUTES
    # (40 после E-59), иначе карта уходит быстрой веткой и гейт не проверяется.
    leads = [0] * 9 + [4500] + [1000] * 9 + [14000] * 26
    match = {"radiantNetworthLeads": leads, "didRadiantWin": True}
    strict = _reload(monkeypatch, ANALISE_EARLY_GATE_MAX_ABS_LEAD=2000)
    assert strict.is_early_match(match)[0] is False
    loose = _reload(monkeypatch, ANALISE_EARLY_GATE_MAX_ABS_LEAD=6000)
    assert loose.is_early_match(match) == (True, 'radiant')


@pytest.fixture(autouse=True)
def _restore():
    yield
    # Финалайзер отрабатывает РАНЬШЕ, чем monkeypatch снимает переменные, поэтому
    # простой reload перечитывал бы env самого теста и оставлял чужие значения
    # в общем модуле — соседний файл (`test_analise_database_post_lane`) ловил
    # чужой гейт и падал. Чистим окружение сами.
    import os
    import analise_database
    for key in [k for k in os.environ if k.startswith("ANALISE_EARLY_")]:
        os.environ.pop(key, None)
    importlib.reload(analise_database)
