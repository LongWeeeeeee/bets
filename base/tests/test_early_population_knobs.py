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
    assert ad.EARLY_GATE_MAX_ABS_LEAD == 2000
    assert ad.EARLY_LEAD_WINDOW == (20, 28)
    assert ad.EARLY_FAST_FINISH_MAX_MINUTES == 34


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
    leads = [0] * 9 + [4500] + [1000] * 9 + [14000] * 21
    match = {"radiantNetworthLeads": leads, "didRadiantWin": True}
    strict = _reload(monkeypatch, ANALISE_EARLY_GATE_MAX_ABS_LEAD=2000)
    assert strict.is_early_match(match)[0] is False
    loose = _reload(monkeypatch, ANALISE_EARLY_GATE_MAX_ABS_LEAD=6000)
    assert loose.is_early_match(match) == (True, 'radiant')


@pytest.fixture(autouse=True)
def _restore():
    yield
    import analise_database
    importlib.reload(analise_database)
