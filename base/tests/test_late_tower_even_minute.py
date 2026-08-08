"""Парное условие перекоса: ровно на ранней минуте, разошлось к поздней.

Дефект, ради которого условие появилось (замер 08.08): одиночное условие на
поздней минуте вырождается. `_tower_structure_score(match, M)` считает падения
башен со временем меньше M и НЕ требует, чтобы матч дожил до M, поэтому у
законченной игры перекос почти всегда есть — правило пропускало 88% корпуса
вместо 26%. Отбор «решившихся поздно» требует ДВУХ минут.

Дефолт `ANALISE_LATE_TOWER_EVEN_MINUTE=0` выключает пару и сохраняет прежнее
поведение — это проверяется отдельно, иначе правка молча поменяла бы сбор.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

T3_RADIANT = 22  # id берём из модуля, см. фикстуру


def _match(t3_times_radiant=(), t3_times_dire=(), duration=45):
    """Матч с падениями T3 в заданные минуты (минуты -> секунды)."""
    import analise_database as ad
    rad_ids = sorted(ad._T3_IDS['radiant'])
    dire_ids = sorted(ad._T3_IDS['dire'])
    deaths = []
    for i, minute in enumerate(t3_times_radiant):
        deaths.append({"npcId": rad_ids[i % len(rad_ids)], "isRadiant": True,
                       "time": int(minute * 60)})
    for i, minute in enumerate(t3_times_dire):
        deaths.append({"npcId": dire_ids[i % len(dire_ids)], "isRadiant": False,
                       "time": int(minute * 60)})
    return {
        "towerDeaths": deaths,
        "didRadiantWin": True,
        "radiantNetworthLeads": [0] * duration,
    }


def _reload(monkeypatch, **env):
    for k, v in env.items():
        monkeypatch.setenv(k, str(v))
    import analise_database as ad
    return importlib.reload(ad)


def test_default_keeps_old_behaviour(monkeypatch):
    """Без EVEN_MINUTE правило прежнее: перекос к 32-й — и достаточно."""
    ad = _reload(monkeypatch, ANALISE_LATE_RULE="tower_gap")
    # radiant потерял две T3 к 20-й минуте: разрыв 2 уже на ранней стадии.
    assert ad.is_late_match(_match(t3_times_radiant=(15, 18))) is True


def test_even_minute_rejects_early_decided(monkeypatch):
    """Игра, решённая рано, парным условием отбраковывается."""
    ad = _reload(monkeypatch, ANALISE_LATE_RULE="tower_gap",
                 ANALISE_LATE_TOWER_MINUTE=44, ANALISE_LATE_TOWER_EVEN_MINUTE=32)
    assert ad.is_late_match(_match(t3_times_radiant=(15, 18))) is False


def test_even_minute_accepts_late_divergence(monkeypatch):
    """Ровно к 32-й, разошлось к 44-й — это и есть нужная популяция."""
    ad = _reload(monkeypatch, ANALISE_LATE_RULE="tower_gap",
                 ANALISE_LATE_TOWER_MINUTE=44, ANALISE_LATE_TOWER_EVEN_MINUTE=32)
    assert ad.is_late_match(_match(t3_times_radiant=(36, 39))) is True


def test_even_minute_allows_configured_slack(monkeypatch):
    """EVEN_MAX_GAP=1 разрешает потерю одной T3 к ранней минуте."""
    ad = _reload(monkeypatch, ANALISE_LATE_RULE="tower_gap",
                 ANALISE_LATE_TOWER_MINUTE=44, ANALISE_LATE_TOWER_EVEN_MINUTE=32,
                 ANALISE_LATE_TOWER_EVEN_MAX_GAP=1)
    assert ad.is_late_match(_match(t3_times_radiant=(20, 38))) is True


@pytest.fixture(autouse=True)
def _restore():
    yield
    import analise_database
    importlib.reload(analise_database)
