"""Снежный ком по командам и разведённые даты сбора.

Две вещи, каждая из которых способна тихо сжечь квоту Stratz или собрать не то:
  * дата: раньше про-ветка перезаписывала общую константу, и правка паблик-даты
    на про не влияла вовсе. Теперь у каждой ветки своя, со старым именем как
    фолбэком — проверяем и приоритет, и фолбэк;
  * посещённые команды: набор обязан переживать перезапуск, иначе каждая волна
    заново опрашивает те же команды.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import maps_research as MR  # noqa: E402


def test_dates_are_separate_for_pro_and_public(monkeypatch):
    import keys

    monkeypatch.setattr(keys, "start_date_time_pro", 111, raising=False)
    monkeypatch.setattr(keys, "start_date_time_publick", 222, raising=False)
    assert MR._start_date_for(pro=True) == 111
    assert MR._start_date_for(pro=False) == 222


def test_dates_fall_back_to_old_names(monkeypatch):
    """Старый keys.py на другой машине не должен ломать сбор."""
    import keys

    monkeypatch.delattr(keys, "start_date_time_pro", raising=False)
    monkeypatch.delattr(keys, "start_date_time_publick", raising=False)
    monkeypatch.setattr(keys, "start_date_time_736", 333, raising=False)
    monkeypatch.setattr(keys, "start_date_time", 444, raising=False)
    assert MR._start_date_for(pro=True) == 333
    assert MR._start_date_for(pro=False) == 444


def test_visited_teams_survive_restart(tmp_path):
    path = tmp_path / "visited_teams.json"
    assert MR._load_visited_teams(str(path)) == set()

    MR._save_visited_teams(str(path), {5, 3, 9})
    assert MR._load_visited_teams(str(path)) == {3, 5, 9}
    # запись атомарная: временного файла после неё не остаётся
    assert not (tmp_path / "visited_teams.json.tmp").exists()

    MR._save_visited_teams(str(path), {3, 5, 9, 12})
    assert MR._load_visited_teams(str(path)) == {3, 5, 9, 12}


def test_visited_teams_tolerate_broken_file(tmp_path):
    path = tmp_path / "visited_teams.json"
    path.write_text("не json", encoding="utf-8")
    # битый сайдкар не должен ронять сбор — начинаем с пустого набора
    assert MR._load_visited_teams(str(path)) == set()


def test_opponents_are_discovered_from_corpus(tmp_path):
    """Соперники из собранных матчей становятся очередью следующей волны."""
    (tmp_path / "7.41_part001.json").write_text(json.dumps({
        "1": {"radiantTeam": {"id": 100}, "direTeam": {"id": 200}},
        "2": {"radiantTeam": {"id": 100}, "direTeam": {"id": 300}},
        "3": {"radiantTeam": {}, "direTeam": {"id": 0}},          # мусор игнорируется
        "4": "не объект",
    }), encoding="utf-8")
    (tmp_path / "merge_patch_summary.json").write_text("{}", encoding="utf-8")

    assert MR._teams_from_corpus(str(tmp_path)) == {100, 200, 300}
    assert MR._teams_from_corpus(str(tmp_path / "нет-такой")) == set()
