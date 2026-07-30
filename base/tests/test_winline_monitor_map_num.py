"""Номер карты серии для запроса кэфов Winline.

Регрессия на живой матч Nemiga Gaming vs Level UP esports (25.07.2026):
SourceTV отдавал `series_game_number = 1`, хотя `dire_series_wins = 1` —
первая карта уже была доиграна и шла вторая. Монитор просил рынок карты 1,
которого больше не существует, и вечно получал `market missing`.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

MONITOR_PATH = Path("/root/main/runtime/winline_parser_monitor.py")


def _load_monitor():
    if not MONITOR_PATH.exists():
        pytest.skip("winline_parser_monitor.py доступен только на сервере")
    spec = importlib.util.spec_from_file_location("winline_parser_monitor", MONITOR_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _match(**overrides):
    payload = {
        "match_id": 8912123729,
        "radiant_team_name": "Nemiga Gaming",
        "dire_team_name": "Level UP esports",
        "series_id": None,
        "series_game_number": 1,
        "radiant_series_wins": 0,
        "dire_series_wins": 0,
    }
    payload.update(overrides)
    return payload


def _map_num(monitor, tmp_path, match):
    path = tmp_path / "sourcetv_matches.json"
    path.write_text(json.dumps({"8912123729": match}), encoding="utf-8")
    monitor.MATCHES_PATH = path
    loaded = monitor._load_current_matches()
    assert loaded, "матч должен пройти фильтр"
    return loaded[0]["map_num"]


def test_stale_game_number_is_corrected_by_series_wins(tmp_path):
    """series_game_number отстал на карту — берём число сыгранных карт."""
    monitor = _load_monitor()
    match = _match(series_game_number=1, radiant_series_wins=0, dire_series_wins=1)
    assert _map_num(monitor, tmp_path, match) == 2


def test_first_map_stays_first(tmp_path):
    """Ничего не сыграно — карта остаётся первой, номер не раздувается."""
    monitor = _load_monitor()
    match = _match(series_game_number=1, radiant_series_wins=0, dire_series_wins=0)
    assert _map_num(monitor, tmp_path, match) == 1


def test_third_map_of_bo3(tmp_path):
    """1:1 по картам — идёт третья."""
    monitor = _load_monitor()
    match = _match(series_game_number=2, radiant_series_wins=1, dire_series_wins=1)
    assert _map_num(monitor, tmp_path, match) == 3


def test_fresh_game_number_is_not_downgraded(tmp_path):
    """Если game_number свежее счётчика побед — не занижаем его."""
    monitor = _load_monitor()
    match = _match(series_game_number=3, radiant_series_wins=1, dire_series_wins=0)
    assert _map_num(monitor, tmp_path, match) == 3


def test_missing_series_wins_falls_back_to_game_number(tmp_path):
    """Полей о победах нет — работаем по series_game_number как раньше."""
    monitor = _load_monitor()
    match = _match(series_game_number=2)
    match.pop("radiant_series_wins")
    match.pop("dire_series_wins")
    assert _map_num(monitor, tmp_path, match) == 2
