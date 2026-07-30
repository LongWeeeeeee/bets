"""Гигиена кэша ProTracker.

Три дефекта, найденные 2026-07-25 на проде:

1. CACHE_DIR был относительным -> каталог зависел от cwd. Прод (systemd,
   WorkingDirectory=/root/main/base) писал в base/..., ручные скрипты из
   /root/main — в другой каталог. Итог: два кэша, в одном 127 героев с lane, в
   другом 98 устаревших + 21 пустышка.
2. Пустой результат попадал в кэш со свежим timestamp, а TTL смотрит только на
   дату -> неудачная загрузка отравляла героя на сутки.
3. Запись через open(...,'w') усекала файл на месте: падение посреди dump
   оставляло битый JSON.
"""
import json
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import dota2protracker as d2pt  # noqa: E402


def test_cache_dir_is_absolute_and_cwd_independent():
    assert os.path.isabs(d2pt.CACHE_DIR)
    # Якорь — каталог модуля, а не текущая директория процесса.
    assert d2pt.CACHE_DIR.startswith(os.path.dirname(os.path.abspath(d2pt.__file__)))


def _cache_file(tmp_path, hero):
    return tmp_path / f"{hero.replace(' ', '_').lower()}.json"


@pytest.fixture
def isolated_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(d2pt, "CACHE_DIR", str(tmp_path))
    return tmp_path


def test_empty_cache_file_counts_as_expired(isolated_cache, monkeypatch):
    """Пустой, но «свежий» файл не должен выдаваться за данные."""
    hero = "Slardar"
    path = _cache_file(isolated_cache, hero)
    path.write_text(json.dumps({
        "hero": hero,
        "matchups": {},
        "synergies": {},
        "timestamp": time.time(),
        "cache_schema_version": d2pt.CACHE_SCHEMA_VERSION,
    }))

    # Фетчера нет -> загрузка не удастся, но важно другое: кэш НЕ вернулся как
    # валидный, то есть рефетч был предпринят.
    monkeypatch.setattr(d2pt, "PROTRACKER_PAYLOAD_FETCHER", None)
    out = d2pt.parse_hero_matchups(hero, use_cache=True)
    assert not out.get("matchups")
    assert out.get("error") == "empty_payload"


def test_nonempty_fresh_cache_is_served(isolated_cache, monkeypatch):
    hero = "Axe"
    payload = {
        "hero": hero,
        "matchups": {"pudge": {"1": {"wr": 55.0, "games": 40, "wins": 22}}},
        "synergies": {},
        "timestamp": time.time(),
        "cache_schema_version": d2pt.CACHE_SCHEMA_VERSION,
    }
    _cache_file(isolated_cache, hero).write_text(json.dumps(payload))

    def _must_not_fetch(*a, **k):
        raise AssertionError("свежий непустой кэш не должен вызывать сеть")

    monkeypatch.setattr(d2pt, "PROTRACKER_PAYLOAD_FETCHER", _must_not_fetch)
    assert d2pt.parse_hero_matchups(hero, use_cache=True)["matchups"] == payload["matchups"]


def test_empty_result_does_not_overwrite_good_cache(isolated_cache, monkeypatch):
    """Неудачная загрузка не имеет права затереть валидные прошлые данные."""
    hero = "Lich"
    good = {
        "hero": hero,
        "matchups": {"pudge": {"1": {"wr": 55.0, "games": 40, "wins": 22}}},
        "synergies": {},
        "timestamp": time.time() - 2 * 86400,  # вчерашний -> истёк
        "cache_schema_version": d2pt.CACHE_SCHEMA_VERSION,
    }
    path = _cache_file(isolated_cache, hero)
    path.write_text(json.dumps(good))

    # Фетчер отвечает пустотой без исключения — самый коварный случай.
    monkeypatch.setattr(d2pt, "PROTRACKER_PAYLOAD_FETCHER",
                        lambda slug, hero_id, proxy=None: {
                            "matchups": {"1": [], "2": [], "3": [], "4": [], "5": []},
                            "synergies": {}, "matchupsLanes": {}, "synergiesLanes": {},
                        })
    out = d2pt.parse_hero_matchups(hero, use_cache=True)
    assert not out.get("matchups")
    assert out.get("error") == "empty_payload"

    # На диске — прежние валидные данные, файл не тронут.
    on_disk = json.loads(path.read_text())
    assert on_disk["matchups"] == good["matchups"]
    assert on_disk["timestamp"] == good["timestamp"]


def test_successful_fetch_writes_atomically(isolated_cache, monkeypatch):
    hero = "Bane"
    rows = [{
        "other_hero_id": 14, "other_hero_name": "Pudge", "other_position": "pos 3",
        "position": "pos 5", "matches": 40, "wins": 22, "win_rate": 55.0,
    }]
    monkeypatch.setattr(d2pt, "PROTRACKER_PAYLOAD_FETCHER",
                        lambda slug, hero_id, proxy=None: {
                            "matchups": {"5": rows, "1": [], "2": [], "3": [], "4": []},
                            "synergies": {}, "matchupsLanes": {}, "synergiesLanes": {},
                        })

    out = d2pt.parse_hero_matchups(hero, use_cache=False)
    assert out["matchups"], "данные должны были разобраться"

    path = _cache_file(isolated_cache, hero)
    assert path.exists()
    # .tmp не остаётся мусором после успешного rename
    assert not (isolated_cache / f"{path.name}.tmp").exists()
    assert json.loads(path.read_text())["matchups"]


def test_lane_absence_is_not_an_error(isolated_cache, monkeypatch):
    """Lane advantage на сайте бывает прочерком — это не повод считать сбой.

    Ключи lane остаются на месте (пустыми), matchups сохраняются, кэш пишется.
    """
    hero = "Tiny"
    rows = [{
        "other_hero_id": 14, "other_hero_name": "Pudge", "other_position": "pos 3",
        "position": "pos 3", "matches": 40, "wins": 22, "win_rate": 55.0,
    }]
    monkeypatch.setattr(d2pt, "PROTRACKER_PAYLOAD_FETCHER",
                        lambda slug, hero_id, proxy=None: {
                            "matchups": {"3": rows, "1": [], "2": [], "4": [], "5": []},
                            "synergies": {},
                            "matchupsLanes": {},   # lane нет вовсе
                            "synergiesLanes": {},
                        })

    out = d2pt.parse_hero_matchups(hero, use_cache=False)
    assert out["matchups"], "отсутствие lane не должно ронять матчапы"
    assert out["_matchups_lane_by_hero_pos"] == {}
    assert _cache_file(isolated_cache, hero).exists()
