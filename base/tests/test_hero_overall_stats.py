"""Сводка по герою из /api/heroes/list: разбор, кэш, отказы.

Живьём проверено на Drow Ranger: 10513 матчей / 52.06% — ровно то, что показано
в шапке страницы героя. Здесь закрепляем контракт без сети.
"""
import json
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import dota2protracker as d2pt  # noqa: E402


ROW_DROW = {
    "displayName": "Drow Ranger",
    "npc": "drow_ranger",
    "hero_id": 6,
    "all elo": 3159,
    "all matches": 10513,
    "all winrate": 0.5206,
    "pos 1 elo": 3159,
    "pos 1 matches": 10000,
    "pos 1 winrate": 0.5226,
    "pos 4 elo": 2989,
    "pos 4 matches": 0,
    "pos 4 winrate": 0.4,
}


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(d2pt, "CACHE_DIR", str(tmp_path))
    monkeypatch.setattr(d2pt, "PROTRACKER_HERO_LIST_FETCHER", None)
    return tmp_path


def test_parse_converts_share_to_percent():
    out = d2pt._parse_hero_list_rows([ROW_DROW])
    entry = out["drow_ranger"]
    assert entry["matches"] == 10513
    assert entry["wr"] == 52.06  # 0.5206 -> проценты, как везде в модуле
    assert entry["elo"] == 3159
    assert entry["hero_id"] == 6


def test_parse_drops_positions_without_matches():
    entry = d2pt._parse_hero_list_rows([ROW_DROW])["drow_ranger"]
    assert "1" in entry["by_pos"]
    # pos 4 сыгран 0 раз — винрейт там бессмыслен, позицию не отдаём
    assert "4" not in entry["by_pos"]
    assert entry["by_pos"]["1"] == {"matches": 10000, "wr": 52.26, "elo": 3159}


def test_parse_survives_garbage_rows():
    out = d2pt._parse_hero_list_rows([ROW_DROW, None, "junk", {}, {"displayName": ""}])
    assert list(out) == ["drow_ranger"]


def test_parse_tolerates_non_numeric_fields():
    row = dict(ROW_DROW, **{"all matches": "n/a", "all winrate": None})
    entry = d2pt._parse_hero_list_rows([row])["drow_ranger"]
    assert entry["matches"] is None and entry["wr"] is None


def test_fetch_without_fetcher_returns_empty(cache_dir):
    # Своего браузера модуль не поднимает — fail closed, как в parse_hero_matchups.
    assert d2pt.fetch_hero_overall_stats(use_cache=False) == {}
    assert not os.path.exists(d2pt._hero_list_cache_file())


def test_fetch_writes_cache_and_reuses_it(cache_dir, monkeypatch):
    calls = []

    def _fetcher(url, proxy=None):
        calls.append(url)
        return [ROW_DROW]

    monkeypatch.setattr(d2pt, "PROTRACKER_HERO_LIST_FETCHER", _fetcher)

    first = d2pt.fetch_hero_overall_stats(use_cache=False)
    assert first["drow_ranger"]["matches"] == 10513
    assert os.path.exists(d2pt._hero_list_cache_file())

    second = d2pt.fetch_hero_overall_stats(use_cache=True)
    assert second == first
    assert len(calls) == 1, "второй вызов должен читать кэш, а не сеть"


def test_fetch_accepts_dict_shaped_payload(cache_dir, monkeypatch):
    monkeypatch.setattr(d2pt, "PROTRACKER_HERO_LIST_FETCHER",
                        lambda url, proxy=None: {"heroes": [ROW_DROW]})
    assert "drow_ranger" in d2pt.fetch_hero_overall_stats(use_cache=False)


def test_empty_response_does_not_poison_cache(cache_dir, monkeypatch):
    monkeypatch.setattr(d2pt, "PROTRACKER_HERO_LIST_FETCHER",
                        lambda url, proxy=None: [ROW_DROW])
    d2pt.fetch_hero_overall_stats(use_cache=False)

    # Следующая загрузка пустая: кэш должен остаться прежним, а не обнулиться.
    monkeypatch.setattr(d2pt, "PROTRACKER_HERO_LIST_FETCHER",
                        lambda url, proxy=None: [])
    assert d2pt.fetch_hero_overall_stats(use_cache=False) == {}
    with open(d2pt._hero_list_cache_file()) as f:
        assert "drow_ranger" in (json.load(f).get("heroes") or {})


def test_fetcher_exception_is_contained(cache_dir, monkeypatch):
    def _boom(url, proxy=None):
        raise RuntimeError("proxy died")

    monkeypatch.setattr(d2pt, "PROTRACKER_HERO_LIST_FETCHER", _boom)
    assert d2pt.fetch_hero_overall_stats(use_cache=False) == {}


def test_stale_cache_is_refetched(cache_dir, monkeypatch):
    stale = {"timestamp": time.time() - 3 * 86400,
             "heroes": {"drow_ranger": {"matches": 1, "wr": 1.0}}}
    with open(d2pt._hero_list_cache_file(), "w") as f:
        json.dump(stale, f)

    monkeypatch.setattr(d2pt, "PROTRACKER_HERO_LIST_FETCHER",
                        lambda url, proxy=None: [ROW_DROW])
    fresh = d2pt.fetch_hero_overall_stats(use_cache=True)
    assert fresh["drow_ranger"]["matches"] == 10513


def test_empty_cache_file_is_treated_as_missing(cache_dir, monkeypatch):
    with open(d2pt._hero_list_cache_file(), "w") as f:
        json.dump({"timestamp": time.time(), "heroes": {}}, f)

    monkeypatch.setattr(d2pt, "PROTRACKER_HERO_LIST_FETCHER",
                        lambda url, proxy=None: [ROW_DROW])
    assert d2pt.fetch_hero_overall_stats(use_cache=True)["drow_ranger"]["matches"] == 10513


def test_getter_by_position_and_unknown_hero(cache_dir, monkeypatch):
    monkeypatch.setattr(d2pt, "PROTRACKER_HERO_LIST_FETCHER",
                        lambda url, proxy=None: [ROW_DROW])
    d2pt.fetch_hero_overall_stats(use_cache=False)

    assert d2pt.get_hero_overall_stats("Drow Ranger")["wr"] == 52.06
    assert d2pt.get_hero_overall_stats("drow_ranger")["wr"] == 52.06
    assert d2pt.get_hero_overall_stats("Drow Ranger", position=1)["matches"] == 10000
    assert d2pt.get_hero_overall_stats("Drow Ranger", position="pos 1")["matches"] == 10000

    # Отсутствие данных отдаём пустым dict, а не 50%: иначе «нет данных»
    # превратилось бы в ложный нейтральный сигнал.
    assert d2pt.get_hero_overall_stats("No Such Hero") == {}
    assert d2pt.get_hero_overall_stats("Drow Ranger", position=4) == {}
