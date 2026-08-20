"""Кэши восстановленной ELO-модели и таблицы рангов.

Оба кэша были на ОДИН слот с ключом `id(snapshot)`, и оба промахивались всегда.
`get_matchup_summary` намеренно строит сводку дважды — от базового снимка и от
мерженого с рантайм-состоянием, чтобы показать сдвиг рейтинга за турнир, — и
единственный слот на каждом шаге вытеснялся соседним. Замер на боевых файлах:
три вызова дали шесть восстановлений, каждое по 7.5-8 секунд, около гигабайта
объектов на запрос. Отсюда и постоянно занятое ядро, и рост RSS с 4 до 6.2 ГБ.

Проверяется не «стало быстрее», а три свойства, поломка которых незаметна:
кэш попадает, отдаёт СВОЮ модель каждому состоянию и не путает объекты после
освобождения.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ELO import live_team_strength as LTS  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_caches(monkeypatch):
    """Подменяет саму модель: восстановление настоящей стоит секунды и гигабайт.

    Класс подменяется целиком, а не только `from_state`, потому что при выдаче из
    кэша стоит `isinstance` — с посторонним типом он отсёк бы попадание.
    """
    calls = {"n": 0}

    class _FakeModel:
        def __init__(self, state):
            self.state = state

        @classmethod
        def from_state(cls, state):
            calls["n"] += 1
            return cls(state)

    monkeypatch.setattr(LTS, "_MODEL_FROM_SNAPSHOT_CACHE", [])
    monkeypatch.setattr(LTS, "_LEADERBOARD_RANK_CACHE",
                        {"table_id": None, "table_ref": None, "rank_map": None})
    monkeypatch.setattr(LTS, "HybridPlayerRosterEloModel", _FakeModel)
    return calls


def test_second_call_on_the_same_state_hits_the_cache(_clean_caches):
    snap = {"model_state": {"a": 1}}
    first = LTS._restore_model_from_snapshot(snap)
    second = LTS._restore_model_from_snapshot(snap)
    assert first is second
    assert _clean_caches["n"] == 1


def test_a_rewrapped_snapshot_still_hits(_clean_caches):
    """Мерженый снимок — новый dict поверх того же состояния.

    Ключ по `id(snapshot)` здесь промахивался бы, хотя строить нечего.
    """
    state = {"a": 1}
    a = LTS._restore_model_from_snapshot({"model_state": state})
    b = LTS._restore_model_from_snapshot({"model_state": state, "meta": {"x": 1}})
    assert a is b
    assert _clean_caches["n"] == 1


def test_two_states_alternating_do_not_evict_each_other(_clean_caches):
    """Ровно тот случай, что был в бою: базовый и мерженый по очереди."""
    base = {"model_state": {"kind": "base"}}
    live = {"model_state": {"kind": "live"}}
    m_base = LTS._restore_model_from_snapshot(base)
    m_live = LTS._restore_model_from_snapshot(live)
    assert _clean_caches["n"] == 2
    for _ in range(5):
        assert LTS._restore_model_from_snapshot(base) is m_base
        assert LTS._restore_model_from_snapshot(live) is m_live
    assert _clean_caches["n"] == 2, "кэш всё ещё пересобирает модели"


def test_each_state_gets_its_own_model(_clean_caches):
    base = {"model_state": {"kind": "base"}}
    live = {"model_state": {"kind": "live"}}
    assert LTS._restore_model_from_snapshot(base).state["kind"] == "base"
    assert LTS._restore_model_from_snapshot(live).state["kind"] == "live"


def test_a_third_state_evicts_the_least_recently_used(_clean_caches):
    s1 = {"model_state": {"n": 1}}
    s2 = {"model_state": {"n": 2}}
    s3 = {"model_state": {"n": 3}}
    LTS._restore_model_from_snapshot(s1)
    m2 = LTS._restore_model_from_snapshot(s2)
    LTS._restore_model_from_snapshot(s3)              # вытесняет s1
    assert _clean_caches["n"] == 3
    assert LTS._restore_model_from_snapshot(s2) is m2  # s2 ещё жив
    assert _clean_caches["n"] == 3
    LTS._restore_model_from_snapshot(s1)               # s1 пересобирается
    assert _clean_caches["n"] == 4


def test_cache_holds_the_state_so_its_id_cannot_be_reused(_clean_caches):
    """Без ссылки на состояние освободившийся `id` мог бы достаться другому
    словарю, и кэш отдал бы чужую модель."""
    LTS._restore_model_from_snapshot({"model_state": {"n": 1}})
    held = [ref for _id, ref, _m in LTS._MODEL_FROM_SNAPSHOT_CACHE]
    assert held and held[0] == {"n": 1}


def test_missing_state_returns_none_and_is_not_cached(_clean_caches):
    assert LTS._restore_model_from_snapshot({}) is None
    assert LTS._restore_model_from_snapshot({"model_state": "не словарь"}) is None
    assert LTS._MODEL_FROM_SNAPSHOT_CACHE == []


def _snapshot_with_table(table):
    return {"teams_by_org_key": table}


def test_rank_map_is_computed_once_per_table():
    table = {"a": {"current_strength": 10.0, "team_name": "A"},
             "b": {"current_strength": 20.0, "team_name": "B"}}
    first = LTS._leaderboard_rank_map(_snapshot_with_table(table))
    # Тот же объект таблицы в ДРУГОЙ обёртке — как у мерженого снимка.
    second = LTS._leaderboard_rank_map(_snapshot_with_table(table))
    assert first is second, "карта рангов пересчитана, хотя таблица та же"
    assert first == {"b": 1, "a": 2}


def test_rank_map_recomputes_for_a_different_table():
    t1 = {"a": {"current_strength": 10.0, "team_name": "A"}}
    t2 = {"z": {"current_strength": 5.0, "team_name": "Z"}}
    assert LTS._leaderboard_rank_map(_snapshot_with_table(t1)) == {"a": 1}
    assert LTS._leaderboard_rank_map(_snapshot_with_table(t2)) == {"z": 1}


def test_rank_map_without_a_table_is_empty():
    assert LTS._leaderboard_rank_map({}) == {}
    assert LTS._leaderboard_rank_map({"teams_by_org_key": "не словарь"}) == {}


def test_runtime_state_is_not_deep_copied(tmp_path, monkeypatch):
    """Копия защищала от несуществующего совладельца и стоила 435 МБ пика.

    `_load_json_dict` разбирает файл заново на каждый вызов, поэтому payload —
    локальный объект. Проверяем, что состояние попадает в снимок КАК ЕСТЬ.
    """
    state = {"players": {"1": 2}}
    payload = {"model_state": state, "updated_at": 123}
    monkeypatch.setattr(LTS, "_load_runtime_model_payload",
                        lambda **_kw: payload)
    monkeypatch.setattr(LTS, "_runtime_file_signature", lambda _p: (True, 42))
    monkeypatch.setattr(LTS, "_RUNTIME_SNAPSHOT_CACHE",
                        {"base_snapshot_id": None, "runtime_signature": None,
                         "snapshot": None})
    merged = LTS._snapshot_with_runtime_model_state(
        {"meta": {}, "model_state": {"старое": True}},
        runtime_model_state_path=tmp_path / "runtime.json")
    assert merged["model_state"] is state, "состояние скопировано, а не взято как есть"
    assert merged["meta"]["runtime_updated_at"] == 123
