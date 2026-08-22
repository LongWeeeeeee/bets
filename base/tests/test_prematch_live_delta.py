"""Проверки дельты по игрокам.

Стерегутся две вещи. Первая — ГРАНИЦА СНИМКА: карта, уже вошедшая в артефакт, не
имеет права попасть в дельту второй раз, иначе счётчики удвоятся. Вторая —
ИДЕМПОТЕНТНОСТЬ: одну и ту же карту прод обрабатывает многократно, и повторная
запись не должна ничего менять.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from prematch_live_delta import (counters, player_maps, record_map,  # noqa: E402
                                 set_snapshot_ts)

T0 = 1_700_000_000


def _match(mid, end, accs, heroes, rad_won=True, positions=None):
    pos = positions or ["POSITION_1", "POSITION_2", "POSITION_3", "POSITION_4", "POSITION_5"] * 2
    return {"id": mid, "endDateTime": end, "startDateTime": end - 2100,
            "durationSeconds": 2100, "leagueId": 19719, "didRadiantWin": rad_won,
            "players": [{"steamAccountId": a, "heroId": h, "isRadiant": i < 5,
                         "isVictory": (i < 5) == rad_won, "position": pos[i],
                         "kills": 5, "deaths": 2, "assists": 7, "numLastHits": 200,
                         "numDenies": 10, "goldPerMinute": 500, "networth": 20000,
                         "experiencePerMinute": 600, "level": 22,
                         "heroDamage": 15000, "imp": 3}
                        for i, (a, h) in enumerate(zip(accs, heroes))]}


def _store(tmp_path: Path) -> Path:
    return tmp_path / "delta.json"


ACCS = list(range(101, 111))
HEROES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


def test_record_and_count(tmp_path: Path) -> None:
    st = _store(tmp_path)
    assert record_map(_match(1, T0, ACCS, HEROES), store_path=st, now=T0 + 60) == 10
    c = counters(101, store_path=st)
    assert c["games"] == 1 and c["wins"] == 1
    assert c["hero_games"] == {1: 1} and c["pos_games"] == {1: 1}
    assert counters(106, store_path=st)["wins"] == 0, "дайр проиграл"


def test_same_map_recorded_twice_is_idempotent(tmp_path: Path) -> None:
    """Прод обрабатывает карту многократно — счётчики удваиваться не должны."""
    st = _store(tmp_path)
    for _ in range(3):
        record_map(_match(1, T0, ACCS, HEROES), store_path=st, now=T0 + 60)
    assert counters(101, store_path=st)["games"] == 1


def test_map_inside_snapshot_is_dropped(tmp_path: Path) -> None:
    """Карта, уже вошедшая в артефакт, в дельте не нужна — иначе двойной счёт."""
    st = _store(tmp_path)
    record_map(_match(1, T0, ACCS, HEROES), store_path=st, now=T0 + 60)
    record_map(_match(2, T0 + 5000, ACCS, HEROES), store_path=st, now=T0 + 5060)
    dropped = set_snapshot_ts(T0 + 100, store_path=st, now=T0 + 6000)
    assert dropped == 1
    assert counters(101, store_path=st)["games"] == 1


def test_two_maps_accumulate_hero_and_position(tmp_path: Path) -> None:
    st = _store(tmp_path)
    record_map(_match(1, T0, ACCS, HEROES), store_path=st, now=T0 + 60)
    record_map(_match(2, T0 + 5000, ACCS, [77] + HEROES[1:], rad_won=False),
               store_path=st, now=T0 + 5060)
    c = counters(101, store_path=st)
    assert c["games"] == 2 and c["wins"] == 1
    assert c["hero_games"] == {1: 1, 77: 1}
    assert c["pos_games"] == {1: 2}
    assert c["heroes"] == [1, 77]


def test_player_maps_are_ordered(tmp_path: Path) -> None:
    st = _store(tmp_path)
    record_map(_match(2, T0 + 5000, ACCS, HEROES), store_path=st, now=T0 + 5060)
    record_map(_match(1, T0, ACCS, HEROES), store_path=st, now=T0 + 5060)
    got = player_maps(101, store_path=st)
    assert [r["match_id"] for r in got] == [1, 2]


def test_garbage_input_is_ignored(tmp_path: Path) -> None:
    st = _store(tmp_path)
    assert record_map({}, store_path=st) == 0
    assert record_map({"id": 0, "players": []}, store_path=st) == 0
    assert record_map(None, store_path=st) == 0
    assert counters(101, store_path=st)["games"] == 0


def test_corrupted_store_is_survived(tmp_path: Path) -> None:
    st = _store(tmp_path)
    st.write_text("не json", encoding="utf-8")
    assert record_map(_match(1, T0, ACCS, HEROES), store_path=st, now=T0 + 60) == 10
    assert counters(101, store_path=st)["games"] == 1


def test_sync_snapshot_drops_what_artifact_already_knows(tmp_path: Path) -> None:
    """Приехал новый артефакт — карты внутри его среза обязаны уйти из дельты."""
    import numpy as np
    from prematch_live_delta import sync_snapshot
    st = _store(tmp_path)
    art = tmp_path / "art.npz"
    np.savez(art, snapshot_ts=np.int64(T0 + 100))
    record_map(_match(1, T0, ACCS, HEROES), store_path=st, now=T0 + 60)
    record_map(_match(2, T0 + 5000, ACCS, HEROES), store_path=st, now=T0 + 5060)
    assert sync_snapshot(artifact_path=art, store_path=st, now=T0 + 6000) == 1
    assert counters(101, store_path=st)["games"] == 1
    # повторный вызов ничего не двигает
    assert sync_snapshot(artifact_path=art, store_path=st, now=T0 + 6000) == 0
