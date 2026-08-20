"""Массивные хранилища полей ELO-модели.

Модель держала тринадцать словарей на 2.5 млн записей — 384 МБ поверх 477 МБ
разобранного `model_state`. При пяти живых картах процесс упирался в лимит:
исторические пики 9.3 и 9.9 ГБ при `MemoryHigh` 10.

Хранилища подменяют поля модели, не трогая её логику, поэтому проверяется
ровно одно: они неотличимы от словарей на всех операциях, которые модель
выполняет, — включая `defaultdict`-семантику счётчиков и упаковку составных
ключей.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ELO.array_store import (  # noqa: E402
    FloatStore, HashedStore, IntCounts, IntStore, PairCounts, StringValues,
    hash_key,
)

KEYS = np.array([500, 100, 900, 42], dtype=np.int64)
VALS = np.array([1512.5, 1488.0, 1500.25, 1601.75], dtype=np.float64)


def test_float_store_matches_a_dict():
    ref = dict(zip(KEYS.tolist(), VALS.tolist()))
    s = FloatStore(KEYS, VALS)
    for k, v in ref.items():
        assert s.get(k) == v
        assert s[k] == v
        assert k in s
    assert len(s) == len(ref)
    assert dict(s.items()) == ref


def test_missing_key_behaves_like_a_dict():
    s = FloatStore(KEYS, VALS)
    assert s.get(123456) is None
    assert s.get(123456, 7.0) == 7.0
    assert 123456 not in s
    with pytest.raises(KeyError):
        s[123456]


def test_int_store_returns_integers():
    s = IntStore(KEYS, np.array([1, 2, 3, 4], dtype=np.int64))
    v = s.get(500)
    assert v == 1 and isinstance(v, int)


def test_counts_default_to_zero_without_inserting():
    """`defaultdict(int)` вставляет ноль при чтении; здесь вставки быть не должно.

    На боевом состоянии таких вставок ноль из 427 763 — в файл они не попадали,
    только копились в памяти живого процесса.
    """
    s = IntCounts(KEYS, np.array([7, 8, 9, 10], dtype=np.int64))
    before = len(s)
    assert s[500] == 7
    assert s[123456] == 0
    assert len(s) == before, "хранилище выросло при чтении неизвестного ключа"


def test_hashed_store_takes_string_keys():
    keys = np.array([hash_key("name:aster::roster:10"),
                     hash_key("name:mtw::lineup:4281729,167")], dtype=np.int64)
    s = HashedStore(keys, np.array([3, 11], dtype=np.int64), default=0, as_int=True)
    assert s["name:aster::roster:10"] == 3
    assert s["name:mtw::lineup:4281729,167"] == 11
    assert s["name:такого::нет"] == 0
    assert "name:aster::roster:10" in s
    assert "name:такого::нет" not in s


def test_hashed_store_without_default_raises():
    keys = np.array([hash_key("a")], dtype=np.int64)
    s = HashedStore(keys, np.array([1.5]), default=None)
    assert s["a"] == 1.5
    with pytest.raises(KeyError):
        s["нет"]


def test_hash_is_stable_and_positive():
    """Ключ обязан быть одинаковым между запусками и влезать в int64 со знаком."""
    a = hash_key("name:aster::roster:10")
    assert a == hash_key("name:aster::roster:10")
    assert 0 <= a < (1 << 63)
    assert hash_key("a") != hash_key("b")


def test_pair_counts_pack_survives_real_ids():
    """Игроки — до полутора миллиардов, организаций 43 487: обе половины влезают."""
    orgs = {"name:aster": 0, "name:tundra": 43486}
    keys = np.array([PairCounts.pack(1_400_000_000, 43486),
                     PairCounts.pack(42, 0)], dtype=np.int64)
    s = PairCounts(keys, np.array([9, 4], dtype=np.int64), orgs)
    assert s[(1_400_000_000, "name:tundra")] == 9
    assert s[(42, "name:aster")] == 4
    assert s[(42, "name:tundra")] == 0          # пара есть, но такой не было
    assert s[(1, "name:неизвестная")] == 0      # организации нет в справочнике
    assert PairCounts.pack(1_400_000_000, 43486) < (1 << 63)


def test_pair_counts_membership():
    orgs = {"name:aster": 0}
    s = PairCounts(np.array([PairCounts.pack(42, 0)], dtype=np.int64),
                   np.array([4], dtype=np.int64), orgs)
    assert (42, "name:aster") in s
    assert (43, "name:aster") not in s
    assert (42, "name:чужая") not in s


def test_string_values_share_one_table():
    """316 828 игроков ссылаются на 43 487 организаций — строка хранится один раз."""
    names = ["name:aster", "name:tundra"]
    s = StringValues(np.array([10, 20, 30], dtype=np.int64),
                     np.array([0, 1, 0], dtype=np.int64), names)
    assert s[10] == "name:aster"
    assert s[20] == "name:tundra"
    assert s[30] == "name:aster"
    assert s[10] is s[30], "одна и та же строка должна быть одним объектом"
    assert s.get(99) is None
    assert dict(s.items()) == {10: "name:aster", 20: "name:tundra", 30: "name:aster"}


def test_stores_are_falsy_when_empty():
    """Модель проверяет поля на истинность — пустое хранилище должно быть ложным."""
    assert not FloatStore(np.zeros(0, np.int64), np.zeros(0))
    assert FloatStore(KEYS, VALS)
    assert not HashedStore(np.zeros(0, np.int64), np.zeros(0), default=0)
    assert not PairCounts(np.zeros(0, np.int64), np.zeros(0, np.int64), {})


def test_values_follow_their_keys_through_sorting():
    s = FloatStore(KEYS, VALS)
    assert s[900] == 1500.25
    assert s[42] == 1601.75


def test_role_store_accepts_tuple_keys():
    """Ключ роль-локального рейтинга модель держит КОРТЕЖЕМ, а файл — строкой.

    На этом промахивались все 918 ключей `player_role_local[TIER1]`:
    роль-локальная поправка тихо исчезала, и сила состава расходилась со
    словарной моделью на 0.15 очка. Ловится только сверкой на реальных данных,
    поэтому проверка закреплена здесь.
    """
    from ELO.array_store import RoleStore

    keys = np.array([hash_key("18180970|POSITION_5"),
                     hash_key("58513047|POSITION_2")], dtype=np.int64)
    s = RoleStore(keys, np.array([1500.0, 1497.2073467742512]), default=None)
    assert s.get((18180970, "POSITION_5")) == 1500.0
    assert s.get((58513047, "POSITION_2")) == 1497.2073467742512
    assert s.get((18180970, "POSITION_1")) is None
    assert (18180970, "POSITION_5") in s
    assert (18180970, "POSITION_1") not in s
    # Строкой тоже должно работать: так ключ выглядит в файле.
    assert s.get("18180970|POSITION_5") == 1500.0


def test_iteration_yields_keys_like_a_dict():
    """`list(store)` обязан давать КЛЮЧИ.

    Без `__iter__` Python откатывается на протокол по индексу: зовёт
    `__getitem__(0)`, получает `KeyError` и роняет любой обход поля. Поймано на
    боевом пути — `list(model.player_global)` падало.
    """
    s = FloatStore(KEYS, VALS)
    assert sorted(s) == sorted(KEYS.tolist())
    assert sorted(list(s)) == sorted(s.keys())
    assert [int(k) for k in IntStore(KEYS, np.array([1, 2, 3, 4]))] == sorted(KEYS.tolist())
    names = ["name:aster", "name:tundra"]
    sv = StringValues(np.array([10, 20], dtype=np.int64),
                      np.array([0, 1], dtype=np.int64), names)
    assert sorted(sv) == [10, 20]


def test_hashed_stores_refuse_iteration_loudly():
    """Ключи упакованы необратимо — молча отдать мусор нельзя."""
    s = HashedStore(np.array([hash_key("a")], dtype=np.int64), np.array([1.0]))
    with pytest.raises(TypeError, match="обойти"):
        list(s)
    p = PairCounts(np.array([PairCounts.pack(1, 0)], dtype=np.int64),
                   np.array([1], dtype=np.int64), {"name:x": 0})
    with pytest.raises(TypeError, match="обойти"):
        list(p)
