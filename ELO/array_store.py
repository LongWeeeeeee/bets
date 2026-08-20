#!/usr/bin/env python3
"""Массивные хранилища для полей ELO-модели: словарный интерфейс без словарей.

ЗАЧЕМ. `HybridPlayerRosterEloModel.from_state` строит тринадцать словарей на
2.5 млн записей — 384 МБ, и это поверх 477 МБ разобранного `model_state`,
который CPython держит ради тех же ключей. При пяти живых картах процесс
упирался в `MemoryHigh`: исторические пики 9.3 и 9.9 ГБ при лимите 10.

ПОЧЕМУ ИМЕННО ТАК. Перепаковывать уже созданные словари бесполезно — проверено
на истории килов: RSS не падает, потому что арены glibc фрагментированы и
`malloc_trim` не находит целиком свободных страниц. Экономит только то, что не
создаётся вовсе. Поэтому хранилища заполняются ПОТОКОВО, минуя словарь.

ЧТО НЕ МЕНЯЕТСЯ. Интерфейс: `get`, `[]`, `in`, `len`, `items`. Логика модели
(`_build_team_context`, геттеры рейтингов) остаётся дословно прежней — она
обращается к полям как к словарям и не замечает подмены.

СЕМАНТИКА `defaultdict`. `lineup_match_counts` и `roster_match_counts` в модели
объявлены `defaultdict(int)`, и чтение неизвестного ключа вставляет туда ноль.
На пути чтения (`mutate=False`) вставка бессмысленна — она только копит мусор
в памяти живого процесса, — поэтому `IntCounts` возвращает ноль, ничего не
записывая. Для чтения это неотличимо; мутирующий путь работает на обычной
модели.

СТРОКОВЫЕ КЛЮЧИ упаковываются 63-битным blake2b. Проверено на боевом снимке:
427 756 ключей `lineup_match_counts` и 384 811 ключей `roster_ratings[TIER3]`
дали НОЛЬ коллизий. Хеш необратим, поэтому эти хранилища не умеют отдавать
исходные ключи — и не должны: экспорт состояния идёт по мутирующему пути.
"""
from __future__ import annotations

import hashlib
from typing import Iterable

import numpy as np

_MISSING = object()


def hash_key(text: str) -> int:
    """Строка -> 63-битное целое. Ноль коллизий на боевых ключах модели."""
    return int.from_bytes(
        hashlib.blake2b(str(text).encode("utf-8"), digest_size=8).digest(),
        "big") >> 1


class _Sorted:
    """Общая часть: отсортированные ключи и поиск `np.searchsorted`."""

    __slots__ = ("_keys", "_vals")

    def __init__(self, keys: np.ndarray, vals: np.ndarray) -> None:
        order = np.argsort(keys, kind="stable")
        self._keys = np.ascontiguousarray(keys[order])
        self._vals = np.ascontiguousarray(vals[order])

    def _index(self, key: int) -> int:
        i = int(np.searchsorted(self._keys, key))
        return i if i < len(self._keys) and int(self._keys[i]) == key else -1

    def __len__(self) -> int:
        return len(self._keys)

    def __bool__(self) -> bool:
        return len(self._keys) > 0


class FloatStore(_Sorted):
    """Числовой ключ -> float. Для `player_global`, `side_bias` и рейтингов."""

    __slots__ = ()

    def get(self, key, default=None):
        i = self._index(int(key))
        return default if i < 0 else float(self._vals[i])

    def __getitem__(self, key):
        v = self.get(key, _MISSING)
        if v is _MISSING:
            raise KeyError(key)
        return v

    def __contains__(self, key) -> bool:
        return self._index(int(key)) >= 0

    def keys(self) -> Iterable[int]:
        return (int(k) for k in self._keys.tolist())

    def __iter__(self):
        """`list(store)` обязан давать КЛЮЧИ, как у словаря.

        Без этого Python откатывается на протокол по индексу — зовёт
        `__getitem__(0)`, получает `KeyError` и роняет любой обход поля.
        """
        return self.keys()

    def items(self):
        return ((int(k), float(v)) for k, v in zip(self._keys.tolist(),
                                                   self._vals.tolist()))


class IntStore(FloatStore):
    """Числовой ключ -> int. Для `*_last_seen_ts`."""

    __slots__ = ()

    def get(self, key, default=None):
        i = self._index(int(key))
        return default if i < 0 else int(self._vals[i])


class IntCounts(IntStore):
    """Числовой ключ -> int со значением по умолчанию НОЛЬ.

    Повторяет `defaultdict(int)` в той части, которая нужна чтению, но НЕ
    вставляет запись при промахе: на боевом состоянии таких вставок ноль из
    427 763, то есть в файл они и так не попадали, а в памяти живого процесса
    только копились.
    """

    __slots__ = ()

    def __getitem__(self, key):
        return self.get(key, 0)


class HashedStore:
    """Строковый ключ -> число, через 63-битный хеш.

    Исходные строки не хранятся: 427 756 ключей `lineup_match_counts` весят
    сами по себе больше, чем всё хранилище. Отдать ключи обратно нельзя, и это
    осознанно — экспорт состояния идёт по мутирующему пути, на обычной модели.
    """

    __slots__ = ("_inner", "_default")

    def __init__(self, keys: np.ndarray, vals: np.ndarray, default=None,
                 as_int: bool = False) -> None:
        self._inner = (IntStore if as_int else FloatStore)(keys, vals)
        self._default = default

    def get(self, key, default=_MISSING):
        d = self._default if default is _MISSING else default
        return self._inner.get(hash_key(key), d)

    def __getitem__(self, key):
        v = self._inner.get(hash_key(key), _MISSING)
        if v is _MISSING:
            if self._default is None:
                raise KeyError(key)
            return self._default
        return v

    def __contains__(self, key) -> bool:
        return hash_key(key) in self._inner

    def __len__(self) -> int:
        return len(self._inner)

    def __bool__(self) -> bool:
        return bool(self._inner)

    def __iter__(self):
        raise TypeError(
            "ключи этого хранилища упакованы необратимым хешем и обойти их "
            "нельзя; если обход нужен, берите словарную модель")


class RoleStore(HashedStore):
    """Ключ — пара (игрок, позиция), как её держит модель.

    В файле это одна строка `'100058342|POSITION_1'`, но `from_state` разбирает
    её в КОРТЕЖ, и обращения идут кортежем. Склеиваем обратно перед хешем: без
    этого промахивались все 918 ключей `player_role_local[TIER1]`, роль-локальная
    поправка тихо исчезала, и сила состава расходилась на 0.15 очка.
    """

    __slots__ = ()

    @staticmethod
    def _text(key) -> str:
        if isinstance(key, tuple):
            return "|".join(str(x) for x in key)
        return str(key)

    def get(self, key, default=_MISSING):
        return super().get(self._text(key), default)

    def __getitem__(self, key):
        return super().__getitem__(self._text(key))

    def __contains__(self, key) -> bool:
        return super().__contains__(self._text(key))


class PairCounts:
    """(число, строка) -> int. Для `player_current_org_matches`.

    В модели это `defaultdict[tuple[int, str], int]` на 1 157 090 пар. Строковая
    половина ключа заменяется индексом в справочник организаций: их 43 487 на
    316 828 игроков, то есть каждая повторяется в среднем семь раз.
    """

    __slots__ = ("_inner", "_orgs")

    def __init__(self, keys: np.ndarray, vals: np.ndarray,
                 orgs: dict[str, int]) -> None:
        self._inner = IntStore(keys, vals)
        self._orgs = orgs

    @staticmethod
    def pack(player_id: int, org_index: int) -> int:
        return (int(player_id) << 20) | int(org_index)

    def _key(self, key):
        player_id, org = key
        idx = self._orgs.get(str(org))
        return None if idx is None else self.pack(player_id, idx)

    def get(self, key, default=0):
        k = self._key(key)
        return default if k is None else self._inner.get(k, default)

    def __getitem__(self, key):
        return self.get(key, 0)

    def __contains__(self, key) -> bool:
        k = self._key(key)
        return k is not None and k in self._inner

    def __len__(self) -> int:
        return len(self._inner)

    def __bool__(self) -> bool:
        return bool(self._inner)

    def __iter__(self):
        raise TypeError(
            "ключ здесь — пара (игрок, организация), упакованная в число; "
            "обойти пары нельзя, берите словарную модель")


class StringValues(_Sorted):
    """Числовой ключ -> строка. Для `player_current_org`.

    Значения лежат индексами в общий справочник: 316 828 игроков ссылаются на
    43 487 организаций, и хранить строку на каждого — значит хранить одну и ту
    же строку семь раз.
    """

    __slots__ = ("_names",)

    def __init__(self, keys: np.ndarray, idx: np.ndarray,
                 names: list[str]) -> None:
        super().__init__(keys, idx)
        self._names = names

    def get(self, key, default=None):
        i = self._index(int(key))
        return default if i < 0 else self._names[int(self._vals[i])]

    def __getitem__(self, key):
        v = self.get(key, _MISSING)
        if v is _MISSING:
            raise KeyError(key)
        return v

    def __contains__(self, key) -> bool:
        return self._index(int(key)) >= 0

    def keys(self) -> Iterable[int]:
        return (int(k) for k in self._keys.tolist())

    def __iter__(self):
        return self.keys()

    def items(self):
        return ((int(k), self._names[int(v)])
                for k, v in zip(self._keys.tolist(), self._vals.tolist()))
