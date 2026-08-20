#!/usr/bin/env python3
"""Модель ELO для ЧТЕНИЯ: те же числа, в двадцать раз меньше памяти.

ЗАЧЕМ. `from_state` строит тринадцать словарей на 2.5 млн записей — 384 МБ, и
это поверх 477 МБ разобранного `model_state`, который CPython держит ради тех же
ключей. При пяти живых картах процесс упирался в `MemoryHigh`: исторические пики
9.3 и 9.9 ГБ при лимите 10.

КАК УСТРОЕНО. Логика модели не переписана ни на строку. Меняются только
ХРАНИЛИЩА полей: вместо словарей туда кладутся массивные объекты из
`array_store` с тем же интерфейсом (`get`, `[]`, `in`, `len`). Геттеры рейтингов
и `_build_team_context` обращаются к полям как к словарям и подмены не замечают.

ПОЧЕМУ ПОТОКОВО. Перепаковывать уже созданные словари бесполезно — проверено на
истории килов: RSS не падает, арены фрагментированы и `malloc_trim` не находит
целиком свободных страниц. Экономит только то, что не создаётся. Поэтому
`model_state` читается `ijson` прямо в массивы, минуя словарь целиком.

ЧТО ЭТА МОДЕЛЬ НЕ УМЕЕТ. Мутировать и экспортировать состояние: строковые ключи
упакованы необратимым хешем, а счётчики не вставляют записи при чтении. Это не
ограничение, а разделение: обновление рейтинга идёт по мутирующему пути
(`register_live_map_context`), где строится обычная словарная модель — и с
20.08 она строится ЛЕНИВО, только когда карта действительно завершилась.
"""
from __future__ import annotations

import os
from pathlib import Path

import numpy as np

from .array_store import (FloatStore, HashedStore, IntCounts, IntStore,
                          PairCounts, RoleStore, StringValues, hash_key)

#: Плоские поля с числовым ключом.
_FLAT_NUMERIC = ("player_global", "player_global_last_seen_ts")
#: Плоские поля со строковым ключом — ключ пакуется хешем.
_HASHED_FLAT = ("lineup_match_counts",)
#: Поля, разложенные по тирам лиг.
_TIERED = ("player_local", "player_local_last_seen_ts", "player_role_local",
           "player_role_local_last_seen_ts", "roster_ratings",
           "roster_last_seen_ts", "roster_match_counts")
#: Из них — со строковым внутренним ключом.
_TIERED_HASHED = ("player_role_local", "player_role_local_last_seen_ts",
                  "roster_ratings", "roster_last_seen_ts", "roster_match_counts")
#: Из них — целочисленные значения.
_TIERED_INT = ("player_local_last_seen_ts", "player_role_local_last_seen_ts",
               "roster_last_seen_ts", "roster_match_counts")
#: Из них — с ключом-КОРТЕЖЕМ (игрок, позиция): в файле это одна строка,
#: но модель разбирает её в пару и обращается парой.
_TIERED_ROLE = ("player_role_local", "player_role_local_last_seen_ts")
#: Из них — счётчики со значением по умолчанию ноль (`defaultdict(int)`).
_TIERED_COUNTS = ("roster_match_counts",)


#: Тиры лиг в состоянии модели. Их ровно три, и они известны заранее — это
#: позволяет разобрать всё за ОДИН проход по файлу вместо прохода на поле.
_TIERS = ("TIER1", "TIER2", "TIER3")


def load_state_arrays(path: Path) -> dict:
    """Крупные поля `model_state` — в массивные хранилища, ЗА ОДИН ПРОХОД.

    `ijson.parse` отдаёт поток событий с полным путём до значения, поэтому имя
    поля, тир и сам ключ берутся из префикса, а промежуточные словари не
    создаются вовсе. Проход на поле обошёлся бы в двадцать пять чтений файла.
    """
    import ijson

    # Сырые накопители: имя поля -> (ключи, значения).
    flat: dict[str, tuple[list, list]] = {}
    tiered: dict[tuple[str, str], tuple[list, list]] = {}
    org_names: list[str] = []
    org_index: dict[str, int] = {}
    cur_keys: list[int] = []
    cur_idx: list[int] = []
    pair_keys: list[int] = []
    pair_vals: list[int] = []

    def org_slot(name: str) -> int:
        s = org_index.get(name)
        if s is None:
            s = len(org_names)
            org_index[name] = s
            org_names.append(name)
        return s

    P = "model_state."
    with open(path, "rb") as fh:
        for prefix, event, value in ijson.parse(fh):
            if event not in ("number", "string") or not prefix.startswith(P):
                continue
            rest = prefix[len(P):]
            head, _, tail = rest.partition(".")
            if not tail:
                continue
            if head in _FLAT_NUMERIC:
                b = flat.setdefault(head, ([], []))
                b[0].append(int(tail))
                b[1].append(value)
            elif head in _HASHED_FLAT:
                b = flat.setdefault(head, ([], []))
                b[0].append(hash_key(tail))
                b[1].append(value)
            elif head in _TIERED:
                tier, _, key = tail.partition(".")
                if not key:
                    continue
                b = tiered.setdefault((head, tier), ([], []))
                b[0].append(hash_key(key) if head in _TIERED_HASHED else int(key))
                b[1].append(value)
            elif head == "player_current_org":
                cur_keys.append(int(tail))
                cur_idx.append(org_slot(str(value)))
            elif head == "player_current_org_matches":
                player, _, org = tail.partition(".")
                if org:
                    pair_keys.append(PairCounts.pack(int(player), org_slot(org)))
                    pair_vals.append(int(value))

    def arr(xs, dtype):
        return np.asarray(xs, dtype) if xs else np.zeros(0, dtype)

    out: dict = {}
    out["player_global"] = FloatStore(
        arr(flat.get("player_global", ([], []))[0], np.int64),
        arr(flat.get("player_global", ([], []))[1], np.float64))
    out["player_global_last_seen_ts"] = IntStore(
        arr(flat.get("player_global_last_seen_ts", ([], []))[0], np.int64),
        arr(flat.get("player_global_last_seen_ts", ([], []))[1], np.int64))
    out["lineup_match_counts"] = HashedStore(
        arr(flat.get("lineup_match_counts", ([], []))[0], np.int64),
        arr(flat.get("lineup_match_counts", ([], []))[1], np.int64),
        default=0, as_int=True)

    for name in _TIERED:
        per_tier: dict = {}
        for tier in _TIERS:
            ks, vs = tiered.get((name, tier), ([], []))
            as_int = name in _TIERED_INT
            ka = arr(ks, np.int64)
            va = arr(vs, np.int64 if as_int else np.float64)
            if name in _TIERED_ROLE:
                # Ключ приходит кортежем (игрок, позиция) — склеивается внутри.
                per_tier[tier] = RoleStore(
                    ka, va, default=None, as_int=as_int)
            elif name in _TIERED_HASHED:
                per_tier[tier] = HashedStore(
                    ka, va, default=(0 if name in _TIERED_COUNTS else None),
                    as_int=as_int)
            elif name in _TIERED_COUNTS:
                per_tier[tier] = IntCounts(ka, va)
            elif as_int:
                per_tier[tier] = IntStore(ka, va)
            else:
                per_tier[tier] = FloatStore(ka, va)
        out[name] = per_tier

    out["player_current_org"] = StringValues(
        arr(cur_keys, np.int64), arr(cur_idx, np.int64), org_names)
    out["player_current_org_matches"] = PairCounts(
        arr(pair_keys, np.int64), arr(pair_vals, np.int64), org_index)
    return out


#: Части `model_state`, которые остаются словарями: они мелкие либо со своей
#: логикой. `roster_tracker` сюда входит намеренно — у него собственный класс
#: с разрешением родословной составов, и трогать его в этой правке не нужно.
_KEEP_AS_IS = ("config", "current_patch_key", "side_bias", "roster_tracker")


def _small_parts(path: Path) -> dict:
    """Мелкие части состояния — обычным разбором: их суммарный вес незначим."""
    import ijson

    out: dict = {}
    with open(path, "rb") as fh:
        for key, value in ijson.kvitems(fh, "model_state"):
            if key in _KEEP_AS_IS:
                out[key] = value
    return out


def build_read_model(path: Path):
    """Модель для ЧТЕНИЯ: логика штатная, крупные поля — массивами.

    Собирается в два шага. Сначала `from_state` поднимает модель по мелким
    частям состояния — конфигурации, смещению сторон, трекеру составов, — и
    заводит все поля пустыми. Затем крупные поля подменяются массивными
    хранилищами. Логика модели при этом не меняется: она обращается к полям как
    к словарям.

    Тиры в модели — это `LeagueTier`, а в файле их имена, поэтому карты по тирам
    перекладываются на элементы перечисления.
    """
    from .domain import LeagueTier
    from .models import HybridPlayerRosterEloModel

    model = HybridPlayerRosterEloModel.from_state(_small_parts(path))
    arrays = load_state_arrays(path)
    by_name = {t.name: t for t in LeagueTier}
    for field, value in arrays.items():
        if isinstance(value, dict):
            setattr(model, field, {by_name[k]: v for k, v in value.items()
                                   if k in by_name})
        else:
            setattr(model, field, value)
    return model
