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
from decimal import Decimal
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


def _empty_raw() -> dict:
    """Сырые накопители: то, из чего потом собираются хранилища."""
    return {
        "flat": {},          # имя поля -> (ключи, значения)
        "tiered": {},        # (имя поля, тир) -> (ключи, значения)
        "org_names": [],
        "org_index": {},
        "cur_keys": [],
        "cur_idx": [],
        "pair_keys": [],
        "pair_vals": [],
    }


def _accumulate_state(path: Path, prefix: str, raw: dict) -> None:
    """Один проход `ijson.parse` по `model_state` — в сырые накопители."""
    import ijson

    flat = raw["flat"]
    tiered = raw["tiered"]
    org_names = raw["org_names"]
    org_index = raw["org_index"]
    cur_keys = raw["cur_keys"]
    cur_idx = raw["cur_idx"]
    pair_keys = raw["pair_keys"]
    pair_vals = raw["pair_vals"]

    def org_slot(name: str) -> int:
        s = org_index.get(name)
        if s is None:
            s = len(org_names)
            org_index[name] = s
            org_names.append(name)
        return s

    P = prefix
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


def _stores_from_raw(raw: dict) -> dict:
    """Хранилища из сырых накопителей.

    Единственное место, где решается, какое поле каким хранилищем представлять:
    и потоковая сборка из JSON, и загрузка из sidecar-`.npz` идут через неё,
    иначе два пути рано или поздно разъехались бы по типам или дефолтам.
    Вход — списки ИЛИ numpy-массивы (sidecar отдаёт массивы), поэтому проверка
    пустоты через `len()`, а не через истинность.
    """
    flat = raw["flat"]
    tiered = raw["tiered"]

    def arr(xs, dtype):
        return np.asarray(xs, dtype) if len(xs) else np.zeros(0, dtype)

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

    org_names = [str(n) for n in raw["org_names"]]
    out["player_current_org"] = StringValues(
        arr(raw["cur_keys"], np.int64), arr(raw["cur_idx"], np.int64), org_names)
    out["player_current_org_matches"] = PairCounts(
        arr(raw["pair_keys"], np.int64), arr(raw["pair_vals"], np.int64),
        {name: i for i, name in enumerate(org_names)})
    return out


def load_state_arrays(path: Path, prefix: str = "model_state.") -> dict:
    """Крупные поля `model_state` — в массивные хранилища, ЗА ОДИН ПРОХОД.

    `ijson.parse` отдаёт поток событий с полным путём до значения, поэтому имя
    поля, тир и сам ключ берутся из префикса, а промежуточные словари не
    создаются вовсе. Проход на поле обошёлся бы в двадцать пять чтений файла.

    Дорого: на боевом состоянии (519 МБ, 962 719 игроков, 1 408 197 ключей
    lineup) проход через Python-списки стоит 1140 МБ RSS и ~60 c, хотя самих
    данных в массивах 74 МБ (замер E-253). Поэтому рядом кладётся sidecar
    `.npz` — см. `save_state_arrays` / `load_state_arrays_cached`.
    """
    raw = _empty_raw()
    _accumulate_state(path, prefix, raw)
    return _stores_from_raw(raw)


#: Sidecar с готовыми массивами: `<источник>.arrays.npz`. Несжатый — сжатый npz
#: читается только распаковкой, а здесь цель именно отдать массивы memcpy'ем.
ARRAYS_SIDECAR_SUFFIX = ".arrays.npz"


def sidecar_path(src: Path) -> Path:
    return Path(str(src) + ARRAYS_SIDECAR_SUFFIX)


def save_state_arrays(src: Path, prefix: str = "model_state.",
                      out: Path | None = None) -> Path:
    """Собрать sidecar-`.npz` из `model_state` (строится один раз, вне прода).

    Внутри — те же сырые накопители, что использует потоковая сборка, поэтому
    содержимое побайтово эквивалентно: хранилища из обоих путей собирает одна
    функция `_stores_from_raw`. Плюс штамп источника (mtime_ns, размер) и
    префикс — по ним sidecar признаётся годным или игнорируется.

    Атомарно: tmp + os.replace (rebuild-then-replace).
    """
    import os

    raw = _empty_raw()
    _accumulate_state(Path(src), prefix, raw)

    def sorted_pair(ks, vs, kdtype, vdtype):
        """Ключи+значения, отсортированные по ключу — хранилище их не копирует.

        Сортировка здесь, а не в `_Sorted.__init__`, экономит полную копию обоих
        массивов при каждой загрузке в живом процессе (stable sort: порядок
        равных ключей сохранён, результат побайтово тот же).
        """
        ka = np.asarray(ks, kdtype)
        va = np.asarray(vs, vdtype)
        if len(ka) > 1:
            order = np.argsort(ka, kind="stable")
            ka, va = ka[order], va[order]
        return ka, va

    arrays: dict[str, Any] = {}
    for field, (ks, vs) in raw["flat"].items():
        ka, va = sorted_pair(ks, vs, np.int64, np.float64)
        arrays[f"flat.{field}.keys"] = ka
        arrays[f"flat.{field}.vals"] = va
    for (name, tier), (ks, vs) in raw["tiered"].items():
        ka, va = sorted_pair(ks, vs, np.int64, np.float64)
        arrays[f"tiered.{name}.{tier}.keys"] = ka
        arrays[f"tiered.{name}.{tier}.vals"] = va
    ck, cv = sorted_pair(raw["cur_keys"], raw["cur_idx"], np.int64, np.int64)
    arrays["cur_keys"], arrays["cur_idx"] = ck, cv
    pk, pv = sorted_pair(raw["pair_keys"], raw["pair_vals"], np.int64, np.int64)
    arrays["pair_keys"], arrays["pair_vals"] = pk, pv
    arrays["org_names"] = np.asarray([str(n) for n in raw["org_names"]], dtype="U")
    st = Path(src).stat()
    arrays["src_mtime_ns"] = np.int64(st.st_mtime_ns)
    arrays["src_size"] = np.int64(st.st_size)
    arrays["prefix"] = np.asarray(prefix, dtype="U")

    out = Path(out) if out is not None else sidecar_path(src)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp.npz")
    with open(tmp, "wb") as fh:
        np.savez(fh, **arrays)
    os.replace(tmp, out)
    return out


def _sidecar_matches(npz: Path, src: Path, prefix: str) -> bool:
    try:
        st = src.stat()
        with np.load(npz, allow_pickle=False) as z:
            for key in ("src_mtime_ns", "src_size", "prefix"):
                if key not in z.files:
                    return False
            return (int(z["src_mtime_ns"]) == int(st.st_mtime_ns)
                    and int(z["src_size"]) == int(st.st_size)
                    and str(z["prefix"]) == prefix)
    except Exception:
        return False


def load_state_arrays_cached(path: Path, prefix: str = "model_state.") -> dict:
    """Массивы из sidecar-`.npz`, если он свежий, иначе потоковая сборка.

    Штамп sidecar'а — mtime_ns и размер источника: после ночной доставки нового
    снимка или после записи рантайм-состояния sidecar автоматически считается
    устаревшим, и модель собирается потоком (а затем sidecar можно пересобрать).
    """
    npz = sidecar_path(path)
    if npz.exists() and _sidecar_matches(npz, path, prefix):
        raw = _empty_raw()
        with np.load(npz, allow_pickle=False) as z:
            for key in z.files:
                if key.startswith("flat."):
                    _scope, field, what = key.split(".", 2)
                    # СПИСОК, а не кортеж: потоковая ветка только зовёт
                    # `.append` у вложенных списков, а здесь элемент
                    # присваивается — `([], [])[0] = ...` упал бы.
                    slot = raw["flat"].setdefault(field, [[], []])
                    slot[0 if what == "keys" else 1] = z[key]
                elif key.startswith("tiered."):
                    _scope, name, tier, what = key.split(".", 3)
                    slot = raw["tiered"].setdefault((name, tier), [[], []])
                    slot[0 if what == "keys" else 1] = z[key]
            for key in ("cur_keys", "cur_idx", "pair_keys", "pair_vals"):
                if key in z.files:
                    raw[key] = z[key]
            if "org_names" in z.files:
                names = [str(n) for n in z["org_names"]]
                raw["org_names"] = names
                raw["org_index"] = {n: i for i, n in enumerate(names)}
        return _stores_from_raw(raw)
    return load_state_arrays(path, prefix)


#: Части `model_state`, которые остаются словарями: они мелкие либо со своей
#: логикой. `roster_tracker` сюда входит намеренно — у него собственный класс
#: с разрешением родословной составов, и трогать его в этой правке не нужно.
_KEEP_AS_IS = ("config", "current_patch_key", "side_bias", "roster_tracker")


def _runtime_is_valid(runtime_path: Path, snapshot_path: Path) -> bool:
    """Рантайм годится, только если он собран ОТ ЭТОГО снимка.

    Проверяются те же два поля, что и у словарного пути: отметка времени
    базового снимка и подпись конфигурации. Без этого можно смешать состояние с
    чужой базой и получить рейтинги, которых никогда не было.
    """
    import ijson

    from .live_team_strength import (_snapshot_model_config_signature,
                                     _snapshot_reference_timestamp)

    if not runtime_path.exists():
        return False

    def plain(value):
        """`ijson` отдаёт числа `Decimal`, а подпись считается по float.

        Без приведения `json.dumps` внутри подписи падает, а если бы и не падал —
        строка вышла бы другой, и рантайм всегда признавался бы чужим.
        """
        if isinstance(value, dict):
            return {k: plain(v) for k, v in value.items()}
        if isinstance(value, list):
            return [plain(v) for v in value]
        if isinstance(value, Decimal):
            return float(value)
        return value

    want_ts = want_sig = None
    with open(snapshot_path, "rb") as fh:
        head = {k: v for k, v in ijson.kvitems(fh, "meta")}
    want_ts = _snapshot_reference_timestamp({"meta": plain(head)})
    with open(snapshot_path, "rb") as fh:
        cfg = {k: v for k, v in ijson.kvitems(fh, "model_state.config")}
    want_sig = _snapshot_model_config_signature(
        {"model_state": {"config": plain(cfg)}})
    got_ts = got_sig = None
    with open(runtime_path, "rb") as fh:
        for prefix, event, value in ijson.parse(fh):
            if prefix == "base_reference_timestamp":
                got_ts = int(value)
            elif prefix == "base_model_config_signature":
                got_sig = str(value)
            if got_ts is not None and got_sig is not None:
                break
    return got_ts == want_ts and got_sig == want_sig


def _small_parts(path: Path) -> dict:
    """Мелкие части состояния, собранные без материализации крупных полей.

    `ijson.kvitems` сначала строит значение каждого ключа `model_state`, в том
    числе многомиллионных полей, и только потом возвращает его вызывающему
    коду. Здесь сохраняемые значения собираются выборочно из событий parser-а;
    события discarded-полей пропускаются целиком.
    """
    import ijson
    from ijson.common import ObjectBuilder

    out: dict = {}
    with open(path, "rb") as fh:
        active_key = None
        builder = None
        depth = 0
        for prefix, event, value in ijson.parse(fh):
            if prefix == "model_state" and event == "map_key":
                active_key = value if value in _KEEP_AS_IS else None
                builder = ObjectBuilder() if active_key is not None else None
                depth = 0
                continue
            if active_key is None:
                continue

            builder.event(event, value)
            if event in ("start_map", "start_array"):
                depth += 1
            elif event in ("end_map", "end_array"):
                depth -= 1

            # A scalar has no container depth; a container is complete when
            # its matching end event returns depth to zero.
            if depth == 0 and event not in ("map_key", "start_map",
                                             "start_array"):
                out[active_key] = builder.value
                active_key = None
                builder = None
    return out


def build_read_model(path: Path, runtime_model_state_path: Path | None = None):
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
    # Рантайм-состояние заменяет ПОЛЯ модели, но не остальной снимок: там лежит
    # только `model_state`, а имена команд и история килов остаются базовыми.
    src, prefix = path, "model_state."
    if runtime_model_state_path is not None and _runtime_is_valid(
            runtime_model_state_path, path):
        src, prefix = runtime_model_state_path, "model_state."
    arrays = load_state_arrays_cached(src, prefix)
    by_name = {t.name: t for t in LeagueTier}
    for field, value in arrays.items():
        if isinstance(value, dict):
            setattr(model, field, {by_name[k]: v for k, v in value.items()
                                   if k in by_name})
        else:
            setattr(model, field, value)
    return model


#: Готовые модели по (базовый снимок, рантайм-состояние) и их отметкам времени.
#: Два слота: базовая и свежая живут рядом ровно как у словарной реализации.
_READ_CACHE: list = []
_READ_SLOTS = 2


def _stamp(path: Path) -> tuple:
    try:
        st = path.stat()
        return (str(path), int(st.st_mtime_ns), int(st.st_size))
    except OSError:
        return (str(path), 0, 0)


def load_read_model(snapshot_path: Path, runtime_model_state_path: Path | None = None,
                    delta_path: Path | None = None):
    """Массивная модель с примешанными живыми обновлениями, с кэшем по отметкам.

    Свежесть здесь обязательна: базовый снимок пересобирается редко, а живые
    обновления приходят после каждой завершённой карты. Читать рейтинги из
    базового значило бы считать `hybrid_strength` по данным недельной давности —
    ровно то, что чинили 20.08.

    Источники живых обновлений, по убыванию предпочтения:
      1. `delta_path` — дельта поверх базовых массивов (килобайты; E-255).
         Это основной путь: он не требует ни разбора 519 МБ, ни словарной модели.
      2. `runtime_model_state_path` — прежнее полное состояние (519 МБ).
         Остаётся запасным: пока дельта не собрана
         (`ELO/convert_state_to_delta.py`), поведение прежнее.

    Кэш ключуется временем изменения и размером файлов: перечитывать снимок на
    каждый вызов дороже, чем держать модель.
    """
    if delta_path is not None and Path(delta_path).exists():
        reference, signature = _snapshot_meta(snapshot_path)
        from . import state_overlay
        if state_overlay.load_delta(delta_path, base_reference_timestamp=reference,
                                    base_model_config_signature=signature) is not None:
            return build_overlay_model(snapshot_path, delta_path)
    key = (_stamp(snapshot_path),
           _stamp(runtime_model_state_path) if runtime_model_state_path else None)
    for i, (k, model) in enumerate(_READ_CACHE):
        if k == key:
            _READ_CACHE.append(_READ_CACHE.pop(i))
            return model
    model = build_read_model(snapshot_path, runtime_model_state_path)
    _READ_CACHE.append((key, model))
    del _READ_CACHE[:-_READ_SLOTS]
    return model


#: Мета снимка по отметке файла: `reference_timestamp` и подпись конфигурации.
#: Нужны для валидации дельты и читаются потоком — meta идёт первым разделом,
#: поэтому дальше начала файла проход не идёт.
_META_CACHE: dict = {}


def _snapshot_meta(snapshot_path: Path) -> tuple[int, str]:
    """(reference_timestamp, model_config_signature) из снимка, потоково.

    Сигнатура обязана считаться ТОЧНО как в `live_team_strength`: сначала
    `meta.model_config_signature`, а если её там нет — по `model_state.config`.
    Расхождение здесь стоило бы молчаливой заморозки живых обновлений: дельта
    признавалась бы чужой, и рейтинги остались бы на срезе снимка.
    """
    key = _stamp(snapshot_path)
    cached = _META_CACHE.get(str(snapshot_path))
    if cached is not None and cached[0] == key:
        return cached[1]
    import ijson

    reference, signature, config = 0, "", None
    try:
        with open(snapshot_path, "rb") as fh:
            for prefix, event, value in ijson.parse(fh, use_float=True):
                if prefix == "meta.reference_timestamp" and event == "number":
                    reference = int(value)
                elif prefix == "meta.model_config_signature" and event == "string":
                    signature = str(value)
                elif prefix.startswith("teams_by_org_key"):
                    break
    except Exception:
        pass
    if not signature:
        # meta сигнатуры не несёт — считаем по конфигурации состояния.
        try:
            with open(snapshot_path, "rb") as fh:
                for cfg in ijson.items(fh, "model_state.config", use_float=True):
                    config = cfg
                    break
        except Exception:
            config = None
        try:
            from .live_team_strength import _model_config_signature
            signature = _model_config_signature({"config": config} if config else None)
        except Exception:
            signature = ""
    _META_CACHE[str(snapshot_path)] = (key, (reference, signature))
    return reference, signature


#: Живая модель МУТИРУЕТСЯ, поэтому кэш отдельный от read-only.
_OVERLAY_CACHE: list = []
_OVERLAY_SLOTS = 2


def build_overlay_model(snapshot_path: Path, delta_path: Path | None = None):
    """Живая модель: базовые массивы + пишущий слой + наложенная дельта.

    Базовые массивы ОБЩИЕ с read-only моделью: `copy.copy` даёт свой словарь
    атрибутов при тех же хранилищах, а `state_overlay.wrap_model` подменяет поля
    обёртками, которые пишут в свои словари и читают из базы. Второй копии
    массивов (74-269 МБ) не возникает, базовая модель остаётся нетронутой.

    Мутирующая модель не умеет `export_state()` — и не должна: наружу уходит
    дельта (`state_overlay.collect_changes`), а полное состояние собирается
    ночью из снимка (`rebase_runtime_model_state.py`).
    """
    import copy as _copy

    from . import state_overlay

    key = (_stamp(snapshot_path), _stamp(delta_path) if delta_path else None)
    for i, (k, model) in enumerate(_OVERLAY_CACHE):
        if k == key:
            _OVERLAY_CACHE.append(_OVERLAY_CACHE.pop(i))
            return model

    base = load_read_model(snapshot_path, None)
    model = _copy.copy(base)
    wrappers = state_overlay.wrap_model(model)
    model._overlay_wrappers = wrappers
    model._overlay_delta_path = Path(delta_path) if delta_path else None
    model._overlay_snapshot_path = Path(snapshot_path)
    applied = 0
    if delta_path is not None:
        reference, signature = _snapshot_meta(snapshot_path)
        payload = state_overlay.load_delta(
            Path(delta_path), base_reference_timestamp=reference,
            base_model_config_signature=signature)
        if payload is not None:
            state_overlay.restore_small_parts(model, payload.get("small_parts") or {})
            applied += state_overlay.apply_resets(model, payload.get("resets") or {})
            applied += state_overlay.apply_changes(model, payload.get("changes") or {})
    model._overlay_applied = applied
    _OVERLAY_CACHE.append((key, model))
    del _OVERLAY_CACHE[:-_OVERLAY_SLOTS]
    return model


def rekey_overlay_cache(model, snapshot_path: Path, delta_path: Path) -> None:
    """Перепривязать кэш живой модели к новой отметке дельты.

    После записи дельты её mtime меняется, и без перепривязки следующий вызов
    собирал бы модель заново (0.2 c и ~0.4 ГБ) вместо того чтобы взять ту же —
    а она и есть источник правды в рамках процесса.
    """
    key = (_stamp(snapshot_path), _stamp(delta_path))
    for i, (_k, cached) in enumerate(_OVERLAY_CACHE):
        if cached is model:
            _OVERLAY_CACHE[i] = (key, cached)
            return
    _OVERLAY_CACHE.append((key, model))
    del _OVERLAY_CACHE[:-_OVERLAY_SLOTS]


def load_team_names(snapshot_path: Path) -> dict[int, str]:
    """id команды -> имя, потоково из `teams_by_org_key`.

    Нужно только для заполнения `MatchRecord`, на числа не влияет — но пустое
    имя мешает разбору org-ключа, поэтому подсовывать пустой словарь нельзя.
    Читается отдельным проходом: это 7 МБ против 67 у разобранного целиком.
    """
    import ijson

    names: dict[int, str] = {}
    with open(snapshot_path, "rb") as fh:
        team_id = None
        for prefix, event, value in ijson.parse(fh):
            if not prefix.startswith("teams_by_org_key."):
                continue
            tail = prefix.split(".")[-1]
            if tail == "team_id" and event == "number":
                team_id = int(value)
            elif tail == "team_name" and event == "string" and team_id:
                names[team_id] = str(value)
                team_id = None
    return names
