#!/usr/bin/env python3
"""Пишущий слой поверх массивных хранилищ ELO + дельта живых обновлений.

ЗАЧЕМ. После каждой завершённой карты состояние модели меняется на ~70-100
значений из ~2.5 млн: 10 игроков x (global, local, role-local, три last_seen,
current_org, current_org_matches) + 2 ростера x 3 поля + 2 ключа lineup +
рейтинги двух команд (13 полей, 42 места записи в `models.py`). Ради этого
сегодня парсится 519 МБ JSON (+1.28 ГБ), строится словарная модель на 2.5 млн
записей (+1.31 ГБ) и перезаписываются те же 519 МБ обратно — ~30-40 c на
главном потоке и аллокации, которые процесс уже не возвращает (E-253).

Здесь: базовые массивы (sidecar `.npz`, E-254) остаются только-для-чтения, а
изменения копятся в маленьком словаре `Overlay` поверх них. Чтение видит
обновление сразу, запись наружу — это дельта (список изменившихся пар), а не
всё состояние.

ЧЕГО ДЕЛЬТА НЕ УМЕЕТ. Экспортировать ПОЛНОЕ состояние: у хешированных
хранилищ (`lineup_match_counts`, `roster_*`, `player_role_local*`) ключи
упакованы необратимым blake2b, поэтому перечислить их нельзя — `export_state()`
на такой модели не работает. Это не нужно: полный файл собирается ночью из
снимка (`rebase_runtime_model_state.py`), а наружу после каждой карты уходит
только дельта.

PATCH-RESET ВКЛЮЧЁН. `HybridEloConfig.patch_local_reset_mode` по умолчанию
"exact" (`ELO/config.py:62`), и в боевом model_state этого ключа нет — то есть
работает дефолт: при смене патча `_maybe_apply_patch_local_reset` сбрасывает
TIER1 `player_local` и `player_role_local` к `initial_rating`
(`player_local_keep=0.0`), а `roster_ratings` оставляет (`roster_keep=1.0`).
Сброс требует перечисления, которого у хешированных хранилищ нет, — поэтому он
сделан ЛЕНИВЫМ (`Overlay.mark_reset`): значение запоминается, и чтение отдаёт
его для любого ключа, не перекрытого последующей записью. Промежуточные `keep`
лениво непредставимы и отказывают громко.

ФОРМАТ ДЕЛЬТЫ. Ключи — списком пар, а не словарём: JSON-словарь требует
строковых ключей, а обратимое кодирование кортежа `(player, org)` строкой
небезопасно (в именах org встречается и ":", и "::"). Поэтому
`[[ключ, значение], ...]`, где ключ — int, str или [int, str].

Мелкие мутируемые части (`current_patch_key`, `side_bias`, `roster_tracker`)
едут в дельте как есть: они не массивные, а `roster_tracker` на боевом
состоянии занимает 26 МБ — это всё равно в 20 раз меньше полной перезаписи
519 МБ, и в отличие от неё не требует ни разбора всего состояния, ни словарной
модели на 2.5 млн записей.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

#: Поля состояния: имя -> ("flat"|"tiered", вид ключа, вид значения).
#: Виды ключа: "int", "str", "pair" (кортеж (int, str)).
FIELD_SPECS: dict[str, tuple[str, str, str]] = {
    "player_global": ("flat", "int", "float"),
    "player_global_last_seen_ts": ("flat", "int", "int"),
    "lineup_match_counts": ("flat", "str", "int"),
    "player_local": ("tiered", "int", "float"),
    "player_local_last_seen_ts": ("tiered", "int", "int"),
    "player_role_local": ("tiered", "pair", "float"),
    "player_role_local_last_seen_ts": ("tiered", "pair", "int"),
    "roster_ratings": ("tiered", "str", "float"),
    "roster_last_seen_ts": ("tiered", "str", "int"),
    "roster_match_counts": ("tiered", "str", "int"),
    "player_current_org": ("flat", "int", "str"),
    "player_current_org_matches": ("flat", "pair", "int"),
}

#: Мелкие поля модели, которые тоже меняются и должны переживать перезапуск.
SMALL_PARTS = ("current_patch_key", "side_bias", "roster_tracker")

DELTA_VERSION = 1

#: Отсутствие ленивого сброса. Именно sentinel, а не None: None — законное
#: значение сброса, если initial_rating когда-нибудь станет None.
_NO_RESET = object()


class Overlay:
    """Словарный интерфейс поверх хранилища только-для-чтения.

    Держит изменения в своём словаре; за отсутствием изменения читает базу.
    Ровно та часть словарного протокола, которую использует живой путь
    (`get`, `[]`, `in`, `=`, `+=` через пару get/set). `items()` доступен,
    только если базовое хранилище перечислимо.

    ЛЕНИВЫЙ RESET. `_maybe_apply_patch_local_reset` при смене патча
    перезаписывает ВСЕ записи тира (`models.py:944-957`), а перечислить
    хешированные хранилища (`player_role_local`, `roster_ratings`) нельзя —
    ключи упакованы необратимым blake2b. Зато сам сброс перечисления не
    требует: при `keep=0.0` каждое значение становится `initial_rating`, то
    есть все ключи базы читаются одинаково. `mark_reset(value)` запоминает это
    значение, и чтение отдаёт его для любого ключа, не перекрытого записью.
    При `keep=1.0` сброс — no-op (`initial + (rating-initial)*1 == rating`),
    поэтому он просто не вызывается. Промежуточные `keep` лениво не
    представляются — на них overlay отказывается громко, а не молча.
    """

    __slots__ = ("_base", "_over", "_name", "_reset")

    def __init__(self, base: Any, name: str = "") -> None:
        self._base = base
        self._over: dict[Any, Any] = {}
        self._name = name
        self._reset: Any = _NO_RESET

    # --- чтение ---------------------------------------------------------- #
    def get(self, key: Any, default: Any = None) -> Any:
        if key in self._over:
            return self._over[key]
        if self._reset is not _NO_RESET:
            return self._reset
        return self._base.get(key, default)

    def __getitem__(self, key: Any) -> Any:
        if key in self._over:
            return self._over[key]
        if self._reset is not _NO_RESET:
            return self._reset
        return self._base[key]

    def __contains__(self, key: Any) -> bool:
        return key in self._over or key in self._base

    def __len__(self) -> int:
        # Приблизительно: точная длина потребовала бы перечисления базы, а у
        # хешированных хранилищ оно невозможно. В живом пути длина не нужна.
        return len(self._base) + len(self._over)

    def __bool__(self) -> bool:
        return bool(self._over) or bool(self._base)

    def items(self):
        base_items = getattr(self._base, "items", None)
        if not callable(base_items):
            raise TypeError(
                f"хранилище {self._name or type(self._base).__name__} не перечислимо "
                "(ключи упакованы необратимым хешем) — полное состояние из "
                "overlay-модели не экспортируется, наружу уходит только дельта"
            )
        if self._reset is not _NO_RESET:
            raise TypeError(
                f"хранилище {self._name} сброшено лениво (reset={self._reset}) — "
                "перечисление после ленивого сброса не определено"
            )
        merged = {k: v for k, v in base_items()}
        merged.update(self._over)
        return iter(merged.items())

    # --- запись ---------------------------------------------------------- #
    def __setitem__(self, key: Any, value: Any) -> None:
        self._over[key] = value

    def mark_reset(self, value: Any) -> None:
        """Сбросить всё хранилище к одному значению, не перечисляя его."""
        self._reset = value
        self._over.clear()

    @property
    def reset_value(self) -> Any:
        return None if self._reset is _NO_RESET else self._reset

    def changes(self) -> dict:
        return dict(self._over)

    @property
    def base(self) -> Any:
        return self._base


def wrap_model(model: Any) -> dict[str, Any]:
    """Подменить поля модели на overlay поверх базовых хранилищ.

    Возвращает карту {поле: Overlay или {tier: Overlay}}, чтобы вызывающий мог
    собрать дельту. Мелкие поля (конфиг, side_bias, roster_tracker) не
    заворачиваются: они и так обычные объекты и мутируются на месте.
    """
    wrappers: dict[str, Any] = {}
    for field, (scope, _key_kind, _val_kind) in FIELD_SPECS.items():
        current = getattr(model, field, None)
        if current is None:
            continue
        if scope == "tiered":
            per_tier = {}
            for tier, store in dict(current).items():
                overlay = Overlay(store, f"{field}[{tier}]")
                per_tier[tier] = overlay
            setattr(model, field, per_tier)
            wrappers[field] = per_tier
        else:
            overlay = Overlay(current, field)
            setattr(model, field, overlay)
            wrappers[field] = overlay
    return wrappers


def _encode_key(key: Any, kind: str) -> Any:
    if kind == "pair":
        player, org = key
        return [int(player), str(org)]
    if kind == "int":
        return int(key)
    return str(key)


def _decode_key(raw: Any, kind: str) -> Any:
    if kind == "pair":
        return (int(raw[0]), str(raw[1]))
    if kind == "int":
        return int(raw)
    return str(raw)


def _cast_value(value: Any, kind: str) -> Any:
    if kind == "int":
        return int(value)
    if kind == "float":
        return float(value)
    return str(value)


def collect_changes(wrappers: dict[str, Any]) -> dict[str, Any]:
    """Дельта: только то, что изменилось относительно базы."""
    out: dict[str, Any] = {}
    for field, wrapper in wrappers.items():
        _scope, key_kind, val_kind = FIELD_SPECS[field]
        if isinstance(wrapper, dict):
            per_tier = {}
            for tier, overlay in wrapper.items():
                rows = overlay.changes()
                if rows:
                    per_tier[str(getattr(tier, "value", tier))] = [
                        [_encode_key(k, key_kind), _cast_value(v, val_kind)]
                        for k, v in rows.items()
                    ]
            if per_tier:
                out[field] = per_tier
        else:
            rows = wrapper.changes()
            if rows:
                out[field] = [
                    [_encode_key(k, key_kind), _cast_value(v, val_kind)]
                    for k, v in rows.items()
                ]
    return out


def apply_changes(model: Any, changes: dict[str, Any]) -> int:
    """Наложить дельту на overlay-модель. Возвращает число наложенных значений."""
    applied = 0
    for field, payload in (changes or {}).items():
        spec = FIELD_SPECS.get(field)
        if spec is None:
            continue
        _scope, key_kind, val_kind = spec
        current = getattr(model, field, None)
        if current is None:
            continue
        if isinstance(payload, dict):        # tiered
            from .domain import LeagueTier
            by_name = {t.value: t for t in LeagueTier}
            for tier_name, rows in payload.items():
                tier = by_name.get(tier_name)
                store = current.get(tier) if isinstance(current, dict) else None
                if store is None:
                    continue
                for raw_key, raw_value in rows:
                    store[_decode_key(raw_key, key_kind)] = _cast_value(raw_value, val_kind)
                    applied += 1
        else:                                # flat
            for raw_key, raw_value in payload:
                current[_decode_key(raw_key, key_kind)] = _cast_value(raw_value, val_kind)
                applied += 1
    return applied


def collect_small_parts(model: Any) -> dict[str, Any]:
    """Мелкие мутируемые поля в JSON-виде — как их сериализует `export_state`.

    Напрямую класть нельзя: `side_bias` ключуется элементами `LeagueTier`, а
    `roster_tracker` — объект `RosterLineageTracker` со своим `export_state()`
    (`ELO/models.py:386-387`).
    """
    out: dict[str, Any] = {}
    patch_key = getattr(model, "current_patch_key", None)
    out["current_patch_key"] = str(patch_key) if patch_key else None
    side_bias = getattr(model, "side_bias", None)
    if isinstance(side_bias, dict):
        out["side_bias"] = {
            str(getattr(tier, "value", tier)): float(value)
            for tier, value in side_bias.items()
        }
    tracker = getattr(model, "roster_tracker", None)
    export = getattr(tracker, "export_state", None)
    if callable(export):
        out["roster_tracker"] = export()
    elif isinstance(tracker, dict):
        out["roster_tracker"] = tracker
    return out


def restore_small_parts(model: Any, parts: dict[str, Any]) -> None:
    """Обратно `collect_small_parts`: те же приёмы, что в `from_state`."""
    if not isinstance(parts, dict):
        return
    from .domain import LeagueTier
    from .roster import RosterLineageTracker

    if "current_patch_key" in parts:
        model.current_patch_key = parts.get("current_patch_key")
    raw_bias = parts.get("side_bias")
    if isinstance(raw_bias, dict):
        by_value = {tier.value: tier for tier in LeagueTier}
        model.side_bias = {
            by_value[key]: float(value)
            for key, value in raw_bias.items() if key in by_value
        }
    raw_tracker = parts.get("roster_tracker")
    if isinstance(raw_tracker, dict):
        model.roster_tracker = RosterLineageTracker.from_state(raw_tracker)


def collect_resets(wrappers: dict[str, Any]) -> dict[str, Any]:
    """Ленивые сбросы (patch-reset): поле -> значение или {тир -> значение}."""
    out: dict[str, Any] = {}
    for field, wrapper in wrappers.items():
        _scope, _key_kind, val_kind = FIELD_SPECS[field]
        if isinstance(wrapper, dict):
            per_tier = {}
            for tier, overlay in wrapper.items():
                value = overlay.reset_value
                if value is not None:
                    per_tier[str(getattr(tier, "value", tier))] = _cast_value(value, val_kind)
            if per_tier:
                out[field] = per_tier
        else:
            value = wrapper.reset_value
            if value is not None:
                out[field] = _cast_value(value, val_kind)
    return out


def apply_resets(model: Any, resets: dict[str, Any]) -> int:
    """Наложить ленивые сбросы. Возвращает число сброшенных хранилищ."""
    from .domain import LeagueTier

    by_name = {t.value: t for t in LeagueTier}
    applied = 0
    for field, payload in (resets or {}).items():
        spec = FIELD_SPECS.get(field)
        if spec is None:
            continue
        _scope, _key_kind, val_kind = spec
        current = getattr(model, field, None)
        if current is None:
            continue
        if isinstance(payload, dict):
            for tier_name, value in payload.items():
                store = current.get(by_name.get(tier_name)) if isinstance(current, dict) else None
                mark = getattr(store, "mark_reset", None)
                if callable(mark):
                    mark(_cast_value(value, val_kind))
                    applied += 1
        else:
            mark = getattr(current, "mark_reset", None)
            if callable(mark):
                mark(_cast_value(payload, val_kind))
                applied += 1
    return applied


def save_delta(path: Path, *, base_reference_timestamp: int,
               base_model_config_signature: str, changes: dict[str, Any],
               small_parts: dict[str, Any], updated_at: int,
               resets: dict[str, Any] | None = None) -> None:
    """Атомарная запись дельты (tmp + os.replace + fsync).

    Атомарность здесь важнее, чем была при перезаписи 519 МБ: файл маленький,
    поэтому падение процесса не оставляет ни окна на десятки секунд, ни
    оборванной записи.
    """
    payload = {
        "delta_version": DELTA_VERSION,
        "base_reference_timestamp": int(base_reference_timestamp),
        "base_model_config_signature": str(base_model_config_signature or ""),
        "updated_at": int(updated_at),
        "small_parts": small_parts,
        "resets": resets or {},
        "changes": changes,
    }
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def load_delta(path: Path, *, base_reference_timestamp: int,
               base_model_config_signature: str) -> dict[str, Any] | None:
    """Дельта, если она собрана ОТ ЭТОЙ базы; иначе None.

    Те же охранники, что у рантайм-состояния (`live_team_strength.py:590-603`):
    при смене снимка накопленная дельта обязана быть выброшена, иначе обновления
    легли бы на чужую базу.
    """
    path = Path(path)
    if not path.exists():
        return None
    try:
        with path.open(encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    if int(payload.get("delta_version") or 0) != DELTA_VERSION:
        return None
    try:
        if int(payload.get("base_reference_timestamp") or 0) != int(base_reference_timestamp):
            return None
    except (TypeError, ValueError):
        return None
    if str(payload.get("base_model_config_signature") or "") != str(base_model_config_signature or ""):
        return None
    return payload
