"""Динамический tier2-onboarding: JSON-overlay вместо дописывания в исходник.

ЗАЧЕМ. Рантайм `cyberscore_try` при первой встрече неизвестной команды
автоматически вписывает её в tier2 (`_ensure_known_team_or_add_to_tier2`).
Раньше каждая такая запись дописывалась Python-блоком прямо в отслеживаемый
`base/id_to_names.py`: к 02.09.2026 на serv1 накопилось 611 блоков, файл
вечно грязный, и любой pull, трогающий его, упирался в конфликт.

КАК ТЕПЕРЬ. Записи живут в JSON-файле рядом со справочником
(`id_to_names_dynamic_tier2.json`, env-переопределение
`TIER2_DYNAMIC_ONBOARDING_PATH`). Читатели применяют overlay поверх
`id_to_names.tier_two_teams` той же семантикой слияния, что и старые блоки
(int / set / None).

СОВМЕСТИМОСТЬ. Старые auto-added блоки в `id_to_names.py` продолжают
исполняться при импорте — пока файл не почищен, ничего не теряется.
`migrate_legacy()` собирает записи из старых блоков в overlay: запускать
ПЕРЕД очисткой файла (см. `base/tools/migrate_tier2_onboarding.py`).

Формат overlay: {"нормализованный_ключ": [team_id, ...]}. Значения — всегда
списки: в памяти ключ может быть int или set, но в JSON сета нет.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_FILENAME = "id_to_names_dynamic_tier2.json"
ENV_PATH_VAR = "TIER2_DYNAMIC_ONBOARDING_PATH"

LEGACY_MARKER = "# auto-added by cyberscore_try (dynamic tier2 onboarding)"
_KEY_RE = re.compile(r"^\s*_key\s*=\s*(?P<q>['\"])(?P<key>.*?)(?P=q)\s*$")
_TEAM_ID_RE = re.compile(r"^\s*_team_id\s*=\s*(?P<id>-?\d+)\s*$")
_LEGACY_ASSIGN_RE = re.compile(
    r"^\s*tier_two_teams\[\s*(?P<q>['\"])(?P<key>.*?)(?P=q)\s*\]\s*=\s*(?P<id>-?\d+)\s*$"
)


def overlay_path(base_dir: Any = None) -> Path:
    """Путь JSON-overlay: env-переопределение, иначе файл рядом со справочником."""
    raw = str(os.getenv(ENV_PATH_VAR, "")).strip()
    if raw:
        return Path(raw).expanduser()
    if base_dir is None:
        base_dir = BASE_DIR
    return Path(base_dir) / DEFAULT_FILENAME


def load_entries(path: Path) -> dict[str, list[int]]:
    """Читает overlay. Отсутствующий файл — пустой overlay; битый — ошибка.

    Битый файл НЕ трактуется как пустой: записи onboarding'а тогда потерялись
    бы молча, а следующий `upsert_entry` перезаписал бы overlay частичными
    данными.
    """
    path = Path(path)
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"overlay is not a JSON object: {path}")
    out: dict[str, list[int]] = {}
    for key, value in data.items():
        ids: list[int] = []
        for raw_id in value if isinstance(value, list) else [value]:
            try:
                ids.append(int(raw_id))
            except Exception:
                continue
        if ids:
            out[str(key)] = ids
    return out


def save_entries(path: Path, entries: dict[str, list[int]]) -> None:
    """Атомарная запись (rebuild-then-replace): tmp + os.replace."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(entries, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, path)


def _merge_ids(existing: Any, ids: list[int]) -> Any:
    """Слияние со старой семантикой блоков: int / set / None."""
    if existing is None:
        return ids[0] if len(ids) == 1 else set(ids)
    if isinstance(existing, set):
        existing.update(ids)
        return existing
    try:
        existing_id = int(existing)
    except Exception:
        existing_id = None
    if existing_id is None:
        return ids[0] if len(ids) == 1 else set(ids)
    new_ids = [i for i in ids if i != existing_id]
    if not new_ids:
        return existing_id
    return {existing_id, *new_ids}


def apply_entries(names_module: Any, entries: dict[str, list[int]]) -> int:
    """Применяет overlay к `names_module.tier_two_teams`. Возвращает число ключей."""
    merged = 0
    for key, ids in entries.items():
        names_module.tier_two_teams[key] = _merge_ids(
            names_module.tier_two_teams.get(key), list(ids)
        )
        merged += 1
    return merged


def upsert_entry(path: Path, key: str, team_id: int) -> bool:
    """Добавляет team_id под key в overlay. False — запись уже была."""
    entries = load_entries(path)
    ids = entries.get(str(key), [])
    team_id = int(team_id)
    if team_id in ids:
        return False
    entries[str(key)] = [*ids, team_id]
    save_entries(path, entries)
    return True


def harvest_legacy_blocks(id_to_names_path: Path) -> dict[str, list[int]]:
    """Собирает записи из auto-added блоков в id_to_names.py (оба поколения).

    Новый формат: `_key = '...'` / `_team_id = N`. Старый (первые блоки,
    до 2025): `tier_two_teams['key'] = N` — прямое присваивание.
    """
    entries: dict[str, list[int]] = {}

    def _record(key: str, team_id: int) -> None:
        ids = entries.setdefault(key, [])
        if team_id not in ids:
            ids.append(team_id)

    try:
        text = Path(id_to_names_path).read_text(encoding="utf-8")
    except OSError:
        return entries

    in_block = False
    pending_key: str | None = None
    pending_id: int | None = None
    for line in text.splitlines():
        if line.startswith(LEGACY_MARKER):
            in_block = True
            pending_key, pending_id = None, None
            continue
        if not in_block:
            continue
        if line.startswith("except Exception"):
            if pending_key is not None and pending_id is not None:
                _record(pending_key, pending_id)
            in_block, pending_key, pending_id = False, None, None
            continue
        legacy = _LEGACY_ASSIGN_RE.match(line)
        if legacy:
            _record(legacy.group("key"), int(legacy.group("id")))
            continue
        key_match = _KEY_RE.match(line)
        if key_match:
            pending_key = key_match.group("key")
            continue
        id_match = _TEAM_ID_RE.match(line)
        if id_match:
            pending_id = int(id_match.group("id"))
    if in_block and pending_key is not None and pending_id is not None:
        _record(pending_key, pending_id)
    return entries


def migrate_legacy(id_to_names_path: Path, overlay: Path) -> int:
    """Переносит legacy-блоки в overlay (идемпотентно). Возвращает число новых id."""
    harvested = harvest_legacy_blocks(id_to_names_path)
    existing = load_entries(overlay)
    captured = 0
    for key, ids in harvested.items():
        current = existing.setdefault(key, [])
        for team_id in ids:
            if team_id not in current:
                current.append(team_id)
                captured += 1
    if captured:
        save_entries(overlay, existing)
    return captured
