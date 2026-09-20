"""Свежесть данных, на которых обучен драфт-артефакт — приписка к ML-строкам.

Владелец 20.09.2026: карта ставки прячет Lanes/Early/Late/All/Mix/Kills_window
по умолчанию (`_bet_show_draft_blocks` в `cyberscore_try.py`), а каждую строку
ML-модели дополняет пометкой «данные до DD.MM (N дн.)» — оператор должен видеть,
насколько устарел корпус, на котором эта модель обучена, даже без подробной
карты. Модуль stdlib-only (импортируется без sklearn/joblib) — используется из
`win_model_veto.py`, `cyberscore_try.py` и `laning_serving.py`.

Порядок источников даты для `<dir>`, первый успешный побеждает:
  1) ``results.json``: ``split_boundaries.train.last.start_time`` — ТОЛЬКО если
     ``counts.shipped_fit_rows == counts.train`` (иначе граница относится к
     train/val/test сплиту, а не к боевому корпусу — модель ставится в прод,
     обучившись на ВСЁМ корпусе поверх сплита; пример: у
     ``data/early_nw_draft/2026-09-01_public_early_nw/results.json``
     ``train.last.start_time`` — июнь, хотя модель обучена на полном корпусе);
  2) ``manifest.json`` / ``results.json`` — ключ верхнего уровня
     ``data_asof_ts`` / ``asof`` / ``snapshot_ts`` / ``history_last_end``
     (int/float epoch или строка "YYYY-MM-DD"/"DD.MM.YYYY");
  3) дата ``YYYY-MM-DD`` в префиксе имени каталога, иначе родителя, иначе деда
     (например, ``data/draft_phase_serving/2026-09-05_position_pairs/early_win``
     → дата родителя) — это ВЕРХНЯЯ граница (дата сборки артефакта, не корпуса);
  4) новейший mtime среди ``*.joblib`` / ``*.npz`` / ``*.cbm`` в каталоге;
  5) ``None`` — приписки не будет.
"""
from __future__ import annotations

import datetime
import functools
import json
import re
from pathlib import Path
from typing import Optional, Union

_DATE_PREFIX_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")
_DDMMYYYY_RE = re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})$")


def _parse_date_str(value: str) -> Optional[int]:
    text = str(value or "").strip()
    if not text:
        return None
    match = _DDMMYYYY_RE.match(text)
    if match:
        day, month, year = match.groups()
        text = f"{year}-{month}-{day}"
    try:
        dt = datetime.datetime.strptime(text[:10], "%Y-%m-%d")
    except ValueError:
        return None
    return int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())


def _coerce_ts(value) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return int(value)
        except (TypeError, ValueError, OverflowError):
            return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(float(stripped))
        except ValueError:
            return _parse_date_str(stripped)
    return None


def _load_json(path: Path) -> Optional[dict]:
    try:
        if not path.is_file():
            return None
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else None
    except Exception:                                  # noqa: BLE001
        return None


def _rule1_train_last(dir_path: Path) -> Optional[int]:
    data = _load_json(dir_path / "results.json")
    if not data:
        return None
    try:
        counts = data.get("counts") or {}
        shipped = counts.get("shipped_fit_rows")
        train = counts.get("train")
        if shipped is None or train is None or shipped != train:
            return None
        last = (data.get("split_boundaries") or {}).get("train", {}).get("last") or {}
        return _coerce_ts(last.get("start_time"))
    except Exception:                                  # noqa: BLE001
        return None


def _rule2_manifest_keys(dir_path: Path) -> Optional[int]:
    for name in ("manifest.json", "results.json"):
        data = _load_json(dir_path / name)
        if not data:
            continue
        for key in ("data_asof_ts", "asof", "snapshot_ts", "history_last_end"):
            if key in data:
                ts = _coerce_ts(data.get(key))
                if ts is not None:
                    return ts
    return None


def _rule3_name_date(dir_path: Path) -> Optional[int]:
    candidates = [dir_path, dir_path.parent, dir_path.parent.parent]
    for candidate in candidates:
        match = _DATE_PREFIX_RE.match(candidate.name)
        if match:
            ts = _parse_date_str(match.group(1))
            if ts is not None:
                return ts
    return None


def _rule4_newest_mtime(dir_path: Path) -> Optional[int]:
    best = None
    try:
        for pattern in ("*.joblib", "*.npz", "*.cbm"):
            for fp in dir_path.glob(pattern):
                try:
                    mtime = int(fp.stat().st_mtime)
                except OSError:
                    continue
                if best is None or mtime > best:
                    best = mtime
    except Exception:                                  # noqa: BLE001
        return None
    return best


@functools.lru_cache(maxsize=256)
def _data_asof_ts_cached(dir_str: str) -> Optional[int]:
    dir_path = Path(dir_str)
    for rule in (_rule1_train_last, _rule2_manifest_keys, _rule3_name_date,
                 _rule4_newest_mtime):
        try:
            ts = rule(dir_path)
        except Exception:                              # noqa: BLE001
            ts = None
        if ts is not None:
            return ts
    return None


def data_asof_ts(model_dir: Union[str, Path, None]) -> Optional[int]:
    """Epoch-секунды самых свежих данных за артефактом в ``model_dir``."""
    if not model_dir:
        return None
    try:
        return _data_asof_ts_cached(str(model_dir))
    except Exception:                                  # noqa: BLE001
        return None


def freshness_note(ts: Optional[int], now: Optional[int] = None) -> str:
    """"данные до DD.MM (N дн.)" — пусто, если ``ts`` не задан."""
    if not ts:
        return ""
    try:
        ts = int(ts)
        if now is None:
            now_ts = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp())
        elif isinstance(now, datetime.datetime):
            now_ts = int(now.timestamp())
        else:
            now_ts = int(now)
        days = max(0, (now_ts - ts) // 86400)
        dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
        return f"данные до {dt.strftime('%d.%m')} ({days} дн.)"
    except Exception:                                  # noqa: BLE001
        return ""


def model_dir_note(model_dir: Union[str, Path, None], now: Optional[int] = None) -> str:
    """``freshness_note(data_asof_ts(model_dir), now)`` — удобный шорткат."""
    try:
        return freshness_note(data_asof_ts(model_dir), now)
    except Exception:                                  # noqa: BLE001
        return ""


def with_freshness_note(verdict, model_dir: Union[str, Path, None], now: Optional[int] = None):
    """Копия словаря вердикта с ключом ``freshness_note`` — либо сам вердикт.

    Ключ добавляется ТОЛЬКО когда приписка непустая: пустой/None вердикт, стаб
    модели без ``MODEL_DIR`` и каталог без даты возвращают объект как есть, и
    равенство словарей в тестах и журналах не меняется. Исходный словарь не
    мутируется — модули моделей могут отдавать общий объект.
    """
    try:
        if not isinstance(verdict, dict) or not verdict:
            return verdict
        note = model_dir_note(model_dir, now)
        if not note:
            return verdict
        return dict(verdict, freshness_note=note)
    except Exception:                                  # noqa: BLE001
        return verdict
