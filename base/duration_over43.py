"""Предматчевая модель длительности: P(карта ≥ 43 минут) по десяти героям.

Контракт как у kills-over-median / duration-regression: вход — ровно 10
hero ID по позициям (R1..R5, D1..D5), никаких ingame-статистик и сторонних
рейтингов. Дизайн — unsigned `hero_role` (длительность не должна меняться
при обмене сторон). Порог 43 минуты = 2580 секунд.

Отказ — в None: нет артефакта, сломан sklearn, битый драфт. Молча не
выдумывает вероятность.
"""
from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any, Optional

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODEL_DIR = Path(os.getenv(
    "DURATION_OVER43_DIR",
    str(PROJECT_ROOT / "data/duration_over43"),
))
THRESHOLD_MINUTES = 43
THRESHOLD_SECONDS = THRESHOLD_MINUTES * 60

_lock = threading.Lock()
_state: dict[str, Any] = {"loaded": False, "encoder": None, "model": None, "error": None}


def _load() -> bool:
    if _state["loaded"]:
        return _state["model"] is not None
    with _lock:
        if _state["loaded"]:
            return _state["model"] is not None
        try:
            import joblib
            encoder = joblib.load(MODEL_DIR / "encoder.joblib")
            model = joblib.load(MODEL_DIR / "model.joblib")
            _state.update(encoder=encoder, model=model, error=None)
        except Exception as exc:  # noqa: BLE001
            _state.update(encoder=None, model=None, error=f"{type(exc).__name__}: {exc}")
        _state["loaded"] = True
    return _state["model"] is not None


def load_error() -> Optional[str]:
    _load()
    return _state["error"]


def reload(path=None) -> None:
    """Сброс кэша. Для тестов и смены каталога без рестарта процесса."""
    global MODEL_DIR
    if path is not None:
        MODEL_DIR = Path(path)
    with _lock:
        _state.update(loaded=False, encoder=None, model=None, error=None)


def heroes_matrix(heroes) -> np.ndarray:
    arr = np.asarray(heroes, dtype=np.int64)
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    if arr.ndim != 2 or arr.shape[1] != 10:
        raise ValueError("heroes must be (10,) or (n, 10)")
    return arr


def predict_proba(heroes) -> Optional[np.ndarray]:
    """P(duration ≥ 43 min) for each row, or None if the model is missing."""
    if not _load():
        return None
    try:
        matrix = _state["encoder"].transform(heroes_matrix(heroes))
        return _state["model"].predict_proba(matrix)[:, 1]
    except Exception:  # noqa: BLE001
        return None


def predict_over43(heroes) -> Optional[dict[str, Any]]:
    """Одна карта: вероятность и бинарный ответ. None — оценить нечем."""
    p = predict_proba(heroes)
    if p is None or len(p) == 0:
        return None
    prob = float(p[0])
    return {
        "probability": prob,
        "over43": bool(prob >= 0.5),
        "threshold_minutes": THRESHOLD_MINUTES,
        "threshold_seconds": THRESHOLD_SECONDS,
    }
