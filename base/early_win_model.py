"""Draft-only map winner among games lasting 20–34 minutes (E-260).

This is a display-only estimate, not the probability that any game ends early.
"""
from __future__ import annotations

import os
import sys
import threading
from pathlib import Path
from typing import Any, Optional, Sequence

# Порядок героев (radiant pos1-5, затем dire pos1-5) обязан совпадать с тем, на
# котором обучен энкодер. Единственный источник этого порядка — `win_model_veto`,
# и он ПЕРЕДАЁТ сюда готовый вектор. Импортировать его отсюда нельзя: прод зовёт
# `import win_model_veto` (верхнего уровня, cyberscore_try.py:67), а `from
# base.win_model_veto import ...` создало бы ВТОРУЮ копию модуля со своим
# состоянием, кэшами и `_LAST_FILL`.

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODEL_DIR = Path(os.getenv(
    "EARLY_WIN_MODEL_DIR",
    str(PROJECT_ROOT / "data/draft_phase_serving/2026-09-05_position_pairs/early_win"),
))
# Отключается без деплоя: EARLY_WIN_MODEL_ENABLED=0.
ENABLED = os.getenv("EARLY_WIN_MODEL_ENABLED", "1") == "1"

_CACHE_LIMIT = 4096
_cache: dict = {}
_lock = threading.Lock()
_state: dict[str, Any] = {"loaded": False, "encoder": None, "model": None, "error": None}


def _load() -> bool:
    if _state["loaded"]:
        return _state["model"] is not None
    with _lock:
        if _state["loaded"]:
            return _state["model"] is not None
        try:
            # Энкодер запиклен как `base.draft_features`, поэтому корень проекта
            # обязан быть в пути ДО распаковки. В бою его кладёт cyberscore_try,
            # но зависеть от порядка импорта нельзя.
            if str(PROJECT_ROOT) not in sys.path:
                sys.path.insert(0, str(PROJECT_ROOT))
            import joblib  # локальный импорт: модуль обязан импортироваться без sklearn
            encoder = joblib.load(MODEL_DIR / "early_win_feature_encoder.joblib")
            model = joblib.load(MODEL_DIR / "early_win_model.joblib")
            if encoder.n_columns != model.n_features_in_:
                raise ValueError("early-win encoder/model width mismatch")
            _state.update(encoder=encoder, model=model, error=None)
        except Exception as exc:                     # noqa: BLE001 — любая поломка = нет оценки
            _state.update(encoder=None, model=None, error=f"{type(exc).__name__}: {exc}")
        _state["loaded"] = True
    return _state["model"] is not None


def load_error() -> Optional[str]:
    """Текст ошибки загрузки (для диагностики в логе), либо None."""
    _load()
    return _state["error"]


def radiant_probability(heroes: Optional[Sequence[int]]) -> Optional[float]:
    """P(победы Radiant) по early-win модели, либо None если оценить нечем.

    `heroes` — готовый вектор из десяти hero_id: radiant pos1-5, затем dire
    pos1-5. Строит его `win_model_veto._heroes_vector`, он же единственный
    источник порядка.

    Вход не доверяем: не итерируемое, не числа, не та длина, неположительный
    hero_id — всё это None. Функцию зовут из `_prematch_index`, который решает
    ставку, и исключение отсюда стоило бы боевой оценки ради строки в карточке.
    """
    if not ENABLED or heroes is None:
        return None
    try:
        heroes = tuple(int(h) for h in heroes)
    except (TypeError, ValueError):                  # не итерируемое или не числа
        return None
    if len(heroes) != 10 or any(h <= 0 for h in heroes):
        return None
    if heroes in _cache:
        return _cache[heroes]
    value: Optional[float] = None
    if _load():
        try:
            import numpy as np
            matrix = _state["encoder"].transform(np.asarray([heroes], dtype=np.int64))
            value = round(float(_state["model"].predict_proba(matrix)[0, 1]), 6)
        except Exception:                            # noqa: BLE001
            value = None
    if len(_cache) >= _CACHE_LIMIT:
        _cache.clear()
    _cache[heroes] = value
    return value


def early_win_index(heroes: Optional[Sequence[int]]) -> Optional[float]:
    """(P(radiant) − 0.5) × 100 — та же шкала «в пользу стороны», что у общей модели."""
    probability = radiant_probability(heroes)
    return None if probability is None else round((probability - 0.5) * 100.0, 3)


def verdict(heroes: Optional[Sequence[int]]) -> Optional[dict]:
    """`{side, probability, confidence}` — сторона и её вероятность, либо None.

    `confidence` — уверенность в НАЗВАННОЙ стороне, то есть max(p, 1−p): та же
    величина, по которой в проде считаются полосы 60/65/70/…
    """
    probability = radiant_probability(heroes)
    if probability is None:
        return None
    radiant = probability >= 0.5
    return {"side": "Radiant" if radiant else "Dire",
            "probability": probability,
            "confidence": probability if radiant else 1.0 - probability}


def panel_line(heroes: Optional[Sequence[int]]) -> Optional[str]:
    """Строка для панели: «Early Win ML-модель: Dire 56.0%». None — строку не печатать."""
    result = verdict(heroes)
    if result is None:
        return None
    return f"Early Win ML-модель: {result['side']} {result['confidence'] * 100:.1f}%"
