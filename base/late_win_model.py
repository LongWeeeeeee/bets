"""Late-модель победы: оценка стороны для карт, которые уходят в лейт.

Отдельная от общей win-модели, потому что в затянувшихся играх драфт работает
иначе. Обучена на паблик-корпусе, отфильтрованном по `duration >= 36 минут` —
это тот же `LATE_MIN_DURATION`, по которому собирается late-словарь
(`base/analise_database.py`). Признаки — ТОЛЬКО десять hero_id по позициям,
дизайн `hero_role_pair` signed. Живого состояния, нетворта, ELO и игроков здесь
нет намеренно: нетворт в late-схеме это ГЕЙТ (когда спрашивать), а не признак.

**Зачем** (E-240, 5 093 540 паблик-карт, late-популяция 3 112 619). На картах,
доживших до 31-й минуты с ровным состоянием:

* общая модель — AUC 0.6003, и она завышает винрейт на 3.7-5.3 п.п.:
  на полосе уверенности 70 заявляет 74.0%, а даёт 68.8%;
* late-модель — AUC 0.6211, на той же полосе даёт 72.2%;
* перекалибровка общей модели этого НЕ чинит: при совпадающем охвате она не
  добавляет ничего, а честности добивается втрое меньшим потоком (72.4% на
  6 466 картах против 72.2% на 19 141 у late-модели).

Механизм: вклад героя в победу зависит от длительности. Разброс сдвига WR между
короткими и длинными картами — sd 6.5 п.п., от −16.9 (Lycan) до +18.3 п.п.
(Faceless Void), у 51 из 127 героев сдвиг больше 5 п.п.

**Отказ — всегда молчаливый.** Нет файлов, не встал sklearn, незнакомый герой,
любая ошибка — None, и строка в панели просто не печатается. Ломать панель
из-за необязательной оценки недопустимо.
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
    "LATE_WIN_MODEL_DIR",
    str(PROJECT_ROOT / "data/late_draft_win/2026-08-29_public_5m_late36"),
))
# Отключается без деплоя: LATE_WIN_MODEL_ENABLED=0.
ENABLED = os.getenv("LATE_WIN_MODEL_ENABLED", "1") == "1"

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
            encoder = joblib.load(MODEL_DIR / "late_win_feature_encoder.joblib")
            model = joblib.load(MODEL_DIR / "late_win_model.joblib")
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
    """P(победы Radiant) по late-модели, либо None если оценить нечем.

    `heroes` — готовый вектор из десяти hero_id: radiant pos1-5, затем dire
    pos1-5. Строит его `win_model_veto._heroes_vector`, он же единственный
    источник порядка.
    """
    if not ENABLED or heroes is None:
        return None
    heroes = tuple(heroes)
    if len(heroes) != 10:
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


def late_index(heroes: Optional[Sequence[int]]) -> Optional[float]:
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
    """Строка для панели: «Late ML-модель: Dire 56.0%». None — строку не печатать."""
    result = verdict(heroes)
    if result is None:
        return None
    return f"Late ML-модель: {result['side']} {result['confidence'] * 100:.1f}%"
