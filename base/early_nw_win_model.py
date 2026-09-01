"""Early-NW модель: сторона раннего перевеса по нетворту, по одному драфту.

Отвечает НЕ на тот вопрос, что общая и late-модели. Те предсказывают победу
карты, эта — КТО ВОЗЬМЁТ РАННИЙ ПЕРЕВЕС: сторону маркера, по которому
собирается словарь `early_dict`. Маркер — первое пересечение растущего порога
(6000 на 20-й минуте -> 8500 на 28-й, по группам алхимика) в окне 20-28 минут;
популяция и метка в обучении построены продовой `is_early_nw_match`
(`base/analise_database.py:999`), а не копией правил.

Признаки — ТОЛЬКО десять hero_id по позициям, дизайн `hero_role_pair` signed,
как у late-модели (E-240). Живого состояния, нетворта, ELO и игроков здесь нет:
нетворт в early-схеме это ЦЕЛЬ, и подавать его на вход нельзя.

Обучена на паблик-корпусе — том же, по которому собирается сам словарь.
Боевой артефакт зафичен на ВСЕЙ популяции без отложенного теста (запрос alex);
честные метрики на хронологическом сплите лежат в `results.json` рядом с ним.

Почему отдельная модель, а не общая: E-242 намерила, что на ПРО-корпусе и на
двадцати предматчевых признаках «кто возьмёт ранний перевес» и «кто выиграет
карту» — одна величина, и отдельная модель не добавила ничего. Здесь другой
дизайн (паблик, только драфт), тот же, на котором late-модель прибавку дала.

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
# состоянием и кэшами. Та же оговорка стоит в late_win_model.py.

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODEL_DIR = Path(os.getenv(
    "EARLY_NW_MODEL_DIR",
    str(PROJECT_ROOT / "data/early_nw_draft/2026-09-01_public_early_nw"),
))
# Отключается без деплоя: EARLY_NW_MODEL_ENABLED=0.
ENABLED = os.getenv("EARLY_NW_MODEL_ENABLED", "1") == "1"

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
            encoder = joblib.load(MODEL_DIR / "early_nw_feature_encoder.joblib")
            model = joblib.load(MODEL_DIR / "early_nw_model.joblib")
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
    """P(ранний перевес возьмёт Radiant), либо None если оценить нечем.

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


def early_nw_index(heroes: Optional[Sequence[int]]) -> Optional[float]:
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
    """Строка для панели: «Early NW ML-модель: Dire 61.0%». None — строку не печатать."""
    result = verdict(heroes)
    if result is None:
        return None
    return f"Early NW ML-модель: {result['side']} {result['confidence'] * 100:.1f}%"
