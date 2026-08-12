"""Драфтовая ML-оценка победы: индекс в пользу стороны + вето для STAR-блоков.

Модель — `radiant_win_model.joblib` из набора `public_draft_hero10_experiment`
(дизайн hero_role_pair, 10 признаков-героев по позициям, AUC 0.610 / точность
58.2% на паблик-тесте 296 768 карт). Обучена на паблике, применяется к любому
драфту: живых данных не требует, доступна сразу после пиков.

Индекс = (P(radiant) − 0.5) × 100, то есть та же шкала «в пользу стороны», что у
драфтовых метрик: положительный — за Radiant, отрицательный — за Dire.

**Зачем.** Замер на дедуплицированном про-корпусе (E-73): блоки, чей знак
противоположен знаку модели, выигрывают заметно ниже своей базы —
post_lane 42.0% против 55.5%, early_win 52.5% против 62.7%, late 53.9% против
59.0%, early_nw 62.0% против 67.4%. Отсюда вето: сигнал против модели не идёт.

**Отказ — всегда в сторону РАЗРЕШЕНИЯ.** Нет файлов модели, не встал sklearn,
незнакомый герой, любая ошибка — возвращается None, и вето не срабатывает.
Молча блокировать весь отбор из-за сломанной модели недопустимо.
"""
from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODEL_DIR = Path(os.getenv(
    "WIN_MODEL_DIR",
    str(PROJECT_ROOT / "data/public_draft_hero10_experiment/2026-08-04_all_public_v5_serv1_full"),
))
# Вето включено по умолчанию; выключается WIN_MODEL_VETO_ENABLED=0 без деплоя.
VETO_ENABLED = os.getenv("WIN_MODEL_VETO_ENABLED", "1") == "1"

# Минимальный модуль индекса, при котором несогласие модели блокирует блок.
# Порог СВОЙ У КАЖДОГО БЛОКА и выбран по правилу «понижать, пока отсекается
# мусор»: сетка от 15 до 0 на про-корпусе, и берётся самая низкая отсечка, где
# заветованные карты ещё выигрывают ощутимо ниже безубытка (E-73).
#
#   early_output      10  — ниже 10 вето начинает резать ПРИБЫЛЬНЫЕ карты
#                           (56-62%), при 10 отсекается 54.5% на 165 картах
#   early_end_output   5  — до 5 отсекается 45-49%, с 4 уже около нуля
#   mid_output         9  — до 9 отсекается 42-49%, с 8 уже 51%+
#   all_output         0  — заветованные не поднимаются выше 42% ни на одной
#                           отсечке, поэтому режем любое несогласие
_DEFAULT_MIN_INDEX = {
    "early_output": 10.0,
    "early_end_output": 5.0,
    "mid_output": 9.0,
    "all_output": 0.0,
    "post_lane_output": 0.0,
}


def _min_index_for(section: str, source: str = "") -> float:
    """Порог секции: env `WIN_MODEL_VETO_MIN_<SECTION>` перекрывает значение по умолчанию.

    У предматчевой модели порог ОДИН на все секции. Перекалибровка методом E-73
    на 3 776-3 837 сигналах показала, что блоки перестали различаться: при
    |индекс| >= 8 заветованные карты берут 25.9-27.7% в early_nw, early_win,
    late и all — разброс меньше двух пунктов. Старая таблица 10/5/9/0 описывала
    паблик-драфтовую модель, где секции расходились, и к новой шкале отношения
    не имеет.
    """
    name = str(section or "")
    override = os.getenv(f"WIN_MODEL_VETO_MIN_{name.upper()}")
    if override is None:
        override = os.getenv("WIN_MODEL_VETO_MIN_INDEX")
    if override is not None:
        try:
            return float(override)
        except (TypeError, ValueError):
            pass
    if str(source or "") == SOURCE_PREMATCH:
        return _PREMATCH_MIN_INDEX
    return _DEFAULT_MIN_INDEX.get(name, 0.0)
# Ключ, под которым индекс кладётся в блоки вывода метрик.
INDEX_KEY = "ml_win_index"
# Источник индекса кладётся рядом: шкалы двух моделей РАЗНЫЕ, и пороги E-73
# (10/5/9/0) калибровались под паблик-драфтовую. Смешивать их нельзя.
SOURCE_KEY = "ml_win_index_src"
SOURCE_PREMATCH = "prematch"
SOURCE_DRAFT = "draft"
# Порог предматчевой модели: |индекс| >= 8 -> вето на ставку против неё.
# Замер на 2 456 настоящих LAN-карт (forward-окна): при >=8 модель берёт 70.0%
# на 72% потока, ставка ПРОТИВ неё окупалась бы только при кэфе выше 3.3.
_PREMATCH_MIN_INDEX = float(os.getenv("WIN_MODEL_VETO_PREMATCH_MIN", "8"))

_lock = threading.Lock()
_state: dict[str, Any] = {"loaded": False, "encoder": None, "model": None, "error": None}
_cache: dict[tuple, Optional[float]] = {}
_CACHE_LIMIT = 4096


def _load() -> bool:
    if _state["loaded"]:
        return _state["model"] is not None
    with _lock:
        if _state["loaded"]:
            return _state["model"] is not None
        try:
            import joblib  # локальный импорт: модуль обязан импортироваться без sklearn
            encoder = joblib.load(MODEL_DIR / "win_feature_encoder.joblib")
            model = joblib.load(MODEL_DIR / "radiant_win_model.joblib")
            _state.update(encoder=encoder, model=model, error=None)
        except Exception as exc:                     # noqa: BLE001 — любая поломка = нет вето
            _state.update(encoder=None, model=None, error=f"{type(exc).__name__}: {exc}")
        _state["loaded"] = True
    return _state["model"] is not None


def load_error() -> Optional[str]:
    """Текст ошибки загрузки (для диагностики в логе), либо None."""
    _load()
    return _state["error"]


def _heroes_vector(radiant_heroes_and_pos, dire_heroes_and_pos) -> Optional[tuple]:
    out = []
    for side in (radiant_heroes_and_pos, dire_heroes_and_pos):
        if not isinstance(side, dict):
            return None
        for i in range(1, 6):
            entry = side.get(f"pos{i}")
            hero = entry.get("hero_id") if isinstance(entry, dict) else None
            try:
                hero = int(hero)
            except (TypeError, ValueError):
                return None
            if hero <= 0:
                return None
            out.append(hero)
    return tuple(out)


def _prematch_index(radiant_heroes_and_pos, dire_heroes_and_pos) -> Optional[float]:
    """Индекс предматчевой модели или None, если данных не хватает.

    Аккаунты берутся из тех же диктов позиций — они там уже есть. Организация
    опознаётся скорером по составу, поэтому team_id не нужен. Любой отказ
    (неизвестный игрок, протухший снимок, сломанные позиции) даёт None: молча
    подставлять дефолты нельзя, это и есть источник вранья.
    """
    try:
        from base import prematch_scorer as ps
    except Exception:                                # noqa: BLE001
        try:
            import prematch_scorer as ps            # запуск из base/
        except Exception:                            # noqa: BLE001
            return None
    try:
        def slots(d):
            acc, her = [], []
            for i in range(1, 6):
                e = (d or {}).get(i) or (d or {}).get(str(i)) or {}
                acc.append(int(e.get("account_id") or 0))
                her.append(int(e.get("hero_id") or 0))
            return acc, her
        ra, rh = slots(radiant_heroes_and_pos)
        da, dh = slots(dire_heroes_and_pos)
        if min(rh + dh) <= 0:
            return None
        draft = win_index_draft(radiant_heroes_and_pos, dire_heroes_and_pos)
        if draft is None:
            return None
        p_draft = draft / 100.0 + 0.5
        import math
        logit = math.log(max(p_draft, 1e-6) / max(1.0 - p_draft, 1e-6))
        model = ps.get_model()
        res = model.score(radiant_accounts=ra, dire_accounts=da,
                          radiant_heroes=rh, dire_heroes=dh,
                          radiant_team_id=0, dire_team_id=0,
                          draft_logit=logit, strictness="accounts")
        return round((res.probability - 0.5) * 100.0, 3)
    except Exception:                                # noqa: BLE001
        return None


def win_index_ex(radiant_heroes_and_pos, dire_heroes_and_pos):
    """(индекс, источник). Предматчевая модель приоритетна, драфтовая — запасная."""
    value = _prematch_index(radiant_heroes_and_pos, dire_heroes_and_pos)
    if value is not None:
        return value, SOURCE_PREMATCH
    return win_index_draft(radiant_heroes_and_pos, dire_heroes_and_pos), SOURCE_DRAFT


def win_index(radiant_heroes_and_pos, dire_heroes_and_pos) -> Optional[float]:
    """Совместимость: возвращает индекс без источника."""
    return win_index_ex(radiant_heroes_and_pos, dire_heroes_and_pos)[0]


def win_index_draft(radiant_heroes_and_pos, dire_heroes_and_pos) -> Optional[float]:
    """(P(radiant) − 0.5) × 100 либо None, если оценить нечем."""
    heroes = _heroes_vector(radiant_heroes_and_pos, dire_heroes_and_pos)
    if heroes is None:
        return None
    cached = _cache.get(heroes)
    if cached is not None or heroes in _cache:
        return cached
    value: Optional[float] = None
    if _load():
        try:
            import numpy as np
            matrix = _state["encoder"].transform(np.asarray([heroes], dtype=np.int64))
            probability = float(_state["model"].predict_proba(matrix)[0, 1])
            value = round((probability - 0.5) * 100.0, 3)
        except Exception:                            # noqa: BLE001
            value = None
    if len(_cache) >= _CACHE_LIMIT:
        _cache.clear()
    _cache[heroes] = value
    return value


def blocks_veto(block_sign: Any, block: Any, section: str = "") -> bool:
    """True, если знак блока противоречит модели и блок надо отменить.

    `block_sign` — +1 за Radiant, −1 за Dire. Индекс берётся из самого блока
    (кладётся туда `synergy_and_counterpick`), поэтому функция не требует драфта
    и одинаково работает и в `format_output_dict`, и в диагностике блоков.
    Порог несогласия — свой у каждой секции, см. `_DEFAULT_MIN_INDEX`.
    """
    if not VETO_ENABLED or block_sign not in (1, -1, 1.0, -1.0):
        return False
    if not isinstance(block, dict):
        return False
    raw = block.get(INDEX_KEY)
    try:
        index = float(raw)
    except (TypeError, ValueError):
        return False
    if abs(index) < _min_index_for(section, block.get(SOURCE_KEY)):
        return False
    model_sign = 1 if index > 0 else (-1 if index < 0 else 0)
    if model_sign == 0:
        return False
    return int(block_sign) != model_sign


def model_bet(*blocks) -> Optional[dict]:
    """Сторона и цена самостоятельной ставки по предматчевой модели.

    Возвращает None, пока модель не набрала |индекс| >= порога, и словарь
    `{side, index, confidence, min_odds, expected_wr}` когда набрала. Ставка
    идёт САМА ПО СЕБЕ, без звёзд: замер на 2 456 настоящих LAN-картах даёт
    1 769 карт (72% потока) с винрейтом 70.0% при |индекс| >= 8.

    Драфтовый источник сюда не пускается: у него другая шкала и другой винрейт,
    порог 8 на ней не проверялся.
    """
    index = None
    for block in blocks:
        if not isinstance(block, dict):
            continue
        if str(block.get(SOURCE_KEY) or "") != SOURCE_PREMATCH:
            continue
        try:
            index = float(block.get(INDEX_KEY))
        except (TypeError, ValueError):
            continue
        break
    if index is None or abs(index) < _PREMATCH_MIN_INDEX:
        return None
    confidence = (50.0 + abs(index)) / 100.0
    try:
        from base import prematch_scorer as ps
    except Exception:                                # noqa: BLE001
        try:
            import prematch_scorer as ps            # запуск из base/
        except Exception:                            # noqa: BLE001
            return None
    return {
        "side": "radiant" if index > 0 else "dire",
        "index": index,
        "confidence": confidence,
        "min_odds": ps.lan_min_odds(confidence),
        "expected_wr": ps.lan_expected_wr(confidence),
    }
