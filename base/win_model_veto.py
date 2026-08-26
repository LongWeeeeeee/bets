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

import math as _math
import os
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODEL_DIR = Path(os.getenv(
    "WIN_MODEL_DIR",
    str(PROJECT_ROOT / "data/public_draft_hero10_experiment/2026-08-15_all_public_5m_full"),
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
# Ветки лестницы, которым разрешена САМОСТОЯТЕЛЬНАЯ ставка. Порог 8 проверялся
# на полной модели (E-142: 1 769 карт, 72% потока, винрейт 70.0%); для коротких
# веток такой проверки нет, а покрытие с лестницей растёт с 42.5% до 99.5% — то
# есть поток ставок вырос бы вдвое с лишним на непроверенном. Остальные ветки
# отдают вердикт и панель, но не ставку. Расширять список — по накопленному ROI.
_PREMATCH_BET_BRANCHES = tuple(
    x.strip() for x in os.getenv("WIN_MODEL_VETO_PREMATCH_BRANCHES",
                                 "full,no_org").split(",") if x.strip())

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


# --- Hybrid-рейтинг для предматчевой модели -----------------------------------
# Колонка `hybrid_strength` артефакта равна (сила радианта − сила дайра)/400, где
# сила — это `team_strength` из `_build_team_context`. Ту же функцию зовут и
# `predict_match` (обучение), и `preview_team_strength` (бой), поэтому величины
# совпадают при совпадении АРГУМЕНТОВ. Порядок игроков — по возрастанию
# account_id: так их складывает `_extract_player_slots` в ELO/data_loader, и
# иначе поедут ролевые рейтинги.
_HYBRID_TIER_DEFAULT = "TIER3"
_HYBRID_MISS = 0
_STALE_WARNED = 0
# Заполненность входа: сколько из 21 величины собрано из реальных данных, а не
# заменено нулём. Отказ бросается только на семь причин, отсутствующая ячейка
# (игрок, герой) молча становится нулём — так же, как в обучении. Замер 15.08:
# полный вход лишь у 17% карт, в среднем занулено 3.29 слота из 10 (E-195).
_COV_N = 0
_COV_SUM = 0.0
# Заполненность входа последней оценки, чтобы показать её В КАРТОЧКЕ рядом с
# процентом модели. Держим вместе с индексом: если индекс в карточке другой,
# значит оценка была не эта, и печатать чужое число нельзя.
_LAST_FILL = {"index": None, "fill": None, "elo": None,
               "draft_rank": None, "draft_share": None,
               "branch": None, "wr": None, "parts": None}
#: Разложения ПРОШЛЫХ оценок, по индексу. Одной записи мало: карточка
#: отложенного матча строится не в момент оценки, а когда до него дойдёт
#: очередь, и к тому времени `_LAST_FILL` уже принадлежит другой карте. Тогда
#: сравнение индексов не сходилось и хвост строки — `ELO модели`, `вклад`,
#: `вход` — молча пропадал (25.08.2026: в логе `p=0.950`, а печаталась карта
#: с 52.1%). Держим небольшую историю: карт в очереди единицы.
_FILL_HISTORY: "OrderedDict[float, dict]" = OrderedDict()
_FILL_HISTORY_MAX = 32


def _remember_fill() -> None:
    """Сложить текущее разложение в историю по его индексу."""
    idx = _LAST_FILL.get("index")
    if idx is None:
        return
    try:
        key = round(float(idx), 6)
    except (TypeError, ValueError):
        return
    _FILL_HISTORY[key] = dict(_LAST_FILL)
    _FILL_HISTORY.move_to_end(key)
    while len(_FILL_HISTORY) > _FILL_HISTORY_MAX:
        _FILL_HISTORY.popitem(last=False)


def _fill_for(index) -> dict:
    """Разложение для ЭТОГО индекса: текущее, иначе из истории. {} — нет."""
    try:
        value = float(index)
    except (TypeError, ValueError):
        return {}
    cur = _LAST_FILL.get("index")
    if cur is not None:
        try:
            if abs(value - float(cur)) < 1e-6:
                return _LAST_FILL
        except (TypeError, ValueError):
            pass
    return _FILL_HISTORY.get(round(value, 6)) or {}
_DRAFT_FIRST_ONLY = str(os.getenv("WIN_MODEL_DRAFT_FIRST_ONLY", "0")).strip() in ("1", "true", "yes", "on")
# Запрет «драфт против стороны ставки». Включён по умолчанию, снимается
# WIN_MODEL_DRAFT_AGAINST_BLOCK=0 без деплоя.
_DRAFT_AGAINST_BLOCK = str(os.getenv("WIN_MODEL_DRAFT_AGAINST_BLOCK", "1")).strip() in ("1", "true", "yes", "on")
# Тот же запрет на STAR-пути: звёздный блок, чья сторона названа вопреки
# драфтовому компоненту модели, ставки не даёт. До 25.08.2026 запрет стоял
# только в `model_bet`, а `blocks_veto` сверял знак блока с вердиктом ВСЕЙ
# модели и про компонент «драфт» не знал — карта RE ARISE 25.08 прошла именно
# так: ставка модели была отменена, star-ставка на ту же сторону ушла.
# Замера у правила нет (E-238: на 49 таких ставках винрейт 0.735, из них 47
# онлайн, на LAN одна) — оно введено решением alex. Снимается
# WIN_MODEL_STAR_DRAFT_BLOCK=0 без деплоя.
_STAR_DRAFT_BLOCK = str(os.getenv("WIN_MODEL_STAR_DRAFT_BLOCK", "1")).strip() in ("1", "true", "yes", "on")
#: Отказы star-запрета печатаются по одному разу на (индекс, сторона).
_STAR_DRAFT_SEEN: set = set()
_STAR_DRAFT_MUTE = False
#: Отказ печатается по одному разу на индекс: `model_bet` зовётся на
#: каждом тике диспетчера, и строка на вызов залила бы лог.
_DRAFT_GATE_SEEN: set = set()
_DRAFT_GATE_MUTE = False
# Чисто драфтовые: считаются только по десяти hero_id (и позициям).
_DRAFT_FEATURES = frozenset({
    "draft_logit", "vs_wr", "cp_lane", "syn_pos_mean", "wr30", "farm_dep",
    "draft_logit_x_elo_gap", "draft_logit_x_games_exp",
})
# Игрок НА ГЕРОЕ — смешанные, в долю драфта не входят.
_HERO_PLAYER_FEATURES = frozenset({
    "hero_games", "hero_gpm_rel", "lh_rel_hero", "a_hdmg_rel_hero",
})
# Путь боевой, но вычисляемый: абсолютная константа `/root/main/...` не
# существует нигде, кроме serv1, а тесты писали прямо в БОЕВОЙ журнал — 23.08.2026
# прогон набора добавил туда четыре записи с пустыми именами команд и причиной
# «не передан hybrid_strength». Журнал диагностический, но по нему судят о
# работе модели, и чужие строки в нём — ложь.
_EVAL_JOURNAL = os.getenv(
    "PREMATCH_EVAL_JOURNAL",
    str(Path(__file__).resolve().parent.parent / "runtime" / "prematch_model_eval.jsonl"),
)
# Панель окон: последний посчитанный набор вердиктов и готовая строка для
# сообщения. Живёт в модуле, потому что считается там, где есть входы, а
# показывается там, где строится карточка.
_LAST_PANEL: dict = {"text": "", "verdicts": [], "error": None, "map_id": None}


def last_panel_text() -> str:
    """Готовый блок ML для карточки. Пустая строка — панели нет."""
    return str(_LAST_PANEL.get("text") or "")


def last_panel_verdicts() -> list:
    """Вердикты панели для журнала."""
    return list(_LAST_PANEL.get("verdicts") or [])


def last_panel_error() -> str:
    return str(_LAST_PANEL.get("error") or "")


# Причина молчания панели, напечатанная ОДИН раз за запуск. `last_panel_error`
# существовал с самого начала и НИ РАЗУ никем не читался — 24.08.2026 панель
# молчала больше часа после выкатки, и по логу это было неотличимо от «панели
# нет в этой сборке»: отказ ложился в словарь, откуда его никто не доставал.
# Пустой список вердиктов сюда попадает наравне с исключением: `evaluate_map`
# возвращает `[]` и когда выключен `ML_PANEL_ENABLED`, и когда бандл не собрался,
# то есть тишина в обоих случаях выглядит одинаково.
_PANEL_SILENCE_REPORTED = False


def _report_panel_silence(reason: str) -> None:
    global _PANEL_SILENCE_REPORTED
    if _PANEL_SILENCE_REPORTED:
        return
    _PANEL_SILENCE_REPORTED = True
    print(f"[win_model] панель молчит: {reason}", flush=True)
_EVAL_SEEN: set = set()


def _journal_eval(**rec) -> None:
    """Одна строка на оценку. Дедуп в памяти: за матч снимок не меняется."""
    try:
        key = (rec.get("radiant_team"), rec.get("dire_team"),
               round(float(rec.get("index") or 0.0), 2), rec.get("reason"))
        if key in _EVAL_SEEN:
            return
        if len(_EVAL_SEEN) > 20000:
            _EVAL_SEEN.clear()
        _EVAL_SEEN.add(key)
        import json as _json, time as _time
        rec["ts"] = int(_time.time())
        with open(_EVAL_JOURNAL, "a", encoding="utf-8") as f:
            f.write(_json.dumps(rec, ensure_ascii=False) + chr(10))
    except Exception:                                # noqa: BLE001
        pass


def last_model_elo(index):
    """Разность рейтингов В ОЧКАХ, какой её видит МОДЕЛЬ.

    На карточке печатается живой ELO по составу (ELO/live_team_strength.py), а
    модель смотрит на свой рейтинг игроков из pro_features_wide (K-обновление
    по про-корпусу, колонка хранится делённой на 400). Это разные системы, и
    они расходятся вплоть до знака: 15.08 на карте Xtreme vs Resilience карточка
    показывала −16, а модель видела +132. Оператор из-за этого читал «модель
    спорит с рейтингом», хотя она с ним согласна — просто с другим.
    """
    try:
        _rec = _fill_for(index)
        if _rec:
            v = _rec["elo"]
            return None if v is None else float(v) * 400.0
    except (TypeError, ValueError):
        pass
    return None


def last_parts(index):
    """Разложение логита по компонентам для ЭТОГО индекса; пусто — если нет."""
    try:
        _rec = _fill_for(index)
        if _rec:
            return dict(_rec.get("parts") or {})
    except (TypeError, ValueError):
        pass
    return {}


def last_draft_rank(index):
    """Место драфта среди признаков, тянущих в сторону ставки, и его вклад."""
    try:
        _rec = _fill_for(index)
        if _rec:
            s = _rec.get("draft_share")
            return (_rec.get("draft_rank"), s) if s is not None else None
    except (TypeError, ValueError):
        pass
    return None


def _draft_agrees(index) -> bool:
    """False, только когда драфт ДОКАЗАНО тянет против стороны ставки.

    `parts` — точное разложение логита по компонентам (E-213): сумма частей
    равна самому логиту, знак стоит в ориентации Radiant, и к стороне ставки
    его приводит знак индекса.

    Ноль и отсутствие ключа `draft` (ветка `pre_draft` считается вовсе без
    драфтовых колонок) — это «драфту нечего сказать», а не возражение. E-217 §1:
    карты, где драфт молчит, дают ЛУЧШИЙ винрейт 0.719, резать их нечем.

    Разложения нет вовсе — запрет стоит в стороне, но не молчит: без лестницы
    веток он не сработал бы ни разу, и узнать об этом было бы неоткуда.
    """
    global _DRAFT_GATE_MUTE
    parts = last_parts(index)
    if not parts:
        if not _DRAFT_GATE_MUTE:
            _DRAFT_GATE_MUTE = True
            print("ВНИМАНИЕ: запрет «драфт против стороны» проверять нечем — "
                  "модель не даёт разложения логита по компонентам", flush=True)
        return True
    try:
        side_sign = 1.0 if float(index) > 0 else -1.0
        toward_bet = float(parts.get("draft", 0.0)) * side_sign
    except (TypeError, ValueError):                  # noqa: BLE001
        return True
    if toward_bet >= 0.0:
        return True
    key = round(float(index), 2)
    if key not in _DRAFT_GATE_SEEN:
        if len(_DRAFT_GATE_SEEN) > 20000:
            _DRAFT_GATE_SEEN.clear()
        _DRAFT_GATE_SEEN.add(key)
        print(f"[win_model] ставка отменена: драфт против стороны "
              f"({'radiant' if index > 0 else 'dire'}, индекс {index:+.2f}, "
              f"вклад драфта {parts['draft']:+.4f} в ориентации Radiant)",
              flush=True)
    return False


def last_fill(index):
    """Заполненность входа для этого индекса или None, если он не тот."""
    try:
        _rec = _fill_for(index)
        if _rec:
            return _rec["fill"]
    except (TypeError, ValueError):
        pass
    return None
# сутки, после которых снимок считается мёртвым (замер E-177)
_SNAPSHOT_MAX_AGE_DAYS = 30.0
# сутки, после которых устаревание уже видно в качестве и его пора чинить
_SNAPSHOT_WARN_DAYS = 3.0


#: Печатать про массивную модель нужно ровно один раз: путь чтения зовётся на
#: каждой карте, и строка на вызов залила бы лог.
_ARRAY_MODEL_REPORTED = False


def _report_array_model(ok: bool, detail: str) -> None:
    """Сообщить, какая модель обслуживает путь ставки.

    Отказ раньше гасился молча: процесс просто начинал есть на гигабайт больше,
    а узнать об этом было неоткуда.
    """
    global _ARRAY_MODEL_REPORTED
    if _ARRAY_MODEL_REPORTED:
        return
    _ARRAY_MODEL_REPORTED = True
    if ok:
        print("[ELO] путь ставки на МАССИВНОЙ модели (около 340 МБ)", flush=True)
    else:
        print(f"ВНИМАНИЕ: массивная модель не поднялась ({detail}); путь ставки "
              "откатился на словарную, это примерно +1.3 ГБ памяти", flush=True)


def _live_elo_model():
    """Модель из снимка, который живой пайплайн уже держит в памяти, либо None.

    Свой `json.load` того же файла запрещён: на проде это вторая копия 349-МБ
    структуры, RSS 4 ГБ и main-thread в D-state на десятки минут.
    """
    import sys as _sys

    module = _sys.modules.get("live_team_strength") or _sys.modules.get("ELO.live_team_strength")
    if module is None:
        # importlib вернёт УЖЕ загруженный модуль, если он есть в sys.modules под
        # другим именем; свежий импорт возможен только когда копии в памяти нет
        # вовсе, так что второй копии 349-МБ снимка не появится.
        try:
            import importlib

            module = importlib.import_module("ELO.live_team_strength")
        except Exception:  # noqa: BLE001
            return None
    # Сначала пробуем МАССИВНУЮ модель: она читает то же состояние, но держит
    # его в numpy, а не в словарях — 337 МБ против 1116. Числа совпадают
    # побитово (сверка 1 800 вызовов на трёх тирах). Мутировать она не умеет и
    # не должна: этот путь только читает силу состава.
    try:
        from ELO.array_model import load_read_model
        from ELO.live_team_strength import (DEFAULT_RUNTIME_MODEL_STATE_PATH,
                                            DEFAULT_SNAPSHOT_PATH)

        fast = load_read_model(DEFAULT_SNAPSHOT_PATH,
                               DEFAULT_RUNTIME_MODEL_STATE_PATH)
        if fast is not None:
            _report_array_model(True, "")
            return fast
        _report_array_model(False, "модель не построилась")
    except Exception as _exc:  # noqa: BLE001
        _report_array_model(False, f"{type(_exc).__name__}: {_exc}")

    # `load_live_snapshot` подмешивает рантайм-состояние к базовому снимку.
    # Базовый пересобирается редко (12.08 на момент правки), рантайм обновляется
    # постоянно (20.08 19:22), и без мержа `hybrid_strength` считался по
    # рейтингам восьмидневной давности. Второй копии снимка не возникает: базовый
    # словарь берётся из того же `_SNAPSHOT_CACHE`, мерженый кэшируется отдельно.
    snapshot = None
    live_loader = getattr(module, "load_live_snapshot", None)
    if live_loader is not None:
        try:
            snapshot = live_loader()
        except Exception:  # noqa: BLE001
            snapshot = None
    if not isinstance(snapshot, dict):
        # Запасной путь — пакет без мержа. Ведёт себя ровно как прежде.
        snapshot = getattr(module, "_SNAPSHOT_CACHE", None)
    if not isinstance(snapshot, dict):
        # снимок ещё не читали в этом процессе. `load_snapshot` кладёт его в тот
        # же модульный кэш `_SNAPSHOT_CACHE`, поэтому копия остаётся одна.
        loader = getattr(module, "load_snapshot", None)
        if loader is None:
            return None
        try:
            snapshot = loader()
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(snapshot, dict):
            return None
    restore = getattr(module, "_restore_model_from_snapshot", None)
    if restore is None:
        return None
    try:
        return restore(snapshot)
    except Exception:  # noqa: BLE001
        return None


def _slots_for_elo(side: Any) -> Optional[tuple]:
    """((account_id, ...), ("POSITION_i", ...)) в порядке возрастания account_id."""
    src = side or {}
    pairs = []
    for i in range(1, 6):
        entry = src.get(f"pos{i}") or src.get(i) or src.get(str(i)) or {}
        if not isinstance(entry, dict):
            return None
        try:
            account = int(entry.get("account_id") or 0)
        except (TypeError, ValueError):
            return None
        if account <= 0:
            return None
        pairs.append((account, f"POSITION_{i}"))
    if len({a for a, _ in pairs}) != 5:
        return None
    pairs.sort()
    return tuple(a for a, _ in pairs), tuple(p for _, p in pairs)


def hybrid_strength_diff(radiant_heroes_and_pos, dire_heroes_and_pos,
                         radiant_team_name, dire_team_name,
                         timestamp: Optional[int] = None) -> Optional[float]:
    """(сила радианта − сила дайра)/400 по Hybrid-рейтингу либо None."""
    if not radiant_team_name or not dire_team_name:
        return None
    model = _live_elo_model()
    if model is None:
        return None
    rad = _slots_for_elo(radiant_heroes_and_pos)
    dire = _slots_for_elo(dire_heroes_and_pos)
    if rad is None or dire is None:
        return None
    try:
        from ELO.domain import LeagueTier
    except Exception:  # noqa: BLE001
        return None
    tier = getattr(LeagueTier, _HYBRID_TIER_DEFAULT, None)
    if tier is None:
        return None
    ts = int(timestamp or time.time())
    try:
        out = []
        for name, (ids, positions) in ((str(radiant_team_name), rad), (str(dire_team_name), dire)):
            preview = model.preview_team_strength(
                team_id=None, team_name=name, player_ids=ids,
                player_positions=positions, tier=tier, timestamp=max(ts, 1),
            )
            out.append(float(preview["team_strength"]))
    except Exception:  # noqa: BLE001
        return None
    return (out[0] - out[1]) / 400.0


def _prematch_index(radiant_heroes_and_pos, dire_heroes_and_pos,
                    radiant_team_name=None, dire_team_name=None,
                    match=None) -> Optional[float]:
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
            # Ключи позиций в проде — строки "pos1".."pos5" (см. `_heroes_vector`
            # и сборку словарей в cyberscore_try). Числовые 1..5 и "1".."5"
            # оставлены запасными: на них зовут тесты и ручные проверки.
            acc, her = [], []
            for i in range(1, 6):
                src = d or {}
                e = src.get(f"pos{i}") or src.get(i) or src.get(str(i)) or {}
                if not isinstance(e, dict):
                    e = {}
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
        hybrid = None
        if "hybrid_strength" in getattr(model, "features", ()):
            ts = None
            if isinstance(match, dict):
                try:
                    ts = int(match.get("startDateTime") or 0) or None
                except (TypeError, ValueError):
                    ts = None
            hybrid = hybrid_strength_diff(radiant_heroes_and_pos, dire_heroes_and_pos,
                                          radiant_team_name, dire_team_name, ts)
            if hybrid is None:
                # Молчаливый уход на драфтовую модель — худший исход: предматчевая
                # сильнее, а по логам этого не видно. Печатаем первые разы и
                # дальше каждый сотый, чтобы не залить журнал.
                global _HYBRID_MISS
                _HYBRID_MISS += 1
                if _HYBRID_MISS <= 5 or _HYBRID_MISS % 100 == 0:
                    print(f"[win_model] hybrid_strength недоступен ({_HYBRID_MISS}-й раз): "
                          f"radiant_team_name={radiant_team_name!r} "
                          f"dire_team_name={dire_team_name!r}", flush=True)
        # E-177: раньше `now_ts` не передавался вовсе, и гейт протухания снимка
        # (`if now_ts is not None and max_age_days > 0`) не срабатывал НИКОГДА.
        # Порог выбран по замеру, а не по умолчанию: снимок старше месяца даёт
        # AUC 0.6883 против 0.7313 на свежем, но запасная драфтовая модель — 0.61.
        # Отказ на трёх сутках обменял бы 0.69 на 0.61, то есть был бы вреден;
        # тридцать суток ловят мёртвый конвейер, не выключая рабочую модель.
        _now = int(time.time())
        _age = (_now - int(getattr(model, "snapshot_ts", _now))) / 86400.0
        if _age > _SNAPSHOT_WARN_DAYS:
            global _STALE_WARNED
            _STALE_WARNED += 1
            if _STALE_WARNED <= 3 or _STALE_WARNED % 200 == 0:
                print(f"[win_model] снимок старше {_SNAPSHOT_WARN_DAYS} суток: "
                      f"{_age:.1f} — ночная пересборка не доехала", flush=True)
        res = model.score(radiant_accounts=ra, dire_accounts=da,
                          radiant_heroes=rh, dire_heroes=dh,
                          radiant_team_id=0, dire_team_id=0,
                          draft_logit=logit, hybrid_strength=hybrid,
                          strictness="accounts",
                          now_ts=_now, max_age_days=_SNAPSHOT_MAX_AGE_DAYS)
        # --- панель окон килов: те же входы, что у предматчевой модели ---
        _LAST_PANEL["text"] = ""
        _LAST_PANEL["verdicts"] = []
        try:
            import prematch_panel_live as _panel
            import ml_panel as _mlp

            _vs = _panel.evaluate_map(rh, dh, ra, da,
                                      getattr(res, "features", None),
                                      list(getattr(model, "features", ()) or ()))
            _wins = [v for v in _vs if str(v.key).startswith("w_")]
            _best = _mlp.best_of(_wins)
            _LAST_PANEL["verdicts"] = _vs
            _LAST_PANEL["text"] = _mlp.render(
                _wins, highlight=[_best.key] if _best else [])
            _LAST_PANEL["error"] = None
            _mid = None
            if isinstance(match, dict):
                # `map_key` — запасной идентификатор и последний по приоритету:
                # в режиме sourcetv прод пишет `match_id = series_id`, и он у
                # всех карт серии один, тогда как ключ карты уникален. Без
                # какого-либо id журнал панели бесполезен: 805 записей с пустым
                # `map_id` невозможно сверить с фактическим исходом карты.
                for _k in ("id", "match_id", "map_id", "matchId", "map_key"):
                    if match.get(_k):
                        _mid = match.get(_k)
                        break
            _LAST_PANEL["map_id"] = _mid
            if _vs:
                _mlp.append_journal(_mlp.journal_row(
                    _mid or "", _vs,
                    extra={"radiant_team": str(radiant_team_name or ""),
                           "dire_team": str(dire_team_name or ""),
                           "ts": int(time.time())}))
            else:
                # Пусто — значит `evaluate_map` вышел на раннем возврате.
                # Спрашиваем загрузчик, а не гадаем: выключена панель, не
                # собрался бандл или отвалилась карточка героев — это три
                # разные поломки с одинаковым внешним видом.
                _st = getattr(_panel, "_state", {}) or {}
                _b = _st.get("bundle")
                _report_panel_silence(
                    f"вердиктов нет; ENABLED={getattr(_panel, 'ENABLED', '?')}, "
                    f"bundle={'есть' if _b is not None else 'НЕТ'}, "
                    f"ready={getattr(_b, 'ready', '?')}, "
                    f"ошибка загрузчика={_st.get('error')!r}, "
                    # `last_error` — отказ САМОГО расчёта, отдельный ключ от
                    # ошибки загрузки. Без него видно только «бандл собрался»,
                    # а упасть может любой из блоков после него.
                    f"ошибка расчёта={_st.get('last_error')!r}, "
                    f"словарь окон={_st.get('dict_error')!r}")
        except Exception as _exc:                    # noqa: BLE001
            # Панель не влияет на ставку, но её отказ не должен теряться:
            # «блок просто не появился» — самый неудобный вид поломки.
            _LAST_PANEL["error"] = f"{type(_exc).__name__}: {_exc}"
            _report_panel_silence(_LAST_PANEL["error"])
        _cov = getattr(res, "coverage", None) or {}
        if _cov:
            global _COV_N, _COV_SUM
            _COV_N += 1
            _COV_SUM += float(_cov.get("filled", 1.0))
            # Пульс раз в сто оценок печатается ДАЖЕ при полном входе: иначе
            # отсутствие строк одинаково выглядит и при заполненности 100%, и
            # при сломанном логировании — ровно та ошибка, что дала E-193.
            if _COV_N % 100 == 0:
                print(f"[win_model] пульс: оценок {_COV_N}, средняя "
                      f"заполненность входа {_COV_SUM/_COV_N:.0%}", flush=True)
            if float(_cov.get("filled", 1.0)) < 1.0 and (_COV_N <= 20 or _COV_N % 50 == 0):
                print(f"[win_model] вход заполнен {_cov['filled']:.0%}: ячейки "
                      f"{_cov['cells']*10:.0f}/10, позиции {_cov['pos']*10:.0f}/10, "
                      f"h2h {'есть' if _cov.get('h2h') else 'нет'}; p={res.probability:.3f}; "
                      f"средняя заполненность за {_COV_N} оценок {_COV_SUM/_COV_N:.0%}",
                      flush=True)
        _idx = round((res.probability - 0.5) * 100.0, 3)
        _LAST_FILL["index"] = _idx
        _LAST_FILL["fill"] = (_cov or {}).get("filled")
        _LAST_FILL["elo"] = (getattr(res, "features", None) or {}).get("elo")
        # Ветка лестницы и ЕЁ винрейт. Без этого ставка на короткой ветке
        # оценивалась бы таблицей полной модели, а это разные величины.
        _LAST_FILL["branch"] = str(getattr(res, "branch", "full") or "full")
        # Точное разложение логита по компонентам: сумма частей равна самому
        # логиту, поэтому доли можно печатать как состав решения, а не как оценку.
        _LAST_FILL["parts"] = {str(k): float(v) for k, v in
                               (getattr(res, "parts", None) or {}).items()}
        try:
            _LAST_FILL["wr"] = float(getattr(res, "lan_winrate", float("nan")))
        except (TypeError, ValueError):              # noqa: BLE001
            _LAST_FILL["wr"] = float("nan")
        _f = getattr(res, "features", None) or {}
        # Разложение логита: вклад признака = коэффициент * стандартизованное
        # значение. Усредняем по моделям ансамбля (сейчас модель одна).
        try:
            import numpy as _np
            _x = _np.array([_f.get(k, 0.0) for k in model.features], dtype=float)
            _contrib = _np.zeros(len(_x))
            for _mu, _sd, _co in zip(model.mu, model.sd, model.coef):
                _contrib += ((_x - _mu) / _sd) * _co
            _contrib /= max(len(model.coef), 1)
            if _idx < 0:
                _contrib = -_contrib          # приводим к стороне ставки
            _tot = float(_np.clip(_contrib, 0, None).sum()) or 1.0
            _feat = list(model.features)
            _grp = {"draft": 0.0, "hero_player": 0.0, "player": 0.0}
            for _j, _n in enumerate(_feat):
                _g = ("draft" if _n in _DRAFT_FEATURES else
                      "hero_player" if _n in _HERO_PLAYER_FEATURES else "player")
                _grp[_g] += float(_contrib[_j])
            _LAST_FILL["draft_share"] = round(_grp["draft"] / _tot, 4)
            _LAST_FILL["groups"] = {k: round(v / _tot, 4) for k, v in _grp.items()}
            # ранг оставляем: место группы драфта среди трёх
            _LAST_FILL["draft_rank"] = 1 + sorted(_grp.values(), reverse=True).index(_grp["draft"])
        except Exception:                            # noqa: BLE001
            _LAST_FILL["draft_rank"] = None
            _LAST_FILL["draft_share"] = None
            _LAST_FILL["groups"] = None
        # Разложение собрано целиком — кладём его в историю по индексу. Карточка
        # отложенного матча строится позже, когда `_LAST_FILL` уже чужой.
        _remember_fill()
        _journal_eval(radiant_team=str(radiant_team_name or ""),
                      dire_team=str(dire_team_name or ""),
                      index=_idx, confidence=round(0.5 + abs(_idx) / 100.0, 4),
                      side="radiant" if _idx > 0 else "dire",
                      model_elo=(None if _f.get("elo") is None
                                 else round(float(_f["elo"]) * 400.0, 1)),
                      draft_logit=_f.get("draft_logit"),
                      fill=(_cov or {}).get("filled"),
                      cells=(_cov or {}).get("cells"),
                      h2h=(_cov or {}).get("h2h"),
                      missing_cells=(_cov or {}).get("missing_cells"),
                      draft_share=_LAST_FILL.get("draft_share"),
                      groups=_LAST_FILL.get("groups"),
                      branch=str(getattr(res, "branch", "full") or "full"),
                      parts={k: round(float(v), 4) for k, v in
                             (getattr(res, "parts", None) or {}).items()},
                      wr=(None if _LAST_FILL.get("wr") is None
                          or _LAST_FILL["wr"] != _LAST_FILL["wr"]
                          else round(float(_LAST_FILL["wr"]), 4)),
                      bet=abs(_idx) >= _PREMATCH_MIN_INDEX,
                      reason="ok")
        return _idx
    except Exception as _exc:                     # noqa: BLE001
        # Причина отказа больше не теряется: без неё нельзя отличить
        # «модель отказала» от «индекс не дотянул до порога ставки».
        _journal_eval(radiant_team=str(radiant_team_name or ""),
                      dire_team=str(dire_team_name or ""), index=0.0,
                      bet=False, reason=str(_exc)[:300] or type(_exc).__name__)
        return None


def win_index_ex(radiant_heroes_and_pos, dire_heroes_and_pos,
                 radiant_team_name=None, dire_team_name=None, match=None):
    """(индекс, источник). Предматчевая модель приоритетна, драфтовая — запасная.

    Имена команд и `match` нужны только Hybrid-рейтингу: по имени резолвится
    ключ организации (`resolve_org_key`), по `startDateTime` — момент оценки.
    Без них предматчевая модель отказывает штатно и работает драфтовая.
    """
    value = _prematch_index(radiant_heroes_and_pos, dire_heroes_and_pos,
                            radiant_team_name, dire_team_name, match)
    if value is not None:
        return value, SOURCE_PREMATCH
    # ЗАПАСНОЙ МОДЕЛИ НЕТ (решение alex 14.08.2026). Раньше здесь отдавалась
    # драфтовая оценка по одним героям: другая модель, другая шкала и заметно
    # слабее (AUC самого драфт-логита 0.6093 против 0.7184 у предматчевой).
    # Под общим ключом индекса она выглядела как та же величина, и по ней
    # срабатывало вето. Лучше молчание, чем число не от той модели.
    return None, None


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


def draft_veto(block_sign: Any, block: Any, section: str = "") -> bool:
    """True, если драфтовый компонент модели тянет ПРОТИВ стороны блока.

    Это `_draft_agrees` для star-пути: там сторона ставки задана знаком
    индекса, здесь — знаком блока, а правило одно и то же. Вклад драфта в
    `parts` стоит в ориентации Radiant (E-213), к стороне блока приводится
    умножением на `block_sign`.

    Молчание драфта возражением НЕ считается: ноль, отсутствие ключа `draft`
    (ветка `pre_draft` считается вовсе без драфтовых колонок) и отсутствие
    разложения оставляют блок в силе. E-217 §1: карты, где драфт молчит, дают
    лучший винрейт, резать их нечем.

    Порог индекса — тот же, что у `blocks_veto`: запрет работает там же, где
    модели вообще позволено возражать, и ни на карту шире.
    """
    global _STAR_DRAFT_MUTE
    if not _STAR_DRAFT_BLOCK or block_sign not in (1, -1, 1.0, -1.0):
        return False
    if not isinstance(block, dict):
        return False
    try:
        index = float(block.get(INDEX_KEY))
    except (TypeError, ValueError):
        return False
    if abs(index) < _min_index_for(section, block.get(SOURCE_KEY)):
        return False
    parts = last_parts(index)
    if not parts:
        if not _STAR_DRAFT_MUTE:
            _STAR_DRAFT_MUTE = True
            print("ВНИМАНИЕ: star-запрет «драфт против стороны» проверять "
                  "нечем — модель не даёт разложения логита по компонентам",
                  flush=True)
        return False                                 # fail-open, как blocks_veto
    try:
        toward_block = float(parts.get("draft", 0.0)) * float(block_sign)
    except (TypeError, ValueError):                  # noqa: BLE001
        return False
    if toward_block >= 0.0:
        return False
    key = (round(float(index), 2), int(block_sign))
    if key not in _STAR_DRAFT_SEEN:
        if len(_STAR_DRAFT_SEEN) > 20000:
            _STAR_DRAFT_SEEN.clear()
        _STAR_DRAFT_SEEN.add(key)
        print(f"[win_model] star-блок отменён: драфт против стороны "
              f"({'radiant' if block_sign > 0 else 'dire'}, секция "
              f"{section or '?'}, индекс {index:+.2f}, вклад драфта "
              f"{float(parts.get('draft', 0.0)):+.4f} в ориентации Radiant)",
              flush=True)
    return True


def model_bet(*blocks) -> Optional[dict]:
    """Сторона и цена самостоятельной ставки по предматчевой модели.

    Возвращает None, пока модель не набрала |индекс| >= порога, и словарь
    `{side, index, confidence, min_odds, expected_wr}` когда набрала. Ставка
    идёт САМА ПО СЕБЕ, без звёзд: замер на 2 456 настоящих LAN-картах даёт
    1 769 карт (72% потока) с винрейтом 70.0% при |индекс| >= 8.

    Драфтовый источник сюда не пускается: у него другая шкала и другой винрейт,
    порог 8 на ней не проверялся.

    Сторона, названная ВОПРЕКИ драфту, ставки не даёт: см. `_draft_agrees`.
    Замера у этого правила нет, оно введено решением; снимается
    WIN_MODEL_DRAFT_AGAINST_BLOCK=0.
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
    if _DRAFT_FIRST_ONLY:
        _dr = last_draft_rank(index)
        if not _dr or _dr[0] != 1:
            return None
    if _DRAFT_AGAINST_BLOCK and not _draft_agrees(index):
        return None
    confidence = (50.0 + abs(index)) / 100.0
    try:
        from base import prematch_scorer as ps
    except Exception:                                # noqa: BLE001
        try:
            import prematch_scorer as ps            # запуск из base/
        except Exception:                            # noqa: BLE001
            return None
    # Какой веткой посчитан именно ЭТОТ индекс. Сверка по значению — тот же
    # приём, что у `_LAST_FILL` в остальных местах модуля.
    branch = "full"
    wr = None
    if _LAST_FILL.get("index") is not None:
        try:
            if abs(float(index) - float(_LAST_FILL["index"])) < 1e-6:
                branch = str(_LAST_FILL.get("branch") or "full")
                _w = _LAST_FILL.get("wr")
                wr = float(_w) if _w is not None else None
        except (TypeError, ValueError):              # noqa: BLE001
            pass
    if branch not in _PREMATCH_BET_BRANCHES:
        return None
    if branch == "full":
        # БУКВАЛЬНО прежнее поведение: у ставок, которые идут и сегодня, цена
        # не имеет права сдвинуться ни на копейку.
        min_odds = ps.lan_min_odds(confidence)
        expected_wr = ps.lan_expected_wr(confidence)
    else:
        if wr is None or wr != wr:                   # nan: полоса не откалибрована
            return None
        expected_wr = wr
        min_odds = _math.ceil(100.0 / max(wr, 1e-9)) / 100.0
    return {
        "side": "radiant" if index > 0 else "dire",
        "index": index,
        "confidence": confidence,
        "branch": branch,
        "min_odds": min_odds,
        "expected_wr": expected_wr,
    }
