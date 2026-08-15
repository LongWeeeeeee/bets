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
import time
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
               "draft_rank": None, "draft_share": None}
_DRAFT_FIRST_ONLY = str(os.getenv("WIN_MODEL_DRAFT_FIRST_ONLY", "0")).strip() in ("1", "true", "yes", "on")
# Чисто драфтовые: считаются только по десяти hero_id (и позициям).
_DRAFT_FEATURES = frozenset({
    "draft_logit", "vs_wr", "cp_lane", "syn_pos_mean", "wr30", "farm_dep",
    "draft_logit_x_elo_gap", "draft_logit_x_games_exp",
})
# Игрок НА ГЕРОЕ — смешанные, в долю драфта не входят.
_HERO_PLAYER_FEATURES = frozenset({
    "hero_games", "hero_gpm_rel", "lh_rel_hero", "a_hdmg_rel_hero",
})
_EVAL_JOURNAL = "/root/main/runtime/prematch_model_eval.jsonl"
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
        if _LAST_FILL["index"] is not None and abs(float(index) - float(_LAST_FILL["index"])) < 1e-6:
            v = _LAST_FILL["elo"]
            return None if v is None else float(v) * 400.0
    except (TypeError, ValueError):
        pass
    return None


def last_draft_rank(index):
    """Место драфта среди признаков, тянущих в сторону ставки, и его вклад."""
    try:
        if _LAST_FILL["index"] is not None and abs(float(index) - float(_LAST_FILL["index"])) < 1e-6:
            s = _LAST_FILL.get("draft_share")
            return (_LAST_FILL.get("draft_rank"), s) if s is not None else None
    except (TypeError, ValueError):
        pass
    return None


def last_fill(index):
    """Заполненность входа для этого индекса или None, если он не тот."""
    try:
        if _LAST_FILL["index"] is not None and abs(float(index) - float(_LAST_FILL["index"])) < 1e-6:
            return _LAST_FILL["fill"]
    except (TypeError, ValueError):
        pass
    return None
# сутки, после которых снимок считается мёртвым (замер E-177)
_SNAPSHOT_MAX_AGE_DAYS = 30.0
# сутки, после которых устаревание уже видно в качестве и его пора чинить
_SNAPSHOT_WARN_DAYS = 3.0


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
    if _DRAFT_FIRST_ONLY:
        _dr = last_draft_rank(index)
        if not _dr or _dr[0] != 1:
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
