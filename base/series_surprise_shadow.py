"""Теневой учёт «сюрприза серии»: насколько прошлые карты разошлись с моделью.

ЗАЧЕМ. Замер 22.08.2026 (`runtime/artifacts/misc/series_upset_prod_scorer.md`)
показал, что боевой скорер перебирает уверенность ТОЛЬКО на продолжениях серий:
на первых картах модель − факт = −0.0108 и отдача +4.90%, на продолжениях +0.0090
и +2.45%. Рабочая величина — накопленный сюрприз `s_sum`: сумма «победила минус
обещано» по уже сыгранным картам серии для РАДИАНТА текущей карты. Прирост AUC на
боевом скорере +0.0123 (95% ДИ +0.0035…+0.0212), вес признака сошёлся на двух
независимых источниках вероятности (+0.156 и +0.215).

ПОЧЕМУ ТЕНЬ, А НЕ СРАЗУ ПОПРАВКА. Боевое окно замера — 136 дней и один сплит.
Модуль ничего не решает: он только пишет `s_sum` рядом с живым индексом, чтобы
через несколько недель появилась выборка, собранная ВПЕРЁД, а не назад.

ПОЧЕМУ СВОЙ УЧЁТ, А НЕ ЖИВОЙ ELO. Прогресс живого ELO для этого не годится:
покарточное применение в бою не срабатывает, у 132 серий из 132 применена ровно
одна карта (E-224). Исход предыдущей карты берётся двумя путями: по сдвигу счёта
серии, если он есть, и — основным — запросом к Stratz по match_id самой карты
(`stratz_map_result.radiant_won`). Второй путь именно основной, потому что счёт
внутри окна наблюдения одной серии не меняется вовсе.

ПОЧЕМУ ЗАКРЫВАЕМ ПО match_id, А НЕ ПО КЛЮЧУ. Ключ карты в проде меняется по
12 раз ЗА одну карту (`...8958607830.10`, `.11`, … `.27`). Сравнение «ключ
отложенной ≠ текущий ключ» срабатывало бы на каждой такой смене и клало бы одну
и ту же карту в историю многократно. Сравнивается match_id, вырезанный из ключа:
он у карты один.

ПОЧЕМУ ОРИЕНТАЦИЯ НА РАДИАНТА. Цель модели — «победил радиант». Первая версия
признака считалась для команды, за которую голосует модель, и на картах с выбором
в пользу дайра знак относительно цели переворачивался: веса выходили разного знака
на разных источниках. Ориентир только радиант текущей карты.
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from stratz_map_result import match_id_from_map_key

#: Хранилище рядом с прочим рантаймом. Пишется атомарно: файл читают и другие
#: процессы, а частичная запись выглядела бы как пустая история серии.
DEFAULT_STORE_PATH = Path(
    os.getenv("SERIES_SURPRISE_STORE",
              str(Path(__file__).resolve().parent.parent / "runtime" / "series_surprise_shadow.json"))
)
#: Серии старше этого срока выбрасываются: серия не длится дольше суток, а файл
#: иначе растёт без предела.
MAX_AGE_SECONDS = 36 * 3600
#: Больше шести карт в серии не бывает даже в бо-5 с переигровкой.
MAX_HISTORY = 6

_lock = threading.Lock()


def _load(path: Path) -> Dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return {"series": {}}
    if not isinstance(data, dict) or not isinstance(data.get("series"), dict):
        return {"series": {}}
    return data


def _save(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _prune(data: Dict[str, Any], now: int) -> None:
    stale = [k for k, v in data["series"].items()
             if now - int((v or {}).get("updated_at") or 0) > MAX_AGE_SECONDS]
    for k in stale:
        data["series"].pop(k, None)


def _winner_slot(prev: Dict[str, int], cur: Dict[str, int]) -> Optional[str]:
    """Кто взял карту между двумя снимками счёта. None — счёт не сдвинулся."""
    d_first = int(cur.get("first", 0)) - int(prev.get("first", 0))
    d_second = int(cur.get("second", 0)) - int(prev.get("second", 0))
    if d_first == 1 and d_second == 0:
        return "first"
    if d_second == 1 and d_first == 0:
        return "second"
    return None


def surprise_from_history(history: List[Dict[str, Any]], radiant_team_id: int) -> Dict[str, float]:
    """`s_sum` и спутники для команды `radiant_team_id` по истории серии.

    Вклад карты = (эта команда победила) − (вероятность, которую ей давали).
    Карты, где команда не участвовала, пропускаются: в серии это невозможно, но
    ключ команды может поменяться при перерегистрации организации.
    """
    total = 0.0
    last = 0.0
    used = 0
    for k, item in enumerate(reversed(history[-MAX_HISTORY:])):
        rad = int(item.get("radiant_team_id") or 0)
        p_rad = item.get("p_radiant")
        won_rad = item.get("radiant_won")
        if p_rad is None or won_rad is None or rad <= 0:
            continue
        if rad == int(radiant_team_id):
            p, won = float(p_rad), bool(won_rad)
        else:
            p, won = 1.0 - float(p_rad), not bool(won_rad)
        s = (1.0 if won else 0.0) - p
        total += s
        if used == 0:
            last = s
        used += 1
    return {"s_sum": total, "s_last": last, "n_prev": float(used)}


def observe(
    *,
    series_key: str,
    map_key: str,
    radiant_team_id: int,
    dire_team_id: int,
    p_radiant: float,
    first_team_is_radiant: bool,
    first_team_score: int,
    second_team_score: int,
    store_path: Optional[Path] = None,
    now: Optional[int] = None,
    winner_lookup=None,
) -> Dict[str, float]:
    """Записать текущую карту и вернуть сюрприз серии ДО неё.

    Исход предыдущей карты восстанавливается двумя путями. Сначала по сдвигу
    счёта серии — он бесплатный. Если счёт не сдвинулся, зовётся `winner_lookup`:
    в бою это `stratz_map_result.radiant_won`, спрашивающий исход по match_id
    самой карты. Второй путь нужен не как запасной, а как основной: по E-224
    счёт внутри окна наблюдения одной серии не меняется вовсе, так что на живом
    пути первый способ не срабатывает никогда.

    Возвращаются величины, посчитанные по истории БЕЗ текущей карты — именно они
    доступны модели в момент решения.
    """
    path = Path(store_path or DEFAULT_STORE_PATH)
    ts = int(now if now is not None else time.time())
    skey = str(series_key or "").strip()
    mkey = str(map_key or "").strip()
    if not skey or not mkey:
        return {"s_sum": 0.0, "s_last": 0.0, "n_prev": 0.0}
    cur_scores = {"first": int(first_team_score or 0), "second": int(second_team_score or 0)}

    with _lock:
        data = _load(path)
        _prune(data, ts)
        state = data["series"].get(skey)
        if not isinstance(state, dict):
            state = {"last_scores": cur_scores, "pending": None, "history": [], "updated_at": ts}

        history: List[Dict[str, Any]] = list(state.get("history") or [])
        pending = state.get("pending")
        slot = _winner_slot(state.get("last_scores") or cur_scores, cur_scores)
        radiant_won = None
        cur_mid = match_id_from_map_key(mkey)
        pend_mid = match_id_from_map_key(str((pending or {}).get("map_key") or ""))
        other_map = (isinstance(pending, dict)
                     and pend_mid > 0 and cur_mid > 0 and pend_mid != cur_mid)
        already = {int(h.get("match_id") or 0) for h in history}
        if other_map and pend_mid not in already:
            if slot is not None:
                first_rad = bool(pending.get("first_team_is_radiant"))
                radiant_won = slot == ("first" if first_rad else "second")
            elif winner_lookup is not None:
                try:
                    radiant_won = winner_lookup(str(pending.get("map_key") or ""))
                except Exception:
                    radiant_won = None
        if radiant_won is not None and isinstance(pending, dict):
            history.append({
                "map_key": pending.get("map_key"),
                "match_id": pend_mid,
                "radiant_team_id": int(pending.get("radiant_team_id") or 0),
                "p_radiant": pending.get("p_radiant"),
                "radiant_won": bool(radiant_won),
                "closed_at": ts,
            })
            history = history[-MAX_HISTORY:]

        out = surprise_from_history(history, int(radiant_team_id))

        data["series"][skey] = {
            "last_scores": cur_scores,
            "pending": {
                "map_key": mkey,
                "radiant_team_id": int(radiant_team_id or 0),
                "dire_team_id": int(dire_team_id or 0),
                "p_radiant": float(p_radiant),
                "first_team_is_radiant": bool(first_team_is_radiant),
                "registered_at": ts,
            },
            "history": history,
            "updated_at": ts,
        }
        _save(path, data)
    return out
