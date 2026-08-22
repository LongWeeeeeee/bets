"""Теневой учёт «сюрприза серии»: насколько прошлые карты разошлись с моделью.

ЗАЧЕМ. Замер 22.08.2026 (`runtime/artifacts/misc/series_upset_prod_scorer.md`)
показал, что боевой скорер перебирает уверенность ТОЛЬКО на продолжениях серий:
на первых картах модель − факт = −0.0108 и отдача +4.90%, на продолжениях +0.0090
и +2.45%. Рабочая величина — накопленный сюрприз `s_sum`: сумма «победила минус
обещано» по уже сыгранным картам серии для РАДИАНТА текущей карты. Прирост AUC на
боевом скорере +0.0123 (95% ДИ +0.0035…+0.0212), вес признака сошёлся на двух
независимых источниках вероятности (+0.156 и +0.215).

ПОЧЕМУ ТЕНЬ. Боевое окно замера — 136 дней и один сплит. Модуль ничего не решает:
он пишет `s_sum` рядом с живым индексом, чтобы набралась выборка, собранная
ВПЕРЁД, а не назад.

ОТКУДА ИСХОДЫ ПРОШЛЫХ КАРТ. Не из прода: живой ELO покарточно не обновляется
(E-224), счёт серии на входе не меняется, а отдельного id карты у прода нет вовсе
— он пишет `match_id = series_id`, и все карты серии несут один номер. Поэтому
история берётся снаружи: `stratz_map_result.series_history` спрашивает матчи
КОМАНДЫ за сутки и группирует их родным `seriesId` Stratz. Ни счёт, ни id карты
не нужны.

ЧТО ХРАНИМ САМИ. Только свои вердикты: какую вероятность модель дала радианту на
каждой карте. Исход приходит извне, вероятность — отсюда, связываются они по
ВРЕМЕНИ: вердикт выносится за минуту-две до старта карты, поэтому совпадение
ищется в окне `LINK_WINDOW` вокруг старта и при той же паре команд.

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

DEFAULT_STORE_PATH = Path(
    os.getenv("SERIES_SURPRISE_STORE",
              str(Path(__file__).resolve().parent.parent / "runtime" / "series_surprise_shadow.json"))
)
#: Вердикты старше суток не нужны: серия столько не длится.
MAX_AGE_SECONDS = 36 * 3600
#: Больше шести карт в серии не бывает даже в бо-5 с переигровкой.
MAX_HISTORY = 6
#: Вердикт выносится незадолго до старта карты; при поиске пары «вердикт ↔ карта»
#: допускается такой разброс. Шире брать нельзя — склеятся соседние карты серии,
#: между которыми по корпусу медиана 25 минут.
LINK_WINDOW = 15 * 60

_lock = threading.Lock()


def _load(path: Path) -> Dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return {"verdicts": []}
    if not isinstance(data, dict) or not isinstance(data.get("verdicts"), list):
        return {"verdicts": []}
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


def _find_verdict(verdicts: List[Dict[str, Any]], start: int,
                  pair: set) -> Optional[Dict[str, Any]]:
    """Наш вердикт по карте, стартовавшей в `start`. Ближайший по времени."""
    best, best_gap = None, LINK_WINDOW + 1
    for v in verdicts:
        try:
            if {int(v["radiant_team_id"]), int(v["dire_team_id"])} != pair:
                continue
            gap = abs(int(v["ts"]) - int(start))
        except Exception:
            continue
        if gap <= LINK_WINDOW and gap < best_gap:
            best, best_gap = v, gap
    return best


def surprise_from_maps(maps: List[Dict[str, Any]], verdicts: List[Dict[str, Any]],
                       radiant_team_id: int) -> Dict[str, float]:
    """`s_sum` и спутники для команды `radiant_team_id` по сыгранным картам серии.

    Вклад карты = (эта команда победила) − (вероятность, которую ей давали). Карты
    без нашего вердикта пропускаются: вероятность взять неоткуда, а подставлять
    0.5 значило бы придумывать сюрприз там, где его не мерили.
    """
    total, last, used = 0.0, 0.0, 0
    tid = int(radiant_team_id)
    for m in list(maps)[-MAX_HISTORY:]:
        pair = {int(m.get("radiant_team_id") or 0), int(m.get("dire_team_id") or 0)}
        v = _find_verdict(verdicts, int(m.get("start") or 0), pair)
        if v is None:
            continue
        p_rad_map = float(v["p_radiant"])
        rad_of_map = int(m.get("radiant_team_id") or 0)
        won_rad_map = bool(m.get("radiant_won"))
        if rad_of_map == tid:
            p, won = p_rad_map, won_rad_map
        else:
            p, won = 1.0 - p_rad_map, not won_rad_map
        total += (1.0 if won else 0.0) - p
        last = (1.0 if won else 0.0) - p
        used += 1
    return {"s_sum": total, "s_last": last, "n_prev": float(used)}


def observe(
    *,
    series_key: str,
    map_key: str,
    radiant_team_id: int,
    dire_team_id: int,
    p_radiant: float,
    store_path: Optional[Path] = None,
    now: Optional[int] = None,
    history_lookup=None,
) -> Dict[str, float]:
    """Записать текущий вердикт и вернуть сюрприз серии ДО этой карты.

    `history_lookup(radiant_team_id, dire_team_id, now)` возвращает уже сыгранные
    карты текущей серии; в бою это `stratz_map_result.series_history`. Без него
    модуль только копит вердикты и возвращает нули — сеть в тестах не нужна.
    """
    path = Path(store_path or DEFAULT_STORE_PATH)
    ts = int(now if now is not None else time.time())
    mkey = str(map_key or "").strip()
    if not mkey or int(radiant_team_id or 0) <= 0:
        return {"s_sum": 0.0, "s_last": 0.0, "n_prev": 0.0}

    with _lock:
        data = _load(path)
        verdicts = [v for v in data["verdicts"]
                    if ts - int((v or {}).get("ts") or 0) <= MAX_AGE_SECONDS]
        # Вердикт по одной карте выносится многократно — индекс тот же, поэтому
        # хранится ПЕРВЫЙ: он ближе всего к старту карты, а по времени мы её
        # потом и опознаём.
        if not any(str((v or {}).get("map_key")) == mkey for v in verdicts):
            verdicts.append({
                "ts": ts, "map_key": mkey, "series_key": str(series_key or ""),
                "radiant_team_id": int(radiant_team_id),
                "dire_team_id": int(dire_team_id or 0),
                "p_radiant": float(p_radiant),
            })
        data["verdicts"] = verdicts
        _save(path, data)

    if history_lookup is None:
        return {"s_sum": 0.0, "s_last": 0.0, "n_prev": 0.0}
    try:
        maps = history_lookup(int(radiant_team_id), int(dire_team_id or 0), ts) or []
    except Exception:
        return {"s_sum": 0.0, "s_last": 0.0, "n_prev": 0.0}
    return surprise_from_maps(maps, verdicts, int(radiant_team_id))


def recent_team_ids(*, within: int = 3 * 3600, store_path: Optional[Path] = None,
                    now: Optional[int] = None) -> List[int]:
    """Команды из свежих вердиктов — их и держит тёплыми фоновый прогрев.

    Три часа выбраны с запасом на бо-5: серия столько не длится, а держать в
    прогреве команду, которая давно доиграла, значит зря жечь квоту.
    """
    ts = int(now if now is not None else time.time())
    with _lock:
        verdicts = _load(Path(store_path or DEFAULT_STORE_PATH))["verdicts"]
    out = set()
    for v in verdicts:
        try:
            if ts - int(v.get("ts") or 0) > within:
                continue
            for k in ("radiant_team_id", "dire_team_id"):
                if int(v.get(k) or 0) > 0:
                    out.add(int(v[k]))
        except Exception:
            continue
    return sorted(out)


#: Прогресс живого ELO прод пишет на КАЖДОЙ живой карте, а не только там, где
#: сделана ставка. Как источник «кто сейчас играет» он полнее нашего хранилища
#: вердиктов, которое наполняется лишь на ставках.
DEFAULT_ELO_PROGRESS_PATH = Path(
    os.getenv("LIVE_ELO_PROGRESS",
              str(Path(__file__).resolve().parent.parent / "runtime" / "live_elo_progress.json"))
)


def teams_from_elo_progress(*, path: Optional[Path] = None,
                            within: int = 3 * 3600,
                            now: Optional[int] = None) -> List[int]:
    """Команды из отложенных карт прогресса живого ELO. Пусто — если файла нет."""
    ts = int(now if now is not None else time.time())
    try:
        with Path(path or DEFAULT_ELO_PROGRESS_PATH).open(encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return []
    out = set()
    for state in (data.get("pending_series") or {}).values():
        try:
            if ts - int(state.get("updated_at") or 0) > within:
                continue
            rec = ((state.get("pending_map") or {}).get("match_record")) or {}
            for k in ("radiant_team_id", "dire_team_id"):
                if int(rec.get(k) or 0) > 0:
                    out.add(int(rec[k]))
        except Exception:
            continue
    return sorted(out)


def teams_to_warm(*, store_path: Optional[Path] = None,
                  elo_progress_path: Optional[Path] = None,
                  now: Optional[int] = None) -> List[int]:
    """Кого держать тёплым: живые карты плюс команды наших свежих вердиктов."""
    a = teams_from_elo_progress(path=elo_progress_path, now=now)
    b = recent_team_ids(store_path=store_path, now=now)
    return sorted(set(a) | set(b))
