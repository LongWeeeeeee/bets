"""Исход карты по её match_id у Stratz — без опоры на счёт серии.

ЗАЧЕМ. Живой путь не знает, кто взял предыдущую карту серии: счёт на входе
регистрации ELO не меняется внутри окна наблюдения одной серии (E-224), и
механизм «победитель = сдвиг счёта» сработать не может. Но match_id каждой карты
у прода ЕСТЬ — он лежит прямо в ключе, `dltv.org/matches/<match_id>.<суффикс>`, —
а Stratz по нему отдаёт `didRadiantWin`. Счёт серии тогда вообще не нужен.

ЧТО ПРОВЕРЕНО (22.08.2026):
  * победитель есть у 60 боевых match_id из 60;
  * промежуток между концом карты и стартом следующей — медиана 25 минут
    (25/75: 20/30, дольше 5 минут — 100% случаев), так что время на запрос есть;
  * а вот РАЗБОР (поминутный нетворс) запаздывает медианно на 17 часов, поэтому
    богатые признаки прошлой карты в живом пути недоступны и здесь не берутся.
    Свежая карта разобралась за 18 минут, но это не правило.

Модуль никогда не бросает: неизвестный исход возвращается как `None` и означает
«не знаем», а не «поражение». Ответы кэшируются на диске — исход карты неизменен,
и повторно спрашивать его незачем.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

DEFAULT_CACHE_PATH = Path(
    os.getenv("STRATZ_MAP_RESULT_CACHE",
              str(Path(__file__).resolve().parent.parent / "runtime" / "stratz_map_results.json"))
)
URL = "https://api.stratz.com/graphql"
#: Запрос идёт из боевого пути ставки, поэтому цена ошибки — задержка отправки.
#: Отсюда короткий таймаут и не больше двух пар ключ↔прокси: худший случай 10 с,
#: и только один раз на переход между картами серии — дальше отвечает кэш.
TIMEOUT = float(os.getenv("STRATZ_MAP_RESULT_TIMEOUT", "5"))
MAX_PAIRS = int(os.getenv("STRATZ_MAP_RESULT_MAX_PAIRS", "2"))
#: Кэш подрезается, чтобы файл не рос без предела; исходы старше суток не нужны.
MAX_AGE_SECONDS = 36 * 3600
MATCH_ID_RE = re.compile(r"/matches/(\d{6,})")
_lock = threading.Lock()


def match_id_from_map_key(map_key: str) -> int:
    """`dltv.org/matches/8958607830.27` -> 8958607830. Ноль, если id не виден."""
    m = MATCH_ID_RE.search(str(map_key or ""))
    if not m:
        return 0
    try:
        v = int(m.group(1))
    except ValueError:
        return 0
    # Заглушка `1700000000` встречается в состоянии прода там, где match_id не
    # приехал: это timestamp, подставленный вместо id. Такие не спрашиваем.
    return v if v > 8_000_000_000 else 0


def _load(path: Path) -> Dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _save(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass


def _query(match_id: int) -> Optional[bool]:
    """Один запрос к Stratz с перебором пар ключ↔прокси. Никогда не бросает."""
    try:
        import requests                                     # noqa: PLC0415
        from keys import api_to_proxy                       # noqa: PLC0415
    except Exception:
        return None
    q = "{match(id:%d){id didRadiantWin}}" % int(match_id)
    for proxy, key in list(api_to_proxy.items())[:MAX_PAIRS]:
        try:
            r = requests.post(
                URL, json={"query": q}, timeout=TIMEOUT,
                headers={"Authorization": f"Bearer {key}",
                         "Content-Type": "application/json",
                         "User-Agent": "STRATZ_API"},
                proxies={"http": proxy, "https": proxy})
            data = (r.json().get("data") or {}).get("match") or {}
            won = data.get("didRadiantWin")
            if isinstance(won, bool):
                return won
        except Exception:
            continue                                        # следующая пара
    return None


def radiant_won(map_key: str = "", match_id: int = 0, *,
                cache_path: Optional[Path] = None,
                now: Optional[int] = None,
                query=_query) -> Optional[bool]:
    """Победил ли радиант на этой карте. `None` — неизвестно.

    `query` вынесен параметром ради тестов: сеть в них не нужна.
    """
    mid = int(match_id) or match_id_from_map_key(map_key)
    if mid <= 0:
        return None
    path = Path(cache_path or DEFAULT_CACHE_PATH)
    ts = int(now if now is not None else time.time())
    key = str(mid)
    with _lock:
        cache = _load(path)
        hit = cache.get(key)
        if isinstance(hit, dict) and isinstance(hit.get("radiant_won"), bool):
            return bool(hit["radiant_won"])
    won = query(mid)
    if won is None:
        return None
    with _lock:
        cache = _load(path)
        cache[key] = {"radiant_won": bool(won), "at": ts}
        for k in [k for k, v in cache.items()
                  if ts - int((v or {}).get("at") or 0) > MAX_AGE_SECONDS]:
            cache.pop(k, None)
        _save(path, cache)
    return bool(won)
