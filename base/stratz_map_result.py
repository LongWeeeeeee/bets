"""Карты серии у Stratz — запросом ПО КОМАНДЕ за сутки, без опоры на id карты.

ЗАЧЕМ. Живой путь не знает, кто взял предыдущую карту серии: счёт на входе
регистрации ELO не меняется внутри окна наблюдения одной серии (E-224). Первая
версия этого модуля спрашивала исход по match_id, вырезанному из ключа карты, но
у прода отдельного id карты НЕТ вовсе: он пишет `match_id = series_id`, и все
карты серии несут один номер (проверено на серии 8958667855 — Nigma vs BoomBoys,
двенадцать ключей с одним номером). По ключу поэтому всегда спрашивалась первая
карта: верно для перехода 1→2 и неверно дальше.

Идея alex: id карты и не нужен — достаточно спросить матчи КОМАНДЫ с началом
«вчера». У Stratz при этом есть собственный `seriesId`, и карты группируются в
серию сами.

ФОРМА ЗАПРОСА (выяснена введением, в документации не очевидна):
    team(teamId: N) { matches(request: {startDateTime: T, take: K, skip: 0}) {...} }
Поле `skip` ОБЯЗАТЕЛЬНО: без него запрос падает целиком с
«Missing required field 'skip'», а не игнорирует аргумент.

ЧТО ПРОВЕРЕНО (22.08.2026):
  * победитель есть у 60 боевых match_id из 60;
  * промежуток между концом карты и стартом следующей — медиана 25 минут
    (25/75: 20/30, дольше 5 минут — 100%), так что время на запрос есть;
  * РАЗБОР (поминутный нетворс) запаздывает медианно на 17 часов, поэтому
    богатые признаки прошлой карты в живом пути недоступны и здесь не берутся.

Модуль никогда не бросает: на любой сбой возвращается пустой список, а не
выдуманный исход.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

DEFAULT_CACHE_PATH = Path(
    os.getenv("STRATZ_MAP_RESULT_CACHE",
              str(Path(__file__).resolve().parent.parent / "runtime" / "stratz_team_matches.json"))
)
URL = "https://api.stratz.com/graphql"
#: Запрос идёт из боевого пути ставки, поэтому цена ошибки — задержка отправки.
#: Отсюда короткий таймаут и не больше двух пар ключ↔прокси.
TIMEOUT = float(os.getenv("STRATZ_MAP_RESULT_TIMEOUT", "5"))
MAX_PAIRS = int(os.getenv("STRATZ_MAP_RESULT_MAX_PAIRS", "2"))
#: Кэш живой: пока серия идёт, ответ меняется по мере доигрывания карт.
CACHE_TTL = float(os.getenv("STRATZ_TEAM_MATCHES_TTL", "120"))
#: Окно «вчера»: серия не растягивается дольше, а лишние сутки удорожают ответ.
LOOKBACK_SECONDS = int(os.getenv("STRATZ_TEAM_LOOKBACK", str(30 * 3600)))
TAKE = 25
#: Под этим ключом в том же файле лежит перевод «id прода -> id фида». Отдельным
#: файлом делать не стоит: связка нужна ровно там же, где кэш, и живёт столько же.
ALIAS_KEY = "_aliases"
MATCH_ID_RE = re.compile(r"/matches/(\d{6,})")
_lock = threading.Lock()

_QUERY = (
    "{team(teamId:%d){id matches(request:{startDateTime:%d,take:%d,skip:0})"
    "{id seriesId startDateTime endDateTime durationSeconds "
    "radiantTeamId direTeamId didRadiantWin}}}"
)


def match_id_from_map_key(map_key: str) -> int:
    """`dltv.org/matches/8958607830.27` -> 8958607830.

    Оставлено для разбора ключей, но исходом карты это число НЕ является: у прода
    оно совпадает с id серии. Заглушка `1700000000` (подставленный timestamp)
    отбрасывается.
    """
    m = MATCH_ID_RE.search(str(map_key or ""))
    if not m:
        return 0
    try:
        v = int(m.group(1))
    except ValueError:
        return 0
    return v if v > 8_000_000_000 else 0


def note_team_alias(prod_team_id: int, feed_team_id: int, *,
                    cache_path: Optional[Path] = None) -> None:
    """Запомнить, что прод зовёт эту команду иначе, чем фид.

    ЗАЧЕМ. Прод подменяет пришедший id команды словарным по имени
    (`TEAM_ID_NAME_MISMATCH`), и до нас доезжает уже подменённый. У Stratz такого
    id нет: 22.08.2026 по боевым `10163435` (BoomBoys) и `7554697` (Nigma)
    возвращался НОЛЬ матчей, тогда как по пришедшим с фида `8255888` и
    `10136357` — по шесть. Справка из-за этого не могла сработать никогда.
    Связку знает только та точка, где происходит подмена, — оттуда её и пишем.
    """
    a, b = int(prod_team_id or 0), int(feed_team_id or 0)
    if a <= 0 or b <= 0 or a == b:
        return
    path = Path(cache_path or DEFAULT_CACHE_PATH)
    with _lock:
        cache = _load(path)
        al = cache.get(ALIAS_KEY)
        if not isinstance(al, dict):
            al = {}
        if al.get(str(a)) == b:
            return
        al[str(a)] = b
        cache[ALIAS_KEY] = al
        _save(path, cache)


def resolve_team_id(team_id: int, *, cache_path: Optional[Path] = None) -> int:
    """id, под которым команду знает Stratz. Без записанной связки — как есть."""
    tid = int(team_id or 0)
    with _lock:
        al = _load(Path(cache_path or DEFAULT_CACHE_PATH)).get(ALIAS_KEY)
    if isinstance(al, dict):
        try:
            return int(al.get(str(tid), tid))
        except Exception:
            return tid
    return tid


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


def _query(team_id: int, since: int) -> Optional[List[Dict[str, Any]]]:
    """Матчи команды с момента `since`. `None` — не дозвонились."""
    try:
        import requests                                     # noqa: PLC0415
        from keys import api_to_proxy                       # noqa: PLC0415
    except Exception:
        return None
    q = _QUERY % (int(team_id), int(since), TAKE)
    for proxy, key in list(api_to_proxy.items())[:MAX_PAIRS]:
        try:
            r = requests.post(
                URL, json={"query": q}, timeout=TIMEOUT,
                headers={"Authorization": f"Bearer {key}",
                         "Content-Type": "application/json",
                         "User-Agent": "STRATZ_API"},
                proxies={"http": proxy, "https": proxy})
            team = ((r.json().get("data") or {}).get("team")) or {}
            ms = team.get("matches")
            if isinstance(ms, list):
                return ms
        except Exception:
            continue                                        # следующая пара
    return None


def team_matches(team_id: int, *, now: Optional[int] = None,
                 cache_path: Optional[Path] = None,
                 query=_query) -> List[Dict[str, Any]]:
    """Завершённые матчи команды за последние сутки. Пустой список — не знаем.

    `query` вынесен параметром ради тестов: сеть в них не нужна.
    """
    path = Path(cache_path or DEFAULT_CACHE_PATH)
    # Перевод в id фида идёт ДО запроса: спрашивать подменённый бессмысленно.
    tid = resolve_team_id(team_id, cache_path=path)
    if tid <= 0:
        return []
    ts = int(now if now is not None else time.time())
    key = str(tid)
    with _lock:
        cache = _load(path)
        hit = cache.get(key)
        if isinstance(hit, dict) and ts - int(hit.get("at") or 0) < CACHE_TTL:
            return list(hit.get("matches") or [])
    got = query(tid, ts - LOOKBACK_SECONDS)
    if got is None:
        with _lock:                                         # отдаём протухшее,
            hit = _load(path).get(key)                      # но не выдумываем
        return list((hit or {}).get("matches") or []) if isinstance(hit, dict) else []
    clean = []
    for m in got:
        try:
            if m.get("didRadiantWin") is None or not m.get("endDateTime"):
                continue                                    # карта ещё идёт
            clean.append({
                "match_id": int(m["id"]),
                "series_id": int(m.get("seriesId") or 0),
                "start": int(m.get("startDateTime") or 0),
                "end": int(m["endDateTime"]),
                "radiant_team_id": int(m.get("radiantTeamId") or 0),
                "dire_team_id": int(m.get("direTeamId") or 0),
                "radiant_won": bool(m["didRadiantWin"]),
            })
        except Exception:
            continue
    with _lock:
        cache = _load(path)
        cache[key] = {"at": ts, "matches": clean}
        for k in [k for k, v in cache.items()
                  if k != ALIAS_KEY and ts - int((v or {}).get("at") or 0) > LOOKBACK_SECONDS]:
            cache.pop(k, None)
        _save(path, cache)
    return clean


def series_history(radiant_team_id: int, dire_team_id: int, *,
                   before: Optional[int] = None,
                   now: Optional[int] = None,
                   cache_path: Optional[Path] = None,
                   query=_query) -> List[Dict[str, Any]]:
    """Уже сыгранные карты ТЕКУЩЕЙ серии этих двух команд, по возрастанию времени.

    Серия определяется не временем и не счётом, а `seriesId` самого Stratz:
    берётся та серия, к которой относится последняя завершённая встреча этих
    команд. Карты позже `before` отбрасываются — предсказывать по будущему нельзя.
    """
    ts = int(now if now is not None else time.time())
    cutoff = int(before if before is not None else ts)
    path = Path(cache_path or DEFAULT_CACHE_PATH)
    # ПЕРЕВОДЯТСЯ ОБЕ СТОРОНЫ. Матчи приходят с идентификаторами Stratz, поэтому
    # и пара для сравнения должна быть в них: первая версия правки переводила
    # только id для запроса, и фильтр пары молча не совпадал никогда.
    rad = resolve_team_id(radiant_team_id, cache_path=path)
    dire = resolve_team_id(dire_team_id, cache_path=path)
    ms = team_matches(rad, now=ts, cache_path=path, query=query)
    pair = {int(rad), int(dire)}
    same = [m for m in ms
            if {m["radiant_team_id"], m["dire_team_id"]} == pair and m["end"] < cutoff]
    if not same:
        return []
    same.sort(key=lambda m: m["start"])
    last_series = same[-1]["series_id"]
    if last_series:
        same = [m for m in same if m["series_id"] == last_series]
    return same


# ---------- фоновый прогрев ----------
#
# ЗАЧЕМ. Справка нужна ровно в момент вердикта по следующей карте серии, а это
# минута-две до её старта. Если спрашивать синхронно, то в пути ставки появляется
# сетевой вызов, и вдобавок ответа может ещё не быть: карта только что кончилась.
# Идея alex — опрашивать карту заранее, с интервалом в полминуты, начиная примерно
# через четверть часа после её конца. Тогда к моменту вердикта ответ уже в кэше, а
# синхронный вызов становится обращением к диску.
#
# ЗАОДНО ЭТО ЗАМЕР. Через сколько Stratz отдаёт ИСХОД (не разбор), мы не знаем:
# известна только задержка разбора — медиана 17 часов. Прогрев логирует момент
# первого появления карты относительно её конца, и через сутки станет видно,
# правильные ли четверть часа.

REFRESH_INTERVAL = float(os.getenv("STRATZ_REFRESH_INTERVAL", "30"))
_refresh_thread: Optional[threading.Thread] = None


def refresh(team_ids, *, cache_path: Optional[Path] = None,
            now: Optional[int] = None, query=_query, on_new=None) -> int:
    """Обновить кэш по списку команд В ОБХОД TTL. Возвращает число новых карт."""
    ts = int(now if now is not None else time.time())
    path = Path(cache_path or DEFAULT_CACHE_PATH)
    fresh = 0
    for tid in {resolve_team_id(t, cache_path=path) for t in team_ids if int(t or 0) > 0}:
        with _lock:
            before = {int(m.get("match_id") or 0)
                      for m in (_load(path).get(str(tid)) or {}).get("matches") or []}
        got = query(tid, ts - LOOKBACK_SECONDS)
        if got is None:
            continue
        clean = []
        for m in got:
            try:
                if m.get("didRadiantWin") is None or not m.get("endDateTime"):
                    continue
                clean.append({
                    "match_id": int(m["id"]),
                    "series_id": int(m.get("seriesId") or 0),
                    "start": int(m.get("startDateTime") or 0),
                    "end": int(m["endDateTime"]),
                    "radiant_team_id": int(m.get("radiantTeamId") or 0),
                    "dire_team_id": int(m.get("direTeamId") or 0),
                    "radiant_won": bool(m["didRadiantWin"]),
                })
            except Exception:
                continue
        with _lock:
            cache = _load(path)
            cache[str(tid)] = {"at": ts, "matches": clean}
            _save(path, cache)
        for m in clean:
            if m["match_id"] not in before:
                fresh += 1
                if on_new is not None:
                    try:
                        on_new(tid, m, ts - m["end"])
                    except Exception:
                        pass
    return fresh


def start_background_refresh(teams_provider, *, interval: Optional[float] = None,
                             cache_path: Optional[Path] = None, on_new=None,
                             query=_query) -> Optional[threading.Thread]:
    """Демон, держащий кэш тёплым по командам, которые вернёт `teams_provider`.

    Заводится один раз; повторный вызов возвращает уже работающий поток. Ошибки
    гасятся целиком: прогрев не имеет права уронить боевой процесс.
    """
    global _refresh_thread
    if _refresh_thread is not None and _refresh_thread.is_alive():
        return _refresh_thread
    step = float(interval if interval is not None else REFRESH_INTERVAL)

    def loop() -> None:
        while True:
            try:
                teams = list(teams_provider() or [])
                if teams:
                    refresh(teams, cache_path=cache_path, query=query, on_new=on_new)
            except Exception:
                pass
            time.sleep(step)

    _refresh_thread = threading.Thread(target=loop, name="stratz-refresh", daemon=True)
    _refresh_thread.start()
    return _refresh_thread
