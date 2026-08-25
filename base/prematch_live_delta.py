"""Дельта по игрокам: что случилось ПОСЛЕ среза боевого снимка.

ЗАЧЕМ. Из 35 признаков предматчевой модели живой ровно один (`hybrid_strength`),
остальные читаются из `data/prematch_model_artifact_v3.npz`. Снимок собирается
вручную цепочкой из четырёх скриптов, крона нет, на боевую машину его копируют
руками: на 22.08.2026 `snapshot_ts` = 21.08 23:28 UTC, отставание 8.5 часа. Все
карты, сыгранные за день, для признаков игроков невидимы.

Цена этого измерена (`runtime/artifacts/misc/prematch_snapshot_staleness.md`):
калибровка монотонно портится с числом сыгранных за день карт — остаток +0.0102
на первой карте дня против −0.0137 на четвёртой, и почти половина вердиктов
(48.2%) выносится там, где команда уже играла сегодня.

ЧТО ДЕЛАЕТ МОДУЛЬ. Копит СЫРЬЁ по завершённым картам: по каждому из десяти
игроков герой, позиция, исход и его строчная статистика. В `score()` из этого
сырья накладываются только СЧЁТЧИКИ (`games`, `hero_games`, `pos_games`).
Скользящие признаки по-прежнему ждут точных определений окон.

ПОЧЕМУ НАКЛАДКА, А НЕ ПЕРЕСБОРКА. Артефакт весит 353 МБ и собирается по всему
корпусу; переcобирать его после каждой карты нельзя. Дельта же мала, переживает
рестарт и обнуляется сама, когда приезжает новый снимок.

ЧТО СЧИТАЕТСЯ ЗДЕСЬ, А ЧТО НЕТ. Точно считаются СЧЁТЧИКИ — сыграно карт, карт на
герое, карт на позиции, побед, размер пула героев: они не зависят ни от какого
окна. Скользящие признаки (`imp_recent`, `kda_player`, `gpm_ewma`, `form`)
задаются пакетными скриптами `ideas_batch*` по корпусу, и повторять их на глаз
нельзя: ошибка в окне тихо испортит признак. Их досчёт — отдельная работа с
тестом на каждый против полного пересчёта.

ПРИОРЫ ПАНЕЛИ. По тем же картам считаются командные величины F6/F7/F8
(`prior_map_metrics.map_metrics`) и накладываются на снимок причинных приоров
в момент вердикта, без записи в npz. Итог карты (килы, винрейт, длительность)
есть сразу. Окна 0-10/10-20/… — только когда Stratz отдал поминутный ряд;
разбор запаздывает, поэтому неполный ряд добирается повторным запросом.

ОГОВОРКА ПРО `imp`. Stratz отдаёт свою оценку вклада игрока, и она здесь
сохраняется. Совпадает ли её шкала с колонками `imp50`/`imp30`/`imp_recent`
артефакта — НЕ проверено, поэтому величина только копится.
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

_DEFAULT_STORE_FALLBACK = (
    Path(__file__).resolve().parent.parent / "runtime" / "prematch_live_delta.json"
)
#: Совместимость: тесты и вызывающие могут патчить константу. Живой путь
#: читает `PREMATCH_LIVE_DELTA` в момент вызова, а не на импорте.
DEFAULT_STORE_PATH = _DEFAULT_STORE_FALLBACK


def _store_path(store_path: Optional[Path] = None) -> Path:
    if store_path is not None:
        return Path(store_path)
    env = os.getenv("PREMATCH_LIVE_DELTA")
    if env:
        return Path(env)
    return Path(DEFAULT_STORE_PATH)


#: Позиции у Stratz строками, в артефакте числами.
POSITION_NUM = {"POSITION_1": 1, "POSITION_2": 2, "POSITION_3": 3,
                "POSITION_4": 4, "POSITION_5": 5}
#: Дольше этого дельта не нужна: снимок пересобирают чаще, чем раз в трое суток,
#: а если нет — накопленное всё равно перестаёт быть «сегодняшним».
MAX_AGE_SECONDS = 3 * 86400
#: Повторный запрос поминутного ряда: разбор Stratz запаздывает часами.
RETRY_EVERY = 15 * 60
RETRY_LIMIT = 1

_lock = threading.Lock()


def _int_list(v: Any) -> List[int]:
    if not isinstance(v, list):
        return []
    out: List[int] = []
    for x in v:
        try:
            out.append(int(x or 0))
        except (TypeError, ValueError):
            return []
    return out


def _empty() -> Dict[str, Any]:
    return {"snapshot_ts": 0, "maps": {}}


def _load(path: Path) -> Dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return _empty()
    if not isinstance(data, dict) or not isinstance(data.get("maps"), dict):
        return _empty()
    data.setdefault("snapshot_ts", 0)
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


def _row(p: Dict[str, Any], radiant_won: bool) -> Optional[Dict[str, Any]]:
    try:
        acc = int(p.get("steamAccountId") or 0)
    except Exception:
        return None
    if acc <= 0:
        return None
    won = p.get("isVictory")
    if not isinstance(won, bool):
        won = bool(p.get("isRadiant")) == bool(radiant_won)
    is_r = p.get("isRadiant")
    if not isinstance(is_r, bool):
        is_r = bool(won) == bool(radiant_won)
    num = lambda k: int(p.get(k) or 0)
    return {"acc": acc, "hero": num("heroId"),
            "pos": POSITION_NUM.get(str(p.get("position") or ""), 0),
            "won": bool(won), "rad": bool(is_r),
            "k": num("kills"), "d": num("deaths"),
            "a": num("assists"), "lh": num("numLastHits"), "dn": num("numDenies"),
            "gpm": num("goldPerMinute"), "nw": num("networth"),
            "xpm": num("experiencePerMinute"), "lvl": num("level"),
            "hdmg": num("heroDamage"), "imp": num("imp")}


def record_map(match: Dict[str, Any], *, store_path: Optional[Path] = None,
               now: Optional[int] = None) -> int:
    """Положить карту в дельту. Возвращает число записанных игроков.

    Повторная запись той же карты ничего не меняет: ключ — match_id.
    """
    if not isinstance(match, dict):
        return 0
    try:
        mid = int(match.get("id") or 0)
    except Exception:
        return 0
    players = match.get("players")
    if mid <= 0 or not isinstance(players, list) or not players:
        return 0
    radiant_won = bool(match.get("didRadiantWin"))
    rows = [r for r in (_row(p, radiant_won) for p in players) if r]
    if not rows:
        return 0
    ts = int(now if now is not None else time.time())
    path = _store_path(store_path)
    with _lock:
        data = _load(path)
        data["maps"][str(mid)] = {
            "end": int(match.get("endDateTime") or 0),
            "start": int(match.get("startDateTime") or 0),
            "dur": int(match.get("durationSeconds") or 0),
            "league": int(match.get("leagueId") or 0),
            "radiant_won": radiant_won,
            "players": rows,
            "rk": _int_list(match.get("radiantKills")),
            "dk": _int_list(match.get("direKills")),
            "nw": _int_list(match.get("radiantNetworthLeads")),
            "xp": _int_list(match.get("radiantExperienceLeads")),
        }
        _prune(data, ts)
        _save(path, data)
    return len(rows)


def _prune(data: Dict[str, Any], now: int) -> None:
    snap = int(data.get("snapshot_ts") or 0)
    drop = [k for k, v in data["maps"].items()
            if int((v or {}).get("end") or 0) <= snap
            or now - int((v or {}).get("end") or 0) > MAX_AGE_SECONDS]
    for k in drop:
        data["maps"].pop(k, None)


def set_snapshot_ts(snapshot_ts: int, *, store_path: Optional[Path] = None,
                    now: Optional[int] = None) -> int:
    """Сдвинуть границу снимка и выбросить всё, что в него уже вошло.

    Зовётся, когда на машину приехал новый артефакт: карты старее его среза
    учитывать второй раз нельзя.
    """
    ts = int(now if now is not None else time.time())
    path = _store_path(store_path)
    with _lock:
        data = _load(path)
        before = len(data["maps"])
        data["snapshot_ts"] = int(snapshot_ts or 0)
        _prune(data, ts)
        _save(path, data)
        return before - len(data["maps"])


def player_maps(account_id: int, *, store_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Карты этого игрока после среза снимка, по возрастанию времени."""
    acc = int(account_id or 0)
    if acc <= 0:
        return []
    with _lock:
        data = _load(_store_path(store_path))
    out = []
    for mid, m in data["maps"].items():
        for r in (m.get("players") or []):
            if int(r.get("acc") or 0) == acc:
                out.append({**r, "match_id": int(mid), "end": int(m.get("end") or 0),
                            "dur": int(m.get("dur") or 0)})
    out.sort(key=lambda r: r["end"])
    return out


def counters(account_id: int, *, store_path: Optional[Path] = None) -> Dict[str, Any]:
    """Только то, что считается ТОЧНО и не зависит ни от какого окна.

    Скользящие признаки сюда не входят намеренно: их определения живут в
    пакетных скриптах, и воспроизводить их на глаз нельзя.
    """
    ms = player_maps(account_id, store_path=store_path)
    return {
        "games": len(ms),
        "wins": sum(1 for r in ms if r["won"]),
        "hero_games": dict(Counter(int(r["hero"]) for r in ms if int(r["hero"]) > 0)),
        "pos_games": dict(Counter(int(r["pos"]) for r in ms if int(r["pos"]) > 0)),
        "heroes": sorted({int(r["hero"]) for r in ms if int(r["hero"]) > 0}),
    }


def sync_to_ts(snapshot_ts: int, *, store_path: Optional[Path] = None,
               now: Optional[int] = None) -> int:
    """Сдвинуть границу дельты до среза модели. Нет файла — ничего не создавать.

    Пишущий путь (новый артефакт) может звать это явно. `score()` только
    читает через `extra_for_accounts` и файл не трогает.
    """
    path = _store_path(store_path)
    if not path.exists():
        return 0
    ts = int(snapshot_ts or 0)
    with _lock:
        cur = int(_load(path).get("snapshot_ts") or 0)
    if cur == ts:
        return 0
    return set_snapshot_ts(ts, store_path=path, now=now)


def extra_for_accounts(account_ids: Sequence[int], snapshot_ts: int,
                       *, store_path: Optional[Path] = None) -> Dict[int, Dict[str, Any]]:
    """Счётчики карт ПОСЛЕ среза снимка. Нет файла — пусто, файл не создаётся.

    Карты с `end <= snapshot_ts` игнорируются даже если граница в самом
    хранилище отстала от артефакта: иначе ночная доставка удвоит games.
    """
    path = _store_path(store_path)
    if not path.exists():
        return {}
    want = {int(a) for a in account_ids if int(a or 0) > 0}
    if not want:
        return {}
    snap = int(snapshot_ts or 0)
    with _lock:
        data = _load(path)
    out: Dict[int, Dict[str, Any]] = {
        a: {"games": 0, "hero_games": Counter(), "pos_games": Counter()}
        for a in want
    }
    for m in (data.get("maps") or {}).values():
        if not isinstance(m, dict):
            continue
        if int(m.get("end") or 0) <= snap:
            continue
        for r in (m.get("players") or []):
            acc = int((r or {}).get("acc") or 0)
            if acc not in want:
                continue
            row = out[acc]
            row["games"] += 1
            hero = int(r.get("hero") or 0)
            pos = int(r.get("pos") or 0)
            if hero > 0:
                row["hero_games"][hero] += 1
            if pos > 0:
                row["pos_games"][pos] += 1
    return {a: row for a, row in out.items() if row["games"]}


def _is_radiant(row: Dict[str, Any], radiant_won: bool) -> bool:
    if isinstance(row.get("rad"), bool):
        return bool(row["rad"])
    return bool(row.get("won")) == bool(radiant_won)


def prior_contribs(snapshot_ts: int, *,
                   store_path: Optional[Path] = None) -> List[Any]:
    """Карты после среза снимка приоров → `MapContrib` для накладки.

    Нет файла — пусто, файл не создаётся. Карта без пяти+пяти слотов
    пропускается: частичная пятёрка сдвинула бы другие ключи.
    """
    path = _store_path(store_path)
    if not path.exists():
        return []
    snap = int(snapshot_ts or 0)
    with _lock:
        data = _load(path)
    from causal_priors import MapContrib
    from prior_map_metrics import map_metrics

    out: List[Any] = []
    for m in (data.get("maps") or {}).values():
        if not isinstance(m, dict):
            continue
        if int(m.get("end") or 0) <= snap:
            continue
        players = [p for p in (m.get("players") or []) if isinstance(p, dict)]
        rad_won = bool(m.get("radiant_won"))
        rad = [p for p in players if _is_radiant(p, rad_won)]
        dire = [p for p in players if not _is_radiant(p, rad_won)]
        if len(rad) != 5 or len(dire) != 5:
            continue
        rad.sort(key=lambda p: int(p.get("pos") or 0))
        dire.sort(key=lambda p: int(p.get("pos") or 0))
        slots = rad + dire
        try:
            vr, vd, mask = map_metrics(
                rad_kills=[int(p.get("k") or 0) for p in rad],
                dire_kills=[int(p.get("k") or 0) for p in dire],
                duration_seconds=int(m.get("dur") or 0),
                radiant_won=rad_won,
                rk_inc=m.get("rk") or None,
                dk_inc=m.get("dk") or None,
                nw=m.get("nw") or None,
                xp=m.get("xp") or None)
        except Exception:
            continue
        out.append(MapContrib(
            heroes=[int(p.get("hero") or 0) for p in slots],
            accounts=[int(p.get("acc") or 0) for p in slots],
            vr=vr, vd=vd, mask=mask))
    return out


def kills_window_contribs(snapshot_ts: int, *,
                          store_path: Optional[Path] = None) -> Dict[str, Any]:
    """Приросты словаря окон килов от карт, сыгранных после его сборки.

    Ключи и накопление берутся ОФЛАЙН-ФУНКЦИЯМИ `analise_database`, а не
    повторяются здесь: грамматика ключей (`solo`, `_vs_`, `_with_`) и раскладка
    счётчиков (leads/draws/games/diff_sum/diff_sq_sum на окно) должны совпасть
    с тем, на чём словарь собран, иначе живые числа поедут против обученных.

    Окна считаются из ПОМИНУТНОГО ряда `rk`/`dk` — ровно того источника, что у
    офлайна (`_kills_window_diff`). Карта без ряда пропускается: он приходит от
    Stratz позже итога карты и добирается `retry_incomplete`.
    """
    path = _store_path(store_path)
    if not path.exists():
        return {}
    snap = int(snapshot_ts or 0)
    with _lock:
        data = _load(path)
    try:
        from analise_database import (KILLS_WINDOWS, _add_kills_window_combinations,
                                      _kills_window_diff)
    except Exception:
        return {}

    out: Dict[str, Any] = {}
    for m in (data.get("maps") or {}).values():
        if not isinstance(m, dict):
            continue
        if int(m.get("end") or 0) <= snap:
            continue
        rk, dk = m.get("rk") or [], m.get("dk") or []
        if not rk or not dk:
            continue
        players = [p for p in (m.get("players") or []) if isinstance(p, dict)]
        rad_won = bool(m.get("radiant_won"))
        rad = [p for p in players if _is_radiant(p, rad_won)]
        dire = [p for p in players if not _is_radiant(p, rad_won)]
        if len(rad) != 5 or len(dire) != 5:
            continue
        # `pos` в дельте — РОЛЬ из Stratz, уже 1..5 (`POSITION_NUM`), и токен
        # словаря строится из неё же. Сдвигать нумерацию нельзя: прибавленная
        # единица превращала `8pos1` в `8pos2`, и накладка попадала в ключ,
        # которого в словаре нет.
        r_by_pos = {int(p.get("pos") or 0): int(p.get("hero") or 0) for p in rad}
        d_by_pos = {int(p.get("pos") or 0): int(p.get("hero") or 0) for p in dire}
        if sorted(r_by_pos) != [1, 2, 3, 4, 5] or sorted(d_by_pos) != [1, 2, 3, 4, 5]:
            # Роли не размечены (у Stratz это бывает) — ключи собрались бы
            # с нулевой позицией и легли мимо словаря.
            continue
        if any(h <= 0 for h in list(r_by_pos.values()) + list(d_by_pos.values())):
            continue
        timeline = {"radiantKills": rk, "direKills": dk}
        diffs = [_kills_window_diff(timeline, s, e) for s, e in KILLS_WINDOWS]
        if all(d is None for d in diffs):
            continue
        _add_kills_window_combinations(r_by_pos, d_by_pos, out, diffs)
    return out


def rating_maps(snapshot_ts: int, *,
                store_path: Optional[Path] = None) -> List[Any]:
    """Карты после среза снимка рейтингов → `(ts, accounts10, radiant_won)`.

    `accounts10` — пять слотов радианта по позициям, затем пять дайра: ровно
    тот порядок, который разбирает `RatingState._split`.

    Отсортировано ПО ВОЗРАСТАНИЮ времени: Glicko и TrueSkill накопительные, и
    две карты, проведённые в обратном порядке, дают другой рейтинг. Карта без
    пяти+пяти слотов пропускается — неполная сторона исказила бы средние
    противника, от которых считается ожидание.
    """
    path = _store_path(store_path)
    if not path.exists():
        return []
    snap = int(snapshot_ts or 0)
    with _lock:
        data = _load(path)

    out: List[Any] = []
    for m in (data.get("maps") or {}).values():
        if not isinstance(m, dict):
            continue
        end = int(m.get("end") or 0)
        if end <= snap:
            continue
        players = [p for p in (m.get("players") or []) if isinstance(p, dict)]
        rad_won = bool(m.get("radiant_won"))
        rad = [p for p in players if _is_radiant(p, rad_won)]
        dire = [p for p in players if not _is_radiant(p, rad_won)]
        if len(rad) != 5 or len(dire) != 5:
            continue
        rad.sort(key=lambda p: int(p.get("pos") or 0))
        dire.sort(key=lambda p: int(p.get("pos") or 0))
        accounts10 = [int(p.get("acc") or 0) for p in rad + dire]
        if any(a <= 0 for a in accounts10):
            # Аноним в составе: `_split` его выбросит, и сторона станет
            # неполной уже внутри расчёта. Лучше не проводить карту вовсе.
            continue
        out.append((end, accounts10, rad_won))
    out.sort(key=lambda row: row[0])
    return out


def retry_incomplete(*, fetch, store_path: Optional[Path] = None,
                     now: Optional[int] = None,
                     limit: int = RETRY_LIMIT) -> int:
    """Дозапросить поминутный ряд у карт, записанных без него.

    `fetch(match_id)` возвращает тот же dict, что `match_players`. Нет файла —
    ничего не создавать. За раз не больше `limit` запросов.
    """
    path = _store_path(store_path)
    if not path.exists():
        return 0
    ts = int(now if now is not None else time.time())
    with _lock:
        data = _load(path)
    snap = int(data.get("snapshot_ts") or 0)
    pending: List[int] = []
    for mid, m in (data.get("maps") or {}).items():
        if not isinstance(m, dict):
            continue
        if int(m.get("end") or 0) <= snap:
            continue
        if m.get("rk"):
            continue
        if ts - int(m.get("last_retry") or 0) < RETRY_EVERY:
            continue
        try:
            pending.append(int(mid))
        except (TypeError, ValueError):
            continue
    pending.sort()
    filled = 0
    for mid in pending[:max(int(limit), 0)]:
        full = None
        try:
            full = fetch(mid)
        except Exception:
            full = None
        if isinstance(full, dict) and _int_list(full.get("radiantKills")):
            record_map(full, store_path=path, now=ts)
            filled += 1
            continue
        with _lock:
            cur = _load(path)
            rec = (cur.get("maps") or {}).get(str(mid))
            if isinstance(rec, dict):
                rec["last_retry"] = ts
                _save(path, cur)
    return filled


#: Боевой артефакт: из него берётся только `snapshot_ts` — граница, до которой
#: карты уже учтены. Значение кэшируется по времени изменения файла: сам файл
#: весит 353 МБ, и открывать его на каждой карте незачем.
DEFAULT_ARTIFACT_PATH = Path(
    os.getenv("PREMATCH_ARTIFACT",
              str(Path(__file__).resolve().parent.parent / "data" / "prematch_model_artifact_v3.npz")))
_snap_cache: Dict[str, Any] = {"mtime": None, "ts": 0}


def snapshot_ts_from_artifact(*, path: Optional[Path] = None) -> int:
    """`snapshot_ts` боевого артефакта. Ноль — не прочитали."""
    p = Path(path or DEFAULT_ARTIFACT_PATH)
    try:
        mt = p.stat().st_mtime
    except Exception:
        return 0
    if _snap_cache["mtime"] == mt:
        return int(_snap_cache["ts"])
    try:
        import numpy as np                                  # noqa: PLC0415
        with np.load(p, allow_pickle=True) as z:
            v = z["snapshot_ts"]
            ts = int(v.item() if getattr(v, "shape", ()) == () else v.max())
    except Exception:
        return 0
    _snap_cache["mtime"], _snap_cache["ts"] = mt, ts
    return ts


def sync_snapshot(*, artifact_path: Optional[Path] = None,
                  store_path: Optional[Path] = None,
                  now: Optional[int] = None) -> int:
    """Подтянуть границу из артефакта, если он сменился. Вернёт число выброшенных.

    Зовётся перед записью карты: если на машину приехал новый снимок, всё, что в
    него уже вошло, обязано уйти из дельты, иначе счётчики удвоятся.
    """
    ts = snapshot_ts_from_artifact(path=artifact_path)
    if ts <= 0:
        return 0
    with _lock:
        cur = int(_load(_store_path(store_path)).get("snapshot_ts") or 0)
    if cur == ts:
        return 0
    return set_snapshot_ts(ts, store_path=store_path, now=now)
