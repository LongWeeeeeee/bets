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
игроков герой, позиция, исход и его строчная статистика. Ничего не подставляет в
признаки — это отдельный шаг, и он требует точных определений окон.

ПОЧЕМУ НАКЛАДКА, А НЕ ПЕРЕСБОРКА. Артефакт весит 353 МБ и собирается по всему
корпусу; переcобирать его после каждой карты нельзя. Дельта же мала, переживает
рестарт и обнуляется сама, когда приезжает новый снимок.

ЧТО СЧИТАЕТСЯ ЗДЕСЬ, А ЧТО НЕТ. Точно считаются СЧЁТЧИКИ — сыграно карт, карт на
герое, карт на позиции, побед, размер пула героев: они не зависят ни от какого
окна. Скользящие признаки (`imp_recent`, `kda_player`, `gpm_ewma`, `form`)
задаются пакетными скриптами `ideas_batch*` по корпусу, и повторять их на глаз
нельзя: ошибка в окне тихо испортит признак. Их досчёт — отдельная работа с
тестом на каждый против полного пересчёта.

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
from typing import Any, Dict, List, Optional

DEFAULT_STORE_PATH = Path(
    os.getenv("PREMATCH_LIVE_DELTA",
              str(Path(__file__).resolve().parent.parent / "runtime" / "prematch_live_delta.json"))
)
#: Позиции у Stratz строками, в артефакте числами.
POSITION_NUM = {"POSITION_1": 1, "POSITION_2": 2, "POSITION_3": 3,
                "POSITION_4": 4, "POSITION_5": 5}
#: Дольше этого дельта не нужна: снимок пересобирают чаще, чем раз в трое суток,
#: а если нет — накопленное всё равно перестаёт быть «сегодняшним».
MAX_AGE_SECONDS = 3 * 86400

_lock = threading.Lock()


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
    num = lambda k: int(p.get(k) or 0)
    return {"acc": acc, "hero": num("heroId"),
            "pos": POSITION_NUM.get(str(p.get("position") or ""), 0),
            "won": bool(won), "k": num("kills"), "d": num("deaths"),
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
    path = Path(store_path or DEFAULT_STORE_PATH)
    with _lock:
        data = _load(path)
        data["maps"][str(mid)] = {
            "end": int(match.get("endDateTime") or 0),
            "start": int(match.get("startDateTime") or 0),
            "dur": int(match.get("durationSeconds") or 0),
            "league": int(match.get("leagueId") or 0),
            "radiant_won": radiant_won,
            "players": rows,
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
    path = Path(store_path or DEFAULT_STORE_PATH)
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
        data = _load(Path(store_path or DEFAULT_STORE_PATH))
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
        cur = int(_load(Path(store_path or DEFAULT_STORE_PATH)).get("snapshot_ts") or 0)
    if cur == ts:
        return 0
    return set_snapshot_ts(ts, store_path=store_path, now=now)
