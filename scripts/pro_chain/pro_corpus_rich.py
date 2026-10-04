#!/usr/bin/env python3
"""Богатая выжимка про-корпуса: игровые показатели, ряды по минутам, объекты.

`pro_corpus_compact.npz` содержит только предматчевое (герои, аккаунты, команды,
исход). Половина идей из `ideas` — про ИСТОРИЧЕСКИЕ игровые показатели команды и
игрока (форма по gpm, конвертация лида, камбек-профиль, распределение ресурсов,
кривые силы). Всё это лежит в сыром корпусе, но каждый замер платить 4 минуты за
проход по 3.9 ГБ не должен.

Здесь один проход вынимает всё сразу:
  * матч: длительность, первая кровь, регион, лига, серия, исходы линий;
  * ряды по минутам 0..40: перевес по нетворту и опыту, накопленные килы сторон;
  * `winRates` Stratz (generic-модель состояния, 30 значений) — база сравнения E-85;
  * башни: время и сторона первой, счётчики павших башен на 15/20/25/30-й минуте;
  * игрок (10 слотов в порядке рад pos1-5, dire pos1-5): 14 показателей.

ВАЖНО про утечку: всё это ИСХОДЫ матча. В признаки они идут только как ИСТОРИЯ
(агрегаты по прошлым матчам игрока/команды), никогда как значения текущего.

Запуск: venv_catboost/bin/python3 scripts/pro_chain/pro_corpus_rich.py
Выход:  runtime/artifacts/misc/pro_corpus_rich.npz
"""
from __future__ import annotations

import glob
import gzip
import json
import os
import tempfile
import zipfile
from contextlib import ExitStack
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT") or Path(__file__).resolve().parents[2])
CORPUS = ROOT / "pro_heroes_data/json_parts_split_from_object"
OUT = ROOT / "runtime/artifacts/misc/pro_corpus_rich.npz"
POS = {f"POSITION_{i}": i for i in range(1, 6)}
MIN = 41                                  # минуты 0..40
WR_LEN = 31
PSTAT = ["kills", "deaths", "assists", "goldPerMinute", "experiencePerMinute", "networth",
         "numLastHits", "numDenies", "heroDamage", "towerDamage", "heroHealing", "imp",
         "level", "dotaPlusHeroXp"]
STYPE = {"BEST_OF_ONE": 1, "BEST_OF_TWO": 2, "BEST_OF_THREE": 3, "BEST_OF_FIVE": 5}
TOWER_MARKS = (15, 20, 25, 30)


def lane_code(v) -> int:
    s = str(v or "")
    sign = 1 if "RADIANT" in s else (-1 if "DIRE" in s else 0)
    return sign * (2 if "STOMP" in s else 1)


def pad(arr, n, cum=False):
    out = np.zeros(n, dtype=np.int32)
    if not arr:
        return out
    a = np.asarray([int(x or 0) for x in arr], dtype=np.int64)
    if cum:
        a = np.cumsum(a)
    k = min(len(a), n)
    out[:k] = a[:k]
    if k < n:
        out[k:] = a[k - 1] if k else 0
    return out


# Output order, dtype and trailing dimensions are the legacy NPZ contract.
FIELDS = {
    "mids": (np.int64, ()), "ts": (np.int64, ()),
    "wins": (np.int8, ()), "teams": (np.int64, (2,)),
    "leagues": (np.int64, ()), "regions": (np.int16, ()),
    "sids": (np.int64, ()), "stypes": (np.int8, ()),
    "durations": (np.int32, ()), "first_blood": (np.int32, ()),
    "lanes": (np.int8, (3,)), "nw": (np.int32, (MIN,)),
    "xp": (np.int32, (MIN,)), "rk": (np.int16, (MIN,)),
    "dk": (np.int16, (MIN,)), "winrates": (np.float32, (WR_LEN,)),
    "towers": (np.int32, (10,)), "heroes": (np.int32, (10,)),
    "accounts": (np.int64, (10,)), "pstats": (np.float32, (10, len(PSTAT))),
}


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    # Keep only one parsed file in RAM. Raw field spools also avoid holding all
    # unsorted and sorted arrays at once. Same filesystem for atomic publication.
    with tempfile.TemporaryDirectory(prefix=".pro_corpus_rich-", dir=OUT.parent) as tmp:
        with ExitStack() as stack:
            _build(Path(tmp), stack)


def _build(tmp, stack) -> None:
    spools = {name: stack.enter_context(open(tmp / name, "w+b")) for name in FIELDS}
    total = 0
    seen: set[str] = set()
    files = sorted(glob.glob(str(CORPUS / "*_part*.json")) +
                   glob.glob(str(CORPUS / "*_part*.json.gz")),
                   key=lambda p: p[:-3] if p.endswith(".gz") else p)
    for n, path in enumerate(files, 1):
        try:
            with (gzip.open(path, "rb") if path.endswith(".gz") else open(path, "rb")) as fh:
                data = json.load(fh)
        except Exception:
            continue
        rows = {name: np.empty((len(data),) + shape, dtype=dtype)
                for name, (dtype, shape) in FIELDS.items()}
        count = 0
        for key, m in data.items():
            if not isinstance(m, dict):
                continue
            mid = str(m.get("id") or key)
            if mid in seen:
                continue
            won, start = m.get("didRadiantWin"), m.get("startDateTime")
            if not isinstance(won, bool) or not start:
                continue
            side_h, side_a, side_p = {True: {}, False: {}}, {True: {}, False: {}}, {True: {}, False: {}}
            ok = True
            for p in m.get("players") or []:
                pos, hero, is_r = POS.get(p.get("position")), p.get("heroId"), p.get("isRadiant")
                if not pos or hero is None or is_r is None:
                    ok = False
                    break
                side_h[bool(is_r)][pos] = int(hero)
                side_a[bool(is_r)][pos] = int((p.get("steamAccount") or {}).get("id") or 0)
                side_p[bool(is_r)][pos] = [float(p.get(k) or 0.0) for k in PSTAT]
            if not ok or len(side_h[True]) != 5 or len(side_h[False]) != 5:
                continue
            seen.add(mid)
            rows["mids"][count] = int(mid)
            rows["ts"][count] = int(start)
            rows["wins"][count] = int(won)
            rows["teams"][count] = [int((m.get("radiantTeam") or {}).get("id") or 0),
                          int((m.get("direTeam") or {}).get("id") or 0)]
            rows["leagues"][count] = int(m.get("leagueId") or (m.get("league") or {}).get("id") or 0)
            rows["regions"][count] = int(m.get("regionId") or 0)
            ser = m.get("series") or {}
            rows["sids"][count] = int(ser.get("id") or 0)
            rows["stypes"][count] = STYPE.get(str(ser.get("type")), 0)
            rows["durations"][count] = int(m.get("durationSeconds") or 0)
            rows["first_blood"][count] = int(m.get("firstBloodTime") or 0)
            rows["lanes"][count] = [lane_code(m.get("topLaneOutcome")), lane_code(m.get("midLaneOutcome")),
                          lane_code(m.get("bottomLaneOutcome"))]
            rows["nw"][count] = pad(m.get("radiantNetworthLeads"), MIN)
            rows["xp"][count] = pad(m.get("radiantExperienceLeads"), MIN)
            rows["rk"][count] = pad(m.get("radiantKills"), MIN, cum=True)
            rows["dk"][count] = pad(m.get("direKills"), MIN, cum=True)
            w = np.zeros(WR_LEN, dtype=np.float32)
            wv = m.get("winRates") or []
            for i2 in range(min(len(wv), WR_LEN)):
                try:
                    w[i2] = float(wv[i2])
                except (TypeError, ValueError):
                    pass
            rows["winrates"][count] = w
            td = m.get("towerDeaths") or []
            first_t, first_s = 0, 0
            cnt = np.zeros(2 * len(TOWER_MARKS), dtype=np.int16)   # [rad@15,20,25,30, dire@...]
            for ev in td:
                if not isinstance(ev, dict):
                    continue
                t = int(ev.get("time") or 0)
                is_r = bool(ev.get("isRadiant"))
                if first_t == 0 or t < first_t:
                    first_t, first_s = t, (1 if is_r else -1)
                for j, mark in enumerate(TOWER_MARKS):
                    if t <= mark * 60:
                        cnt[(0 if is_r else len(TOWER_MARKS)) + j] += 1
            rows["towers"][count] = np.concatenate([[first_t, first_s], cnt]).astype(np.int32)
            rows["heroes"][count] = [side_h[True][i] for i in range(1, 6)] + [side_h[False][i] for i in range(1, 6)]
            rows["accounts"][count] = [side_a[True][i] for i in range(1, 6)] + [side_a[False][i] for i in range(1, 6)]
            rows["pstats"][count] = [side_p[True][i] for i in range(1, 6)] + [side_p[False][i] for i in range(1, 6)]
            count += 1
        for name in FIELDS:
            rows[name][:count].tofile(spools[name])
        total += count
        del rows, data
        if n % 20 == 0 or n == len(files):
            print(f"  {n}/{len(files)} файлов, матчей {total:,}", flush=True)

    del seen
    spools["ts"].seek(0)
    timestamps = np.fromfile(spools["ts"], dtype=np.int64)
    order = np.argsort(timestamps, kind="stable")
    del timestamps
    output = tmp / "output.npz"
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, (dtype, shape) in FIELDS.items():
            spools[name].seek(0)
            values = np.fromfile(spools[name], dtype=dtype)
            # Legacy np.asarray([]) yields (0,), even for normally matrix fields.
            if total:
                values = values.reshape((total,) + shape)
            ordered = values[order]
            del values
            with archive.open(name + ".npy", "w", force_zip64=True) as fh:
                np.lib.format.write_array(fh, ordered, allow_pickle=False)
            del ordered
        with archive.open("pstat_names.npy", "w", force_zip64=True) as fh:
            np.lib.format.write_array(fh, np.asarray(PSTAT), allow_pickle=False)
    os.replace(output, OUT)
    print(f"готово: {OUT} ({total:,} матчей)", flush=True)


if __name__ == "__main__":
    main()
