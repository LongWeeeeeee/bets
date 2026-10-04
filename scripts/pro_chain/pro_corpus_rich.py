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


def main() -> None:
    mids, ts, wins, teams, leagues, regions = [], [], [], [], [], []
    sids, stypes, durs, fbs, lanes = [], [], [], [], []
    nw, xp, rk, dk, wrs = [], [], [], [], []
    towers, pstats, heroes, accounts = [], [], [], []
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
            mids.append(int(mid))
            ts.append(int(start))
            wins.append(int(won))
            teams.append([int((m.get("radiantTeam") or {}).get("id") or 0),
                          int((m.get("direTeam") or {}).get("id") or 0)])
            leagues.append(int(m.get("leagueId") or (m.get("league") or {}).get("id") or 0))
            regions.append(int(m.get("regionId") or 0))
            ser = m.get("series") or {}
            sids.append(int(ser.get("id") or 0))
            stypes.append(STYPE.get(str(ser.get("type")), 0))
            durs.append(int(m.get("durationSeconds") or 0))
            fbs.append(int(m.get("firstBloodTime") or 0))
            lanes.append([lane_code(m.get("topLaneOutcome")), lane_code(m.get("midLaneOutcome")),
                          lane_code(m.get("bottomLaneOutcome"))])
            nw.append(pad(m.get("radiantNetworthLeads"), MIN))
            xp.append(pad(m.get("radiantExperienceLeads"), MIN))
            rk.append(pad(m.get("radiantKills"), MIN, cum=True))
            dk.append(pad(m.get("direKills"), MIN, cum=True))
            w = np.zeros(WR_LEN, dtype=np.float32)
            wv = m.get("winRates") or []
            for i2 in range(min(len(wv), WR_LEN)):
                try:
                    w[i2] = float(wv[i2])
                except (TypeError, ValueError):
                    pass
            wrs.append(w)
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
            towers.append(np.concatenate([[first_t, first_s], cnt]).astype(np.int32))
            heroes.append([side_h[True][i] for i in range(1, 6)] + [side_h[False][i] for i in range(1, 6)])
            accounts.append([side_a[True][i] for i in range(1, 6)] + [side_a[False][i] for i in range(1, 6)])
            pstats.append([side_p[True][i] for i in range(1, 6)] + [side_p[False][i] for i in range(1, 6)])
        del data
        if n % 20 == 0 or n == len(files):
            print(f"  {n}/{len(files)} файлов, матчей {len(mids):,}", flush=True)

    order = np.argsort(np.asarray(ts), kind="stable")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(
        OUT,
        mids=np.asarray(mids, dtype=np.int64)[order],
        ts=np.asarray(ts, dtype=np.int64)[order],
        wins=np.asarray(wins, dtype=np.int8)[order],
        teams=np.asarray(teams, dtype=np.int64)[order],
        leagues=np.asarray(leagues, dtype=np.int64)[order],
        regions=np.asarray(regions, dtype=np.int16)[order],
        sids=np.asarray(sids, dtype=np.int64)[order],
        stypes=np.asarray(stypes, dtype=np.int8)[order],
        durations=np.asarray(durs, dtype=np.int32)[order],
        first_blood=np.asarray(fbs, dtype=np.int32)[order],
        lanes=np.asarray(lanes, dtype=np.int8)[order],
        nw=np.asarray(nw, dtype=np.int32)[order],
        xp=np.asarray(xp, dtype=np.int32)[order],
        rk=np.asarray(rk, dtype=np.int16)[order],
        dk=np.asarray(dk, dtype=np.int16)[order],
        winrates=np.asarray(wrs, dtype=np.float32)[order],
        towers=np.asarray(towers, dtype=np.int32)[order],
        heroes=np.asarray(heroes, dtype=np.int32)[order],
        accounts=np.asarray(accounts, dtype=np.int64)[order],
        pstats=np.asarray(pstats, dtype=np.float32)[order],
        pstat_names=np.asarray(PSTAT),
    )
    print(f"готово: {OUT} ({len(mids):,} матчей)", flush=True)


if __name__ == "__main__":
    main()
