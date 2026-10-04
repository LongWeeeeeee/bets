#!/usr/bin/env python3
"""Компактная выжимка про-корпуса: id, время, герои, аккаунты, команды, исход.

Корпус вырос до 258 794 матчей и 1.9 ГБ; грузить его целиком в память под каждый
замер больше нельзя. Здесь он один раз проходится потоково и складывается в
массивы numpy — дальше все расчёты идут по ним.

Берётся только предматчевое плюс исход: героев по позициям, account id по
слотам, id команд, время старта, победа радианта. Никаких игровых показателей.
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
OUT = ROOT / "runtime/artifacts/misc/pro_corpus_compact.npz"
POS = {f"POSITION_{i}": i for i in range(1, 6)}


def main() -> None:
    mids, ts, wins, heroes, accounts, teams, leagues = [], [], [], [], [], [], []
    seen: set[str] = set()
    files = sorted(glob.glob(str(CORPUS / "*_part*.json")) +
                   glob.glob(str(CORPUS / "*_part*.json.gz")),
                   key=lambda p: p[:-3] if p.endswith(".gz") else p)
    print(f"файлов: {len(files)}", flush=True)
    for n, path in enumerate(files, 1):
        try:
            with (gzip.open(path, "rb") if path.endswith(".gz") else open(path, "rb")) as fh:
                data = json.load(fh)
        except Exception as exc:
            print(f"  пропуск {os.path.basename(path)}: {exc}", flush=True)
            continue
        for key, m in data.items():
            if not isinstance(m, dict):
                continue
            mid = str(m.get("id") or key)
            if mid in seen:                      # дедуп по match_id (E-70)
                continue
            won, start = m.get("didRadiantWin"), m.get("startDateTime")
            if not isinstance(won, bool) or not start:
                continue
            sides, accs = {True: {}, False: {}}, {True: {}, False: {}}
            ok = True
            for p in m.get("players") or []:
                pos, hero, is_r = POS.get(p.get("position")), p.get("heroId"), p.get("isRadiant")
                if not pos or hero is None or is_r is None:
                    ok = False
                    break
                sides[bool(is_r)][pos] = int(hero)
                a = (p.get("steamAccount") or {}).get("id")
                accs[bool(is_r)][pos] = int(a) if a else 0
            if not ok or len(sides[True]) != 5 or len(sides[False]) != 5:
                continue
            seen.add(mid)
            mids.append(int(mid))
            ts.append(int(start))
            wins.append(int(won))
            heroes.append([sides[True][i] for i in range(1, 6)] + [sides[False][i] for i in range(1, 6)])
            accounts.append([accs[True].get(i, 0) for i in range(1, 6)] +
                            [accs[False].get(i, 0) for i in range(1, 6)])
            teams.append([int((m.get("radiantTeam") or {}).get("id") or 0),
                          int((m.get("direTeam") or {}).get("id") or 0)])
            lg = m.get("league") or {}
            leagues.append(int(m.get("leagueId") or lg.get("id") or 0))
        del data
        if n % 20 == 0 or n == len(files):
            print(f"  {n}/{len(files)} файлов, матчей {len(mids):,}", flush=True)

    order = np.argsort(np.asarray(ts), kind="stable")      # хронологически
    OUT.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(
        OUT,
        mids=np.asarray(mids, dtype=np.int64)[order],
        ts=np.asarray(ts, dtype=np.int64)[order],
        wins=np.asarray(wins, dtype=np.int8)[order],
        heroes=np.asarray(heroes, dtype=np.int32)[order],
        accounts=np.asarray(accounts, dtype=np.int64)[order],
        teams=np.asarray(teams, dtype=np.int64)[order],
        leagues=np.asarray(leagues, dtype=np.int64)[order],
    )
    print(f"готово: {OUT} ({len(mids):,} матчей с позициями)", flush=True)


if __name__ == "__main__":
    main()
