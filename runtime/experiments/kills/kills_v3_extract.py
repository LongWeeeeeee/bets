#!/usr/bin/env python3
"""Разбор корпусов под килы v3: то же, что v2, плюс поля, которых не хватило.

Что добавлено против `window_features_v2.py` и ЗАЧЕМ (каждое поле куплено конкретной
незакрытой задачей, а не «пусть будет»):

* `kills_side` — итог килов карты по сторонам. Без него нельзя построить цель
  «команда наберёт >= 27 килов» (прод-модель `team_kills25_shadow.py`) и цель
  «тотал карты >= 51», к которой E-164 отправила гипотезу про кровавую карту.
  В v2 сохранялись только оконные суммы, итога не было вовсе;
* `nwlead` / `xplead` — поминутный перевес по нетворсу и опыту на минутах гейтов
  прод-политики (3, 8, 13, 18 → индексы 2/7/12/17). E-165 закрылась на том, что
  «нужны `nw_t_*`, которых в наборе нет, они есть только в дампе на 1096 карт».
  Они есть в самом корпусе: `radiantNetworthLeads` — поминутный массив, длина
  которого совпадает с `radiantKills`. Это открывает прод-политику на всех
  28 тыс. карт патча вместо 1096;
* `patch` — код патча из имени файла (7.41e -> 74105). Нужен, чтобы обучать драфт
  ПРО только на 7.41+, как просил alex, а форму игроков брать по всей истории;
* `league`, `tier`, `teams` — лига, её тир и id команд (только про). Тир нужен для
  среза Tier-1, id команд — для командной формы, которой в v2 не было;
* `imp` игрока — 20-я величина. Stratz отдаёт её в корпусе (диапазон -58..59,
  пропусков ~6%), а в v2 разбор её выбрасывал.

Всё остальное — как в v2, чтобы числа были сравнимы. `small` вырос с 5 полей до 6
(добавлен imp), поэтому шарды пишутся под ДРУГИМ именем: `rowsv3_*`, старые
`rows_*` не трогаются и E-165 остаётся воспроизводимой.

Запуск (шард = каждый K-й файл корпуса):
    python3 kills_v3_extract.py --corpus public --shard 0 --shards 5
    python3 kills_v3_extract.py --corpus pro --shard 0 --shards 2
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))

from train_public_draft_hero10_experiment import iter_json_objects, _int  # noqa: E402

CORPUS = {
    "public": ROOT / "bets_data/analise_pub_matches/json_parts_split_from_object",
    "pro": ROOT / "pro_heroes_data/json_parts_split_from_object",
}
OUT_DIR = ROOT / "runtime/artifacts/kills/window_model_v2"
WINDOWS = ((5, 15), (10, 20), (15, 25), (20, 30))
# Минуты, на которых прод-политика проверяет NW-гейт: START-2 для каждого окна,
# а читатель берёт `nw_t_{minute-1}` (kills_policy_fixed.nw_at) -> индекс minute-1.
NW_IDX = (2, 7, 12, 17)
IMP_NA = -32768
TIERS = ("UNKNOWN", "AMATEUR", "PROFESSIONAL", "MINOR", "MAJOR", "INTERNATIONAL",
         "PREMIUM", "DIVISION_ONE", "DIVISION_TWO")
_PATCH_RE = re.compile(r"^(\d+)\.(\d+)([a-z]?)")


def patch_code(name: str) -> int:
    """'7.41e_part003.json' -> 74105; 'historical_part1.json' -> 0 (доберём по времени)."""
    m = _PATCH_RE.match(name)
    if not m:
        return 0
    major, minor, letter = int(m.group(1)), int(m.group(2)), m.group(3)
    return major * 10000 + minor * 100 + (ord(letter) - 96 if letter else 0)


def shard_path(corpus: str, shard: int) -> Path:
    return OUT_DIR / f"rowsv3_{corpus}.shard{shard}.npz"


def _lead_at(arr, idx: int) -> int:
    if not isinstance(arr, list) or idx >= len(arr):
        return 0
    v = _int(arr[idx])
    return 0 if v is None else int(v)


def parse(raw):
    """Компактная строка матча либо None. Порядок слотов: radiant pos1-5, dire pos1-5."""
    if not isinstance(raw, dict):
        return None
    start = _int(raw.get("startDateTime"))
    if start is None or start <= 0:
        return None
    players = raw.get("players")
    if not isinstance(players, list) or len(players) != 10:
        return None
    slots: dict[bool, dict[int, dict]] = {True: {}, False: {}}
    seen_heroes = set()
    for p in players:
        if not isinstance(p, dict):
            return None
        side, pos = p.get("isRadiant"), p.get("position")
        if not isinstance(side, bool) or not isinstance(pos, str) or not pos.startswith("POSITION_"):
            return None
        position = _int(pos.removeprefix("POSITION_"))
        hero = _int(p.get("heroId"))
        if position not in range(1, 6) or hero is None or not 0 < hero < 65536:
            return None
        if position in slots[side] or hero in seen_heroes:
            return None
        slots[side][position] = p
        seen_heroes.add(hero)
    if any(set(slots[s]) != set(range(1, 6)) for s in (True, False)):
        return None

    win = raw.get("didRadiantWin")
    if not isinstance(win, bool):
        return None
    r, d = raw.get("radiantKills"), raw.get("direKills")
    if not isinstance(r, list) or not isinstance(d, list):
        return None
    try:
        r = [int(x) for x in r]
        d = [int(x) for x in d]
    except (TypeError, ValueError):
        return None
    if any(x < 0 for x in r + d):
        return None

    diffs, totals, valid = [], [], []
    for start_min, end_min in WINDOWS:
        ok = len(r) > end_min and len(d) > end_min
        valid.append(ok)
        rk = sum(r[start_min:end_min]) if ok else 0
        dk = sum(d[start_min:end_min]) if ok else 0
        diffs.append(rk - dk)
        totals.append(rk + dk)
    if not any(valid):
        return None
    # Итог карты: `radiantKills` — ПОМИНУТНЫЙ массив (раздел 2б журнала), сумма и есть счёт.
    kills_side = [sum(r), sum(d)]

    heroes, accounts, kda, small, big = [], [], [], [], []
    for side in (True, False):
        for position in range(1, 6):
            p = slots[side][position]
            heroes.append(_int(p.get("heroId")))
            acc = p.get("steamAccount")
            aid = _int(acc.get("id")) if isinstance(acc, dict) else None
            anon = bool(acc.get("isAnonymous")) if isinstance(acc, dict) else True
            accounts.append(0 if (aid is None or anon or aid <= 0) else aid)
            kda.append([_int(p.get("kills")) or 0, _int(p.get("deaths")) or 0,
                        _int(p.get("assists")) or 0])
            imp = _int(p.get("imp"))
            small.append([_int(p.get("numLastHits")) or 0, _int(p.get("numDenies")) or 0,
                          _int(p.get("level")) or 0, _int(p.get("goldPerMinute")) or 0,
                          _int(p.get("experiencePerMinute")) or 0,
                          IMP_NA if imp is None else max(-30000, min(30000, imp))])
            big.append([_int(p.get("networth")) or 0, _int(p.get("heroDamage")) or 0,
                        _int(p.get("heroHealing")) or 0, _int(p.get("towerDamage")) or 0])

    nw_arr = raw.get("radiantNetworthLeads")
    xp_arr = raw.get("radiantExperienceLeads")
    nwlead = [_lead_at(nw_arr, i) for i in NW_IDX]
    xplead = [_lead_at(xp_arr, i) for i in NW_IDX]
    # Массивы лидов обязаны совпадать по длине с массивом килов; иначе индексы
    # означают не те минуты и гейт молча считается по чужому времени.
    nw_ok = isinstance(nw_arr, list) and len(nw_arr) == len(r)

    league = _int(raw.get("leagueId")) or 0
    lg = raw.get("league")
    tier = 0
    if isinstance(lg, dict):
        t = str(lg.get("tier") or "").strip().upper()
        tier = TIERS.index(t) if t in TIERS else 0
    teams = []
    for key in ("radiantTeam", "direTeam"):
        t = raw.get(key)
        teams.append(_int(t.get("id")) if isinstance(t, dict) else 0)
    teams = [x or 0 for x in teams]

    dur = _int(raw.get("durationSeconds")) or 0
    return (heroes, accounts, kda, small, big, start, dur, int(win), diffs, totals,
            valid, kills_side, nwlead, xplead, int(nw_ok), league, tier, teams)


def extract(corpus: str, shard: int, shards: int) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    root = CORPUS[corpus]
    files = sorted(p for p in root.glob("*.json") if p.name != "merge_patch_summary.json")
    mine = [p for i, p in enumerate(files) if i % shards == shard]
    print(f"[{corpus}] шард {shard}: {len(mine)} файлов из {len(files)}", flush=True)

    parts: list[dict[str, np.ndarray]] = []
    total = 0
    for path in mine:
        pc = patch_code(path.name)
        H, A, K, S, B, T, D, W, DF, TT, V, M = ([] for _ in range(12))
        KS, NW, XP, NOK, LG, TR, TM = ([] for _ in range(7))
        for key, raw in iter_json_objects(path):
            got = parse(raw)
            if got is None:
                continue
            (h, a, k, sm, bg, t, dur, win, df, tt, v, ks, nw, xp, nok, lg, tr, tm) = got
            H.append(h); A.append(a); K.append(k); S.append(sm); B.append(bg)
            T.append(t); D.append(dur); W.append(win)
            DF.append(df); TT.append(tt); V.append(v)
            KS.append(ks); NW.append(nw); XP.append(xp); NOK.append(nok)
            LG.append(lg); TR.append(tr); TM.append(tm)
            M.append(_int(key) or _int(raw.get("id")) or 0)
        if not M:
            print(f"  {path.name}: +0", flush=True)
            continue
        parts.append({
            "mid": np.asarray(M, dtype=np.int64),
            "heroes": np.asarray(H, dtype=np.int32),
            "accounts": np.asarray(A, dtype=np.int64),
            "kda": np.asarray(K, dtype=np.int16),
            "small": np.asarray(S, dtype=np.int16),
            "big": np.asarray(B, dtype=np.int32),
            "ts": np.asarray(T, dtype=np.int64),
            "duration": np.asarray(D, dtype=np.int32),
            "wins": np.asarray(W, dtype=np.int8),
            "diffs": np.asarray(DF, dtype=np.int16),
            "totals": np.asarray(TT, dtype=np.int16),
            "valid": np.asarray(V, dtype=bool),
            "kills_side": np.asarray(KS, dtype=np.int16),
            "nwlead": np.asarray(NW, dtype=np.int32),
            "xplead": np.asarray(XP, dtype=np.int32),
            "nwok": np.asarray(NOK, dtype=np.int8),
            "league": np.asarray(LG, dtype=np.int64),
            "tier": np.asarray(TR, dtype=np.int8),
            "teams": np.asarray(TM, dtype=np.int64),
            "patch": np.full(len(M), pc, dtype=np.int32),
        })
        total += len(M)
        del H, A, K, S, B, T, D, W, DF, TT, V, M, KS, NW, XP, NOK, LG, TR, TM
        print(f"  {path.name}: +{len(parts[-1]['mid']):,} (всего {total:,})", flush=True)

    if not parts:
        print(f"[{corpus}] шард {shard}: пусто", flush=True)
        return
    merged = {k: np.concatenate([p[k] for p in parts]) for k in parts[0]}
    np.savez_compressed(shard_path(corpus, shard), **merged)
    print(f"[{corpus}] шард {shard} готов: {total:,} строк -> {shard_path(corpus, shard)}",
          flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", default="public", choices=sorted(CORPUS))
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    args = ap.parse_args()
    extract(args.corpus, args.shard, args.shards)


if __name__ == "__main__":
    main()
