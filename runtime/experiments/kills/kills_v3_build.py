#!/usr/bin/env python3
"""Признаки v3 из шардов `kills_v3_extract.py`: разницы + УРОВНИ + командная форма.

Против v2 три содержательные правки, каждая под конкретную цель.

1. **Уровневые колонки (`_lvl`).** В v2 любой признак — разница сторон. Для целей
   «кто больше» этого достаточно: они антисимметричны. Но цели «сторона наберёт
   >= 27 килов» и «тотал карты >= 51» от разницы почти не зависят — они зависят от
   ОБЩЕЙ кровавости карты. Разница радиант-дайр одинакова у матча 30:5 и 8:3,
   а ответ на «>= 27» противоположный. Поэтому для отобранных величин добавлена
   вторая колонка — среднее ДВУХ сторон.
2. **Оконный тотал игрока (`pl_wtot_*`).** В v2 у игрока был только оконный
   ПЕРЕВЕС. Средний тотал килов в этом окне у игрока — прямая мера кровавости.
3. **Командная форма (только про).** id команд есть в про-корпусе; в паблике их
   нет. Блок отдельный и в общий набор НЕ входит: иначе паблик-модель училась бы
   на константе, а на про эта же колонка внезапно оживала бы — типичная тихая
   поломка переноса. Про-модель получает его как дополнительный вход.

Плюс 20-я величина игрока (`imp`) и патч/лига/тир/команды в выходном файле.

Что НЕ входит по построению: ничего, что известно только по ходу карты. Нетворс,
опыт и итог сохраняются РЯДОМ с признаками (для гейтов прод-политики и целей),
но в матрицу X не попадают — модель должна работать на 00:00.

Запуск:
    python3 kills_v3_build.py --corpus public
    python3 kills_v3_build.py --corpus pro
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))
sys.path.insert(0, str(ROOT / "runtime/experiments/kills"))

from window_model_v2 import TrailingIndex, team_mean  # noqa: E402
from kills_v3_extract import IMP_NA, WINDOWS  # noqa: E402

OUT_DIR = ROOT / "runtime/artifacts/kills/window_model_v2"
PLAYER_WINDOWS = (5, 10, 30)
HERO_RECENT = 500
TEAM_WINDOWS = (10, 30)

# Величины игрока: индекс в массиве шарда.
SRC = {
    "kills": ("kda", 0), "deaths": ("kda", 1), "assists": ("kda", 2),
    "lasthits": ("small", 0), "denies": ("small", 1), "level": ("small", 2),
    "gpm": ("small", 3), "xpm": ("small", 4), "imp": ("small", 5),
    "networth": ("big", 0), "herodmg": ("big", 1),
    "healing": ("big", 2), "towerdmg": ("big", 3),
}
SHARES = {
    "kp": ("kills+assists", "kills"), "dshare": ("deaths", "deaths"),
    "dmgshare": ("herodmg", "herodmg"), "towershare": ("towerdmg", "towerdmg"),
    "healshare": ("healing", "healing"), "nwshare": ("networth", "networth"),
}
BASE_STATS = ("kills", "deaths", "assists", "kp", "dshare", "win")
EXTRA_STATS = ("lasthits", "denies", "level", "gpm", "xpm", "imp", "networth",
               "herodmg", "healing", "towerdmg", "dmgshare", "towershare",
               "healshare", "nwshare")
# Уровень (среднее двух сторон) считается там, где он несёт кровавость/темп карты.
LEVEL_STATS = ("kills", "deaths", "assists", "kp", "lasthits", "gpm", "xpm",
               "networth", "herodmg", "level", "imp")


def load_rows(corpus: str) -> dict[str, np.ndarray]:
    parts = sorted(OUT_DIR.glob(f"rowsv3_{corpus}.shard*.npz"))
    if not parts:
        raise SystemExit(f"нет шардов rowsv3_{corpus} — сначала kills_v3_extract.py")
    acc: dict[str, list[np.ndarray]] = {}
    for p in parts:
        z = np.load(p)
        for k in z.files:
            acc.setdefault(k, []).append(z[k])
    data = {k: np.concatenate(v) for k, v in acc.items()}
    order = np.argsort(data["ts"], kind="stable")
    data = {k: v[order] for k, v in data.items()}
    # Дедуп по mid обязателен: в про-корпусе 5.6% строк — копии одного матча
    # (собран живьём и историческим добором) с тем же ts. В скользящем среднем
    # копия видит копию, то есть матч попадает в собственную историю (E-165).
    _, first = np.unique(data["mid"], return_index=True)
    keep = np.sort(first)
    if len(keep) != len(data["mid"]):
        print(f"  дедуп по mid: {len(data['mid']):,} -> {len(keep):,} "
              f"(убрано {len(data['mid']) - len(keep):,} копий)", flush=True)
    data = {k: v[keep] for k, v in data.items()}

    # Патч из имени файла есть не у всех (в про есть historical_* и combined_*).
    # Недостающим ставим патч ПО ВРЕМЕНИ: границей считается первая карта каждого
    # именованного патча. Это только для отбора выборок, в признаки патч не идёт.
    patch = data["patch"].astype(np.int32)
    known = patch > 0
    if known.any() and (~known).any():
        codes = np.unique(patch[known])
        starts = np.asarray([data["ts"][known & (patch == c)].min() for c in codes])
        srt = np.argsort(starts)
        codes, starts = codes[srt], starts[srt]
        pos = np.searchsorted(starts, data["ts"][~known], side="right") - 1
        filled = np.where(pos >= 0, codes[np.clip(pos, 0, len(codes) - 1)], 0)
        patch[~known] = filled
        print(f"  патч по времени проставлен {int((~known).sum()):,} строкам", flush=True)
        data["patch"] = patch
    return data


def build(corpus: str, max_rows: int = 0) -> None:
    d = load_rows(corpus)
    n = len(d["ts"])
    if max_rows and n > max_rows:
        d = {k: v[n - max_rows:] for k, v in d.items()}
        n = max_rows
        print(f"[{corpus}] ограничение: беру свежие {n:,} карт", flush=True)
    print(f"[{corpus}] карт: {n:,}", flush=True)

    heroes, accounts, ts = d["heroes"], d["accounts"], d["ts"]
    side_of_slot = np.concatenate([np.zeros(5, int), np.ones(5, int)])

    def column(name: str) -> np.ndarray:
        arr, idx = SRC[name]
        col = d[arr][:, :, idx].astype(np.float32)
        if name == "imp":                       # -32768 = поля не было в записи
            col[d[arr][:, :, idx] == IMP_NA] = np.nan
        return col

    def team_sum(name: str) -> np.ndarray:
        col = np.nan_to_num(column(name), nan=0.0)
        both = np.stack([col[:, :5].sum(1), col[:, 5:].sum(1)], axis=1)
        return both[:, side_of_slot]

    def per_slot(name: str) -> np.ndarray:
        if name == "win":
            w = np.stack([d["wins"], 1 - d["wins"]], axis=1)[:, side_of_slot]
            return w.astype(np.float32).ravel()
        if name in SHARES:
            num, den = SHARES[name]
            top = (column("kills") + column("assists")) if num == "kills+assists" else column(num)
            return (top / np.maximum(team_sum(den), np.float32(1.0))).ravel()
        return column(name).ravel()

    stats = list(BASE_STATS) + list(EXTRA_STATS)
    print(f"  величин на игрока: {len(stats)}", flush=True)

    acc_flat = accounts.ravel()
    hero_flat = heroes.ravel().astype(np.int64)
    pos_flat = np.tile(np.concatenate([np.arange(5), np.arange(5)]), n)
    ts_flat = np.repeat(ts, 10)
    hero_pos_key = hero_flat * 8 + pos_flat
    acc_key = np.where(acc_flat > 0, acc_flat, -1)
    anon = acc_flat <= 0

    feats: dict[str, np.ndarray] = {}
    order: list[str] = []

    def add(name: str, values: np.ndarray, level: bool = False,
            coverage: bool = False) -> None:
        m, c = team_mean(values, n)
        feats[f"{name}_diff"] = (m[:, 0] - m[:, 1]).astype(np.float32)
        order.append(f"{name}_diff")
        if level:
            feats[f"{name}_lvl"] = ((m[:, 0] + m[:, 1]) / 2.0).astype(np.float32)
            order.append(f"{name}_lvl")
        if coverage:
            feats[f"{name}_cov"] = (c[:, 0] - c[:, 1]).astype(np.float32)
            order.append(f"{name}_cov")

    idx_acc = TrailingIndex(acc_key, ts_flat)
    idx_hero = TrailingIndex(hero_flat, ts_flat)
    idx_hp = TrailingIndex(hero_pos_key, ts_flat)

    print("  форма игрока, база героя и героя+позиции...", flush=True)
    for i, stat in enumerate(stats, 1):
        raw = per_slot(stat)
        src = np.where(anon, np.nan, raw).astype(np.float32)
        lvl = stat in LEVEL_STATS
        for w in PLAYER_WINDOWS:
            v = idx_acc.mean(src, w)
            v[anon] = np.nan
            add(f"pl_{stat}_{w}", v, level=(lvl and w == 30), coverage=(stat == "kills"))
        add(f"hero_{stat}_all", idx_hero.mean(raw, None), level=lvl)
        add(f"heropos_{stat}_all", idx_hp.mean(raw, None))
        if stat in BASE_STATS:
            add(f"hero_{stat}_r{HERO_RECENT}", idx_hero.mean(raw, HERO_RECENT))
            add(f"heropos_{stat}_r{HERO_RECENT}", idx_hp.mean(raw, HERO_RECENT))
        del raw, src
        print(f"    {i}/{len(stats)} {stat}", flush=True)

    print("  привычность позиции и форма на этом герое...", flush=True)
    share = np.full(len(acc_flat), np.nan, dtype=np.float32)
    for q in range(5):
        ind = np.where(anon, np.nan, (pos_flat == q).astype(np.float32)).astype(np.float32)
        v = idx_acc.mean(ind, 30)
        take = pos_flat == q
        share[take] = v[take]
    share[anon] = np.nan
    add("pl_posshare_30", share, coverage=True)
    del share

    acc_hero_key = np.where(anon, -1, acc_key * 256 + hero_flat)
    idx_ah = TrailingIndex(acc_hero_key, ts_flat)
    for stat in ("kills", "deaths", "kp"):
        src = np.where(anon, np.nan, per_slot(stat)).astype(np.float32)
        v = idx_ah.mean(src, None)
        v[anon] = np.nan
        add(f"plhero_{stat}_all", v, coverage=(stat == "kills"))
    del idx_ah

    print("  длительность и оконный темп...", flush=True)
    dur_slot = np.broadcast_to(d["duration"].astype(np.float32)[:, None], (n, 10)).ravel()
    for index, tag in ((idx_hero, "hero"), (idx_hp, "heropos")):
        add(f"{tag}_duration_all", index.mean(dur_slot, None), level=True)
    del dur_slot

    diffs = d["diffs"].astype(np.float32)
    totals = d["totals"].astype(np.float32)
    valid = d["valid"]
    own_side = (side_of_slot[None, :] == 0)
    for wi, (a, b) in enumerate(WINDOWS):
        tag = f"{a}_{b}"
        col = diffs[:, wi][:, None]
        signed = np.where(own_side, col, -col)
        signed = np.where(valid[:, wi][:, None], signed, np.nan).ravel().astype(np.float32)
        tot = np.where(valid[:, wi][:, None],
                       np.broadcast_to(totals[:, wi][:, None], (n, 10)), np.nan
                       ).ravel().astype(np.float32)
        for index, ktag in ((idx_hero, "hero"), (idx_hp, "heropos")):
            add(f"{ktag}_wdiff_{tag}", index.mean(signed, None))
            add(f"{ktag}_wtot_{tag}", index.mean(tot, None), level=True)
        psig = np.where(anon, np.nan, signed).astype(np.float32)
        ptot = np.where(anon, np.nan, tot).astype(np.float32)
        for pw in (10, 30):
            v = idx_acc.mean(psig, pw)
            v[anon] = np.nan
            add(f"pl_wdiff_{tag}_{pw}", v)
        v = idx_acc.mean(ptot, 30)
        v[anon] = np.nan
        add(f"pl_wtot_{tag}_30", v, level=True)
        del signed, tot, psig, ptot
    del idx_hero, idx_hp, idx_acc

    # --------------------------------------------------------------- сборка X
    names = sorted(order)
    X = np.empty((n, len(names)), dtype=np.float32)
    for j, k in enumerate(names):                 # переносим по колонке и сразу
        X[:, j] = feats.pop(k)                    # освобождаем — иначе пик вдвое
    del feats
    print(f"  матрица {X.shape[1]} колонок, {X.nbytes / 2**30:.2f} ГБ", flush=True)

    # ------------------------------------------------ командная форма (только про)
    team_names: list[str] = []
    Xteam = np.zeros((n, 0), dtype=np.float32)
    elo_pre = np.full((n, 2), np.nan, dtype=np.float32)
    teams = d["teams"]
    if (teams > 0).any():
        print("  командная форма...", flush=True)
        tkey = np.concatenate([teams[:, 0], teams[:, 1]])
        tkey = np.where(tkey > 0, tkey, -1)
        tts = np.concatenate([ts, ts])
        idx_team = TrailingIndex(tkey, tts)
        ks = d["kills_side"].astype(np.float32)
        own = np.concatenate([ks[:, 0], ks[:, 1]])
        opp = np.concatenate([ks[:, 1], ks[:, 0]])
        wins = d["wins"].astype(np.float32)
        quantities = {
            "tkills": own, "tkillsopp": opp, "ttotal": own + opp,
            "twin": np.concatenate([wins, 1 - wins]),
            "tdur": np.concatenate([d["duration"], d["duration"]]).astype(np.float32),
        }
        for wi, (a, b) in enumerate(WINDOWS):
            tag = f"{a}_{b}"
            dv = np.where(valid[:, wi], diffs[:, wi], np.nan).astype(np.float32)
            tv = np.where(valid[:, wi], totals[:, wi], np.nan).astype(np.float32)
            quantities[f"twdiff_{tag}"] = np.concatenate([dv, -dv])
            quantities[f"twtot_{tag}"] = np.concatenate([tv, tv])
        bad = np.concatenate([teams[:, 0] <= 0, teams[:, 1] <= 0])
        cols: dict[str, np.ndarray] = {}
        for qname, val in quantities.items():
            v0 = np.where(bad, np.nan, val).astype(np.float32)
            for tw in TEAM_WINDOWS:
                m = idx_team.mean(v0, tw)
                m[bad] = np.nan
                r, dd = m[:n], m[n:]
                # NaN остаётся NaN: перцентильная шкала отправит «истории нет» в
                # середину (0.5), а зануление отправило бы в низ шкалы у уровней.
                cols[f"{qname}_{tw}_diff"] = (r - dd).astype(np.float32)
                cols[f"{qname}_{tw}_lvl"] = ((r + dd) / 2.0).astype(np.float32)
        ones = np.where(bad, np.nan, 1.0).astype(np.float32)
        for tw in TEAM_WINDOWS:
            c = idx_team.mean(ones, tw)
            cols[f"tcov_{tw}"] = np.nan_to_num(c[:n] + c[n:], nan=0.0)

        # ELO команд по про-корпусу. Нужен дважды: как признак и как БАЗА для
        # сравнения — по E-164 прод-модель 27+ проигрывала одному числу
        # `elo_target_win_prob`, и без этой колонки вывод «модель лучше» повиснет.
        # Строго прошлое: рейтинг записывается ДО обновления по исходу.
        rating: dict[int, float] = {}
        K_ELO, START = 20.0, 1500.0
        w_arr = d["wins"].astype(np.float64)
        for i in range(n):
            ta, tb = int(teams[i, 0]), int(teams[i, 1])
            if ta <= 0 or tb <= 0:
                continue
            ra, rb = rating.get(ta, START), rating.get(tb, START)
            elo_pre[i] = (ra, rb)
            exp = 1.0 / (1.0 + 10.0 ** ((rb - ra) / 400.0))
            rating[ta] = ra + K_ELO * (w_arr[i] - exp)
            rating[tb] = rb + K_ELO * ((1.0 - w_arr[i]) - (1.0 - exp))
        cols["elo_diff"] = (elo_pre[:, 0] - elo_pre[:, 1]).astype(np.float32)
        cols["elo_lvl"] = ((elo_pre[:, 0] + elo_pre[:, 1]) / 2.0).astype(np.float32)
        print(f"  ELO построен для {int(np.isfinite(elo_pre[:, 0]).sum()):,} карт "
              f"({len(rating):,} команд)", flush=True)
        team_names = sorted(cols)
        Xteam = np.stack([cols[k] for k in team_names], axis=1).astype(np.float32)
        del cols, idx_team
        print(f"  командных колонок: {Xteam.shape[1]}", flush=True)

    out = OUT_DIR / f"featuresv3_{corpus}.npz"
    np.savez_compressed(
        out, X=X, names=np.asarray(names), Xteam=Xteam, elo=elo_pre,
        team_names=np.asarray(team_names), ts=ts, mid=d["mid"], heroes=heroes,
        diffs=d["diffs"], totals=d["totals"], valid=d["valid"],
        duration=d["duration"], wins=d["wins"], kills_side=d["kills_side"],
        nwlead=d["nwlead"], xplead=d["xplead"], nwok=d["nwok"],
        patch=d["patch"], league=d["league"], tier=d["tier"], teams=teams,
    )
    print(f"[{corpus}] признаков {X.shape[1]} (+{Xteam.shape[1]} командных) "
          f"на {X.shape[0]:,} карт -> {out}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", default="public")
    ap.add_argument("--max-rows", type=int, default=0)
    args = ap.parse_args()
    build(args.corpus, args.max_rows)


if __name__ == "__main__":
    main()
