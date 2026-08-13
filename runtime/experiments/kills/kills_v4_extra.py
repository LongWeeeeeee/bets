#!/usr/bin/env python3
"""Пакет признаков v4: то, чего в v3 нет и что известно ДО карты.

v3 знает про игрока только средние по последним 5/10/30 картам. Здесь добавлено
пять групп, каждая отвечает на свой вопрос, которого средние не задают:

* **отдых и загрузка** — сколько прошло с прошлой карты игрока и сколько карт он
  сыграл за последние 7 суток. Одинаковое среднее у отдохнувшего и у восьмого
  часа подряд, а кровавость ранних минут от этого зависит;
* **разброс** — стандартное отклонение килов, смертей и оконного перевеса по
  последним 30. Игрок со средним +1 и разбросом 8 и игрок со средним +1 и
  разбросом 2 в модели v3 неотличимы;
* **форма как ИЗМЕНЕНИЕ** — среднее последних 5 минус среднее последних 30.
  Уровень и тренд — разные величины, v3 видит только уровень;
* **опыт и контекст карты** — сколько карт у игрока и у героя накоплено к этому
  моменту, возраст патча в сутках, время суток и день недели;
* **сыгранность пары** (только про) — сколько карт эти двое сыграли вместе и
  какой у них совместный оконный темп. В паблике пар не считаем: 5 млн карт по
  20 пар — это 100 млн строк, память кончится раньше пользы, поэтому колонки
  остаются NaN, и бустинг по ним просто не делает разрезов.

Ни одна величина не смотрит в текущую карту: все скользящие берут строго прошлое,
как в `kills_v3_build.py`.

Запуск:
    python3 kills_v4_extra.py --corpus pro
    python3 kills_v4_extra.py --corpus public
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
from kills_v3_build import SRC, SHARES, load_rows  # noqa: E402
from kills_v3_extract import IMP_NA, WINDOWS  # noqa: E402

OUT_DIR = ROOT / "runtime/artifacts/kills/window_model_v2"
DAY = 86400
PAIR_IDX = [(i, j) for i in range(5) for j in range(i + 1, 5)]


class Index(TrailingIndex):
    """TrailingIndex плюс две величины, которых у него нет: счётчик прошлых карт
    группы и число карт за последние N суток."""

    def __init__(self, group: np.ndarray, ts: np.ndarray) -> None:
        super().__init__(group, ts)
        self._ts = ts[self.order].astype(np.int64)
        # Ключ строго возрастает внутри группы и между группами — тогда поиск
        # «первая карта не раньше чем t» делается одним searchsorted по всему
        # массиву, а не циклом по группам. Ранг вместо самого id: произведение
        # id * 2**32 у стим-аккаунтов упирается в потолок int64.
        rank = np.unique(group[self.order], return_inverse=True)[1].astype(np.int64)
        self._key = rank * np.int64(1 << 32) + self._ts
        self._rank = rank

    def _unsort(self, v: np.ndarray) -> np.ndarray:
        out = np.empty(len(v), dtype=np.float32)
        out[self.order] = v.astype(np.float32)
        return out

    def count_prev(self) -> np.ndarray:
        return self._unsort((self.idx - self.gstart).astype(np.float32))

    def prev_ts(self) -> np.ndarray:
        """ts предыдущей карты группы; NaN, если карта первая."""
        prev = np.full(len(self._ts), np.nan, dtype=np.float64)
        has = self.idx > self.gstart
        prev[has] = self._ts[(self.idx - 1)[has]]
        return self._unsort(prev)

    def count_days(self, days: int) -> np.ndarray:
        lo = np.searchsorted(self._key, self._rank * np.int64(1 << 32)
                             + (self._ts - days * DAY), side="left")
        return self._unsort((self.idx - lo).astype(np.float32))

    def std(self, values: np.ndarray, window: int) -> np.ndarray:
        m1 = self.mean(values, window)
        m2 = self.mean(np.square(values, dtype=np.float32), window)
        return np.sqrt(np.maximum(m2 - m1 * m1, 0.0)).astype(np.float32)


def build(corpus: str) -> None:
    d = load_rows(corpus)
    n = len(d["ts"])
    ts = d["ts"]
    accounts = d["accounts"]
    heroes = d["heroes"]
    side_of_slot = np.concatenate([np.zeros(5, int), np.ones(5, int)])
    print(f"[{corpus}] карт {n:,}", flush=True)

    def column(name: str) -> np.ndarray:
        arr, idx = SRC[name]
        col = d[arr][:, :, idx].astype(np.float32)
        if name == "imp":
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

    acc_flat = accounts.ravel()
    anon = acc_flat <= 0
    acc_key = np.where(acc_flat > 0, acc_flat, -1)
    ts_flat = np.repeat(ts, 10)
    hero_flat = heroes.ravel().astype(np.int64)

    feats: dict[str, np.ndarray] = {}

    def add(name: str, values: np.ndarray, level: bool = True) -> None:
        m, _ = team_mean(values, n)
        feats[f"{name}_diff"] = (m[:, 0] - m[:, 1]).astype(np.float32)
        if level:
            feats[f"{name}_lvl"] = ((m[:, 0] + m[:, 1]) / 2.0).astype(np.float32)

    print("  отдых, загрузка, опыт...", flush=True)
    idx_acc = Index(acc_key, ts_flat)
    prev = idx_acc.prev_ts()
    rest = np.where(anon, np.nan, (ts_flat - prev) / DAY).astype(np.float32)
    rest[~np.isfinite(prev)] = np.nan
    add("pl_rest_days", np.clip(rest, 0, 60))
    for dd in (1, 7):
        v = idx_acc.count_days(dd)
        add(f"pl_load_{dd}d", np.where(anon, np.nan, v).astype(np.float32))
    add("pl_games_total", np.where(anon, np.nan, np.log1p(idx_acc.count_prev())
                                   ).astype(np.float32))

    print("  разброс и тренд формы...", flush=True)
    for stat in ("kills", "deaths", "kp", "win"):
        raw = per_slot(stat)
        src = np.where(anon, np.nan, raw).astype(np.float32)
        add(f"pl_{stat}_std30", np.where(anon, np.nan, idx_acc.std(src, 30)))
        m5, m30 = idx_acc.mean(src, 5), idx_acc.mean(src, 30)
        add(f"pl_{stat}_trend", np.where(anon, np.nan, m5 - m30).astype(np.float32),
            level=False)
        del raw, src

    print("  оконный разброс и опыт героя...", flush=True)
    diffs = d["diffs"].astype(np.float32)
    totals = d["totals"].astype(np.float32)
    valid = d["valid"]
    own = (side_of_slot[None, :] == 0)
    idx_hero = Index(hero_flat, ts_flat)
    add("hero_games_total", np.log1p(idx_hero.count_prev()).astype(np.float32))
    for wi, (a, b) in enumerate(WINDOWS):
        tag = f"{a}_{b}"
        col = diffs[:, wi][:, None]
        signed = np.where(own, col, -col)
        signed = np.where(valid[:, wi][:, None], signed, np.nan).ravel().astype(np.float32)
        tot = np.where(valid[:, wi][:, None],
                       np.broadcast_to(totals[:, wi][:, None], (n, 10)), np.nan
                       ).ravel().astype(np.float32)
        psig = np.where(anon, np.nan, signed).astype(np.float32)
        add(f"pl_wdiff_{tag}_std30", np.where(anon, np.nan, idx_acc.std(psig, 30)))
        m5, m30 = idx_acc.mean(psig, 5), idx_acc.mean(psig, 30)
        add(f"pl_wdiff_{tag}_trend", np.where(anon, np.nan, m5 - m30).astype(np.float32),
            level=False)
        add(f"hero_wtot_{tag}_std", idx_hero.std(tot, None))
        del signed, tot, psig
    del idx_hero

    print("  контекст карты...", flush=True)
    patch = d["patch"].astype(np.int64)
    start_of_patch = {int(p): int(ts[patch == p].min()) for p in np.unique(patch)}
    age = np.asarray([(t - start_of_patch[int(p)]) / DAY for t, p in zip(ts, patch)],
                     dtype=np.float32)
    feats["patch_age_days"] = age
    hour = ((ts % DAY) / DAY * 2 * np.pi).astype(np.float32)
    feats["tod_sin"] = np.sin(hour).astype(np.float32)
    feats["tod_cos"] = np.cos(hour).astype(np.float32)
    feats["dow"] = ((ts // DAY + 4) % 7).astype(np.float32)
    feats["tier"] = d["tier"].astype(np.float32)
    feats["duration_of_patch_rank"] = (np.argsort(np.argsort(age)) / max(n, 1)
                                       ).astype(np.float32)

    # ---------------------------------------------------- сыгранность пар (про)
    pair_names = ["pl_pair_games_diff", "pl_pair_games_lvl",
                  "pl_pair_wdiff_diff", "pl_pair_win_diff"]
    for k in pair_names:
        feats[k] = np.full(n, np.nan, dtype=np.float32)
    if corpus == "pro":
        print("  сыгранность пар...", flush=True)
        rows_i, rows_j, side_id = [], [], []
        for s in range(2):
            for (i, j) in PAIR_IDX:
                rows_i.append(accounts[:, s * 5 + i])
                rows_j.append(accounts[:, s * 5 + j])
                side_id.append(np.full(n, s, dtype=np.int8))
        A = np.concatenate(rows_i)
        B = np.concatenate(rows_j)
        sid = np.concatenate(side_id)
        lo, hi = np.minimum(A, B), np.maximum(A, B)
        okp = (lo > 0) & (hi > 0)
        # Ранги вместо сырых id: lo*4e9 + hi переполнило бы int64 на больших
        # аккаунтах и склеило бы разные пары в один ключ.
        uniq, inv = np.unique(np.concatenate([lo, hi]), return_inverse=True)
        rl, rh = inv[:len(lo)], inv[len(lo):]
        key = np.where(okp, rl.astype(np.int64) * (len(uniq) + 1) + rh, -1)
        tsp = np.tile(ts, 20)
        idx_pair = Index(key, tsp)
        cnt = np.where(okp, idx_pair.count_prev(), np.nan).astype(np.float32)
        winv = np.tile(d["wins"].astype(np.float32), 20)
        winv = np.where(sid == 0, winv, 1 - winv)
        wsig = np.tile(np.where(valid[:, 1], diffs[:, 1], np.nan).astype(np.float32), 20)
        wsig = np.where(sid == 0, wsig, -wsig)
        mw = np.where(okp, idx_pair.mean(np.where(okp, winv, np.nan).astype(np.float32),
                                        None), np.nan)
        md = np.where(okp, idx_pair.mean(np.where(okp, wsig, np.nan).astype(np.float32),
                                         None), np.nan)

        def side_mean(v: np.ndarray) -> np.ndarray:
            v = v.reshape(20, n)
            r = np.nanmean(np.where(np.isfinite(v[:10]), v[:10], np.nan), axis=0)
            dd = np.nanmean(np.where(np.isfinite(v[10:]), v[10:], np.nan), axis=0)
            return r, dd

        with np.errstate(invalid="ignore"):
            cr, cd = side_mean(cnt)
            wr, wd = side_mean(mw)
            dr, dl = side_mean(md)
        feats["pl_pair_games_diff"] = np.log1p(np.nan_to_num(cr)) - np.log1p(np.nan_to_num(cd))
        feats["pl_pair_games_lvl"] = np.log1p((np.nan_to_num(cr) + np.nan_to_num(cd)) / 2)
        feats["pl_pair_win_diff"] = (wr - wd).astype(np.float32)
        feats["pl_pair_wdiff_diff"] = (dr - dl).astype(np.float32)
        del idx_pair, A, B, lo, hi, key, tsp, cnt, mw, md

    names = sorted(feats)
    X = np.empty((n, len(names)), dtype=np.float32)
    for j, k in enumerate(names):
        X[:, j] = feats.pop(k)
    out = OUT_DIR / f"extrav4_{corpus}.npz"
    np.savez_compressed(out, X=X, names=np.asarray(names), mid=d["mid"], ts=ts)
    print(f"[{corpus}] пакет v4: {X.shape[1]} колонок на {n:,} карт -> {out}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", default="pro")
    args = ap.parse_args()
    build(args.corpus)


if __name__ == "__main__":
    main()
