#!/usr/bin/env python3
"""Потолок AUC для «кто сделает больше килов в окне» — из структуры самой цели.

Вопрос: 0.8 AUC — это «плохо стараемся» или «столько там нет»? Ответ не зависит
от модели и признаков, он считается по корпусу.

## Идея

Разница килов в окне раскладывается на две части:
    разница_окна = a (свойство карты, одинаково для всех окон) + e (шум окна).
Предматчевая модель в лучшем случае угадывает `a` — реализацию `e` не знает никто.
Значит потолок задаётся долей `a` в общей дисперсии, а она измеряется НАПРЯМУЮ:
для двух НЕПЕРЕСЕКАЮЩИХСЯ окон одной карты
    corr(разница_1, разница_2) = Var(a) / (Var(a) + Var(e)).

## Почему одной корреляции мало

`a` — это не только состав и класс игроков. В неё входит инерция преимущества:
кто повёл в первом окне, тот чаще ведёт и во втором. Инерция предматчево
непознаваема — она возникает ВНУТРИ карты. Отличить одно от другого можно по
затуханию: статическая часть от расстояния между окнами не зависит, инерция
затухает. Поэтому считается corr по расстоянию между центрами окон и
экстраполируется на бесконечное расстояние — остаток и есть статическая часть.

## Что печатается

1. корреляции по всем парам окон (перекрывающиеся — отдельно, они завышены);
2. затухание corr с расстоянием и экстраполяция к статической доле;
3. таблица «доля сигнала f -> достижимый AUC» — по ней читается, какой AUC
   соответствует какой доле, и наоборот: какому f отвечает наш нынешний AUC;
4. потолок при трёх оценках f: вся persistence, линейная экстраполяция,
   пуассоновская декомпозиция (самая грубая и самая завышенная).

Запуск:
    venv_catboost/bin/python3 runtime/experiments/kills/kills_v4_ceiling_theory.py
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "runtime/experiments/kills"))

from window_model_v2 import auc_of  # noqa: E402

OUT_DIR = ROOT / "runtime/artifacts/kills/window_model_v2"
WINDOWS = ((5, 15), (10, 20), (15, 25), (20, 30))
OUT = ROOT / "runtime/artifacts/kills/kills_v4_ceiling_theory.md"
F_GRID = (0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50, 0.70, 1.00)


def load(corpus: str):
    parts = sorted(glob.glob(str(OUT_DIR / f"rowsv3_{corpus}.shard*.npz")))
    if not parts:
        raise SystemExit(f"нет шардов rowsv3_{corpus}")
    D, V, T = [], [], []
    for p in parts:
        z = np.load(p)
        D.append(z["diffs"]); V.append(z["valid"]); T.append(z["totals"])
    return (np.concatenate(D).astype(np.float64), np.concatenate(V),
            np.concatenate(T).astype(np.float64))


def ceiling_auc(diff: np.ndarray, share: float, rng, draws: int = 3) -> tuple[float, float]:
    """AUC модели, знающей долю `share` дисперсии разницы.

    Шум берётся не из распределения, а ПЕРЕМЕШИВАНИЕМ наблюдаемых разниц: так
    сохраняются и дискретность, и тяжёлые хвосты, и доля ничьих. Ничья считается
    поражением — как в проде.
    """
    v = float(diff.var())
    mu = float(diff.mean())
    aucs, accs = [], []
    for _ in range(draws):
        a = rng.normal(mu, np.sqrt(max(share, 0.0) * v), size=len(diff))
        e = rng.permutation(diff - mu) * np.sqrt(max(1.0 - share, 0.0))
        sim = np.rint(a + e)
        y = (sim > 0).astype(np.int32)
        if 0 < y.mean() < 1:
            aucs.append(auc_of(y, a))
            accs.append(float(((a > 0).astype(np.int32) == y).mean()))
    return float(np.mean(aucs)), float(np.mean(accs))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", default="pro")
    ap.add_argument("--rows", type=int, default=400000)
    args = ap.parse_args()
    d, v, t = load(args.corpus)
    rng = np.random.default_rng(12345)
    L = [f"# Потолок AUC для оконных килов: {args.corpus}", ""]

    # ---------- 1. корреляции пар окон ----------
    L += ["## 1. Насколько разница килов держится на уровне карты", "",
          "| пара окон | общих минут | расстояние центров | карт | corr |",
          "|---|---:|---:|---:|---:|"]
    pts: list[tuple[float, float, int]] = []
    for i in range(len(WINDOWS)):
        for j in range(i + 1, len(WINDOWS)):
            ok = v[:, i] & v[:, j]
            if ok.sum() < 1000:
                continue
            a, b = WINDOWS[i], WINDOWS[j]
            overlap = max(0, min(a[1], b[1]) - max(a[0], b[0]))
            dist = abs((a[0] + a[1]) / 2 - (b[0] + b[1]) / 2)
            r = float(np.corrcoef(d[ok, i], d[ok, j])[0, 1])
            L.append(f"| {a[0]}-{a[1]} и {b[0]}-{b[1]} | {overlap} | {dist:.0f} | "
                     f"{int(ok.sum()):,} | {r:.3f} |")
            if overlap == 0:
                pts.append((dist, r, int(ok.sum())))
    L.append("")
    L.append("Пары с общими минутами в оценку НЕ идут: они делят часть одних и тех же")
    L.append("килов, и их корреляция завышена механически.")
    L.append("")

    # ---------- 2. затухание ----------
    dists = np.asarray([p[0] for p in pts])
    rs = np.asarray([p[1] for p in pts])
    L += ["## 2. Затухание: сколько из этого — инерция, а не свойство карты", ""]
    if len(set(dists.tolist())) >= 2:
        k, b0 = np.polyfit(dists, rs, 1)
        static = float(np.clip(k * 30 + b0, 0.0, 1.0))
        L += [f"Линейно по {len(pts)} парам: corr = {b0:.3f} {k:+.4f}·расстояние.",
              f"На расстоянии 30 минут (окна не соседствуют вовсе) остаётся "
              f"**{static:.3f}** — это оценка СТАТИЧЕСКОЙ доли, той, что задана до карты.",
              "", "Экстраполяция по двум различным расстояниям — грубая: она даёт",
              "порядок величины, а не третий знак."]
    else:
        static = float(rs.min())
        L.append(f"Разных расстояний мало, беру минимальную корреляцию: {static:.3f}.")
    persist = float(rs.min())
    L.append("")

    # ---------- 3. f -> AUC ----------
    L += ["## 3. Какой доле сигнала какой AUC соответствует", "",
          "`f` — доля дисперсии разницы килов, которую модель угадывает.", "",
          "| окно | " + " | ".join(f"f={f:.2f}" for f in F_GRID) + " |",
          "|---|" + "---:|" * len(F_GRID)]
    for wi, (a, b) in enumerate(WINDOWS):
        ok = v[:, wi].astype(bool)
        dd = d[ok, wi]
        if len(dd) > args.rows:
            dd = dd[rng.choice(len(dd), args.rows, replace=False)]
        cells = [f"{ceiling_auc(dd, f, rng, draws=2)[0]:.3f}" for f in F_GRID]
        L.append(f"| {a}-{b} | " + " | ".join(cells) + " |")
    L.append("")

    # ---------- 4. потолки ----------
    L += ["## 4. Потолок при трёх оценках доли сигнала", "",
          "| окно | вся persistence (f=%.2f) | статическая часть (f=%.2f) | "
          "пуассон (завышено) |" % (persist, static), "|---|---:|---:|---:|"]
    for wi, (a, b) in enumerate(WINDOWS):
        ok = v[:, wi].astype(bool)
        dd = d[ok, wi]
        tt = t[ok, wi]
        if len(dd) > args.rows:
            take = rng.choice(len(dd), args.rows, replace=False)
            dd, tt = dd[take], tt[take]
        f_pois = float(np.clip((dd.var() - tt.mean()) / dd.var(), 0, 1))
        L.append(f"| {a}-{b} | {ceiling_auc(dd, persist, rng)[0]:.4f} | "
                 f"{ceiling_auc(dd, static, rng)[0]:.4f} | "
                 f"{ceiling_auc(dd, f_pois, rng)[0]:.4f} |")
    L += ["", "Пуассоновская колонка приведена как заведомо ЗАВЫШЕННАЯ граница: она",
          "считает весь сверхпуассоновский разброс (пачки килов в тимфайтах)",
          "предсказуемым, а он не предсказуем.", "",
          "**Проверка счётчика:** при f=1.00 в таблице выше AUC обязан быть близок к 1 —",
          "если это не так, сломан сам симулятор, и остальные числа недействительны."]
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L))
    print("OUT:", OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
