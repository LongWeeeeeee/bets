#!/usr/bin/env python3
"""Сколько в разнице килов СТАТИЧЕСКОГО, а сколько — инерции: замер по блокам.

Зачем. Потолок предматчевой модели задаётся долей дисперсии, которая постоянна
внутри карты: состав, класс игроков, мета. Инерция преимущества («повёл в начале —
ведёт и дальше») тоже держит корреляцию между окнами, но предматчево она
непознаваема, потому что рождается внутри карты.

Различаются они затуханием. Статическая часть одинакова на любом расстоянии между
окнами; инерция затухает. Поэтому берём НЕПЕРЕСЕКАЮЩИЕСЯ пятиминутные блоки карты
и смотрим corr(разница в блоке i, разница в блоке j) как функцию расстояния |i−j|.
Фитуем corr(d) = A + B·exp(−d/τ): A — статическая доля, B — инерция.

Пересчёт на 10-минутное окно: если блок = a + e, то окно = 2a + e1 + e2, и доля
статического в окне равна 2A/(1+A), а не A.

Читается прямо из корпуса (нужны только поминутные массивы килов), поэтому не
зависит ни от признаков, ни от моделей.

Запуск:
    venv_catboost/bin/python3 runtime/experiments/kills/kills_v4_persistence.py \
        --corpus pro --files 8
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))

from train_public_draft_hero10_experiment import iter_json_objects  # noqa: E402

CORPUS = {
    "public": ROOT / "bets_data/analise_pub_matches/json_parts_split_from_object",
    "pro": ROOT / "pro_heroes_data/json_parts_split_from_object",
}
OUT = ROOT / "runtime/artifacts/kills/kills_v4_persistence.md"
BLOCK = 5
NBLOCK = 9                     # блоки 0-5 ... 40-45


def collect(corpus: str, files: int, seed: int = 0) -> np.ndarray:
    root = CORPUS[corpus]
    paths = sorted(p for p in root.glob("*.json") if p.name != "merge_patch_summary.json")
    rng = np.random.default_rng(seed)
    pick = paths if files >= len(paths) else [paths[i] for i in
                                              sorted(rng.choice(len(paths), files,
                                                                replace=False))]
    rows = []
    for p in pick:
        got = 0
        for _, raw in iter_json_objects(p):
            if not isinstance(raw, dict):
                continue
            r, d = raw.get("radiantKills"), raw.get("direKills")
            if not isinstance(r, list) or not isinstance(d, list) or len(r) != len(d):
                continue
            v = np.full(NBLOCK, np.nan)
            for b in range(NBLOCK):
                lo, hi = b * BLOCK, (b + 1) * BLOCK
                if len(r) > hi:
                    v[b] = sum(r[lo:hi]) - sum(d[lo:hi])
            rows.append(v)
            got += 1
        print(f"  {p.name}: +{got:,} (всего {len(rows):,})", flush=True)
    return np.asarray(rows, dtype=np.float64)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", default="pro")
    ap.add_argument("--files", type=int, default=6)
    args = ap.parse_args()
    M = collect(args.corpus, args.files)
    # ЛОВУШКА ОТБОРА, из-за которой первая версия дала ноль. Пара блоков на
    # расстоянии 8 существует только у карт длиннее 45 минут, а длинная карта —
    # это по построению РАВНАЯ карта: перевес там не набрали ни те, ни другие.
    # Поэтому корреляция на дальних расстояниях падала не от затухания инерции, а
    # от того, что её считали на другой популяции. Фиксируем популяцию: только
    # карты, у которых есть ВСЕ блоки, и все расстояния меряются на одних и тех же
    # картах.
    full = np.isfinite(M).all(axis=1)
    L = [f"# Статическая доля против инерции: {args.corpus}", "",
         f"Карт всего: {len(M):,}; с полными {NBLOCK} блоками ({NBLOCK * BLOCK} минут): "
         f"{int(full.sum()):,}. Блоки по {BLOCK} минут, разница килов радиант минус дайр.",
         "", "Все расстояния считаются на ОДНИХ И ТЕХ ЖЕ картах (полная популяция) —",
         "иначе дальние пары автоматически берутся только из длинных, то есть равных,",
         "карт, и затухание меряется вперемешку с отбором.", ""]
    MF = M[full]

    L += ["## Корреляция блоков по расстоянию (фиксированная популяция)", "",
          "| расстояние, блоков | минут | пар | средняя corr |", "|---|---:|---:|---:|"]
    by_dist: dict[int, list[tuple[float, int]]] = {}
    for i in range(NBLOCK):
        for j in range(i + 1, NBLOCK):
            if len(MF) < 400:
                continue
            r = float(np.corrcoef(MF[:, i], MF[:, j])[0, 1])
            by_dist.setdefault(j - i, []).append((r, len(MF)))
    xs, ys = [], []
    for dist in sorted(by_dist):
        vals = by_dist[dist]
        w = np.asarray([n for _, n in vals], dtype=float)
        r = float(np.average([v for v, _ in vals], weights=w))
        L.append(f"| {dist} | {dist * BLOCK} | {len(vals)} | {r:.4f} |")
        xs.append(dist * BLOCK)
        ys.append(r)
    L.append("")

    xs_a, ys_a = np.asarray(xs), np.asarray(ys)
    best = None
    for tau in np.arange(2.0, 60.0, 0.5):
        basis = np.stack([np.ones_like(xs_a), np.exp(-xs_a / tau)], axis=1)
        coef, res, *_ = np.linalg.lstsq(basis, ys_a, rcond=None)
        err = float(np.sum((basis @ coef - ys_a) ** 2))
        if best is None or err < best[0]:
            best = (err, float(coef[0]), float(coef[1]), float(tau))
    err, A, B, tau = best
    win_share = max(0.0, 2 * A / (1 + A)) if A > 0 else 0.0
    L += ["## Разложение", "",
          f"Фит `corr(d) = A + B·exp(-d/τ)`: **A = {A:.4f}**, B = {B:.4f}, "
          f"τ = {tau:.1f} мин, ошибка {err:.2e}.", "",
          f"* **A — статическая доля** дисперсии блока: то, что задано ДО карты "
          f"(состав, класс игроков, мета). Только её и может выучить предматчевая модель;",
          f"* **B** — инерция преимущества: затухает с постоянной {tau:.0f} минут, "
          f"внутри карты рождается, предматчево непознаваема;",
          f"* пересчёт на 10-минутное окно (сумма двух блоков): статическая доля "
          f"**{win_share:.4f}**.", "",
          "Оговорка: если реальное затухание не экспоненциально, A смещается. "
          "Проверка — форма таблицы выше: корреляция должна выходить на плато, "
          "а не падать к нулю."]
    # Для сравнения — как выглядит та же кривая БЕЗ фиксации популяции.
    L += ["", "## Та же кривая без фиксации популяции (для контраста)", "",
          "| расстояние, минут | карт в паре | corr |", "|---|---:|---:|"]
    for dist in sorted(by_dist):
        pairs = [(i, i + dist) for i in range(NBLOCK - dist)]
        rr, nn = [], 0
        for i, j in pairs:
            ok = np.isfinite(M[:, i]) & np.isfinite(M[:, j])
            if ok.sum() < 400:
                continue
            rr.append(float(np.corrcoef(M[ok, i], M[ok, j])[0, 1]) * ok.sum())
            nn += int(ok.sum())
        if nn:
            L.append(f"| {dist * BLOCK} | {nn // max(len(pairs),1):,} | {sum(rr)/nn:.4f} |")
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L))
    print("OUT:", OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
