#!/usr/bin/env python3
"""Веса веток лестницы: по одной логрегрессии на шаблон доступности данных.

ЗАЧЕМ. Боевая модель отказывалась считать, если снимок не знает хотя бы одного
из десяти игроков: в сквозном аудите это 14 828 отказов из 14 952, то есть 57.5%
тестовых карт. Отказ правильный — подставлять дефолты нельзя. Но можно посчитать
моделью, которая недостающих колонок НИКОГДА НЕ ВИДЕЛА, и её уверенность будет
честной. Здесь такие модели и обучаются.

КОНФИГУРАЦИЯ — БОЕВАЯ. С 15.08 в бою стоит одна логрегрессия на окне 120 суток
(E-192, E-193), а не ансамбль вложенных окон. На всей истории (456 470 карт) та
же матрица даёт 0.7130 — это худшая строка таблицы E-191, и мерить на ней
значило бы занижать все ветки разом.

ЧТО ПРОВЕРЯЕТСЯ ПОМИМО AUC:
  1. полная ветка обязана воспроизвести боевой ориентир 0.7186 (допуск 0.0015,
     разница от регуляризации и реализации);
  2. сумма частичных сумм по компонентам обязана давать сам логит — иначе
     разложение, которое увидит человек в панели, будет выдумкой;
  3. набор колонок ветки обязан совпадать с `prematch_components.columns_for`,
     потому что ровно по этому правилу ветку выбирает боевой скорер.

Матрица собирается тем же кодом, что и `audit_live_path.train_columns`, а
рабочий набор берётся ИЗ ЗАМОРОЖЕННОГО ПОРЯДКА `hybrid_features.npz`: половина
обучающих кэшей адресуется позицией и не несёт `mids`. Признак верного
выравнивания — ровно 26 016 тестовых карт.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/train_branch_weights.py
Выход:  runtime/artifacts/misc/branch_weights.npz + branch_weights.md
"""
from __future__ import annotations

import math
import os
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))
sys.path.insert(0, str(ROOT / "base"))

import audit_live_path as A  # noqa: E402
import prematch_components as C  # noqa: E402
import prematch_scorer as ps  # noqa: E402
from ideas_batch2 import COMPACT, RICH, TEST_FROM  # noqa: E402
from pro_features_wide import auc  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
# Боевые веса. Полная ветка берётся ОТСЮДА побитово, а не переобучается: иначе
# у каждой карты, которая и сегодня получает вердикт, число молча поехало бы.
# Проверено на сквозном прогоне: при переобучении совпало 0 вердиктов из 11 064.
DEPLOYED = Path(os.getenv("PREMATCH_WEIGHTS", ART / "prematch_weights_win120.npz"))
OUT_NPZ = ART / "branch_weights.npz"
OUT_MD = ART / "branch_weights.md"
HYB = ART / "map_winner_hybrid_quality_forward/hybrid_features.npz"

WINDOW_DAYS = int(os.getenv("BRANCH_WINDOW_DAYS", "120"))
C_REG = float(os.getenv("BRANCH_C", "1.0"))
FULL_TARGET = 0.7186
FULL_TOL = 0.0015


def build_matrix():
    """35 обучающих колонок в порядке `feature_names` артефакта."""
    zc, zr = np.load(COMPACT), np.load(RICH)
    mids = np.load(HYB, allow_pickle=True)["mids"].astype(np.int64)
    cpos = {int(x): i for i, x in enumerate(zc["mids"].tolist())}
    rpos = {int(x): i for i, x in enumerate(zr["mids"].tolist())}
    absent = [int(m) for m in mids.tolist() if int(m) not in cpos or int(m) not in rpos]
    if absent:
        raise SystemExit(
            f"из замороженного набора {len(absent)} карт нет в корпусе — "
            "позиционные кэши перестали отвечать строкам, пересобрать их")
    ci = np.array([cpos[int(m)] for m in mids.tolist()])
    ri = np.array([rpos[int(m)] for m in mids.tolist()])
    ts, wins = zc["ts"][ci], zc["wins"][ci].astype(int)
    m = ps.PrematchModel(A.AUDIT)
    names = list(m.features)
    X, _ = A.train_columns(ts, zc["accounts"][ci], zr["pstats"][ri],
                           zr["durations"][ri].astype(float) / 60.0, mids,
                           np.ones(len(mids), dtype=bool), m.ctx_mu, m.ctx_sd, names)
    X = np.asarray(X, dtype=np.float64)
    bad = int((~np.isfinite(X)).sum())
    if bad:
        print(f"  нечисловых значений в матрице: {bad} — заменены нулём", flush=True)
        X[~np.isfinite(X)] = 0.0
    return X, wins, ts, names


def fit(X, y):
    """Логрегрессия со стандартизацией; отдаёт (mu, sd, coef, intercept).

    Нормировка кладётся в саму ветку, потому что боевой скорер применяет её из
    артефакта: считать её на месте по боевым данным нельзя — она обязана быть
    той же, на которой подбирались веса.
    """
    from sklearn.linear_model import LogisticRegression

    mu, sd = X.mean(0), X.std(0)
    sd = np.where(sd < 1e-9, 1.0, sd)
    lr = LogisticRegression(C=C_REG, max_iter=5000)
    lr.fit((X - mu) / sd, y)
    return mu, sd, lr.coef_[0].copy(), float(lr.intercept_[0])


def main() -> None:
    t0 = time.time()
    X, y, ts, names = build_matrix()
    te = ts >= TEST_FROM
    tr = (ts < TEST_FROM) & (ts >= TEST_FROM - WINDOW_DAYS * 86400)
    if int(te.sum()) != 26016:
        raise SystemExit(f"тестовых карт {int(te.sum())}, а должно быть 26 016 — "
                         "порядок кэшей поехал, дальше идти нельзя")
    print(f"обучение {int(tr.sum()):,} карт (окно {WINDOW_DAYS} суток), "
          f"тест {int(te.sum()):,}", flush=True)

    lines = ["# Веса веток лестницы предматчевой модели", "",
             f"Окно обучения {WINDOW_DAYS} суток — как в бою (E-192, E-193): "
             f"{int(tr.sum()):,} карт. Тест {int(te.sum()):,} карт после "
             f"`TEST_FROM = {TEST_FROM}`.", "",
             "| ветка | колонок | AUC | Δ к полной | веса |",
             "|---|---:|---:|---:|---|"]

    zd = np.load(DEPLOYED, allow_pickle=True)
    dn = [str(x) for x in zd["feature_names"]]
    if dn != names:
        raise SystemExit(f"порядок признаков в {DEPLOYED.name} разошёлся с артефактом")
    if len(zd["coef"]) != 1:
        raise SystemExit(
            f"в {DEPLOYED.name} весов {len(zd['coef'])} наборов, а ветка держит один. "
            "Это ансамбль вложенных окон (E-191); лестница на нём не собирается — "
            "сначала перевести бой на одиночное окно.")

    branches, aucs, source = {}, {}, {}
    for name, keys in C.BRANCHES:
        cols = C.columns_for(keys, names)
        if not cols:
            print(f"  {name}: колонок не осталось, ветка пропущена", flush=True)
            continue
        j = [names.index(c) for c in cols]
        if name == "full":
            # ПОБИТОВАЯ КОПИЯ БОЕВЫХ ВЕСОВ. Полная ветка обязана отдавать ровно
            # то же число, что и сегодня: лестница добавляет вердикты там, где
            # их не было, и не имеет права трогать те, что уже есть.
            mu, sd = zd["mu"][0].copy(), zd["sd"][0].copy()
            coef, itc = zd["coef"][0].copy(), float(zd["intercept"][0])
            source[name] = "боевые веса как есть"
        else:
            mu, sd, coef, itc = fit(X[tr][:, j], y[tr])
            source[name] = f"обучена здесь, окно {WINDOW_DAYS} суток"
        logit = ((X[te][:, j] - mu) / sd) @ coef + itc
        a = auc(y[te], logit)
        aucs[name] = a
        branches[name] = ps.Branch(cols=cols, mu=mu, sd=sd, coef=coef, intercept=itc)
        print(f"  {name:20s} ({len(cols):2d}): AUC {a:.4f}  [{source[name]}]", flush=True)

    base = aucs["full"]
    for name, _ in C.BRANCHES:
        if name in aucs:
            lines.append(f"| `{name}` | {len(branches[name].cols)} | "
                         f"{aucs[name]:.4f} | {aucs[name] - base:+.4f} | "
                         f"{source[name]} |")

    # --- проверка 1: полная ветка воспроизводит боевой ориентир
    if abs(base - FULL_TARGET) > FULL_TOL:
        raise SystemExit(
            f"полная ветка дала {base:.4f} против ориентира {FULL_TARGET:.4f} "
            f"(допуск {FULL_TOL}). Веса взяты боевые, поэтому расхождение здесь "
            f"означает, что поехал порядок колонок или сам набор тестовых карт.")

    # --- проверка 2: сумма частичных сумм равна логиту
    b = branches["full"]
    jb = [names.index(c) for c in b.cols]
    Z = (X[te][:, jb] - b.mu) / b.sd
    contrib = Z * b.coef
    parts = {}
    for k, c in enumerate(b.cols):
        comp = C.component_of(c)
        parts[comp] = parts.get(comp, 0.0) + contrib[:, k]
    total = sum(parts.values()) + b.intercept
    err = float(np.max(np.abs(total - (Z @ b.coef + b.intercept))))
    if err > 1e-9:
        raise SystemExit(f"разложение по компонентам расходится с логитом на {err:.2e}")
    lines += ["", f"Разложение по компонентам точное: наибольшее расхождение "
                  f"суммы частей с логитом {err:.2e}.", "",
              "| компонент | колонок | AUC частичной суммы |", "|---|---:|---:|"]
    for comp, group in C.COMPONENTS.items():
        cols = [c for c in b.cols if C.component_of(c) == comp]
        lines.append(f"| {comp} | {len(cols)} | {auc(y[te], parts[comp]):.4f} |")

    # --- проверка 3: набор колонок совпадает с тем, по чему выбирает скорер
    for name, keys in C.BRANCHES:
        if name in branches:
            assert branches[name].cols == C.columns_for(keys, names), name

    np.savez_compressed(OUT_NPZ, **ps.pack_branches(branches, names),
                        feature_names=np.array(names))
    lines += ["", f"Веса записаны в `{OUT_NPZ.name}` ({len(branches)} веток).",
              "", "Ветка выбирается в бою первой из тех, чьи ключи данных "
                  "доступны целиком; порядок задан в `prematch_components.BRANCHES`.",
              f"\nПрогон занял {time.time() - t0:.0f} c."]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines), flush=True)


if __name__ == "__main__":
    main()
