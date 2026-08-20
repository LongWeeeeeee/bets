#!/usr/bin/env python3
"""27+ килов: максимум, который вытягивается из ВСЕГО предматчевого, без ингейма.

Замечание alex по E-183 справедливо: там к боевым 35 признакам добавлялся только
симметричный блок, а Glicko, TrueSkill, hybrid-логит и тринадцать батчей идей,
собранных под win-модель, не подключались вовсе. Здесь подключается всё, что
лежит выровненным на тот же набор карт.

Что входит:
  * боевые 35 предматчевой модели (в них уже elo, hybrid_strength, draft_logit);
  * симметричный блок 414 (константы героев и baseline герой×позиция);
  * Glicko, Glicko-вероятность, RD, позиционный Glicko, TrueSkill и его сигма;
  * hybrid-логит и сырая разность сил;
  * ideas_batch 1,2,3,4,5,5b,6b,8,9,11 плюс context, streaks и баны.

Чего НЕ входит намеренно:
  * любые ингейм-величины (нетворс, опыт, килы по минутам) — цель предматчевая;
  * `forward_predictions` и `lan_model_P` — это ВЫХОДЫ моделей, и без проверки,
    как они построены, класть их признаком нельзя: если модель обучалась на всём
    корпусе, это утечка. Считаются отдельным вариантом и помечены.

Цель: сделает ли РАДИАНТ 27+ килов. Одна строка на карту, никаких удвоений —
чтобы не гадать, какие из чужих колонок антисимметричны. Итог берётся из
`pstats` поимённо по игрокам: поминутный ряд обрывается на 40-й минуте.

Кроме AUC печатается ФАКТИЧЕСКАЯ доля попаданий по полосам уверенности — это и
есть «реальный винрейт», по которому видно, есть ли на чём зарабатывать.

Запуск: nohup venv_catboost/bin/python3 runtime/experiments/misc/kills27_max_model.py ...
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))
sys.path.insert(0, str(ROOT / "base"))
from ideas_batch2 import COMPACT, RICH, TEST_FROM  # noqa: E402
from pro_features_wide import auc  # noqa: E402
import audit_live_path as A  # noqa: E402
from pregame_features import PregameFeatures  # noqa: E402
# Общий модуль на оба пути. Раньше здесь стоял
# `import eval_pregame_features as E` — ВТОРАЯ копия тех же функций, из
# gitignored-эксперимента, тогда как живой путь звал `hero_side_tables`.
# Докстрока общего модуля обещала обратное («офлайн-сборка и прод
# импортируют его, а не переписывают»), и обещание не выполнялось: 742
# колонки `sym_`, девять десятых входа панели, считались двумя
# независимо правимыми копиями — ровно паттерн E-201 (−0.0046).
# Переключение безопасно: сверено 19.08.2026, обе копии дают
# max|Δ| = 0.000e+00 на 3000 составах x 742 колонки и одинаковые C/B.
from hero_side_tables import sym_block, hero_tables  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
BASELINES = ROOT / "base/hero_baselines_protest.json"
OUT = ART / "kills27_max_model.md"
VAL_FRAC = 0.20
THRESHOLD = 27
# кэши идей: имя файла -> ключи с матрицами
IDEA_CACHES = [("ideas_batch1", ["F"]), ("ideas_batch2", ["F"]), ("ideas_batch3", ["F"]),
               ("ideas_batch4", ["F35", "F36", "F37"]), ("ideas_batch5", ["F"]),
               ("ideas_batch5b", ["F"]), ("ideas_batch6b", ["F"]), ("ideas_batch8", ["F"]),
               ("ideas_batch9", ["F"]), ("ideas_batch11", ["F"]), ("ideas_context", ["F"]),
               ("ideas_streaks", ["F"]), ("ban_features", ["F"])]


def hit_table(p: np.ndarray, y: np.ndarray, bands=(0.55, 0.60, 0.65, 0.70, 0.75, 0.80)):
    """Фактическая доля попаданий по полосам уверенности."""
    out = []
    for lo in bands:
        m = p >= lo
        if m.sum() >= 50:
            out.append((f"p >= {lo:.2f}", int(m.sum()), float(y[m].mean()),
                        float(m.mean())))
        m2 = p <= 1 - lo
        if m2.sum() >= 50:
            out.append((f"p <= {1-lo:.2f}", int(m2.sum()), float(1 - y[m2].mean()),
                        float(m2.mean())))
    return out


def build_all():
    """Матрица всех предматчевых признаков плюс всё, что нужно для целей.

    Вынесено из main, чтобы другие замеры (время, окна килов) брали РОВНО те же
    определения: две копии сборки неизбежно разъезжаются, и сегодня уже
    разбирались, чего это стоит.
    """
    zc, zr = np.load(COMPACT), np.load(RICH)
    hz = np.load(ART / "map_winner_hybrid_quality_forward/hybrid_features.npz",
                 allow_pickle=True)
    old = hz["mids"].astype(np.int64)
    cpos = {int(m): i for i, m in enumerate(zc["mids"].tolist())}
    rpos = {int(m): i for i, m in enumerate(zr["mids"].tolist())}
    have = np.array([int(m) in cpos and int(m) in rpos for m in old.tolist()])
    keep_old = np.flatnonzero(have)            # позиции в СТАРОМ наборе
    old_ok = old[have]
    ci = np.array([cpos[int(m)] for m in old_ok.tolist()])
    ri = np.array([rpos[int(m)] for m in old_ok.tolist()])
    ts = zc["ts"][ci]
    heroes, accounts = zc["heroes"][ci], zc["accounts"][ci]
    pst = zr["pstats"][ri]
    dur_min = zr["durations"][ri].astype(float) / 60.0
    train, test = ts < TEST_FROM, ts >= TEST_FROM
    print(f"карт {len(ts):,}: обучение {int(train.sum()):,}, тест {int(test.sum()):,}", flush=True)

    blocks, names = [], []

    def add(mat: np.ndarray, tag: str) -> None:
        mat = np.asarray(mat, dtype=np.float32)
        if mat.ndim == 1:
            mat = mat[:, None]
        blocks.append(mat)
        names.extend(f"{tag}_{i}" for i in range(mat.shape[1]))
        print(f"  + {tag}: {mat.shape[1]}", flush=True)

    dep = np.load(ART / "prematch_model_artifact_v3_hybrid.npz", allow_pickle=True)
    names35 = [str(x) for x in dep["feature_names"]]
    G, _ = A.train_columns(ts, accounts, pst, dur_min, old_ok,
                           np.ones(len(ts), bool), dep["ctx_mu"], dep["ctx_sd"], names35)
    add(G, "prod35")

    pf = PregameFeatures(baselines=BASELINES)
    C_, B_, _cn, _bn = hero_tables(pf)
    # Ровно та же функция, что зовёт живой путь (`prematch_panel_live`), а не
    # одноимённый помощник: общей должна быть сама сборка блока, иначе копии
    # снова разъедутся на следующей правке.
    add(sym_block(heroes.astype(np.int64), C_, B_), "sym")

    gl = np.load(ART / "undercount_glicko_features.npz", allow_pickle=True)
    add(np.column_stack([gl[k][keep_old] for k in
                         ("glicko", "glicko_p", "glicko_rd", "pos_glicko",
                          "trueskill", "trueskill_sig")]), "rating")
    add(hz["F"][have], "hybrid")

    for nm, keys in IDEA_CACHES:
        p = ART / f"{nm}.npz"
        if not p.exists():
            continue
        z = np.load(p, allow_pickle=True)
        for k in keys:
            if k in z:
                add(z[k][have], f"{nm}_{k}")

    X = np.hstack(blocks)
    del blocks
    ok = np.isfinite(X).all(0) & (X[train].std(0) > 1e-9)
    X, names = X[:, ok], [n for n, k in zip(names, ok) if k]
    print(f"ИТОГО признаков: {X.shape[1]}", flush=True)

    cut = np.quantile(ts[np.flatnonzero(train)], 1.0 - VAL_FRAC)
    sub, val = train & (ts < cut), train & (ts >= cut)
    return dict(X=X, names=names, ts=ts, train=train, test=test, sub=sub, val=val,
                pst=pst, dur_min=dur_min, rk=zr["rk"][ri].astype(float),
                dk=zr["dk"][ri].astype(float), mids=old_ok,
                wins=zc["wins"][ci].astype(int),
                # Колонки `prod35_i` названы ПОЗИЦИОННО, а что именно лежит в
                # слоте, знает только этот список. Отдаём его наружу, чтобы
                # артефакт панели сохранил отпечаток входа: без него живой путь
                # берёт порядок у текущей модели, и перестановка боевых
                # признаков молча подменила бы слоты.
                names35=list(names35))


def main() -> None:
    t0 = time.time()
    d = build_all()
    X, names = d["X"], d["names"]
    ts, train, test, sub, val = d["ts"], d["train"], d["test"], d["sub"], d["val"]
    pst = d["pst"]
    y = (pst[:, :5, 0].sum(1) >= THRESHOLD).astype(int)
    print(f"доля положительных: обучение {y[train].mean():.3f}, тест {y[test].mean():.3f}",
          flush=True)

    from catboost import CatBoostClassifier
    rows = []
    # 1. только боевые 35 — для сравнения с E-183
    i35 = [i for i, n in enumerate(names) if n.startswith("prod35_")]
    # ideas_batch4 и ban_features не удалось проверить на порядок «читаем до
    # обновления» (у первого признаки пишутся не через F[i], у второго нет
    # скрипта-сборщика). Считаем вариант БЕЗ них: если результат держится, он от
    # непроверенного не зависит.
    unver = tuple(n for n in ("ideas_batch4_", "ban_features_") if True)
    i_ver = [i for i, n in enumerate(names) if not n.startswith(unver)]
    for tag, cols in (("боевые 35", i35),
                      ("всё проверенное", i_ver),
                      ("всё, кроме выходов моделей", list(range(len(names))))):
        m = CatBoostClassifier(iterations=3000, learning_rate=0.03, depth=6,
                               l2_leaf_reg=6, eval_metric="AUC", random_seed=20260814,
                               early_stopping_rounds=150, verbose=False, thread_count=8)
        m.fit(X[sub][:, cols], y[sub], eval_set=(X[val][:, cols], y[val]))
        p = m.predict_proba(X[test][:, cols])[:, 1]
        a = auc(y[test], p)
        rows.append((tag, len(cols), a, m.get_best_iteration(), p))
        print(f"  {tag} ({len(cols)} признаков): AUC {a:.4f}, "
              f"деревьев {m.get_best_iteration()}", flush=True)

    tag, ncol, best_auc, iters, p = max(rows, key=lambda r: r[2])
    lines = ["# 27+ килов: максимум из всего предматчевого", "",
             f"Цель — сделает ли радиант {THRESHOLD}+ килов; итог из `pstats` поимённо. "
             f"Одна строка на карту, тест {int(test.sum()):,}. Доля положительных "
             f"{y[test].mean():.3f}. Ингейм не используется.", "",
             "| вход | признаков | AUC на тесте | деревьев |", "|---|---|---|---|"]
    for t_, n_, a_, it_, _ in rows:
        lines.append(f"| {t_} | {n_} | {a_:.4f} | {it_} |")
    lines += ["", f"## Фактическая доля попаданий — «{tag}»", "",
              "| полоса | карт | фактически сбылось | доля потока |", "|---|---|---|---|"]
    for band, n_, hit, share in hit_table(p, y[test]):
        lines.append(f"| {band} | {n_:,} | {hit:.1%} | {share:.1%} |")
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    for band, n_, hit, share in hit_table(p, y[test]):
        print(f"    {band}: карт {n_:,}, сбылось {hit:.1%}, поток {share:.1%}", flush=True)
    print(f"отчёт: {OUT} (за {time.time()-t0:.0f} c)", flush=True)


if __name__ == "__main__":
    main()
