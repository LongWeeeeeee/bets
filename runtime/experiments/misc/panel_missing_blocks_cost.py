#!/usr/bin/env python3
"""Сколько стоит панели то, что на боевой машине не поставлены четыре блока.

ЧТО ИЗМЕРЯЕТСЯ. Модели панели обучены на 928 колонках, где `public` (9),
`pairs` (8), `rating` (6) и `hybrid` (2) несут реальные значения. На serv1
провайдеров этих блоков нет (аудит 19.08.2026: файлов `public_kills_block.py`,
`pair_priors.py`, `team_ratings.py`, `hybrid_block.py` на боевой машине не
существует), и `assemble` заполняет их колонки нейтралью. Нейтраль была общим
нулём — а девять `publogit_*` это ВЕРОЯТНОСТИ со средним 0.30-0.88, то есть ноль
лежит на 3.4-11.8 sd ниже нормы обучения, и у `F8_pair_syn0_max_sum` среднее
+3.09. Модель получала вход, которого в обучении не было ни разу.

Три состояния одного и того же теста, модели НЕ переобучаются:
  * `как учили`   — реальные значения 25 колонок (недостижимый на проде потолок);
  * `ноль`        — то, что боевая машина считает прямо сейчас;
  * `среднее`     — правка 19.08: нейтраль равна среднему колонки в обучении.

Разница «как учили» минус «ноль» — цена непоставленных блоков.
Разница «среднее» минус «ноль» — что даёт правка, не трогая serv1.

ПОЧЕМУ ЭТО ЧЕСТНО. Сравниваются предсказания ОДНИХ И ТЕХ ЖЕ обученных моделей на
одном и том же тесте; меняется только 25 колонок входа из 928. Ни отбор колонок,
ни разбиение, ни веса не двигаются.
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "base"))
sys.path.insert(0, str(ROOT / "runtime" / "experiments" / "misc"))

import build_panel_models as B          # noqa: E402
from prematch_panel_scorer import group_of  # noqa: E402

OUT = ROOT / "runtime/artifacts/misc/panel_missing_blocks_cost.md"
PANEL_DIR = ROOT / "ml-models/prematch_panel"
GONE = ("public", "pairs", "rating", "hybrid")


def auc(y: np.ndarray, p: np.ndarray) -> float:
    o = np.argsort(p)
    r = np.empty(len(p), float)
    r[o] = np.arange(1, len(p) + 1)
    # средний ранг для совпадающих значений — иначе AUC зависит от порядка ввода
    ps = p[o]
    i = 0
    while i < len(ps):
        j = i
        while j + 1 < len(ps) and ps[j + 1] == ps[i]:
            j += 1
        if j > i:
            r[o[i:j + 1]] = (i + j + 2) / 2.0
        i = j + 1
    pos, neg = y == 1, y == 0
    n1, n0 = int(pos.sum()), int(neg.sum())
    if n1 == 0 or n0 == 0:
        return float("nan")
    return float((r[pos].sum() - n1 * (n1 + 1) / 2.0) / (n1 * n0))


def main() -> None:
    t0 = time.time()
    print("сборка матрицы панели…", flush=True)
    d = B.K.build_all()
    names = list(d["names"])
    parts = [d["X"]]
    off = d["X"].shape[1]
    n_base = off
    spans: dict[str, tuple[int, int]] = {}
    for fname in B.EXTRA:
        p = B.ART / fname
        if not p.exists():
            continue
        z = np.load(p, allow_pickle=True)
        if not np.array_equal(z["mids"], d["mids"]):
            raise SystemExit(f"{fname}: другой набор карт")
        F = z["F"].astype(np.float32)
        cn = ([str(x) for x in z["names"]] if "names" in z.files
              else [f"{Path(fname).stem}_{i}" for i in range(F.shape[1])])
        spans[fname] = (off, off + F.shape[1])
        parts.append(F)
        names += cn
        off += F.shape[1]
    X = np.empty((len(d["ts"]), off), dtype=np.float32)
    o = 0
    for blk in parts:
        X[:, o:o + blk.shape[1]] = blk
        o += blk.shape[1]
    del parts, blk
    kept, _n_prior = B.select_columns(names, spans, n_base, off)
    X = np.ascontiguousarray(X[:, kept])
    cols = [names[i] for i in kept]
    print(f"матрица: {X.shape[0]:,} x {X.shape[1]}", flush=True)

    art_cols = [str(c) for c in json.loads(
        (PANEL_DIR / "feature_names.json").read_text(encoding="utf-8"))["columns"]]
    if cols != art_cols:
        raise SystemExit("колонки матрицы разошлись с артефактом панели — "
                         "замер бессмысленен, пересобрать панель")
    neutral = {str(k): float(v) for k, v in json.loads(
        (PANEL_DIR / "feature_names.json").read_text(encoding="utf-8")
    ).get("neutral_by_column", {}).items()}

    idx = [i for i, c in enumerate(cols) if group_of(c) in GONE]
    if not idx:
        raise SystemExit("колонки выбитых блоков не найдены")
    print(f"выбитых колонок: {len(idx)} из {len(cols)}", flush=True)

    # У каждой цели СВОЯ маска (окно должно было доиграть, тотал вне «серой»
    # полосы и т.п.). Считать на голом `test` значило бы мерить не то, на чём
    # модель обучалась и на чём стоят числа в отчётах панели.
    test = d["test"]
    by_key = {t[0]: (np.asarray(t[4]), np.asarray(t[5])) for t in B.build_targets(d)}

    from catboost import CatBoostClassifier
    rows = []
    for path in sorted(PANEL_DIR.glob("*.cbm")):
        key = path.stem
        if key not in by_key:
            print(f"  {key}: цель не найдена, пропуск", flush=True)
            continue
        y_all, mask = by_key[key]
        sel = test & mask
        n = int(sel.sum())
        if n < 500:
            print(f"  {key}: карт {n}, слишком мало, пропуск", flush=True)
            continue
        Xs = X[sel]
        y = y_all[sel]
        variants = {"как учили": Xs}
        zero = Xs.copy()
        zero[:, idx] = 0.0
        variants["ноль (боевая машина)"] = zero
        mean = Xs.copy()
        for i in idx:
            mean[:, i] = np.float32(neutral.get(cols[i], 0.0))
        variants["среднее (правка)"] = mean
        m = CatBoostClassifier()
        m.load_model(str(path))
        got = {tag: auc(y, m.predict_proba(M)[:, 1]) for tag, M in variants.items()}
        rows.append((key, n, got))
        print(f"  {key} ({n:,} карт): "
              + "  ".join(f"{t} {v:.4f}" for t, v in got.items()), flush=True)
        del variants, zero, mean, Xs

    lines = ["# Цена непоставленных блоков панели", "",
             "Модели НЕ переобучались: одни и те же веса, тот же тест и та же "
             f"маска цели, меняются только {len(idx)} колонок входа из "
             f"{len(cols)} — те, чьих провайдеров нет на боевой машине.", "",
             "| модель | карт | как учили | ноль (прод) | среднее (правка) | "
             "цена нуля | польза правки |", "|---|---:|---:|---:|---:|---:|---:|"]
    for key, n, g in rows:
        a, z_, m_ = g["как учили"], g["ноль (боевая машина)"], g["среднее (правка)"]
        lines.append(f"| {key} | {n:,} | {a:.4f} | {z_:.4f} | {m_:.4f} | "
                     f"{z_ - a:+.4f} | {m_ - z_:+.4f} |")
    lines += ["", f"Прогон занял {time.time() - t0:.0f} c."]
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nотчёт: {OUT}", flush=True)


if __name__ == "__main__":
    main()
