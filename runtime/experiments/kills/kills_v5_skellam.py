#!/usr/bin/env python3
"""Килы v5: моделировать СЧЁТЧИКИ, а не знак. Три оценщика на одной выборке.

Почему это может дать больше. Вся линия (E-164, E-165, v3, v4) учит классификатор
знака: карта, где сторона выиграла окно 12:2, и карта, где выиграла 6:5, дают ему
одну и ту же метку «1». Между тем первая говорит про темпы килов несравнимо
больше. Информация о величине выбрасывается на этапе разметки, и никакая модель
её потом не вернёт.

Три оценщика, все на одном сплите и одних признаках:

  C  классификация знака (как раньше) — база;
  R  регрессия РАЗНИЦЫ килов, ранжирование по предсказанной разнице;
  S  два пуассоновских регрессора — ожидаемые килы каждой стороны, — и точная
     вероятность P(Kr > Kd) по распределению Скеллама (разность пуассонов).

У S есть свойство, которого нет у C и R: он даёт вероятность, согласованную с
дискретностью и с ничьими. Ничья у нас считается поражением, а Скеллам даёт
P(Kr > Kd) напрямую, без «а куда девать ноль».

Симметрия сторон встроена: оценщик S учится на ОБЪЕДИНЁННОЙ выборке из двух
видов карты (взгляд радианта и взгляд дайра с перевёрнутыми разностными
колонками), поэтому одна и та же модель предсказывает килы любой стороны, а не
«килы радианта».

Запуск:
    venv_catboost/bin/python3 runtime/experiments/kills/kills_v5_skellam.py \
        --corpus pro --targets w_5_15,w_10_20,w_15_25,w_20_30
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))
sys.path.insert(0, str(ROOT / "runtime/experiments/kills"))

from window_model_v2 import auc_of  # noqa: E402
from kills_v3_train import SYM, make_targets, metrics, split_pro, split_public  # noqa: E402
from kills_v4_gbdt import PARAMS, draft_scores, load_matrix  # noqa: E402

OUT_DIR = ROOT / "runtime/artifacts/kills/window_model_v2"
WINDOWS = ((5, 15), (10, 20), (15, 25), (20, 30))


def flip_cols(names: list[str]) -> np.ndarray:
    return np.asarray([s.endswith("_diff") or s.endswith("_cov") for s in names])


def fit_lgb(Xtr, ytr, Xva, yva, params, rounds, cols):
    import lightgbm as lgb
    p = dict(PARAMS)
    p.update(params)
    dtr = lgb.Dataset(Xtr, label=ytr, feature_name=cols, free_raw_data=True)
    dva = lgb.Dataset(Xva, label=yva, reference=dtr, free_raw_data=True)
    return lgb.train(p, dtr, num_boost_round=rounds, valid_sets=[dva],
                     callbacks=[lgb.early_stopping(100, verbose=False),
                                lgb.log_evaluation(0)])


def skellam_sf0(mu1: np.ndarray, mu2: np.ndarray) -> np.ndarray:
    """P(K1 > K2) для независимых пуассонов; ничья НЕ считается победой."""
    from scipy.stats import skellam
    return skellam.sf(0, np.clip(mu1, 1e-3, None), np.clip(mu2, 1e-3, None))


def run(corpus: str, targets: list[str], rounds: int, max_train: int, tag: str) -> None:
    X, names, z = load_matrix(corpus, True)
    n = len(X)
    heroes, patch = z["heroes"], z["patch"]
    tg = make_targets(z, np.arange(n))
    parts, cut = split_public(n) if corpus == "public" else split_pro(patch, n)
    neg = flip_cols(names)
    diffs, totals, valid = z["diffs"], z["totals"], z["valid"]
    print(f"[{corpus}] карт {n:,}, колонок {X.shape[1]}; "
          + ", ".join(f"{k} {len(v):,}" for k, v in parts.items()), flush=True)

    report = {"corpus": corpus, "rows": int(n), "columns": int(X.shape[1]),
              "max_train": max_train, "targets": {}}
    for name in targets:
        wi = [f"w_{a}_{b}" for a, b in WINDOWS].index(name)
        t0 = time.time()
        t = tg[name]
        sel = {k: t["mask"][v] for k, v in parts.items()}
        rows = {k: parts[k][sel[k]] for k in parts}
        y = {k: t["y"][v][sel[k]] for k, v in parts.items()}
        if max_train and len(rows["train"]) > max_train:
            rows["train"] = rows["train"][-max_train:]
            y["train"] = y["train"][-max_train:]
        ds = draft_scores(z, parts, sel, y, SYM[name], "stack", heroes)
        if max_train and len(ds["train"]) > len(rows["train"]):
            ds["train"] = ds["train"][-len(rows["train"]):]
        cols = names + ["draft_score"]

        def design(part: str, flip: bool = False) -> np.ndarray:
            r = rows[part]
            out = np.empty((len(r), len(cols)), dtype=np.float32)
            for s in range(0, len(r), 200_000):
                chunk = r[s:s + 200_000]
                e = s + len(chunk)
                blk = X[chunk]
                if flip:
                    blk[:, neg] = -blk[:, neg]
                out[s:e, :len(names)] = blk
                d = ds[part][s:e]
                out[s:e, len(names)] = -d if flip else d
            return out

        kr = {k: ((totals[rows[k], wi] + diffs[rows[k], wi]) // 2).astype(np.float32)
              for k in rows}
        kd = {k: ((totals[rows[k], wi] - diffs[rows[k], wi]) // 2).astype(np.float32)
              for k in rows}
        dif = {k: diffs[rows[k], wi].astype(np.float32) for k in rows}
        entry = {}

        # ---- C: классификация знака
        m = fit_lgb(design("train"), y["train"], design("val"), y["val"],
                    {"objective": "binary", "metric": "auc"}, rounds, cols)
        pc = m.predict(design("test"), num_iteration=m.best_iteration)
        entry["C_sign"] = metrics(y["test"], pc)

        # ---- R: регрессия разницы
        m = fit_lgb(design("train"), dif["train"], design("val"), dif["val"],
                    {"objective": "regression", "metric": "l2"}, rounds, cols)
        pr = m.predict(design("test"), num_iteration=m.best_iteration)
        entry["R_diff"] = {"auc": auc_of(y["test"], pr), "n": int(len(y["test"])),
                           "base": float(y["test"].mean()),
                           "accuracy": float(((pr > 0).astype(int) == y["test"]).mean()),
                           "corr": float(np.corrcoef(dif["test"], pr)[0, 1])}

        # ---- S: два пуассона на объединённом виде обеих сторон
        Xtr = np.vstack([design("train"), design("train", flip=True)])
        ytr = np.concatenate([kr["train"], kd["train"]])
        Xva = np.vstack([design("val"), design("val", flip=True)])
        yva = np.concatenate([kr["val"], kd["val"]])
        m = fit_lgb(Xtr, ytr, Xva, yva, {"objective": "poisson", "metric": "poisson"},
                    rounds, cols)
        del Xtr, Xva
        lam_r = m.predict(design("test"), num_iteration=m.best_iteration)
        lam_d = m.predict(design("test", flip=True), num_iteration=m.best_iteration)
        ps = skellam_sf0(lam_r, lam_d)
        entry["S_skellam"] = metrics(y["test"], ps)
        entry["S_skellam"]["lam_corr"] = float(
            np.corrcoef(lam_r - lam_d, dif["test"])[0, 1])
        entry["seconds"] = round(time.time() - t0, 1)
        report["targets"][name] = entry
        for k in ("C_sign", "R_diff", "S_skellam"):
            print(f"  {name} {k:10s} AUC {entry[k]['auc']:.4f}", flush=True)
        np.savez_compressed(OUT_DIR / f"{tag}_{corpus}_{name}_test.npz",
                            y=y["test"], p_stack=ps, p_gbdt=pc, p_draft=pr,
                            lam_r=lam_r, lam_d=lam_d, mid=z["mid"][rows["test"]])
        del ds

    out = OUT_DIR / f"report_{tag}_{corpus}.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nотчёт: {out}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", default="pro")
    ap.add_argument("--targets", default="w_5_15,w_10_20,w_15_25,w_20_30")
    ap.add_argument("--rounds", type=int, default=3000)
    ap.add_argument("--max-train", type=int, default=0)
    ap.add_argument("--tag", default="v5skellam")
    args = ap.parse_args()
    run(args.corpus, [s for s in args.targets.split(",") if s], args.rounds,
        args.max_train, args.tag)


if __name__ == "__main__":
    main()
