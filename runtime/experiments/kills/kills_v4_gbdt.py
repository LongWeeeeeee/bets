#!/usr/bin/env python3
"""Килы v4: градиентный бустинг поверх тех же признаков вместо линейной модели.

Зачем. Вся линия E-164/E-165 линейная: логистическая регрессия на перцентилях.
Это единственный крупный неиспользованный рычаг, потому что признаки уже
относительные и предматчевые, а взаимодействий между ними линейная модель не
видит вовсе: «слабый мидер против сильного» и «слабый мидер, когда команда в
целом сильнее» для неё одно и то же.

Что именно сравнивается (на одной и той же выборке и одном сплите):
  L   линейная связка C из `kills_v3_train.py` — база, число берётся из её отчёта;
  G   бустинг на плотных признаках (+ командный блок, если он есть);
  GA  тот же бустинг, но драфт входит как ЧИСЛО — оценка линейной модели по
      парам. Для train-строк она обязана быть OOF, иначе бустинг увидит в ней
      подогнанную под эти же строки цель. Складки хронологические;
  GS  стек: бустинг(плотные) + драфт, комбайнер учится на валидации.

Утечки, за которыми следим отдельно:
  * скользящие средние строго по прошлому — это уже в `kills_v3_build.py`;
  * оценка драфта для train-строк — OOF по хронологическим складкам (режим `oof`);
  * ранняя остановка по валидации, метрика — на тесте, который в отборе
    гиперпараметров не участвует ни разу.

Запуск:
    python3 kills_v4_gbdt.py --corpus public --draft stack
    python3 kills_v4_gbdt.py --corpus pro --draft oof --targets w_5_15,ge27
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

from draft_features import DraftFeatureEncoder, KIND_PAIR, KIND_ROLE  # noqa: E402
from train_public_draft_hero10_experiment import atomic_joblib_dump  # noqa: E402
from window_model_v2 import auc_of  # noqa: E402
from kills_v3_train import (  # noqa: E402
    C_DRAFT, PAIR_MIN_SUPPORT, SYM, fit_logistic, make_targets, metrics,
    split_pro, split_public,
)

OUT_DIR = ROOT / "runtime/artifacts/kills/window_model_v2"
WINDOWS = ((5, 15), (10, 20), (15, 25), (20, 30))
OOF_FOLDS = 3
PARAMS = {
    "objective": "binary", "metric": "auc", "learning_rate": 0.05,
    "num_leaves": 255, "min_data_in_leaf": 300, "feature_fraction": 0.75,
    "bagging_fraction": 0.8, "bagging_freq": 1, "lambda_l2": 5.0,
    "max_bin": 127, "verbosity": -1, "num_threads": 0, "force_col_wise": True,
}


def load_matrix(corpus: str, use_extra: bool) -> tuple[np.ndarray, list[str], dict]:
    z = np.load(OUT_DIR / f"featuresv3_{corpus}.npz", allow_pickle=True)
    names = [str(s) for s in z["names"]]
    X = z["X"]
    if z["Xteam"].size:
        X = np.hstack([X, z["Xteam"]])
        names += [str(s) for s in z["team_names"]]
    if use_extra:
        for kind in ("extrav4", "dictv4"):
            p = OUT_DIR / f"{kind}_{corpus}.npz"
            if not p.exists():
                print(f"  ВНИМАНИЕ: {p.name} нет, иду без него", flush=True)
                continue
            e = np.load(p, allow_pickle=True)
            if len(e["X"]) != len(X):
                raise SystemExit(f"{p.name}: {len(e['X'])} строк против {len(X)} — "
                                 "пересоберите его после пересборки признаков")
            X = np.hstack([X, e["X"]])
            names += [str(s) for s in e["names"]]
            print(f"  добавлен {kind}: {e['X'].shape[1]} колонок", flush=True)
    return X, names, z


def draft_scores(z, parts, sel, y, sym: str, mode: str, heroes) -> dict[str, np.ndarray]:
    """Оценка драфта для всех трёх срезов; train — OOF, если mode='oof'."""
    import scipy.sparse as sp
    enc_pair = DraftFeatureEncoder.fit(heroes[parts["train"]], KIND_PAIR, signed=True,
                                       pair_min_support=PAIR_MIN_SUPPORT)
    enc_role = DraftFeatureEncoder.fit(heroes[parts["train"]], KIND_ROLE, signed=False)

    def block(rows):
        h = heroes[rows]
        if sym == "sym":
            return enc_role.transform(h)
        if sym == "side":
            return sp.hstack([enc_pair.transform(h), enc_role.transform(h)], format="csr")
        return enc_pair.transform(h)

    tr = parts["train"][sel["train"]]
    out = {}
    full = fit_logistic(block(tr), y["train"], C_DRAFT)
    for k in ("val", "test"):
        out[k] = full.decision_function(block(parts[k][sel[k]])).astype(np.float32)
    if mode == "oof":
        oof = np.empty(len(tr), dtype=np.float32)
        bounds = np.linspace(0, len(tr), OOF_FOLDS + 1).astype(int)
        for f in range(OOF_FOLDS):
            a, b = bounds[f], bounds[f + 1]
            hold = np.arange(a, b)
            rest = np.concatenate([np.arange(0, a), np.arange(b, len(tr))])
            m = fit_logistic(block(tr[rest]), y["train"][rest], C_DRAFT)
            oof[hold] = m.decision_function(block(tr[hold])).astype(np.float32)
            print(f"      OOF складка {f + 1}/{OOF_FOLDS}", flush=True)
        out["train"] = oof
    else:
        out["train"] = full.decision_function(block(tr)).astype(np.float32)
    out["_model"] = full
    return out


def fit_backend(backend: str, Xtr, ytr, Xva, yva, cols, rounds):
    """Возвращает (predict(X) -> proba, число деревьев, AUC валидации, важности).

    Два движка, потому что на serv2 нет libomp и lightgbm там не грузится, а
    sklearn везёт свой OpenMP в колесе. Числа их не смешиваются: движок пишется
    в отчёт.
    """
    if backend == "lgb":
        import lightgbm as lgb
        dtr = lgb.Dataset(Xtr, label=ytr, feature_name=cols, free_raw_data=True)
        dva = lgb.Dataset(Xva, label=yva, reference=dtr, free_raw_data=True)
        b = lgb.train(PARAMS, dtr, num_boost_round=rounds, valid_sets=[dva],
                      valid_names=["val"],
                      callbacks=[lgb.early_stopping(100, verbose=False),
                                 lgb.log_evaluation(200)])
        imp = dict(zip(cols, b.feature_importance("gain")))
        return ((lambda X: b.predict(X, num_iteration=b.best_iteration)),
                int(b.best_iteration), float(b.best_score["val"]["auc"]), imp, b)
    from sklearn.ensemble import HistGradientBoostingClassifier
    m = HistGradientBoostingClassifier(
        max_iter=rounds, learning_rate=PARAMS["learning_rate"],
        max_leaf_nodes=63, min_samples_leaf=PARAMS["min_data_in_leaf"],
        l2_regularization=PARAMS["lambda_l2"], max_bins=PARAMS["max_bin"],
        early_stopping=True, validation_fraction=0.1, n_iter_no_change=30,
        random_state=0).fit(Xtr, ytr)
    pv = m.predict_proba(Xva)[:, 1]
    return ((lambda X: m.predict_proba(X)[:, 1]), int(m.n_iter_),
            float(auc_of(yva, pv)), {}, m)


def run(corpus: str, targets: list[str], draft_mode: str, use_extra: bool,
        rounds: int, tag: str, backend: str = "lgb", ingame: bool = False) -> None:
    X, names, z = load_matrix(corpus, use_extra)
    if ingame:
        # ТОЛЬКО для верхней оценки: сколько вообще можно выжать, если знать
        # состояние карты на минуте гейта. В предматчевую модель это НЕ входит.
        X = np.hstack([X, z["nwlead"].astype(np.float32), z["xplead"].astype(np.float32)])
        names = names + [f"nw_at_{i}" for i in range(z["nwlead"].shape[1])] \
                      + [f"xp_at_{i}" for i in range(z["xplead"].shape[1])]
        print("  ВНИМАНИЕ: включены ИНГЕЙМ-колонки — это потолок, не продукт", flush=True)
    n = len(X)
    heroes, patch = z["heroes"], z["patch"]
    keep = np.arange(n)
    tg = make_targets(z, keep)
    parts, cut = split_public(n) if corpus == "public" else split_pro(patch, n)
    print(f"[{corpus}] карт {n:,}, колонок {X.shape[1]}; "
          + ", ".join(f"{k} {len(v):,}" for k, v in parts.items())
          + (f"; тест = патчи >= {cut}" if cut else ""), flush=True)

    report = {"corpus": corpus, "rows": int(n), "columns": int(X.shape[1]),
              "draft_mode": draft_mode, "extra": bool(use_extra), "params": PARAMS,
              "split": {k: int(len(v)) for k, v in parts.items()}, "targets": {}}
    for name in targets:
        if name not in tg:
            print(f"  цель {name} неизвестна, пропуск", flush=True)
            continue
        t0 = time.time()
        t = tg[name]
        sel = {k: t["mask"][v] for k, v in parts.items()}
        y = {k: t["y"][v][sel[k]] for k, v in parts.items()}
        rows = {k: parts[k][sel[k]] for k in parts}
        print(f"\n=== {name}: train {len(y['train']):,} / test {len(y['test']):,}, "
              f"база {y['test'].mean():.3f} ===", flush=True)

        ds = draft_scores(z, parts, sel, y, SYM[name], draft_mode, heroes)
        cols = names + ["draft_score"]
        def design(k):
            return np.hstack([X[rows[k]], ds[k][:, None].astype(np.float32)])

        predict, trees, val_auc, imp_map, model = fit_backend(
            backend, design("train"), y["train"], design("val"), y["val"], cols, rounds)
        Xte = design("test")
        p_g = predict(Xte)
        entry = {"G_gbdt": metrics(y["test"], p_g), "trees": trees,
                 "val_auc": val_auc, "backend": backend,
                 "seconds": round(time.time() - t0, 1)}

        # стек с драфтом: комбайнер на валидации, где бустинг ещё не учился
        from sklearn.linear_model import LogisticRegression
        p_gv = np.clip(predict(design("val")), 1e-6, 1 - 1e-6)
        p_g = np.clip(p_g, 1e-6, 1 - 1e-6)
        comb = LogisticRegression(C=1.0, max_iter=1000).fit(
            np.stack([np.log(p_gv / (1 - p_gv)), ds["val"]], axis=1), y["val"])
        p_s = comb.predict_proba(
            np.stack([np.log(p_g / (1 - p_g)), ds["test"]], axis=1))[:, 1]
        entry["GS_stacked"] = metrics(y["test"], p_s)
        entry["A_draft"] = metrics(y["test"], 1.0 / (1.0 + np.exp(-ds["test"])))
        imp = sorted(imp_map.items(), key=lambda x: -x[1])[:25]
        entry["top_gain"] = [[k, round(float(v), 1)] for k, v in imp]
        # Доля объяснённой дисперсии САМОЙ разницы килов — она сравнима с
        # таблицей «f -> AUC» из kills_v4_ceiling_theory.py, а AUC не сравним.
        wi = [f"w_{a}_{b}" for a, b in WINDOWS].index(name) if name.startswith("w_") else -1
        if wi >= 0:
            dd = z["diffs"][rows["test"], wi].astype(np.float64)
            entry["explained_var_share"] = float(np.corrcoef(dd, p_s)[0, 1] ** 2)
        np.savez_compressed(OUT_DIR / f"{tag}_{corpus}_{name}_test.npz",
                            y=y["test"], p_gbdt=p_g, p_stack=p_s,
                            p_draft=1.0 / (1.0 + np.exp(-ds["test"])),
                            mid=z["mid"][rows["test"]], ts=z["ts"][rows["test"]])
        report["targets"][name] = entry
        for k in ("A_draft", "G_gbdt", "GS_stacked"):
            v = entry[k]
            print(f"  {k:12s} AUC {v['auc']:.4f}  точн {v['accuracy']:.4f}", flush=True)
        print(f"  деревьев {entry['trees']}, {entry['seconds']:.0f}с; "
              f"топ-5 по вкладу: {[k for k, _ in imp[:5]]}", flush=True)
        if backend == "lgb":
            model.save_model(str(OUT_DIR / f"{tag}_{corpus}_{name}.lgb"))
        else:
            atomic_joblib_dump(model, OUT_DIR / f"{tag}_{corpus}_{name}.joblib")
        atomic_joblib_dump({"comb": comb, "draft": ds["_model"]},
                           OUT_DIR / f"{tag}_{corpus}_{name}_aux.joblib")
        del Xte, ds

    out = OUT_DIR / f"report_{tag}_{corpus}.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nотчёт: {out}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", default="public", choices=("public", "pro"))
    ap.add_argument("--targets", default="w_5_15,w_10_20,w_15_25,w_20_30")
    ap.add_argument("--draft", default="stack", choices=("stack", "oof"))
    ap.add_argument("--extra", action="store_true")
    ap.add_argument("--rounds", type=int, default=4000)
    ap.add_argument("--tag", default="v4gbdt")
    ap.add_argument("--backend", default="lgb", choices=("lgb", "hgb"))
    ap.add_argument("--ingame", action="store_true",
                    help="верхняя оценка: добавить состояние карты на минуте гейта")
    args = ap.parse_args()
    run(args.corpus, [s for s in args.targets.split(",") if s], args.draft,
        args.extra, args.rounds, args.tag, args.backend, args.ingame)


if __name__ == "__main__":
    main()
