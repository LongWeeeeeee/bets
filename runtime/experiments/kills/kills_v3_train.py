#!/usr/bin/env python3
"""Обучение килов v3: окна + итог карты + «сторона наберёт 27» + «тотал 51».

Одна выборка признаков (`kills_v3_build.py`) и семь целей, потому что килы везде
одни и те же, а различается только то, ЧТО спрашивают у карты:

| цель | что предсказываем | симметрия |
|---|---|---|
| `w_5_15` … `w_20_30` | кто сделает больше килов в окне, ничья = поражение | антисимметрична |
| `map` | кто сделает больше килов за карту, ничья = поражение | антисимметрична |
| `ge27` | наберёт ли СТОРОНА 27+ килов (цель прод-модели) | сторонняя |
| `tot51` | наберёт ли КАРТА 51+ килов суммарно | симметрична |

Из симметрии вытекает конструкция входа — это не украшение, а обязательное условие:

* антисимметричная цель: знаковый парный кодировщик драфта (`KIND_PAIR, signed`)
  и разностные колонки. Меняем стороны местами — предсказание обязано перевернуться;
* симметричная цель: РАЗНОСТИ бесполезны по построению (у карт 30:5 и 8:3 разница
  одна, а тотал разный). Вход — беззнаковый `KIND_ROLE` (свойство карты, а не
  противостояния) плюс уровневые колонки `_lvl` плюс модули разностей;
* сторонняя цель `ge27` — обе части сразу: кто сильнее И насколько кровавая карта.
  Для неё считается отдельная проверка на виде со стороны dire: перевернув карту,
  модель обязана предсказывать 27+ у другой стороны с тем же качеством.

Стадии:
    --stage public   обучение на всех паблик-картах, хронологический сплит 60/20/20
    --stage pro      обучение на про + перенос паблик-модели как отдельная колонка

У про сплит другой и это принципиально: тест — свежие патчи (7.41c+ по умолчанию),
чтобы драфт успел увидеть 7.41 в обучении. Хронологические 60/20/20 на про-корпусе
отправили бы ВЕСЬ 7.41 в тест (его там 28 тыс. из 303 тыс.), и «драфт по 7.41+»
стало бы нечем учить.

Запуск:
    python3 kills_v3_train.py --stage public
    python3 kills_v3_train.py --stage pro --transfer-from v3pub
    python3 kills_v3_train.py --stage public --smoke 200000   # проверка конвейера
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
import scipy.sparse as sp

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))
sys.path.insert(0, str(ROOT / "runtime/experiments/kills"))

from draft_features import DraftFeatureEncoder, KIND_PAIR, KIND_ROLE  # noqa: E402
from train_public_draft_hero10_experiment import atomic_joblib_dump  # noqa: E402
from window_model_v2 import percentile_apply, percentile_fit, auc_of  # noqa: E402

OUT_DIR = ROOT / "runtime/artifacts/kills/window_model_v2"
WINDOWS = ((5, 15), (10, 20), (15, 25), (20, 30))
C_DENSE = 0.003
C_DRAFT = 0.003
PAIR_MIN_SUPPORT = 30
TARGET_KILLS = 27
TARGET_TOTAL = 51
PRO_TEST_MIN_ROWS = 8000
SWAP = np.asarray([5, 6, 7, 8, 9, 0, 1, 2, 3, 4])
SYM = {**{f"w_{a}_{b}": "anti" for a, b in WINDOWS},
       "map": "anti", "ge27": "side", "tot51": "sym"}

# Модули разностей для уровневых целей: при сильном перекосе карта короче и
# кровавость падает — знак тут не важен, важна величина.
ABS_PREFIX = ("hero_wdiff_", "heropos_wdiff_", "pl_wdiff_")
ABS_EXACT = ("pl_kills_30_diff", "pl_deaths_30_diff", "pl_assists_30_diff",
             "pl_win_30_diff", "pl_gpm_30_diff", "pl_networth_30_diff",
             "hero_kills_all_diff", "hero_win_all_diff", "hero_deaths_all_diff")


# --------------------------------------------------------------------- цели
def make_targets(z, keep: np.ndarray) -> dict[str, dict]:
    ks = z["kills_side"][keep].astype(np.int32)
    diffs, valid = z["diffs"][keep], z["valid"][keep]
    n = len(ks)
    out: dict[str, dict] = {}
    for wi, (a, b) in enumerate(WINDOWS):
        out[f"w_{a}_{b}"] = {"y": (diffs[:, wi] > 0).astype(np.int32),
                             "mask": valid[:, wi].astype(bool)}
    out["map"] = {"y": (ks[:, 0] > ks[:, 1]).astype(np.int32), "mask": np.ones(n, bool)}
    out["ge27"] = {"y": (ks[:, 0] >= TARGET_KILLS).astype(np.int32), "mask": np.ones(n, bool)}
    out["tot51"] = {"y": (ks.sum(1) >= TARGET_TOTAL).astype(np.int32), "mask": np.ones(n, bool)}
    for k in out:
        out[k]["sym"] = SYM[k]
    return out


def abs_index(names: list[str]) -> np.ndarray:
    return np.asarray([j for j, s in enumerate(names)
                       if s in ABS_EXACT or any(s.startswith(p) for p in ABS_PREFIX)],
                      dtype=np.int64)


def lvl_index(names: list[str]) -> np.ndarray:
    return np.asarray([j for j, s in enumerate(names) if s.endswith("_lvl")], dtype=np.int64)


def flip_signs(X: np.ndarray, names) -> np.ndarray:
    """Вид со стороны dire: разностные и покрытийные колонки меняют знак, уровневые нет."""
    out = np.array(X, copy=True)
    for j, s in enumerate(names):
        s = str(s)
        if s.endswith("_diff") or s.endswith("_cov"):
            out[:, j] = -out[:, j]
    return out


def fit_logistic(X, y, c: float):
    from sklearn.linear_model import LogisticRegression
    m = LogisticRegression(C=c, max_iter=1000, solver="lbfgs").fit(X, y)
    if int(np.max(m.n_iter_)) >= 1000:
        print("    ВНИМАНИЕ: lbfgs не сошёлся за 1000 итераций — числу верить нельзя",
              flush=True)
    return m


def metrics(y: np.ndarray, p: np.ndarray) -> dict:
    p = np.clip(np.asarray(p, dtype=np.float64), 1e-6, 1 - 1e-6)
    return {"auc": auc_of(y, p),
            "accuracy": float(((p > 0.5).astype(np.int32) == y).mean()),
            "logloss": float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p))),
            "base": float(y.mean()), "n": int(len(y))}


# ------------------------------------------------------------------- сплиты
def split_public(n: int) -> tuple[dict[str, np.ndarray], int]:
    a, b = n * 60 // 100, n * 80 // 100
    idx = np.arange(n)
    return {"train": idx[:a], "val": idx[a:b], "test": idx[b:]}, 0


def split_pro(patch: np.ndarray, n: int) -> tuple[dict[str, np.ndarray], int]:
    """Тест — самые свежие патчи; val — 15% строк перед тестом; остальное train."""
    idx = np.arange(n)
    cut = 0
    for c in sorted(np.unique(patch[patch > 0]), reverse=True):
        if int((patch >= c).sum()) >= PRO_TEST_MIN_ROWS:
            cut = int(c)
            break
    if cut:
        test, rest = idx[patch >= cut], idx[patch < cut]
    else:
        b = n * 80 // 100
        test, rest = idx[b:], idx[:b]
    v = max(1, int(len(rest) * 0.15))
    return {"train": rest[:-v], "val": rest[-v:], "test": test}, cut


# ------------------------------------------------------------------- стадия
def run(corpus: str, smoke: int, transfer_from: str | None) -> None:
    z = np.load(OUT_DIR / f"featuresv3_{corpus}.npz", allow_pickle=True)
    names = [str(s) for s in z["names"]]
    team_names = [str(s) for s in z["team_names"]]
    n_all = len(z["ts"])
    keep = np.arange(n_all - smoke, n_all) if (smoke and n_all > smoke) else np.arange(n_all)
    n = len(keep)
    heroes, patch = z["heroes"][keep], z["patch"][keep]
    wins = z["wins"][keep].astype(np.int32)

    targets = make_targets(z, keep)
    parts, cut = split_public(n) if corpus == "public" else split_pro(patch, n)
    print(f"[{corpus}] карт {n:,}; " + ", ".join(f"{k} {len(v):,}" for k, v in parts.items())
          + (f"; тест = патчи >= {cut}" if cut else ""), flush=True)

    prefix = ("v3pub" if corpus == "public"
              else ("v3prot" if transfer_from else "v3pro"))
    report = {"corpus": corpus, "rows": int(n), "features": len(names),
              "team_features": len(team_names), "C_dense": C_DENSE, "C_draft": C_DRAFT,
              "transfer_from": transfer_from or "",
              "split": {k: int(len(v)) for k, v in parts.items()},
              "pro_test_patch": int(cut),
              "patches": {k: {str(p): int(c) for p, c in
                              zip(*np.unique(patch[v], return_counts=True))}
                          for k, v in parts.items()},
              "targets": {}}
    for k, t in targets.items():
        report.setdefault("base_rate", {})[k] = float(
            t["y"][parts["test"]][t["mask"][parts["test"]]].mean())

    # ------------------------------------------------ фаза 1: драфт
    print("\n[фаза 1] кодировщики драфта", flush=True)
    enc_pair = DraftFeatureEncoder.fit(heroes[parts["train"]], KIND_PAIR, signed=True,
                                       pair_min_support=PAIR_MIN_SUPPORT)
    enc_role = DraftFeatureEncoder.fit(heroes[parts["train"]], KIND_ROLE, signed=False)
    print(f"  колонок: пары {enc_pair.n_columns:,}, роли {enc_role.n_columns:,}", flush=True)
    atomic_joblib_dump({"pair": enc_pair, "role": enc_role},
                       OUT_DIR / f"{prefix}_encoders.joblib")

    def draft(rows: np.ndarray, sym: str, swap: bool = False):
        """Разреженный блок драфта для выбранных строк; матрицы не кэшируются —
        три сплита сразу на 5 млн строк не помещаются в память."""
        h = heroes[rows][:, SWAP] if swap else heroes[rows]
        if sym == "sym":
            return enc_role.transform(h)
        if sym == "side":
            return sp.hstack([enc_pair.transform(h), enc_role.transform(h)], format="csr")
        return enc_pair.transform(h)

    wm = fit_logistic(draft(parts["train"], "anti"), wins[parts["train"]], C_DRAFT)
    atomic_joblib_dump(wm, OUT_DIR / f"{prefix}_winmodel.joblib")
    wl = np.empty(n, dtype=np.float32)          # логит победы радианта, по кускам
    for s in range(0, n, 500_000):
        rows = np.arange(s, min(s + 500_000, n))
        wl[rows] = wm.decision_function(draft(rows, "anti")).astype(np.float32)

    a_scores: dict[str, dict] = {}
    for name, t in targets.items():
        sym = t["sym"]
        sel = {k: t["mask"][v] for k, v in parts.items()}
        y = {k: t["y"][v][sel[k]] for k, v in parts.items()}
        if min(len(y[k]) for k in y) < 2000:
            print(f"  {name}: строк мало, пропуск", flush=True)
            continue
        a_tr = fit_logistic(draft(parts["train"][sel["train"]], sym), y["train"], C_DRAFT)
        cur = {k: a_tr.decision_function(draft(parts[k][sel[k]], sym)).astype(np.float32)
               for k in ("val", "test")}
        rows_fit = np.concatenate([parts["train"][sel["train"]], parts["val"][sel["val"]]])
        full = fit_logistic(draft(rows_fit, sym),
                            np.concatenate([y["train"], y["val"]]), C_DRAFT)
        cur["A_metrics"] = metrics(
            y["test"], full.predict_proba(draft(parts["test"][sel["test"]], sym))[:, 1])
        a_scores[name] = cur
        atomic_joblib_dump(full, OUT_DIR / f"{prefix}_A_{name}.joblib")
        atomic_joblib_dump(a_tr, OUT_DIR / f"{prefix}_Atr_{name}.joblib")
        print(f"  {name:8s} A(драфт) AUC {cur['A_metrics']['auc']:.4f}", flush=True)

    # ------------------------------------------------ фаза 2: плотные признаки
    print("\n[фаза 2] плотные признаки", flush=True)
    # z["X"] и так отдаёт свежий массив; `np.array(...)[keep]` держал бы ДВЕ копии
    # по 3.9 ГБ одновременно — на 5 млн карт это разница между «влезло» и свопом.
    X = z["X"]
    if len(keep) != len(X):
        X = X[keep]
    edges = []
    for j in range(X.shape[1]):
        e = percentile_fit(X[parts["train"], j])
        X[:, j] = percentile_apply(X[:, j], e) - 0.5
        edges.append(e)
    atomic_joblib_dump({"names": names, "edges": edges}, OUT_DIR / f"{prefix}_scaler.joblib")
    print(f"  шкала по train, матрица {X.nbytes / 2**30:.2f} ГБ", flush=True)

    Xteam = z["Xteam"] if z["Xteam"].size else np.zeros((n, 0), np.float32)
    if len(Xteam) != n:
        Xteam = Xteam[keep]
    if Xteam.shape[1]:
        tedges = []
        for j in range(Xteam.shape[1]):
            e = percentile_fit(Xteam[parts["train"], j])
            Xteam[:, j] = percentile_apply(Xteam[:, j], e) - 0.5
            tedges.append(e)
        atomic_joblib_dump({"names": team_names, "edges": tedges},
                           OUT_DIR / f"{prefix}_team_scaler.joblib")
        print(f"  командных колонок: {Xteam.shape[1]}", flush=True)

    ai, li = abs_index(names), lvl_index(names)
    print(f"  колонок с модулем: {len(ai)}, уровневых: {len(li)}", flush=True)
    trans = load_transfer(transfer_from, z, keep, names) if transfer_from else {}

    neg = np.asarray([s.endswith("_diff") or s.endswith("_cov") for s in names])
    neg_team = np.asarray([s.endswith("_diff") or s.endswith("_cov") for s in team_names])

    def dense(rows: np.ndarray, sym: str, tname: str, swap: bool = False) -> np.ndarray:
        """Плотный вход. Заполняется КУСКАМИ в готовую матрицу: `np.hstack` на
        3 млн строк держал бы промежуточную копию X той же величины, и пик
        памяти вырастал вдвое."""
        base = li if sym == "sym" else np.arange(X.shape[1])
        width = len(base) + (len(ai) if sym in ("sym", "side") else 0) + 2 + Xteam.shape[1]
        width += 1 if tname in trans else 0
        out = np.empty((len(rows), width), dtype=np.float32)
        for s in range(0, len(rows), 200_000):
            r = rows[s:s + 200_000]
            e = s + len(r)
            block = X[r]
            if swap:
                block[:, neg] = -block[:, neg]
            k = len(base)
            out[s:e, :k] = block[:, base]
            if sym in ("sym", "side"):
                out[s:e, k:k + len(ai)] = np.abs(block[:, ai])
                k += len(ai)
            w = -wl[r] if swap else wl[r]
            out[s:e, k] = w / 4.0
            out[s:e, k + 1] = np.abs(w) / 4.0
            k += 2
            if Xteam.shape[1]:
                tt = Xteam[r]
                if swap:
                    tt[:, neg_team] = -tt[:, neg_team]
                out[s:e, k:k + Xteam.shape[1]] = tt
                k += Xteam.shape[1]
            if tname in trans:
                out[s:e, k] = trans[f"{tname}_flip" if swap else tname][r]
        return out

    from sklearn.linear_model import LogisticRegression
    for name, t in targets.items():
        if name not in a_scores:
            continue
        sym = t["sym"]
        sel = {k: t["mask"][v] for k, v in parts.items()}
        y = {k: t["y"][v][sel[k]] for k, v in parts.items()}
        b_tr = fit_logistic(dense(parts["train"][sel["train"]], sym, name), y["train"], C_DENSE)
        b_val = b_tr.decision_function(dense(parts["val"][sel["val"]], sym, name)).astype(np.float32)
        Bt = dense(parts["test"][sel["test"]], sym, name)
        b_test = b_tr.decision_function(Bt).astype(np.float32)
        b_prob = b_tr.predict_proba(Bt)[:, 1]
        del Bt
        comb = LogisticRegression(C=1.0, max_iter=1000).fit(
            np.stack([a_scores[name]["val"], b_val], axis=1), y["val"])
        c_test = comb.predict_proba(np.stack([a_scores[name]["test"], b_test], axis=1))[:, 1]

        entry = {"A_draft": a_scores[name]["A_metrics"],
                 "B_features": metrics(y["test"], b_prob),
                 "C_stacked": {"weights": comb.coef_[0].tolist(),
                               **metrics(y["test"], c_test)}}
        if name in trans:
            entry["T_transfer"] = metrics(y["test"], trans[name][parts["test"]][sel["test"]])
        atomic_joblib_dump(b_tr, OUT_DIR / f"{prefix}_B_{name}.joblib")
        atomic_joblib_dump(comb, OUT_DIR / f"{prefix}_C_{name}.joblib")

        if name == "ge27":
            rows = parts["test"][sel["test"]]
            import joblib
            a_flip = joblib.load(OUT_DIR / f"{prefix}_Atr_ge27.joblib").decision_function(
                draft(rows, sym, swap=True)).astype(np.float32)
            b_flip = b_tr.decision_function(dense(rows, sym, name, swap=True)).astype(np.float32)
            p_flip = comb.predict_proba(np.stack([a_flip, b_flip], axis=1))[:, 1]
            y_flip = (z["kills_side"][keep][rows][:, 1] >= TARGET_KILLS).astype(np.int32)
            entry["dire_view_C"] = metrics(y_flip, p_flip)
            # Объединённый честный замер: обе стороны как отдельные наблюдения.
            entry["both_sides_C"] = metrics(np.concatenate([y["test"], y_flip]),
                                            np.concatenate([c_test, p_flip]))

        report["targets"][name] = entry
        for k, v in entry.items():
            print(f"  {name:8s} {k:12s} AUC {v['auc']:.4f}  точн {v['accuracy']:.4f}"
                  f"  база {v['base']:.3f}  n {v['n']:,}", flush=True)

    (OUT_DIR / f"report_{prefix}.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nотчёт: {OUT_DIR / f'report_{prefix}.json'}")


def score_all(src: str, z, keep: np.ndarray, names: list[str], *,
              with_team: bool = False, use_saved_scale: bool = False,
              flips: tuple[bool, ...] = (False,)) -> dict[str, np.ndarray]:
    """Вероятности связки C модели `src` по всем целям на строках текущего корпуса.

    Два режима шкалы, и путать их нельзя:

    * `use_saved_scale=False` — перцентили считаются ВНУТРИ текущего корпуса. Это
      режим ПЕРЕНОСА между корпусами и весь смысл относительных величин: игрок
      80-го перцентиля среди про сопоставим с игроком 80-го перцентиля среди
      пабликов, хотя абсолютные килы разные;
    * `use_saved_scale=True` — берутся границы, сохранённые при обучении. Это режим
      скоринга СВОИМ корпусом: пересчёт по всему корпусу подмешал бы распределение
      теста в шкалу.

    `flips=(False, True)` добавляет перевёрнутый вид (`*_flip`) — вид со стороны
    dire, нужный сторонней цели `ge27`.
    """
    import joblib
    saved = joblib.load(OUT_DIR / f"{src}_scaler.joblib")
    if list(saved["names"]) != names:
        raise SystemExit("наборы признаков корпусов разошлись — скоринг невозможен")
    encs = joblib.load(OUT_DIR / f"{src}_encoders.joblib")
    wm = joblib.load(OUT_DIR / f"{src}_winmodel.joblib")
    heroes = z["heroes"][keep]
    X = z["X"]
    if len(keep) != len(X):
        X = X[keep]
    for j in range(X.shape[1]):
        e = saved["edges"][j] if use_saved_scale else percentile_fit(X[:, j])
        X[:, j] = percentile_apply(X[:, j], e) - 0.5
    team_names: list[str] = []
    T = None
    if with_team:
        tsaved = joblib.load(OUT_DIR / f"{src}_team_scaler.joblib")
        team_names = list(tsaved["names"])
        T = z["Xteam"]
        if len(keep) != len(T):
            T = T[keep]
        for j in range(T.shape[1]):
            e = tsaved["edges"][j] if use_saved_scale else percentile_fit(T[:, j])
            T[:, j] = percentile_apply(T[:, j], e) - 0.5
    ai, li = abs_index(names), lvl_index(names)
    out: dict[str, np.ndarray] = {}
    for flip in flips:
        h = heroes[:, SWAP] if flip else heroes
        Dp, Rr = encs["pair"].transform(h), encs["role"].transform(h)
        wl = wm.decision_function(Dp).astype(np.float32)
        if flip:
            wl = -wl
        Xv = flip_signs(X, names) if flip else X
        Tv = (flip_signs(T, team_names) if flip else T) if T is not None else None
        for name, sym in SYM.items():
            pa = OUT_DIR / f"{src}_Atr_{name}.joblib"
            if not pa.exists():
                continue
            A = Rr if sym == "sym" else (sp.hstack([Dp, Rr], format="csr") if sym == "side" else Dp)
            a_s = joblib.load(pa).decision_function(A).astype(np.float32)
            blocks = [Xv[:, li]] if sym == "sym" else [Xv]
            if sym in ("sym", "side"):
                blocks.append(np.abs(Xv[:, ai]))
            blocks += [wl[:, None] / 4.0, np.abs(wl)[:, None] / 4.0]
            if Tv is not None:
                blocks.append(Tv)
            b_s = joblib.load(OUT_DIR / f"{src}_B_{name}.joblib").decision_function(
                np.hstack(blocks).astype(np.float32)).astype(np.float32)
            p = joblib.load(OUT_DIR / f"{src}_C_{name}.joblib").predict_proba(
                np.stack([a_s, b_s], axis=1))[:, 1].astype(np.float32)
            out[f"{name}_flip" if flip else name] = p
        del Dp, Rr, Xv
    print(f"  скоринг {src}: колонок {len(out)}", flush=True)
    return out


def load_transfer(src: str, z, keep: np.ndarray, names: list[str]) -> dict[str, np.ndarray]:
    """Перенос модели другого корпуса: обе стороны, шкала — по текущему корпусу."""
    return score_all(src, z, keep, names, flips=(False, True))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stage", default="public", choices=("public", "pro"))
    ap.add_argument("--smoke", type=int, default=0)
    ap.add_argument("--transfer-from", default="")
    args = ap.parse_args()
    run(args.stage, args.smoke, args.transfer_from or None)


if __name__ == "__main__":
    main()
