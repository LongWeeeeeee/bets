#!/usr/bin/env python3
"""Откуда берётся сигнал: абляция блоков признаков и перебор параметров бустинга.

Смысл не в «сделать чуть лучше», а в том, чтобы знать, ЧТО именно работает, —
иначе следующая итерация признаков делается вслепую. Каждый блок выключается
целиком, и меряется падение AUC на тесте; отдельно каждый блок меряется в
одиночку. Одна и та же выборка, один и тот же сплит, один и тот же движок.

Блоки:
  draft     оценка линейной модели по парам героев (число)
  form      скользящие средние игрока (pl_*)
  hero      базы героя и героя+позиции (hero_*, heropos_*)
  tempo     оконный темп (все *_wdiff_*, *_wtot_*)
  level     уровневые колонки (*_lvl) — кровавость карты
  team      командный блок (только про)
  extra     пакет v4 (отдых, разброс, тренд, контекст)
  dict      значения прод-словаря `ed` (только про)

Запуск:
    python3 kills_v4_ablation.py --corpus public --target w_10_20 --rows 1500000
    python3 kills_v4_ablation.py --corpus pro --target w_10_20 --backend hgb --sweep
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
from kills_v3_train import SYM, make_targets, split_pro, split_public  # noqa: E402
from kills_v4_gbdt import PARAMS, draft_scores, fit_backend, load_matrix  # noqa: E402

OUT_DIR = ROOT / "runtime/artifacts/kills/window_model_v2"


def block_of(name: str, extra_names: set, dict_names: set, team_names: set) -> str:
    if name == "draft_score":
        return "draft"
    if name in dict_names:
        return "dict"
    if name in extra_names:
        return "extra"
    if name in team_names:
        return "team"
    if name.endswith("_lvl"):
        return "level"
    if "_wdiff_" in name or "_wtot_" in name:
        return "tempo"
    if name.startswith("pl_") or name.startswith("plhero_"):
        return "form"
    return "hero"


def run(corpus: str, target: str, rows_cap: int, backend: str, sweep: bool,
        rounds: int) -> None:
    X, names, z = load_matrix(corpus, True)
    n_all = len(X)
    keep = np.arange(max(0, n_all - rows_cap), n_all) if rows_cap else np.arange(n_all)
    X = X[keep] if len(keep) != n_all else X
    heroes, patch = z["heroes"][keep], z["patch"][keep]
    n = len(keep)
    tg = make_targets(z, keep)[target]
    parts, cut = split_public(n) if corpus == "public" else split_pro(patch, n)
    sel = {k: tg["mask"][v] for k, v in parts.items()}
    y = {k: tg["y"][v][sel[k]] for k, v in parts.items()}
    rows = {k: parts[k][sel[k]] for k in parts}
    print(f"[{corpus}/{target}] карт {n:,}, колонок {X.shape[1]}, "
          f"train {len(y['train']):,}, test {len(y['test']):,}", flush=True)

    ds = draft_scores(z, parts, sel, y, SYM[target], "stack", z["heroes"][keep])
    cols = names + ["draft_score"]
    extra_names, dict_names, team_names = set(), set(), set()
    for kind, holder in (("extrav4", extra_names), ("dictv4", dict_names)):
        p = OUT_DIR / f"{kind}_{corpus}.npz"
        if p.exists():
            holder.update(str(s) for s in np.load(p, allow_pickle=True)["names"])
    if z["Xteam"].size:
        team_names.update(str(s) for s in z["team_names"])
    groups: dict[str, list[int]] = {}
    for j, c in enumerate(cols):
        groups.setdefault(block_of(c, extra_names, dict_names, team_names), []).append(j)
    print("  блоки: " + ", ".join(f"{k} {len(v)}" for k, v in sorted(groups.items())),
          flush=True)

    def design(part: str, take: np.ndarray) -> np.ndarray:
        full = np.hstack([X[rows[part]], ds[part][:, None].astype(np.float32)])
        return full if len(take) == full.shape[1] else full[:, take]

    def score(take: np.ndarray, label: str) -> dict:
        t0 = time.time()
        pred, trees, va, _, _ = fit_backend(
            backend, design("train", take), y["train"], design("val", take), y["val"],
            [cols[j] for j in take], rounds)
        a = auc_of(y["test"], pred(design("test", take)))
        print(f"  {label:22s} AUC {a:.4f}  (val {va:.4f}, деревьев {trees}, "
              f"{time.time() - t0:.0f}с)", flush=True)
        return {"auc": float(a), "val_auc": va, "trees": trees, "cols": int(len(take))}

    report = {"corpus": corpus, "target": target, "rows": int(n), "backend": backend,
              "blocks": {k: len(v) for k, v in groups.items()}, "results": {}}
    allj = np.arange(len(cols))
    report["results"]["ВСЁ"] = score(allj, "ВСЁ")
    for name, idxs in sorted(groups.items()):
        rest = np.setdiff1d(allj, np.asarray(idxs))
        report["results"][f"без {name}"] = score(rest, f"без {name}")
        report["results"][f"только {name}"] = score(np.asarray(idxs), f"только {name}")

    if sweep:
        base = dict(PARAMS)
        grid = [("листья 63", {"num_leaves": 63}),
                ("листья 511", {"num_leaves": 511}),
                ("lr 0.02", {"learning_rate": 0.02}),
                ("min_leaf 1000", {"min_data_in_leaf": 1000}),
                ("ff 0.4", {"feature_fraction": 0.4}),
                ("l2 50", {"lambda_l2": 50.0})]
        for label, upd in grid:
            PARAMS.clear(); PARAMS.update(base); PARAMS.update(upd)
            report["results"][f"параметры: {label}"] = score(allj, f"параметры: {label}")
        PARAMS.clear(); PARAMS.update(base)

    out = OUT_DIR / f"ablation_{corpus}_{target}.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nотчёт: {out}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", default="public")
    ap.add_argument("--target", default="w_10_20")
    ap.add_argument("--rows", type=int, default=0, help="взять только свежие N карт")
    ap.add_argument("--backend", default="lgb", choices=("lgb", "hgb"))
    ap.add_argument("--sweep", action="store_true")
    ap.add_argument("--rounds", type=int, default=2000)
    args = ap.parse_args()
    run(args.corpus, args.target, args.rows, args.backend, args.sweep, args.rounds)


if __name__ == "__main__":
    main()
