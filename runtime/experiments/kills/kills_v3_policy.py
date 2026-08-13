#!/usr/bin/env python3
"""Прод-политика и словарь против моделей v3 на ВСЕЙ про-выборке патча.

Зачем отдельно от AUC. Прод не ранжирует все карты: он берёт окно с максимальным
`|ed|` среди прошедших свои гейты и ставит ОДНУ ставку на карту. По E-28/E-144
прирост AUC на всех картах систематически НЕ переносится на этот отобранный поток.
E-165 упёрлась ровно сюда: прод-политику удалось прогнать только на 1096 картах
дампа (McNemar p=0.19), потому что поминутного нетворса не было больше нигде.

Он есть в самом корпусе: `radiantNetworthLeads` — поминутный массив той же длины,
что и `radiantKills`. `kills_v3_extract.py` кладёт его значения на минутах гейтов,
и прод-политика считается на всех 7.41+ картах, а не на 1096.

Чего по-прежнему нет: `lane_kills` (словарь линий локально отсутствует). Он нужен
ТОЛЬКО окну 5-15, поэтому политика считается в двух режимах — без окна 5-15
(строгий) и с ним при допущении, что линейный гейт пройден (верхняя оценка).

Запуск:
    venv_catboost/bin/python3 runtime/experiments/kills/kills_v3_policy.py
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))
sys.path.insert(0, str(ROOT / "runtime/experiments/kills"))

from kills_policy_fixed import POLICY, DEADLINE  # noqa: E402
from kills_v3_extract import NW_IDX  # noqa: E402
from kills_v3_train import TARGET_KILLS, score_all, split_pro  # noqa: E402
from window_model_v2 import auc_of, percentile_apply, percentile_fit  # noqa: E402
from window_v2_vs_dict_bigsample import StatsLookup, ed_for  # noqa: E402

ART = ROOT / "runtime/artifacts/kills"
V2 = ART / "window_model_v2"
DB = ROOT / "bets_data/analise_pub_matches/kills_window_dict_raw.sqlite3"
OUT = ART / "kills_v3_policy.md"
WINDOWS = ((5, 15), (10, 20), (15, 25), (20, 30))
PATCH_741 = 74100
ODDS = 1.8
lines: list[str] = []


def emit(s: str = "") -> None:
    print(s, flush=True)
    lines.append(s)


def wilson(k: int, n: int) -> tuple[float, float]:
    if not n:
        return 0.0, 0.0
    p, zc = k / n, 1.96
    d = 1 + zc * zc / n
    c = (p + zc * zc / (2 * n)) / d
    h = zc * math.sqrt(p * (1 - p) / n + zc * zc / (4 * n * n)) / d
    return max(0.0, c - h), min(1.0, c + h)


def summary(picks: dict[int, bool]) -> str:
    n = len(picks)
    if not n:
        return "0 | — | — | —"
    w = sum(1 for ok in picks.values() if ok)
    wr = w / n
    lo, hi = wilson(w, n)
    profit = w * ODDS - n                      # ставка 1u на карту при кэфе ODDS
    return f"{n:,} | {wr:.1%} | {wr * ODDS - 1:+.1%} | {lo:.1%}–{hi:.1%} | {profit:+.1f}u"


def mcnemar(a: dict[int, bool], b: dict[int, bool]) -> tuple[int, int, float]:
    """Точный двусторонний биномиальный тест на дискордантных парах."""
    win = lose = 0
    for k, ok_a in a.items():
        ok_b = b.get(k)
        if ok_b is None or ok_a == ok_b:
            continue
        win += bool(ok_b and not ok_a)
        lose += bool(ok_a and not ok_b)
    nd = win + lose
    if not nd:
        return 0, 0, 1.0
    k = min(win, lose)
    tail = sum(math.comb(nd, i) for i in range(0, k + 1)) / (2.0 ** nd)
    return win, lose, min(1.0, 2 * tail)


def v2_scores(mids_wanted: set[int]) -> dict[int, dict[str, float]]:
    """Оценки модели E-165 (v2) на тех же картах — контроль «а стало ли лучше»."""
    import joblib
    import scipy.sparse as sp
    path = V2 / "features_pro.npz"
    if not path.exists() or not (V2 / "C_draft_plus_10_20.joblib").exists():
        return {}
    z = np.load(path, allow_pickle=True)
    X, heroes, mids, valid = z["X"], z["heroes"], z["mid"], z["valid"]
    take = np.asarray([i for i, m in enumerate(mids) if int(m) in mids_wanted])
    if not len(take):
        return {}
    enc = joblib.load(V2 / "pair_encoder.joblib")
    Xn = np.stack([percentile_apply(X[:, j], percentile_fit(X[:, j]))
                   for j in range(X.shape[1])], axis=1) - 0.5
    out: dict[int, dict[str, float]] = {}
    for wi, (a, b) in enumerate(WINDOWS):
        tag = f"{a}_{b}"
        p_model = V2 / f"C_draft_plus_{tag}.joblib"
        if not p_model.exists():
            continue
        rows = take[valid[take, wi].astype(bool)]
        D = enc.transform(heroes[rows])
        F = sp.csr_matrix(Xn[rows])
        lg = joblib.load(V2 / f"win_model_{tag}.joblib").decision_function(D).astype(np.float32)
        WL = sp.csr_matrix(np.stack([lg / 4.0, np.abs(lg) / 4.0], axis=1))
        p = joblib.load(p_model).predict_proba(sp.hstack([D, F, WL], format="csr"))[:, 1]
        for m, pv in zip(mids[rows], p):
            out.setdefault(int(m), {})[tag] = float(pv)
    return out


def pick(i: int, ed: dict, scores: dict, nw, valid, mode: str, weight: float,
         allow_515: bool):
    """(окно, сторона) по выбранному режиму либо None. Гейты — прод-овские."""
    best = None
    for cfg in POLICY:
        w = cfg["window"]
        wi = [f"{a}_{b}" for a, b in WINDOWS].index(w)
        if w == "5_15" and not allow_515:
            continue
        if not valid[wi]:
            continue
        e = ed.get(w)
        if e is None or abs(e) < cfg["min_ed"]:
            continue
        ml = scores.get(w)
        if mode == "prod":
            strength, side = abs(e), (1 if e > 0 else -1)
        elif mode == "model":
            if ml is None:
                continue
            strength, side = abs(ml - 0.5), (1 if ml > 0.5 else -1)
        else:                                    # смесь рангов, сторона по смеси
            if ml is None:
                continue
            mix = (1 - weight) * np.tanh(e) / 2.0 + weight * (ml - 0.5)
            strength, side = abs(mix), (1 if mix > 0 else -1)
        lead = nw[wi] if side > 0 else -nw[wi]
        if lead < cfg["nw_min"]:
            continue
        if best is None or strength > best[0]:
            best = (strength, w, side)
    return None if best is None else (best[1], best[2])


def run_policy(rows, ed_rows, scores, z, keep, tag: str, allow_515: bool,
               veto_threshold: float = 0.5) -> None:
    valid, diffs, nw_all, nwok = z["valid"][keep], z["diffs"][keep], z["nwlead"][keep], z["nwok"][keep]
    wtags = [f"{a}_{b}" for a, b in WINDOWS]
    variants: dict[str, dict[int, bool]] = {k: {} for k in
                                            ("прод", "модель", "смесь 0.5", "вето")}
    for i in rows:
        if not nwok[i]:
            continue
        ed = ed_rows.get(int(i)) or {}
        sc = scores.get(int(i)) or {}
        nw = nw_all[i]
        for mode, key, w in (("prod", "прод", 0.0), ("model", "модель", 1.0),
                             ("mix", "смесь 0.5", 0.5)):
            got = pick(i, ed, sc, nw, valid[i], mode, w, allow_515)
            if got is None:
                continue
            wtag, side = got
            wi = wtags.index(wtag)
            variants[key][int(i)] = (side * int(diffs[i, wi])) > 0
        got = pick(i, ed, sc, nw, valid[i], "prod", 0.0, allow_515)
        if got is not None:
            wtag, side = got
            ml = sc.get(wtag)
            drop = ml is not None and ((ml - 0.5) * side) < -(veto_threshold - 0.5) - 1e-9
            if not drop:
                variants["вето"][int(i)] = variants["прод"][int(i)]

    emit(f"### {tag}")
    emit()
    emit("| правило | ставок | WR | ROI@1.8 | CI95 | профит |")
    emit("|---|---:|---:|---:|---|---:|")
    for k, v in variants.items():
        emit(f"| {k} | {summary(v)} |")
    emit()
    prod, model = variants["прод"], variants["модель"]
    common = set(prod) & set(model)
    if common:
        pw = sum(prod[i] for i in common)
        mw = sum(model[i] for i in common)
        win, lose, p = mcnemar(prod, model)
        emit(f"На пересечении {len(common):,} карт: прод {pw / len(common):.1%}, "
             f"модель {mw / len(common):.1%}. Дискордантных пар {win + lose} "
             f"(модель {win} : прод {lose}), точный двусторонний **p = {p:.3g}**.")
        only_m = {i: v for i, v in model.items() if i not in prod}
        only_p = {i: v for i, v in prod.items() if i not in model}
        if only_m:
            emit(f"Ставит только модель: {summary(only_m)}")
        if only_p:
            emit(f"Ставит только прод: {summary(only_p)}")
    emit()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--veto", type=float, default=0.55)
    args = ap.parse_args()
    if not DB.exists():
        raise SystemExit(f"нет словаря {DB}")

    z = np.load(V2 / "featuresv3_pro.npz", allow_pickle=True)
    names = [str(s) for s in z["names"]]
    n = len(z["ts"])
    keep = np.arange(n)
    patch, mids, valid, diffs = z["patch"], z["mid"], z["valid"], z["diffs"]
    sel741 = np.flatnonzero(patch >= PATCH_741)
    parts, cut = split_pro(patch, n)
    test_rows = parts["test"]
    emit("# Килы v3: словарь и прод-политика на всей про-выборке патча")
    emit()
    emit(f"Про-карт всего {n:,}; патч 7.41+ — {len(sel741):,}; тестовый срез про-моделей "
         f"(патчи >= {cut}) — {len(test_rows):,}.")
    emit()

    # ---------- оценки моделей ----------
    emit("## Оценки моделей")
    emit()
    pub = score_all("v3pub", z, keep, names)
    pro = {}
    if (V2 / "v3pro_C_map.joblib").exists():
        pro = score_all("v3pro", z, keep, names, with_team=True, use_saved_scale=True)
    wtags = [f"{a}_{b}" for a, b in WINDOWS]
    pub_scores = {int(m): {t: float(pub[f"w_{t}"][i]) for t in wtags if f"w_{t}" in pub}
                  for i, m in enumerate(mids)}
    pro_scores = ({int(m): {t: float(pro[f"w_{t}"][i]) for t in wtags if f"w_{t}" in pro}
                   for i, m in enumerate(mids)} if pro else {})

    # ---------- контроль читателя словаря и пересчёт ed ----------
    lookup = StatsLookup(DB)
    dump_path = ART / "kills_combo_features_v2_newthr.jsonl"
    checked = matched = 0
    worst = 0.0
    if dump_path.exists():
        idx_by_mid = {int(m): i for i, m in enumerate(mids)}
        with dump_path.open(encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                r = json.loads(line)
                i = idx_by_mid.get(int(r.get("mid", -1)))
                if i is None:
                    continue
                got = ed_for(z["heroes"][i], lookup)
                for t, val in got.items():
                    ref = r.get(f"ed_{t}")
                    if ref is None or val is None:
                        continue
                    checked += 1
                    worst = max(worst, abs(float(ref) - val))
                    matched += abs(float(ref) - val) < 1e-6
                if checked > 3000:
                    break
    emit(f"**Контроль читателя словаря:** сверено {checked:,} значений `ed` с дампом, "
         f"совпало {matched:,}, макс. расхождение {worst:.2e}.")
    if checked and matched / checked < 0.99:
        emit("Сверка НЕ сошлась — дальше считать нечего.")
        OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return 1
    emit()

    t0 = time.time()
    ed_rows: dict[int, dict] = {}
    for c, i in enumerate(sel741, 1):
        ed_rows[int(i)] = ed_for(z["heroes"][i], lookup)
        if c % 10000 == 0:
            print(f"  ed: {c:,}/{len(sel741):,}", flush=True)
    emit(f"Пересчитано `ed` для {len(ed_rows):,} карт 7.41+ "
         f"({len(ed_rows) / max(time.time() - t0, 1e-9):.0f} карт/с).")
    emit()

    # ---------- голова к голове по AUC ----------
    v2s = v2_scores({int(mids[i]) for i in sel741})
    emit("## Ранжирование (AUC) на 7.41+: словарь, v2 (E-165) и v3")
    emit()
    elo = z["elo"]
    emit("| окно | карт | словарь `ed` | ELO один | v2 C | v3 паблик C | v3 про C | "
         "лучшая смесь ed+v3 | вес |")
    emit("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for wi, t in enumerate(wtags):
        y, e, m2, m3, m3p, el = [], [], [], [], [], []
        for i in sel741:
            if not valid[i, wi]:
                continue
            ev = (ed_rows.get(int(i)) or {}).get(t)
            pv = pub_scores.get(int(mids[i]), {}).get(t)
            if ev is None or pv is None:
                continue
            y.append(1 if diffs[i, wi] > 0 else 0)
            e.append(ev)
            m3.append(pv)
            m2.append(v2s.get(int(mids[i]), {}).get(t, np.nan))
            m3p.append(pro_scores.get(int(mids[i]), {}).get(t, np.nan))
            el.append(float(elo[i, 0] - elo[i, 1]))
        if len(y) < 500:
            emit(f"| {t} | {len(y)} | — | — | — | — | — | — | — |")
            continue
        y = np.asarray(y)
        e, m2, m3, m3p, el = (np.asarray(v, float) for v in (e, m2, m3, m3p, el))
        rk = lambda v: np.argsort(np.argsort(v)).astype(float) / len(v)  # noqa: E731
        grid = [(w, auc_of(y, (1 - w) * rk(e) + w * rk(m3))) for w in np.arange(0, 1.01, 0.1)]
        bw, ba = max(grid, key=lambda x: x[1])
        emit(f"| {t} | {len(y):,} | {auc_of(y, e):.4f} | {auc_of(y, el):.4f} | "
             f"{auc_of(y, m2):.4f} | {auc_of(y, m3):.4f} | {auc_of(y, m3p):.4f} | "
             f"{ba:.4f} | {bw:.1f} |")
    emit()
    emit("v3 про-модель на 7.41+ частично видела эти карты в обучении (её тест — "
         f"патчи >= {cut}), поэтому её колонка здесь СПРАВОЧНАЯ; честный замер — "
         "в таблице по тестовому срезу ниже.")
    emit()

    # ---------- прод-политика ----------
    emit("## Прод-политика: одна ставка на карту, гейты как в бою")
    emit()
    emit("Гейты: `|ed|` по окну, NW-лид таргета на минуте `начало окна - 2` "
         f"(индексы массива {NW_IDX} = минуты {[DEADLINE[w] for w in wtags]}).")
    emit()
    run_policy(sel741, ed_rows, pub_scores, z, keep,
               "7.41+, модель обучена на паблике (полностью вне обучения), без окна 5-15",
               allow_515=False, veto_threshold=args.veto)
    run_policy(sel741, ed_rows, pub_scores, z, keep,
               "то же, с окном 5-15 при допущении, что линейный гейт пройден",
               allow_515=True, veto_threshold=args.veto)
    if pro_scores:
        run_policy(test_rows, ed_rows, pro_scores, z, keep,
                   f"тестовый срез про-модели (патчи >= {cut}), про-модель, без окна 5-15",
                   allow_515=False, veto_threshold=args.veto)

    # ---------- цель 27+ ----------
    emit("## Цель «сторона наберёт 27+ килов»")
    emit()
    ks = z["kills_side"]
    for label, rows, sc in (("7.41+, паблик-модель", sel741, pub),
                            (f"тест про-модели (>= {cut})", test_rows, pro)):
        if not sc or "ge27" not in sc:
            continue
        p = np.concatenate([sc["ge27"][rows], sc["ge27_flip"][rows]]) if "ge27_flip" in sc \
            else sc["ge27"][rows]
        y = np.concatenate([(ks[rows, 0] >= TARGET_KILLS).astype(int),
                            (ks[rows, 1] >= TARGET_KILLS).astype(int)]) \
            if "ge27_flip" in sc else (ks[rows, 0] >= TARGET_KILLS).astype(int)
        emit(f"**{label}:** наблюдений {len(y):,}, база {y.mean():.3f}, "
             f"AUC {auc_of(y, p):.4f}.")
        emit()
        emit("| порог p | ставок | доля 27+ | ROI@1.8 |")
        emit("|---:|---:|---:|---:|")
        for thr in (0.50, 0.55, 0.60, 0.65, 0.70):
            take = p >= thr
            if take.sum() < 50:
                continue
            wr = float(y[take].mean())
            emit(f"| {thr:.2f} | {int(take.sum()):,} | {wr:.3f} | {wr * ODDS - 1:+.1%} |")
        emit()
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("OUT:", OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
