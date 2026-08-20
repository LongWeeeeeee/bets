#!/usr/bin/env python3
"""Сверка живых блоков `pairs` (F8, 8 колонок) и `dict` (kwdict_, 12) с обучением.

ЗАЧЕМ. Из восьми блоков панели эти два оставались единственными без числовой
сверки живого пути с обучающими колонками (аудит 19.08.2026). У остальных она
есть: `rating` — max|Δ| = 0 на 482 486 картах (`build_rating_snapshot.md`),
`public` — 3e-08 (`verify_public_block.md`), `hybrid` — тренд корреляции по
возрасту карты (`verify_hybrid_block.md`), `priors` — цена снимка против
причинного значения (`snapshot_vs_causal.md`), `core/sym` — общий модуль на оба
пути, `core/prod35` — сквозной аудит боевого скорера.

ЧЕМ МЕРИТЬ. Оба блока читают СНИМОК, а обучающие колонки считались as-of на дату
каждой карты. Поэтому точного равенства может не быть, и правильный признак
верной величины — не max|Δ|, а ЗАВИСИМОСТЬ согласия от возраста карты: чем ближе
карта к дате снимка, тем выше согласие. Ровная корреляция означала бы, что
считается вообще другая величина. Обе меры печатаются: если совпадение точное,
это видно сразу и вопрос закрыт сильнее.

ЧТО СРАВНИВАЕТСЯ. Обучающие значения берутся ПО `mids` из кэшей, а не по позиции:
`catalog_features.npz` (там живут восемь F8_pair_*) и `kills_dict_features.npz`.
Живые значения считаются теми же функциями, что зовёт `prematch_panel_live`.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))

ART = ROOT / "runtime/artifacts/misc"
OUT = ART / "verify_pairs_dict_blocks.md"
SAMPLE = int(os.getenv("VERIFY_SAMPLE", "400"))
BANDS = ((0, 7), (7, 30), (30, 90), (90, 365), (365, 10**6))


def _corr(a: np.ndarray, b: np.ndarray) -> float:
    ok = np.isfinite(a) & np.isfinite(b)
    if ok.sum() < 3:
        return float("nan")
    x, y = a[ok], b[ok]
    if x.std() == 0 or y.std() == 0:
        return float("nan")
    return float(np.corrcoef(x, y)[0, 1])


def main() -> None:
    t0 = time.time()
    zc = np.load(ART / "pro_corpus_compact.npz", allow_pickle=True)
    cpos = {int(m): i for i, m in enumerate(zc["mids"].tolist())}
    heroes_all, ts_all = zc["heroes"], zc["ts"]

    cat = np.load(ART / "catalog_features.npz", allow_pickle=True)
    cat_names = [str(x) for x in cat["names"]]
    kd = np.load(ART / "kills_dict_features.npz", allow_pickle=True)
    kd_names = [str(x) for x in kd["names"]]

    import pair_priors
    import prematch_panel_live as live
    from causal_priors import PRIOR_NAMES, load_snapshot

    snap = load_snapshot()
    if snap is None:
        raise SystemExit("снимок причинных приоров не поднялся")
    jq = [PRIOR_NAMES.index(m) for m in pair_priors.SYN_METRICS]

    pair_cols = list(pair_priors.COLUMNS)
    pj = [cat_names.index(c) for c in pair_cols]
    dict_cols = [f"kwdict_{w}_{f}" for w in live.WINDOWS for f in live.DICT_FIELDS]
    dj = [kd_names.index(c) for c in dict_cols]

    # Дата снимка приоров — от неё считается возраст карты.
    snap_ts = int(getattr(snap, "built_ts", 0) or max(ts_all))
    # ВЫБОРКА ПОСЛОЙНАЯ, а не случайная. Замороженный набор тянется с 2016
    # года, и свежих карт в нём доли процента: случайные 400 из 482 486 дали 347
    # карт старше года и НИ ОДНОЙ моложе месяца — то есть не покрыли именно тот
    # режим, в котором блок работает вживую. Берём поровну из каждой полосы.
    rng = np.random.default_rng(20260819)
    mids = cat["mids"].astype(np.int64)
    per_band = max(1, SAMPLE // len(BANDS))
    ages_all = np.full(len(mids), np.nan)
    ci_all = np.full(len(mids), -1, dtype=np.int64)
    for k, m in enumerate(mids.tolist()):
        ci = cpos.get(int(m))
        if ci is None:
            continue
        ci_all[k] = ci
        ages_all[k] = (snap_ts - int(ts_all[ci])) / 86400.0
    picked: list[int] = []
    for lo, hi in BANDS:
        sel = np.flatnonzero(np.isfinite(ages_all) & (ages_all >= lo) & (ages_all < hi))
        if not len(sel):
            continue
        take = rng.choice(sel, size=min(per_band, len(sel)), replace=False)
        picked.extend(int(x) for x in take)
        print(f"  полоса {lo}-{hi}: доступно {len(sel):,}, взято {len(take)}", flush=True)
    idx = np.array(picked)

    rows_p: list[tuple] = []
    rows_d: list[tuple] = []
    for k in idx:
        ci = int(ci_all[k])
        if ci < 0:
            continue
        h10 = heroes_all[ci].astype(np.int64)
        age = float(ages_all[k])

        hp = snap.hero_priors(h10)[:, jq]
        fb = pair_priors.block(h10, hp)
        if fb is not None:
            got = np.array([fb[c] for c in pair_cols], float)
            rows_p.append((age, got, np.asarray(cat["F"][k, pj], float)))

        db = live._dict_block(h10[None, :])
        if db is not None:
            got = np.array([db[c] for c in dict_cols], float)
            rows_d.append((age, got, np.asarray(kd["F"][k, dj], float)))


    lines = ["# Живые блоки `pairs` и `dict` против колонок обучения", ""]
    lines.append("Обучающие значения берутся ПО `mids`, а не по позиции. Оба блока "
                 "читают снимок, поэтому кроме max|Δ| печатается зависимость "
                 "согласия от возраста карты: у верной величины согласие растёт "
                 "к дате снимка, у неверной корреляция ровная.")
    lines.append("")

    for title, rows, cols in (("pairs (F8, 8 колонок)", rows_p, pair_cols),
                              ("dict (kwdict_, 12 колонок)", rows_d, dict_cols)):
        lines += [f"## {title}", ""]
        if not rows:
            lines += ["Блок не поднялся — сверять нечего.", ""]
            print(f"{title}: блок не поднялся", flush=True)
            continue
        age = np.array([r[0] for r in rows])
        got = np.vstack([r[1] for r in rows])
        want = np.vstack([r[2] for r in rows])
        both = np.isfinite(got) & np.isfinite(want)
        d = np.where(both, np.abs(got - want), 0.0)
        lines += [f"Карт сверено: {len(rows):,}. NaN совпадает по расположению: "
                  f"{bool(np.array_equal(np.isnan(got), np.isnan(want)))}.", "",
                  "| колонка | max\\|Δ\\| | корреляция |", "|---|---:|---:|"]
        for j, c in enumerate(cols):
            lines.append(f"| {c} | {d[:, j].max():.3e} | {_corr(got[:, j], want[:, j]):.4f} |")
        lines += ["", f"**max|Δ| по всем колонкам: {d.max():.3e}**", ""]
        print(f"{title}: карт {len(rows):,}, max|Δ| {d.max():.3e}", flush=True)

        lines += ["| возраст карты | карт | средняя корреляция по колонкам |",
                  "|---|---:|---:|"]
        for lo, hi in BANDS:
            sel = (age >= lo) & (age < hi)
            if sel.sum() < 10:
                continue
            cs = [_corr(got[sel, j], want[sel, j]) for j in range(got.shape[1])]
            cs = [c for c in cs if np.isfinite(c)]
            tag = f"{lo}-{hi} дн" if hi < 10**6 else f"{lo}+ дн"
            lines.append(f"| {tag} | {int(sel.sum())} | "
                         f"{(np.mean(cs) if cs else float('nan')):.4f} |")
        lines.append("")
        # Поколоночно на САМОЙ СВЕЖЕЙ полосе: живой путь работает именно там, и
        # средняя по колонкам может прятать одну расходящуюся.
        for lo, hi in BANDS:
            sel = (age >= lo) & (age < hi)
            if sel.sum() < 10:
                continue
            tag = f"{lo}-{hi} дн" if hi < 10**6 else f"{lo}+ дн"
            lines += [f"Поколоночно на свежей полосе ({tag}, {int(sel.sum())} карт) — "
                      "это и есть живой режим:", "",
                      "| колонка | max\\|Δ\\| | корреляция |", "|---|---:|---:|"]
            for j, c in enumerate(cols):
                lines.append(f"| {c} | {d[sel, j].max():.3e} | "
                             f"{_corr(got[sel, j], want[sel, j]):.4f} |")
            lines.append("")
            break

    lines.append(f"Прогон занял {time.time() - t0:.0f} c.")
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nотчёт: {OUT}", flush=True)


if __name__ == "__main__":
    main()
