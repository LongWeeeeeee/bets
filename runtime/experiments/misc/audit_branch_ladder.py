#!/usr/bin/env python3
"""Сквозная проверка лестницы на БОЕВОМ пути: сколько карт получают вердикт и какого качества.

ЗАЧЕМ. Числа веток (0.7186 / 0.6864 / …) сняты на ОБУЧАЮЩЕМ представлении
колонок. Это законно для сравнения веток между собой, но не отвечает на главный
вопрос: что происходит, когда те же ветки считает код самого скорера по снимку,
собранному строго из прошлого. Корреляция обучения и боя у трети колонок 0.4-0.7,
и переносить туда обучающие числа нельзя.

Здесь берётся тот же аудит-артефакт, что и в `audit_live_path` (снимок обрезан
по `TEST_FROM`), в него подмешиваются веса веток, и все тестовые карты
прогоняются через `PrematchScorer.score`. Меряется:

  * сколько карт получило вердикт против прежних 42.5%;
  * какой веткой и с каким AUC — отдельно по каждой;
  * совпадает ли вердикт полной ветки с прежним (на картах, где обе считают).

ВЫЗОВ ПОВТОРЯЕТ БОЕВОЙ: `strictness="accounts"`, id команд не передаются — так
зовёт `win_model_veto._prematch_index`. Поэтому ключ `org` недоступен по
построению, и достижимы ветки `no_org` и `no_account_no_org`. Это не упрощение
проверки, а ровно то, что происходит в бою.

Гейт свежести снят намеренно: его цена меряется отдельной таблицей в
`audit_live_path`, а здесь проверяется покрытие и качество веток.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/audit_branch_ladder.py
Выход:  runtime/artifacts/misc/audit_branch_ladder.md
"""
from __future__ import annotations

import os
import sys
import time
from collections import Counter
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))
sys.path.insert(0, str(ROOT / "base"))

import audit_live_path as A  # noqa: E402
import prematch_scorer as ps  # noqa: E402
from ideas_batch2 import COMPACT, RICH, TEST_FROM  # noqa: E402
from pro_features_wide import auc  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
HYB = ART / "map_winner_hybrid_quality_forward/hybrid_features.npz"
BRANCHES = ART / "branch_weights.npz"
AUDIT_BR = ART / f"prematch_audit_branches_cut{TEST_FROM}.npz"
OUT = ART / "audit_branch_ladder.md"


def assemble_audit_with_branches() -> None:
    """Аудит-артефакт плюс веса веток. Снимок остаётся строго из прошлого."""
    if not A.AUDIT.exists():
        A.assemble()
    z = dict(np.load(A.AUDIT, allow_pickle=True))
    zb = np.load(BRANCHES, allow_pickle=True)
    names = [str(x) for x in z["feature_names"]]
    bn = [str(x) for x in zb["feature_names"]]
    if bn != names:
        raise SystemExit("порядок признаков в весах веток разошёлся с аудит-артефактом")
    for k in zb.files:
        if k.startswith("branch_"):
            z[k] = zb[k]
    np.savez_compressed(AUDIT_BR, **z)
    print(f"аудит-артефакт с ветками собран: {AUDIT_BR.name}", flush=True)


def main() -> None:
    t0 = time.time()
    if not BRANCHES.exists():
        raise SystemExit(f"нет {BRANCHES} — сначала train_branch_weights.py")
    if not AUDIT_BR.exists():
        assemble_audit_with_branches()

    zc, zr = np.load(COMPACT), np.load(RICH)
    mids = np.load(HYB, allow_pickle=True)["mids"].astype(np.int64)
    cpos = {int(x): i for i, x in enumerate(zc["mids"].tolist())}
    rpos = {int(x): i for i, x in enumerate(zr["mids"].tolist())}
    ci = np.array([cpos[int(m)] for m in mids.tolist()])
    ri = np.array([rpos[int(m)] for m in mids.tolist()])
    ts, wins = zc["ts"][ci], zc["wins"][ci].astype(int)
    heroes, accounts = zc["heroes"][ci], zc["accounts"][ci]
    test = ts >= TEST_FROM
    lgt = np.load(ART / "pro_draft_logit_full.npz")["logit"]

    m_new = ps.PrematchModel(AUDIT_BR)
    m_old = ps.PrematchModel(A.AUDIT)
    if not m_new.branches:
        raise SystemExit("в аудит-артефакте нет веток — подмешивание не сработало")

    # `hybrid_strength` для тестовых карт берётся так же, как в audit_live_path
    _, hstr = A.train_columns(ts, accounts, zr["pstats"][ri],
                              zr["durations"][ri].astype(float) / 60.0, mids,
                              test, m_new.ctx_mu, m_new.ctx_sd, list(m_new.features))

    idx = np.flatnonzero(test)
    by_branch: dict = {}
    refused_new: Counter = Counter()
    refused_old = 0
    same, both = 0, 0
    for k, i in enumerate(idx):
        h = [int(x) for x in heroes[i]]
        a = [int(x) for x in accounts[i]]
        kw = dict(radiant_accounts=a[:5], dire_accounts=a[5:],
                  radiant_heroes=h[:5], dire_heroes=h[5:],
                  radiant_team_id=0, dire_team_id=0,
                  draft_logit=float(lgt[i]), hybrid_strength=float(hstr[k]),
                  strictness="accounts", now_ts=int(ts[i]), max_age_days=0.0)
        try:
            p_old = m_old.score(**kw).probability
        except ps.MissingData:
            p_old = None
            refused_old += 1
        try:
            r = m_new.score(**kw)
        except ps.MissingData as e:
            refused_new[str(e).split(":")[0][:45]] += 1
            continue
        by_branch.setdefault(r.branch, {"y": [], "p": []})
        by_branch[r.branch]["y"].append(int(wins[i]))
        by_branch[r.branch]["p"].append(r.probability)
        if p_old is not None:
            both += 1
            same += abs(p_old - r.probability) < 1e-9
        if (k + 1) % 5000 == 0:
            print(f"  {k+1:,}/{len(idx):,}", flush=True)

    n_test = int(test.sum())
    done = sum(len(v["y"]) for v in by_branch.values())
    lines = ["# Лестница веток на боевом пути", "",
             f"Снимок обрезан по `TEST_FROM = {TEST_FROM}`; тестовые карты прогнаны "
             f"кодом скорера. Вызов повторяет боевой (`strictness=\"accounts\"`, id "
             f"команд не передаются), поэтому ключ `org` недоступен и достижимы "
             f"ветки `no_org` и `no_account_no_org`.", "",
             f"**Вердикт получили {done:,} карт из {n_test:,} ({done / n_test:.1%})** "
             f"против {n_test - refused_old:,} ({(n_test - refused_old) / n_test:.1%}) "
             f"у прежней модели на том же снимке.", "",
             "| ветка | карт | доля | AUC |", "|---|---:|---:|---:|"]
    for name in sorted(by_branch, key=lambda x: -len(by_branch[x]["y"])):
        v = by_branch[name]
        y, p = np.array(v["y"]), np.array(v["p"])
        a = auc(y, p) if 0 < y.mean() < 1 else float("nan")
        lines.append(f"| `{name}` | {len(y):,} | {len(y) / n_test:.1%} | {a:.4f} |")
    ally = np.concatenate([v["y"] for v in by_branch.values()])
    allp = np.concatenate([v["p"] for v in by_branch.values()])
    lines.append(f"| **всё вместе** | {len(ally):,} | {len(ally) / n_test:.1%} | "
                 f"{auc(ally, allp):.4f} |")
    lines += ["", f"На {both:,} картах, которые считают обе модели, вердикт совпал "
                  f"побитово у {same:,} ({same / max(both, 1):.1%}) — полная ветка "
                  f"обязана быть тождественна прежней модели.", ""]
    if refused_new:
        lines += ["Оставшиеся отказы:", ""]
        for reason, cnt in refused_new.most_common():
            lines.append(f"- {reason}: {cnt:,}")
    else:
        lines.append("Отказов не осталось ни одного.")
    lines.append(f"\nПрогон занял {time.time() - t0:.0f} c.")
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines), flush=True)


if __name__ == "__main__":
    main()
