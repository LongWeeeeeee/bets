#!/usr/bin/env python3
"""Винрейт ставок в разрезе того, КАКОЙ компонент был главным доводом.

ЗАЧЕМ. Вопрос alex: как играют ставки, когда первым признаком идёт драфтовая ML,
когда ELO и когда сила команды. По E-209 состав решения на живых картах — ELO
57%, драфт 20%, рейтинг команд 15%, игроки 8%, но это доли ВКЛАДА, а не винрейт.
Здесь считается исход: сколько карт, какой фактический винрейт и окупается ли он
по действующей пороговой сетке.

КАК СЧИТАЕТСЯ ГЛАВНЫЙ ДОВОД. Вклад признака = коэффициент × стандартизованное
значение, всё переориентировано НА СТОРОНУ СТАВКИ (иначе «главный» означал бы
«самый сильный аргумент за радианта», что к ставке отношения не имеет). Главным
считается компонент с наибольшей ПОЛОЖИТЕЛЬНОЙ суммой: доводы против ставки в
соревновании за первое место не участвуют.

Пять корзин, а не четыре: `hybrid_strength` вынесен из ELO отдельной «силой
команды». Это разные вычислители — `elo` и `opp_elo` идут из таблицы аккаунтов
артефакта, а `hybrid_strength` из живого пакета `ELO/` с ростером и тиром лиги, —
и alex спрашивает про них раздельно.

Считается на БОЕВОМ пути: снимок обрезан по `TEST_FROM`, карты прогнаны кодом
скорера. Порог — тот же, что у самостоятельной ставки: |индекс| >= 8.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/bets_by_leading_component.py
Выход:  runtime/artifacts/misc/bets_by_leading_component.md
"""
from __future__ import annotations

import os
import sys
import time
from collections import defaultdict
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))
sys.path.insert(0, str(ROOT / "base"))

import audit_live_path as A  # noqa: E402
import prematch_scorer as ps  # noqa: E402
from ideas_batch2 import COMPACT, RICH, TEST_FROM  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
HYB = ART / "map_winner_hybrid_quality_forward/hybrid_features.npz"
AUDIT_BR = ART / f"prematch_audit_branches_cut{TEST_FROM}.npz"
OUT = ART / "bets_by_leading_component.md"
MIN_INDEX = float(os.getenv("PREMATCH_MIN_INDEX", "8"))

#: Пять корзин. `hybrid_strength` вынесен из ELO: другой вычислитель и другой
#: вопрос. Интеракции идут к своему главному признаку.
BUCKET = {}
for _f in ("elo", "opp_elo", "elo_x_elo_gap", "elo_x_games_exp"):
    BUCKET[_f] = "ELO игроков"
BUCKET["hybrid_strength"] = "сила команды"
for _f in ("draft_logit", "wr30", "vs_wr", "cp_lane", "syn_pos_mean", "farm_dep",
           "draft_logit_x_elo_gap", "draft_logit_x_games_exp"):
    BUCKET[_f] = "драфт"
BUCKET["h2h_resid"] = "очные встречи"


def bucket_of(name: str) -> str:
    return BUCKET.get(name, "игроки")


def main() -> None:
    t0 = time.time()
    if not AUDIT_BR.exists():
        raise SystemExit(f"нет {AUDIT_BR} — сначала audit_branch_ladder.py")
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

    m = ps.PrematchModel(AUDIT_BR)
    _, hstr = A.train_columns(ts, accounts, zr["pstats"][ri],
                              zr["durations"][ri].astype(float) / 60.0, mids,
                              test, m.ctx_mu, m.ctx_sd, list(m.features))

    idx = np.flatnonzero(test)
    rows = []
    for k, i in enumerate(idx):
        h = [int(x) for x in heroes[i]]
        a = [int(x) for x in accounts[i]]
        try:
            r = m.score(radiant_accounts=a[:5], dire_accounts=a[5:],
                        radiant_heroes=h[:5], dire_heroes=h[5:],
                        radiant_team_id=0, dire_team_id=0,
                        draft_logit=float(lgt[i]), hybrid_strength=float(hstr[k]),
                        strictness="accounts", now_ts=int(ts[i]), max_age_days=0.0)
        except ps.MissingData:
            continue
        b = m.branches[r.branch]
        x = np.array([r.features[c] for c in b.cols])
        contrib = ((x - b.mu) / b.sd) * b.coef
        index = (r.probability - 0.5) * 100.0
        if index < 0:                      # приводим к стороне ставки
            contrib = -contrib
        by = defaultdict(float)
        for c, w in zip(b.cols, contrib):
            by[bucket_of(c)] += float(w)
        pos = {kk: v for kk, v in by.items() if v > 0}
        lead = max(pos, key=pos.get) if pos else "нет довода"
        share = pos[lead] / sum(pos.values()) if pos else 0.0
        hit = int(wins[i]) == 1 if index > 0 else int(wins[i]) == 0
        rows.append((lead, share, abs(index), int(hit), r.branch))
        if (k + 1) % 5000 == 0:
            print(f"  {k+1:,}/{len(idx):,}", flush=True)

    lines = ["# Винрейт в разрезе главного довода", "",
             f"Боевой путь: снимок обрезан по `TEST_FROM`, карты прогнаны кодом "
             f"скорера. Вклад признака переориентирован НА СТОРОНУ СТАВКИ, главным "
             f"считается компонент с наибольшей положительной суммой. Карт с "
             f"вердиктом: {len(rows):,}.", ""]

    for tag, sel in (("на пороге самостоятельной ставки (|индекс| ≥ 8)",
                      [r for r in rows if r[2] >= MIN_INDEX]),
                     ("все вердикты", rows)):
        lines += [f"## {tag}", "",
                  "| главный довод | карт | доля | винрейт | обещано сеткой | отдача |",
                  "|---|---:|---:|---:|---:|---:|"]
        n_all = len(sel) or 1
        agg = defaultdict(list)
        for lead, share, conf, hit, _br in sel:
            agg[lead].append((conf, hit))
        for lead in sorted(agg, key=lambda x: -len(agg[x])):
            v = agg[lead]
            wr = sum(h for _c, h in v) / len(v)
            promised = float(np.mean([ps.lan_expected_wr(min(0.5 + c / 100.0, 0.99))
                                      for c, _h in v]))
            odds = float(np.mean([ps.lan_min_odds(min(0.5 + c / 100.0, 0.99))
                                  for c, _h in v]))
            lines.append(f"| {lead} | {len(v):,} | {len(v)/n_all:.1%} | {wr:.3f} | "
                         f"{promised:.3f} | {wr*odds-1:+.1%} |")
        lines.append("")

    lines.append("Отдача — сколько вернётся на единицу ставки, если получить ровно "
                 "`min_odds` из сетки. Ноль означает «сетка честна для этой "
                 "группы», плюс — «перестраховываемся», минус — «обещаем больше, "
                 "чем даём».")
    lines.append(f"\nПрогон занял {time.time() - t0:.0f} c.")
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines), flush=True)


if __name__ == "__main__":
    main()
