#!/usr/bin/env python3
"""Эталон вердикта на полном входе — снимается ДО разнесения расчёта по компонентам.

ЗАЧЕМ. Расчёт 35 признаков в `prematch_scorer.score` разносится на три куска по
ключу данных, чтобы ветка без аккаунтов вообще стала достижимой: сегодня первая
же строка блока игроков (`A = np.array([self.acc[int(a)] for a in a5])`) падает
по KeyError, если аккаунт неизвестен.

Перенос строк — ровно тот класс правки, на котором проект уже терял качество
молча: E-166 стоил 0.116 AUC из-за потерянного деления на 100 у одной колонки.
Поэтому эталон снимается ДО рефакторинга и сравнивается побитово: probability и
все 35 признаков.

Карта берётся с конца корпуса — там снимок знает больше игроков, и вердикт
находится быстрее. Порог свежести снят (`max_age_days=1e9`): эталон про
арифметику признаков, а не про гейт протухания.

Запуск: venv_catboost/bin/python3 runtime/experiments/misc/make_score_golden.py
Выход:  base/tests/fixtures/prematch_score_golden.json
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT", "/Users/alex/Documents/ingame"))
sys.path.insert(0, str(ROOT / "base"))
sys.path.insert(0, str(ROOT / "runtime/experiments/misc"))

import prematch_scorer as ps  # noqa: E402
from ideas_batch2 import COMPACT  # noqa: E402

ART = ROOT / "runtime/artifacts/misc/prematch_model_artifact_v3_hybrid.npz"
HYB = ROOT / "runtime/artifacts/misc/map_winner_hybrid_quality_forward/hybrid_features.npz"
LGT = ROOT / "runtime/artifacts/misc/pro_draft_logit_full.npz"
OUT = ROOT / "base/tests/fixtures/prematch_score_golden.json"


def main() -> None:
    z = np.load(COMPACT)
    m = ps.PrematchModel(ART)
    lg = np.load(LGT)["logit"]
    hz = np.load(HYB, allow_pickle=True)
    hmids = {int(x): i for i, x in enumerate(hz["mids"].tolist())}
    tried = 0
    for i in range(len(z["mids"]) - 1, -1, -1):
        mid = int(z["mids"][i])
        if mid not in hmids:
            continue
        tried += 1
        if tried > 200000:
            break
        acc = [int(a) for a in z["accounts"][i].tolist()]
        her = [int(h) for h in z["heroes"][i].tolist()]
        team = [int(t) for t in z["teams"][i].tolist()]
        j = hmids[mid]
        try:
            r = m.score(radiant_accounts=acc[:5], dire_accounts=acc[5:],
                        radiant_heroes=her[:5], dire_heroes=her[5:],
                        radiant_team_id=team[0], dire_team_id=team[1],
                        draft_logit=float(lg[j]),
                        hybrid_strength=float(hz["F"][j, 1]),
                        strictness="teams", now_ts=int(z["ts"][i]),
                        max_age_days=1e9)
        except ps.MissingData:
            continue
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps({
            "mid": mid,
            "radiant_accounts": acc[:5], "dire_accounts": acc[5:],
            "radiant_heroes": her[:5], "dire_heroes": her[5:],
            "radiant_team_id": team[0], "dire_team_id": team[1],
            "draft_logit": float(lg[j]),
            "hybrid_strength": float(hz["F"][j, 1]),
            "now_ts": int(z["ts"][i]),
            "probability": r.probability,
            "features": {k: float(v) for k, v in r.features.items()},
        }, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"эталон снят на карте {mid} (просмотрено {tried}): "
              f"p = {r.probability:.9f}, признаков {len(r.features)}", flush=True)
        return
    raise SystemExit("не нашлось карты, на которой артефакт отдаёт вердикт")


if __name__ == "__main__":
    main()
