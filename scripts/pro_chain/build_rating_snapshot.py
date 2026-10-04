#!/usr/bin/env python3
"""Снимок рейтингов для живой панели плюс сверка порта с обучением.

Проходит историю тем же порядком, что офлайн-сборщик (`undercount_glicko.py`),
и на каждой карте сравнивает шесть чисел с сохранённым эталоном
`undercount_glicko_features.npz`. Расхождение означает, что порт в
`base/team_ratings.py` разъехался с обучением, — тогда снимок не пишется вовсе:
блок с чужими числами хуже, чем непоставленный блок.

Запуск: nohup venv_catboost/bin/python3 scripts/pro_chain/build_rating_snapshot.py &
Выход:  data/rating_snapshot.npz, runtime/artifacts/misc/build_rating_snapshot.md
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(os.getenv("DRAFT_ROOT") or Path(__file__).resolve().parents[2])
sys.path.insert(0, str(ROOT / "base"))
from team_ratings import RatingState, SNAPSHOT, save_snapshot, advance_newer_than  # noqa: E402

ART = ROOT / "runtime/artifacts/misc"
COMPACT = ART / "pro_corpus_compact.npz"
RICH = ART / "pro_corpus_rich.npz"
REF = ART / "undercount_glicko_features.npz"
OUT_MD = ART / "build_rating_snapshot.md"
KEYS = ("glicko", "glicko_p", "glicko_rd", "pos_glicko", "trueskill",
        "trueskill_sig")
TOL = 1e-9


def main() -> None:
    t0 = time.time()
    # Порядок карт берётся ИЗ ЭТАЛОНА, а не восстанавливается фильтром: файл
    # рейтингов покрывает набор карт гибридного блока (482 486), и нынешний
    # фильтр COMPACT∩RICH даёт другое множество. Совпасть обязано построчно,
    # иначе сверка проверяла бы не то.
    ref = np.load(REF)
    mids = ref["mids"].astype(np.int64)
    ts = ref["ts"].astype(np.int64)
    assert (np.diff(ts) >= 0).all(), "эталон не по возрастанию времени"

    zc = np.load(COMPACT)
    cpos = {int(m): i for i, m in enumerate(zc["mids"].tolist())}
    miss = [int(m) for m in mids.tolist() if int(m) not in cpos]
    assert not miss, f"в COMPACT нет {len(miss)} карт эталона"
    ci = np.array([cpos[int(m)] for m in mids.tolist()])
    wins = zc["wins"][ci].astype(int)
    accounts = zc["accounts"][ci]
    n = len(ts)
    print(f"карт {n:,}", flush=True)

    R = np.column_stack([ref[k] for k in KEYS])

    st = RatingState()
    worst = np.zeros(6)
    worst_at = np.zeros(6, dtype=np.int64)
    for i in range(n):
        now = int(ts[i])
        got = st.advance(now, accounts[i], bool(wins[i]))
        d = np.abs(np.asarray(got) - R[i])
        upd = d > worst
        worst[upd] = d[upd]
        worst_at[upd] = i
        if (i + 1) % 100_000 == 0:
            print(f"  {i+1:,}/{n:,}  max|Δ| {worst.max():.2e} "
                  f"({time.time()-t0:.0f} c)", flush=True)

    lines = ["# Снимок рейтингов: сверка порта с обучением", "",
             f"Карт пройдено: {n:,}. Порог совпадения: {TOL:g}.", "",
             "| колонка | величина | max\\|Δ\\| | карта |", "|---|---|---|---|"]
    for j, k in enumerate(KEYS):
        lines.append(f"| rating_{j} | {k} | {worst[j]:.3e} | {worst_at[j]} |")
    ok = bool(worst.max() <= TOL)
    lines += ["", f"**Итог: {'порт совпал с обучением' if ok else 'РАСХОЖДЕНИЕ'}**",
              ""]
    if ok:
        extra_n, newest_ts = advance_newer_than(
            st,
            zc["ts"].astype(np.int64),
            zc["accounts"],
            zc["wins"].astype(int),
            int(ts[-1]),
        )
        save_snapshot(SNAPSHOT, st, newest_ts)
        lines.append(f"Снимок записан: `{SNAPSHOT}`, аккаунтов "
                     f"{len(st.glicko):,}, пар (аккаунт, позиция) "
                     f"{len(st.pos):,}, последняя карта "
                     f"{newest_ts}.")
        if extra_n:
            lines.append(f"После эталона допроведено compact-карт: {extra_n:,} "
                         f"(с {int(ts[-1])} по {newest_ts}).")
    else:
        lines.append("Снимок НЕ записан: блок с чужими числами хуже, "
                     "чем непоставленный блок.")
    lines.append(f"\nПрогон занял {time.time()-t0:.0f} c.")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines[-6:]), flush=True)


if __name__ == "__main__":
    main()
