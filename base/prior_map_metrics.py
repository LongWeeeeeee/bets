"""Величины одной карты для причинных приоров.

Формула и порядок колонок — ровно `catalog_features.prior_values`. Живой путь
не имеет корпуса, поэтому считает ту же карточку из построчных килов и
поминутного ряда Stratz. Расхождение с офлайном сдвинуло бы приор после среза
снимка в другую сторону, чем ночная пересборка.

`radiantKills` / `direKills` у Stratz — ПРИРОСТЫ по минутам. Корпус кладёт их
накопленными (`pad(..., cum=True)`). Окно 0-10 — это `rk[10] - rk[0]` по
накопленному ряду длины 41 (минуты 0..40). Нетворс и опыт — точка на минуте,
без cumsum.
"""
from __future__ import annotations

from typing import Optional, Sequence

import numpy as np

from causal_priors import PRIOR_NAMES

MINUTES = 41
_M = len(PRIOR_NAMES)


def pad_series(arr: Optional[Sequence], n: int = MINUTES, *,
               cum: bool = False) -> np.ndarray:
    """Как `pro_corpus_rich.pad`: обрезка/дописка последним значением."""
    out = np.zeros(n, dtype=np.int32)
    if not arr:
        return out
    a = np.asarray([int(x or 0) for x in arr], dtype=np.int64)
    if cum:
        a = np.cumsum(a)
    k = min(len(a), n)
    out[:k] = a[:k]
    if k < n:
        out[k:] = a[k - 1] if k else 0
    return out


def map_metrics(*, rad_kills: Sequence[float], dire_kills: Sequence[float],
                duration_seconds: int, radiant_won: bool,
                rk_inc: Optional[Sequence] = None,
                dk_inc: Optional[Sequence] = None,
                nw: Optional[Sequence] = None,
                xp: Optional[Sequence] = None,
                ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(Vr, Vd, mask) длины `len(PRIOR_NAMES)` для радианта и дайра.

    `rad_kills` / `dire_kills` — килы пяти игроков стороны (итог карты, как
    `pstats[..., 0]`). Минутный ряд необязателен: без него оконные метрики
    маскируются и в приор не идут.
    """
    rkills = float(np.sum(np.asarray(list(rad_kills), dtype=np.float32)))
    dkills = float(np.sum(np.asarray(list(dire_kills), dtype=np.float32)))
    tot = rkills + dkills
    has_ps = tot > 0
    dur = np.float32(max(int(duration_seconds or 0), 0) / 60.0)
    rk = pad_series(rk_inc, MINUTES, cum=True).astype(np.float32)
    dk = pad_series(dk_inc, MINUTES, cum=True).astype(np.float32)
    nw_a = pad_series(nw, MINUTES, cum=False).astype(np.float32)
    xp_a = pad_series(xp, MINUTES, cum=False).astype(np.float32)
    has_ser = bool((rk.max() + dk.max()) > 0)
    wins = 1.0 if radiant_won else 0.0

    def w(a: int, b: int, arr: np.ndarray) -> float:
        return float(arr[min(b, 40)] - arr[min(a, 40)])

    cols_r: list[float] = []
    cols_d: list[float] = []
    masks: list[bool] = []

    def push(vr: float, vd: float, m: bool) -> None:
        cols_r.append(float(np.nan_to_num(vr)))
        cols_d.append(float(np.nan_to_num(vd)))
        masks.append(bool(m))

    push(rkills, dkills, has_ps)
    push(dkills, rkills, has_ps)
    push(rkills - dkills, dkills - rkills, has_ps)
    push(tot, tot, has_ps)
    push(float(rkills >= 27), float(dkills >= 27), has_ps)
    push(float(tot >= 54), float(tot >= 54), has_ps)
    push(float(dur), float(dur), True)
    for t in (32, 36, 40):
        push(float(dur >= t), float(dur >= t), True)
    for a, b in ((0, 10), (10, 20), (20, 30), (30, 40)):
        push(w(a, b, rk), w(a, b, dk), has_ser and (dur >= b))
    for a, b in ((0, 10), (10, 20), (20, 30), (30, 40)):
        push(w(a, b, dk), w(a, b, rk), has_ser and (dur >= b))
    for a, b in ((0, 10), (10, 20), (20, 30)):
        d_ = w(a, b, rk) - w(a, b, dk)
        ok = has_ser and (dur >= b) and (d_ != 0)
        push(float(d_ > 0), float(d_ < 0), ok)
    for mnt in (10, 20, 30):
        push(float(nw_a[mnt]), float(-nw_a[mnt]), has_ser and (dur >= mnt))
    for mnt in (10, 20):
        push(float(xp_a[mnt]), float(-xp_a[mnt]), has_ser and (dur >= mnt))
    push(wins, 1.0 - wins, True)
    vr = np.asarray(cols_r, dtype=np.float32)
    vd = np.asarray(cols_d, dtype=np.float32)
    mask = np.asarray(masks, dtype=bool)
    if vr.shape != (_M,):
        raise ValueError(f"метрик {vr.shape[0]}, в PRIOR_NAMES {_M}")
    return vr, vd, mask
