#!/usr/bin/env python3
"""Живой провайдер блока `F8_pair_syn*` — парная синергия по килам.

ЧТО ЭТО. Для каждой из десяти пар героев внутри пятёрки берётся причинный приор
ПАРЫ и из него вычитается полусумма одиночных приоров тех же двух героев:

    syn(a, b) = prior(пара a+b) − ½·(prior(a) + prior(b))

То есть остаток пары над тем, что и так объясняется героями по отдельности.
Метрик две — они и дают `syn0`/`syn1` (`catalog_features.py:447`):

    syn0  own_kills   килы своей стороны
    syn1  k_10_20     килы в окне 10-20

Дальше по десяти парам берутся среднее и максимум, и стороны сводятся в
разность и сумму — ровно как F6/F7 в `causal_priors.aggregate_side`.

ПОЧЕМУ ОТДЕЛЬНЫЙ ФАЙЛ СНИМКА. Боевой `prior_snapshot.npz` уже лежит на проде и
читается работающей панелью; дописывать в него парные ключи значило бы менять
формат под живым читателем. Парные ключи живут своим файлом, а `shrink[2]`
(K_PAIR) в боевом снимке был объявлен заранее — именно под это.

КЛЮЧ ПАРЫ — `min(hero_a, hero_b) * 1000 + max(...)` по СЫРЫМ hero_id. Офлайн
использовал плотные индексы внутри своего прогона, но кодировка ключа — деталь
накопления: важно лишь, чтобы сборщик снимка и живой путь считали одинаково, а
они оба берут её отсюда.
"""
from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any, Sequence

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = Path(os.getenv("PANEL_PAIR_SNAPSHOT",
                          str(PROJECT_ROOT / "data" / "pair_prior_snapshot.npz")))

# Порядок обязан совпадать с `feature_names.json`: сначала все разности, затем
# все суммы, внутри — syn0_mean, syn0_max, syn1_mean, syn1_max.
COLUMNS: tuple[str, ...] = (
    "F8_pair_syn0_mean_diff", "F8_pair_syn0_max_diff",
    "F8_pair_syn1_mean_diff", "F8_pair_syn1_max_diff",
    "F8_pair_syn0_mean_sum", "F8_pair_syn0_max_sum",
    "F8_pair_syn1_mean_sum", "F8_pair_syn1_max_sum",
)
# Метрики парной синергии и их места в PRIOR_NAMES (проверяются при загрузке).
SYN_METRICS: tuple[str, ...] = ("own_kills", "k_10_20")
PAIRS: tuple[tuple[int, int], ...] = tuple(
    (i, j) for i in range(5) for j in range(i + 1, 5))

_lock = threading.Lock()
_state: dict[str, Any] = {"loaded": False, "snap": None, "error": None}


def pair_key(a: int, b: int) -> int:
    """Ключ неупорядоченной пары героев."""
    lo, hi = (a, b) if a <= b else (b, a)
    return int(lo) * 1000 + int(hi)


def _load() -> dict[str, Any]:
    with _lock:
        if _state["loaded"]:
            return _state
        _state["loaded"] = True
        try:
            z = np.load(SNAPSHOT, allow_pickle=False)
            snap = {"keys": z["keys"], "sums": z["sums"], "counts": z["counts"],
                    "globals": z["globals"], "k": float(z["shrink"]),
                    "metrics": [str(x) for x in z["metrics"]],
                    "built_ts": int(z["built_ts"])}
            if tuple(snap["metrics"]) != SYN_METRICS:
                raise ValueError(f"метрики снимка {snap['metrics']} "
                                 f"не совпали с {list(SYN_METRICS)}")
            if not (np.diff(snap["keys"]) > 0).all():
                raise ValueError("ключи снимка не отсортированы")
            _state["snap"] = snap
        except Exception as exc:                     # noqa: BLE001
            _state["error"] = f"{type(exc).__name__}: {exc}"
        return _state


def _pair_prior(snap: dict[str, Any], keys: np.ndarray) -> np.ndarray:
    """(len(keys), 2) приоров пары. Неизвестная пара = глобальное среднее."""
    tk = snap["keys"]
    if not len(tk):
        return np.repeat(snap["globals"][None, :], len(keys), 0)
    pos = np.clip(np.searchsorted(tk, keys), 0, len(tk) - 1)
    known = tk[pos] == keys
    s = np.where(known[:, None], snap["sums"][pos], 0.0)
    c = np.where(known[:, None], snap["counts"][pos], 0.0)
    k = snap["k"]
    return (s + k * snap["globals"][None, :]) / (c + k)


def block(heroes10: Sequence[int],
          hero_priors: np.ndarray) -> dict[str, float] | None:
    """Восемь колонок по десяти героям и их одиночным приорам.

    `hero_priors` — (10, 2) приоры тех же десяти героев ПО ТЕМ ЖЕ двум метрикам
    в порядке `SYN_METRICS`; берутся из боевого снимка, чтобы вычитаемое и
    уменьшаемое считались по одному источнику.
    """
    st = _load()
    snap = st.get("snap")
    if snap is None:
        return None
    try:
        h = np.asarray(list(heroes10), dtype=np.int64)
        hp = np.asarray(hero_priors, dtype=np.float64)
        if h.shape != (10,) or hp.shape != (10, 2):
            raise ValueError(f"ожидались 10 героев и (10, 2) приоров, "
                             f"получено {h.shape} и {hp.shape}")
        sides = []
        for off in (0, 5):
            keys = np.array([pair_key(h[off + i], h[off + j])
                             for i, j in PAIRS], dtype=np.int64)
            pp = _pair_prior(snap, keys)                      # (10 пар, 2)
            singles = np.array([0.5 * (hp[off + i] + hp[off + j])
                                for i, j in PAIRS])           # (10 пар, 2)
            syn = pp - singles
            # порядок внутри стороны: syn0_mean, syn0_max, syn1_mean, syn1_max
            sides.append(np.array([syn[:, 0].mean(), syn[:, 0].max(),
                                   syn[:, 1].mean(), syn[:, 1].max()]))
        rad, dire = sides
        vals = np.concatenate([rad - dire, rad + dire])
        return {c: float(v) for c, v in zip(COLUMNS, vals)}
    except Exception as exc:                         # noqa: BLE001
        _state["error"] = f"{type(exc).__name__}: {exc}"
        return None


def status() -> dict[str, Any]:
    st = _load()
    snap = st.get("snap")
    return {"ready": snap is not None, "error": st.get("error"),
            "path": str(SNAPSHOT),
            "pairs": 0 if snap is None else int(len(snap["keys"]))}
