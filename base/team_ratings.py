#!/usr/bin/env python3
"""Живой провайдер блока `rating_*` — Glicko-1, позиционный Glicko и TrueSkill.

Шесть колонок и их ПОРЯДОК взяты не из имён, а из сборщика обучающей матрицы
(`runtime/experiments/misc/kills27_max_model.py:126-128`):

    rating_0  glicko          (сред. рейтинг Rad − Dire) / 400
    rating_1  glicko_p        ожидаемая доля побед Rad минус 0.5
    rating_2  glicko_rd       (RD Rad − RD Dire) / 100
    rating_3  pos_glicko      то же, что glicko, но рейтинг ведётся на ПАРУ
                              (аккаунт, позиция), а не на аккаунт
    rating_4  trueskill       (сред. mu Rad − сред. mu Dire) / (25/3)
    rating_5  trueskill_sig   сред. sigma Rad − сред. sigma Dire

Привязка проверена не только по исходнику. Обучение адресует колонки ПО ИМЕНИ
(`gl["glicko"]` и т.д.), а здесь они пересчитываются портом формулы, поэтому
сверка шла числами: `runtime/artifacts/misc/build_rating_snapshot.md` — все
482 486 карт, каждая из шести колонок, `max|Δ| = 0.000e+00` при пороге 1e-9.
Порядок и формула доказаны вместе, перепроверять по комментарию нечего.

ПОЧЕМУ ОТДЕЛЬНЫЙ СНИМОК. Рейтинг — величина накопительная: чтобы узнать её
сегодня, нужно пройти всю историю матчей по порядку. На живом пути этого времени
нет, поэтому состояние копится офлайн и кладётся в снимок, как это уже сделано
для причинных приоров. Живой путь только читает.

СНИМОК СТАРЕЕТ. Между пересборками рейтинги стоят на месте, а RD по формуле
Glicko растёт со временем простоя — и растёт он ПРАВИЛЬНО, потому что считается
от даты последнего матча игрока, а не от даты снимка. То есть устаревание
снимка портит `glicko`/`trueskill`, но не ломает `glicko_rd`.

ПОРТ БУКВАЛЬНЫЙ, ВКЛЮЧАЯ СТРАННОСТИ ОРИГИНАЛА. Чтение состояния в
`_side_glicko` записывает раздутый RD обратно и заводит запись для неизвестного
аккаунта — модель училась на числах, полученных именно так. «Причесать» это
здесь значило бы подать другой признак под старым именем.
"""
from __future__ import annotations

import math
import os
import threading
from pathlib import Path
from typing import Any, Sequence

import time
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = Path(os.getenv("PANEL_RATING_SNAPSHOT",
                          str(PROJECT_ROOT / "data" / "rating_snapshot.npz")))

COLUMNS: tuple[str, ...] = tuple(f"rating_{i}" for i in range(6))

# Константы Glicko-1 и TrueSkill — из `undercount_glicko.py`, менять нельзя:
# модель училась ровно на этих.
Q = math.log(10.0) / 400.0
C_TIME = 63.2
RD_MAX = 350.0
PI2 = math.pi * math.pi
R0 = 1500.0
TS_MU0, TS_SIG0, TS_BETA = 25.0, 25.0 / 3.0, 25.0 / 6.0
TS_TAU = 25.0 / 300.0

_lock = threading.Lock()
_state: dict[str, Any] = {"loaded": False, "snap": None, "error": None}


def g_rd(rd: float) -> float:
    return 1.0 / math.sqrt(1.0 + 3.0 * Q * Q * rd * rd / PI2)


class RatingState:
    """Состояние рейтингов: аккаунт → Glicko, (аккаунт, позиция) → поз-Glicko,
    аккаунт → TrueSkill. Одинаково используется офлайн-накоплением и живым
    чтением, чтобы две копии формулы не разъехались."""

    __slots__ = ("glicko", "pos", "ts_mu", "ts_sig", "ts_last")

    def __init__(self) -> None:
        self.glicko: dict[int, list] = {}            # acc -> [r, rd, last_ts]
        self.pos: dict[tuple[int, int], list] = {}   # (acc, pos) -> [r, rd, ts]
        self.ts_mu: dict[int, float] = {}
        self.ts_sig: dict[int, float] = {}
        self.ts_last: dict[int, int] = {}

    # ---------- чтение ----------
    def _side_glicko(self, players: list[tuple[int, int]], now: int,
                     mutate: bool) -> tuple[float, float, float]:
        """Средний рейтинг стороны, её RD и средний позиционный рейтинг.

        `mutate=True` воспроизводит запись раздутого RD обратно в состояние —
        так делал офлайн-сборщик, и от этого зависят последующие карты.
        """
        rs, rds, prs = [], [], []
        for acc, p in players:
            rec = self.glicko.get(acc)
            if rec is None:
                r, rd = R0, RD_MAX
            else:
                r, rd, last = rec
                months = max(0.0, (now - last) / 86400.0 / 30.0)
                rd = min(math.sqrt(rd * rd + C_TIME * C_TIME * months), RD_MAX)
            rs.append(r)
            rds.append(rd)
            prec = self.pos.get((acc, p))
            if prec is None:
                prs.append(R0)
            else:
                pr, prd, plast = prec
                months = max(0.0, (now - plast) / 86400.0 / 30.0)
                prd = min(math.sqrt(prd * prd + C_TIME * C_TIME * months), RD_MAX)
                prs.append(pr)
                if mutate:
                    self.pos[(acc, p)] = [pr, prd, plast]
            if mutate:
                self.glicko[acc] = [r, rd, rec[2] if rec else now]
        if not rs:
            return R0, RD_MAX, R0
        return (float(sum(rs) / len(rs)),
                float(math.sqrt(sum(x * x for x in rds) / len(rds))),
                float(sum(prs) / len(prs)))

    def _side_ts(self, players: list[tuple[int, int]],
                 now: int) -> tuple[float, float, list[int], list[float]]:
        mus, sigs, ids = [], [], []
        for acc, _p in players:
            mu = self.ts_mu.get(acc, TS_MU0)
            sg = self.ts_sig.get(acc, TS_SIG0)
            last = self.ts_last.get(acc)
            if last is not None:
                weeks = max(0.0, (now - last) / 86400.0 / 7.0)
                sg = math.sqrt(sg * sg + (TS_TAU * TS_TAU) * weeks)
            mus.append(mu)
            sigs.append(sg)
            ids.append(acc)
        if not mus:
            return TS_MU0, TS_SIG0, [], []
        return (float(sum(mus)),
                float(math.sqrt(sum(s * s for s in sigs))), ids, sigs)

    @staticmethod
    def _split(accounts10: Sequence[int]) -> list[list[tuple[int, int]]]:
        live: list[list[tuple[int, int]]] = [[], []]
        for s in range(2):
            for p in range(5):
                a = int(accounts10[s * 5 + p])
                if a > 0:
                    live[s].append((a, p))
        return live

    def _read(self, live: list[list[tuple[int, int]]], now: int,
              mutate: bool) -> tuple[tuple[float, ...], tuple[float, float,
                                                              float, float]]:
        """Шесть чисел и агрегаты сторон, нужные для последующего обновления.

        Читать состояние можно РОВНО ОДИН РАЗ на карту: чтение раздувает RD по
        времени простоя и пишет его обратно, поэтому второй проход раздул бы его
        повторно от той же даты. Именно на этом порт разошёлся с обучением, пока
        `features` и `update` читали состояние каждый сам.
        """
        r0, rd0, pr0 = self._side_glicko(live[0], now, mutate)
        r1, rd1, pr1 = self._side_glicko(live[1], now, mutate)
        e0 = 1.0 / (1.0 + 10.0 ** (-g_rd(rd1) * (r0 - r1) / 400.0))
        mu0, sg0, _, _ = self._side_ts(live[0], now)
        mu1, sg1, _, _ = self._side_ts(live[1], now)
        n0, n1 = max(len(live[0]), 1), max(len(live[1]), 1)
        vals = (
            (r0 - r1) / 400.0,                       # rating_0 glicko
            e0 - 0.5,                                # rating_1 glicko_p
            (rd0 - rd1) / 100.0,                     # rating_2 glicko_rd
            (pr0 - pr1) / 400.0,                     # rating_3 pos_glicko
            (mu0 / n0 - mu1 / n1) / (25.0 / 3.0),    # rating_4 trueskill
            sg0 / n0 - sg1 / n1,                     # rating_5 trueskill_sig
        )
        return vals, (r0, rd0, r1, rd1)

    def features(self, now: int, accounts10: Sequence[int],
                 mutate: bool = False) -> tuple[float, ...]:
        """Шесть чисел в порядке rating_0..rating_5."""
        vals, _ = self._read(self._split(accounts10), now, mutate)
        return vals

    # ---------- накопление ----------
    def advance(self, now: int, accounts10: Sequence[int],
                radiant_won: bool) -> tuple[float, ...]:
        """Прочитать карту и провести через неё состояние — одним проходом.

        Возвращает те же шесть чисел, что видел бы живой путь ДО результата,
        поэтому офлайн-накопление и живое чтение не могут разъехаться.
        """
        live = self._split(accounts10)
        vals, (r0, rd0, r1, rd1) = self._read(live, now, mutate=True)
        self._apply(live, now, radiant_won, r0, rd0, r1, rd1)
        return vals

    def _apply(self, live: list[list[tuple[int, int]]], now: int,
               radiant_won: bool, r0: float, rd0: float, r1: float,
               rd1: float) -> None:
        for s, won in ((0, radiant_won), (1, not radiant_won)):
            opp_r, opp_rd = (r1, rd1) if s == 0 else (r0, rd0)
            gopp = g_rd(opp_rd)
            for acc, p in live[s]:
                r, rd, _last = self.glicko[acc]
                e = 1.0 / (1.0 + 10.0 ** (-gopp * (r - opp_r) / 400.0))
                d2_inv = (Q * Q) * (gopp * gopp) * e * (1.0 - e)
                d2 = 1.0 / d2_inv if d2_inv > 1e-18 else 1e18
                den = 1.0 / (rd * rd) + 1.0 / d2
                self.glicko[acc] = [r + (Q / den) * gopp * (float(won) - e),
                                    min(math.sqrt(1.0 / den), RD_MAX), now]
                pr, prd, _ = self.pos.get((acc, p), [R0, RD_MAX, now])
                e_p = 1.0 / (1.0 + 10.0 ** (-gopp * (pr - opp_r) / 400.0))
                d2i_p = (Q * Q) * (gopp * gopp) * e_p * (1.0 - e_p)
                d2_p = 1.0 / d2i_p if d2i_p > 1e-18 else 1e18
                den_p = 1.0 / (prd * prd) + 1.0 / d2_p
                self.pos[(acc, p)] = [
                    pr + (Q / den_p) * gopp * (float(won) - e_p),
                    min(math.sqrt(1.0 / den_p), RD_MAX), now]

        id0, m0, s0 = _ts_lists(self, live[0], now)
        id1, m1, s1 = _ts_lists(self, live[1], now)
        if not (id0 and id1):
            return
        c = math.sqrt(sum(x * x for x in s0) + sum(x * x for x in s1)
                      + 2.0 * TS_BETA * TS_BETA)
        c = max(c, 1e-9)
        t = ((sum(m0) - sum(m1)) if radiant_won else (sum(m1) - sum(m0))) / c
        cdf = max(0.5 * (1.0 + math.erf(t / math.sqrt(2.0))), 1e-12)
        v = (math.exp(-0.5 * t * t) / math.sqrt(2.0 * math.pi)) / cdf
        w = v * (v + t)
        win_ids, win_s, lose_ids, lose_s = (
            (id0, s0, id1, s1) if radiant_won else (id1, s1, id0, s0))
        for acc, sg in zip(win_ids, win_s):
            var = sg * sg
            self.ts_mu[acc] = self.ts_mu.get(acc, TS_MU0) + var / c * v
            self.ts_sig[acc] = math.sqrt(max(var * (1.0 - var / (c * c) * w), 1e-6))
            self.ts_last[acc] = now
        for acc, sg in zip(lose_ids, lose_s):
            var = sg * sg
            self.ts_mu[acc] = self.ts_mu.get(acc, TS_MU0) - var / c * v
            self.ts_sig[acc] = math.sqrt(max(var * (1.0 - var / (c * c) * w), 1e-6))
            self.ts_last[acc] = now


def _ts_lists(st: RatingState, players: list[tuple[int, int]],
              now: int) -> tuple[list[int], list[float], list[float]]:
    mus, sigs, ids = [], [], []
    for acc, _p in players:
        mu = st.ts_mu.get(acc, TS_MU0)
        sg = st.ts_sig.get(acc, TS_SIG0)
        last = st.ts_last.get(acc)
        if last is not None:
            weeks = max(0.0, (now - last) / 86400.0 / 7.0)
            sg = math.sqrt(sg * sg + (TS_TAU * TS_TAU) * weeks)
        mus.append(mu)
        sigs.append(sg)
        ids.append(acc)
    return ids, mus, sigs


# ---------- снимок ----------
def save_snapshot(path: Path, st: RatingState, built_ts: int) -> None:
    """Пишем во временный файл и переименовываем поверх: живой читатель не
    должен увидеть полуфайл."""
    path.parent.mkdir(parents=True, exist_ok=True)
    g_keys = np.fromiter(st.glicko.keys(), dtype=np.int64, count=len(st.glicko))
    g_vals = np.array([st.glicko[k] for k in g_keys], dtype=np.float64) \
        if len(g_keys) else np.zeros((0, 3))
    p_items = list(st.pos.items())
    p_keys = np.array([[a, p] for (a, p), _ in p_items], dtype=np.int64) \
        if p_items else np.zeros((0, 2), dtype=np.int64)
    p_vals = np.array([v for _, v in p_items], dtype=np.float64) \
        if p_items else np.zeros((0, 3))
    t_keys = np.fromiter(st.ts_mu.keys(), dtype=np.int64, count=len(st.ts_mu))
    t_vals = np.array([[st.ts_mu[k], st.ts_sig.get(k, TS_SIG0),
                        st.ts_last.get(k, 0)] for k in t_keys],
                      dtype=np.float64) if len(t_keys) else np.zeros((0, 3))
    tmp = path.with_suffix(".tmp.npz")
    np.savez_compressed(tmp, g_keys=g_keys, g_vals=g_vals, p_keys=p_keys,
                        p_vals=p_vals, t_keys=t_keys, t_vals=t_vals,
                        built_ts=np.int64(built_ts))
    os.replace(tmp, path)


# Возраст, после которого снимок рейтингов считается протухшим. Не гейт:
# снимок всё равно отдаётся, но в лог уходит предупреждение. `built_ts` писался
# здесь с самого начала и НИ РАЗУ не читался обратно — аудит 19.08.2026.
SNAPSHOT_WARN_DAYS = 14.0
_STALE_WARNED = False


def load_snapshot(path: Path | None = None) -> RatingState | None:
    global _STALE_WARNED
    p = Path(path or SNAPSHOT)
    try:
        z = np.load(p, allow_pickle=False)
    except (OSError, ValueError):
        return None
    try:
        _built = int(z["built_ts"])
    except (KeyError, ValueError):
        _built = 0
    if _built:
        _age = (time.time() - float(_built)) / 86400.0
        if _age > SNAPSHOT_WARN_DAYS and not _STALE_WARNED:
            _STALE_WARNED = True
            print(f"ВНИМАНИЕ: снимок рейтингов команд старше "
                  f"{SNAPSHOT_WARN_DAYS:.0f} суток (возраст {_age:.1f}) — {p}. "
                  "Рейтинги считаются по устаревшему корпусу.", flush=True)
    st = RatingState()
    for k, v in zip(z["g_keys"].tolist(), z["g_vals"].tolist()):
        st.glicko[int(k)] = [v[0], v[1], int(v[2])]
    for k, v in zip(z["p_keys"].tolist(), z["p_vals"].tolist()):
        st.pos[(int(k[0]), int(k[1]))] = [v[0], v[1], int(v[2])]
    for k, v in zip(z["t_keys"].tolist(), z["t_vals"].tolist()):
        a = int(k)
        st.ts_mu[a], st.ts_sig[a], st.ts_last[a] = v[0], v[1], int(v[2])
    return st


def _load() -> dict[str, Any]:
    with _lock:
        if _state["loaded"]:
            return _state
        _state["loaded"] = True
        try:
            _state["snap"] = load_snapshot()
            if _state["snap"] is None:
                _state["error"] = f"снимок рейтингов не найден: {SNAPSHOT}"
        except Exception as exc:                     # noqa: BLE001
            _state["error"] = f"{type(exc).__name__}: {exc}"
        return _state


def block(now_ts: int, accounts10: Sequence[int]) -> dict[str, float] | None:
    """Шесть колонок по времени карты и десяти аккаунтам. `None` без снимка."""
    st = _load()
    snap: RatingState | None = st.get("snap")
    if snap is None:
        return None
    try:
        # mutate=False: живое чтение не смеет менять снимок — иначе два подряд
        # вызова по одной карте дали бы разные числа.
        vals = snap.features(int(now_ts), accounts10, mutate=False)
    except Exception as exc:                         # noqa: BLE001
        _state["error"] = f"{type(exc).__name__}: {exc}"
        return None
    return {c: float(v) for c, v in zip(COLUMNS, vals)}


def status() -> dict[str, Any]:
    st = _load()
    snap = st.get("snap")
    return {"ready": snap is not None, "error": st.get("error"),
            "path": str(SNAPSHOT),
            "accounts": 0 if snap is None else len(snap.glicko)}
