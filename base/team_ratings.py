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


class _RowStore:
    """Ключ → строка из трёх чисел, на отсортированных массивах вместо словаря.

    Живой путь состояние ТОЛЬКО ЧИТАЕТ: `block()` зовёт `features(mutate=False)`,
    а это несколько `.get()`. Словарь же стоил 480 МБ при 47 МБ полезных данных —
    два миллиона записей, у каждой объект-ключ и список из трёх Python-float. Ровно
    эти накладные уже вычищены в `prematch_scorer` (`PairTable`/`AccTable`), здесь
    тот же приём.

    Запись поддержана наложением: офлайн-сборщик (`advance`/`_apply`) работает как
    прежде, его изменения ложатся в обычный словарь поверх массивов. Пока пишут
    мало — а живое чтение не пишет вовсе, — наложение почти ничего не стоит.
    """

    __slots__ = ("_keys", "_vals", "_over")

    def __init__(self, keys: np.ndarray, vals: np.ndarray) -> None:
        order = np.argsort(keys, kind="stable")
        self._keys = np.ascontiguousarray(keys[order])
        self._vals = np.ascontiguousarray(vals[order])
        self._over: dict = {}

    def _lookup(self, key, default=None):
        """Поиск по УЖЕ приведённому ключу. Отдельно от `get`, потому что у
        парного хранилища `get` ключ пакует, и вызов одного через другой упаковал
        бы его дважды."""
        found = self._over.get(key, _MISSING)
        if found is not _MISSING:
            return found
        i = int(np.searchsorted(self._keys, key))
        if i < len(self._keys) and self._keys[i] == key:
            # Кортеж обычных float, а не строка ndarray: дальше числа идут в
            # `sum()`, и накопление в np.float64 давало другой последний разряд.
            # Расхождение было 2.33e-15 — для вердикта пустяк, но побитовое
            # совпадение со словарной реализацией того стоит, а цена — двадцать
            # кортежей на карту.
            return self._row(self._vals[i])
        return default

    @staticmethod
    def _row(values):
        return tuple(float(x) for x in values)

    def get(self, key, default=None):
        return self._lookup(key, default)

    def __getitem__(self, key):
        v = self.get(key, _MISSING)
        if v is _MISSING:
            raise KeyError(key)
        return v

    def __setitem__(self, key, value) -> None:
        self._over[key] = value

    def overlay_clone(self) -> "_RowStore":
        """Копия для наложения: массивы ОБЩИЕ, словарь наложения свой.

        `__init__` не зовём намеренно — он сортирует ключи, а их два миллиона,
        и платить эту цену на каждой живой карте нельзя. Массивы после сборки
        только читаются, поэтому делить их между копиями безопасно; расходятся
        копии ровно в `_over`, куда пишет `advance`.
        """
        new = object.__new__(type(self))
        new._keys = self._keys
        new._vals = self._vals
        new._over = dict(self._over)
        return new

    def __contains__(self, key) -> bool:
        return self.get(key, _MISSING) is not _MISSING

    def __len__(self) -> int:
        base = len(self._keys)
        extra = sum(1 for k in self._over if not self._in_base(k))
        return base + extra

    def _in_base(self, key) -> bool:
        i = int(np.searchsorted(self._keys, key))
        return i < len(self._keys) and self._keys[i] == key

    def keys(self):
        seen = set(self._over)
        yield from (k for k in self._keys.tolist() if k not in seen)
        yield from self._over

    def items(self):
        for k in self.keys():
            yield k, self.get(k)


class _PairStore(_RowStore):
    """То же, но ключ — пара `(аккаунт, позиция)`, упакованная в одно число.

    Кортеж-ключ стоит дороже всего остального: 586 251 запись давала 586 251
    объект-кортеж сверх самих данных. Позиций всего пять (проверено на снимке:
    максимальный индекс 4), поэтому трёх бит хватает, и `acc << 3 | pos` не даёт
    ни одной коллизии.
    """

    __slots__ = ()

    @staticmethod
    def pack(key) -> int:
        acc, pos = key
        if not 0 <= pos < 8:
            raise ValueError(f"позиция {pos} не влезает в упаковку (нужно 0..7)")
        return (int(acc) << 3) | int(pos)

    def get(self, key, default=None):
        return self._lookup(self.pack(key), default)

    def __getitem__(self, key):
        v = self._lookup(self.pack(key), _MISSING)
        if v is _MISSING:
            raise KeyError(key)
        return v

    def __setitem__(self, key, value) -> None:
        self._over[self.pack(key)] = value

    def __contains__(self, key) -> bool:
        return self._lookup(self.pack(key), _MISSING) is not _MISSING

    def keys(self):
        for k in super().keys():
            yield (int(k) >> 3, int(k) & 7)


class _ScalarStore(_RowStore):
    """Ключ → одно число. Для `ts_mu`, `ts_sig`, `ts_last`."""

    __slots__ = ()

    @staticmethod
    def _row(values):
        return float(values)


#: Отличает «в наложении лежит None» от «ключа нет вовсе».
_MISSING = object()


class RatingState:
    """Состояние рейтингов: аккаунт → Glicko, (аккаунт, позиция) → поз-Glicko,
    аккаунт → TrueSkill. Одинаково используется офлайн-накоплением и живым
    чтением, чтобы две копии формулы не разъехались.

    Пустое состояние держит словари: так работает офлайн-накопление, которому
    нужна свободная запись. Состояние, поднятое из снимка (`from_arrays`), держит
    массивы — оно только читается, и словарь на нём стоил бы вдесятеро дороже.
    """

    __slots__ = ("glicko", "pos", "ts_mu", "ts_sig", "ts_last")

    def __init__(self) -> None:
        self.glicko: dict[int, list] = {}            # acc -> [r, rd, last_ts]
        self.pos: dict[tuple[int, int], list] = {}   # (acc, pos) -> [r, rd, ts]
        self.ts_mu: dict[int, float] = {}
        self.ts_sig: dict[int, float] = {}
        self.ts_last: dict[int, int] = {}

    @classmethod
    def from_arrays(cls, g_keys, g_vals, p_keys, p_vals,
                    t_keys, t_vals) -> "RatingState":
        """Состояние на массивах: то же поведение, вдесятеро меньше памяти."""
        st = cls()
        st.glicko = _RowStore(np.asarray(g_keys, dtype=np.int64),
                              np.asarray(g_vals, dtype=np.float64))
        pk = np.asarray(p_keys, dtype=np.int64).reshape(-1, 2)
        if len(pk) and (pk[:, 1].min() < 0 or pk[:, 1].max() > 7):
            # Молча перепутать ячейки нельзя: при позиции вне 0..7 упаковка
            # склеила бы разные пары в один ключ.
            raise ValueError("позиции в снимке не влезают в упаковку")
        packed = ((pk[:, 0] << 3) | pk[:, 1]) if len(pk) else np.zeros(0, np.int64)
        st.pos = _PairStore(packed, np.asarray(p_vals, dtype=np.float64))
        tk = np.asarray(t_keys, dtype=np.int64)
        tv = np.asarray(t_vals, dtype=np.float64)
        st.ts_mu = _ScalarStore(tk, tv[:, 0] if len(tv) else np.zeros(0))
        st.ts_sig = _ScalarStore(tk, tv[:, 1] if len(tv) else np.zeros(0))
        st.ts_last = _ScalarStore(tk, tv[:, 2] if len(tv) else np.zeros(0))
        return st

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
                # `rec is not None`, а не `rec`: со снимка на массивах запись —
                # это строка ndarray, и её истинность неоднозначна.
                self.glicko[acc] = [r, rd, rec[2] if rec is not None else now]
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

    def overlay_clone(self) -> "RatingState":
        """Состояние для наложения живых карт поверх снимка.

        Массивы снимка общие с исходным состоянием, расходятся только словари
        наложения. Нужно, потому что `advance` пишет, а снимок в `_load()`
        живёт один на процесс: провести карты прямо в нём — значит накопить их
        дважды на следующем же вызове.
        """
        st = RatingState()
        for name in RatingState.__slots__:
            value = getattr(self, name)
            clone = getattr(value, "overlay_clone", None)
            setattr(st, name, clone() if clone is not None else dict(value))
        return st

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


def advance_newer_than(st: RatingState, ts, accounts, wins,
                       last_ts: int) -> tuple[int, int]:
    """Провести compact-карты СТРОГО новее `last_ts`. Возвращает (n, newest_ts).

    Эталон гибридного блока закрывается последней своей картой. Всё, что в
    compact старше или равно этому срезу — в том числе карты, которых в
    482k-подмножестве не было, — не проводится: иначе рейтинг получил бы
    историю в неправильном порядке. Новее среза — можно и нужно.
    """
    ts_a = np.asarray(ts, dtype=np.int64)
    acc_a = np.asarray(accounts)
    win_a = np.asarray(wins)
    last = int(last_ts)
    newest = last
    n = 0
    if len(ts_a) == 0:
        return n, newest
    for i in np.argsort(ts_a, kind="mergesort"):
        now = int(ts_a[i])
        if now <= last:
            continue
        st.advance(now, [int(x) for x in acc_a[i]], bool(win_a[i]))
        n += 1
        newest = now
    return n, newest


def overlay(snap: RatingState, maps: Sequence[tuple]) -> RatingState:
    """Копия снимка плюс карты, сыгранные после его среза.

    ЗАЧЕМ. Снимок рейтингов собирается ночью, а Glicko и TrueSkill — величины
    накопительные: сыграл карту — сила изменилась. До этого живой путь читал
    вчерашние числа с `mutate=False`, то есть на четвёртой карте дня показывал
    состояние игрока до первой. Живой ELO этот разрыв закрывает с 23.08, у
    рейтингов он оставался.

    `maps` — `(ts, accounts10, radiant_won)` по ВОЗРАСТАНИЮ времени: рейтинг
    зависит от порядка, и перестановка двух карт даёт другой ответ. Исходный
    снимок не мутируется; ошибка на одной карте роняет всю накладку, а не
    оставляет состояние проведённым наполовину.
    """
    if not maps:
        return snap
    st = snap.overlay_clone()
    for ts, accounts10, radiant_won in maps:
        st.advance(int(ts), accounts10, bool(radiant_won))
    return st


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
    # Массивы кладутся как есть, без разбора в словари: `.tolist()` на этих же
    # данных материализовывал два миллиона списков Python и стоил 480 МБ вместо
    # 47. Живой путь состояние только читает, а офлайн-запись работает через
    # наложение внутри хранилищ.
    return RatingState.from_arrays(z["g_keys"], z["g_vals"], z["p_keys"],
                                   z["p_vals"], z["t_keys"], z["t_vals"])


def _load() -> dict[str, Any]:
    with _lock:
        if _state["loaded"]:
            return _state
        _state["loaded"] = True
        try:
            _state["snap"] = load_snapshot()
            if _state["snap"] is None:
                _state["error"] = f"снимок рейтингов не найден: {SNAPSHOT}"
            else:
                # Срез снимка нужен наложению живых карт: без него граница
                # берётся нулевой и карты проводятся повторно на каждой оценке.
                # В самом состоянии его не держим — у `RatingState` слоты.
                _state["built_ts"] = _snapshot_built_ts()
        except Exception as exc:                     # noqa: BLE001
            _state["error"] = f"{type(exc).__name__}: {exc}"
        return _state


def _snapshot_built_ts(path: Path | None = None) -> int:
    """Когда собран снимок. 0 — метки нет или файл не читается."""
    p = Path(path or SNAPSHOT)
    try:
        with np.load(p, allow_pickle=False) as z:
            return int(z["built_ts"])
    except (OSError, ValueError, KeyError):
        return 0


def block(now_ts: int, accounts10: Sequence[int], *,
          snap: RatingState | None = None) -> dict[str, float] | None:
    """Шесть колонок по времени карты и десяти аккаунтам. `None` без снимка.

    `snap` — состояние с уже наложенными живыми картами (`overlay`). Без него
    берётся снимок из `_load()`, то есть ночной срез.
    """
    if snap is None:
        st = _load()
        snap = st.get("snap")
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
