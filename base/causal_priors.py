#!/usr/bin/env python3
"""Причинные приоры героя, игрока и пары: снимок и чтение в живом пути.

Это единственная часть предматчевой панели, которой нужно накопленное
состояние. Замерами показано, что она же единственная, которая окупается: на
тотале вход без приоров даёт 0.7121, с приорами 0.7442; на стороне 0.7190 и
0.7331; на времени 0.6188 и 0.6542. А 844 карточные колонки каталога, которые
выглядели ценными, дали −0.0020, 0.0000 и −0.0038 — их в живой путь не везём.

ЧТО ТАКОЕ ПРИОР. Для ключа (герой, аккаунт или пара) и метрики это среднее по
ВСЕМ ПРЕДЫДУЩИМ картам этого ключа, со шринкеджем к глобальному среднему:

    prior = (сумма + K · g) / (счётчик + K)

Шринкедж обязателен: у героя с тремя картами среднее — шум, и без него редкая
ячейка вносит больше вреда, чем пользы. K подобраны офлайн и лежат в снимке
рядом с данными, а не в коде.

ПОЧЕМУ СНИМОК, А НЕ ПЕРЕСЧЁТ. Офлайн приор считается кумулятивной суммой со
сбросом на границе группы по всему корпусу — на 12.6 млн строк это секунды, но
требует всего корпуса. В момент ставки корпуса нет; есть накопленное состояние
на момент сборки снимка, и оно даёт ровно ту же величину для карты, идущей
после снимка. Снимок мал: около 156 героев и несколько тысяч аккаунтов на 27
метрик — единицы мегабайт.

ГЛАВНОЕ — ОДИН КОД НА ОБА ПУТИ. Агрегация по пятёрке (какие метрики берутся
средним, какие ещё и разбросом, максимумом, минимумом) описана здесь один раз;
и офлайн-сборщик, и живой скорер зовут `aggregate_side`. Две копии этой
таблицы неизбежно разъедутся — так уже разъехался draft_logit, и это стоило
−0.0046.

Формат снимка (npz):
    keys_<kind>    int64  (n,)        ключи: hero_id, account_id, код пары
    sums_<kind>    float64 (n, m)     накопленные суммы по метрикам
    counts_<kind>  float64 (n, m)     накопленные счётчики
    globals        float64 (m,)       глобальное среднее метрики
    metrics        str    (m,)        имена метрик, порядок фиксирован
    shrink         float64 (3,)       K для hero/player/pair
    built_ts       int64              когда собран
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import time
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SNAPSHOT = Path(os.getenv(
    "PRIOR_SNAPSHOT", str(PROJECT_ROOT / "data" / "prior_snapshot.npz")))

# Порядок обязан совпадать с офлайн-сборщиком: колонки модели позиционные.
PRIOR_NAMES: tuple[str, ...] = (
    "own_kills", "enemy_kills", "kill_diff", "tot_kills", "p_own27", "p_tot54",
    "dur", "p_dur32", "p_dur36", "p_dur40",
    "k_0_10", "k_10_20", "k_20_30", "k_30_40",
    "ek_0_10", "ek_10_20", "ek_20_30", "ek_30_40",
    "win_0_10", "win_10_20", "win_20_30",
    "nwd_10", "nwd_20", "nwd_30", "xpd_10", "xpd_20", "winrate",
)
# Приоры игрока: полный список дал бы 27 колонок на сторону, что для пяти слотов
# перебор. Берутся величины, прямо связанные с целями панели.
P_KEEP: tuple[str, ...] = (
    "own_kills", "kill_diff", "p_own27", "dur", "p_dur36", "k_0_10",
    "k_10_20", "k_20_30", "win_10_20", "nwd_10", "winrate", "tot_kills",
)
# Метрики, у которых кроме среднего берутся ещё разброс и крайние значения.
H_WIDE: frozenset[str] = frozenset(
    {"own_kills", "k_10_20", "dur", "p_own27", "win_10_20", "nwd_10"})
P_WIDE: frozenset[str] = frozenset({"own_kills", "k_10_20", "win_10_20"})
K_HERO, K_PLAYER, K_PAIR = 200.0, 60.0, 120.0
TEAM_SLOTS = 5


@dataclass(frozen=True)
class PriorSnapshot:
    """Накопленное состояние приоров на момент сборки."""

    metrics: tuple[str, ...]
    hero_keys: np.ndarray
    hero_sums: np.ndarray
    hero_counts: np.ndarray
    player_keys: np.ndarray
    player_sums: np.ndarray
    player_counts: np.ndarray
    globals_: np.ndarray
    shrink: tuple[float, float, float] = (K_HERO, K_PLAYER, K_PAIR)
    built_ts: int = 0

    @property
    def age_days(self) -> float:
        """Возраст снимка в сутках; -1, если время сборки не записано.

        `built_ts` писался с самого начала, но НИ РАЗУ ни с чем не сравнивался —
        аудит 19.08.2026 не нашёл ни одного места, где он читается. Ровно так же
        молча протухал снимок предматчевой модели, пока в E-177 не измерили цену:
        AUC 0.7313 на свежем против 0.6883 на снимке старше 30 суток.
        """
        if not self.built_ts:
            return -1.0
        return (time.time() - float(self.built_ts)) / 86400.0

    @property
    def index(self) -> dict[str, int]:
        return {m: i for i, m in enumerate(self.metrics)}

    def _lookup(self, keys: Sequence[int], table_keys: np.ndarray,
                sums: np.ndarray, counts: np.ndarray, k: float) -> np.ndarray:
        """(len(keys), метрик) приоров; неизвестный ключ = глобальное среднее."""
        q = np.asarray(keys, dtype=np.int64)
        m = len(self.globals_)
        if not len(table_keys):
            # Пустая таблица — состояние прода до первой сборки снимка. Здесь
            # нужен глобальный приор, а не исключение: панель обязана молчать.
            return np.repeat(self.globals_[None, :].astype(np.float32), len(q), 0)
        pos = np.clip(np.searchsorted(table_keys, q), 0, len(table_keys) - 1)
        known = table_keys[pos] == q
        s = np.where(known[:, None], sums[pos], 0.0)
        c = np.where(known[:, None], counts[pos], 0.0)
        assert s.shape[1] == m, "снимок и список метрик разошлись"
        return ((s + k * self.globals_[None, :]) / (c + k)).astype(np.float32)

    def hero_priors(self, hero_ids: Sequence[int]) -> np.ndarray:
        return self._lookup(hero_ids, self.hero_keys, self.hero_sums,
                            self.hero_counts, self.shrink[0])

    def player_priors(self, account_ids: Sequence[int]) -> np.ndarray:
        return self._lookup(account_ids, self.player_keys, self.player_sums,
                            self.player_counts, self.shrink[1])

    def coverage(self, hero_ids: Sequence[int],
                 account_ids: Sequence[int]) -> dict[str, float]:
        """Доля слотов, для которых ключ реально нашёлся.

        Идёт в заполненность панели: приор неизвестного аккаунта равен
        глобальному среднему, и вердикт по такой карте читается иначе.
        """
        def share(q, table):
            if not len(table):
                return 0.0
            a = np.asarray(q, dtype=np.int64)
            pos = np.clip(np.searchsorted(table, a), 0, len(table) - 1)
            return float((table[pos] == a).mean())
        return {"hero": share(hero_ids, self.hero_keys),
                "player": share(account_ids, self.player_keys)}


def aggregate_side(HPS: np.ndarray, PPS: np.ndarray,
                   names_out: list[str] | None = None) -> np.ndarray:
    """Колонки F6/F7 одной стороны из приоров её пяти слотов.

    HPS — (n, 5, len(PRIOR_NAMES)) приоры героя, PPS — то же для игрока.
    Таблица «что берём средним, а что ещё и разбросом» описана здесь ОДИН раз
    и используется и офлайном, и живым путём.
    """
    HPS = np.asarray(HPS, dtype=np.float32)
    PPS = np.asarray(PPS, dtype=np.float32)
    if HPS.ndim != 3 or HPS.shape[1] != TEAM_SLOTS:
        raise ValueError(f"HPS ожидался (n, {TEAM_SLOTS}, m), получено {HPS.shape}")
    if PPS.shape[:2] != HPS.shape[:2]:
        raise ValueError("PPS и HPS должны совпадать по картам и слотам")
    out: list[np.ndarray] = []

    def add(v: np.ndarray, nm: str) -> None:
        out.append(np.asarray(v, dtype=np.float32))
        if names_out is not None:
            names_out.append(nm)

    for j, nm in enumerate(PRIOR_NAMES):
        V = HPS[:, :, j]
        add(V.mean(1), f"F6_h_{nm}_mean")
        if nm in H_WIDE:
            add(V.std(1), f"F6_h_{nm}_std")
            add(V.max(1), f"F6_h_{nm}_max")
    for j, nm in enumerate(PRIOR_NAMES):
        if nm not in P_KEEP:
            continue
        V = PPS[:, :, j]
        add(V.mean(1), f"F7_p_{nm}_mean")
        if nm in P_WIDE:
            add(V.max(1), f"F7_p_{nm}_max")
            add(V.min(1), f"F7_p_{nm}_min")
    return np.column_stack(out)


def sym_priors(heroes10: np.ndarray, accounts10: np.ndarray,
               snap: PriorSnapshot,
               names_out: list[str] | None = None) -> np.ndarray:
    """Блок приоров карты: [разность сторон, сумма сторон]."""
    heroes10 = np.asarray(heroes10, dtype=np.int64)
    accounts10 = np.asarray(accounts10, dtype=np.int64)
    if heroes10.shape != accounts10.shape or heroes10.shape[1] != 2 * TEAM_SLOTS:
        raise ValueError("ожидались (n, 10) hero_id и account_id одинаковой формы")
    n = len(heroes10)
    H = snap.hero_priors(heroes10.ravel()).reshape(n, 2 * TEAM_SLOTS, -1)
    P = snap.player_priors(accounts10.ravel()).reshape(n, 2 * TEAM_SLOTS, -1)
    side_names: list[str] | None = [] if names_out is not None else None
    R = aggregate_side(H[:, :TEAM_SLOTS], P[:, :TEAM_SLOTS], side_names)
    D = aggregate_side(H[:, TEAM_SLOTS:], P[:, TEAM_SLOTS:], None)
    if names_out is not None and side_names is not None:
        names_out.extend([f"{s}_diff" for s in side_names])
        names_out.extend([f"{s}_sum" for s in side_names])
    return np.hstack([R - D, R + D])


# Сколько суток снимок считается свежим. Не гейт: при превышении снимок ВСЁ РАВНО
# отдаётся, но в лог уходит предупреждение — ровно как `_SNAPSHOT_WARN_DAYS` в
# боевом `win_model_veto.py`. Жёсткий отказ здесь сделал бы поломку такой же
# невидимой, какой она была без проверки вообще.
SNAPSHOT_WARN_DAYS = 14.0
_STALE_WARNED = False


def load_snapshot(path: Path | None = None) -> PriorSnapshot | None:
    """Снимок с диска. `None`, если его нет: панель обязана молчать, а не падать."""
    global _STALE_WARNED
    target = Path(path or DEFAULT_SNAPSHOT)
    try:
        z = np.load(target, allow_pickle=False)
    except (OSError, ValueError):
        return None
    try:
        snap = PriorSnapshot(
            metrics=tuple(str(x) for x in z["metrics"]),
            hero_keys=z["keys_hero"], hero_sums=z["sums_hero"],
            hero_counts=z["counts_hero"], player_keys=z["keys_player"],
            player_sums=z["sums_player"], player_counts=z["counts_player"],
            globals_=z["globals"], shrink=tuple(float(v) for v in z["shrink"]),
            built_ts=int(z["built_ts"]))
    except KeyError:
        return None
    age = snap.age_days
    if age > SNAPSHOT_WARN_DAYS and not _STALE_WARNED:
        _STALE_WARNED = True
        print(f"ВНИМАНИЕ: снимок причинных приоров старше {SNAPSHOT_WARN_DAYS:.0f} "
              f"суток (возраст {age:.1f}) — {target}. Приоры считаются по устаревшей "
              "статистике; цена устаревания измерена в E-177 для соседнего снимка.",
              flush=True)
    return snap


def save_snapshot(path: Path, *, metrics: Iterable[str],
                  hero: Mapping[int, tuple[np.ndarray, np.ndarray]],
                  player: Mapping[int, tuple[np.ndarray, np.ndarray]],
                  globals_: np.ndarray, built_ts: int,
                  shrink: tuple[float, float, float] = (K_HERO, K_PLAYER, K_PAIR)
                  ) -> Path:
    """Запись снимка: во временный файл и переименованием поверх."""
    def pack(d: Mapping[int, tuple[np.ndarray, np.ndarray]]):
        if not d:
            m = len(tuple(metrics))
            return (np.zeros(0, np.int64), np.zeros((0, m)), np.zeros((0, m)))
        ks = np.array(sorted(d), dtype=np.int64)        # отсортированы под searchsorted
        # float32: снимок с полутора миллионами аккаунтов в float64 весит 650 МБ,
        # а приор всё равно шринкуется к глобальному — седьмой знак там не нужен.
        s = np.stack([np.asarray(d[int(k)][0], dtype=np.float32) for k in ks])
        c = np.stack([np.asarray(d[int(k)][1], dtype=np.float32) for k in ks])
        return ks, s, c

    hk, hs, hc = pack(hero)
    pk, ps, pc = pack(player)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.stem + ".tmp.npz")
    np.savez(tmp, metrics=np.array(list(metrics)), keys_hero=hk, sums_hero=hs,
             counts_hero=hc, keys_player=pk, sums_player=ps, counts_player=pc,
             globals=np.asarray(globals_, dtype=np.float64),
             shrink=np.asarray(shrink, dtype=np.float64),
             built_ts=np.int64(built_ts))
    tmp.replace(path)
    return path
