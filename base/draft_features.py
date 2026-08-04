"""Serializable draft encoders: 10 hero IDs -> sparse design matrix.

Everything here is derived from the ten fixed-position hero IDs alone, so the
leakage-safe contract of the public draft experiment is preserved: no ingame
statistics, no third-party ratings, nothing observed after the draft.

Blocks
  hero    one column per hero, +1 radiant / -1 dire when `signed`, else +1
  role    one column per (role, hero) pair, same sign convention
  synergy one column per unordered same-team hero pair
  counter one column per unordered cross-team hero pair; when `signed` the sign
          says which side held the lower hero index, which is what makes the
          block antisymmetric and therefore side-consistent

`signed=True` is for the win target (swapping sides must flip the prediction);
`signed=False` is for kills and duration, which are properties of the map.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations

import numpy as np
import scipy.sparse as sp

TEAM_PAIRS = tuple(combinations(range(5), 2))
KIND_ROLE = "hero_role"
KIND_PAIR = "hero_role_pair"


def _csr(cols: np.ndarray, vals: np.ndarray, ncols: int) -> sp.csr_matrix:
    """float64 on purpose: lbfgs diverges on a float32 design of this width."""
    n, k = cols.shape
    indptr = np.arange(0, n * k + 1, k, dtype=np.int64)
    return sp.csr_matrix(
        (vals.ravel().astype(np.float64), cols.ravel().astype(np.int32), indptr), shape=(n, ncols)
    )


@dataclass
class DraftFeatureEncoder:
    """Fit on train hero IDs only; `transform` is pure and reproducible."""

    kind: str
    signed: bool
    hero_ids: np.ndarray
    hero_lut: np.ndarray
    synergy_lut: np.ndarray | None = None
    counter_lut: np.ndarray | None = None
    pair_min_support: int = 30

    @property
    def n_heroes(self) -> int:
        return int(len(self.hero_ids))

    @property
    def n_columns(self) -> int:
        total = self.n_heroes * 6
        if self.kind == KIND_PAIR:
            assert self.synergy_lut is not None and self.counter_lut is not None
            total += int(self.synergy_lut.max()) + 1 + int(self.counter_lut.max()) + 1
        return total

    @classmethod
    def fit(cls, heroes: np.ndarray, kind: str, signed: bool, pair_min_support: int = 30) -> "DraftFeatureEncoder":
        if kind not in (KIND_ROLE, KIND_PAIR):
            raise ValueError(f"unknown kind {kind!r}")
        if heroes.ndim != 2 or heroes.shape[1] != 10:
            raise ValueError("heroes must be (n, 10)")
        ids = np.unique(heroes.astype(np.int64))
        if ids.size == 0:
            raise ValueError("no heroes to fit on")
        lut = np.full(int(ids.max()) + 1, -1, dtype=np.int32)
        lut[ids] = np.arange(len(ids), dtype=np.int32)
        enc = cls(kind=kind, signed=signed, hero_ids=ids, hero_lut=lut, pair_min_support=pair_min_support)
        if kind == KIND_PAIR:
            dense = enc._dense(heroes)
            enc.synergy_lut = enc._pair_lut(dense, same_team=True)
            enc.counter_lut = enc._pair_lut(dense, same_team=False)
        return enc

    def _dense(self, heroes: np.ndarray) -> np.ndarray:
        h = heroes.astype(np.int64)
        inside = (h >= 0) & (h < len(self.hero_lut))
        return np.where(inside, self.hero_lut[np.clip(h, 0, len(self.hero_lut) - 1)], -1).astype(np.int32)

    def _pair_codes(self, dense: np.ndarray, same_team: bool) -> list[np.ndarray]:
        H = self.n_heroes
        codes = []
        if same_team:
            for side in (0, 5):
                for i, j in TEAM_PAIRS:
                    a, b = dense[:, side + i], dense[:, side + j]
                    codes.append(np.where((a < 0) | (b < 0), -1, np.minimum(a, b) * H + np.maximum(a, b)))
        else:
            for i in range(5):
                for j in range(5):
                    a, b = dense[:, i], dense[:, 5 + j]
                    codes.append(np.where((a < 0) | (b < 0), -1, np.minimum(a, b) * H + np.maximum(a, b)))
        return codes

    def _pair_lut(self, dense: np.ndarray, same_team: bool) -> np.ndarray:
        flat = np.concatenate(self._pair_codes(dense, same_team))
        flat = flat[flat >= 0]
        uniq, counts = np.unique(flat, return_counts=True)
        keep = uniq[counts >= self.pair_min_support]
        lut = np.full(self.n_heroes * self.n_heroes, -1, dtype=np.int32)
        lut[keep] = np.arange(len(keep), dtype=np.int32)
        if not len(keep):
            raise ValueError("no hero pair reached the support threshold")
        return lut

    def transform(self, heroes: np.ndarray) -> sp.csr_matrix:
        heroes = np.asarray(heroes)
        if heroes.ndim != 2 or heroes.shape[1] != 10:
            raise ValueError("heroes must be (n, 10)")
        dense = self._dense(heroes)
        n, H = len(dense), self.n_heroes
        sign = np.where(np.arange(10) < 5, 1.0, -1.0) if self.signed else np.ones(10)
        sign = np.broadcast_to(sign, (n, 10)).astype(np.float64)
        role = np.broadcast_to(np.tile(np.arange(5, dtype=np.int32), 2), (n, 10))
        blocks: list[tuple[np.ndarray, np.ndarray, int]] = [
            (dense.copy(), sign.copy(), H),
            (role * H + dense, sign.copy(), 5 * H),
        ]
        if self.kind == KIND_PAIR:
            assert self.synergy_lut is not None and self.counter_lut is not None
            syn_codes = self._pair_codes(dense, same_team=True)
            syn_cols = np.stack([np.where(c < 0, -1, self.synergy_lut[np.clip(c, 0, None)]) for c in syn_codes], 1)
            syn_sign = np.concatenate([
                np.full((n, len(TEAM_PAIRS)), 1.0),
                np.full((n, len(TEAM_PAIRS)), -1.0 if self.signed else 1.0),
            ], axis=1)
            blocks.append((syn_cols, syn_sign, int(self.synergy_lut.max()) + 1))
            cnt_codes = self._pair_codes(dense, same_team=False)
            cnt_cols = np.stack([np.where(c < 0, -1, self.counter_lut[np.clip(c, 0, None)]) for c in cnt_codes], 1)
            cnt_sign = np.stack([
                (np.where(dense[:, i] < dense[:, 5 + j], 1.0, -1.0) if self.signed else np.ones(n))
                for i in range(5) for j in range(5)
            ], 1)
            blocks.append((cnt_cols, cnt_sign, int(self.counter_lut.max()) + 1))

        mats = []
        for cols, vals, width in blocks:
            cols = np.asarray(cols).copy()
            vals = np.asarray(vals, dtype=np.float64).copy()
            bad = cols < 0
            if bad.any():
                cols[bad] = 0
                vals[bad] = 0.0
            mats.append(_csr(cols, vals, width))
        return sp.hstack(mats, format="csr", dtype=np.float64)


@dataclass
class KillSaturationScale:
    """Empirical CDF of train total kills: 0 = quietest map, 1 = bloodiest.

    Min-max on raw totals is dominated by the tails (train spans 6..231 kills),
    so a perfectly ordinary map lands near 0.32 and no draft can ever score
    above ~0.5.  The ECDF spends the whole [0, 1] range on the mass that
    actually occurs, so the median map sits at 0.5 and the number reads as
    "this draft's expected kill count is bloodier than N% of maps".
    """

    values: np.ndarray
    cdf: np.ndarray
    train_min: float
    train_max: float

    @classmethod
    def fit(cls, kills: np.ndarray) -> "KillSaturationScale":
        k = np.asarray(kills, dtype=np.float64)
        if k.size == 0:
            raise ValueError("no kills to fit on")
        values, counts = np.unique(k, return_counts=True)
        below = np.concatenate([[0.0], np.cumsum(counts, dtype=np.float64)[:-1]])
        cdf = (below + counts / 2.0) / float(k.size)
        return cls(values=values, cdf=cdf, train_min=float(k.min()), train_max=float(k.max()))

    def saturation(self, kills: np.ndarray) -> np.ndarray:
        return np.clip(np.interp(np.asarray(kills, dtype=np.float64), self.values, self.cdf), 0.0, 1.0)

    def kills(self, saturation: np.ndarray) -> np.ndarray:
        return np.interp(np.asarray(saturation, dtype=np.float64), self.cdf, self.values)
