"""Величины одной карты для приоров — те же, что офлайн `prior_values`.

Живой путь не имеет корпуса, но формула обязана совпасть: иначе дозапись
после среза снимка сдвинет приор в другую сторону, чем ночная пересборка.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from causal_priors import PRIOR_NAMES  # noqa: E402
from prior_map_metrics import MINUTES, map_metrics, pad_series  # noqa: E402

M = len(PRIOR_NAMES)
IDX = {n: i for i, n in enumerate(PRIOR_NAMES)}


def _vec(**kwargs):
    rk = kwargs.get("rk_inc")
    dk = kwargs.get("dk_inc")
    return map_metrics(
        rad_kills=kwargs.get("rad_kills", [6, 6, 6, 6, 6]),
        dire_kills=kwargs.get("dire_kills", [4, 4, 4, 4, 4]),
        duration_seconds=kwargs.get("duration_seconds", 40 * 60),
        radiant_won=kwargs.get("radiant_won", True),
        rk_inc=rk, dk_inc=dk,
        nw=kwargs.get("nw"), xp=kwargs.get("xp"),
    )


class TestTotalsWithoutSeries:
    def test_own_kills_come_from_player_rows_not_the_minute_row(self):
        vr, vd, mask = _vec()
        assert vr[IDX["own_kills"]] == pytest.approx(30.0)
        assert vd[IDX["own_kills"]] == pytest.approx(20.0)
        assert vr[IDX["enemy_kills"]] == pytest.approx(20.0)
        assert vr[IDX["kill_diff"]] == pytest.approx(10.0)
        assert vr[IDX["tot_kills"]] == pytest.approx(50.0)
        assert mask[IDX["own_kills"]]

    def test_winrate_and_duration_flags(self):
        vr, vd, mask = _vec(radiant_won=True, duration_seconds=36 * 60)
        assert vr[IDX["winrate"]] == pytest.approx(1.0)
        assert vd[IDX["winrate"]] == pytest.approx(0.0)
        assert vr[IDX["dur"]] == pytest.approx(36.0)
        assert vr[IDX["p_dur32"]] == pytest.approx(1.0)
        assert vr[IDX["p_dur36"]] == pytest.approx(1.0)
        assert vr[IDX["p_dur40"]] == pytest.approx(0.0)
        assert mask[IDX["winrate"]] and mask[IDX["dur"]]

    def test_twenty_seven_plus_is_per_side(self):
        vr, vd, _ = _vec(rad_kills=[8] * 5, dire_kills=[2] * 5)
        assert vr[IDX["p_own27"]] == pytest.approx(1.0)
        assert vd[IDX["p_own27"]] == pytest.approx(0.0)
        assert vr[IDX["tot_kills"]] == pytest.approx(50.0)
        assert vr[IDX["p_tot54"]] == pytest.approx(0.0)

    def test_windows_stay_invalid_without_the_minute_row(self):
        vr, vd, mask = _vec(rk_inc=None, dk_inc=None)
        for name in ("k_0_10", "k_10_20", "win_10_20", "nwd_10", "xpd_10"):
            assert not mask[IDX[name]], name
            assert vr[IDX[name]] == pytest.approx(0.0)


class TestWindowsFromIncrements:
    def test_pad_matches_corpus_cumsum_and_hold_last(self):
        got = pad_series([1, 2, 3], MINUTES, cum=True)
        assert got[0] == 1 and got[1] == 3 and got[2] == 6
        assert got[3] == 6 and got[-1] == 6

    def test_window_kills_use_cumulative_difference(self):
        # 1 кил в каждую из первых 40 минут у радианта, у дайра тишина.
        rk = [1] * 40
        dk = [0] * 40
        vr, vd, mask = _vec(rk_inc=rk, dk_inc=dk, duration_seconds=40 * 60)
        assert mask[IDX["k_0_10"]] and mask[IDX["k_10_20"]]
        # pad+cumsum, затем rk[b]-rk[a] — как catalog_features.prior_values.
        cum = np.cumsum(rk)
        padded = np.zeros(MINUTES, dtype=np.int32)
        padded[:40] = cum
        padded[40:] = cum[-1]
        assert vr[IDX["k_0_10"]] == pytest.approx(float(padded[10] - padded[0]))
        assert vr[IDX["k_10_20"]] == pytest.approx(float(padded[20] - padded[10]))
        assert vd[IDX["k_0_10"]] == pytest.approx(0.0)

    def test_win_window_is_who_got_more_kills(self):
        rk = [2] * 20 + [0] * 20
        dk = [1] * 20 + [0] * 20
        vr, vd, mask = _vec(rk_inc=rk, dk_inc=dk, duration_seconds=40 * 60)
        assert mask[IDX["win_10_20"]]
        assert vr[IDX["win_10_20"]] == pytest.approx(1.0)
        assert vd[IDX["win_10_20"]] == pytest.approx(0.0)

    def test_short_map_does_not_count_a_window_it_never_reached(self):
        rk = [1] * 12
        dk = [0] * 12
        vr, vd, mask = _vec(rk_inc=rk, dk_inc=dk, duration_seconds=12 * 60)
        assert mask[IDX["k_0_10"]]
        assert not mask[IDX["k_20_30"]]
        assert not mask[IDX["win_20_30"]]

    def test_networth_and_xp_leads_are_not_cumsumed(self):
        nw = [0] * 10 + [500] + [0] * 30
        xp = [0] * 10 + [-200] + [0] * 30
        vr, vd, mask = _vec(
            rk_inc=[1] * 40, dk_inc=[0] * 40,
            nw=nw, xp=xp, duration_seconds=40 * 60)
        assert mask[IDX["nwd_10"]] and mask[IDX["xpd_10"]]
        assert vr[IDX["nwd_10"]] == pytest.approx(500.0)
        assert vd[IDX["nwd_10"]] == pytest.approx(-500.0)
        assert vr[IDX["xpd_10"]] == pytest.approx(-200.0)


class TestMatchesOfflineBatch:
    def test_one_map_equals_catalog_prior_values(self):
        misc = Path(__file__).resolve().parents[2] / "runtime/experiments/misc"
        sys.path.insert(0, str(misc))
        try:
            from catalog_features import prior_values
        except ImportError:
            pytest.skip("офлайн-сборщик недоступен")
        rad_k = np.array([8, 7, 6, 5, 4], dtype=np.float32)
        dire_k = np.array([3, 3, 3, 3, 3], dtype=np.float32)
        pstats = np.zeros((1, 10, 1), dtype=np.float32)
        pstats[0, :5, 0] = rad_k
        pstats[0, 5:, 0] = dire_k
        rk_inc = ([1] * 10 + [2] * 10 + [0] * 20)
        dk_inc = ([0] * 10 + [1] * 10 + [0] * 20)
        rk = pad_series(rk_inc, MINUTES, cum=True)[None, :]
        dk = pad_series(dk_inc, MINUTES, cum=True)[None, :]
        nw = pad_series([10 * i for i in range(41)], MINUTES, cum=False)[None, :]
        xp = pad_series([5 * i for i in range(41)], MINUTES, cum=False)[None, :]
        zr = {
            "pstats": np.concatenate(
                [pstats, np.zeros((1, 10, 13), dtype=np.float32)], axis=2),
            "durations": np.array([38 * 60], dtype=np.int32),
            "rk": rk.astype(np.float32), "dk": dk.astype(np.float32),
            "nw": nw.astype(np.float32), "xp": xp.astype(np.float32),
            "wins": np.array([1.0], dtype=np.float32),
        }
        Vr, Vd, Mask = prior_values(zr)
        vr, vd, mask = map_metrics(
            rad_kills=list(rad_k), dire_kills=list(dire_k),
            duration_seconds=38 * 60, radiant_won=True,
            rk_inc=rk_inc, dk_inc=dk_inc,
            nw=list(nw[0]), xp=list(xp[0]))
        assert Vr.shape[1] == len(PRIOR_NAMES)
        assert np.allclose(Vr[0], vr, atol=1e-5)
        assert np.allclose(Vd[0], vd, atol=1e-5)
        assert np.array_equal(Mask[0].astype(bool), np.asarray(mask, dtype=bool))
