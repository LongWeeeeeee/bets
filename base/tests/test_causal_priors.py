"""Тесты причинных приоров: снимок, шринкедж, агрегация, антисимметрия.

Приоры — единственная часть панели, которой нужно накопленное состояние, и
единственная, которая по замерам окупается. Ломается она тихо: неизвестный
ключ, разъехавшийся порядок метрик или потерянный шринкедж не дают исключения,
а дают другое число. Поэтому проверяется именно это.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from causal_priors import (  # noqa: E402
    K_HERO, K_PLAYER, PRIOR_NAMES, PriorSnapshot, aggregate_side, load_snapshot,
    save_snapshot, sym_priors,
)

M = len(PRIOR_NAMES)


def snap(hero=None, player=None, g=None):
    hero = hero or {}
    player = player or {}
    ks = np.array(sorted(hero), dtype=np.int64)
    ps = np.array(sorted(player), dtype=np.int64)
    return PriorSnapshot(
        metrics=PRIOR_NAMES,
        hero_keys=ks,
        hero_sums=np.stack([hero[int(k)][0] for k in ks]) if len(ks) else np.zeros((0, M)),
        hero_counts=np.stack([hero[int(k)][1] for k in ks]) if len(ks) else np.zeros((0, M)),
        player_keys=ps,
        player_sums=np.stack([player[int(k)][0] for k in ps]) if len(ps) else np.zeros((0, M)),
        player_counts=np.stack([player[int(k)][1] for k in ps]) if len(ps) else np.zeros((0, M)),
        globals_=np.full(M, 1.0) if g is None else g)


def entry(value, count):
    return (np.full(M, float(value)), np.full(M, float(count)))


class TestShrinkage:
    def test_unknown_key_falls_back_to_global(self):
        s = snap(g=np.full(M, 7.0))
        assert s.hero_priors([1, 2])[0, 0] == pytest.approx(7.0)

    def test_huge_count_approaches_own_mean(self):
        """Ключ с миллионом карт почти не тянется к глобальному."""
        s = snap(hero={5: entry(2_000_000.0, 1_000_000.0)}, g=np.full(M, 1.0))
        assert s.hero_priors([5])[0, 0] == pytest.approx(2.0, abs=1e-3)

    def test_single_game_is_pulled_to_global(self):
        """Одна карта не должна давать своё среднее — иначе редкая ячейка шумит."""
        s = snap(hero={5: entry(100.0, 1.0)}, g=np.full(M, 1.0))
        v = s.hero_priors([5])[0, 0]
        assert v == pytest.approx((100.0 + K_HERO) / (1.0 + K_HERO))
        assert v < 2.0

    def test_player_uses_its_own_constant(self):
        s = snap(player={9: entry(50.0, 10.0)}, g=np.zeros(M))
        assert s.player_priors([9])[0, 0] == pytest.approx(50.0 / (10.0 + K_PLAYER))

    def test_coverage_reports_found_share(self):
        s = snap(hero={1: entry(1, 1), 2: entry(1, 1)}, player={7: entry(1, 1)})
        cov = s.coverage([1, 2, 3, 4], [7, 8])
        assert cov["hero"] == pytest.approx(0.5)
        assert cov["player"] == pytest.approx(0.5)


class TestAggregate:
    def test_column_count_matches_names(self):
        names: list[str] = []
        out = aggregate_side(np.zeros((3, 5, M)), np.zeros((3, 5, M)), names)
        assert out.shape[1] == len(names)

    def test_mean_is_mean_over_five_slots(self):
        H = np.zeros((1, 5, M))
        H[0, :, 0] = [1.0, 2.0, 3.0, 4.0, 5.0]
        names: list[str] = []
        out = aggregate_side(H, np.zeros((1, 5, M)), names)
        assert out[0, names.index("F6_h_own_kills_mean")] == pytest.approx(3.0)

    def test_wide_metric_gets_std_and_max(self):
        names: list[str] = []
        aggregate_side(np.zeros((1, 5, M)), np.zeros((1, 5, M)), names)
        assert "F6_h_own_kills_std" in names and "F6_h_own_kills_max" in names

    def test_narrow_metric_has_mean_only(self):
        names: list[str] = []
        aggregate_side(np.zeros((1, 5, M)), np.zeros((1, 5, M)), names)
        assert "F6_h_enemy_kills_mean" in names
        assert "F6_h_enemy_kills_std" not in names

    def test_player_block_limited_to_keep_list(self):
        names: list[str] = []
        aggregate_side(np.zeros((1, 5, M)), np.zeros((1, 5, M)), names)
        assert "F7_p_own_kills_mean" in names
        assert "F7_p_enemy_kills_mean" not in names

    def test_wrong_shape_rejected(self):
        with pytest.raises(ValueError):
            aggregate_side(np.zeros((2, 4, M)), np.zeros((2, 4, M)))


class TestSymPriors:
    def test_swapping_sides_negates_diff_and_keeps_sum(self):
        s = snap(hero={h: entry(h, 10.0) for h in range(1, 11)},
                 player={a: entry(a, 10.0) for a in range(101, 111)})
        her = np.arange(1, 11)[None, :]
        acc = np.arange(101, 111)[None, :]
        mirror = [5, 6, 7, 8, 9, 0, 1, 2, 3, 4]
        a = sym_priors(her, acc, s)
        b = sym_priors(her[:, mirror], acc[:, mirror], s)
        half = a.shape[1] // 2
        assert np.allclose(a[:, :half], -b[:, :half], atol=1e-5)
        assert np.allclose(a[:, half:], b[:, half:], atol=1e-5)

    def test_identical_sides_give_zero_diff(self):
        s = snap(hero={h: entry(h, 10.0) for h in (1, 2)},
                 player={a: entry(a, 10.0) for a in (101, 102)})
        her = np.array([[1, 2, 1, 2, 1, 1, 2, 1, 2, 1]])
        acc = np.array([[101, 102, 101, 102, 101, 101, 102, 101, 102, 101]])
        out = sym_priors(her, acc, s)
        assert np.allclose(out[:, :out.shape[1] // 2], 0.0, atol=1e-6)

    def test_names_cover_both_halves(self):
        s = snap()
        names: list[str] = []
        out = sym_priors(np.ones((1, 10), int), np.ones((1, 10), int), s, names)
        assert len(names) == out.shape[1]
        assert names[0].endswith("_diff") and names[-1].endswith("_sum")

    def test_mismatched_shapes_rejected(self):
        with pytest.raises(ValueError):
            sym_priors(np.ones((1, 10), int), np.ones((1, 9), int), snap())


class TestSnapshotIO:
    def test_roundtrip_preserves_values(self, tmp_path):
        p = tmp_path / "snap.npz"
        save_snapshot(p, metrics=PRIOR_NAMES,
                      hero={3: entry(30.0, 6.0)}, player={77: entry(7.0, 2.0)},
                      globals_=np.full(M, 0.5), built_ts=123)
        back = load_snapshot(p)
        assert back is not None
        assert back.built_ts == 123
        assert back.hero_priors([3])[0, 0] == pytest.approx(
            (30.0 + K_HERO * 0.5) / (6.0 + K_HERO))

    def test_keys_are_sorted_for_search(self, tmp_path):
        p = tmp_path / "snap.npz"
        save_snapshot(p, metrics=PRIOR_NAMES,
                      hero={9: entry(1, 1), 2: entry(1, 1), 5: entry(1, 1)},
                      player={}, globals_=np.zeros(M), built_ts=0)
        back = load_snapshot(p)
        assert list(back.hero_keys) == [2, 5, 9]

    def test_missing_file_returns_none(self, tmp_path):
        assert load_snapshot(tmp_path / "nope.npz") is None

    def test_empty_tables_do_not_crash(self, tmp_path):
        p = tmp_path / "snap.npz"
        save_snapshot(p, metrics=PRIOR_NAMES, hero={}, player={},
                      globals_=np.full(M, 3.0), built_ts=0)
        back = load_snapshot(p)
        assert back.hero_priors([1, 2, 3])[0, 0] == pytest.approx(3.0)
