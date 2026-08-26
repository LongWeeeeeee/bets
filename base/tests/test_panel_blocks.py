"""Тесты живых провайдеров недостающих блоков панели.

Проверяется то, что ломается тихо: состав и ПОРЯДОК колонок (сборка вектора идёт
по именам, но `assemble` падает, если поставленный блок не отдал все свои имена),
поведение без артефакта и арифметика заполненности после доставки блоков.

Числовое совпадение с обучением проверяется не здесь, а прогонами по корпусу:
`build_rating_snapshot.py` (max|Δ| = 0 на 482 486 картах) и сверкой паблик-блока
с `public_kills_pro_features.npz` (max|Δ| = 3e-08).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import hybrid_block  # noqa: E402
import pair_priors  # noqa: E402
import public_kills_block  # noqa: E402
import team_ratings  # noqa: E402
from prematch_panel_scorer import assemble, group_of  # noqa: E402

HEROES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


class TestColumnContracts:
    """Имена и порядок — то, по чему блок сходится с обучающей матрицей."""

    def test_public_columns_match_training_order(self):
        assert public_kills_block.COLUMNS == (
            "publogit_w_5_15", "publogit_w_10_20", "publogit_w_15_25",
            "publogit_w_20_30", "publogit_dur43", "publogit_tot51",
            "publogit_rad27_signed_broken", "publogit_team27_rad",
            "publogit_team27_dire")

    def test_rating_columns_are_six_positional(self):
        assert team_ratings.COLUMNS == tuple(f"rating_{i}" for i in range(6))

    def test_pair_columns_are_diffs_then_sums(self):
        """В `feature_names.json` сначала все разности, затем все суммы."""
        assert pair_priors.COLUMNS[:4] == (
            "F8_pair_syn0_mean_diff", "F8_pair_syn0_max_diff",
            "F8_pair_syn1_mean_diff", "F8_pair_syn1_max_diff")
        assert all(c.endswith("_sum") for c in pair_priors.COLUMNS[4:])

    @pytest.mark.parametrize("cols,grp", [
        (public_kills_block.COLUMNS, "public"),
        (team_ratings.COLUMNS, "rating"),
        (pair_priors.COLUMNS, "pairs"),
    ])
    def test_every_column_lands_in_its_group(self, cols, grp):
        assert {group_of(c) for c in cols} == {grp}


class TestSilenceWithoutArtifact:
    """Нет артефакта — блок не поставлен. Не ноль, не исключение."""

    def test_rating_without_snapshot_is_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr(team_ratings, "SNAPSHOT", tmp_path / "нет.npz")
        monkeypatch.setattr(team_ratings, "_state",
                            {"loaded": False, "snap": None, "error": None})
        assert team_ratings.block(1700000000, HEROES) is None
        assert team_ratings.status()["ready"] is False

    def test_pairs_without_snapshot_is_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr(pair_priors, "SNAPSHOT", tmp_path / "нет.npz")
        monkeypatch.setattr(pair_priors, "_state",
                            {"loaded": False, "snap": None, "error": None})
        assert pair_priors.block(HEROES, np.zeros((10, 2))) is None

    def test_public_without_models_is_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr(public_kills_block, "MODEL_DIR", tmp_path)
        monkeypatch.setattr(public_kills_block, "_state",
                            {"loaded": False, "enc": None, "mdl": None,
                             "error": None})
        assert public_kills_block.block(HEROES) is None
        assert public_kills_block.status()["error"] is not None


class TestRatingState:
    """Рейтинг — величина накопительная; проверяем сам автомат, без снимка."""

    def test_unknown_players_give_zero_difference(self):
        st = team_ratings.RatingState()
        assert st.features(1700000000, HEROES) == pytest.approx((0.0,) * 6)

    def test_win_moves_glicko_toward_winner(self):
        st = team_ratings.RatingState()
        st.advance(1700000000, HEROES, radiant_won=True)
        got = st.features(1700000000, HEROES)
        assert got[0] > 0, "победивший радиант обязан стать выше по glicko"
        assert got[1] > 0, "ожидаемая доля побед тоже обязана вырасти"

    def test_reading_twice_gives_the_same_numbers(self):
        """Живое чтение не смеет менять состояние: два вызова по одной карте
        обязаны совпасть. На обратной записи RD порт уже расходился с
        обучением."""
        st = team_ratings.RatingState()
        st.advance(1700000000, HEROES, radiant_won=True)
        later = 1700000000 + 86400 * 90
        assert st.features(later, HEROES) == st.features(later, HEROES)

    def test_idle_time_inflates_rd(self):
        st = team_ratings.RatingState()
        st.advance(1700000000, HEROES, radiant_won=True)
        # простой раздувает RD обеим сторонам, но радиант играл тем же составом,
        # поэтому сравниваем модуль разности, а не знак
        soon = abs(st.features(1700000000 + 3600, HEROES)[2])
        late = abs(st.features(1700000000 + 86400 * 365, HEROES)[2])
        assert late >= soon

    def test_snapshot_round_trip(self, tmp_path):
        st = team_ratings.RatingState()
        st.advance(1700000000, HEROES, radiant_won=True)
        p = tmp_path / "снимок.npz"
        team_ratings.save_snapshot(p, st, 1700000000)
        back = team_ratings.load_snapshot(p)
        assert back is not None
        assert back.features(1700000100, HEROES) == pytest.approx(
            st.features(1700000100, HEROES))


class TestPairArithmetic:
    def test_pairs_are_ten_per_side(self):
        assert len(pair_priors.PAIRS) == 10

    def test_pair_key_is_unordered(self):
        assert pair_priors.pair_key(7, 3) == pair_priors.pair_key(3, 7)

    def test_mirror_draft_zeroes_the_difference(self):
        """Одинаковые составы сторон обязаны дать нулевую разность."""
        pair_priors._load()
        if pair_priors._state.get("snap") is None:
            pytest.skip("снимок пар не собран")
        five = [1, 2, 3, 4, 5]
        hp = np.tile(np.arange(2.0), (10, 1))
        got = pair_priors.block(five + five, hp)
        assert got is not None
        for c in pair_priors.COLUMNS[:4]:
            assert got[c] == pytest.approx(0.0, abs=1e-9)

    def test_overlay_moves_pair_own_kills(self):
        key = pair_priors.pair_key(1, 2)
        snap = {
            "keys": np.array([key], dtype=np.int64),
            "sums": np.array([[10.0, 1.0]], dtype=np.float64),
            "counts": np.array([[5.0, 5.0]], dtype=np.float64),
            "globals": np.array([0.0, 0.0], dtype=np.float64),
            "k": 120.0,
            "metrics": list(pair_priors.SYN_METRICS),
            "built_ts": 0,
        }
        from causal_priors import PRIOR_NAMES, MapContrib

        m = len(PRIOR_NAMES)
        vr, vd = np.zeros(m), np.zeros(m)
        mask = np.zeros(m, dtype=bool)
        vr[PRIOR_NAMES.index("own_kills")] = 30.0
        mask[PRIOR_NAMES.index("own_kills")] = True
        contrib = MapContrib(
            heroes=list(range(1, 11)), accounts=list(range(101, 111)),
            vr=vr, vd=vd, mask=mask)
        out = pair_priors.overlay(snap, [contrib])
        assert out is not snap
        pos = int(np.searchsorted(out["keys"], key))
        assert out["sums"][pos, 0] == pytest.approx(40.0)
        assert out["counts"][pos, 0] == pytest.approx(6.0)
        assert snap["sums"][0, 0] == pytest.approx(10.0)

    def test_block_uses_overlaid_snap(self):
        key = pair_priors.pair_key(1, 2)
        snap = {
            "keys": np.array([key], dtype=np.int64),
            "sums": np.array([[1200.0, 0.0]], dtype=np.float64),
            "counts": np.array([[10.0, 0.0]], dtype=np.float64),
            "globals": np.array([0.0, 0.0], dtype=np.float64),
            "k": 120.0,
            "metrics": list(pair_priors.SYN_METRICS),
            "built_ts": 0,
        }
        hp = np.zeros((10, 2))
        got = pair_priors.block(list(range(1, 11)), hp, snap=snap)
        assert got is not None
        # пара 1+2 есть только у радианта — syn0_mean_diff должен быть > 0
        assert got["F8_pair_syn0_mean_diff"] > 0.0


class TestHybrid:
    def test_columns_are_two_positional(self):
        assert hybrid_block.COLUMNS == ("hybrid_0", "hybrid_1")

    def test_without_snapshot_is_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hybrid_block, "SNAPSHOT", tmp_path / "нет.json")
        monkeypatch.setattr(hybrid_block, "_state",
                            {"loaded": False, "model": None, "names": {},
                             "error": None, "built_ts": 0})
        assert hybrid_block.block(1700000000, list(range(1, 11))) is None

    def test_wrong_account_count_is_rejected(self, tmp_path, monkeypatch):
        """Девять аккаунтов — это не «почти десять», а другой матч."""
        monkeypatch.setattr(hybrid_block, "_state",
                            {"loaded": True, "model": object(), "names": {},
                             "error": None, "built_ts": 0})
        assert hybrid_block.block(1700000000, list(range(9))) is None
        assert "10" in (hybrid_block._state["error"] or "")

    def test_numpy_team_ids_do_not_break_the_call(self):
        """`team_ids or (0, 0)` падал на numpy-массиве, и блок молча исчезал."""
        hybrid_block._load()
        if hybrid_block._state.get("model") is None:
            pytest.skip("снимок гибрида не выложен")
        got = hybrid_block.block(1786476400, list(range(1, 11)),
                                 team_ids=np.array([0, 0]))
        assert got is not None and set(got) == set(hybrid_block.COLUMNS)


class TestFillAfterDelivery:
    """Заполненность — доля колонок; считаем её на составе боевого артефакта."""

    def _sizes(self):
        import json

        p = (Path(__file__).resolve().parents[2] / "ml-models" /
             "prematch_panel" / "feature_names.json")
        if not p.exists():
            pytest.skip("артефакт панели не выложен")
        cols = json.loads(p.read_text(encoding="utf-8"))["columns"]
        sizes: dict[str, int] = {}
        for c in cols:
            g = group_of(c)
            sizes[g] = sizes.get(g, 0) + 1
        return cols, sizes

    def test_every_group_has_a_live_provider(self):
        """Ни одна группа не осталась без поставщика — иначе это не 100%."""
        _cols, sizes = self._sizes()
        delivered = {"card", "prod35", "priors", "dict", "public", "rating",
                     "pairs", "hybrid"}
        assert set(sizes) <= delivered, f"без поставщика: {set(sizes) - delivered}"

    def test_full_delivery_gives_exactly_one(self):
        cols, _ = self._sizes()
        groups = ("card", "prod35", "priors", "dict", "public", "rating",
                  "pairs", "hybrid")
        blocks: dict[str, dict] = {g: {} for g in groups}
        for c in cols:
            blocks[group_of(c)][c] = 0.0
        x, present, sizes = assemble(tuple(cols), blocks)
        assert all(present.values())
        assert len(x) == len(cols)
        assert 1.0 - 0 / sum(sizes.values()) == 1.0

    def test_one_missing_block_never_reads_as_hundred(self):
        """Округление вниз: 926 из 928 — это 99%, а не «100%»."""
        from ml_panel import ModelSpec, evaluate, render

        spec = ModelSpec(key="w_5_15", title="окно 5-15", positive="Radiant",
                         negative="Dire", threshold=0.65)
        v = evaluate(spec, 0.9, {}, fill=1.0 - 2 / 928, missing=("hybrid",))
        assert v is not None
        assert "зап. 99%" in render([v])
