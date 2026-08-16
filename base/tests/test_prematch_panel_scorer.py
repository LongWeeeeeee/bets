"""Тесты живого скорера панели: сборка по именам и честная заполненность.

Проверяется то, что ломается тихо: порядок колонок, непоставленный блок,
рассинхрон имён внутри поставленного блока и поведение без артефакта.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from prematch_panel_scorer import (  # noqa: E402
    NEUTRAL, PanelBundle, assemble, block_from_matrix,
    block_from_prod_features, group_of, load_bundle, score,
)

COLS = ("prod35_0", "prod35_1", "sym_0", "kwdict_a", "publogit_x",
        "F6_h_dur_mean_diff", "F7_p_dur_mean_sum")


class TestGrouping:
    @pytest.mark.parametrize("name,grp", [
        ("F6_h_dur_mean_diff", "priors"), ("F7_p_x", "priors"),
        ("F8_pair_syn0_mean", "priors"), ("kwdict_5_15_games", "dict"),
        ("publogit_dur43", "public"), ("prod35_7", "core"),
        ("sym_100", "core"), ("rating_2", "rating"), ("hybrid_1", "core"),
        ("нечто_новое", "core"),
    ])
    def test_prefix_maps_to_group(self, name, grp):
        assert group_of(name) == grp


class TestAssemble:
    def test_values_land_in_column_order(self):
        blocks = {"core": {"prod35_0": 1.0, "prod35_1": 2.0, "sym_0": 3.0},
                  "dict": {"kwdict_a": 4.0}, "public": {"publogit_x": 5.0},
                  "priors": {"F6_h_dur_mean_diff": 6.0, "F7_p_dur_mean_sum": 7.0}}
        x, present = assemble(COLS, blocks)
        assert list(x) == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
        assert all(present.values())

    def test_missing_block_marked_and_filled_neutral(self):
        blocks = {"core": {"prod35_0": 1.0, "prod35_1": 2.0, "sym_0": 3.0},
                  "dict": {"kwdict_a": 4.0}, "public": {"publogit_x": 5.0},
                  "priors": None}
        x, present = assemble(COLS, blocks)
        assert present["priors"] is False
        assert x[5] == NEUTRAL and x[6] == NEUTRAL
        assert x[0] == 1.0

    def test_partial_block_is_an_error_not_silence(self):
        """Пропуск колонки внутри поставленного блока = рассинхрон имён."""
        blocks = {"core": {"prod35_0": 1.0}, "dict": {"kwdict_a": 4.0},
                  "public": {"publogit_x": 5.0},
                  "priors": {"F6_h_dur_mean_diff": 6.0,
                             "F7_p_dur_mean_sum": 7.0}}
        with pytest.raises(KeyError):
            assemble(COLS, blocks)

    def test_order_follows_columns_not_dict(self):
        cols = ("sym_0", "prod35_0")
        x, _ = assemble(cols, {"core": {"prod35_0": 9.0, "sym_0": 1.0}})
        assert list(x) == [1.0, 9.0]


class TestBlockHelpers:
    def test_prod_features_follow_artifact_order(self):
        feats = {"elo": 5.0, "draft_logit": 1.0, "form": 3.0}
        order = ["draft_logit", "elo", "form"]
        got = block_from_prod_features(feats, order)
        assert got == {"prod35_0": 1.0, "prod35_1": 5.0, "prod35_2": 3.0}

    def test_absent_feature_becomes_zero(self):
        got = block_from_prod_features({}, ["a", "b"])
        assert got == {"prod35_0": 0.0, "prod35_1": 0.0}

    def test_matrix_block_is_positional(self):
        got = block_from_matrix("sym_", np.array([[1.5, 2.5]]))
        assert got == {"sym_0": 1.5, "sym_1": 2.5}


class FakeModel:
    n_features_in_ = len(COLS)

    def __init__(self, p=0.8):
        self.p = p
        self.seen = None

    def predict_proba(self, x):
        self.seen = np.asarray(x)
        return np.array([[1 - self.p, self.p]])


def spec(key="w_5_15", threshold=0.60):
    from ml_panel import ModelSpec
    return ModelSpec(key=key, title="окно", positive="Radiant", negative="Dire",
                     threshold=threshold, groups=("core", "dict", "priors"))


class TestScore:
    def _blocks(self, priors=True):
        b = {"core": {"prod35_0": 0.0, "prod35_1": 0.0, "sym_0": 0.0},
             "dict": {"kwdict_a": 0.0}, "public": {"publogit_x": 0.0},
             "priors": {"F6_h_dur_mean_diff": 0.0, "F7_p_dur_mean_sum": 0.0}}
        if not priors:
            b["priors"] = None
        return b

    def test_returns_verdict_per_model(self):
        b = PanelBundle((spec("a"), spec("b")), COLS,
                        {"a": FakeModel(), "b": FakeModel()})
        out = score(b, self._blocks())
        assert [v.key for v in out] == ["a", "b"]
        assert out[0].fill == pytest.approx(1.0)

    def test_missing_priors_lower_fill_and_block_verdict(self):
        b = PanelBundle((spec("a"),), COLS, {"a": FakeModel(p=0.95)})
        v = score(b, self._blocks(priors=False))[0]
        assert v.fill == pytest.approx(2 / 3)
        assert v.ok is False

    def test_vector_passed_to_model_has_full_width(self):
        m = FakeModel()
        score(PanelBundle((spec("a"),), COLS, {"a": m}), self._blocks())
        assert m.seen.shape == (1, len(COLS))

    def test_empty_bundle_is_silent(self):
        assert score(PanelBundle((), (), {}), self._blocks()) == []

    def test_model_without_file_skipped(self):
        b = PanelBundle((spec("a"), spec("b")), COLS, {"a": FakeModel()})
        assert [v.key for v in score(b, self._blocks())] == ["a"]


class TestLoadBundle:
    def test_missing_directory_is_silent(self, tmp_path):
        got = load_bundle(tmp_path / "nope")
        assert got.ready is False
        assert score(got, {}) == []

    def test_specs_without_columns_is_silent(self, tmp_path):
        from ml_panel import atomic_write_specs
        atomic_write_specs([spec()], tmp_path)
        assert load_bundle(tmp_path).ready is False

    def test_broken_columns_file_is_silent(self, tmp_path):
        from ml_panel import atomic_write_specs
        atomic_write_specs([spec()], tmp_path)
        (tmp_path / "feature_names.json").write_text("{", encoding="utf-8")
        assert load_bundle(tmp_path).ready is False
