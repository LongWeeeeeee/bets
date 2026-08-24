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
    block_from_prod_features, draft_mask, group_of, load_bundle, score,
)

COLS = ("prod35_0", "prod35_1", "sym_0", "kwdict_a", "publogit_x",
        "F6_h_dur_mean_diff", "F7_p_dur_mean_sum")
CORE = {"prod35": {"prod35_0": 1.0, "prod35_1": 2.0}, "card": {"sym_0": 3.0}}


class TestGrouping:
    @pytest.mark.parametrize("name,grp", [
        ("F6_h_dur_mean_diff", "priors"), ("F7_p_x", "priors"),
        ("F8_pair_syn0_mean", "pairs"), ("kwdict_5_15_games", "dict"),
        ("publogit_dur43", "public"), ("prod35_7", "prod35"),
        ("sym_100", "card"), ("rating_2", "rating"), ("hybrid_1", "hybrid"),
        ("нечто_новое", "other"),
    ])
    def test_prefix_maps_to_group(self, name, grp):
        assert group_of(name) == grp


class TestAssemble:
    def test_values_land_in_column_order(self):
        blocks = {**CORE, "dict": {"kwdict_a": 4.0},
                  "public": {"publogit_x": 5.0},
                  "priors": {"F6_h_dur_mean_diff": 6.0, "F7_p_dur_mean_sum": 7.0}}
        x, present, _ = assemble(COLS, blocks)
        assert list(x) == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
        assert all(present.values())

    def test_missing_block_marked_and_filled_neutral(self):
        blocks = {**CORE, "dict": {"kwdict_a": 4.0},
                  "public": {"publogit_x": 5.0}, "priors": None}
        x, present, _ = assemble(COLS, blocks)
        assert present["priors"] is False
        assert x[5] == NEUTRAL and x[6] == NEUTRAL
        assert x[0] == 1.0

    def test_partial_block_is_an_error_not_silence(self):
        """Пропуск колонки внутри поставленного блока = рассинхрон имён."""
        blocks = {"prod35": {"prod35_0": 1.0}, "card": {"sym_0": 3.0},
                  "dict": {"kwdict_a": 4.0}, "public": {"publogit_x": 5.0},
                  "priors": {"F6_h_dur_mean_diff": 6.0,
                             "F7_p_dur_mean_sum": 7.0}}
        with pytest.raises(KeyError):
            assemble(COLS, blocks)

    def test_order_follows_columns_not_dict(self):
        cols = ("sym_0", "prod35_0")
        x, _, _ = assemble(cols, {"prod35": {"prod35_0": 9.0},
                                  "card": {"sym_0": 1.0}})
        assert list(x) == [1.0, 9.0]


class TestBlockHelpers:
    def test_prod_features_follow_artifact_order(self):
        feats = {"elo": 5.0, "draft_logit": 1.0, "form": 3.0}
        order = ["draft_logit", "elo", "form"]
        got = block_from_prod_features(feats, order)
        assert got == {"prod35_0": 1.0, "prod35_1": 5.0, "prod35_2": 3.0}

    def test_absent_feature_gets_column_neutral(self):
        # Ветка лестницы отдаёт ПОДМНОЖЕСТВО признаков полной модели: на
        # `no_org` нет `h2h_resid`, на `no_account` — признаков игроков, а
        # `order` всегда полный. 24.08.2026 такой промах считали рассинхроном и
        # роняли расчёт — панель молчала на каждой карте без h2h (боевая ветка
        # была ровно `no_org`), и по логу это было неотличимо от «панели нет в
        # сборке».
        #
        # Пропущенный слот берёт нейтраль КОЛОНКИ из артефакта. Ноль негоден
        # как общее правило: он нейтрален для `h2h_resid`, но не для `elo`.
        feats = {"draft_logit": 1.0, "elo": 5.0}
        got = block_from_prod_features(feats, ["draft_logit", "elo", "h2h_resid"],
                                       neutral={"prod35_2": 0.25})
        assert got == {"prod35_0": 1.0, "prod35_1": 5.0, "prod35_2": 0.25}

    def test_absent_without_neutral_falls_back_to_zero(self):
        # Нейтрали для колонки нет — общий NEUTRAL, а не отказ.
        got = block_from_prod_features({"elo": 5.0}, ["elo", "h2h_resid"])
        assert got == {"prod35_0": 5.0, "prod35_1": 0.0}

    def test_nothing_matched_raises(self):
        # Не совпало НИ ОДНО имя — это чужой порядок, а не короткая ветка.
        with pytest.raises(KeyError, match="ни одного"):
            block_from_prod_features({"elo": 1.0}, ["a", "b"])

    def test_full_branch_still_builds_block(self):
        # Полная ветка отдаёт все имена — блок собирается как раньше.
        feats = {"draft_logit": 1.0, "elo": 5.0, "h2h_resid": -0.5}
        assert block_from_prod_features(
            feats, ["draft_logit", "elo", "h2h_resid"]) == {
                "prod35_0": 1.0, "prod35_1": 5.0, "prod35_2": -0.5}

    def test_order_mismatch_with_artifact_raises(self):
        # Отпечаток из артефакта панели: на чём её учили. Смена ДЛИНЫ ловилась и
        # раньше (сборка по именам не досчиталась бы колонки), а перестановка при
        # той же длине — нет, и значения ложились не в свои слоты.
        feats = {"draft_logit": 1.0, "elo": 5.0, "form": 3.0}
        trained_on = ["draft_logit", "elo", "form"]
        with pytest.raises(ValueError, match="слот 1"):
            block_from_prod_features(feats, ["draft_logit", "form", "elo"],
                                     expected=trained_on)
        with pytest.raises(ValueError, match="разных поколений"):
            block_from_prod_features(feats, ["draft_logit", "elo"],
                                     expected=trained_on)

    def test_matching_order_passes(self):
        feats = {"draft_logit": 1.0, "elo": 5.0}
        order = ["draft_logit", "elo"]
        assert block_from_prod_features(feats, order, expected=order) == {
            "prod35_0": 1.0, "prod35_1": 5.0}

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
        b = {"prod35": {"prod35_0": 0.0, "prod35_1": 0.0},
             "card": {"sym_0": 0.0},
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

    def test_missing_priors_lower_fill_by_column_share(self):
        """Заполненность — доля КОЛОНОК: из семи пропали две приорных."""
        b = PanelBundle((spec("a"),), COLS, {"a": FakeModel(p=0.95)})
        v = score(b, self._blocks(priors=False))[0]
        assert v.fill == pytest.approx(5 / 7)
        assert v.ok is False

    def test_small_missing_block_barely_moves_fill(self):
        """Блок из одной колонки не должен гасить вердикт как треть входа."""
        cols = tuple(f"sym_{i}" for i in range(99)) + ("rating_0",)
        blocks = {"card": {f"sym_{i}": 0.0 for i in range(99)}, "rating": None}

        class Wide(FakeModel):
            pass

        b = PanelBundle((spec("a", 0.55),), cols, {"a": Wide(p=0.9)})
        v = score(b, blocks, with_draft=False)[0]
        assert v.fill == pytest.approx(0.99)
        assert v.ok is True

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


class TestDraftMask:
    def test_hero_derived_blocks_are_draft(self):
        m = draft_mask(("sym_0", "publogit_x", "kwdict_a", "F6_h_dur_mean_diff",
                        "F8_pair_syn0_mean"))
        assert m.all()

    def test_player_and_team_blocks_are_not_draft(self):
        """F7 — приоры ИГРОКА, rating и hybrid — сила команд, это не пик."""
        m = draft_mask(("F7_p_dur_mean_sum", "rating_0", "hybrid_1"))
        assert not m.any()

    def test_prod35_split_by_name_order(self):
        cols = ("prod35_0", "prod35_1", "prod35_2")
        m = draft_mask(cols, prod35_names=["draft_logit", "elo", "cp_lane"])
        assert list(m) == [True, False, True]

    def test_prod35_without_order_counts_as_not_draft(self):
        assert not draft_mask(("prod35_0",)).any()

    def test_length_matches_columns(self):
        cols = ("sym_0", "rating_1", "F6_x", "prod35_0")
        assert len(draft_mask(cols, ["draft_logit"])) == len(cols)


class TestNeutralByGroup:
    def test_missing_dict_becomes_nan_not_zero(self):
        """Офлайн писал NaN там, где словарь молчит; ноль модель не видела."""
        cols = ("sym_0", "kwdict_5_15_games")
        x, present, _ = assemble(cols, {"card": {"sym_0": 1.0}, "dict": None})
        assert x[0] == 1.0
        assert np.isnan(x[1])
        assert present["dict"] is False

    def test_missing_priors_stay_zero(self):
        """У приоров офлайн NaN не бывало — там нейтраль остаётся нулём."""
        cols = ("F6_x",)
        x, _, _ = assemble(cols, {"priors": None})
        assert x[0] == 0.0


class TestArtifactFingerprints:
    """Отпечатки входа. Оба уже существовали как ЗНАЧЕНИЯ и ни разу не
    сличались: `card_fingerprint` писался в манифест и в `status()`, а состав
    колонок не проверялся вовсе — при загрузке сравнивалась только ширина.
    Модели панели позиционные (`feature_names_` у `.cbm` это `['0'..'927']`),
    поэтому перестановка колонок при той же длине шла бы молча."""

    def _artifact(self, tmp_path, columns, manifest):
        import json
        (tmp_path / "feature_names.json").write_text(
            json.dumps({"columns": list(columns)}), encoding="utf-8")
        (tmp_path / "manifest.json").write_text(
            json.dumps(manifest), encoding="utf-8")
        return tmp_path

    def test_columns_sha_is_order_sensitive(self):
        from hero_side_tables import columns_sha
        assert columns_sha(["a", "b"]) == columns_sha(["a", "b"])
        assert columns_sha(["a", "b"]) != columns_sha(["b", "a"])

    def test_reordered_columns_are_rejected(self, tmp_path, monkeypatch):
        from hero_side_tables import columns_sha
        import prematch_panel_scorer as S
        cols = ["sym_0", "sym_1", "sym_2"]
        d = self._artifact(tmp_path, ["sym_0", "sym_2", "sym_1"],
                           {"columns_sha": columns_sha(cols)})
        monkeypatch.setattr(S, "load_specs", lambda _d: (), raising=False)
        with pytest.raises(ValueError, match="состав колонок"):
            S.load_bundle(d)

    def test_missing_fingerprint_is_not_an_error(self, tmp_path, monkeypatch):
        # Артефакт мог быть собран до появления отпечатка — это не поломка.
        import prematch_panel_scorer as S
        d = self._artifact(tmp_path, ["sym_0"], {"columns": 1})
        monkeypatch.setattr(S, "load_specs", lambda _d: (), raising=False)
        S.load_bundle(d)   # не должно бросить
