"""Разложение вердикта панели по блокам ELO / драфт / игроки.

Число печатается в живом сообщении рядом с такой же строкой предматчевой
модели, поэтому проверяется не «что-то посчиталось», а совпадение соглашений:
одинаковая разметка блоков, одинаковый знаменатель, одинаковая ориентация знака.
Разойдись любое из трёх — и в одном сообщении окажутся два числа, которые
называются одинаково и значат разное.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import prematch_panel_scorer as P  # noqa: E402

#: Порядок боевых 35 колонок — хватает первых семи, дальше идут игроцкие.
ORDER = ["draft_logit", "elo", "games", "hero_games", "pos_games", "opp_elo",
         "hero_pool"]


def test_prefixes_land_in_the_right_block():
    assert P.block_of("sym_123", ORDER) == "драфт"
    assert P.block_of("publogit_2", ORDER) == "драфт"
    assert P.block_of("kwdict_5", ORDER) == "драфт"
    assert P.block_of("F6_9", ORDER) == "драфт"     # приоры ГЕРОЯ
    assert P.block_of("F8_2", ORDER) == "драфт"     # пары героев
    assert P.block_of("rating_0", ORDER) == "ELO"
    assert P.block_of("hybrid_1", ORDER) == "ELO"
    assert P.block_of("F7_3", ORDER) == "игроки"    # приоры ИГРОКА


def test_prod35_slots_follow_their_names():
    assert P.block_of("prod35_0", ORDER) == "драфт"   # draft_logit
    assert P.block_of("prod35_1", ORDER) == "ELO"     # elo
    assert P.block_of("prod35_5", ORDER) == "ELO"     # opp_elo
    assert P.block_of("prod35_2", ORDER) == "игроки"  # games


def test_hero_pool_is_a_player_feature_not_a_draft_one():
    """Размер пула героев — свойство ИГРОКА (E-214 §1, кластер карьеры).

    В предматчевой линии он размечен игроцким. Пока панель печатала только свою
    долю драфта, расхождение было незаметно; теперь оба числа стоят в одном
    сообщении и обязаны считать драфт одинаково.
    """
    assert P.block_of("prod35_6", ORDER) == "игроки"
    assert "hero_pool" not in P.DRAFT_PROD35


def test_unknown_columns_do_not_land_in_draft_or_elo():
    """Незнакомое имя не должно молча раздувать драфт: это дало бы ложную долю."""
    assert P.block_of("что_то_новое", ORDER) == "игроки"
    assert P.block_of("prod35_999", ORDER) == "игроки"
    assert P.block_of("prod35_x", ORDER) == "игроки"


class _FakeModel:
    """Модель с заданными вкладами: SHAP подменён, проверяется арифметика."""

    def __init__(self, values):
        self._v = np.asarray(values, dtype=float)

    def get_feature_importance(self, pool, type=None):   # noqa: A002
        return np.asarray([list(self._v) + [0.0]])       # +базовое значение


COLS = ["sym_0", "rating_0", "F7_0"]                     # драфт, ELO, игроки


def test_shares_are_normalised_over_blocks_not_columns():
    """Знаменатель — сумма модулей АГРЕГАТОВ, как в строке предматчевой модели.

    По колонкам доли не складывались бы: 858 драфтовых колонок гасят друг друга,
    а их шум остаётся в знаменателе, и сумма выходила по 21-41%.
    """
    m = _FakeModel([0.6, -0.2, 0.2])
    p = P.group_shares(m, np.zeros((1, 3)), COLS)
    assert sum(abs(v) for v in p.values()) == 1.0
    assert p["драфт"] == 0.6
    assert p["ELO"] == -0.2
    assert p["игроки"] == 0.2


def test_columns_of_one_block_are_summed_with_their_signs():
    """Внутри блока вклады складываются знаково: два довода врозь гасятся."""
    cols = ["sym_0", "sym_1", "rating_0"]
    m = _FakeModel([0.5, -0.3, 0.2])
    p = P.group_shares(m, np.zeros((1, 3)), cols)
    assert abs(p["драфт"] - 0.5) < 1e-12       # 0.2 из 0.4 суммарных модулей
    assert abs(p["ELO"] - 0.5) < 1e-12


def test_flip_turns_the_sign_toward_the_verdict():
    """У целей без сторон («да/нет») знак разворачивается к вердикту."""
    m = _FakeModel([0.6, -0.2, 0.2])
    straight = P.group_shares(m, np.zeros((1, 3)), COLS, flip=False)
    flipped = P.group_shares(m, np.zeros((1, 3)), COLS, flip=True)
    for k in straight:
        assert flipped[k] == -straight[k]


def test_precomputed_values_are_reused():
    """SHAP стоит ~90 мс: посчитанный вектор обязан приниматься извне."""
    m = _FakeModel([0.6, -0.2, 0.2])
    given = np.array([0.0, 1.0, 0.0])
    p = P.group_shares(m, np.zeros((1, 3)), COLS, values=given)
    assert p == {"драфт": 0.0, "ELO": 1.0, "игроки": 0.0}


def test_zero_contributions_give_no_shares_instead_of_a_division_by_zero():
    m = _FakeModel([0.0, 0.0, 0.0])
    assert P.group_shares(m, np.zeros((1, 3)), COLS) == {}


def test_a_model_that_cannot_explain_itself_returns_nothing():
    class _Broken:
        def get_feature_importance(self, pool, type=None):   # noqa: A002
            raise RuntimeError("нет SHAP")

    assert P.shap_values(_Broken(), np.zeros((1, 3)), 3) is None
    assert P.group_shares(_Broken(), np.zeros((1, 3)), COLS) == {}


def test_wrong_length_is_rejected_rather_than_misaligned():
    """Вектор не той длины означает рассинхрон колонок — молча сдвигать нельзя."""
    m = _FakeModel([0.6, -0.2])
    assert P.shap_values(m, np.zeros((1, 3)), 3) is None


def _spec(key="w_5_15", positive="Radiant", negative="Dire"):
    import ml_panel as MP
    return MP.ModelSpec(key=key, title="окно 5-15", positive=positive,
                        negative=negative, threshold=0.5, groups=("card",))


def test_verdict_carries_the_parts_through():
    import ml_panel as MP
    v = MP.evaluate(_spec(), 0.62, {"card": True},
                    parts={"ELO": -0.1, "драфт": -0.7, "игроки": -0.2},
                    fill=1.0, missing=())
    assert v.parts == {"ELO": -0.1, "драфт": -0.7, "игроки": -0.2}


def test_the_breakdown_replaces_the_old_draft_number():
    """Два числа со словом «драфт» в одной строке читать невозможно."""
    import ml_panel as MP
    v = MP.evaluate(_spec(), 0.62, {"card": True}, draft_share=-0.15,
                    parts={"ELO": -0.1, "драфт": -0.7, "игроки": -0.2},
                    fill=1.0, missing=())
    line = MP.render([v])
    assert "вклад: ELO -10% драфт -70% игроки -20%" in line
    assert "· драфт -15%" not in line


def test_without_the_breakdown_the_old_number_still_prints():
    """Панель без SHAP не должна терять то, что показывала раньше."""
    import ml_panel as MP
    v = MP.evaluate(_spec(), 0.62, {"card": True}, draft_share=-0.15,
                    fill=1.0, missing=())
    assert "драфт -15%" in MP.render([v])


def test_the_journal_keeps_the_parts():
    """Без записи разложение нельзя ни проверить задним числом, ни померить."""
    import ml_panel as MP
    v = MP.evaluate(_spec(), 0.62, {"card": True},
                    parts={"ELO": -0.1234567, "драфт": -0.7, "игроки": -0.2},
                    fill=1.0, missing=())
    row = MP.journal_row("8955934230", [v])
    assert row["schema"] >= 3
    assert row["models"][0]["parts"] == {"ELO": -0.1235, "драфт": -0.7,
                                         "игроки": -0.2}
