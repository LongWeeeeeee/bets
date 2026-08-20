"""Разметка 35 боевых признаков по ключу данных и выбор ветки.

Ключ определяет не смысл признака, а то, переживёт ли он отсутствие данных:
`wr30` это винрейт ГЕРОЕВ за 30 дней (`hero_wr30`), поэтому он переживает
незнакомых снимку игроков, а `elo` из таблицы аккаунтов — нет.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import prematch_components as C  # noqa: E402
import prematch_scorer as ps  # noqa: E402

ALL = ps.FEATURES + ps.NEW6


def test_every_prod_feature_has_a_key():
    assert set(C.REQUIRES) == set(ALL)


def test_keys_are_known():
    for name, keys in C.REQUIRES.items():
        assert keys <= {"account", "hero", "roster", "org"}, name
        assert keys, name


def test_hero_keyed_features_survive_unknown_accounts():
    cols = C.columns_for(frozenset({"hero", "roster", "org"}), ALL)
    assert sorted(cols) == sorted([
        "cp_lane", "draft_logit", "farm_dep", "h2h_resid", "hybrid_strength",
        "syn_pos_mean", "vs_wr", "wr30"])


def test_draft_interactions_need_the_account_key():
    """Контекст интеракций строится из |elo| и |games| — обе из таблицы аккаунтов."""
    assert C.REQUIRES["draft_logit_x_elo_gap"] == frozenset({"hero", "account"})
    assert "draft_logit_x_elo_gap" not in C.columns_for(
        frozenset({"hero", "roster", "org"}), ALL)


def test_pre_draft_branch_drops_hero_features_only():
    cols = C.columns_for(frozenset({"account", "roster", "org"}), ALL)
    assert len(cols) == 27
    assert "draft_logit" not in cols and "elo" in cols


def test_components_partition_all_features():
    seen = [f for group in C.COMPONENTS.values() for f in group]
    assert sorted(seen) == sorted(ALL)
    assert len(seen) == len(set(seen))


def test_component_sizes_match_the_spec():
    sizes = {k: len(v) for k, v in C.COMPONENTS.items()}
    assert sizes == {"elo": 5, "draft": 8, "players": 21, "h2h": 1}


def test_pick_branch_prefers_the_most_complete():
    assert C.pick_branch(frozenset({"account", "hero", "roster", "org"})) == "full"
    assert C.pick_branch(frozenset({"account", "hero", "roster"})) == "no_org"
    assert C.pick_branch(frozenset({"hero", "roster", "org"})) == "no_account"
    assert C.pick_branch(frozenset({"hero", "roster"})) == "no_account_no_org"
    assert C.pick_branch(frozenset({"account", "roster", "org"})) == "pre_draft"
    assert C.pick_branch(frozenset({"roster"})) == "rating_only"


def test_pick_branch_returns_none_without_rating_and_draft():
    assert C.pick_branch(frozenset({"org"})) is None
    assert C.pick_branch(frozenset()) is None


def test_columns_keep_the_order_of_the_artifact():
    """Порядок обязан идти от `features`, иначе веса встанут не на свои места."""
    cols = C.columns_for(frozenset({"account", "hero", "roster", "org"}), ALL)
    assert cols == ALL
