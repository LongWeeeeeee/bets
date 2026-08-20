"""Незнакомые снимку игроки больше не отменяют вердикт.

Проверяется на синтетическом артефакте с двумя ветками: полной и той, что
переживает отсутствие аккаунтов. Настоящие веса здесь не нужны — нужен контракт
выбора ветки и то, что короткая ветка не лезет в таблицу, которой нет.
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import prematch_scorer as ps  # noqa: E402

FEATURES = ["draft_logit", "wr30", "vs_wr", "cp_lane", "syn_pos_mean", "farm_dep",
            "hybrid_strength", "h2h_resid", "elo", "opp_elo"]
NO_ACC = ["draft_logit", "wr30", "vs_wr", "cp_lane", "syn_pos_mean", "farm_dep",
          "hybrid_strength", "h2h_resid"]
ACCOUNTS = list(range(101, 111))
HEROES_R, HEROES_D = [1, 2, 3, 4, 5], [6, 7, 8, 9, 10]


def _artifact(tmp_path, known_accounts, branches=None):
    n = len(FEATURES)
    if branches is None:
        branches = {
            "full": ps.Branch(cols=list(FEATURES), mu=np.zeros(n), sd=np.ones(n),
                              coef=np.full(n, 0.1), intercept=0.0),
            "no_account": ps.Branch(cols=list(NO_ACC), mu=np.zeros(len(NO_ACC)),
                                    sd=np.ones(len(NO_ACC)),
                                    coef=np.full(len(NO_ACC), 0.2), intercept=0.0),
        }
    # 19 колонок таблицы аккаунтов — столько читает `_acc_side`
    acc = (np.array([[a] + [1500.0] + [1.0] * 18 for a in known_accounts])
           if known_accounts else np.zeros((0, 20)))
    z = {
        "snapshot_ts": np.array([1700000000], dtype=np.int64),
        "mu": np.zeros((1, n)), "sd": np.ones((1, n)),
        "coef": np.zeros((1, n)), "intercept": np.zeros(1),
        "accounts": acc,
        "acc_hero": np.zeros((0, 6)), "acc_pos": np.zeros((0, 3)),
        "hero_wr30": np.array([[h, 0.5 + 0.01 * h] for h in range(1, 21)]),
        "hero_farm": np.array([[h, 0.4] for h in range(1, 21)]),
        "vs_pairs": np.zeros((0, 4)),
        # Без истории встреч ключ `org` недоступен, и ветки, которые его
        # требуют (в том числе полная), выбраться не могут вовсе.
        "h2h": np.array([[11.0, 22.0, 0.05]]),
        "feature_names": np.array(FEATURES),
    }
    z.update(ps.pack_branches(branches, FEATURES))
    p = tmp_path / "art.npz"
    np.savez_compressed(p, **z)
    return ps.PrematchModel(p)


def _call(m, draft_logit=0.3, hybrid_strength=0.2):
    return m.score(radiant_accounts=ACCOUNTS[:5], dire_accounts=ACCOUNTS[5:],
                   radiant_heroes=HEROES_R, dire_heroes=HEROES_D,
                   radiant_team_id=11, dire_team_id=22,
                   draft_logit=draft_logit, hybrid_strength=hybrid_strength,
                   strictness="teams", now_ts=1700000000, max_age_days=1e9)


def test_unknown_accounts_fall_back_instead_of_raising(tmp_path):
    m = _artifact(tmp_path, known_accounts=[])
    r = _call(m)
    assert r.branch == "no_account"
    assert "account" in r.missing_keys
    assert 0.0 < r.probability < 1.0


def test_known_accounts_use_the_full_branch(tmp_path):
    m = _artifact(tmp_path, known_accounts=ACCOUNTS)
    r = _call(m)
    assert r.branch == "full"
    assert r.missing_keys == []


def test_fallback_never_touches_the_account_table(tmp_path):
    """Ветка без аккаунтов не имеет права даже заглянуть в таблицу."""
    m = _artifact(tmp_path, known_accounts=[])

    class Boom(dict):
        def __getitem__(self, k):
            raise AssertionError("короткая ветка полезла в таблицу аккаунтов")

    m.acc = Boom()
    assert _call(m).branch == "no_account"


def test_missing_hybrid_and_draft_still_refuses(tmp_path):
    """Нет ни рейтинга ростера, ни драфта — считать не из чего."""
    m = _artifact(tmp_path, known_accounts=[])
    with pytest.raises(ps.MissingData):
        _call(m, draft_logit=None, hybrid_strength=None)


def test_refuses_when_no_branch_is_trained_for_the_pattern(tmp_path):
    """Ветку без аккаунтов не обучили — врать нечем, отказываемся."""
    n = len(FEATURES)
    only_full = {"full": ps.Branch(cols=list(FEATURES), mu=np.zeros(n),
                                   sd=np.ones(n), coef=np.full(n, 0.1),
                                   intercept=0.0)}
    m = _artifact(tmp_path, known_accounts=[], branches=only_full)
    with pytest.raises(ps.MissingData):
        _call(m)


def test_short_branch_uses_its_own_weights(tmp_path):
    """Проверяем арифметику: короткая ветка считает по своим коэффициентам."""
    m = _artifact(tmp_path, known_accounts=[])
    r = _call(m)
    b = m.branches["no_account"]
    x = np.array([r.features[c] for c in b.cols])
    want = 1.0 / (1.0 + math.exp(-(float(((x - b.mu) / b.sd) @ b.coef) + b.intercept)))
    assert r.probability == pytest.approx(want, abs=1e-12)


def test_short_branch_reports_only_its_own_features(tmp_path):
    """Признаков недоступного компонента в отчёте быть не должно вовсе."""
    m = _artifact(tmp_path, known_accounts=[])
    r = _call(m)
    assert "elo" not in r.features and "opp_elo" not in r.features
    assert set(r.features) == set(NO_ACC)


def test_stale_snapshot_still_refuses(tmp_path):
    """Протухший снимок — жёсткая причина, а не выбор ветки."""
    m = _artifact(tmp_path, known_accounts=ACCOUNTS)
    with pytest.raises(ps.MissingData, match="протух"):
        m.score(radiant_accounts=ACCOUNTS[:5], dire_accounts=ACCOUNTS[5:],
                radiant_heroes=HEROES_R, dire_heroes=HEROES_D,
                radiant_team_id=11, dire_team_id=22,
                draft_logit=0.3, hybrid_strength=0.2, strictness="teams",
                now_ts=1700000000 + 40 * 86400, max_age_days=3.0)
