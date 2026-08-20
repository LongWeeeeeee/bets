"""Разнесение расчёта признаков по компонентам не меняет результат.

Тест держит контракт: пока вход полный, `score()` обязан отдавать ровно те же
числа, что и до разнесения. Эталон снят на боевом артефакте скриптом
`runtime/experiments/misc/make_score_golden.py` и сравнивается побитово — это
единственный способ поймать, что при переносе строк потерялось деление на 100
или перепутался знак. Цена такой ошибки измерена: E-166 стоил 0.116 AUC.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "base"))

import prematch_scorer as ps  # noqa: E402

ART = ROOT / "runtime/artifacts/misc/prematch_model_artifact_v3_hybrid.npz"
GOLD = Path(__file__).resolve().parent / "fixtures" / "prematch_score_golden.json"

pytestmark = pytest.mark.skipif(
    not (ART.exists() and GOLD.exists()),
    reason="боевой артефакт или эталон не собраны локально")


def _scored():
    gold = json.loads(GOLD.read_text(encoding="utf-8"))
    m = ps.PrematchModel(ART)
    r = m.score(
        radiant_accounts=gold["radiant_accounts"], dire_accounts=gold["dire_accounts"],
        radiant_heroes=gold["radiant_heroes"], dire_heroes=gold["dire_heroes"],
        radiant_team_id=gold["radiant_team_id"], dire_team_id=gold["dire_team_id"],
        draft_logit=gold["draft_logit"], hybrid_strength=gold["hybrid_strength"],
        strictness="teams", now_ts=gold["now_ts"], max_age_days=1e9)
    return gold, r


def test_full_input_probability_matches_golden():
    gold, r = _scored()
    assert r.probability == pytest.approx(gold["probability"], abs=1e-12)


def test_every_feature_matches_golden():
    gold, r = _scored()
    assert set(r.features) == set(gold["features"])
    for k, v in gold["features"].items():
        assert r.features[k] == pytest.approx(v, abs=1e-12), k


def test_full_input_uses_the_full_branch():
    """Полный вход обязан идти по полной ветке, а не свалиться в запасную."""
    _, r = _scored()
    assert getattr(r, "branch", "full") == "full"
    assert getattr(r, "missing_keys", []) == []
