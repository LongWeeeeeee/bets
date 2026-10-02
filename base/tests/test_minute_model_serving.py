"""Parity and contract tests for base/minute_model_serving.py (E-347 minute model).

Fixture `fixtures/minute_model_parity_20261002.json` is a byte copy of
runtime/artifacts/draft-cp/minute_model_20261002/parity_fixture.json, captured
2026-10-02 by runtime/experiments/draft-cp/minute_model_20261002/train_export.py
(sklearn Pipeline.predict_proba of the final serving fits: 150 rows at minute 10
and 150 at minute 31, global extremes of every feature included). The model is
the real data/minute_model/minute_model_v1.json (sha256 d4a5af0d...); nothing
of the model under test is mocked.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
import minute_model_serving as M  # noqa: E402

REPO = BASE_DIR.parent
FIXTURE = json.loads((Path(__file__).parent / "fixtures/minute_model_parity_20261002.json").read_text())
MODEL_SHA = "d4a5af0df9203157b130aba4d90fd0d454fc439cc7e225bd64d8efcbb2d96f28"
RAW_OK = dict(nw_lead=3200, radiant_kills=18, dire_kills=11,
              p_all=0.6, p_late=0.55, p_early_win=0.62, p_enw_rad=0.58, p_elo=0.7)


@pytest.fixture(autouse=True)
def _fresh_model(monkeypatch):
    monkeypatch.delenv("MINUTE_MODEL_ENABLED", raising=False)
    monkeypatch.delenv("MINUTE_MODEL_PATH", raising=False)
    M.reset()
    yield
    M.reset()


def test_shipped_artifact_is_the_exported_one() -> None:
    raw = (REPO / "data/minute_model/minute_model_v1.json").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == MODEL_SHA == FIXTURE["model_sha256"]
    assert len(FIXTURE["rows"]) == 300


def test_all_300_captured_rows_match_sklearn_within_1e9() -> None:
    worst = 0.0
    seen = {10: 0, 31: 0}
    for row in FIXTURE["rows"]:
        out = M.predict(row["minute"], **row["raw"])
        assert out is not None and out["model_sha256"] == MODEL_SHA
        worst = max(worst, abs(out["p_head"] - row["expected"]["head"]),
                    abs(out["p_stack"] - row["expected"]["stack"]))
        seen[row["minute"]] += 1
    assert seen == {10: 150, 31: 150}
    assert worst < 1e-9, worst


def test_missing_or_non_finite_required_input_returns_none() -> None:
    assert M.predict(31, **RAW_OK) is not None
    for key in ("nw_lead", "radiant_kills", "dire_kills", "p_all", "p_late",
                "p_early_win", "p_enw_rad"):
        for bad in (None, float("nan"), float("inf"), "x"):
            assert M.predict(31, **dict(RAW_OK, **{key: bad})) is None, (key, bad)
    assert M.predict(31, **dict(RAW_OK, p_all=1.2)) is None
    assert M.predict(20, **RAW_OK) is None            # a minute the model does not know


def test_missing_elo_gives_head_only() -> None:
    for elo in (None, float("nan"), 1.5):
        out = M.predict(31, **dict(RAW_OK, p_elo=elo))
        assert out is not None and out["p_stack"] is None and 0 < out["p_head"] < 1
    with_elo = M.predict(31, **RAW_OK)
    assert with_elo["p_stack"] is not None
    assert with_elo["p_head"] == M.predict(31, **dict(RAW_OK, p_elo=None))["p_head"]


def test_probabilities_at_the_clip_edge_are_finite() -> None:
    out = M.predict(10, **dict(RAW_OK, p_all=0.0, p_late=1.0, p_early_win=0.0, p_enw_rad=1.0, p_elo=1.0))
    assert out is not None and math.isfinite(out["p_head"]) and math.isfinite(out["p_stack"])


def test_wrong_schema_disables(tmp_path, monkeypatch) -> None:
    data = json.loads((REPO / "data/minute_model/minute_model_v1.json").read_text())
    data["schema"] = "minute-model-v2"
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(data))
    monkeypatch.setenv("MINUTE_MODEL_PATH", str(bad))
    M.reset()
    assert M.predict(31, **RAW_OK) is None
    assert "schema" in (M.load_error() or "")


def test_broken_feature_list_or_missing_file_disables(tmp_path, monkeypatch) -> None:
    data = json.loads((REPO / "data/minute_model/minute_model_v1.json").read_text())
    broken = copy.deepcopy(data)
    broken["minutes"]["31"]["head"]["coef"] = broken["minutes"]["31"]["head"]["coef"][:-1]
    path = tmp_path / "broken.json"
    path.write_text(json.dumps(broken))
    monkeypatch.setenv("MINUTE_MODEL_PATH", str(path))
    M.reset()
    assert M.predict(31, **RAW_OK) is None
    monkeypatch.setenv("MINUTE_MODEL_PATH", str(tmp_path / "absent.json"))
    M.reset()
    assert M.predict(31, **RAW_OK) is None


def test_kill_switch(monkeypatch) -> None:
    assert M.predict(31, **RAW_OK) is not None
    monkeypatch.setenv("MINUTE_MODEL_ENABLED", "0")
    assert M.enabled() is False
    assert M.predict(31, **RAW_OK) is None


def test_verdict_transforms_live_semantics() -> None:
    # probability is already P(Radiant) for BOTH sides: a Dire verdict must NOT be inverted.
    dire = {"side": "Dire", "probability": 0.38, "confidence": 0.62}
    rad = {"side": "Radiant", "probability": 0.71, "confidence": 0.71}
    assert M.radiant_probability(dire) == 0.38
    assert M.radiant_probability(rad) == 0.71
    # Early NW conditional direction: same dict shape, same reading.
    assert M.radiant_probability({"side": "Dire", "probability": 0.299, "confidence": 0.701}) == 0.299
    # dispatch-view shape (no probability): rebuilt from the named side's confidence.
    assert M.radiant_probability({"side": "Dire", "confidence": 0.62}) == pytest.approx(0.38)
    assert M.radiant_probability({"side": "Radiant", "confidence": 0.62}) == pytest.approx(0.62)
    for bad in (None, {}, {"side": "tie", "confidence": 0.7}, {"probability": float("nan")},
                {"probability": 1.5}, {"side": "Radiant", "confidence": 0.3}, "Radiant"):
        assert M.radiant_probability(bad) is None, bad
    # All: win_index_draft is (P - 0.5) * 100 -> P = 0.5 + index / 100, sign preserved for Dire.
    assert M.all_probability_from_index(20.346) == pytest.approx(0.70346)
    assert M.all_probability_from_index(-12.5) == pytest.approx(0.375)
    assert M.all_probability_from_index(None) is None
    assert M.all_probability_from_index(float("nan")) is None
    assert M.all_probability_from_index(80) is None
