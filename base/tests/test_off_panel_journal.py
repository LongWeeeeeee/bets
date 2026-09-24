"""Journal coverage for the independent panel with general prematch ML off."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import ml_panel  # noqa: E402 - bare import, as base/win_model_veto.py does

from base import win_model_veto as V


def _lineups():
    radiant = {f"pos{i}": {"hero_id": i, "account_id": i} for i in range(1, 6)}
    dire = {f"pos{i}": {"hero_id": i + 5, "account_id": i + 5}
            for i in range(1, 6)}
    return radiant, dire


def _stub_panels(monkeypatch, verdicts):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    monkeypatch.setitem(sys.modules, "prematch_panel_live", SimpleNamespace(
        evaluate_map=lambda *a, **k: verdicts))
    monkeypatch.setitem(sys.modules, "kills_transfer_serving", SimpleNamespace(
        forecast_probabilities=lambda *a: (0.6, 0.4, 0.5),
        manifest_history_date=lambda: None,
        render=lambda *a: "kills panel"))


def test_off_journals_all_verdicts_with_model_metadata(tmp_path, monkeypatch):
    journal = tmp_path / "ml_panel.jsonl"
    production_journal = ml_panel.DEFAULT_JOURNAL
    before = production_journal.stat().st_size if production_journal.exists() else None
    monkeypatch.setattr(ml_panel, "DEFAULT_JOURNAL", journal)
    assert sys.modules["ml_panel"] is ml_panel
    verdicts = [
        ml_panel.ModelVerdict("w_test", "Win", "Radiant", 0.61, 0.55, 1.0,
                              True, metadata={"model": "B_kv3", "a_p": 0.61}),
        ml_panel.ModelVerdict("total_55_50", "Total", "Dire", 0.56, 0.55, 1.0,
                              True, metadata={"model": "A_fallback",
                                              "reason": "state_stale"}),
    ]
    _stub_panels(monkeypatch, verdicts)

    index, source, details = V.win_prediction_ex(
        *_lineups(), "Team A", "Team B", match={"id": 9013821098})

    assert (index, source) == (None, None)
    assert details["panel_text"]
    lines = journal.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert row["schema"] == ml_panel.JOURNAL_SCHEMA
    assert row["map_id"] == "9013821098"
    assert (row["radiant_team"], row["dire_team"]) == ("Team A", "Team B")
    models = {model["key"]: model for model in row["models"]}
    assert set(models) == {"w_test", "total_55_50"}
    assert models["w_test"]["metadata"] == {"model": "B_kv3", "a_p": 0.61}
    assert models["total_55_50"]["metadata"] == {
        "model": "A_fallback", "reason": "state_stale"}
    after = production_journal.stat().st_size if production_journal.exists() else None
    assert after == before


def test_off_empty_verdicts_do_not_create_journal(tmp_path, monkeypatch):
    journal = tmp_path / "ml_panel.jsonl"
    monkeypatch.setattr(ml_panel, "DEFAULT_JOURNAL", journal)
    _stub_panels(monkeypatch, [])

    V.win_prediction_ex(*_lineups(), "Team A", "Team B", match={"id": 9013821098})

    assert not journal.exists() or not journal.read_text(encoding="utf-8")
