"""Telegram card boundary for the display-only B kills switch."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import kills_transfer_serving as kills  # noqa: E402
import ml_panel  # noqa: E402
import prematch_panel_live as panel  # noqa: E402
from base import win_model_veto as veto  # noqa: E402


B_BUNDLE = Path(__file__).resolve().parents[2] / "ml-models" / "prematch_panel_kv3"
E281_NUMBERS = (0.61, 0.42, 0.57)
E281_DATE = "2026-09-12"


def _lineups():
    return ({f"pos{i}": {"hero_id": i, "account_id": i} for i in range(1, 6)},
            {f"pos{i}": {"hero_id": i + 5, "account_id": i + 5}
             for i in range(1, 6)})


def _verdicts(model, *, dire=False):
    specs = {spec.key: spec for spec in ml_panel.load_specs(B_BUNDLE)}
    assert {"rad_30_25", "total_55_50"} <= specs.keys()
    window = ml_panel.ModelVerdict("w_5_15", "окно 5-15", "Radiant", 0.72,
                                   0.65, 1.0, True)
    targets = []
    for key, probability in (("rad_30_25", 0.82), ("total_55_50", 0.995)):
        spec = specs[key]
        targets.append(ml_panel.ModelVerdict(
            key, spec.title, spec.positive, probability, spec.threshold, 1.0,
            True, metadata={"model": model, "bundle_sha": "fixture"}))
    if dire:
        targets.insert(1, ml_panel.ModelVerdict(
            "dire_30_25", "дайр ≥30", "≥30", 0.83, 0.78, 1.0,
            True, metadata={"model": model, "bundle_sha": "fixture"}))
    duration = ml_panel.ModelVerdict("dur43", "Длительность", "Radiant", 0.63,
                                     0.60, 1.0, True, metadata={"model": "duration43"})
    return [window, *targets, duration]


def _card(monkeypatch, tmp_path, model="B_kv3", display=None, panel_error=False,
          dire=False):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    monkeypatch.setenv("ML_PANEL_KV3", "0")
    monkeypatch.setenv("SERIES_TEMPO_SHADOW", "0")
    if display is None:
        monkeypatch.delenv("ML_PANEL_KILLS_DISPLAY", raising=False)
    else:
        monkeypatch.setenv("ML_PANEL_KILLS_DISPLAY", display)
    monkeypatch.setattr(ml_panel, "DEFAULT_JOURNAL", tmp_path / "panel.jsonl")
    monkeypatch.setattr(panel, "ENABLED", True)
    monkeypatch.setattr(panel, "HYBRID_ENABLED", False)
    monkeypatch.setattr(panel, "DRAFT_KEYS", ())
    monkeypatch.setattr(panel, "_load", lambda: {
        "bundle": SimpleNamespace(ready=True), "tables": (None, None), "snap": None})
    monkeypatch.setattr(panel, "_dict_block", lambda *_: None)
    monkeypatch.setattr(panel, "live_rating_state", lambda: None)
    monkeypatch.setitem(sys.modules, "public_kills_block",
                        SimpleNamespace(block=lambda *_: None))
    monkeypatch.setitem(sys.modules, "team_ratings",
                        SimpleNamespace(block=lambda *a, **k: None))
    monkeypatch.setitem(sys.modules, "hero_side_tables",
                        SimpleNamespace(sym_block=lambda *a: []))
    monkeypatch.setitem(sys.modules, "prematch_panel_scorer", SimpleNamespace(
        block_from_matrix=lambda *a: {}, block_from_prod_features=lambda *a, **k: {},
        score=lambda *a, **k: _verdicts(model, dire=dire)))
    monkeypatch.setitem(sys.modules, "duration43_serving", SimpleNamespace(
        replace_verdict=lambda verdicts, *a, **k: verdicts,
        status=lambda: {"ready": True}))
    if panel_error:
        def fail_panel(*_args, **_kwargs):
            raise RuntimeError("panel failed")
        monkeypatch.setattr(panel, "evaluate_map", fail_panel)
    monkeypatch.setattr(kills, "forecast_probabilities", lambda *a: E281_NUMBERS)
    monkeypatch.setattr(kills, "manifest_history_date", lambda: E281_DATE)
    mid = 9014406398
    index, source, details = veto.win_prediction_ex(
        *_lineups(), "Team A", "Team B", match={"id": mid, "match_id": mid})
    assert (index, source) == (None, None)
    if panel_error:
        assert veto._LAST_PANEL["error"] == "RuntimeError: panel failed"
    else:
        assert veto._LAST_PANEL["error"] is None
    assert veto._LAST_PANEL["text"] == details["panel_text"]
    assert veto._LAST_PANEL["kills30"] == dict(zip(
        ("radiant", "dire", "total"), E281_NUMBERS))
    assert veto._LAST_PANEL["kills_error"] is None
    return details["panel_text"]


def test_b_card_shows_served_band_targets_without_e281(monkeypatch, tmp_path):
    text = _card(monkeypatch, tmp_path)
    assert "радиант ≥30: ≥30 82%" in text
    assert "тотал ≥55: ≥55 100%" in text
    assert "Килы ML · E-281" not in text
    assert text.index("окно 5-15") < text.index("радиант ≥30")
    assert text.index("радиант ≥30") < text.index("тотал ≥55")
    assert text.index("тотал ≥55") < text.index("Длительность")


def test_b_card_includes_optional_dire_in_order(monkeypatch, tmp_path):
    text = _card(monkeypatch, tmp_path, dire=True)
    assert "дайр ≥30: ≥30 83%" in text
    assert text.index("окно 5-15") < text.index("радиант ≥30")
    assert text.index("радиант ≥30") < text.index("дайр ≥30")
    assert text.index("дайр ≥30") < text.index("тотал ≥55")
    assert text.index("тотал ≥55") < text.index("Длительность")
    assert "Килы ML · E-281" not in text


@pytest.mark.parametrize("model", ["A_fallback", "B_kv3"])
def test_e281_text_when_b_unavailable_or_rollback(monkeypatch, tmp_path, model):
    display = "e281" if model == "B_kv3" else None
    text = _card(monkeypatch, tmp_path, model=model, display=display, dire=True)
    old_panel = ml_panel.render([_verdicts(model)[0], _verdicts(model)[-1]],
                                highlight=["w_5_15"])
    assert text == old_panel + "\n" + kills.render(E281_NUMBERS, E281_DATE)


def test_both_b_verdicts_required_by_shared_renderer(monkeypatch):
    verdicts = _verdicts("B_kv3")
    monkeypatch.delenv("ML_PANEL_KILLS_DISPLAY", raising=False)
    rendered, use_b = veto._render_panel_kills_display(verdicts, ml_panel)
    assert use_b is True
    assert "радиант ≥30" in rendered and "тотал ≥55" in rendered
    rendered, use_b = veto._render_panel_kills_display(verdicts[:-2] + verdicts[-1:], ml_panel)
    assert use_b is False
    assert rendered == ml_panel.render([verdicts[0], verdicts[-1]], highlight=["w_5_15"])


def test_panel_error_still_shows_e281(monkeypatch, tmp_path):
    text = _card(monkeypatch, tmp_path, panel_error=True)
    assert text == kills.render(E281_NUMBERS, E281_DATE)


@pytest.mark.parametrize("failure", ["empty", "exception"])
def test_b_render_failure_falls_back_to_e281_layout(monkeypatch, failure):
    verdicts = _verdicts("B_kv3")
    original = ml_panel.render

    def failing_b_render(selected, highlight=()):
        if any(v.key == "rad_30_25" for v in selected):
            if failure == "exception":
                raise RuntimeError("B render failed")
            return ""
        return original(selected, highlight=highlight)

    monkeypatch.setattr(ml_panel, "render", failing_b_render)
    text, use_b = veto._render_panel_kills_display(verdicts, ml_panel)
    assert use_b is False
    assert text == original([verdicts[0], verdicts[-1]], highlight=["w_5_15"])
