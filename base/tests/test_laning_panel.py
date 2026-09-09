"""Display-only laning panel integration contracts."""
import ast
from pathlib import Path
from types import SimpleNamespace

import numpy as np


ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "base/cyberscore_try.py"


def _function(name):
    tree = ast.parse(SOURCE.read_text())
    return next(node for node in ast.walk(tree)
                if isinstance(node, ast.FunctionDef) and node.name == name)


def _compiled(name, env):
    exec(compile(ast.Module(body=[_function(name)], type_ignores=[]), "<panel>", "exec"), env)
    return env[name]


def test_lane_panel_follows_top_mid_bot_and_keeps_old_default():
    build_lane = _compiled(
        "_build_lane_block",
        {"Any": object, "_build_lane_kills_adv_line": lambda value: ""},
    )
    assert build_lane("Top", "Mid", "Bot") == "Lanes:\nTop\nMid\nBot\n\n"
    text = build_lane("Top", "Mid", "Bot", ml_laning_line="ML Laning: Radiant 61.0%")
    assert text.splitlines()[:4] == ["Lanes:", "Top", "Mid", "Bot"]
    assert text.splitlines()[4] == "ML Laning: Radiant 61.0%"


def test_all_model_follows_early_win_and_survives_missing_ensemble_index():
    record = {"side": "Dire", "confidence": .554}
    veto = SimpleNamespace(
        INDEX_KEY="index", SOURCE_KEY="source", SOURCE_PREMATCH="prematch",
        last_parts=lambda index: None, last_draft_rank=lambda index: None,
        last_fill=lambda index: None, last_early_nw=lambda index: record,
        last_early_win=lambda index: record, last_late=lambda index: record,
        last_panel_text=lambda: "",
    )
    format_line = _compiled("_format_win_model_line", {"win_model_veto": veto})
    all_line = "🌐 All ML-модель: Radiant 62.0%"
    lines = format_line({"index": -5.4}, all_model_line=all_line).splitlines()
    early_win = lines.index("🏁 Early Win ML-модель: Dire 55.4%")
    assert lines[early_win + 1] == all_line
    assert lines[early_win + 2] == "🕑 Late ML-модель: Dire 55.4%"
    assert format_line({}, all_model_line=all_line) == all_line + "\n"


def test_serving_uses_each_card_even_when_ensemble_indices_match(monkeypatch):
    from base import laning_serving

    cards = []

    class DraftModel:
        def win_index_draft(self, radiant, dire):
            cards.append((radiant, dire))
            return 7.0

        @staticmethod
        def _heroes_vector(radiant, dire):
            return tuple(range(1, 11))

    monkeypatch.setattr(laning_serving._SERVICE, "predict", lambda *args: None)
    first, second = {"pos1": {"account_id": 1}}, {"pos1": {"account_id": 2}}
    assert laning_serving.panel_lines(first, {}, 1, draft_model=DraftModel())["all_model_line"]
    assert laning_serving.panel_lines(second, {}, 2, draft_model=DraftModel())["all_model_line"]
    assert cards == [(first, {}), (second, {})]


def test_serving_omits_only_the_failed_new_line(monkeypatch):
    from base import laning_serving

    class DraftModel:
        @staticmethod
        def win_index_draft(radiant, dire):
            raise RuntimeError("no ensemble")

        @staticmethod
        def _heroes_vector(radiant, dire):
            return tuple(range(1, 11))

    monkeypatch.setattr(
        laning_serving._SERVICE, "predict", lambda *args: np.array([.1, .2, .7])
    )
    result = laning_serving.panel_lines({}, {}, 1, draft_model=DraftModel())
    assert result["all_model_line"] == ""
    assert result["ml_laning_line"].startswith("ML Laning: Radiant")


def test_every_lane_card_builder_accepts_explicit_laning_line():
    tree = ast.parse(SOURCE.read_text())
    calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_build_lane_block"
    ]
    # Five helper builders plus the three check_head cards.
    assert len(calls) == 8
    assert all(any(keyword.arg == "ml_laning_line" for keyword in call.keywords)
               for call in calls)


def test_check_head_evaluates_adapter_once_and_keeps_strings_explicit():
    check_head = _function("check_head")
    adapter_calls = [
        node for node in ast.walk(check_head)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_build_laning_panel_lines"
    ]
    assert len(adapter_calls) == 1
    call = adapter_calls[0]
    assert [arg.id for arg in call.args if isinstance(arg, ast.Name)] == [
        "radiant_heroes_and_pos", "dire_heroes_and_pos", "team_elo_timestamp",
    ]


def test_adapter_failure_is_fail_open_in_source():
    fn = _function("_build_laning_panel_lines")
    handlers = [node for node in ast.walk(fn) if isinstance(node, ast.ExceptHandler)]
    assert any(isinstance(handler.type, ast.Name) and handler.type.id == "Exception"
               for handler in handlers)
