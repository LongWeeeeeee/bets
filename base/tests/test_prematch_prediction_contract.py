"""Contract regressions for the pre-match prediction card and snapshot APIs."""
from __future__ import annotations

import ast
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
import sys
sys.path.insert(0, str(ROOT))

from base import prematch_scorer as ps  # noqa: E402
from base import win_model_veto as veto  # noqa: E402
from test_prematch_ladder import _artifact  # noqa: E402


def _format_win_model_line():
    source = (ROOT / "base" / "cyberscore_try.py").read_text(encoding="utf-8")
    node = next(n for n in ast.walk(ast.parse(source))
                if isinstance(n, ast.FunctionDef) and n.name == "_format_win_model_line")
    env = {"win_model_veto": veto}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "<panel>", "exec"), env)
    return env["_format_win_model_line"]


def _block(index, branch, *, calibration=None, wr=None, bet=None):
    details = {
        "index": index, "branch": branch, "calibration": calibration or {},
        "wr": wr, "bet": bet, "parts": {}, "fill": None,
        "late": None, "early_nw": None, "early_win": None,
    }
    return {veto.INDEX_KEY: index, veto.SOURCE_KEY: veto.SOURCE_PREMATCH,
            veto.DETAILS_KEY: details}


@pytest.mark.parametrize(
    ("index", "branch", "calibration", "wr", "needles"),
    [
        (22.5, "full", {"expected_wr": .7057, "min_odds": 1.42,
                         "source": "lan_grid"}, .706,
         ("исторический WR: 70.6%", "ML от кэфа: 1.42")),
        (29.0, "no_org", {"expected_wr": .815785, "min_odds": 1.23,
                           "source": "branch_table"}, .815785,
         ("81.6%", "1.23")),
        (22.5, "no_account_no_org", {"expected_wr": .765746, "min_odds": 1.31,
                                      "source": "branch_table"}, .765746,
         ("76.6%", "без статистики игроков")),
    ],
)
def test_card_displays_branch_owned_quote_and_label(index, branch, calibration, wr, needles):
    line = _format_win_model_line()(_block(index, branch, calibration=calibration, wr=wr))
    for needle in needles:
        assert needle in line, line
    if not calibration:
        assert "70.6%" not in line
        assert "1.42" not in line


def test_absent_calibration_does_not_borrow_quote_from_wr():
    line = _format_win_model_line()(_block(
        22.5, "no_account_no_org", calibration={}, wr=.765746))
    assert "76.6%" not in line and "1.31" not in line


def test_live_delta_outcome_uses_env_journal_path_only(tmp_path, monkeypatch):
    """The result hook must respect the test/runtime override, not module default."""
    from base import prematch_prediction_journal as journal
    from base import prematch_live_delta as delta

    selected = tmp_path / "selected-outcomes.jsonl"
    forbidden = tmp_path / "forbidden-default.jsonl"
    monkeypatch.setenv("PREMATCH_OUTCOME_JOURNAL", str(selected))
    monkeypatch.setattr(journal, "DEFAULT_OUTCOME_PATH", forbidden)
    match = {
        "id": 991, "startDateTime": 100, "endDateTime": 200,
        "didRadiantWin": True,
        "players": [{"steamAccountId": 1, "isRadiant": True, "isVictory": True}],
    }
    assert delta.record_map(match, store_path=tmp_path / "delta.json") == 1
    assert selected.exists()
    assert not forbidden.exists()


def test_no_account_branch_never_gets_default_bet(monkeypatch):
    block = _block(22.5, "no_account", wr=.765746,
                   calibration={"expected_wr": .706, "min_odds": 1.42}, bet=False)
    monkeypatch.setattr(veto, "_DRAFT_AGAINST_BLOCK", False)
    assert veto.model_bet(block) is None


def test_eligible_no_org_uses_attached_frozen_quote(monkeypatch):
    block = _block(29.0, "no_org", wr=.815785,
                   calibration={"expected_wr": .815785, "min_odds": 1.23,
                                "source": "branch_table"}, bet=True)
    block[veto.DETAILS_KEY]["parts"] = {"draft": 1.0}
    monkeypatch.setattr(veto, "_DRAFT_AGAINST_BLOCK", True)
    bet = veto.model_bet(block)
    assert bet is not None
    assert bet["branch"] == "no_org"
    assert bet["expected_wr"] == pytest.approx(.815785)
    assert bet["min_odds"] == pytest.approx(1.23)


def test_equal_index_attached_parts_drive_draft_veto_independently():
    global_positive = {veto.INDEX_KEY: 29.0, veto.SOURCE_KEY: veto.SOURCE_PREMATCH,
                       veto.DETAILS_KEY: {"index": 29.0, "branch": "no_org",
                                          "wr": .815785, "parts": {"draft": 1.0},
                                          "calibration": {"expected_wr": .815785,
                                                           "min_odds": 1.23}}}
    attached_negative = {veto.INDEX_KEY: 29.0, veto.SOURCE_KEY: veto.SOURCE_PREMATCH,
                         veto.DETAILS_KEY: {"index": 29.0, "branch": "no_org",
                                            "wr": .815785, "parts": {"draft": -1.0},
                                            "calibration": {"expected_wr": .815785,
                                                             "min_odds": 1.23}}}
    veto._LAST_FILL.update({"index": 29.0, "parts": {"draft": 1.0}})
    veto._FILL_HISTORY[29.0] = dict(veto._LAST_FILL)
    assert veto.model_bet(global_positive) is not None
    assert veto.model_bet(attached_negative) is None


def test_late_side_uses_attached_details_not_colliding_last_fill(monkeypatch):
    monkeypatch.setitem(veto._LAST_FILL, "index", 7.0)
    monkeypatch.setitem(veto._LAST_FILL, "late", {"side": "dire", "confidence": .61})
    first = {veto.INDEX_KEY: 7.0, veto.DETAILS_KEY: {
        "index": 7.0, "late": {"side": "radiant", "confidence": .72}}}
    second = {veto.INDEX_KEY: 7.0, veto.DETAILS_KEY: {
        "index": 7.0, "late": {"side": "dire", "confidence": .61}}}
    source = (ROOT / "base" / "cyberscore_try.py").read_text(encoding="utf-8")
    node = next(n for n in ast.walk(ast.parse(source))
                if isinstance(n, ast.FunctionDef) and n.name == "_late_model_side_from_blocks")
    env = {"win_model_veto": veto}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "<late>", "exec"), env)
    assert env["_late_model_side_from_blocks"](first) == "radiant"
    assert env["_late_model_side_from_blocks"](second) == "dire"


def test_equal_indices_keep_card_owned_details_after_global_collision(monkeypatch):
    first_details = {"index": 8.0, "branch": "full", "parts": {"elo": 1.0}, "bet": True}
    second_details = {"index": 8.0, "branch": "no_org", "parts": {"draft": -1.0}, "bet": False}
    monkeypatch.setattr(veto, "_win_index_ex", lambda *args: (8.0, veto.SOURCE_PREMATCH))
    monkeypatch.setattr(veto, "_LAST_FILL", {**veto._LAST_FILL, **first_details})
    _, _, first = veto.win_prediction_ex({}, {})
    monkeypatch.setattr(veto, "_LAST_FILL", {**veto._LAST_FILL, **second_details})
    _, _, second = veto.win_prediction_ex({}, {})
    assert first["branch"] == "full" and first["parts"] == {"elo": 1.0} and first["bet"] is True
    assert second["branch"] == "no_org" and second["parts"] == {"draft": -1.0} and second["bet"] is False


def test_equal_index_threaded_snapshots_keep_panel_phase_and_branch_quote(monkeypatch):
    payloads = [
        {"panel_text": "PANEL-A", "late": {"side": "radiant", "confidence": .61},
         "branch": "full", "calibration": {"expected_wr": .706, "min_odds": 1.42,
                                                "source": "lan_point_grid"},
         "parts": {"elo": 1.0}, "wr": .706, "bet": True},
        {"panel_text": "PANEL-B", "late": {"side": "dire", "confidence": .62},
         "branch": "no_org", "calibration": {"expected_wr": .815785, "min_odds": 1.23,
                                                "source": "branch_table"},
         "parts": {"draft": -1.0}, "wr": .815785, "bet": False},
    ]
    monkeypatch.setattr(veto, "_LAST_FILL", {"index": None})

    def fake_index(*_args):
        payload = payloads.pop(0)
        veto._LAST_FILL.update({"index": 8.0, **payload})
        return 8.0, veto.SOURCE_PREMATCH

    monkeypatch.setattr(veto, "_win_index_ex", fake_index)
    with ThreadPoolExecutor(max_workers=2) as pool:
        snapshots = list(pool.map(lambda _: veto.win_prediction_ex({}, {})[2], (1, 2)))
    assert {row["panel_text"] for row in snapshots} == {"PANEL-A", "PANEL-B"}
    assert {row["late"]["side"] for row in snapshots} == {"radiant", "dire"}
    assert {row["branch"] for row in snapshots} == {"full", "no_org"}
    assert {row["calibration"]["min_odds"] for row in snapshots} == {1.42, 1.23}
    format_line = _format_win_model_line()
    for row in snapshots:
        card = {veto.INDEX_KEY: 8.0, veto.SOURCE_KEY: veto.SOURCE_PREMATCH,
                veto.DETAILS_KEY: row}
        rendered = format_line(card)
        assert row["panel_text"] in rendered
        assert row["late"]["side"] in rendered


def test_prematch_index_freezes_panel_before_fill_history():
    source = (ROOT / "base" / "win_model_veto.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "_prematch_index")
    panel_line = next(n.lineno for n in ast.walk(fn)
                      if isinstance(n, ast.Assign) and "panel_text" in ast.unparse(n))
    remember_line = next(n.lineno for n in ast.walk(fn)
                         if isinstance(n, ast.Expr) and "_remember_fill" in ast.unparse(n))
    assert panel_line < remember_line


def test_artifact_digest_is_loaded_bytes_and_survives_atomic_replacement(tmp_path):
    first_dir, second_dir = tmp_path / "first", tmp_path / "second"
    first_dir.mkdir(); second_dir.mkdir()
    first = _artifact(first_dir, known_accounts=[])
    second = _artifact(second_dir, known_accounts=[101, 102, 103, 104, 105, 106, 107, 108, 109, 110])
    first_path, second_path = first_dir / "art.npz", second_dir / "art.npz"
    expected_first = hashlib.sha256(first_path.read_bytes()).hexdigest()
    expected_second = hashlib.sha256(second_path.read_bytes()).hexdigest()
    assert first.artifact_sha256 == expected_first
    assert second.artifact_sha256 == expected_second
    old_digest = first.artifact_sha256
    os.replace(second_path, first_path)
    assert first.artifact_sha256 == old_digest
    reloaded = ps.PrematchModel(first_path)
    assert reloaded.artifact_sha256 == expected_second
