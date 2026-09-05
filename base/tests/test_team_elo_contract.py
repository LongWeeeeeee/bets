"""One served team Elo for ML and Telegram, with no silent rating fallback."""
from __future__ import annotations

import ast
import copy
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "base"))

import win_model_veto as W
from ELO import live_team_strength as L
from ELO.config import HybridEloConfig
from ELO.domain import LeagueTier
from ELO.models import HybridPlayerRosterEloModel, prematch_lineup_summary


def _runtime_functions():
    """Run production formatting functions without starting/importing the bot."""
    tree = ast.parse((ROOT / "base/cyberscore_try.py").read_text())
    names = {"_format_team_elo_block", "_elo_probability_from_ratings",
             "_build_team_elo_matchup_summary", "_build_team_elo_matchup_summary_from_live_snapshot"}
    selected = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    ns = dict(Any=Any, Dict=Dict, List=List, Optional=Optional, Tuple=Tuple)
    exec(compile(ast.Module(body=selected, type_ignores=[]), "elo_runtime", "exec"), ns)
    return ns


@pytest.fixture
def matchup(monkeypatch):
    # Real account/position ordering from the existing 50-map pro fixture.
    card = json.loads((ROOT / "base/tests/fixtures/prematch_h2h_teamid_cards.json").read_text())[0]
    rad, dire = card["radiant_accounts"], card["dire_accounts"]
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    for ids, global_rating, tier1_rating in ((rad, 1700.0, 900.0), (dire, 1500.0, 2300.0)):
        for account in ids:
            model.player_global[account] = global_rating
            model.player_local[LeagueTier.TIER1][account] = tier1_rating
            model.player_local[LeagueTier.TIER3][account] = global_rating
    snapshot = {"meta": {"reference_timestamp": card["ts"] - 1},
                "teams_by_org_key": {}, "model_state": model.export_state()}
    monkeypatch.setattr(L, "ensure_snapshot", lambda **kw: snapshot)
    monkeypatch.setattr(L, "_snapshot_with_runtime_model_state", lambda s, **kw: s)
    monkeypatch.setattr(L, "_restore_model_from_snapshot", lambda s: model)
    monkeypatch.setattr(W, "_live_elo_model", lambda: model)
    return card, model, snapshot


def _draft(ids):
    return {f"pos{i}": {"account_id": a} for i, a in enumerate(ids, 1)}


def test_live_summary_ml_and_telegram_use_identical_rating(matchup):
    card, model, snapshot = matchup
    rad, dire = card["radiant_accounts"], card["dire_accounts"]
    before = copy.deepcopy(model.export_state())
    kwargs = dict(radiant_team_id=card["radiant_team_id"], dire_team_id=card["dire_team_id"],
                  radiant_team_name="Fixture Radiant", dire_team_name="Fixture Dire",
                  radiant_account_ids=rad, dire_account_ids=dire)
    legacy = L.build_matchup_summary_from_snapshot(snapshot, match_tier=LeagueTier.TIER1, **kwargs)
    assert legacy["elo_diff"] < 0  # Reproducer: the old card backed the other side.
    summary = L.get_matchup_summary(timestamp=card["ts"], match_tier=LeagueTier.TIER1, **kwargs)
    ml = W.hybrid_strength_diff(_draft(rad), _draft(dire), "Fixture Radiant", "Fixture Dire", card["ts"])
    assert summary["elo_diff"] == pytest.approx(ml * 400.0)
    assert summary["elo_diff"] == pytest.approx(200.0)
    assert summary["radiant"]["base_rating"] == summary["radiant"]["team_strength"]
    assert summary["tier_gap_bonus"] == 0.0
    text, meta = _runtime_functions()["_format_team_elo_block"](
        summary, radiant_team_name="Fixture Radiant", dire_team_name="Fixture Dire")
    assert "ELO состава (как в ML):" in text
    assert "ΔELO +200" in text
    assert meta["raw_diff"] == pytest.approx(ml * 400.0)
    assert meta["raw_radiant_wr"] / 100 == pytest.approx(summary["radiant_win_prob"])
    assert model.export_state() == before, "Preview must not apply outcomes or mutate ratings"


def test_contract_preserves_training_arguments_and_positions(matchup):
    card, _, _ = matchup
    calls = []

    class Recorder:
        def preview_team_strength(self, **kw):
            calls.append(kw)
            # Team strength deliberately differs from cold-roster player strength.
            return {"team_strength": 1650.0 if len(calls) == 1 else 1500.0,
                    "player_strength": 1400.0, "roster_matches": 0}

    summary = prematch_lineup_summary(
        Recorder(), radiant_team_name="A", dire_team_name="B",
        radiant_account_ids=card["radiant_accounts"], dire_account_ids=card["dire_accounts"],
        timestamp=card["ts"])
    assert summary["elo_diff"] == 150.0
    assert calls[0]["team_id"] is None
    assert calls[0]["tier"] == LeagueTier.TIER3
    assert calls[0]["timestamp"] == card["ts"]
    expected = sorted(zip(card["radiant_accounts"], [f"POSITION_{i}" for i in range(1, 6)]))
    assert list(zip(calls[0]["player_ids"], calls[0]["player_positions"])) == expected


@pytest.mark.parametrize("bad", [[1, 2, 3, 4], [1, 2, 3, 4, 0], [1, 2, 3, 4, 4], None])
def test_invalid_roster_is_unavailable_instead_of_default_elo(matchup, bad):
    _, model, _ = matchup
    assert prematch_lineup_summary(model, radiant_team_name="A", dire_team_name="B",
                                  radiant_account_ids=bad, dire_account_ids=[6, 7, 8, 9, 10],
                                  timestamp=100) is None


def test_side_swap_reverses_only_the_sign(matchup):
    card, model, _ = matchup
    def score(rad, dire, rn, dn):
        return prematch_lineup_summary(model, radiant_team_name=rn, dire_team_name=dn,
                                      radiant_account_ids=rad, dire_account_ids=dire, timestamp=card["ts"])
    a = score(card["radiant_accounts"], card["dire_accounts"], "A", "B")
    b = score(card["dire_accounts"], card["radiant_accounts"], "B", "A")
    assert a["hybrid_strength"] == -b["hybrid_strength"]
    assert a["radiant_win_prob"] == pytest.approx(1.0 - b["radiant_win_prob"])


def test_missing_live_elo_never_falls_back_to_kills_rating():
    ns = _runtime_functions()
    ns["_build_team_elo_matchup_summary_from_live_snapshot"] = lambda **kw: None
    def forbidden(**kw):
        raise AssertionError("must not switch to a different rating system")
    ns["_build_team_elo_matchup_summary_from_kills_priors"] = forbidden
    assert ns["_build_team_elo_matchup_summary"](1, 2, "A", "B") is None


def test_legacy_summary_is_not_mislabeled_as_ml_elo():
    text, _ = _runtime_functions()["_format_team_elo_block"](
        {"source": "elo_live_lineup_snapshot",
         "radiant": {"rating": 1600.0, "lineup_used": True},
         "dire": {"rating": 1500.0, "lineup_used": True}},
        radiant_team_name="A", dire_team_name="B")
    assert "как в ML" not in text


def test_card_has_no_competing_account_elo_label():
    tree = ast.parse((ROOT / "base/cyberscore_try.py").read_text())
    labels = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
    assert not any("ELO модели:" in label for label in labels)


def test_runtime_passes_map_time_through_both_wrappers(matchup):
    card, model, _ = matchup
    ns = _runtime_functions()
    seen = []
    ns["ELO_LIVE_SNAPSHOT_AVAILABLE"] = True
    def get_summary(**kwargs):
        seen.append(kwargs["timestamp"])
        return L.get_matchup_summary(**kwargs)
    ns["_elo_live_get_matchup_summary"] = get_summary
    # A nonzero decay makes a wall-clock/start-time mismatch observable.
    from dataclasses import replace
    model.config = replace(model.config, player_global_decay_half_life_days=1.0)
    for account in card["radiant_accounts"] + card["dire_accounts"]:
        model.player_global_last_seen_ts[account] = card["ts"] - 86400
    ts = W.elo_evaluation_timestamp({"startDateTime": str(card["ts"])})
    summary = ns["_build_team_elo_matchup_summary"](
        1, 2, "A", "B", card["radiant_accounts"], card["dire_accounts"], timestamp=ts)
    ml = W.hybrid_strength_diff(_draft(card["radiant_accounts"]), _draft(card["dire_accounts"]), "A", "B", ts)
    assert seen == [card["ts"]]
    assert summary["elo_diff"] == pytest.approx(ml * 400.0)
    tree = ast.parse((ROOT / "base/cyberscore_try.py").read_text())
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == "_build_team_elo_matchup_summary"]
    assert calls
    for call in calls:
        timestamp = next(k.value for k in call.keywords if k.arg == "timestamp")
        assert isinstance(timestamp, ast.Name) and timestamp.id == "team_elo_timestamp"
    # The map wrapper previously dropped startDateTime when constructing the
    # reduced match payload for ML. Merely fixing the ELO API was insufficient.
    assignments = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
                   and any(isinstance(t, ast.Name) and t.id == "team_elo_timestamp" for t in n.targets)]
    assert len(assignments) == 1
    assert assignments[0].value.func.attr == "elo_evaluation_timestamp"
    metric_calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
                    and isinstance(n.func, ast.Name) and n.func.id == "synergy_and_counterpick"]
    wired = []
    for call in metric_calls:
        kwargs = {k.arg: k.value for k in call.keywords}
        payload = kwargs.get("match")
        if not isinstance(payload, ast.Dict):
            continue
        values = {k.value: v for k, v in zip(payload.keys, payload.values) if isinstance(k, ast.Constant)}
        if "startDateTime" in values:
            wired.append(call)
            assert values["startDateTime"].id == "team_elo_timestamp"
            assert kwargs["radiant_team_name"].id == "radiant_team_name_original"
            assert kwargs["dire_team_name"].id == "dire_team_name_original"
    assert len(wired) == 1


@pytest.mark.parametrize("value", [None, 0, -1, "bad", float("inf")])
def test_invalid_map_time_uses_current_time(monkeypatch, value):
    monkeypatch.setattr(W.time, "time", lambda: 123456)
    assert W.elo_evaluation_timestamp({"startDateTime": value}) == 123456
