"""Production boundaries for the general prematch winner model switch."""
from __future__ import annotations

import sys
from types import SimpleNamespace

import numpy as np
import pytest

from base import cyberscore_try as C
from base import laning_serving as _laning_serving_module
from base import ml_dispatch as md
from base import prematch_scorer as ps
from base import win_model_veto as V


def _bet_kwargs():
    return dict(match_key="dltv.org/matches/prematch-off.0", status="live",
                radiant_team_name="Team A", dire_team_name="Team B",
                live_league={}, top={}, mid={}, bot={}, protracker_payload=None,
                team_elo_block="", game_time_seconds=0.0, radiant_lead=0)


def _ctx(**kwargs):
    return md.Ctx(match_key="prematch-off", base_url="series", map_num=1,
                  game_time=600.0, radiant_team="Team A", dire_team="Team B",
                  heroes=None, elo_radiant=1500.0, elo_dire=1500.0, **kwargs)


def test_off_panel_and_normal_bet_builder_omit_general_ml(monkeypatch):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    block = {V.INDEX_KEY: -10.9, V.SOURCE_KEY: V.SOURCE_PREMATCH}
    model_line = C._format_win_model_line(block, all_model_line="🌐 All ML-модель: Dire 60%")
    message = C._build_prematch_model_bet_message(
        radiant_team_name="Team A", dire_team_name="Team B",
        target_team_name="Team B", live_league={}, top={}, mid={}, bot={},
        protracker_payload=None, team_elo_block="", game_time_seconds=0,
        radiant_lead=0, model_line=model_line, min_odds=1.7)
    assert "🤖 ML-модель:" not in message
    assert "🌐 All ML-модель:" in message
    assert "Ставить от кэфа 1.70" in message


def test_off_dispatch_ignores_prematch_even_if_configured(monkeypatch):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    config = md.Config.from_env({"PREMATCH_ML_ENABLED": "0",
                                 "ML_DISPATCH_WIN_MODELS": "prematch"})
    ctx = _ctx(prematch=md.ModelVerdict("Dire", 0.671))
    assert not [d for d in md.evaluate(ctx, config).decisions if d.market == "win"]
    assert C._ml_dispatch_prematch_pair(-17.1, V.SOURCE_PREMATCH) is None


def test_off_standalone_bet_does_not_reach_model_or_journal(monkeypatch):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    monkeypatch.setattr(C, "PREMATCH_MODEL_BET_ENABLED", True)
    reached = []
    monkeypatch.setattr(V, "model_bet", lambda *a: reached.append("model_bet"))
    monkeypatch.setattr(C, "_record_map_verdict", lambda *a, **k: reached.append("journal"))
    monkeypatch.setattr(C, "_deliver_and_persist_signal", lambda *a, **k: reached.append("send"))
    assert C._try_dispatch_prematch_model_bet(**_bet_kwargs()) is False
    assert reached == []


def test_off_star_delivery_does_not_require_general_ml_line(monkeypatch):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    monkeypatch.setenv("DISPATCH_MODE", "star")
    monkeypatch.setattr(C, "BET_REQUIRE_WIN_MODEL", True)
    text = "СТАВКА НА Team A x1\nTeam A VS Team B\nAll: Team A WR≈65.0% от кэфа 1.54"
    assert C._win_model_reject_for_delivery(
        text, {"target_side": "radiant", "stake_team_name": "Team A"}) is None
    assert C._win_model_reject_for_delivery(
        text + "\n🌐 All ML-модель: Dire 70.0%",
        {"target_side": "radiant", "stake_team_name": "Team A"}) is None
    assert V.blocks_veto(-1, {V.INDEX_KEY: 12.0, V.SOURCE_KEY: V.SOURCE_PREMATCH},
                         "all_output") is False


def test_off_does_not_call_general_prematch_loader(monkeypatch):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    monkeypatch.setattr(V, "_off_auxiliary_panels",
                        lambda *a: {"panel_text": "", "kills30": None})
    calls = []
    scorer = SimpleNamespace(get_model=lambda: calls.append("load"))
    monkeypatch.setitem(sys.modules, "prematch_scorer", scorer)
    monkeypatch.setitem(sys.modules, "base.prematch_scorer", scorer)
    monkeypatch.setattr(V, "win_index_draft", lambda *a: 0.0)
    radiant = {f"pos{i}": {"hero_id": i, "account_id": i} for i in range(1, 6)}
    dire = {f"pos{i}": {"hero_id": i + 5, "account_id": i + 5} for i in range(1, 6)}
    index, source, _ = V.win_prediction_ex(radiant, dire)
    assert (index, source) == (None, None)
    assert calls == []


def test_off_keeps_independent_panel_and_kills_verdicts(monkeypatch):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    calls = []
    monkeypatch.setitem(sys.modules, "prematch_panel_live", SimpleNamespace(
        evaluate_map=lambda *a, **k: calls.append((a, k)) or []))
    monkeypatch.setitem(sys.modules, "ml_panel", SimpleNamespace(
        best_of=lambda wins: None, render=lambda verdicts, highlight: "draft panel"))
    monkeypatch.setitem(sys.modules, "kills_transfer_serving", SimpleNamespace(
        forecast_probabilities=lambda *a: (0.6, 0.4, 0.5),
        manifest_history_date=lambda: None,
        render=lambda *a: "kills panel"))
    radiant = {f"pos{i}": {"hero_id": i, "account_id": i} for i in range(1, 6)}
    dire = {f"pos{i}": {"hero_id": i + 5, "account_id": i + 5} for i in range(1, 6)}
    index, source, details = V.win_prediction_ex(radiant, dire)
    assert (index, source) == (None, None)
    assert calls and calls[0][0][4:6] == (None, ())
    assert details["kills30"] == {"radiant": 0.6, "dire": 0.4, "total": 0.5}
    assert "kills panel" in details["panel_text"]


def test_off_late_delivery_gate_reads_fallback_verdict(monkeypatch):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    block = {V.DETAILS_KEY: {"refusal_reason": "prematch_ml_disabled",
                             "late": {"side": "Dire", "confidence": 0.65}}}
    assert "Late ML-модель (карта ≥36 мин): Dire 65.0%" in C._format_win_model_line(block)
    assert C._late_model_side_from_blocks(block) == "dire"


def test_off_removes_stale_general_ml_line_from_dispatch_message(monkeypatch):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    message = C._build_prematch_model_bet_message(
        radiant_team_name="Team A", dire_team_name="Team B",
        target_team_name="Team B", live_league={}, top={}, mid={}, bot={},
        protracker_payload=None, team_elo_block="", game_time_seconds=0,
        radiant_lead=0, model_line="🤖 ML-модель: Dire 60.9%",
        full_message_text="СТАВКА НА Team A x1\n🤖 ML-модель: Dire 60.9%\nELO: Dire",
        min_odds=1.7)
    assert "🤖 ML-модель:" not in message
    assert "ELO: Dire" in message
    assert "Ставить от кэфа 1.70" in message


def test_enabled_restores_panel_dispatch_and_gate(monkeypatch):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "1")
    block = {V.INDEX_KEY: -10.9, V.SOURCE_KEY: V.SOURCE_PREMATCH}
    assert "🤖 ML-модель: Dire 60.9%" in C._format_win_model_line(block)
    assert C._ml_dispatch_prematch_pair(-17.1, V.SOURCE_PREMATCH) == {
        "side": "Dire", "confidence": pytest.approx(0.671)}
    config = md.Config.from_env({"PREMATCH_ML_ENABLED": "1"})
    assert "prematch" in config.win_models
    assert [d for d in md.evaluate(_ctx(prematch=md.ModelVerdict("Dire", 0.671)),
                                   config).decisions if d.market == "win"]
    assert C._win_model_reject_for_delivery("СТАВКА НА Team A x1\nTeam A VS Team B",
                                            {"target_side": "radiant"}) == {
        "reason": "model_missing"}


# --- E-296 guard without the winner model (PREMATCH_ML_ENABLED=0) -----------
#
# Synthetic npz with the real key names ("accounts", "acc_pos"). The light
# checker reads only these two keys; "accounts" carries columns 0 and 2
# (account id, games — same layout the model sees: elo sits at 1) — the
# full 19-column width is a PrematchModel requirement, not the guard's.
_CONFLICT_RADIANT_ACCS = [101, 102, 103, 104, 105]
_CONFLICT_DIRE_ACCS = [201, 202, 203, 204, 205]
_CONFLICT_HARD_SLOTS = [(104, 4, 5), (105, 5, 2), (203, 3, 4)]


def _write_position_npz(path):
    accounts = np.array([[a, 1500, 100] for a in
                         _CONFLICT_RADIANT_ACCS + _CONFLICT_DIRE_ACCS],
                        dtype=np.int64)
    rows = []
    usual = {104: 5, 105: 2, 203: 4}
    for i, a in enumerate(_CONFLICT_RADIANT_ACCS + _CONFLICT_DIRE_ACCS):
        assigned = (i % 5) + 1
        rows.append([a, usual.get(a, assigned), 100.0])
    acc_pos = np.array(rows, dtype=np.float64)
    np.savez(str(path), accounts=accounts, acc_pos=acc_pos)
    return path


def _conflict_sides():
    radiant = {f"pos{i + 1}": {"hero_id": 10 + i, "account_id": a}
               for i, a in enumerate(_CONFLICT_RADIANT_ACCS)}
    dire = {f"pos{i + 1}": {"hero_id": 20 + i, "account_id": a}
            for i, a in enumerate(_CONFLICT_DIRE_ACCS)}
    return radiant, dire


def _point_light_checker_at(monkeypatch, tmp_path):
    npz = _write_position_npz(tmp_path / "pos_guard.npz")
    monkeypatch.setattr(ps, "ARTIFACT_PATH", str(npz))
    ps._reset_light_position_tables()
    return npz


class _FakeLedger:
    def __init__(self):
        self._keys = set()

    def as_set(self):
        return set(self._keys)

    def add(self, key):
        self._keys.add(tuple(key))

    def save(self):
        pass


def _call_tick_with_details(monkeypatch, details, radiant, dire):
    monkeypatch.setenv("DISPATCH_MODE", "ml")
    monkeypatch.setattr(C, "_match_has_tier1_team", lambda *a: True)
    monkeypatch.setattr(_laning_serving_module, "verdicts", lambda *a, **k: {
        "all": {"side": "Radiant", "confidence": 0.6222},
        "lane": {"side": "Radiant", "confidence": 0.5955}})
    delivered, logged = [], []
    monkeypatch.setattr(C, "_ml_dispatch_sent_ledger", lambda: _FakeLedger())
    monkeypatch.setattr(C, "_ml_dispatch_record_decisions",
                        lambda record, *, dedup_view: logged.append(record))
    monkeypatch.setattr(C, "_deliver_and_persist_signal",
                        lambda *a, **k: delivered.append((a, k)) or True)
    C._ml_dispatch_tick(
        match_key="dltv.org/matches/8995525359.11",
        radiant_team_name="VooDooSh Club", dire_team_name="Stariy_Bog Club",
        live_league={}, top="", mid="", bot="", protracker_payload=None,
        team_elo_block="", team_elo_meta={}, game_time_seconds=650.0,
        radiant_lead=0,
        early_output={C.win_model_veto.DETAILS_KEY: details},
        radiant_heroes_and_pos=radiant, dire_heroes_and_pos=dire,
        full_message_text="VooDooSh Club VS Stariy_Bog Club")
    return delivered, logged


def test_off_position_conflict_drops_ml_dispatch_like_e296(monkeypatch, tmp_path):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    monkeypatch.setattr(V, "_off_auxiliary_panels",
                        lambda *a: {"panel_text": "", "kills30": None})
    _point_light_checker_at(monkeypatch, tmp_path)
    radiant, dire = _conflict_sides()
    index, source, details = V.win_prediction_ex(radiant, dire)
    assert (index, source) == (None, None)
    assert details["reason"] == "prematch_ml_disabled"
    assert [tuple(s) for s in details["position_mismatch"]] == _CONFLICT_HARD_SLOTS
    delivered, logged = _call_tick_with_details(
        monkeypatch,
        {"refusal_reason": details["reason"],
         "position_mismatch": details["position_mismatch"],
         "late": {"side": "Radiant", "confidence": 0.70}},
        radiant, dire)
    assert delivered == []
    row = logged[0]
    assert row["decisions"] == []
    assert any(s["market"] == "win" and s["reason"] == "position_mismatch"
               for s in row["skipped"])
    assert row["draft_input"]["position_mismatch"] == [list(x) for x in
                                                       _CONFLICT_HARD_SLOTS]


def test_off_normal_positions_let_decision_out(monkeypatch, tmp_path):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    monkeypatch.setattr(V, "_off_auxiliary_panels",
                        lambda *a: {"panel_text": "", "kills30": None})
    _point_light_checker_at(monkeypatch, tmp_path)
    radiant = {f"pos{i + 1}": {"hero_id": 10 + i, "account_id": a}
               for i, a in enumerate([101, 102, 103, 206, 207])}
    dire = {f"pos{i + 1}": {"hero_id": 20 + i, "account_id": a}
            for i, a in enumerate([201, 202, 208, 204, 205])}
    # 206/207/208 are unknown to the snapshot: precondition fails open.
    _, _, details = V.win_prediction_ex(radiant, dire)
    assert details["position_mismatch"] is None
    delivered, logged = _call_tick_with_details(
        monkeypatch,
        {"refusal_reason": details["reason"],
         "position_mismatch": details["position_mismatch"],
         "late": {"side": "Radiant", "confidence": 0.70}},
        radiant, dire)
    assert delivered != [] or logged[0]["decisions"] != []


def test_off_light_checker_never_constructs_scorer(monkeypatch, tmp_path):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "0")
    _point_light_checker_at(monkeypatch, tmp_path)

    def _boom(*a, **k):
        raise AssertionError("PrematchScorer must not load off-path")

    monkeypatch.setattr(ps, "PrematchModel", _boom)
    monkeypatch.setattr(ps, "get_model", _boom)
    radiant, dire = _conflict_sides()
    ra = [int(radiant[f"pos{i}"]["account_id"]) for i in range(1, 6)]
    da = [int(dire[f"pos{i}"]["account_id"]) for i in range(1, 6)]
    assert ps.light_position_hard_slots(ra, da) == _CONFLICT_HARD_SLOTS


def test_light_checker_rereads_rebuilt_artifact(monkeypatch, tmp_path):
    # serv1 24.09: the snapshot choice flipped 4 of 11 recorded refusals, so a
    # nightly rebuild must reach the guard without a restart.
    import os
    npz = _point_light_checker_at(monkeypatch, tmp_path)
    radiant, dire = _conflict_sides()
    ra = [int(radiant[f"pos{i}"]["account_id"]) for i in range(1, 6)]
    da = [int(dire[f"pos{i}"]["account_id"]) for i in range(1, 6)]
    assert ps.light_position_hard_slots(ra, da) == _CONFLICT_HARD_SLOTS
    accounts = np.array([[a, 1500, 100] for a in ra + da], dtype=np.int64)
    acc_pos = np.array([[a, (i % 5) + 1, 100.0] for i, a in enumerate(ra + da)],
                       dtype=np.float64)
    np.savez(str(npz), accounts=accounts, acc_pos=acc_pos)   # rebuilt: no conflicts
    st = os.stat(npz)
    os.utime(npz, (st.st_atime, st.st_mtime + 60))
    assert ps.light_position_hard_slots(ra, da) == _CONFLICT_HARD_SLOTS  # within 10 min
    real_time = ps.time.time
    monkeypatch.setattr(ps.time, "time", lambda: real_time() + 601)
    assert ps.light_position_hard_slots(ra, da) == []
    os.remove(npz)                                            # mid-rename: keep last tables
    monkeypatch.setattr(ps.time, "time", lambda: real_time() + 1300)
    assert ps.light_position_hard_slots(ra, da) == []


def test_enabled_late_side_matches_head_on_refused_map(monkeypatch):
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "1")
    block = {V.DETAILS_KEY: {"refusal_reason": "prematch_ml_disabled",
                             "late": {"side": "Dire", "confidence": 0.65}}}
    assert C._late_model_side_from_blocks(block) is None
