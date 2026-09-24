"""`no_account_no_org` — сигнал заблокирован, как настоящий отказ модели.

Владелец (18.09.2026): прод-журнал `runtime/prematch_model_eval.jsonl` за
17.09+ показал 52 из 497 боевых карт на ветке `no_account_no_org` (7 колонок,
ни аккаунтов, ни организации не опознано) с `bet=True` и уверенностью
0.62-0.66 — хотя `_PREMATCH_BET_BRANCHES` (WIN_MODEL_VETO_PREMATCH_BRANCHES,
дефолт "full,no_org") самостоятельную ставку с этой ветки и так не пускал.
Панель, вето (`blocks_veto`/`draft_veto`) и `_win_model_reject_for_delivery`
читают индекс модели независимо от `_PREMATCH_BET_BRANCHES`, поэтому ветка
всё равно вела себя как валидный сигнал.

`win_model_veto._prematch_index` теперь возвращает `None` для этой ветки —
ровно тот же контракт, что у любого другого отказа модели
(`win_prediction_ex` уходит в `_LAST_REFUSAL`, `functions.py` строит
fallback-карточку без ML-строки поддержки/вето). Журнал при этом сохраняет
диагностику (index/confidence/calibration), но `bet=False` и
`reason="no_account_no_org_blocked"`. Откат — `PREMATCH_ML_NO_ACCOUNT_NO_ORG_
BET=1` (читается один раз при импорте модуля в `_NO_ACCOUNT_NO_ORG_BET_
ENABLED`, поэтому тест патчит сам флаг, а не переменную окружения).
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from base import win_model_veto as veto  # noqa: E402
from base import prematch_scorer as ps_module  # noqa: E402

RADIANT_DICT = {f"pos{i}": {"account_id": 100 + i, "hero_id": i} for i in range(1, 6)}
DIRE_DICT = {f"pos{i}": {"account_id": 200 + i, "hero_id": 10 + i} for i in range(1, 6)}


def _fake_res(branch):
    return SimpleNamespace(
        probability=0.63, branch=branch,
        features={"draft_logit": 0.1}, missing_keys=["account", "org"],
        parts={"draft": 0.63}, lan_winrate=0.62,
        coverage={"filled": 1.0, "cells": 1.0, "pos": 1.0, "h2h": False},
    )


def _fake_model(branch):
    return SimpleNamespace(
        branches={}, cal={}, features=["draft_logit"],
        mu=[0.0], sd=[1.0], coef=[0.1],
        artifact_sha256="deadbeef", snapshot_ts=1_700_000_000,
        score=lambda **kw: _fake_res(branch),
    )


def _run(monkeypatch, branch, *, env_enabled):
    """Drive `_prematch_index` with the heavy real dependencies stubbed out.

    Late/early_nw/early_win, the kills-window panel and the ml_panel live
    board are all off the critical path for this decision (fail-soft in the
    real function too) — stubbed so the test is deterministic and fast.
    """
    monkeypatch.setenv("PREMATCH_ML_ENABLED", "1")
    monkeypatch.setattr(veto, "win_index_draft", lambda *a, **k: 5.0)
    monkeypatch.setattr(ps_module, "get_model", lambda: _fake_model(branch))
    monkeypatch.setattr(veto, "_NO_ACCOUNT_NO_ORG_BET_ENABLED", env_enabled)
    calls = []
    monkeypatch.setattr(veto, "_journal_eval", lambda **kw: calls.append(kw))
    for name in ("late_win_model", "early_nw_win_model", "early_win_model"):
        monkeypatch.setitem(sys.modules, name,
                            SimpleNamespace(verdict=lambda h: None, load_error=lambda: None))
    monkeypatch.setitem(sys.modules, "prematch_panel_live",
                        SimpleNamespace(evaluate_map=lambda *a, **k: [],
                                        ENABLED=False, _state={}))
    monkeypatch.setitem(sys.modules, "ml_panel",
                        SimpleNamespace(best_of=lambda w: None,
                                        render=lambda w, highlight=None: "",
                                        append_journal=lambda row: None,
                                        journal_row=lambda *a, **k: {}))
    monkeypatch.setitem(sys.modules, "kills_transfer_serving",
                        SimpleNamespace(forecast_probabilities=lambda *a, **k: (0.5, 0.5, 0.0),
                                        manifest_history_date=lambda: None,
                                        render=lambda *a, **k: ""))
    veto._LAST_REFUSAL.clear()
    index = veto._prematch_index(RADIANT_DICT, DIRE_DICT,
                                 "Radiant Team", "Dire Team", {})
    assert len(calls) == 1
    return index, calls[0]


def test_no_account_no_org_blocked_by_default(monkeypatch):
    index, rec = _run(monkeypatch, "no_account_no_org", env_enabled=False)
    assert index is None
    assert rec["bet"] is False
    assert rec["reason"] == "no_account_no_org_blocked"
    # Диагностика в журнале не теряется — только сама ставка отменена.
    # probability=0.63 в fake res -> idx = (0.63-0.5)*100 = 13.0.
    assert rec["branch"] == "no_account_no_org"
    assert rec["index"] == 13.0
    assert veto._LAST_REFUSAL["reason"] == "ветка no_account_no_org — сигнал заблокирован"


def test_no_account_no_org_old_behaviour_under_kill_switch(monkeypatch):
    index, rec = _run(monkeypatch, "no_account_no_org", env_enabled=True)
    assert index == 13.0
    assert rec["bet"] is True                 # |13.0| >= _PREMATCH_MIN_INDEX (8)
    assert rec["reason"] == "ok"


def test_full_branch_unaffected_by_the_block(monkeypatch):
    index, rec = _run(monkeypatch, "full", env_enabled=False)
    assert index == 13.0
    assert rec["reason"] == "ok"
    assert rec["branch"] == "full"
