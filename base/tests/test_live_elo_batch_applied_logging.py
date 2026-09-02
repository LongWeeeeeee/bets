"""Живой ELO: пакет применённых карт логируется целиком, а не первой картой.

`ELO/live_team_strength.py` (слияние сегодня утром) отдаёт из
`register_live_map_context`/`finalize_live_series_from_scores` не только
одиночный `applied_update`, но и `applied_updates` — список, когда одним
вызовом применяется НЕСКОЛЬКО карт подряд (пропущенные обновления, догоняющая
очередь). Три места потребления в `cyberscore_try.py` читали только
одиночный `applied_update` и молча теряли остальные карты пакета:
`_sweep_orphaned_live_elo` (через `_finalize_orphaned_live_elo_series`),
call-сайт `_finalize_finished_live_series_for_elo` («Live ELO finalized from
finished series») и call-сайт `_register_completed_live_map_for_elo` («Live
ELO updated from completed map»).

Общая точка исправления — `_applied_updates_from_result`: предпочитает
`applied_updates`, при его отсутствии/пустоте отдаёт `[applied_update]`.
"""
from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402


def _update(map_key: str) -> dict:
    return {
        "map_key": map_key, "winner_slot": "first", "winner_team_name": "A",
        "first_team_name": "A", "second_team_name": "B",
        "series_score_before": {"first": 0, "second": 0},
        "series_score_after": {"first": 1, "second": 0},
        "k_global": 0.0, "k_local": 0.0, "k_roster": 0.0,
    }


# ---------------------------------------------------------------------------
# _applied_updates_from_result: общая точка выбора пакет/одиночный/пусто
# ---------------------------------------------------------------------------

def test_applied_updates_from_result_prefers_batch_list():
    u1, u2 = _update("m1"), _update("m2")
    result = {"applied_update": u1, "applied_updates": [u1, u2]}
    assert cs._applied_updates_from_result(result) == [u1, u2]


def test_applied_updates_from_result_falls_back_to_singular():
    u1 = _update("m1")
    result = {"applied_update": u1, "applied_updates": []}
    assert cs._applied_updates_from_result(result) == [u1]

    result_no_list_key = {"applied_update": u1}
    assert cs._applied_updates_from_result(result_no_list_key) == [u1]


def test_applied_updates_from_result_empty_when_nothing_applied():
    assert cs._applied_updates_from_result({"applied_update": None, "applied_updates": []}) == []
    assert cs._applied_updates_from_result(None) == []
    assert cs._applied_updates_from_result("not a dict") == []


# ---------------------------------------------------------------------------
# _sweep_orphaned_live_elo: стубленный результат с ДВУМЯ картами -> два лога
# ---------------------------------------------------------------------------

def test_sweep_orphaned_live_elo_logs_every_map_in_a_two_map_batch(monkeypatch):
    u1, u2 = _update("dltv.org/matches/1.0"), _update("dltv.org/matches/2.0")
    stub_result = {
        "series_key": "s", "series_url": "u", "current_scores": {"first": 2, "second": 0},
        "winner_slot": "first", "applied_update": u1, "applied_updates": [u1, u2],
        "age_seconds": 900,
    }
    monkeypatch.setattr(cs, "_finalize_orphaned_live_elo_series", lambda seen: [stub_result])

    logged: list[tuple[str, dict]] = []
    monkeypatch.setattr(cs, "_emit_live_elo_applied_log",
                        lambda prefix, payload: logged.append((prefix, payload)))

    applied = cs._sweep_orphaned_live_elo(set(), "test")

    assert applied == 2, f"обязаны применить обе карты пакета, применили {applied}"
    assert len(logged) == 2, f"обязаны залогировать обе карты, залогировали {len(logged)}"
    assert [p.get("map_key") for _, p in logged] == ["dltv.org/matches/1.0", "dltv.org/matches/2.0"]
    assert all(prefix == "Live ELO finalized from orphaned finished series" for prefix, _ in logged)


def test_sweep_orphaned_live_elo_falls_back_to_single_update(monkeypatch):
    """Старый/частичный результат без `applied_updates` — по-прежнему одна карта."""
    u1 = _update("dltv.org/matches/1.0")
    stub_result = {
        "series_key": "s", "series_url": "u", "current_scores": {"first": 1, "second": 0},
        "winner_slot": "first", "applied_update": u1, "age_seconds": 900,
    }
    monkeypatch.setattr(cs, "_finalize_orphaned_live_elo_series", lambda seen: [stub_result])

    logged: list[tuple[str, dict]] = []
    monkeypatch.setattr(cs, "_emit_live_elo_applied_log",
                        lambda prefix, payload: logged.append((prefix, payload)))

    applied = cs._sweep_orphaned_live_elo(set(), "test")
    assert applied == 1
    assert len(logged) == 1
    assert logged[0][1]["map_key"] == "dltv.org/matches/1.0"
