"""Разовый допуск анонимной карты по глазному подтверждению (опция C).

10.09.2026, Dawn Bulls — Kalmychata: мост несёт полностью анонимную карту
тикета 10877, составы по тегам (Lynx 4/5 split-id, Bald 3/5 weak) пару
Dawn Bulls–Kalmychata не подтверждают, а переименовывать по тегам запрещено.
Пользователь опознал матч глазами (DotaTV) + якорь: persona `Ankou ♡`
(account 1675517497) в dire-составе 8991706137 — единственная в мосте.

Контракт ручной записи (runtime/manual_sourcetv_admissions.json, в репо
не коммитится, читается каждый цикл):
- имена подставляются ТОЛЬКО в стороны-плейсхолдеры (правду GC не трогаем);
- id не выдумываются (остаются нули из моста);
- запись с прошедшим expires_at мертва;
- лига обязана быть allowlist-id или гейтовым тикетом + титул вне
  SKIPPED_LIVE_LEAGUE_TITLES (deny-гейты не обходятся);
- плейсхолдеры в самой записи — отказ.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402

MATCH = "8991706137"
LEAGUE_ID = 10877
LEAGUE_NAME = "Challengermode Daily Tournaments"
NOW = time.time()


def _row(radiant="Radiant", dire="Dire", game_time=1500):
    return {
        "match_id": 8991706137,
        "league_id": LEAGUE_ID,
        "league_name": LEAGUE_NAME,
        "radiant_team_name": radiant,
        "radiant_team_id": 0,
        "dire_team_name": dire,
        "dire_team_id": 0,
        "series_game_number": 1,
        "series_type": 0,
        "radiant_series_wins": 0,
        "dire_series_wins": 0,
        "game_time": game_time,
        "radiant_score": 26,
        "dire_score": 13,
        "status": "live",
    }


def _write(tmp_path, payload):
    path = tmp_path / "manual.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _live_entry(**overrides):
    entry = {
        "radiant": "Dawn Bulls",
        "dire": "Kalmychata",
        "by": "test",
        "expires_at": NOW + 3600,
    }
    entry.update(overrides)
    return entry


class TestManualAdmissionHit:
    def test_hit_returns_names(self, tmp_path, monkeypatch):
        path = _write(tmp_path, {MATCH: _live_entry()})
        monkeypatch.setenv("MANUAL_SOURCETV_ADMISSION_PATH", str(path))
        hit = cs._manual_sourcetv_admission_for(MATCH, _row())
        assert hit is not None and hit["radiant"] == "Dawn Bulls"
        assert hit["dire"] == "Kalmychata"

    def test_expired_entry_is_dead(self, tmp_path, monkeypatch):
        path = _write(tmp_path, {MATCH: _live_entry(expires_at=NOW - 10)})
        monkeypatch.setenv("MANUAL_SOURCETV_ADMISSION_PATH", str(path))
        assert cs._manual_sourcetv_admission_for(MATCH, _row()) is None

    def test_missing_file_is_dead(self, tmp_path, monkeypatch):
        monkeypatch.setenv(
            "MANUAL_SOURCETV_ADMISSION_PATH", str(tmp_path / "absent.json"))
        assert cs._manual_sourcetv_admission_for(MATCH, _row()) is None

    def test_placeholder_names_in_entry_refused(self, tmp_path, monkeypatch):
        path = _write(tmp_path, {MATCH: _live_entry(radiant="Radiant")})
        monkeypatch.setenv("MANUAL_SOURCETV_ADMISSION_PATH", str(path))
        assert cs._manual_sourcetv_admission_for(MATCH, _row()) is None

    def test_unwanted_league_refused(self, tmp_path, monkeypatch):
        path = _write(tmp_path, {MATCH: _live_entry()})
        monkeypatch.setenv("MANUAL_SOURCETV_ADMISSION_PATH", str(path))
        row = _row()
        row["league_id"] = 99999
        row["league_name"] = "Some Random Cup"
        assert cs._manual_sourcetv_admission_for(MATCH, row) is None

    def test_skipped_title_refused(self, tmp_path, monkeypatch):
        path = _write(tmp_path, {MATCH: _live_entry()})
        monkeypatch.setenv("MANUAL_SOURCETV_ADMISSION_PATH", str(path))
        row = _row()
        row["league_name"] = "BLAST Slam VII: China Open Qualifier 2"
        assert cs._manual_sourcetv_admission_for(MATCH, row) is None


class TestManualIdentityFill:
    def test_fills_placeholders_only(self, tmp_path, monkeypatch):
        path = _write(tmp_path, {MATCH: _live_entry()})
        monkeypatch.setenv("MANUAL_SOURCETV_ADMISSION_PATH", str(path))
        matches = {MATCH: _row()}
        cs._resolve_sourcetv_bridge_identity(matches)
        assert matches[MATCH]["radiant_team_name"] == "Dawn Bulls"
        assert matches[MATCH]["dire_team_name"] == "Kalmychata"

    def test_keeps_real_gc_names(self, tmp_path, monkeypatch):
        path = _write(tmp_path, {MATCH: _live_entry()})
        monkeypatch.setenv("MANUAL_SOURCETV_ADMISSION_PATH", str(path))
        matches = {MATCH: _row(radiant="Some Real Team")}
        cs._resolve_sourcetv_bridge_identity(matches)
        assert matches[MATCH]["radiant_team_name"] == "Some Real Team"
        assert matches[MATCH]["dire_team_name"] == "Kalmychata"


class TestLeagueGateSkip:
    def test_unknown_league_skipped(self):
        row = _row()
        row["league_id"] = 99999
        row["league_name"] = "Some Random Cup"
        assert cs._sourcetv_league_gate_skip(row) is True

    def test_fully_anonymous_ticket_skipped_without_manual(self):
        assert cs._sourcetv_league_gate_skip(_row()) is True

    def test_allowlisted_league_passes(self):
        row = _row()
        row["league_id"] = 19722
        assert cs._sourcetv_league_gate_skip(row) is False
