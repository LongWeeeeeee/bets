"""Orphan live-ELO sweep: map duration comes from the OpenDota reply that gave the winner.

Ledger review 06.10.2026 (card ingame-jzzx): 25 of 41 live-applied maps carried
`duration_seconds=None`, all 9 maps applied by the orphan sweep among them.
The sweep asked OpenDota for `radiant_win` only; the score advance then took
its duration from the Stratz disk cache, which is empty for Stratz-null maps,
so variant A used m = 1.0 instead of clip(1981 / max(dur, 600), 0.6, 1.6).
A failed OpenDota lookup (429 quota vs not-yet-parsed) returned None silently.

Input: the CAPTURED OpenDota /api/matches response, see
fixtures/opendota_match_8830340000_20260919.README.md. Only the outbound HTTP
(`requests.get`) and the Stratz cache (Stratz-null by construction) are
replaced; the sweep, the ELO finalize, the drain and the A update are real.
"""
from __future__ import annotations

import functools
import gzip
import json
import logging
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as cs  # noqa: E402
from ELO import live_team_strength as live  # noqa: E402
from ELO.config import HybridEloConfig  # noqa: E402
from ELO.domain import LeagueTier, MatchRecord  # noqa: E402
from ELO.models import HybridPlayerRosterEloModel  # noqa: E402

FIXTURE = BASE_DIR / "tests/fixtures/opendota_match_8830340000_20260919.json.gz"
MATCH_ID = 8830340000


def _opendota_payload() -> dict:
    with gzip.open(FIXTURE, "rt", encoding="utf-8") as fh:
        return json.load(fh)


class _Resp:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if self._payload is None:
            raise ValueError("no body")
        return self._payload


def _reset_live_caches() -> None:
    live._SNAPSHOT_CACHE = None
    live._MODEL_FROM_SNAPSHOT_CACHE.clear()
    live._RUNTIME_SNAPSHOT_CACHE["base_snapshot_id"] = None
    live._RUNTIME_SNAPSHOT_CACHE["runtime_signature"] = None
    live._RUNTIME_SNAPSHOT_CACHE["snapshot"] = None
    live._LIVE_PROBABILITY_POLICY_CACHE["path"] = None
    live._LIVE_PROBABILITY_POLICY_CACHE["signature"] = None
    live._LIVE_PROBABILITY_POLICY_CACHE["policy"] = None


def _live_env(root: Path) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    data_dir = root / "data"
    data_dir.mkdir(exist_ok=True)
    snapshot_path = root / "live_snapshot.json"
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    snapshot_path.write_text(json.dumps({
        "meta": {**live._rating_replay_meta(), "reference_timestamp": 1771153251},
        "teams_by_org_key": {},
        "model_state": model.export_state(),
    }), encoding="utf-8")
    return dict(
        data_dir=data_dir, snapshot_path=snapshot_path,
        progress_path=root / "live_progress.json",
        runtime_model_state_path=root / "live_model_state.json",
        runtime_lock_path=root / "live_state.lock",
        rebuild_if_missing=False,
    )


def _sourcetv_record() -> MatchRecord:
    return MatchRecord(
        match_id=MATCH_ID, timestamp=1771153200, radiant_win=False,
        radiant_team_id=1, radiant_team_name="Elegia",
        dire_team_id=2, dire_team_name="Team Mariachi",
        radiant_player_ids=(1, 2, 3, 4, 5), dire_player_ids=(6, 7, 8, 9, 10),
        league_id=11, league_name="Test League", source_league_tier="TIER2",
        series_id=None, series_type="3", derived_league_tier=LeagueTier.TIER2,
    )


@pytest.fixture
def orphan_env(tmp_path, monkeypatch):
    """A sourcetv series with one pending map, wired to the real ELO finalize."""
    _reset_live_caches()
    env = _live_env(tmp_path)
    series_url = f"dltv.org/matches/{MATCH_ID}"
    registered = live.register_live_map_context(
        series_key=str(MATCH_ID), series_url=series_url,
        map_key=f"{series_url}.0", first_team_score=0, second_team_score=0,
        first_team_is_radiant=True, match_record=_sourcetv_record(), **env)
    assert registered is not None and registered["applied_update"] is None

    monkeypatch.setattr(cs, "ELO_LIVE_SNAPSHOT_AVAILABLE", True)
    monkeypatch.setattr(cs, "_elo_live_default_progress_path", env["progress_path"])
    monkeypatch.setattr(
        cs, "_elo_live_finalize_series_from_scores",
        functools.partial(live.finalize_live_series_from_scores, **env))
    monkeypatch.setattr(cs, "LIVE_ELO_ORPHAN_PENDING_MIN_AGE_SECONDS", 0)
    # A Stratz-null map: neither the outcome nor the duration is in the cache.
    monkeypatch.setattr(cs, "_live_elo_winner_lookup", lambda *a, **k: None)
    monkeypatch.setattr(cs, "_drop_delayed_match", lambda *a, **k: True)
    monkeypatch.setattr(cs, "_OPENDOTA_ORPHAN_LOG_LAST", {}, raising=False)
    yield env
    _reset_live_caches()


def _player_a(env: dict) -> float:
    state = json.loads(env["runtime_model_state_path"].read_text())
    return HybridPlayerRosterEloModel.from_state(state["model_state"]).player_a[1]


def test_orphan_sweep_threads_opendota_duration_into_variant_a(orphan_env, monkeypatch):
    payload = _opendota_payload()
    assert payload["match_id"] == MATCH_ID and payload["radiant_win"] is True
    duration = payload["duration"]
    multiplier = max(0.6, min(1.6, 1981 / max(duration, 600)))
    assert multiplier != pytest.approx(1.0)  # the fixture must distinguish A from m = 1.0

    calls = []

    def _get(url, **kwargs):
        calls.append(url)
        return _Resp(200, payload)

    monkeypatch.setattr(cs.requests, "get", _get)

    finalized = cs._finalize_orphaned_live_elo_series(set())

    assert calls == [f"https://api.opendota.com/api/matches/{MATCH_ID}"]
    assert len(finalized) == 1
    assert finalized[0]["winner_slot"] == "first"
    # Boundary 1: the A update itself (K-multiplied rating step of the radiant side).
    assert _player_a(orphan_env) == pytest.approx(1500 + 36 * multiplier)
    assert _player_a(orphan_env) != pytest.approx(1500 + 36 * 1.0)
    # Boundary 2: the ledger row that the nightly rebase replays.
    progress = json.loads(orphan_env["progress_path"].read_text())
    rows = list(progress["applied_maps"].values())
    assert len(rows) == 1 and rows[0]["duration_seconds"] == duration


@pytest.mark.parametrize("status", [429, 404])
def test_orphan_sweep_logs_opendota_status_and_applies_nothing(orphan_env, monkeypatch, caplog, status):
    monkeypatch.setattr(cs.requests, "get", lambda url, **kw: _Resp(status))

    with caplog.at_level(logging.WARNING):
        finalized = cs._finalize_orphaned_live_elo_series(set())

    assert finalized == []
    assert not orphan_env["runtime_model_state_path"].exists()
    progress = json.loads(orphan_env["progress_path"].read_text())
    assert progress["applied_maps"] == {}
    assert str(MATCH_ID) in progress["pending_series"]  # still queued for the next cycle
    assert any(f"HTTP {status}" in rec.getMessage() and str(MATCH_ID) in rec.getMessage()
               for rec in caplog.records), caplog.text


def test_failure_log_is_not_repeated_every_cycle(monkeypatch, caplog):
    monkeypatch.setattr(cs, "_OPENDOTA_ORPHAN_LOG_LAST", {}, raising=False)
    monkeypatch.setattr(cs.requests, "get", lambda url, **kw: _Resp(429))
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            assert cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None) is None
    assert sum("HTTP 429" in r.getMessage() for r in caplog.records) == 1


def test_fetch_keeps_its_return_contract(monkeypatch):
    payload = _opendota_payload()
    monkeypatch.setattr(cs.requests, "get", lambda url, **kw: _Resp(200, payload))
    assert cs._fetch_finished_sourcetv_series_scores(MATCH_ID, {"first": 1, "second": 0}) == (2, 0)
    out: dict = {}
    assert cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None, result_out=out) == (1, 0)
    assert out == {"radiant_win": True, "duration_seconds": payload["duration"], "match_id": MATCH_ID}


def test_stratz_values_are_only_filled_never_overridden():
    fallback = {"radiant_win": True, "duration_seconds": 3275, "match_id": MATCH_ID}
    pending = {"match_record": {"match_id": MATCH_ID}}
    fill = cs._fill_missing_duration_from_opendota
    # Stratz miss -> OpenDota fills both fields.
    assert fill(None, pending, fallback) == {"radiant_won": True, "duration_seconds": 3275}
    # Stratz knows the outcome but not the duration -> only the duration is added.
    assert fill({"radiant_won": True, "duration_seconds": None}, pending, fallback) == {
        "radiant_won": True, "duration_seconds": 3275}
    # Stratz duration present -> untouched.
    stratz = {"radiant_won": True, "duration_seconds": 3300}
    assert fill(stratz, pending, fallback) is stratz
    # Disagreeing outcome or another map of the queue -> untouched.
    disagree = {"radiant_won": False, "duration_seconds": None}
    assert fill(disagree, pending, fallback) is disagree
    assert fill(None, {"match_record": {"match_id": MATCH_ID + 1}}, fallback) is None
    assert fill(None, pending, None) is None


def test_orphan_sweep_rejects_opendota_answer_for_another_match(orphan_env, monkeypatch, caplog):
    # OpenDota answering with a different match must not settle this map: neither the
    # winner nor the duration of another game may reach the ledger (astra review 07.10).
    payload = dict(_opendota_payload())
    payload["match_id"] = MATCH_ID + 1
    monkeypatch.setattr(cs.requests, "get", lambda url, **kw: _Resp(200, payload))

    with caplog.at_level(logging.WARNING):
        finalized = cs._finalize_orphaned_live_elo_series(set())

    assert finalized == []
    assert not orphan_env["runtime_model_state_path"].exists()
    progress = json.loads(orphan_env["progress_path"].read_text())
    assert progress["applied_maps"] == {}
    assert str(MATCH_ID) in progress["pending_series"]
    assert any("another match_id" in rec.getMessage() for rec in caplog.records), caplog.text
    out: dict = {}
    assert cs._fetch_finished_sourcetv_series_scores(MATCH_ID, None, result_out=out) is None
    assert out == {}


@pytest.mark.parametrize("advance", [0.0, 3600.0])
def test_failure_log_throttle_stays_bounded(monkeypatch, advance):
    # One key per (match_id, reason) for weeks of uptime must not grow without limit.
    monkeypatch.setattr(cs, "_OPENDOTA_ORPHAN_LOG_LAST", {}, raising=False)
    clock = [1_800_000_000.0]
    monkeypatch.setattr(cs.time, "time", lambda: clock[0])
    for i in range(5000):
        cs._log_opendota_orphan_lookup_failure(9_000_000_000 + i, "HTTP 429")
        clock[0] += advance / 5000
    assert len(cs._OPENDOTA_ORPHAN_LOG_LAST) <= cs._OPENDOTA_ORPHAN_LOG_MAX_KEYS
