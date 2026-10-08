"""Live-ELO registration must honour the test isolation of the progress path.

Incident 23.08.2026: a suite run on serv1 wiped `pending_series` in the PROD
`runtime/live_elo_progress.json`; `conftest._isolate_live_elo_progress` was the
fix, but it patches the module attribute `live_team_strength.DEFAULT_RUNTIME_*`
while `register_live_map_context` / `finalize_live_series_from_scores` bound
the same values as parameter defaults at import. `cyberscore_try.
_register_completed_live_map_for_elo` passes no paths, so every unit test that
registered a map wrote the checkout's real `runtime/live_elo_progress.json`
(08.10.2026: test keys `dltv.org/matches/8900882416`, `test-match`, ... seen in
real runtime files; the documented serv1 suite command runs in /root/main).

These tests assert at the boundary: every file the registration writes (the
`_write_json_atomic` destinations) lies under the test's tmp dir, and the series
lands in the file the conftest points `LIVE_ELO_PROGRESS` at. Writes OUTSIDE tmp
are redirected to a sink so a RED run on the unfixed module cannot touch the
checkout either. The lock and model-state defaults are patched per test here
(conftest does not cover them), which also proves they are call-time defaults.
"""
from __future__ import annotations

import contextlib
import json
import os
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

SERIES_URL = "dltv.org/matches/8900882416"


def _reset_live_caches() -> None:
    live._SNAPSHOT_CACHE = None
    live._MODEL_FROM_SNAPSHOT_CACHE.clear()
    live._RUNTIME_SNAPSHOT_CACHE["base_snapshot_id"] = None
    live._RUNTIME_SNAPSHOT_CACHE["runtime_signature"] = None
    live._RUNTIME_SNAPSHOT_CACHE["snapshot"] = None


def _record() -> MatchRecord:
    return MatchRecord(
        match_id=8900882416, timestamp=1771153200, radiant_win=False,
        radiant_team_id=1, radiant_team_name="Elegia",
        dire_team_id=2, dire_team_name="Team Mariachi",
        radiant_player_ids=(1, 2, 3, 4, 5), dire_player_ids=(6, 7, 8, 9, 10),
        league_id=11, league_name="Test League", source_league_tier="TIER2",
        series_id=None, series_type="3", derived_league_tier=LeagueTier.TIER2,
    )


def _real_runtime_files() -> list[Path]:
    root = Path(live.__file__).resolve().parents[1] / "runtime"
    return [root / "live_elo_progress.json", root / "live_elo_model_state.json",
            root / "live_elo_state.lock"]


def _fingerprint(path: Path):
    if not path.exists():
        return None
    st = path.stat()
    return (st.st_mtime_ns, st.st_size)


@pytest.fixture
def guarded(tmp_path, monkeypatch):
    _reset_live_caches()
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    snapshot = {
        "meta": {**live._rating_replay_meta(), "reference_timestamp": 1771153251},
        "teams_by_org_key": {},
        "model_state": model.export_state(),
    }
    monkeypatch.setattr(live, "ensure_snapshot", lambda **kwargs: snapshot)

    tmp_root = tmp_path.resolve()
    # A RED run (fix reverted) reads the real progress file; a pending overlay
    # commit there would make rebase call state_overlay.save_delta directly,
    # bypassing the _write sink below. Keep the delta inside tmp as well.
    monkeypatch.setenv("LIVE_ELO_DELTA", str(tmp_root / "live_elo_delta.npz"))
    writes: list[Path] = []
    real_write = live._write_json_atomic

    def _write(path, payload):
        path = Path(path)
        writes.append(path.resolve())
        if tmp_root in path.resolve().parents:
            real_write(path, payload)

    @contextlib.contextmanager
    def _lock(lock_path):
        writes.append(Path(lock_path).resolve())
        yield None  # never open/create a lock file here

    monkeypatch.setattr(live, "_write_json_atomic", _write)
    monkeypatch.setattr(live, "_runtime_file_lock", _lock)
    # conftest isolates only the progress path; the model-state default is a
    # module attribute too, so a per-test patch must be enough (call-time default).
    monkeypatch.setattr(live, "DEFAULT_RUNTIME_MODEL_STATE_PATH",
                        tmp_path / "live_elo_model_state.json")
    monkeypatch.setattr(live, "DEFAULT_RUNTIME_LOCK_PATH", tmp_path / "live.lock")
    before = {p: _fingerprint(p) for p in _real_runtime_files()}
    yield writes, tmp_root, before
    assert {p: _fingerprint(p) for p in _real_runtime_files()} == before
    _reset_live_caches()


def _assert_isolated(writes, tmp_root):
    outside = [str(p) for p in writes if tmp_root not in p.parents]
    assert not outside, f"registration wrote outside the test tmp dir: {outside}"
    progress = Path(os.environ["LIVE_ELO_PROGRESS"])
    assert tmp_root in progress.resolve().parents
    assert progress.resolve() in writes, "series was not written to the isolated progress file"
    payload = json.loads(progress.read_text(encoding="utf-8"))
    assert f"{SERIES_URL}.0" in json.dumps(payload["pending_series"])


def test_register_live_map_context_default_paths_follow_module_patch(guarded):
    writes, tmp_root, _ = guarded
    result = live.register_live_map_context(
        series_key="8900882416", series_url=SERIES_URL, map_key=f"{SERIES_URL}.0",
        first_team_score=0, second_team_score=0, first_team_is_radiant=True,
        match_record=_record(), rebuild_if_missing=False)
    assert result is not None
    _assert_isolated(writes, tmp_root)


def test_cyberscore_registration_wrapper_does_not_touch_real_progress(guarded, monkeypatch):
    writes, tmp_root, _ = guarded
    assert cs.ELO_LIVE_SNAPSHOT_AVAILABLE
    result = cs._register_completed_live_map_for_elo(
        series_key="8900882416", series_url=SERIES_URL, map_key=f"{SERIES_URL}.0",
        first_team_score=0, second_team_score=0, first_team_is_radiant=True,
        map_match_id=8900882416, observed_timestamp=1771153200,
        radiant_team_id=1, dire_team_id=2,
        radiant_team_name="Elegia", dire_team_name="Team Mariachi",
        radiant_account_ids=[1, 2, 3, 4, 5], dire_account_ids=[6, 7, 8, 9, 10],
        league_id=11, league_name="Test League", series_type=3, match_tier=2)
    assert result is not None
    _assert_isolated(writes, tmp_root)
