"""The base test-suite isolation covers the live-ELO lock, model state and snapshot pin.

Card ingame-gmoi (08.10.2026). `base/conftest.py::_isolate_live_elo_progress`
redirected only the progress file. Three more defaults still pointed at the
checkout's real files, and the documented serv1 suite command ran in /root/main:

- `live_team_strength.DEFAULT_RUNTIME_LOCK_PATH` = `<checkout>/runtime/live_elo_state.lock`,
  the flock prod takes around every live-ELO update;
- `live_team_strength.DEFAULT_RUNTIME_MODEL_STATE_PATH` = `<checkout>/runtime/live_elo_model_state.json`
  (734 MB on the Mac, read into the live snapshot merge);
- `ELO_SNAPSHOT_PIN`: prod runs with `ELO_SNAPSHOT_PIN=1` (checked in /proc of the
  cyberscore pid 08.10). A test process without it treats the transferred snapshot as
  stale as soon as any corpus json is newer, and `ensure_snapshot()` rebuilds it on the
  local corpus - the rollback the pin exists to prevent (`_snapshot_is_pinned` docstring).

Each test asserts at the boundary the code really uses: the lock path handed to
`_runtime_file_lock`, the model-state path handed to the snapshot merge, and whether
`ensure_snapshot` reaches `build_snapshot`. Lock and writes are stubbed so a RED run
(conftest fix reverted) cannot touch the checkout either.
"""
from __future__ import annotations

import contextlib
import json
import os
import sys
import time
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from ELO import live_team_strength as live  # noqa: E402
from ELO.config import HybridEloConfig  # noqa: E402
from ELO.domain import LeagueTier, MatchRecord  # noqa: E402
from ELO.models import HybridPlayerRosterEloModel  # noqa: E402

SERIES_URL = "dltv.org/matches/8900882417"


def _reset_live_caches() -> None:
    live._SNAPSHOT_CACHE = None
    live._MODEL_FROM_SNAPSHOT_CACHE.clear()
    live._RUNTIME_SNAPSHOT_CACHE["base_snapshot_id"] = None
    live._RUNTIME_SNAPSHOT_CACHE["runtime_signature"] = None
    live._RUNTIME_SNAPSHOT_CACHE["snapshot"] = None


def _snapshot() -> dict:
    model = HybridPlayerRosterEloModel(HybridEloConfig())
    return {
        "meta": {**live._rating_replay_meta(), "reference_timestamp": 1771153251,
                 "team_kills_history_schema_version": live.TEAM_KILLS_HISTORY_SCHEMA_VERSION},
        "teams_by_org_key": {},
        "team_kills_history_by_team_id": {},
        "model_state": model.export_state(),
    }


def _record() -> MatchRecord:
    return MatchRecord(
        match_id=8900882417, timestamp=1771153200, radiant_win=False,
        radiant_team_id=1, radiant_team_name="Elegia",
        dire_team_id=2, dire_team_name="Team Mariachi",
        radiant_player_ids=(1, 2, 3, 4, 5), dire_player_ids=(6, 7, 8, 9, 10),
        league_id=11, league_name="Test League", source_league_tier="TIER2",
        series_id=None, series_type="3", derived_league_tier=LeagueTier.TIER2,
    )


@pytest.fixture
def clean_caches():
    _reset_live_caches()
    yield
    _reset_live_caches()


def test_live_registration_takes_the_lock_inside_tmp(tmp_path, monkeypatch, clean_caches):
    snapshot = _snapshot()
    monkeypatch.setattr(live, "ensure_snapshot", lambda **kwargs: snapshot)
    # The model-state read has its own test below; keep this one on the lock.
    monkeypatch.setattr(live, "_snapshot_with_runtime_model_state",
                        lambda base, **kwargs: base)
    monkeypatch.setenv("LIVE_ELO_DELTA", str(tmp_path / "live_elo_delta.npz"))
    tmp_root = tmp_path.resolve()
    touched: list[Path] = []
    real_write = live._write_json_atomic

    def _write(path, payload):
        touched.append(Path(path).resolve())
        if tmp_root in Path(path).resolve().parents:
            real_write(path, payload)

    @contextlib.contextmanager
    def _lock(lock_path):
        touched.append(Path(lock_path).resolve())
        yield None  # never open/create a lock file here

    monkeypatch.setattr(live, "_write_json_atomic", _write)
    monkeypatch.setattr(live, "_runtime_file_lock", _lock)
    # No per-test patch of DEFAULT_RUNTIME_LOCK_PATH / _MODEL_STATE_PATH: the
    # conftest isolation alone must keep them out of the checkout.
    result = live.register_live_map_context(
        series_key="8900882417", series_url=SERIES_URL, map_key=f"{SERIES_URL}.0",
        first_team_score=0, second_team_score=0, first_team_is_radiant=True,
        match_record=_record(), rebuild_if_missing=False)
    # The lock is taken before the model-state rebase, so check it first: with the
    # fix reverted the rebase also sees the real state file and returns None.
    real_lock = Path(live.__file__).resolve().parents[1] / "runtime" / "live_elo_state.lock"
    assert real_lock not in touched, "test took the checkout's real live-ELO lock"
    locks = [p for p in touched if p.name.endswith(".lock")]
    assert locks, "registration took no lock at all - the boundary moved, update this test"
    assert result is not None
    outside = [str(p) for p in touched if tmp_root not in p.parents]
    assert not outside, f"registration touched files outside the test tmp dir: {outside}"


def test_live_snapshot_merge_reads_model_state_from_tmp(tmp_path, monkeypatch, clean_caches):
    snapshot = _snapshot()
    monkeypatch.setattr(live, "load_snapshot", lambda *a, **k: snapshot)
    seen: list[Path] = []

    def _merge(base, *, runtime_model_state_path):
        seen.append(Path(runtime_model_state_path).resolve())
        return base

    monkeypatch.setattr(live, "_snapshot_with_runtime_model_state", _merge)
    assert live.load_live_snapshot(tmp_path / "snapshot.json") is snapshot
    assert len(seen) == 1
    real_state = Path(live.__file__).resolve().parents[1] / "runtime" / "live_elo_model_state.json"
    assert seen[0] != real_state, "test read the checkout's real runtime model state"
    assert tmp_path.resolve() in seen[0].parents


def test_stale_corpus_does_not_rebuild_a_pinned_snapshot(tmp_path, monkeypatch, clean_caches):
    snap_path = tmp_path / "live_team_elo_snapshot.json"
    snap_path.write_text(json.dumps(_snapshot()), encoding="utf-8")
    old = time.time() - 3600
    os.utime(snap_path, (old, old))
    data_dir = tmp_path / "corpus"
    data_dir.mkdir()
    (data_dir / "part_0001.json").write_text("{}", encoding="utf-8")  # newer than the snapshot
    built: list[Path] = []

    def _build(**kwargs):
        built.append(Path(kwargs["snapshot_path"]))
        return {}

    monkeypatch.setattr(live, "build_snapshot", _build)
    result = live.ensure_snapshot(snapshot_path=snap_path, data_dir=data_dir)
    assert built == [], (
        "a stale corpus rebuilt the snapshot: the suite must run with the prod pin "
        f"{live.SNAPSHOT_PIN_ENV}=1 (env now {os.environ.get(live.SNAPSHOT_PIN_ENV)!r})")
    assert isinstance(result, dict) and result.get("model_state")


def test_live_delta_path_points_into_tmp(tmp_path):
    # Read per call from env LIVE_ELO_DELTA (ELO/runtime_paths.py); the delta is
    # written whenever its base matches the snapshot, which holds in /root/main.
    delta = live._live_delta_path().resolve()
    real = Path(live.DEFAULT_LIVE_DELTA_PATH).resolve()
    assert delta != real, "test would read/write the checkout's real live-ELO delta"
    assert tmp_path.resolve() in delta.parents
