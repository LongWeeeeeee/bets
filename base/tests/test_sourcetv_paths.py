import importlib
import sys
from pathlib import Path

import pytest
BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime
import sourcetv_probe as probe

from sourcetv_bridge import resolve_sourcetv_matches_path


def test_resolve_sourcetv_matches_path_ignores_cwd_for_overrides(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    project_root = tmp_path / "project"
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SOURCETV_MATCHES_PATH", raising=False)
    assert resolve_sourcetv_matches_path(project_root) == (
        project_root / "runtime" / "sourcetv_matches.json"
    ).resolve()

    absolute_override = tmp_path / "outside" / "matches.json"
    monkeypatch.setenv("SOURCETV_MATCHES_PATH", str(absolute_override))
    assert resolve_sourcetv_matches_path(project_root) == absolute_override.resolve()

    monkeypatch.setenv("SOURCETV_MATCHES_PATH", "bridge/matches.json")
    assert resolve_sourcetv_matches_path(project_root) == (
        project_root / "bridge" / "matches.json"
    ).resolve()

def test_sourcetv_default_paths_share_repo_root_runtime() -> None:
    expected = Path(runtime.PROJECT_ROOT) / "runtime" / "sourcetv_matches.json"
    assert Path(runtime.SOURCETV_MATCHES_PATH) == expected
    assert Path(probe.SOURCETV_MATCHES_PATH) == expected


def test_sourcetv_bridge_timestamp_tracks_real_progress_not_rewrites() -> None:
    state = {"game": None, "last_seen": 0.0, "last_progress_at": 0.0}
    first = {
        "game_time": 1800,
        "radiant_score": 20,
        "dire_score": 18,
        "radiant_lead": 1500,
    }
    probe._note_sourcetv_snapshot(state, first, now=100.0)
    state["game"] = first
    assert probe._sourcetv_snapshot_timestamp(state, now=100.0) == 100.0

    # An identical finished row can be received forever, but its exported
    # freshness must stay at the last real game change.
    probe._note_sourcetv_snapshot(state, dict(first), now=500.0)
    assert state["last_seen"] == 500.0
    assert probe._sourcetv_snapshot_timestamp(state, now=500.0) == 100.0

    progressed = dict(first, game_time=1801)
    probe._note_sourcetv_snapshot(state, progressed, now=501.0)
    state["game"] = progressed
    assert probe._sourcetv_snapshot_timestamp(state, now=501.0) == 501.0


def test_sourcetv_pregame_timestamp_uses_latest_receipt() -> None:
    state = {
        "game": {"game_time": -45},
        "last_seen": 700.0,
        "last_progress_at": 100.0,
    }
    assert probe._sourcetv_snapshot_timestamp(state, now=701.0) == 700.0


def test_sourcetv_module_paths_anchor_relative_override_to_repo_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    expected = (Path(runtime.PROJECT_ROOT) / "bridge" / "matches.json").resolve()
    monkeypatch.setenv("SOURCETV_MATCHES_PATH", "bridge/matches.json")
    for cwd in (tmp_path, tmp_path / "other"):
        cwd.mkdir(exist_ok=True)
        monkeypatch.chdir(cwd)
        assert Path(importlib.reload(runtime).SOURCETV_MATCHES_PATH) == expected
        assert Path(importlib.reload(probe).SOURCETV_MATCHES_PATH) == expected

    monkeypatch.delenv("SOURCETV_MATCHES_PATH")
    importlib.reload(runtime)
    importlib.reload(probe)


def test_stake_multiplier_uses_empirical_wr_bands() -> None:
    common = dict(
        team_elo_meta=None,
        target_side="radiant",
        selected_early_sign=1,
        selected_late_sign=1,
        has_selected_early_star=True,
        has_selected_late_star=True,
        early_wr_pct=70.0,
        late_wr_pct=61.0,
        game_time_seconds=30 * 60,
        radiant_lead=2000.0,
        late_star_hit_count=3,
        early_star_hit_count=2,
    )
    assert runtime._stake_multiplier_for_signal(**common) == 0.5
    assert runtime._stake_multiplier_for_signal(**{**common, "late_wr_pct": 62.5}) == 1
    assert runtime._stake_multiplier_for_signal(**{**common, "late_wr_pct": 70.0}) == 2
