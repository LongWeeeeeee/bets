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



def test_stake_multiplier_requires_complete_late_core_coverage() -> None:
    common = dict(
        team_elo_meta=None,
        target_side="radiant",
        selected_early_sign=1,
        selected_late_sign=1,
        has_selected_early_star=True,
        has_selected_late_star=True,
        early_wr_pct=70.0,
        late_wr_pct=65.0,
        game_time_seconds=30 * 60,
        radiant_lead=2000.0,
        late_star_hit_count=3,
        early_star_hit_count=2,
    )
    assert runtime._stake_multiplier_for_signal(
        late_star_hit_metrics=["counterpick_1vs1", "solo"], **common
    ) == 0.5
    assert runtime._stake_multiplier_for_signal(
        late_star_hit_metrics=["counterpick_1vs1", "counterpick_1vs2", "solo"], **common
    ) != 0.5
