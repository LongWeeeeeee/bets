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


def test_live_list_keeps_bridge_record_fresh_while_gc_is_silent() -> None:
    """Замерший GC при живом матче не имеет права состарить запись.

    Регрессия 16.08.2026 (LGD Gaming — Team Yandex, карта 3): ретрансляция
    молчала с 51:47 до 57:23, запись протухла по last_progress_at, probe сам
    вычистил живую карту из моста, и потребитель прочитал пустой файл как
    доказанный конец карты.
    """
    state = {"game": None, "last_seen": 0.0, "last_progress_at": 0.0}
    row = {
        "game_time": 3107,
        "radiant_score": 24,
        "dire_score": 23,
        "radiant_lead": 18553,
    }
    probe._note_sourcetv_snapshot(state, row, now=100.0)
    state["game"] = row

    # GC молчит пять с половиной минут, но live-список Valve держит матч.
    state["last_api_seen"] = 420.0
    assert probe._sourcetv_snapshot_timestamp(state, now=430.0) == 420.0


def test_ghost_match_without_live_list_confirmation_still_ages() -> None:
    """Матч-призрак исчезает из live-списка — запись стареет ровно как раньше."""
    state = {"game": None, "last_seen": 0.0, "last_progress_at": 0.0}
    row = {
        "game_time": 4092,
        "radiant_score": 26,
        "dire_score": 40,
        "radiant_lead": 80583,
    }
    probe._note_sourcetv_snapshot(state, row, now=100.0)
    state["game"] = row
    state["last_api_seen"] = 100.0

    # Valve продолжает отдавать ту же законченную строку по GC.
    probe._note_sourcetv_snapshot(state, dict(row), now=600.0)
    assert state["last_seen"] == 600.0
    assert probe._sourcetv_snapshot_timestamp(state, now=600.0) == 100.0


class _LogSink:
    def __init__(self) -> None:
        self.warnings: list = []
        self.infos: list = []

    def warning(self, *args) -> None:
        self.warnings.append(args)

    def info(self, *args) -> None:
        self.infos.append(args)


def test_gc_stall_is_reported_once_per_episode() -> None:
    """Молчание ретранслятора видно в логе, но не заливает его каждую секунду."""
    sink = _LogSink()
    state = {"last_progress_at": 100.0, "last_api_seen": 400.0, "gc_stall_logged": False}

    assert probe._note_gc_stall(1, state, now=150.0, logger=sink) is False
    assert probe._note_gc_stall(1, state, now=400.0, logger=sink) is True
    assert probe._note_gc_stall(1, state, now=402.0, logger=sink) is False
    assert len(sink.warnings) == 1

    state["last_progress_at"] = 410.0
    assert probe._note_gc_stall(1, state, now=411.0, logger=sink) is True
    assert len(sink.infos) == 1


def test_idle_probe_still_beats_so_watchdog_sees_a_live_process() -> None:
    """Пауза между матчами — не залипание: лог обязан обновляться.

    Watchdog судит о жизни probe по mtime лога (порог 900 с), а тихий probe в
    паузе не печатал ничего и перезапускался на ровном месте (16.08.2026,
    рестарты в 15:15 и 15:35 при живом процессе).
    """
    assert probe.HEARTBEAT_SECONDS < 900.0
    assert probe._heartbeat_due(0, 0.0, now=probe.HEARTBEAT_SECONDS) is True
    assert probe._heartbeat_due(0, 0.0, now=probe.HEARTBEAT_SECONDS - 1) is False
    # Пока матчи на экране, лог и так пишется каждой итерацией.
    assert probe._heartbeat_due(2, 0.0, now=probe.HEARTBEAT_SECONDS * 10) is False


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
