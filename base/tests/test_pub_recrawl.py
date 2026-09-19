"""Regression cases for overlap, crash resume and concurrent starts."""
import fcntl
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pub_recrawl as runner


def test_failure_resume_then_next_sweep_uses_start_watermark(tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    runtime = tmp_path / "runtime"
    graph = data / "processed_ids_to_graph.txt"
    graph.write_text("[1, 2]")
    now = 2_000_000
    calls = []

    def failure(cutoff):
        calls.append(cutoff)
        assert not graph.exists()
        graph.write_text("[3]")
        raise RuntimeError("API unavailable")

    with pytest.raises(RuntimeError):
        runner.run_sweep(data, runtime, failure, initial_since=1_000_000, now=now)
    state = json.loads((runtime / "pub_recrawl.json").read_text())
    assert state["status"] == "failed"
    assert Path(state["cursor_backup"]).read_text() == "[1, 2]"

    def resume(cutoff):
        calls.append(cutoff)
        assert graph.read_text() == "[3]"
    assert runner.run_sweep(data, runtime, resume, initial_since=999, now=now + 100) == "complete"
    assert calls == [1_000_000, 1_000_000]
    assert runner.run_sweep(data, runtime, calls.append, now=now + 200) == "not_due"

    def next_sweep(cutoff):
        assert not graph.exists()
        assert cutoff == now - runner.OVERLAP_SECONDS
    assert runner.run_sweep(data, runtime, next_sweep, now=now + runner.INTERVAL_SECONDS) == "complete"
    assert len(list(data.glob("*.bak_recrawl_*"))) == 2


def test_busy_does_not_touch_cursor_or_pid(tmp_path):
    graph = tmp_path / "processed_ids_to_graph.txt"
    graph.write_text("[1]")
    (tmp_path / "get_pubs.pid").write_text("original")
    with (tmp_path / "pub_recrawl.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert runner.run_sweep(tmp_path, tmp_path, pytest.fail, now=200, initial_since=100) == "busy"
    assert graph.read_text() == "[1]"
    assert (tmp_path / "get_pubs.pid").read_text() == "original"


def test_recovers_crash_after_cursor_rename(tmp_path):
    backup = tmp_path / "backup"
    backup.write_text("[1]")
    runner.write_state(tmp_path / "pub_recrawl.json", {
        "status": "preparing", "started_at": 200, "cutoff": 100, "cursor_backup": str(backup)})
    calls = []
    assert runner.run_sweep(tmp_path, tmp_path, calls.append, now=300) == "complete"
    assert calls == [100]
    assert backup.read_text() == "[1]"


def test_bootstrap_requires_cutoff_before_mutating_cursor(tmp_path):
    graph = tmp_path / "processed_ids_to_graph.txt"
    graph.write_text("[1]")
    with pytest.raises(ValueError):
        runner.run_sweep(tmp_path, tmp_path, pytest.fail)
    assert graph.read_text() == "[1]"
