"""Terminal notifications must survive restart without duplicate delivery."""
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace


def load_monitor():
    path = Path(__file__).resolve().parents[2]/"scripts/ops/watch_draft_phase_training.py"
    spec = importlib.util.spec_from_file_location("draft_training_monitor", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_one_terminal_queue_per_process_even_after_monitor_restart(tmp_path, monkeypatch):
    monitor = load_monitor()
    calls = []
    def queue(cmd, **kwargs):
        calls.append(cmd)
        return SimpleNamespace(returncode=0, stdout="queued", stderr="")
    monkeypatch.setattr(monitor.subprocess, "run", queue)
    monkeypatch.setattr(sys, "argv", ["monitor", "--run-dir", str(tmp_path), "--thread", "task-id"])
    status = tmp_path/"status.json"
    status.write_text(json.dumps(dict(state="FAIL", stage="public_corpus", pid=123)))
    monitor.main()
    monitor.main()
    assert len(calls) == 1
    assert calls[0][1:4] == ["queue", "--thread", "task-id"]
    # A restarted training process is a new attempt, not the old duplicate.
    status.write_text(json.dumps(dict(state="DONE", stage="complete", pid=456)))
    monitor.main()
    assert len(calls) == 2
    saved = json.loads((tmp_path/"monitor.json").read_text())
    assert set(saved["sent"]) == {"123:FAIL", "456:DONE"}


def test_progress_activity_counts_descendants_but_not_unrelated_processes(monkeypatch):
    monitor = load_monitor()
    monkeypatch.setattr(monitor.subprocess, "run", lambda *a, **kw: SimpleNamespace(
        returncode=0, stderr="", stdout="100 1 00:00.10\n101 100 02:10.00\n102 101 00:15.00\n999 1 10:00.00\n"))
    alive, cpu = monitor.process_activity(100)
    assert alive and abs(cpu-145.1) < 1e-6
    assert monitor.process_activity(404) == (False, 0)
