"""Execute the nightly remote ELO block against isolated command stubs."""
from pathlib import Path
import os
import subprocess

import pytest


SCRIPT = Path(__file__).resolve().parents[2] / "scripts/run/rebuild_prematch_snapshot.sh"


@pytest.mark.parametrize("rebase_status", [0, 1, 2, 137])
def test_snapshot_is_promoted_only_after_successful_rebase(tmp_path, rebase_status):
    source = SCRIPT.read_text()
    remote = source.split("<<'ELO_REBASE_REMOTE'\n", 1)[1].split("\nELO_REBASE_REMOTE", 1)[0]
    root = tmp_path / "main"
    snapshot = root / "ELO/output/live_team_elo_snapshot.json"
    snapshot.parent.mkdir(parents=True)
    snapshot.write_text("old")
    staged = snapshot.with_suffix(".json.tmp")
    staged.write_text("new")
    checked = tmp_path / "map_id_check.txt"
    checked.write_text("seen map")
    commands = tmp_path / "bin"
    commands.mkdir()
    event_log = tmp_path / "events"
    stubs = {
        commands / "systemctl": 'echo "systemctl $*" >> "$EVENT_LOG"\n',
        commands / "sleep": "exit 0\n",
        root / "venv/bin/python3": '''case "$1" in
  ELO/rebase_runtime_model_state.py)
    test "$(cat ELO/output/live_team_elo_snapshot.json)" = old || exit 20
    test "$2" = --snapshot || exit 21
    test "$(cat "$3")" = new || exit 22
    echo rebase >> "$EVENT_LOG"
    exit "$REBASE_STATUS" ;;
  ELO/convert_state_to_delta.py)
    test "$(cat ELO/output/live_team_elo_snapshot.json)" = new || exit 23
    echo convert >> "$EVENT_LOG" ;;
  ELO/build_state_arrays.py) echo arrays >> "$EVENT_LOG" ;;
  *) exit 24 ;;
esac
''',
    }
    for path, body in stubs.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("#!/bin/bash\n" + body)
        path.chmod(0o755)
    remote = remote.replace("/root/main", str(root)).replace(
        "/root/.local/state/ingame/map_id_check.txt", str(checked)
    )
    result = subprocess.run(
        ["bash", "-s", "--", "1"], input=remote, text=True, capture_output=True,
        env={**os.environ, "PATH": str(commands) + os.pathsep + os.environ["PATH"],
             "EVENT_LOG": str(event_log), "REBASE_STATUS": str(rebase_status)},
    )
    events = event_log.read_text().splitlines()
    assert events[:2] == ["systemctl stop cyberscore.service", "rebase"]
    if rebase_status not in (0, 1):
        assert result.returncode == rebase_status
        assert snapshot.read_text() == "old"
        assert staged.read_text() == "new"
        assert checked.read_text() == "seen map"
        assert events[2:] == []
    elif rebase_status:
        assert result.returncode == 1
        assert checked.read_text() == ""
        assert snapshot.read_text() == "old"
        assert staged.read_text() == "new"
        assert events[2:] == ["systemctl start cyberscore.service"]
    else:
        assert result.returncode == 0, result.stderr
        assert checked.read_text() == ""
        assert snapshot.read_text() == "new"
        assert events[2:] == ["convert", "arrays", "systemctl start cyberscore.service",
                              "systemctl is-active cyberscore.service"]
