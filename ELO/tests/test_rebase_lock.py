"""The ELO rebase CLI owns an exclusive flock: "a writer is alive" == "the lock is held".

Why (08.10.2026 chain hardening, round 5): the nightly chain used to find a live
rebase writer with a pgrep regex over argv; three review rounds kept finding argv
forms it misread. The kernel-held lock is exact: it is released on ANY death of the
holder, SIGKILL included.

Holders here are `python -c <code> <lock path>`: their argv does not name the
rebase script, so they cannot be mistaken for a rebase run by anything that scans
the process table. Nothing here scans it.
"""
from __future__ import annotations

import fcntl
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import ELO.live_team_strength as lts  # noqa: E402
from ELO import rebase_runtime_model_state as cli  # noqa: E402

# Same protocol as the CLI: open without O_TRUNC, flock NB, then write the pid.
HOLDER = (
    "import fcntl, os, sys, time\n"
    "fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o644)\n"
    "fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
    "os.ftruncate(fd, 0)\n"
    "os.write(fd, (str(os.getpid()) + chr(10)).encode())\n"
    "print('LOCKED', flush=True)\n"
    "time.sleep(600)\n"
)


def _start_holder(lock: Path) -> subprocess.Popen:
    proc = subprocess.Popen([sys.executable, "-c", HOLDER, str(lock)],
                            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, text=True)
    assert proc.stdout.readline().strip() == "LOCKED", "holder did not take the lock"
    return proc


def _lock_is_free(lock: Path) -> bool:
    fd = os.open(str(lock), os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    finally:
        os.close(fd)
    return True


@pytest.fixture
def paths(tmp_path, monkeypatch):
    # the delta path is resolved from the env like live_team_strength does; without this
    # the CLI would lock the repo's own runtime/ directory
    monkeypatch.setenv("LIVE_ELO_DELTA", str(tmp_path / "live_elo_delta.json"))
    state = tmp_path / "live_elo_model_state.json"
    progress = tmp_path / "live_elo_progress.json"
    snapshot = tmp_path / "snapshot.json"
    state.write_bytes(b"state-v1\n")
    progress.write_bytes(b"progress-v1\n")
    snapshot.write_bytes(b"this is not json\n")   # reading it would raise: proves lock-first
    return state, progress, snapshot, tmp_path / "live_elo_rebase.lock"


def _argv(state, progress, snapshot):
    return ["--state", str(state), "--progress", str(progress), "--snapshot", str(snapshot)]


def test_second_rebase_is_refused_with_rc_3_and_writes_nothing(paths, capsys):
    state, progress, snapshot, lock = paths
    holder = _start_holder(lock)
    try:
        holder_pid = holder.pid
        before = {p: p.read_bytes() for p in (state, progress, snapshot)}
        assert cli.main(_argv(state, progress, snapshot)) == 3
        err = capsys.readouterr().err
        assert "перебазировка уже выполняется" in err and f"pid {holder_pid}" in err, err
        assert {p: p.read_bytes() for p in (state, progress, snapshot)} == before
        assert lock.read_text().strip() == str(holder_pid), "the refused run must not touch the holder's pid"
        assert sorted(x.name for x in lock.parent.iterdir()) == sorted(
            [state.name, progress.name, snapshot.name, lock.name]), "no stray file may appear"
        assert holder.poll() is None
    finally:
        holder.kill()
        holder.wait()


def test_lock_file_names_the_holder_and_is_held_for_the_whole_run(paths, monkeypatch):
    state, progress, snapshot, lock = paths
    snapshot.write_text('{"meta": {}}', encoding="utf-8")
    seen = {}

    def fake_rebase(**kwargs):
        seen["free_during_run"] = _lock_is_free(lock)
        seen["pid"] = lock.read_text().strip()
        return False

    monkeypatch.setattr(lts, "_snapshot_reference_timestamp", lambda s: 1)
    monkeypatch.setattr(lts, "_snapshot_model_config_signature", lambda s: "sig")
    monkeypatch.setattr(lts, "rebase_runtime_model_state", fake_rebase)
    assert cli.main(_argv(state, progress, snapshot)) == 0
    assert seen == {"free_during_run": False, "pid": str(os.getpid())}
    assert _lock_is_free(lock), "the lock must be released when main() returns"
    assert lock.exists(), "the lock file is never deleted"


def test_lock_default_location_follows_the_state_directory():
    assert cli.REBASE_LOCK_NAME == "live_elo_rebase.lock"
    assert lts.DEFAULT_RUNTIME_MODEL_STATE_PATH.parent / cli.REBASE_LOCK_NAME == \
        ROOT / "runtime" / "live_elo_rebase.lock"


@pytest.mark.parametrize("how", ["term", "kill"])
def test_lock_is_released_when_the_holder_dies(paths, how):
    state, progress, snapshot, lock = paths
    holder = _start_holder(lock)
    assert not _lock_is_free(lock)
    holder.terminate() if how == "term" else holder.kill()
    holder.wait()
    assert _lock_is_free(lock), "the kernel must drop the lock with the process"
    # a new run now gets past the lock and stops at the missing snapshot (rc 1), not rc 3
    assert cli.main(_argv(state, progress, snapshot.parent / "missing.json")) == 1
    assert lock.read_text().strip() == str(os.getpid()), "the new holder rewrites the pid"
    assert _lock_is_free(lock)


def test_unopenable_lock_refuses_before_any_write(paths, capsys):
    state, progress, snapshot, lock = paths
    lock.mkdir()   # a directory cannot be flocked as a file
    before = {p: p.read_bytes() for p in (state, progress, snapshot)}
    assert cli.main(_argv(state, progress, snapshot)) == 1
    assert "не удалось" in capsys.readouterr().err
    assert {p: p.read_bytes() for p in (state, progress, snapshot)} == before


# ----------------------------------------------------------------- round 6 hardening

CLI_PATH = ROOT / "ELO" / "rebase_runtime_model_state.py"

# Runs the CLI exactly as the chain does (a script file, __main__) and reports whether
# live_team_strength had been imported by the time it returned.
RUN_CLI = (
    "import runpy, sys\n"
    "sys.argv = sys.argv[1:]\n"
    "try:\n"
    "    runpy.run_path(sys.argv[0], run_name='__main__')\n"
    "except SystemExit as exc:\n"
    "    rc = exc.code\n"
    "print('RESULT', rc, 'ELO.live_team_strength' in sys.modules, flush=True)\n"
)


def _run_cli(argv, delta):
    env = dict(os.environ, LIVE_ELO_DELTA=str(delta), PYTHONDONTWRITEBYTECODE="1")
    res = subprocess.run([sys.executable, "-c", RUN_CLI, str(CLI_PATH), *argv],
                         capture_output=True, text=True, env=env, cwd=str(ROOT), timeout=300)
    line = [l for l in res.stdout.splitlines() if l.startswith("RESULT ")]
    assert line, (res.stdout, res.stderr)
    _, rc, loaded = line[-1].split()
    return int(rc), loaded == "True", res.stderr


def test_held_lock_refuses_before_live_team_strength_is_imported(paths):
    # F1: `timeout` may kill the CLI while it is still importing; the lock must already be
    # held (or refused) by then, so nothing heavy is imported before the lock decision.
    state, progress, snapshot, lock = paths
    holder = _start_holder(lock)
    try:
        rc, lts_loaded, err = _run_cli(_argv(state, progress, snapshot), state.parent / "live_elo_delta.json")
        assert rc == 3, err
        assert not lts_loaded, "live_team_strength was imported before the lock was taken"
        assert "перебазировка уже выполняется" in err
    finally:
        holder.kill()
        holder.wait()


def test_free_lock_imports_live_team_strength_only_after_taking_it(paths):
    # positive control for the probe above: with a free lock the same harness sees the
    # import (and the run stops at the missing snapshot, rc 1)
    state, progress, snapshot, lock = paths
    rc, lts_loaded, err = _run_cli(_argv(state, progress, snapshot.parent / "missing.json"),
                                   state.parent / "live_elo_delta.json")
    assert (rc, lts_loaded) == (1, True), err


def test_cli_defaults_are_the_live_team_strength_constants(monkeypatch):
    # single source of truth for the default paths: the stdlib-only module the CLI uses
    # to lock before importing lts is the very one lts takes its constants from
    from ELO import runtime_paths as rp
    assert lts.DEFAULT_SNAPSHOT_PATH == rp.DEFAULT_SNAPSHOT_PATH
    assert lts.DEFAULT_RUNTIME_PROGRESS_PATH == rp.DEFAULT_RUNTIME_PROGRESS_PATH
    assert lts.DEFAULT_RUNTIME_MODEL_STATE_PATH == rp.DEFAULT_RUNTIME_MODEL_STATE_PATH
    assert lts.DEFAULT_LIVE_DELTA_PATH == rp.DEFAULT_LIVE_DELTA_PATH
    monkeypatch.delenv("LIVE_ELO_DELTA", raising=False)
    assert lts._live_delta_path() == rp.live_delta_path(rp.DEFAULT_LIVE_DELTA_PATH) == rp.DEFAULT_LIVE_DELTA_PATH
    monkeypatch.setenv("LIVE_ELO_DELTA", "~/x_delta.json")
    assert lts._live_delta_path() == rp.live_delta_path(rp.DEFAULT_LIVE_DELTA_PATH) == \
        Path("~/x_delta.json").expanduser()


def test_progress_in_a_locked_directory_refuses_even_with_a_scratch_state(tmp_path, monkeypatch, capsys):
    # F2: --state in scratch dir A, --progress (default: prod) in dir B whose lock is held.
    a, b = tmp_path / "a", tmp_path / "b"
    a.mkdir(); b.mkdir()
    monkeypatch.setenv("LIVE_ELO_DELTA", str(a / "live_elo_delta.json"))
    state, progress, snapshot = a / "state.json", b / "progress.json", a / "snapshot.json"
    state.write_bytes(b"s\n"); progress.write_bytes(b"p\n"); snapshot.write_bytes(b"not json\n")
    holder = _start_holder(b / "live_elo_rebase.lock")
    try:
        before = {p: p.read_bytes() for p in (state, progress, snapshot)}
        assert cli.main(_argv(state, progress, snapshot)) == 3
        assert f"pid {holder.pid}" in capsys.readouterr().err
        assert {p: p.read_bytes() for p in (state, progress, snapshot)} == before
        assert (b / "live_elo_rebase.lock").read_text().strip() == str(holder.pid)
        assert _lock_is_free(a / "live_elo_rebase.lock"), "the lock taken in A must be released"
        assert (a / "live_elo_rebase.lock").read_bytes() == b"", "no pid is written unless every lock is held"
        assert sorted(x.name for x in a.iterdir()) == ["live_elo_rebase.lock", "snapshot.json", "state.json"]
    finally:
        holder.kill()
        holder.wait()


def test_delta_in_a_locked_directory_refuses(tmp_path, monkeypatch, capsys):
    # F2: the live delta (LIVE_ELO_DELTA, default runtime/) is the third output
    a, c = tmp_path / "a", tmp_path / "c"
    a.mkdir(); c.mkdir()
    monkeypatch.setenv("LIVE_ELO_DELTA", str(c / "live_elo_delta.json"))
    state, progress, snapshot = a / "state.json", a / "progress.json", a / "snapshot.json"
    for p in (state, progress):
        p.write_bytes(b"x\n")
    snapshot.write_bytes(b"not json\n")
    holder = _start_holder(c / "live_elo_rebase.lock")
    try:
        assert cli.main(_argv(state, progress, snapshot)) == 3
        assert f"pid {holder.pid}" in capsys.readouterr().err
        assert _lock_is_free(a / "live_elo_rebase.lock")
        assert [p.read_bytes() for p in (state, progress)] == [b"x\n", b"x\n"]
    finally:
        holder.kill()
        holder.wait()


def test_every_distinct_output_directory_is_locked_once_in_sorted_order(tmp_path, monkeypatch):
    a, b = tmp_path / "a", tmp_path / "b"
    a.mkdir(); b.mkdir()
    (tmp_path / "link").symlink_to(a, target_is_directory=True)
    monkeypatch.setenv("LIVE_ELO_DELTA", str(tmp_path / "link" / "d.json"))   # same dir as state, via a symlink
    args = cli.argparse.Namespace(state=b / "s.json", progress=a / "p.json", snapshot=a / "x")
    names = [d.name for d in cli._lock_dirs(args)]
    assert names == ["a", "b"], names
    # all three in one directory -> one lock; the run still gets through it
    args = cli.argparse.Namespace(state=a / "s.json", progress=a / "p.json", snapshot=a / "x")
    assert [d.name for d in cli._lock_dirs(args)] == ["a"]


# ----------------------------------------------------------------- round 7 hardening

@pytest.mark.parametrize("held", ["spelled", "resolved"])
def test_symlinked_output_file_locks_both_its_own_and_its_targets_directory(tmp_path, monkeypatch, capsys, held):
    # G2: `os.replace` writes the path AS GIVEN: with --state a/state.json -> b/real.json the
    # new regular file appears in `a` (the symlink is replaced), while resolve() names `b`.
    # A writer holding the lock of either directory must refuse this run (rc 3, nothing written).
    a, b, c = tmp_path / "a", tmp_path / "b", tmp_path / "0free"   # sorts first: its lock is taken before the refusal
    for d in (a, b, c):
        d.mkdir()
    (b / "real.json").write_bytes(b"real\n")
    (a / "state.json").symlink_to(b / "real.json")
    monkeypatch.setenv("LIVE_ELO_DELTA", str(c / "live_elo_delta.json"))
    progress, snapshot = c / "progress.json", c / "snapshot.json"
    progress.write_bytes(b"p\n"); snapshot.write_bytes(b"not json\n")
    holder = _start_holder((a if held == "spelled" else b) / "live_elo_rebase.lock")
    try:
        assert cli.main(_argv(a / "state.json", progress, snapshot)) == 3
        assert f"pid {holder.pid}" in capsys.readouterr().err
        assert (b / "real.json").read_bytes() == b"real\n" and (a / "state.json").is_symlink()
        assert progress.read_bytes() == b"p\n" and snapshot.read_bytes() == b"not json\n"
        assert _lock_is_free(c / "live_elo_rebase.lock"), "locks taken before the refusal must be released"
    finally:
        holder.kill()
        holder.wait()


def test_relative_output_path_is_locked_in_its_absolute_directory(tmp_path, monkeypatch, capsys):
    a, c = tmp_path / "a", tmp_path / "0free"
    a.mkdir(); c.mkdir()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LIVE_ELO_DELTA", str(c / "live_elo_delta.json"))
    (a / "state.json").write_bytes(b"s\n")
    progress, snapshot = c / "progress.json", c / "snapshot.json"
    progress.write_bytes(b"p\n"); snapshot.write_bytes(b"not json\n")
    holder = _start_holder(a / "live_elo_rebase.lock")
    try:
        assert cli.main(["--state", "a/state.json", "--progress", str(progress),
                         "--snapshot", str(snapshot)]) == 3
        assert f"pid {holder.pid}" in capsys.readouterr().err
    finally:
        holder.kill()
        holder.wait()


def test_spelled_and_resolved_directories_are_one_lock_when_they_are_the_same_directory(tmp_path, monkeypatch):
    a, b = tmp_path / "a", tmp_path / "b"
    a.mkdir(); b.mkdir()
    (tmp_path / "link").symlink_to(a, target_is_directory=True)
    (a / "real.json").write_bytes(b"x")
    (a / "ptr.json").symlink_to(a / "real.json")             # symlink inside the same directory
    monkeypatch.setenv("LIVE_ELO_DELTA", str(tmp_path / "link" / "d.json"))   # `a` through a second spelling
    args = cli.argparse.Namespace(state=a / "ptr.json", progress=tmp_path / "link" / "p.json", snapshot=a / "x")
    dirs = cli._lock_dirs(args)
    assert [d.name for d in dirs] == ["a"], dirs
    # a symlink into another directory adds that directory, sorted, once
    (b / "r.json").write_bytes(b"x")
    (a / "to_b.json").symlink_to(b / "r.json")
    args = cli.argparse.Namespace(state=a / "to_b.json", progress=a / "p.json", snapshot=a / "x")
    assert [d.name for d in cli._lock_dirs(args)] == ["a", "b"]
