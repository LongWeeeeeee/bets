"""Tests for scripts/ops/pub_parts_offload.py (serv1 -> Mac pub-corpus part offload).

The remote is a local directory behind a pluggable command runner (no ssh): the very same
stdlib remote helper that runs on serv1 runs here against tmp dirs.

Guard tests are red/green: every guard has (a) a scenario test that must pass on the real
module and (b) a MUTATION test that deletes that guard from the source text and requires the
scenario's assertions to FAIL (otherwise the test would not detect a removed guard).
PUB_OFFLOAD_MUTATE=<guard> runs the scenario tests against the mutated module, to see them red.
"""
from __future__ import annotations

import fcntl
import importlib.util
import json
import plistlib
import shlex
import shutil
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "scripts/ops/pub_parts_offload.py"
PATCH = "7.41e"
NOW = time.time()
HOUR = 3600


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


REAL = load_module(SRC, "pub_parts_offload_real")

# guard name -> list of (exact snippet, replacement) applied to the module source text
MUTATIONS = {
    "merge_process": [('if hits:\n            raise Abort(f"a corpus merge process', 'if False:\n            raise Abort(f"a corpus merge process')],
    "sweep_lock": [("if busy and not self.cfg.allow_busy_sweep:", "if False:")],
    "lock_held_at_delete": [('        if require_lock:\n            fail(11,', '        if False:\n            fail(11,')],
    "quiescent_age": [("            if age < limit:", "            if False:")],
    "in_progress_tmp": [('if TMP_RE.search(name) and listing["now_ns"] - st["mtime_ns"] < limit:', "if False:")],
    "candidate_age": [("if now_ns - st[\"mtime_ns\"] < age_limit_ns:", "if False:")],
    "hash_equal": [("        if remote_sha != local_sha:", "        if False:")],
    "name_collision": [("        if local_sha != remote_sha:\n            raise Abort(f\"NAME COLLISION", "        if False:\n            raise Abort(f\"NAME COLLISION")],
    "ids_present": [('        if missing:\n            raise Abort(f"{name}: {len(missing)} of', '        if False:\n            raise Abort(f"{name}: {len(missing)} of')],
    "ids_nonempty": [("        if n == 0:", "        if False:")],
    "mac_ids_subset": [("        if extra:", "        if False:")],
    "counter_vs_mac": [("            if serv.get(patch, 0) < mac_n:", "            if False:")],
    "counter_vs_part": [('            if serv.get(c["patch"], 0) < c["num"]:', "            if False:")],
    "stamp_recheck": [('    return cur is not None and cur["size"] == want["size"] and cur["mtime_ns"] == want["mtime_ns"]',
                       "    return cur is not None")],
    "control_stamp_recheck": [('    for g in req["guards"]:\n        if not same(g["path"], g):', '    for g in req["guards"]:\n        if False:')],
    "name_safety": [('if not NAME_RE.match(f["name"]) or "/" in f["name"]:', "if False:")],
    "control_backup": [("                shutil.copy2(target, bak)", "                pass")],
    "local_reader": [("            if hit is not None and pid not in mine:", "            if False:")],
    "local_reader_waiter": [('            probe = _SEARCH_CMD_RE.sub(" ", cmd)', "            probe = cmd")],
    # --- temp_files phase (card ingame-xmas.2) ---
    "temp_status_complete": [('        if info.get("status") != "complete" or not _is_epoch(info.get("completed_at")):', "        if False:")],
    "temp_parts_remaining": [("        if left and not cfg.dry_run:", "        if False:")],
    "temp_lock_free": [("        if remote.lock_busy(cfg.remote_lock):", "        if False:")],
    "temp_merge_process": [("        if remote.merge_procs():", "        if False:")],
    "temp_quiescent": [("            self._guard_quiescent(fresh)", "            pass")],
    "temp_verdict_ok_only": [('if v.get("ok") is True and', "if True and")],
    "tc_newer_than_sweep": [('    if st["mtime_ns"] >= completed_ns:', "    if False:")],
    "tc_too_large": [('    if st["size"] > max_bytes:', "    if False:")],
    "tc_unparsable": [("        doc = strict_loads(raw)\n",
                       "        try:\n            doc = strict_loads(raw)\n        except Exception:\n"
                       "            doc = {k.decode(): {} for k in re.findall(rb'\"(\\d+)\": ', raw)}\n")],
    "tc_not_object": [("    if not isinstance(doc, dict):", "    if False:")],
    "tc_empty": [("    if not n_keys:", "    if False:")],
    "tc_bad_key": [("        if n is None:", "        if False:")],
    "tc_ids_missing": [("    if n_missing:", "    if False:")],
    "dt_lock_held": [('        fail(11, "pub_recrawl.lock is held: a sweep started before the temp_files delete")', "        pass")],
    "dt_status": [('    if cur_state is None or cur_state.get("status") != "complete" or cur_state.get("completed_at") != want_completed:', "    if False:")],
    "dt_parts_remain": [("    if left_parts:", "    if False:")],
    "dt_stamp": [("def unchanged_temp(path, want):\n    return same(path, want)", "def unchanged_temp(path, want):\n    return True")],
    "dt_guards": [('        if not same(gd["path"], gd):', "        if False:")],
    "dt_newer": [('        if f["mtime_ns"] >= want_completed * 10 ** 9:', "        if False:")],
    "dt_name_safety": [('if not TEMP_NAME_RE.match(f["name"]) or "/" in f["name"]:', "if False:")],
}


def mutated_module(guard: str, tmp_path: Path):
    text = SRC.read_text()
    for old, new in MUTATIONS[guard]:
        assert text.count(old) == 1, f"mutation anchor for {guard} not unique/found: {old!r}"
        text = text.replace(old, new)
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / f"mutated_{guard}.py"
    path.write_text(text)
    return load_module(path, f"pub_parts_offload_mut_{guard}")


def module_for(guard: str, tmp_path: Path):
    if os.environ.get("PUB_OFFLOAD_MUTATE") == guard:
        return mutated_module(guard, tmp_path / "mut")
    return REAL


class World:
    """serv1 + Mac as tmp dirs; `hooks_before`/`hooks_after` see every remote command."""

    def __init__(self, mod, tmp: Path, nums=(1, 2, 3), part_age_h=10, ctl_age_h=3, ids_per_part=4):
        self.mod, self.tmp = mod, tmp
        self.rdir = tmp / "serv1/json_parts_split_from_object"
        self.rparent = tmp / "serv1"
        self.lock = tmp / "serv1_runtime/pub_recrawl.lock"
        self.mparent = tmp / "mac"
        self.mdir = self.mparent / "json_parts_split_from_object"
        for d in (self.rdir, self.lock.parent, self.mdir):
            d.mkdir(parents=True, exist_ok=True)
        self.lock.write_text("")
        self.hooks_before, self.hooks_after = [], []
        self.notified: list[str] = []
        self.cmds: list[list[str]] = []
        self.base_ids = [10, 11, 12]
        self.part_ids: dict[int, list[int]] = {}
        nxt = 1000
        for n in nums:
            self.part_ids[n] = list(range(nxt, nxt + ids_per_part))
            nxt += 100
            self.write_part(self.rdir, n, self.part_ids[n])
            self.age(self.rdir / self.pname(n), part_age_h)
        self.set_json(self.rdir / "processed_ids.txt", self.base_ids + [i for v in self.part_ids.values() for i in v] + [5, 6])
        self.set_json(self.rdir / "part_counters.json", {PATCH: max(nums) if nums else 0})
        self.set_json(self.rparent / "pub_player_steam_ids.json", {"ids": [1, 2, 3], "source": "serv1"})
        for f in (self.rdir / "processed_ids.txt", self.rdir / "part_counters.json", self.rparent / "pub_player_steam_ids.json"):
            self.age(f, ctl_age_h)
        # Mac: older state, a subset of serv1's
        self.set_json(self.mdir / "processed_ids.txt", self.base_ids)
        self.set_json(self.mdir / "part_counters.json", {PATCH: 0})
        self.set_json(self.mparent / "pub_player_steam_ids.json", {"ids": [1, 2], "source": "mac"})
        self.remote = mod.Remote("fake", use_ssh=False, python=sys.executable, io_prefix=[],
                                 sha_argv=["shasum", "-a", "256"], runner=self._runner)
        # temp-phase attributes are set after construction so that the pre-temp-phase module can still be loaded
        self.remote.np_python, self.remote.nice_prefix = sys.executable, []
        self.rtemp = self.rparent / "temp_files"
        self.state_file = self.lock.parent / "pub_recrawl.json"
        self.manifest_file = tmp / "temp_manifest.jsonl"
        self.completed_at = None

    # -- fixtures helpers -------------------------------------------------------------
    @staticmethod
    def pname(n, patch=PATCH):
        return f"{patch}_part{n:03d}.json"

    @staticmethod
    def write_part(directory: Path, n, ids, patch=PATCH):
        (directory / World.pname(n, patch)).write_text(
            json.dumps({str(i): {"id": i, "payload": "yyyy" * 8} for i in ids}))

    @staticmethod
    def set_json(path: Path, obj):
        path.write_text(json.dumps(obj))

    @staticmethod
    def age(path: Path, hours: float):
        t = NOW - hours * HOUR
        os.utime(path, (t, t))

    def _runner(self, argv, **kw):
        self.cmds.append(list(argv))
        for h in self.hooks_before:
            h(argv)
        res = subprocess.run(argv, **kw)
        for h in self.hooks_after:
            h(argv, res)
        return res

    # -- temp_files fixtures -----------------------------------------------------------
    def write_state(self, status="complete", completed_ago_h=5.0, **extra):
        st = {"status": status, "started_at": int(NOW - 30 * HOUR), "cutoff": int(NOW - 60 * HOUR)}
        if status == "complete":
            self.completed_at = int(NOW - completed_ago_h * HOUR)
            st["completed_at"] = self.completed_at
        st.update(extra)
        self.state_file.write_text(json.dumps(st))

    def enable_temps(self, status="complete", completed_ago_h=5.0):
        """serv1 temp_files/ plus pub_recrawl.json of a finished sweep (completed `completed_ago_h` ago)."""
        self.rtemp.mkdir(parents=True, exist_ok=True)
        self.write_state(status, completed_ago_h)

    def add_temp(self, name, ids, *, known=True, age_h=20, raw=None):
        """known: True = all ids go to serv1 processed_ids.txt, False = none, or a list of ids to register."""
        p = self.rtemp / name
        p.write_text(raw if raw is not None else json.dumps({str(i): {"id": i, "payload": "yyyy"} for i in ids}))
        self.age(p, age_h)
        reg = list(ids) if known is True else ([] if known is False else list(known))
        if reg:
            pid = self.rdir / "processed_ids.txt"
            self.set_json(pid, json.loads(pid.read_text()) + reg)
            self.age(pid, 3)
        return p

    def remote_temps(self):
        return sorted(p.name for p in self.rtemp.iterdir()) if self.rtemp.exists() else []

    def manifest(self):
        if not self.manifest_file.exists():
            return []
        return [json.loads(line) for line in self.manifest_file.read_text().splitlines() if line.strip()]

    def temp_ops(self):
        return [op_of(c) for c in self.cmds if op_of(c) in ("tempcheck", "delete_temp")]

    # -- run ---------------------------------------------------------------------------
    def cfg(self, **over):
        c = self.mod.Config(remote_dir=str(self.rdir), remote_parent=str(self.rparent), remote_lock=str(self.lock),
                            mac_dir=self.mdir, mac_parent=self.mparent, log_path=self.tmp / "offload.log",
                            min_free_margin_bytes=0, local_reader_markers=("__no_such_reader_marker__",))
        for k, v in dict(remote_temp_dir=str(self.rtemp), remote_state=str(self.state_file),
                         state_path=self.tmp / "offload_state.json", temp_manifest_path=self.manifest_file).items():
            setattr(c, k, v)  # setattr: tolerated by the pre-temp-phase Config, which is how the red run works
        for k, v in over.items():
            setattr(c, k, v)
        return c

    def run(self, **over) -> int:
        now = over.pop("now", None)
        cfg = self.cfg(**over)
        kw = {"now": now} if now is not None else {}
        return self.mod.execute(cfg, self.remote, self.mod.Log(cfg.log_path, echo=False),
                                notify=self.notified.append, **kw)

    # -- observations ------------------------------------------------------------------
    def remote_parts(self):
        return sorted(p.name for p in self.rdir.iterdir() if "_part" in p.name and not p.name.endswith(".tmp"))

    def mac_parts(self):
        return sorted(p.name for p in self.mdir.iterdir() if "_part" in p.name)

    def all_remote_still_there(self):
        return self.remote_parts() == [self.pname(n) for n in sorted(self.part_ids)]

    def mac_has_no_new_parts(self):
        return self.mac_parts() == []


def op_of(argv):
    if "-c" in argv:
        i = argv.index("-c")
        return argv[i + 2] if len(argv) > i + 2 else None
    return None


# ======================================================================================
# plain behaviour
# ======================================================================================
def test_happy_path(tmp_path):
    w = World(REAL, tmp_path)
    old_mac_pid = (w.mdir / "processed_ids.txt").read_bytes()
    assert w.run() == 0
    assert w.mac_parts() == [w.pname(n) for n in (1, 2, 3)]
    for n in (1, 2, 3):  # byte-identical to what serv1 had
        assert json.loads((w.mdir / w.pname(n)).read_text()).keys() == {str(i) for i in w.part_ids[n]}
    assert w.remote_parts() == []
    # control files refreshed from serv1, old Mac versions backed up
    assert (w.mdir / "processed_ids.txt").read_bytes() == (w.rdir / "processed_ids.txt").read_bytes()
    assert json.loads((w.mdir / "part_counters.json").read_text()) == {PATCH: 3}
    assert json.loads((w.mparent / "pub_player_steam_ids.json").read_text())["source"] == "serv1"
    baks = sorted(p.name for p in w.mdir.iterdir() if ".bak_before_offload_" in p.name)
    assert [b.split(".bak")[0] for b in baks] == ["part_counters.json", "processed_ids.txt"]
    assert (w.mdir / baks[1]).read_bytes() == old_mac_pid
    assert any(".bak_before_offload_" in p.name for p in w.mparent.iterdir())
    # serv1 control files untouched
    assert (w.rdir / "processed_ids.txt").exists() and (w.rdir / "part_counters.json").exists()
    assert not list((w.mparent / "_offload_staging").glob("*/*"))
    assert w.notified == []
    assert "deleted 3 files on serv1" in (w.tmp / "offload.log").read_text()


def test_rerun_after_partial_transfer_is_idempotent(tmp_path):
    w = World(REAL, tmp_path)
    w.write_part(w.mdir, 1, w.part_ids[1])  # already transferred earlier, identical content
    assert w.run() == 0
    assert w.remote_parts() == [] and w.mac_parts() == [w.pname(n) for n in (1, 2, 3)]
    assert "already on the Mac with identical sha256" in (w.tmp / "offload.log").read_text()


def test_zero_candidates_touches_nothing(tmp_path):
    w = World(REAL, tmp_path, nums=())
    before = {p.name: p.read_bytes() for p in w.mdir.iterdir()}
    assert w.run() == 0
    assert {p.name: p.read_bytes() for p in w.mdir.iterdir()} == before
    assert not (w.mparent / "_offload_staging").exists()
    assert w.notified == []


def test_non_part_names_never_candidates(tmp_path):
    w = World(REAL, tmp_path, nums=(1,))
    for junk in (f"{PATCH}_part002.json.tmp", f"{PATCH}_part003.json.bak", "notes_part.json", "x_part1.json; rm -rf"):
        (w.rdir / junk).write_text("{}")
        w.age(w.rdir / junk, 20)
    assert w.run() == 0
    assert w.mac_parts() == [w.pname(1)]
    assert (w.rdir / f"{PATCH}_part002.json.tmp").exists() and (w.rdir / "x_part1.json; rm -rf").exists()


def test_gz_part_supported(tmp_path):
    import gzip

    w = World(REAL, tmp_path, nums=())
    w.write_part(w.rdir, 4, [5000, 5001])
    raw = (w.rdir / w.pname(4)).read_bytes()
    (w.rdir / w.pname(4)).unlink()
    gz = w.rdir / (w.pname(4) + ".gz")
    gz.write_bytes(gzip.compress(raw))
    w.age(gz, 10)
    w.set_json(w.rdir / "processed_ids.txt", w.base_ids + [5000, 5001])
    w.set_json(w.rdir / "part_counters.json", {PATCH: 4})
    for f in ("processed_ids.txt", "part_counters.json"):
        w.age(w.rdir / f, 3)
    assert w.run() == 0
    assert (w.mdir / (w.pname(4) + ".gz")).exists() and not gz.exists()


def test_truncated_part_aborts(tmp_path):
    w = World(REAL, tmp_path)
    p = w.rdir / w.pname(2)
    p.write_text(p.read_text()[:-30])
    w.age(p, 10)
    assert w.run() == 1
    assert w.all_remote_still_there() and w.mac_has_no_new_parts()
    assert len(w.notified) == 1 and "nothing deleted" in w.notified[0]


def test_dry_run_is_read_only_and_bounded(tmp_path):
    w = World(REAL, tmp_path, nums=(1, 2, 3, 4))
    mac_before = {p.name: p.read_bytes() for p in w.mdir.iterdir()}
    streams = []
    orig = w.remote.open_stream
    w.remote.open_stream = lambda path: (streams.append(path), orig(path))[1]
    assert w.run(dry_run=True) == 0
    assert w.remote_parts() == [w.pname(n) for n in (1, 2, 3, 4)]
    assert {p.name: p.read_bytes() for p in w.mdir.iterdir()} == mac_before
    assert not (w.mparent / "_offload_staging").exists()
    hashes = [c for c in w.cmds if c[:3] == ["shasum", "-a", "256"]]
    assert len(hashes) == 2 and len(streams) == 1
    assert not any(c[0] == "rsync" for c in w.cmds)
    assert not any(op_of(c) == "delete" for c in w.cmds)
    assert w.notified == []
    assert "WOULD copy+verify+move 4 parts" in (w.tmp / "offload.log").read_text()


def test_dry_run_abort_does_not_notify(tmp_path):
    w = World(REAL, tmp_path)
    w.set_json(w.rdir / "processed_ids.txt", w.base_ids)  # parts' ids missing
    w.age(w.rdir / "processed_ids.txt", 3)
    assert w.run(dry_run=True) == 1
    assert w.notified == [] and w.all_remote_still_there()


def test_abort_notifies_and_nothing_deleted(tmp_path):
    w = World(REAL, tmp_path)
    w.set_json(w.rdir / "processed_ids.txt", w.base_ids)
    w.age(w.rdir / "processed_ids.txt", 3)
    assert w.run() == 1
    assert len(w.notified) == 1 and w.notified[0].startswith("pub_parts_offload ABORT")
    assert "ABORT" in (w.tmp / "offload.log").read_text()


def test_busy_sweep_skips_then_allowed(tmp_path):
    w = World(REAL, tmp_path)
    fh = open(w.lock, "a+")
    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert w.run() == 3
        assert w.all_remote_still_there() and w.notified == []
        assert w.run(allow_busy_sweep=True) == 0  # quiescence + id checks still apply
        assert w.remote_parts() == []
    finally:
        fh.close()


def test_busy_sweep_with_stale_candidates_notifies(tmp_path):
    w = World(REAL, tmp_path, part_age_h=100)
    fh = open(w.lock, "a+")
    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert w.run() == 1
        assert w.all_remote_still_there() and "pub_recrawl.lock is held" in w.notified[0]
    finally:
        fh.close()


def test_local_staging_lock_prevents_parallel_runs(tmp_path):
    w = World(REAL, tmp_path)
    stage = w.mparent / "_offload_staging"
    stage.mkdir()
    fh = open(stage / ".offload.lock", "a+")
    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert w.run() == 1 and w.all_remote_still_there()
    finally:
        fh.close()


@pytest.mark.parametrize("error", [PermissionError, RuntimeError])
def test_link_failure_logs_traceback_and_notifies(tmp_path, monkeypatch, error):
    w = World(REAL, tmp_path)

    def fail_link(src, dest):
        raise error("Mac cannot link corpus file")

    monkeypatch.setattr(REAL.os, "link", fail_link)
    assert w.run() == REAL.EXIT_ABORT
    assert w.all_remote_still_there()
    assert not any(op_of(c) == "delete" for c in w.cmds)
    assert len(w.notified) == 1 and "nothing deleted on serv1" in w.notified[0]
    log = (w.tmp / "offload.log").read_text()
    assert "Traceback (most recent call last)" in log
    assert f"{error.__name__}: Mac cannot link corpus file" in log


@pytest.mark.parametrize("error", [PermissionError, RuntimeError])
def test_dry_run_control_report_unexpected_failure_notifies(tmp_path, monkeypatch, error):
    w = World(REAL, tmp_path, nums=())
    original = Path.read_bytes

    def fail_mac_read(path):
        if path == w.mdir / "processed_ids.txt":
            raise error("Mac cannot read control file")
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", fail_mac_read)
    assert w.run(dry_run=True) == REAL.EXIT_ABORT
    assert len(w.notified) == 1 and "Mac cannot read control file" in w.notified[0]
    assert "Traceback (most recent call last)" in (w.tmp / "offload.log").read_text()
    assert not any(op_of(c) == "delete" for c in w.cmds)


@pytest.mark.parametrize("failure", ["ssh", "timeout", "unexpected", "partial"])
def test_delete_failure_reports_unknown_and_rerun_reconciles(tmp_path, failure):
    w = World(REAL, tmp_path)

    def lose_reply(argv, res):
        if op_of(argv) != "delete":
            return
        assert res.returncode == 0  # the fake remote actually deleted the parts
        if failure == "ssh":
            res.returncode, res.stderr = 255, b"connection lost"
        elif failure == "timeout":
            raise subprocess.TimeoutExpired(argv, 600)
        elif failure == "partial":
            raise REAL.RemoteError("delete", 13, "reply lost after rm")
        else:
            raise RuntimeError("reply lost after rm")

    w.hooks_after.append(lose_reply)
    assert w.run() == REAL.EXIT_ABORT
    assert w.remote_parts() == []
    assert w.mac_parts() == [w.pname(n) for n in sorted(w.part_ids)]
    assert len(w.notified) == 1
    message = w.notified[0]
    assert "delete outcome UNKNOWN" in message
    assert "parts may have been deleted on serv1" in message
    assert "Mac copies are verified at" in message
    assert "nothing deleted" not in message and "nothing was deleted" not in message
    for name in w.mac_parts():
        assert str(w.mdir / name) in message
    assert "delete outcome UNKNOWN" in (w.tmp / "offload.log").read_text()
    before = {name: (w.mdir / name).read_bytes() for name in w.mac_parts()}
    w.hooks_after.clear()
    w.cmds.clear()
    assert w.run() == REAL.EXIT_OK
    assert any(op_of(c) == "list" for c in w.cmds)
    assert not any(op_of(c) == "delete" for c in w.cmds)
    assert {name: (w.mdir / name).read_bytes() for name in w.mac_parts()} == before


@pytest.mark.parametrize("failure", ["ssh", "timeout"])
def test_transport_failure_before_delete_reports_nothing_deleted(tmp_path, failure):
    w = World(REAL, tmp_path)

    def lose_connection(argv):
        if op_of(argv) == "list":
            if failure == "ssh":
                raise REAL.RemoteError("list", 255, "connection lost")
            raise subprocess.TimeoutExpired(argv, 300)

    w.hooks_before.append(lose_connection)
    assert w.run() == REAL.EXIT_ABORT
    assert w.all_remote_still_there()
    # hourly job: a first connect failure is only logged; it is notified once it persists (tests below)
    assert w.notified == []
    log = (w.tmp / "offload.log").read_text()
    assert "ABORT" in log and "nothing deleted on serv1" in log and "UNKNOWN" not in log
    assert not any(op_of(c) == "delete" for c in w.cmds)


@pytest.mark.parametrize("reuse", [False, True])
def test_final_files_and_directories_fsynced_before_delete(tmp_path, monkeypatch, reuse):
    w = World(REAL, tmp_path)
    if reuse:
        w.write_part(w.mdir, 1, w.part_ids[1])
    stage = w.cfg().staging_dir()
    paths = [w.mdir / w.pname(n) for n in sorted(w.part_ids)]
    paths += [w.mdir, stage / "parts", stage / "control", stage]
    # refreshed control files and the directory of the player-ids file (L1, 06.10)
    paths += [w.mdir / "processed_ids.txt", w.mdir / "part_counters.json",
              w.mparent / "pub_player_steam_ids.json", w.mparent]
    events, full = [], []
    original = os.fsync
    original_fcntl = fcntl.fcntl

    def which(fd):
        stat = os.fstat(fd)
        baks = [p for d in (w.mdir, w.mparent) for p in d.glob("*.bak_before_offload_*")]
        return next(p for p in paths + baks if (p.stat().st_dev, p.stat().st_ino) == (stat.st_dev, stat.st_ino))

    def record_sync(fd):
        events.append(which(fd))
        original(fd)

    def record_fcntl(fd, op, *a):
        if op == getattr(fcntl, "F_FULLFSYNC", object()):
            full.append(which(fd))
        return original_fcntl(fd, op, *a)

    def record_delete(argv):
        if op_of(argv) == "delete":
            events.append("delete")

    monkeypatch.setattr(REAL.os, "fsync", record_sync)
    monkeypatch.setattr(REAL.fcntl, "fcntl", record_fcntl)
    w.hooks_before.append(record_delete)
    assert w.run() == REAL.EXIT_OK
    assert events[-1] == "delete"
    baks = [p for d in (w.mdir, w.mparent) for p in d.glob("*.bak_before_offload_*")]
    assert len(baks) == 3  # all three Mac control files differ from serv1's in this World
    for path in paths + baks:
        assert events.count(path) == 1
        if hasattr(fcntl, "F_FULLFSYNC"):  # macOS: drive cache flushed too (L2, 06.10)
            assert full.count(path) == 1


@pytest.mark.parametrize("target", ["part", "corpus", "staging"])
def test_fsync_failure_prevents_delete(tmp_path, monkeypatch, target):
    w = World(REAL, tmp_path)
    paths = {"part": w.mdir / w.pname(1), "corpus": w.mdir, "staging": w.cfg().staging_dir()}
    original = os.fsync

    def fail_sync(fd):
        stat, expected = os.fstat(fd), paths[target].stat()
        if (stat.st_dev, stat.st_ino) == (expected.st_dev, expected.st_ino):
            raise OSError("fsync failed")
        original(fd)

    monkeypatch.setattr(REAL.os, "fsync", fail_sync)
    assert w.run() == REAL.EXIT_ABORT
    assert w.all_remote_still_there()
    assert not any(op_of(c) == "delete" for c in w.cmds)
    assert len(w.notified) == 1 and "nothing deleted on serv1" in w.notified[0]


@pytest.mark.skipif(not hasattr(fcntl, "F_FULLFSYNC"), reason="macOS-only fcntl")
@pytest.mark.parametrize("code,tolerated", [("ENOTSUP", True), ("EINVAL", True), ("EIO", False)])
def test_fullfsync_error_tolerated_only_when_unsupported(tmp_path, monkeypatch, code, tolerated):
    f = tmp_path / "f"
    f.write_text("x")
    original_fcntl = fcntl.fcntl

    def failing(fd, op, *a):
        if op == fcntl.F_FULLFSYNC:
            raise OSError(getattr(__import__("errno"), code), code)
        return original_fcntl(fd, op, *a)

    monkeypatch.setattr(REAL.fcntl, "fcntl", failing)
    if tolerated:
        REAL.durable_sync(f)
    else:
        with pytest.raises(OSError):
            REAL.durable_sync(f)


def test_fullfsync_io_error_prevents_delete(tmp_path, monkeypatch):
    if not hasattr(fcntl, "F_FULLFSYNC"):
        pytest.skip("macOS-only fcntl")
    import errno as _errno
    w = World(REAL, tmp_path)
    original_fcntl = fcntl.fcntl

    def failing(fd, op, *a):
        if op == fcntl.F_FULLFSYNC:
            raise OSError(_errno.EIO, "I/O error")
        return original_fcntl(fd, op, *a)

    monkeypatch.setattr(REAL.fcntl, "fcntl", failing)
    assert w.run() == REAL.EXIT_ABORT
    assert w.all_remote_still_there()
    assert not any(op_of(c) == "delete" for c in w.cmds)
    assert len(w.notified) == 1 and "nothing deleted on serv1" in w.notified[0]


@pytest.mark.parametrize("error", ["KeyboardInterrupt", "Terminated"])
@pytest.mark.parametrize("when", ["before_delete", "during_delete"])
def test_interrupt_is_reported_then_reraised(tmp_path, monkeypatch, error, when):
    w = World(REAL, tmp_path)
    exc = KeyboardInterrupt if error == "KeyboardInterrupt" else REAL.Terminated

    if when == "before_delete":
        def fail_link(src, dest):
            raise exc("SIGTERM")
        monkeypatch.setattr(REAL.os, "link", fail_link)
    else:
        def fail_delete(argv):
            if op_of(argv) == "delete":
                raise exc("SIGTERM")
        w.hooks_before.append(fail_delete)
    with pytest.raises(exc):
        w.run()
    assert len(w.notified) == 1 and "interrupted" in w.notified[0]
    if when == "before_delete":
        assert w.all_remote_still_there() and "nothing deleted on serv1" in w.notified[0]
    else:
        assert "delete outcome UNKNOWN" in w.notified[0]
    assert "Traceback (most recent call last)" in (w.tmp / "offload.log").read_text()


def test_main_turns_sigterm_and_sighup_into_terminated(tmp_path, monkeypatch):
    import signal
    seen = {}

    def fake_execute(cfg, remote, log, notify=None):
        seen.update({s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGHUP)})
        return 0

    saved = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGHUP)}
    monkeypatch.setattr(REAL, "execute", fake_execute)
    try:
        assert REAL.main(["--dry-run", "--log", str(tmp_path / "x.log")]) == 0
    finally:
        for s, h in saved.items():
            signal.signal(s, h)
    assert seen == {signal.SIGTERM: REAL._raise_terminated, signal.SIGHUP: REAL._raise_terminated}
    with pytest.raises(REAL.Terminated, match="SIGTERM"):
        REAL._raise_terminated(signal.SIGTERM, None)


# ======================================================================================
# temp_files phase (card ingame-xmas.2): serv1 keeps only map-id state; merged temp files go away
# ======================================================================================
def _temp_world(mod, tmp, **kw):
    w = World(mod, tmp, nums=(), **kw)
    w.enable_temps()
    w.add_temp("t1.txt", [2000, 2001, 2002])
    w.add_temp("t2.txt", [2003, 2004])
    return w


def test_temp_happy_path_deletes_merged_files_and_records_manifest(tmp_path):
    w = _temp_world(REAL, tmp_path)
    ctl = {n: (w.rdir / n).read_bytes() for n in ("processed_ids.txt", "part_counters.json")}
    ctl_player = (w.rparent / "pub_player_steam_ids.json").read_bytes()
    mac_before = sorted(p.name for p in w.mdir.iterdir())
    assert w.run() == 0
    assert w.remote_temps() == []
    # serv1 keeps the map-id state untouched; the Mac corpus dir gets nothing from the temp phase
    assert {n: (w.rdir / n).read_bytes() for n in ctl} == ctl
    assert (w.rparent / "pub_player_steam_ids.json").read_bytes() == ctl_player
    assert sorted(p.name for p in w.mdir.iterdir()) == mac_before
    assert w.notified == []
    events = {m["event"]: m for m in w.manifest()}
    assert {f["name"]: f["n_ids"] for f in events["temp_delete_intent"]["files"]} == {"t1.txt": 3, "t2.txt": 2}
    assert events["temp_deleted"]["count"] == 2 and events["temp_deleted"]["completed_at"] == w.completed_at
    assert "deleted 2 temp files" in (w.tmp / "offload.log").read_text()


def test_temp_phase_runs_after_the_parts_were_moved_in_the_same_run(tmp_path):
    w = World(REAL, tmp_path)  # 3 parts, all older than the min age
    w.enable_temps()
    w.add_temp("t1.txt", [2000, 2001])
    assert w.run() == 0
    assert w.remote_parts() == [] and w.mac_parts() == [w.pname(n) for n in (1, 2, 3)]
    assert w.remote_temps() == []
    ops = [op_of(c) for c in w.cmds if op_of(c) in ("delete", "tempcheck", "delete_temp")]
    assert ops == ["delete", "tempcheck", "delete_temp"]  # parts first, temp files only afterwards
    assert w.notified == []


def test_temp_not_attempted_when_the_part_phase_aborts(tmp_path):
    w = World(REAL, tmp_path)
    w.enable_temps()
    w.add_temp("t1.txt", [2000, 2001])
    p = w.rdir / w.pname(2)
    p.write_text(p.read_text()[:-30])  # truncated part: the whole part phase aborts
    w.age(p, 10)
    assert w.run() == 1
    assert w.temp_ops() == [] and w.remote_temps() == ["t1.txt"]


def test_temp_kept_files_are_reported_with_reasons_and_never_deleted(tmp_path):
    w = _temp_world(REAL, tmp_path)
    w.add_temp("miss.txt", [2005, 99999], known=[2005])
    full = json.dumps({str(i): {"id": i} for i in (2000, 2001)})
    w.add_temp("trunc.txt", [], raw=full[:-5])
    w.add_temp("fresh.txt", [2000], age_h=1)  # newer than the sweep's completed_at
    w.add_temp("empty.txt", [], raw="{}")
    w.add_temp("list.txt", [], raw="[2000, 2001]")
    w.add_temp("badkey.txt", [], raw=json.dumps({"abc": {"id": 2000}}))
    keep = {"miss.txt", "trunc.txt", "fresh.txt", "empty.txt", "list.txt", "badkey.txt"}
    assert w.run() == 0
    assert set(w.remote_temps()) == keep
    log = (w.tmp / "offload.log").read_text()
    for reason in ("ids_missing", "unparsable", "newer_than_sweep", "empty", "not_object", "bad_key"):
        assert reason in log
    for name in keep:
        assert name in log
    assert "deleted 2 temp files" in log and w.notified == []
    kept = w.manifest()[0]["kept"]
    assert {n for names in kept.values() for n in names} == keep


def test_temp_non_txt_files_and_subdirs_are_never_touched(tmp_path):
    w = _temp_world(REAL, tmp_path)
    (w.rtemp / "notes.json").write_text("{}")
    (w.rtemp / "x.txt.bak").write_text("{}")
    (w.rtemp / "sub").mkdir()
    (w.rtemp / "sub" / "inner.txt").write_text("{}")
    assert w.run() == 0
    assert w.remote_temps() == ["notes.json", "sub", "x.txt.bak"]
    assert (w.rtemp / "sub" / "inner.txt").exists()


@pytest.mark.parametrize("state", [
    {"status": "running"}, {"status": "failed", "failed_at": 1}, {"status": "preparing"},
    {"status": "complete"}, {"status": "complete", "completed_at": "soon"}, {"status": "complete", "completed_at": True},
    None, "corrupt"])
def test_temp_phase_skipped_unless_the_sweep_is_complete(tmp_path, state):
    w = _temp_world(REAL, tmp_path)
    if state is None:
        w.state_file.unlink()
    elif state == "corrupt":
        w.state_file.write_text("{not json")
    else:
        w.state_file.write_text(json.dumps(state))
    assert w.run() == 0
    assert w.temp_ops() == [] and w.remote_temps() == ["t1.txt", "t2.txt"]
    assert w.notified == []
    assert "temp phase skipped" in (w.tmp / "offload.log").read_text()


def test_temp_phase_off_switch(tmp_path):
    w = _temp_world(REAL, tmp_path)
    assert w.run(temp_phase=False) == 0
    assert w.temp_ops() == [] and w.remote_temps() == ["t1.txt", "t2.txt"]


def test_temp_dry_run_is_read_only(tmp_path):
    w = _temp_world(REAL, tmp_path)
    w.add_temp("miss.txt", [99999], known=False)
    assert w.run(dry_run=True) == 0
    assert w.remote_temps() == ["miss.txt", "t1.txt", "t2.txt"]
    assert w.temp_ops() == ["tempcheck"]  # parsed on serv1, nothing deleted
    assert w.manifest() == [] and not (w.tmp / "offload_state.json").exists()
    assert not (w.mparent / "_offload_staging").exists()
    log = (w.tmp / "offload.log").read_text()
    assert "dry-run: WOULD delete 2 temp files" in log and "miss.txt" in log
    assert w.notified == []


def test_temp_unchanged_verdict_is_not_recomputed_every_hour(tmp_path):
    w = _temp_world(REAL, tmp_path)
    w.add_temp("miss.txt", [99999], known=False)
    assert w.run() == 0
    assert w.remote_temps() == ["miss.txt"] and w.temp_ops().count("tempcheck") == 1
    w.cmds.clear()
    assert w.run() == 0  # same sweep, same leftover, same processed_ids: nothing can have changed
    assert w.temp_ops() == []
    assert "unchanged" in (w.tmp / "offload.log").read_text()
    w.age(w.rtemp / "miss.txt", 19)  # the leftover changes -> checked again
    assert w.run() == 0
    assert w.temp_ops() == ["tempcheck"]


def test_temp_delete_lost_reply_reports_unknown_and_rerun_is_clean(tmp_path):
    w = _temp_world(REAL, tmp_path)

    def lose_reply(argv, res):
        if op_of(argv) == "delete_temp":
            assert res.returncode == 0  # the fake remote really deleted the files
            res.returncode, res.stderr = 255, b"connection lost"

    w.hooks_after.append(lose_reply)
    assert w.run() == REAL.EXIT_ABORT
    assert w.remote_temps() == []
    assert len(w.notified) == 1
    msg = w.notified[0]
    assert "temp_files delete outcome UNKNOWN" in msg and str(w.manifest_file) in msg
    assert "nothing deleted" not in msg
    assert [m["event"] for m in w.manifest()] == ["temp_delete_intent"]  # intent is on disk before the delete is sent
    w.hooks_after.clear()
    w.cmds.clear()
    assert w.run() == 0 and w.temp_ops() == []
    assert len(w.notified) == 1


def test_temp_refused_when_a_newer_sweep_completed_before_the_delete(tmp_path):
    w = _temp_world(REAL, tmp_path)

    def newer_sweep(argv):
        if op_of(argv) == "delete_temp":
            w.write_state("complete", 1.0)  # another sweep finished meanwhile: different completed_at

    w.hooks_before.append(newer_sweep)
    assert w.run() == 1
    assert w.remote_temps() == ["t1.txt", "t2.txt"]
    assert "nothing deleted on serv1" in w.notified[0]


def test_abort_after_a_successful_part_move_does_not_claim_nothing_was_deleted(tmp_path):
    w = World(REAL, tmp_path)
    w.enable_temps()
    w.add_temp("t1.txt", [2000, 2001])

    def sweep_starts(argv):
        if op_of(argv) == "delete_temp":
            w.write_state("running")

    w.hooks_before.append(sweep_starts)
    assert w.run() == 1
    assert w.remote_parts() == [] and w.remote_temps() == ["t1.txt"]
    msg = w.notified[0]
    assert "parts of this run were already moved" in msg and "no temp file was deleted" in msg
    assert "UNKNOWN" not in msg


# ---- schedule-safe notifications: hourly runs must not spam -------------------------------
@pytest.mark.parametrize("failure", ["ssh255", "timeout"])
def test_transient_connect_failure_is_silent_until_persistent(tmp_path, failure):
    w = World(REAL, tmp_path, nums=())
    t0 = NOW
    assert w.run(now=lambda: t0) == 0  # a good contact with serv1 first

    def down(argv):
        if op_of(argv) == "list":
            if failure == "ssh255":
                raise REAL.RemoteError("list", 255, "ssh: connect to host serv1 port 22: Operation timed out")
            raise subprocess.TimeoutExpired(argv, 300)

    w.hooks_before.append(down)
    assert w.run(now=lambda: t0 + 1 * HOUR) == REAL.EXIT_ABORT
    assert w.run(now=lambda: t0 + 5 * HOUR) == REAL.EXIT_ABORT
    assert w.notified == []  # < 6 h since the last contact: logged only
    assert (w.tmp / "offload.log").read_text().count("ABORT") == 2
    assert w.run(now=lambda: t0 + 7 * HOUR) == REAL.EXIT_ABORT
    assert len(w.notified) == 1 and "unreachable" in w.notified[0] and "7.0 h" in w.notified[0]
    assert w.run(now=lambda: t0 + 8 * HOUR) == REAL.EXIT_ABORT
    assert w.run(now=lambda: t0 + 20 * HOUR) == REAL.EXIT_ABORT
    assert len(w.notified) == 1  # not hourly
    assert w.run(now=lambda: t0 + 32 * HOUR) == REAL.EXIT_ABORT
    assert len(w.notified) == 2  # a reminder after 24 h
    w.hooks_before.clear()
    assert w.run(now=lambda: t0 + 33 * HOUR) == 0  # recovered: the failure clock resets
    w.hooks_before.append(down)
    assert w.run(now=lambda: t0 + 34 * HOUR) == REAL.EXIT_ABORT
    assert len(w.notified) == 2


def test_transient_failure_without_any_previous_contact_starts_the_clock_at_the_first_failure(tmp_path):
    w = World(REAL, tmp_path, nums=())

    def down(argv):
        raise REAL.RemoteError("list", 255, "ssh: Could not resolve hostname serv1")

    w.hooks_before.append(down)
    t0 = NOW
    for h in (0, 3, 5):
        assert w.run(now=lambda h=h: t0 + h * HOUR) == REAL.EXIT_ABORT
    assert w.notified == []
    assert w.run(now=lambda: t0 + 6.5 * HOUR) == REAL.EXIT_ABORT
    assert len(w.notified) == 1


def test_connect_failure_rule_does_not_hide_a_real_abort(tmp_path):
    w = World(REAL, tmp_path)
    w.set_json(w.rdir / "processed_ids.txt", w.base_ids)  # parts' ids missing: a real fail-closed abort
    w.age(w.rdir / "processed_ids.txt", 3)
    assert w.run(now=lambda: NOW) == 1
    assert len(w.notified) == 1 and "NOT in serv1 processed_ids" in w.notified[0]


def test_repeated_identical_abort_is_notified_once_per_day(tmp_path):
    w = World(REAL, tmp_path)
    w.set_json(w.rdir / "processed_ids.txt", w.base_ids)
    w.age(w.rdir / "processed_ids.txt", 3)
    t0 = NOW
    for h in (0, 1, 2, 3):
        assert w.run(now=lambda h=h: t0 + h * HOUR) == 1
    assert len(w.notified) == 1
    assert (w.tmp / "offload.log").read_text().count("ABORT") == 4  # every failure stays in the log
    # a different failure is a new signal
    w.set_json(w.rdir / "processed_ids.txt", w.base_ids + [i for v in w.part_ids.values() for i in v])
    w.age(w.rdir / "processed_ids.txt", 3)
    w.set_json(w.rdir / "part_counters.json", {PATCH: 1})
    w.age(w.rdir / "part_counters.json", 3.5)  # other mtime: rsync's quick check would reuse the staged copy
    assert w.run(now=lambda: t0 + 4 * HOUR) == 1
    assert len(w.notified) == 2 and "part counter" in w.notified[1]
    assert w.run(now=lambda: t0 + 5 * HOUR) == 1
    assert len(w.notified) == 2
    assert w.run(now=lambda: t0 + 30 * HOUR) == 1
    assert len(w.notified) == 3  # daily reminder while it persists


def test_dry_run_never_writes_the_state_file(tmp_path):
    w = World(REAL, tmp_path)
    w.set_json(w.rdir / "processed_ids.txt", w.base_ids)
    w.age(w.rdir / "processed_ids.txt", 3)
    assert w.run(dry_run=True) == 1
    assert not (w.tmp / "offload_state.json").exists()


def test_unwritable_state_file_never_breaks_a_run(tmp_path):
    w = World(REAL, tmp_path)
    assert w.run(state_path=Path("/nonexistent_root_dir/deeper/state.json")) == 0
    assert w.remote_parts() == []


# ---- schedule / defaults ------------------------------------------------------------------
def test_defaults_hourly_friendly():
    cfg = REAL.Config()
    assert cfg.min_age_hours == 1.0 and cfg.quiet_minutes == 30.0
    assert cfg.min_age_hours * 60 > cfg.quiet_minutes  # a moved part is always older than the processed_ids quiet window
    assert cfg.temp_phase is True
    assert cfg.remote_temp_dir == "/root/main/bets_data/analise_pub_matches/temp_files"
    assert cfg.remote_state == "/root/main/runtime/pub_recrawl.json"
    assert cfg.transient_notify_hours == 6.0 and cfg.abort_renotify_hours == 24.0


def test_cli_flags_reach_config(tmp_path, monkeypatch):
    seen = {}

    def fake_execute(cfg, remote, log, notify=None, **kw):
        seen["cfg"] = cfg
        return 0

    monkeypatch.setattr(REAL, "execute", fake_execute)
    import signal
    saved = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGHUP)}
    try:
        assert REAL.main(["--log", str(tmp_path / "x.log")]) == 0
        assert seen["cfg"].min_age_hours == 1.0 and seen["cfg"].temp_phase is True
        assert REAL.main(["--log", str(tmp_path / "x.log"), "--no-temp-phase", "--min-age-hours", "2"]) == 0
        assert seen["cfg"].temp_phase is False and seen["cfg"].min_age_hours == 2.0
    finally:
        for s, h in saved.items():
            signal.signal(s, h)


def test_part_younger_than_one_hour_waits_and_older_moves(tmp_path):
    w = World(REAL, tmp_path, nums=(1, 2))
    w.age(w.rdir / w.pname(2), 0.5)
    assert w.run() == 0
    assert w.mac_parts() == [w.pname(1)] and w.remote_parts() == [w.pname(2)]


PLIST = ROOT / "scripts/ops/launchd/com.ingame.pub-parts-offload.plist"


def test_plist_is_hourly_calendar_interval():
    d = plistlib.loads(PLIST.read_bytes())
    sci = d["StartCalendarInterval"]
    assert set(sci) == {"Minute"} and 0 <= sci["Minute"] < 60  # no Hour key -> every hour
    assert "StartInterval" not in d and not d.get("RunAtLoad", False)
    assert d["ProgramArguments"][-1].endswith("scripts/ops/pub_parts_offload.sh")
    if shutil.which("plutil"):
        r = subprocess.run(["plutil", "-lint", str(PLIST)], capture_output=True, text=True)
        assert r.returncode == 0, r.stdout + r.stderr


def test_docs_describe_the_temp_phase_and_the_schedule():
    layout = (ROOT / "docs/SERVER_LAYOUT.md").read_text()
    assert "temp_files" in layout and "pub_recrawl.json" in layout and "каждый час" in layout
    assert "ежедневно 06:40" not in layout
    assert "temp_files" in REAL.__doc__ and "hourly" in REAL.__doc__.lower()


# ======================================================================================
# guard scenarios: pass on the real module, must FAIL when the guard is mutated away
# ======================================================================================
def scn_merge_process(mod, tmp):
    w = World(mod, tmp)
    w.remote.merge_procs = lambda: [{"pid": 1, "cmd": "python maps_research.py --merge-temp-files"}]
    assert w.run() == 1
    assert w.all_remote_still_there() and w.mac_has_no_new_parts()


def scn_sweep_lock(mod, tmp):
    w = World(mod, tmp)
    fh = open(w.lock, "a+")
    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert w.run() == 3
        assert w.all_remote_still_there() and w.mac_has_no_new_parts()
    finally:
        fh.close()


def scn_lock_held_at_delete(mod, tmp):
    w = World(mod, tmp)
    held = []

    def grab(argv):
        if op_of(argv) == "delete":  # a sweep starts between the checks and the delete
            fh = open(w.lock, "a+")
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            held.append(fh)

    w.hooks_before.append(grab)
    try:
        assert w.run() == 1
        assert w.all_remote_still_there()
    finally:
        for fh in held:
            fh.close()


def scn_quiescent_age(mod, tmp):
    w = World(mod, tmp, ctl_age_h=5 / 60)  # processed_ids / counters touched 5 min ago
    assert w.run() == 1
    assert w.all_remote_still_there() and w.mac_has_no_new_parts()


def scn_in_progress_tmp(mod, tmp):
    w = World(mod, tmp)
    (w.rdir / f"{PATCH}_part004.json.tmp").write_text("{")  # merge writing right now
    assert w.run() == 1
    assert w.all_remote_still_there() and w.mac_has_no_new_parts()


def scn_candidate_age(mod, tmp):
    w = World(mod, tmp, nums=(1, 2))
    w.write_part(w.rdir, 3, [7000, 7001])
    w.age(w.rdir / w.pname(3), 0.5)  # 30 min old (< the 1 h default): too fresh
    w.set_json(w.rdir / "processed_ids.txt", w.base_ids + [7000, 7001] + w.part_ids[1] + w.part_ids[2])
    w.set_json(w.rdir / "part_counters.json", {PATCH: 3})
    for f in ("processed_ids.txt", "part_counters.json"):
        w.age(w.rdir / f, 3)
    assert w.run() == 0
    assert w.remote_parts() == [w.pname(3)]
    assert w.mac_parts() == [w.pname(1), w.pname(2)]


def scn_hash_equal(mod, tmp):
    w = World(mod, tmp)

    def corrupt(argv, res):
        staged = Path(argv[-1]) / w.pname(2)
        if argv[0] == "rsync" and argv[-1].endswith("parts/") and staged.exists():
            staged.write_bytes(staged.read_bytes().replace(b"yyyy", b"zzzz", 1))  # still valid JSON

    w.hooks_after.append(corrupt)
    assert w.run() == 1
    assert w.all_remote_still_there() and w.mac_has_no_new_parts()


def scn_name_collision(mod, tmp):
    w = World(mod, tmp)
    # same map ids and same size (so neither the id nor the size check can catch it), different bytes
    (w.mdir / w.pname(2)).write_text(json.dumps({str(i): {"id": i, "payload": "zzzz" * 8} for i in w.part_ids[2]}))
    assert (w.mdir / w.pname(2)).stat().st_size == (w.rdir / w.pname(2)).stat().st_size
    before = (w.mdir / w.pname(2)).read_bytes()
    assert w.run() == 1
    assert w.all_remote_still_there()
    assert (w.mdir / w.pname(2)).read_bytes() == before
    assert w.mac_parts() == [w.pname(2)]


def scn_ids_present(mod, tmp):
    w = World(mod, tmp)
    ids = [i for v in w.part_ids.values() for i in v if i != w.part_ids[2][1]] + w.base_ids
    w.set_json(w.rdir / "processed_ids.txt", ids)  # one map of part 2 is unknown to serv1
    w.age(w.rdir / "processed_ids.txt", 3)
    assert w.run() == 1
    assert w.all_remote_still_there() and w.mac_has_no_new_parts()
    assert "NOT in serv1 processed_ids.txt" in (w.tmp / "offload.log").read_text()


def scn_ids_nonempty(mod, tmp):
    w = World(mod, tmp, nums=(1,))
    (w.rdir / w.pname(1)).write_text("{}")
    w.age(w.rdir / w.pname(1), 10)
    assert w.run() == 1
    assert (w.rdir / w.pname(1)).exists()


def scn_mac_ids_subset(mod, tmp):
    w = World(mod, tmp)
    w.set_json(w.mdir / "processed_ids.txt", w.base_ids + [424242])  # Mac has an id serv1 lacks
    assert w.run() == 1
    assert w.all_remote_still_there() and w.mac_has_no_new_parts()
    assert 424242 in json.loads((w.mdir / "processed_ids.txt").read_text())
    assert not any(".bak_before_offload_" in p.name for p in w.mdir.iterdir())


def scn_counter_vs_mac(mod, tmp):
    w = World(mod, tmp)
    w.set_json(w.mdir / "part_counters.json", {PATCH: 99})
    assert w.run() == 1
    assert w.all_remote_still_there() and w.mac_has_no_new_parts()
    assert json.loads((w.mdir / "part_counters.json").read_text()) == {PATCH: 99}


def scn_counter_vs_part(mod, tmp):
    w = World(mod, tmp)
    w.set_json(w.rdir / "part_counters.json", {PATCH: 1})  # parts 2,3 exist but counter says 1
    w.age(w.rdir / "part_counters.json", 3)
    assert w.run() == 1
    assert w.all_remote_still_there() and w.mac_has_no_new_parts()


def scn_stamp_recheck(mod, tmp):
    w = World(mod, tmp)

    def touch(argv):
        if op_of(argv) == "delete":  # part 2 modified after it was hashed, before the rm
            p = w.rdir / w.pname(2)
            with open(p, "ab") as f:
                f.write(b" ")

    w.hooks_before.append(touch)
    assert w.run() == 1
    assert w.all_remote_still_there()  # nothing deleted, not even the unchanged parts
    assert "changed since hashing" in (w.tmp / "offload.log").read_text()


def scn_control_stamp_recheck(mod, tmp):
    w = World(mod, tmp)

    def rewrite_ids(argv):
        if op_of(argv) == "delete":  # a merge rewrote processed_ids.txt during the run
            p = w.rdir / "processed_ids.txt"
            p.write_text(p.read_text() + " ")

    w.hooks_before.append(rewrite_ids)
    assert w.run() == 1
    assert w.all_remote_still_there()
    assert "control file changed during the run" in (w.tmp / "offload.log").read_text()


def scn_name_safety(mod, tmp):
    w = World(mod, tmp, nums=(1,))
    victim = w.tmp / "serv1" / "escape_part001.json"  # outside the parts dir
    victim.write_text("{}")
    st = victim.stat()
    guards = [{"path": str(w.rdir / g), "size": (w.rdir / g).stat().st_size, "mtime_ns": (w.rdir / g).stat().st_mtime_ns}
              for g in ("processed_ids.txt", "part_counters.json")]
    entry = {"name": "../escape_part001.json", "size": st.st_size, "mtime_ns": st.st_mtime_ns}
    try:
        w.remote.delete_verified(str(w.rdir), str(w.lock), False, [entry], guards)
    except mod.RemoteError:
        pass
    assert victim.exists()


def scn_control_backup(mod, tmp):
    w = World(mod, tmp)
    old = (w.mdir / "processed_ids.txt").read_bytes()
    assert w.run() == 0
    baks = [p for p in w.mdir.iterdir() if p.name.startswith("processed_ids.txt.bak_before_offload_")]
    assert len(baks) == 1 and baks[0].read_bytes() == old


def scn_local_reader(mod, tmp):
    # a real process whose command line names the dict builder (default marker), e.g. a pub dict rebuild
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", "base/explore_database.py"])
    try:
        w = World(mod, tmp)
        default = mod.Config().local_reader_markers
        assert w.run(local_reader_markers=default) == mod.EXIT_SKIP
        assert w.all_remote_still_there() and w.mac_has_no_new_parts()
        assert not any(op_of(c) == "delete" for c in w.cmds) and w.notified == []
        assert "reads the Mac pub corpus" in (w.tmp / "offload.log").read_text()
    finally:
        proc.kill()
        proc.wait()


# Command lines as `ps -axo pid=,command=` printed them on the Mac, 06.10.2026 ~20:45 MSK, during the
# ingame-g5yv dict rebuild (driver between groups = only the driver line is alive) and rebuild_dicts.sh.
LIVE_READER_CMDLINES = [
    "/Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/3.9/Resources/Python.app"
    "/Contents/MacOS/Python base/explore_database.py",
    "/Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/3.9/Resources/Python.app"
    "/Contents/MacOS/Python runtime/artifacts/pubs-rebuild/rebuild_20261006/build_driver.py",
    "/bin/bash scripts/run/rebuild_dicts.sh",
    # 08.10.2026 22:53 MSK `pgrep -fl build_driver` (ingame-g5yv stage-2 rebuild): the dated driver name
    "caffeinate -ims venv_catboost/bin/python3 runtime/experiments/pubs-rebuild/build_driver_20261008.py",
]


@pytest.mark.parametrize("cmd", LIVE_READER_CMDLINES)
def test_local_reader_markers_cover_builders_and_their_drivers(cmd):
    # pure match: an end-to-end ps run would also see whatever build happens to be alive on this Mac
    assert any(m in cmd for m in REAL.Config().local_reader_markers)
    assert not any(m in "/usr/sbin/cron" or m in "python3 scripts/ops/pub_parts_offload.py"
                   for m in REAL.Config().local_reader_markers)


@pytest.mark.parametrize("cmd", [
    "tail -F runtime/artifacts/pubs-rebuild/pub_parts_offload.log",  # a Monitor on the offload's own log
    "/bin/bash /Users/alex/Documents/ingame/scripts/ops/pub_parts_offload.sh",  # the launchd job itself
])
def test_offload_log_watchers_and_the_job_itself_are_not_readers(cmd):
    assert not any(m in cmd for m in REAL.Config().local_reader_markers)


def test_own_ancestor_shell_with_a_marker_is_not_a_reader(tmp_path):
    # Shape of an agent Bash call: `zsh -c "<python ...> ; true"` stays alive as the parent and its argv
    # carries the whole command (here the marker); the guard must ignore its own ancestors only.
    marker = "__offload_ancestor_marker_%d__" % os.getpid()
    code = ("import importlib.util, sys, types\n"
            f"spec = importlib.util.spec_from_file_location('o', {str(SRC)!r})\n"
            "m = importlib.util.module_from_spec(spec); sys.modules['o'] = m; spec.loader.exec_module(m)\n"
            f"cfg = m.Config(local_reader_markers=({marker!r},))\n"
            "try:\n"
            "    m.Offload._guard_no_local_reader(types.SimpleNamespace(cfg=cfg, log=lambda *a: None))\n"
            "    print('NO_READER')\n"
            "except m.Skip as e:\n"
            "    print('SKIP', e)\n")
    script = tmp_path / "probe.py"
    script.write_text(code)
    shell = f"{shlex.quote(sys.executable)} {shlex.quote(str(script))} {marker}; true"
    res = subprocess.run(["/bin/bash", "-c", shell], capture_output=True, text=True, timeout=60)
    assert res.stdout.strip() == "NO_READER", res.stdout + res.stderr
    # ...while the same marker in an unrelated live process still blocks
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", marker])
    try:
        res = subprocess.run(["/bin/bash", "-c", shell], capture_output=True, text=True, timeout=60)
        assert res.stdout.startswith("SKIP") and f"pid {proc.pid}" in res.stdout, res.stdout + res.stderr
    finally:
        proc.kill()
        proc.wait()


def test_dict_build_starting_mid_run_aborts_before_mac_commit(tmp_path):
    w = World(REAL, tmp_path)
    marker = "__offload_test_reader_%d__" % os.getpid()
    procs = []

    def start_reader_at_first_rsync(argv):
        if not procs and any("rsync" in str(a) for a in argv):
            procs.append(subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", marker]))

    w.hooks_before.append(start_reader_at_first_rsync)
    try:
        assert w.run(local_reader_markers=(marker,)) == REAL.EXIT_ABORT
    finally:
        for proc in procs:
            proc.kill()
            proc.wait()
    assert procs, "the hook never saw an rsync command"
    assert w.all_remote_still_there() and w.mac_has_no_new_parts()
    assert not any(op_of(c) == "delete" for c in w.cmds)
    assert len(w.notified) == 1 and "started during the run" in w.notified[0]
    assert "time.sleep" not in w.notified[0]  # pid + marker go to the chat, the command line only to the log
    assert "time.sleep" in (w.tmp / "offload.log").read_text()
    assert "nothing deleted on serv1" in w.notified[0]


# ---- temp_files phase guards -------------------------------------------------------------
def _kept_ok(w, keep, deleted=("t1.txt", "t2.txt")):
    """Everything in `keep` is still on serv1, every file in `deleted` is gone, exit 0 already asserted."""
    left = set(w.remote_temps())
    assert set(keep) <= left, f"deleted a file that must stay: {set(keep) - left}"
    assert not (set(deleted) & left), f"did not delete {set(deleted) & left}"


def scn_temp_status_complete(mod, tmp):
    w = _temp_world(mod, tmp)
    # a sweep is mid-way; its merge has not run: temp files are its dedupe set. The stale completed_at of the
    # previous sweep is left in the file so that only the status check can stop the phase.
    w.write_state("running", completed_at=int(NOW - 40 * HOUR))
    assert w.run() == 0
    assert w.remote_temps() == ["t1.txt", "t2.txt"] and w.temp_ops() == []


def scn_temp_parts_remaining(mod, tmp):
    w = _temp_world(mod, tmp)
    w.write_part(w.rdir, 1, [3000, 3001])
    w.age(w.rdir / w.pname(1), 0.2)  # not a candidate yet (< min age), but still a part on serv1
    assert w.run() == 0
    assert w.remote_parts() == [w.pname(1)]
    assert w.remote_temps() == ["t1.txt", "t2.txt"] and w.temp_ops() == []


def scn_temp_lock_free(mod, tmp):
    w = _temp_world(mod, tmp)
    fh = open(w.lock, "a+")
    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert w.run() == 0
        assert w.remote_temps() == ["t1.txt", "t2.txt"] and w.temp_ops() == []
    finally:
        fh.close()


def scn_temp_merge_process(mod, tmp):
    w = _temp_world(mod, tmp)
    w.remote.merge_procs = lambda: [{"pid": 1, "cmd": "python maps_research.py --merge-temp-files"}]
    assert w.run() == 0
    assert w.remote_temps() == ["t1.txt", "t2.txt"] and w.temp_ops() == []


def scn_temp_quiescent(mod, tmp):
    w = _temp_world(mod, tmp)
    w.age(w.rdir / "processed_ids.txt", 5 / 60)  # rewritten 5 min ago: a merge may still be writing
    assert w.run() == 0
    assert w.remote_temps() == ["t1.txt", "t2.txt"] and w.temp_ops() == []


def scn_temp_verdict_ok_only(mod, tmp):
    w = _temp_world(mod, tmp)
    real_check = w.remote.temp_check

    def forged(*a, **k):
        res = real_check(*a, **k)
        res["files"]["t2.txt"].update(ok=False, reason="forged")
        return res

    w.remote.temp_check = forged
    assert w.run() == 0
    assert w.remote_temps() == ["t2.txt"]


def scn_tc_newer_than_sweep(mod, tmp):
    w = _temp_world(mod, tmp)
    w.add_temp("fresh.txt", [2000, 2001], age_h=1)  # written after the sweep completed (5 h ago)
    assert w.run() == 0
    _kept_ok(w, ["fresh.txt"])


def scn_tc_too_large(mod, tmp):
    w = _temp_world(mod, tmp)
    w.add_temp("big.txt", list(range(3000, 3200)))  # ~8 KB, all ids known
    assert w.run(temp_max_file_bytes=4000) == 0
    _kept_ok(w, ["big.txt"])


def scn_tc_unparsable(mod, tmp):
    w = _temp_world(mod, tmp)
    full = json.dumps({str(i): {"id": i, "payload": "yyyy"} for i in (2000, 2001, 2002)})
    w.add_temp("trunc.txt", [], raw=full[:-30])  # every id that is still readable is known to serv1
    assert w.run() == 0
    _kept_ok(w, ["trunc.txt"])
    assert "unparsable" in (w.tmp / "offload.log").read_text()


def scn_tc_not_object(mod, tmp):
    w = _temp_world(mod, tmp)
    w.add_temp("list.txt", [], raw="[2000, 2001]")
    assert w.run() == 0
    _kept_ok(w, ["list.txt"])


def scn_tc_empty(mod, tmp):
    w = _temp_world(mod, tmp)
    w.add_temp("empty.txt", [], raw="{}")
    assert w.run() == 0
    _kept_ok(w, ["empty.txt"])


def scn_tc_bad_key(mod, tmp):
    w = _temp_world(mod, tmp)
    w.add_temp("badkey.txt", [], raw=json.dumps({"abc": {"id": 2000}}))
    assert w.run() == 0
    _kept_ok(w, ["badkey.txt"])


def scn_tc_ids_missing(mod, tmp):
    w = _temp_world(mod, tmp)
    w.add_temp("miss.txt", [2005, 99999], known=[2005])  # one map of this file is unknown to processed_ids.txt
    assert w.run() == 0
    _kept_ok(w, ["miss.txt"])
    assert "ids_missing" in (w.tmp / "offload.log").read_text()


def _delete_refused(mod, tmp, hook, expect_in_log=None, files=("t1.txt", "t2.txt")):
    w = _temp_world(mod, tmp)
    held = []
    w.hooks_before.append(lambda argv: hook(w, argv, held) if op_of(argv) == "delete_temp" else None)
    try:
        assert w.run() == 1
        assert w.remote_temps() == sorted(files)  # not even the unchanged file was deleted
        assert len(w.notified) == 1 and "nothing deleted on serv1" in w.notified[0]
        if expect_in_log:
            assert expect_in_log in (w.tmp / "offload.log").read_text()
    finally:
        for fh in held:
            fh.close()
    return w


def scn_dt_lock_held(mod, tmp):
    def hook(w, argv, held):  # a sweep starts between the checks and the delete
        fh = open(w.lock, "a+")
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        held.append(fh)

    _delete_refused(mod, tmp, hook, "pub_recrawl.lock is held")


def scn_dt_status(mod, tmp):
    _delete_refused(mod, tmp, lambda w, argv, held: w.write_state("running"), "no longer complete")


def scn_dt_parts_remain(mod, tmp):
    def hook(w, argv, held):
        w.write_part(w.rdir, 9, [7000])  # a part appeared on serv1 meanwhile

    w = _delete_refused(mod, tmp, hook, "part file")
    assert w.remote_parts() == [w.pname(9)]


def scn_dt_stamp(mod, tmp):
    def hook(w, argv, held):
        with open(w.rtemp / "t2.txt", "ab") as f:  # modified after it was verified
            f.write(b" ")

    _delete_refused(mod, tmp, hook, "changed since the check")


def scn_dt_guards(mod, tmp):
    def hook(w, argv, held):
        p = w.rdir / "processed_ids.txt"  # a merge rewrote the dedupe state during the run
        p.write_text(p.read_text() + " ")

    _delete_refused(mod, tmp, hook, "control file changed during the run")


def _stamp_entry(path):
    st = path.stat()
    return {"name": path.name, "size": st.st_size, "mtime_ns": st.st_mtime_ns}


def _direct_delete(w, entries):
    guards = [{"path": str(w.rdir / g), "size": (w.rdir / g).stat().st_size, "mtime_ns": (w.rdir / g).stat().st_mtime_ns}
              for g in ("processed_ids.txt", "part_counters.json")]
    try:
        w.remote.delete_temp(str(w.rtemp), str(w.lock), str(w.state_file), str(w.rdir), w.completed_at, entries, guards)
    except w.mod.RemoteError:
        pass


def scn_dt_newer(mod, tmp):
    w = _temp_world(mod, tmp)
    fresh = w.add_temp("fresh.txt", [2000], age_h=1)  # newer than completed_at, stamp is exactly right
    _direct_delete(w, [_stamp_entry(fresh)])
    assert fresh.exists()


def scn_dt_name_safety(mod, tmp):
    w = _temp_world(mod, tmp)
    outside = w.rparent / "escape.txt"
    outside.write_text("{}")
    w.age(outside, 20)
    non_txt = w.rtemp / "t3.json"
    non_txt.write_text("{}")
    w.age(non_txt, 20)
    inner = w.rtemp / "sub"
    inner.mkdir()
    (inner / "x.txt").write_text("{}")
    w.age(inner / "x.txt", 20)
    for name, path in (("../escape.txt", outside), ("t3.json", non_txt), ("sub/x.txt", inner / "x.txt")):
        entry = _stamp_entry(path)
        entry["name"] = name
        _direct_delete(w, [entry])
    assert outside.exists() and non_txt.exists() and (inner / "x.txt").exists()


# 09.10.2026 02:00 MSK: an agent waiter shell (ingame-59, pid 68861, alive 7 h) had this shape; its
# command line named explore_database.py only as a pgrep pattern, so the 00:40 and 01:40 runs SKIPped
# on it (and after the real build pid 5160 ended, every later run would have).
WAITER_CMDLINE = ("/bin/zsh -c source /Users/alex/.claude/shell-snapshots/snapshot-zsh-1791453677202-b9i3pi.sh "
                  "2>/dev/null || true && cd /Users/alex/Documents/ingame && until ! pgrep -f explore_database.py "
                  ">/dev/null && [ \"$(memory_pressure | tail -1 | grep -o '[0-9]*')\" -ge 50 ]; do sleep 300; done")


def scn_local_reader_waiter(mod, tmp):
    # a live shell that only WAITS for a build (marker inside a pgrep pattern) is not a corpus reader
    proc = subprocess.Popen(["/bin/sh", "-c", "sleep 60; pgrep -f base/explore_database.py >/dev/null; true"])
    try:
        w = World(mod, tmp)
        assert w.run(local_reader_markers=mod.Config().local_reader_markers) == 0
        assert "reads the Mac pub corpus" not in (w.tmp / "offload.log").read_text()
    finally:
        proc.kill()
        proc.wait()


def test_captured_waiter_cmdline_is_not_a_reader_but_builders_still_are():
    probe = REAL._SEARCH_CMD_RE.sub(" ", WAITER_CMDLINE)
    assert not any(m in probe for m in REAL.Config().local_reader_markers)
    for cmd in LIVE_READER_CMDLINES:
        assert any(m in REAL._SEARCH_CMD_RE.sub(" ", cmd) for m in REAL.Config().local_reader_markers)


SCENARIOS = {
    "merge_process": scn_merge_process,
    "sweep_lock": scn_sweep_lock,
    "lock_held_at_delete": scn_lock_held_at_delete,
    "quiescent_age": scn_quiescent_age,
    "in_progress_tmp": scn_in_progress_tmp,
    "candidate_age": scn_candidate_age,
    "hash_equal": scn_hash_equal,
    "name_collision": scn_name_collision,
    "ids_present": scn_ids_present,
    "ids_nonempty": scn_ids_nonempty,
    "mac_ids_subset": scn_mac_ids_subset,
    "counter_vs_mac": scn_counter_vs_mac,
    "counter_vs_part": scn_counter_vs_part,
    "stamp_recheck": scn_stamp_recheck,
    "control_stamp_recheck": scn_control_stamp_recheck,
    "name_safety": scn_name_safety,
    "control_backup": scn_control_backup,
    "local_reader": scn_local_reader,
    "local_reader_waiter": scn_local_reader_waiter,
    "temp_status_complete": scn_temp_status_complete,
    "temp_parts_remaining": scn_temp_parts_remaining,
    "temp_lock_free": scn_temp_lock_free,
    "temp_merge_process": scn_temp_merge_process,
    "temp_quiescent": scn_temp_quiescent,
    "temp_verdict_ok_only": scn_temp_verdict_ok_only,
    "tc_newer_than_sweep": scn_tc_newer_than_sweep,
    "tc_too_large": scn_tc_too_large,
    "tc_unparsable": scn_tc_unparsable,
    "tc_not_object": scn_tc_not_object,
    "tc_empty": scn_tc_empty,
    "tc_bad_key": scn_tc_bad_key,
    "tc_ids_missing": scn_tc_ids_missing,
    "dt_lock_held": scn_dt_lock_held,
    "dt_status": scn_dt_status,
    "dt_parts_remain": scn_dt_parts_remain,
    "dt_stamp": scn_dt_stamp,
    "dt_guards": scn_dt_guards,
    "dt_newer": scn_dt_newer,
    "dt_name_safety": scn_dt_name_safety,
}
assert SCENARIOS.keys() == MUTATIONS.keys()


@pytest.mark.parametrize("guard", sorted(SCENARIOS))
def test_guard_scenario(guard, tmp_path):
    SCENARIOS[guard](module_for(guard, tmp_path), tmp_path / "w")


@pytest.mark.parametrize("guard", sorted(SCENARIOS))
def test_guard_scenario_goes_red_when_guard_is_removed(guard, tmp_path):
    mutated = mutated_module(guard, tmp_path / "mut")
    with pytest.raises(AssertionError):
        SCENARIOS[guard](mutated, tmp_path / "w")


def test_script_wrapper_syntax():
    r = subprocess.run(["bash", "-n", str(ROOT / "scripts/ops/pub_parts_offload.sh")], capture_output=True)
    assert r.returncode == 0, r.stderr
