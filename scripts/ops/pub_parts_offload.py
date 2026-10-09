#!/usr/bin/env python3
"""Move finished pub-corpus part files from serv1 to the Mac, then free serv1 right after a sweep.

Documented design: docs/SERVER_LAYOUT.md (pub-corpus row). Owner rule (08.10.2026): serv1
keeps only the map ids used to compare maps (`processed_ids.txt`, `processed_ids_to_graph.txt`,
`part_counters.json`, `pub_player_steam_ids.json`, `trash_maps.txt`, ...); the collected
data lives on the Mac and is moved there and removed from serv1 as soon as a sweep ends.
Two phases per run, parts first:

PHASE 1 - part files `<patch>_partNNN.json[.gz]`. The crawler on serv1 only ever CREATES new
part numbers (base/maps_research.py:3027 `_open_part`, number from `_next_part_numbers`
:2797 = max(disk, part_counters)+1, written via `<name>.tmp` + os.replace :3030-3033), it
never reopens an existing part, so no part needs to be excluded as "still appendable".
Copy, sha256-verify, id-verify, then delete exactly those files on serv1.

PHASE 2 - `temp_files/*.txt` (base/maps_research.py:1360-1364 merges them with cleanup=False,
so they pile up: 14,064 files / 4.9 GB on 08.10.2026). Each is a JSON object keyed by map id.
A temp file is deleted only when ALL hold (the sweep's merge already wrote its data into a part
file, and every part is on the Mac):
  1. serv1 `pub_recrawl.lock` is free AND `runtime/pub_recrawl.json` says status == "complete".
     A failed/running sweep resumes using temp_files as its dedupe set (maps_research.py:1107-1124)
     and its merge has not run yet, so nothing is touched until the sweep is complete.
  2. no part file (or `.tmp`) remains on serv1 after phase 1, i.e. all parts are verified on the Mac.
  3. per file, parsed completely ON serv1 (nice -n 10, numpy; only the verdict list crosses ssh):
     mtime < the sweep's completed_at, a non-empty JSON object, every key a numeric map id and
     EVERY id present in serv1 `processed_ids.txt` (the merge adds an id there only after writing
     its record: maps_research.py:3269-3270, saved at :3313). Unparsable / empty / newer / any-id-missing /
     oversize files are KEPT and reported with reason and name (log + manifest), never deleted.
  4. the delete runs under the sweep lock and re-checks lock, status/completed_at, parts, the
     control-file stamps and each file's size+mtime_ns; the intent is written to the Mac manifest
     (`runtime/artifacts/pubs-rebuild/pub_temp_cleanup_manifest.jsonl`) BEFORE the delete is sent.
Phase 2 never touches the Mac corpus and never deletes anything in serv1's corpus directory.
Preconditions that are merely "not yet" (sweep running, parts still moving, lock held, merge
running, state files <30 min old) skip phase 2 quietly (INFO log); the next run retries.

Everything is FAIL CLOSED: any failed check aborts the whole run (exit 1, admin notified).
Before deletion is sent, nothing is deleted on serv1; failures after sending it report an
UNKNOWN outcome and the verified Mac paths. Deletion only touches the exact
files that were copied, sha256-verified, id-verified and are byte-for-byte unchanged
(size + mtime_ns) since they were listed.

Schedule: HOURLY (launchd StartCalendarInterval Minute=40, scripts/ops/launchd/*.plist), so serv1
is cleaned within ~1-2 h of a sweep ending instead of up to 24 h. Consequences handled here:
  * --min-age-hours defaults to 1 (was 6): a part is created whole (`.tmp` + os.replace) by a
    merge that runs inside the sweep under pub_recrawl.lock, and the lock, the merge-process check,
    the in-progress `.tmp` check and the 30-min processed_ids/part_counters quiet window plus the
    stamp re-check at delete time already prove the part is final; the 1 h age only has to exceed
    the quiet window (30 min) so a part is never moved while `processed_ids.txt` is still fresh.
  * ABORT notifications are deduplicated (state: runtime/artifacts/pubs-rebuild/pub_parts_offload_state.json):
    an ssh/connect failure before any contact with serv1 is only logged, and notified once when serv1
    has been unreachable for >6 h (reminder every 24 h); every other ABORT is notified once per distinct
    message per 24 h; a delete outcome UNKNOWN/PARTIAL is always notified. SKIP after 72 h as before.
  * the temp verdict is cached per (completed_at, control-file stamps, temp listing): an unchanged
    leftover set is not re-parsed every hour.

Exit codes: 0 ok (also "0 candidates"), 1 ABORT (nothing deleted unless the message says
UNKNOWN/PARTIAL), 2 usage, 3 SKIP (a sweep holds serv1's pub_recrawl.lock, or a local
dict build (explore_database.py / rebuild_dicts.sh / build_driver.py) reads the Mac corpus;
nothing done; notified after 72 h; a build that starts mid-run = ABORT before the Mac commit).

Usage: pub_parts_offload.py [--dry-run] [--min-age-hours 1] [--quiet-minutes 30]
                            [--allow-busy-sweep] [--no-temp-phase] [--host serv1]
"""
from __future__ import annotations

import argparse
import dataclasses
import errno
import fcntl
import gzip
import hashlib
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Callable, Iterable, Optional

REPO = Path(__file__).resolve().parents[2]

# `pgrep|pkill|grep <opts> <pattern>` up to the next shell separator (see _guard_no_local_reader).
_SEARCH_CMD_RE = re.compile(r"\b(?:pgrep|pkill|grep)\b[^;|&\n]*")


class Terminated(BaseException):
    """SIGTERM/SIGHUP as an exception that the inner `except Exception` blocks cannot swallow."""


def _raise_terminated(signum, _frame):
    raise Terminated(signal.Signals(signum).name)


NO_FULLFSYNC_ERRNOS = frozenset(e for e in (getattr(errno, n, None) for n in
                                            ("ENOTSUP", "EOPNOTSUPP", "ENOTTY", "EINVAL")) if e is not None)


def durable_sync(path: Path) -> None:
    """os.fsync, then F_FULLFSYNC where available: on macOS plain fsync may leave data in the drive cache."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
        full = getattr(fcntl, "F_FULLFSYNC", None)
        if full is not None:
            try:
                fcntl.fcntl(fd, full)
            except OSError as exc:
                # Only "this filesystem has no F_FULLFSYNC" is tolerated (os.fsync above succeeded);
                # a real I/O error from the cache flush aborts the run before anything is deleted.
                if exc.errno not in NO_FULLFSYNC_ERRNOS:
                    raise
    finally:
        os.close(fd)

PART_RE = re.compile(r"^(?P<patch>[A-Za-z0-9._-]+?)_part(?P<num>\d+)\.json(?:\.gz)?$")
TMP_RE = re.compile(r"_part\d+\.json(?:\.gz)?\.tmp$")
CONTROL_IN_PARTS_DIR = ("processed_ids.txt", "part_counters.json")
PLAYER_IDS_NAME = "pub_player_steam_ids.json"
GUARD_FILES = ("processed_ids.txt", "part_counters.json")

TEMP_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+\.txt$")
PART_ISH_RE = re.compile(r"_part\d+\.json")  # any part-like name, also .tmp/.gz/.bak: all block phase 2

EXIT_OK, EXIT_ABORT, EXIT_USAGE, EXIT_SKIP = 0, 1, 2, 3


class Abort(Exception):
    """A fail-closed check failed; the run stops and nothing more is deleted."""

    def __init__(self, message: str, partial_delete: bool = False):
        super().__init__(message)
        self.partial_delete = partial_delete


class NotQuiet(Abort):
    """processed_ids/part_counters/a part .tmp was modified < quiet_minutes ago (phase 2 treats it as 'not yet')."""


class Skip(Exception):
    """Nothing to do right now (serv1 sweep holds the lock); not an error."""


# --------------------------------------------------------------------------------------
# Remote side. The remote operations are a stdlib-only python helper sent over stdin, so
# the very same code runs on serv1 (via ssh) and, in tests, on a local fake directory.
# --------------------------------------------------------------------------------------
REMOTE_HELPER = r'''
import fcntl, json, os, re, subprocess, sys, time

NAME_RE = re.compile(r"^[A-Za-z0-9._-]+_part[0-9]+\.json(\.gz)?$")


def out(obj):
    sys.stdout.write(json.dumps(obj))
    sys.stdout.flush()


def fail(code, msg):
    sys.stderr.write(msg + "\n")
    sys.exit(code)


def stamp(path):
    try:
        st = os.lstat(path)
    except OSError:
        return None
    if not os.path.isfile(path) or os.path.islink(path):
        return None
    return {"size": st.st_size, "mtime_ns": st.st_mtime_ns}


def same(path, want):
    cur = stamp(path)
    return cur is not None and cur["size"] == want["size"] and cur["mtime_ns"] == want["mtime_ns"]


# ---- temp_files phase helpers ----
TEMP_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+\.txt$")
PART_ISH_RE = re.compile(r"_part[0-9]+\.json")


def read_state(path):
    """runtime/pub_recrawl.json as a dict, or None when missing / unreadable / not an object."""
    try:
        with open(path, "rb") as fh:
            doc = json.loads(fh.read())
    except (OSError, ValueError):
        return None
    return doc if isinstance(doc, dict) else None


def unchanged_temp(path, want):
    return same(path, want)


def strict_loads(raw):
    """orjson when present (what the merge parses with), else json that refuses NaN/Infinity."""
    try:
        import orjson
    except ImportError:
        orjson = None
    if orjson is not None:
        return orjson.loads(raw)

    def refuse(const):
        raise ValueError("non-finite JSON constant " + const)

    return json.loads(raw, parse_constant=refuse)


def norm_key(k):
    """Map id as maps_research._normalize_map_id accepts it, else None."""
    if isinstance(k, bool):
        return None
    if isinstance(k, int):
        return k
    if isinstance(k, str):
        s = k.strip()
        if s.isascii() and (s.isdigit() or (s.startswith("-") and s[1:].isdigit())):
            return int(s)
    return None


def check_one(known, path, st, completed_ns, max_bytes):
    """Verdict for one temp file: ok only if complete, parseable and every map id is in `known`."""
    v = {"size": st["size"], "mtime_ns": st["mtime_ns"], "ok": False}
    if st["mtime_ns"] >= completed_ns:
        v["reason"] = "newer_than_sweep"
        return v
    if st["size"] > max_bytes:
        v["reason"] = "too_large"
        return v
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
        doc = strict_loads(raw)
    except Exception as exc:
        v["reason"] = "unparsable"
        v["detail"] = ("%s: %s" % (type(exc).__name__, exc))[:120]
        return v
    raw = None
    if not isinstance(doc, dict):
        v["reason"] = "not_object"
        return v
    n_keys = len(doc)
    if not n_keys:
        v["reason"] = "empty"
        return v
    ids = []
    for key in doc:
        n = norm_key(key)
        if n is None:
            v["reason"] = "bad_key"
            return v
        ids.append(n)
    doc = None
    try:
        keys = np.array(ids, dtype=np.int64)
    except OverflowError:
        v["reason"] = "bad_key"
        return v
    if known.size:
        pos = np.searchsorted(known, keys)
        pos[pos == known.size] = known.size - 1
        absent = known[pos] != keys
        n_missing = int(absent.sum())
        sample = [int(x) for x in keys[absent][:3]]
    else:
        n_missing, sample = len(ids), ids[:3]
    v["n_ids"] = len(ids)
    if n_missing:
        v["reason"] = "ids_missing"
        v["missing"] = n_missing
        v["sample_missing"] = sample
        return v
    v["ok"] = True
    return v


op = sys.argv[1]
if op == "list":
    d = sys.argv[2]
    files = {}
    for e in os.scandir(d):
        s = stamp(e.path)
        if s is not None:
            files[e.name] = s
    out({"now_ns": time.time_ns(), "files": files})
elif op == "stat":
    out({"now_ns": time.time_ns(), "files": {p: stamp(p) for p in sys.argv[2:]}})
elif op == "lock":
    fh = open(sys.argv[2], "a+")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        out({"busy": False})
    except BlockingIOError:
        out({"busy": True})
elif op == "procs":
    hits = []
    if os.path.isdir("/proc"):
        for pid in os.listdir("/proc"):
            if not pid.isdigit() or int(pid) == os.getpid():
                continue
            try:
                with open("/proc/%s/cmdline" % pid, "rb") as f:
                    cmd = f.read().replace(b"\0", b" ").decode("utf-8", "replace")
            except OSError:
                continue
            if "maps_research" in cmd and ("--merge-temp-files" in cmd or "merge_temp_files" in cmd):
                hits.append({"pid": int(pid), "cmd": cmd[:160]})
    out({"hits": hits[:5]})
elif op == "delete":
    # argv: delete <dir> <lockfile> <require_lock 0|1>; stdin: {"files":[{name,size,mtime_ns}],
    # "guards":[{path,size,mtime_ns}]}. Verify EVERYTHING first, then rm explicit paths.
    d, lockfile, require_lock = sys.argv[2], sys.argv[3], sys.argv[4] == "1"
    req = json.load(sys.stdin)
    lock_fh = open(lockfile, "a+")
    try:
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        if require_lock:
            fail(11, "pub_recrawl.lock is held: a sweep started during the run")
    for g in req["guards"]:
        if not same(g["path"], g):
            fail(12, "control file changed during the run: %s" % g["path"])
    paths = []
    for f in req["files"]:
        if not NAME_RE.match(f["name"]) or "/" in f["name"]:
            fail(12, "refusing unexpected name: %r" % f["name"])
        p = os.path.join(d, f["name"])
        if not same(p, f):
            fail(12, "part file changed since hashing (size/mtime): %s" % f["name"])
        paths.append((p, f))
    before = os.statvfs(d)
    deleted = []
    for p, f in paths:
        if not same(p, f):
            fail(13, "part file changed right before rm: %s; deleted so far: %s"
                 % (f["name"], ",".join(deleted)))
        r = subprocess.run(["rm", "--", p], capture_output=True, text=True)
        if r.returncode != 0:
            fail(13, "rm failed for %s: %s; deleted so far: %s" % (f["name"], r.stderr.strip(), ",".join(deleted)))
        deleted.append(f["name"])
    after = os.statvfs(d)
    out({"deleted": deleted, "freed_bytes_reported": sum(f["size"] for _, f in paths),
         "avail_before": before.f_bavail * before.f_frsize, "avail_after": after.f_bavail * after.f_frsize})
elif op == "state":
    # argv: state <pub_recrawl.json>; the three fields the temp phase needs
    if not os.path.exists(sys.argv[2]):
        out({"exists": False})
    else:
        doc = read_state(sys.argv[2])
        if doc is None:
            out({"exists": True, "error": "unreadable or not a JSON object"})
        else:
            out({"exists": True, "state": {k: doc.get(k) for k in ("status", "started_at", "completed_at")}})
elif op == "tlist":
    # argv: tlist <temp_dir>; like `list` but only temp *.txt files and a missing directory is not an error
    d = sys.argv[2]
    files = {}
    if os.path.isdir(d):
        for e in os.scandir(d):
            if TEMP_NAME_RE.match(e.name):
                s = stamp(e.path)
                if s is not None:
                    files[e.name] = s
    out({"now_ns": time.time_ns(), "exists": os.path.isdir(d), "files": files})
elif op == "tempcheck":
    # argv: tempcheck <temp_dir> <processed_ids.txt> <completed_at_s> <max_file_bytes>. Read-only.
    # Parses every temp file COMPLETELY here (nothing is copied); sorted int64 array + searchsorted, not a set.
    import numpy as np

    d, ids_path, completed_at, max_bytes = sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
    ids_stamp = stamp(ids_path)
    if ids_stamp is None:
        fail(16, "processed_ids.txt is missing: %s" % ids_path)
    with open(ids_path, "rb") as fh:
        ids_doc = strict_loads(fh.read())
    if not isinstance(ids_doc, list):
        fail(16, "processed_ids.txt is not a JSON list")
    vals = [x for x in ids_doc if type(x) is int]
    vals += [n for n in map(norm_key, (x for x in ids_doc if type(x) is not int)) if n is not None]
    del ids_doc
    try:
        known = np.unique(np.array(vals, dtype=np.int64))
    except OverflowError:
        fail(16, "processed_ids.txt holds an id beyond int64")
    del vals
    if not same(ids_path, ids_stamp):
        fail(16, "processed_ids.txt changed while it was loaded")
    files = {}
    for e in sorted(os.scandir(d), key=lambda ent: ent.name):
        if not TEMP_NAME_RE.match(e.name):
            continue
        st = stamp(e.path)
        if st is None:
            continue
        files[e.name] = check_one(known, e.path, st, completed_at * 10 ** 9, max_bytes)
    out({"now_ns": time.time_ns(), "completed_at": completed_at, "ids_stamp": ids_stamp,
         "n_known": int(known.size), "files": files})
elif op == "delete_temp":
    # argv: delete_temp <temp_dir> <lockfile> <state_path> <parts_dir>; stdin: {"completed_at", "files":[{name,size,
    # mtime_ns}], "guards":[{path,size,mtime_ns}]}. Under the sweep lock: verify EVERYTHING first, then unlink.
    d, lockfile, state_path, parts_dir = sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
    req = json.load(sys.stdin)
    want_completed = req["completed_at"]
    lock_fh = open(lockfile, "a+")
    try:
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fail(11, "pub_recrawl.lock is held: a sweep started before the temp_files delete")
    cur_state = read_state(state_path)
    if cur_state is None or cur_state.get("status") != "complete" or cur_state.get("completed_at") != want_completed:
        fail(14, "sweep status is no longer complete (or another sweep completed): %r"
             % (cur_state.get("status") if cur_state else None))
    left_parts = [n for n in os.listdir(parts_dir) if PART_ISH_RE.search(n)]
    if left_parts:
        fail(15, "a part file is on serv1 (%s): the part phase has not finished" % left_parts[0])
    for gd in req["guards"]:
        if not same(gd["path"], gd):
            fail(12, "control file changed during the run: %s" % gd["path"])
    paths = []
    for f in req["files"]:
        if not TEMP_NAME_RE.match(f["name"]) or "/" in f["name"]:
            fail(12, "refusing unexpected name: %r" % f["name"])
        if f["mtime_ns"] >= want_completed * 10 ** 9:
            fail(12, "temp file is newer than the sweep's completed_at: %s" % f["name"])
        p = os.path.join(d, f["name"])
        if not unchanged_temp(p, f):
            fail(12, "temp file changed since the check (size/mtime): %s" % f["name"])
        paths.append((p, f))
    before = os.statvfs(d)
    deleted = []
    for p, f in paths:
        if not unchanged_temp(p, f):
            fail(13, "temp file changed right before unlink: %s; deleted so far: %d" % (f["name"], len(deleted)))
        try:
            os.unlink(p)
        except OSError as exc:
            fail(13, "unlink failed for %s: %s; deleted so far: %d" % (f["name"], exc, len(deleted)))
        deleted.append(f["name"])
    after = os.statvfs(d)
    out({"deleted": deleted, "freed_bytes_reported": sum(f["size"] for _, f in paths),
         "avail_before": before.f_bavail * before.f_frsize, "avail_after": after.f_bavail * after.f_frsize})
else:
    fail(2, "unknown op %s" % op)
'''


class Remote:
    """serv1 (or, in tests, a local directory) behind a pluggable command runner."""

    def __init__(
        self,
        host: str = "serv1",
        *,
        use_ssh: bool = True,
        python: str = "python3",
        io_prefix: Iterable[str] = ("ionice", "-c3", "nice", "-n", "15"),
        sha_argv: Iterable[str] = ("sha256sum",),
        runner: Optional[Callable[..., subprocess.CompletedProcess]] = None,
        np_python: str = "/root/venvnp/bin/python",  # needs numpy (serv1: 3.12); only the temp check uses it
        nice_prefix: Iterable[str] = ("nice", "-n", "10"),  # NOT ionice -c3: idle class starved a 4.9 GB read for 25+ min
    ):
        self.host = host
        self.use_ssh = use_ssh
        self.python = python
        self.io_prefix = list(io_prefix)
        self.sha_argv = list(sha_argv)
        self.runner = runner or subprocess.run
        self.np_python = np_python
        self.nice_prefix = list(nice_prefix)

    # -- command construction ------------------------------------------------------------
    def wrap(self, argv: list[str]) -> list[str]:
        if not self.use_ssh:
            return list(argv)
        return ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20",
                "-o", "ServerAliveInterval=30", self.host, shlex.join(argv)]

    def run(self, argv: list[str], *, input: Optional[bytes] = None, timeout: Optional[int] = 600):
        return self.runner(self.wrap(argv), input=input, capture_output=True, timeout=timeout)

    def helper(self, op: str, *args: str, stdin: Optional[dict] = None, timeout: int = 300,
               python: Optional[str] = None, prefix: Iterable[str] = ()) -> dict:
        """Run one op of REMOTE_HELPER (program via -c, JSON request via stdin)."""
        proc = self.run([*prefix, python or self.python, "-c", REMOTE_HELPER, op, *args],
                        input=json.dumps(stdin).encode() if stdin is not None else b"", timeout=timeout)
        if proc.returncode != 0:
            raise RemoteError(op, proc.returncode, proc.stderr.decode("utf-8", "replace").strip())
        try:
            return json.loads(proc.stdout.decode())
        except ValueError as exc:
            raise RemoteError(op, 0, f"unparseable helper output: {exc}")

    # -- operations ------------------------------------------------------------------------
    def list_dir(self, directory: str) -> dict:
        return self.helper("list", directory)

    def stat_paths(self, paths: list[str]) -> dict:
        return self.helper("stat", *paths)

    def lock_busy(self, lock_path: str) -> bool:
        return bool(self.helper("lock", lock_path)["busy"])

    def merge_procs(self) -> list[dict]:
        return self.helper("procs")["hits"]

    def sha256(self, path: str) -> str:
        proc = self.run(self.io_prefix + self.sha_argv + ["--", path], timeout=3600)
        if proc.returncode != 0:
            raise RemoteError("sha256", proc.returncode, proc.stderr.decode("utf-8", "replace").strip())
        token = proc.stdout.decode().split()[0] if proc.stdout.strip() else ""
        if not re.fullmatch(r"[0-9a-f]{64}", token):
            raise RemoteError("sha256", 0, f"bad sha256 output for {path}: {proc.stdout[:80]!r}")
        return token

    def read_bytes(self, path: str) -> bytes:
        proc = self.run(self.io_prefix + ["cat", "--", path], timeout=3600)
        if proc.returncode != 0:
            raise RemoteError("cat", proc.returncode, proc.stderr.decode("utf-8", "replace").strip())
        return proc.stdout

    def open_stream(self, path: str):
        """Popen of `cat path` on the remote side; caller reads .stdout and calls wait()."""
        return subprocess.Popen(self.wrap(self.io_prefix + ["cat", "--", path]),
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def fetch(self, remote_path: str, dest_dir: Path) -> None:
        argv = ["rsync", "-a", "--partial", "--timeout=300"]
        if self.use_ssh:
            argv += ["--rsync-path=" + shlex.join(self.io_prefix + ["rsync"]),
                     "-e", "ssh -o BatchMode=yes -o ConnectTimeout=20 -o ServerAliveInterval=30",
                     f"{self.host}:{remote_path}"]
        else:
            argv += [remote_path]
        argv.append(str(dest_dir) + "/")
        proc = self.runner(argv, capture_output=True, timeout=4 * 3600)
        if proc.returncode != 0:
            raise RemoteError("rsync", proc.returncode, proc.stderr.decode("utf-8", "replace").strip())

    def delete_verified(self, directory: str, lock_path: str, require_lock: bool,
                        files: list[dict], guards: list[dict]) -> dict:
        return self.helper("delete", directory, lock_path, "1" if require_lock else "0",
                           stdin={"files": files, "guards": guards}, timeout=600)

    # -- temp_files phase ------------------------------------------------------------------
    def read_state(self, state_path: str) -> dict:
        return self.helper("state", state_path)

    def tlist(self, directory: str) -> dict:
        return self.helper("tlist", directory)

    def temp_check(self, temp_dir: str, ids_path: str, completed_at: int, max_bytes: int) -> dict:
        """Parse every temp file on the remote host (numpy, nice); returns per-file verdicts only."""
        return self.helper("tempcheck", temp_dir, ids_path, str(int(completed_at)), str(int(max_bytes)),
                           timeout=7200, python=self.np_python, prefix=self.nice_prefix)

    def delete_temp(self, temp_dir: str, lock_path: str, state_path: str, parts_dir: str, completed_at: int,
                    files: list[dict], guards: list[dict]) -> dict:
        return self.helper("delete_temp", temp_dir, lock_path, state_path, parts_dir,
                           stdin={"completed_at": int(completed_at), "files": files, "guards": guards}, timeout=1800)


class RemoteError(Exception):
    def __init__(self, op: str, code: int, detail: str):
        super().__init__(f"remote {op} failed (exit {code}): {detail[:500]}")
        self.op, self.code, self.detail = op, code, detail


# --------------------------------------------------------------------------------------
# Config / logging
# --------------------------------------------------------------------------------------
@dataclasses.dataclass
class Config:
    remote_dir: str = "/root/main/bets_data/analise_pub_matches/json_parts_split_from_object"
    remote_parent: str = "/root/main/bets_data/analise_pub_matches"
    remote_lock: str = "/root/main/runtime/pub_recrawl.lock"
    mac_dir: Path = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches/json_parts_split_from_object")
    mac_parent: Path = Path("/Users/alex/Documents/ingame/bets_data/analise_pub_matches")
    staging: Optional[Path] = None  # default: <mac_parent>/_offload_staging (same filesystem)
    log_path: Path = REPO / "runtime/artifacts/pubs-rebuild/pub_parts_offload.log"
    min_age_hours: float = 1.0  # hourly schedule; must stay > quiet_minutes (see the module docstring)
    quiet_minutes: float = 30.0
    allow_busy_sweep: bool = False
    dry_run: bool = False
    min_free_margin_bytes: int = 2 * 1024 ** 3
    stale_notify_hours: float = 72.0
    # --- phase 2: temp_files (card ingame-xmas.2) ---
    temp_phase: bool = True
    remote_temp_dir: str = "/root/main/bets_data/analise_pub_matches/temp_files"
    remote_state: str = "/root/main/runtime/pub_recrawl.json"
    temp_max_file_bytes: int = 128 * 1024 ** 2  # a bigger temp file is kept: parsing it would need GBs of RAM on serv1
    temp_manifest_path: Optional[Path] = REPO / "runtime/artifacts/pubs-rebuild/pub_temp_cleanup_manifest.jsonl"
    temp_log_names: int = 20  # names per kept-reason in the log line (the manifest has all of them)
    # --- hourly schedule: keep notifications quiet ---
    state_path: Optional[Path] = REPO / "runtime/artifacts/pubs-rebuild/pub_parts_offload_state.json"
    transient_notify_hours: float = 6.0  # serv1 unreachable (ssh failure before any contact) for this long -> one message
    abort_renotify_hours: float = 24.0  # the same ABORT message / unreachable reminder at most this often
    dry_run_max_hashes: int = 2
    dry_run_max_id_checks: int = 1
    # Local readers of mac_dir: base/explore_database.py globs the parts once per process, so a
    # multi-group dict build would see different corpora if parts arrived between its groups.
    # A multi-group driver (runtime/artifacts/pubs-rebuild/*/build_driver.py, scripts/run/rebuild_dicts.sh)
    # sleeps between groups with no explore_database.py alive, so the drivers are markers too. Not the
    # directory "pubs-rebuild/": the offload's own log lives there and a `tail -F` of it would block runs.
    local_reader_markers: tuple = ("explore_database.py", "rebuild_dicts.sh", "build_driver")  # also build_driver_<date>.py

    def staging_dir(self) -> Path:
        return self.staging or (Path(self.mac_parent) / "_offload_staging")


class Log:
    def __init__(self, path: Optional[Path], echo: bool = True):
        self.path, self.echo = path, echo

    def __call__(self, level: str, msg: str) -> None:
        line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {level} {msg}"
        if self.echo:
            print(line, flush=True)
        if self.path is not None:
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                with open(self.path, "a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
            except OSError as exc:  # logging must never break a run
                print(f"log write failed: {exc}", file=sys.stderr)


class RunState:
    """Small Mac-side json that lets an HOURLY job stay quiet: last successful contact with serv1, the last
    notified ABORT (signature + time), and the last temp_files verdict key. Losing or corrupting it only
    costs one extra notification / one extra parse, never a skipped guard."""

    def __init__(self, path: Optional[Path], now: Callable[[], float] = time.time):
        self.path, self.now, self.data = path, now, {}
        if path is not None:
            try:
                doc = json.loads(Path(path).read_text(encoding="utf-8"))
                if isinstance(doc, dict):
                    self.data = doc
            except (OSError, ValueError):
                pass

    def save(self, log: Optional[Callable[[str, str], None]] = None) -> None:
        if self.path is None:
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_name(self.path.name + ".tmp")
            tmp.write_text(json.dumps(self.data, indent=1, sort_keys=True), encoding="utf-8")
            os.replace(tmp, self.path)
        except OSError as exc:  # never break a run over bookkeeping
            if log is not None:
                log("WARN", f"state file {self.path} not written: {exc}")

    def contact(self) -> None:
        """serv1 answered: the unreachable clock restarts."""
        self.data["last_contact"] = self.now()
        self.data.pop("first_transient", None)
        self.data.pop("transient_notified", None)

    @staticmethod
    def _num(v) -> Optional[float]:
        return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None

    def transient_should_notify(self, threshold_h: float, renotify_h: float) -> tuple[bool, float]:
        t = self.now()
        since = self._num(self.data.get("last_contact"))
        if since is None:
            since = self._num(self.data.setdefault("first_transient", t)) or t
        down_h = (t - since) / 3600
        if down_h < threshold_h:
            return False, down_h
        last = self._num(self.data.get("transient_notified"))
        if last is not None and (t - last) / 3600 < renotify_h:
            return False, down_h
        self.data["transient_notified"] = t
        return True, down_h

    def abort_should_notify(self, signature: str, renotify_h: float) -> bool:
        t = self.now()
        last = self._num(self.data.get("abort_ts"))
        if self.data.get("abort_sig") == signature and last is not None and (t - last) / 3600 < renotify_h:
            return False
        self.data["abort_sig"], self.data["abort_ts"] = signature, t
        return True


def abort_signature(message: str) -> str:
    """Numbers (minutes, counts, ids, pids) vary between hourly repeats of the same failure."""
    return re.sub(r"\d+(?:\.\d+)?", "#", message)[:240]


def _is_epoch(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def normalize_id(value) -> Optional[int]:
    """Same acceptance as maps_research._normalize_map_id (base/maps_research.py:149)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        s = value.strip()
        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            return int(s)
    return None


def iter_top_level_keys(source, gz: bool) -> Iterable[str]:
    """Top-level object keys of a part, streamed like maps_research._iter_json_object_keys
    (base/maps_research.py:175). Reimplemented: importing maps_research loads keys/proxies."""
    import ijson

    is_path = isinstance(source, (str, os.PathLike))
    if gz:
        handle = gzip.open(source, "rb")  # path or binary stream
    else:
        handle = open(source, "rb") if is_path else source
    try:
        for prefix, event, value in ijson.parse(handle):
            if prefix == "" and event == "map_key":
                yield value
    finally:
        if gz or is_path:
            handle.close()


def load_ids(blob: bytes, what: str) -> set[int]:
    import orjson

    try:
        data = orjson.loads(blob)
    except Exception as exc:
        raise Abort(f"{what}: not parseable JSON ({exc})")
    if not isinstance(data, list):
        raise Abort(f"{what}: expected a JSON list of ids, got {type(data).__name__}")
    ids = set()
    for v in data:
        n = normalize_id(v)
        if n is not None:
            ids.add(n)
    return ids


def load_counters(blob: bytes, what: str) -> dict[str, int]:
    try:
        data = json.loads(blob)
        if not isinstance(data, dict):
            raise ValueError("not an object")
        return {str(k): int(v) for k, v in data.items()}
    except Exception as exc:
        raise Abort(f"{what}: bad part_counters ({exc})")


# --------------------------------------------------------------------------------------
# The offload
# --------------------------------------------------------------------------------------
class Offload:
    def __init__(self, cfg: Config, remote: Remote, log: Log, now: Callable[[], float] = time.time,
                 state: Optional[RunState] = None):
        self.cfg, self.remote, self.log, self.now, self.state = cfg, remote, log, now, state
        self.hashes_done = 0
        self.id_checks_done = 0
        self.delete_sent = False  # parts delete sent and its reply not (yet) seen
        self.parts_deleted_ok = False  # parts delete confirmed by the remote
        self.temp_delete_sent = False  # temp_files delete sent and its outcome unknown
        self.contacted = False  # serv1 answered at least one command in this run
        self.verified_mac_paths: list[Path] = []

    # ---- paths -------------------------------------------------------------------------
    def rpath(self, name: str) -> str:
        return f"{self.cfg.remote_dir.rstrip('/')}/{name}"

    def ppath(self) -> str:
        return f"{self.cfg.remote_parent.rstrip('/')}/{PLAYER_IDS_NAME}"

    # ---- step 1: candidates --------------------------------------------------------------
    def find_candidates(self, listing: dict) -> list[dict]:
        age_limit_ns = int(self.cfg.min_age_hours * 3600 * 1e9)
        now_ns = listing["now_ns"]
        out = []
        for name, st in sorted(listing["files"].items()):
            m = PART_RE.match(name)
            if not m:
                continue
            if now_ns - st["mtime_ns"] < age_limit_ns:
                continue
            out.append({"name": name, "patch": m["patch"], "num": int(m["num"]),
                        "size": st["size"], "mtime_ns": st["mtime_ns"]})
        return out

    # ---- step 2b: no local dict build reading mac_dir -------------------------------------
    def _guard_no_local_reader(self) -> None:
        out = subprocess.run(["ps", "-axo", "pid=,ppid=,command="], capture_output=True, text=True,
                             timeout=30, check=True).stdout
        procs: dict[int, tuple[int, str]] = {}
        for line in out.splitlines():
            fields = line.split(None, 2)
            if len(fields) >= 2 and fields[0].isdigit() and fields[1].isdigit():
                procs[int(fields[0])] = (int(fields[1]), fields[2] if len(fields) == 3 else "")
        # This process and its ancestors never count (an agent shell `zsh -c "... pub_parts_offload ..."`).
        mine, pid = set(), os.getpid()
        while pid > 0 and pid not in mine:
            mine.add(pid)
            pid = procs.get(pid, (0, ""))[0]
        for pid, (_ppid, cmd) in sorted(procs.items()):
            # A waiter shell `zsh -c "until ! pgrep -f explore_database.py; do sleep 300; done"` names the
            # marker only as a pgrep/pkill/grep pattern; it never reads the corpus (09.10.2026: such a waiter
            # matched itself for 7 h and would have blocked every hourly run). The real build it waits for
            # is a separate process with its own command line, so it is still detected.
            probe = _SEARCH_CMD_RE.sub(" ", cmd)
            hit = next((m for m in self.cfg.local_reader_markers if m in probe), None)
            if hit is not None and pid not in mine:
                self.log("INFO", f"local corpus reader pid {pid}: {cmd.strip()[:160]}")
                raise Skip(f"a local dict build reads the Mac pub corpus: pid {pid}, marker {hit!r}")

    # ---- step 2: no merge running ------------------------------------------------------
    def _guard_no_merge_process(self) -> None:
        hits = self.remote.merge_procs()
        if hits:
            raise Abort(f"a corpus merge process is running on serv1: {hits[0]}")

    def _guard_lock(self) -> bool:
        """Returns True if the sweep lock is free. Busy lock = skip unless allowed."""
        busy = self.remote.lock_busy(self.cfg.remote_lock)
        if busy and not self.cfg.allow_busy_sweep:
            raise Skip(f"serv1 {self.cfg.remote_lock} is held (a sweep, possibly its merge, is running)")
        return not busy

    def _guard_quiescent(self, listing: dict) -> None:
        files = listing["files"]
        limit = int(self.cfg.quiet_minutes * 60 * 1e9)
        for name in GUARD_FILES:
            st = files.get(name)
            if st is None:
                raise Abort(f"serv1 {name} is missing: refusing to delete anything")
            age = listing["now_ns"] - st["mtime_ns"]
            if age < limit:
                raise NotQuiet(f"serv1 {name} modified {age / 6e10:.1f} min ago (< {self.cfg.quiet_minutes:g} min): "
                            "a merge may be writing the corpus")
        for name, st in files.items():
            if TMP_RE.search(name) and listing["now_ns"] - st["mtime_ns"] < limit:
                raise NotQuiet(f"in-progress part file {name} (modified < {self.cfg.quiet_minutes:g} min ago)")

    # ---- step 3: copy + verify -----------------------------------------------------------
    def _guard_hash_equal(self, name: str, remote_sha: str, local_sha: str) -> None:
        if remote_sha != local_sha:
            raise Abort(f"sha256 mismatch for {name}: serv1 {remote_sha[:12]} != Mac {local_sha[:12]}")

    def _guard_collision(self, name: str, remote_sha: str, existing: Path) -> None:
        local_sha = sha256_file(existing)
        if local_sha != remote_sha:
            raise Abort(f"NAME COLLISION: {existing} exists on the Mac with a different sha256 "
                        f"({local_sha[:12]} vs serv1 {remote_sha[:12]})")

    # ---- step 4: ids ---------------------------------------------------------------------
    def _guard_ids_present(self, name: str, keys: Iterable[str], serv_ids: set[int]) -> int:
        n = 0
        missing = []
        for raw in keys:
            norm = normalize_id(raw)
            if norm is None:
                raise Abort(f"{name}: non-numeric map key {raw!r}: cannot verify it against processed_ids")
            n += 1
            if norm not in serv_ids:
                missing.append(norm)
        if n == 0:
            raise Abort(f"{name}: no map ids found (empty or not a JSON object): refusing to delete")
        if missing:
            raise Abort(f"{name}: {len(missing)} of {n} map ids are NOT in serv1 processed_ids.txt "
                        f"(e.g. {missing[:3]}): deleting would let the crawl re-write them")
        return n

    def _check_part_ids(self, name: str, source_path: Path, serv_ids: set[int]) -> int:
        try:
            return self._guard_ids_present(
                name, iter_top_level_keys(source_path, name.endswith(".gz")), serv_ids)
        except Abort:
            raise
        except Exception as exc:  # truncated / invalid JSON
            raise Abort(f"{name}: cannot parse part ({type(exc).__name__}: {exc})")

    # ---- step 5: control files ---------------------------------------------------------
    def _guard_mac_ids_subset(self, mac_ids: set[int], serv_ids: set[int]) -> None:
        extra = mac_ids - serv_ids
        if extra:
            raise Abort(f"Mac processed_ids.txt has {len(extra)} ids that serv1 lacks (e.g. {sorted(extra)[:3]}): "
                        "overwriting would lose them")

    def _guard_counters(self, serv: dict[str, int], mac: dict[str, int], candidates: list[dict]) -> None:
        for patch, mac_n in mac.items():
            if serv.get(patch, 0) < mac_n:
                raise Abort(f"serv1 part counter for {patch} ({serv.get(patch, 0)}) < Mac counter ({mac_n})")
        for c in candidates:
            if serv.get(c["patch"], 0) < c["num"]:
                raise Abort(f"{c['name']}: serv1 part counter for {c['patch']} is {serv.get(c['patch'], 0)} < {c['num']}: "
                            "deleting it would let a later merge reuse the part number")

    # ---- orchestration -----------------------------------------------------------------
    def run(self) -> int:
        cfg, log = self.cfg, self.log
        log("INFO", f"start dry_run={cfg.dry_run} host={self.remote.host} min_age_h={cfg.min_age_hours:g} "
                    f"quiet_min={cfg.quiet_minutes:g} allow_busy_sweep={cfg.allow_busy_sweep}")
        listing = self.remote.list_dir(cfg.remote_dir)
        self.contacted = True
        candidates = self.find_candidates(listing)
        total = sum(c["size"] for c in candidates)
        if not candidates:
            log("INFO", f"0 candidate part files older than {cfg.min_age_hours:g} h; nothing to do")
            if cfg.dry_run:
                self._dry_run_control_report(listing)
            self._temp_phase(listing)
            return EXIT_OK
        log("INFO", f"{len(candidates)} candidates, {total / 1e9:.2f} GB: {candidates[0]['name']} .. {candidates[-1]['name']}")

        try:
            self._guard_no_merge_process()
            self._guard_no_local_reader()
            lock_free = self._guard_lock()
        except Skip as exc:
            oldest_h = (listing["now_ns"] - min(c["mtime_ns"] for c in candidates)) / 3.6e12
            log("SKIP", f"{exc}; oldest candidate is {oldest_h:.1f} h old")
            if oldest_h > cfg.stale_notify_hours and not cfg.dry_run:
                raise Abort(f"candidates wait {oldest_h:.0f} h (> {cfg.stale_notify_hours:g} h): {exc}")
            return EXIT_SKIP
        self._guard_quiescent(listing)

        if cfg.dry_run:
            rc = self._dry_run(listing, candidates)
        else:
            rc = self._execute(listing, candidates, lock_free)
        self._temp_phase(None)  # phase 2 re-lists serv1: it must see that no part is left
        return rc

    # ---- dry run -----------------------------------------------------------------------
    def _dry_run_control_report(self, listing: dict) -> None:
        """Read-only informational pass when there is nothing to move."""
        try:
            self._guard_no_merge_process()
            self.log("INFO", f"dry-run: serv1 sweep lock busy={self.remote.lock_busy(self.cfg.remote_lock)}")
            self._guard_quiescent(listing)
            serv_ids = load_ids(self.remote.read_bytes(self.rpath("processed_ids.txt")), "serv1 processed_ids.txt")
            self._compare_with_mac(serv_ids, listing, candidates=[])
        except Abort as exc:
            self.log("WARN", f"dry-run informational check failed: {exc}")

    def _compare_with_mac(self, serv_ids: set[int], listing: dict, candidates: list[dict],
                          serv_counters_blob: Optional[bytes] = None) -> None:
        mac_pid = self.cfg.mac_dir / "processed_ids.txt"
        if mac_pid.exists():
            mac_ids = load_ids(mac_pid.read_bytes(), "Mac processed_ids.txt")
        else:
            mac_ids = set()
        self._guard_mac_ids_subset(mac_ids, serv_ids)
        self.log("INFO", f"processed_ids: serv1 {len(serv_ids)} ⊇ Mac {len(mac_ids)} ok")
        blob = serv_counters_blob if serv_counters_blob is not None else self.remote.read_bytes(self.rpath("part_counters.json"))
        serv_c = load_counters(blob, "serv1 part_counters.json")
        mac_cp = self.cfg.mac_dir / "part_counters.json"
        mac_c = load_counters(mac_cp.read_bytes(), "Mac part_counters.json") if mac_cp.exists() else {}
        self._guard_counters(serv_c, mac_c, candidates)
        self.log("INFO", f"part_counters: serv1 {serv_c} >= Mac {mac_c} ok")

    def _dry_run(self, listing: dict, candidates: list[dict]) -> int:
        cfg, log = self.cfg, self.log
        serv_ids = load_ids(self.remote.read_bytes(self.rpath("processed_ids.txt")), "serv1 processed_ids.txt")
        self._compare_with_mac(serv_ids, listing, candidates)
        for c in candidates[:cfg.dry_run_max_hashes]:
            sha = self.remote.sha256(self.rpath(c["name"]))
            self.hashes_done += 1
            log("INFO", f"dry-run: serv1 sha256 {c['name']} {sha}")
        for c in candidates[:cfg.dry_run_max_id_checks]:
            proc = self.remote.open_stream(self.rpath(c["name"]))
            try:
                n = self._guard_ids_present(c["name"], iter_top_level_keys(proc.stdout, c["name"].endswith(".gz")), serv_ids)
            except Abort:
                raise
            except Exception as exc:
                raise Abort(f"{c['name']}: cannot parse streamed part ({type(exc).__name__}: {exc})")
            finally:
                proc.stdout.close()
                proc.kill()
                proc.wait()
            self.id_checks_done += 1
            log("INFO", f"dry-run: {c['name']}: {n} map ids all present in serv1 processed_ids.txt")
        log("INFO", f"dry-run: WOULD copy+verify+move {len(candidates)} parts "
                    f"({sum(c['size'] for c in candidates) / 1e9:.2f} GB) to {cfg.mac_dir}, refresh "
                    f"processed_ids/part_counters/{PLAYER_IDS_NAME} on the Mac (with .bak_before_offload_*), "
                    f"then delete exactly those {len(candidates)} files on serv1; nothing was changed")
        return EXIT_OK

    # ---- real run ----------------------------------------------------------------------
    def _execute(self, listing: dict, candidates: list[dict], lock_free: bool) -> int:
        cfg, log, remote = self.cfg, self.log, self.remote
        stage = cfg.staging_dir()
        stage_parts, stage_ctl = stage / "parts", stage / "control"
        for d in (stage_parts, stage_ctl):
            d.mkdir(parents=True, exist_ok=True)
        lock_fh = open(stage / ".offload.lock", "a+")
        try:
            fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise Abort("another pub_parts_offload run holds the Mac staging lock")
        cfg.mac_dir.mkdir(parents=True, exist_ok=True)

        need = sum(c["size"] for c in candidates if not (cfg.mac_dir / c["name"]).exists())
        free = shutil.disk_usage(stage).free
        if free < need + cfg.min_free_margin_bytes:
            raise Abort(f"Mac free space {free / 1e9:.1f} GB < needed {need / 1e9:.1f} GB + margin")

        guards = [{"path": self.rpath(n), **{k: listing["files"][n][k] for k in ("size", "mtime_ns")}}
                  for n in GUARD_FILES]

        # control files: hash on serv1, copy, hash on the Mac, must match
        control = {}
        plan = [("processed_ids.txt", self.rpath("processed_ids.txt"), cfg.mac_dir / "processed_ids.txt"),
                ("part_counters.json", self.rpath("part_counters.json"), cfg.mac_dir / "part_counters.json")]
        player_stat = remote.stat_paths([self.ppath()])["files"][self.ppath()]
        if player_stat is not None:
            plan.append((PLAYER_IDS_NAME, self.ppath(), cfg.mac_parent / PLAYER_IDS_NAME))
        else:
            log("WARN", f"serv1 {self.ppath()} missing: not refreshing it on the Mac")
        for name, rp, target in plan:
            rsha = remote.sha256(rp)
            remote.fetch(rp, stage_ctl)
            staged = stage_ctl / name
            self._guard_hash_equal(name, rsha, sha256_file(staged))
            control[name] = (staged, target, rsha)
            log("INFO", f"control file {name} fetched and sha256-verified ({staged.stat().st_size} B)")

        serv_ids = load_ids(control["processed_ids.txt"][0].read_bytes(), "serv1 processed_ids.txt")
        self._compare_with_mac(serv_ids, listing, candidates, control["part_counters.json"][0].read_bytes())

        # parts: hash on serv1, reuse identical Mac copy or rsync, hash on the Mac
        verified: list[dict] = []
        already = 0
        for c in candidates:
            name = c["name"]
            rsha = remote.sha256(self.rpath(name))
            dest = cfg.mac_dir / name
            if dest.exists():
                self._guard_collision(name, rsha, dest)
                src, already_there = dest, True
                already += 1
                log("INFO", f"{name}: already on the Mac with identical sha256 (treated as transferred)")
            else:
                remote.fetch(self.rpath(name), stage_parts)
                src, already_there = stage_parts / name, False
                self._guard_hash_equal(name, rsha, sha256_file(src))
                log("INFO", f"{name}: copied, sha256 equal {rsha[:12]}")
            if src.stat().st_size != c["size"]:
                raise Abort(f"{name}: Mac size {src.stat().st_size} != serv1 size {c['size']}")
            n = self._check_part_ids(name, src, serv_ids)
            log("INFO", f"{name}: {n} map ids, all in serv1 processed_ids.txt")
            verified.append({**c, "src": src, "already": already_there})

        # ---- commit on the Mac: nothing has been deleted on serv1 so far ----
        try:  # a dict build may have started while the parts were copied and hashed
            self._guard_no_local_reader()
        except Skip as exc:
            raise Abort(f"{exc} (started during the run; nothing committed on the Mac)")
        for v in verified:
            if v["already"]:
                continue
            dest = cfg.mac_dir / v["name"]
            try:
                os.link(v["src"], dest)  # same filesystem, atomic, refuses to clobber
            except FileExistsError:
                raise Abort(f"{dest} appeared during the run: refusing to overwrite")
            os.unlink(v["src"])
        stamp = time.strftime("%Y%m%d_%H%M", time.localtime(self.now()))
        for name, (staged, target, rsha) in control.items():
            if target.exists() and sha256_file(target) == rsha:
                staged.unlink()
                continue
            if target.exists():
                bak = target.with_name(f"{target.name}.bak_before_offload_{stamp}")
                if bak.exists():
                    bak = target.with_name(f"{bak.name}_{time.strftime('%S')}")
                if bak.exists():
                    raise Abort(f"backup name {bak} already exists")
                shutil.copy2(target, bak)
                durable_sync(bak)
                log("INFO", f"backed up Mac {name} -> {bak.name}")
                if name == PLAYER_IDS_NAME:
                    self._warn_player_ids(target, staged)
            os.replace(staged, target)  # same filesystem: atomic
            log("INFO", f"Mac {name} refreshed from serv1")
        for v in verified:
            dest = cfg.mac_dir / v["name"]
            if not dest.is_file() or dest.stat().st_size != v["size"]:
                raise Abort(f"{dest} missing or wrong size after the move: not deleting on serv1")
            # The final hard link is the same inode as the rsync'd staging file.
            # Sync reused copies as well: a previous interrupted run may not have done so.
            durable_sync(dest)
            self.verified_mac_paths.append(dest)
        # Refreshed control files too; the player-ids target lives one directory up.
        dirs = [cfg.mac_dir, stage_parts, stage_ctl, stage]
        for _staged, target, _rsha in control.values():
            if target.exists():
                durable_sync(target)
            if target.parent not in dirs:
                dirs.append(target.parent)
        for directory in dirs:
            durable_sync(directory)

        # ---- delete on serv1: the exact verified list, under the sweep lock ----
        files = [{"name": v["name"], "size": v["size"], "mtime_ns": v["mtime_ns"]} for v in verified]
        self.delete_sent = True  # a lost reply cannot prove whether the remote rm ran
        try:
            res = remote.delete_verified(cfg.remote_dir, cfg.remote_lock, lock_free, files, guards)
        except RemoteError as exc:
            partial = exc.code == 13
            raise Abort(f"{'PARTIAL DELETE: ' if partial else ''}serv1 delete failed: {exc.detail}",
                        partial_delete=partial)
        self.delete_sent, self.parts_deleted_ok = False, True  # reply seen: the outcome is known
        freed = sum(v["size"] for v in verified)
        log("INFO", f"deleted {len(res['deleted'])} files on serv1, {freed / 1e9:.2f} GB; serv1 avail "
                    f"{res['avail_before'] / 1e9:.1f} -> {res['avail_after'] / 1e9:.1f} GB; "
                    f"already_on_mac={already} copied={len(verified) - already}")
        for d in (stage_parts, stage_ctl):
            try:
                d.rmdir()
            except OSError:
                pass
        return EXIT_OK

    # ---- phase 2: temp_files -------------------------------------------------------------
    def _manifest(self, record: dict) -> None:
        path = self.cfg.temp_manifest_path
        if path is None:
            return
        rec = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(self.now())), **record}
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, sort_keys=True) + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    @staticmethod
    def _temp_key(completed_at: int, listing_files: dict, temp_files: dict) -> str:
        """What the verdict depends on: the sweep, the dedupe state's stamps and the temp files' stamps."""
        ctl = [[n, (listing_files.get(n) or {}).get("size"), (listing_files.get(n) or {}).get("mtime_ns")]
               for n in GUARD_FILES]
        temps = [[n, st["size"], st["mtime_ns"]] for n, st in sorted(temp_files.items())]
        return hashlib.sha256(json.dumps([completed_at, ctl, temps]).encode()).hexdigest()[:32]

    def _log_kept(self, kept: dict[str, list[str]]) -> None:
        for reason, names in sorted(kept.items()):
            shown = ", ".join(names[:self.cfg.temp_log_names])
            more = f" (+{len(names) - self.cfg.temp_log_names} more, full list in the manifest)" \
                if len(names) > self.cfg.temp_log_names else ""
            self.log("INFO" if reason == "newer_than_sweep" else "WARN",
                     f"temp_files KEPT {len(names)} x {reason}: {shown}{more}")

    def _temp_phase(self, listing: Optional[dict]) -> None:
        """Delete temp_files/*.txt whose data is provably merged, only once the sweep is complete and every
        part is on the Mac. Preconditions that are merely 'not yet' skip quietly; real inconsistencies abort."""
        cfg, log, remote = self.cfg, self.log, self.remote
        if not cfg.temp_phase:
            return

        def skip(reason: str) -> None:
            log("INFO", f"temp phase skipped: {reason}")

        res = remote.read_state(cfg.remote_state)
        info = res.get("state") or {}
        if info.get("status") != "complete" or not _is_epoch(info.get("completed_at")):
            if not res.get("exists"):
                return skip(f"{cfg.remote_state} does not exist")
            return skip(f"serv1 sweep state is {info.get('status', res.get('error'))!r}, not 'complete' "
                        "(temp files are the dedupe set of a sweep that has not finished)")
        completed_at = info["completed_at"]
        fresh = listing if listing is not None else remote.list_dir(cfg.remote_dir)
        left = sorted(n for n in fresh["files"] if PART_ISH_RE.search(n))
        if left and not cfg.dry_run:
            return skip(f"{len(left)} part file(s) still on serv1 (first: {left[0]}); temp files are removed only "
                        "after every part is verified on the Mac")
        try:
            self._guard_quiescent(fresh)
        except NotQuiet as exc:
            return skip(str(exc))
        if remote.merge_procs():
            return skip("a corpus merge process is running on serv1")
        if remote.lock_busy(cfg.remote_lock):
            return skip("serv1 pub_recrawl.lock is held (a sweep is running)")
        tl = remote.tlist(cfg.remote_temp_dir)
        if not tl["files"]:
            return skip("no temp_files/*.txt on serv1")
        key = self._temp_key(completed_at, fresh["files"], tl["files"])
        if self.state is not None and not cfg.dry_run and self.state.data.get("temp_key") == key:
            return skip("verdict unchanged since the last pass (same sweep, same dedupe-state stamps, "
                        f"same {len(tl['files'])} leftover files)")

        lock_fh = None
        if not cfg.dry_run:
            stage = cfg.staging_dir()
            stage.mkdir(parents=True, exist_ok=True)
            lock_fh = open(stage / ".offload.lock", "a+")
            try:
                fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                lock_fh.close()
                return skip("another pub_parts_offload run holds the Mac staging lock")
        try:
            self._temp_verify_and_delete(completed_at, fresh, tl, key, left)
        finally:
            if lock_fh is not None:
                lock_fh.close()

    def _temp_verify_and_delete(self, completed_at: int, fresh: dict, tl: dict, key: str, parts_left: list) -> None:
        cfg, log, remote = self.cfg, self.log, self.remote
        res = remote.temp_check(cfg.remote_temp_dir, self.rpath("processed_ids.txt"), completed_at,
                                cfg.temp_max_file_bytes)
        files = res["files"]
        deletable = [n for n, v in sorted(files.items()) if v.get("ok") is True and TEMP_NAME_RE.match(n)]
        kept: dict[str, list[str]] = {}
        delete_set = set(deletable)
        for n, v in sorted(files.items()):
            if n not in delete_set:
                kept.setdefault(str(v.get("reason") or "unknown"), []).append(n)
        freed = sum(files[n]["size"] for n in deletable)
        n_ids = sum(files[n].get("n_ids", 0) for n in deletable)
        log("INFO", f"temp_files: {len(files)} .txt files on serv1 (sweep completed_at={completed_at}, "
                    f"{(res['now_ns'] / 1e9 - completed_at) / 3600:.1f} h ago, processed_ids has {res['n_known']} ids): "
                    f"{len(deletable)} verified merged ({freed / 1e9:.2f} GB, {n_ids} map ids), "
                    f"{len(files) - len(deletable)} kept")
        self._log_kept(kept)
        entries = [{"name": n, "size": files[n]["size"], "mtime_ns": files[n]["mtime_ns"]} for n in deletable]
        if cfg.dry_run:
            note = f" (a real run first moves {len(parts_left)} part file(s) still on serv1)" if parts_left else ""
            log("INFO", f"dry-run: WOULD delete {len(deletable)} temp files ({freed / 1e9:.2f} GB){note}; "
                        "nothing was changed")
            return
        if not deletable:
            if self.state is not None:
                self.state.data["temp_key"] = key
            return

        guards = [{"path": self.rpath(n), **{k: fresh["files"][n][k] for k in ("size", "mtime_ns")}}
                  for n in GUARD_FILES]
        self._manifest({"event": "temp_delete_intent", "completed_at": completed_at, "kept": kept,
                        "files": [{**e, "n_ids": files[e["name"]]["n_ids"]} for e in entries]})
        self.temp_delete_sent = True  # a lost reply cannot prove whether the remote unlink ran
        try:
            out = remote.delete_temp(cfg.remote_temp_dir, cfg.remote_lock, cfg.remote_state, cfg.remote_dir,
                                     completed_at, entries, guards)
        except RemoteError as exc:
            if exc.code in (11, 12, 14, 15):  # refused by the remote's own checks, before the first unlink
                self.temp_delete_sent = False
            raise Abort(f"{'PARTIAL TEMP DELETE: ' if exc.code == 13 else ''}serv1 temp_files delete failed: "
                        f"{exc.detail}", partial_delete=exc.code == 13)
        self.temp_delete_sent = False
        log("INFO", f"deleted {len(out['deleted'])} temp files on serv1, {freed / 1e9:.2f} GB, {n_ids} map ids; "
                    f"serv1 avail {out['avail_before'] / 1e9:.1f} -> {out['avail_after'] / 1e9:.1f} GB; "
                    f"kept {len(files) - len(deletable)}; manifest {cfg.temp_manifest_path}")
        try:
            self._manifest({"event": "temp_deleted", "completed_at": completed_at, "count": len(out["deleted"]),
                            "freed_bytes": freed, "avail_before": out["avail_before"],
                            "avail_after": out["avail_after"]})
        except OSError as exc:
            log("WARN", f"manifest not updated after the delete: {exc}")
        if self.state is not None:  # key of what is left, so the next hour does not re-parse the leftover
            gone = set(out["deleted"])
            self.state.data["temp_key"] = self._temp_key(
                completed_at, fresh["files"], {n: st for n, st in tl["files"].items() if n not in gone})

    def _warn_player_ids(self, old: Path, new: Path) -> None:
        try:
            import orjson

            a = set(orjson.loads(old.read_bytes()).get("ids") or [])
            b = set(orjson.loads(new.read_bytes()).get("ids") or [])
            if a - b:
                self.log("WARN", f"Mac {PLAYER_IDS_NAME} had {len(a - b)} player ids absent from serv1's; "
                                 "kept in the .bak_before_offload_* copy")
        except Exception as exc:
            self.log("WARN", f"player-ids comparison skipped: {exc}")


# --------------------------------------------------------------------------------------
# Notification + CLI
# --------------------------------------------------------------------------------------
def notify_admin(text: str) -> None:
    """scripts/ops/notify_admin.py "text": never raises, exit code always 0, no secrets printed."""
    script = REPO / "scripts/ops/notify_admin.py"
    try:
        subprocess.run([sys.executable, str(script), text[:3000]], timeout=60,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass


def execute(cfg: Config, remote: Remote, log: Log, notify: Callable[[str], None] = notify_admin,
            now: Callable[[], float] = time.time) -> int:
    offload = None
    # Dry runs are read-only: no state file, and every failure is reported as before.
    state = RunState(cfg.state_path, now) if cfg.state_path is not None and not cfg.dry_run else None

    def finish(rc: int) -> int:
        if state is not None:
            if offload is not None and offload.contacted:
                state.contact()
            state.save(log)
        return rc

    def report_failure(message: str, *, unexpected: bool = False, transport: bool = False) -> int:
        contacted = offload is not None and offload.contacted
        if offload is not None and offload.delete_sent:
            paths = ", ".join(str(p) for p in offload.verified_mac_paths)
            outcome = ("delete outcome UNKNOWN - parts may have been deleted on serv1; "
                       f"Mac copies are verified at {paths}; next run reconciles the serv1 listing")
        elif offload is not None and offload.temp_delete_sent:
            outcome = ("temp_files delete outcome UNKNOWN - some temp files may have been deleted on serv1 "
                       f"(intent and verdicts: {cfg.temp_manifest_path}); the parts were moved and verified earlier; "
                       "next run reconciles the serv1 listing")
        elif offload is not None and offload.parts_deleted_ok:
            outcome = "parts of this run were already moved and verified on the Mac; no temp file was deleted."
        else:
            outcome = "nothing deleted on serv1."
        log("ABORT", f"{message} -- {outcome}")
        if not cfg.dry_run or unexpected:
            text = f"pub_parts_offload ABORT: {message} -- {outcome}"
            send = True
            critical = offload is not None and (offload.delete_sent or offload.temp_delete_sent)
            if state is not None and not critical:
                if transport and not contacted:  # ssh could not even connect: hourly noise unless it persists
                    send, down_h = state.transient_should_notify(cfg.transient_notify_hours, cfg.abort_renotify_hours)
                    text = (f"pub_parts_offload ABORT: serv1 unreachable for {down_h:.1f} h: {message} -- {outcome}")
                else:
                    send = state.abort_should_notify(abort_signature(message), cfg.abort_renotify_hours)
            if send:
                notify(text)
        return finish(EXIT_ABORT)

    try:
        offload = Offload(cfg, remote, log, now, state)
        return finish(offload.run())
    except Abort as exc:
        return report_failure(str(exc))
    except RemoteError as exc:
        return report_failure(str(exc), transport=exc.code == 255)
    except subprocess.TimeoutExpired as exc:
        return report_failure(f"timeout {exc.timeout}s: {exc}", transport=True)
    except Exception as exc:
        log("ERROR", traceback.format_exc())
        return report_failure(f"{type(exc).__name__}: {exc}", unexpected=True)
    except (KeyboardInterrupt, Terminated) as exc:
        log("ERROR", traceback.format_exc())
        report_failure(f"interrupted ({type(exc).__name__} {exc})", unexpected=True)
        raise


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", help="read-only: list candidates, hash <=2, id-check <=1 part, "
                   "and report which temp files would be deleted")
    p.add_argument("--host", default="serv1")
    p.add_argument("--min-age-hours", type=float, default=1.0)
    p.add_argument("--quiet-minutes", type=float, default=30.0)
    p.add_argument("--allow-busy-sweep", action="store_true",
                   help="proceed although a sweep holds pub_recrawl.lock (quiescence + id checks still apply)")
    p.add_argument("--no-temp-phase", action="store_true",
                   help="move parts only; leave serv1 temp_files/*.txt alone")
    p.add_argument("--log", type=Path, default=None)
    args = p.parse_args(argv)
    cfg = Config(min_age_hours=args.min_age_hours, quiet_minutes=args.quiet_minutes,
                 allow_busy_sweep=args.allow_busy_sweep, dry_run=args.dry_run,
                 temp_phase=not args.no_temp_phase)
    if args.log:
        cfg.log_path = args.log
    for sig in (signal.SIGTERM, signal.SIGHUP):  # launchd unload / logout send SIGTERM
        signal.signal(sig, _raise_terminated)
    return execute(cfg, Remote(args.host), Log(cfg.log_path))


if __name__ == "__main__":
    sys.exit(main())
