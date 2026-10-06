#!/usr/bin/env python3
"""Move finished pub-corpus part files from serv1 to the Mac, then free them on serv1.

Documented design: docs/SERVER_LAYOUT.md (pub-corpus row). serv1 keeps the dedup state
(`processed_ids.txt`, `part_counters.json`, `pub_player_steam_ids.json`); the part files
`<patch>_partNNN.json[.gz]` live on the Mac. The crawler on serv1 only ever CREATES new
part numbers (base/maps_research.py:3027 `_open_part`, number from `_next_part_numbers`
:2797 = max(disk, part_counters)+1, written via `<name>.tmp` + os.replace :3030-3033), it
never reopens an existing part, so no part needs to be excluded as "still appendable".

Everything is FAIL CLOSED: any failed check aborts the whole run (exit 1, admin notified).
Before deletion is sent, nothing is deleted on serv1; failures after sending it report an
UNKNOWN outcome and the verified Mac paths. Deletion only touches the exact
files that were copied, sha256-verified, id-verified and are byte-for-byte unchanged
(size + mtime_ns) since they were listed.

Exit codes: 0 ok (also "0 candidates"), 1 ABORT (nothing deleted unless the message says
UNKNOWN/PARTIAL), 2 usage, 3 SKIP (a sweep holds serv1's pub_recrawl.lock, or a local
dict build (explore_database.py / rebuild_dicts.sh / build_driver.py) reads the Mac corpus;
nothing done; notified after 72 h; a build that starts mid-run = ABORT before the Mac commit).

Usage: pub_parts_offload.py [--dry-run] [--min-age-hours 6] [--quiet-minutes 30]
                            [--allow-busy-sweep] [--host serv1]
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

EXIT_OK, EXIT_ABORT, EXIT_USAGE, EXIT_SKIP = 0, 1, 2, 3


class Abort(Exception):
    """A fail-closed check failed; the run stops and nothing more is deleted."""

    def __init__(self, message: str, partial_delete: bool = False):
        super().__init__(message)
        self.partial_delete = partial_delete


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
    ):
        self.host = host
        self.use_ssh = use_ssh
        self.python = python
        self.io_prefix = list(io_prefix)
        self.sha_argv = list(sha_argv)
        self.runner = runner or subprocess.run

    # -- command construction ------------------------------------------------------------
    def wrap(self, argv: list[str]) -> list[str]:
        if not self.use_ssh:
            return list(argv)
        return ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20",
                "-o", "ServerAliveInterval=30", self.host, shlex.join(argv)]

    def run(self, argv: list[str], *, input: Optional[bytes] = None, timeout: Optional[int] = 600):
        return self.runner(self.wrap(argv), input=input, capture_output=True, timeout=timeout)

    def helper(self, op: str, *args: str, stdin: Optional[dict] = None, timeout: int = 300) -> dict:
        """Run one op of REMOTE_HELPER (program via -c, JSON request via stdin)."""
        proc = self.run([self.python, "-c", REMOTE_HELPER, op, *args],
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
    min_age_hours: float = 6.0
    quiet_minutes: float = 30.0
    allow_busy_sweep: bool = False
    dry_run: bool = False
    min_free_margin_bytes: int = 2 * 1024 ** 3
    stale_notify_hours: float = 72.0
    dry_run_max_hashes: int = 2
    dry_run_max_id_checks: int = 1
    # Local readers of mac_dir: base/explore_database.py globs the parts once per process, so a
    # multi-group dict build would see different corpora if parts arrived between its groups.
    # A multi-group driver (runtime/artifacts/pubs-rebuild/*/build_driver.py, scripts/run/rebuild_dicts.sh)
    # sleeps between groups with no explore_database.py alive, so the drivers are markers too. Not the
    # directory "pubs-rebuild/": the offload's own log lives there and a `tail -F` of it would block runs.
    local_reader_markers: tuple = ("explore_database.py", "rebuild_dicts.sh", "build_driver.py")

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
    def __init__(self, cfg: Config, remote: Remote, log: Log, now: Callable[[], float] = time.time):
        self.cfg, self.remote, self.log, self.now = cfg, remote, log, now
        self.hashes_done = 0
        self.id_checks_done = 0
        self.delete_sent = False
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
            hit = next((m for m in self.cfg.local_reader_markers if m in cmd), None)
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
                raise Abort(f"serv1 {name} modified {age / 6e10:.1f} min ago (< {self.cfg.quiet_minutes:g} min): "
                            "a merge may be writing the corpus")
        for name, st in files.items():
            if TMP_RE.search(name) and listing["now_ns"] - st["mtime_ns"] < limit:
                raise Abort(f"in-progress part file {name} (modified < {self.cfg.quiet_minutes:g} min ago)")

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
        candidates = self.find_candidates(listing)
        total = sum(c["size"] for c in candidates)
        if not candidates:
            log("INFO", f"0 candidate part files older than {cfg.min_age_hours:g} h; nothing to do")
            if cfg.dry_run:
                self._dry_run_control_report(listing)
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
            return self._dry_run(listing, candidates)
        return self._execute(listing, candidates, lock_free)

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


def execute(cfg: Config, remote: Remote, log: Log, notify: Callable[[str], None] = notify_admin) -> int:
    offload = None

    def report_failure(message: str, *, unexpected: bool = False) -> int:
        if offload is not None and offload.delete_sent:
            paths = ", ".join(str(p) for p in offload.verified_mac_paths)
            outcome = ("delete outcome UNKNOWN - parts may have been deleted on serv1; "
                       f"Mac copies are verified at {paths}; next run reconciles the serv1 listing")
        else:
            outcome = "nothing deleted on serv1."
        log("ABORT", f"{message} -- {outcome}")
        if not cfg.dry_run or unexpected:
            notify(f"pub_parts_offload ABORT: {message} -- {outcome}")
        return EXIT_ABORT

    try:
        offload = Offload(cfg, remote, log)
        return offload.run()
    except Abort as exc:
        return report_failure(str(exc))
    except RemoteError as exc:
        return report_failure(str(exc))
    except subprocess.TimeoutExpired as exc:
        return report_failure(f"timeout {exc.timeout}s: {exc}")
    except Exception as exc:
        log("ERROR", traceback.format_exc())
        return report_failure(f"{type(exc).__name__}: {exc}", unexpected=True)
    except (KeyboardInterrupt, Terminated) as exc:
        log("ERROR", traceback.format_exc())
        report_failure(f"interrupted ({type(exc).__name__} {exc})", unexpected=True)
        raise


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true", help="read-only: list candidates, hash <=2, id-check <=1 part")
    p.add_argument("--host", default="serv1")
    p.add_argument("--min-age-hours", type=float, default=6.0)
    p.add_argument("--quiet-minutes", type=float, default=30.0)
    p.add_argument("--allow-busy-sweep", action="store_true",
                   help="proceed although a sweep holds pub_recrawl.lock (quiescence + id checks still apply)")
    p.add_argument("--log", type=Path, default=None)
    args = p.parse_args(argv)
    cfg = Config(min_age_hours=args.min_age_hours, quiet_minutes=args.quiet_minutes,
                 allow_busy_sweep=args.allow_busy_sweep, dry_run=args.dry_run)
    if args.log:
        cfg.log_path = args.log
    for sig in (signal.SIGTERM, signal.SIGHUP):  # launchd unload / logout send SIGTERM
        signal.signal(sig, _raise_terminated)
    return execute(cfg, Remote(args.host), Log(cfg.log_path))


if __name__ == "__main__":
    sys.exit(main())
