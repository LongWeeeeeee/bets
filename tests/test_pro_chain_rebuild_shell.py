"""Shell harness for the nightly prematch-snapshot rebuild chain, three modes.

Question answered: does `scripts/run/rebuild_prematch_snapshot.sh` (+ the kv3 state
script it calls) behave (a) in remote mode exactly like the pre-library original,
(b) in local mode with zero ssh/scp, delivering by cp/bash into a separate prod
tree, (c) in local+shadow mode with a byte-identical prod tree and a summary of
would-be deliveries, (d) refusing to run when the build tree IS the prod tree.

Nothing real runs: python, ssh, scp, systemctl, sleep, sha1sum and `bash -s` are
stubs first on PATH; the "prod" is a temp tree; the stubs log every call to one
events file. The ORIGINAL scripts (commit ORIGINAL_REV) are rewritten in the test
(drop the hardcoded `cd`, point PY at the stub, runtime/experiments/misc ->
scripts/pro_chain) and run under the same stubs. They are never run untransformed.

Red run (original scripts as the target of the b/c/d tests):
    PRO_CHAIN_TEST_TARGET=original pytest base/tests/test_pro_chain_rebuild_shell.py
"""
from __future__ import annotations

import hashlib
import os
import re
import signal
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
ORIGINAL_REV = "f5353230"
TARGET = os.environ.get("PRO_CHAIN_TEST_TARGET", "new")
# Red runs of tests added after a commit: PRO_CHAIN_TEST_SCRIPTS_REV=<rev> feeds the
# "new" target the scripts of that revision (git show) instead of the working tree.
SCRIPTS_REV = os.environ.get("PRO_CHAIN_TEST_SCRIPTS_REV")
BASH = "/bin/bash"

REBUILD_REL = "scripts/run/rebuild_prematch_snapshot.sh"
KV3_REL = "scripts/ops/build_kv3_state.sh"
LIB_REL = "scripts/run/lib_pro_chain.sh"

OLD_ARTIFACT = b"old-artifact\n"
# The chain starts the CLI as `timeout -k 60 N bash -c WRAPPER _ CHILD_PID_FILE venv/bin/python3 ...`:
# the wrapper records its own pid, then exec()s the CLI (same pid), so the chain knows the
# child's pid before python starts (a pre-flock orphan holds no lock yet).
CHILD_PID_REL = "runtime/live_elo_rebase.child.pid"
PID_WRAPPER = 'printf "%010d\\n" "$$" 1<> "$1" || exit 125; shift; exec "$@"'
CHILD_PID_PLACEHOLDER = b"0000000000\n"   # written before `systemctl stop`; the wrapper overwrites it in place
REBASE_TIMEOUT_DEFAULT = 1800   # seconds; 4.2 x the whole 06.10.2026 outage window (430 s)
BUILT = {
    "artifact": b"artifact-v3-hybrid\n",
    "elo": b'{"elo": 1}\n',
    "aliases": b'{"aliases": 1}\n',
    "prior": b"prior\n",
    "rating": b"rating\n",
    "pair": b"pair\n",
    "kv3": b"kv3-state-bytes\n",
}
# prod-relative delivery target -> BUILT key
DELIVERIES = {
    "data/prematch_model_artifact_v3.npz": "artifact",
    "ELO/output/live_team_elo_snapshot.json": "elo",
    "data/team_org_aliases.json": "aliases",
    "data/prior_snapshot.npz": "prior",
    "data/rating_snapshot.npz": "rating",
    "data/pair_prior_snapshot.npz": "pair",
    "data/kills_v3_state/state.npz": "kv3",
}

# --------------------------------------------------------------------------- stubs

PY_STUB = r'''#!/bin/bash
# Stub python for the build tree: logs argv, writes the output each step owes.
EV="$STUB_EVENTS"
if [ "${1:-}" = "-c" ]; then exec "$STUB_REAL_PY" "$@"; fi
if [ "${1:-}" = "-" ]; then
  cat >/dev/null
  case "${2:-}" in *manifest.json) echo "1 2 3 4 5 6";; esac
  echo "PY - ${*:2}" >> "$EV"
  exit 0
fi
note=""
[ -n "${PREMATCH_SNAPSHOT_ONLY:-}" ] && note="$note PREMATCH_SNAPSHOT_ONLY=$PREMATCH_SNAPSHOT_ONLY"
[ -n "${PREMATCH_SRC:-}" ] && note="$note PREMATCH_SRC=$PREMATCH_SRC"
[ -n "${PREMATCH_OUT:-}" ] && note="$note PREMATCH_OUT=$PREMATCH_OUT"
if [ -n "${TEAM_NAMES_DIR:-}" ]; then
  note="$note NAMES=$(cd "$TEAM_NAMES_DIR" && ls | tr '\n' ',')"
fi
here="$(pwd -P | sed "s#^$STUB_BUILD#.#")"
case "$1" in
  scripts/ops/notify_admin.py)
    first="$(head -1)"; cat >/dev/null
    echo "NOTIFY $first" >> "$EV"; exit 0;;
esac
echo "PY $* |$note |@$here" >> "$EV"
case "$1" in
  scripts/pro_chain/finalize_artifact.py) printf 'artifact-v3-hybrid\n' > "$PREMATCH_OUT";;
  base/tools/build_team_org_aliases.py) printf '{"aliases": 1}\n' > data/team_org_aliases.json;;
  ELO/live_team_strength.py) printf '{"elo": 1}\n' > "$3";;
  scripts/pro_chain/build_prior_snapshot.py) printf 'prior\n' > data/prior_snapshot.npz;;
  scripts/pro_chain/build_rating_snapshot.py) printf 'rating\n' > data/rating_snapshot.npz;;
  scripts/pro_chain/build_pair_snapshot.py) printf 'pair\n' > data/pair_prior_snapshot.npz;;
  -m)
    out=""; prev=""
    for a in "$@"; do [ "$prev" = "--output" ] && out="$a"; prev="$a"; done
    [ -n "$out" ] && printf 'kv3-state-bytes\n' > "$out";;
esac
exit 0
'''

PRODPY_STUB = r'''#!/bin/bash
if [ "${1:-}" = "-c" ]; then exec "$STUB_REAL_PY" "$@"; fi
echo "PRODPY $*" >> "$STUB_EVENTS"
exit 0
'''

SSH_STUB = r'''#!/bin/bash
args=("$@")
i=0
while [ "${args[$i]:-}" = "-o" ]; do i=$((i+2)); done
i=$((i+1))   # host
rest=("${args[@]:$i}")
norm="$(printf '%s' "${rest[*]}" | tr -s ' \t\n' ' ')"
echo "SSH $norm" >> "$STUB_EVENTS"
if [ "${rest[0]:-}" = bash ]; then
  exec bash "${rest[@]:1}"
fi
cmd="${rest[*]}"
cmd="$(printf '%s' "$cmd" | sed "s#/root/main#$FAKE_PROD#g")"
exec /bin/bash -c "$cmd"
'''

SCP_STUB = r'''#!/bin/bash
args=(); skip=0
for a in "$@"; do
  if [ $skip = 1 ]; then skip=0; continue; fi
  case "$a" in -o) skip=1;; -q) ;; *) args+=("$a");; esac
done
src="${args[0]}"; dst="${args[1]}"
case "$src" in
  serv1:*)
    p="${src#serv1:}"; echo "SCP-DOWN $p" >> "$STUB_EVENTS"
    cp "${p/\/root\/main/$FAKE_PROD}" "$dst"; exit $?;;
esac
case "$dst" in
  serv1:*)
    p="${dst#serv1:}"; echo "SCP-UP $p" >> "$STUB_EVENTS"
    cp "$src" "${p/\/root\/main/$FAKE_PROD}" || exit $?
    if [ -n "${STUB_CORRUPT_UP:-}" ]; then
      case "$p" in *"$STUB_CORRUPT_UP"*) printf 'X' >> "${p/\/root\/main/$FAKE_PROD}";; esac
    fi
    exit 0;;
esac
echo "scp stub: no serv1 side: $*" >&2; exit 1
'''

SYSTEMCTL_STUB = r'''#!/bin/bash
echo "SYSTEMCTL $*" >> "$STUB_EVENTS"
if [ "${1:-}" = start ] && [ -n "${STUB_ORPHAN_PIDFILE:-}" ] && [ -s "$STUB_ORPHAN_PIDFILE" ] \
   && kill -0 "$(cat "$STUB_ORPHAN_PIDFILE")" 2>/dev/null; then
  echo "START-WITH-WRITER-ALIVE" >> "$STUB_EVENTS"
fi
[ "${1:-}" = is-active ] && echo active
exit 0
'''

SLEEP_STUB = '#!/bin/bash\n[ -n "${STUB_REAL_SLEEP:-}" ] && exec /bin/sleep "$@"\nexit 0\n'
# mv: fails on demand (STUB_MV_FAIL=1) for the staged ELO snapshot, otherwise the real one.
MV_STUB = r'''#!/bin/bash
echo "MV $*" >> "$STUB_EVENTS"
if [ -n "${STUB_MV_FAIL:-}" ]; then
  case "$*" in *live_team_elo_snapshot.json.tmp*) echo "mv: stub failure" >&2; exit 1;; esac
fi
exec /bin/mv "$@"
'''
SHA1SUM_STUB = '#!/bin/bash\nexec shasum -a 1 "$@"\n'

# `bash -s` is how a rebase script reaches the prod host (ssh `bash -s` remote,
# lib prod_script local). The heredoc hardcodes /root/main, so the stub rewrites
# it to the fake prod and logs the sha1 of the ORIGINAL text.
BASH_STUB = r'''#!/bin/bash
if [ "${1:-}" = "-s" ]; then
  tmp="$(mktemp "${TMPDIR:-/tmp}/bashs.XXXXXX")"
  cat > "$tmp.orig"
  echo "BASH-S ${*:2} $(shasum -a 1 < "$tmp.orig" | cut -d' ' -f1)" >> "$STUB_EVENTS"
  echo "SCOPE-ENV ${STUB_IN_SCOPE:-0}" >> "$STUB_EVENTS"
  sed "s#/root/main#$FAKE_PROD#g; s#/root/.local/state#$FAKE_STATE#g" "$tmp.orig" > "$tmp"
  /bin/bash "$@" < "$tmp"; rc=$?
  rm -f "$tmp" "$tmp.orig"; exit $rc
fi
exec /bin/bash "$@"
'''

# systemd-run for `--scope`: logs argv, drops its own options and runs the rest in
# the current process, like the real one. STUB_SYSTEMD_RUN_FAIL=1 simulates a host
# where a transient scope cannot be created (no systemd, not root).
SYSTEMD_RUN_STUB = r'''#!/bin/bash
echo "SYSTEMD-RUN $*" >> "$STUB_EVENTS"
[ -n "${STUB_SYSTEMD_RUN_FAIL:-}" ] && exit 1
args=("$@"); i=0
while [ "$i" -lt "$#" ]; do
  case "${args[$i]}" in
    --scope|--quiet|--collect) i=$((i+1));;
    -p) i=$((i+2));;
    *) break;;
  esac
done
export STUB_IN_SCOPE=1
exec "${args[@]:$i}"
'''

# coreutils `timeout` does not exist on macOS: same contract (-k D N cmd...), rc 124
# when the limit fires, the child's own rc otherwise.
TIMEOUT_STUB = r'''#!/bin/bash
echo "TIMEOUT $*" >> "$STUB_EVENTS"
exec "$STUB_REAL_PY" -I "$(dirname "$0")/timeout_impl.py" "$@"
'''
# STUB_TIMEOUT_CAP="name=seconds,..." shortens the limit of the commands whose argv
# mentions `name` (the validator keeps real limits >= 60 s; tests cannot wait that long).
TIMEOUT_IMPL = '''import os, subprocess, sys
a = sys.argv[1:]
if a[0] == "-k":
    a = a[2:]
secs, cmd = float(a[0]), a[1:]
for pair in filter(None, os.environ.get("STUB_TIMEOUT_CAP", "").split(",")):
    name, _, cap = pair.partition("=")
    if name in " ".join(cmd):
        secs = min(secs, float(cap))
p = subprocess.Popen(cmd)
try:
    rc = p.wait(timeout=secs)
except subprocess.TimeoutExpired:
    p.terminate()
    try:
        p.wait(timeout=2)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()
    sys.exit(124)
sys.exit(rc if rc >= 0 else 128 - rc)
'''

# Prod-side python: the rebase outcome is chosen by STUB_REBASE; the other prod
# python steps (delta conversion, sidecar) just succeed.
PRODPY_REBASE_STUB = r'''#!/bin/bash
if [ "${1:-}" = "-c" ]; then exec "$STUB_REAL_PY" "$@"; fi
echo "PRODPY $*" >> "$STUB_EVENTS"
if [ "${1:-}" = ELO/rebase_runtime_model_state.py ]; then
  case "${STUB_REBASE:-ok}" in
    hang) exec /bin/sleep 600;;
    kill-clean) exit 137;;
    kill-state) printf 'half-rebased\n' > runtime/live_elo_model_state.json; exit 137;;
    kill-progress) printf 'half-rebased\n' > runtime/live_elo_progress.json; exit 137;;
    kill-delta) printf '{}' > runtime/live_elo_delta.json; exit 137;;
    kill-env-delta) printf '{}' > "$STUB_DELTA_FILE"; exit 137;;
    reject) exit 1;;
    reject-state) printf 'rebased-then-crashed\n' > runtime/live_elo_model_state.json; exit 1;;
    reject-env-delta) printf '{}' > "$STUB_DELTA_FILE"; exit 1;;
    rollback-clean) exit 2;;
    lock-refused)
      # the CLI lost the flock race to another writer: it exits 3 and the file
      # still names that writer
      printf '4242\n' > runtime/live_elo_rebase.lock; exit 3;;
    lock-refused-touched)
      # as lock-refused, but a state file was already rewritten before the exit
      printf '4242\n' > runtime/live_elo_rebase.lock
      printf 'rebased\n' > runtime/live_elo_model_state.json; exit 3;;
    pre-lock-orphan)
      # a real python (STUB_HOLDER_SCRIPT, argv names the script) that is still "starting":
      # it takes the flock and rewrites the state only STUB_ORPHAN_DELAY seconds from now
      exec "$STUB_REAL_PY" "$STUB_HOLDER_SCRIPT" runtime/live_elo_rebase.lock \
        "${STUB_ORPHAN_DELAY:-0}" runtime/live_elo_model_state.json;;
    lock-orphan)
      # a real python (STUB_REAL_PY) takes the flock on the lock file and outlives the
      # `timeout` wrapper: it ignores SIGTERM and rewrites the state STUB_ORPHAN_DELAY
      # seconds later. STUB_HOLDER_SCRIPT decides what its argv looks like. The foreground
      # part waits until the lock is really held, then hangs.
      ( trap '' TERM
        exec "$STUB_REAL_PY" "$STUB_HOLDER_SCRIPT" runtime/live_elo_rebase.lock \
          "${STUB_ORPHAN_DELAY:-0}" runtime/live_elo_model_state.json \
          ) </dev/null >"$STUB_ORPHAN_PIDFILE.ready" 2>/dev/null &
      echo $! > "$STUB_ORPHAN_PIDFILE"
      for _ in $(seq 1 200); do
        grep -q LOCKED "$STUB_ORPHAN_PIDFILE.ready" 2>/dev/null && break
        /bin/sleep 0.05
      done
      if [ -n "${STUB_PROC_LOCKS_FILE:-}" ]; then
        "$STUB_REAL_PY" -c "$STUB_PROC_LOCKS_WRITER" "$STUB_PROC_LOCKS_FILE" "$(cat "$STUB_ORPHAN_PIDFILE")" \
          "$("$STUB_REAL_PY" -c 'import os,sys;print(os.stat(sys.argv[1]).st_ino)' runtime/live_elo_rebase.lock)" \
          "${STUB_PROC_LOCKS_MODE:-match}" \
          "${STUB_PROC_LOCKS_DEV:-$("$STUB_REAL_PY" -c 'import os,sys;d=os.stat(sys.argv[1]).st_dev;print("%02x:%02x" % (os.major(d), os.minor(d)))' runtime/live_elo_rebase.lock)}"
      fi
      exec /bin/sleep 600;;
  esac
fi
# a stuck post-rebase step: finishes (and says so) only if nobody cuts it first
if { [ "${1:-}" = ELO/convert_state_to_delta.py ] && [ "${STUB_CONVERT:-}" = hang ]; } \
   || { [ "${1:-}" = ELO/build_state_arrays.py ] && [ "${STUB_BUILD:-}" = hang ]; }; then
  /bin/sleep "${STUB_HANG_SECONDS:-600}"
  echo "HANG-COMPLETED $1" >> "$STUB_EVENTS"
fi
exit 0
'''

# A rebase writer as the kernel sees it: opens the lock file WITHOUT O_TRUNC, takes
# flock(LOCK_EX|LOCK_NB), writes its pid, says LOCKED. With a delay it writes the state
# file when the delay ends (a late write the chain must prevent), else it sleeps.
HOLDER_PY = """import fcntl, os, sys, time
fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o644)
fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
os.ftruncate(fd, 0)
os.write(fd, (str(os.getpid()) + chr(10)).encode())
print("LOCKED", flush=True)
delay = float(sys.argv[2]) if len(sys.argv) > 2 else 0.0
if delay:
    time.sleep(delay)
    with open(sys.argv[3], "w") as fh:
        fh.write("late-write")
else:
    time.sleep(600)
"""
# /proc/locks as Linux writes it (fs/locks.c lock_get_status: `%d: ` id, type FLOCK|POSIX,
# ADVISORY, READ|WRITE, pid, `%02x:%02x:%ld` = hex major:minor + DECIMAL inode, start, end;
# a `->` after the id marks a BLOCKED waiter, which holds nothing). Real lines, captured
# read-only on serv1 by the lead on 08.10.2026 (/root/main/runtime is local ext4):
#   1: POSIX  ADVISORY  WRITE 298297 fd:02:398966 1073741826 1073742335
#   N: FLOCK  ADVISORY  WRITE <pid> fd:02:<inode> 0 EOF      (a python fcntl.flock holder)
# The stub writes such a file for its orphan; the mode picks what the file claims.
PROC_LOCKS_WRITER = """import sys
path, pid, ino, mode = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
dev = sys.argv[5]    # the lock file's own "maj:min" (hex), as the kernel prints it
lines = ["1: POSIX  ADVISORY  WRITE 298297 fd:02:398966 1073741826 1073742335"]
fl = "FLOCK  ADVISORY  WRITE"
if mode == "match":
    lines.append(f"2: {fl} {pid} {dev}:{ino} 0 EOF")
elif mode == "other-pid":      # the inode is flocked, but by somebody else
    lines.append(f"2: {fl} {pid + 7} {dev}:{ino} 0 EOF")
elif mode == "other-inode":    # the pid flocks something else
    lines.append(f"2: {fl} {pid} {dev}:{ino + 1} 0 EOF")
elif mode == "other-dev":      # same pid, same inode number, ANOTHER filesystem
    major, minor = dev.split(":")
    lines.append(f"2: {fl} {pid} {int(major, 16) + 1:02x}:{minor}:{ino} 0 EOF")
elif mode == "waiter":         # blocked waiter: holds nothing
    lines.append(f"2: -> {fl} {pid} {dev}:{ino} 0 EOF")
elif mode == "posix":          # a POSIX record lock is not the flock the writer takes
    lines.append(f"2: POSIX  ADVISORY  WRITE {pid} {dev}:{ino} 0 EOF")
open(path, "w").write("\\n".join(lines) + "\\n")
"""
RELEVANT_HOLDER = "rebase_runtime_model_state.py"   # argv names the rebase script
OTHER_HOLDER = "some_other_tool.py"                 # argv does not

TOPUP_STUB = '#!/bin/bash\necho "TOPUP $*" >> "$STUB_EVENTS"\nexit 0\n'


def _write(path: Path, text: str | bytes, exe: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(text, str):
        text = text.encode("utf-8")
    path.write_bytes(text)
    if exe:
        path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


# --------------------------------------------------------------- original scripts

def _git_show(rel: str) -> str | None:
    res = subprocess.run(["git", "-C", str(REPO), "show", f"{ORIGINAL_REV}:{rel}"],
                         capture_output=True, text=True)
    return res.stdout if res.returncode == 0 else None


def _scripts_text(rel: str) -> str:
    if SCRIPTS_REV:
        res = subprocess.run(["git", "-C", str(REPO), "show", f"{SCRIPTS_REV}:{rel}"],
                             capture_output=True, text=True, check=True)
        return res.stdout
    return (REPO / rel).read_text(encoding="utf-8")


def transform_original(rel: str, text: str, stub_py: str) -> str:
    """The sed of the brief: drop the hardcoded cd, repoint PY, map misc -> pro_chain."""
    out = []
    for line in text.split("\n"):
        if line.startswith("cd /Users/alex/Documents/ingame"):
            continue
        if re.match(r"^PY=", line):
            line = f"PY={stub_py}"
        out.append(line)
    text = "\n".join(out).replace("runtime/experiments/misc/", "scripts/pro_chain/")
    # Safety: any remaining reference to the real checkout must be inside a python
    # heredoc, which the stub python never executes.
    for line in text.split("\n"):
        if "/Users/alex" in line:
            assert line.lstrip().startswith(("R = Path(", "sys.path.insert(")), line
    return text


# --------------------------------------------------------------------- environment

class Env:
    def __init__(self, base: Path, kv3: bool, target: str, mode: str,
                 shadow: bool = False, prod_is_build: bool = False):
        self.base = base
        self.build = Path(os.path.realpath(base / "build"))
        self.prod = base / "prod"
        self.state = base / "state"
        self.events = base / "events.log"
        self.stubs = base / "stubs"
        self.summary = base / "summary.tsv"
        self.log = base / "build" / "runtime" / "rebuild.log"
        self.mode, self.shadow, self.kv3 = mode, shadow, kv3
        self.prod_is_build = prod_is_build
        self.target = target
        self._make(target)

    def _make(self, target: str) -> None:
        base, build, prod = self.base, self.build, self.prod
        for d in (build / "runtime", build / "data", build / "ELO/output", build / "base/tools",
                  build / "runtime/artifacts/misc", base / "tmp", base / "home"):
            d.mkdir(parents=True, exist_ok=True)
        py = self.stubs / "python3"
        for name, text in (("python3", PY_STUB), ("ssh", SSH_STUB), ("scp", SCP_STUB),
                           ("systemctl", SYSTEMCTL_STUB), ("sleep", SLEEP_STUB),
                           ("sha1sum", SHA1SUM_STUB), ("bash", BASH_STUB),
                           ("systemd-run", SYSTEMD_RUN_STUB), ("timeout", TIMEOUT_STUB),
                           ("mv", MV_STUB)):
            _write(self.stubs / name, text, exe=True)
        _write(self.stubs / "timeout_impl.py", TIMEOUT_IMPL)
        # build tree scripts
        _write(build / LIB_REL, _scripts_text(LIB_REL), exe=True)
        _write(build / "scripts/run/topup_pro_corpus.sh", TOPUP_STUB, exe=True)
        _write(build / "runtime/pro_topup_fresh.log", "fresh\n")  # skip the 20 h safety top-up
        if target == "original":
            for rel in (REBUILD_REL, KV3_REL):
                orig = _git_show(rel)
                assert orig is not None, f"{ORIGINAL_REV}:{rel} not found"
                _write(build / rel, transform_original(rel, orig, str(py)), exe=True)
        else:
            for rel in (REBUILD_REL, KV3_REL):
                _write(build / rel, _scripts_text(rel), exe=True)
        if self.kv3:
            _write(build / "ml-models/prematch_panel_kv3/manifest.json", "{}\n")
            _write(build / "runtime/kv3_state_deliver.on", "")
        # fake prod
        (prod / ".git").mkdir(parents=True, exist_ok=True)
        _write(prod / "base/id_to_names.py", "NAMES = 1\n")
        _write(prod / "base/id_to_names_dynamic_tier2.json", "{}\n")
        _write(prod / "venv/bin/python3", PRODPY_STUB, exe=True)
        for rel in DELIVERIES:
            _write(prod / rel, OLD_ARTIFACT)
        _write(prod / "runtime/prematch_model_bet_sent.jsonl", "")
        _write(prod / "runtime/sourcetv_matches.json", "{}")
        _write(self.state / "ingame/map_id_check.txt", "old-map\n")
        if self.prod_is_build:
            (build / ".git").mkdir(exist_ok=True)

    def env(self) -> dict:
        prod_root = str(self.build if self.prod_is_build else self.prod)
        e = {
            "PATH": f"{self.stubs}:/usr/bin:/bin:/usr/sbin:/sbin",
            "HOME": str(self.base / "home"),
            "TMPDIR": str(self.base / "tmp"),
            "STUB_EVENTS": str(self.events),
            "STUB_BUILD": str(self.build),
            "FAKE_PROD": str(self.prod),
            "FAKE_STATE": str(self.state),
            "PRO_CHAIN_PY": str(self.stubs / "python3"),
            "STUB_REAL_PY": sys.executable,
            "LOG": str(self.log),
            "PRO_CHAIN_SUMMARY": str(self.summary),
        }
        if self.mode == "remote":
            e["PRO_CHAIN_MODE"] = "remote"   # PROD_ROOT stays the default /root/main
        else:
            e["PRO_CHAIN_MODE"] = "local"
            e["PROD_ROOT"] = prod_root
            e["PRO_CHAIN_ALLOW_FAKE_PROD"] = "1"  # the rebase heredoc is rewritten to FAKE_PROD
        if self.shadow:
            e["PRO_CHAIN_SHADOW"] = "1"
        return e

    def run(self, script_rel: str, *args: str, extra: dict | None = None):
        e = self.env()
        if extra:
            e.update(extra)
        return subprocess.run([BASH, str(self.build / script_rel), *args], cwd=self.build, env=e,
                              capture_output=True, text=True, timeout=120)

    # ---- observations
    def events_lines(self) -> list[str]:
        if not self.events.exists():
            return []
        return self.events.read_text(encoding="utf-8").splitlines()

    def py_steps(self) -> list[str]:
        # --cutoff is `date +%s` of the run: not part of the contract being compared
        return [re.sub(r"(--cutoff |state\.npz\.tmp )\d+", r"\1N", l)
                for l in self.events_lines() if l.startswith("PY ")]

    def ops(self, prefixes: tuple[str, ...]) -> list[str]:
        return [l for l in self.events_lines() if l.startswith(prefixes)]

    def prod_tree(self) -> dict[str, str]:
        out = {}
        for root in (self.prod, self.state):
            for p in sorted(root.rglob("*")):
                if p.is_file():
                    out[f"{root.name}/{p.relative_to(root)}"] = hashlib.sha1(p.read_bytes()).hexdigest()
        return out


def _normalize_ops(lines: list[str]) -> list[str]:
    out = []
    for l in lines:
        l = re.sub(r"(--cutoff |state\.npz\.tmp )\d+", r"\1N", l)
        if l.startswith("SSH sha1sum "):          # read-only verification, shape differs by design
            continue
        if l.startswith("SSH ") and "sourcetv_matches.json" in l:
            continue  # read-only live-map gate
        if l.startswith("BASH-S"):
            l = "BASH-S " + l.split()[2]  # compare the original staged-snapshot argument
        if l.startswith("SSH bash -s --"):
            l = " ".join(l.split()[:5])
        out.append(l.replace(" 2>/dev/null", ""))
    return out


@pytest.fixture
def make_env(tmp_path):
    counter = [0]

    def factory(**kw) -> Env:
        counter[0] += 1
        return Env(tmp_path / f"e{counter[0]}", **kw)
    return factory


needs_original = pytest.mark.skipif(_git_show(REBUILD_REL) is None,
                                    reason=f"{ORIGINAL_REV} not in git history")


# ------------------------------------------------------------------------------ (a)

@needs_original
def test_remote_mode_matches_original(make_env):
    orig = make_env(kv3=True, target="original", mode="remote")
    new = make_env(kv3=True, target="new", mode="remote")
    r_orig = orig.run(REBUILD_REL)
    r_new = new.run(REBUILD_REL)
    assert r_orig.returncode == 0, r_orig.stdout + r_orig.stderr + orig.log.read_text()
    assert r_new.returncode == 0, r_new.stdout + r_new.stderr + new.log.read_text()
    assert orig.py_steps(), "original ran no python steps"
    # ordered list of python steps (argv + env notes + cwd) is the same
    assert new.py_steps() == orig.py_steps()
    # same ssh/scp/rebase/systemctl sequence modulo read-only sha1 verification reads
    prefixes = ("SSH ", "SCP-", "BASH-S", "SYSTEMCTL", "PRODPY", "TOPUP")
    assert _normalize_ops(new.ops(prefixes)) == _normalize_ops(orig.ops(prefixes))
    # rebase heredoc really went through `ssh serv1 bash -s`
    assert any(l.startswith("SSH bash -s -- 1") for l in new.events_lines())
    # every upload is followed by a sha1 read of its .tmp (the new path verifies, the old one did too)
    lines = new.events_lines()
    for i, l in enumerate(lines):
        if l.startswith("SCP-UP ") and l.endswith(".tmp"):
            assert any(x.startswith("SSH sha1sum ") and l.split(" ", 1)[1] in x for x in lines[i + 1:]), l
    # the resulting prod trees are identical
    # (the one deliberate addition: runtime/live_elo_rebase.child.pid, the recorded rebase child pid)
    new_tree = {k: v for k, v in new.prod_tree().items() if k != f"prod/{CHILD_PID_REL}"}
    assert len(new.prod_tree()) - len(new_tree) == 1
    assert new_tree == orig.prod_tree()
    for rel, key in DELIVERIES.items():
        assert (new.prod / rel).read_bytes() == BUILT[key], rel
    # notification: no shadow prefix in remote non-shadow mode
    assert not any("[тень]" in l for l in new.ops(("NOTIFY",)))


# ------------------------------------------------------------------------------ (b)

def test_local_mode_no_ssh_delivers_into_prod(make_env):
    remote = make_env(kv3=True, target="new", mode="remote")
    local = make_env(kv3=True, target=TARGET, mode="local")
    assert remote.run(REBUILD_REL).returncode == 0
    r = local.run(REBUILD_REL)
    log = local.log.read_text() if local.log.exists() else ""
    assert r.returncode == 0, r.stdout + r.stderr + log
    assert local.ops(("SSH ", "SCP-")) == [], "local mode must not touch ssh/scp"
    for rel, key in DELIVERIES.items():
        assert (local.prod / rel).read_bytes() == BUILT[key], rel
    # no leftover .tmp in prod
    assert not [p for p in local.prod.rglob("*.tmp")]
    # same stop -> rebase -> start order as remote mode
    seq = ("SYSTEMCTL", "PRODPY", "BASH-S")
    assert local.ops(seq) == remote.ops(seq)
    sysctl = [l for l in local.ops(("SYSTEMCTL",))]
    assert sysctl == ["SYSTEMCTL stop cyberscore.service", "SYSTEMCTL start cyberscore.service",
                      "SYSTEMCTL is-active cyberscore.service"]
    # same build steps as remote mode
    assert local.py_steps() == remote.py_steps()
    # map_id_check cleared by the rebase script
    assert (local.state / "ingame/map_id_check.txt").read_bytes() == b""


# ------------------------------------------------------------------------------ (c)

def test_local_shadow_delivers_nothing(make_env):
    e = make_env(kv3=True, target=TARGET, mode="local", shadow=True)
    before = e.prod_tree()
    r = e.run(REBUILD_REL)
    log = e.log.read_text() if e.log.exists() else ""
    assert r.returncode == 0, r.stdout + r.stderr + log
    assert e.prod_tree() == before, "shadow run changed the prod tree"
    assert e.ops(("SYSTEMCTL", "PRODPY", "BASH-S", "SSH ", "SCP-")) == []
    # every build step still ran
    steps = e.py_steps()
    for needle in ("pro_corpus_extract.py", "finalize_artifact.py", "build_team_org_aliases.py",
                   "live_team_strength.py", "build_prior_snapshot.py", "build_rating_snapshot.py",
                   "build_pair_snapshot.py", "kills_v3_serving"):
        assert any(needle in s for s in steps), needle
    # summary lists every would-be delivery with the sha1 of the built file
    assert e.summary.exists(), "no shadow summary"
    rows = [l.split("\t") for l in e.summary.read_text().splitlines() if l]
    got = {r[0]: r[1] for r in rows}
    assert set(got) == set(DELIVERIES), sorted(set(DELIVERIES) ^ set(got))
    for rel, key in DELIVERIES.items():
        assert got[rel] == hashlib.sha1(BUILT[key]).hexdigest(), rel
    assert all(len(r) == 3 for r in rows)
    # notify_admin messages carry the shadow prefix
    notes = e.ops(("NOTIFY",))
    assert notes and all(n.startswith("NOTIFY [тень]") for n in notes), notes


# ------------------------------------------------------------------------------ (d)

@pytest.mark.parametrize("script", [REBUILD_REL, KV3_REL])
def test_guard_refuses_build_tree_equal_to_prod(make_env, script):
    e = make_env(kv3=True, target=TARGET, mode="local", prod_is_build=True)
    r = e.run(script, *(["--deliver"] if script == KV3_REL else []))
    assert r.returncode == 2, (r.returncode, r.stdout, r.stderr)
    assert e.py_steps() == [], "a build step ran before the guard"
    assert "совпадает с боевым" in r.stderr


def test_guard_refuses_symlinked_build_tree_pointing_at_prod(make_env):
    """ROOT is canonicalised: a symlink to the prod checkout must not slip past the guard."""
    e = make_env(kv3=False, target=TARGET, mode="local")
    if TARGET == "original":
        pytest.skip("the original scripts have no guard")
    link = e.base / "link_to_prod"
    link.symlink_to(e.prod)
    r = e.run(REBUILD_REL, extra={"PRO_CHAIN_ROOT": str(link)})
    assert r.returncode == 2, (r.returncode, r.stdout, r.stderr)
    assert e.py_steps() == [], "a build step ran before the guard"
    assert "совпадает с боевым" in r.stderr


def test_guard_refuses_prod_root_other_than_root_main(make_env):
    """The rebase heredoc is pinned to /root/main; another PROD_ROOT is refused outside tests."""
    e = make_env(kv3=False, target=TARGET, mode="local")
    if TARGET == "original":
        pytest.skip("the original scripts have no guard")
    r = e.run(REBUILD_REL, extra={"PRO_CHAIN_ALLOW_FAKE_PROD": "0"})
    assert r.returncode == 2, (r.returncode, r.stdout, r.stderr)
    assert e.py_steps() == []
    assert "перебазировка работает с /root/main" in r.stderr


# ----------------------------------------------------------------- extra: E-193 guard

def test_corrupted_artifact_delivery_stops_before_restart(make_env):
    """Remote mode: the scp stub appends a byte to the uploaded artifact."""
    e = make_env(kv3=False, target=TARGET, mode="remote")
    r = e.run(REBUILD_REL, extra={"STUB_CORRUPT_UP": "prematch_model_artifact_v3.npz"})
    assert r.returncode == 1, (r.returncode, e.log.read_text())
    assert "ОШИБКА" in e.log.read_text()
    assert e.ops(("SYSTEMCTL", "BASH-S")) == [], "restart must not happen after a bad delivery"


# ------------------------------------------------------------------------------ (e)

@pytest.mark.parametrize("rel", [REBUILD_REL, KV3_REL, LIB_REL])
def test_bash_syntax_under_system_bash(rel):
    res = subprocess.run([BASH, "-n", str(REPO / rel)], capture_output=True, text=True)
    assert res.returncode == 0, res.stderr


def test_shellcheck_if_installed():
    sc = shutil.which("shellcheck")
    if not sc:
        pytest.skip("shellcheck not installed")
    res = subprocess.run([sc, "-S", "warning", "-x", str(REPO / REBUILD_REL), str(REPO / KV3_REL)],
                         capture_output=True, text=True)
    assert res.returncode == 0, res.stdout


# ------------------------------------------------------- shadow staging must not hide errors

def _stage_in_shadow(tmp_path, src, summary):
    script = (f'source "{REPO / LIB_REL}"; prod_stage "$1" data/x.npz; echo "rc=$?"')
    env = {"PATH": os.environ["PATH"], "HOME": str(tmp_path), "PRO_CHAIN_MODE": "remote",
           "PRO_CHAIN_SHADOW": "1", "PRO_CHAIN_SUMMARY": str(summary),
           "PRO_CHAIN_ROOT": str(tmp_path), "PRO_CHAIN_PY": "/usr/bin/true"}
    return subprocess.run([BASH, "-c", script, "stage", str(src)], env=env,
                          capture_output=True, text=True, timeout=30)


def test_shadow_stage_reports_missing_source_and_unwritable_summary(tmp_path):
    good = tmp_path / "built.npz"
    good.write_bytes(b"artifact")
    ok = _stage_in_shadow(tmp_path, good, tmp_path / "sum" / "s.tsv")
    assert "rc=0" in ok.stdout, ok.stdout + ok.stderr
    row = (tmp_path / "sum" / "s.tsv").read_text().split("\t")
    assert row[0] == "data/x.npz" and len(row[1]) == 40 and row[2].strip() == "8"
    missing = _stage_in_shadow(tmp_path, tmp_path / "absent.npz", tmp_path / "sum" / "s.tsv")
    assert "rc=1" in missing.stdout and "нет собранного файла" in missing.stdout
    blocked = tmp_path / "blocked"
    blocked.write_text("a file, not a dir")
    unwritable = _stage_in_shadow(tmp_path, good, blocked / "s.tsv")
    assert "rc=1" in unwritable.stdout and "не записан в сводку" in unwritable.stdout


# ------------------------------------------------------- library safety boundaries

def _run_library(e: Env, script: str, *args: str, extra: dict | None = None):
    env = e.env()
    if extra:
        env.update(extra)
    return subprocess.run(
        [BASH, "-c", 'source "$1"; shift\n' + script,
         "library", str(e.build / LIB_REL), *args],
        cwd=e.build, env=env, capture_output=True, text=True, timeout=5,
    )


def test_library_exports_canonical_draft_root_to_child(make_env):
    e = make_env(kv3=False, target="new", mode="local")
    link = e.base / "build_link"
    link.symlink_to(e.build, target_is_directory=True)
    r = _run_library(e, "/usr/bin/env", extra={"PRO_CHAIN_ROOT": str(link)})
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert f"DRAFT_ROOT={e.build}" in r.stdout.splitlines()


def test_local_stage_rejects_failed_copy_with_matching_stale_tmp(make_env):
    e = make_env(kv3=False, target="new", mode="local")
    rel = "data/prematch_model_artifact_v3.npz"
    src = e.build / rel
    staged = e.prod / (rel + ".tmp")
    _write(src, BUILT["artifact"])
    _write(staged, BUILT["artifact"])
    before = e.prod_tree()
    before.pop(f"prod/{rel}.tmp")
    # A stale, byte-identical .tmp would pass the digest checks if cp's rc were ignored.
    _write(e.stubs / "cp", '#!/bin/bash\necho "CP $*" >> "$STUB_EVENTS"\nexit 1\n', exe=True)
    r = _run_library(e, 'if prod_stage "$1" "$2"; then exit 0; else exit $?; fi',
                     str(src), rel)
    assert r.returncode == 1, (r.stdout, r.stderr)
    assert "не скопирован на прод (rc=1)" in r.stdout
    assert e.ops(("CP ",)) == [f"CP {src} {staged}"]
    assert not staged.exists()
    assert e.prod_tree() == before


def test_local_stage_rejects_empty_prod_sha1(make_env):
    e = make_env(kv3=False, target="new", mode="local")
    rel = "data/prematch_model_artifact_v3.npz"
    src = e.build / rel
    _write(src, BUILT["artifact"])
    before = e.prod_tree()
    # Override only the prod digest: cp succeeds and sha1_of still hashes the real source.
    # The empty-digest warning distinguishes this failure from a nonempty digest mismatch.
    r = _run_library(e, '''
prod_sha1() { :; }
if prod_stage "$1" "$2"; then exit 0; else exit $?; fi
''', str(src), rel)
    assert r.returncode == 1, (r.stdout, r.stderr)
    digest = hashlib.sha1(BUILT["artifact"]).hexdigest()
    assert f"пустой sha1 (локально '{digest}', на проде '')" in r.stdout
    assert not (e.prod / (rel + ".tmp")).exists()
    assert e.prod_tree() == before


def test_guard_refuses_build_root_in_remote_mode(make_env):
    e = make_env(kv3=False, target="new", mode="remote")
    before = e.prod_tree()
    r = _run_library(e, "pro_chain_guard", extra={
        "PROD_ROOT": str(e.build), "PRO_CHAIN_ALLOW_FAKE_PROD": "1",
    })
    assert r.returncode == 2, (r.stdout, r.stderr)
    assert "совпадает с боевым" in r.stderr
    assert e.prod_tree() == before
    assert e.events_lines() == []


def test_guard_requires_existing_prod_root_in_local_mode(make_env):
    e = make_env(kv3=False, target="new", mode="local")
    missing = e.base / "missing_prod"
    before = e.prod_tree()
    r = _run_library(e, "pro_chain_guard", extra={"PROD_ROOT": str(missing)})
    assert r.returncode == 2, (r.stdout, r.stderr)
    assert f"боевой checkout {missing} недоступен" in r.stderr
    assert not missing.exists()
    assert e.prod_tree() == before


def test_library_exits_when_build_root_is_unreachable(make_env):
    e = make_env(kv3=False, target="new", mode="local")
    missing = e.base / "missing_build"
    # No set -e: sourcing must exit the caller itself, rather than relying on errexit.
    r = _run_library(e, "echo reached", extra={"PRO_CHAIN_ROOT": str(missing)})
    assert r.returncode == 2, (r.stdout, r.stderr)
    assert "reached" not in r.stdout
    assert f"дерево сборки {missing} недоступно" in r.stderr
    assert not missing.exists()


def test_guard_refuses_symlinked_prod_root_pointing_at_build(make_env):
    e = make_env(kv3=False, target="new", mode="local")
    link = e.base / "prod_link"
    link.symlink_to(e.build, target_is_directory=True)
    before = e.prod_tree()
    r = _run_library(e, "pro_chain_guard", extra={"PROD_ROOT": str(link)})
    assert r.returncode == 2, (r.stdout, r.stderr)
    assert "совпадает с боевым" in r.stderr
    assert e.prod_tree() == before


# ------------------------------------------------------- live-map gate / memory watchdog

@pytest.mark.parametrize("mode", ["local", "remote"])
def test_chain_gate_waits_for_empty_live_state(make_env, mode):
    e = make_env(kv3=False, target="new", mode=mode)
    _write(e.prod / "runtime/sourcetv_matches.json", '{"map": {}}')
    _write(e.stubs / "sleep", '''#!/bin/bash
echo GATE-SLEEP >> "$STUB_EVENTS"
printf '{}' > "$FAKE_PROD/runtime/sourcetv_matches.json"
''', exe=True)
    # Event-driven: the sleep stub clears the live state on its first call, so the
    # wait ends in milliseconds whatever the budget; 60 s only keeps a loaded machine
    # (first remote probe overran the old 2 s budget) from exhausting it first.
    r = _run_library(e, 'wait_no_live_map 60 test-gate; echo PROCEEDED',
                     extra={"PRO_CHAIN_LIVE_POLL_SECONDS": "0.01"})
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert "жду окончания живой карты" in r.stdout and "PROCEEDED" in r.stdout
    assert e.ops(("GATE-SLEEP",)) == ["GATE-SLEEP"]
    assert "ВНИМАНИЕ" not in r.stdout
    assert (e.prod / "runtime/sourcetv_matches.json").read_text() == "{}"


@pytest.mark.parametrize("state", ['{"map": 1}', "bad json", None, "[]"])
def test_chain_gate_bound_warns_and_proceeds(make_env, state):
    e = make_env(kv3=False, target="new", mode="local")
    live = e.prod / "runtime/sourcetv_matches.json"
    if state is None:
        live.unlink()
    else:
        live.write_text(state)
    r = _run_library(e, 'set -e; wait_no_live_map 0 bounded-gate; echo PROCEEDED')
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert "ВНИМАНИЕ" in r.stdout and "bounded-gate" in r.stdout
    assert "PROCEEDED" in r.stdout


def test_chain_gate_before_elo_snapshot(make_env):
    e = make_env(kv3=False, target="new", mode="local")
    text = (e.stubs / "python3").read_text().replace(
        'printf \'{"aliases": 1}\\n\' > data/team_org_aliases.json;',
        'printf \'{"aliases": 1}\\n\' > data/team_org_aliases.json; '
        'printf \'{"map": 1}\' > "$FAKE_PROD/runtime/sourcetv_matches.json";',
    )
    _write(e.stubs / "python3", text, exe=True)
    _write(e.stubs / "sleep", '''#!/bin/bash
[ "$1" = 3 ] && exit 0
echo ELO-GATE-SLEEP >> "$STUB_EVENTS"
printf '{}' > "$FAKE_PROD/runtime/sourcetv_matches.json"
''', exe=True)
    r = e.run(REBUILD_REL, extra={"PRO_CHAIN_LIVE_POLL_SECONDS": "0.01"})
    assert r.returncode == 0, e.log.read_text()
    events = e.events_lines()
    gate = events.index("ELO-GATE-SLEEP")
    assert gate < next(i for i, line in enumerate(events) if "PY ELO/live_team_strength.py" in line)


@pytest.mark.parametrize("mode", ["local", "remote"])
@pytest.mark.parametrize("clears", [True, False])
@pytest.mark.parametrize("shadow", [False, True])
def test_chain_gate_restart_wait_or_bound_under_errexit(make_env, mode, clears, shadow):
    e = make_env(kv3=False, target="new", mode=mode, shadow=shadow)
    # Turn live only after the heavy steps, immediately before the rebase.
    text = (e.stubs / "python3").read_text().replace(
        'echo "PY $* |$note |@$here" >> "$EV"',
        'echo "PY $* |$note |@$here" >> "$EV"\n'
        '[ "$1" != scripts/pro_chain/prematch_bet_roi.py ] || '
        'printf \'{"map": 1}\' > "$FAKE_PROD/runtime/sourcetv_matches.json"',
    )
    _write(e.stubs / "python3", text, exe=True)
    # Observe the CHAIN/prod boundary before bash -s executes any transaction text.
    # Only the live-state read counts: the transaction's own `python3 -c` helpers
    # (rebase lock probe, delta expanduser) are not live reads.
    _write(e.prod / "venv/bin/python3", PRODPY_STUB.replace(
        'then exec "$STUB_REAL_PY" "$@"; fi',
        'then case "$*" in *sourcetv_matches.json*) echo LIVE-READ >> "$STUB_EVENTS";; esac; '
        'exec "$STUB_REAL_PY" "$@"; fi',
    ), exe=True)
    transaction_stdin = e.base / "transaction.stdin"
    _write(e.stubs / "bash", BASH_STUB.replace(
        'cat > "$tmp.orig"',
        'cat > "$tmp.orig"\n'
        'cp "$tmp.orig" "$D1_TRANSACTION_STDIN"\n'
        'echo "TRANSACTION-LIVE $(cat "$FAKE_PROD/runtime/sourcetv_matches.json")" >> "$STUB_EVENTS"\n'
        'if /usr/bin/grep -q "ВНИМАНИЕ: рестарт cyberscore" "$LOG"; then\n'
        '  echo BOUND-WARNING >> "$STUB_EVENTS"\n'
        'fi\n'
        'if /usr/bin/grep -q "watchdog памяти остановлен перед рестартом" "$LOG"; then\n'
        '  echo WATCHDOG-DISARMED >> "$STUB_EVENTS"\n'
        'fi',
    ), exe=True)
    _write(e.stubs / "sleep", '''#!/bin/bash
[ "$1" = 3 ] && exit 0
echo RESTART-GATE-SLEEP >> "$STUB_EVENTS"
printf '{}' > "$FAKE_PROD/runtime/sourcetv_matches.json"
''', exe=True)
    r = e.run(REBUILD_REL, extra={
        # clears: event-driven (the sleep stub clears the state), 60 s is only headroom
        # against load; not clears: 0 s is deterministic (exhausted before any read).
        "PRO_CHAIN_RESTART_WAIT_SECONDS": "60" if clears else "0",
        "PRO_CHAIN_LIVE_POLL_SECONDS": "0.01",
        "D1_TRANSACTION_STDIN": str(transaction_stdin),
    })
    log = e.log.read_text()
    assert r.returncode == 0, log
    events = e.events_lines()
    roi = next(i for i, line in enumerate(events) if "PY scripts/pro_chain/prematch_bet_roi.py" in line)
    reads = [i for i, line in enumerate(events) if i > roi and line == "LIVE-READ"]
    if shadow:
        assert not reads and "RESTART-GATE-SLEEP" not in events
        assert "рестарт cyberscore — жду" not in log
        assert "ВНИМАНИЕ: рестарт cyberscore" not in log
        assert not transaction_stdin.exists()
        assert not e.ops(("BASH-S", "SYSTEMCTL"))
        return
    transaction = next(i for i, line in enumerate(events) if line.startswith("BASH-S"))
    assert reads and max(reads) < transaction, "restart reads must precede the transaction session"
    assert events.index("WATCHDOG-DISARMED") < transaction
    if mode == "remote":
        ssh_transaction = events.index(f"SSH bash -s -- 1 {REBASE_TIMEOUT_DEFAULT}")
        ssh_reads = [i for i, line in enumerate(events)
                     if i > roi and line.startswith("SSH ") and "sourcetv_matches.json" in line]
        assert ssh_reads and max(ssh_reads) < ssh_transaction < transaction
    else:
        assert not e.ops(("SSH ",))
    # The transaction text reaches `bash -s` byte for byte as the script's own heredoc
    # (the rebase-timeout rewrite of 08.10.2026 changed the text since 3de5a5af, so the
    # pin is the script under test, not that old revision).
    base = _scripts_text(REBUILD_REL).encode("utf-8")
    base_stdin = base.split(b"<<'ELO_REBASE_REMOTE'\n", 1)[1].split(b"ELO_REBASE_REMOTE\n", 1)[0]
    assert transaction_stdin.read_bytes() == base_stdin
    assert e.ops(("BASH-S",)) == [f"BASH-S -- 1 {REBASE_TIMEOUT_DEFAULT} {hashlib.sha1(base_stdin).hexdigest()}"]
    stop = events.index("SYSTEMCTL stop cyberscore.service")
    if clears:
        assert events.index("RESTART-GATE-SLEEP") < transaction < stop
        assert "TRANSACTION-LIVE {}" in events
        assert "BOUND-WARNING" not in events
        assert "жду окончания живой карты" in log
    else:
        assert events.index("BOUND-WARNING") < transaction < stop
        assert 'TRANSACTION-LIVE {"map": 1}' in events
        assert "ВНИМАНИЕ: рестарт cyberscore" in log
    assert e.ops(("SYSTEMCTL",))[-1] == "SYSTEMCTL is-active cyberscore.service"


def _memory_probe(e, *, live=True, available=1, descendant=0.6):
    # MemAvailable starts HIGH and the probe stub itself writes `available` only AFTER
    # it has spawned the background descendant and logged PROBE-CHILD: the watchdog
    # (0.05 s poll) can only fire while the probe and its descendant both exist, so
    # "descendant not finished" really proves the descendant was killed. Writing the low value from t=0 let a loaded
    # machine kill the group before pro_corpus_extract.py started (3/3 red on HEAD).
    _write(e.prod / "runtime/sourcetv_matches.json", '{"map": 1}' if live else "{}")
    meminfo = e.base / "meminfo"
    _write(meminfo, "MemAvailable: 90000000 kB\n")
    _write(e.stubs / "sleep", '''#!/bin/bash
[ "$1" = 3 ] && exit 0
exec /bin/sleep "$@"
''', exe=True)
    text = (e.stubs / "python3").read_text().replace(
        'echo "PY $* |$note |@$here" >> "$EV"',
        'echo "PY $* |$note |@$here" >> "$EV"\n'
        'if [ "$1" = scripts/pro_chain/pro_corpus_extract.py ]; then\n'
        '  echo PROBE-BEGIN >> "$EV"\n'
        f'  (/bin/sleep {descendant}; echo PROBE-DESCENDANT-FINISHED >> "$EV") &\n'
        '  echo "PROBE-CHILD $!" >> "$EV"\n'
        f'  printf \'MemAvailable: {available} kB\\n\' > "$PRO_CHAIN_MEMINFO_PATH.new"\n'
        '  mv "$PRO_CHAIN_MEMINFO_PATH.new" "$PRO_CHAIN_MEMINFO_PATH"\n'
        '  wait $!\n'
        '  echo PROBE-FINISHED >> "$EV"\n'
        'fi',
    )
    _write(e.stubs / "python3", text, exe=True)
    return {
        "PRO_CHAIN_MEMINFO_PATH": str(meminfo),
        "PRO_CHAIN_MIN_AVAILABLE_KB": "100",
        "PRO_CHAIN_WATCHDOG_INTERVAL_SECONDS": "0.05",
        "PRO_CHAIN_HEAVY_WAIT_SECONDS": "0",
        "PRO_CHAIN_RESTART_WAIT_SECONDS": "0",
    }


@pytest.mark.parametrize("shadow", [False, True])
def test_chain_gate_watchdog_kills_group_and_prevents_delivery(make_env, shadow):
    e = make_env(kv3=False, target="new", mode="local", shadow=shadow)
    extra = _memory_probe(e, descendant=1.5)
    before = e.prod_tree()
    r = e.run(REBUILD_REL, extra=extra)
    log = e.log.read_text()
    assert r.returncode != 0, log
    assert "ОШИБКА: watchdog памяти" in log
    # Wait beyond the stub's sleep: killing only the shell PID would leave the
    # background descendant alive to append its marker. No ps dependency.
    time.sleep(1.6)
    events = e.events_lines()
    assert "PROBE-BEGIN" in events and "PROBE-FINISHED" not in events
    # The descendant existed when the kill fired (its pid line precedes the low
    # MemAvailable write), so its missing marker means it was killed, not never born.
    assert any(l.startswith("PROBE-CHILD ") for l in events)
    assert "PROBE-DESCENDANT-FINISHED" not in events
    assert e.prod_tree() == before
    assert e.ops(("SYSTEMCTL", "PRODPY", "BASH-S")) == []
    assert not e.summary.exists()


@pytest.mark.parametrize("mode,live,available", [
    ("local", False, 1), ("local", True, 100), ("remote", True, 1),
])
def test_chain_gate_watchdog_requires_local_live_and_low_memory(make_env, mode, live, available):
    e = make_env(kv3=False, target="new", mode=mode)
    r = e.run(REBUILD_REL, extra=_memory_probe(e, live=live, available=available))
    log = e.log.read_text()
    assert r.returncode == 0, log
    assert "PROBE-FINISHED" in e.events_lines()
    assert "ОШИБКА: watchdog памяти" not in log
    assert e.ops(("SYSTEMCTL",))[-1] == "SYSTEMCTL is-active cyberscore.service"


def test_chain_gate_watchdog_disarmed_before_rebase(make_env):
    e = make_env(kv3=False, target="new", mode="local")
    extra = _memory_probe(e, live=False, available=100)
    _write(e.prod / "venv/bin/python3", PRODPY_STUB.replace(
        'echo "PRODPY $*" >> "$STUB_EVENTS"',
        'echo "PRODPY $*" >> "$STUB_EVENTS"\n'
        'if [ "$1" = ELO/rebase_runtime_model_state.py ]; then\n'
        '  printf \'{"map": 1}\' > runtime/sourcetv_matches.json\n'
        '  printf \'MemAvailable: 1 kB\\n\' > "$PRO_CHAIN_MEMINFO_PATH"\n'
        '  /bin/sleep 0.3\n'
        'fi',
    ), exe=True)
    r = e.run(REBUILD_REL, extra=extra)
    log = e.log.read_text()
    assert r.returncode == 0, log
    assert "watchdog памяти остановлен перед рестартом" in log
    assert "ОШИБКА: watchdog памяти" not in log
    assert e.ops(("SYSTEMCTL",))[-1] == "SYSTEMCTL is-active cyberscore.service"
    assert (e.state / "ingame/map_id_check.txt").read_bytes() == b""


def test_chain_gate_watchdog_cleaned_on_build_failure(make_env):
    e = make_env(kv3=False, target="new", mode="local")
    extra = _memory_probe(e, live=False, available=100)
    _write(e.stubs / "python3", PY_STUB.replace(
        'echo "PY $* |$note |@$here" >> "$EV"',
        'echo "PY $* |$note |@$here" >> "$EV"\n'
        '[ "$1" != scripts/pro_chain/pro_corpus_extract.py ] || exit 7',
    ), exe=True)
    r = e.run(REBUILD_REL, extra=extra)
    assert r.returncode == 7, e.log.read_text()
    match = re.search(r"watchdog памяти: PID=(\d+)", e.log.read_text())
    assert match, e.log.read_text()
    with pytest.raises(ProcessLookupError):
        os.kill(int(match.group(1)), 0)


@pytest.mark.parametrize("read_timeout", ["0", "1"])
def test_chain_gate_bounds_a_blocked_live_state_read(make_env, read_timeout):
    e = make_env(kv3=False, target="new", mode="local")
    live = e.prod / "runtime/sourcetv_matches.json"
    live.rename(live.with_suffix(".saved"))
    os.mkfifo(live)
    start = time.monotonic()
    r = _run_library(e, 'set -e; wait_no_live_map 1 blocked-read; echo PROCEEDED', extra={
        "PRO_CHAIN_READ_TIMEOUT_SECONDS": read_timeout, "PRO_CHAIN_LIVE_POLL_SECONDS": "0.01",
    })
    assert r.returncode == 0, (r.stdout, r.stderr)
    # Wall-time assertion: a blocked read must neither return at once nor hang. The read
    # budget here is 1 s; lib_pro_chain.sh falls back to a 15 s default when the env
    # knob is ignored, so the upper bound has to sit BELOW 15 s or a regression to the
    # default would pass. 10 s = 10x the 0.8-1.0 s measured locally (load jitter room).
    assert 0.8 <= time.monotonic() - start < 10
    assert "ВНИМАНИЕ: blocked-read" in r.stdout and "PROCEEDED" in r.stdout


def test_chain_gate_caps_fractional_poll_at_remaining_wait(make_env):
    e = make_env(kv3=False, target="new", mode="local")
    _write(e.prod / "runtime/sourcetv_matches.json", '{"map": 1}')
    _write(e.stubs / "sleep", '''#!/bin/bash
echo "GATE-SLEEP $1" >> "$STUB_EVENTS"
printf '{}' > "$FAKE_PROD/runtime/sourcetv_matches.json"
''', exe=True)
    # A 30 s bound: SECONDS ticks on whole wall-clock seconds, so a small bound lets
    # a tick (or load) during the first live-state read exhaust the wait before any
    # sleep. The poll must still be capped at the remaining wait, never 100.5.
    r = _run_library(e, 'set -e; wait_no_live_map 30 capped-poll', extra={
        "PRO_CHAIN_LIVE_POLL_SECONDS": "100.5",
    })
    assert r.returncode == 0, (r.stdout, r.stderr)
    sleeps = e.ops(("GATE-SLEEP",))
    assert len(sleeps) == 1, sleeps
    assert 1 <= float(sleeps[0].split()[1]) <= 30, sleeps


def test_chain_gate_parent_signal_waits_for_shadow_rebuild(make_env):
    e = make_env(kv3=False, target="new", mode="local", shadow=True)
    extra = _memory_probe(e, live=False, available=100)
    before = e.prod_tree()
    env = dict(e.env(), **extra)
    process = subprocess.Popen([BASH, str(e.build / REBUILD_REL)], cwd=e.build, env=env,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 30
        while "PROBE-BEGIN" not in e.events_lines() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert "PROBE-BEGIN" in e.events_lines()
        process.send_signal(signal.SIGTERM)
        out, err = process.communicate(timeout=30)
        assert process.returncode == 143, (out, err)
        assert "PROBE-FINISHED" in e.events_lines(), "parent exited before rebuild finished"
        assert e.prod_tree() == before
        assert e.ops(("SYSTEMCTL", "PRODPY", "BASH-S")) == []
    finally:
        # Let the bounded stub finish even when running against an old script.
        process.wait(timeout=5)
        time.sleep(0.8)


# ------------------------------------------------------------- ELO rebase transaction
#
# Question: can the nightly ELO rebase keep prod cyberscore stopped for hours?
# 08.10.2026 it did (04:18-10:21 MSK): the rebase ran inside the chain unit's cgroup
# (MemoryHigh 7.5G, no swap) and was throttled 6 h; nothing bounded it. Two guards:
# (A) local mode runs the transaction in its own systemd scope, (B) the rebase runs
# under `timeout` and a failure with untouched runtime ELO files restarts prod.
# All assertions are at the delivery boundary: the systemctl call sequence, the
# map_id_check file, the exit code and the line the admin chat receives.

RUNTIME_ELO = ("runtime/live_elo_model_state.json", "runtime/live_elo_progress.json")


def _rebase_env(make_env, mode="local", **kw):
    e = make_env(kv3=False, target=TARGET, mode=mode, **kw)
    _write(e.prod / "venv/bin/python3", PRODPY_REBASE_STUB, exe=True)
    for rel in RUNTIME_ELO:
        _write(e.prod / rel, f"{rel}-v1\n")
    return e


def _run_rebase(e, outcome, **extra):
    env = {"STUB_REBASE": outcome, "PRO_CHAIN_RESTART_WAIT_SECONDS": "0",
           "STUB_TIMEOUT_CAP": "rebase_runtime_model_state=2"}
    env.update(extra)
    r = e.run(REBUILD_REL, extra=env)
    log = e.log.read_text() if e.log.exists() else ""
    return r, log


def _rebase_launch() -> str:
    """The command the chain hands to `timeout` (the stub logs its argv with spaces)."""
    return (f"bash -c {PID_WRAPPER} _ {CHILD_PID_REL} "
            "venv/bin/python3 ELO/rebase_runtime_model_state.py")


def _map_id_check(e) -> bytes:
    return (e.state / "ingame/map_id_check.txt").read_bytes()


def _prod_snapshot(e) -> bytes:
    return (e.prod / "ELO/output/live_team_elo_snapshot.json").read_bytes()


def test_local_transaction_runs_in_its_own_systemd_scope(make_env):
    e = _rebase_env(make_env)
    r, log = _run_rebase(e, "ok")
    assert r.returncode == 0, r.stdout + r.stderr + log
    events = e.events_lines()
    # availability probe first, then the real run with no memory throttle and no swap cap
    scope = [l for l in events if l.startswith("SYSTEMD-RUN ")]
    assert scope == [
        "SYSTEMD-RUN --scope --quiet --collect true",
        "SYSTEMD-RUN --scope --quiet --collect -p MemoryHigh=infinity -p MemorySwapMax=infinity "
        f"bash -s -- 1 {REBASE_TIMEOUT_DEFAULT}",
    ]
    # the transaction itself ran inside that scope, after it was created
    transaction = next(i for i, l in enumerate(events) if l.startswith("BASH-S"))
    assert events.index(scope[1]) < transaction
    assert events[transaction + 1] == "SCOPE-ENV 1"
    assert "ВНИМАНИЕ: systemd-run" not in log
    # rebase went through timeout with the kill-after grace and the unchanged argv
    assert (f"TIMEOUT -k 60 {REBASE_TIMEOUT_DEFAULT} {_rebase_launch()} "
            "--snapshot ELO/output/live_team_elo_snapshot.json.tmp") in events
    assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service",
                                     "SYSTEMCTL start cyberscore.service",
                                     "SYSTEMCTL is-active cyberscore.service"]
    assert _map_id_check(e) == b""


def test_remote_transaction_does_not_use_systemd_run(make_env):
    e = _rebase_env(make_env, mode="remote")
    r, log = _run_rebase(e, "ok")
    assert r.returncode == 0, r.stdout + r.stderr + log
    assert not e.ops(("SYSTEMD-RUN",))
    assert f"SSH bash -s -- 1 {REBASE_TIMEOUT_DEFAULT}" in e.events_lines()


def test_local_transaction_without_scope_falls_back_with_a_warning(make_env):
    e = _rebase_env(make_env)
    r, log = _run_rebase(e, "ok", STUB_SYSTEMD_RUN_FAIL="1")
    assert r.returncode == 0, r.stdout + r.stderr + log
    assert "ВНИМАНИЕ: systemd-run --scope недоступен" in log
    events = e.events_lines()
    assert [l for l in events if l.startswith("SYSTEMD-RUN ")] == [
        "SYSTEMD-RUN --scope --quiet --collect true"]   # only the failed probe
    transaction = next(i for i, l in enumerate(events) if l.startswith("BASH-S"))
    assert events[transaction + 1] == "SCOPE-ENV 0"
    assert e.ops(("SYSTEMCTL",))[-1] == "SYSTEMCTL is-active cyberscore.service"


def test_rebase_timeout_is_passed_as_a_positional_argument(make_env):
    e = _rebase_env(make_env)
    r, log = _run_rebase(e, "ok", PRO_CHAIN_REBASE_TIMEOUT_SECONDS="777")
    assert r.returncode == 0, r.stdout + r.stderr + log
    assert any(l.startswith(f"TIMEOUT -k 60 777 {_rebase_launch()}")
               for l in e.events_lines())
    assert any(l.startswith("BASH-S -- 1 777 ") for l in e.events_lines())


@pytest.mark.parametrize("mode", ["local", "remote"])
@pytest.mark.parametrize("outcome,rc", [("hang", 124), ("kill-clean", 137), ("rollback-clean", 2)])
def test_rebase_failure_with_untouched_runtime_files_brings_prod_back(make_env, mode, outcome, rc):
    e = _rebase_env(make_env, mode=mode)
    before = {rel: (e.prod / rel).read_bytes() for rel in RUNTIME_ELO}
    snapshot_before = _prod_snapshot(e)
    r, log = _run_rebase(e, outcome, PRO_CHAIN_REBASE_TIMEOUT_SECONDS="60")
    assert r.returncode == 1, r.stdout + r.stderr + log
    # prod stopped, rebase attempted, prod started again on the old snapshot; never "active" polled
    assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service",
                                     "SYSTEMCTL start cyberscore.service"]
    assert _map_id_check(e) == b"", "map_id_check must be cleared before the start"
    assert {rel: (e.prod / rel).read_bytes() for rel in RUNTIME_ELO} == before
    assert _prod_snapshot(e) == snapshot_before, "a failed rebase must not install the new snapshot"
    assert not e.ops(("PRODPY ELO/convert_state_to_delta.py", "PRODPY ELO/build_state_arrays.py"))
    err = [l for l in log.splitlines() if l.startswith("ОШИБКА: перебазировка ELO прервана")]
    assert len(err) == 1, log
    assert f"rc={rc}" in err[0] and "лимит 60 с" in err[0]
    assert "runtime ELO не изменён, прод поднят на прежнем снимке" in err[0]


@pytest.mark.parametrize("touched", ["kill-state", "kill-progress", "kill-delta"])
def test_rebase_killed_after_touching_runtime_files_leaves_prod_stopped(make_env, touched):
    e = _rebase_env(make_env)
    r, log = _run_rebase(e, touched)
    assert r.returncode == 137, r.stdout + r.stderr + log
    assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service"], "prod must stay stopped"
    assert _map_id_check(e) == b"old-map\n", "map_id_check must not be touched"
    assert _prod_snapshot(e) == OLD_ARTIFACT
    assert "ОШИБКА: целостность runtime ELO не подтверждена (rc=137" in log
    assert "оставлен остановленным" in log
    assert "прод поднят" not in log


def test_rebase_rejected_with_rc_1_clears_map_id_check_and_restarts(make_env):
    e = _rebase_env(make_env)
    r, log = _run_rebase(e, "reject")
    assert r.returncode == 1, r.stdout + r.stderr + log
    assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service",
                                     "SYSTEMCTL start cyberscore.service"]
    assert _map_id_check(e) == b""
    assert "ОШИБКА: перебазировка ELO отклонена; новый снимок не установлен" in log
    assert "прервана" not in log
    assert _prod_snapshot(e) == OLD_ARTIFACT


def test_transaction_refuses_to_stop_prod_without_timeout_binary(make_env):
    e = _rebase_env(make_env)
    (e.stubs / "timeout").unlink()   # and hide a host `timeout` (serv1 has coreutils) via PATH
    r, log = _run_rebase(e, "ok", PATH=f"{e.stubs}:{_path_without_timeout(e)}")
    assert r.returncode == 1, r.stdout + r.stderr + log
    assert not e.ops(("SYSTEMCTL",)), "prod must not be stopped when the rebase cannot be bounded"
    assert _map_id_check(e) == b"old-map\n"
    assert "ОШИБКА: нет команды timeout" in log


def _path_without_timeout(e) -> str:
    """A PATH of symlinks to /usr/bin:/bin minus `timeout`/`gtimeout` (serv1 has coreutils there)."""
    shim = e.base / "path_no_timeout"
    shim.mkdir(exist_ok=True)
    for d in ("/usr/bin", "/bin", "/usr/sbin", "/sbin"):
        for p in Path(d).iterdir():
            if p.name in ("timeout", "gtimeout", "systemd-run"):
                continue
            link = shim / p.name
            if not link.exists() and not link.is_symlink():
                link.symlink_to(p)
    return str(shim)


# ------------------------------------------------------------------ round 2 hardening
# F1 orphan writer, F2 LIVE_ELO_DELTA path, F3 bounded post-rebase steps, F4 limit
# validator, F5 failed mv. Same delivery boundary as above: systemctl sequence,
# map_id_check, exit code, snapshot bytes, admin-chat lines.

def _kill_pidfile(path: Path) -> None:
    """Leave no stub writer behind, whatever the test did."""
    try:
        pid = int(path.read_text().strip())
    except (OSError, ValueError):
        return
    if pid <= 1:
        # the child pid file holds the placeholder 0000000000 until the wrapper runs:
        # os.kill(0, ...) would SIGKILL this whole process group (pytest included), and
        # pid 1 is init (astra r7b P1)
        return
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _alive(pid: int) -> bool:
    if pid <= 1:
        return False   # 0 = "this process group", never a recorded child
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _wait_dead(pid: int, seconds: float = 5.0) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if not _alive(pid):
            return True
        time.sleep(0.05)
    return not _alive(pid)


@pytest.mark.parametrize("env_value,stub_file", [
    ("runtime/custom_delta.json", "runtime/custom_delta.json"),
    ("~/elo_delta.json", "HOME/elo_delta.json"),
])
def test_live_elo_delta_env_path_is_fingerprinted(make_env, env_value, stub_file):
    e = _rebase_env(make_env)
    target = stub_file.replace("HOME", str(e.base / "home"))
    r, log = _run_rebase(e, "kill-env-delta", LIVE_ELO_DELTA=env_value, STUB_DELTA_FILE=target)
    assert r.returncode == 137, r.stdout + r.stderr + log
    assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service"], \
        "a changed LIVE_ELO_DELTA file is a changed runtime ELO: prod must stay stopped"
    assert _map_id_check(e) == b"old-map\n"
    assert "ОШИБКА: целостность runtime ELO не подтверждена" in log


@pytest.mark.parametrize("hang_in", ["convert", "build"])
def test_post_rebase_steps_are_bounded_and_prod_still_starts(make_env, hang_in):
    e = _rebase_env(make_env)
    extra = {"STUB_HANG_SECONDS": "4",
             "STUB_TIMEOUT_CAP": f"{'convert_state_to_delta' if hang_in == 'convert' else 'build_state_arrays'}=1"}
    extra["STUB_CONVERT" if hang_in == "convert" else "STUB_BUILD"] = "hang"
    r, log = _run_rebase(e, "ok", **extra)
    assert r.returncode == 0, r.stdout + r.stderr + log
    events = e.events_lines()
    conv = "TIMEOUT -k 30 900 venv/bin/python3 ELO/convert_state_to_delta.py --if-stale"
    build = "TIMEOUT -k 30 900 venv/bin/python3 ELO/build_state_arrays.py"
    assert conv in events and build in events, events
    assert events.index(conv) < events.index(build) < events.index("SYSTEMCTL start cyberscore.service")
    time.sleep(4.5)   # past the stub's own 4 s: only an unbounded step gets to say it completed
    assert not [l for l in e.events_lines() if l.startswith("HANG-COMPLETED")], \
        "the stuck step must be cut at its limit, not left to run"
    marker = ("ВНИМАНИЕ: обновление ELO-дельты не удалось или превысило 900 с" if hang_in == "convert"
              else "ВНИМАНИЕ: sidecar массивов ELO не собрался или превысил 900 с")
    assert marker in log
    assert e.ops(("SYSTEMCTL",))[-1] == "SYSTEMCTL is-active cyberscore.service"
    assert _map_id_check(e) == b""
    assert _prod_snapshot(e) == BUILT["elo"]


@pytest.mark.parametrize("bad", ["00", "0", "59", "7201", "1e3", "-5", "abc", "99999999999"])
def test_invalid_rebase_limit_falls_back_to_1800_with_a_warning(make_env, bad):
    e = _rebase_env(make_env)
    r, log = _run_rebase(e, "ok", PRO_CHAIN_REBASE_TIMEOUT_SECONDS=bad)
    assert r.returncode == 0, r.stdout + r.stderr + log
    assert f"ВНИМАНИЕ: лимит перебазировки '{bad}' недопустим" in log
    assert any(l.startswith(f"TIMEOUT -k 60 {REBASE_TIMEOUT_DEFAULT} {_rebase_launch()}")
               for l in e.events_lines()), e.events_lines()


@pytest.mark.parametrize("good,used", [("60", 60), ("7200", 7200), ("0090", 90)])
def test_valid_rebase_limit_is_used_without_a_warning(make_env, good, used):
    e = _rebase_env(make_env)
    r, log = _run_rebase(e, "ok", PRO_CHAIN_REBASE_TIMEOUT_SECONDS=good)
    assert r.returncode == 0, r.stdout + r.stderr + log
    assert "недопустим" not in log
    assert any(l.startswith(f"TIMEOUT -k 60 {used} {_rebase_launch()}")
               for l in e.events_lines()), e.events_lines()


def test_failed_snapshot_mv_keeps_prod_stopped_with_explicit_error(make_env):
    # Runtime ELO is already rebased onto the NEW snapshot when mv fails, so
    # starting prod on the old snapshot would mix bases silently: stay stopped,
    # exit nonzero (notify_chain alerts), leave the .tmp for manual install.
    e = _rebase_env(make_env)
    r, log = _run_rebase(e, "ok", STUB_MV_FAIL="1")
    assert r.returncode == 1, r.stdout + r.stderr + log
    assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service"]
    assert _prod_snapshot(e) == OLD_ARTIFACT, "the old snapshot must stay in place"
    assert "ОШИБКА: не удалось установить новый снимок (mv rc=1); runtime ELO уже перебазирован на него, прод оставлен остановленным" in log
    assert not e.ops(("PRODPY ELO/convert_state_to_delta.py", "PRODPY ELO/build_state_arrays.py"))


# ------------------------------------------------------------------ round 3 hardening
# R1 rc=1 is fingerprinted too, R2 LIVE_ELO_DELTA resolved like Path.expanduser,
# (R3, the argv-anchored writer pattern, was replaced by the flock in round 5.)

def test_rebase_rc_1_after_touching_runtime_files_leaves_prod_stopped(make_env):
    # The rebase script prints and stat()s AFTER a successful write, outside any
    # handler: an exception there exits 1 with a changed base. Prod on the old
    # snapshot next to a rebased runtime ELO would count on a mixed base.
    e = _rebase_env(make_env)
    r, log = _run_rebase(e, "reject-state")
    assert r.returncode == 1, r.stdout + r.stderr + log
    assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service"], "prod must stay stopped"
    assert _map_id_check(e) == b"old-map\n", "map_id_check must not be touched"
    assert _prod_snapshot(e) == OLD_ARTIFACT
    assert "ОШИБКА: целостность runtime ELO не подтверждена (rc=1" in log
    assert "runtime ELO изменён" in log and "оставлен остановленным" in log
    assert "отклонена" not in log and "прод поднят" not in log


def test_rebase_rc_1_after_touching_env_delta_leaves_prod_stopped(make_env):
    e = _rebase_env(make_env)
    target = e.base / "home/elo_delta.json"
    r, log = _run_rebase(e, "reject-env-delta", LIVE_ELO_DELTA="~/elo_delta.json",
                         STUB_DELTA_FILE=str(target))
    assert r.returncode == 1, r.stdout + r.stderr + log
    assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service"]
    assert "ОШИБКА: целостность runtime ELO не подтверждена (rc=1" in log


def _pw_home() -> tuple[str, str]:
    import pwd
    pw = pwd.getpwuid(os.getuid())
    return pw.pw_name, pw.pw_dir


def test_live_elo_delta_tilde_user_path_is_fingerprinted(make_env):
    # Python opens Path(env).expanduser(): '~user/x' is user's home from the passwd
    # database, not $HOME. The fingerprint must follow the same file. The path goes
    # from that home up and into the test's temp dir, so nothing is written to the
    # real home directory.
    name, home = _pw_home()
    e = _rebase_env(make_env)
    target = e.base / "tilde_user_delta.json"
    rel = os.path.relpath(target, home)
    value = f"~{name}/{rel}"
    assert Path(os.path.expanduser(value)).resolve() == target.resolve()
    r, log = _run_rebase(e, "kill-env-delta", LIVE_ELO_DELTA=value,
                         STUB_DELTA_FILE=os.path.expanduser(value))
    assert r.returncode == 137, r.stdout + r.stderr + log
    assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service"], \
        "a changed '~user/' LIVE_ELO_DELTA file is a changed runtime ELO: prod must stay stopped"
    assert _map_id_check(e) == b"old-map\n"
    assert "ОШИБКА: целостность runtime ELO не подтверждена" in log
    assert "ВНИМАНИЕ: не удалось раскрыть путь LIVE_ELO_DELTA" not in log


def test_live_elo_delta_unresolvable_tilde_falls_back_with_a_warning(make_env):
    # The resolver python failing must not abort the chain: literal value + warning.
    e = _rebase_env(make_env)
    _write(e.prod / "venv/bin/python3", PRODPY_REBASE_STUB.replace(
        'if [ "${1:-}" = "-c" ]; then exec "$STUB_REAL_PY" "$@"; fi',
        'if [ "${1:-}" = "-c" ]; then case "$2" in *expanduser*) exit 3;; esac; exec "$STUB_REAL_PY" "$@"; fi'),
        exe=True)
    r, log = _run_rebase(e, "ok", LIVE_ELO_DELTA="~/elo_delta.json")
    assert r.returncode == 0, r.stdout + r.stderr + log
    assert "ВНИМАНИЕ: не удалось раскрыть путь LIVE_ELO_DELTA '~/elo_delta.json'" in log
    assert e.ops(("SYSTEMCTL",))[-1] == "SYSTEMCTL is-active cyberscore.service"


# ------------------------------------------------------------------ round 5 hardening
# The writer is identified by the flock it owns (ELO/rebase_runtime_model_state.py takes
# it first), not by an argv regex: three review rounds kept finding argv forms the
# pgrep pattern misread (`-uX dev`, `-BW ignore`, `...state.py.bak`, `-X ELO/rebase...py
# tool.py`). These tests no longer scan or depend on the machine-wide process table, so
# they are safe to run in parallel with anything. The only process lookups left are the
# script's own `ps -p <the pid from the lock file>` pid-reuse guard and `os.kill(pid, 0)`.

LOCK_REL = "runtime/live_elo_rebase.lock"


def _holder_script(e, name: str) -> Path:
    path = e.base / "holders" / name
    _write(path, HOLDER_PY)
    return path


def _start_lock_holder(e, name: str = RELEVANT_HOLDER, python_opts: tuple[str, ...] = ()):
    """A real python holding the fake prod's rebase lock, started before the chain."""
    script = _holder_script(e, name)
    proc = subprocess.Popen([sys.executable, *python_opts, str(script), str(e.prod / LOCK_REL)],
                            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, text=True)
    assert proc.stdout.readline().strip() == "LOCKED", "holder did not take the lock"
    return proc


def _stop(proc: subprocess.Popen) -> None:
    proc.kill()
    proc.wait()
    if proc.stdout:
        proc.stdout.close()


def _lock_free(e) -> bool:
    import fcntl
    fd = os.open(str(e.prod / LOCK_REL), os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    finally:
        os.close(fd)
    return True


@pytest.mark.parametrize("mode", ["local", "remote"])
@pytest.mark.parametrize("python_opts", [(), ("-BW", "ignore"), ("-uX", "dev")])
def test_held_rebase_lock_refuses_before_prod_is_stopped(make_env, mode, python_opts):
    # The holder's argv names the script exactly as the old regex-missed forms did
    # (`python -BW ignore ...rebase_runtime_model_state.py`); the lock does not care.
    e = _rebase_env(make_env, mode=mode)
    holder = _start_lock_holder(e, python_opts=python_opts)
    try:
        r, log = _run_rebase(e, "ok")
        assert r.returncode == 1, r.stdout + r.stderr + log
        assert not e.ops(("SYSTEMCTL",)), "prod must not be stopped next to a running writer"
        assert not e.ops(("TIMEOUT",)) and not e.ops(("PRODPY ELO/rebase",))
        assert _map_id_check(e) == b"old-map\n"
        assert "ОШИБКА: перебазировка ELO уже выполняется" in log
        assert holder.poll() is None, "a foreign writer is not ours to kill"
    finally:
        _stop(holder)


def test_unprobeable_lock_refuses_before_prod_is_stopped(make_env):
    # Unknown is not free: a lock path that cannot be flocked (here: a directory)
    # proves nothing about the absence of a writer.
    e = _rebase_env(make_env)
    (e.prod / LOCK_REL).mkdir()
    r, log = _run_rebase(e, "ok")
    assert r.returncode == 1, r.stdout + r.stderr + log
    assert not e.ops(("SYSTEMCTL",)) and not e.ops(("TIMEOUT",))
    assert _map_id_check(e) == b"old-map\n"
    assert "ОШИБКА: не удалось проверить замок перебазировки" in log
    assert "уже выполняется" not in log


@pytest.mark.parametrize("argv0", [
    "codex exec please run venv/bin/python3 ELO/rebase_runtime_model_state.py --snapshot x and report",
    "venv/bin/python3 -X dev tools/check_rebase_runtime_model_state.py",
    "venv/bin/python3 ELO/rebase_runtime_model_state.py.bak",
    "venv/bin/python3 -X ELO/rebase_runtime_model_state.py tool.py",
    "venv/bin/python3 ELO/rebase_runtime_model_state.py --manual",
])
@pytest.mark.parametrize("outcome,rc", [("ok", 0), ("kill-clean", 1)])
def test_process_naming_the_script_but_holding_no_lock_is_neither_a_writer_nor_killed(
        make_env, argv0, outcome, rc):
    # Whatever the argv says, a process that holds no lock is not a writer: the rebase
    # runs, and the failure path never kills it (the old regex refused the rebase for
    # some of these forms and SIGKILLed others).
    e = _rebase_env(make_env)
    other = subprocess.Popen(
        [BASH, "-c", f"exec -a '{argv0}' /bin/sleep 30"],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 3.0   # the argv must really be visible, else this proves nothing
        while time.monotonic() < deadline:
            seen = subprocess.run(["ps", "-ww", "-p", str(other.pid), "-o", "command="],
                                  capture_output=True, text=True).stdout
            if "rebase_runtime_model_state" in seen:
                break
            time.sleep(0.05)
        assert "rebase_runtime_model_state" in seen, seen
        r, log = _run_rebase(e, outcome)
        assert r.returncode == rc, r.stdout + r.stderr + log
        assert "уже выполняется" not in log and "посылаю SIGKILL" not in log
        assert e.ops(("SYSTEMCTL",))[:2] == ["SYSTEMCTL stop cyberscore.service",
                                             "SYSTEMCTL start cyberscore.service"]
        assert other.poll() is None, "an unrelated process quoting the command must not be killed"
    finally:
        other.kill()
        other.wait()


def test_lock_holder_that_outlives_the_rebase_is_killed_before_prod_restarts(make_env):
    # The python writer outlives its `timeout` wrapper (rc 124) and would rewrite the
    # state at +3 s. The chain finds it by the lock, reads its pid from the lock file,
    # checks that pid's command names the script, SIGKILLs it, waits for the lock.
    e = _rebase_env(make_env)
    pidfile = e.base / "orphan.pid"
    script = _holder_script(e, RELEVANT_HOLDER)
    delay = 3.0
    t0 = time.monotonic()
    try:
        r, log = _run_rebase(e, "lock-orphan", PRO_CHAIN_REBASE_TIMEOUT_SECONDS="60",
                             STUB_ORPHAN_PIDFILE=str(pidfile), STUB_ORPHAN_DELAY=str(delay),
                             STUB_HOLDER_SCRIPT=str(script), STUB_REAL_SLEEP="1")
        assert r.returncode == 1, r.stdout + r.stderr + log
        pid = int(pidfile.read_text())
        assert _wait_dead(pid), "the lock holder must be dead when the script returns"
        assert _lock_free(e)
        assert "START-WITH-WRITER-ALIVE" not in e.events_lines(), "prod started next to a live writer"
        assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service",
                                         "SYSTEMCTL start cyberscore.service"]
        assert f"ВНИМАНИЕ: после прерывания жив процесс перебазировки ELO (pid {pid}); посылаю SIGKILL" in log
        assert _map_id_check(e) == b""
        # the point the holder would have written at has passed: the file must be intact
        time.sleep(max(0.0, delay + 0.7 - (time.monotonic() - t0)))
        assert (e.prod / "runtime/live_elo_model_state.json").read_text() == \
            "runtime/live_elo_model_state.json-v1\n"
        assert "лимит 60 с); runtime ELO не изменён" in log
    finally:
        _kill_pidfile(pidfile)


def test_lock_holder_that_is_not_the_rebase_script_is_not_killed_and_prod_stays_stopped(make_env):
    # pid-reuse guard: the lock file's pid is alive but its command does not name the
    # rebase script, so it is not ours to kill; the lock never frees, prod stays stopped.
    e = _rebase_env(make_env)
    pidfile = e.base / "orphan.pid"
    script = _holder_script(e, OTHER_HOLDER)
    try:
        r, log = _run_rebase(e, "lock-orphan", PRO_CHAIN_REBASE_TIMEOUT_SECONDS="60",
                             STUB_ORPHAN_PIDFILE=str(pidfile), STUB_ORPHAN_DELAY="30",
                             STUB_HOLDER_SCRIPT=str(script), STUB_REAL_SLEEP="1",
                             PRO_CHAIN_REBASE_WRITER_WAIT_SECONDS="1")
        assert r.returncode == 124, r.stdout + r.stderr + log
        assert _alive(int(pidfile.read_text())), "the guard must not have killed it"
        assert not _lock_free(e)
        assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service"], "prod must stay stopped"
        assert "START-WITH-WRITER-ALIVE" not in e.events_lines()
        assert _map_id_check(e) == b"old-map\n"
        assert "посылаю SIGKILL" not in log and "это не скрипт перебазировки" in log
        assert "ОШИБКА: процесс перебазировки ELO не остановлен (rc=124" in log
        assert "оставлен остановленным" in log
        assert _prod_snapshot(e) == OLD_ARTIFACT
    finally:
        _kill_pidfile(pidfile)


def test_unknown_lock_state_after_a_failed_rebase_keeps_prod_stopped(make_env):
    # The pre-check passes, the rebase dies (rc 137), and now the lock probe cannot
    # answer: without proof that no writer is left, prod must not start.
    e = _rebase_env(make_env)
    _write(e.prod / "venv/bin/python3", PRODPY_REBASE_STUB.replace(
        'if [ "${1:-}" = "-c" ]; then exec "$STUB_REAL_PY" "$@"; fi',
        'if [ "${1:-}" = "-c" ]; then [ -e runtime/.rebase_ran ] && exit 3; exec "$STUB_REAL_PY" "$@"; fi'
    ).replace('  case "${STUB_REBASE:-ok}" in',
              '  : > runtime/.rebase_ran\n  case "${STUB_REBASE:-ok}" in'), exe=True)
    r, log = _run_rebase(e, "kill-clean", PRO_CHAIN_REBASE_WRITER_WAIT_SECONDS="1", STUB_REAL_SLEEP="1")
    assert r.returncode == 137, r.stdout + r.stderr + log
    assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service"], "prod must stay stopped"
    assert _map_id_check(e) == b"old-map\n"
    assert "состояние замка перебазировки неизвестно" in log
    assert "ОШИБКА: процесс перебазировки ELO не остановлен (rc=137" in log
    assert _prod_snapshot(e) == OLD_ARTIFACT


def test_rebase_lock_probe_does_not_create_the_lock_file(make_env):
    # The probe only reads: a normal night leaves runtime/ exactly as the old script
    # did except for the lock file the real rebase CLI creates (the stub CLI does not).
    e = _rebase_env(make_env)
    r, log = _run_rebase(e, "ok")
    assert r.returncode == 0, r.stdout + r.stderr + log
    assert not (e.prod / LOCK_REL).exists()


# ------------------------------------------------------------------ round 6 hardening

@pytest.mark.parametrize("name", [
    "rebase_runtime_model_state.py.bak",           # exact token, not substring
    "check_rebase_runtime_model_state.py",          # suffix of the file NAME, not of a path part
])
def test_lock_holder_whose_argv_only_contains_the_script_name_is_not_killed(make_env, name):
    e = _rebase_env(make_env)
    pidfile = e.base / "orphan.pid"
    script = _holder_script(e, name)
    try:
        r, log = _run_rebase(e, "lock-orphan", PRO_CHAIN_REBASE_TIMEOUT_SECONDS="60",
                             STUB_ORPHAN_PIDFILE=str(pidfile), STUB_ORPHAN_DELAY="30",
                             STUB_HOLDER_SCRIPT=str(script), STUB_REAL_SLEEP="1",
                             PRO_CHAIN_REBASE_WRITER_WAIT_SECONDS="1")
        assert r.returncode == 124, r.stdout + r.stderr + log
        assert _alive(int(pidfile.read_text())), "a lookalike holder must not be SIGKILLed"
        assert "посылаю SIGKILL" not in log and "это не скрипт перебазировки" in log
        assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service"], "prod must stay stopped"
        assert _map_id_check(e) == b"old-map\n"
    finally:
        _kill_pidfile(pidfile)


def _orphan_with_proc_locks(make_env, mode: str):
    """The orphan holds the lock and carries the right script name; /proc/locks says `mode`."""
    e = _rebase_env(make_env)
    pidfile = e.base / "orphan.pid"
    script = _holder_script(e, RELEVANT_HOLDER)
    proc_locks = e.base / "proc_locks"
    try:
        r, log = _run_rebase(e, "lock-orphan", PRO_CHAIN_REBASE_TIMEOUT_SECONDS="60",
                             STUB_ORPHAN_PIDFILE=str(pidfile), STUB_ORPHAN_DELAY="30",
                             STUB_HOLDER_SCRIPT=str(script), STUB_REAL_SLEEP="1",
                             PRO_CHAIN_REBASE_WRITER_WAIT_SECONDS="1",
                             PRO_CHAIN_PROC_LOCKS=str(proc_locks),
                             STUB_PROC_LOCKS_FILE=str(proc_locks),
                             STUB_PROC_LOCKS_WRITER=PROC_LOCKS_WRITER,
                             STUB_PROC_LOCKS_MODE=mode)
        pid = int(pidfile.read_text())
        alive = _alive(pid)
        return e, r, log, pid, alive
    finally:
        _kill_pidfile(pidfile)


def test_proc_locks_confirming_the_holder_allows_the_kill(make_env):
    # The Linux branch (PRO_CHAIN_PROC_LOCKS overrides /proc/locks; macOS has none):
    # a FLOCK line for this pid and this lock file's inode is what makes it a writer.
    e, r, log, pid, alive = _orphan_with_proc_locks(make_env, "match")
    assert r.returncode == 1, r.stdout + r.stderr + log
    assert not alive and f"жив процесс перебазировки ELO (pid {pid}); посылаю SIGKILL" in log
    assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service",
                                     "SYSTEMCTL start cyberscore.service"]


@pytest.mark.parametrize("mode", ["other-pid", "other-inode", "other-dev", "waiter", "posix", "empty"])
def test_proc_locks_not_listing_the_pid_for_the_lock_inode_means_no_kill(make_env, mode):
    # Fail-safe: /proc/locks exists but does not show this pid holding this inode as an
    # FLOCK (a reused pid, another file, a blocked waiter, a POSIX lock): do not kill,
    # prod stays stopped.
    e, r, log, pid, alive = _orphan_with_proc_locks(make_env, mode)
    assert r.returncode == 124, r.stdout + r.stderr + log
    assert alive, "the pid was not proven to hold the lock; it must not be killed"
    assert "посылаю SIGKILL" not in log and "не показывает, что он держит FLOCK" in log
    assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service"], "prod must stay stopped"
    assert _map_id_check(e) == b"old-map\n"
    assert "оставлен остановленным" in log


def test_cli_rc3_names_the_lock_file_pid_and_keeps_the_restore_behaviour(make_env):
    # Another rebase held the lock when this CLI started: rc 3. Nothing was written, so
    # prod is restored as before, but the message names the cause and the pid instead of
    # the generic "interrupted" line.
    e = _rebase_env(make_env)
    r, log = _run_rebase(e, "lock-refused")
    assert r.returncode == 1, r.stdout + r.stderr + log
    assert "ОШИБКА: перебазировка ELO уже выполняется другим процессом (rc=3, pid 4242 в runtime/live_elo_rebase.lock)" in log
    assert "перебазировка ELO прервана" not in log
    assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service",
                                     "SYSTEMCTL start cyberscore.service"]
    assert _map_id_check(e) == b""


# ------------------------------------------------------------------ round 7 hardening
# G1 pre-flock orphan of the chain's OWN child, G2 (ELO/tests) symlinked outputs, G3 the
# /proc/locks binding compares the device too, L3 rc 3 message names the pid.

# `timeout` that dies (rc 137, as after SIGKILL) while its child is still starting: the child
# lives on as an orphan that holds NO lock yet.
ORPHANING_TIMEOUT_STUB = r"""#!/bin/bash
echo "TIMEOUT $*" >> "$STUB_EVENTS"
shift 3
"$@" </dev/null >/dev/null 2>&1 &
# at most 3 s, well under the writer's 8 s "import" delay (test_pre_flock_orphan...): on a script
# without the pid wrapper (round 6) the orphan is still lock-free when the chain decides, which is
# the real hazard; 3 s (not 1 s) leaves room for a slow wrapper on a loaded host (Opus r7b P3)
for _ in $(seq 1 60); do
  p="$(cat runtime/live_elo_rebase.child.pid 2>/dev/null)"
  [ -n "$p" ] && [ "$p" != 0000000000 ] && break
  /bin/sleep 0.05
done
exit 137
"""
# `timeout` that dies before it ever started a child: the pid file stays empty
DEAD_TIMEOUT_STUB = '#!/bin/bash\necho "TIMEOUT $*" >> "$STUB_EVENTS"\nexit 137\n'
# `timeout` whose recorded child pid belongs to an unrelated live process (pid reuse)
REUSED_PID_TIMEOUT_STUB = (
    '#!/bin/bash\necho "TIMEOUT $*" >> "$STUB_EVENTS"\n'
    "printf '%s\\n' \"$STUB_UNRELATED_PID\" > runtime/live_elo_rebase.child.pid\nexit 137\n")
# a late rebase writer: sleeps (the "import" phase, no lock), THEN takes the flock and writes
PRELOCK_HOLDER_PY = """import fcntl, os, sys, time
if os.environ.get("STUB_HOLDER_SELFPID"):
    # test-only record of the orphan, independent of the script's own pid file, so the
    # systemctl stub can tell "prod started next to a live writer" on any script version
    with open(os.environ["STUB_HOLDER_SELFPID"], "w") as fh:
        fh.write(str(os.getpid()))
time.sleep(float(sys.argv[2]))
fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o644)
fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
os.ftruncate(fd, 0)
os.write(fd, (str(os.getpid()) + chr(10)).encode())
with open(sys.argv[3], "w") as fh:
    fh.write("late-write")
time.sleep(600)
"""


def _prelock_orphan(make_env, timeout_stub: str, name: str = RELEVANT_HOLDER, delay: float = 8.0, **extra):
    e = _rebase_env(make_env)
    _write(e.stubs / "timeout", timeout_stub, exe=True)
    script = e.base / "holders" / name
    _write(script, PRELOCK_HOLDER_PY)
    child_pid = e.prod / CHILD_PID_REL
    selfpid = e.base / "prelock_holder.selfpid"
    t0 = time.monotonic()
    try:
        r, log = _run_rebase(e, "pre-lock-orphan", PRO_CHAIN_REBASE_TIMEOUT_SECONDS="60",
                             STUB_ORPHAN_PIDFILE=str(selfpid), STUB_HOLDER_SELFPID=str(selfpid),
                             STUB_ORPHAN_DELAY=str(delay),
                             STUB_HOLDER_SCRIPT=str(script), STUB_REAL_SLEEP="1", **extra)
        return e, r, log, child_pid, t0
    except BaseException:
        _kill_prelock(e)
        raise


def _kill_prelock(e) -> None:
    for f in (e.prod / CHILD_PID_REL, e.base / "prelock_holder.selfpid"):
        try:
            _kill_pidfile(f)
        except (OSError, ValueError):
            pass


def test_pre_flock_orphan_of_our_own_child_is_killed_before_prod_restarts(make_env):
    # The timeout wrapper dies (137) while the python child is still importing: it holds no
    # flock, so the lock probe says "free" and, until round 7, prod was restored while the
    # orphan went on to rewrite the state. The pid recorded by the wrapper before python
    # started finds it; its command names the script, so it is SIGKILLed and awaited.
    delay = 8.0
    e, r, log, child_pid, t0 = _prelock_orphan(make_env, ORPHANING_TIMEOUT_STUB, delay=delay)
    try:
        assert r.returncode == 1, r.stdout + r.stderr + log
        assert "START-WITH-WRITER-ALIVE" not in e.events_lines(), "prod started next to a live writer"
        pid = int(child_pid.read_text())
        assert _wait_dead(pid), "the pre-lock orphan must be dead when the script returns"
        assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service",
                                         "SYSTEMCTL start cyberscore.service"]
        assert f"жив дочерний процесс перебазировки ELO (pid {pid}); посылаю SIGKILL" in log
        assert _map_id_check(e) == b""
        time.sleep(max(0.0, delay + 0.7 - (time.monotonic() - t0)))   # the moment it would have written
        assert (e.prod / "runtime/live_elo_model_state.json").read_text() == \
            "runtime/live_elo_model_state.json-v1\n"
        assert not (e.prod / LOCK_REL).exists() or _lock_free(e)
    finally:
        _kill_prelock(e)


def test_unrecorded_child_pid_after_a_failed_rebase_keeps_prod_stopped(make_env):
    # `timeout` died before any child pid was recorded (the file was emptied before the run):
    # an orphan cannot be ruled out, so prod stays stopped (fail-safe).
    e = _rebase_env(make_env)
    _write(e.stubs / "timeout", DEAD_TIMEOUT_STUB, exe=True)
    r, log = _run_rebase(e, "ok", STUB_REAL_SLEEP="1")
    assert r.returncode == 137, r.stdout + r.stderr + log
    assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service"], "prod must stay stopped"
    assert _map_id_check(e) == b"old-map\n"
    assert "pid дочернего процесса перебазировки не записан" in log
    assert "ОШИБКА: процесс перебазировки ELO не остановлен (rc=137" in log and "оставлен остановленным" in log
    assert (e.prod / CHILD_PID_REL).read_bytes() == CHILD_PID_PLACEHOLDER, "reset before the run, never deleted"


def test_child_pid_file_is_overwritten_each_run_and_never_deleted(make_env):
    e = _rebase_env(make_env)
    _write(e.prod / CHILD_PID_REL, "stale-pid-from-last-night\n")
    r, log = _run_rebase(e, "ok")
    assert r.returncode == 0, r.stdout + r.stderr + log
    text = (e.prod / CHILD_PID_REL).read_text().strip()
    assert text.isdigit() and text != "stale-pid-from-last-night", text


def test_recorded_pid_of_an_unrelated_live_process_is_not_killed(make_env):
    # pid reuse: the recorded child is gone and its number now belongs to someone else
    e = _rebase_env(make_env)
    _write(e.stubs / "timeout", REUSED_PID_TIMEOUT_STUB, exe=True)
    other = subprocess.Popen(["/bin/sleep", "30"], stdin=subprocess.DEVNULL,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        r, log = _run_rebase(e, "ok", STUB_UNRELATED_PID=str(other.pid), STUB_REAL_SLEEP="1")
        assert r.returncode == 1, r.stdout + r.stderr + log
        assert other.poll() is None, "an unrelated process must not be killed"
        assert "посылаю SIGKILL" not in log
        assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service",
                                         "SYSTEMCTL start cyberscore.service"]
    finally:
        other.kill()
        other.wait()


def test_live_recorded_child_that_does_not_name_the_script_is_not_killed(make_env):
    # the orphan stand-in is alive but its argv does not name the script: not ours to kill
    e, r, log, child_pid, _ = _prelock_orphan(make_env, ORPHANING_TIMEOUT_STUB,
                                              name=OTHER_HOLDER, delay=30.0)
    try:
        assert r.returncode == 1, r.stdout + r.stderr + log
        assert _alive(int(child_pid.read_text())), "must not be killed"
        assert "посылаю SIGKILL" not in log
    finally:
        _kill_prelock(e)


def _remote_heredoc() -> str:
    text = (REPO / REBUILD_REL).read_text(encoding="utf-8")
    m = re.search(r"<<'ELO_REBASE_REMOTE'\n(.*?)\nELO_REBASE_REMOTE\n", text, re.S)
    assert m, "step-8 heredoc not found"
    return m.group(1)


def test_step8_heredoc_is_syntactically_valid_and_keeps_its_dollars_literal():
    body = _remote_heredoc()
    assert subprocess.run([BASH, "-n"], input=body, text=True, capture_output=True).returncode == 0
    # the heredoc delimiter is quoted: `$$` and `$1` reach the remote bash untouched
    assert f"bash -c '{PID_WRAPPER}' _ \"$child_pid_file\"" in re.sub(r"\s*\\\n\s*", " ", body)
    assert "<<'ELO_REBASE_REMOTE'" in (REPO / REBUILD_REL).read_text(encoding="utf-8")


def test_pid_wrapper_records_its_own_pid_and_passes_the_exit_code_through(tmp_path):
    pidfile, seen = tmp_path / "child.pid", tmp_path / "seen"
    for rc in (0, 1, 3, 124):
        res = subprocess.run(
            [BASH, "-c", PID_WRAPPER, "_", str(pidfile), BASH, "-c", f'echo $$ > "{seen}"; exit {rc}'],
            capture_output=True, text=True)
        assert res.returncode == rc, res
        assert int(pidfile.read_text()) == int(seen.read_text()), "exec keeps the pid"
    seen.unlink()
    res = subprocess.run([BASH, "-c", PID_WRAPPER, "_", str(tmp_path / "no_dir" / "x"), BASH, "-c", f'echo ran > "{seen}"'],
                         capture_output=True, text=True)
    assert res.returncode == 125 and not seen.exists(), "an unrecordable pid means the CLI is not started"


def test_pid_wrapper_overwrites_the_placeholder_in_place(tmp_path):
    # astra r7 P1: the pid is written AFTER `systemctl stop`; on a full disk (ENOSPC) a write that
    # needs a new block would fail (125) and, with no pid on record, keep prod stopped although
    # python never ran. The placeholder is written BEFORE the stop and the wrapper overwrites the
    # same 11 bytes without truncation (`1<>`), so the post-stop write needs no allocation.
    pidfile, seen = tmp_path / "child.pid", tmp_path / "seen"
    pidfile.write_bytes(CHILD_PID_PLACEHOLDER)
    ino = pidfile.stat().st_ino
    res = subprocess.run(
        [BASH, "-c", PID_WRAPPER, "_", str(pidfile), BASH, "-c", f'echo $$ > "{seen}"'],
        capture_output=True, text=True)
    assert res.returncode == 0, res
    raw = pidfile.read_bytes()
    assert len(raw) == len(CHILD_PID_PLACEHOLDER) and raw.endswith(b"\n"), raw
    assert pidfile.stat().st_ino == ino, "overwritten in place, not recreated"
    assert int(raw) == int(seen.read_text()) and raw != CHILD_PID_PLACEHOLDER


@pytest.mark.parametrize("content", [CHILD_PID_PLACEHOLDER, b"0\n", b"1\n", b"-1\n", b""])
def test_kill_pidfile_never_signals_the_process_group_or_init(tmp_path, monkeypatch, content):
    # astra r7b P1: the placeholder parses as pid 0, and os.kill(0, SIGKILL) kills the whole
    # process group of the test run; pid 1 is init, -1 is every process we may signal
    calls = []
    monkeypatch.setattr(os, "kill", lambda pid, sig: calls.append((pid, sig)))
    f = tmp_path / "child.pid"
    f.write_bytes(content)
    _kill_pidfile(f)
    assert calls == []
    assert _alive(0) is False and calls == []


@pytest.mark.parametrize("major,minor", [(0xFD, 0x2), (0x103, 0x1234), (0x12345, 0x12345678), (0, 0)])
def test_lock_file_devino_matches_glibc_major_minor(major, minor):
    # glibc gnu_dev_major/minor: major = (dev>>8 & 0xfff) | (dev>>32 & 0xfffff000),
    # minor = (dev & 0xff) | (dev>>12 & 0xffffff00). Unbounded ~0xfff/~0xff masks leak major
    # bits into minor (astra r7 P3: 0x12345/0x12345678 decoded as minor 0x1212345678).
    dev = ((major & 0xFFF) << 8) | (minor & 0xFF) | ((minor & ~0xFF) << 12) | ((major & ~0xFFF) << 32)
    text = (REPO / REBUILD_REL).read_text(encoding="utf-8")
    fn = re.search(r"^lock_file_devino\(\) \{\n.*?^\}\n", text, re.S | re.M).group(0)
    prog = (f'stat() {{ if [ "$1" = -c ]; then echo "{dev}:77"; return 0; fi; return 1; }}\n'
            f"lock_file=x\n{fn}lock_file_devino\n")
    out = subprocess.run([BASH, "-c", prog], capture_output=True, text=True)
    assert out.returncode == 0, out
    assert out.stdout.strip() == f"{major:x}:{minor:x}:77"


@pytest.mark.parametrize("mode", ["local", "remote"])
def test_wrapper_text_reaches_timeout_unexpanded_in_both_modes(make_env, mode):
    e = _rebase_env(make_env, mode=mode)
    r, log = _run_rebase(e, "ok")
    assert r.returncode == 0, r.stdout + r.stderr + log
    line = next(l for l in e.events_lines() if l.startswith("TIMEOUT -k 60 1800 bash -c"))
    assert PID_WRAPPER in line and "_ runtime/live_elo_rebase.child.pid venv/bin/python3 ELO/rebase" in line


# --- L3: the integrity-unconfirmed message names the rc 3 holder as the other paths do
def test_rc3_with_changed_files_names_the_lock_holder_in_the_error(make_env):
    e = _rebase_env(make_env)
    r, log = _run_rebase(e, "lock-refused-touched")
    assert r.returncode == 3, r.stdout + r.stderr + log
    assert e.ops(("SYSTEMCTL",)) == ["SYSTEMCTL stop cyberscore.service"], "prod must stay stopped"
    assert "ОШИБКА: целостность runtime ELO не подтверждена (rc=3" in log
    assert "pid 4242 в runtime/live_elo_rebase.lock" in log


# --- G3: GNU stat reports st_dev as ONE decimal number; /proc/locks prints hex major:minor
GNU_STAT_STUB = r"""#!/bin/bash
# GNU `stat -c '%d:%i'` for the lock file; a Linux-encoded st_dev (major 0x103, minor 0x1234)
if [ "${1:-}" = -c ] && [ "${2:-}" = '%d:%i' ]; then
  echo "$STUB_GNU_DEV:$("$STUB_REAL_PY" -c 'import os,sys;print(os.stat(sys.argv[1]).st_ino)' "$3")"; exit 0
fi
exec /usr/bin/stat "$@"
"""


@pytest.mark.parametrize("claimed,killed", [("103:1234", True), ("103:1235", False), ("104:1234", False)])
def test_gnu_stat_device_is_decoded_the_way_proc_locks_prints_it(make_env, claimed, killed):
    major, minor = 0x103, 0x1234
    dev = ((major & 0xfff) << 8) | (minor & 0xff) | ((minor & ~0xff) << 12) | ((major & ~0xfff) << 32)
    assert dev == 0x1210334
    e = _rebase_env(make_env)
    _write(e.stubs / "stat", GNU_STAT_STUB, exe=True)
    pidfile = e.base / "orphan.pid"
    script = _holder_script(e, RELEVANT_HOLDER)
    proc_locks = e.base / "proc_locks"
    try:
        r, log = _run_rebase(e, "lock-orphan", PRO_CHAIN_REBASE_TIMEOUT_SECONDS="60",
                             STUB_ORPHAN_PIDFILE=str(pidfile), STUB_ORPHAN_DELAY="30",
                             STUB_HOLDER_SCRIPT=str(script), STUB_REAL_SLEEP="1",
                             PRO_CHAIN_REBASE_WRITER_WAIT_SECONDS="1",
                             PRO_CHAIN_PROC_LOCKS=str(proc_locks), STUB_PROC_LOCKS_FILE=str(proc_locks),
                             STUB_PROC_LOCKS_WRITER=PROC_LOCKS_WRITER, STUB_PROC_LOCKS_MODE="match",
                             STUB_PROC_LOCKS_DEV=claimed, STUB_GNU_DEV=str(dev))
        alive = _alive(int(pidfile.read_text()))
        assert (not alive) == killed, (r.returncode, log)
        assert r.returncode == (1 if killed else 124)
    finally:
        _kill_pidfile(pidfile)
