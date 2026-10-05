"""Shell harness for the serv1 pro-corpus chain scripts.

Covers scripts/run/pro_nightly_chain.sh, scripts/run/topup_pro_corpus.sh (host-agnostic via
scripts/run/lib_pro_chain.sh) and scripts/ops/setup_pro_chain_serv1.sh. Real `git` in tmp dirs
(fake PROD repo + `git worktree` BUILD tree), stubbed topup/rebuild/notify. Nothing here touches
the network, ssh, the real corpus or any path outside pytest's tmp_path.

Run:
    PYTHONPYCACHEPREFIX=/private/tmp/hw-nightly \
      /Users/alex/Documents/ingame/venv_catboost/bin/python3 -m pytest \
      base/tests/test_pro_chain_nightly_shell.py -q -p no:cacheprovider

Red-before-green for topup: the same topup tests against the ORIGINAL script
(sed-transformed to a tmp tree, never executed untransformed):
    TOPUP_UNDER_TEST=orig ... pytest ... -k topup

Platform gap (macOS has no flock/ionice/timeout; nice exists): PATH stubs are put first for
nice/ionice/timeout (always, they record argv so the argument shape is asserted; the real
binaries are bypassed), and for flock only when missing (python fcntl on the inherited fd 9,
same open-file-description semantics as flock(1)). On Linux the real flock is used.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import fcntl
import os
import pty
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
RUN_DIR = REPO / "scripts" / "run"
OPS_DIR = REPO / "scripts" / "ops"
BASH = "/bin/bash"
ORIG_REF = os.environ.get("TOPUP_ORIG_REF", "f5353230")
UNDER_TEST = os.environ.get("TOPUP_UNDER_TEST", "new")  # "new" | "orig"
GIT_ID = ["-c", "user.name=t", "-c", "user.email=t@t"]


def git(cwd: Path, *args: str) -> str:
    out = subprocess.run(["git", *GIT_ID, *args], cwd=cwd, check=True,
                         capture_output=True, text=True)
    return out.stdout.strip()


def write(path: Path, text: str, mode: int = 0o755) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    path.chmod(mode)
    return path


def clean_env(tmp: Path, **extra: str) -> dict:
    env = {
        "PATH": os.environ["PATH"],
        "HOME": str(tmp / "home"),
        "LC_ALL": "en_US.UTF-8",
        # fake PROD_ROOT in tmp; the lib refuses anything but /root/main outside tests
        "PRO_CHAIN_ALLOW_FAKE_PROD": "1",
    }
    if os.environ.get("PYTHONPYCACHEPREFIX"):
        env["PYTHONPYCACHEPREFIX"] = os.environ["PYTHONPYCACHEPREFIX"]
    env.update(extra)
    return env


# --------------------------------------------------------------------------- PATH stubs

@pytest.fixture
def stubbin(tmp_path: Path) -> Path:
    d = tmp_path / "stubbin"
    d.mkdir()
    write(d / "nice", '#!/bin/bash\necho "nice $1 $2" >> "${STUB_CALLS:-/dev/null}"\nshift 2\nexec "$@"\n')
    write(d / "ionice", '#!/bin/bash\necho "ionice $1 $2" >> "${STUB_CALLS:-/dev/null}"\nshift 2\nexec "$@"\n')
    # timeout [-k N] DURATION cmd...; STUB_TIMEOUT_FIRE=<substring> simulates a timeout (rc 124)
    write(d / "timeout", '''#!/bin/bash
args=("$@")
if [ "$1" = -k ]; then k="$2"; shift 2; else k=none; fi
dur="$1"; shift
echo "timeout k=$k dur=$dur" >> "${STUB_CALLS:-/dev/null}"
if [ -n "${STUB_TIMEOUT_FIRE:-}" ] && [[ "$*" == *"$STUB_TIMEOUT_FIRE"* ]]; then exit 124; fi
exec "$@"
''')
    if shutil.which("flock") is None:
        write(d / "flock", f'''#!/bin/bash
[ "$1" = -n ] && shift
exec {sys.executable} -c 'import fcntl,sys
try: fcntl.flock(int(sys.argv[1]), fcntl.LOCK_EX | fcntl.LOCK_NB)
except OSError: sys.exit(1)' "$1"
''')
    return d


# --------------------------------------------------------------------------- nightly chain

STUB_STEP = '''#!/bin/bash
echo "{name} v={ver} mode=${{PRO_CHAIN_MODE-unset}} prod=${{PROD_ROOT-unset}} gzip=${{PRO_CORPUS_GZIP-unset}} shadow=${{PRO_CHAIN_SHADOW-unset}} cwdtree=$(cd "$(dirname "$0")/../.." && pwd -P)" >> "$STUB_LOG"
exit ${{{rcvar}:-0}}
'''
STUB_NOTIFY = '''import sys, os
text = sys.stdin.read()
with open(os.environ["STUB_NOTIFY"], "a") as fh:
    fh.write(text.strip() + "\\n---\\n")
'''


class Chain:
    def __init__(self, tmp: Path, stubbin: Path):
        self.tmp = tmp
        self.stubbin = stubbin
        self.prod = tmp / "prod"
        self.build = tmp / "build"
        self.log = tmp / "stub.log"
        self.notify = tmp / "notify.log"
        self.calls = tmp / "calls.log"
        self.prod.mkdir()
        git(self.prod, "init", "-q")
        self.commit(ver="1")
        git(self.prod, "worktree", "add", "-q", "--detach", str(self.build), "HEAD")
        write(self.prod / "runtime/sourcetv_matches.json", "{}", 0o644)
        write(self.prod / "venv/bin/python3", f'#!/bin/bash\nexec {sys.executable} "$@"\n')

    def commit(self, ver: str) -> str:
        run = self.prod / "scripts" / "run"
        write(run / "pro_nightly_chain.sh", (RUN_DIR / "pro_nightly_chain.sh").read_text())
        write(run / "lib_pro_chain.sh", (RUN_DIR / "lib_pro_chain.sh").read_text(), 0o644)
        write(run / "topup_pro_corpus.sh",
              STUB_STEP.format(name="topup", ver=ver, rcvar="STUB_TOPUP_RC"))
        write(run / "rebuild_prematch_snapshot.sh",
              STUB_STEP.format(name="rebuild", ver=ver, rcvar="STUB_REBUILD_RC"))
        write(self.prod / "scripts" / "ops" / "notify_admin.py", STUB_NOTIFY, 0o644)
        write(self.prod / ".gitignore", "runtime/\n", 0o644)
        git(self.prod, "add", "-A")
        git(self.prod, "commit", "-q", "-m", f"v{ver}")
        return git(self.prod, "rev-parse", "HEAD")

    def run(self, prod=None, build=None, **extra: str) -> subprocess.CompletedProcess:
        prod = prod or self.prod
        env = clean_env(
            self.tmp,
            PATH=f"{self.stubbin}{os.pathsep}{os.environ['PATH']}",
            PROD_ROOT=str(prod),
            PRO_CHAIN_BUILD_ROOT=str(build or self.build),
            PY=sys.executable,
            STUB_LOG=str(self.log), STUB_NOTIFY=str(self.notify), STUB_CALLS=str(self.calls),
            **extra,
        )
        script = Path(prod) / "scripts" / "run" / "pro_nightly_chain.sh"
        return subprocess.run([BASH, str(script)], env=env, capture_output=True,
                              text=True, timeout=60)

    def steps(self) -> list:
        return self.log.read_text().splitlines() if self.log.exists() else []

    def notified(self) -> str:
        return self.notify.read_text() if self.notify.exists() else ""

    def chain_log(self) -> str:
        return "\n".join(p.read_text() for p in (self.build / "runtime").glob("pro_nightly_chain_*.log"))


@pytest.fixture
def chain(tmp_path: Path, stubbin: Path) -> Chain:
    return Chain(tmp_path, stubbin)


def test_order_topup_then_rebuild_and_env(chain: Chain):
    r = chain.run()
    assert r.returncode == 0, (r.stdout, r.stderr, chain.chain_log())
    steps = chain.steps()
    assert [s.split()[0] for s in steps] == ["topup", "rebuild"]
    for s in steps:
        assert "mode=local" in s and f"prod={chain.prod.resolve()}" in s
        assert "gzip=unset" in s and "shadow=0" in s
        assert f"cwdtree={chain.build.resolve()}" in s  # steps run from the BUILD tree
    calls = chain.calls.read_text()
    assert "nice -n 10" in calls and "ionice -c 3" in calls
    # topup is time-boxed; the rebuild is NOT (a group kill inside the ELO rebase
    # after `systemctl stop` would leave prod stopped)
    assert "timeout k=120 dur=3h" in calls and calls.count("timeout ") == 1
    assert chain.notified() == ""  # all green: no noise


def test_rebuild_runs_when_topup_fails_and_rc_is_rebuild_rc(chain: Chain):
    r = chain.run(STUB_TOPUP_RC="3", STUB_REBUILD_RC="0")
    assert r.returncode == 0
    assert [s.split()[0] for s in chain.steps()] == ["topup", "rebuild"]
    assert "rc=3" in chain.notified()
    chain.log.unlink()
    r = chain.run(STUB_TOPUP_RC="0", STUB_REBUILD_RC="5")
    assert r.returncode == 5
    assert "rc=5" in chain.notified()


def test_timeout_notifies_and_rebuild_still_runs(chain: Chain):
    r = chain.run(STUB_TIMEOUT_FIRE="topup_pro_corpus.sh")
    assert r.returncode == 0
    assert [s.split()[0] for s in chain.steps()] == ["rebuild"]  # topup never ran (timed out)
    assert "таймаут 3h" in chain.notified()


def test_lock_held_exits_zero_without_running(chain: Chain):
    (chain.build / "runtime").mkdir(exist_ok=True)
    fd = os.open(chain.build / "runtime" / "pro_nightly_chain.lock", os.O_WRONLY | os.O_CREAT)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        r = chain.run()
    finally:
        os.close(fd)
    assert r.returncode == 0
    assert chain.steps() == []
    assert "другой прогон" in r.stdout
    # and once the lock is released the same invocation runs
    assert chain.run().returncode == 0
    assert len(chain.steps()) == 2


def test_dirty_build_tree_refuses_with_exit_1(chain: Chain):
    tracked = chain.build / "scripts" / "ops" / "notify_admin.py"
    tracked.write_text(tracked.read_text() + "# local edit\n")
    sha_before = git(chain.build, "rev-parse", "HEAD")
    r = chain.run()
    assert r.returncode == 1, (r.stdout, r.stderr)
    assert chain.steps() == []
    assert "грязное" in chain.notified()
    assert git(chain.build, "rev-parse", "HEAD") == sha_before


def test_allowlisted_chain_output_is_restored_other_tracked_change_refuses(chain: Chain):
    out = "data/team_org_aliases.json"
    write(chain.prod / out, '{"orig": 1}\n', 0o644)
    git(chain.prod, "add", "-A")
    git(chain.prod, "commit", "-q", "-m", "aliases")
    (chain.build / "data").mkdir(exist_ok=True)
    git(chain.build, "checkout", "-q", "--detach", git(chain.prod, "rev-parse", "HEAD"))
    (chain.build / out).write_text('{"rebuilt": 2}\n')
    r = chain.run()
    assert r.returncode == 0, (r.stdout, r.stderr, chain.chain_log())
    assert [s.split()[0] for s in chain.steps()] == ["topup", "rebuild"]
    assert (chain.build / out).read_text() == '{"orig": 1}\n'
    assert f"восстановлен выход цепочки из коммита: {out}" in chain.chain_log()
    # allowlisted AND non-allowlisted modified together: still exit 1, nothing runs
    chain.log.unlink()
    (chain.build / out).write_text('{"rebuilt": 3}\n')
    other = chain.build / "scripts" / "ops" / "notify_admin.py"
    other.write_text(other.read_text() + "# local edit\n")
    r = chain.run()
    assert r.returncode == 1 and chain.steps() == []
    assert "грязное" in chain.notified() and "notify_admin.py" in chain.notified()


def test_untracked_files_in_build_tree_do_not_block(chain: Chain):
    (chain.build / "data").mkdir()
    (chain.build / "data" / "corpus.bin").write_text("x")
    assert chain.run().returncode == 0
    assert len(chain.steps()) == 2


def test_build_tree_follows_prod_head(chain: Chain):
    old = git(chain.build, "rev-parse", "HEAD")
    new = chain.commit(ver="2")
    assert old != new and git(chain.build, "rev-parse", "HEAD") == old
    r = chain.run()
    assert r.returncode == 0, (r.stdout, r.stderr, chain.chain_log())
    assert git(chain.build, "rev-parse", "HEAD") == new
    assert all(" v=2 " in s for s in chain.steps())  # code of the NEW commit ran
    assert f"{old} -> {new}" in chain.chain_log()
    assert git(chain.prod, "rev-parse", "HEAD") == new  # prod untouched


def test_build_equals_prod_exits_2(chain: Chain):
    r = chain.run(build=chain.prod)
    assert r.returncode == 2
    assert "совпадает" in r.stderr
    assert chain.steps() == []
    # symlink to prod is the same tree too
    link = chain.tmp / "link_to_prod"
    link.symlink_to(chain.prod)
    r = chain.run(build=link)
    assert r.returncode == 2 and chain.steps() == []


def test_misconfiguration_exits_2(chain: Chain, tmp_path: Path):
    # prod is not a git checkout (script lives in a plain dir)
    plain = tmp_path / "plain"
    write(plain / "scripts" / "run" / "pro_nightly_chain.sh", (RUN_DIR / "pro_nightly_chain.sh").read_text())
    assert chain.run(prod=plain).returncode == 2
    # build tree is a plain dir, not a worktree
    notwt = tmp_path / "notwt"
    notwt.mkdir()
    assert chain.run(build=notwt).returncode == 2
    # build tree is a worktree of a DIFFERENT repo
    other = tmp_path / "other"
    other.mkdir()
    git(other, "init", "-q")
    write(other / "f", "x", 0o644)
    git(other, "add", "-A")
    git(other, "commit", "-q", "-m", "x")
    owt = tmp_path / "other_wt"
    git(other, "worktree", "add", "-q", "--detach", str(owt), "HEAD")
    r = chain.run(build=owt)
    assert r.returncode == 2 and "не worktree репозитория" in r.stderr
    assert chain.steps() == []


def test_leaked_gzip_flag_is_not_inherited_and_shadow_passes_through(chain: Chain):
    r = chain.run(PRO_CORPUS_GZIP="1", PRO_CHAIN_SHADOW="1")
    assert r.returncode == 0
    for s in chain.steps():
        assert "gzip=unset" in s and "shadow=1" in s


# --------------------------------------------------------------------------- topup_pro_corpus.sh

STUB_PY = '''#!/bin/bash
case "$1" in
  *topup_pro_corpus.py)
    echo "topup gzip=${PRO_CORPUS_GZIP-unset} cwd=$(pwd -P)" >> "$STUB_LOG"
    echo "файлов в корпусе: 123"; echo "свежайшая карта: 2026-10-03"
    exit ${STUB_TOPUP_RC:-0};;
  *notify_admin.py)
    echo "notify gzip=${PRO_CORPUS_GZIP-unset}" >> "$STUB_LOG"
    cat >> "$STUB_NOTIFY"; echo "---" >> "$STUB_NOTIFY";;
esac
'''


class Topup:
    def __init__(self, tmp: Path):
        self.tmp = tmp
        self.tree = (tmp / "tree").resolve()
        self.prod = tmp / "prod"
        self.prod.mkdir()
        (self.prod / ".git").mkdir()
        self.stubpy = write(tmp / "stubpy", STUB_PY)
        self.log = tmp / "stub.log"
        self.notify = tmp / "notify.log"
        (self.tree / "runtime").mkdir(parents=True)
        write(self.tree / "scripts" / "ops" / "notify_admin.py", "# stub, never executed\n", 0o644)
        if UNDER_TEST == "orig":
            text = subprocess.run(["git", "show", f"{ORIG_REF}:scripts/run/topup_pro_corpus.sh"],
                                  cwd=REPO, check=True, capture_output=True, text=True).stdout
            # transform the hardcoded paths so the original can never touch the real checkout
            text = text.replace("cd /Users/alex/Documents/ingame", f"cd {self.tree}")
            text = text.replace("PY=venv_catboost/bin/python3", f"PY={self.stubpy}")
            assert "/Users/alex" not in text and str(self.tree) in text and str(self.stubpy) in text
            self.script = write(self.tree / "scripts" / "run" / "topup_pro_corpus.sh", text)
        else:
            self.script = write(self.tree / "scripts" / "run" / "topup_pro_corpus.sh",
                                (RUN_DIR / "topup_pro_corpus.sh").read_text())
            write(self.tree / "scripts" / "run" / "lib_pro_chain.sh",
                  (RUN_DIR / "lib_pro_chain.sh").read_text(), 0o644)

    def env(self, **extra: str) -> dict:
        base = dict(PY=str(self.stubpy), PROD_ROOT=str(self.prod),
                    STUB_LOG=str(self.log), STUB_NOTIFY=str(self.notify))
        base.update(extra)
        return clean_env(self.tmp, **base)

    def run(self, **extra: str) -> subprocess.CompletedProcess:
        return subprocess.run([BASH, str(self.script)], env=self.env(**extra),
                              capture_output=True, text=True, timeout=60)

    def calls(self) -> list:
        return self.log.read_text().splitlines() if self.log.exists() else []

    def notified(self) -> str:
        return self.notify.read_text() if self.notify.exists() else ""

    def topup_log(self) -> str:
        return "\n".join(p.read_text() for p in (self.tree / "runtime").glob("pro_topup_*.log"))


@pytest.fixture
def topup(tmp_path: Path) -> Topup:
    return Topup(tmp_path)


def test_topup_local_passes_gzip_to_the_python_process_only(topup: Topup):
    r = topup.run(PRO_CHAIN_MODE="local")
    assert r.returncode == 0, (r.stdout, r.stderr)
    calls = topup.calls()
    assert calls[0].startswith("topup gzip=1"), calls
    assert calls[1] == "notify gzip=unset"  # a later command in the same shell does not see it
    assert "serv1: ✅ добор: файлов в корпусе" in topup.notified()


def test_topup_remote_is_plain_json_and_notify_unchanged(topup: Topup):
    r = topup.run(PRO_CHAIN_MODE="remote")
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert topup.calls()[0].startswith("topup gzip=unset")
    n = topup.notified()
    assert "✅ добор: файлов в корпусе" in n and "serv1" not in n
    assert "✅ добор: свежайшая карта" in n


def test_topup_failure_notify_and_rc(topup: Topup):
    r = topup.run(PRO_CHAIN_MODE="remote", STUB_TOPUP_RC="4")
    assert r.returncode == 4
    assert "⚠️ добор про-корпуса: rc=4" in topup.notified()
    assert "serv1" not in topup.notified()
    topup.notify.unlink()
    r = topup.run(PRO_CHAIN_MODE="local", STUB_TOPUP_RC="4")
    assert r.returncode == 4
    assert "serv1: ⚠️ добор про-корпуса: rc=4" in topup.notified()


def test_topup_shadow_skips_python(topup: Topup):
    r = topup.run(PRO_CHAIN_MODE="local", PRO_CHAIN_SHADOW="1")
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert topup.calls() == []
    assert "[тень] добор про-корпуса пропущен" in topup.topup_log()
    # explicit opt-in still runs it (and still with gzip, local mode)
    r = topup.run(PRO_CHAIN_MODE="local", PRO_CHAIN_SHADOW="1", PRO_CHAIN_SHADOW_TOPUP="1")
    assert r.returncode == 0
    assert topup.calls()[0].startswith("topup gzip=1")


def test_topup_guard_exit_2_when_build_tree_is_prod(topup: Topup):
    r = topup.run(PRO_CHAIN_MODE="local", PROD_ROOT=str(topup.tree))
    assert r.returncode == 2, (r.stdout, r.stderr)
    assert topup.calls() == [] and topup.notified() == ""
    r = topup.run(PRO_CHAIN_MODE="bogus")
    assert r.returncode == 2 and topup.calls() == []


def test_topup_interactive_tty_goes_to_background_with_pid_file(topup: Topup):
    master, slave = pty.openpty()
    try:
        p = subprocess.Popen([BASH, str(topup.script)], env=topup.env(PRO_CHAIN_MODE="remote"),
                             stdout=slave, stderr=slave, close_fds=True)
        assert p.wait(timeout=30) == 0
        out = os.read(master, 4096).decode()  # slave still open here: macOS drops buffered data on last close
    finally:
        os.close(slave)
        os.close(master)
    assert "добор запущен в фоне, PID" in out and "лог: runtime/pro_topup_" in out
    pid_file = topup.tree / "runtime" / "pro_topup.pid"
    assert pid_file.read_text().strip().isdigit()
    deadline = time.time() + 20
    while time.time() < deadline and "✅ добор: свежайшая карта" not in topup.notified():
        time.sleep(0.2)
    assert "✅ добор: свежайшая карта" in topup.notified()


# --------------------------------------------------------------------------- setup_pro_chain_serv1.sh

def manifest(root: Path, skip=(".git",)) -> dict:
    out = {}
    for p in sorted(root.rglob("*")):
        rel = p.relative_to(root)
        if rel.parts and rel.parts[0] in skip:
            continue
        st = p.lstat()
        out[str(rel)] = (p.is_symlink(), os.readlink(p) if p.is_symlink() else None,
                         st.st_size if p.is_file() else 0, st.st_mtime_ns)
    return out


@pytest.fixture
def setup_env(tmp_path: Path):
    prod = tmp_path / "prod"
    prod.mkdir()
    git(prod, "init", "-q")
    write(prod / ".gitignore", "base/keys.py\npro_heroes_data/\nruntime/\n", 0o644)
    write(prod / "base" / "code.py", "x = 1\n", 0o644)
    git(prod, "add", "-A")
    git(prod, "commit", "-q", "-m", "init")
    write(prod / "base" / "keys.py", "SECRET = 1\n", 0o600)  # gitignored secret
    write(prod / "pro_heroes_data" / "json_parts_split_from_object" / "part_0.json.gz", "gz", 0o644)
    build = tmp_path / "build"

    def run(**extra):
        env = clean_env(tmp_path, PROD_ROOT=str(prod), PRO_CHAIN_BUILD_ROOT=str(build), **extra)
        return subprocess.run([BASH, str(OPS_DIR / "setup_pro_chain_serv1.sh")], env=env,
                              capture_output=True, text=True, timeout=60)
    return prod, build, run


def test_setup_creates_worktree_keys_link_dirs_and_is_idempotent(setup_env):
    prod, build, run = setup_env
    prod_before = manifest(prod)
    head = git(prod, "rev-parse", "HEAD")
    status_before = git(prod, "status", "--porcelain", "--ignored")

    r = run()
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert str(build.resolve()) in git(prod, "worktree", "list")
    assert git(build, "rev-parse", "HEAD") == head
    assert git(build, "rev-parse", "--abbrev-ref", "HEAD") == "HEAD"  # detached
    link = build / "base" / "keys.py"
    assert link.is_symlink() and os.readlink(link) == str(prod / "base" / "keys.py")
    for d in ("runtime/artifacts/misc", "data", "ELO/output",
              "pro_heroes_data/json_parts_split_from_object"):
        assert (build / d).is_dir()
    assert "ОТСУТСТВУЕТ" in r.stdout and "processed_ids.txt" in r.stdout
    assert "ml-models/prematch_panel_kv3/manifest.json" in r.stdout

    # PROD working tree, HEAD, status unchanged (only .git/worktrees bookkeeping differs)
    assert manifest(prod) == prod_before
    assert git(prod, "rev-parse", "HEAD") == head
    assert git(prod, "status", "--porcelain", "--ignored") == status_before
    assert (prod / "pro_heroes_data" / "json_parts_split_from_object" / "part_0.json.gz").read_text() == "gz"

    # fill some inputs, second run: no-op on both trees, checklist reflects the inputs
    parts = build / "pro_heroes_data" / "json_parts_split_from_object"
    write(parts / "part_0.json.gz", "12345", 0o644)
    write(parts / "processed_ids.txt", "1\n", 0o644)
    build_before = manifest(build)
    r2 = run()
    assert r2.returncode == 0, (r2.stdout, r2.stderr)
    assert manifest(build) == build_before
    assert manifest(prod) == prod_before
    assert "создаю" not in r2.stdout and "уже есть" in r2.stdout
    assert "1 файлов, 5 байт" in r2.stdout
    lines = [ln for ln in r2.stdout.splitlines() if "processed_ids.txt" in ln]
    assert lines and lines[0].startswith("есть")


def test_setup_without_prod_keys_still_exits_0_and_reports(setup_env):
    prod, build, run = setup_env
    (prod / "base" / "keys.py").unlink()
    r = run()
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert not (build / "base" / "keys.py").exists()
    assert "ОТСУТСТВУЕТ" in r.stdout and "keys.py" in r.stdout


def test_setup_failed_actions_exit_1(setup_env, tmp_path: Path):
    prod, build, run = setup_env
    build.mkdir()
    (build / "stray.txt").write_text("x")  # non-empty, not a worktree: refuse, touch nothing
    r = run()
    assert r.returncode == 1 and (build / "stray.txt").exists()
    assert not (build / "base").exists()
    # PROD not a git repo
    plain = tmp_path / "plain"
    plain.mkdir()
    env = clean_env(tmp_path, PROD_ROOT=str(plain), PRO_CHAIN_BUILD_ROOT=str(tmp_path / "b2"))
    r = subprocess.run([BASH, str(OPS_DIR / "setup_pro_chain_serv1.sh")], env=env,
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 1 and not (tmp_path / "b2").exists()
    # BUILD == PROD
    env = clean_env(tmp_path, PROD_ROOT=str(prod), PRO_CHAIN_BUILD_ROOT=str(prod))
    r = subprocess.run([BASH, str(OPS_DIR / "setup_pro_chain_serv1.sh")], env=env,
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 1


# --------------------------------------------------------------------------- syntax

@pytest.mark.parametrize("script", [
    RUN_DIR / "topup_pro_corpus.sh", RUN_DIR / "pro_nightly_chain.sh",
    OPS_DIR / "setup_pro_chain_serv1.sh", RUN_DIR / "lib_pro_chain.sh",
])
def test_bash_n_under_system_bash(script: Path):
    r = subprocess.run([BASH, "-n", str(script)], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_chain_gate_before_topup_and_rebuild(chain: Chain):
    write(chain.prod / "runtime/sourcetv_matches.json", '{"map": 1}', 0o644)
    write(chain.stubbin / "sleep", '''#!/bin/bash
echo "live-gate" >> "$STUB_LOG"
printf '{}' > "$PROD_ROOT/runtime/sourcetv_matches.json"
''')
    r = chain.run(PRO_CHAIN_HEAVY_WAIT_SECONDS="2", PRO_CHAIN_LIVE_POLL_SECONDS="0.01")
    assert r.returncode == 0, chain.chain_log()
    assert [s.split()[0] for s in chain.steps()] == ["live-gate", "topup", "rebuild"]
    assert "жду окончания живой карты" in chain.chain_log()


@pytest.mark.parametrize("shadow", ["0", "1"])
def test_chain_waits_before_topup_until_live_map_clears(chain: Chain, shadow: str):
    live = chain.prod / "runtime/sourcetv_matches.json"
    write(live, '{"map": 1}', 0o644)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(chain.run, PRO_CHAIN_HEAVY_WAIT_SECONDS="10",
                                 PRO_CHAIN_LIVE_POLL_SECONDS="0.01", PRO_CHAIN_SHADOW=shadow)
        try:
            deadline = time.monotonic() + 5
            while "добор про-корпуса — жду окончания живой карты" not in chain.chain_log():
                assert chain.steps() == [], "topup ran while the live map was active"
                assert not future.done(), chain.chain_log()
                assert time.monotonic() < deadline, "topup gate did not start waiting"
                time.sleep(0.01)
            assert chain.steps() == [], "topup ran while the live map was active"
        finally:
            write(live, "{}", 0o644)
        r = future.result(timeout=10)
    assert r.returncode == 0, chain.chain_log()
    assert [s.split()[0] for s in chain.steps()] == ["topup", "rebuild"]
    assert "ВНИМАНИЕ" not in chain.chain_log()


@pytest.mark.parametrize("live_state", ['{"map": 1}', "unparsable"])
def test_chain_gate_heavy_bound_warns_and_still_runs_both_steps(chain: Chain, live_state: str):
    write(chain.prod / "runtime/sourcetv_matches.json", live_state, 0o644)
    r = chain.run(PRO_CHAIN_HEAVY_WAIT_SECONDS="0")
    assert r.returncode == 0, chain.chain_log()
    assert [s.split()[0] for s in chain.steps()] == ["topup", "rebuild"]
    log = chain.chain_log()
    assert log.index("ВНИМАНИЕ: добор про-корпуса") < log.index("добор про-корпуса ===")
    assert log.index("ВНИМАНИЕ: пересборка снимка") < log.index("пересборка снимка ===")


def test_chain_gate_refuses_rebuild_timeout_before_any_step(chain: Chain):
    r = chain.run(PRO_CHAIN_REBUILD_TIMEOUT="1s")
    assert r.returncode == 2, (r.stdout, r.stderr, chain.chain_log())
    assert "ОШИБКА" in r.stderr and "PRO_CHAIN_REBUILD_TIMEOUT" in r.stderr
    assert chain.steps() == []


def test_chain_gate_systemd_units_are_bounded_and_use_moscow_time():
    # Parse directives without requiring systemd on the Mac.
    service = (OPS_DIR / "systemd/pro-chain-nightly.service").read_text()
    timer = (OPS_DIR / "systemd/pro-chain-nightly.timer").read_text()
    required = [
        "Type=oneshot", "ExecStart=/root/main/scripts/run/pro_nightly_chain.sh",
        "Nice=10", "IOSchedulingClass=idle", "MemoryHigh=7.5G", "MemoryMax=8.5G",
        "MemorySwapMax=0", "OOMScoreAdjust=900", "Environment=HOME=/root",
        "TimeoutStartSec=infinity",
    ]
    assert all(line in service.splitlines() for line in required)
    assert "OnCalendar=*-*-* 01:30:00 Europe/Moscow" in timer.splitlines()
    assert "Persistent=false" in timer.splitlines()
    assert "Unit=pro-chain-nightly.service" in timer.splitlines()
    assert "WantedBy=timers.target" in timer.splitlines()


def test_chain_gate_setup_prints_install_commands_only(setup_env, tmp_path):
    prod, build, run = setup_env
    stubs = tmp_path / "install-stubs"
    calls = tmp_path / "install-calls"
    for name in ("install", "systemctl"):
        write(stubs / name, f'#!/bin/bash\necho "{name} $*" >> "{calls}"\nexit 99\n')
    r = run(PATH=f"{stubs}:{os.environ['PATH']}")
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert "install -m 0644" in r.stdout and "pro-chain-nightly.timer" in r.stdout
    assert "systemctl daemon-reload" in r.stdout
    assert "systemctl enable --now pro-chain-nightly.timer" in r.stdout
    assert "shadow" in r.stdout and "launchd" in r.stdout
    assert not calls.exists(), "setup executed installation/enable commands"
