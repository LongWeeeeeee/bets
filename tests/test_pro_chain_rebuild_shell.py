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
BASH = "/bin/bash"

REBUILD_REL = "scripts/run/rebuild_prematch_snapshot.sh"
KV3_REL = "scripts/ops/build_kv3_state.sh"
LIB_REL = "scripts/run/lib_pro_chain.sh"

OLD_ARTIFACT = b"old-artifact\n"
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
[ "${1:-}" = is-active ] && echo active
exit 0
'''

SLEEP_STUB = "#!/bin/bash\nexit 0\n"
SHA1SUM_STUB = '#!/bin/bash\nexec shasum -a 1 "$@"\n'

# `bash -s` is how a rebase script reaches the prod host (ssh `bash -s` remote,
# lib prod_script local). The heredoc hardcodes /root/main, so the stub rewrites
# it to the fake prod and logs the sha1 of the ORIGINAL text.
BASH_STUB = r'''#!/bin/bash
if [ "${1:-}" = "-s" ]; then
  tmp="$(mktemp "${TMPDIR:-/tmp}/bashs.XXXXXX")"
  cat > "$tmp.orig"
  echo "BASH-S ${*:2} $(shasum -a 1 < "$tmp.orig" | cut -d' ' -f1)" >> "$STUB_EVENTS"
  sed "s#/root/main#$FAKE_PROD#g; s#/root/.local/state#$FAKE_STATE#g" "$tmp.orig" > "$tmp"
  /bin/bash "$@" < "$tmp"; rc=$?
  rm -f "$tmp" "$tmp.orig"; exit $rc
fi
exec /bin/bash "$@"
'''

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
                           ("sha1sum", SHA1SUM_STUB), ("bash", BASH_STUB)):
            _write(self.stubs / name, text, exe=True)
        # build tree scripts
        lib = (REPO / LIB_REL).read_text(encoding="utf-8")
        _write(build / LIB_REL, lib, exe=True)
        _write(build / "scripts/run/topup_pro_corpus.sh", TOPUP_STUB, exe=True)
        _write(build / "runtime/pro_topup_fresh.log", "fresh\n")  # skip the 20 h safety top-up
        if target == "original":
            for rel in (REBUILD_REL, KV3_REL):
                orig = _git_show(rel)
                assert orig is not None, f"{ORIGINAL_REV}:{rel} not found"
                _write(build / rel, transform_original(rel, orig, str(py)), exe=True)
        else:
            for rel in (REBUILD_REL, KV3_REL):
                _write(build / rel, (REPO / rel).read_text(encoding="utf-8"), exe=True)
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
    assert new.prod_tree() == orig.prod_tree()
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
    r = _run_library(e, 'wait_no_live_map 2 test-gate; echo PROCEEDED',
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
    _write(e.prod / "venv/bin/python3", PRODPY_STUB.replace(
        'then exec "$STUB_REAL_PY" "$@"; fi',
        'then echo LIVE-READ >> "$STUB_EVENTS"; '
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
        "PRO_CHAIN_RESTART_WAIT_SECONDS": "2" if clears else "0",
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
        ssh_transaction = events.index("SSH bash -s -- 1")
        ssh_reads = [i for i, line in enumerate(events)
                     if i > roi and line.startswith("SSH ") and "sourcetv_matches.json" in line]
        assert ssh_reads and max(ssh_reads) < ssh_transaction < transaction
    else:
        assert not e.ops(("SSH ",))
    base = subprocess.run(
        ["git", "-C", str(REPO), "show", f"3de5a5af:{REBUILD_REL}"],
        capture_output=True, check=True,
    ).stdout
    base_stdin = base.split(b"<<'ELO_REBASE_REMOTE'\n", 1)[1].split(b"ELO_REBASE_REMOTE\n", 1)[0]
    assert transaction_stdin.read_bytes() == base_stdin
    assert e.ops(("BASH-S",)) == [f"BASH-S -- 1 {hashlib.sha1(base_stdin).hexdigest()}"]
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


def _memory_probe(e, *, live=True, available=1):
    _write(e.prod / "runtime/sourcetv_matches.json", '{"map": 1}' if live else "{}")
    meminfo = e.base / "meminfo"
    _write(meminfo, f"MemAvailable: {available} kB\n")
    _write(e.stubs / "sleep", '''#!/bin/bash
[ "$1" = 3 ] && exit 0
exec /bin/sleep "$@"
''', exe=True)
    text = (e.stubs / "python3").read_text().replace(
        'echo "PY $* |$note |@$here" >> "$EV"',
        'echo "PY $* |$note |@$here" >> "$EV"\n'
        'if [ "$1" = scripts/pro_chain/pro_corpus_extract.py ]; then\n'
        '  echo PROBE-BEGIN >> "$EV"\n'
        '  (/bin/sleep 0.6; echo PROBE-DESCENDANT-FINISHED >> "$EV") &\n'
        '  echo "PROBE-CHILD $!" >> "$EV"\n'
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
    extra = _memory_probe(e)
    before = e.prod_tree()
    r = e.run(REBUILD_REL, extra=extra)
    log = e.log.read_text()
    assert r.returncode != 0, log
    assert "ОШИБКА: watchdog памяти" in log
    # Wait beyond the stub's sleep: killing only the shell PID would leave the
    # background descendant alive to append its marker. No ps dependency.
    time.sleep(0.65)
    assert "PROBE-BEGIN" in e.events_lines() and "PROBE-FINISHED" not in e.events_lines()
    assert "PROBE-DESCENDANT-FINISHED" not in e.events_lines()
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
    assert 0.8 <= time.monotonic() - start < 3
    assert "ВНИМАНИЕ: blocked-read" in r.stdout and "PROCEEDED" in r.stdout


def test_chain_gate_caps_fractional_poll_at_remaining_wait(make_env):
    e = make_env(kv3=False, target="new", mode="local")
    _write(e.prod / "runtime/sourcetv_matches.json", '{"map": 1}')
    _write(e.stubs / "sleep", '''#!/bin/bash
echo "GATE-SLEEP $1" >> "$STUB_EVENTS"
printf '{}' > "$FAKE_PROD/runtime/sourcetv_matches.json"
''', exe=True)
    r = _run_library(e, 'set -e; wait_no_live_map 1 capped-poll', extra={
        "PRO_CHAIN_LIVE_POLL_SECONDS": "100.5",
    })
    assert r.returncode == 0, (r.stdout, r.stderr)
    assert e.ops(("GATE-SLEEP",)) == ["GATE-SLEEP 1"]


def test_chain_gate_parent_signal_waits_for_shadow_rebuild(make_env):
    e = make_env(kv3=False, target="new", mode="local", shadow=True)
    extra = _memory_probe(e, live=False, available=100)
    before = e.prod_tree()
    env = dict(e.env(), **extra)
    process = subprocess.Popen([BASH, str(e.build / REBUILD_REL)], cwd=e.build, env=env,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 3
        while "PROBE-BEGIN" not in e.events_lines() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert "PROBE-BEGIN" in e.events_lines()
        process.send_signal(signal.SIGTERM)
        out, err = process.communicate(timeout=5)
        assert process.returncode == 143, (out, err)
        assert "PROBE-FINISHED" in e.events_lines(), "parent exited before rebuild finished"
        assert e.prod_tree() == before
        assert e.ops(("SYSTEMCTL", "PRODPY", "BASH-S")) == []
    finally:
        # Let the bounded stub finish even when running against an old script.
        process.wait(timeout=5)
        time.sleep(0.8)
