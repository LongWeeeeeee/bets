"""Guard: no Steam Web API key literal in tracked base/**/*.py.

The origin repo is public. A 32-hex Steam key assigned to a *KEY* name in tracked
code is readable by anyone (card ingame-5sji). Keys live in the untracked
base/keys.py or in an env var.

Only tracked files under base/ are scanned (via ``git ls-files``) -- never
data/, runtime/ or ml-models/.
"""
import ast
import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
_HEX32 = re.compile(r"^[0-9A-Fa-f]{32}$")


def _target_names(target):
    if isinstance(target, ast.Name):
        yield target.id
    elif isinstance(target, ast.Attribute):
        yield target.attr
    elif isinstance(target, (ast.Tuple, ast.List)):
        for elt in target.elts:
            yield from _target_names(elt)


def find_key_literals(source: str):
    """Return (lineno, name) for 32-hex string constants assigned to *KEY* names."""
    hits = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets, value = [node.target], node.value
        else:
            continue
        if not (isinstance(value, ast.Constant) and isinstance(value.value, str)):
            continue
        if not _HEX32.match(value.value):
            continue
        for target in targets:
            for name in _target_names(target):
                if "KEY" in name.upper():
                    hits.append((node.lineno, name))
    return hits


_UNTRACKED_SECRET_FILES = {"base/keys.py"}


def _tracked_base_py_files(root=None):
    """Tracked base/**/*.py; outside a git checkout, every base/**/*.py file.

    The delivery check runs in an exported tree with no .git (only tracked
    files, no base/keys.py), where `git ls-files` returns nothing.
    """
    root = REPO_ROOT if root is None else root
    if (root / ".git").exists():
        out = subprocess.run(
            ["git", "ls-files", "-z", "--", "base/*.py"],
            cwd=root, check=True, capture_output=True, text=True,
        ).stdout
        return [p for p in out.split("\0") if p]
    return sorted(
        rel for rel in (p.relative_to(root).as_posix()
                        for p in (root / "base").rglob("*.py"))
        if rel not in _UNTRACKED_SECRET_FILES)


def test_detector_flags_key_assignment_and_ignores_other_names():
    assert find_key_literals('KEY = "' + "AB" * 16 + '"\n') == [(1, "KEY")]
    assert find_key_literals('STEAM_API_KEY: str = "' + "0f" * 16 + '"\n') == [(1, "STEAM_API_KEY")]
    assert find_key_literals('CHECKSUM = "' + "AB" * 16 + '"\n') == []
    assert find_key_literals('KEY = "short"\n') == []


def test_no_hardcoded_steam_key_literals_in_tracked_base_python():
    files = _tracked_base_py_files()
    assert files, "no base/**/*.py found (git ls-files or exported tree)"
    offenders = []
    for rel in files:
        path = REPO_ROOT / rel
        if not path.is_file():
            continue
        try:
            hits = find_key_literals(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        offenders.extend(f"{rel}:{lineno} ({name})" for lineno, name in hits)
    assert not offenders, (
        "32-hex key literal assigned to a KEY name in tracked code "
        "(repo is public; read it from env / base/keys.py): " + ", ".join(offenders)
    )
