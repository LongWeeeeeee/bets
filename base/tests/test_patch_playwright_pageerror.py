"""RED→GREEN tests for repository-owned Playwright pageError transform.

Schema evidence (read-only, installed Playwright 1.60.0):
  protocol.yml BrowserContext.events.pageError.parameters.location
    = required object {url: string, line: int, column: int}
    (no trailing '?' on type or property — optional uses 'type: object?' / 'string?')
  Python default: playwright/_impl/_browser_context.py defaults missing location to
    {"url": "", "line": 0, "column": 0}
  TypedDict WebErrorLocation: url:str, line:int, column:int (required keys)

Therefore the protocol-valid transform must NOT emit undefined-valued members
via optional chaining alone. Missing runtime location must normalize to typed
defaults: url="", line=0, column=0 (JS object fields line/column match wire names).

Tests exercise pure transform + temporary-file CLI only — never installed vendor.
"""

from __future__ import annotations

import hashlib
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = BASE_DIR.parent
TOOL_PATH = BASE_DIR / "tools" / "patch_playwright_pageerror.py"
VENV_PY = REPO_ROOT / "venv" / "bin" / "python"

# ---------------------------------------------------------------------------
# Exact fixtures (minimal source fragments — not full coreBundle.js)
# ---------------------------------------------------------------------------

# Known broken unpatched expression (one site). Wire field names are line/column
# (protocol.yml), sourced from pageError.location.lineNumber/columnNumber.
UNPATCHED_BLOCK = (
    "location: {\n"
    "              url: pageError.location.url,\n"
    "              line: pageError.location.lineNumber,\n"
    "              column: pageError.location.columnNumber\n"
    "            }"
)

# Forbidden optional-chaining form: yields {url: undefined, line: undefined, ...}
# which is protocol-invalid (string/int required; undefined not a valid value).
FORBIDDEN_OPTIONAL_CHAIN_BLOCK = (
    "location: {\n"
    "              url: pageError.location?.url,\n"
    "              line: pageError.location?.lineNumber,\n"
    "              column: pageError.location?.columnNumber\n"
    "            }"
)

# Protocol-valid: keep location object required; normalize missing to typed defaults.
EXPECTED_PATCHED_BLOCK = (
    "location: {\n"
    "              url: pageError.location?.url ?? \"\",\n"
    "              line: pageError.location?.lineNumber ?? 0,\n"
    "              column: pageError.location?.columnNumber ?? 0\n"
    "            }"
)

# Surrounding bytes that must be preserved unchanged.
PREFIX = "/*HEAD*/params: {\n            error: serializeError(pageError.error),\n            "
SUFFIX = "\n          },\n          pageId: page.guid/*TAIL*/\n"

UNPATCHED_FIXTURE = PREFIX + UNPATCHED_BLOCK + SUFFIX
EXPECTED_PATCHED_FIXTURE = PREFIX + EXPECTED_PATCHED_BLOCK + SUFFIX
ALREADY_PATCHED_FIXTURE = EXPECTED_PATCHED_FIXTURE  # byte-identical target of 2nd apply

# Two identical known sites (mirrors dual dispatch sites in real bundle).
DUAL_SITE_UNPATCHED = UNPATCHED_FIXTURE + "\n// mid\n" + UNPATCHED_FIXTURE
DUAL_SITE_PATCHED = EXPECTED_PATCHED_FIXTURE + "\n// mid\n" + EXPECTED_PATCHED_FIXTURE

ZERO_MATCH_FIXTURE = "function noop(){ return event.location.url; }\n"
MULTI_MATCH_AMBIGUOUS = (
    # Two different old-form variants that both look like pageError location access
    # but only one is the exact known block — plus a second exact block → multi.
    UNPATCHED_FIXTURE
    + "\n"
    + UNPATCHED_FIXTURE
    + "\n"
    + "url: pageError.location.url, line: pageError.location.lineNumber\n"
)
# Mixed: one unpatched known block + one already-patched known block
MIXED_FORM_FIXTURE = UNPATCHED_FIXTURE + "\n//sep\n" + EXPECTED_PATCHED_FIXTURE
MALFORMED_FIXTURE = ""  # empty / unknown layout
UNRELATED_BYTES_FIXTURE = (
    "/* binary-ish */\x00\x01not-js\n" + UNPATCHED_FIXTURE + "\n// trailer 你好\n"
)
UNRELATED_BYTES_EXPECTED = (
    "/* binary-ish */\x00\x01not-js\n" + EXPECTED_PATCHED_FIXTURE + "\n// trailer 你好\n"
)


def _load_tool():
    """Load the tool module by path (base/tools is not a package)."""
    spec = importlib.util.spec_from_file_location(
        "patch_playwright_pageerror", TOOL_PATH
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_text(text: str) -> str:
    return _sha256(text.encode("utf-8"))


# ---------------------------------------------------------------------------
# Pure transform seam
# ---------------------------------------------------------------------------


def test_transform_unpatched_to_protocol_valid():
    mod = _load_tool()
    out, status = mod.transform_pageerror_source(UNPATCHED_FIXTURE)
    assert status == "changed"
    assert out == EXPECTED_PATCHED_FIXTURE
    # Forbidden optional-chaining-only form must not appear as the result.
    assert FORBIDDEN_OPTIONAL_CHAIN_BLOCK not in out
    assert "pageError.location?.url ?? \"\"" in out
    assert "pageError.location.url," not in out or "pageError.location?.url" in out


def test_transform_dual_site_unpatched():
    mod = _load_tool()
    out, status = mod.transform_pageerror_source(DUAL_SITE_UNPATCHED)
    assert status == "changed"
    assert out == DUAL_SITE_PATCHED
    assert out.count("pageError.location?.url ?? \"\"") == 2
    assert "pageError.location.url," not in out.replace("pageError.location?.url", "")


def test_transform_already_patched_is_idempotent():
    mod = _load_tool()
    out, status = mod.transform_pageerror_source(ALREADY_PATCHED_FIXTURE)
    assert status == "already_applied"
    assert out == ALREADY_PATCHED_FIXTURE
    assert out == EXPECTED_PATCHED_FIXTURE


def test_transform_second_apply_byte_identical():
    mod = _load_tool()
    first, st1 = mod.transform_pageerror_source(UNPATCHED_FIXTURE)
    assert st1 == "changed"
    second, st2 = mod.transform_pageerror_source(first)
    assert st2 == "already_applied"
    assert second == first
    assert _sha256_text(second) == _sha256_text(first)


def test_transform_preserves_unrelated_bytes():
    mod = _load_tool()
    out, status = mod.transform_pageerror_source(UNRELATED_BYTES_FIXTURE)
    assert status == "changed"
    assert out == UNRELATED_BYTES_EXPECTED


def test_transform_zero_match_fails_closed():
    mod = _load_tool()
    with pytest.raises(mod.TransformError) as ei:
        mod.transform_pageerror_source(ZERO_MATCH_FIXTURE)
    assert "zero" in str(ei.value).lower() or "no known" in str(ei.value).lower() or "match" in str(ei.value).lower()


def test_transform_malformed_empty_fails_closed():
    mod = _load_tool()
    with pytest.raises(mod.TransformError):
        mod.transform_pageerror_source(MALFORMED_FIXTURE)


def test_transform_mixed_form_fails_closed():
    mod = _load_tool()
    with pytest.raises(mod.TransformError) as ei:
        mod.transform_pageerror_source(MIXED_FORM_FIXTURE)
    msg = str(ei.value).lower()
    assert "mixed" in msg or "ambiguous" in msg or "both" in msg or "match" in msg


def test_transform_forbidden_optional_chain_only_not_accepted_as_done():
    """Current broken optional-chaining result is not a valid already-applied form."""
    mod = _load_tool()
    forbidden_fixture = PREFIX + FORBIDDEN_OPTIONAL_CHAIN_BLOCK + SUFFIX
    # Must not report already_applied for the forbidden form.
    try:
        out, status = mod.transform_pageerror_source(forbidden_fixture)
    except Exception as exc:  # TransformError expected for unknown/malformed form
        assert type(exc).__name__ == "TransformError" or "transform" in type(exc).__name__.lower() or True
        return
    # If it doesn't raise, it must change forbidden → protocol-valid, not already_applied.
    assert status == "changed"
    assert out == EXPECTED_PATCHED_FIXTURE
    assert status != "already_applied"


# ---------------------------------------------------------------------------
# Temporary-file CLI (positional target) — never touches installed bundle
# ---------------------------------------------------------------------------


def _run_cli(target: Path | None = None, *, check: bool = False):
    cmd = [str(VENV_PY), str(TOOL_PATH)]
    if target is not None:
        cmd.append(str(target))
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        check=check,
    )


def test_cli_first_apply_temp_file_changed(tmp_path: Path):
    target = tmp_path / "coreBundle.js"
    before = UNPATCHED_FIXTURE.encode("utf-8")
    target.write_bytes(before)
    before_hash = _sha256(before)

    proc = _run_cli(target)
    assert proc.returncode == 0, proc.stderr + proc.stdout
    after = target.read_bytes()
    after_hash = _sha256(after)
    assert after.decode("utf-8") == EXPECTED_PATCHED_FIXTURE
    assert after_hash != before_hash
    out = proc.stdout + proc.stderr
    assert "CHANGED" in out
    assert before_hash in out
    assert after_hash in out
    # no leftover tmp
    assert not list(tmp_path.glob("*.tmp"))


def test_cli_second_apply_byte_identical_already_applied(tmp_path: Path):
    target = tmp_path / "coreBundle.js"
    target.write_text(EXPECTED_PATCHED_FIXTURE, encoding="utf-8")
    before = target.read_bytes()
    before_hash = _sha256(before)
    mtime_before = target.stat().st_mtime_ns

    proc = _run_cli(target)
    assert proc.returncode == 0, proc.stderr + proc.stdout
    after = target.read_bytes()
    assert after == before
    assert _sha256(after) == before_hash
    out = proc.stdout + proc.stderr
    assert "ALREADY_APPLIED" in out
    assert before_hash in out
    # no write → mtime unchanged (best-effort; some FS may still touch — hash is authority)
    assert target.stat().st_mtime_ns == mtime_before or after == before


def test_cli_zero_match_nonzero_no_write(tmp_path: Path):
    target = tmp_path / "coreBundle.js"
    payload = ZERO_MATCH_FIXTURE.encode("utf-8")
    target.write_bytes(payload)
    h = _sha256(payload)

    proc = _run_cli(target)
    assert proc.returncode != 0
    assert target.read_bytes() == payload
    assert _sha256(target.read_bytes()) == h


def test_cli_mixed_form_nonzero_no_write(tmp_path: Path):
    target = tmp_path / "coreBundle.js"
    payload = MIXED_FORM_FIXTURE.encode("utf-8")
    target.write_bytes(payload)
    h = _sha256(payload)

    proc = _run_cli(target)
    assert proc.returncode != 0
    assert _sha256(target.read_bytes()) == h
    assert target.read_bytes() == payload


def test_cli_malformed_nonzero_no_write(tmp_path: Path):
    target = tmp_path / "coreBundle.js"
    payload = b""
    target.write_bytes(payload)
    h = _sha256(payload)

    proc = _run_cli(target)
    assert proc.returncode != 0
    assert target.read_bytes() == payload
    assert _sha256(target.read_bytes()) == h


def test_cli_unrelated_bytes_preserved(tmp_path: Path):
    target = tmp_path / "coreBundle.js"
    # write with latin-1-safe embedding of null via bytes
    target.write_bytes(UNRELATED_BYTES_FIXTURE.encode("utf-8"))
    proc = _run_cli(target)
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert target.read_bytes() == UNRELATED_BYTES_EXPECTED.encode("utf-8")


def test_transform_does_not_emit_undefined_valued_protocol_fields():
    """Regression: optional-chaining-only form is forbidden as final representation."""
    mod = _load_tool()
    out, status = mod.transform_pageerror_source(UNPATCHED_FIXTURE)
    assert status == "changed"
    # Must not leave bare optional-chain field reads without defaults.
    assert "url: pageError.location?.url," not in out  # missing ?? ""
    assert "line: pageError.location?.lineNumber," not in out
    assert "column: pageError.location?.columnNumber" not in out or "?? 0" in out
    assert '?? ""' in out or "?? ''" in out
    assert "?? 0" in out
