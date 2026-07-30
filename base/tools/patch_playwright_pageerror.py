#!/usr/bin/env python3
"""Guard Playwright's Firefox driver against a null pageError.location crash.

Playwright 1.60.0's vendored node driver (``driver/package/lib/coreBundle.js``)
serializes the ``BrowserContext.PageError`` event with an UNGUARDED access:

    location: {
      url: pageError.location.url,        // crashes when location is undefined
      line: pageError.location.lineNumber,
      column: pageError.location.columnNumber
    }

Firefox (Camoufox) reports some uncaught page errors WITHOUT a ``location``
(cross-origin scripts, SecurityError, ...). dota2protracker.com triggers this.
Because the read happens inside a synchronous ``emit`` listener, the resulting
``TypeError: Cannot read properties of undefined (reading 'url')`` is an
uncaught exception that kills the entire node driver process.

Protocol evidence (installed Playwright 1.60.0, read-only):
  ``driver/package/protocol.yml`` → BrowserContext events ``pageError``:
    parameters.location is a **required** object
      {url: string, line: int, column: int}
    (no trailing ``?`` — optional fields use ``type: object?`` / ``string?``).
  Python client default (``_browser_context.py``):
    ``params.get("location") or {"url": "", "line": 0, "column": 0}``
  ``WebErrorLocation`` TypedDict: required ``url``/``line``/``column``.

Therefore optional-chaining alone (``pageError.location?.url`` → ``undefined``)
is **forbidden**: it can emit protocol-invalid undefined-valued members.
The repository-owned transform normalizes missing location to typed defaults:
  url = "" , line = 0 , column = 0
while still short-circuiting the throw via optional chaining + nullish coalescing.

This file lives in the venv (git-ignored vendor) and is overwritten by
``playwright install`` / a venv rebuild, so re-run after any such operation:

    /root/main/venv/bin/python base/tools/patch_playwright_pageerror.py

No-arg form discovers the active interpreter's installed ``coreBundle.js``.
A positional path may be supplied for temporary fixtures (tests).

Exit codes:
  0 = CHANGED or ALREADY_APPLIED (machine-readable stdout)
  1 = error / fail-closed (zero/mixed/malformed/ambiguous) — no write
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from pathlib import Path
from typing import Literal, Tuple

# Exact known unpatched block (two identical dispatch sites in coreBundle.js).
# Wire field names are line/column (protocol.yml); source uses lineNumber/columnNumber.
OLD_BLOCK = (
    "location: {\n"
    "              url: pageError.location.url,\n"
    "              line: pageError.location.lineNumber,\n"
    "              column: pageError.location.columnNumber\n"
    "            }"
)

# Forbidden prior form: optional chaining without defaults → undefined members.
FORBIDDEN_OPTIONAL_CHAIN_BLOCK = (
    "location: {\n"
    "              url: pageError.location?.url,\n"
    "              line: pageError.location?.lineNumber,\n"
    "              column: pageError.location?.columnNumber\n"
    "            }"
)

# Protocol-valid: required location object with typed defaults for missing fields.
NEW_BLOCK = (
    "location: {\n"
    "              url: pageError.location?.url ?? \"\",\n"
    "              line: pageError.location?.lineNumber ?? 0,\n"
    "              column: pageError.location?.columnNumber ?? 0\n"
    "            }"
)

# Legacy substring forms (for detection of partial / mixed states only).
LEGACY_NEEDLE = "pageError.location."
LEGACY_OPTIONAL = "pageError.location?."

Status = Literal["changed", "already_applied"]


class TransformError(Exception):
    """Fail-closed transform: unknown, mixed, zero-match, or malformed source."""


def transform_pageerror_source(source: str) -> Tuple[str, Status]:
    """Pure transform seam.

    Returns ``(transformed_text, status)`` where status is:
      - ``"changed"`` — exactly one or more pure OLD_BLOCK occurrences replaced
      - ``"already_applied"`` — source already contains only NEW_BLOCK form(s)

    Raises ``TransformError`` on zero matches, mixed old+new, forbidden-only
    optional-chain form that cannot be uniquely upgraded in isolation when mixed
    with other ambiguity, empty/malformed input, or residual unguarded accesses.
    """
    if source is None:
        raise TransformError("malformed source: None")
    if not isinstance(source, str):
        raise TransformError(f"malformed source: expected str, got {type(source)!r}")
    if source == "":
        raise TransformError("malformed source: empty")

    old_count = source.count(OLD_BLOCK)
    new_count = source.count(NEW_BLOCK)
    forbidden_count = source.count(FORBIDDEN_OPTIONAL_CHAIN_BLOCK)

    # Mixed known forms → fail closed (do not partial-write).
    present_forms = sum(1 for c in (old_count, new_count, forbidden_count) if c > 0)
    if present_forms > 1:
        raise TransformError(
            f"mixed pageError location forms: old={old_count} "
            f"new={new_count} forbidden_optional={forbidden_count}"
        )

    if old_count > 0:
        # Upgrade pure old → new. Only replace the exact known block.
        out = source.replace(OLD_BLOCK, NEW_BLOCK)
        # Residual unguarded pageError.location. property access is fatal.
        # After replace, NEW_BLOCK still contains "pageError.location?." so
        # check the bare unguarded needle carefully.
        residual_unguarded = _count_unguarded_pageerror_location(out)
        if residual_unguarded:
            raise TransformError(
                f"ambiguous residual unguarded pageError.location access "
                f"({residual_unguarded}); refusing partial transform"
            )
        if out == source:
            raise TransformError("internal: old block count > 0 but text unchanged")
        return out, "changed"

    if new_count > 0:
        # Already protocol-valid. Ensure no residual unguarded accesses.
        residual_unguarded = _count_unguarded_pageerror_location(source)
        if residual_unguarded:
            raise TransformError(
                f"already-patched markers present but residual unguarded "
                f"pageError.location access remains ({residual_unguarded})"
            )
        return source, "already_applied"

    if forbidden_count > 0:
        # Upgrade forbidden optional-chain-only → protocol-valid defaults.
        out = source.replace(FORBIDDEN_OPTIONAL_CHAIN_BLOCK, NEW_BLOCK)
        residual_unguarded = _count_unguarded_pageerror_location(out)
        if residual_unguarded:
            raise TransformError(
                f"ambiguous residual unguarded pageError.location access "
                f"({residual_unguarded}) after forbidden-form upgrade"
            )
        if out == source:
            raise TransformError(
                "internal: forbidden block count > 0 but text unchanged"
            )
        return out, "changed"

    # Zero known forms.
    # Any leftover pageError.location token is an unknown layout → fail closed.
    if "pageError.location" in source:
        raise TransformError(
            "unknown/malformed pageError.location expression "
            "(no exact known old/new/forbidden block)"
        )
    raise TransformError(
        "zero matches: no known pageError location expression found"
    )


def _count_unguarded_pageerror_location(text: str) -> int:
    """Count ``pageError.location.`` that is NOT already ``pageError.location?.``."""
    # Walk occurrences of LEGACY_NEEDLE; skip those that are actually optional-chained.
    count = 0
    start = 0
    while True:
        i = text.find(LEGACY_NEEDLE, start)
        if i < 0:
            break
        # LEGACY_NEEDLE is "pageError.location." — optional form is
        # "pageError.location?." which does NOT contain LEGACY_NEEDLE as substring
        # because of the '?' before the final '.' ... wait: "pageError.location?."
        # does NOT include "pageError.location." — correct, optional has '?' between
        # location and '.'. So LEGACY_NEEDLE only matches unguarded form.
        count += 1
        start = i + len(LEGACY_NEEDLE)
    return count


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _find_core_bundle() -> Path:
    import playwright  # noqa: PLC0415 - imported lazily so --help works without it

    bundle = (
        Path(playwright.__file__).parent
        / "driver"
        / "package"
        / "lib"
        / "coreBundle.js"
    )
    if not bundle.is_file():
        raise FileNotFoundError(f"coreBundle.js not found at {bundle}")
    return bundle


def apply_to_path(path: Path) -> Tuple[str, str, str]:
    """Read ``path``, transform, write atomically if changed.

    Returns ``(status, before_sha256, after_sha256)`` where status is
    ``"changed"`` or ``"already_applied"``.

    On ``TransformError`` the file is left byte-identical and the error propagates.
    """
    raw = path.read_bytes()
    before_sha = _sha256_bytes(raw)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TransformError(f"malformed source: not utf-8 ({exc})") from exc

    out, status = transform_pageerror_source(text)
    out_bytes = out.encode("utf-8")
    after_sha = _sha256_bytes(out_bytes)

    if status == "already_applied":
        return status, before_sha, after_sha

    # Atomic rebuild-then-replace only after successful validation.
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_bytes(out_bytes)
        os.replace(tmp, path)
    except Exception:
        # Best-effort cleanup of tmp; never leave partial replace mid-flight.
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass
        raise

    # One-time backup of the pristine content for easy manual revert (installed
    # bundle path only; tests may create it too — never overwrite existing bak).
    backup = path.with_suffix(path.suffix + ".pageerror-guard.bak")
    if not backup.exists():
        try:
            backup.write_bytes(raw)
        except OSError:
            # Backup failure is non-fatal for the transform itself.
            pass

    return status, before_sha, after_sha


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Apply protocol-valid pageError.location guard to Playwright coreBundle.js"
        )
    )
    parser.add_argument(
        "target",
        nargs="?",
        default=None,
        help=(
            "Optional path to a coreBundle.js (or fixture). "
            "Default: active interpreter's installed Playwright driver bundle."
        ),
    )
    args = parser.parse_args(argv)

    try:
        if args.target:
            bundle = Path(args.target).resolve()
            if not bundle.is_file():
                print(f"ERROR: target not found: {bundle}", file=sys.stderr)
                return 1
        else:
            bundle = _find_core_bundle().resolve()
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: could not locate Playwright driver bundle: {exc}", file=sys.stderr)
        return 1

    try:
        status, before_sha, after_sha = apply_to_path(bundle)
    except TransformError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    abs_path = str(bundle)
    if status == "changed":
        print(
            f"CHANGED path={abs_path} before_sha256={before_sha} after_sha256={after_sha}"
        )
        return 0
    print(f"ALREADY_APPLIED path={abs_path} sha256={before_sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
