#!/usr/bin/env python3
"""Guard Playwright's Firefox driver against a null pageError.location crash.

Playwright 1.60.0's vendored node driver (``driver/package/lib/coreBundle.js``)
serializes the ``BrowserContext.PageError`` event with an UNGUARDED access:

    location: {
      url: pageError.location.url,        // <-- crashes when location is undefined
      line: pageError.location.lineNumber,
      column: pageError.location.columnNumber
    }

Firefox (Camoufox) reports some uncaught page errors WITHOUT a ``location``
(cross-origin scripts, SecurityError, ...). dota2protracker.com triggers this.
Because the read happens inside a synchronous ``emit`` listener, the resulting
``TypeError: Cannot read properties of undefined (reading 'url')`` is an
uncaught exception that kills the entire node driver process -> our shared
Camoufox session dies with a TimeoutError and falls back to the slow subprocess
path. Observed 100+ times in the live sourcetv runtime log.

The fix mirrors how upstream guards it: turn ``pageError.location.<x>`` into
``pageError.location?.<x>``. Optional chaining yields ``undefined`` instead of
throwing, and crucially KEEPS the ``location`` object present (as ``{}`` once
the fields are dropped during JSON serialization), so the Python client's
``_on_page_error(error, page, location)`` still receives its ``location`` arg.

This file lives in the venv (git-ignored) and is overwritten by
``playwright install`` / a venv rebuild, so re-run this script after any such
operation (e.g. as a post-deploy step):

    python3 base/tools/patch_playwright_pageerror.py

The patch is idempotent (a no-op if already applied) and writes atomically
(``coreBundle.js.tmp`` -> rename) per the project's rebuild-then-replace rule.
A one-time ``coreBundle.js.pageerror-guard.bak`` backup is kept for easy revert.

Exit codes: 0 = patched or already-patched, 1 = error (driver not found, etc).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# The exact unguarded access and its guarded replacement. A single targeted
# substring swap covers BOTH duplicated dispatch sites in the bundle and the
# three fields (url / lineNumber / columnNumber) at once. It deliberately does
# NOT touch other ``*.location.url`` patterns (e.g. console-message locations,
# which use a different ``event.location`` object).
NEEDLE = "pageError.location."
REPLACEMENT = "pageError.location?."


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


def main() -> int:
    try:
        bundle = _find_core_bundle()
    except Exception as exc:  # noqa: BLE001 - report any discovery failure cleanly
        print(f"❌ Could not locate Playwright driver bundle: {exc}", file=sys.stderr)
        return 1

    text = bundle.read_text(encoding="utf-8")
    occurrences = text.count(NEEDLE)

    if occurrences == 0:
        if REPLACEMENT in text:
            print(f"✅ Already patched (guard present): {bundle}")
            return 0
        print(
            "⚠️  Neither the unguarded access nor the guard was found — the "
            "driver layout may have changed. Nothing to do.\n"
            f"    {bundle}",
            file=sys.stderr,
        )
        # Not fatal for a redeploy hook: a future Playwright that already fixed
        # this simply has nothing to patch.
        return 0

    patched = text.replace(NEEDLE, REPLACEMENT)

    # One-time backup of the pristine bundle for easy manual revert. Never
    # overwrite an existing backup (project rule: don't clobber backups).
    backup = bundle.with_suffix(bundle.suffix + ".pageerror-guard.bak")
    if not backup.exists():
        backup.write_text(text, encoding="utf-8")

    # Atomic rebuild-then-replace: write the new content to a sibling .tmp and
    # rename over the target only after a successful write.
    tmp = bundle.with_suffix(bundle.suffix + ".tmp")
    tmp.write_text(patched, encoding="utf-8")
    os.replace(tmp, bundle)  # atomic on the same filesystem

    print(
        f"✅ Patched {occurrences} occurrence(s) of '{NEEDLE}' -> "
        f"'{REPLACEMENT}'\n    file:   {bundle}\n    backup: {backup}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
