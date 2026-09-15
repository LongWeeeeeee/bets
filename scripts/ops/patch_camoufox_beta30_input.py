#!/usr/bin/env python3
"""Build a separate beta.30 omni.ja with the upstream 35b45c1 input fix.

Never writes the source archive. Requires the exact upstream JS files, obtained
from https://github.com/daijro/camoufox/tree/35b45c145c1a04504fe8cacb42ddc03f4cd2ca93.
The output is a local patched build, not the unmodified official release.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import zipfile


UPSTREAM = "35b45c145c1a04504fe8cacb42ddc03f4cd2ca93"
# Relative to additions/juggler and chrome/juggler/content, respectively.
FILES = {
    "Helper.js": (
        "4875ec793cf2bda6f084fe4c0722eb3ddf948c1de1b19a5f0b0c91ceda196948",
        "967cc16ef6a41964c7b15fb9f8e36c2518351c7950663e3db5eb00319f8b5ca0",
    ),
    "TargetRegistry.js": (
        "01c55e3aad7b2e2d1076091731c1bafdd156b2b2a7a6226ab531026fcd92a7e1",
        "abf12c31264c7861b03633c6ba9e70f18f82c03265595ed48e11d9a6e424a31d",
    ),
    "protocol/PageHandler.js": (
        "6ed7718164bd5532e40170193d2d3915dfcba55f0a372c2f4a26dfc90f6e1b8d",
        "2a27e304eb93cd853c0cb470a6dae908ef55480e8e51380f31e92c43bb6a44e0",
    ),
    "input/MouseDispatch.js": (
        None,
        "63755d1f453094586765fb010375f98bc8a1ad334dc4a0bfefefac970b5c3bbc",
    ),
}
PREFIX = "chrome/juggler/content/"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def build(source, upstream_dir, output):
    source, output = source.resolve(), output.resolve()
    temporary = output.with_name(output.name + ".tmp")
    if source == output or output.exists() or temporary.exists():
        raise ValueError("Use a new output path; source/output/temp must not overlap")
    replacements = {}
    for relative, (_, expected) in FILES.items():
        data = (upstream_dir / relative).read_bytes()
        if digest(data) != expected:
            raise ValueError(f"Upstream hash mismatch: {relative}")
        replacements[PREFIX + relative] = data

    original_hashes = {}
    with zipfile.ZipFile(source) as original:
        names = original.namelist()
        if len(names) != len(set(names)):
            raise ValueError("Duplicate entries in source archive")
        for relative, (expected, _) in FILES.items():
            name = PREFIX + relative
            actual = digest(original.read(name)) if name in names else None
            if actual != expected:
                raise ValueError(f"Source is not the inspected beta.30: {relative}")
        with zipfile.ZipFile(temporary, "x", compression=zipfile.ZIP_DEFLATED) as patched:
            patched.comment = original.comment
            for info in original.infolist():
                data = original.read(info.filename)
                original_hashes[info.filename] = digest(data)
                patched.writestr(info, replacements.get(info.filename, data))
            for name, data in replacements.items():
                if name not in original_hashes:
                    patched.writestr(name, data)

    # Verify every entry, not just the replacements; preserve unrelated payloads.
    with zipfile.ZipFile(temporary) as patched:
        expected_names = set(original_hashes) | set(replacements)
        if set(patched.namelist()) != expected_names:
            raise ValueError("Patched archive entry set changed unexpectedly")
        for name in patched.namelist():
            expected = (digest(replacements[name]) if name in replacements
                        else original_hashes[name])
            if digest(patched.read(name)) != expected:
                raise ValueError(f"Patched entry verification failed: {name}")
    os.chmod(temporary, source.stat().st_mode & 0o777)
    os.replace(temporary, output)
    return {
        "upstream_commit": UPSTREAM,
        "source_sha256": digest(source.read_bytes()),
        "output_sha256": digest(output.read_bytes()),
        "changed_entries": sorted(replacements),
        "verified_entries": len(expected_names),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("upstream_dir", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    print(json.dumps(build(args.source, args.upstream_dir, args.output), indent=2))
