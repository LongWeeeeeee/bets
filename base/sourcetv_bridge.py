"""Shared SourceTV bridge path resolution."""

import os
from pathlib import Path


def resolve_sourcetv_matches_path(project_root: Path) -> Path:
    """Return the SourceTV bridge path without depending on the process CWD."""
    root = Path(project_root).expanduser().resolve()
    override = os.getenv("SOURCETV_MATCHES_PATH")
    if not override:
        return (root / "runtime" / "sourcetv_matches.json").resolve()

    path = Path(override).expanduser()
    if not path.is_absolute():
        path = root / path
    return path.resolve()
