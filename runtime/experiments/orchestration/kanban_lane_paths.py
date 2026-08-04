"""Shared path discovery for the three isolated Hermes orchestration kanbans."""
from __future__ import annotations

import os
from pathlib import Path


LANE_HOMES = {
    "default": Path("/root/.hermes"),
    "orchestration1": Path("/root/.hermes/profiles/orchestration1"),
    "orchestration2": Path("/root/.hermes/profiles/orchestration2"),
}


def selected_lane() -> str:
    lane = (os.environ.get("HERMES_ORCHESTRATION_LANE") or "default").strip()
    if lane not in LANE_HOMES:
        raise ValueError(f"unknown HERMES_ORCHESTRATION_LANE={lane!r}")
    return lane


def lane_home(lane: str | None = None) -> Path:
    return LANE_HOMES[lane or selected_lane()]


def lane_runtime_root(lane: str | None = None) -> Path:
    name = lane or selected_lane()
    if name == "default":
        return Path("/root/main/runtime")
    return Path("/root/main/runtime/orchestration-guards") / name


def discover_boards(home: Path | None = None) -> list[tuple[str, Path]]:
    """Return the root kanban plus named boards for one lane only."""
    home = home or lane_home()
    out: list[tuple[str, Path]] = []
    root = home / "kanban.db"
    if root.exists():
        out.append(("default", root))
    boards = home / "kanban" / "boards"
    if boards.is_dir():
        for p in sorted(boards.glob("*/kanban.db")):
            out.append((p.parent.name, p))
    return out
