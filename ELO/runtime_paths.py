"""Default ELO file locations, stdlib only.

Why a separate module: `ELO/rebase_runtime_model_state.py` must take its writer locks
BEFORE importing `live_team_strength` (a multi-second import that `timeout` can interrupt),
yet the locks live in the directories of the default output paths. `live_team_strength`
takes these very constants from here, so there is one definition.
"""
from __future__ import annotations

import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_SNAPSHOT_PATH = Path(__file__).resolve().parent / "output" / "live_team_elo_snapshot.json"
DEFAULT_RUNTIME_PROGRESS_PATH = _ROOT / "runtime" / "live_elo_progress.json"
DEFAULT_RUNTIME_MODEL_STATE_PATH = _ROOT / "runtime" / "live_elo_model_state.json"
DEFAULT_LIVE_DELTA_PATH = _ROOT / "runtime" / "live_elo_delta.json"


def live_delta_path(default: Path = DEFAULT_LIVE_DELTA_PATH) -> Path:
    """Env `LIVE_ELO_DELTA` (with `~` expanded), else `default`."""
    env = os.getenv("LIVE_ELO_DELTA")
    return Path(env).expanduser() if env else default
