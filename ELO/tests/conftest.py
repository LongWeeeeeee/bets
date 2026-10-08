from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def _isolate_live_delta_path(tmp_path, monkeypatch):
    """The rebase CLI locks the directory of the live delta too (LIVE_ELO_DELTA, default
    runtime/). Without this, any test that calls it with a scratch --state would create
    the lock file in the repo's own runtime/. Tests that need a specific delta set it."""
    monkeypatch.setenv("LIVE_ELO_DELTA", str(tmp_path / "live_elo_delta.default.json"))
