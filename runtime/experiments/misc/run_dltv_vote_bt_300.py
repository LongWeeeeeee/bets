#!/usr/bin/env python3
"""Run DLTV draft-vote backtest until 300 scored matches with likes."""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

ROOT = Path("/root/main")
script = ROOT / "runtime" / "dltv_draft_vote_backtest_tier1.py"
out = ROOT / "runtime" / "dltv_draft_vote_backtest_tier1_n300.json"

sys.argv = [
    str(script),
    "--limit",
    "1013",
    "--target-scored",
    "300",
    "--sleep",
    "0.12",
]
# monkeypatch OUT_JSON before main runs
ns = runpy.run_path(str(script), run_name="__not_main__")
ns["OUT_JSON"] = out
sys.exit(ns["main"]())
