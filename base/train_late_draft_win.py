#!/usr/bin/env python3
"""Train draft-only winners on maps lasting at least 36 minutes.

Reads fresh raw maps by default, not the old kills-model NPZ shards.
--out and --min-minutes=36 remain accepted. Evaluation and full-fit bundles
are separate and never replace the current production artifact.
"""
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from base.train_draft_phase_models import main as _train


def main():
    legacy = argparse.ArgumentParser(add_help=False)
    legacy.add_argument("--min-minutes", type=int, choices=[36], default=36)
    _, remaining = legacy.parse_known_args()
    _train(remaining, default_models=["late"])


if __name__ == "__main__":
    main()
