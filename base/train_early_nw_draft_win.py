#!/usr/bin/env python3
"""Train Early NW from fresh raw maps, including the no-marker outcome.

--corpus accepts canonical rows; otherwise --source is rebuilt from raw JSON.
--out remains an alias of --output-dir. Outputs are versioned offline bundles,
not the legacy binary production artifact.
"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from base.train_draft_phase_models import main as _train


def main():
    _train(default_models=["early_nw"])


if __name__ == "__main__":
    main()
