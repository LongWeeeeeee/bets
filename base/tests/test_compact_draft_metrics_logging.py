from __future__ import annotations

import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


def test_compact_draft_metrics_use_early_late_all_labels(capsys) -> None:
    runtime._print_compact_draft_metrics(
        {
            "counterpick_1vs1": 3,
            "counterpick_1vs2": None,
            "solo": 1,
            "synergy_duo": 4,
            "synergy_trio": None,
        },
        {
            "counterpick_1vs1": 1,
            "counterpick_1vs2": None,
            "solo": 0,
            "synergy_duo": -3,
            "synergy_trio": None,
        },
        {
            "counterpick_1vs1": 3,
            "counterpick_1vs2": None,
            "synergy_duo": 1,
            "synergy_trio": -10,
            "dota2protracker_cp1vs1": -1.26,
        },
    )

    output = capsys.readouterr().out

    assert "📊 EARLY (20-28 min): 3, None, 1, 4, None" in output
    assert "📊 LATE (28-60 min): 1, None, 0, -3, None" in output
    assert "📊 ALL: 3, None, N/A, 1, -10, d2pt=-1.26" in output
    assert "📊 LANING (20-28 min)" not in output
