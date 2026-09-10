import json

import numpy as np
import pytest

from base.tools.evaluate_phase_forward import evaluate, paired_intervals
from base.tools.export_draft_phase_serving import FILES


def test_pairing_keeps_daily_counts_and_original_predictions():
    y = np.array([1, 0, 1, 0])
    p = np.array([.8, .3, .7, .2])
    result = paired_intervals(y, p, p, np.array([0, 1, 2, 86400]))
    assert result["loss_delta"] == result["accuracy_delta"] == 0
    assert result["loss_delta_ci95"] == result["accuracy_delta_ci95"] == [0, 0]
    assert [d["rows"] for d in result["daily"]] == [3, 1]


def test_confidence_tie_matches_binary_argmax_dire():
    result = paired_intervals([0, 1], [.4, .6], [.5, .5], [0, 86400])
    assert result["accuracy_delta"] == -.5
    assert result["loss_delta"] > 0


def test_one_day_does_not_get_a_fake_interval():
    result = paired_intervals([1], [.6], [.7], [123])
    assert "interval_unavailable" in result
    assert "loss_delta_ci95" not in result
    with pytest.raises(ValueError, match="equally sized"):
        paired_intervals([1, 0], [.6], [.7], [123])


def test_partial_forward_run_cannot_silently_consume_again(tmp_path):
    (tmp_path / "forward_started.json").write_text('{"started_at": 123}')
    with pytest.raises(FileExistsError, match="already started"):
        evaluate(tmp_path / "missing_candidates", tmp_path / "unread_holdout",
                 tmp_path / "missing_production", tmp_path / "missing_audit", tmp_path)


def test_modified_report_rejected_before_reading_holdout(tmp_path):
    candidates, holdout = tmp_path / "candidates", tmp_path / "holdout"
    candidates.mkdir()
    holdout.mkdir()
    (holdout / "manifest.json").write_text(json.dumps({
        "complete": True, "public_rows_sha256": "public",
        "selection": {"fresh_after_ts": 123},
    }))
    audit = tmp_path / "audit.json"
    audit.write_text('{"status": "PASS"}')
    (candidates / "status.json").write_text(json.dumps({
        "status": "DONE", "identity": {
            "corpus_sha256": "public", "fresh_after_ts": 123,
        }, "phases": {phase: {"results_sha256": "frozen"} for phase in FILES},
    }))
    phase = next(iter(FILES))
    (candidates / phase).mkdir()
    (candidates / phase / "results.json").write_text('{"modified": true}')
    # No rows.npz or models exist: touching either before rejection would fail.
    output = tmp_path / "output"
    with pytest.raises(ValueError, match="differs from frozen DONE"):
        evaluate(candidates, holdout, tmp_path / "production", audit, output)
    assert not (output / "forward_started.json").exists()
