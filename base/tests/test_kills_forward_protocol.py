"""Guard a future cohort against repeated peeking and reused map/series IDs."""
import json
from pathlib import Path
import sys

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kills_forward_protocol import check, cohort_window
from kills_transfer_data import sha256


def protocol(tmp_path):
    exposed = tmp_path / "exposed.npz"
    np.savez(exposed, mids=[1], sids=[10])
    path = tmp_path / "protocol.json"
    path.write_text(json.dumps(dict(files={str(exposed): sha256(exposed)}, exposed_ids=str(exposed),
                                    cohort_start=1000, cohort_end=2000, panel_artifact_sha256="frozen")))
    return path


def test_window_starts_next_utc_day_and_does_not_move_with_check():
    assert cohort_window(86400) == (172800, 2764800)
    assert cohort_window(86400 + 86399) == cohort_window(86400)


def test_early_check_does_not_open_future_corpus_or_journal(tmp_path):
    p = protocol(tmp_path)
    result = check(p, tmp_path / "does-not-exist.npz", tmp_path / "no-journal", now=1999)
    assert result["status"] == "WAITING_WINDOW_END"
    assert not result["labels_read"]


def test_modified_frozen_input_is_rejected(tmp_path):
    p = protocol(tmp_path)
    (tmp_path / "exposed.npz").write_bytes(b"modified")
    with pytest.raises(ValueError, match="Frozen input changed"):
        check(p, now=3000)


def test_only_new_series_with_prestart_bound_forecasts_can_enter(tmp_path):
    p = protocol(tmp_path)
    rows = tmp_path / "future.npz"
    # 1 consumed map, 2 consumed series, 3/4 cross boundary, 5 eligible,
    # 6 lacks bound model, 7 only in-play, 8 unknown series.
    np.savez(rows, mids=[1, 2, 3, 4, 5, 6, 7, 8], sids=[11, 10, 12, 12, 13, 14, 15, 0],
             ts=[1200, 1200, 900, 1200, 1200, 1200, 1200, 1200],
             ends=[1500, 1500, 950, 1500, 1500, 1500, 1500, 1500])
    journal = tmp_path / "journal.json"
    journal.write_text(json.dumps({"rows": [dict(map_id=mid, ts=1250 if mid == 7 else 1100,
        panel_artifact_sha256="unbound" if mid == 6 else "frozen", models=[
        dict(key="total_55_50", p=.5), dict(key="rad_30_25", p=.5)]) for mid in range(1, 9)]}))
    result = check(p, rows, journal, now=3000)
    assert result["eligible_maps"] == 3
    assert result["paired_maps"] == 1
    assert result["status"] == "READY_FOR_ONE_SHOT_EVALUATION"
    assert not result["labels_read"]
