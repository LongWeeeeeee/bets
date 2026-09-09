import json

import numpy as np
import pytest

from base.laning_history_store import LaningHistoryStore
from base.laning_model import build_history
from base.tools.export_laning_history import export_laning_history


def _corpus(tmp_path):
    directory = tmp_path / "corpus"
    directory.mkdir()
    # Row 2 ends exactly at row 3's start and must be excluded at that cutoff.
    arrays = {
        "mid": np.arange(1, 6),
        "ts": np.array([100, 1000, 1400, 2000, 5000], dtype=np.int64),
        "duration": np.array([600, 600, 600, 600, 600], dtype=np.int32),
        "heroes": np.tile(np.arange(1, 11), (5, 1)),
        "accounts": np.tile(np.arange(101, 111), (5, 1)),
        "lane_labels": np.array([[4, 1, 0], [0, 4, 4], [1, 0, 3], [2, 2, 2],
                                  [3, 2, 1]], dtype=np.int8),
    }
    # The same account plays another hero in row 2: role and role+hero must diverge.
    arrays["heroes"][1, 0] = 11
    np.savez(directory / "rows.npz", **arrays)
    (directory / "manifest.json").write_text(json.dumps({"complete": True, "rows": 5,
                                                            "rows_file": "rows.npz"}))
    return directory, arrays


def test_exported_history_matches_build_history_with_dire_and_boundaries(tmp_path):
    corpus_dir, corpus = _corpus(tmp_path)
    output = tmp_path / "history"
    manifest = export_laning_history(corpus_dir, output)
    store = LaningHistoryStore(output)

    assert manifest["complete"] and manifest["counts"]["rows"] == 5
    assert manifest["source_fingerprint"] and manifest["max_end_ts"] == 5600
    assert len(manifest["files_sha256"]) == 25
    assert isinstance(store._roles[0]["end_ts"], np.memmap)
    # At 2000, the game ending at 2000 is strictly unavailable.  The recent
    # lower edge at 700 includes the first game (end == 700).
    expected = build_history(corpus, np.array([3]), availability_delay_seconds=0,
                             recent_window_seconds=1300)[0]
    actual = store.history(corpus["heroes"][3], corpus["accounts"][3], 2000,
                           availability_delay_seconds=0, recent_window_seconds=1300)
    np.testing.assert_allclose(actual, expected)
    assert actual.dtype == np.float32
    assert actual[0, 2] > actual[0, 5]  # The hero mismatch is not counted as hero history.
    # The exact lower recent boundary includes both games ending at 700 and 1600.
    assert actual[0, 8] == pytest.approx(np.log(3))
    # Dire role-1's first result is flipped into its own-side perspective.
    dire_only = store.history(corpus["heroes"][1], corpus["accounts"][1], 1000,
                              availability_delay_seconds=0, recent_window_seconds=1000)
    assert dire_only[5, 0] > 0


def test_default_live_history_matches_build_history(tmp_path):
    corpus_dir, corpus = _corpus(tmp_path)
    output = tmp_path / "history"
    export_laning_history(corpus_dir, output)
    expected = build_history(corpus, np.array([4]), availability_delay_seconds=3600,
                             recent_window_seconds=2592000)[0]
    actual = LaningHistoryStore(output).history(corpus["heroes"][4], corpus["accounts"][4],
                                                corpus["ts"][4])
    np.testing.assert_allclose(actual, expected)


def test_unknown_accounts_are_zero_and_export_is_immutable(tmp_path):
    corpus_dir, corpus = _corpus(tmp_path)
    output = tmp_path / "history"
    export_laning_history(corpus_dir, output)
    result = LaningHistoryStore(output).history(corpus["heroes"][0], np.zeros(10, dtype=int), 900)
    assert result.shape == (10, 12)
    assert np.all(result == 0)
    with pytest.raises(FileExistsError, match="already exists"):
        export_laning_history(corpus_dir, output)


def test_incomplete_and_malformed_inputs_fail_closed(tmp_path):
    incomplete = tmp_path / "incomplete"
    incomplete.mkdir()
    (incomplete / "manifest.json").write_text(json.dumps({"schema_version": 1, "complete": False}))
    with pytest.raises(ValueError, match="incomplete"):
        LaningHistoryStore(incomplete)

    corpus_dir, corpus = _corpus(tmp_path)
    store_dir = tmp_path / "history"
    export_laning_history(corpus_dir, store_dir)
    store = LaningHistoryStore(store_dir)
    with pytest.raises(ValueError, match="shape"):
        store.history(np.arange(9), corpus["accounts"][0], 1000)
    with pytest.raises(ValueError, match="timestamp"):
        store.history(corpus["heroes"][0], corpus["accounts"][0], float("nan"))

    (corpus_dir / "manifest.json").write_text(json.dumps({"complete": False, "rows": 5}))
    with pytest.raises(ValueError, match="incomplete"):
        export_laning_history(corpus_dir, tmp_path / "rejected")


def test_reader_rejects_malformed_events_only_when_the_account_is_queried(tmp_path):
    corpus_dir, corpus = _corpus(tmp_path)
    for name in ("end_ts", "heroes", "scores"):
        output = tmp_path / f"history-{name}"
        export_laning_history(corpus_dir, output)
        path = output / f"role_1_{name}.npy"
        values = np.load(path)
        if name == "end_ts":
            values[1] = values[0] - 1
        elif name == "heroes":
            values[0] = 0
        else:
            values[0] = 3
        np.save(path, values)
        store = LaningHistoryStore(output)
        with pytest.raises(ValueError, match="queried account"):
            store.history(corpus["heroes"][3], corpus["accounts"][3], corpus["ts"][3],
                          availability_delay_seconds=0)
