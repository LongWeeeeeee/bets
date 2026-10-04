"""Exact legacy snapshots from 6ac173b3, using a small frozen input corpus.

PREMATCH_ARTIFACT_TEST_SCRIPT selects a temporary mutant for the RED check.
The fixture repeats real player statistics with controlled identities and dates;
provenance.json records its construction and the unmodified golden producer.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import numpy as np
import pytest


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures/prematch_artifact"
SCRIPT = Path(os.environ.get(
    "PREMATCH_ARTIFACT_TEST_SCRIPT",
    str(ROOT / "scripts/pro_chain/build_prematch_artifact.py"),
))


def run_builder(tmp_path, snapshot, cutoff=0, no_accounts=False):
    inputs = tmp_path / "runtime/artifacts/misc"
    inputs.mkdir(parents=True)
    for name in ("pro_corpus_compact.npz", "pro_corpus_rich.npz"):
        shutil.copyfile(FIXTURES / name, inputs / name)
    if no_accounts:
        compact = inputs / "pro_corpus_compact.npz"
        with np.load(compact) as z:
            arrays = {key: z[key] for key in z.files}
        arrays["accounts"][:] = 0
        replacement = inputs / "compact.tmp.npz"
        np.savez_compressed(replacement, **arrays)
        replacement.replace(compact)
    # A mismatched cache must still reach the established training guard.
    np.savez(inputs / "pro_features_ext.npz", F=np.zeros((1, 1)))
    env = dict(os.environ, DRAFT_ROOT=str(tmp_path),
               PREMATCH_SNAPSHOT_ONLY=str(snapshot), PREMATCH_CUTOFF=str(cutoff),
               PYTHONPYCACHEPREFIX="/private/tmp/pm-pyc",
               PYTHONPATH=os.pathsep.join((str(ROOT / "scripts/pro_chain"),
                                         str(ROOT / "base"))))
    return subprocess.run([sys.executable, str(SCRIPT)], env=env,
                          capture_output=True, text=True, timeout=30)


@pytest.mark.parametrize("snapshot,cutoff,golden,no_accounts", [
    (1, 0, "golden_snapshot.npz", False),
    (0, 1609504000, "golden_cutoff.npz", False),
    (1, 0, "golden_no_accounts.npz", True),
])
def test_legacy_arrays(tmp_path, snapshot, cutoff, golden, no_accounts):
    process = run_builder(tmp_path, snapshot, cutoff, no_accounts)
    assert process.returncode == 0, process.stdout + process.stderr
    suffix = "_cut" + str(cutoff) if cutoff else ""
    output = tmp_path / ("runtime/artifacts/misc/prematch_model_artifact" + suffix + ".npz")
    with np.load(FIXTURES / golden) as expected, np.load(output) as actual:
        assert actual.files == expected.files
        for key in expected.files:
            reference, result = expected[key], actual[key]
            assert result.dtype == reference.dtype, "dtype mismatch: " + key
            assert result.shape == reference.shape, "shape mismatch: " + key
            assert np.array_equal(result, reference), "value/order mismatch: " + key


def test_training_cache_guard(tmp_path):
    process = run_builder(tmp_path, 0)
    assert process.returncode != 0
    assert "РАССИНХРОН КОРПУСА И КЭШЕЙ" in process.stderr
    assert not (tmp_path / "runtime/artifacts/misc/prematch_model_artifact.npz").exists()


def test_fixture_covers_history_boundaries():
    metadata = json.loads((FIXTURES / "provenance.json").read_text())
    with np.load(FIXTURES / "pro_corpus_compact.npz") as c:
        accounts = c["accounts"]
        before = accounts[c["ts"] < metadata["cutoff"]]
        ids, counts = np.unique(before[before > 0], return_counts=True)
        assert len(ids) and counts.max() > 50
        assert (accounts == 0).any() and (accounts < 0).any()
        teams = c["teams"]
        assert ((teams[:, 0] == 100) & (teams[:, 1] == 200)).any()
        assert ((teams[:, 0] == 200) & (teams[:, 1] == 100)).any()
        assert c["ts"].max() - c["ts"].min() > 30 * 86400
    with np.load(FIXTURES / "golden_snapshot.npz") as g:
        assert len(g["h2h"]) > 0
        # A first-match-only account exercises pos_cnt == 0 and empty residuals.
        row = g["accounts"][g["accounts"][:, 0] == 1000][0]
        assert row[2] == 1 and np.array_equal(row[8:11], np.zeros(3))
