"""Frozen legacy outputs from HEAD 81f12016; no corpus or Git needed at test time.

PRO_CORPUS_RICH_TEST_SCRIPT can point at a temporary mutant for a RED check.
"""
import os
from pathlib import Path
import shutil
import subprocess
import sys

import numpy as np
import pytest


FIXTURES = Path(__file__).parent / "fixtures/pro_corpus_rich"
SCRIPT = Path(os.environ.get(
    "PRO_CORPUS_RICH_TEST_SCRIPT",
    str(Path(__file__).resolve().parents[1] / "scripts/pro_chain/pro_corpus_rich.py"),
))


@pytest.mark.parametrize("parts,golden", [
    (["7.41e_part063.json"], "golden_real.npz"),
    (["7.41e_part064.json.gz"], "golden_real.npz"),
    (["7.41e_part062.json", "7.41e_part063.json", "7.41e_part064.json.gz"], "golden.npz"),
    ([], "golden_empty.npz"),
])
def test_legacy_arrays(tmp_path, parts, golden):
    corpus = tmp_path / "pro_heroes_data/json_parts_split_from_object"
    corpus.mkdir(parents=True)
    for name in parts:
        shutil.copyfile(FIXTURES / name, corpus / name)
    # Corrupt files must still be skipped by the existing json.load guard.
    (corpus / "broken_part999.json").write_text("{invalid")
    env = dict(os.environ, DRAFT_ROOT=str(tmp_path), PYTHONDONTWRITEBYTECODE="1")
    subprocess.run([sys.executable, str(SCRIPT)], env=env, check=True,
                   capture_output=True, text=True, timeout=20)
    output = tmp_path / "runtime/artifacts/misc/pro_corpus_rich.npz"
    with np.load(FIXTURES / golden) as expected, np.load(output) as actual:
        assert actual.files == expected.files
        for key in expected.files:
            reference, result = expected[key], actual[key]
            assert result.dtype == reference.dtype, "dtype mismatch: " + key
            assert result.shape == reference.shape, "shape mismatch: " + key
            assert np.array_equal(result, reference), "value/order mismatch: " + key
    assert not list(output.parent.glob(".pro_corpus_rich-*"))
