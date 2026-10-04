"""Real-map fixture and frozen legacy golden; no full corpus needed.

ADD_ORG_IDENTITY_TEST_SCRIPT selects a temporary mutant for a RED check.
Fixture provenance and coverage counts are in fixtures/add_org_identity/.
"""
import os
from pathlib import Path
import shutil
import subprocess
import sys

import numpy as np


FIXTURES = Path(__file__).parent / "fixtures/add_org_identity"
SCRIPT = Path(os.environ.get(
    "ADD_ORG_IDENTITY_TEST_SCRIPT",
    str(Path(__file__).resolve().parents[1] / "scripts/pro_chain/add_org_identity.py"),
))


def test_legacy_arrays(tmp_path):
    output_dir = tmp_path / "runtime/artifacts/misc"
    output_dir.mkdir(parents=True)
    shutil.copyfile(FIXTURES / "compact.npz", output_dir / "pro_corpus_compact.npz")
    shutil.copyfile(FIXTURES / "src.npz", output_dir / "prematch_model_artifact_v2.npz")
    output = output_dir / "prematch_model_artifact_v3.npz"
    env = dict(os.environ, DRAFT_ROOT=str(tmp_path), PYTHONDONTWRITEBYTECODE="1")
    process = subprocess.run([sys.executable, str(SCRIPT)], env=env, check=True,
                             capture_output=True, text=True, timeout=20)
    with np.load(FIXTURES / "golden.npz") as expected, np.load(output) as actual:
        assert actual.files == expected.files, "key/order mismatch"
        for key in expected.files:
            reference, result = expected[key], actual[key]
            assert result.dtype == reference.dtype, "dtype mismatch: " + key
            assert result.shape == reference.shape, "shape mismatch: " + key
            assert np.array_equal(result, reference), "value/order mismatch: " + key
    assert process.stdout == (
        "\nорганизаций: 1,372 из 1,411 team_id\n"
        "составов организаций: 1,372; пар с историей: 788\n"
        "сохранено: " + str(output) + "\n"
    )
