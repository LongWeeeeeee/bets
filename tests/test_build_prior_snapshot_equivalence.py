"""Exact standalone OLD outputs on real corpus windows (see fixture provenance).

PRIOR_SNAPSHOT_TEST_SCRIPT accepts an alternate script for mutation checks.
No full corpus, main checkout, network, or Git is needed at test time.
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
FIXTURES = Path(__file__).parent / 'fixtures/build_prior_snapshot'
SCRIPT = Path(os.environ.get(
    'PRIOR_SNAPSHOT_TEST_SCRIPT',
    str(ROOT / 'scripts/pro_chain/build_prior_snapshot.py'),
))
CASES = json.loads((FIXTURES / 'provenance.json').read_text())['cases']


@pytest.mark.parametrize('case', CASES, ids=[c['name'] for c in CASES])
def test_standalone_old_arrays(tmp_path, case):
    art = tmp_path / 'runtime/artifacts/misc'
    art.mkdir(parents=True)
    for source, target in [('compact.npz', 'pro_corpus_compact.npz'),
                           ('rich.npz', 'pro_corpus_rich.npz')]:
        shutil.copyfile(FIXTURES / source, art / target)
    output = tmp_path / 'data/prior_snapshot.npz'
    # Input root is isolated; imports use repository code without copying it.
    env = dict(os.environ, DRAFT_ROOT=str(tmp_path),
               PRIOR_SNAPSHOT=str(output), SNAPSHOT_CUTOFF=str(case['cutoff']),
               SNAPSHOT_MIN_GAMES=str(case['min_games']),
               PYTHONPYCACHEPREFIX='/private/tmp/ps-pyc',
               PYTHONPATH=os.pathsep.join([str(ROOT / 'scripts/pro_chain'),
                                         str(ROOT / 'base')]))
    subprocess.run([sys.executable, str(SCRIPT)], env=env, check=True,
                   capture_output=True, text=True, timeout=25)
    with np.load(FIXTURES / ('golden_' + case['name'] + '.npz')) as expected, \
            np.load(output) as actual:
        assert actual.files == expected.files, 'key/order mismatch'
        for key in expected.files:
            reference, result = expected[key], actual[key]
            assert result.dtype == reference.dtype, 'dtype mismatch: ' + key
            assert result.shape == reference.shape, 'shape mismatch: ' + key
            assert np.array_equal(result, reference), 'value/order mismatch: ' + key
    assert not (output.parent / 'prior_snapshot.tmp.npz').exists()
