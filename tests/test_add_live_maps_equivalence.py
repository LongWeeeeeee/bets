"""Legacy arrays from 3de5a5af, on 64 real maps spanning TEST_FROM.

ADD_LIVE_MAPS_TEST_SCRIPT selects a temporary mutant for a RED check.
"""
import os
from pathlib import Path
import shutil
import subprocess
import sys

import numpy as np
import pytest


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / 'fixtures/add_live_maps'
SCRIPT = Path(os.environ.get('ADD_LIVE_MAPS_TEST_SCRIPT',
                            str(ROOT / 'scripts/pro_chain/add_live_maps.py')))


@pytest.mark.parametrize('tag,boundary', [
    ('default', 1774742400), ('empty', 0), ('late', 2000000000),
])
def test_legacy_arrays(tmp_path, tag, boundary):
    artifacts = tmp_path / 'runtime/artifacts/misc'
    artifacts.mkdir(parents=True)
    for name in ('pro_corpus_compact.npz', 'pro_corpus_rich.npz'):
        shutil.copyfile(FIXTURES / name, artifacts / name)
    # Boundary override is confined to this harness; production CLI is unchanged.
    runner = (
        'import runpy, sys; '
        'ns = runpy.run_path(sys.argv[1]); '
        "ns['main'].__globals__['TEST_FROM'] = int(sys.argv[2]); "
        "ns['main']()"
    )
    env = dict(os.environ, DRAFT_ROOT=str(tmp_path),
               PYTHONPYCACHEPREFIX='/private/tmp/lm-pyc',
               PYTHONPATH=os.pathsep.join((str(ROOT / 'scripts/pro_chain'),
                                          str(ROOT / 'base'))))
    subprocess.run([sys.executable, '-c', runner, str(SCRIPT), str(boundary)],
                   env=env, check=True, capture_output=True, text=True, timeout=20)
    outputs = ('full',) if tag == 'late' else ('full', 'at_test')
    if tag == 'late':
        assert not (artifacts / 'live_maps_at_test.npz').exists()
    for suffix in outputs:
        with np.load(FIXTURES / ('golden_' + tag + '_' + suffix + '.npz')) as expected, \
                np.load(artifacts / ('live_maps_' + suffix + '.npz')) as actual:
            assert actual.files == expected.files
            for key in expected.files:
                reference, result = expected[key], actual[key]
                assert result.dtype == reference.dtype, 'dtype mismatch: ' + key
                assert result.shape == reference.shape, 'shape mismatch: ' + key
                assert np.array_equal(result, reference), 'value/order mismatch: ' + suffix + '/' + key
