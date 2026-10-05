"""Real time-ordered prefix; isolated inputs and snapshot output.

RATING_SNAPSHOT_TEST_SCRIPT can select the old script for the red regression
check. Its report predates per-column verdicts, so only that schema check is
skipped when selecting an alternate script.
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
FIXTURES = Path(__file__).parent / 'fixtures/build_rating_snapshot'
SCRIPT = Path(os.environ.get(
    'RATING_SNAPSHOT_TEST_SCRIPT',
    str(ROOT / 'scripts/pro_chain/build_rating_snapshot.py'),
))
PROVENANCE = json.loads((FIXTURES / 'provenance.json').read_text())
KEYS = ('glicko', 'glicko_p', 'glicko_rd', 'pos_glicko',
        'trueskill', 'trueskill_sig')
TOLERANCES = (1e-9, 1e-9, 1e-9, 1e-9, 1e-5, 1e-5)


@pytest.mark.parametrize('case', PROVENANCE['cases'],
                         ids=[c['name'] for c in PROVENANCE['cases']])
def test_snapshot_write_boundary(tmp_path, case):
    art = tmp_path / 'runtime/artifacts/misc'
    art.mkdir(parents=True)
    shutil.copyfile(FIXTURES / 'compact.npz', art / 'pro_corpus_compact.npz')
    with np.load(FIXTURES / 'reference.npz') as z:
        reference = {key: z[key].copy() for key in z.files}
    for key, shift in case['shifts'].items():
        reference[key][PROVENANCE['perturbation_row']] += shift
    np.savez_compressed(art / 'undercount_glicko_features.npz', **reference)
    output = tmp_path / 'data/rating_snapshot.npz'
    env = dict(os.environ, DRAFT_ROOT=str(tmp_path),
               PANEL_RATING_SNAPSHOT=str(output),
               PYTHONPYCACHEPREFIX=(os.environ.get('PYTHONPYCACHEPREFIX')
                                    or str(tmp_path / 'pycache')),
               PYTHONPATH=str(ROOT / 'base'))
    subprocess.run([sys.executable, str(SCRIPT)], env=env, check=True,
                   capture_output=True, text=True, timeout=25)
    assert output.exists() == case['written'], (
        case['name'] + ': snapshot write boundary violated'
    )
    report = (art / 'build_rating_snapshot.md').read_text()
    rows = [line.split('|')[1:-1] for line in report.splitlines()
            if line.startswith('| rating_')]
    assert [row[1].strip() for row in rows] == list(KEYS)
    failed = set()
    for row, key, tolerance in zip(rows, KEYS, TOLERANCES):
        delta = float(row[2])
        if delta > tolerance:
            failed.add(key)
            assert int(row[3]) == PROVENANCE['perturbation_row']
        if 'RATING_SNAPSHOT_TEST_SCRIPT' not in os.environ:
            assert float(row[4]) == tolerance, key
            assert row[5].strip() == ('FAIL' if delta > tolerance else 'PASS'), key
    assert failed == (set() if case['written'] else set(case['shifts']))
    if case['written']:
        assert 'Снимок записан:' in report
        assert 'порт совпал с обучением' in report
        assert 'После эталона допроведено compact-карт: 3 ' in report
        with np.load(output) as snapshot:
            assert int(snapshot['built_ts']) == PROVENANCE['newest_ts']
    else:
        assert 'Снимок НЕ записан:' in report
        assert 'РАСХОЖДЕНИЕ' in report
