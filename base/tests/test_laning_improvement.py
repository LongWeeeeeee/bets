import json
from types import SimpleNamespace

import numpy as np
import pytest

from scripts.ops.improve_laning_model import (
    calibration, fit_temperature, paired_comparison, partition_v2, run, temperature_scale,
)


def test_new_confirmation_excludes_old_test_and_preserves_temporal_purge():
    n = 2000
    corpus = {'mid': np.arange(n) + 1, 'ts': np.arange(n) * 60 + 1000,
              'duration': np.full(n, 600), 'lane_labels': np.ones((n, 3), dtype=int)}
    previous = {'mid': corpus['mid'][[0, 1601, 1602, 1700]], 'test_offset': 1}
    rows, small, info = partition_v2(corpus, previous, 300, 200, 100)
    train, validation, test = np.split(rows, [300, 400])
    assert len(small) == 200 and np.all(small < 300)
    assert not set(corpus['mid'][test]) & set(previous['mid'][1:])
    assert np.all(corpus['ts'][train] + 600 < info['cuts_unix'][0])
    assert np.all(corpus['ts'][validation] + 600 < info['cuts_unix'][1])
    assert np.all(corpus['ts'][test] >= info['cuts_unix'][1])


def test_temperature_improves_validation_loss_and_preserves_normalization():
    y = np.tile(np.arange(5), 20)
    p = np.full((100, 5), .15)
    p[np.arange(100), y] = .4
    t = fit_temperature(y, p)
    assert .6 <= t < 1
    calibrated = temperature_scale(p, t)
    np.testing.assert_allclose(calibrated.sum(axis=1), 1)
    assert calibration(y, calibrated)['brier_multiclass_sum'] < calibration(y, p)['brier_multiclass_sum']
    np.testing.assert_array_equal(calibrated.argmax(axis=1), p.argmax(axis=1))


def test_paired_comparison_keeps_direction_and_lanes():
    y = np.zeros((30, 3), dtype=int)
    baseline = np.full((30, 3, 5), .2)
    candidate = baseline.copy()
    candidate[..., 0] = .4
    candidate[..., 1:] = .15
    result = paired_comparison(np.arange(30) * 86400, y, baseline, candidate)
    assert result['days'] == 30
    assert result['overall']['candidate_minus_baseline_loss'] == pytest.approx(-np.log(2))
    assert result['overall']['day_block_95ci'][1] < 0
    assert set(result['lanes']) == {'easy', 'mid', 'hard'}


def test_v2_end_to_end_selection_and_new_id_confirmation(tmp_path):
    from scripts.ops.train_laning_model import train
    corpus = tmp_path / 'corpus'
    corpus.mkdir()
    n = 1500
    rng = np.random.default_rng(265)
    np.savez(corpus / 'rows.npz', mid=np.arange(n) + 1, ts=np.arange(n) * 3600 + 1000,
        duration=np.full(n, 600), heroes=np.tile(np.arange(1, 11), (n, 1)),
        accounts=np.tile(np.arange(101, 111), (n, 1)),
        lane_labels=rng.integers(0, 5, (n, 3)), team_nw10=np.full(n, np.nan))
    (corpus / 'manifest.json').write_text(json.dumps({'complete': True, 'rows': n}))
    baseline = tmp_path / 'v1'
    train(SimpleNamespace(corpus=corpus, output_dir=baseline, train_maps=200,
                          eval_maps=100, iterations=2, threads=1))
    output = tmp_path / 'v2'
    run(SimpleNamespace(corpus=corpus, baseline=baseline, output_dir=output,
        train_maps=300, small_train_maps=200, eval_maps=100, iterations=2, threads=1))
    result = json.loads((output / 'summary.json').read_text())
    assert result['complete'] and not result['full_refit']
    assert result['reload_max_delta'] < 1e-12
    assert set(result['validation']['validation']) == {'v1', 'longer', 'context_recent'}
    with np.load(baseline / 'test_predictions.npz') as old, np.load(output / 'confirmation_predictions.npz') as new:
        assert not np.intersect1d(old['mid'], new['mid']).size
    assert (output / 'selection.json').stat().st_mtime_ns <= (output / 'confirmation_predictions.npz').stat().st_mtime_ns
