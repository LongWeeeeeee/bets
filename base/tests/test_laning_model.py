import numpy as np
import pytest

from base.laning_model import PLAYER_LANES, build_history, lane_features, prior_history


def test_expected_lane_topology_and_missing_mid_support():
    frame = lane_features(np.arange(1, 11).reshape(1, 10))
    assert frame[['own_core', 'own_support', 'opp_core', 'opp_support']].values.tolist() == [
        ['1', '5', '8', '9'], ['2', '0', '7', '0'], ['3', '4', '6', '10']]
    with pytest.raises(ValueError, match='distinct'):
        lane_features(np.ones((1, 10), dtype=int))


def test_history_strict_completion_cutoff_and_unknown_accounts():
    keys = np.array([11, 11, 22, 0])
    end = np.array([10, 20, 5, 1])
    score = np.array([2., -2., -1., 2.])
    out = prior_history(keys, end, score, np.array([11, 11, 11, 22, 0, 99]),
                        np.array([10, 20, 21, 6, 100, 100]))
    assert np.all(out[[0, 4, 5]] == 0)
    assert out[1, 0] == pytest.approx(2 / 11)
    assert out[2, 0] == 0
    assert out[2, 2] == pytest.approx(np.log(3))
    assert out[3, 0] == pytest.approx(-1 / 11)


def test_player_history_perspective_future_invariance_and_overlap():
    corpus = {'ts': np.array([100, 1000, 2000]), 'duration': np.array([600, 1500, 600]),
              'heroes': np.tile(np.arange(1, 11), (3, 1)),
              'accounts': np.tile(np.arange(101, 111), (3, 1)),
              'lane_labels': np.array([[4, 1, 0], [0, 4, 4], [2, 2, 2]])}
    history = build_history(corpus, np.array([0, 1, 2]))
    assert np.all(history[0] == 0)
    expected = (corpus['lane_labels'][0, PLAYER_LANES] - 2) / 11
    expected[5:] *= -1
    np.testing.assert_allclose(history[1, :, 0], expected)
    # Map 2 has not ended when map 3 starts; its outcomes must not appear.
    np.testing.assert_allclose(history[2], history[1])
    corpus['lane_labels'][2] = [0, 0, 0]
    np.testing.assert_array_equal(history, build_history(corpus, np.arange(3)))


def test_serialized_model_boundary(tmp_path):
    from catboost import CatBoostClassifier
    from base.laning_model import LaningModel
    heroes = np.tile(np.arange(1, 11), (10, 1))
    x = lane_features(heroes)
    model = CatBoostClassifier(iterations=2, depth=2, verbose=False, allow_writing_files=False)
    model.fit(x, np.tile(np.arange(5), 6), cat_features=list(x.columns))
    model.save_model(str(tmp_path / 'draft.cbm'))
    np.testing.assert_array_equal(LaningModel.load(tmp_path).predict_proba(heroes),
                                  model.predict_proba(x).reshape(10, 3, 5))


def test_trainer_end_to_end_without_optional_team_networth(tmp_path):
    import json
    from types import SimpleNamespace
    from scripts.ops.train_laning_model import train
    corpus = tmp_path / 'corpus'
    corpus.mkdir()
    n = 600
    rng = np.random.default_rng(42)
    np.savez(corpus / 'rows.npz', mid=np.arange(n) + 1, ts=np.arange(n) * 3600 + 1000,
             duration=np.full(n, 600), heroes=np.tile(np.arange(1, 11), (n, 1)),
             accounts=np.tile(np.arange(101, 111), (n, 1)),
             lane_labels=rng.integers(0, 5, size=(n, 3)), team_nw10=np.full(n, np.nan))
    (corpus / 'manifest.json').write_text(json.dumps({'complete': True, 'rows': n}))
    output = tmp_path / 'model'
    train(SimpleNamespace(corpus=corpus, output_dir=output, train_maps=200,
                          eval_maps=100, iterations=2, threads=1))
    summary = json.loads((output / 'summary.json').read_text())
    assert summary['complete'] and summary['held_out_test']
    assert summary['team_nw_diagnostic']['1500']['direction_accuracy'] is None
    assert summary['split']['sampled_maps'] == [200, 100, 100]
    assert all(summary['validation'][key]['reload_max_delta'] == 0 for key in ('draft', 'history'))


def test_split_keeps_timestamp_groups_and_purges_unfinished_labels():
    from scripts.ops.train_laning_model import splits
    ts = np.repeat(np.arange(500) * 60, 2) + 1000
    corpus = {'mid': np.arange(1000) + 1, 'ts': ts, 'duration': np.full(1000, 600),
              'lane_labels': np.ones((1000, 3), dtype=int)}
    partitions, report = splits(corpus, 10000, 10000)
    train, validation, test = partitions
    cut1, cut2 = report['cuts_unix']
    assert np.all(ts[train] + 600 < cut1)
    assert np.all(ts[validation] + 600 < cut2)
    assert not set(ts[train]) & set(ts[validation])
    assert not set(ts[validation]) & set(ts[test])
    assert set(np.flatnonzero(ts == cut1)) <= set(validation)
    assert set(np.flatnonzero(ts == cut2)) <= set(test)
    assert report['purged_boundary_maps'] > 0
