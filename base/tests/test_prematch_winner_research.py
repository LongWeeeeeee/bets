"""Temporal and paired-comparison regressions for general winner retraining."""
import hashlib
import json
from types import SimpleNamespace

import numpy as np
import pytest

from base.tools.prematch_winner_research import (
    RECIPES, _roster_matrix, _roster_numeric_columns, _roster_vocabulary, completed_indices, fit_model,
    indices, load_data, paired_report, predict, refit, train_indices, verify_selection,
)


def test_train_excludes_unfinished_results_and_shared_series():
    data = {'ts': np.array([100, 200, 300, 400]),
            'end': np.array([500, 6600, 500, 8000]),
            'series': np.array([10, 20, 30, 30])}
    # Cutoff 10,000 minus one-hour embargo is 6,400.
    assert train_indices(data, 10000, np.array([3]), 0).tolist() == [0]


def test_paired_bootstrap_keeps_series_and_disagreement_counts():
    y = np.array([1, 1, 0, 0, 1, 0])
    p = np.array([.8, .8, .2, .2, .8, .2])
    elo = np.array([.2, .2, .2, .2, .8, .8])
    result = paired_report(y, p, elo, np.array([1, 1, 2, 2, 0, 0]), np.arange(10, 16))
    assert result['clusters'] == 4
    assert result['disagreements'] == result['ml_disagreement_wins'] == 3
    assert result['elo_disagreement_wins'] == 0
    assert result['accuracy_gain_pp'] == 50.
    assert result['logloss_gain'] > 0


def test_baseline_admission_uses_same_evaluation_maps():
    data = {'ts': np.array([1, 2, 3]), 'league': np.array([1, 1, 0]),
            'elo_eligible': np.array([True, False, True])}
    assert indices(data, 0, 10).tolist() == [0]


def test_terminal_does_not_use_unavailable_outcomes():
    data = {'ts': np.array([100, 200, 300]), 'end': np.array([500, 10001, 6400]),
            'league': np.ones(3), 'elo_eligible': np.ones(3, dtype=bool)}
    assert completed_indices(data, 0, 10000).tolist() == [0]


def test_reversed_side_metadata_is_rejected(tmp_path):
    path = tmp_path/'reversed.npz'
    np.savez(path, label_side='dire')
    with pytest.raises(AssertionError, match='side semantics'):
        load_data(SimpleNamespace(dataset=path, draft=[]))


def test_terminal_recipe_and_dataset_are_bound_to_selection(tmp_path):
    data = tmp_path/'dataset.npz'; data.write_bytes(b'frozen dataset')
    manifest = tmp_path/'selection.json'
    manifest.write_text(json.dumps({'recipe_ids': ['linear_all'],
                                   'recipes': {'linear_all': RECIPES['linear_all']},
                                   'dataset_sha256': hashlib.sha256(data.read_bytes()).hexdigest(),
                                   'draft_sha256': [], 'selection_data_end': '2026-07-15'}))
    args = SimpleNamespace(dataset=data, draft=[], selection=manifest)
    verify_selection(args, ['linear_all'])
    with pytest.raises(AssertionError, match='recipe differs'):
        verify_selection(args, ['linear_recent'])
    data.write_bytes(b'changed dataset')
    with pytest.raises(AssertionError):
        verify_selection(args, ['linear_all'])


def test_public_projection_needs_no_outcomes_and_keeps_pre_snapshot_missing(tmp_path):
    import joblib
    from sklearn.linear_model import SGDClassifier
    from base.draft_features import DraftFeatureEncoder
    from base.tools.prematch_public_draft import project_snapshots

    heroes = np.array([np.arange(1, 11), np.arange(11, 21)])
    encoder = DraftFeatureEncoder.fit(heroes, kind='hero_role', signed=True)
    model = SGDClassifier(loss='log_loss', random_state=1)
    model.fit(encoder.transform(heroes), [0, 1])
    snapshots = tmp_path/'snapshots'; snapshots.mkdir()
    joblib.dump({'encoder': encoder, 'model': model, 'cutoff': 100, 'max_label_end': 99,
                 'kind': 'hero_role'}, snapshots/'draft_19700101.joblib')
    targets = tmp_path/'targets.npz'
    # Deliberately no y/wins/pstats member: projection must only read known draft inputs.
    np.savez(targets, mid=np.array([1, 2]), ts=np.array([99, 100]), heroes=heroes,
             role_assignment='past_completed_player_roles')
    project_snapshots(SimpleNamespace(targets=targets, project_snapshots=snapshots,
                                     output_dir=tmp_path, threads=1, kind='hero_role'))
    with np.load(tmp_path/'draft_features.npz') as z:
        assert np.isnan(z['p'][0]) and np.isfinite(z['p'][1])
        assert z['snapshot'].tolist() == [0, 100]


def test_refit_rejects_terminal_from_another_selection_before_training(tmp_path):
    dataset = tmp_path/'dataset.npz'; dataset.write_bytes(b'frozen dataset')
    selection = tmp_path/'selection.json'
    dataset_hash = hashlib.sha256(dataset.read_bytes()).hexdigest()
    selection.write_text(json.dumps({'recipe_ids': ['linear_all'],
                                    'recipes': {'linear_all': RECIPES['linear_all']},
                                    'dataset_sha256': dataset_hash, 'draft_sha256': [],
                                    'selection_data_end': '2026-07-15'}))
    terminal = tmp_path/'terminal.json'
    terminal.write_text(json.dumps({'mode': 'terminal', 'recipe_ids': ['linear_all'],
                                   'dataset_sha256': dataset_hash,
                                   'selection_sha256': 'another selection'}))
    args = SimpleNamespace(dataset=dataset, draft=[], selection=selection,
                           recipe='linear_all', terminal_report=terminal)
    # Empty data deliberately cannot train: provenance must fail first.
    with pytest.raises(AssertionError):
        refit({}, args)


def _roster_data():
    names = ['k24_rating_diff', 'team_rating_diff', 'recent_winrate_diff',
             'player_hero_winrate_diff', 'public_4_logit']
    accounts = np.array([[1, 2, 3, 4, 5, 11, 12, 13, 14, 15],
                         [6, 7, 8, 9, 10, 16, 17, 18, 19, 20]] * 3 +
                        [[11, 12, 13, 14, 15, 1, 2, 3, 4, 5],
                         [901, 902, 903, 904, 905, 911, 912, 913, 914, 915]])
    heroes = np.array([[101, 102, 103, 104, 105, 201, 202, 203, 204, 205],
                       [106, 107, 108, 109, 110, 206, 207, 208, 209, 210]] * 3 +
                      [[201, 202, 203, 204, 205, 101, 102, 103, 104, 105],
                       [1001, 1002, 1003, 1004, 1005, 1011, 1012, 1013, 1014, 1015]])
    x = np.array([[40, 20, .2, .3, .5], [-40, -20, -.2, -.3, -.5]] * 3 +
                 [[-40, -20, -.2, -.3, -.5], [0, 0, 0, 0, np.nan]], dtype=np.float32)
    ts = np.array([100, 1100, 2100, 3100, 4100, 5100, 6100, 8000])
    return {'X': x, 'names': names, 'accounts': accounts, 'heroes': heroes,
            'y': np.array([1, 0, 1, 0, 1, 0, 0, 1]), 'ts': ts,
            'end': np.array([200, 1200, 2200, 3200, 4200, 5200, 6200, 10000]),
            'league': np.ones(8, dtype=int), 'series': np.zeros(8, dtype=int),
            'numeric_count': len(names), 'public_columns': [4]}


def test_roster_uses_train_vocab_signed_sides_and_joblib_replay(tmp_path):
    import joblib
    data = _roster_data()
    model, columns, audit = fit_model(data, RECIPES['roster_all'], 10000, np.array([7]), threads=1)
    assert audit['train_n'] == 7  # end-time embargo excludes the future query row.
    assert audit['feature_count'] > 0 and audit['n_iter_']
    assert 901 not in model['account_vocab'] and (1001, 0) not in model['hero_vocab']
    unknown = _roster_matrix(data, [7], model['account_vocab'], model['hero_vocab'], model['numeric'])
    assert unknown.nnz == 0
    vocab = _roster_vocabulary(data, np.arange(7))
    signed = _roster_matrix(data, [0, 6], *vocab, _roster_numeric_columns(data)).toarray()
    np.testing.assert_allclose(signed[0], -signed[1])
    expected = predict(model, columns, data, np.array([0, 7]))
    path = tmp_path/'roster.joblib'; joblib.dump((model, columns), path)
    loaded_model, loaded_columns = joblib.load(path)
    np.testing.assert_allclose(expected, predict(loaded_model, loaded_columns, data, np.array([0, 7])))
    reordered = dict(data, names=list(reversed(data['names'])))
    with pytest.raises(ValueError, match='feature schema'):
        predict(loaded_model, loaded_columns, reordered, np.array([0]))
