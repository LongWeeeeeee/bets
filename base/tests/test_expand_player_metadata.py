import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from base.player_metadata import PlayerHistory, parse_dltv_match, timestamp
from base.tools.expand_player_metadata import augmented_fit, build_metadata, collect_dates, match_urls, verify_plan


def test_discovery_only_returns_canonical_match_urls():
    html = '<a href="https://dltv.org/matches/427986/kalmychata-vs-uralan">x</a>'
    assert match_urls(html * 2 + 'https://evil.test/matches/3/other') == [
        'https://dltv.org/matches/427986/kalmychata-vs-uralan']


def test_real_snapshot_joins_positions_and_excludes_other_months():
    data = json.loads((Path(__file__).parent / 'fixtures/player_metadata_dltv.json').read_text())
    snapshot = parse_dltv_match('series_item = ' + json.dumps(data['series_item']),
                               source_url=data['source_url'], observed_at=data['observed_at'])
    # Fixture contains source fields from the user's actual match.
    lineup = [p['account_id'] for p in sorted(snapshot['players'], key=lambda p: (p['dltv_team_id'], p['position']))]
    rows = {'mids': np.array([1, 2]), 'ts': np.array([timestamp('2026-08-30T12:00:00Z'),
                                                    timestamp('2026-09-10T12:00:00Z')]),
            'accounts': np.array([lineup, lineup])}
    X, names, diagnostics = build_metadata(rows, PlayerHistory([snapshot]))
    assert not X[0].any()
    assert X[1, names.index('team_earnings_coverage_joint')] == 1
    assert diagnostics[0]['mid'] == 2
    assert diagnostics[0]['earnings_players'] == 10
    assert diagnostics[0]['rank_players'] == 0
    rows['accounts'][1] = lineup[5:] + lineup[:5]
    swapped, _, _ = build_metadata(rows, PlayerHistory([snapshot]))
    for name in names:
        if name.endswith('_diff'):
            assert swapped[1, names.index(name)] == pytest.approx(-X[1, names.index(name)])
    rows['mids'][1] = 1
    with pytest.raises(ValueError, match='duplicate'):
        build_metadata(rows, PlayerHistory([snapshot]))


def test_collection_stops_on_rate_limit_and_keeps_receipt(tmp_path, monkeypatch):
    import requests
    import base.tools.expand_player_metadata as module
    class Response:
        status_code = 200
        text = 'https://dltv.org/matches/1/a https://dltv.org/matches/2/b'
        def raise_for_status(self):
            pass
    monkeypatch.setattr(requests, 'get', lambda *a, **k: Response())
    monkeypatch.setattr(module.time, 'sleep', lambda *_: None)
    calls = []
    def blocked(url, output):
        calls.append(url)
        raise requests.HTTPError('429', response=SimpleNamespace(status_code=429))
    monkeypatch.setattr(module, 'collect', blocked)
    out = tmp_path / 'new'
    with pytest.raises(RuntimeError, match='blocked'):
        collect_dates(['2026-09-12'], out)
    assert len(calls) == 1
    assert json.loads((out / 'collection.json').read_text())['failures'][0]['status'] == 429
    with pytest.raises(FileExistsError):
        collect_dates(['2026-09-12'], out)


def test_augmented_fit_excludes_test_only_features_and_keeps_routing(monkeypatch):
    import base.tools.refit_prematch_draft_component as packing
    import base.tools.retrain_prematch_general as training
    monkeypatch.setattr(packing, 'unpack_branches', lambda _: {
        'full': SimpleNamespace(cols=['x', 'org']), 'no_org': SimpleNamespace(cols=['x'])})
    fitted_shapes = []
    def fit(X, y, c):
        fitted_shapes.append(X.shape)
        return SimpleNamespace(mu=np.zeros(X.shape[1]), sd=np.ones(X.shape[1]),
                               coef=np.zeros(X.shape[1]), intercept=.1 * X.shape[1]), {}
    monkeypatch.setattr(training, 'fit', fit)
    monkeypatch.setattr(training, 'probability', lambda X, b: np.full(len(X), b.intercept))
    mids = np.arange(6)
    matrix = {'mids': mids, 'X': np.ones((6, 2)), 'y': np.array([0, 1, 0, 1, 0, 1]),
              'feature_names': np.array(['x', 'org'])}
    split = {'mids': mids, 'train': np.array([1, 1, 1, 1, 0, 0], dtype=bool),
             'test': np.array([0, 0, 0, 0, 1, 1], dtype=bool),
             'routed_branch': np.array(['full', 'full', 'no_org', 'no_org', 'full', 'no_org'])}
    metadata = {'mids': mids, 'X': np.array([[0, 0], [1, 0], [2, 0], [3, 0], [4, 10], [5, 11]]),
                'feature_names': np.array(['earnings', 'test_only'])}
    p, arrays, details = augmented_fit(matrix, split, metadata, {})
    assert fitted_shapes == [(4, 3), (2, 2)]
    assert p == pytest.approx([.3, .2])
    assert list(arrays['full_feature_names']) == ['x', 'org', 'earnings']
    assert details['no_org']['added_features'] == 1
    metadata['mids'] = mids[::-1]
    with pytest.raises(ValueError, match='identity'):
        augmented_fit(matrix, split, metadata, {})


def test_discovery_error_receipt_and_date_budget(tmp_path, monkeypatch):
    import requests
    def blocked(*args, **kwargs):
        raise requests.HTTPError('403', response=SimpleNamespace(status_code=403))
    monkeypatch.setattr(requests, 'get', blocked)
    out = tmp_path / 'new'
    with pytest.raises(requests.HTTPError):
        collect_dates(['2026-09-12'], out)
    report = json.loads((out / 'collection.json').read_text())
    assert report['phase'] == 'discovery'
    assert report['failures'][0]['status'] == 403
    with pytest.raises(ValueError, match='dates'):
        collect_dates(['2026-09-%02d' % day for day in range(1, 12)], tmp_path / 'other')
    assert not (tmp_path / 'other').exists()


def test_input_hashes_require_and_protect_account_rows(tmp_path):
    import hashlib
    path = tmp_path / 'rows.npz'
    path.write_bytes(b'original-account-order')
    plan = {'rows': str(path), 'sha256': {}}
    with pytest.raises(ValueError, match='missing required'):
        verify_plan(plan, ['rows'])
    plan['sha256'][str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    verify_plan(plan, ['rows'])
    path.write_bytes(b'changed-account-order')
    with pytest.raises(ValueError, match='input hash mismatch'):
        verify_plan(plan, ['rows'])


def test_retained_html_newlines_are_hashed_without_normalization(tmp_path):
    from base.tools.player_metadata import load_snapshot
    data = json.loads((Path(__file__).parent / 'fixtures/player_metadata_dltv.json').read_text())
    html = '<script>\r\nseries_item = ' + json.dumps(data['series_item']) + ';\r\n</script>'
    snapshot = parse_dltv_match(html, source_url=data['source_url'], observed_at=data['observed_at'])
    (tmp_path / 'source.html').write_bytes(html.encode('utf-8'))
    (tmp_path / 'snapshot.json').write_text(json.dumps(snapshot))
    assert load_snapshot(tmp_path / 'snapshot.json') == snapshot
