"""Production KV3 panel boundary and fallback regression tests."""
from __future__ import annotations

import os
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def test_module_exposes_disabled_default(monkeypatch):
    import kv3_panel_serving as serving

    monkeypatch.delenv('ML_PANEL_KV3', raising=False)
    assert serving.enabled() is False


def test_prediction_context_is_available_for_panel_flag(monkeypatch):
    import win_model_veto as veto

    monkeypatch.delenv("KV3_SHADOW_ENABLED", raising=False)
    monkeypatch.setenv("ML_PANEL_KV3", "1")
    context = veto._prediction_context({"match_id": 123, "startDateTime": 1782864100,
                                        "radiantTeam": {"id": 10}, "direTeam": {"id": -20}})
    assert context["start_ts"] == 1782864100
    assert context["radiant_team_id"] == 10
    assert context["dire_team_id"] == -20


def _a_verdicts():
    from ml_panel import ModelVerdict
    import kv3_panel_serving as serving

    out = [ModelVerdict(key=key, title=key, side='Dire', probability=.2,
                        threshold=.7, fill=.95, ok=True, missing=('rating',))
           for key in serving.TARGETS]
    out.insert(2, ModelVerdict(key='dur43', title='duration', side='≥43',
                               probability=.6, threshold=.5, fill=1, ok=True))
    return out


class _Model:
    def predict_proba(self, row, thread_count=1):
        assert row.shape == (1, 1050)
        assert thread_count == 1
        return np.array([[.1, .9]])


def _candidate():
    from ml_panel import ModelSpec
    import kv3_panel_serving as serving

    specs = {key: ModelSpec(key=key, title=key, positive='Radiant', negative='Dire',
                            threshold=.7, knots_x=(0, 1), knots_y=(0, 1))
             for key in serving.TARGETS}
    return SimpleNamespace(panel_columns=['p'] * 928, kv3_columns=['v'] * 122,
                           models={key: _Model() for key in serving.TARGETS},
                           specs=specs, manifest_sha256='hash', cutoff=123,
                           state=SimpleNamespace(serving_last_overvisible_seconds=np.int64(2)))


def test_boundary_replaces_six_and_keeps_order_and_duration(monkeypatch):
    import kv3_panel_serving as serving
    import json
    from ml_panel import render, journal_row

    monkeypatch.setenv('ML_PANEL_KV3', '1')
    a = _a_verdicts()
    b = serving.replace_verdicts(None, np.zeros(928), a, {},
                                 feature_vector=np.zeros(122), candidate=_candidate())
    assert [v.key for v in b] == [v.key for v in a]
    assert b[2] is a[2]
    assert all(v.probability == pytest.approx(.9) for v in b if v.key != 'dur43')
    assert all(v.metadata['model'] == 'B_kv3' for v in b if v.key != 'dur43')
    assert 'Radiant 90%' in render(b)
    assert journal_row(1, b)['models'][0]['metadata']['a_p'] == .2
    json.dumps(journal_row(1, b))


def test_fallback_for_missing_start_stale_and_exception(monkeypatch):
    import kv3_panel_serving as serving

    monkeypatch.setenv('ML_PANEL_KV3', '1')
    a = _a_verdicts()
    ready, reason = serving.prepare(None, {})
    assert not ready and reason == 'missing_start_ts'
    assert serving.prepare(None, {'start_ts': -1}) == (False, 'missing_start_ts')
    marked = serving.fallback(a, reason)
    assert all(v.title.endswith(' (старая)') and v.metadata['reason'] == reason
               for v in marked if v.key != 'dur43')
    assert marked[2] is a[2]

    fake = _candidate()
    monkeypatch.setattr(serving, '_load', lambda bundle: fake)
    monkeypatch.setattr(serving, '_reload', lambda candidate: None)
    ready, reason = serving.prepare(None, {'start_ts': 123 + 259201,
                                          'radiant_team_id': 1, 'dire_team_id': 2})
    assert not ready and reason == 'state_stale'
    failed = serving.replace_verdicts(None, np.zeros(928), a, {},
                                      feature_vector=np.zeros(121), candidate=fake)
    assert failed[0].metadata['model'] == 'A_fallback'
    assert 'Invalid panel or KV3 vector' in failed[0].metadata['reason']


def test_state_reload_keeps_old_until_contract_checked(monkeypatch):
    import kv3_panel_serving as serving
    from base import kills_v3_serving

    old = SimpleNamespace(serving_meta={'cutoff': 100})
    new = SimpleNamespace(serving_meta={'cutoff': 200})
    candidate = SimpleNamespace(state=old, state_mtime=1, state_size=1,
                                state_path=Path('unused'), cutoff=100,
                                tp_names=['old'], tp_indices=[0],
                                _signature=lambda: (2, 2))
    monkeypatch.setattr(kills_v3_serving, 'load_state', lambda path: new)

    def reject():
        candidate.tp_names = ['new']
        candidate.tp_indices = [1]
        raise ValueError('contract mismatch')

    candidate._check_state_contract = reject
    with pytest.raises(ValueError, match='contract mismatch'):
        serving._reload(candidate)
    assert candidate.state is old and candidate.cutoff == 100
    assert (candidate.state_mtime, candidate.state_size) == (1, 1)
    assert candidate.tp_names == ['old'] and candidate.tp_indices == [0]

    candidate._signature = lambda: (3, 3)
    candidate._check_state_contract = lambda: None
    serving._reload(candidate)
    assert candidate.state is new and candidate.cutoff == 200
    assert (candidate.state_mtime, candidate.state_size) == (3, 3)


def test_failed_initial_load_is_cached_until_state_changes(monkeypatch, tmp_path):
    import kv3_panel_serving as serving
    import kv3_shadow

    state = tmp_path / 'state.npz'
    state.write_bytes(b'bad')
    monkeypatch.setenv('ML_PANEL_KV3', '1')
    monkeypatch.setenv('KV3_STATE_PATH', str(state))
    monkeypatch.setenv('KV3_PANEL_DIR', str(tmp_path / 'bundle'))
    monkeypatch.setattr(serving, '_candidate', None)
    attempts = []

    def fail(bundle):
        attempts.append(1)
        raise ValueError('invalid state contract')

    monkeypatch.setattr(kv3_shadow, 'Candidate', fail)
    context = {'start_ts': 200, 'radiant_team_id': 1, 'dire_team_id': 2}
    for _ in range(5):
        assert serving.prepare(None, context) == (False, 'ValueError: invalid state contract')
    assert len(attempts) == 1
    assert serving.replace_verdicts(None, np.zeros(928), _a_verdicts(), context)[0].metadata['reason'] == 'ValueError: invalid state contract'
    assert len(attempts) == 1
    state.write_bytes(b'changed')
    assert serving.prepare(None, context)[0] is False
    assert len(attempts) == 2


def test_failed_reload_is_cached_until_state_changes(monkeypatch):
    import kv3_panel_serving as serving
    from base import kills_v3_serving

    old = SimpleNamespace(serving_meta={'cutoff': 100})
    current = [2, 2]
    attempts = []
    candidate = SimpleNamespace(state=old, state_mtime=1, state_size=1,
                                state_path=Path('unused'), cutoff=100,
                                tp_names=['old'], tp_indices=[0],
                                _signature=lambda: tuple(current))
    def load(path):
        attempts.append(1)
        return SimpleNamespace(serving_meta={'cutoff': 200})
    monkeypatch.setattr(kills_v3_serving, 'load_state', load)
    candidate._check_state_contract = lambda: (_ for _ in ()).throw(ValueError('bad replacement'))
    for _ in range(5):
        with pytest.raises(ValueError, match='bad replacement'):
            serving._reload(candidate)
    assert len(attempts) == 1
    assert candidate.state is old
    monkeypatch.setenv('ML_PANEL_KV3', '1')
    monkeypatch.setattr(serving, '_load', lambda bundle: candidate)
    ready, reason = serving.prepare(None, {'start_ts': 200,
                                           'radiant_team_id': 1, 'dire_team_id': 2})
    assert (ready, reason) == (False, 'ValueError: bad replacement')
    assert len(attempts) == 1 and candidate.state is old
    current[:] = [3, 3]
    with pytest.raises(ValueError, match='bad replacement'):
        serving._reload(candidate)
    assert len(attempts) == 2
    assert candidate.state is old


@pytest.mark.parametrize('context,known', [
    ({'start_ts': 200, 'start_ts_source': 'now_fallback',
      'radiant_team_id': 1, 'dire_team_id': 2}, True),
    ({'start_ts': 200, 'start_ts_source': 'startDateTime',
      'radiant_team_id': 0, 'dire_team_id': 2}, False),
])
def test_now_fallback_and_zero_team_id_serve_b(monkeypatch, context, known):
    import kv3_panel_serving as serving
    from base import kills_v3_serving

    fake = _candidate()
    fake.tp_names = ['v'] * 152
    fake.tp_indices = list(range(122))
    monkeypatch.setenv('ML_PANEL_KV3', '1')
    monkeypatch.setattr(serving, '_load', lambda bundle: fake)
    monkeypatch.setattr(serving, '_reload', lambda candidate: None)
    monkeypatch.setattr(kills_v3_serving, 'features_for_map',
                        lambda *args, **kwargs: (fake.tp_names, np.zeros(152)))
    context = dict(context, radiant_accounts=[], dire_accounts=[])
    assert serving.prepare(None, context) == (True, None)
    result = serving.replace_verdicts(None, np.zeros(928), _a_verdicts(), context)
    assert result[0].metadata['model'] == 'B_kv3'
    assert result[0].metadata['start_ts_source'] == context['start_ts_source']
    assert result[0].metadata['team_ids_known'] is known


def test_live_wiring_off_and_on_with_fake_score(monkeypatch):
    import prematch_panel_live as live
    import kv3_panel_serving as serving

    a = _a_verdicts()
    bundle = SimpleNamespace(ready=True, columns=tuple('p' + str(i) for i in range(928)))
    monkeypatch.setattr(live, '_load', lambda: {'bundle': bundle, 'tables': (None, None),
                                               'snap': None})
    monkeypatch.setattr(live, '_dict_block', lambda heroes: None)
    monkeypatch.setattr(live, 'HYBRID_ENABLED', False)
    seen = []

    def score(_bundle, _blocks, **kwargs):
        seen.append(kwargs)
        if kwargs.get('row_observer'):
            kwargs['row_observer'](np.zeros(928, dtype=np.float32), tuple(a))
        return list(a)

    monkeypatch.setitem(sys.modules, 'prematch_panel_scorer', SimpleNamespace(
        block_from_matrix=lambda prefix, matrix: {},
        block_from_prod_features=lambda *args, **kwargs: {}, score=score))
    monkeypatch.setitem(sys.modules, 'hero_side_tables', SimpleNamespace(
        sym_block=lambda *args: np.zeros((1, 1))))
    monkeypatch.setitem(sys.modules, 'public_kills_block', SimpleNamespace(block=lambda x: None))
    monkeypatch.setitem(sys.modules, 'team_ratings', SimpleNamespace(block=lambda *a, **kw: None))
    monkeypatch.setitem(sys.modules, 'duration43_serving', SimpleNamespace(
        replace_verdict=lambda verdicts, *a, **kw: verdicts, status=lambda: {}))
    heroes = list(range(1, 6))
    accounts = list(range(11, 16))
    context = {'start_ts': 1782864100, 'radiant_team_id': 1, 'dire_team_id': 2}
    monkeypatch.delenv('ML_PANEL_KV3', raising=False)
    monkeypatch.delenv('KV3_SHADOW_ENABLED', raising=False)
    monkeypatch.delenv('DURATION43_SHADOW_ENABLED', raising=False)
    off = live.evaluate_map(heroes, heroes, accounts, accounts, None,
                            shadow_context=context)
    assert off == a
    assert seen[-1]['draft_keys'] == live.DRAFT_KEYS
    assert seen[-1]['row_observer'] is None

    monkeypatch.setenv('ML_PANEL_KV3', '1')
    monkeypatch.setenv('KV3_SHADOW_ENABLED', '1')
    monkeypatch.setitem(sys.modules, 'kv3_shadow', SimpleNamespace(
        submit=lambda *args: pytest.fail('shadow must not duplicate B state')))
    monkeypatch.setattr(serving, 'prepare', lambda *args: (True, None))
    monkeypatch.setattr(serving, 'status', lambda: {'ready': True})
    monkeypatch.setattr(serving, 'replace_verdicts',
                        lambda bundle, x, verdicts, ctx, **kw: [
                            replace(v, probability=.9) if v.key in serving.TARGETS else v
                            for v in verdicts])
    on = live.evaluate_map(heroes, heroes, accounts, accounts, None,
                           shadow_context=context)
    assert [v.key for v in on] == [v.key for v in a]
    assert on[2] is a[2]
    assert all(v.probability == .9 for v in on if v.key in serving.TARGETS)
    assert not set(seen[-1]['draft_keys']) & set(serving.TARGETS)
    assert seen[-1]['row_observer'] is not None

    monkeypatch.setattr(serving, 'replace_verdicts',
                        lambda bundle, x, verdicts, ctx, **kw:
                        serving.fallback(verdicts, 'predict_exception'))
    recovered = live.evaluate_map(heroes, heroes, accounts, accounts, None,
                                  shadow_context=context)
    assert recovered[0].metadata['reason'] == 'predict_exception'
    assert seen[-2]['draft_keys'] == ()
    assert seen[-1]['draft_keys'] == live.DRAFT_KEYS

    monkeypatch.setattr(serving, 'prepare', lambda *args: (False, 'state_stale'))
    stale = live.evaluate_map(heroes, heroes, accounts, accounts, None,
                              shadow_context=context)
    assert stale[0].title.endswith(' (старая)')
    assert stale[0].metadata['reason'] == 'state_stale'
    assert seen[-1]['draft_keys'] == live.DRAFT_KEYS


def test_real_bundle_served_fixture_probability_and_render(monkeypatch):
    import json
    from catboost import CatBoostClassifier
    import kv3_panel_serving as serving
    from ml_panel import ModelVerdict, load_specs, render

    path = Path(__file__).parent / 'fixtures/kv3_panel_b_maps.npz'
    bundle_dir = Path(__file__).resolve().parents[2] / 'ml-models/prematch_panel_kv3'
    monkeypatch.setenv('ML_PANEL_KV3', '1')
    specs = {s.key: s for s in load_specs(bundle_dir)}
    assert set(specs) == set(serving.TARGETS)
    names = json.loads((bundle_dir / 'feature_names.json').read_text())
    models = {}
    for key in serving.TARGETS:
        model = CatBoostClassifier()
        model.load_model(str(bundle_dir / (key + '.cbm')))
        models[key] = model
    candidate = SimpleNamespace(panel_columns=names['panel_columns'],
                                kv3_columns=names['kv3_columns'], models=models,
                                specs=specs, manifest_sha256='fixture', cutoff=1782864000,
                                state=SimpleNamespace(serving_last_overvisible_seconds=0))
    with np.load(path, allow_pickle=False) as z:
        metadata = json.loads(str(z['metadata']))
        assert '2026-09-24' in metadata['captured_utc']
        assert '--capture-fixture' in metadata['command']
        assert len(z['keys']) == 6
        assert set(z['keys']) == set(serving.TARGETS)
        assert np.all((z['ts'] > 1782864000) & (z['ts'] <= 1782950400))
        for i, key in enumerate(z['keys']):
            spec = specs[str(key)]
            a_p = float(z['expected_a'][i])
            a = ModelVerdict(key=str(key), title=spec.title,
                             side=spec.positive if a_p >= .5 else spec.negative,
                             probability=a_p, threshold=spec.threshold,
                             fill=1, ok=False)
            incumbents = [a if other == str(key) else ModelVerdict(
                key=other, title=specs[other].title, side=specs[other].negative,
                probability=.2, threshold=specs[other].threshold, fill=1, ok=False)
                for other in serving.TARGETS]
            result = serving.replace_verdicts(None, z['x928'][i], incumbents, {},
                                               feature_vector=z['kv3'][i],
                                               candidate=candidate)
            b = next(v for v in result if v.key == str(key))
            assert b.metadata['model'] == 'B_kv3'
            assert abs(b.probability - float(z['expected_b'][i])) <= 1e-9
            expected_line = f'{b.side} {b.confidence*100:.0f}%'
            assert expected_line in render([b])
            assert render([b]) != render([a])
