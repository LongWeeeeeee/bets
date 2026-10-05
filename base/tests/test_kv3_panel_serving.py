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


@pytest.mark.parametrize('invalid_meta,error', [
    ({}, KeyError),
    ({'cutoff': 'invalid'}, ValueError),
])
def test_reload_invalid_cutoff_keeps_previous_state_and_retries_after_change(
        monkeypatch, tmp_path, invalid_meta, error):
    import kv3_panel_serving as serving
    from base import kills_v3_serving

    state_path = tmp_path / 'state.npz'
    state_path.write_bytes(b'old')
    old_stat = state_path.stat()
    old_signature = old_stat.st_mtime_ns, old_stat.st_size
    old = SimpleNamespace(serving_meta={'cutoff': 100})
    invalid = SimpleNamespace(serving_meta=invalid_meta)
    valid = SimpleNamespace(serving_meta={'cutoff': 300})
    candidate = SimpleNamespace(state=old, state_mtime=old_signature[0],
                                state_size=old_signature[1], state_path=state_path,
                                cutoff=100, tp_names=['old'], tp_indices=[0],
                                _signature=lambda: (state_path.stat().st_mtime_ns,
                                                    state_path.stat().st_size))
    candidate._check_state_contract = lambda: None
    calls = []

    def load(path):
        calls.append(path)
        return invalid if len(calls) == 1 else valid

    monkeypatch.setattr(kills_v3_serving, 'load_state', load)
    state_path.write_bytes(b'invalid')
    os.utime(state_path, ns=(old_signature[0] + 1_000_000_000,) * 2)
    for _ in range(2):
        with pytest.raises(error):
            serving._reload(candidate)
        assert candidate.state is old
        assert candidate.cutoff == 100
        assert (candidate.state_mtime, candidate.state_size) == old_signature
        assert candidate.tp_names == ['old'] and candidate.tp_indices == [0]
    assert calls == [state_path]

    state_path.write_bytes(b'valid replacement')
    os.utime(state_path, ns=(old_signature[0] + 2_000_000_000,) * 2)
    serving._reload(candidate)
    assert calls == [state_path, state_path]
    assert candidate.state is valid and candidate.cutoff == 300
    assert (candidate.state_mtime, candidate.state_size) == candidate._signature()


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
    import hashlib
    import json
    from catboost import CatBoostClassifier
    import kv3_panel_serving as serving
    from ml_panel import ModelVerdict, load_specs, render

    path = Path(__file__).parent / 'fixtures/kv3_panel_b_maps.npz'
    bundle_dir = Path(__file__).resolve().parents[2] / 'ml-models/prematch_panel_kv3'
    monkeypatch.setenv('ML_PANEL_KV3', '1')
    manifest_sha = hashlib.sha256((bundle_dir / 'manifest.json').read_bytes()).hexdigest()
    specs = {s.key: s for s in load_specs(bundle_dir)}
    assert set(serving.TARGETS) <= set(specs)
    assert set(specs) <= set(serving.TARGETS + serving.EXTRA_TARGETS + serving.LADDER_TARGETS)
    names = json.loads((bundle_dir / 'feature_names.json').read_text())
    models = {}
    for key in serving.TARGETS:
        model = CatBoostClassifier()
        model.load_model(str(bundle_dir / (key + '.cbm')))
        models[key] = model
    candidate = SimpleNamespace(panel_columns=names['panel_columns'],
                                kv3_columns=names['kv3_columns'], models=models,
                                specs=specs, manifest_sha256=manifest_sha, cutoff=1782864000,
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
            assert b.metadata['bundle_sha'] == manifest_sha
            assert abs(b.probability - float(z['expected_b'][i])) <= 1e-9
            expected_line = f'{b.side} {b.confidence*100:.0f}%'
            assert expected_line in render([b])
            assert render([b]) != render([a])


def test_real_bundle_serves_fixture_kv3_features_by_name(monkeypatch):
    import json
    from catboost import CatBoostClassifier
    from base import kills_v3_serving
    import kv3_panel_serving as serving
    import kv3_shadow
    from ml_panel import ModelVerdict, load_specs

    bundle_dir = Path(__file__).resolve().parents[2] / 'ml-models/prematch_panel_kv3'
    fixture = Path(__file__).parent / 'fixtures/kv3_panel_b_maps.npz'
    names = json.loads((bundle_dir / 'feature_names.json').read_text())
    parameters = json.loads((bundle_dir / 'manifest.json').read_text())['od3_parameters']
    source_names = names['kv3_source_names']
    assert names['kv3_columns'] == ['kv3_' + name for name in source_names]
    assert len(source_names) == 122
    tp_names = list(np.random.default_rng(20260924).permutation(
        source_names + [f'unused_tp_{i}' for i in range(30)]))
    specs = {spec.key: spec for spec in load_specs(bundle_dir)}
    models = {}
    for key in serving.TARGETS:
        model = CatBoostClassifier()
        model.load_model(str(bundle_dir / (key + '.cbm')))
        models[key] = model

    candidate = SimpleNamespace(
        panel_columns=names['panel_columns'], kv3_columns=names['kv3_columns'],
        source_names=source_names, od3_parameters=parameters, models=models,
        specs=specs, manifest_sha256='fixture', cutoff=1782864000,
        state=SimpleNamespace(
            serving_meta=dict(history_start=parameters['history_start'],
                              visibility_delay=parameters['visibility_delay'],
                              builder_params=parameters, tp_names=tp_names),
            serving_last_overvisible_seconds=0),
        _signature=lambda: (1, 1), state_mtime=1, state_size=1)
    kv3_shadow.Candidate._check_state_contract(candidate)
    assert candidate.tp_names == tp_names
    assert candidate.tp_indices != list(range(122))

    monkeypatch.setenv('ML_PANEL_KV3', '1')
    with np.load(fixture, allow_pickle=False) as z:
        assert len(z['keys']) == 6
        for i, key in enumerate(z['keys']):
            values_by_name = dict(zip(source_names, z['kv3'][i]))
            tp_vector = np.asarray([values_by_name.get(name, 1e6) for name in tp_names])
            monkeypatch.setattr(kills_v3_serving, 'features_for_map',
                                lambda *args, **kwargs: (tp_names, tp_vector))
            incumbents = [ModelVerdict(
                key=target, title=specs[target].title, side=specs[target].negative,
                probability=.2, threshold=specs[target].threshold, fill=1, ok=False)
                for target in serving.TARGETS]
            context = dict(start_ts=int(z['ts'][i]), radiant_team_id=1,
                           dire_team_id=2, radiant_accounts=[], dire_accounts=[])
            result = serving.replace_verdicts(None, z['x928'][i], incumbents,
                                               context, candidate=candidate)
            verdict = next(v for v in result if v.key == str(key))
            assert verdict.metadata['model'] == 'B_kv3'
            assert abs(verdict.probability - float(z['expected_b'][i])) <= 1e-9


def _real_candidate_with_ladder(bundle_dir):
    """Real B + plain + six E-352 ladder bundles, loaded like the production loader does."""
    import hashlib
    import json
    from catboost import CatBoostClassifier
    import kv3_panel_serving as serving
    from ml_panel import load_specs

    specs = {s.key: s for s in load_specs(bundle_dir)}
    names = json.loads((bundle_dir / 'feature_names.json').read_text())
    models, calibration = {}, {}
    for key in serving.TARGETS + serving.EXTRA_TARGETS + serving.LADDER_TARGETS:
        model = CatBoostClassifier()
        model.load_model(str(bundle_dir / (key + '.cbm')))
        models[key] = model
        calib = json.loads((bundle_dir / (key + '.calib.json')).read_text())
        calibration[key] = (np.asarray(calib['knots_x']), np.asarray(calib['knots_y']))
    manifest_sha = hashlib.sha256((bundle_dir / 'manifest.json').read_bytes()).hexdigest()
    return SimpleNamespace(panel_columns=names['panel_columns'], kv3_columns=names['kv3_columns'],
                           models=models, specs=specs, manifest_sha256=manifest_sha,
                           cutoff=1782864000,
                           state=SimpleNamespace(serving_last_overvisible_seconds=0)), calibration


def _serve_fixture_row(candidate, index=0):
    import json
    from ml_panel import ModelVerdict
    import kv3_panel_serving as serving

    path = Path(__file__).parent / 'fixtures/kv3_panel_b_maps.npz'
    with np.load(path, allow_pickle=False) as z:
        x928, kv3 = z['x928'][index], z['kv3'][index]
    incumbents = [ModelVerdict(key=key, title=key, side='Dire', probability=.2,
                               threshold=.7, fill=1, ok=False) for key in serving.TARGETS]
    result = serving.replace_verdicts(None, x928, incumbents, {}, feature_vector=kv3,
                                      candidate=candidate)
    return result, np.concatenate((x928, kv3)).reshape(1, -1)


def test_ladder_shadow_scored_on_the_same_row_calibrated_and_journaled(monkeypatch):
    import json
    import kv3_panel_serving as serving
    import ml_panel

    bundle_dir = Path(__file__).resolve().parents[2] / 'ml-models/prematch_panel_kv3'
    monkeypatch.setenv('ML_PANEL_KV3', '1')
    monkeypatch.delenv('ML_PANEL_KV3_LADDER_SHADOW', raising=False)
    candidate, calibration = _real_candidate_with_ladder(bundle_dir)
    result, row = _serve_fixture_row(candidate)
    by_key = {v.key: v for v in result}
    assert set(serving.LADDER_TARGETS) <= set(by_key)
    for key in serving.LADDER_TARGETS:
        raw = float(candidate.models[key].predict_proba(row, thread_count=1)[0, 1])
        knots_x, knots_y = calibration[key]
        verdict = by_key[key]
        assert verdict.raw == pytest.approx(raw, abs=1e-12)
        assert verdict.probability == pytest.approx(float(np.interp(raw, knots_x, knots_y)), abs=1e-9)
        assert verdict.ok is False and verdict.threshold > 1
        assert verdict.metadata['model'] == 'B_kv3_ladder_shadow'
        assert verdict.metadata['bundle_sha'] == candidate.manifest_sha256
    # Journal row (delivery boundary 1): all six present with their probabilities.
    journal = {m['key']: m for m in ml_panel.journal_row(1, result)['models']}
    for key in serving.LADDER_TARGETS:
        assert journal[key]['p'] == pytest.approx(by_key[key].probability, abs=1e-6)
        assert journal[key]['ok'] is False
        assert journal[key]['metadata']['model'] == 'B_kv3_ladder_shadow'
    json.dumps(ml_panel.journal_row(1, result))
    # B itself is unchanged by the shadow models.
    assert all(by_key[key].metadata['model'] == 'B_kv3' for key in serving.TARGETS)


def test_ladder_shadow_never_reaches_the_card(monkeypatch):
    import kv3_panel_serving as serving
    import ml_panel
    from base import win_model_veto as veto

    bundle_dir = Path(__file__).resolve().parents[2] / 'ml-models/prematch_panel_kv3'
    monkeypatch.setenv('ML_PANEL_KV3', '1')
    monkeypatch.delenv('ML_PANEL_KV3_LADDER_SHADOW', raising=False)
    candidate, _ = _real_candidate_with_ladder(bundle_dir)
    result, _row = _serve_fixture_row(candidate)
    ladder_titles = {v.title for v in result if v.key in serving.LADDER_TARGETS}
    assert len(ladder_titles) == 6                      # titles exist to be searched for
    for mode in (None, 'band', 'e281'):
        if mode is None:
            monkeypatch.delenv('ML_PANEL_KILLS_DISPLAY', raising=False)
        else:
            monkeypatch.setenv('ML_PANEL_KILLS_DISPLAY', mode)
        text, _use_b = veto._render_panel_kills_display(result, ml_panel)
        assert text
        for key in serving.LADDER_TARGETS:
            assert key not in text
        for title in ladder_titles:
            assert title not in text
        without = [v for v in result if v.key not in serving.LADDER_TARGETS]
        assert text == veto._render_panel_kills_display(without, ml_panel)[0]
    assert not any(v.ok for v in result if v.key in serving.LADDER_TARGETS)


def test_ladder_shadow_env_zero_is_not_scored(monkeypatch):
    import kv3_panel_serving as serving

    bundle_dir = Path(__file__).resolve().parents[2] / 'ml-models/prematch_panel_kv3'
    monkeypatch.setenv('ML_PANEL_KV3', '1')
    candidate, _ = _real_candidate_with_ladder(bundle_dir)
    monkeypatch.delenv('ML_PANEL_KV3_LADDER_SHADOW', raising=False)
    with_shadow, _ = _serve_fixture_row(candidate)
    monkeypatch.setenv('ML_PANEL_KV3_LADDER_SHADOW', '0')
    without, _ = _serve_fixture_row(candidate)
    assert not any(v.key in serving.LADDER_TARGETS for v in without)
    assert [v.key for v in without] == [v.key for v in with_shadow
                                        if v.key not in serving.LADDER_TARGETS]
    kept = [v for v in with_shadow if v.key not in serving.LADDER_TARGETS]
    assert kept == without


def test_ladder_shadow_failure_never_takes_b_down(monkeypatch):
    import kv3_panel_serving as serving

    bundle_dir = Path(__file__).resolve().parents[2] / 'ml-models/prematch_panel_kv3'
    monkeypatch.setenv('ML_PANEL_KV3', '1')
    monkeypatch.delenv('ML_PANEL_KV3_LADDER_SHADOW', raising=False)
    candidate, _ = _real_candidate_with_ladder(bundle_dir)

    def boom(*_a, **_k):
        raise ValueError('ladder model failed')

    candidate.models['dire_ge21'].predict_proba = boom
    result, _row = _serve_fixture_row(candidate)
    by_key = {v.key: v for v in result}
    assert all(by_key[key].metadata['model'] == 'B_kv3' for key in serving.TARGETS)
    assert 'dire_ge21' not in by_key
    assert {'rad_ge16', 'rad_ge21', 'rad_ge26', 'dire_ge16', 'dire_ge26'} <= set(by_key)


@pytest.mark.parametrize("mode", ["present", "disabled", "bad_calibration", "contract_mismatch"])
def test_real_loader_ladder_keys_are_optional_and_never_fatal(monkeypatch, tmp_path, mode):
    import json
    import shutil
    from base import kills_v3_serving
    import kv3_panel_serving as serving
    from ml_panel import load_specs

    src = Path(__file__).resolve().parents[2] / 'ml-models/prematch_panel_kv3'
    directory = tmp_path / 'bundle'
    shutil.copytree(src, directory)
    if mode == 'bad_calibration':
        calib = json.loads((directory / 'rad_ge21.calib.json').read_text())
        calib['knots_x'] = calib['knots_x'][::-1]
        (directory / 'rad_ge21.calib.json').write_text(json.dumps(calib))
    if mode == 'contract_mismatch':       # valid model, calibration differs from panel.json
        shutil.copy2(directory / 'w_5_15.cbm', directory / 'rad_ge21.cbm')
        shutil.copy2(directory / 'w_5_15.calib.json', directory / 'rad_ge21.calib.json')
    features = json.loads((directory / 'feature_names.json').read_text())
    parameters = json.loads((directory / 'manifest.json').read_text())['od3_parameters']
    state = SimpleNamespace(serving_meta={
        'cutoff': 1782864000, 'history_start': parameters['history_start'],
        'visibility_delay': parameters['visibility_delay'], 'builder_params': parameters,
        'tp_names': features['kv3_source_names'] + [f'unused_{i}' for i in range(30)]})
    monkeypatch.setattr(kills_v3_serving, 'load_state', lambda path: state)
    state_file = tmp_path / 'state.npz'
    state_file.write_bytes(b'')
    monkeypatch.setenv('KV3_STATE_PATH', str(state_file))
    monkeypatch.setenv('KV3_PANEL_DIR', str(directory))
    monkeypatch.delenv('ML_PANEL_KV3_LADDER_SHADOW', raising=False)
    if mode == 'disabled':
        monkeypatch.setenv('ML_PANEL_KV3_LADDER_SHADOW', '0')
    monkeypatch.setattr(serving, '_candidate', None)
    specs = {spec.key: spec for spec in load_specs(directory)}
    bundle = SimpleNamespace(columns=features['panel_columns'],
                             specs=tuple(specs[key] for key in serving.TARGETS))
    candidate = serving._load_uncached(bundle)
    assert set(serving.TARGETS) <= set(candidate.models)          # B always loads
    loaded = {key for key in serving.LADDER_TARGETS if key in candidate.models}
    expected = {'present': set(serving.LADDER_TARGETS), 'disabled': set(),
                'bad_calibration': set(serving.LADDER_TARGETS) - {'rad_ge21'},
                'contract_mismatch': set(serving.LADDER_TARGETS) - {'rad_ge21'}}[mode]
    assert loaded == expected
    assert set(candidate.calibration) == set(candidate.models)


def _load_real_loader_candidate(monkeypatch, tmp_path, name, mutate=None):
    """Copy the real bundle dir, optionally corrupt it, load it with the production loader."""
    import json
    import shutil
    from base import kills_v3_serving
    import kv3_panel_serving as serving
    from ml_panel import load_specs

    src = Path(__file__).resolve().parents[2] / 'ml-models/prematch_panel_kv3'
    root = tmp_path / name
    directory = root / 'bundle'
    shutil.copytree(src, directory)
    if mutate is not None:
        mutate(directory)
    features = json.loads((directory / 'feature_names.json').read_text())
    parameters = json.loads((directory / 'manifest.json').read_text())['od3_parameters']
    state = SimpleNamespace(serving_last_overvisible_seconds=0, serving_meta={
        'cutoff': 1782864000, 'history_start': parameters['history_start'],
        'visibility_delay': parameters['visibility_delay'], 'builder_params': parameters,
        'tp_names': features['kv3_source_names'] + [f'unused_{i}' for i in range(30)]})
    monkeypatch.setattr(kills_v3_serving, 'load_state', lambda path: state)
    state_file = root / 'state.npz'
    state_file.write_bytes(b'')
    monkeypatch.setenv('KV3_STATE_PATH', str(state_file))
    monkeypatch.setenv('KV3_PANEL_DIR', str(directory))
    monkeypatch.setenv('ML_PANEL_KV3', '1')
    monkeypatch.delenv('ML_PANEL_KV3_LADDER_SHADOW', raising=False)
    monkeypatch.setattr(serving, '_candidate', None)
    monkeypatch.setattr(serving, '_LADDER_WARNED', set())
    specs = {spec.key: spec for spec in load_specs(directory)}
    bundle = SimpleNamespace(columns=features['panel_columns'],
                             specs=tuple(specs[key] for key in serving.TARGETS))
    candidate = serving._load_uncached(bundle)
    return candidate, serving


def _corrupt_calib(key, mutation):
    def mutate(directory):
        import json
        path = directory / (key + '.calib.json')
        if mutation == 'invalid_json':
            path.write_text('{not json')
            return
        calib = json.loads(path.read_text())
        if mutation == 'nested_knots':
            calib['knots_x'] = [[.1, .2], [.8, .9]]
            calib['knots_y'] = [[.1, .2], [.8, .9]]
        elif mutation == 'nan_knots':
            calib['knots_x'][3] = float('nan')
        elif mutation == 'length_mismatch':
            calib['knots_y'] = calib['knots_y'][:-1]
        elif mutation == 'non_monotone':
            calib['knots_x'][2], calib['knots_x'][3] = calib['knots_x'][3], calib['knots_x'][2]
        elif mutation == 'out_of_range_y':
            calib['knots_y'][0] = 1.5
        elif mutation == 'differs_from_panel_json':
            calib['knots_y'] = [min(1.0, v + .01) for v in calib['knots_y']]
        path.write_text(json.dumps(calib))
    return mutate


@pytest.mark.parametrize('mutation', ['nested_knots', 'nan_knots', 'length_mismatch',
                                      'non_monotone', 'out_of_range_y', 'invalid_json',
                                      'differs_from_panel_json'])
def test_corrupt_shadow_calibration_drops_only_that_shadow_key(monkeypatch, tmp_path, mutation):
    clean, serving = _load_real_loader_candidate(monkeypatch, tmp_path, 'clean')
    clean_result, _ = _serve_fixture_row(clean)
    bad, serving = _load_real_loader_candidate(monkeypatch, tmp_path, 'bad',
                                               _corrupt_calib('rad_ge21', mutation))
    assert set(serving.TARGETS) <= set(bad.models)                  # B/A selection untouched
    assert 'rad_ge21' not in bad.models and 'rad_ge21' not in bad.calibration
    result, _ = _serve_fixture_row(bad)
    by_key = {v.key: v for v in result}
    assert 'rad_ge21' not in by_key
    assert {'rad_ge16', 'rad_ge26', 'dire_ge16', 'dire_ge21', 'dire_ge26'} <= set(by_key)
    assert all(by_key[key].metadata['model'] == 'B_kv3' for key in serving.TARGETS)
    # B verdicts are byte-identical to the uncorrupted run.
    assert [repr(v) for v in result if v.key in serving.TARGETS] == \
        [repr(v) for v in clean_result if v.key in serving.TARGETS]


def test_shadow_ok_is_false_even_if_panel_json_threshold_is_low(monkeypatch, tmp_path):
    import json
    import ml_panel

    def lower_threshold(directory):
        panel = json.loads((directory / 'panel.json').read_text())
        for model in panel['models']:
            if model['key'] in ('rad_ge16', 'dire_ge16'):
                model['threshold'] = 0.5
        (directory / 'panel.json').write_text(json.dumps(panel))

    candidate, serving = _load_real_loader_candidate(monkeypatch, tmp_path, 'low',
                                                     lower_threshold)
    assert candidate.models['rad_ge16'] is not None
    assert serving._specs['rad_ge16'].threshold == 0.5            # the trap is really armed
    result, _ = _serve_fixture_row(candidate)
    by_key = {v.key: v for v in result}
    assert by_key['rad_ge16'].confidence > 0.5                     # would pass the low threshold
    assert not any(v.ok for v in result if v.key in serving.LADDER_TARGETS)
    plain = [v for v in result if v.key not in serving.LADDER_TARGETS]
    assert ml_panel.best_of(result) == ml_panel.best_of(plain)
