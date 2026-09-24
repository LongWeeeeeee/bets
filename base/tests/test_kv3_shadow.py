"""The KV3 observer is prospective and cannot alter panel verdicts."""
import base64
import gc
import json
import os
import queue
import sys
import time
import weakref
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from catboost import CatBoostClassifier

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(autouse=True)
def reset_shadow(monkeypatch):
    import kv3_shadow as shadow
    monkeypatch.setattr(shadow, '_CACHED', None)
    monkeypatch.setattr(shadow, '_DISABLED', False)
    monkeypatch.setattr(shadow, '_WARNED', False)
    monkeypatch.setattr(shadow, '_CONSECUTIVE_ERRORS', 0)
    monkeypatch.setattr(shadow, '_STATUS', {'submitted': 0, 'dropped': 0, 'processed': 0, 'errors': 0})


@pytest.fixture
def artifacts(tmp_path, monkeypatch):
    from base import kills_v3_serving as serving
    from base.tools import kills_v3_research as v3
    from base.tests.test_kills_v3_research import toy_rich

    start = v3.QUERY_START + v3.DAY
    cutoff = start - 7000
    rich = tmp_path / 'toy.npz'
    toy_rich(rich, [start - 20000, start - 7200, start], [1800] * 3,
             [(10, 15), (40, 15), (25, 10)])
    state = serving.build_state(rich, [], history_start=v3.HISTORY_START,
                                cutoff=cutoff, pseudo_games=0)
    state_path = tmp_path / 'state.npz'
    serving.save_state(state, state_path)
    columns = ['panel_' + str(i) for i in range(928)]
    names = state.serving_meta['tp_names']
    directory = tmp_path / 'models'
    directory.mkdir()
    (directory / 'feature_names.json').write_text(json.dumps({
        'panel_columns': columns,
        'kv3_columns': ['kv3_' + name for name in names],
        'kv3_source_names': names,
    }))
    (directory / 'manifest.json').write_text('{}')
    rng = np.random.default_rng(7)
    samples = rng.normal(size=(20, 1080)).astype(np.float32)
    for key in ('w_5_15', 'w_10_20', 'w_15_25', 'w_20_30', 'rad_30_25', 'total_55_50'):
        model = CatBoostClassifier(iterations=5, depth=2, verbose=False, thread_count=1)
        model.fit(samples, np.arange(20) % 2)
        model.save_model(str(directory / (key + '.cbm')))
        (directory / (key + '.calib.json')).write_text(json.dumps({
            'knots_x': [0, 1], 'knots_y': [0, 1],
        }))
    monkeypatch.setenv('KV3_SHADOW_ENABLED', '1')
    monkeypatch.setenv('KV3_SHADOW_DIR', str(directory))
    monkeypatch.setenv('KV3_STATE_PATH', str(state_path))
    return SimpleNamespace(directory=directory, state_path=state_path,
                           bundle=SimpleNamespace(columns=tuple(columns)), start=start)


def _context(start, radiant=1, dire=2):
    return {'match_id': '42', 'start_ts': start,
            'radiant_team_id': radiant, 'dire_team_id': dire}


def test_live_context_uses_match_team_ids_and_start(monkeypatch):
    import win_model_veto as veto
    monkeypatch.delenv('KV3_SHADOW_ENABLED', raising=False)
    assert set(veto._prediction_context({})) == {
        'match_id', 'observed_at', 'map_key', 'game_time', 'elo_evaluation_timestamp'}
    monkeypatch.setenv('KV3_SHADOW_ENABLED', '1')
    match = {'match_id': 42, 'id': 555, 'startDateTime': 1800000000,
             'radiantTeam': {'id': 101}, 'dire_team_id': 202}
    context = veto._prediction_context(match)
    assert context['map_id'] == '42'
    assert context['radiant_team_id'] == 101
    assert context['dire_team_id'] == 202
    assert context['start_ts'] == 1800000000
    assert context['start_ts_source'] == 'startDateTime'
    fallback = veto._prediction_context({'startDateTime': 'invalid',
                                         'radiant_team_id': '-17', 'direTeam': {'id': 'bad'}})
    assert fallback['start_ts_source'] == 'now_fallback'
    assert fallback['radiant_team_id'] == -17 and fallback['dire_team_id'] == 0


def test_live_context_signed_team_real_map_and_diagnostics(artifacts, tmp_path, monkeypatch):
    import kv3_shadow as shadow
    import win_model_veto as veto
    from base import kills_v3_serving as serving

    match = {'match_id': 8992864996, 'id': 555, 'startDateTime': 1800000000,
             'radiantTeam': {'id': -17}, 'direTeam': {'id': 202},
             'map_key': 'series/9.2'}
    context = veto._prediction_context(match)
    assert context['map_id'] == '8992864996'
    assert context['radiant_team_id'] == -17
    assert context['dire_team_id'] == 202
    seen = []
    candidate = shadow.Candidate(artifacts.bundle)
    names = candidate.state.serving_meta['tp_names']
    monkeypatch.setattr(serving, 'features_for_map',
                        lambda state, rt, dt, ra, da, ts:
                        (seen.append((rt, dt)) or names, np.zeros(152, np.float32)))
    candidate.predict = lambda x: {key: .5 for key in shadow.TARGETS}
    path = tmp_path / 'journal.jsonl'
    panel = np.zeros(928, np.float32)
    panel[2] = np.nan
    result = shadow.capture(context, (), tuple(range(1, 11)), artifacts.bundle,
                            panel, (), candidate=candidate, path=path)
    assert result == 'recorded'
    row = json.loads(path.read_text().splitlines()[0])
    assert seen == [(-17, 202)]
    assert row['gate_reason'] is None and len(row['targets']) == 6
    assert row['map_id'] == '8992864996' and row['map_key'] == 'series/9.2'
    assert row['x_panel_nan_share'] == pytest.approx(1 / 928)
    assert row['start_ts_source'] == 'startDateTime'


def test_missing_match_id_dedups_by_numbered_map_key(artifacts, tmp_path):
    import kv3_shadow as shadow
    import win_model_veto as veto

    candidate = shadow.Candidate(artifacts.bundle)
    candidate.predict = lambda x: {key: .5 for key in shadow.TARGETS}
    path = tmp_path / 'journal.jsonl'
    match = {'id': 555, 'startDateTime': artifacts.start,
             'radiantTeam': {'id': 1}, 'direTeam': {'id': 2}}
    for suffix in ('.1', '.2'):
        context = veto._prediction_context(dict(match, map_key='series/9' + suffix))
        assert context['map_id'] is None
        assert shadow.capture(context, (), tuple(range(1, 11)), artifacts.bundle,
                              np.zeros(928), (), candidate=candidate, path=path) == 'recorded'
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert [row['map_key'] for row in rows] == ['series/9.1', 'series/9.2']
    assert [row['map_id'] for row in rows] == [None, None]
    assert [row['start_ts_source'] for row in rows] == ['startDateTime'] * 2


def test_nan_prod_summary_precedes_single_features_line(artifacts, tmp_path, monkeypatch):
    import kv3_shadow as shadow

    features_path = tmp_path / 'features.jsonl'
    monkeypatch.setenv('KV3_SHADOW_FEATURES', str(features_path))
    path = tmp_path / 'journal.jsonl'
    candidate = shadow.Candidate(artifacts.bundle)
    verdicts = [SimpleNamespace(key='w_5_15', probability=float('nan'))]
    context = _context(artifacts.start)
    append_features = shadow._append_features

    def summary_first(row):
        assert len(path.read_text().splitlines()) == 1
        append_features(row)

    monkeypatch.setattr(shadow, '_append_features', summary_first)
    assert shadow.capture(context, (), tuple(range(1, 11)), artifacts.bundle,
                          np.zeros(928), verdicts, candidate=candidate, path=path) == 'recorded'
    assert shadow.capture(context, (), tuple(range(1, 11)), artifacts.bundle,
                          np.zeros(928), verdicts, candidate=candidate, path=path) == 'duplicate'
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert len(rows) == 1 and rows[0]['targets']['w_5_15']['p_prod'] is None
    assert len(features_path.read_text().splitlines()) == 1


def test_disabled_does_not_load(monkeypatch, artifacts, tmp_path):
    import kv3_shadow as shadow
    monkeypatch.delenv('KV3_SHADOW_ENABLED')
    monkeypatch.setattr(shadow, '_candidate', lambda b: pytest.fail('loaded while off'))
    path = tmp_path / 'journal.jsonl'
    assert shadow.capture(_context(artifacts.start), (), (), artifacts.bundle,
                          np.zeros(928), (), path=path) == 'disabled'
    assert not path.exists()


def test_full_queue_never_waits_for_shadow_worker(artifacts, monkeypatch):
    import kv3_shadow as shadow
    pending = queue.Queue(maxsize=1)
    pending.put_nowait(object())
    monkeypatch.setattr(shadow, '_QUEUE', pending)
    monkeypatch.setattr(shadow, '_WORKER', SimpleNamespace(is_alive=lambda: True))
    started = time.monotonic()
    result = shadow.submit(_context(artifacts.start), (), tuple(range(1, 11)),
                           artifacts.bundle, np.zeros(928), ())
    assert result == 'queue_full'
    assert time.monotonic() - started < .1


def test_six_calibrated_targets_and_prod_pair(artifacts, tmp_path):
    import kv3_shadow as shadow
    path = tmp_path / 'journal.jsonl'
    verdicts = [SimpleNamespace(key='w_5_15', probability=.62)]
    assert shadow.capture(_context(artifacts.start), (), tuple(range(1, 11)),
                          artifacts.bundle, np.zeros(928), verdicts, path=path) == 'recorded'
    row = json.loads(path.read_text().splitlines()[0])
    assert row['gate_reason'] is None and row['error'] is None
    assert row['state_cutoff'] == artifacts.start - 7000
    assert row['state_cutoff_age_hours'] > 0
    assert len(row['manifest_sha256']) == 64
    assert len(row['targets']) == 6
    assert row['targets']['w_5_15']['p_prod'] == .62
    assert row['targets']['w_10_20']['p_prod'] is None
    assert all(0 <= t['p_b'] <= 1 for t in row['targets'].values())
    assert 0 <= row['kv3_finite_share'] <= 1


def test_no_team_gate_without_loading_state(artifacts, tmp_path, monkeypatch):
    import kv3_shadow as shadow
    monkeypatch.setattr(shadow, '_candidate', lambda b: pytest.fail('state loaded'))
    path = tmp_path / 'journal.jsonl'
    assert shadow.capture(_context(artifacts.start, radiant=0), (), (), artifacts.bundle,
                          np.zeros(928), (), path=path) == 'recorded'
    row = json.loads(path.read_text().splitlines()[0])
    assert row['gate_reason'] == 'no_team' and row['targets'] == {}


def test_state_newer_than_map_gate(artifacts, tmp_path):
    import kv3_shadow as shadow
    path = tmp_path / 'journal.jsonl'
    assert shadow.capture(_context(artifacts.start - 7000), (), tuple(range(1, 11)),
                          artifacts.bundle, np.zeros(928), (), path=path) == 'recorded'
    row = json.loads(path.read_text().splitlines()[0])
    assert row['gate_reason'] == 'state_newer_than_map' and row['targets'] == {}


def test_panel_column_order_mismatch_disables_shadow(artifacts, tmp_path):
    import kv3_shadow as shadow
    path = tmp_path / 'journal.jsonl'
    wrong = SimpleNamespace(columns=tuple(reversed(artifacts.bundle.columns)))
    assert shadow.capture(_context(artifacts.start), (), tuple(range(1, 11)),
                          wrong, np.zeros(928), (), path=path) == 'recorded'
    row = json.loads(path.read_text().splitlines()[0])
    assert 'feature contract' in row['error'] and row['targets'] == {}
    assert shadow.status()['disabled']


def test_state_source_names_mismatch_disables_shadow(artifacts, tmp_path):
    import kv3_shadow as shadow
    path = tmp_path / 'journal.jsonl'
    feature_path = artifacts.directory / 'feature_names.json'
    features = json.loads(feature_path.read_text())
    features['kv3_source_names'][0] = 'wrong_name'
    features['kv3_columns'][0] = 'kv3_wrong_name'
    feature_path.write_text(json.dumps(features))
    assert shadow.capture(_context(artifacts.start), (), tuple(range(1, 11)),
                          artifacts.bundle, np.zeros(928), (), path=path) == 'recorded'
    row = json.loads(path.read_text().splitlines()[0])
    assert 'feature contract' in row['error'] and shadow.status()['disabled']


def test_missing_state_warns_once_and_disables(artifacts, tmp_path, monkeypatch, caplog):
    import kv3_shadow as shadow
    monkeypatch.setenv('KV3_STATE_PATH', str(tmp_path / 'absent.npz'))
    path = tmp_path / 'journal.jsonl'
    args = (_context(artifacts.start), (), tuple(range(1, 11)),
            artifacts.bundle, np.zeros(928), ())
    assert shadow.capture(*args, path=path) == 'recorded'
    assert shadow.capture(*args, path=path) == 'disabled'
    assert len(path.read_text().splitlines()) == 1
    assert len([r for r in caplog.records if 'KV3 shadow disabled' in r.message]) == 1


def test_worker_exception_is_audited_without_raising(artifacts, tmp_path):
    import kv3_shadow as shadow
    path = tmp_path / 'journal.jsonl'
    candidate = shadow.Candidate(artifacts.bundle)
    candidate.predict = lambda x: (_ for _ in ()).throw(RuntimeError('forced'))
    assert shadow.capture(_context(artifacts.start), (), tuple(range(1, 11)),
                          artifacts.bundle, np.zeros(928), (), candidate=candidate,
                          path=path) == 'recorded'
    row = json.loads(path.read_text().splitlines()[0])
    assert row['error'] == 'RuntimeError: forced' and row['targets'] == {}


def test_map_error_does_not_disable_next_map(artifacts, tmp_path, monkeypatch):
    import kv3_shadow as shadow
    from base import kills_v3_serving as serving
    path = tmp_path / 'journal.jsonl'
    candidate = shadow.Candidate(artifacts.bundle)
    original = serving.features_for_map
    calls = [0]

    def fail_once(*args):
        calls[0] += 1
        if calls[0] == 1:
            raise ValueError('bad accounts')
        return original(*args)

    monkeypatch.setattr(serving, 'features_for_map', fail_once)
    args = ((), tuple(range(1, 11)), artifacts.bundle, np.zeros(928), ())
    assert shadow.capture(_context(artifacts.start), *args, candidate=candidate, path=path) == 'recorded'
    assert not shadow.status()['disabled']
    second = dict(_context(artifacts.start), match_id='43')
    assert shadow.capture(second, *args, candidate=candidate, path=path) == 'recorded'
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert rows[0]['error'] == 'ValueError: bad accounts'
    assert len(rows[1]['targets']) == 6
    assert shadow.status()['errors'] == 1
    assert shadow._CONSECUTIVE_ERRORS == 0


def test_50_consecutive_map_errors_disable(artifacts, tmp_path, monkeypatch, caplog):
    import kv3_shadow as shadow
    from base import kills_v3_serving as serving
    candidate = shadow.Candidate(artifacts.bundle)
    monkeypatch.setattr(serving, 'features_for_map', lambda *args: (_ for _ in ()).throw(ValueError('bad map')))
    path = tmp_path / 'journal.jsonl'
    for i in range(50):
        context = dict(_context(artifacts.start), match_id=str(i + 100))
        assert shadow.capture(context, (), tuple(range(1, 11)), artifacts.bundle,
                              np.zeros(928), (), candidate=candidate, path=path) == 'recorded'
    assert shadow.status()['disabled']
    assert shadow.status()['errors'] == 50
    assert len([r for r in caplog.records if 'KV3 shadow disabled' in r.message]) == 1


def test_changed_state_reloads_after_releasing_old(artifacts, tmp_path, monkeypatch):
    import kv3_shadow as shadow
    from base import kills_v3_serving as serving
    candidate = shadow.Candidate(artifacts.bundle)
    old_ref = weakref.ref(candidate.state)
    original = serving.load_state
    calls = []

    def checked_loader(path):
        gc.collect()
        assert candidate.state is None
        assert old_ref() is None
        calls.append(path)
        return original(path)

    monkeypatch.setattr(serving, 'load_state', checked_loader)
    stat = artifacts.state_path.stat()
    os.utime(artifacts.state_path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10_000_000))
    candidate.last_state_check = 0
    monkeypatch.setattr(shadow.time, 'monotonic', lambda: 10_000.0)
    path = tmp_path / 'journal.jsonl'
    for mid in ('101', '102'):
        context = dict(_context(artifacts.start), match_id=mid)
        assert shadow.capture(context, (), tuple(range(1, 11)), artifacts.bundle,
                              np.zeros(928), (), candidate=candidate, path=path) == 'recorded'
    assert calls == [artifacts.state_path]
    assert all(json.loads(line)['state_mtime'] == artifacts.state_path.stat().st_mtime_ns
               for line in path.read_text().splitlines())


def test_bad_replacement_gates_until_next_file_change(artifacts, tmp_path, monkeypatch):
    import kv3_shadow as shadow
    from base import kills_v3_serving as serving
    candidate = shadow.Candidate(artifacts.bundle)
    original = serving.load_state
    calls = [0]

    def fail_once(path):
        calls[0] += 1
        if calls[0] == 1:
            raise ValueError('bad replacement')
        return original(path)

    monkeypatch.setattr(serving, 'load_state', fail_once)
    stat = artifacts.state_path.stat()
    os.utime(artifacts.state_path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10_000_000))
    clock = [10_000.0]
    monkeypatch.setattr(shadow.time, 'monotonic', lambda: clock[0])
    candidate.last_state_check = 0
    path = tmp_path / 'journal.jsonl'
    args = ((), tuple(range(1, 11)), artifacts.bundle, np.zeros(928), ())
    for mid in ('101', '102'):
        assert shadow.capture(dict(_context(artifacts.start), match_id=mid), *args,
                              candidate=candidate, path=path) == 'recorded'
        clock[0] += 601
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert [r['gate_reason'] for r in rows] == ['state_unavailable'] * 2
    assert rows[0]['error'] == 'ValueError: bad replacement'
    assert not shadow.status()['disabled'] and calls == [1]
    stat = artifacts.state_path.stat()
    os.utime(artifacts.state_path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10_000_000))
    assert shadow.capture(dict(_context(artifacts.start), match_id='103'), *args,
                          candidate=candidate, path=path) == 'recorded'
    assert calls == [2]
    assert len(json.loads(path.read_text().splitlines()[-1])['targets']) == 6


def test_subset_candidate_selects_named_columns(artifacts, tmp_path, monkeypatch):
    import kv3_shadow as shadow
    from base import kills_v3_serving as serving
    feature_path = artifacts.directory / 'feature_names.json'
    features = json.loads(feature_path.read_text())
    full_names = features['kv3_source_names']
    subset = full_names[::2][:76] + full_names[1::2][:46]
    features['kv3_source_names'] = subset
    features['kv3_columns'] = ['kv3_' + name for name in subset]
    feature_path.write_text(json.dumps(features))
    candidate = shadow.Candidate(artifacts.bundle)
    full = np.arange(152, dtype=np.float32)
    monkeypatch.setattr(serving, 'features_for_map', lambda *args: (full_names, full))
    seen = []
    candidate.predict = lambda x: (seen.append(x.copy()) or {key: .5 for key in shadow.TARGETS})
    path = tmp_path / 'journal.jsonl'
    assert shadow.capture(_context(artifacts.start), (), tuple(range(1, 11)),
                          artifacts.bundle, np.zeros(928), (), candidate=candidate, path=path) == 'recorded'
    expected = full[[full_names.index(name) for name in subset]]
    np.testing.assert_array_equal(seen[0][0, 928:], expected)


def test_features_journal_has_full_float32_vectors(artifacts, tmp_path, monkeypatch):
    import kv3_shadow as shadow
    from base import kills_v3_serving as serving
    candidate = shadow.Candidate(artifacts.bundle)
    candidate.state.serving_last_overvisible_seconds = 17
    names = candidate.state.serving_meta['tp_names']
    panel = np.arange(928, dtype=np.float32) / 3
    full = np.arange(152, dtype=np.float32) / 7
    monkeypatch.setattr(serving, 'features_for_map', lambda *args: (names, full))
    features_path = tmp_path / 'features.jsonl'
    monkeypatch.setenv('KV3_SHADOW_FEATURES', str(features_path))
    path = tmp_path / 'journal.jsonl'
    assert shadow.capture(_context(artifacts.start), (), tuple(range(1, 11)),
                          artifacts.bundle, panel, (), candidate=candidate, path=path) == 'recorded'
    row = json.loads(features_path.read_text().splitlines()[0])
    np.testing.assert_array_equal(np.frombuffer(base64.b64decode(row['x_panel_f32_b64']), dtype='<f4'), panel)
    np.testing.assert_array_equal(np.frombuffer(base64.b64decode(row['x_kv3_tp_f32_b64']), dtype='<f4'), full)
    assert row['map_id'] == '42' and row['state_cutoff'] == candidate.cutoff
    assert json.loads(path.read_text().splitlines()[0])['overvisible_s'] == 17
    assert shadow.capture(_context(artifacts.start), (), tuple(range(1, 11)),
                          artifacts.bundle, panel, (), candidate=candidate, path=path) == 'duplicate'
    assert len(features_path.read_text().splitlines()) == 1


def test_features_write_failure_keeps_summary(artifacts, tmp_path, monkeypatch):
    import kv3_shadow as shadow
    monkeypatch.setattr(shadow, '_append_features', lambda row: (_ for _ in ()).throw(OSError('disk full')))
    path = tmp_path / 'journal.jsonl'
    assert shadow.capture(_context(artifacts.start), (), tuple(range(1, 11)),
                          artifacts.bundle, np.zeros(928), (), path=path) == 'recorded'
    row = json.loads(path.read_text().splitlines()[0])
    assert len(row['targets']) == 6 and not shadow.status()['disabled']
    assert shadow.status()['errors'] == 1


def test_negative_team_is_scored(artifacts, tmp_path):
    import kv3_shadow as shadow
    path = tmp_path / 'journal.jsonl'
    assert shadow.capture(_context(artifacts.start, radiant=-17), (), tuple(range(1, 11)),
                          artifacts.bundle, np.zeros(928), (), path=path) == 'recorded'
    row = json.loads(path.read_text().splitlines()[0])
    assert row['gate_reason'] is None and len(row['targets']) == 6


def test_evaluate_map_hook_preserves_verdicts(monkeypatch):
    import prematch_panel_live as live
    import prematch_panel_scorer as scorer
    from ml_panel import ModelVerdict, render

    verdict = ModelVerdict('w_5_15', 'window', 'Radiant', .61, .5, 1., True, raw=.58)
    bundle = SimpleNamespace(ready=True, columns=('sym_0',))
    monkeypatch.setattr(live, '_load', lambda: {'bundle': bundle, 'tables': (None, None), 'snap': None})
    monkeypatch.setattr(live, '_dict_block', lambda h: None)
    monkeypatch.setattr(live, 'HYBRID_ENABLED', False)
    monkeypatch.setitem(sys.modules, 'hero_side_tables', SimpleNamespace(sym_block=lambda h, c, b: np.ones((1, 1))))
    monkeypatch.setitem(sys.modules, 'public_kills_block', SimpleNamespace(block=lambda h: None))
    monkeypatch.setitem(sys.modules, 'team_ratings', SimpleNamespace(block=lambda *a, **kw: None))
    monkeypatch.setitem(sys.modules, 'duration43_serving', SimpleNamespace(replace_verdict=lambda vs, *a, **kw: vs, status=lambda: {}))
    monkeypatch.setattr(scorer, 'block_from_matrix', lambda *a: {'sym_0': 1.0})
    monkeypatch.setattr(scorer, 'score', lambda *a, **kw: (kw['row_observer'](np.array([1.], np.float32), (verdict,)) if kw['row_observer'] else None) or [verdict])
    captured = []
    monkeypatch.setitem(sys.modules, 'kv3_shadow', SimpleNamespace(submit=lambda *a: captured.append(a)))
    args = (list(range(1, 6)), list(range(6, 11)), list(range(11, 16)), list(range(16, 21)), None)
    context = {'match_id': '42', 'radiant_team_id': 1, 'dire_team_id': 2, 'start_ts': 100}
    monkeypatch.delenv('KV3_SHADOW_ENABLED', raising=False)
    before = live.evaluate_map(*args, shadow_context=context)
    monkeypatch.setenv('KV3_SHADOW_ENABLED', '1')
    after = live.evaluate_map(*args, shadow_context=context)
    assert after == before == [verdict], live._state.get('last_error')
    assert render(after, highlight=[]) == render(before, highlight=[])
    assert len(captured) == 1 and captured[0][0] == context
    monkeypatch.setitem(sys.modules, 'kv3_shadow', SimpleNamespace(
        submit=lambda *a: (_ for _ in ()).throw(RuntimeError('forced submit failure'))))
    assert live.evaluate_map(*args, shadow_context=context) == before
