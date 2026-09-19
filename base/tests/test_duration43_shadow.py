"""Regressions for the E300 missing-input and after-start comparison failures."""
import json
import sys
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import duration43_shadow as shadow
from ml_panel import ModelSpec, evaluate
from prematch_panel_scorer import PanelBundle, score
from tools.evaluate_duration43_shadow import select_cohort
from draft_features import DraftFeatureEncoder, KIND_ROLE


class Model:
    def predict_proba(self, x, **kwargs):
        return np.tile([.6, .4], (len(x), 1))


def fixture():
    spec = ModelSpec('dur43', 'duration', 'yes', 'no', .5)
    bundle = PanelBundle((spec,), ('sym_0', 'kwdict_0'), {'dur43': Model()}, artifact_hashes={'dur43': 'old'})
    manifest = {'built_at': 1, 'max_history_end': .5, 'incumbent_sha256': 'old',
                'incumbent_spec': json.loads(json.dumps(asdict(spec))), 'incumbent_columns': list(bundle.columns),
                'candidate_sha256': 'new'}
    candidate = SimpleNamespace(manifest=manifest, manifest_sha='exp',
                                predict=lambda h, a: (np.array([1., 2.]), .3, .31))
    return bundle, candidate, [evaluate(spec, .4, {}, fill=1)]


def test_disabled_has_no_artifact_io(monkeypatch):
    monkeypatch.delenv('DURATION43_SHADOW_ENABLED', raising=False)
    monkeypatch.setattr(shadow, '_candidate', lambda: pytest.fail('disabled loaded artifact'))
    assert shadow.capture({}, [], [], None, None, []) == 'disabled'


@pytest.mark.parametrize('clock', [None, 0, 95, float('nan'), '-20', True])
def test_only_negative_numeric_clock_can_enroll(monkeypatch, clock):
    monkeypatch.setenv('DURATION43_SHADOW_ENABLED', '1')
    assert shadow.capture({'match_id': '8992864996', 'game_time': clock}, [], [], None, None, []) == 'not_pregame'


def test_common_inputs_exact_vector_nan_and_durable_dedup(monkeypatch, tmp_path):
    monkeypatch.setenv('DURATION43_SHADOW_ENABLED', '1')
    bundle, candidate, verdicts = fixture()
    heroes = list(range(1, 11)); accounts = list(range(101, 111))
    args = ({'match_id': '8992864996', 'game_time': -79}, heroes, accounts, bundle, [2., np.nan], verdicts)
    path = tmp_path / 'capture.jsonl'
    assert shadow.capture(*args, candidate=candidate, path=path) == 'recorded'
    # Dedup relies on the durable file rather than an in-memory prediction cache.
    assert shadow.capture(*args, candidate=candidate, path=path) == 'duplicate'
    rows = [json.loads(s) for s in path.read_text().splitlines()]
    assert len(rows) == 1
    r = rows[0]
    assert r['heroes'] == heroes and r['accounts'] == accounts
    assert r['incumbent_vector'] == [2., None] and r['candidate_vector'] == [1., 2.]
    assert r['incumbent_model_sha256'] == 'old' and r['candidate_model_sha256'] == 'new'
    assert r['recorded_at'] >= r['capture_started_at']
    bundle.artifact_hashes['dur43'] = 'changed'
    with pytest.raises(ValueError, match='version changed'):
        shadow.capture(*args, candidate=candidate, path=path)


def test_observer_failure_preserves_incumbent_verdict_and_actual_input():
    bundle, _, _ = fixture()
    seen = []
    def observe(x, verdicts):
        seen.append(x.copy()); x[:] = 999
        raise OSError('disk full')
    normal = score(bundle, {'card': {'sym_0': 2.}}, with_draft=False)
    got = score(bundle, {'card': {'sym_0': 2.}}, with_draft=False, row_observer=observe)
    assert got == normal
    assert seen[0][0] == 2 and np.isnan(seen[0][1])


def test_portable_candidate_encoding_and_fixed_history():
    heroes = np.arange(1, 11).reshape(1, 10)
    enc = DraftFeatureEncoder.fit(heroes, KIND_ROLE, signed=False)
    manifest = {'hero_ids': enc.hero_ids.tolist(), 'history': {'accounts': {}, 'heroes': {'1': [60, 1, 1, 1]}}}
    x = shadow.candidate_vector(heroes[0], [0]*10, manifest)
    assert np.array_equal(x[:60], enc.transform(heroes).toarray()[0])
    assert len(x) == 92
    np.testing.assert_allclose(x[60:64], [36, .3, .5, 0], atol=1e-6)
    assert x[76] > 36  # Hero history includes the one completed long game.


def test_endpoint_chosen_without_outcomes_and_after_start_excluded():
    # Real E300 failure: negative/stale clock cannot overrule actual start.
    rows = [{'experiment_sha256':'exp', 'match_id': str(i), 'recorded_at': ts}
            for i, ts in [(1,100), (2,86500), (3,86600)]]
    status, selected = select_cohort(rows[:1], {}, 'exp', min_maps=2, min_days=1)
    assert status['status'] == 'PENDING_ENROLLMENT' and not selected
    status, selected = select_cohort(rows, {}, 'exp', min_maps=2, min_days=1)
    assert status['status'] == 'PENDING_OUTCOMES' and status['enrolled'] == 2
    outcomes = {'1': {'start':101, 'end':2681, 'observed_ts':3000, 'schema_version':1, 'source':'stratz_player_result'},
                '2': {'start':86000, 'end':89000, 'observed_ts':90000, 'schema_version':1, 'source':'stratz_player_result'}}
    status, selected = select_cohort(rows, outcomes, 'exp', min_maps=2, min_days=1)
    assert status['status'] == 'INSUFFICIENT_ELIGIBLE' and status['excluded_not_prestart'] == ['2']
    assert len(selected) == 1 and selected[0][1] is True  # exactly43min
    outcomes['1']['observed_ts'] = 200
    with pytest.raises(ValueError, match='chronology'):
        select_cohort(rows, outcomes, 'exp', min_maps=2, min_days=1)


def test_queue_is_bounded_and_serving_does_not_wait_for_capture(monkeypatch):
    import queue
    import threading
    monkeypatch.setenv('DURATION43_SHADOW_ENABLED', '1')
    q = queue.Queue(maxsize=1)
    monkeypatch.setattr(shadow, '_QUEUE', q)
    monkeypatch.setattr(shadow, '_WORKER', None)
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    received = []
    def blocked_capture(*args):
        received.append(args)
        started.set()
        assert release.wait(3)
        finished.set()
        return 'recorded'
    monkeypatch.setattr(shadow, 'capture', blocked_capture)
    bundle, _, verdicts = fixture()
    x = np.array([2., 3.]); heroes = list(range(1,11))
    try:
        assert shadow.submit({'game_time':-50}, heroes, heroes, bundle, x, verdicts) == 'queued'
        assert started.wait(1)
        x[:] = 999; heroes[0] = 999
        assert shadow.submit({}, [], [], bundle, x, verdicts) == 'queued'
        assert shadow.submit({}, [], [], bundle, x, verdicts) == 'queue_full'
        assert not finished.is_set()  # Both enqueues returned while capture was blocked.
        assert received[0][1][0] == 1 and received[0][4][0] == 2
    finally:
        release.set()
        assert finished.wait(1)
        q.join()
        q.put(None)
        shadow._WORKER.join(1)
        assert not shadow._WORKER.is_alive()


def test_contended_journal_lock_rejects_without_waiting(tmp_path):
    import fcntl
    path = tmp_path / 'locked.jsonl'
    with path.open('a+') as held:
        fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            shadow._append_first(path, {'experiment_sha256':'exp', 'match_id':'1'})
    assert path.read_text() == ''


def test_builder_rejects_other_candidate_before_loading_pickle(tmp_path):
    from tools.build_duration43_shadow import verify_candidate
    (tmp_path / 'causal_history.cbm').write_bytes(b'not-E299')
    with pytest.raises(ValueError, match='E299/August'):
        verify_candidate(tmp_path)


def test_outcome_contract_and_hint_discrepancy_are_explicit():
    rows = [{'experiment_sha256':'exp', 'match_id':'1', 'recorded_at':100, 'start_hint':110}]
    outcome = {'start':101, 'end':2680, 'observed_ts':3000,
               'schema_version':1, 'source':'unknown'}
    with pytest.raises(ValueError, match='producer/schema'):
        select_cohort(rows, {'1':outcome}, 'exp', 1, 0)
    outcome['source'] = 'stratz_team_result_api'
    status, selected = select_cohort(rows, {'1':outcome}, 'exp', 1, 0)
    assert status['status'] == 'READY' and selected[0][1] is False
    assert status['start_hint_differences'] == [{'match_id':'1', 'seconds':9}]
