import copy
import json
import multiprocessing
from types import SimpleNamespace
from pathlib import Path

import numpy as np
import pytest

import duration43_serving as serving
from duration43_shadow import candidate_vector
from ml_panel import ModelVerdict, journal_row, render

ROOT = Path(__file__).resolve().parents[2]
HEROES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
ACCOUNTS = list(range(101, 111))


@pytest.fixture
def model():
    return serving.DurationModel(ROOT / 'ml-models/duration43_production')


@pytest.fixture
def isolated(monkeypatch, tmp_path, model):
    monkeypatch.setattr(serving, '_MODEL', model)
    monkeypatch.setenv('DURATION43_HISTORY', str(tmp_path / 'history.jsonl'))
    monkeypatch.setenv('DURATION43_PREDICTIONS', str(tmp_path / 'predictions.jsonl'))
    monkeypatch.setenv('DURATION43_SERVING', '1')
    return model


def test_frozen_vector_and_platt_exact(model):
    m = model.manifest
    x, raw, p, meta = model.predict(HEROES, ACCOUNTS, m['built_at'] + 100, 123, {})
    np.testing.assert_array_equal(x, candidate_vector(HEROES, ACCOUNTS, m))
    a, b = m['platt']
    assert p == pytest.approx(1 / (1 + np.exp(-(a * np.log(raw / (1-raw)) + b))), abs=1e-12)
    assert meta['added_maps'] == 0
    assert len(x) == 794


def test_history_strict_end_observation_current_map_and_unknown_accounts(model):
    m = model.manifest
    cutoff = m['built_at'] + 1000
    row = dict(end=cutoff-100, observed_at=cutoff-50, duration=2580, heroes=HEROES, accounts=ACCOUNTS)
    rows = {'1': row, '2': dict(row, end=cutoff), '3': dict(row, observed_at=cutoff),
            '4': dict(row, end=m['max_history_end']), '123': row}
    x, _, _, meta = model.predict(HEROES, ACCOUNTS, cutoff, 123, rows)
    expected = copy.deepcopy(m)
    for kind, keys in [('heroes', HEROES), ('accounts', ACCOUNTS)]:
        for key in keys:
            v = expected['history'][kind].get(str(key), [0, 0, 0, 0])
            expected['history'][kind][str(key)] = [a+b for a, b in zip(v, [43, 1, 1, 1])]
    np.testing.assert_array_equal(x, candidate_vector(HEROES, ACCOUNTS, expected))
    assert meta['added_maps'] == 1
    missing, *_ = model.predict(HEROES, [0]*10, cutoff, 123, rows)
    np.testing.assert_array_equal(missing[-32:-28], np.array([36, .3, .5, 0], dtype=np.float32))
    with pytest.raises(ValueError, match='as-of'):
        model.predict(HEROES, ACCOUNTS, m['built_at'], 123, {})


def result(mid=42):
    return dict(id=mid, leagueId=1, startDateTime=1000, endDateTime=4000,
                durationSeconds=3000, didRadiantWin=True,
                players=[dict(heroId=h, steamAccountId=a) for h, a in zip(HEROES, ACCOUNTS)])


def test_durable_history_idempotent_and_incomplete_excluded(tmp_path):
    path = tmp_path / 'history.jsonl'
    assert serving.record_completed(result(), path=path, now=5000)
    assert not serving.record_completed(result(), path=path, now=6000)
    assert len(path.read_text().splitlines()) == 1
    assert serving.read_rows(path)['42']['observed_at'] == 5000
    invalid = result(43)
    invalid['leagueId'] = 0
    assert not serving.record_completed(invalid, path=path, now=5000)
    assert not serving.record_completed(result(44), path=path, now=3999)
    invalid = result(45)
    invalid['players'] = invalid['players'][:9]
    assert not serving.record_completed(invalid, path=path, now=5000)


def test_pregame_survives_live_clock_and_restart_without_reprediction(isolated, monkeypatch):
    now = isolated.manifest['built_at'] + 100
    other = ModelVerdict('w_5_15', 'window', 'Radiant', .7, .6, 1, True)
    old = ModelVerdict('dur43', 'old', 'no', .1, .99, 1, False)
    ctx = dict(match_id='123', game_time=-30, observed_at=now)
    out = serving.replace_verdict([other, old], HEROES, ACCOUNTS, ctx, now=now+20)
    assert out[0] is other and len(out) == 2
    v = out[-1]
    assert v.metadata['asof'] == now and not v.ok
    monkeypatch.setattr(serving, '_PREDICTION_PATH', None)  # Simulate reload from durable file.
    monkeypatch.setattr(isolated, 'predict', lambda *a: pytest.fail('must reuse pregame prediction'))
    live = serving.replace_verdict([other, old], HEROES, ACCOUNTS, dict(ctx, game_time=1800), now=now+2000)
    assert live[-1].probability == v.probability
    assert live[-1].metadata == v.metadata
    row = journal_row('123', live)
    assert row['models'][-1]['metadata']['model_sha256'] == isolated.manifest['candidate_sha256']


def test_no_live_first_prediction_and_failure_preserves_other_models(isolated, monkeypatch):
    other = ModelVerdict('w_5_15', 'window', 'Radiant', .7, .6, 1, True)
    old = ModelVerdict('dur43', 'old', 'no', .1, .99, 1, False)
    out = serving.replace_verdict([other, old], HEROES, ACCOUNTS, dict(match_id='123', game_time=10))
    assert out == [other]
    assert 'No saved pregame' in serving.status()['error']
    monkeypatch.setenv('DURATION43_SERVING', '0')
    assert serving.replace_verdict([other, old], [], [], {}) == [other, old]


def test_duration_render_positive_probability_not_complement():
    v = ModelVerdict('dur43', 'duration', 'нет', .3, 1, 1, False,
                     metadata={'version':'fixture'}, band_hit=.29, band_n=500)
    text = render([v])
    assert 'P(≥43 мин) 30%' in text
    assert '70%' not in text and 'кэф от' not in text
    assert 'предматчевая' in text and '29.0%' in text


def test_artifact_tampering_rejected(tmp_path, model):
    (tmp_path/'manifest.json').write_text(json.dumps(model.manifest))
    (tmp_path/'candidate.cbm').write_bytes(b'not-the-model')
    with pytest.raises(ValueError, match='hash mismatch'):
        serving.DurationModel(tmp_path)


def _persist_worker(path, barrier, queue, p):
    verdict = ModelVerdict('dur43', 'duration', 'да', p, 1, 1, False)
    barrier.wait(timeout=10)
    got = serving.persist_first_prediction(Path(path), 'same-map-draft', verdict)
    queue.put(got.probability)


def test_two_processes_use_same_first_prediction(tmp_path):
    ctx = multiprocessing.get_context('spawn')
    barrier, queue = ctx.Barrier(2), ctx.Queue()
    path = tmp_path/'predictions.jsonl'
    workers = [ctx.Process(target=_persist_worker, args=(str(path), barrier, queue, p)) for p in (.6, .7)]
    for worker in workers:
        worker.start()
    results = [queue.get(timeout=15) for _ in workers]
    for worker in workers:
        worker.join(timeout=15)
        assert worker.exitcode == 0
    assert results[0] == results[1]
    assert len(path.read_text().splitlines()) == 1


def test_interrupted_history_append_does_not_swallow_new_result(tmp_path):
    path = tmp_path/'history.jsonl'
    path.write_text('{"match_id": "broken"')
    assert serving.record_completed(result(), path=path, now=5000)
    assert serving.read_rows(path)['42']['observed_at'] == 5000
    # A new reader, not just the process-local cache, can recover the row.
    assert json.loads(path.read_text().splitlines()[-1])['match_id'] == '42'


def test_panel_integration_and_candidate_failure_isolation(isolated, monkeypatch):
    import prematch_panel_live as panel
    import prematch_panel_scorer
    import hero_side_tables
    import public_kills_block
    import team_ratings
    bundle = SimpleNamespace(ready=True)
    monkeypatch.setattr(panel, '_load', lambda: {'bundle': bundle, 'tables': (None, None), 'snap': None})
    monkeypatch.setattr(panel, 'ENABLED', True)
    monkeypatch.setattr(panel, 'HYBRID_ENABLED', False)
    monkeypatch.setattr(panel, '_dict_block', lambda _: None)
    monkeypatch.setattr(panel, 'live_rating_state', lambda: None)
    monkeypatch.setattr(hero_side_tables, 'sym_block', lambda *a: np.zeros((1, 2)))
    monkeypatch.setattr(public_kills_block, 'block', lambda *a: None)
    monkeypatch.setattr(team_ratings, 'block', lambda *a, **kw: None)
    other = ModelVerdict('w_5_15', 'window', 'Radiant', .7, .6, 1, True)
    old = ModelVerdict('dur43', 'old', 'no', .1, .99, 1, False)
    monkeypatch.setattr(prematch_panel_scorer, 'score', lambda *a, **kw: [other, old])
    args = (HEROES[:5], HEROES[5:], ACCOUNTS[:5], ACCOUNTS[5:], None)
    context = dict(match_id='987', game_time=-30)
    out = panel.evaluate_map(*args, now_ts=isolated.manifest['built_at']+100, shadow_context=context)
    assert out[0] is other and out[-1].metadata['match_id'] == '987'
    assert 'P(≥43 мин)' in render(out)
    def fail(*args, **kwargs):
        raise RuntimeError('candidate failed')
    monkeypatch.setattr(serving, 'replace_verdict', fail)
    assert panel.evaluate_map(*args, shadow_context=context) == [other]


def test_production_retry_preserves_default_store_for_duration_ingestion(tmp_path, monkeypatch):
    import prematch_live_delta as delta
    path = tmp_path/'delta.json'
    path.write_text(json.dumps({'snapshot_ts': 0, 'maps': {'42': {'end': 4000}}}))
    monkeypatch.setattr(delta, '_store_path', lambda _: path)
    calls = []
    monkeypatch.setattr(delta, 'record_map', lambda m, **kwargs: calls.append(kwargs))
    full = dict(result(), radiantKills=[1, 2])
    assert delta.retry_incomplete(fetch=lambda _: full, now=5000) == 1
    assert calls[0]['store_path'] is None
    assert delta.retry_incomplete(fetch=lambda _: full, store_path=path, now=5000) == 1
    assert calls[1]['store_path'] == path
