"""Display isolation and causal cache contract for team-NW10 serving."""
from types import SimpleNamespace

import numpy as np
import pytest

from base import laning_serving as serving


def fake_service():
    service = serving.LaningService()
    calls = []
    def history(heroes, accounts, timestamp, **kwargs):
        calls.append((tuple(heroes), tuple(accounts), timestamp, kwargs))
        return np.zeros((10, 12), dtype=np.float32)
    service.history_store = SimpleNamespace(history=history)
    service.model = SimpleNamespace(history_config={
        'availability_delay_seconds': 3600, 'recent_window_seconds': 2592000},
        predict_proba=lambda heroes, history: np.array([[.39, .01, .60]]))
    service.loaded = True
    return service, calls


def test_cache_includes_draft_players_and_time(monkeypatch):
    monkeypatch.setattr(serving, 'ENABLED', True)
    service, calls = fake_service()
    heroes, accounts = tuple(range(1, 11)), tuple(range(101, 111))
    expected = service.predict(heroes, accounts, 10000)
    expected[0] = 1  # A caller cannot corrupt future cards.
    assert service.predict(heroes, accounts, 10000)[0] == .39
    service.predict(heroes, accounts, 10001)
    service.predict(heroes, tuple(range(201, 211)), 10000)
    service.predict(tuple(reversed(heroes)), accounts, 10000)
    assert len(calls) == 4
    assert calls[0][3] == {'availability_delay_seconds': 3600,
                           'recent_window_seconds': 2592000}


@pytest.mark.parametrize('probability', [[[.5, .5]], [[.4, .1, np.nan]],
                                       [[.4, .1, .6]], [[-.1, .1, 1.]]])
def test_invalid_probabilities_are_isolated(probability, monkeypatch):
    monkeypatch.setattr(serving, 'ENABLED', True)
    service, _ = fake_service()
    service.model.predict_proba = lambda *args: probability
    assert service.predict(range(1, 11), range(101, 111), 10000) is None
    assert service.error


def test_missing_artifact_isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(serving, 'ENABLED', True)
    service = serving.LaningService(tmp_path, tmp_path)
    assert service.predict(range(1, 11), range(101, 111), 10000) is None
    assert service.error


def test_disabled_does_not_load(monkeypatch):
    monkeypatch.setattr(serving, 'ENABLED', False)
    service = serving.LaningService()
    assert service.predict(range(1, 11), range(101, 111), 10000) is None
    assert not service.loaded


def test_draft_winner_needs_no_history_query(monkeypatch):
    monkeypatch.setattr(serving, 'ENABLED', True)
    service, calls = fake_service()
    service.model.with_history = False
    service.model.history_config = {'availability_delay_seconds': 0,
                                    'recent_window_seconds': None}
    assert service.predict(range(1, 11), range(101, 111), 10000) is not None
    assert calls == []


def test_all_and_laning_fail_independently(monkeypatch):
    heroes = tuple(range(1, 11))
    draft = SimpleNamespace(_heroes_vector=lambda *args: heroes,
                            win_index_draft=lambda *args: -5.4)
    teams = [{f'pos{i}': {'account_id': i + offset} for i in range(1, 6)}
             for offset in (0, 5)]
    calls = []
    def predict(h, a, t):
        calls.append((h, a, t))
        return np.array([.39, .01, .60])
    monkeypatch.setattr(serving, '_SERVICE', SimpleNamespace(predict=predict))
    lines = serving.panel_lines(*teams, 12345, draft_model=draft)
    # ml_laning_line hits 60.0%, exactly ML_DISPATCH_MIN_CONF's default
    # threshold, so it gets the " ★" marker; all_model_line at 55.4% does not.
    assert lines == {'ml_laning_line': 'ML Laning: Radiant 60.0% (золото, 10 мин) ★',
                     'all_model_line': '🌐 All ML-модель: Dire 55.4%'}
    assert calls == [(heroes, list(range(1, 11)), 12345)]
    draft.win_index_draft = lambda *args: None
    assert serving.panel_lines(*teams, 12345, draft_model=draft) == dict(
        lines, all_model_line='')
    draft.win_index_draft = lambda *args: -5.4
    serving._SERVICE.predict = lambda *args: None
    assert serving.panel_lines(*teams, 12345, draft_model=draft) == dict(
        lines, ml_laning_line='')


def test_equal_gold_is_a_real_class(monkeypatch):
    monkeypatch.setattr(serving, '_SERVICE', SimpleNamespace(
        predict=lambda *args: np.array([.2, .6, .2])))
    draft = SimpleNamespace(_heroes_vector=lambda *args: tuple(range(1, 11)),
                            win_index_draft=lambda *args: None)
    result = serving.panel_lines({}, {}, 12345, draft_model=draft)
    # 60.0% also hits the default ML_DISPATCH_MIN_CONF threshold -> starred,
    # even though "Равенство" itself is not a bettable side (display-only).
    assert result['ml_laning_line'] == 'ML Laning: Равенство 60.0% (золото, 10 мин) ★'
