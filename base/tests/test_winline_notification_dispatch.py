"""Slow Telegram and side-adoption regressions for the asynchronous sender."""
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import cyberscore_try as cs

SERIES = 'test-series'
KEY = SERIES + '|map1|Alpha|Bravo'
SWAP = SERIES + '|map1|Bravo|Alpha'
NEXT = SERIES + '|map2|Alpha|Bravo'
QUOTE = {'p1_odds': 1.8, 'p2_odds': 2.1, 'market_status': 'open', 'accepted': True}
END = {'market_status': 'missing'}
TERMINAL = dict(is_terminal=True, map_end_proven=True, map_confirmed_live=True)


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    assert cs.stop_winline_notification_worker()
    cs._winline_notification_stop.clear()
    for name in ('_winline_notification_queue', '_winline_notification_aliases',
                 '_winline_odds_notify_state', '_winline_odds_orientation_state',
                 '_winline_pending_map_winners', '_winline_deferred_terminals',
                 '_winline_current_map_pollers'):
        getattr(cs, name).clear()
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_ENABLED_ENV, '1')
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_MIN_SPACING_ENV, '0')
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_MAX_PER_MIN_ENV, '0')
    monkeypatch.setenv(cs.WINLINE_MAP_WINNER_ENABLED_ENV, '0')
    monkeypatch.setattr(cs, 'send_winline_odds_message', lambda *a, **k: True)
    monkeypatch.setattr(cs, '_winline_resolve_map_winner', lambda *a, **k: None)
    monkeypatch.setattr(cs, '_winline_write_current_map_evidence', lambda *a, **k: None)
    monkeypatch.setattr(cs, '_winline_map_clock_label', lambda *a: '00:00')
    monkeypatch.setattr(cs, '_winline_net_worth_label', lambda *a: None)
    yield
    assert cs.stop_winline_notification_worker(join_timeout_s=2)
    cs._winline_notification_queue.clear()
    cs._winline_notification_aliases.clear()
    cs._winline_odds_notify_state.clear()
    cs._winline_odds_orientation_state.clear()
    cs._winline_current_map_pollers.clear()


def frozen(monkeypatch):
    monkeypatch.setattr(cs, '_winline_start_notification_worker', lambda: None)


def drain():
    while cs._winline_notification_queue:
        cs._winline_process_notification(cs._winline_notification_queue.pop(0))


def poller():
    return SimpleNamespace(_identity=dict(series=SERIES, map_num=1, team1='Alpha', team2='Bravo'),
                           is_active=lambda: True, terminal=lambda: None,
                           tick=lambda **kw: {'attempt': dict(QUOTE), 'status': 'success'})


def test_slow_sender_does_not_block_next_poll_tick_or_reset_into_second_sender(monkeypatch):
    entered, release, completed = threading.Event(), threading.Event(), threading.Event()
    calls = []
    def send(*a, **kw):
        calls.append(a)
        entered.set()
        assert release.wait(2)
        return True
    monkeypatch.setattr(cs, 'send_winline_odds_message', send)
    cs._winline_current_map_pollers[KEY] = poller()
    try:
        cs.tick_winline_current_map_polling()
        assert entered.wait(1)
        worker = cs._winline_notification_thread
        def tick_again():
            cs.tick_winline_current_map_polling()
            completed.set()
        runner = threading.Thread(target=tick_again)
        runner.start()
        assert completed.wait(0.5), 'poll tick waited on Telegram'
        runner.join(1)
        with pytest.raises(RuntimeError, match='sender is still running'):
            cs.reset_winline_current_map_polling_state()
        cs._winline_start_notification_worker()
        assert cs._winline_notification_thread is worker
        assert len(calls) == 1
    finally:
        release.set()
    assert cs.stop_winline_notification_worker()


def test_fifo_terminal_failure_retries_before_next_map(monkeypatch):
    frozen(monkeypatch)
    messages = []
    def send(message, **kw):
        messages.append(message)
        if 'карта завершена' in message and sum('карта завершена' in m for m in messages) == 1:
            return False
        return True
    monkeypatch.setattr(cs, 'send_winline_odds_message', send)
    # No clock sleep is needed to verify the retry ordering.
    monkeypatch.setattr(cs._winline_notification_stop, 'wait', lambda timeout: False)
    cs._winline_enqueue_notification(QUOTE, KEY)
    cs._winline_enqueue_notification(END, KEY, **TERMINAL)
    cs._winline_enqueue_notification(END, KEY, **TERMINAL)
    cs._winline_enqueue_notification(QUOTE, NEXT)
    drain()
    assert len(messages) == 4
    assert 'карта 1' in messages[0] and '🆕' in messages[0]
    assert all('карта завершена' in m for m in messages[1:3])
    assert 'карта 2' in messages[3] and '🆕' in messages[3]


def test_terminal_failure_exhausts_then_releases_next_map(monkeypatch):
    frozen(monkeypatch)
    messages = []
    monkeypatch.setattr(cs, 'send_winline_odds_message', lambda m, **kw: messages.append(m) or ('карта завершена' not in m))
    monkeypatch.setattr(cs._winline_notification_stop, 'wait', lambda timeout: False)
    for payload, key, kwargs in [(QUOTE, KEY, {}), (END, KEY, TERMINAL), (QUOTE, NEXT, {})]:
        cs._winline_enqueue_notification(payload, key, **kwargs)
    drain()
    assert sum('карта завершена' in m for m in messages) == cs._WINLINE_DEFERRED_TERMINAL_RETRIES + 1
    assert 'карта 2' in messages[-1]


def test_queued_snapshot_coalesces_without_regressing_orientation(monkeypatch):
    frozen(monkeypatch)
    payload = dict(QUOTE, nested={'value': 1}, attempts=['large history'])
    cs._winline_enqueue_notification(payload, KEY)
    payload['nested']['value'] = 99
    assert cs._winline_notification_queue[0]['payload']['nested']['value'] == 1
    assert 'attempts' not in cs._winline_notification_queue[0]['payload']
    cs._winline_enqueue_notification(dict(QUOTE, p1_odds=1.9), KEY)
    assert len(cs._winline_notification_queue) == 1
    latest = {'p1': 2.0, 'p2': 2.2}
    cs._winline_odds_orientation_state[KEY] = dict(latest)
    drain()
    assert cs._winline_odds_orientation_state[KEY] == latest


def test_pressure_keeps_quote_introducing_terminal_and_status_boundaries(monkeypatch):
    frozen(monkeypatch)
    monkeypatch.setattr(cs, '_WINLINE_NOTIFICATION_SOFT_CAP', 2)
    cs._winline_enqueue_notification(QUOTE, KEY)
    cs._winline_enqueue_notification(END, KEY, **TERMINAL)
    cs._winline_enqueue_notification(QUOTE, NEXT)
    assert [e['key'] for e in cs._winline_notification_queue] == [KEY, KEY]
    for i in range(8):
        cs._winline_enqueue_notification(END, str(i), **TERMINAL)
    assert len(cs._winline_notification_queue) == 10  # explicitly a soft cap
    cs._winline_notification_queue.clear()
    monkeypatch.setattr(cs, '_WINLINE_NOTIFICATION_SOFT_CAP', 256)
    cs._winline_enqueue_notification(QUOTE, KEY)
    cs._winline_enqueue_notification(dict(QUOTE, market_status='closed'), KEY)
    cs._winline_enqueue_notification(QUOTE, KEY)
    assert len(cs._winline_notification_queue) == 3


def test_side_adoption_during_http_commits_to_current_key(monkeypatch):
    entered, release = threading.Event(), threading.Event()
    def send(*a, **kw):
        entered.set()
        assert release.wait(2)
        return True
    monkeypatch.setattr(cs, 'send_winline_odds_message', send)
    cs._winline_current_map_pollers[KEY] = poller()
    try:
        cs._winline_enqueue_notification(QUOTE, KEY)
        assert entered.wait(1)
        cs._winline_adopt_transposed_poller(SWAP, SERIES, 1, 'Bravo', 'Alpha')
    finally:
        release.set()
    assert cs.stop_winline_notification_worker()
    assert KEY not in cs._winline_odds_notify_state
    assert cs._winline_odds_notify_state[SWAP]['p1'] == QUOTE['p2_odds']
    assert cs._winline_odds_notify_state[SWAP]['p2'] == QUOTE['p1_odds']


def test_alias_round_trip_and_pending_state(monkeypatch):
    frozen(monkeypatch)
    cs._winline_current_map_pollers[KEY] = poller()
    cs._winline_enqueue_notification(QUOTE, KEY)
    cs._winline_pending_map_winners[KEY] = dict(team1='Alpha', team2='Bravo')
    cs._winline_adopt_transposed_poller(SWAP, SERIES, 1, 'Bravo', 'Alpha')
    cs._winline_enqueue_notification(dict(QUOTE, p1_odds=2.1, p2_odds=1.8), SWAP)
    cs._winline_adopt_transposed_poller(KEY, SERIES, 1, 'Alpha', 'Bravo')
    assert cs._winline_resolve_notification_key(KEY) == (KEY, False)
    assert cs._winline_resolve_notification_key(SWAP) == (KEY, True)
    assert cs._winline_pending_map_winners[KEY]['team1'] == 'Alpha'
    drain()
    assert cs._winline_odds_notify_state[KEY]['p1'] == 1.8


def test_maintenance_runs_without_poll_ticks_and_disabled_has_no_worker(monkeypatch):
    seen = [threading.Event() for _ in range(3)]
    for name, event in zip(('_winline_flush_deferred_terminal_notices',
                            '_winline_flush_pending_stop_notices',
                            '_winline_flush_pending_map_winners'), seen):
        monkeypatch.setattr(cs, name, lambda e=event: e.set())
    cs._winline_start_notification_worker()
    assert all(e.wait(1) for e in seen)
    assert cs.stop_winline_notification_worker()
    worker = cs._winline_notification_thread
    monkeypatch.setenv(cs.WINLINE_ODDS_TELEGRAM_ENABLED_ENV, '0')
    cs._winline_enqueue_notification(QUOTE, KEY)
    cs._winline_start_notification_worker()
    assert cs._winline_notification_queue == []
    assert cs._winline_notification_thread is worker and not worker.is_alive()


@pytest.mark.parametrize('missing_proof', ['map_end_proven', 'map_confirmed_live'])
def test_unconfirmed_terminal_does_not_suppress_later_proven_end(monkeypatch, missing_proof):
    frozen(monkeypatch)
    messages = []
    monkeypatch.setattr(cs, 'send_winline_odds_message', lambda m, **kw: messages.append(m) or True)
    unconfirmed = dict(TERMINAL, **{missing_proof: False})
    cs._winline_enqueue_notification(QUOTE, KEY)
    cs._winline_enqueue_notification(END, KEY, **unconfirmed)
    cs._winline_enqueue_notification(END, KEY, **TERMINAL)
    drain()
    assert sum('карта завершена' in m for m in messages) == 1
    assert cs._winline_odds_notify_state[KEY]['kind'] == 'terminal'


def test_due_maintenance_joins_fifo_under_continuous_backlog(monkeypatch):
    entered, release, maintained = threading.Event(), threading.Event(), threading.Event()
    order = []
    original_start = cs._winline_start_notification_worker
    frozen(monkeypatch)
    for key in [KEY, NEXT]:
        cs._winline_enqueue_notification(QUOTE, key)
    def process(entry):
        order.append(entry['key'])
        # A producer always leaves more work: maintenance must not require idle.
        cs._winline_enqueue_notification(QUOTE, 'more-' + entry['key'])
        if entry['key'] == KEY:
            entered.set()
            assert release.wait(2)
    monkeypatch.setattr(cs, '_winline_process_notification', process)
    monkeypatch.setattr(cs, '_winline_flush_deferred_terminal_notices', lambda: None)
    monkeypatch.setattr(cs, '_winline_flush_pending_stop_notices', lambda: None)
    def maintain():
        order.append('maintenance')
        maintained.set()
        cs._winline_notification_stop.set()
    monkeypatch.setattr(cs, '_winline_flush_pending_map_winners', maintain)
    try:
        original_start()
        assert entered.wait(1)
        release.set()
        assert maintained.wait(1), 'maintenance starved behind incoming quotes'
    finally:
        release.set()
    assert cs.stop_winline_notification_worker()
    assert order == [KEY, NEXT, 'maintenance']
    assert cs._winline_notification_queue  # maintenance happened with a backlog


def test_frozen_observation_cannot_replace_first_sendable_quote(monkeypatch):
    frozen(monkeypatch)
    messages = []
    monkeypatch.setattr(cs, 'send_winline_odds_message', lambda m, **kw: messages.append(m) or True)
    cs._winline_enqueue_notification(QUOTE, KEY)
    cs._winline_enqueue_notification(dict(QUOTE, odds_bettable=False), KEY)
    cs._winline_enqueue_notification(END, KEY, **TERMINAL)
    drain()
    assert len(messages) == 2
    assert '🆕' in messages[0] and 'карта завершена' in messages[1]
