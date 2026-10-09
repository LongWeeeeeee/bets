"""Offline regression cases from the 08.10.2026 03:00 MSK explorer capture."""
import importlib.util
import io
import json
import time
import urllib.error
import urllib.parse
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / 'fixtures/od_explorer_20261008'
PERIODS = (1704067200, 1719792000, 1735689600, 1751328000, 1767225600, 1783209600)


def rows(name):
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def fetcher():
    path = REPO / 'scripts/pro_chain/od_explorer_fetch.py'
    spec = importlib.util.spec_from_file_location('od_fetch', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def cache(tmp_path):
    directory = tmp_path / 'cache'
    directory.mkdir()
    # Fully cached periods prevent accidental live calls; only urlopen is mocked.
    for start in PERIODS:
        (directory / f'listing_{start}.json').write_text('[]')
    return directory


def arguments(cache, excluded, refresh=45):
    return ['--cache-dir', str(cache), '--corpus-ids', str(excluded),
            '--refresh-days', str(refresh)]


def install_http(monkeypatch, handler):
    queries = []

    def urlopen(request, timeout):
        assert timeout == 180
        assert request.get_header('User-agent').startswith('Mozilla/5.0')
        query = urllib.parse.parse_qs(urllib.parse.urlparse(request.full_url).query)['sql'][0]
        queries.append(query)
        payload, remaining = handler(query)
        response = io.BytesIO(json.dumps(payload).encode())
        response.headers = {'x-rate-limit-remaining-day': str(remaining)}
        return response

    monkeypatch.setattr('urllib.request.urlopen', urlopen)
    return queries


def player_ids(query):
    return [int(mid) for mid in query.split('IN (', 1)[1].split(')', 1)[0].split(',')]


def test_incremental_refresh_and_exact_missing_set(fetcher, cache, tmp_path, monkeypatch):
    old = rows('listing_1704067200.json')
    recent = rows('listing_1783209600.json')[:3]
    players = rows('players_7515635423.json')
    cached_ids = {r['match_id'] for r in players}
    (cache / 'listing_1704067200.json').write_text(json.dumps(old))
    (cache / 'players_7515635423.json').write_text(json.dumps(players))
    excluded = tmp_path / 'ids.txt'
    excluded.write_text(json.dumps([old[-1]['match_id'], recent[0]['match_id']]))

    def handler(query):
        if 'FROM matches m' in query:
            assert f'm.start_time >= {PERIODS[-1]}' in query
            return {'rows': recent}, 2500
        return {'rows': []}, 2499

    queries = install_http(monkeypatch, handler)
    assert fetcher.main(arguments(cache, excluded)) == 0
    assert len([q for q in queries if 'FROM matches m' in q]) == 1
    expected = {r['match_id'] for r in old + recent} - cached_ids - set(json.loads(excluded.read_text()))
    assert set(player_ids(queries[-1])) == expected
    assert json.loads((cache / f'listing_{PERIODS[0]}.json').read_text()) == old
    assert json.loads((cache / f'listing_{PERIODS[-1]}.json').read_text()) == recent


@pytest.mark.parametrize('failure', ['429', '522', 'err', 'quota'])
def test_partial_night_keeps_published_files(fetcher, cache, tmp_path, monkeypatch, failure):
    excluded = tmp_path / 'ids.txt'
    excluded.write_text('[]')
    cached = rows('players_7515635423.json')
    (cache / 'players_7515635423.json').write_text(json.dumps(cached))
    # A new old-period cache is published before the recent listing fails.
    uncached = cache / f'listing_{PERIODS[0]}.json'
    uncached.rename(cache / 'old_listing_evidence.json')
    old = rows('listing_1704067200.json')

    def handler(query):
        if f'm.start_time >= {PERIODS[0]}' in query:
            return {'rows': old}, 2500
        if failure in ('429', '522'):
            raise urllib.error.HTTPError('offline', int(failure), 'captured outage', {}, None)
        if failure == 'err':
            return {'err': 'explorer unavailable'}, 2499
        return {'rows': rows('listing_1783209600.json')}, 0

    queries = install_http(monkeypatch, handler)
    assert fetcher.main(arguments(cache, excluded)) == 3
    assert len(queries) <= 4  # no retry storm
    assert json.loads(uncached.read_text()) == old
    assert json.loads((cache / 'players_7515635423.json').read_text()) == cached
    recent = json.loads((cache / f'listing_{PERIODS[-1]}.json').read_text())
    assert recent == (rows('listing_1783209600.json') if failure == 'quota' else [])
    assert all('FROM player_matches' not in q for q in queries)


def test_resume_only_missing_maps_and_chunk_limit(fetcher, cache, tmp_path, monkeypatch):
    listing = []
    for start in PERIODS[:3]:
        page = rows(f'listing_{start}.json')
        listing.extend(page)
        (cache / f'listing_{start}.json').write_text(json.dumps(page))
    captured = rows('players_7515635423.json')
    excluded = tmp_path / 'ids.txt'
    # Exercise the shared newline parser, rather than a second JSON-only parser.
    excluded.write_text(str(listing[-1]['match_id']) + '\n')
    first_response = True

    def partial(query):
        nonlocal first_response
        if not first_response:
            raise urllib.error.HTTPError('offline', 429, 'quota', {}, None)
        first_response = False
        return {'rows': captured}, 2500

    queries = install_http(monkeypatch, partial)
    assert fetcher.main(arguments(cache, excluded, refresh=0)) == 3
    assert [len(player_ids(q)) for q in queries] == [400, 49]
    published = list(cache.glob('players_*.json'))
    assert len(published) == 1
    persisted = published[0]
    assert json.loads(persisted.read_text()) == captured
    resumed = install_http(monkeypatch, lambda q: ({'rows': []}, 2400))
    assert fetcher.main(arguments(cache, excluded, refresh=0)) == 0
    # Successfully requested maps with no rows are cached separately too;
    # the failed second request must remain eligible on the next night.
    needed = {r['match_id'] for r in listing} - set(player_ids(queries[0])) - {listing[-1]['match_id']}
    assert set(mid for q in resumed for mid in player_ids(q)) == needed
    assert all(len(player_ids(q)) <= 400 for q in resumed)
    # A changing first ID must not overwrite yesterday's differently-sized chunk.
    assert json.loads(persisted.read_text()) == captured


def test_usage_rejects_negative_refresh_days(fetcher, cache, tmp_path):
    with pytest.raises(SystemExit) as exc:
        fetcher.main(arguments(cache, tmp_path / 'ids.txt', refresh=-1))
    assert exc.value.code == 2


def test_zero_row_maps_do_not_overwrite_chunks_across_nights(fetcher, cache, tmp_path, monkeypatch):
    captured_listing = rows('listing_1704067200.json')
    captured_players = rows('players_7515635423.json')
    available = {r['match_id'] for r in captured_players}
    # Simulate unparsed maps using captured listing rows and exact captured players
    # for the other maps. Without empty-ID caching the endpoints/count collide.
    empty_rows = [captured_listing[4], captured_listing[-1]]
    empty_ids = {r['match_id'] for r in empty_rows}
    assert empty_ids.isdisjoint(available)
    middle = [r for r in captured_listing
              if r['match_id'] in available
              and empty_rows[0]['match_id'] < r['match_id'] < empty_rows[1]['match_id']][:6]
    assert len(middle) == 6
    legacy = [r for r in captured_players if r['match_id'] == captured_listing[0]['match_id']]
    (cache / 'players_7515635423.json').write_text(json.dumps(legacy))
    excluded = tmp_path / 'ids.txt'
    excluded.write_text('[]')
    listing_path = cache / f'listing_{PERIODS[0]}.json'

    def handler(query):
        if 'FROM matches m' in query:
            start = int(query.split('m.start_time >= ', 1)[1].split()[0])
            return {'rows': json.loads((cache / f'listing_{start}.json').read_text())}, 2500
        requested = set(player_ids(query))
        assert len(requested) <= 400
        return {'rows': [r for r in captured_players if r['match_id'] in requested]}, 2500

    queries = install_http(monkeypatch, handler)
    snapshots = []
    for night in range(1, 4):
        listing_path.write_text(json.dumps(empty_rows + middle[:2 * night]))
        assert fetcher.main(arguments(cache, excluded, refresh=0)) == 0
        snapshots.append({p.name: {r['match_id'] for r in json.loads(p.read_text())}
                          for p in cache.glob('players_*.json')})
    for previous, current in zip(snapshots, snapshots[1:]):
        assert set().union(*previous.values()) <= set().union(*current.values())
        for name, ids in previous.items():
            assert current[name] == ids, 'a players file changed its map IDs'
    player_queries = [player_ids(q) for q in queries if 'FROM player_matches' in q]
    assert all(sum(mid in chunk for chunk in player_queries) == 1 for mid in empty_ids)
    recorded_empty = {mid for p in cache.glob('noplayers_*.json')
                      for mid in json.loads(p.read_text())}
    assert recorded_empty == empty_ids
    assert fetcher.main(arguments(cache, excluded, refresh=0)) == 0
    assert len(queries) == len(player_queries) == 3

    # Use the captured timestamps without mocking the clock: widen the window
    # to include them, then return to reuse-only mode after the second attempt.
    refresh_days = int((time.time() - min(r['start_time'] for r in empty_rows)) / 86400) + 2
    before_refresh = len(queries)
    assert fetcher.main(arguments(cache, excluded, refresh=refresh_days)) == 0
    retried = [player_ids(q) for q in queries[before_refresh:] if 'FROM player_matches' in q]
    assert len(retried) == 1 and set(retried[0]) == empty_ids
    after_refresh = len(queries)
    assert fetcher.main(arguments(cache, excluded, refresh=0)) == 0
    assert len(queries) == after_refresh


def test_atomic_write_reuses_stale_sibling_tmp(fetcher, tmp_path):
    target = tmp_path / 'noplayers_test.json'
    temporary = tmp_path / 'noplayers_test.json.tmp'
    temporary.write_text('interrupted incomplete JSON')
    fetcher._write_atomic(target, [7515769503])
    assert json.loads(target.read_text()) == [7515769503]
    assert not list(tmp_path.glob('*.tmp'))


def test_chunk_identity_includes_middle_ids_after_interrupted_publication(
        fetcher, cache, tmp_path, monkeypatch):
    listing = rows('listing_1704067200.json')
    captured = rows('players_7515635423.json')
    available = {r['match_id'] for r in captured}
    endpoints = [listing[4], listing[-1]]
    middle = [r for r in listing if r['match_id'] in available
              and endpoints[0]['match_id'] < r['match_id'] < endpoints[1]['match_id']][:2]
    assert len(middle) == 2
    excluded = tmp_path / 'ids.txt'
    excluded.write_text('[]')
    queries = install_http(monkeypatch, lambda q: (
        {'rows': [r for r in captured if r['match_id'] in player_ids(q)]}, 2500))
    snapshots = []
    for row in middle:
        (cache / f'listing_{PERIODS[0]}.json').write_text(json.dumps(endpoints + [row]))
        assert fetcher.main(arguments(cache, excluded, refresh=0)) == 0
        snapshots.append({p.name: p.read_bytes() for p in cache.glob('players_*.json')})
        # Model timeout after players publication and before the negative cache
        # is published, retaining the marker as evidence rather than deleting it.
        for path in cache.glob('noplayers_*.json'):
            path.rename(cache / ('interrupted_' + path.name))
    first, second = [player_ids(q) for q in queries]
    assert (first[0], first[-1], len(first)) == (second[0], second[-1], len(second))
    assert first != second
    assert len(snapshots[1]) == 2
    assert all(snapshots[1][name] == content for name, content in snapshots[0].items())
