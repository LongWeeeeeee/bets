"""Real-body transport tests and extraction-boundary regression coverage.

BACKFILL_DISABLE_WRITES=1 proves the suite detects a reverted writer.
BACKFILL_SKIP_STEP=1 proves the original extraction failure before backfill.
Captured GraphQL/OpenDota bodies are never synthesized by the main regression.
"""
import ast
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import sys

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / 'fixtures/backfill_by_id'
sys.path.insert(0, str(Path(sys.prefix).parent / 'base'))  # ignored keys.py in the approved venv's checkout
sys.path.insert(0, str(ROOT / 'base'))
sys.path.insert(0, str(ROOT / 'scripts/pro_chain'))
import maps_research as M
import backfill_by_id as B
import topup_pro_corpus as T
import pro_corpus_extract as E

C, NEW, NULL = 8991024043, 9010162791, 9021397264
SINCE = int(datetime(2026, 8, 25, tzinfo=timezone.utc).timestamp())
NOW = int(datetime(2026, 10, 5, 7, tzinfo=timezone.utc).timestamp())


def fixture(name):
    path = FIXTURES / name
    assert path.exists(), 'Required captured response missing: ' + str(path)
    return json.loads(path.read_text())


@pytest.fixture(autouse=True)
def reverted_writer(monkeypatch):
    if os.getenv('BACKFILL_DISABLE_WRITES') == '1':
        monkeypatch.setattr(B, 'write_records', lambda *a, **kw: None)


def parts(corpus):
    return sorted(list(corpus.glob('*_part*.json')) + list(corpus.glob('*_part*.json.gz')))


def snapshot(corpus):
    return {p.relative_to(corpus).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in corpus.rglob('*') if p.is_file()}


def tiny_corpus(tmp_path, compressed=True):
    corpus = tmp_path / 'corpus'
    corpus.mkdir()
    c, normal = fixture('stored_c.json'), fixture('stored_normal.json')
    suffix = '.json.gz' if compressed else '.json'
    M._atomic_write_bytes(corpus / ('7.41e_part039' + suffix),
                          gzip.compress(M.orjson.dumps({str(C): c})) if compressed else
                          M.orjson.dumps({str(C): c}))
    M._atomic_write_bytes(corpus / '7.41e_part040.json', M.orjson.dumps({str(normal['id']): normal}))
    # Model the historical processed-ID cache outside these two recent parts.
    # Retain every captured OpenDota ID except the three measured gap examples.
    processed = {m['match_id'] for p in sorted(FIXTURES.glob('opendota_page_*.json'))
                 for m in json.loads(p.read_text())} - {C, NEW, NULL}
    processed.update((C, normal['id']))
    M._atomic_write_bytes(corpus / 'processed_ids.txt', M.orjson.dumps(sorted(processed)))
    return corpus, normal


class Response:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status_code = status
        self.headers = {}
        self.text = json.dumps(payload)

    def json(self):
        return self.payload


@pytest.fixture
def transport(monkeypatch):
    pages = [p.read_bytes() for p in sorted(FIXTURES.glob('opendota_page_*.json'))]
    calls = {'od': [], 'stratz': [], 'mutations': []}

    def get(req, timeout):
        assert req.get_header('User-agent')
        index = len(calls['od'])
        if index:
            previous = json.loads(pages[index - 1])
            assert req.full_url.endswith('less_than_match_id=%d' % min(m['match_id'] for m in previous))
        calls['od'].append(req.full_url)
        assert index < len(pages), 'OpenDota pagination failed to stop at window'
        return io.BytesIO(pages[index])

    def post(url, **kwargs):
        assert url == 'https://api.stratz.com/graphql'
        assert kwargs['proxies']['https']
        assert kwargs['headers']['Authorization'].startswith('Bearer ')
        # Lead probe 05.10.2026: a 20-alias query URL-encoded into Referer
        # (18 335 bytes) gets Cloudflare 520; the same query with a short
        # Referer returns 200 (scratchpad stratz_referer_probe.py).
        assert len(kwargs['headers'].get('Referer', '')) <= 1024
        query = kwargs['json']['query']
        calls['stratz'].append(query)
        if query.startswith('mutation'):
            calls['mutations'].append(query)
            # Mutation receipt is intentionally a transport control: ingestion
            # is neither assumed nor asserted. Real null match stays absent.
            return Response({'data': {'retryMatchDownload': True}})
        return Response(fixture('stratz_response.json'))

    monkeypatch.setattr(B, 'urlopen', get)
    monkeypatch.setattr(M.cf_requests, 'post', post)
    monkeypatch.setattr(M, 'proxy_pool', None)
    monkeypatch.setattr(M, 'STRATZ_PROXY_MAP', {'http://fixture.invalid:80': 'offline-test-token'})
    # No network sleep in fixture pagination; pool pacing is otherwise unchanged.
    monkeypatch.setattr(B.time, 'sleep', lambda seconds: None)
    return calls


def extract(corpus, tmp_path, monkeypatch):
    output = tmp_path / 'compact.npz'
    monkeypatch.setattr(E, 'CORPUS', corpus)
    monkeypatch.setattr(E, 'OUT', output)
    E.main()
    with np.load(output) as data:
        return set(data['mids'].tolist())


@pytest.mark.parametrize('compressed', [False, True])
def test_extraction_delivery_boundary(tmp_path, monkeypatch, transport, compressed):
    corpus, normal = tiny_corpus(tmp_path, compressed)
    # This assertion is the old production failure: C exists on disk but is
    # excluded by the actual extractor; B has never been stored.
    assert extract(corpus, tmp_path, monkeypatch) == {normal['id']}
    if os.getenv('BACKFILL_SKIP_STEP') != '1':
        result = B.backfill_by_id(SINCE, corpus, M=M, now=NOW)
        assert result == dict(candidates=3, fetched=2, new_written=1, replaced=1,
                              still_unparsed=0, stratz_null=1, retry_requested=1,
                              errors=0, league_skipped=0)
    mids = extract(corpus, tmp_path, monkeypatch)
    assert {C, NEW} <= mids
    assert NULL not in mids
    counts = Counter(int(m.get('id') or key) for path in parts(corpus)
                     for key, m in B.read_json(path).items())
    assert counts[C] == counts[NEW] == 1
    assert all(n == 1 for n in counts.values())
    assert {C, NEW} <= set(B.read_json(corpus / 'processed_ids.txt'))
    original = corpus / ('7.41e_part039.json.gz' if compressed else '7.41e_part039.json')
    assert B.complete(B.read_json(original)[str(C)])
    if compressed:
        assert original.read_bytes().startswith(b'\x1f\x8b')
    manifest = B.read_json(corpus / 'scan_manifest.json')
    for path in parts(corpus):
        assert manifest[path.name][:2] == M._file_stamp(path)
    assert transport['mutations'] == ['mutation { retryMatchDownload(matchId: %d) }' % NULL]


def test_dry_run_changes_no_files(tmp_path, transport):
    corpus, _ = tiny_corpus(tmp_path)
    before = snapshot(corpus)
    result = B.backfill_by_id(SINCE, corpus, dry_run=True, M=M, now=NOW)
    assert result['errors'] == 0 and result['fetched'] == 2 and result['stratz_null'] == 1
    assert result['new_written'] == result['replaced'] == result['retry_requested'] == 0
    assert snapshot(corpus) == before
    assert not transport['mutations']


def selection_tree(text):
    """Parse field nesting, preserving aliases/order to catch contract drift."""
    tokens = re.findall(r'[A-Za-z_][A-Za-z_0-9]*|[{}]', text)
    def parse(index):
        result = []
        while index < len(tokens) and tokens[index] != '}':
            name = tokens[index]
            index += 1
            child = []
            if index < len(tokens) and tokens[index] == '{':
                child, index = parse(index + 1)
            result.append((name, child))
        return result, index + 1
    return parse(0)[0]


def test_selection_matches_get_pros_source():
    source = ast.parse((ROOT / 'base/maps_research.py').read_text())
    function = next(n for n in ast.walk(source) if isinstance(n, ast.AsyncFunctionDef)
                    and n.name == 'proceed_get_maps_with_data')
    query = next(n.value for n in ast.walk(function) if isinstance(n, ast.Assign)
                 and any(isinstance(t, ast.Name) and t.id == 'query' for t in n.targets)
                 and isinstance(n.value, ast.JoinedStr))
    tail = next(v.value for v in query.values if isinstance(v, ast.Constant) and 'isStats:true' in v.value)
    tail = tail[tail.index('isStats:true') + len('isStats:true'):]
    tail = tail[tail.index('{') + 1:]
    assert selection_tree(B.MATCH_FIELDS) == selection_tree(tail)
    # get_pros still routes through this pro query.
    get_pros = next(n for n in ast.walk(source) if isinstance(n, ast.FunctionDef) and n.name == 'get_pros')
    assert any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == 'get_maps_new'
               and any(k.arg == 'pro' and isinstance(k.value, ast.Constant) and k.value.value is True
                       for k in n.keywords) for n in ast.walk(get_pros))


def test_provenance_hashes():
    provenance = fixture('provenance.json')
    assert provenance['capture_command']
    assert provenance['capture_date_utc'].startswith('2026-10-05')
    for name, expected in provenance['files'].items():
        assert hashlib.sha256((FIXTURES / name).read_bytes()).hexdigest() == expected
    data = fixture('stratz_response.json')['data']
    assert B.complete(data['m%d' % C]) and B.complete(data['m%d' % NEW])
    assert data['m%d' % NULL] is None


@pytest.mark.parametrize('compressed', [False, True])
def test_writer_rotation_and_stale_cache(tmp_path, monkeypatch, compressed):
    corpus, normal = tiny_corpus(tmp_path, compressed)
    # Representative real normal record, with a controlled incomplete stored copy.
    path = corpus / '7.41e_part040.json'
    incomplete = json.loads(json.dumps(normal))
    incomplete['players'][0]['position'] = None
    M._atomic_write_bytes(path, M.orjson.dumps({str(normal['id']): incomplete}))
    processed, manifest, locations, unfinished = B.scan_recent(corpus, SINCE, M)
    assert normal['id'] in unfinished
    stats = dict(new_written=0, replaced=0, errors=0)
    B.write_records(M, corpus, {normal['id']: normal}, locations, processed, manifest, stats)
    assert stats['replaced'] == 1
    assert B.complete(B.read_json(path)[str(normal['id'])])
    assert extract(corpus, tmp_path, monkeypatch) == {normal['id']}
    # New IDs use counters even if the numbered part is absent (serv1 contract).
    M._save_part_counters(corpus, {'7.41e': 91})
    processed.discard(normal['id'])
    # Different fresh corpus, not an existing ID masquerading as new.
    fresh = tmp_path / 'new'
    fresh.mkdir()
    M._save_part_counters(fresh, {'7.41e': 91})
    monkeypatch.setenv('PRO_CORPUS_GZIP', '1' if compressed else '0')
    B.write_records(M, fresh, {normal['id']: normal}, {}, set(), {}, stats)
    expected = fresh / ('7.41e_part092.json.gz' if compressed else '7.41e_part092.json')
    assert expected.exists()
    assert B.read_json(expected)[str(normal['id'])] == normal
    assert B.read_json(fresh / 'part_counters.json')['7.41e'] == 92
    assert normal['id'] in B.read_json(fresh / 'processed_ids.txt')


def test_old_parts_are_not_loaded(tmp_path, monkeypatch):
    corpus, _ = tiny_corpus(tmp_path)
    old = corpus / '7.41e_part001.json'
    old.write_text('this old part must not be read')
    os.utime(old, (SINCE - 1, SINCE - 1))
    _, _, locations, unfinished = B.scan_recent(corpus, SINCE, M)
    assert C in unfinished and C in locations


@pytest.mark.parametrize('status', [429, 522])
def test_http_stop_escapes_pool_retry(monkeypatch, status):
    calls = []
    def post(*args, **kwargs):
        calls.append(1)
        return Response({'error': 'gateway'}, status)
    monkeypatch.setattr(M.cf_requests, 'post', post)
    monkeypatch.setattr(M, 'proxy_pool', None)
    monkeypatch.setattr(M, 'STRATZ_PROXY_MAP', {'http://fixture.invalid:80': 'offline-test-token'})
    original = M.cf_requests.post
    with pytest.raises(B.TransportStop):
        with B.bounded_stratz(M, 10):
            B.asyncio.run(B.stratz(M, 'query { match(id: 1) { id } }'))
    assert len(calls) == 1
    assert M.cf_requests.post is original


def test_topup_backfill_exception_restores_visited(tmp_path, monkeypatch, capsys):
    # Exercise main's delivery/restore boundary using the actual module import.
    # These local get_pros/visited controls are necessary to prohibit real writes.
    pro = tmp_path / 'pro'
    corpus = pro / 'json_parts_split_from_object'
    corpus.mkdir(parents=True)
    visited_path = pro / M.PRO_VISITED_TEAMS_FILE
    visited_path.write_text('[11, 12]')
    monkeypatch.setattr(M, 'PRO_HEROES_DIR', pro)
    monkeypatch.setattr(M, '_seed_team_ids', lambda: [11])
    monkeypatch.setattr(M, 'get_pros', lambda **kwargs: None)
    monkeypatch.setattr(T, 'LOCK', tmp_path / 'topup.lock')
    monkeypatch.setattr(T, '_newest_map_ts', lambda path: int(B.time.time()))
    monkeypatch.setattr(B, 'backfill_by_id', lambda **kwargs: (_ for _ in ()).throw(RuntimeError('fixture failure')))
    assert T.main() == 0
    assert M._load_visited_teams(str(visited_path)) == {11, 12}
    assert 'ВНИМАНИЕ: id-backfill' in capsys.readouterr().out
    assert not T.LOCK.exists()


def test_write_keeps_original_on_atomic_publish_failure(tmp_path, monkeypatch):
    corpus, normal = tiny_corpus(tmp_path)
    path = corpus / '7.41e_part040.json'
    bad = json.loads(json.dumps(normal))
    bad['players'][0]['position'] = None
    M._atomic_write_bytes(path, M.orjson.dumps({str(normal['id']): bad}))
    before = path.read_bytes()
    processed, manifest, locations, _ = B.scan_recent(corpus, SINCE, M)
    def fail_rename(src, dst):
        raise OSError('publication denied')
    monkeypatch.setattr(M.os, 'replace', fail_rename)
    with pytest.raises(OSError, match='publication denied'):
        B.write_records(M, corpus, {normal['id']: normal}, locations, processed, manifest,
                        dict(new_written=0, replaced=0, errors=0))
    assert path.read_bytes() == before
    # Rebuild-then-replace preserves the built temp instead of deleting evidence.
    assert path.with_name(path.name + '.tmp').exists()


def test_duplicate_recent_match_aborts_before_any_write(tmp_path):
    corpus, _ = tiny_corpus(tmp_path)
    M._atomic_write_bytes(corpus / '7.41e_part041.json', M.orjson.dumps({str(C): fixture('stored_c.json')}))
    before = snapshot(corpus)
    with pytest.raises(ValueError, match='duplicate stored match'):
        B.scan_recent(corpus, SINCE, M)
    assert snapshot(corpus) == before


def test_stratz_physical_cap_and_no_direct_transport(monkeypatch):
    calls = []
    def post(*args, **kwargs):
        calls.append(1)
        return Response({'data': {}})
    monkeypatch.setattr(M.cf_requests, 'post', post)
    with B.bounded_stratz(M, 1):
        with pytest.raises(B.TransportStop, match='proxy missing'):
            M.cf_requests.post('https://api.stratz.com/graphql', proxies=None)
        M.cf_requests.post('https://api.stratz.com/graphql', proxies={'https': 'http://fixture.invalid:80'})
        with pytest.raises(B.TransportStop, match='cap reached'):
            M.cf_requests.post('https://api.stratz.com/graphql', proxies={'https': 'http://fixture.invalid:80'})
    assert len(calls) == 1


def test_retry_receipt_throttles_three_days(tmp_path, transport):
    corpus, _ = tiny_corpus(tmp_path)
    result = B.backfill_by_id(SINCE, corpus, M=M, now=NOW)
    assert result['retry_requested'] == 1
    assert len(transport['mutations']) == 1
    for age, expected in ((86400, 0), (3 * 86400, 1)):
        transport['od'].clear()
        result = B.backfill_by_id(SINCE, corpus, M=M, now=NOW + age)
        assert result['retry_requested'] == expected and result['errors'] == 0
    assert len(transport['mutations']) == 2
    date = B.read_json(corpus / B.RETRY_FILE)[str(NULL)]
    assert datetime.fromisoformat(date).timestamp() == NOW + 3 * 86400


def test_still_unparsed_never_replaces_or_adds(tmp_path, monkeypatch):
    corpus, _ = tiny_corpus(tmp_path)
    processed, manifest, locations, _ = B.scan_recent(corpus, SINCE, M)
    before = snapshot(corpus)
    candidates = [C]
    calls = []
    def post(url, **kwargs):
        calls.append(kwargs['json']['query'])
        # Original captured corpus record is the real unparsed input.
        return Response({'data': {'m%d' % C: fixture('stored_c.json')}})
    monkeypatch.setattr(M.cf_requests, 'post', post)
    monkeypatch.setattr(M, 'proxy_pool', None)
    monkeypatch.setattr(M, 'STRATZ_PROXY_MAP', {'http://fixture.invalid:80': 'offline-test-token'})
    for existing in (True, False):
        stats = dict(fetched=0, new_written=0, replaced=0, stratz_null=0,
                     retry_requested=0, still_unparsed=0, errors=0)
        with B.bounded_stratz(M, 1):
            B.asyncio.run(B.fetch_and_write(M, corpus, SINCE, candidates,
                                           locations if existing else {},
                                           processed if existing else set(), manifest,
                                           False, NOW, stats))
        assert stats['still_unparsed'] == 1
        assert stats['new_written'] == stats['replaced'] == 0
        assert snapshot(corpus) == before
        # Initialize inside the next event loop (Python 3.9 locks bind at creation).
        monkeypatch.setattr(M, 'proxy_pool', None)


def test_written_records_match_stored_schema(tmp_path, transport):
    corpus, _ = tiny_corpus(tmp_path)
    result = B.backfill_by_id(SINCE, corpus, M=M, now=NOW)
    assert result['new_written'] == result['replaced'] == 1
    written = {int(key): record for path in parts(corpus)
               for key, record in B.read_json(path).items()}
    for mid, name in ((NEW, 'stored_normal.json'), (C, 'stored_c.json')):
        assert set(written[mid]) == set(fixture(name))
        assert written[mid]['league'] == {'id': written[mid]['leagueId'], 'tier': 'UNKNOWN'}


def test_replacement_preserves_existing_league(tmp_path, transport):
    corpus, _ = tiny_corpus(tmp_path, compressed=False)
    path = corpus / '7.41e_part039.json'
    stored = fixture('stored_c.json')
    stored['league'].update(tier='PROFESSIONAL', name='retained metadata')
    M._atomic_write_bytes(path, M.orjson.dumps({str(C): stored}))
    result = B.backfill_by_id(SINCE, corpus, M=M, now=NOW)
    assert result['replaced'] == 1
    assert B.read_json(path)[str(C)]['league'] == stored['league']


def offline_batches(monkeypatch, payloads):
    calls = []

    def post(url, **kwargs):
        assert kwargs['proxies']['https']
        calls.append(kwargs['json']['query'])
        assert len(calls) <= len(payloads), 'unexpected transport retry or batch'
        return Response(payloads[len(calls) - 1])

    monkeypatch.setattr(M.cf_requests, 'post', post)
    monkeypatch.setattr(M, 'proxy_pool', None)
    monkeypatch.setattr(M, 'STRATZ_PROXY_MAP', {'http://fixture.invalid:80': 'offline-test-token'})
    return calls


def failed_batch_ids(count):
    ids = sorted({m['match_id'] for p in sorted(FIXTURES.glob('opendota_page_*.json'))
                  for m in json.loads(p.read_text())} - {C, NEW, NULL})
    return ids[:count * B.BATCH_SIZE]


def run_batches(corpus, candidates, dry_run=False):
    processed, manifest, locations, _ = B.scan_recent(corpus, SINCE, M, NOW)
    stats = dict(fetched=0, new_written=0, replaced=0, stratz_null=0,
                 retry_requested=0, still_unparsed=0, errors=0, league_skipped=0)
    with B.bounded_stratz(M, 10):
        B.asyncio.run(B.fetch_and_write(M, corpus, SINCE, candidates, locations,
                                       processed, manifest, dry_run, NOW, stats))
    return stats


@pytest.mark.parametrize('failure', ['520', 'graphql', 'non_dict', 'non_dict_data',
                                   'missing_alias', 'wrong_id'])
def test_failed_batch_continues_without_partial_writes(tmp_path, monkeypatch, capsys, failure):
    corpus, _ = tiny_corpus(tmp_path)
    ids = failed_batch_ids(1)
    # A valid first alias followed by missing aliases must still publish nothing.
    record = fixture('stratz_response.json')['data']['m%d' % NEW]
    record['id'] = ids[0]
    failures = {
        '520': fixture('stratz_520_reconstructed.json'),
        'graphql': {'data': {}, 'errors': [{'message': 'fixture GraphQL failure'}]},
        'non_dict': [],
        'non_dict_data': {'data': []},
        'missing_alias': {'data': {'m%d' % ids[0]: record}},
        # A valid first alias must not be published before the wrong last ID.
        'wrong_id': {'data': {'m%d' % mid: dict(record, id=mid if mid != ids[-1] else NEW)
                              for mid in ids}},
    }
    calls = offline_batches(monkeypatch, [failures[failure], fixture('stratz_response.json')])
    stats = run_batches(corpus, ids + [C, NEW])
    assert len(calls) == 2
    assert stats['errors'] == 1
    assert stats['fetched'] == 2 and stats['stratz_null'] == 0
    assert stats['new_written'] == stats['replaced'] == 1
    written = {int(key) for path in parts(corpus) for key in B.read_json(path)}
    assert {C, NEW} <= written and not (set(ids) & written)
    assert capsys.readouterr().out.count('ВНИМАНИЕ:') == 1


@pytest.mark.parametrize('failure', ['graphql', 'missing_boolean'])
def test_retry_failure_keeps_receipt_and_continues(tmp_path, monkeypatch, capsys, failure):
    corpus, _ = tiny_corpus(tmp_path)
    monkeypatch.setattr(B, 'BATCH_SIZE', 2)
    other_null = failed_batch_ids(1)[0]
    null_batch = {'data': {'m%d' % NULL: None, 'm%d' % other_null: None}}
    failures = {
        'graphql': {'errors': [{'message': 'fixture mutation failure'}]},
        'missing_boolean': {'data': {'retryMatchDownload': None}},
    }
    calls = offline_batches(monkeypatch, [null_batch, failures[failure],
                                         {'data': {'retryMatchDownload': True}},
                                         fixture('stratz_response.json')])
    original = B.stratz

    async def check_receipt(module, query):
        if query.startswith('mutation'):
            mid = re.search(r'matchId: (\d+)', query).group(1)
            date = B.read_json(corpus / B.RETRY_FILE)[mid]
            assert datetime.fromisoformat(date).timestamp() == NOW
        return await original(module, query)

    monkeypatch.setattr(B, 'stratz', check_receipt)
    stats = run_batches(corpus, [NULL, other_null, C, NEW])
    assert len(calls) == 4
    assert stats['errors'] == stats['retry_requested'] == 1
    assert stats['stratz_null'] == stats['fetched'] == 2
    assert stats['new_written'] == stats['replaced'] == 1
    receipt = corpus / B.RETRY_FILE
    assert set(B.read_json(receipt)) == {str(NULL), str(other_null)}
    output = capsys.readouterr().out
    assert output.count('ВНИМАНИЕ:') == 1
    assert 'ВНИМАНИЕ: backfill-by-id retryMatchDownload %d failed:' % NULL in output
    # A later run within the throttle window never repeats the failed mutation.
    before = receipt.read_bytes()
    calls = offline_batches(monkeypatch, [null_batch, fixture('stratz_response.json')])
    stats = run_batches(corpus, [NULL, other_null, C, NEW])
    assert len(calls) == 2 and not any(q.startswith('mutation') for q in calls)
    assert stats['retry_requested'] == stats['errors'] == 0
    assert receipt.read_bytes() == before


@pytest.mark.parametrize('league_case', ['missing', 'amateur'])
def test_league_gate_for_replacements_and_new_records(tmp_path, monkeypatch, league_case):
    corpus, _ = tiny_corpus(tmp_path, compressed=False)
    before = snapshot(corpus)
    monkeypatch.setattr(M, 'PRO_REQUIRE_LEAGUE', True)
    monkeypatch.setenv('PRO_CORPUS_GZIP', '0')
    response = fixture('stratz_response.json')
    for mid in (C, NEW):
        record = response['data']['m%d' % mid]
        if league_case == 'missing':
            record['leagueId'] = None
            record['league'] = {}
        else:
            record['league'] = {'id': record['leagueId'], 'tier': 'AMATEUR'}
    for dry_run in (False, True):
        offline_batches(monkeypatch, [response])
        stats = run_batches(corpus, [C, NEW], dry_run=dry_run)
        assert stats['fetched'] == 2 and stats['errors'] == 0
        assert stats['league_skipped'] == 2
        assert stats['new_written'] == stats['replaced'] == 0
        assert snapshot(corpus) == before
    # With the flag off, keep the original enrichment/preservation bytes.
    monkeypatch.setattr(M, 'PRO_REQUIRE_LEAGUE', False)
    offline_batches(monkeypatch, [response])
    stats = run_batches(corpus, [C, NEW])
    assert stats['league_skipped'] == stats['errors'] == 0
    assert stats['new_written'] == stats['replaced'] == 1
    expected_league = ({'tier': 'UNKNOWN'} if league_case == 'missing' else
                       {'id': response['data']['m%d' % NEW]['leagueId'], 'tier': 'AMATEUR'})
    expected = {
        C: dict(response['data']['m%d' % C], league=fixture('stored_c.json')['league']),
        NEW: dict(response['data']['m%d' % NEW], league=expected_league),
    }
    for path in parts(corpus):
        data = B.read_json(path)
        for mid in (C, NEW):
            if str(mid) in data:
                assert path.read_bytes() == M.orjson.dumps({str(mid): expected[mid]})
    # The enabled gate also admits a top-level ID with UNKNOWN tier, and a
    # nested league ID with a professional tier (get_pros' fallback rule).
    monkeypatch.setattr(M, 'PRO_REQUIRE_LEAGUE', True)
    response = fixture('stratz_response.json')
    if league_case == 'amateur':
        for mid in (C, NEW):
            record = response['data']['m%d' % mid]
            record['league'] = {'id': record.pop('leagueId'), 'tier': 'PROFESSIONAL'}
    offline_batches(monkeypatch, [response])
    stats = run_batches(corpus, [C, NEW])
    assert stats['league_skipped'] == stats['errors'] == 0
    assert stats['new_written'] == 0 and stats['replaced'] == 2


def test_three_consecutive_failed_batches_abort(tmp_path, monkeypatch, capsys):
    corpus, _ = tiny_corpus(tmp_path)
    before = snapshot(corpus)
    calls = offline_batches(monkeypatch, [fixture('stratz_520_reconstructed.json')] * 3
                            + [fixture('stratz_response.json')])
    stats = run_batches(corpus, failed_batch_ids(3) + [C, NEW])
    assert len(calls) == 3
    assert stats['errors'] == 3 and stats['new_written'] == stats['replaced'] == 0
    assert snapshot(corpus) == before
    assert capsys.readouterr().out.count('ВНИМАНИЕ:') == 3


@pytest.mark.parametrize('lock_state', ['own', 'replaced', 'missing'])
def test_cli_releases_only_own_lock(tmp_path, monkeypatch, lock_state):
    lock = tmp_path / 'topup.lock'
    monkeypatch.setattr(T, 'LOCK', lock)
    monkeypatch.setattr(sys, 'argv', ['backfill_by_id.py', '--corpus-dir', str(tmp_path)])

    def backfill(**kwargs):
        assert lock.read_text() == str(os.getpid())
        if lock_state == 'replaced':
            lock.write_text('987654321')
        elif lock_state == 'missing':
            lock.unlink()
        return {'errors': 0}

    monkeypatch.setattr(B, 'backfill_by_id', backfill)
    assert B.main() == 0
    if lock_state == 'replaced':
        assert lock.read_text() == '987654321'
    else:
        assert not lock.exists()


@pytest.mark.parametrize('start', [SINCE - 1, NOW + 1])
def test_fetched_record_outside_window_is_counted_not_written(tmp_path, monkeypatch, capsys, start):
    corpus, _ = tiny_corpus(tmp_path)
    before = snapshot(corpus)
    record = fixture('stratz_response.json')['data']['m%d' % NEW]
    record['startDateTime'] = start
    offline_batches(monkeypatch, [{'data': {'m%d' % NEW: record}}])
    stats = run_batches(corpus, [NEW])
    assert stats['fetched'] == stats['errors'] == 1
    assert stats['new_written'] == stats['replaced'] == 0
    assert snapshot(corpus) == before
    assert 'outside window' in capsys.readouterr().out


def test_successful_batch_resets_consecutive_failures(tmp_path, monkeypatch, capsys):
    corpus, _ = tiny_corpus(tmp_path)
    # Control batching only; retain the captured IDs, bodies and real writer.
    monkeypatch.setattr(B, 'BATCH_SIZE', 1)
    failure = fixture('stratz_520_reconstructed.json')
    success = fixture('stratz_response.json')
    calls = offline_batches(monkeypatch, [failure, failure, success, failure, failure, success])
    stats = run_batches(corpus, [C, C, C, NEW, NEW, NEW])
    assert len(calls) == 6 and stats['errors'] == 4
    assert stats['fetched'] == 2 and stats['new_written'] == stats['replaced'] == 1
    assert capsys.readouterr().out.count('ВНИМАНИЕ:') == 4


def test_batch_transport_stop_is_immediate(tmp_path, monkeypatch):
    corpus, _ = tiny_corpus(tmp_path)
    before = snapshot(corpus)
    calls = offline_batches(monkeypatch, [{'message': 'API rate limit exceeded'},
                                         fixture('stratz_response.json')])
    with pytest.raises(B.TransportStop, match='rate limit'):
        run_batches(corpus, failed_batch_ids(1) + [C, NEW])
    assert len(calls) == 1 and snapshot(corpus) == before


def test_mutation_transport_stop_is_immediate(tmp_path, monkeypatch):
    # A rate-limit/HTTP stop on retryMatchDownload must stop the whole run (it is a
    # BaseException), not be swallowed by the per-id fail-open handler; the receipt
    # written before the request stays, so the mutation is never repeated.
    corpus, _ = tiny_corpus(tmp_path)
    monkeypatch.setattr(B, 'BATCH_SIZE', 2)
    other_null = failed_batch_ids(1)[0]
    null_batch = {'data': {'m%d' % NULL: None, 'm%d' % other_null: None}}
    calls = offline_batches(monkeypatch, [null_batch, {'message': 'API rate limit exceeded'},
                                         fixture('stratz_response.json')])
    with pytest.raises(B.TransportStop, match='rate limit'):
        run_batches(corpus, [NULL, other_null, C, NEW])
    assert len(calls) == 2 and calls[1].startswith('mutation')
    assert set(B.read_json(corpus / B.RETRY_FILE)) == {str(NULL)}
