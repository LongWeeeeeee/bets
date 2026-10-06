"""Manual capture helper; never run by pytest. Writes only this fixture folder."""
import asyncio
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'scripts/pro_chain'))
sys.path.insert(0, '/Users/alex/Documents/ingame/base')
import maps_research as M
import backfill_by_id as B

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--offline', action='store_true', help='Only extract already captured local bodies')
args = parser.parse_args()
HERE = Path(__file__).resolve().parent
source = Path('/Users/alex/Documents/ingame/runtime/artifacts/misc/corpus_gap_20261005')
page_records = json.loads((source / 'od_promatches.json').read_text())
for n in range(0, len(page_records), 100):
    (HERE / ('opendota_page_%02d.json' % (n // 100))).write_text(json.dumps(page_records[n:n + 100]))
part = Path('/Users/alex/Documents/ingame/pro_heroes_data/json_parts_split_from_object/7.41e_part039.json')
with part.open() as fh:
    stored = json.load(fh)
(HERE / 'stored_c.json').write_text(json.dumps(stored['8991024043']))
normal = next(m for m in stored.values() if B.complete(m))
(HERE / 'stored_normal.json').write_text(json.dumps(normal))
ids = [8991024043, 9010162791, 9021397264]
query = 'query { ' + ' '.join('m%d: match(id: %d) { %s }' % (mid, mid, B.MATCH_FIELDS) for mid in ids) + ' }'
# Save the exact response body before the shared pool consumes it.
original = M.cf_requests.post
calls = [0]
def capture(*args, **kwargs):
    calls[0] += 1
    response = original(*args, **kwargs)
    if response.status_code not in (429, 522):
        (HERE / 'stratz_response.json').write_text(response.text)
    return response
provenance = {
    'capture_date_utc': datetime.now(timezone.utc).isoformat(),
    'capture_command': '/Users/alex/Documents/ingame/venv_catboost/bin/python3 tests/fixtures/backfill_by_id/capture.py' + (' --offline' if args.offline else ''),
    'stratz_query': query,
    'opendota': {
        'capture_date': '2026-10-05',
        'reproduction_command': '/Users/alex/Documents/ingame/venv_catboost/bin/python3 ' + str(source / 'od_promatches.py'),
        'original_capture_command_verified': False,
        'original_capture_script': (source / 'od_promatches.py').read_text(),
        'source': str(source / 'od_promatches.json'),
        'source_sha256': hashlib.sha256((source / 'od_promatches.json').read_bytes()).hexdigest(),
        'extraction': 'Each contiguous 100-record slice is one original response page; 800 records = 8 pages.'},
    'stored_part': {'source': str(part), 'match_id': 8991024043,
                    'extraction': "json.load(open(part))['8991024043']", 'normal_id': normal['id']},
}
try:
    if not args.offline:
        M.cf_requests.post = capture
        with B.bounded_stratz(M, 1):
            result = asyncio.run(B.stratz(M, query))
        print('Stratz:', {key: ('null' if value is None else 'positions=%d' % sum(p.get('position') is not None for p in value['players'])) for key, value in result.items()})
    provenance['stratz_status'] = 'NOT_CAPTURED' if args.offline else 'CAPTURED'
except BaseException as exc:
    provenance['stratz_status'] = 'CAPTURE_FAILED'
    provenance['stratz_error'] = str(exc)
    raise
finally:
    M.cf_requests.post = original
    provenance['stratz_physical_calls'] = calls[0]
    provenance['files'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in HERE.glob('*.json') if p.name != 'provenance.json'}
    (HERE / 'provenance.json').write_text(json.dumps(provenance, indent=2))
    print('Stratz physical calls:', calls[0])
