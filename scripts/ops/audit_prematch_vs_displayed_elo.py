#!/usr/bin/env python3
"""Pair saved panel ML and displayed ELO, without rescoring or using ELO outcomes.

One earliest same-tick pair per real map ID; verdicts.prematch is mandatory.
prematch_index is not a substitute. Source clocks and exclusions stay visible.
"""
import argparse
import collections
import datetime as dt
import hashlib
import json
import math
import re
from pathlib import Path


def load_rows(path):
    return [dict(json.loads(line), _line=i) for i, line in
            enumerate(path.read_text().splitlines(), 1) if line.strip()]


def summarize(rows):
    n = len(rows)
    disagreement = [r for r in rows if r['disagree']]
    ml = sum(r['ml_correct'] for r in rows)
    elo = sum(r['elo_correct'] for r in rows)
    wins = sum(r['ml_correct'] for r in disagreement)
    nd = len(disagreement)
    # Exact paired test; independence between maps within a series is NOT assured.
    p = min(1., 2 * sum(math.comb(nd, k) for k in range(min(wins, nd-wins)+1)) / 2**nd)
    return {'n': n, 'ml_wins': ml, 'elo_wins': elo,
            'ml_wr': ml/n if n else None, 'elo_wr': elo/n if n else None,
            'disagreements': nd, 'disagreement_ml_wins': wins,
            'disagreement_elo_wins': nd-wins,
            'paired_exact_p_map_independence_only': p if nd else None}


def audit(directory, corpus, cutoff):
    sources = {}

    def bind(path):
        sources[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()

    dispatch_path = directory / 'ml_dispatch_decisions.jsonl'
    outcome_path = directory / 'prematch_model_outcomes.jsonl'
    eval_path = directory / 'prematch_model_eval.jsonl'
    for path in (dispatch_path, outcome_path, eval_path):
        bind(path)
    dispatch = load_rows(dispatch_path)
    first = {}
    for r in sorted(dispatch, key=lambda r: r['ts']):
        v = r.get('verdicts', {}).get('prematch')
        if not v or r.get('elo_diff') is None:
            continue
        mid = re.fullmatch(r'(?:https?://)?dltv\.org/matches/(\d+)', r['base_url'])
        if mid is None:
            raise ValueError('Unrecognized real map ID: ' + r['base_url'])
        assert v['side'] in ('Radiant', 'Dire') and .5 <= v['confidence'] <= 1
        assert math.isclose(r['elo_r']-r['elo_d'], r['elo_diff'], abs_tol=1e-7)
        first.setdefault(mid[1], r)

    labels, crosschecks = {}, collections.Counter()
    for r in load_rows(outcome_path):
        mid = str(r['match_id'])
        assert isinstance(r['radiant_win'], bool)
        if mid in labels:
            assert labels[mid]['radiant_win'] == r['radiant_win']
        labels[mid] = dict(r, source=str(outcome_path))
    for path in sorted(corpus.glob('7.41e_part*.json')):
        data = json.loads(path.read_text())
        relevant = set(data) & set(first)
        if not relevant:
            continue
        bind(path)
        for mid in relevant:
            r = data[mid]
            assert isinstance(r.get('didRadiantWin'), bool)
            if mid in labels:
                assert labels[mid]['radiant_win'] == r['didRadiantWin'], mid
                crosschecks['winner_agreement_with_raw'] += 1
                assert labels[mid]['start'] == r['startDateTime'], mid
                assert labels[mid]['end'] == r['startDateTime']+r['durationSeconds'], mid
            labels[mid] = {'radiant_win': r['didRadiantWin'], 'start': r['startDateTime'],
                           'end': r['startDateTime']+r['durationSeconds'],
                           'series': r.get('series'), 'source': str(path),
                           'radiant_team': r.get('radiantTeam'), 'dire_team': r.get('direTeam')}
            # If card-bound account IDs exist, verify source orientation by roster.
            slots = first[mid].get('draft_input', {}).get('slots', [])
            for side in ('radiant', 'dire'):
                accounts = {int(s['account_id']) for s in slots
                            if s.get('side') == side and s.get('account_id')}
                raw = {int(s['steamAccount']['id']) for s in r.get('players', [])
                       if s.get('isRadiant') == (side == 'radiant')
                       and (s.get('steamAccount') or {}).get('id')}
                if accounts and raw:
                    assert accounts == raw, (mid, side, accounts, raw)
                    crosschecks['roster_side_agreement'] += 1

    evaluations = collections.defaultdict(list)
    for r in load_rows(eval_path):
        if r.get('match_id'):
            evaluations[str(r['match_id'])].append(r)
    result, excluded = [], collections.defaultdict(list)
    for mid, r in first.items():
        label = labels.get(mid)
        reason = ('missing_outcome' if not label else
                  'at_or_after_source_end' if r['ts'] >= label['end'] else
                  'elo_tie' if r['elo_diff'] == 0 else None)
        if reason:
            excluded[reason].append(mid)
            continue
        v = r['verdicts']['prematch']
        elo_side = 'Radiant' if r['elo_diff'] > 0 else 'Dire'
        winner = 'Radiant' if label['radiant_win'] else 'Dire'
        # Attribution is auxiliary; pair itself comes from one dispatch record.
        matches = [e for e in evaluations[mid]
                   if 0 <= r['ts']-e['ts'] <= 120
                   and e.get('side', '').lower() == v['side'].lower()
                   and abs(e.get('confidence', -1)-v['confidence']) < .00011
                   and e.get('radiant_team') == r['teams']['radiant']
                   and e.get('dire_team') == r['teams']['dire']]
        ev = max(matches, key=lambda e: e['ts']) if matches else {}
        result.append({'match_id': mid, 'dispatch_line': r['_line'], 'ts': r['ts'],
                       'utc': dt.datetime.fromtimestamp(r['ts'], dt.timezone.utc).isoformat(),
                       'teams': r['teams'], 'ml_side': v['side'], 'confidence': v['confidence'],
                       'elo_side': elo_side, 'elo_diff': r['elo_diff'], 'winner': winner,
                       'ml_correct': v['side'] == winner, 'elo_correct': elo_side == winner,
                       'disagree': v['side'] != elo_side, 'k24': r['ts'] >= cutoff,
                       'game_time': r.get('game_time'), 'source_start': label['start'],
                       'source_end': label['end'], 'delay_after_source_start': r['ts']-label['start'],
                       'outcome_source': label['source'], 'series': label.get('series'),
                       'branch': ev.get('branch'), 'eval_line': ev.get('_line'),
                       'artifact_sha256': ev.get('artifact_sha256')})
    assert len({r['match_id'] for r in result}) == len(result)
    assert all((r['ml_correct'] != r['elo_correct']) == r['disagree'] for r in result)
    cohorts = {'all': result, 'legacy_elo': [r for r in result if not r['k24']],
               'k24': [r for r in result if r['k24']],
               'ml_ge60': [r for r in result if r['confidence'] >= .6],
               'k24_ml_ge60': [r for r in result if r['k24'] and r['confidence'] >= .6],
               'game_time_le0': [r for r in result if r['game_time'] is not None and r['game_time'] <= 0],
               'strict_before_source_start': [r for r in result if r['ts'] < r['source_start']]}
    for branch in sorted({r['branch'] for r in result if r['branch']}):
        cohorts['branch_'+branch] = [r for r in result if r['branch'] == branch]
    return {'method': 'earliest logged same-tick general-ML/displayed-ELO pair per real map ID',
            'k24_cutoff_utc': dt.datetime.fromtimestamp(cutoff, dt.timezone.utc).isoformat(),
            'dispatch_rows': len(dispatch), 'first_pairs': len(first),
            'excluded': dict(excluded), 'crosschecks': dict(crosschecks),
            'eval_attribution_count': sum(r['eval_line'] is not None for r in result),
            'summary': {name: summarize(rs) for name, rs in cohorts.items()},
            'rows': result, 'source_sha256': sources,
            'limitations': ['Same-series maps are dependent; exact p assumes independent maps.',
                            'Prediction clocks differ from source clocks; not strict prestart validation.',
                            'Logged model versions/branches are mixed; no hindsight rescoring.',
                            'Earliest available pair may not be the first displayed card.',
                            'No executed prices or ROI measured.']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--corpus', type=Path, required=True)
    parser.add_argument('--k24-cutoff', required=True, help='Verified deployment UTC ISO timestamp')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    cutoff = dt.datetime.fromisoformat(args.k24_cutoff.replace('Z', '+00:00'))
    if cutoff.tzinfo is None:
        parser.error('--k24-cutoff must include timezone')
    output = audit(args.directory, args.corpus, cutoff.timestamp())
    temporary = args.output.with_suffix(args.output.suffix+'.tmp')
    temporary.write_text(json.dumps(output, indent=2, ensure_ascii=False)+'\n')
    temporary.replace(args.output)
    print(json.dumps({k: v for k, v in output.items() if k not in ('rows', 'source_sha256')}, indent=2))
