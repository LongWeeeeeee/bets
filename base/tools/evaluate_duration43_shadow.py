"""One preregistered evaluation after enrollment closes and outcomes resolve."""
import argparse
import json
import hashlib
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / 'base')]

OUTCOME_SOURCES = {'stratz_team_result_cache', 'stratz_team_result_cache_stale',
                   'stratz_team_result_api', 'stratz_team_result_refresh',
                   'stratz_player_result', 'stratz_player_result_cache'}


def select_cohort(records, outcomes, experiment, min_maps=300, min_days=30):
    records = sorted((r for r in records if r.get('experiment_sha256') == experiment), key=lambda r: r['recorded_at'])
    if any(not math.isfinite(r['recorded_at']) or r['recorded_at'] <= 0 for r in records):
        raise ValueError('Invalid capture chronology')
    if len({r['match_id'] for r in records}) != len(records):
        raise ValueError('Duplicate capture IDs')
    stop = next((i + 1 for i, r in enumerate(records) if i + 1 >= min_maps and
                 r['recorded_at'] - records[0]['recorded_at'] >= min_days * 86400), None)
    if stop is None:
        return {'status': 'PENDING_ENROLLMENT', 'enrolled': len(records)}, []
    records = records[:stop]  # Endpoint selected without reading outcomes.
    selected = []; missing = []; excluded = []; hints = []
    for r in records:
        o = outcomes.get(r['match_id'])
        if o is None:
            missing.append(r['match_id']); continue
        start, end, observed = (float(o[k]) for k in ('start', 'end', 'observed_ts'))
        if o.get('schema_version') != 1 or o.get('source') not in OUTCOME_SOURCES:
            raise ValueError('Unknown outcome producer/schema')
        if not all(math.isfinite(t) for t in [start, end, observed]) or end <= start or observed < end:
            raise ValueError('Invalid outcome chronology')
        if not (r['recorded_at'] < start and observed > r['recorded_at']):
            excluded.append(r['match_id']); continue
        hint = r.get('start_hint')
        if isinstance(hint, (int, float)) and math.isfinite(hint) and hint != start:
            hints.append({'match_id': r['match_id'], 'seconds': hint-start})
        selected.append((r, end - start >= 2580))
    state = ('PENDING_OUTCOMES' if missing else
             'INSUFFICIENT_ELIGIBLE' if len(selected) < min_maps else 'READY')
    return {'status': state, 'enrolled': stop, 'eligible': len(selected),
            'missing_outcomes': missing, 'excluded_not_prestart': excluded,
            'start_hint_differences': hints}, selected


def main():
    from duration43_shadow import FrozenCandidate
    import numpy as np
    from catboost import CatBoostClassifier
    from sklearn.metrics import log_loss, roc_auc_score, brier_score_loss
    p = argparse.ArgumentParser()
    p.add_argument('--artifact-dir', type=Path, required=True)
    p.add_argument('--journal', type=Path, required=True)
    p.add_argument('--outcomes', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    if args.output.exists():
        raise ValueError('Use a new report path')
    c = FrozenCandidate(args.artifact_dir)
    journal_blob = args.journal.read_bytes()
    outcome_blob = args.outcomes.read_bytes()
    records = [json.loads(s) for s in journal_blob.splitlines() if s.strip()]
    outcomes = {}
    for line in outcome_blob.splitlines():
        o = json.loads(line); mid = str(o['match_id'])
        if mid in outcomes and (outcomes[mid]['start'], outcomes[mid]['end']) != (o['start'], o['end']):
            raise ValueError('Conflicting outcome; reconcile sources first')
        if mid not in outcomes or o['observed_ts'] < outcomes[mid]['observed_ts']:
            outcomes[mid] = o
    plan = c.manifest['evaluation']
    result, selected = select_cohort(records, outcomes, c.manifest_sha, plan['min_maps'], plan['min_days'])
    if result['status'] == 'READY':
        incumbent = CatBoostClassifier(); incumbent.load_model(str(args.artifact_dir / 'incumbent.cbm'))
        from duration43_shadow import sha256
        if sha256(args.artifact_dir / 'incumbent.cbm') != c.manifest['incumbent_sha256']:
            raise ValueError('Incumbent artifact changed')
        for r, y in selected:
            if (r['candidate_model_sha256'] != c.manifest['candidate_sha256']
                    or r['incumbent_model_sha256'] != c.manifest['incumbent_sha256']
                    or not c.manifest['built_at'] < r['capture_started_at'] <= r['recorded_at']
                    or not math.isfinite(r['game_time']) or r['game_time'] >= 0):
                raise ValueError('Invalid capture provenance')
            cx, raw, calibrated = c.predict(r['heroes'], r['accounts'])
            if (not np.array_equal(cx, np.asarray(r['candidate_vector'], dtype=np.float32))
                    or not abs(raw - r['candidate_raw']) < 1e-12
                    or not abs(calibrated - r['candidate_p']) < 1e-12):
                raise ValueError('Candidate replay mismatch')
            ix = np.asarray([float('nan') if v is None else v for v in r['incumbent_vector']], dtype=np.float32)
            old = float(incumbent.predict_proba(ix.reshape(1, -1), thread_count=1)[0, 1])
            spec = c.manifest['incumbent_spec']
            oldcal = float(np.interp(old, spec['knots_x'], spec['knots_y'])) if spec['knots_x'] else old
            if (r['incumbent_columns'] != c.manifest['incumbent_columns']
                    or not abs(old-r['incumbent_raw']) < 1e-12
                    or not abs(oldcal-r['incumbent_p']) < 1e-12):
                raise ValueError('Incumbent replay mismatch')
        y = np.array([v for r, v in selected], dtype=int)
        if len(np.unique(y)) != 2:
            result['status'] = 'INSUFFICIENT_CLASSES'
        else:
            result['metrics'] = {}
            for name in ['incumbent_p', 'candidate_p']:
                pred = np.array([r[name] for r, v in selected])
                result['metrics'][name] = {'logloss': log_loss(y, pred), 'auc': roc_auc_score(y, pred), 'brier': brier_score_loss(y, pred)}
            ps = [np.clip([r[n] for r, v in selected], 1e-9, 1-1e-9) for n in ['incumbent_p', 'candidate_p']]
            loss = [-(y*np.log(p)+(1-y)*np.log1p(-p)) for p in ps]
            days, idx = np.unique([int(r['recorded_at']//86400) for r,v in selected], return_inverse=True)
            counts = np.bincount(idx); sums = np.bincount(idx, weights=loss[1]-loss[0])
            draws = np.random.default_rng(302).integers(len(days), size=(5000,len(days)))
            boots = sums[draws].sum(1)/counts[draws].sum(1)
            result['paired_delta_logloss'] = {'mean': float(np.mean(loss[1]-loss[0])), 'ci95': np.quantile(boots,[.025,.975]).tolist(), 'ci99': np.quantile(boots,[.005,.995]).tolist()}
            result['status'] = 'COMPLETE'
    result['experiment_sha256'] = c.manifest_sha
    result['journal_sha256'] = hashlib.sha256(journal_blob).hexdigest()
    result['outcomes_sha256'] = hashlib.sha256(outcome_blob).hexdigest()
    result['enrollment_universe'] = 'First successfully committed paired captures, not all eligible attempts'
    result['limitations'] = [
        'No durable denominator of queue drops, failed writes or process-exit losses; missingness may depend on load',
        'Source-fetch freshness is unknown; actual STRATZ start is checked after outcome collection',
        'Conditional on complete draft/accounts and successful upstream panel; not all maps',
    ]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    tmp = args.output.with_suffix(args.output.suffix+'.tmp');tmp.write_text(json.dumps(result,indent=2,allow_nan=False)+'\n');tmp.replace(args.output)
    print(json.dumps(result,indent=2))


if __name__ == '__main__':
    main()
