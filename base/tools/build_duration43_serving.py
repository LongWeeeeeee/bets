"""Package the exact E299 model/calibration and verified frozen history for serving."""
import argparse
import hashlib
import json
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--snapshot', type=Path, required=True)
    ap.add_argument('--confidence', type=Path, required=True)
    ap.add_argument('--output', type=Path, required=True)
    a = ap.parse_args()
    m = json.loads((a.snapshot / 'manifest.json').read_text())
    model = (a.snapshot / 'candidate.cbm').read_bytes()
    digest = hashlib.sha256(model).hexdigest()
    assert digest == m['candidate_sha256'] == 'b21442378c3c96d3a80a4b43630b08f906c4ecb556186b466009bbd3540f692c'
    assert m['max_history_end'] < m['built_at']
    keep = ['built_at', 'max_history_end', 'history_rows', 'history_from', 'candidate_origin',
            'candidate_source_sha256', 'candidate_sha256', 'hero_ids', 'platt', 'history', 'corpus_sha256']
    out = {k: m[k] for k in keep}
    out.update(schema='duration43-serving-v1', model_version='e299-august-platt-july-v1',
               target_seconds=2580, history_policy='Frozen completed history plus durable end/observation-causal increments',
               calibration_evidence=json.loads(a.confidence.read_text()),
               snapshot_sha256=hashlib.sha256((a.snapshot / 'manifest.json').read_bytes()).hexdigest())
    a.output.mkdir(parents=True, exist_ok=False)
    for name, blob in [('candidate.cbm', model), ('manifest.json', (json.dumps(out, separators=(',', ':'), allow_nan=False)+'\n').encode())]:
        tmp = a.output / (name + '.tmp')
        tmp.write_bytes(blob)
        tmp.replace(a.output / name)
    print(json.dumps({'model_sha256': digest, 'manifest_sha256': hashlib.sha256((a.output/'manifest.json').read_bytes()).hexdigest(), 'history_rows': out['history_rows']}))


if __name__ == '__main__':
    main()
