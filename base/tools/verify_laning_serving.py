#!/usr/bin/env python3
"""Verify actual model/history serving against sealed training and All probes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def sides(heroes, accounts):
    return [{f'pos{position + 1}': {'hero_id': int(heroes[offset + position]),
                                  'account_id': int(accounts[offset + position])}
             for position in range(5)} for offset in (0, 5)]


def verify(args):
    os.environ['WIN_MODEL_DIR'] = str(args.all_model_dir.resolve())
    from base import laning_serving as serving
    from base import win_model_veto as veto
    service = serving.LaningService(args.model_dir, args.history_dir)
    if not service._load():
        raise ValueError(service.error)
    manifest = service.history_store.manifest
    for name, digest in manifest['files_sha256'].items():
        if sha256(args.history_dir / name) != digest:
            raise ValueError(f'History SHA256 mismatch: {name}')
    with np.load(args.probe, allow_pickle=False) as archive:
        probe = {name: archive[name] for name in archive.files}
    serving._SERVICE = service
    history_delta = probability_delta = 0.0
    latencies = []
    preview = []
    for row, heroes in enumerate(probe['heroes']):
        accounts, timestamp = probe['accounts'][row], probe['ts'][row]
        history = service.history_store.history(heroes, accounts, timestamp)
        history_delta = max(history_delta, float(np.max(np.abs(history - probe['hc'][row]))))
        started = time.monotonic()
        probability = service.predict(heroes, accounts, timestamp)
        latencies.append(time.monotonic() - started)
        if probability is None:
            raise ValueError(service.error)
        probability_delta = max(probability_delta, float(np.max(
            np.abs(probability - probe['probabilities'][row]))))
        lines = serving.panel_lines(*sides(heroes, accounts), timestamp, draft_model=veto)
        if not lines['ml_laning_line'] or not lines['all_model_line']:
            raise ValueError(f'Actual panel adapter omitted a line: {lines}')
        if row < 3:
            preview.append(lines)
    if history_delta > 2e-6 or probability_delta > 1e-10:
        raise ValueError(f'Training/serving mismatch: history={history_delta}, p={probability_delta}')
    all_delta = 0.0
    with np.load(args.phase_probe, allow_pickle=False) as phase:
        for heroes, expected in zip(phase['heroes'], phase['all']):
            index = veto.win_index_draft(*sides(heroes, np.zeros(10, dtype=int)))
            if index is None:
                raise ValueError(veto.load_error())
            all_delta = max(all_delta, abs(index / 100 + .5 - float(expected)))
    if all_delta > 5.000001e-6:
        raise ValueError(f'All serving differs from phase probe: {all_delta}')
    return {'complete': True, 'probe_rows': len(probe['heroes']),
            'history_max_abs_delta': history_delta,
            'probability_max_abs_delta': probability_delta,
            'all_probability_max_abs_delta': all_delta,
            'prediction_seconds_p50': float(np.median(latencies)),
            'prediction_seconds_max': float(max(latencies)),
            'model_sha256': sha256(args.model_dir / 'team.cbm'),
            'history_manifest_sha256': sha256(args.history_dir / 'manifest.json'),
            'history_max_end_ts': manifest['max_end_ts'],
            'all_model_sha256': sha256(args.all_model_dir / 'radiant_win_model.joblib'),
            'all_encoder_sha256': sha256(args.all_model_dir / 'win_feature_encoder.joblib'),
            'preview': preview}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for argument in ('model-dir', 'history-dir', 'probe', 'all-model-dir', 'phase-probe', 'output'):
        parser.add_argument('--' + argument, type=Path, required=True)
    args = parser.parse_args()
    result = verify(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(args.output.name + '.tmp')
    temporary.write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n')
    os.replace(temporary, args.output)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
