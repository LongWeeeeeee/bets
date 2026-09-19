"""Prospective paired duration capture. Never influences serving decisions."""
from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import queue
import threading
import time
from dataclasses import asdict
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
_LOCK = threading.Lock()
_CACHED = None
_QUEUE = queue.Queue(maxsize=16)
_START_LOCK = threading.Lock()
_WORKER = None
_STATUS = {'submitted': 0, 'dropped': 0, 'processed': 0, 'errors': 0}


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def enabled():
    return os.getenv('DURATION43_SHADOW_ENABLED', '0') == '1'


def status():
    """Best-effort process-local health, not a durable coverage denominator."""
    return dict(_STATUS, queued=_QUEUE.qsize())


def _work(pending):
    while True:
        args = pending.get()
        try:
            if args is None:  # Bounded shutdown for tests; runtime worker is a daemon.
                return
            _STATUS['last_result'] = capture(*args)
        except Exception as exc:
            _STATUS['errors'] += 1
            _STATUS['last_result'] = type(exc).__name__ + ': ' + str(exc)
        finally:
            _STATUS['processed'] += 1
            pending.task_done()


def submit(context, heroes, accounts, bundle, incumbent_x, verdicts):
    """Copy a bounded row; all artifact/inference/journal work stays off serving."""
    global _WORKER
    if not enabled():
        return 'disabled'
    if not _START_LOCK.acquire(blocking=False):
        _STATUS['dropped'] += 1
        return 'busy'
    try:
        if _WORKER is None or not _WORKER.is_alive():
            _WORKER = threading.Thread(target=_work, args=(_QUEUE,), name='duration43-shadow', daemon=True)
            _WORKER.start()
    finally:
        _START_LOCK.release()
    ctx = dict(context or {})
    ctx['submitted_at'] = time.time()  # Local observation; never a source fetch time.
    try:
        _QUEUE.put_nowait((ctx, tuple(heroes), tuple(accounts), bundle,
                          np.asarray(incumbent_x, dtype=np.float32).copy(), tuple(verdicts)))
    except queue.Full:
        _STATUS['dropped'] += 1
        return 'queue_full'
    _STATUS['submitted'] += 1
    return 'queued'


def candidate_vector(heroes, accounts, manifest):
    """Unsigned hero + role columns, then frozen 32 duration-history features."""
    ids = manifest['hero_ids']
    lookup = {int(h): i for i, h in enumerate(ids)}
    width = len(ids)
    draft = np.zeros(6 * width, dtype=np.float32)
    for slot, hero in enumerate(heroes):
        j = lookup.get(int(hero))
        if j is not None:  # Same unknown-hero zero encoding as training.
            draft[j] += 1
            draft[width + (slot % 5) * width + j] += 1
    parts = []
    for name, keys in [('accounts', accounts), ('heroes', heroes)]:
        stats = []
        for key in keys:
            dur, over43, over36, count = manifest['history'][name].get(str(int(key)), [0, 0, 0, 0])
            stats.append([(dur + 720) / (count + 20), (over43 + 6) / (count + 20),
                          (over36 + 10) / (count + 20), math.log1p(count)])
        a = np.asarray(stats, dtype=np.float64)
        parts.extend([a.mean(0), a.std(0), a.min(0), a.max(0)])
    return np.concatenate([draft, np.concatenate(parts).astype(np.float32)])


class FrozenCandidate:
    def __init__(self, directory):
        from catboost import CatBoostClassifier
        directory = Path(directory)
        manifest_path = directory / 'manifest.json'
        blob = manifest_path.read_bytes()
        self.manifest_sha = hashlib.sha256(blob).hexdigest()
        self.manifest = json.loads(blob)
        if self.manifest['schema'] != 'duration43-shadow-v1':
            raise ValueError('Unsupported shadow manifest')
        path = directory / 'candidate.cbm'
        expected = self.manifest['candidate_sha256']
        if sha256(path) != expected:
            raise ValueError('Candidate artifact hash mismatch')
        self.model = CatBoostClassifier()
        self.model.load_model(str(path))
        if sha256(path) != expected:
            raise ValueError('Candidate changed while loading')

    def predict(self, heroes, accounts):
        x = candidate_vector(heroes, accounts, self.manifest)
        raw = float(self.model.predict_proba(x.reshape(1, -1), thread_count=1)[0, 1])
        clipped = min(max(raw, 1e-6), 1 - 1e-6)
        a, b = self.manifest['platt']
        logit = a * math.log(clipped / (1 - clipped)) + b
        p = 1 / (1 + math.exp(-logit))
        return x, raw, p


def _candidate():
    global _CACHED
    if _CACHED is None:
        directory = os.getenv('DURATION43_SHADOW_DIR', str(ROOT / 'ml-models/duration43_shadow'))
        _CACHED = FrozenCandidate(directory)
    return _CACHED


def _append_first(path, row):
    """One durable prediction per experiment/map across threads and processes."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+', encoding='utf-8') as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        stream.seek(0)
        text = stream.read()
        for line in text.splitlines():
            try:
                old = json.loads(line)
            except ValueError:
                continue  # Preserve torn audit bytes; never truncate.
            if (old.get('experiment_sha256'), old.get('match_id')) == (row['experiment_sha256'], row['match_id']):
                return False
        stream.seek(0, 2)
        if text and not text.endswith('\n'):
            stream.write('\n')
        # Persist completion time, not just start of possibly slow inference.
        row['recorded_at'] = time.time()
        stream.write(json.dumps(row, allow_nan=False, separators=(',', ':')) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    return True


def capture(context, heroes, accounts, bundle, incumbent_x, verdicts, *, candidate=None, path=None):
    """Record only negative-clock complete drafts; evaluator confirms actual start."""
    if not enabled():
        return 'disabled'
    context = context or {}
    mid = context.get('match_id')
    gt = context.get('game_time')
    if (isinstance(mid, bool) or not str(mid).isdigit() or int(mid) <= 0
            or isinstance(gt, bool) or not isinstance(gt, (int, float))
            or not math.isfinite(gt) or gt >= 0):
        return 'not_pregame'
    if (len(heroes) != 10 or len(accounts) != 10 or len(set(heroes)) != 10
            or len(set(accounts)) != 10 or min(heroes) <= 0 or min(accounts) <= 0):
        return 'incomplete_draft'
    v = next((v for v in verdicts if v.key == 'dur43'), None)
    if v is None or v.raw is None:
        return 'missing_incumbent'
    with _LOCK:
        candidate = candidate or _candidate()
        manifest = candidate.manifest
        now = time.time()
        if now <= manifest['built_at'] or manifest['max_history_end'] >= manifest['built_at']:
            raise ValueError('History must precede prospective capture')
        loaded_hash = bundle.artifact_hashes.get('dur43')
        if loaded_hash != manifest['incumbent_sha256']:
            raise ValueError('Incumbent version changed; new protocol required')
        spec = next(s for s in bundle.specs if s.key == 'dur43')
        if json.loads(json.dumps(asdict(spec))) != manifest['incumbent_spec'] or list(bundle.columns) != manifest['incumbent_columns']:
            raise ValueError('Incumbent calibration or columns changed')
        x = np.asarray(incumbent_x, dtype=np.float32).reshape(-1)
        if len(x) != len(bundle.columns) or np.isinf(x).any():
            raise ValueError('Invalid incumbent vector')
        replay = float(bundle.models['dur43'].predict_proba(x.reshape(1, -1), thread_count=1)[0, 1])
        if abs(replay - v.raw) > 1e-12 or abs(spec.calibrate(replay) - v.probability) > 1e-12:
            raise ValueError('Incumbent vector/probability mismatch')
        cx, raw, calibrated = candidate.predict(heroes, accounts)
        if not np.isfinite(cx).all() or not all(math.isfinite(p) and 0 <= p <= 1 for p in [raw, calibrated, v.raw, v.probability]):
            raise ValueError('Invalid paired prediction')
        row = {'schema': 1, 'experiment_sha256': candidate.manifest_sha,
               'match_id': str(int(mid)), 'map_key': context.get('map_key'),
               'capture_started_at': now, 'game_time': gt,
               'submitted_at': context.get('submitted_at'),
               'start_hint': context.get('elo_evaluation_timestamp'),
               'source_observed_at': context.get('source_observed_at'),
               'heroes': list(map(int, heroes)), 'accounts': list(map(int, accounts)),
               'positions': [1, 2, 3, 4, 5] * 2,
               'candidate_model_sha256': manifest['candidate_sha256'],
               'incumbent_model_sha256': loaded_hash,
               'incumbent_columns': list(bundle.columns),
               'incumbent_vector': [None if np.isnan(t) else float(t) for t in x],
               'candidate_vector': cx.tolist(),
               'incumbent_raw': float(v.raw), 'incumbent_p': float(v.probability),
               'candidate_raw': raw, 'candidate_p': calibrated,
               'incumbent_fill': float(v.fill), 'incumbent_missing': list(v.missing),
               'eligibility': 'pending_outcome_start_confirmation'}
        dest = path or os.getenv('DURATION43_SHADOW_JOURNAL', str(ROOT / 'runtime/duration43_shadow.jsonl'))
        return 'recorded' if _append_first(dest, row) else 'duplicate'
