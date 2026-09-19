"""E299 duration >=43: causal draft/history inputs, July Platt, display only."""
from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import threading
import time
from dataclasses import asdict
from pathlib import Path

import numpy as np

from duration43_shadow import candidate_vector
from ml_panel import ModelVerdict

ROOT = Path(__file__).resolve().parents[1]
_LOCK = threading.Lock()
_MODEL = None
_STATUS = {}
_ROWS = {}
_STAMP = None
_PREDICTIONS = {}
_PREDICTION_PATH = None
_PREDICTION_STAMP = None


def finish_partial_line(stream):
    """Keep a prior interrupted append from swallowing the next JSON record."""
    stream.seek(0, 2)
    size = stream.tell()
    if size:
        stream.seek(size - 1)
        if stream.read(1) != '\n':
            stream.write('\n')


def history_path():
    return Path(os.getenv('DURATION43_HISTORY', str(ROOT / 'runtime/duration43_completed.jsonl')))


def completed_row(match, observed_at):
    """Canonical STRATZ result -> minimal history; incomplete cards are excluded."""
    try:
        mid = int(match['id'])
        start, end, dur = (int(match[k]) for k in ('startDateTime', 'endDateTime', 'durationSeconds'))
        players = match['players']
        heroes = [int(p['heroId']) for p in players]
        accounts = [max(0, int(p.get('steamAccountId') or (p.get('steamAccount') or {}).get('id') or 0)) for p in players]
        # End is also required to agree with the duration-based training boundary.
        effective_end = max(end, start + dur)
        if (mid <= 0 or int(match.get('leagueId') or 0) <= 0 or dur <= 0 or start <= 0 or end <= start
                or effective_end >= observed_at or len(heroes) != 10
                or min(heroes) <= 0 or len(set(heroes)) != 10
                or not isinstance(match.get('didRadiantWin'), bool)):
            return None
        return dict(match_id=str(mid), start=start, end=effective_end, duration=dur,
                    heroes=heroes, accounts=accounts, observed_at=float(observed_at))
    except (ValueError, TypeError, KeyError):
        return None


def record_completed(match, *, path=None, now=None):
    """Append durable outcomes independently of the three-day prematch delta prune."""
    global _STAMP
    if path is None and os.getenv('DURATION43_SERVING', '1') == '0':
        return False
    row = completed_row(match, time.time() if now is None else now)
    if row is None:
        return False
    target = Path(path) if path is not None else history_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    with _LOCK:
        if path is None and row['end'] <= model().manifest['max_history_end']:
            return False
        with target.open('a+') as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            rows = read_rows(target)
            if row['match_id'] in rows:
                return False
            finish_partial_line(f)
            f.write(json.dumps(row, separators=(',', ':'), allow_nan=False) + '\n')
            f.flush()
            rows[row['match_id']] = row
            stat = target.stat()
            _STAMP = (str(target), stat.st_ino, stat.st_mtime_ns, stat.st_size)
    return True


def read_rows(path):
    """Cache on file identity/size; first observation wins, duplicates count once."""
    global _STAMP, _ROWS
    path = Path(path)
    try:
        stat = path.stat()
        stamp = (str(path), stat.st_ino, stat.st_mtime_ns, stat.st_size)
    except FileNotFoundError:
        return {}
    if stamp != _STAMP:
        rows = {}
        with path.open() as f:
            for line in f:
                try:
                    row = json.loads(line)
                    rows.setdefault(str(row['match_id']), row)
                except (ValueError, KeyError, TypeError):
                    continue  # An incomplete final write is retried after size changes.
        _ROWS, _STAMP = rows, stamp
    return _ROWS


def prediction_store():
    """Persist the first complete pregame draft so live cards retain that forecast."""
    global _PREDICTION_PATH, _PREDICTIONS, _PREDICTION_STAMP
    path = Path(os.getenv('DURATION43_PREDICTIONS', str(ROOT / 'runtime/duration43_predictions.jsonl')))
    stat = path.stat() if path.exists() else None
    stamp = (stat.st_ino, stat.st_mtime_ns, stat.st_size) if stat else None
    if path != _PREDICTION_PATH or stamp != _PREDICTION_STAMP:
        values = {}
        if path.exists():
            with path.open() as f:
                for line in f:
                    try:
                        row = json.loads(line)
                        values.setdefault(row['key'], row['verdict'])
                    except (ValueError, KeyError, TypeError):
                        continue
        _PREDICTION_PATH, _PREDICTION_STAMP, _PREDICTIONS = path, stamp, values
    return path, _PREDICTIONS


def persist_first_prediction(path, signature, verdict):
    """Recheck under a process-shared lock; both callers must use the first row."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.seek(0)
        for line in f:
            try:
                row = json.loads(line)
                if row['key'] == signature:
                    return ModelVerdict(**row['verdict'])
            except (ValueError, KeyError, TypeError):
                continue
        finish_partial_line(f)
        f.write(json.dumps(dict(key=signature, verdict=asdict(verdict)), allow_nan=False) + '\n')
        f.flush()
    return verdict


class DurationModel:
    def __init__(self, directory):
        from catboost import CatBoostClassifier
        directory = Path(directory)
        blob = (directory / 'manifest.json').read_bytes()
        self.manifest_sha = hashlib.sha256(blob).hexdigest()
        self.manifest = json.loads(blob)
        m = self.manifest
        if m['schema'] != 'duration43-serving-v1' or not m['max_history_end'] < m['built_at']:
            raise ValueError('Invalid duration43 manifest')
        model_path = directory / 'candidate.cbm'
        if hashlib.sha256(model_path.read_bytes()).hexdigest() != m['candidate_sha256']:
            raise ValueError('Duration43 artifact hash mismatch')
        self.model = CatBoostClassifier()
        self.model.load_model(str(model_path))
        if hashlib.sha256(model_path.read_bytes()).hexdigest() != m['candidate_sha256']:
            raise ValueError('Duration43 artifact changed during load')

    def predict(self, heroes, accounts, cutoff, match_id, rows):
        m = self.manifest
        if (len(heroes) != 10 or len(accounts) != 10 or min(heroes) <= 0
                or len(set(heroes)) != 10 or not math.isfinite(cutoff)
                or cutoff <= m['built_at']):
            raise ValueError('Invalid draft or as-of precedes frozen history availability')
        # Copy only queried keys. Unknown accounts <=0 retain the training prior.
        h = {kind: {str(int(k)): list(m['history'][kind].get(str(int(k)), [0., 0., 0., 0.]))
                    for k in keys if int(k) > 0}
             for kind, keys in [('heroes', heroes), ('accounts', accounts)]}
        added, latest = 0, m['max_history_end']
        for mid, row in rows.items():
            end = row['end']
            if (str(mid) == str(match_id) or not m['max_history_end'] < end < cutoff
                    or row['observed_at'] >= cutoff):
                continue
            dur = row['duration']
            value = [dur / 60., float(dur >= 2580), float(dur >= 2160), 1.]
            for kind in ('heroes', 'accounts'):
                for key in row[kind]:
                    old = h[kind].get(str(key))
                    if old is not None:
                        h[kind][str(key)] = [a + b for a, b in zip(old, value)]
            added += 1
            latest = max(latest, end)
        x = candidate_vector(heroes, accounts, dict(m, history=h))
        raw = float(self.model.predict_proba(x.reshape(1, -1), thread_count=1)[0, 1])
        clipped = np.clip(raw, 1e-6, 1 - 1e-6)
        a, b = m['platt']
        p = 1 / (1 + math.exp(-(a * math.log(clipped / (1 - clipped)) + b)))
        metadata = dict(version=m['model_version'], model_sha256=m['candidate_sha256'],
                        manifest_sha256=self.manifest_sha, asof=cutoff, history_end=latest,
                        added_maps=added, known_accounts=sum(int(k) > 0 for k in accounts),
                        target='duration_seconds >= 2580', calibration='Platt on July; no August fit')
        return x, raw, p, metadata


def model():
    global _MODEL
    if _MODEL is None:
        _MODEL = DurationModel(os.getenv('DURATION43_MODEL_DIR', str(ROOT / 'ml-models/duration43_production')))
        print(f"[duration43] loaded {_MODEL.manifest['model_version']} model_sha={_MODEL.manifest['candidate_sha256']}", flush=True)
    return _MODEL


def replace_verdict(verdicts, heroes, accounts, context, now=None):
    """Replace only dur43; unavailable candidate never masquerades as old model."""
    global _MODEL
    if os.getenv('DURATION43_SERVING', '1') == '0':
        return list(verdicts)
    out = [v for v in verdicts if v.key != 'dur43']
    try:
        now = float(time.time() if now is None else now)
        context = context or {}
        gt = context.get('game_time')
        if isinstance(gt, bool) or not isinstance(gt, (int, float)) or not math.isfinite(gt):
            raise ValueError('Missing game clock')
        mid = context.get('match_id')
        if isinstance(mid, bool) or not str(mid).isdigit() or int(mid) <= 0:
            raise ValueError('Missing real match ID')
        with _LOCK:
            model()
            signature = hashlib.sha256(json.dumps([_MODEL.manifest_sha, str(mid), heroes, accounts]).encode()).hexdigest()
            path, saved = prediction_store()
            if signature in saved:
                verdict = ModelVerdict(**saved[signature])
            else:
                # A wall-clock ELO fallback is not actual map start. Never build a new
                # prematch forecast for an already-running card; reuse its saved draft.
                if gt >= 0:
                    raise ValueError('No saved pregame forecast for this draft')
                cutoff = float(context.get('observed_at', now))
                if not 0 < cutoff <= now:
                    raise ValueError('Invalid observation timestamp')
                _, raw, p, metadata = _MODEL.predict(heroes, accounts, cutoff, mid, read_rows(history_path()))
                metadata['match_id'] = str(mid)
                metadata['observed_game_time'] = gt
                metadata['input_sha256'] = signature
                band = next((b for b in _MODEL.manifest['calibration_evidence']['bins'] if b['lo'] <= p < b['hi']), {})
                verdict = ModelVerdict(key='dur43', title='до карты ≥43 мин', side='да' if p >= .5 else 'нет',
                                       probability=p, raw=raw, threshold=1., fill=1., ok=False,
                                       band_hit=band.get('wr'), band_n=band.get('n', 0), metadata=metadata)
                verdict = persist_first_prediction(path, signature, verdict)
                saved[signature] = asdict(verdict)
                print(f"[duration43] pregame map={mid} p_ge43={verdict.probability:.6f} asof={verdict.metadata['asof']}", flush=True)
        out.append(verdict)
        _STATUS.update(ready=True, error=None, **verdict.metadata)
    except Exception as exc:
        _STATUS.update(ready=False, error=f'{type(exc).__name__}: {exc}')
    return out


def status():
    return dict(_STATUS)
