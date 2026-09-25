"""Prospective kills-v3 panel comparison; never participates in serving decisions."""
from __future__ import annotations

import base64
import fcntl
import gc
import hashlib
import json
import logging
import math
import os
import queue
import threading
import time
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
TARGETS = ('w_5_15', 'w_10_20', 'w_15_25', 'w_20_30', 'rad_30_25', 'total_55_50')
EXTRA_TARGETS = ('dire_30_25',)
_QUEUE = queue.Queue(maxsize=16)
_START_LOCK = threading.Lock()
_LOAD_LOCK = threading.Lock()
_WORKER = None
_CACHED = None
_DISABLED = False
_WARNED = False
_CONSECUTIVE_ERRORS = 0
_STATE_CHECK_INTERVAL = 600
_STATUS = {'submitted': 0, 'dropped': 0, 'processed': 0, 'errors': 0}


class FeatureContractError(ValueError):
    """Candidate and live feature layouts cannot be joined safely."""


def enabled():
    return os.getenv('KV3_SHADOW_ENABLED', '0') == '1'


def status():
    return dict(_STATUS, queued=_QUEUE.qsize(), disabled=_DISABLED)


def _path(name, default):
    return Path(os.getenv(name, str(ROOT / default)))


class Candidate:
    def __init__(self, bundle):
        from catboost import CatBoostClassifier
        from base.kills_v3_serving import load_state

        directory = _path('KV3_SHADOW_DIR', 'ml-models/prematch_panel_kv3')
        self.state_path = _path('KV3_STATE_PATH', 'data/kills_v3_state/state.npz')
        feature_blob = (directory / 'feature_names.json').read_bytes()
        features = json.loads(feature_blob)
        self.panel_columns = features['panel_columns']
        self.kv3_columns = features['kv3_columns']
        self.source_names = features['kv3_source_names']
        if (self.panel_columns != list(bundle.columns) or len(self.panel_columns) != 928
                or not self.source_names or len(self.kv3_columns) != len(self.source_names)
                or len(set(self.source_names)) != len(self.source_names)
                or self.kv3_columns != ['kv3_' + name for name in self.source_names]):
            raise FeatureContractError('KV3 feature contract differs from panel columns or state T+P names')
        manifest_blob = (directory / 'manifest.json').read_bytes()
        parameters = json.loads(manifest_blob).get('od3_parameters')
        required = ('history_start', 'visibility_delay', 'half_life_days',
                    'pseudo_games', 'poisson_lr', 'elo_k')
        missing = [key for key in required if not isinstance(parameters, dict) or key not in parameters]
        if missing:
            raise FeatureContractError('KV3 model manifest missing od3_parameters: ' + ', '.join(missing))
        self.od3_parameters = parameters
        self.state_contract_error = None
        self.state_mtime, self.state_size = self._signature()
        self.state = load_state(self.state_path)
        self.cutoff = int(self.state.serving_meta['cutoff'])
        self._check_state_contract()
        self.last_state_check = time.monotonic()
        self.manifest_sha256 = hashlib.sha256(manifest_blob).hexdigest()
        self.models = {}
        self.calibration = {}
        from ml_panel import load_specs
        optional = {s.key for s in load_specs(directory)} & set(EXTRA_TARGETS)
        optional = tuple(key for key in EXTRA_TARGETS if key in optional
                         and os.getenv('ML_PANEL_KV3_DIRE', '1') != '0'
                         and (directory / (key + '.cbm')).is_file()
                         and (directory / (key + '.calib.json')).is_file())
        for key in TARGETS + optional:
            model = CatBoostClassifier()
            model.load_model(str(directory / (key + '.cbm')))
            calib = json.loads((directory / (key + '.calib.json')).read_text())
            knots_x = np.asarray(calib['knots_x'], dtype=np.float64)
            knots_y = np.asarray(calib['knots_y'], dtype=np.float64)
            if (len(knots_x) < 2 or len(knots_x) != len(knots_y)
                    or not np.isfinite(knots_x).all() or not np.isfinite(knots_y).all()
                    or not np.all(np.diff(knots_x) > 0)
                    or np.any(knots_y < 0) or np.any(knots_y > 1)):
                raise ValueError('Invalid calibration for ' + key)
            self.models[key] = model
            self.calibration[key] = (knots_x, knots_y)

    def _signature(self):
        stat = self.state_path.stat()
        return stat.st_mtime_ns, stat.st_size

    def _check_state_contract(self):
        meta = self.state.serving_meta
        builder = meta.get('builder_params')
        mismatches = []
        for key in ('history_start', 'visibility_delay', 'half_life_days',
                    'pseudo_games', 'poisson_lr', 'elo_k'):
            if key in ('half_life_days', 'pseudo_games', 'poisson_lr', 'elo_k'):
                actual = builder.get(key) if isinstance(builder, dict) else None
            else:
                actual = meta.get(key)
            expected = self.od3_parameters[key]
            if (type(actual) not in (int, float) or type(expected) not in (int, float)
                    or not math.isfinite(actual) or not math.isfinite(expected)
                    or actual != expected):
                mismatches.append(key)
        if mismatches:
            raise FeatureContractError('KV3 model/state parameters differ: ' + ', '.join(mismatches))
        self.tp_names = list(meta['tp_names'])
        positions = {name: i for i, name in enumerate(self.tp_names)}
        if len(positions) != 152 or any(name not in positions for name in self.source_names):
            raise FeatureContractError('KV3 feature contract differs from panel columns or state T+P names')
        self.tp_indices = [positions[name] for name in self.source_names]

    def refresh_state(self):
        """Check at most every ten minutes; a bad replacement waits for another change."""
        now = time.monotonic()
        if now - self.last_state_check < _STATE_CHECK_INTERVAL:
            return None
        self.last_state_check = now
        try:
            signature = self._signature()
        except OSError as exc:
            signature = None
            error = exc
        else:
            error = None
        if signature == (self.state_mtime, self.state_size):
            return None
        self.state_mtime, self.state_size = signature if signature is not None else (None, None)
        self.state = None
        self.cutoff = None
        self.state_contract_error = None
        gc.collect()
        if error is not None:
            return f'{type(error).__name__}: {error}'
        try:
            from base.kills_v3_serving import load_state
            self.state = load_state(self.state_path)
            self.cutoff = int(self.state.serving_meta['cutoff'])
            self._check_state_contract()
        except Exception as exc:  # noqa: BLE001 - keep the worker alive until next replacement
            self.state = None
            self.cutoff = None
            if isinstance(exc, FeatureContractError):
                self.state_contract_error = f'{type(exc).__name__}: {exc}'
            gc.collect()
            return f'{type(exc).__name__}: {exc}'
        return None

    def predict(self, x):
        result = {}
        for key in TARGETS:
            raw = float(self.models[key].predict_proba(x.reshape(1, -1), thread_count=1)[0, 1])
            xp, yp = self.calibration[key]
            p = float(np.interp(raw, xp, yp))
            if not math.isfinite(p) or not 0 <= p <= 1:
                raise ValueError('Invalid probability for ' + key)
            result[key] = p
        return result


def _candidate(bundle):
    global _CACHED
    with _LOAD_LOCK:
        if _CACHED is None:
            _CACHED = Candidate(bundle)
        elif _CACHED.panel_columns != list(bundle.columns):
            raise FeatureContractError('Live panel columns changed after KV3 load')
        return _CACHED


def _lock(stream):
    deadline = time.monotonic() + 2
    while True:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            if time.monotonic() >= deadline:
                raise TimeoutError('KV3 shadow journal lock timed out')
            time.sleep(.02)


def _append_features(row):
    path = _path('KV3_SHADOW_FEATURES', 'runtime/kv3_shadow_features.jsonl')
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as stream:
        _lock(stream)
        stream.write(json.dumps(row, allow_nan=False, separators=(',', ':')) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


def _append(row, path, features=None):
    # Commit the deduplicated summary before its optional feature vector.
    def json_finite(value):
        if isinstance(value, np.generic):
            value = value.item()
        if isinstance(value, (float, np.floating)) and not math.isfinite(value):
            return None
        if isinstance(value, dict):
            return {key: json_finite(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [json_finite(item) for item in value]
        return value

    map_key = row.get('map_id') or row.get('map_key')
    if map_key is None:
        map_key = str(row.get('ts')) + ':' + str(time.time_ns())
    record = dict(row, experiment_sha256=row.get('manifest_sha256') or 'kv3',
                  match_id=str(map_key))
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+', encoding='utf-8') as stream:
        _lock(stream)
        stream.seek(0)
        previous = stream.read()
        for line in previous.splitlines():
            try:
                old = json.loads(line)
            except ValueError:
                continue
            if ((old.get('experiment_sha256'), old.get('match_id'))
                    == (record['experiment_sha256'], record['match_id'])):
                return False
        record['recorded_at'] = time.time()
        stream.seek(0, 2)
        if previous and not previous.endswith('\n'):
            stream.write('\n')
        stream.write(json.dumps(json_finite(record), allow_nan=False, separators=(',', ':')) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
        if features is not None:
            try:
                _append_features(features)
            except Exception as exc:  # noqa: BLE001 - summary remains committed
                _STATUS['errors'] += 1
                logging.getLogger(__name__).warning('KV3 shadow feature write failed: %s', exc)
    return True


def _failure(row, exc, *, fatal=False):
    global _CONSECUTIVE_ERRORS, _DISABLED, _WARNED
    row['error'] = f'{type(exc).__name__}: {exc}'
    _STATUS['errors'] += 1
    if fatal:
        _DISABLED = True
    else:
        _CONSECUTIVE_ERRORS += 1
        _DISABLED = _CONSECUTIVE_ERRORS >= 50
    if _DISABLED and not _WARNED:
        logging.getLogger(__name__).warning('KV3 shadow disabled: %s', row['error'])
        _WARNED = True


def capture(context, heroes, accounts, bundle, incumbent_x, verdicts, *, candidate=None, path=None):
    """Direct worker entry point; catches all failures and writes an audit row."""
    global _CONSECUTIVE_ERRORS
    if not enabled() or _DISABLED:
        return 'disabled'
    started = time.monotonic()
    context = context or {}
    row = {'schema': 1, 'map_id': context.get('map_id') or context.get('match_id'),
           'map_key': context.get('map_key'),
           'teams': [context.get('radiant_team_id'), context.get('dire_team_id')],
           'ts': context.get('start_ts'), 'state_cutoff': None, 'state_mtime': None,
           'state_cutoff_age_hours': None, 'kv3_finite_share': None,
           'x_panel_nan_share': None, 'start_ts_source': context.get('start_ts_source', 'now_fallback'),
           'manifest_sha256': None, 'targets': {}, 'gate_reason': None,
           'overvisible_s': None, 'error': None}
    path = path or _path('KV3_SHADOW_JOURNAL', 'runtime/kv3_shadow.jsonl')
    feature_row = None
    try:
        teams = [int(v or 0) for v in row['teams']]
        if 0 in teams:
            row['gate_reason'] = 'no_team'
        else:
            try:
                candidate = candidate or _candidate(bundle)
            except Exception as exc:  # noqa: BLE001 - load or model contract failure
                _failure(row, exc, fatal=True)
            else:
                row['manifest_sha256'] = candidate.manifest_sha256
                reload_error = candidate.refresh_state()
                row['state_mtime'] = candidate.state_mtime
                row['state_cutoff'] = candidate.cutoff
                if reload_error is not None:
                    row['error'] = reload_error
                    _STATUS['errors'] += 1
                if candidate.state is None:
                    if candidate.state_contract_error:
                        row['gate_reason'] = 'state_contract_mismatch'
                        row['error'] = candidate.state_contract_error
                    else:
                        row['gate_reason'] = 'state_unavailable'
                else:
                    ts = int(row['ts'])
                    row['state_cutoff_age_hours'] = (time.time() - candidate.cutoff) / 3600.0
                    if ts <= candidate.cutoff:
                        row['gate_reason'] = 'state_newer_than_map'
                    else:
                        from base.kills_v3_serving import features_for_map
                        names, kv3 = features_for_map(candidate.state, teams[0], teams[1],
                                                      accounts[:5], accounts[5:], ts)
                        row['overvisible_s'] = getattr(candidate.state, 'serving_last_overvisible_seconds', None)
                        if names != candidate.tp_names or len(kv3) != 152:
                            raise FeatureContractError('KV3 query feature layout changed')
                        kv3 = np.asarray(kv3, dtype=np.float32).reshape(-1)
                        row['kv3_finite_share'] = float(np.isfinite(kv3).mean())
                        panel = np.asarray(incumbent_x, dtype=np.float32).reshape(-1)
                        row['x_panel_nan_share'] = float(np.isnan(panel).mean()) if len(panel) == 928 else None
                        if len(panel) != 928 or np.isinf(panel).any() or np.isinf(kv3).any():
                            raise ValueError('Invalid panel or KV3 vector')
                        joined = np.concatenate((panel, kv3[candidate.tp_indices])).reshape(1, -1)
                        predicted = candidate.predict(joined)
                        prod = {v.key: v.probability for v in verdicts}
                        row['targets'] = {key: {'p_b': predicted[key], 'p_prod': prod.get(key)}
                                          for key in TARGETS}
                        feature_row = {
                            'map_id': row['map_id'], 'ts': row['ts'], 'teams': row['teams'],
                            'state_cutoff': row['state_cutoff'],
                            'x_panel_f32_b64': base64.b64encode(panel.astype('<f4', copy=False).tobytes()).decode('ascii'),
                            'x_kv3_tp_f32_b64': base64.b64encode(kv3.astype('<f4', copy=False).tobytes()).decode('ascii'),
                        }
        _CONSECUTIVE_ERRORS = 0
    except Exception as exc:  # noqa: BLE001 - never enter the production failure path
        _failure(row, exc, fatal=isinstance(exc, FeatureContractError))
    row['elapsed_ms'] = (time.monotonic() - started) * 1000
    try:
        return 'recorded' if _append(row, path, feature_row) else 'duplicate'
    except Exception as exc:  # noqa: BLE001
        _failure(row, exc)
        return 'journal_error'


def _work(pending):
    while True:
        args = pending.get()
        try:
            if args is None:
                return
            _STATUS['last_result'] = capture(*args)
        except Exception as exc:  # noqa: BLE001
            _STATUS['errors'] += 1
            logging.getLogger(__name__).error('KV3 shadow worker failed: %s', exc)
        finally:
            _STATUS['processed'] += 1
            pending.task_done()


def submit(context, heroes, accounts, bundle, incumbent_x, verdicts):
    """Bounded, nonblocking submission from the panel observer."""
    global _WORKER
    try:
        if not enabled() or _DISABLED:
            return 'disabled'
        if not _START_LOCK.acquire(blocking=False):
            _STATUS['dropped'] += 1
            return 'busy'
        try:
            if _WORKER is None or not _WORKER.is_alive():
                _WORKER = threading.Thread(target=_work, args=(_QUEUE,), name='kv3-shadow', daemon=True)
                _WORKER.start()
        finally:
            _START_LOCK.release()
        _QUEUE.put_nowait((dict(context or {}), tuple(heroes), tuple(accounts), bundle,
                          np.asarray(incumbent_x, dtype=np.float32).copy(), tuple(verdicts)))
        _STATUS['submitted'] += 1
        return 'queued'
    except queue.Full:
        _STATUS['dropped'] += 1
        return 'queue_full'
    except Exception as exc:  # noqa: BLE001
        _STATUS['errors'] += 1
        logging.getLogger(__name__).error('KV3 shadow submit failed: %s', exc)
        return 'error'
