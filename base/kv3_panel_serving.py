"""Serve the six kills-v3 panel models with an explicit A fallback."""
from __future__ import annotations

import dataclasses
import os
import threading
from collections import Counter
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
TARGETS = ('w_5_15', 'w_10_20', 'w_15_25', 'w_20_30', 'rad_30_25', 'total_55_50')
_lock = threading.RLock()
_candidate = None
_specs = None
_error = None
_last_reason = None
_counts = Counter()
_reported = set()
_failed_load = None
_failed_reload = None


def enabled() -> bool:
    return os.getenv('ML_PANEL_KV3', '0') == '1'


def _reason(reason: str) -> str:
    global _last_reason
    _last_reason = reason
    _counts[reason] += 1
    if reason not in _reported:
        _reported.add(reason)
        print(f'[kv3_panel] A fallback: {reason}', flush=True)
    return reason


def status() -> dict:
    with _lock:
        return {'ready': _candidate is not None and _specs is not None
                and _error is None and _last_reason is None,
                'error': _error, 'last_reason': _last_reason,
                'counters': dict(_counts),
                'state_cutoff': getattr(_candidate, 'cutoff', None)}


def _state_signature():
    path = Path(os.getenv('KV3_STATE_PATH', str(ROOT / 'data/kills_v3_state/state.npz')))
    try:
        stat = path.stat()
        return str(path), stat.st_mtime_ns, stat.st_size
    except OSError:
        return str(path), None, None


def _load(bundle):
    """Skip a failed initial load until its state file or bundle changes."""
    global _failed_load
    if _candidate is not None:
        return _load_uncached(bundle)
    signature = (str(Path(os.getenv('KV3_PANEL_DIR', str(ROOT / 'ml-models/prematch_panel_kv3')))),
                 _state_signature())
    if _failed_load is not None and _failed_load[0] == signature:
        raise _failed_load[1](_failed_load[2])
    try:
        result = _load_uncached(bundle)
    except Exception as exc:
        _failed_load = (signature, type(exc), str(exc))
        raise
    _failed_load = None
    return result


def _load_uncached(bundle):
    """Use the shadow loader's model, layout and state parameter checks."""
    global _candidate, _specs, _error
    import kv3_shadow
    from ml_panel import load_specs

    directory = Path(os.getenv('KV3_PANEL_DIR', str(ROOT / 'ml-models/prematch_panel_kv3')))
    if _candidate is None:
        # ML_PANEL_KV3 disables the shadow observer. Candidate's existing loader
        # reads KV3_SHADOW_DIR; scope its override to this locked first load.
        old = os.environ.get('KV3_SHADOW_DIR')
        os.environ['KV3_SHADOW_DIR'] = str(directory)
        try:
            new_candidate = kv3_shadow.Candidate(bundle)
        finally:
            if old is None:
                os.environ.pop('KV3_SHADOW_DIR', None)
            else:
                os.environ['KV3_SHADOW_DIR'] = old
        specs = {s.key: s for s in load_specs(directory)}
        if set(specs) != set(TARGETS):
            raise kv3_shadow.FeatureContractError('KV3 panel.json target set differs')
        incumbent = {s.key: s for s in bundle.specs}
        for key in TARGETS:
            if (key not in incumbent or specs[key].threshold != incumbent[key].threshold
                    or (specs[key].title, specs[key].positive, specs[key].negative)
                    != (incumbent[key].title, incumbent[key].positive, incumbent[key].negative)):
                raise kv3_shadow.FeatureContractError(f'{key}: B display contract differs from A')
            if len(new_candidate.models[key].feature_names_) != 1050:
                raise kv3_shadow.FeatureContractError(f'{key}: B model width differs')
            if tuple(specs[key].knots_x) != tuple(new_candidate.calibration[key][0]):
                raise kv3_shadow.FeatureContractError(f'{key}: panel calibration x differs')
            if tuple(specs[key].knots_y) != tuple(new_candidate.calibration[key][1]):
                raise kv3_shadow.FeatureContractError(f'{key}: panel calibration y differs')
        _candidate, _specs, _error = new_candidate, specs, None
    elif _candidate.panel_columns != list(bundle.columns):
        raise kv3_shadow.FeatureContractError('Live panel columns changed after KV3 load')
    return _candidate


def _reload(candidate):
    """Keep the previous state until the replacement loads and passes contract."""
    global _failed_reload
    signature = candidate._signature()
    if signature == (candidate.state_mtime, candidate.state_size):
        return
    failed_signature = (str(candidate.state_path), signature)
    if (_failed_reload is not None and _failed_reload[0] is candidate
            and _failed_reload[1] == failed_signature):
        raise _failed_reload[2](_failed_reload[3])
    from base.kills_v3_serving import load_state

    previous = candidate.state
    previous_names = getattr(candidate, 'tp_names', None)
    previous_indices = getattr(candidate, 'tp_indices', None)
    try:
        newer = load_state(candidate.state_path)
        cutoff = int(newer.serving_meta['cutoff'])
        candidate.state = newer
        candidate._check_state_contract()
    except Exception as exc:
        candidate.state = previous
        candidate.tp_names = previous_names
        candidate.tp_indices = previous_indices
        _failed_reload = (candidate, failed_signature, type(exc), str(exc))
        raise
    _failed_reload = None
    candidate.state_mtime, candidate.state_size = signature
    candidate.cutoff = cutoff


def _gate(candidate, context):
    if not context:
        return 'missing_start_ts'
    try:
        ts = int(context['start_ts'])
    except (KeyError, TypeError, ValueError, OverflowError):
        return 'missing_start_ts'
    try:
        teams = (int(context['radiant_team_id']), int(context['dire_team_id']))
    except (KeyError, TypeError, ValueError, OverflowError):
        return 'missing_context'
    if ts <= 0:
        return 'missing_start_ts'
    age = ts - candidate.cutoff
    if age <= 0:
        return 'state_newer_than_map'
    limit = int(os.getenv('KV3_PANEL_MAX_STATE_AGE_S', '259200'))
    if age > limit:
        return 'state_stale'
    return None


def prepare(bundle, context) -> tuple[bool, str | None]:
    """Decide whether A may skip SHAP before score() constructs its row."""
    global _error, _last_reason
    if not enabled():
        return False, None
    with _lock:
        try:
            start_ts = int(context['start_ts'])
        except (KeyError, TypeError, ValueError, OverflowError):
            start_ts = 0
        if start_ts <= 0:
            return False, _reason('missing_start_ts')
        try:
            candidate = _load(bundle)
            _reload(candidate)
            reason = _gate(candidate, context)
            if reason:
                return False, _reason(reason)
            _error = None
            _last_reason = None
            return True, None
        except Exception as exc:  # noqa: BLE001 - panel must retain A
            _error = f'{type(exc).__name__}: {exc}'
            return False, _reason(_error)


def fallback(verdicts, reason):
    """Make the incumbent source visible in text and schema-4 journal metadata."""
    reason = reason or 'B_unavailable'
    return [dataclasses.replace(v, title=v.title + ' (старая)',
                                metadata=dict(v.metadata or {}, model='A_fallback', reason=reason))
            if v.key in TARGETS else v for v in verdicts]


def replace_verdicts(bundle, x, verdicts, context, *, prod35_names=(),
                     draft_keys=(), feature_vector=None, candidate=None):
    """Replace six verdicts in place; feature_vector is a boundary test seam."""
    if not enabled():
        return list(verdicts)
    with _lock:
        try:
            candidate = candidate or _load(bundle)
            if feature_vector is None:
                _reload(candidate)
                reason = _gate(candidate, context)
                if reason:
                    return fallback(verdicts, _reason(reason))
                from base.kills_v3_serving import features_for_map

                names, tp = features_for_map(
                    candidate.state, int(context['radiant_team_id']),
                    int(context['dire_team_id']), context['radiant_accounts'],
                    context['dire_accounts'], int(context['start_ts']), strict=False)
                if names != candidate.tp_names or len(tp) != 152:
                    raise ValueError('KV3 query feature layout changed')
                kv3 = np.asarray(tp, dtype=np.float32)[candidate.tp_indices]
            else:
                kv3 = np.asarray(feature_vector, dtype=np.float32)
            panel = np.asarray(x, dtype=np.float32).reshape(-1)
            if len(panel) != 928 or len(kv3) != 122 or np.isinf(panel).any() or np.isinf(kv3).any():
                raise ValueError('Invalid panel or KV3 vector')
            row = np.concatenate((panel, kv3)).reshape(1, -1)
            from ml_panel import evaluate
            from prematch_panel_scorer import draft_mask, shap_values, draft_share, group_shares

            mask = draft_mask(tuple(candidate.panel_columns + candidate.kv3_columns),
                              prod35_names) if draft_keys else None
            old = {v.key: v for v in verdicts}
            replacements = {}
            for key in TARGETS:
                a = old.get(key)
                if a is None:
                    raise ValueError(f'A verdict missing: {key}')
                spec = _specs[key] if candidate is _candidate else candidate.specs[key]
                model = candidate.models[key]
                raw = float(model.predict_proba(row, thread_count=1)[0, 1])
                if not np.isfinite(raw):
                    raise ValueError(f'Invalid raw probability: {key}')
                share, parts = None, {}
                if key in draft_keys:
                    sv = shap_values(model, row, row.shape[1])
                    share = draft_share(model, row, mask, values=sv,
                                        toward_positive=spec.calibrate(raw) >= .5)
                    sides = {str(spec.positive), str(spec.negative)} == {'Radiant', 'Dire'}
                    parts = group_shares(model, row,
                                         candidate.panel_columns + candidate.kv3_columns,
                                         prod35_names, values=sv,
                                         flip=(not sides) and spec.calibrate(raw) < .5)
                b = evaluate(spec, raw, {}, draft_share=share, parts=parts,
                             fill=a.fill, missing=a.missing)
                replacements[key] = dataclasses.replace(
                    b, metadata={'model': 'B_kv3', 'bundle_sha': candidate.manifest_sha256,
                                 'state_cutoff': candidate.cutoff,
                                 'start_ts_source': context.get('start_ts_source'),
                                 'team_ids_known': bool(int(context.get('radiant_team_id', 0))
                                                        and int(context.get('dire_team_id', 0))),
                                 'kv3_overvisible_s': (int(candidate.state.serving_last_overvisible_seconds)
                                     if hasattr(candidate.state, 'serving_last_overvisible_seconds') else None),
                                 'a_p': a.probability, 'a_ok': a.ok})
            _counts['served'] += 1
            return [replacements.get(v.key, v) for v in verdicts]
        except Exception as exc:  # noqa: BLE001 - no partial replacement
            return fallback(verdicts, _reason(f'{type(exc).__name__}: {exc}'))
